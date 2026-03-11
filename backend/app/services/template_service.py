"""
services/template_service.py
═══════════════════════════════════════════════════════════════════════════════
Serwis Szablonów — System Windykacja

Odpowiedzialność:
    - Pełny CRUD szablonów (get_list, get_by_id, create, update, deactivate)
    - Walidacja i sanityzacja danych wejściowych (NFC, długości, typy)
    - Reguła biznesowa: Subject wymagany dla email, NULL dla sms/print
    - Reguła biznesowa: TemplateName globalnie unikalne
    - Soft-delete (is_active=False) — nigdy fizyczny DELETE z bazy
    - Cache Redis:
        templates:{id}              TTL 300s — pojedynczy szablon
        templates:list:{hash}       TTL 60s  — lista z paginacją
        templates:type:{type}       TTL 120s — lista aktywnych per typ
    - Inwalidacja cache po każdej mutacji
    - AuditLog (audit_service.log_crud) — old_value → new_value JSON
    - Plik logów templates_YYYY-MM-DD.jsonl (append-only, nie blokuje)

Decyzje projektowe:
    - Brak dwuetapowego DELETE — szablony mniej wrażliwe niż użytkownicy
    - is_active=False zamiast fizycznego usunięcia (zachowanie historii monitów)
    - Szablon używany przez aktywny MonitHistory NIE może być dezaktywowany
      (business rule — sprawdzane przed deactivate())
    - Jinja2 body NIE jest renderowany tutaj — tylko przechowywany
    - Logi JSONL nie zawierają treści Body (mogą być duże)

Zależności:
    - services/audit_service.py

Ścieżka: backend/app/services/template_service.py
Autor:   System Windykacja
Wersja:  1.0.0
Data:    2026-03-11
"""

from __future__ import annotations

import hashlib
import logging
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import orjson
import secrets
from jose import JWTError, jwt
from redis.asyncio import Redis
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.template import Template, TEMPLATE_TYPES
from app.services import audit_service
from app.services import config_service
from app.core.config import settings

# ─────────────────────────────────────────────────────────────────────────────
# Logger
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Stałe
# ─────────────────────────────────────────────────────────────────────────────

_CACHE_TEMPLATE_TTL: int    = 300   # 5 min — pojedynczy szablon
_CACHE_LIST_TTL: int        = 60    # 1 min — lista (zmienia się częściej)
_CACHE_TYPE_TTL: int        = 120   # 2 min — lista per typ

_MAX_TEMPLATE_NAME: int     = 100
_MAX_SUBJECT: int           = 200
_MAX_BODY: int              = 500_000  # ~500 KB — bezpieczny limit dla NVARCHAR(MAX)

_LOG_FILE_PATTERN = "logs/templates_{date}.jsonl"

_DEFAULT_DELETE_TOKEN_TTL: int = 300  # fallback gdy brak w SystemConfig
_REDIS_KEY_DELETE = "template_delete:{jti}"


# ─────────────────────────────────────────────────────────────────────────────
# Wyjątki
# ─────────────────────────────────────────────────────────────────────────────

class TemplateError(Exception):
    """Bazowy wyjątek serwisu szablonów."""


class TemplateNotFoundError(TemplateError):
    """Szablon o podanym ID nie istnieje lub jest nieaktywny."""


class TemplateValidationError(TemplateError):
    """Błąd walidacji danych wejściowych."""


class TemplateDuplicateError(TemplateError):
    """Szablon o podanej nazwie już istnieje."""


class TemplateInUseError(TemplateError):
    """Szablon jest używany przez aktywne monity — nie można dezaktywować."""


class TemplateDeleteTokenError(TemplateError):
    """Token potwierdzający usunięcie jest nieprawidłowy, wygasł lub został już użyty."""

# ─────────────────────────────────────────────────────────────────────────────
# Dataclasses wejściowe
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class TemplateCreateData:
    """Zwalidowane dane do tworzenia szablonu."""
    template_name: str
    template_type: str
    body: str
    subject: Optional[str] = None
    is_active: bool = True


@dataclass(frozen=True)
class TemplateUpdateData:
    """Zwalidowane dane do aktualizacji szablonu (None = nie zmieniaj)."""
    template_name: Optional[str] = None
    template_type: Optional[str] = None
    body: Optional[str] = None
    subject: Optional[str] = None
    is_active: Optional[bool] = None


@dataclass(frozen=True)
class DeleteConfirmData:
    """Dane zwracane przez initiate_delete — token + podgląd szablonu."""
    token: str
    expires_in: int
    template_id: int
    template_name: str
    template_type: str

# ─────────────────────────────────────────────────────────────────────────────
# Funkcje pomocnicze — prywatne
# ─────────────────────────────────────────────────────────────────────────────

def _get_log_file() -> Path:
    """Zwraca ścieżkę pliku JSONL dla dzisiejszego dnia."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = Path(_LOG_FILE_PATTERN.format(date=today))
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _append_to_file(path: Path, record: dict) -> None:
    """
    Dopisuje rekord JSON do pliku JSONL.
    Nigdy nie rzuca wyjątku — błąd logowania nie może zatrzymać operacji.
    """
    try:
        with path.open("ab") as fh:
            fh.write(orjson.dumps(record) + b"\n")
    except Exception as exc:
        logger.warning(
            "Nie udało się zapisać logu szablonu do pliku",
            extra={"error": str(exc), "path": str(path)},
        )


def _build_log_record(action: str, **kwargs: Any) -> dict:
    """Buduje ustandaryzowany rekord logu JSONL."""
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": "template_service",
        "action": action,
        **kwargs,
    }


def _sanitize(value: Optional[str], max_len: int) -> Optional[str]:
    """
    NFC normalizacja + strip + obcięcie do max_len.
    Zwraca None jeśli wejście jest None lub pusty string.
    """
    if value is None:
        return None
    value = unicodedata.normalize("NFC", value.strip())
    if not value:
        return None
    return value[:max_len]


def _template_to_dict(t: Template) -> dict:
    """Konwertuje ORM Template do słownika (bez Body — patrz get_by_id)."""
    return {
        "id_template":   t.id_template,
        "template_name": t.template_name,
        "template_type": t.template_type,
        "subject":       t.subject,
        "body":          t.body,
        "is_active":     t.is_active,
        "created_at":    t.created_at.isoformat() if t.created_at else None,
        "updated_at":    t.updated_at.isoformat() if t.updated_at else None,
    }


def _template_to_list_item(t: Template) -> dict:
    """Lekki słownik dla listy — bez Body (może być bardzo duże)."""
    return {
        "id_template":   t.id_template,
        "template_name": t.template_name,
        "template_type": t.template_type,
        "subject":       t.subject,
        "is_active":     t.is_active,
        "created_at":    t.created_at.isoformat() if t.created_at else None,
        "updated_at":    t.updated_at.isoformat() if t.updated_at else None,
    }


def _validate_create_data(
    template_name: Optional[str],
    template_type: Optional[str],
    body: Optional[str],
    subject: Optional[str],
) -> TemplateCreateData:
    """
    Waliduje i sanityzuje dane wejściowe dla CREATE.
    Rzuca TemplateValidationError przy błędzie.
    """
    errors: list[dict] = []

    # --- template_name ---
    name = _sanitize(template_name, _MAX_TEMPLATE_NAME)
    if not name:
        errors.append({"field": "template_name", "message": "Pole wymagane (max 100 znaków)."})

    # --- template_type ---
    ttype = _sanitize(template_type, 20)
    if not ttype:
        errors.append({"field": "template_type", "message": "Pole wymagane."})
    elif ttype not in TEMPLATE_TYPES:
        errors.append({
            "field": "template_type",
            "message": f"Nieprawidłowy typ. Dozwolone: {', '.join(sorted(TEMPLATE_TYPES))}.",
        })

    # --- body ---
    body_clean = _sanitize(body, _MAX_BODY)
    if not body_clean:
        errors.append({"field": "body", "message": "Treść szablonu jest wymagana."})

    # --- subject (wymagany dla email) ---
    subject_clean = _sanitize(subject, _MAX_SUBJECT)
    if ttype == "email" and not subject_clean:
        errors.append({
            "field": "subject",
            "message": "Temat wiadomości (Subject) jest wymagany dla szablonów email.",
        })
    if ttype and ttype != "email" and subject_clean:
        # Ostrzeżenie — nie błąd, ale czyścimy subject dla sms/print
        subject_clean = None
        logger.debug("Subject zignorowany dla typu %s", ttype)

    if errors:
        raise TemplateValidationError(orjson.dumps(errors).decode())

    return TemplateCreateData(
        template_name=name,  # type: ignore[arg-type]
        template_type=ttype,  # type: ignore[arg-type]
        body=body_clean,  # type: ignore[arg-type]
        subject=subject_clean,
    )


def _validate_update_data(
    template_name: Optional[str],
    template_type: Optional[str],
    body: Optional[str],
    subject: Optional[str],
    is_active: Optional[bool],
    current_type: str,
) -> TemplateUpdateData:
    """
    Waliduje i sanityzuje dane wejściowe dla UPDATE.
    current_type: aktualny typ szablonu (potrzebny do walidacji subject).
    """
    errors: list[dict] = []

    name = _sanitize(template_name, _MAX_TEMPLATE_NAME) if template_name is not None else None
    if template_name is not None and not name:
        errors.append({"field": "template_name", "message": "Nazwa nie może być pusta."})

    ttype: Optional[str] = None
    if template_type is not None:
        ttype = _sanitize(template_type, 20)
        if not ttype:
            errors.append({"field": "template_type", "message": "Typ nie może być pusty."})
        elif ttype not in TEMPLATE_TYPES:
            errors.append({
                "field": "template_type",
                "message": f"Nieprawidłowy typ. Dozwolone: {', '.join(sorted(TEMPLATE_TYPES))}.",
            })

    body_clean = _sanitize(body, _MAX_BODY) if body is not None else None
    if body is not None and not body_clean:
        errors.append({"field": "body", "message": "Treść nie może być pusta."})

    # Efektywny typ po zmianie (albo nowy albo stary)
    effective_type = ttype if ttype else current_type
    subject_clean = _sanitize(subject, _MAX_SUBJECT) if subject is not None else None

    if subject is not None:
        if effective_type == "email" and not subject_clean:
            errors.append({
                "field": "subject",
                "message": "Subject nie może być pusty dla szablonu email.",
            })
        if effective_type != "email":
            subject_clean = None  # wymuś NULL dla sms/print

    if errors:
        raise TemplateValidationError(orjson.dumps(errors).decode())

    return TemplateUpdateData(
        template_name=name,
        template_type=ttype,
        body=body_clean,
        subject=subject_clean,
        is_active=is_active,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Cache Redis — pomocnicze
# ─────────────────────────────────────────────────────────────────────────────

async def _cache_get(redis: Redis, key: str) -> Optional[dict]:
    """Odczytuje JSON z Redis. Zwraca None przy miss lub błędzie."""
    try:
        raw = await redis.get(key)
        if raw:
            return orjson.loads(raw)
    except Exception as exc:
        logger.warning("Redis cache miss (get)", extra={"key": key, "error": str(exc)})
    return None


async def _cache_set(redis: Redis, key: str, value: Any, ttl: int) -> None:
    """Zapisuje JSON do Redis. Nigdy nie rzuca wyjątku."""
    try:
        await redis.setex(key, ttl, orjson.dumps(value))
    except Exception as exc:
        logger.warning("Redis cache write failed", extra={"key": key, "error": str(exc)})


async def _cache_delete(redis: Redis, *keys: str) -> None:
    """Usuwa klucze z Redis. Nigdy nie rzuca wyjątku."""
    try:
        if keys:
            await redis.delete(*keys)
    except Exception as exc:
        logger.warning("Redis cache delete failed", extra={"keys": keys, "error": str(exc)})


async def _invalidate_template_cache(redis: Redis, template_id: int, template_type: Optional[str] = None) -> None:
    """
    Inwaliduje wszystkie klucze cache związane z szablonem.
    Usuwa: templates:{id}, templates:type:{type}, skany templates:list:*
    """
    keys_to_delete = [f"templates:{template_id}"]
    if template_type:
        keys_to_delete.append(f"templates:type:{template_type}")

    # Skan list cache — może być wiele kluczy z różnymi parametrami
    try:
        async for key in redis.scan_iter("templates:list:*"):
            keys_to_delete.append(key)
    except Exception as exc:
        logger.warning("Redis scan failed during cache invalidation", extra={"error": str(exc)})

    await _cache_delete(redis, *keys_to_delete)

    _append_to_file(
        _get_log_file(),
        _build_log_record(
            action="cache_invalidated",
            template_id=template_id,
            keys_deleted=len(keys_to_delete),
        ),
    )


# ─────────────────────────────────────────────────────────────────────────────
# READ — publiczne API serwisu
# ─────────────────────────────────────────────────────────────────────────────

async def get_list(
    db: AsyncSession,
    redis: Redis,
    page: int = 1,
    limit: int = 20,
    template_type: Optional[str] = None,
    is_active: Optional[bool] = None,
    search: Optional[str] = None,
    sort: Optional[str] = None,
) -> dict:
    """
    Zwraca paginowaną listę szablonów.

    Args:
        db:            Sesja SQLAlchemy.
        redis:         Klient Redis.
        page:          Numer strony (>= 1).
        limit:         Elementów na stronę (1-100).
        template_type: Filtr po typie (email/sms/print).
        is_active:     Filtr po statusie aktywności.
        search:        Wyszukiwanie po nazwie (LIKE).
        sort:          Sortowanie: "name", "-name", "created_at", "-created_at".

    Returns:
        Słownik z kluczami: items, total, page, limit, pages.
    """
    page  = max(1, page)
    limit = max(1, min(100, limit))
    offset = (page - 1) * limit

    # Cache key — hash parametrów
    cache_params = orjson.dumps({
        "p": page, "l": limit,
        "t": template_type, "a": is_active,
        "s": search, "so": sort,
    })
    cache_key = f"templates:list:{hashlib.md5(cache_params).hexdigest()}"  # noqa: S324

    cached = await _cache_get(redis, cache_key)
    if cached:
        logger.debug("templates.get_list: cache hit", extra={"cache_key": cache_key})
        return cached

    # --- Budowa zapytania ---
    stmt = select(Template)
    count_stmt = select(func.count()).select_from(Template)

    filters = []
    if template_type and template_type in TEMPLATE_TYPES:
        filters.append(Template.template_type == template_type)
    if is_active is not None:
        filters.append(Template.is_active == is_active)
    if search:
        search_clean = _sanitize(search, 100) or ""
        if search_clean:
            filters.append(Template.template_name.ilike(f"%{search_clean}%"))

    if filters:
        stmt       = stmt.where(and_(*filters))
        count_stmt = count_stmt.where(and_(*filters))

    # Sortowanie
    sort_map = {
        "name":        Template.template_name.asc(),
        "-name":       Template.template_name.desc(),
        "created_at":  Template.created_at.asc(),
        "-created_at": Template.created_at.desc(),
        "type":        Template.template_type.asc(),
    }
    order_col = sort_map.get(sort or "-created_at", Template.created_at.desc())
    stmt = stmt.order_by(order_col).offset(offset).limit(limit)

    # Wykonanie zapytań
    total_result = await db.execute(count_stmt)
    total: int   = total_result.scalar_one()

    rows_result = await db.execute(stmt)
    templates   = rows_result.scalars().all()

    items = [_template_to_list_item(t) for t in templates]

    result = {
        "items":  items,
        "total":  total,
        "page":   page,
        "limit":  limit,
        "pages":  max(1, -(-total // limit)),  # ceil division
    }

    await _cache_set(redis, cache_key, result, _CACHE_LIST_TTL)

    _append_to_file(
        _get_log_file(),
        _build_log_record(
            action="templates_listed",
            page=page,
            limit=limit,
            total=total,
            filters={"type": template_type, "is_active": is_active, "search": bool(search)},
        ),
    )

    return result


async def get_by_id(
    db: AsyncSession,
    redis: Redis,
    template_id: int,
) -> dict:
    """
    Zwraca pełne dane szablonu (włącznie z Body).

    Args:
        db:          Sesja SQLAlchemy.
        redis:       Klient Redis.
        template_id: ID szablonu.

    Returns:
        Słownik z pełnymi danymi szablonu.

    Raises:
        TemplateNotFoundError: Szablon nie istnieje.
    """
    cache_key = f"templates:{template_id}"
    cached    = await _cache_get(redis, cache_key)
    if cached:
        return cached

    result = await db.execute(
        select(Template).where(Template.id_template == template_id)
    )
    template = result.scalar_one_or_none()

    if template is None:
        raise TemplateNotFoundError(f"Szablon ID={template_id} nie istnieje.")

    data = _template_to_dict(template)
    await _cache_set(redis, cache_key, data, _CACHE_TEMPLATE_TTL)

    return data


async def get_active_by_type(
    db: AsyncSession,
    redis: Redis,
    template_type: str,
) -> list[dict]:
    """
    Zwraca listę aktywnych szablonów danego typu.
    Używana przez worker przy wyborze szablonu do monitu.

    Args:
        db:            Sesja SQLAlchemy.
        redis:         Klient Redis.
        template_type: Typ szablonu (email/sms/print).

    Returns:
        Lista słowników z danymi szablonów (bez Body dla wydajności).
    """
    if template_type not in TEMPLATE_TYPES:
        raise TemplateValidationError(
            f"Nieprawidłowy typ: {template_type}. Dozwolone: {', '.join(sorted(TEMPLATE_TYPES))}."
        )

    cache_key = f"templates:type:{template_type}"
    cached    = await _cache_get(redis, cache_key)
    if cached:
        return cached

    result = await db.execute(
        select(Template).where(
            and_(
                Template.template_type == template_type,
                Template.is_active == True,  # noqa: E712
            )
        ).order_by(Template.template_name.asc())
    )
    templates = result.scalars().all()
    items     = [_template_to_list_item(t) for t in templates]

    await _cache_set(redis, cache_key, items, _CACHE_TYPE_TTL)

    return items


# ─────────────────────────────────────────────────────────────────────────────
# WRITE — publiczne API serwisu
# ─────────────────────────────────────────────────────────────────────────────

async def create(
    db: AsyncSession,
    redis: Redis,
    raw_name: Optional[str],
    raw_type: Optional[str],
    raw_body: Optional[str],
    raw_subject: Optional[str],
    created_by_user_id: int,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Tworzy nowy szablon.

    Args:
        db:                  Sesja SQLAlchemy.
        redis:               Klient Redis.
        raw_name:            Nazwa szablonu (surowa — zostanie zsanityzowana).
        raw_type:            Typ szablonu.
        raw_body:            Treść Jinja2.
        raw_subject:         Temat email (wymagany dla type=email).
        created_by_user_id:  ID użytkownika tworzącego.
        ip_address:          IP inicjatora.

    Returns:
        Słownik z danymi nowego szablonu.

    Raises:
        TemplateValidationError:  Błąd walidacji.
        TemplateDuplicateError:   Nazwa już istnieje.
    """
    data = _validate_create_data(raw_name, raw_type, raw_body, raw_subject)

    # Sprawdź unikalność nazwy
    existing = await db.execute(
        select(Template).where(Template.template_name == data.template_name)
    )
    if existing.scalar_one_or_none() is not None:
        raise TemplateDuplicateError(
            f"Szablon o nazwie '{data.template_name}' już istnieje."
        )

    new_template = Template(
        template_name=data.template_name,
        template_type=data.template_type,
        body=data.body,
        subject=data.subject,
        is_active=True,
    )
    db.add(new_template)
    await db.flush()

    template_id = new_template.id_template
    result_dict = _template_to_dict(new_template)

    await db.commit()

    # Cache — nowy szablon
    await _cache_set(redis, f"templates:{template_id}", result_dict, _CACHE_TEMPLATE_TTL)

    # Inwaliduj listy i cache per type
    await _invalidate_template_cache(redis, template_id, data.template_type)

    # Log plikowy
    _append_to_file(
        _get_log_file(),
        _build_log_record(
            action="template_created",
            template_id=template_id,
            template_name=data.template_name,
            template_type=data.template_type,
            has_subject=bool(data.subject),
            body_length=len(data.body),
            created_by=created_by_user_id,
            ip_address=ip_address,
        ),
    )

    # AuditLog
    audit_service.log_crud(
        db=db,
        action="template_created",
        entity_type="Template",
        entity_id=template_id,
        new_value={
            "template_name": data.template_name,
            "template_type": data.template_type,
            "has_subject":   bool(data.subject),
            "body_length":   len(data.body),
        },
        user_id=created_by_user_id,
        success=True,
    )

    logger.info(
        "Szablon utworzony",
        extra={
            "template_id":   template_id,
            "template_name": data.template_name,
            "template_type": data.template_type,
            "created_by":    created_by_user_id,
            "ip_address":    ip_address,
        },
    )

    return result_dict


async def update(
    db: AsyncSession,
    redis: Redis,
    template_id: int,
    raw_name: Optional[str],
    raw_type: Optional[str],
    raw_body: Optional[str],
    raw_subject: Optional[str],
    is_active: Optional[bool],
    updated_by_user_id: int,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Aktualizuje szablon. Wszystkie pola opcjonalne — None = nie zmieniaj.

    Args:
        db:                  Sesja SQLAlchemy.
        redis:               Klient Redis.
        template_id:         ID szablonu do aktualizacji.
        raw_name:            Nowa nazwa (None = bez zmian).
        raw_type:            Nowy typ (None = bez zmian).
        raw_body:            Nowa treść (None = bez zmian).
        raw_subject:         Nowy temat (None = bez zmian).
        is_active:           Nowy status aktywności (None = bez zmian).
        updated_by_user_id:  ID użytkownika wykonującego operację.
        ip_address:          IP inicjatora.

    Returns:
        Słownik z zaktualizowanymi danymi szablonu.

    Raises:
        TemplateNotFoundError:    Szablon nie istnieje.
        TemplateValidationError:  Błąd walidacji.
        TemplateDuplicateError:   Nowa nazwa już istnieje.
        TemplateInUseError:       Dezaktywacja niemożliwa — szablon w użyciu.
    """
    result = await db.execute(
        select(Template).where(Template.id_template == template_id)
    )
    template = result.scalar_one_or_none()
    if template is None:
        raise TemplateNotFoundError(f"Szablon ID={template_id} nie istnieje.")

    old_value = _template_to_dict(template)

    # Walidacja danych
    data = _validate_update_data(
        raw_name, raw_type, raw_body, raw_subject, is_active,
        current_type=template.template_type,
    )

    # Sprawdź unikalność nazwy jeśli zmieniana
    if data.template_name and data.template_name != template.template_name:
        existing = await db.execute(
            select(Template).where(
                and_(
                    Template.template_name == data.template_name,
                    Template.id_template != template_id,
                )
            )
        )
        if existing.scalar_one_or_none() is not None:
            raise TemplateDuplicateError(
                f"Szablon o nazwie '{data.template_name}' już istnieje."
            )

    # Dezaktywacja — sprawdź czy szablon nie jest używany przez aktywne monity
    if data.is_active is False and template.is_active is True:
        from app.db.models.monit_history import MonitHistory  # lazy import
        in_use = await db.execute(
            select(func.count()).select_from(MonitHistory).where(
                and_(
                    MonitHistory.template_id == template_id,
                    MonitHistory.status.in_(["pending", "queued"]),
                )
            )
        )
        active_count = in_use.scalar_one()
        if active_count > 0:
            raise TemplateInUseError(
                f"Szablon ID={template_id} jest używany przez {active_count} "
                f"aktywnych monitów w kolejce. Dezaktywacja niemożliwa."
            )

    # Zastosuj zmiany
    changed_fields: list[str] = []
    if data.template_name is not None:
        template.template_name = data.template_name
        changed_fields.append("template_name")
    if data.template_type is not None:
        template.template_type = data.template_type
        changed_fields.append("template_type")
    if data.body is not None:
        template.body = data.body
        changed_fields.append("body")
    if data.subject is not None or (data.template_type and data.template_type != "email"):
        template.subject = data.subject
        changed_fields.append("subject")
    if data.is_active is not None:
        template.is_active = data.is_active
        changed_fields.append("is_active")

    if not changed_fields:
        # Brak zmian — zwróć aktualne dane z cache
        return await get_by_id(db, redis, template_id)

    template.updated_at = datetime.now(timezone.utc)
    await db.flush()
    await db.commit()

    new_value = _template_to_dict(template)

    # Inwaliduj cache
    await _invalidate_template_cache(redis, template_id, template.template_type)
    if old_value["template_type"] != template.template_type:
        # Typ się zmienił — inwaliduj też stary typ
        await _cache_delete(redis, f"templates:type:{old_value['template_type']}")

    # Log plikowy
    _append_to_file(
        _get_log_file(),
        _build_log_record(
            action="template_updated",
            template_id=template_id,
            changed_fields=changed_fields,
            updated_by=updated_by_user_id,
            ip_address=ip_address,
            is_active=template.is_active,
        ),
    )

    # AuditLog — old/new bez Body (może być gigantyczne)
    def _sanitize_for_audit(d: dict) -> dict:
        return {k: v for k, v in d.items() if k != "body"}

    audit_service.log_crud(
        db=db,
        action="template_updated",
        entity_type="Template",
        entity_id=template_id,
        old_value=_sanitize_for_audit(old_value),
        new_value=_sanitize_for_audit(new_value),
        details={"changed_fields": changed_fields},
        user_id=updated_by_user_id,
        success=True,
    )

    logger.info(
        "Szablon zaktualizowany",
        extra={
            "template_id":    template_id,
            "changed_fields": changed_fields,
            "updated_by":     updated_by_user_id,
            "ip_address":     ip_address,
        },
    )

    return new_value


async def _deactivate_internal(
    db: AsyncSession,
    redis: Redis,
    template_id: int,
    deleted_by_user_id: int,
    ip_address: Optional[str] = None,
) -> dict:
    """Wewnętrzny soft-delete — wywoływany tylko przez confirm_delete."""
    return await update(
        db=db,
        redis=redis,
        template_id=template_id,
        raw_name=None,
        raw_type=None,
        raw_body=None,
        raw_subject=None,
        is_active=False,
        updated_by_user_id=deleted_by_user_id,
        ip_address=ip_address,
    )


async def initiate_delete(
    db: AsyncSession,
    redis: Redis,
    template_id: int,
    initiated_by_user_id: int,
    ip_address: Optional[str] = None,
) -> DeleteConfirmData:
    """
    Inicjuje dwuetapowe usunięcie szablonu — Krok 1.

    Sprawdza czy szablon istnieje i nie jest w użyciu,
    generuje jednorazowy token JWT i zapisuje JTI w Redis.

    Args:
        db:                    Sesja SQLAlchemy.
        redis:                 Klient Redis.
        template_id:           ID szablonu.
        initiated_by_user_id:  ID użytkownika inicjującego.
        ip_address:            IP inicjatora.

    Returns:
        DeleteConfirmData z tokenem i podglądem szablonu.

    Raises:
        TemplateNotFoundError:  Szablon nie istnieje.
        TemplateInUseError:     Szablon używany przez pending/queued monity.
    """
    result = await db.execute(
        select(Template).where(Template.id_template == template_id)
    )
    template = result.scalar_one_or_none()
    if template is None:
        raise TemplateNotFoundError(f"Szablon ID={template_id} nie istnieje.")

    # Sprawdź czy szablon jest w użyciu
    from app.db.models.monit_history import MonitHistory  # lazy import
    in_use = await db.execute(
        select(func.count()).select_from(MonitHistory).where(
            and_(
                MonitHistory.template_id == template_id,
                MonitHistory.status.in_(["pending", "queued"]),
            )
        )
    )
    active_count = in_use.scalar_one()
    if active_count > 0:
        raise TemplateInUseError(
            f"Szablon ID={template_id} jest używany przez {active_count} "
            f"aktywnych monitów w kolejce. Dezaktywacja niemożliwa."
        )

    # TTL z SystemConfig
    ttl_seconds = await config_service.get_int(
        db, redis,
        key="delete_token.ttl_seconds",
        default=_DEFAULT_DELETE_TOKEN_TTL,
    )

    now = datetime.now(timezone.utc)
    jti = secrets.token_hex(16)
    expires_at = now + timedelta(seconds=ttl_seconds)

    token_payload = {
        "sub":          str(template_id),
        "type":         "delete_confirm",
        "action":       "delete_template",
        "entity_type":  "Template",
        "initiated_by": initiated_by_user_id,
        "jti":          jti,
        "iat":          int(now.timestamp()),
        "exp":          int(expires_at.timestamp()),
    }

    delete_token = jwt.encode(
        token_payload,
        settings.secret_key.get_secret_value(),
        algorithm=settings.algorithm,
    )

    # Zapisz JTI w Redis (jednorazowość)
    await redis.set(
        _REDIS_KEY_DELETE.format(jti=jti),
        str(template_id),
        ex=ttl_seconds,
    )

    _append_to_file(
        _get_log_file(),
        _build_log_record(
            action="template_delete_initiated",
            template_id=template_id,
            template_name=template.template_name,
            initiated_by=initiated_by_user_id,
            ttl_seconds=ttl_seconds,
            ip_address=ip_address,
        ),
    )

    audit_service.log_crud(
        db=db,
        action="template_delete_initiated",
        entity_type="Template",
        entity_id=template_id,
        details={
            "template_name": template.template_name,
            "template_type": template.template_type,
            "initiated_by":  initiated_by_user_id,
            "ttl_seconds":   ttl_seconds,
        },
        user_id=initiated_by_user_id,
        success=True,
    )

    logger.info(
        "Zainicjowano usunięcie szablonu — krok 1",
        extra={
            "template_id":   template_id,
            "template_name": template.template_name,
            "initiated_by":  initiated_by_user_id,
            "ttl_seconds":   ttl_seconds,
            "ip_address":    ip_address,
        },
    )

    return DeleteConfirmData(
        token=delete_token,
        expires_in=ttl_seconds,
        template_id=template_id,
        template_name=template.template_name,
        template_type=template.template_type,
    )


async def confirm_delete(
    db: AsyncSession,
    redis: Redis,
    template_id: int,
    confirm_token: str,
    requesting_user_id: int,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Potwierdza i wykonuje soft-delete szablonu — Krok 2.

    Weryfikuje token JWT, sprawdza jednorazowość JTI w Redis,
    następnie wykonuje soft-delete (is_active=False).

    Args:
        db:                   Sesja SQLAlchemy.
        redis:                Klient Redis.
        template_id:          ID szablonu.
        confirm_token:        Token JWT z initiate_delete().
        requesting_user_id:   ID użytkownika potwierdzającego.
        ip_address:           IP inicjatora.

    Returns:
        Słownik z potwierdzeniem dezaktywacji.

    Raises:
        TemplateDeleteTokenError: Nieprawidłowy/wygasły/użyty token.
        TemplateNotFoundError:    Szablon nie istnieje.
        TemplateInUseError:       Szablon w użyciu (race condition).
    """
    # Weryfikacja tokenu
    try:
        payload = jwt.decode(
            confirm_token,
            settings.secret_key.get_secret_value(),
            algorithms=[settings.algorithm],
        )
    except JWTError as exc:
        raise TemplateDeleteTokenError(
            f"Token potwierdzający jest nieprawidłowy lub wygasł: {exc}"
        )

    if payload.get("type") != "delete_confirm" or payload.get("action") != "delete_template":
        raise TemplateDeleteTokenError(
            "Token nie jest tokenem potwierdzającym usunięcia szablonu."
        )

    token_template_id = payload.get("sub")
    if token_template_id is None or int(token_template_id) != template_id:
        raise TemplateDeleteTokenError("Token dotyczy innego szablonu.")

    token_by = payload.get("initiated_by")
    if token_by is None or int(token_by) != requesting_user_id:
        raise TemplateDeleteTokenError("Token został wygenerowany przez innego użytkownika.")

    # Sprawdź jednorazowość w Redis
    jti = payload.get("jti")
    redis_key = _REDIS_KEY_DELETE.format(jti=jti)
    stored = await redis.get(redis_key)
    if stored is None:
        raise TemplateDeleteTokenError("Token wygasł lub został już użyty.")
    await redis.delete(redis_key)

    # Wykonaj soft-delete
    result = await _deactivate_internal(
        db=db,
        redis=redis,
        template_id=template_id,
        deleted_by_user_id=requesting_user_id,
        ip_address=ip_address,
    )

    _append_to_file(
        _get_log_file(),
        _build_log_record(
            action="template_delete_confirmed",
            template_id=template_id,
            confirmed_by=requesting_user_id,
            ip_address=ip_address,
        ),
    )

    audit_service.log_crud(
        db=db,
        action="template_deleted",
        entity_type="Template",
        entity_id=template_id,
        details={"confirmed_by": requesting_user_id},
        user_id=requesting_user_id,
        success=True,
    )

    logger.info(
        "Szablon usunięty (soft-delete) — krok 2 potwierdzony",
        extra={
            "template_id":  template_id,
            "confirmed_by": requesting_user_id,
            "ip_address":   ip_address,
        },
    )

    return result