# backend/app/services/source_admin_service.py
"""
Serwis administracyjny zrodel dokumentow — F6.

Pokrywa logike biznesowa dla:
  list_sources / get_source / create_source / update_source / delete_source
  generate_webhook_token / revoke_webhook_token
  test_connection
  trigger_sync / get_sync_status
  get_health
  set_test_mode

Wzorce bezpieczenstwa:
  - webhook_token: plaintext w DB (kolumna istniejaca od migracji 0039),
    ale weryfikacja przez constant-time compare (secrets.compare_digest)
    zeby nie wyciekac informacji przez timing attack.
  - connection_config: nigdy nie zwracany w pelnej postaci z serwisu —
    tylko liste kluczy (connection_config_keys) lub get_config_safe()
    z wymazanymi polami wrazliwymi.
  - Kazda zmiana zrodla loguje sie do AuditLog.

UWAGA: from __future__ import annotations — NIGDY w tym pliku (SQLAlchemy ORM).
"""

import asyncio
import fnmatch
import imaplib
import json
import logging
import secrets
import time
from datetime import datetime, timezone
from ftplib import FTP
from typing import Any

from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.approval.document_source import (
    DocumentSource,
    SOURCE_TYPES,
    CONNECTION_MODES,
)

logger = logging.getLogger(__name__)

# =============================================================================
# Logowanie snapshotow konfiguracji zrodel (2026-07-16)
# =============================================================================
# NAPRAWA: skw_AuditLog zapisuje tylko "<zmieniono>" (placeholder) dla
# connection_config — brak realnej wartosci uniemozliwial szybkie ustalenie
# "co dokladnie sie zmienilo" bez recznego skryptu diagnostycznego za kazdym
# razem (patrz incydenty z tej sesji: host.docker.internal vs 192.168.0.50,
# zgubione haslo przy PUT, brakujacy endpoint_line_items). Ten log daje
# pelny (zredagowany z sekretow) stan konfiguracji przy KAZDEJ zmianie,
# z data — bez potrzeby deszyfrowania niczego recznie nastepnym razem.
#
# Osobny strumien JSONL, zgodny z istniejacym wzorcem projektu (jeden plik
# na dzien, per domena — audit_*.jsonl, events_*.jsonl, schema_integrity_*.jsonl).
_config_snapshot_logger = logging.getLogger("app.services.source_config_snapshot")

# Klucze traktowane jako wrazliwe — maskowane bez wzgledu na source_type.
# auth_config maskowany w calosci (moze zawierac dowolne zagniezdzone sekrety
# zaleznie od auth_type — bezpieczniej zamaskowac caly obiekt niz zgadywac
# ktore jego pola sa wrazliwe).
_SENSITIVE_CONFIG_KEYS = frozenset({
    "password", "pwd", "secret", "token", "api_key", "apikey",
    "auth_config", "webhook_token", "private_key", "certificate",
})


def _redact_config(config: dict[str, Any]) -> dict[str, Any]:
    """
    Maskuje wrazliwe pola przed zapisem do logu — rekurencyjnie dla
    zagniezdzonych slownikow, oraz osobno dla connection_string zawierajacego
    PWD=... (wzorzec MSSQL) w jednym stringu.
    """
    redacted: dict[str, Any] = {}
    for k, v in config.items():
        if k.lower() in _SENSITIVE_CONFIG_KEYS:
            redacted[k] = "***"
        elif isinstance(v, dict):
            redacted[k] = _redact_config(v)
        elif isinstance(v, str) and "PWD=" in v.upper():
            import re
            redacted[k] = re.sub(r"(PWD=)[^;]*", r"\1***", v, flags=re.IGNORECASE)
        else:
            redacted[k] = v
    return redacted


def _log_config_snapshot(source: DocumentSource, changed_by_user_id: int | None, reason: str) -> None:
    """
    Zapisuje pelny (zredagowany) stan konfiguracji zrodla do dedykowanego
    strumienia logow — wywolywane po kazdym udanym create_source()/
    update_source(). Bledy logowania NIGDY nie blokuja glownej operacji
    (best-effort, zgodnie z wzorcem _log_webhook_attempt w webhook_service.py).
    """
    try:
        raw_config = source.get_config() or {}
        entry = {
            "ts_utc":                datetime.now(timezone.utc).isoformat(),
            "event":                 "source_config_snapshot",
            "reason":                reason,  # "created" | "updated" | "periodic"
            "id_source":             source.id_source,
            "source_name":           source.source_name,
            "source_type":           source.source_type,
            "connection_mode":       source.connection_mode,
            "is_active":             source.is_active,
            "sync_interval_minutes": source.sync_interval_minutes,
            "connection_config":     _redact_config(raw_config),
            "changed_by_user_id":    changed_by_user_id,
        }
        _config_snapshot_logger.info(json.dumps(entry, ensure_ascii=False, default=str))
    except Exception as exc:
        logger.error(
            "_log_config_snapshot: blad zapisu snapshotu configu dla id_source=%s: %s",
            getattr(source, "id_source", "?"), exc,
        )

_SCHEMA = "dbo"

# Dlugosc tokenu webhooka — 48 bajtow losowych -> ~64 znaki base64url
_WEBHOOK_TOKEN_BYTES = 48

# Prog "stary sync" dla health check (minuty) — konfigurowalny przez SystemConfig
_DEFAULT_SYNC_WARNING_MINUTES  = 60
_DEFAULT_SYNC_CRITICAL_MINUTES = 240


class SourceNotFoundError(Exception):
    """Zrodlo o podanym ID nie istnieje."""


class SourceNameConflictError(Exception):
    """source_name juz istnieje (UNIQUE constraint)."""


class SourceValidationError(Exception):
    """Walidacja modelu DocumentSource nie powiodla sie."""


# =============================================================================
# Tier 2 — walidacja connection_config zalezna od connection_mode
# (Recenzja Krytyczna Tier1/Tier2 + Rozstrzygniecia Koncowe)
# =============================================================================

# Pola typowe dla connection_mode='pull' w zrodlach source_type='api'.
# Obecnosc ktoregokolwiek z nich w konfiguracji zrodla push jest odrzucana
# (422) — swiadoma decyzja: glosny fail, zero cichej tolerancji, zeby
# integrator natychmiast wiedzial o pomylce w konfiguracji, zamiast
# odkryc to dopiero przy proba uzycia (np. line-items zwracajace mylacy
# 409 not_applicable, jak w incydencie z sesji 2026-07-16).
_PULL_ONLY_API_FIELDS = frozenset({
    "base_url", "endpoint_list", "endpoint_detail", "endpoint_line_items",
})


def _validate_connection_config_for_mode(
    source_type: str,
    connection_mode: str,
    config: dict[str, Any],
) -> None:
    """
    Waliduje connection_config wzgledem connection_mode.

    Swiadomie NIE dzieli SourceCreate/SourceUpdate na osobne, dyskryminowane
    modele Pydantic (duza, ryzykowna zmiana obejmujaca tez zrodla
    database/ftp/email) — to jedna funkcja walidacyjna w warstwie serwisu,
    wolana przed source.set_config() w create_source() i update_source().

    Raises:
        SourceValidationError: obecne pole typowe dla pull w konfiguracji
                                zrodla push (tylko source_type='api').
    """
    if source_type != "api" or connection_mode != "push":
        return
    present = _PULL_ONLY_API_FIELDS & config.keys()
    if present:
        raise SourceValidationError(
            f"Pola {sorted(present)} sa przeznaczone dla connection_mode='pull' "
            f"i nie naleza do trybu 'push'. Usun je z connection_config."
        )


# =============================================================================
# CRUD — lista i odczyt
# =============================================================================

async def list_sources(
    db: AsyncSession,
    *,
    page: int = 1,
    per_page: int = 50,
    source_type: str | None = None,
    is_active: bool | None = None,
) -> dict[str, Any]:
    """
    Lista zrodel z paginacja i opcjonalnymi filtrami.

    Returns:
        {"items": [...], "total": int, "page": int, "per_page": int}
    """
    stmt = select(DocumentSource)
    count_stmt = select(DocumentSource)

    if source_type:
        stmt = stmt.where(DocumentSource.source_type == source_type)
        count_stmt = count_stmt.where(DocumentSource.source_type == source_type)
    if is_active is not None:
        stmt = stmt.where(DocumentSource.is_active == is_active)
        count_stmt = count_stmt.where(DocumentSource.is_active == is_active)

    total_result = await db.execute(count_stmt)
    total = len(total_result.scalars().all())

    stmt = (
        stmt.order_by(DocumentSource.id_source.asc())
        .offset((page - 1) * per_page)
        .limit(per_page)
    )
    result = await db.execute(stmt)
    sources = list(result.scalars().all())

    items = [to_source_out_dict(s) for s in sources]
    return {"items": items, "total": total, "page": page, "per_page": per_page}


async def get_source(db: AsyncSession, id_source: int) -> DocumentSource:
    """Pobiera zrodlo po ID. Rzuca SourceNotFoundError jesli nie istnieje."""
    result = await db.execute(
        select(DocumentSource).where(DocumentSource.id_source == id_source)
    )
    source = result.scalar_one_or_none()
    if source is None:
        raise SourceNotFoundError(f"Zrodlo ID={id_source} nie istnieje.")
    return source


def to_source_out_dict(source: DocumentSource) -> dict[str, Any]:
    """Konwertuje DocumentSource na dict zgodny z SourceOut (bez sekretow)."""
    try:
        config_keys = list(source.get_config().keys()) if source.connection_config else []
    except ValueError:
        config_keys = ["<blad deszyfrowania>"]

    return {
        "id_source":              source.id_source,
        "source_name":            source.source_name,
        "source_type":            source.source_type,
        "connection_mode":        source.connection_mode,
        "connection_config_keys": config_keys,
        # NAPRAWA 2026-07-16 (Rozstrzygniecia Koncowe #1): kolumna w bazie
        # pozostaje NOT NULL i NIETKNIETA (zero migracji, zero zmiany
        # semantyki partial-update w update_source()) — maskujemy
        # WYLACZNIE w odpowiedzi API, bo dla connection_mode='push' ta
        # wartosc jest operacyjnie nieuzywana (needs_sync zwraca False
        # zanim do niej dojdzie) i pokazywanie jej administratorowi
        # myliloby ("czemu tu jest 15 minut, skoro to push?").
        "sync_interval_minutes": (
            None if source.connection_mode == "push" else source.sync_interval_minutes
        ),
        "last_sync_at":           source.last_sync_at,
        "last_sync_status":       source.last_sync_status,
        "last_sync_message":      source.last_sync_message,
        "is_test_mode":           source.is_test_mode,
        "has_webhook_token":      bool(source.webhook_token),
        "is_active":              source.is_active,
        "created_at":             getattr(source, "created_at", None),
        "updated_at":             source.updated_at,
    }


# =============================================================================
# CRUD — create / update / delete
# =============================================================================

async def create_source(
    db: AsyncSession,
    *,
    source_name: str,
    source_type: str,
    connection_mode: str,
    connection_config: dict[str, Any],
    sync_interval_minutes: int | None,
    is_active: bool,
    actor_id: int,
) -> DocumentSource:
    """
    Tworzy nowe zrodlo. Zawsze startuje z is_test_mode=True (decyzja bezpieczenstwa).

    Dla connection_mode='push' webhook_token NIE jest generowany automatycznie —
    operator musi wywolac POST /sources/{id}/webhook-token osobno (jawna decyzja).

    Raises:
        SourceNameConflictError: source_name juz istnieje.
        SourceValidationError:   walidacja modelu nie powiodla sie.
    """
    source = DocumentSource(
        source_name=source_name,
        source_type=source_type,
        connection_mode=connection_mode,
        sync_interval_minutes=sync_interval_minutes or 15,
        is_test_mode=True,  # zawsze — nowe zrodlo nigdy nie startuje produkcyjnie
        is_active=is_active,
    )

    if connection_config:
        _validate_connection_config_for_mode(source_type, connection_mode, connection_config)
        try:
            source.set_config(connection_config)
        except ValueError as exc:
            raise SourceValidationError(f"Blad szyfrowania konfiguracji: {exc}") from exc

    errors = source.validate()
    if errors:
        raise SourceValidationError("; ".join(errors))

    db.add(source)
    try:
        await db.flush()
    except IntegrityError as exc:
        await db.rollback()
        raise SourceNameConflictError(
            f"Zrodlo o nazwie '{source_name}' juz istnieje."
        ) from exc

    await _audit_log(
        db, actor_id=actor_id, action="source.created",
        entity_id=source.id_source,
        details={"source_name": source_name, "source_type": source_type},
    )
    await db.commit()

    _log_config_snapshot(source, changed_by_user_id=actor_id, reason="created")

    # NAPRAWA 2026-07-17: webhook_token juz NIE jest wymagany przy tworzeniu
    # (patrz document_source.py::validate()) — czysto diagnostyczny log,
    # zeby bylo widac w logach, ze zrodlo push zostalo utworzone bez tokenu
    # (oczekiwany stan przejsciowy w przeplywie dwuetapowym, nie blad).
    if connection_mode == "push" and not source.webhook_token:
        logger.info(
            "Zrodlo push utworzone BEZ webhook_token (has_webhook_token=false) | "
            "id=%s name=%r — oczekuje POST /admin/sources/%s/webhook-token",
            source.id_source, source_name, source.id_source,
        )

    logger.info(
        "Zrodlo utworzone | id=%s name=%r type=%r mode=%r actor=%s",
        source.id_source, source_name, source_type, connection_mode, actor_id,
    )
    return source


async def update_source(
    db: AsyncSession,
    id_source: int,
    *,
    actor_id: int,
    source_name: str | None = None,
    source_type: str | None = None,
    connection_mode: str | None = None,
    connection_config: dict[str, Any] | None = None,
    sync_interval_minutes: int | None = None,
    is_active: bool | None = None,
) -> DocumentSource:
    """Aktualizuje zrodlo (partial update). Tylko podane pola sa zmieniane."""
    source = await get_source(db, id_source)

    changes: dict[str, Any] = {}

    if source_name is not None and source_name != source.source_name:
        source.source_name = source_name
        changes["source_name"] = source_name
    if source_type is not None and source_type != source.source_type:
        source.source_type = source_type
        changes["source_type"] = source_type
    if connection_mode is not None and connection_mode != source.connection_mode:
        source.connection_mode = connection_mode
        changes["connection_mode"] = connection_mode
        # Przelaczenie pull -> push bez tokenu jest niepoprawne — ostrzegamy
        if connection_mode == "push" and not source.webhook_token:
            logger.warning(
                "Zrodlo id=%s przelaczone na push bez webhook_token — "
                "wywolaj POST /sources/%s/webhook-token", id_source, id_source,
            )
    if connection_config is not None:
        # Uzywamy source.source_type / source.connection_mode PO ewentualnej
        # zmianie powyzej w tym samym wywolaniu (przypisania juz sie
        # wykonaly) — to daje efektywny tryb/typ DLA TEJ operacji, nie
        # stary stan sprzed partial-update.
        _validate_connection_config_for_mode(
            source.source_type, source.connection_mode, connection_config
        )
        try:
            source.set_config(connection_config)
        except ValueError as exc:
            raise SourceValidationError(f"Blad szyfrowania konfiguracji: {exc}") from exc
        changes["connection_config"] = "<zmieniono>"
    if sync_interval_minutes is not None:
        source.sync_interval_minutes = sync_interval_minutes
        changes["sync_interval_minutes"] = sync_interval_minutes
    if is_active is not None:
        source.is_active = is_active
        changes["is_active"] = is_active

    errors = source.validate()
    if errors:
        raise SourceValidationError("; ".join(errors))

    if changes:
        await _audit_log(
            db, actor_id=actor_id, action="source.updated",
            entity_id=id_source, details=changes,
        )

    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise SourceNameConflictError(
            f"Zrodlo o nazwie '{source_name}' juz istnieje."
        ) from exc

    _log_config_snapshot(source, changed_by_user_id=actor_id, reason="updated")

    logger.info("Zrodlo zaktualizowane | id=%s changes=%s actor=%s", id_source, list(changes), actor_id)
    return source


async def delete_source(db: AsyncSession, id_source: int, *, actor_id: int) -> None:
    """
    Usuwa zrodlo (hard delete).

    Blokuje usuniecie jesli istnieja powiazane instancje obiegu —
    zrodlo musi zostac dezaktywowane (is_active=False) zamiast usuniete
    gdy ma historie.
    """
    source = await get_source(db, id_source)

    count_result = await db.execute(
        text(
            f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_document_approval_instances] "
            f"WHERE [id_source] = :id"
        ),
        {"id": id_source},
    )
    instance_count = count_result.scalar() or 0

    if instance_count > 0:
        raise HTTPException(
            status_code=409,
            detail={
                "code":    "source.has_instances",
                "message": (
                    f"Zrodlo ma {instance_count} powiazanych instancji obiegu. "
                    f"Nie mozna usunac — dezaktywuj zrodlo (PUT z is_active=false) "
                    f"zamiast usuwac."
                ),
                "instance_count": instance_count,
            },
        )

    await _audit_log(
        db, actor_id=actor_id, action="source.deleted",
        entity_id=id_source, details={"source_name": source.source_name},
    )

    await db.delete(source)
    await db.commit()

    logger.warning("Zrodlo usuniete | id=%s name=%r actor=%s", id_source, source.source_name, actor_id)


# =============================================================================
# WEBHOOK TOKEN
# =============================================================================

async def generate_webhook_token(
    db: AsyncSession,
    id_source: int,
    *,
    actor_id: int,
    base_url: str,
) -> dict[str, Any]:
    """
    Generuje nowy token webhooka. Stary token (jesli istnial) jest natychmiast
    uniewazniony — to jest jednoczesnie operacja "regeneracji".

    Token jest zwracany w plaintext WYLACZNIE w tej odpowiedzi — nigdy ponownie.

    Args:
        base_url: Bazowy URL API (np. 'https://api.example.com/api/v1')
                  do zbudowania pelnego webhook_url w odpowiedzi.

    Raises:
        HTTPException(400): Zrodlo nie ma connection_mode='push'.
    """
    source = await get_source(db, id_source)

    if source.connection_mode != "push":
        raise HTTPException(
            status_code=400,
            detail={
                "code":    "source.not_webhook",
                "message": (
                    f"Zrodlo '{source.source_name}' ma connection_mode="
                    f"'{source.connection_mode}'. Webhook token wymaga 'push'."
                ),
            },
        )

    token = secrets.token_urlsafe(_WEBHOOK_TOKEN_BYTES)
    had_previous = bool(source.webhook_token)
    source.webhook_token = token

    await _audit_log(
        db, actor_id=actor_id, action="source.webhook_token_regenerated",
        entity_id=id_source,
        details={"had_previous_token": had_previous},
    )
    await db.commit()

    now = datetime.now(timezone.utc)
    webhook_url = f"{base_url.rstrip('/')}/webhooks/sources/{token}"

    logger.warning(
        "Webhook token wygenerowany | id_source=%s actor=%s had_previous=%s",
        id_source, actor_id, had_previous,
    )

    return {
        "id_source":    id_source,
        "token":        token,
        "webhook_url":  webhook_url,
        "generated_at": now,
    }


async def revoke_webhook_token(db: AsyncSession, id_source: int, *, actor_id: int) -> None:
    """Uniewazni token webhooka bez generowania nowego (zrodlo przestaje przyjmowac push)."""
    source = await get_source(db, id_source)

    if not source.webhook_token:
        return  # nic do uniewaznienia — idempotentne

    source.webhook_token = None
    await _audit_log(
        db, actor_id=actor_id, action="source.webhook_token_revoked",
        entity_id=id_source, details={},
    )
    await db.commit()

    logger.warning("Webhook token uniewazniony | id_source=%s actor=%s", id_source, actor_id)


async def verify_webhook_token(db: AsyncSession, token: str) -> DocumentSource | None:
    """
    Weryfikuje token webhooka i zwraca odpowiadajace zrodlo.

    Uzywa constant-time comparison (secrets.compare_digest) zeby nie
    wyciekac informacji o poprawnosci tokenu przez timing attack.

    Returns:
        DocumentSource jesli token poprawny i zrodlo aktywne, None w przeciwnym razie.
    """
    if not token or len(token) > 200:
        return None

    # Pobierz wszystkie aktywne zrodla push — porownanie constant-time
    # wymaga iteracji (nie mozemy uzyc WHERE webhook_token = token bezpiecznie
    # pod katem timing, ale przy malej liczbie zrodel narzut jest niewielki)
    result = await db.execute(
        text(
            f"SELECT [id_source] FROM [{_SCHEMA}].[skw_document_sources] "
            f"WHERE [connection_mode] = N'push' "
            f"  AND [is_active] = 1 "
            f"  AND [webhook_token] IS NOT NULL"
        )
    )
    candidate_ids = [r[0] for r in result.fetchall()]
    if not candidate_ids:
        return None

    sources_result = await db.execute(
        select(DocumentSource).where(DocumentSource.id_source.in_(candidate_ids))
    )
    for source in sources_result.scalars().all():
        if source.webhook_token and secrets.compare_digest(source.webhook_token, token):
            return source

    return None


# =============================================================================
# TEST CONNECTION
# =============================================================================

async def test_connection(db: AsyncSession, id_source: int, redis: Any = None) -> dict[str, Any]:
    """
    Testuje polaczenie ze zrodlem bez zapisywania zadnych danych.

    Dla source_type='database' — wykonuje proste zapytanie weryfikacyjne
    (SELECT TOP 1) na widoku/procedurze z connection_config.
    Dla innych typow — placeholder (do rozszerzenia w F7 przy dodawaniu adapterow).
    """
    source = await get_source(db, id_source)
    t_start = time.monotonic()

    # NAPRAWA 2026-07-16 (Tier 2, Rozstrzygniecia Koncowe #2): test-connection
    # zaklada wychodzace polaczenie (sensowne dla pull) — dla push nie ma nic
    # do "testowania" w ten sposob (zewnetrzny system sam inicjuje polaczenie
    # do naszego webhooka, nie odwrotnie).
    # UWAGA: analogiczny blad "zly connection_mode dla operacji" w
    # generate_webhook_token() (ponizej w tym pliku) uzywa HTTP 400.
    # Tutaj celowo 409 (Conflict) — semantyka lepiej pasuje do proby
    # operacji sprzecznej z biezacym stanem zasobu. Niespojnosc kodow
    # HTTP miedzy tymi dwoma miejscami jest znana i zaakceptowana,
    # nie przeoczeniem — nie ujednolicac bez osobnej decyzji.
    if source.connection_mode != "pull":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "source.test_connection_not_applicable",
                "message": (
                    f"Test polaczenia niedostepny dla connection_mode='{source.connection_mode}'. "
                    "Zrodla push nie maja polaczenia wychodzacego do przetestowania."
                ),
            },
        )

    try:
        cfg = source.get_config()
    except ValueError as exc:
        return {
            "success":      False,
            "message":      f"Blad odczytu konfiguracji: {exc}",
            "latency_ms":   None,
            "sample_count": None,
            "tested_at":    datetime.now(timezone.utc),
        }

    if source.source_type == "database":
        view_name = cfg.get("view_name")
        if not view_name:
            return {
                "success":      False,
                "message":      "connection_config nie zawiera 'view_name'.",
                "latency_ms":   None,
                "sample_count": None,
                "tested_at":    datetime.now(timezone.utc),
            }

        import re
        if not re.match(r"^[\w.]+$", view_name):
            return {
                "success":      False,
                "message":      "view_name zawiera niedozwolone znaki.",
                "latency_ms":   None,
                "sample_count": None,
                "tested_at":    datetime.now(timezone.utc),
            }

        # POPRAWKA: [{view_name}] dla nazwy z kropka (np. 'dbo.widok') dawalo
        # blednie '[dbo.widok]' (jeden identyfikator z kropka), zamiast
        # dwuczesciowego '[dbo].[widok]'. Patrz identyczna poprawka w
        # DatabaseAdapter._bracket_qualify (unified_document.py / source_adapter.py).
        qualified_view = ".".join(f"[{p}]" for p in view_name.split(".") if p)

        try:
            result = await db.execute(text(f"SELECT TOP 5 * FROM {qualified_view}"))
            rows = result.fetchall()
            cols = list(result.keys()) if rows else []
            latency_ms = round((time.monotonic() - t_start) * 1000)

            sample_fields = []
            if rows and cols:
                first_row = rows[0]
                for i, col in enumerate(cols):
                    val = first_row[i] if i < len(first_row) else None
                    sample_fields.append({
                        "field_name":   col,
                        "sample_value": str(val) if val is not None else None,
                    })

            if redis and sample_fields:
                try:
                    import json as _json
                    await redis.set(
                        f"field_preview:{id_source}",
                        _json.dumps(sample_fields, ensure_ascii=False, default=str),
                        ex=3600,
                    )
                except Exception as cache_exc:
                    logger.warning(
                        "test_connection: blad zapisu field_preview cache: %s", cache_exc
                    )

            return {
                "success":      True,
                "message":      f"Polaczenie OK. Widok '{view_name}' dostepny.",
                "latency_ms":   latency_ms,
                "sample_count": len(rows),
                "fields":       sample_fields,
                "tested_at":    datetime.now(timezone.utc),
            }
        except Exception as exc:
            latency_ms = round((time.monotonic() - t_start) * 1000)
            return {
                "success":      False,
                "message":      f"Blad polaczenia: {type(exc).__name__}: {str(exc)[:200]}",
                "latency_ms":   latency_ms,
                "sample_count": None,
                "tested_at":    datetime.now(timezone.utc),
            }

    if source.source_type == "ftp":
        return await _test_connection_ftp(cfg, t_start, id_source, redis)

    if source.source_type == "email":
        return await _test_connection_email(cfg, t_start, id_source, redis)

    if source.source_type == "api":
        return await _test_connection_api(cfg, t_start, id_source, redis)

    # Inne typy zrodel (api, ksef20, manual) — test connection dla tych typow
    # albo nie ma zastosowania (manual: brak polaczenia zewnetrznego), albo
    # zostanie dodany osobno przy nastepnej turze prac (api, ksef20).
    return {
        "success":      False,
        "message":      f"Test polaczenia dla source_type='{source.source_type}' nie jest jeszcze zaimplementowany.",
        "latency_ms":   None,
        "sample_count": None,
        "tested_at":    datetime.now(timezone.utc),
    }


# =============================================================================
# FIELD PREVIEW — podglad pol do mapowania (TYLKO database/api)
# =============================================================================

# source_type, dla ktorych mapowanie pol architektonicznie nie ma zastosowania —
# patrz notatka "Mapowanie pol nie dotyczy zrodel plikowych (FTP/Email)" +
# ustalenie dla ksef20 z tej samej sesji (14.07.2026). Zrodlo prawdy: konstruktory
# FtpAdapter/EmailAdapter/KSeF20Adapter nie przyjmuja field_mappings.
_SOURCE_TYPES_WITHOUT_FIELD_MAPPING = frozenset({"ftp", "email", "ksef20", "manual"})


class FieldPreviewNotApplicableError(Exception):
    """source_type nie obsluguje mapowania pol — nie jest to blad cache, tylko architektury."""


class FieldPreviewCacheEmptyError(Exception):
    """source_type obsluguje mapowanie, ale cache jeszcze nie zostal zapelniony."""


async def get_field_preview(db: AsyncSession, redis: Any, id_source: int) -> dict[str, Any]:
    """
    Zwraca probke pol do mapowania — WYLACZNIE dla source_type, ktore realnie
    obsluguja field_mappings w adapterze (database, api).

    Dla ftp/email/ksef20/manual — rzuca FieldPreviewNotApplicableError zamiast
    sugerowac "wykonaj test-connection", bo dla tych typow zaden test-connection
    nigdy nie zapelni cache (adaptery nie przyjmuja field_mappings w konstruktorze).

    Raises:
        SourceNotFoundError:            zrodlo nie istnieje.
        FieldPreviewNotApplicableError: source_type nie wspiera mapowania.
        FieldPreviewCacheEmptyError:    source_type wspiera mapowanie, ale cache pusty.
    """
    source = await get_source(db, id_source)

    if source.source_type in _SOURCE_TYPES_WITHOUT_FIELD_MAPPING:
        logger.info(
            "get_field_preview: source_type=%r nie obsluguje mapowania pol | id_source=%s",
            source.source_type, id_source,
        )
        raise FieldPreviewNotApplicableError(source.source_type)

    if not redis:
        raise FieldPreviewCacheEmptyError(id_source)

    try:
        cached_raw = await redis.get(f"field_preview:{id_source}")
    except Exception as exc:
        logger.warning("get_field_preview: blad odczytu cache Redis dla id_source=%s: %s", id_source, exc)
        raise FieldPreviewCacheEmptyError(id_source) from exc

    if not cached_raw:
        raise FieldPreviewCacheEmptyError(id_source)

    try:
        fields = json.loads(cached_raw)
    except (TypeError, ValueError) as exc:
        logger.error("get_field_preview: cache field_preview:%s zawiera nieprawidlowy JSON: %s", id_source, exc)
        raise FieldPreviewCacheEmptyError(id_source) from exc

    return {
        "id_source": id_source,
        "source_type": source.source_type,
        "fields": fields,
    }

# =============================================================================
# SYNC TRIGGER + STATUS
# =============================================================================

async def trigger_sync(db: AsyncSession, redis: Any, id_source: int, *, actor_id: int) -> dict[str, Any]:
    """
    Kolejkuje natychmiastowa synchronizacje zrodla (poza normalnym cyklem cron).

    Sprawdza distributed lock sync_lock:{id_source} — jesli synchronizacja
    juz trwa, zwraca queued=False z odpowiednia wiadomoscia.
    """
    source = await get_source(db, id_source)

    if source.connection_mode != "pull":
        return {
            "queued":  False,
            "job_id":  None,
            "message": f"Zrodlo ma connection_mode='{source.connection_mode}' — sync recznie nie ma zastosowania (push czeka na webhook).",
        }

    if not source.is_active:
        return {
            "queued":  False,
            "job_id":  None,
            "message": "Zrodlo jest nieaktywne (is_active=False).",
        }

    lock_key = f"sync_lock:{id_source}"
    if redis:
        is_locked = await redis.get(lock_key)
        if is_locked:
            return {
                "queued":  False,
                "job_id":  None,
                "message": "Synchronizacja tego zrodla juz trwa (lock aktywny).",
            }

    # Enqueue do ARQ — wspoldzieli ten sam task co cron (source_sync_task),
    # ale tylko dla jednego zrodla
    job_id = None
    try:
        from app.core.arq_pool import get_arq_pool
        arq_pool = get_arq_pool()
        job = await arq_pool.enqueue_job("source_sync_task_single", id_source=id_source)
        job_id = job.job_id if job else None
    except Exception as exc:
        logger.error("trigger_sync: blad enqueue ARQ dla id_source=%s: %s", id_source, exc)
        return {
            "queued":  False,
            "job_id":  None,
            "message": f"Blad kolejkowania: {exc}",
        }

    await _audit_log(
        db, actor_id=actor_id, action="source.sync_triggered_manually",
        entity_id=id_source, details={"job_id": job_id},
    )
    await db.commit()

    logger.info("Sync recznie wywolany | id_source=%s job_id=%s actor=%s", id_source, job_id, actor_id)

    return {
        "queued":  True,
        "job_id":  job_id,
        "message": "Synchronizacja zakolejkowana.",
    }


async def get_sync_status(db: AsyncSession, redis: Any, id_source: int) -> dict[str, Any]:
    """Status synchronizacji zrodla — do polling przez UI panelu admina."""
    source = await get_source(db, id_source)

    next_sync_at = None
    if source.connection_mode == "pull" and source.last_sync_at and source.sync_interval_minutes:
        from datetime import timedelta
        last = source.last_sync_at
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        next_sync_at = last + timedelta(minutes=source.sync_interval_minutes)

    is_syncing = False
    if redis:
        is_syncing = bool(await redis.get(f"sync_lock:{id_source}"))

    return {
        "id_source":            id_source,
        "last_sync_at":         source.last_sync_at,
        "last_sync_status":     source.last_sync_status,
        "last_sync_message":    source.last_sync_message,
        "next_sync_at":         next_sync_at,
        "is_currently_syncing": is_syncing,
    }


# =============================================================================
# HEALTH DASHBOARD
# =============================================================================

async def get_health(db: AsyncSession) -> dict[str, Any]:
    """
    Przeglad zdrowia wszystkich zrodel — dashboard admina.

    Klasyfikacja per zrodlo:
      ok       — ostatni sync < warning_minutes temu, status='ok'
      warning  — ostatni sync miedzy warning a critical, LUB status='partial'
      critical — ostatni sync > critical_minutes temu, LUB status='error'
      unknown  — nigdy nie zsynchronizowane (last_sync_at is NULL) i is_active=True
    """
    warning_min  = await _get_config_int(db, "source_health.warning_minutes", _DEFAULT_SYNC_WARNING_MINUTES)
    critical_min = await _get_config_int(db, "source_health.critical_minutes", _DEFAULT_SYNC_CRITICAL_MINUTES)

    result = await db.execute(select(DocumentSource).order_by(DocumentSource.id_source.asc()))
    sources = list(result.scalars().all())

    entries: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for s in sources:
        minutes_since_sync: int | None = None
        if s.last_sync_at:
            last = s.last_sync_at
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            minutes_since_sync = round((now - last).total_seconds() / 60)

        if not s.is_active:
            health = "unknown"
        elif s.connection_mode == "push":
            # push zrodla nie mają cyklicznego sync — health = ok jesli ostatni status nie byl error
            health = "critical" if s.last_sync_status == "error" else "ok"
        elif s.last_sync_status == "error":
            health = "critical"
        elif minutes_since_sync is None:
            health = "unknown"
        elif minutes_since_sync >= critical_min:
            health = "critical"
        elif minutes_since_sync >= warning_min or s.last_sync_status == "partial":
            health = "warning"
        else:
            health = "ok"

        entries.append({
            "id_source":          s.id_source,
            "source_name":        s.source_name,
            "is_active":          s.is_active,
            "is_test_mode":       s.is_test_mode,
            "last_sync_status":   s.last_sync_status,
            "last_sync_at":       s.last_sync_at,
            "minutes_since_sync": minutes_since_sync,
            "health":             health,
        })

    if any(e["health"] == "critical" for e in entries):
        overall = "critical"
    elif any(e["health"] == "warning" for e in entries):
        overall = "warning"
    else:
        overall = "ok"

    return {
        "sources":        entries,
        "overall_health": overall,
        "checked_at":     now,
    }


# =============================================================================
# TEST MODE TOGGLE
# =============================================================================

async def set_test_mode(
    db: AsyncSession, id_source: int, *, is_test_mode: bool, actor_id: int,
) -> DocumentSource:
    """
    Przelacza tryb testowy zrodla.

    Przejscie test->produkcyjny (is_test_mode: True->False) jest logowane
    z wyzszym priorytetem — to moment od ktorego hooki krytyczne zaczynaja
    realnie wplywac na systemy zewnetrzne (Fakir).
    """
    source = await get_source(db, id_source)
    was_test = source.is_test_mode
    source.is_test_mode = is_test_mode

    if was_test and not is_test_mode:
        logger.warning(
            "Zrodlo PRZELACZONE NA PRODUKCYJNE | id=%s name=%r actor=%s",
            id_source, source.source_name, actor_id,
        )
        await _audit_log(
            db, actor_id=actor_id, action="source.switched_to_production",
            entity_id=id_source, details={"source_name": source.source_name},
        )
    else:
        await _audit_log(
            db, actor_id=actor_id, action="source.test_mode_changed",
            entity_id=id_source, details={"is_test_mode": is_test_mode},
        )

    await db.commit()
    return source


# =============================================================================
# Pomocnicze
# =============================================================================

# =============================================================================
# TEST CONNECTION — implementacje per source_type: ftp, email
# =============================================================================
# UWAGA: rozstrzygniecia wlasne (dokumentacja nie precyzuje):
#   - Timeout 15s per polaczenie testowe — wartosc arbitralna, wystarczajaca
#     dla polaczenia+login+listowania, bez ryzyka wieszania requestu HTTP.
#   - Test NIGDY nie pobiera plikow/wiadomosci — tylko listuje/liczy, spojne
#     z zasada "test nie zapisuje zadnych danych" z docstringa test_connection().
#   - Probka nazw plikow ograniczona do 5 — spojne z DatabaseAdapter (SELECT TOP 5).
#   - Walidacja pol connection_config lustrzana wzgledem FtpAdapter/EmailAdapter
#     (worker/services/ftp_adapter.py, email_adapter.py) — ten sam kontrakt,
#     zeby "Testuj polaczenie" w UI nigdy nie klamalo wzgledem realnej synchronizacji.
#   - Haslo NIGDY nie trafia do logow — logowane sa wylacznie host/port/login/
#     protocol/folder, nigdy connection_config w calosci.
# =============================================================================

_FTP_TEST_TIMEOUT_SECONDS = 15
_EMAIL_TEST_TIMEOUT_SECONDS = 15
_MAX_DIRECTORY_LENGTH = 500
_MAX_FILE_PATTERN_LENGTH = 100
_MAX_FOLDER_LENGTH = 200


def _test_connection_fail(message: str, t_start: float) -> dict[str, Any]:
    """Buduje jednolita odpowiedz bledu dla test_connection_* — DRY."""
    return {
        "success":      False,
        "message":      message,
        "latency_ms":   round((time.monotonic() - t_start) * 1000),
        "sample_count": None,
        "tested_at":    datetime.now(timezone.utc),
    }


async def _test_connection_ftp(
    cfg: dict[str, Any], t_start: float, id_source: int, redis: Any,
) -> dict[str, Any]:
    """
    Testuje polaczenie FTP/SFTP bez pobierania zadnych plikow.

    Waliduje connection_config identycznie jak FtpAdapter
    (worker/services/ftp_adapter.py) — 'protocol' jest WYMAGANY, zero
    domyslnego zgadywania po porcie (ta sama decyzja co tam, patrz UWAGA 1
    w naglowku tamtego pliku). Sprawdza polaczenie + login + dostepnosc
    katalogu, liczy pliki pasujace do file_pattern, zwraca probke nazw.
    """
    host          = cfg.get("host")
    login         = cfg.get("login")
    password      = cfg.get("password", "")
    directory     = cfg.get("directory", "/")
    file_pattern  = cfg.get("file_pattern", "*")
    protocol      = cfg.get("protocol")

    # --- Walidacja wejscia (redundantna, defensywna) ---
    if not host or not isinstance(host, str):
        return _test_connection_fail("Brak lub nieprawidlowy 'host' w connection_config.", t_start)
    if not login or not isinstance(login, str):
        return _test_connection_fail("Brak lub nieprawidlowy 'login' w connection_config.", t_start)
    if protocol not in ("ftp", "sftp"):
        return _test_connection_fail(
            f"Pole 'protocol' jest WYMAGANE i musi byc 'ftp' albo 'sftp' "
            f"(otrzymano: {protocol!r}). Swiadomy wybor administratora — brak "
            f"domyslnego zgadywania po porcie.",
            t_start,
        )
    if not isinstance(directory, str) or len(directory) > _MAX_DIRECTORY_LENGTH:
        return _test_connection_fail("Pole 'directory' ma nieprawidlowy typ lub przekracza dozwolona dlugosc.", t_start)
    if not isinstance(file_pattern, str) or len(file_pattern) > _MAX_FILE_PATTERN_LENGTH:
        return _test_connection_fail("Pole 'file_pattern' ma nieprawidlowy typ lub przekracza dozwolona dlugosc.", t_start)

    default_port = 22 if protocol == "sftp" else 21
    try:
        port = int(cfg.get("port") or default_port)
        if not (1 <= port <= 65535):
            raise ValueError
    except (TypeError, ValueError):
        return _test_connection_fail("Pole 'port' musi byc liczba calkowita w zakresie 1-65535.", t_start)

    logger.info(
        "test_connection[ftp]: rozpoczynam test | host=%s port=%s protocol=%s login=%s directory=%s",
        host, port, protocol, login, directory,
    )

    try:
        if protocol == "sftp":
            sample = await asyncio.to_thread(
                _ftp_test_sftp_sync, host, port, login, password, directory, file_pattern
            )
        else:
            sample = await asyncio.to_thread(
                _ftp_test_ftp_sync, host, port, login, password, directory, file_pattern
            )
    except ImportError as exc:
        return _test_connection_fail(
            f"Brak biblioteki wymaganej dla protokolu '{protocol}' w kontenerze backendu: {exc}. "
            f"Sprawdz backend/requirements.txt (paramiko).",
            t_start,
        )
    except Exception as exc:
        latency_ms = round((time.monotonic() - t_start) * 1000)
        logger.warning(
            "test_connection[ftp]: polaczenie nieudane | host=%s port=%s protocol=%s "
            "blad=%s typ=%s latency_ms=%s",
            host, port, protocol, str(exc)[:200], type(exc).__name__, latency_ms,
        )
        return {
            "success":      False,
            "message":      f"Blad polaczenia {protocol.upper()}: {type(exc).__name__}: {str(exc)[:200]}",
            "latency_ms":   latency_ms,
            "sample_count": None,
            "tested_at":    datetime.now(timezone.utc),
        }

        # Cache pol do podgladu (diagnostyczny — patrz docstring _cache_field_preview).
    # Probka bierze PIERWSZY dopasowany plik; jesli sample_names jest puste
    # (katalog istnieje, ale nic nie pasuje do wzorca), fields = same nazwy pol
    # bez wartosci przykladowej — front i tak zobaczy STRUKTURE.
    first_name = sample["sample_names"][0] if sample["sample_names"] else None
    preview_fields = [
        {"field_name": "original_filename", "sample_value": first_name},
        {"field_name": "file_path", "sample_value": None},  # znane dopiero po pobraniu — nie testujemy pobierania
        {"field_name": "file_size", "sample_value": None},  # jw. — test nie pobiera plikow, wylacznie listuje
    ]
    await _cache_field_preview(redis, id_source, preview_fields)

    latency_ms = round((time.monotonic() - t_start) * 1000)
    logger.info(
        "test_connection[ftp]: polaczenie OK | host=%s port=%s protocol=%s "
        "plikow_pasujacych=%d latency_ms=%s",
        host, port, protocol, sample["matched_count"], latency_ms,
    )
    return {
        "success":      True,
        "message":      (
            f"Polaczenie {protocol.upper()} OK. Katalog '{directory}' dostepny, "
            f"{sample['matched_count']} plik(ow) pasuje do wzorca '{file_pattern}'."
        ),
        "latency_ms":   latency_ms,
        "sample_count": sample["matched_count"],
        "fields":       [{"field_name": "plik", "sample_value": n} for n in sample["sample_names"]],
        "tested_at":    datetime.now(timezone.utc),
    }


def _ftp_test_ftp_sync(host: str, port: int, login: str, password: str,
                        directory: str, file_pattern: str) -> dict[str, Any]:
    """Synchroniczna czesc testu FTP — wywolywana przez asyncio.to_thread."""
    with FTP() as ftp:
        ftp.connect(host, port, timeout=_FTP_TEST_TIMEOUT_SECONDS)
        ftp.login(login, password)
        ftp.cwd(directory)
        filenames = ftp.nlst()
    matched = [n for n in filenames if fnmatch.fnmatch(n, file_pattern)]
    return {"matched_count": len(matched), "sample_names": matched[:5]}


def _ftp_test_sftp_sync(host: str, port: int, login: str, password: str,
                         directory: str, file_pattern: str) -> dict[str, Any]:
    """Synchroniczna czesc testu SFTP — wywolywana przez asyncio.to_thread."""
    import paramiko

    transport = paramiko.Transport((host, port))
    try:
        transport.banner_timeout = _FTP_TEST_TIMEOUT_SECONDS
        transport.connect(username=login, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            filenames = sftp.listdir(directory)
        finally:
            sftp.close()
    finally:
        transport.close()
    matched = [n for n in filenames if fnmatch.fnmatch(n, file_pattern)]
    return {"matched_count": len(matched), "sample_names": matched[:5]}


async def _test_connection_email(
    cfg: dict[str, Any], t_start: float, id_source: int, redis: Any,
) -> dict[str, Any]:
    """
    Testuje polaczenie IMAP bez pobierania zadnych wiadomosci.
    Waliduje connection_config identycznie jak EmailAdapter
    (worker/services/email_adapter.py).
    """
    host    = cfg.get("host")
    login   = cfg.get("login")
    password = cfg.get("password", "")
    folder  = cfg.get("folder", "INBOX")
    use_ssl = bool(cfg.get("use_ssl", True))

    if not host or not isinstance(host, str):
        return _test_connection_fail("Brak lub nieprawidlowy 'host' w connection_config.", t_start)
    if not login or not isinstance(login, str):
        return _test_connection_fail("Brak lub nieprawidlowy 'login' w connection_config.", t_start)
    if not isinstance(folder, str) or len(folder) > _MAX_FOLDER_LENGTH:
        return _test_connection_fail("Pole 'folder' ma nieprawidlowy typ lub przekracza dozwolona dlugosc.", t_start)

    default_port = 993 if use_ssl else 143
    try:
        port = int(cfg.get("port") or default_port)
        if not (1 <= port <= 65535):
            raise ValueError
    except (TypeError, ValueError):
        return _test_connection_fail("Pole 'port' musi byc liczba calkowita w zakresie 1-65535.", t_start)

    logger.info(
        "test_connection[email]: rozpoczynam test | host=%s port=%s ssl=%s login=%s folder=%s",
        host, port, use_ssl, login, folder,
    )

    try:
        message_count, sample_envelope = await asyncio.to_thread(
            _email_test_sync, host, port, login, password, folder, use_ssl
        )
    except Exception as exc:
        latency_ms = round((time.monotonic() - t_start) * 1000)
        logger.warning(
            "test_connection[email]: polaczenie nieudane | host=%s port=%s "
            "blad=%s typ=%s latency_ms=%s",
            host, port, str(exc)[:200], type(exc).__name__, latency_ms,
        )
        return {
            "success":      False,
            "message":      f"Blad polaczenia IMAP: {type(exc).__name__}: {str(exc)[:200]}",
            "latency_ms":   latency_ms,
            "sample_count": None,
            "tested_at":    datetime.now(timezone.utc),
        }

    latency_ms = round((time.monotonic() - t_start) * 1000)
    # NOWY (zastępuje cały fragment z Diff 4 dotyczący sample_envelope):
    # UWAGA: rozstrzygniecie wlasne — test connection NIGDY nie pobiera tresci
    # wiadomosci (zgodnie z zasada ustalona w naglowku tej sekcji). Cache
    # field_preview zawiera WYLACZNIE stale nazwy pol z EmailAdapter.raw_data
    # (worker/services/email_adapter.py), bez probek wartosci — bo jedyny
    # sposob zdobycia realnej probki wymagalby pobrania tresci wiadomosci,
    # co jest poza zakresem tej funkcji. To i tak cache czysto diagnostyczny:
    # EmailAdapter nie przyjmuje field_mappings w konstruktorze, wiec front
    # NIE powinien oferowac kroku mapowania dla tego typu zrodla (patrz
    # rekomendacja w dokumencie dla frontu, sekcja 7).
    preview_fields = [
        {"field_name": "email_subject",    "sample_value": None},
        {"field_name": "email_from",       "sample_value": None},
        {"field_name": "email_message_id", "sample_value": None},
        {"field_name": "original_filename","sample_value": None},
        {"field_name": "file_path",        "sample_value": None},
    ]
    await _cache_field_preview(redis, id_source, preview_fields)
    logger.info(
        "test_connection[email]: polaczenie OK | host=%s port=%s folder=%s "
        "wiadomosci=%d latency_ms=%s",
        host, port, folder, message_count, latency_ms,
    )
    return {
        "success":      True,
        "message":      f"Polaczenie IMAP OK. Folder '{folder}' dostepny, {message_count} wiadomosci.",
        "latency_ms":   latency_ms,
        "sample_count": message_count,
        "tested_at":    datetime.now(timezone.utc),
    }


def _email_test_sync(host: str, port: int, login: str, password: str,
                      folder: str, use_ssl: bool) -> int:
    """Synchroniczna czesc testu IMAP — wywolywana przez asyncio.to_thread."""
    if use_ssl:
        conn = imaplib.IMAP4_SSL(host, port, timeout=_EMAIL_TEST_TIMEOUT_SECONDS)
    else:
        conn = imaplib.IMAP4(host, port, timeout=_EMAIL_TEST_TIMEOUT_SECONDS)
    try:
        conn.login(login, password)
        status, data = conn.select(folder, readonly=True)
        if status != "OK":
            raise RuntimeError(f"Nie mozna otworzyc folderu '{folder}': {status}")
        count = int(data[0]) if data and data[0] else 0

        sample_envelope: dict[str, Optional[str]] = {"subject": None, "from": None}
        if count > 0:
            # Probka NAJNOWSZEJ wiadomosci — tylko koperta (ENVELOPE), zero
            # pobierania tresci/zalacznikow. UWAGA: rozstrzygniecie wlasne —
            # dokumentacja nie precyzuje ktora wiadomosc powinna byc probka.
            _status, msg_nums = conn.search(None, "ALL")
            if msg_nums and msg_nums[0]:
                last_num = msg_nums[0].split()[-1]
                _status, env_data = conn.fetch(last_num, "(BODY[HEADER.FIELDS (SUBJECT FROM)])")
                if env_data and env_data[0] and len(env_data[0]) > 1:
                    header_bytes = env_data[0][1]
                    header_text = header_bytes.decode("utf-8", errors="replace")
                    for line in header_text.splitlines():
                        if line.lower().startswith("subject:"):
                            sample_envelope["subject"] = line.split(":", 1)[1].strip()
                        elif line.lower().startswith("from:"):
                            sample_envelope["from"] = line.split(":", 1)[1].strip()

        return count, sample_envelope
    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            conn.logout()
        except Exception:
            pass

async def _get_config_int(db: AsyncSession, key: str, default: int) -> int:
    try:
        result = await db.execute(
            text(
                f"SELECT [ConfigValue] FROM [{_SCHEMA}].[skw_SystemConfig] "
                f"WHERE [ConfigKey] = :k AND [IsActive] = 1"
            ),
            {"k": key},
        )
        row = result.fetchone()
        return int(row[0]) if row else default
    except Exception:
        return default


async def _audit_log(
    db: AsyncSession,
    *,
    actor_id: int,
    action: str,
    entity_id: int,
    details: dict[str, Any],
) -> None:
    """Zapisuje wpis do AuditLog. Blad zapisu nie przerywa operacji."""
    try:
        await db.execute(
            text(
                f"INSERT INTO [{_SCHEMA}].[skw_AuditLog] "
                f"([ID_USER], [Action], [EntityType], [EntityID], [NewValue], [Success], [Timestamp]) "
                f"VALUES (:uid, :action, N'DocumentSource', :eid, :details, 1, SYSUTCDATETIME())"
            ),
            {
                "uid":     actor_id,
                "action":  action,
                "eid":     str(entity_id),
                "details": json.dumps(details, ensure_ascii=False, default=str),
            },
        )
    except Exception as exc:
        logger.error("_audit_log: blad zapisu dla action=%s: %s", action, exc)

_FIELD_PREVIEW_TTL_SECONDS = 3600  # spojne z galezia database (linia ~519 oryginalu)


async def _cache_field_preview(redis: Any, id_source: int, fields: list[dict[str, Any]]) -> None:
    """
    Zapisuje probke pol do Redis pod kluczem field_preview:{id_source} — DOKLADNIE
    ten sam format i TTL co galaz source_type='database' (patrz test_connection()).

    UWAGA: dla ftp/email te pola sa STALE (raw_data z FtpAdapter/EmailAdapter),
    NIE pochodza z konfigurowalnego field_mappings — te adaptery go nie przyjmuja
    w konstruktorze. Front NIE powinien oferowac kroku "mapowania" tych pol,
    bo adapter i tak go zignoruje. Cache istnieje wylacznie zeby GET /field-preview
    nie zwracalo bledu i dawalo podglad diagnostyczny.
    """
    if not redis or not fields:
        return
    try:
        await redis.set(
            f"field_preview:{id_source}",
            json.dumps(fields, ensure_ascii=False, default=str),
            ex=_FIELD_PREVIEW_TTL_SECONDS,
        )
    except Exception as exc:
        logger.warning(
            "_cache_field_preview: blad zapisu cache dla id_source=%s: %s",
            id_source, exc,
        )

# =============================================================================
# TEST CONNECTION — source_type='api'
# =============================================================================
# UWAGA: wierne odwzorowanie logiki juz istniejacego RestApiAdapter
# (backend/app/schemas/unified_document.py) — te same nazwy pol config,
# ta sama walidacja auth_type, ten sam sposob budowania naglowkow i
# odswiezania tokenu Bearer. Rozstrzygniecia wlasne (dokumentacja tego
# nie precyzuje dla samego testu, tylko dla realnej synchronizacji):
#   - page_size=1 wymuszone w tescie (nie z configu source'a) — minimalizuje
#     obciazenie zewnetrznego API, testujemy DOSTEPNOSC, nie pobieramy danych.
#   - Nazwy pol do field_preview brane z pierwszego zwroconego rekordu —
#     dla API ma to sens (w przeciwienstwie do ftp/email), bo RestApiAdapter
#     REALNIE uzywa field_mappings, wiec ten podglad jest uzyteczny, nie
#     kosmetyczny.
#   - Limit 30 pol w probce — zabezpieczenie przed absurdalnie szerokim
#     JSON-em (np. API zwracajace 200 kolumn) zapychajacym cache Redis.
# =============================================================================

_API_TEST_TIMEOUT_SECONDS = 15
_MAX_BASE_URL_LENGTH = 500
_MAX_ENDPOINT_LENGTH = 300
_VALID_API_AUTH_TYPES = {"bearer_refresh", "api_key", "basic", "none"}


async def _test_connection_api(
    cfg: dict[str, Any], t_start: float, id_source: int, redis: Any,
) -> dict[str, Any]:
    """
    Testuje polaczenie REST API — jedno zapytanie GET z page_size=1,
    bez pobierania pelnej listy dokumentow. Waliduje connection_config
    identycznie jak RestApiAdapter._validate_config().
    """
    base_url    = (cfg.get("base_url") or "").rstrip("/")
    auth_type   = cfg.get("auth_type", "api_key")
    auth_config = cfg.get("auth_config", {})
    ep_list     = cfg.get("endpoint_list", "")
    pagination  = cfg.get("pagination", {})

    if not base_url or not isinstance(base_url, str):
        return _test_connection_fail("Brak lub nieprawidlowy 'base_url' w connection_config.", t_start)
    if len(base_url) > _MAX_BASE_URL_LENGTH:
        return _test_connection_fail("Pole 'base_url' przekracza dozwolona dlugosc.", t_start)
    if not ep_list or not isinstance(ep_list, str):
        return _test_connection_fail("Brak lub nieprawidlowy 'endpoint_list' w connection_config.", t_start)
    if len(ep_list) > _MAX_ENDPOINT_LENGTH:
        return _test_connection_fail("Pole 'endpoint_list' przekracza dozwolona dlugosc.", t_start)
    if auth_type not in _VALID_API_AUTH_TYPES:
        return _test_connection_fail(
            f"Pole 'auth_type' musi byc jednym z {sorted(_VALID_API_AUTH_TYPES)} "
            f"(otrzymano: {auth_type!r}).",
            t_start,
        )
    if not isinstance(auth_config, dict):
        return _test_connection_fail("Pole 'auth_config' musi byc obiektem JSON.", t_start)

    logger.info(
        "test_connection[api]: rozpoczynam test | base_url=%s auth_type=%s endpoint_list=%s",
        base_url, auth_type, ep_list,
    )

    try:
        sample = await _api_test_request(base_url, ep_list, auth_type, auth_config, pagination)
    except ImportError as exc:
        return _test_connection_fail(f"Brak biblioteki httpx w kontenerze backendu: {exc}.", t_start)
    except Exception as exc:
        latency_ms = round((time.monotonic() - t_start) * 1000)
        logger.warning(
            "test_connection[api]: polaczenie nieudane | base_url=%s blad=%s typ=%s latency_ms=%s",
            base_url, str(exc)[:200], type(exc).__name__, latency_ms,
        )
        return {
            "success":      False,
            "message":      f"Blad polaczenia API: {type(exc).__name__}: {str(exc)[:200]}",
            "latency_ms":   latency_ms,
            "sample_count": None,
            "tested_at":    datetime.now(timezone.utc),
        }

    latency_ms = round((time.monotonic() - t_start) * 1000)

    if sample["field_names"]:
        preview_fields = [
            {"field_name": name, "sample_value": sample["first_item"].get(name)}
            for name in sample["field_names"]
        ]
        await _cache_field_preview(redis, id_source, preview_fields)

    logger.info(
        "test_connection[api]: polaczenie OK | base_url=%s status_code=%s pol_znalezionych=%d latency_ms=%s",
        base_url, sample["status_code"], len(sample["field_names"]), latency_ms,
    )
    return {
        "success":      True,
        "message":      (
            f"Polaczenie API OK (HTTP {sample['status_code']}). "
            f"Znaleziono {sample['item_count']} rekord(ow), {len(sample['field_names'])} pol w probce."
        ),
        "latency_ms":   latency_ms,
        "sample_count": sample["item_count"],
        "fields":       [
            {"field_name": n, "sample_value": sample["first_item"].get(n)}
            for n in sample["field_names"]
        ],
        "tested_at":    datetime.now(timezone.utc),
    }


def _api_build_auth_headers(auth_type: str, auth_config: dict[str, Any]) -> dict[str, str]:
    """Identyczna logika jak RestApiAdapter._get_auth_headers() — zero rozjazdu zachowania."""
    if auth_type == "none":
        return {}

    if auth_type == "api_key":
        key = auth_config.get("api_key", "")
        hdr = auth_config.get("header_name", "X-Api-Key")
        return {hdr: key}
    if auth_type == "basic":
        import base64
        login = auth_config.get("login", "")
        pwd   = auth_config.get("password", "")
        token = base64.b64encode(f"{login}:{pwd}".encode()).decode()
        return {"Authorization": f"Basic {token}"}
    if auth_type == "bearer_refresh":
        token = auth_config.get("access_token", "")
        return {"Authorization": f"Bearer {token}"}
    return {}


async def _api_refresh_bearer_token(auth_config: dict[str, Any]) -> None:
    """Identyczna logika jak RestApiAdapter._refresh_bearer_token() — mutuje auth_config in-place."""
    import httpx

    refresh_url = auth_config.get("token_url", "")
    if not refresh_url:
        return
    async with httpx.AsyncClient(timeout=_API_TEST_TIMEOUT_SECONDS) as client:
        resp = await client.post(
            refresh_url,
            data={
                "grant_type":    "refresh_token",
                "refresh_token": auth_config.get("refresh_token", ""),
                "client_id":     auth_config.get("client_id", ""),
                "client_secret": auth_config.get("client_secret", ""),
            },
        )
        resp.raise_for_status()
        data = resp.json()
        auth_config["access_token"] = data.get("access_token", "")


async def _api_test_request(
    base_url: str, ep_list: str, auth_type: str,
    auth_config: dict[str, Any], pagination: dict[str, Any],
) -> dict[str, Any]:
    """
    Jedno zapytanie GET testowe (page_size=1). Zwraca liczbe rekordow i
    nazwy pol pierwszego rekordu do field_preview. Wylacznie odczyt.
    """
    import httpx

    url     = f"{base_url}{ep_list}"
    headers = _api_build_auth_headers(auth_type, auth_config)

    page_param = pagination.get("page_param", "page")
    params = {page_param: 1, "page_size": 1}

    async with httpx.AsyncClient(timeout=_API_TEST_TIMEOUT_SECONDS) as client:
        resp = await client.get(url, headers=headers, params=params)

        if resp.status_code == 401 and auth_type == "bearer_refresh":
            await _api_refresh_bearer_token(auth_config)
            headers = _api_build_auth_headers(auth_type, auth_config)
            resp = await client.get(url, headers=headers, params=params)

        resp.raise_for_status()
        data = resp.json()

    items_key = pagination.get("items_key", "data")
    items = data if isinstance(data, list) else data.get(items_key, [])

    first_item = items[0] if items and isinstance(items[0], dict) else {}

    return {
        "status_code": resp.status_code,
        "item_count":  len(items) if isinstance(items, list) else 0,
        "first_item":  first_item,
        "field_names": list(first_item.keys())[:30],
    }