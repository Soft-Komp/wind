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

import json
import logging
import secrets
import time
from datetime import datetime, timezone
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
        "sync_interval_minutes":  source.sync_interval_minutes,
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

async def test_connection(db: AsyncSession, id_source: int) -> dict[str, Any]:
    """
    Testuje polaczenie ze zrodlem bez zapisywania zadnych danych.

    Dla source_type='database' — wykonuje proste zapytanie weryfikacyjne
    (SELECT TOP 1) na widoku/procedurze z connection_config.
    Dla innych typow — placeholder (do rozszerzenia w F7 przy dodawaniu adapterow).
    """
    source = await get_source(db, id_source)
    t_start = time.monotonic()

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

        try:
            result = await db.execute(text(f"SELECT TOP 5 1 AS probe FROM [{view_name}]"))
            sample_count = len(result.fetchall())
            latency_ms = round((time.monotonic() - t_start) * 1000)
            return {
                "success":      True,
                "message":      f"Polaczenie OK. Widok '{view_name}' dostepny.",
                "latency_ms":   latency_ms,
                "sample_count": sample_count,
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

    # Inne typy zrodel — test connection bedzie rozszerzony przy implementacji
    # konkretnych adapterow (RestApiAdapter, FtpAdapter, EmailAdapter)
    return {
        "success":      False,
        "message":      f"Test polaczenia dla source_type='{source.source_type}' nie jest jeszcze zaimplementowany.",
        "latency_ms":   None,
        "sample_count": None,
        "tested_at":    datetime.now(timezone.utc),
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
    if redis:
        try:
            from arq.connections import ArqRedis
            arq_redis: ArqRedis = redis  # type: ignore[assignment]
            job = await arq_redis.enqueue_job("source_sync_task_single", id_source=id_source)
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
                f"([UserId], [Action], [EntityType], [EntityId], [NewValue], [Success], [Timestamp]) "
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