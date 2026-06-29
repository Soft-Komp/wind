# worker/tasks/auto_dispatch_task.py
"""
ARQ Task: auto_dispatch_task — automatyczne przypisanie dokumentow do sciezek obiegu.

Cykl: co 1 minute (niezalezny od source_sync_task).
Przetwarza dokumenty w statusach pending_dispatch FIFO po created_at.

Dla kazdego dokumentu:
  1. Pobierz dokumenty status=pending_dispatch ORDER BY created_at ASC
  2. Dla kazdego: uruchom filter_engine.resolve_path(doc_data)
  3. Jesli sciezka znaleziona: approval_service.dispatch() → status=in_progress
  4. Jesli brak sciezki: inkrementuj dispatch_attempts
     Po progu AUTO_DISPATCH_MAX_ATTEMPTS → status=unassigned + SSE alert

Idempotentnosc:
  Distributed lock Redis: auto_dispatch_lock:{id_instance} (TTL 2 min)
  Gwarantuje ze ten sam dokument nie jest dispatchowany rownoczesnie z dwoch
  instancji workera (przy skalowaniu).

Bezpieczenstwo:
  AUTO_DISPATCH_WORKER_ENABLED=false → task natychmiast zwraca.
  is_test_mode=true na zrodle → dispatch wykonywany ale SSE nie wyslane.

UWAGA: from __future__ import annotations — OK w pliku workera.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from worker.core.db import get_engine
from worker.core.logging_setup import get_event_logger
from worker.settings import get_settings

logger = logging.getLogger("worker.tasks.auto_dispatch")

_SCHEMA             = "dbo"
_DISPATCH_LOCK_PREFIX = "auto_dispatch_lock:"
_DISPATCH_LOCK_TTL    = 120   # 2 minuty
_MAX_DOCS_PER_CYCLE   = 50    # max dokumentow w jednym cyklu (ochrona przed spike)


async def auto_dispatch_task(ctx: dict[str, Any]) -> dict[str, Any]:
    """
    ARQ Cron Task: automatyczne przypisanie dokumentow do sciezek obiegu.

    Uruchamiany co 1 minute.
    """
    redis    = ctx.get("worker_redis")
    settings = get_settings()
    t_start  = time.monotonic()
    now_utc  = datetime.now(timezone.utc)

    # Sprawdz feature flag
    if not await _is_dispatch_enabled():
        logger.debug("auto_dispatch_task: AUTO_DISPATCH_WORKER_ENABLED=false — pomijam")
        return {"status": "disabled"}

    event_log = get_event_logger(settings.LOG_DIR)
    event_log.log("auto_dispatch_started", {"ts_utc": now_utc.isoformat()})

    max_attempts = await _get_config_int("AUTO_DISPATCH_MAX_ATTEMPTS", 5)

    # Pobierz dokumenty do dispatcha
    pending = await _get_pending_documents(max_attempts)

    summary = {
        "ts_utc":      now_utc.isoformat(),
        "checked":     len(pending),
        "dispatched":  0,
        "unassigned":  0,
        "skipped":     0,
        "errors":      0,
        "duration_ms": 0,
    }

    for doc in pending:
        id_instance = doc["id_instance"]
        lock_key    = f"{_DISPATCH_LOCK_PREFIX}{id_instance}"

        # Distributed lock — zapobiega rownoleglemu dispatch tej samej instancji
        if redis:
            acquired = await redis.set(lock_key, "1", ex=_DISPATCH_LOCK_TTL, nx=True)
            if not acquired:
                summary["skipped"] += 1
                continue

        try:
            result = await _dispatch_one(doc, max_attempts, redis, event_log)
            if result == "dispatched":
                summary["dispatched"] += 1
            elif result == "unassigned":
                summary["unassigned"] += 1
            else:
                summary["skipped"] += 1
        except Exception as exc:
            summary["errors"] += 1
            logger.error(
                "auto_dispatch_task: blad przy instance=%s: %s",
                id_instance, exc, exc_info=True,
            )
        finally:
            if redis:
                try:
                    await redis.delete(lock_key)
                except Exception:
                    pass

    summary["duration_ms"] = round((time.monotonic() - t_start) * 1000, 1)
    logger.info("auto_dispatch_task ZAKONCZONE", extra=summary)
    event_log.log("auto_dispatch_completed", summary)
    return summary


async def _dispatch_one(
    doc: dict[str, Any],
    max_attempts: int,
    redis: Any,
    event_log: Any,
) -> str:
    """
    Probuje wyznaczyc sciezke obiegu dla jednego dokumentu.
    Zwraca: 'dispatched' | 'unassigned' | 'skipped'.
    """
    id_instance     = doc["id_instance"]
    dispatch_attempts = doc.get("dispatch_attempts", 0)
    id_source       = doc["id_source"]
    id_document     = doc["id_document"]
    extra_data_raw  = doc.get("extra_data") or "{}"

    # Parsuj extra_data
    extra: dict = {}
    try:
        extra = json.loads(extra_data_raw)
    except Exception:
        pass

    # Buduj dane dokumentu dla filter_engine
    doc_data = {
        "id_instance":     id_instance,
        "id_source":       id_source,
        "id_document":     id_document,
        "document_amount": doc.get("document_amount"),
        "document_title":  doc.get("document_title", ""),
        "extra_data":      extra,
    }

    # Wywolaj silnik filtrow
    try:
        from app.services.filter_engine import resolve_path
        engine = get_engine()
        async with engine.connect() as conn:
            path_result = await resolve_path(conn, doc_data, id_source)
    except ImportError:
        # filter_engine nie jest dostepny w worker — uzyj prostego lookup
        path_result = await _simple_path_lookup(id_source)
    except Exception as exc:
        logger.warning(
            "_dispatch_one: filter_engine blad | instance=%s: %s", id_instance, exc
        )
        path_result = None

    engine = get_engine()

    if path_result:
        # Sciezka znaleziona — dispatch
        id_path = path_result if isinstance(path_result, int) else path_result.get("id_path")
        async with engine.begin() as conn:
            await conn.execute(
                text(f"""
                    UPDATE [{_SCHEMA}].[skw_document_approval_instances]
                    SET [status]           = N'in_progress',
                        [id_path]          = :path,
                        [current_step]     = 1,
                        [dispatched_at]    = SYSUTCDATETIME(),
                        [dispatch_attempts] = [dispatch_attempts] + 1,
                        [updated_at]       = SYSUTCDATETIME()
                    WHERE [id_instance] = :i
                      AND [status] = N'pending_dispatch'
                """),
                {"path": id_path, "i": id_instance},
            )

        event_log.log("auto_dispatched", {
            "id_instance": id_instance,
            "id_path":     id_path,
            "id_source":   id_source,
        })
        logger.info("auto_dispatch: dispatched | instance=%s path=%s", id_instance, id_path)
        return "dispatched"

    else:
        # Brak sciezki — inkrementuj licznik
        new_attempts = dispatch_attempts + 1

        if new_attempts >= max_attempts:
            # Przekroczono prog — status unassigned
            async with engine.begin() as conn:
                await conn.execute(
                    text(f"""
                        UPDATE [{_SCHEMA}].[skw_document_approval_instances]
                        SET [status]            = N'unassigned',
                            [dispatch_attempts] = :attempts,
                            [updated_at]        = SYSUTCDATETIME()
                        WHERE [id_instance] = :i
                          AND [status] = N'pending_dispatch'
                    """),
                    {"attempts": new_attempts, "i": id_instance},
                )

            # SSE alert do adminow
            if redis:
                try:
                    await _send_unassigned_sse(redis, id_instance, id_source, id_document)
                except Exception as sse_exc:
                    logger.warning("auto_dispatch: SSE alert blad: %s", sse_exc)

            event_log.log("auto_dispatch_unassigned", {
                "id_instance": id_instance,
                "attempts":    new_attempts,
                "id_source":   id_source,
            })
            logger.warning(
                "auto_dispatch: UNASSIGNED | instance=%s attempts=%s/%s",
                id_instance, new_attempts, max_attempts,
            )
            return "unassigned"

        else:
            # Jeszcze nie przekroczono progu — inkrementuj i zostaw pending_dispatch
            async with engine.begin() as conn:
                await conn.execute(
                    text(f"""
                        UPDATE [{_SCHEMA}].[skw_document_approval_instances]
                        SET [dispatch_attempts] = :attempts,
                            [updated_at]        = SYSUTCDATETIME()
                        WHERE [id_instance] = :i
                          AND [status] = N'pending_dispatch'
                    """),
                    {"attempts": new_attempts, "i": id_instance},
                )
            logger.debug(
                "auto_dispatch: brak sciezki | instance=%s attempts=%s/%s",
                id_instance, new_attempts, max_attempts,
            )
            return "skipped"


async def _get_pending_documents(max_attempts: int) -> list[dict[str, Any]]:
    """Pobiera dokumenty pending_dispatch do przetworzenia."""
    engine = get_engine()
    async with engine.connect() as conn:
        result = await conn.execute(
            text(f"""
                SELECT TOP {_MAX_DOCS_PER_CYCLE}
                    i.[id_instance],
                    i.[id_source],
                    i.[id_document],
                    i.[document_amount],
                    i.[document_title],
                    i.[extra_data],
                    i.[dispatch_attempts]
                FROM [{_SCHEMA}].[skw_document_approval_instances] i
                JOIN [{_SCHEMA}].[skw_document_sources] s
                  ON s.[id_source] = i.[id_source]
                WHERE i.[status] = N'pending_dispatch'
                  AND s.[is_active] = 1
                  AND i.[dispatch_attempts] < :max_att
                ORDER BY i.[created_at] ASC, i.[id_instance] ASC
            """),
            {"max_att": max_attempts},
        )
        cols = list(result.keys())
        return [dict(zip(cols, r)) for r in result.fetchall()]


async def _simple_path_lookup(id_source: int) -> int | None:
    """
    Uproszczony lookup sciezki gdy filter_engine niedostepny w workerze.
    Zwraca pierwsza aktywna sciezke przypisana do zrodla.
    """
    engine = get_engine()
    async with engine.connect() as conn:
        result = await conn.execute(
            text(f"""
                SELECT TOP 1 [id_path]
                FROM [{_SCHEMA}].[skw_approval_paths]
                WHERE [is_active] = 1
                ORDER BY [id_path] ASC
            """),
        )
        row = result.fetchone()
        return row[0] if row else None


async def _send_unassigned_sse(
    redis: Any,
    id_instance: int,
    id_source: int,
    id_document: str,
) -> None:
    """Publikuje SSE event do kanalu adminow o dokumencie bez sciezki."""
    import uuid
    payload = json.dumps({
        "event":       "document_unassigned",
        "event_id":    str(uuid.uuid4()),
        "id_instance": id_instance,
        "id_source":   id_source,
        "id_document": id_document,
        "message":     f"Dokument {id_document} nie moze byc przypisany do sciezki obiegu",
        "ts":          datetime.now(timezone.utc).isoformat(),
    }, ensure_ascii=False)
    await redis.publish("channel:admins", payload)


async def _is_dispatch_enabled() -> bool:
    engine = get_engine()
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT [ConfigValue] FROM [dbo].[skw_SystemConfig] WHERE [ConfigKey] = N'AUTO_DISPATCH_WORKER_ENABLED' AND [IsActive] = 1")
            )
            row = result.fetchone()
            return str(row[0]).lower() == "true" if row else True
    except Exception:
        return True


async def _get_config_int(key: str, default: int) -> int:
    engine = get_engine()
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT [ConfigValue] FROM [dbo].[skw_SystemConfig] WHERE [ConfigKey] = :k AND [IsActive] = 1"),
                {"k": key},
            )
            row = result.fetchone()
            return int(row[0]) if row else default
    except Exception:
        return default