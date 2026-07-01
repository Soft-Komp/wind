# worker/tasks/source_sync_task.py
"""
ARQ Task: source_sync_task — cykliczna synchronizacja zrodel dokumentow.

Cykl bazowy: co SOURCE_SYNC_WORKER_INTERVAL_MINUTES (domyslnie 5 min).
Per-zrodlo: co sync_interval_minutes (z skw_document_sources).

Dla kazdego aktywnego zrodla pull ktore potrzebuje sync (needs_sync=True):
  1. Pobierz adapter (get_adapter_by_source_id)
  2. Wywolaj adapter.fetch_new_documents(since=last_sync_at)
  3. Zapisz kazdy UnifiedDocument przez _upsert_instance()
  4. Zaktualizuj last_sync_at / last_sync_status / last_sync_message

Idempotentnosc:
  MERGE nie INSERT — jesli dokument juz istnieje (id_source, id_document),
  worker aktualizuje pola (contractor_name, amount, document_title) zamiast
  tworzyc duplikat. Nowy wiersz tylko gdy brakuje.

Bezpieczenstwo:
  - Distributed lock Redis: source_sync_lock:{id_source} (TTL = 2 * timeout)
    Zapobiega rownoczesnemu uruchomieniu sync dla tego samego zrodla.
  - Test mode: dokumenty nie wchodza do obiegu (status=pending_dispatch blokowany).

Logowanie:
  - Strukturowany JSON do workera logger
  - JSONL event log per synchronizacja

Rejestracja w WorkerSettings.cron_jobs:
    cron(source_sync_task, minute={*/5})

UWAGA: from __future__ import annotations — OK w pliku workera (nie FastAPI router).
"""

from __future__ import annotations

import asyncio
from email.policy import default
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from worker.core.db import get_engine
from worker.core.logging_setup import get_event_logger
from worker.settings import get_settings

logger = logging.getLogger("worker.tasks.source_sync")

# Klucz Redis dla distributed locka synchronizacji zrodla
_SYNC_LOCK_PREFIX = "source_sync_lock:"
_SYNC_LOCK_TTL    = 600  # 10 minut — maks. czas jednej synchronizacji

# Statusy instancji w zaleznosci od trybu
_STATUS_NORMAL    = "pending_dispatch"
_STATUS_TEST_MODE = "pending_dispatch"  # tak samo — test_mode blokowany na poziomie dispatch


async def source_sync_task(ctx: dict[str, Any]) -> dict[str, Any]:
    """
    ARQ Cron Task: synchronizacja wszystkich aktywnych zrodel pull.

    Uruchamiany co SOURCE_SYNC_WORKER_INTERVAL_MINUTES.
    Dla kazdego zrodla z needs_sync=True: lock -> sync -> unlock.

    Returns:
        Slownik z podsumowaniem synchronizacji.
    """
    settings  = get_settings()
    redis     = ctx.get("worker_redis")
    task_start = time.monotonic()
    now_utc   = datetime.now(timezone.utc)

    logger.info(
        "source_sync_task START",
        extra={"ts_utc": now_utc.isoformat()},
    )

    event_log = get_event_logger(settings.LOG_DIR)
    event_log.log("source_sync_started", {"ts_utc": now_utc.isoformat()})

    # Pobierz liste zrodel do synchronizacji
    sources = await _get_sources_needing_sync()

    summary = {
        "ts_utc":          now_utc.isoformat(),
        "sources_checked": len(sources),
        "sources_synced":  0,
        "sources_skipped": 0,
        "sources_error":   0,
        "total_docs":      0,
        "total_new":       0,
        "total_updated":   0,
        "duration_ms":     0,
    }

    for source in sources:
        id_source   = source["id_source"]
        source_name = source["source_name"]
        source_type = source["source_type"]

        result = await _sync_one_source(ctx, source, redis, settings, event_log)

        if result["status"] == "ok":
            summary["sources_synced"] += 1
            summary["total_docs"]     += result.get("docs_fetched", 0)
            summary["total_new"]      += result.get("docs_new", 0)
            summary["total_updated"]  += result.get("docs_updated", 0)
        elif result["status"] == "skipped":
            summary["sources_skipped"] += 1
        else:
            summary["sources_error"] += 1

    summary["duration_ms"] = round((time.monotonic() - task_start) * 1000, 1)

    logger.info("source_sync_task ZAKONCZONE", extra=summary)
    event_log.log("source_sync_completed", summary)

    return summary


async def _get_sources_needing_sync() -> list[dict[str, Any]]:
    """
    Pobiera liste zrodel ktore potrzebuja synchronizacji.

    Filtruje: is_active=1, connection_mode='pull',
    (last_sync_at IS NULL OR minuty_od_last_sync >= sync_interval_minutes)
    """
    engine = get_engine()
    now_utc = datetime.now(timezone.utc)

    async with engine.connect() as conn:
        result = await conn.execute(text("""
            SELECT
                id_source,
                source_name,
                source_type,
                connection_config,
                sync_interval_minutes,
                last_sync_at,
                is_test_mode
            FROM dbo.skw_document_sources
            WHERE is_active = 1
              AND connection_mode = 'pull'
              AND source_type NOT IN ('manual', 'ksef20')
        """))

        cols = list(result.keys())   # MUSI byc przed fetchall() — SQLAlchemy 2.x async
        rows = result.fetchall()

    sources_to_sync = []
    for row in rows:
        s = dict(zip(cols, row))
        last_sync = s.get("last_sync_at")
        interval  = s.get("sync_interval_minutes", 15)

        needs_sync = False
        if last_sync is None:
            needs_sync = True
        else:
            if last_sync.tzinfo is None:
                last_sync = last_sync.replace(tzinfo=timezone.utc)
            elapsed = (now_utc - last_sync).total_seconds() / 60
            needs_sync = elapsed >= interval

        if needs_sync:
            sources_to_sync.append(s)
            logger.debug(
                "Zrodlo zakwalifikowane do sync | id=%s name=%s type=%s",
                s["id_source"], s["source_name"], s["source_type"],
            )

    return sources_to_sync


async def _sync_one_source(
    ctx: dict[str, Any],
    source: dict[str, Any],
    redis: Any,
    settings: Any,
    event_log: Any,
) -> dict[str, Any]:
    """
    Synchronizuje jedno zrodlo z distributed lockiem.

    Returns:
        dict z kluczami: status (ok|skipped|error), docs_fetched, docs_new, duration_ms
    """
    id_source    = source["id_source"]
    source_name  = source["source_name"]
    source_type  = source["source_type"]
    last_sync_at = source.get("last_sync_at")
    is_test_mode = bool(source.get("is_test_mode", False))

    lock_key = f"{_SYNC_LOCK_PREFIX}{id_source}"
    t_start  = time.monotonic()

    # Distributed lock — zapobiega rownoleglem sync tego samego zrodla
    if redis:
        acquired = await redis.set(lock_key, "1", ex=_SYNC_LOCK_TTL, nx=True)
        if not acquired:
            logger.info(
                "source_sync: zrodlo id=%s jest juz synchronizowane — pomijam", id_source
            )
            return {"status": "skipped", "reason": "lock_held"}

    try:
        logger.info(
            "source_sync START | id=%s name=%s type=%s test=%s since=%s",
            id_source, source_name, source_type, is_test_mode, last_sync_at,
        )

        # Pobierz adapter przez SQLAlchemy session
        engine = get_engine()
        async with engine.connect() as db_conn:
            # Tworzymy pseudo-session dla get_adapter_by_source_id
            from sqlalchemy.ext.asyncio import AsyncConnection
            from app.schemas.unified_document import get_adapter_by_source_id

            adapter = await get_adapter_by_source_id(db_conn, id_source, redis)

        if adapter is None:
            logger.warning(
                "source_sync: brak adaptera dla id=%s type=%s — pomijam",
                id_source, source_type,
            )
            await _mark_sync_status(id_source, "error", "Brak adaptera dla tego source_type")
            return {"status": "error", "reason": "no_adapter"}

        # Pobierz nowe dokumenty
        max_docs = int(await _get_config_value("SOURCE_SYNC_MAX_DOCUMENTS_PER_CYCLE", "500"))
        docs = await adapter.fetch_new_documents(
            db=None,  # adapter uzywa wlasnego polaczenia
            since=last_sync_at,
            limit=max_docs,
        )

        logger.info(
            "source_sync: pobrano %d dokumentow | id=%s", len(docs), id_source
        )

        # Zapisz do bazy
        docs_new = 0
        docs_updated = 0
        errors = 0

        for doc in docs:
            try:
                result = await _upsert_instance(doc, is_test_mode)
                if result == "inserted":
                    docs_new += 1
                else:
                    docs_updated += 1
            except Exception as exc:
                errors += 1
                logger.error(
                    "source_sync: blad upsert dokumentu id=%s source=%s: %s",
                    doc.id_document, source_name, exc,
                )

        # OCR — tylko dla zrodel FTP i email
        if source_type in ("ftp", "email") and redis:
            await _enqueue_ocr_for_new_docs(redis, id_source=id_source)

        # Zaktualizuj status zrodla
        if errors == 0:
            msg = f"Pobrano {len(docs)}: {docs_new} nowych, {docs_updated} zaktualizowanych"
            await _mark_sync_status(id_source, "ok", msg)
        elif errors < len(docs):
            msg = f"Czesciowy sukces: {len(docs) - errors}/{len(docs)} dokumentow OK, {errors} bledow"
            await _mark_sync_status(id_source, "partial", msg)
        else:
            await _mark_sync_status(id_source, "error", f"Wszystkie {errors} dokumenty nie powiodly sie")

        duration_ms = round((time.monotonic() - t_start) * 1000, 1)

        event_log.log("source_synced", {
            "id_source":    id_source,
            "source_name":  source_name,
            "docs_fetched": len(docs),
            "docs_new":     docs_new,
            "docs_updated": docs_updated,
            "errors":       errors,
            "duration_ms":  duration_ms,
            "is_test_mode": is_test_mode,
        })

        return {
            "status":      "ok",
            "docs_fetched": len(docs),
            "docs_new":     docs_new,
            "docs_updated": docs_updated,
            "errors":       errors,
            "duration_ms":  duration_ms,
        }

    except asyncio.CancelledError:
        logger.warning("source_sync: task anulowany dla id=%s", id_source)
        await _mark_sync_status(id_source, "error", "Task anulowany")
        raise

    except Exception as exc:
        duration_ms = round((time.monotonic() - t_start) * 1000, 1)
        logger.error(
            "source_sync BLAD | id=%s name=%s: %s", id_source, source_name, exc,
            exc_info=True,
        )
        await _mark_sync_status(id_source, "error", str(exc)[:500])
        event_log.log("source_sync_error", {
            "id_source":   id_source,
            "source_name": source_name,
            "error":       str(exc),
            "duration_ms": duration_ms,
        })
        return {"status": "error", "reason": str(exc)[:200], "duration_ms": duration_ms}

    finally:
        if redis:
            try:
                await redis.delete(lock_key)
            except Exception:
                pass


async def _upsert_instance(doc: Any, is_test_mode: bool) -> str:
    """
    MERGE dokumentu do skw_document_approval_instances.
    Zwraca 'inserted' jesli nowy rekord, 'updated' jesli istniejacy zaktualizowany.

    Idempotentnosc: identyfikacja po (id_source, id_document).
    Jesli status jest terminalny (approved/cancelled) — nie nadpisujemy.

    is_test_mode=True: extra_data aktualizowane, status pozostaje 'pending_dispatch'
    (nie wchodzi do obiegu az test_mode=False na poziomie dispatch).
    """
    engine = get_engine()
    now_utc = datetime.now(timezone.utc)
    extra_data_json = json.dumps(doc.to_extra_data_json(), ensure_ascii=False, default=str)
    document_title = (
        f"{doc.doc_number or 'Dok.'} | {doc.contractor_name or ''}"
    ).strip(" |") or f"Dokument #{doc.id_document}"

    async with engine.begin() as conn:
        # Sprawdz czy juz istnieje
        exists_result = await conn.execute(
            text("""
                SELECT id_instance, status
                FROM dbo.skw_document_approval_instances
                WHERE id_source = :src AND id_document = :doc
            """),
            {"src": doc.id_source, "doc": doc.id_document},
        )
        existing = exists_result.fetchone()

        if existing:
            id_instance, current_status = existing

            # Nie nadpisuj terminalnych statusow
            if current_status in ("approved", "cancelled", "rejected"):
                return "updated"

            # Zaktualizuj dostepne kolumny.
            # Tabela skw_document_approval_instances (migracja 0028) ma:
            #   document_title   NVARCHAR(500)
            #   document_amount  DECIMAL(18,2)
            #   extra_data       NVARCHAR(MAX) — JSON z pelnym zestawem pol
            # Kolumny doc_number, contractor_name, document_date sa w extra_data,
            # nie jako osobne kolumny — dane szczegolowe idą do extra_data JSON.
            await conn.execute(
                text("""
                    UPDATE dbo.skw_document_approval_instances
                    SET
                        document_title  = :title,
                        document_amount = :amount,
                        extra_data      = :extra,
                        updated_at      = :now
                    WHERE id_instance = :id
                """),
                {
                    "title":  document_title[:500],
                    "amount": float(doc.amount_gross) if doc.amount_gross else None,
                    "extra":  extra_data_json,
                    "now":    now_utc,
                    "id":     id_instance,
                },
            )
            return "updated"

        else:
            # Nowy dokument — tylko kolumny ktore istnieja w tabeli (migracja 0028 + 0039)
            await conn.execute(
                text("""
                    INSERT INTO dbo.skw_document_approval_instances (
                        id_source, id_document, status, document_title,
                        document_amount, extra_data, dispatch_attempts,
                        created_at, updated_at
                    ) VALUES (
                        :src, :doc, :status, :title,
                        :amount, :extra, 0,
                        :now, :now
                    )
                """),
                {
                    "src":    doc.id_source,
                    "doc":    doc.id_document,
                    "status": _STATUS_NORMAL,
                    "title":  document_title[:500],
                    "amount": float(doc.amount_gross) if doc.amount_gross else None,
                    "extra":  extra_data_json,
                    "now":    now_utc,
                },
            )
            return "inserted"


async def _mark_sync_status(id_source: int, status: str, message: str) -> None:
    """Aktualizuje last_sync_at, last_sync_status, last_sync_message w skw_document_sources."""
    engine = get_engine()
    now_utc = datetime.now(timezone.utc)

    try:
        async with engine.begin() as conn:
            await conn.execute(
                text("""
                    UPDATE dbo.skw_document_sources
                    SET
                        last_sync_at      = :now,
                        last_sync_status  = :status,
                        last_sync_message = :msg,
                        updated_at        = :now
                    WHERE id_source = :id
                """),
                {
                    "now":    now_utc,
                    "status": status[:20],
                    "msg":    message[:500],
                    "id":     id_source,
                },
            )
    except Exception as exc:
        logger.error(
            "_mark_sync_status blad | id=%s status=%s: %s", id_source, status, exc
        )

async def _get_config_value(key: str, default: str) -> str:
    """Pobiera wartosc z skw_SystemConfig. Fallback = default."""
    engine = get_engine()
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT ConfigValue FROM dbo.skw_SystemConfig
                    WHERE ConfigKey = :key AND IsActive = 1
                """),
                {"key": key},
            )
            row = result.fetchone()
            return row[0] if row and row[0] else default
    except Exception:
        return default
    except Exception:
        return default


async def _enqueue_ocr_for_new_docs(
    redis: Any,
    *,
    id_source: int,
) -> None:
    """Kolejkuje ocr_task dla nowych instancji ze zrodla FTP/email z ostatnich 6 minut."""
    engine = get_engine()
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text("""
                SELECT [id_instance], [extra_data]
                FROM [dbo].[skw_document_approval_instances]
                WHERE [id_source] = :s
                  AND [created_at] >= DATEADD(MINUTE, -6, SYSUTCDATETIME())
                  AND [extra_data] IS NOT NULL
                  AND [extra_data] LIKE '%file_path%'
            """), {"s": id_source})
            rows = result.fetchall()
    except Exception as exc:
        logger.warning("_enqueue_ocr: blad pobierania instancji: %s", exc)
        return

    if not rows:
        return

    queued = 0
    for id_instance, extra_data_raw in rows:
        try:
            extra = json.loads(extra_data_raw or "{}")
            file_path = extra.get("file_path")
            if not file_path:
                continue
            from arq.connections import ArqRedis
            arq: ArqRedis = redis
            await arq.enqueue_job("ocr_task", id_instance=id_instance, file_path=file_path)
            queued += 1
        except Exception as exc:
            logger.warning("_enqueue_ocr: blad dla id_instance=%d: %s", id_instance, exc)

    if queued:
        logger.info(
            "_enqueue_ocr_for_new_docs: zakolejkowano %d taskow OCR | id_source=%d",
            queued, id_source,
        )