# worker/tasks/ksef_sync_task.py
"""
ARQ Task: ksef_sync_task — cykliczna synchronizacja zrodel KSeF 2.0.

Osobny task/cron od source_sync_task, bo eksport paczek KSeF moze trwac
do 30 minut (oczekiwanie na przygotowanie paczki przez serwery MF) —
umieszczenie tego w tym samym cyklu co Fakir/FTP/inne zrodla blokowaloby
ich synchronizacje na ten czas.

Cykl bazowy: co 60 minut (WorkerSettings.cron_jobs).
Timeout zadania ARQ: 45 minut (2700s) — patrz worker/main.py.

Dla kazdego aktywnego zrodla source_type='ksef20':
  1. Pobierz adapter (worker.services.source_adapter.get_adapter_by_source_id)
  2. Wywolaj adapter.fetch_new_documents(since=last_sync_at)
  3. Zapisz kazdy dokument przez _upsert_instance — REUZYTE z source_sync_task.py,
     zawiera juz logike invoice_type=1 -> cancelled (patrz tamten plik)
  4. Zaktualizuj last_sync_at / last_sync_status / last_sync_message

Bezpieczenstwo:
  - Distributed lock Redis: ksef_source_sync_lock:{id_source} (TTL 50 min) —
    ODREBNY od source_sync_lock uzywanego przez source_sync_task dla innych
    zrodel, zeby dlugi czas synchronizacji KSeF nie kolidowal z nimi.
  - KSeF20Adapter ma WLASNY, wewnetrzny lock (ksef_lock:{id_source}) chroniacy
    sama autoryzacje XAdES — to dwa rozne, niezalezne poziomy ochrony.

UWAGA: from __future__ import annotations — OK w pliku workera.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from worker.core.db import get_engine
from worker.core.logging_setup import get_event_logger
from worker.settings import get_settings
from worker.services.source_adapter import get_adapter_by_source_id
from worker.tasks.source_sync_task import _upsert_instance, _mark_sync_status

logger = logging.getLogger("worker.tasks.ksef_sync")

_SCHEMA = "dbo"
_KSEF_SYNC_LOCK_PREFIX = "ksef_source_sync_lock:"
# TTL nieco wiekszy niz timeout ARQ (2700s/45min), zeby lock nie wygasl
# przed zakonczeniem taska w skrajnym przypadku dlugiej synchronizacji.
_KSEF_SYNC_LOCK_TTL = 3000


async def ksef_sync_task(ctx: dict[str, Any]) -> dict[str, Any]:
    """
    ARQ Cron Task: synchronizacja wszystkich aktywnych zrodel source_type='ksef20'.

    Uruchamiany co 60 minut (WorkerSettings.cron_jobs).
    """
    redis     = ctx.get("worker_redis")
    settings  = get_settings()
    t_start   = time.monotonic()
    now_utc   = datetime.now(timezone.utc)
    event_log = get_event_logger(settings.LOG_DIR)

    event_log.log("ksef_sync_started", {"ts_utc": now_utc.isoformat()})

    sources = await _get_ksef_sources_needing_sync()

    summary = {
        "ts_utc":      now_utc.isoformat(),
        "checked":     len(sources),
        "synced":      0,
        "skipped":     0,
        "errors":      0,
        "duration_ms": 0,
    }

    for source in sources:
        id_source = source["id_source"]
        lock_key = f"{_KSEF_SYNC_LOCK_PREFIX}{id_source}"

        if redis:
            acquired = await redis.set(lock_key, "1", ex=_KSEF_SYNC_LOCK_TTL, nx=True)
            if not acquired:
                logger.info(
                    "ksef_sync_task: zrodlo id=%s zablokowane (juz synchronizowane), pomijam",
                    id_source,
                )
                summary["skipped"] += 1
                continue

        try:
            result = await _sync_one_ksef_source(source, redis)
            if result == "ok":
                summary["synced"] += 1
            else:
                summary["errors"] += 1
        except Exception as exc:
            summary["errors"] += 1
            logger.error(
                "ksef_sync_task: blad dla zrodla id=%s: %s", id_source, exc, exc_info=True,
            )
            await _mark_sync_status(id_source, "error", str(exc)[:500])
        finally:
            if redis:
                try:
                    await redis.delete(lock_key)
                except Exception:
                    pass

    summary["duration_ms"] = round((time.monotonic() - t_start) * 1000, 1)
    logger.info("ksef_sync_task ZAKONCZONE", extra=summary)
    event_log.log("ksef_sync_completed", summary)
    return summary


async def _get_ksef_sources_needing_sync() -> list[dict[str, Any]]:
    """
    Pobiera aktywne zrodla source_type='ksef20', ktore potrzebuja synchronizacji
    (last_sync_at NULL lub minal sync_interval_minutes).
    """
    engine = get_engine()
    async with engine.connect() as conn:
        result = await conn.execute(
            text(f"""
                SELECT [id_source], [source_name], [last_sync_at],
                       [sync_interval_minutes], [is_test_mode]
                FROM [{_SCHEMA}].[skw_document_sources]
                WHERE [source_type] = N'ksef20'
                  AND [is_active] = 1
                  AND [connection_mode] = N'pull'
                  AND (
                      [last_sync_at] IS NULL
                      OR DATEADD(MINUTE, [sync_interval_minutes], [last_sync_at]) <= SYSUTCDATETIME()
                  )
            """),
        )
        cols = list(result.keys())
        return [dict(zip(cols, r)) for r in result.fetchall()]


async def _sync_one_ksef_source(source: dict[str, Any], redis: Any) -> str:
    """Synchronizuje jedno zrodlo KSeF. Zwraca 'ok' albo 'error'."""
    id_source    = source["id_source"]
    source_name  = source["source_name"]
    last_sync_at = source.get("last_sync_at")
    is_test_mode = bool(source.get("is_test_mode", False))

    engine = get_engine()
    async with engine.connect() as conn:
        adapter = await get_adapter_by_source_id(conn, id_source, redis=redis)

    if adapter is None:
        await _mark_sync_status(id_source, "error", "Brak adaptera dla tego source_type")
        return "error"

    try:
        docs = await adapter.fetch_new_documents(since=last_sync_at, limit=10000)
    except Exception as exc:
        logger.error(
            "ksef_sync: blad fetch_new_documents dla id_source=%s: %s",
            id_source, exc, exc_info=True,
        )
        await _mark_sync_status(id_source, "error", str(exc)[:500])
        return "error"

    inserted = 0
    updated  = 0
    errors   = 0
    for doc in docs:
        try:
            result = await _upsert_instance(doc, is_test_mode)
            if result == "inserted":
                inserted += 1
            else:
                updated += 1
        except Exception as exc:
            errors += 1
            # NAPRAWA (2026-07-23): podniesiony poziom z WARNING na ERROR —
            # bledy zapisu dokumentow KSeF nie moga byc niewidoczne przy
            # standardowej konfiguracji LOG_LEVEL=ERROR na produkcji. Dodano
            # exc_info=True (pelny traceback) i typ wyjatku — samo str(exc)
            # bez tego bylo niewystarczajace do diagnozy (zgloszenie
            # 2026-07-22: "bledow=7", zero informacji w logach).
            logger.error(
                "ksef_sync: blad zapisu dokumentu doc_id=%s ksef_numer=%s "
                "id_source=%s typ_wyjatku=%s: %s",
                doc.id_document,
                getattr(doc, "doc_number", "brak"),
                id_source,
                type(exc).__name__,
                exc,
                exc_info=True,
            )

    # NAPRAWA (2026-07-23): status nie moze byc na sztywno "ok" niezaleznie
    # od liczby bledow — analogicznie do source_sync_task._sync_one_source,
    # ktory juz rozroznia ok/partial/error. Wczesniej 7/7 bledow i tak
    # zapisywalo status "ok", co ukrywalo calkowita nieudana synchronizacje
    # przed administratorem patrzacym na dashboard zrodel.
    if len(docs) == 0:
        sync_status = "ok"
    elif errors == 0:
        sync_status = "ok"
    elif errors < len(docs):
        sync_status = "partial"
    else:
        sync_status = "error"

    await _mark_sync_status(
        id_source, sync_status,
        f"Zsynchronizowano {len(docs)} dokumentow "
        f"(nowych={inserted}, zaktualizowanych={updated}, bledow={errors})",
    )
    logger.info(
        "ksef_sync: zrodlo=%s (id=%s) zakonczone | fetched=%d inserted=%d updated=%d errors=%d status=%s",
        source_name, id_source, len(docs), inserted, updated, errors, sync_status,
    )
    return sync_status if sync_status != "error" else "error"