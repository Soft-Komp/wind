# backend/app/services/maintenance_data_service.py
"""
Serwis danych konserwacyjnych — zbiera stan systemu do sekcji maintenance
w kazdej odpowiedzi API (gdy maintenance_mode.enabled=true).

Trzy zrodla danych:
  1. ARQ job registry     — ile success/failed w ostatniej godzinie
  2. Synchronizacja zrodel — ostatni sync per zrodlo, status, liczba dokumentow
  3. OCR pipeline          — ile przetworzone w ostatniej godzinie, srednia confidence

Wszystkie dane cachowane w Redis (klucz: maintenance:data, TTL 15s).
Jeden zbiorczy klucz zamiast trzech — jeden round-trip do Redis per request.
Baza odpytywana maksymalnie raz na 15 sekund niezaleznie od ruchu.

UWAGA: from __future__ import annotations OK (nie ORM, nie router).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_SCHEMA        = "dbo"
_CACHE_KEY     = "maintenance:data"
_CACHE_TTL     = 15   # sekund
_HOURS_WINDOW  = 1    # okno czasowe dla ARQ i OCR — ostatnia godzina


async def get_maintenance_data(db: Any, redis: Any) -> dict[str, Any]:
    """
    Glowna funkcja — zwraca dane konserwacyjne do sekcji maintenance.

    Kolejnosc:
      1. Probuje zwrocic z Redis cache (< 1ms)
      2. Jesli cache miss — odpytuje baze (3 zapytania) i zapisuje do cache
      3. Przy kazdym bledzie zwraca czesciowe dane (fail-safe)

    Returns:
        {
            "jobs":    {...},   # stan zadan ARQ
            "sources": {...},   # synchronizacja zrodel
            "ocr":     {...},   # wyniki OCR
            "cached_at": "...", # kiedy dane zostaly zebrane
        }
    """
    # Probuj z cache
    if redis:
        try:
            cached = await redis.get(_CACHE_KEY)
            if cached is not None:
                raw = cached.decode() if isinstance(cached, bytes) else cached
                return json.loads(raw)
        except Exception as exc:
            logger.debug("maintenance_data: cache miss / blad Redis: %s", exc)

    # Cache miss — zbierz z bazy
    data = await _collect_fresh(db)

    # Zapisz do cache
    if redis:
        try:
            await redis.set(_CACHE_KEY, json.dumps(data, ensure_ascii=False, default=str), ex=_CACHE_TTL)
        except Exception as exc:
            logger.debug("maintenance_data: blad zapisu cache: %s", exc)

    return data


async def _collect_fresh(db: Any) -> dict[str, Any]:
    """Odpytuje baze i sklada kompletny obiekt maintenance."""
    from sqlalchemy import text

    now_iso = datetime.now(timezone.utc).isoformat()

    jobs_data    = await _collect_jobs(db, text)
    sources_data = await _collect_sources(db, text)
    ocr_data     = await _collect_ocr(db, text)

    return {
        "jobs":      jobs_data,
        "sources":   sources_data,
        "ocr":       ocr_data,
        "cached_at": now_iso,
    }


# =============================================================================
# 1. ARQ Job Registry
# =============================================================================

async def _collect_jobs(db: Any, text: Any) -> dict[str, Any]:
    """
    Stan zadan ARQ z ostatniej godziny.

    Zwraca:
      by_status:    {"success": N, "failed": N, "running": N, "queued": N}
      failed_tasks: lista nazw taskow z liczba bledow (max 5 najgorszych)
      total:        laczna liczba zadan
    """
    try:
        result = await db.execute(text(f"""
            SELECT [status], COUNT(*) AS cnt
            FROM [{_SCHEMA}].[skw_ArqJobRegistry]
            WHERE [enqueued_at] >= DATEADD(HOUR, -{_HOURS_WINDOW}, SYSUTCDATETIME())
            GROUP BY [status]
        """))
        by_status = {r[0]: r[1] for r in result.fetchall()}

        failed_result = await db.execute(text(f"""
            SELECT TOP 5 [task_name], COUNT(*) AS cnt
            FROM [{_SCHEMA}].[skw_ArqJobRegistry]
            WHERE [status] = N'failed'
              AND [enqueued_at] >= DATEADD(HOUR, -{_HOURS_WINDOW}, SYSUTCDATETIME())
            GROUP BY [task_name]
            ORDER BY cnt DESC
        """))
        failed_tasks = [
            {"task": r[0], "failed": r[1]}
            for r in failed_result.fetchall()
        ]

        total = sum(by_status.values())
        return {
            "by_status":    by_status,
            "failed_tasks": failed_tasks,
            "total":        total,
            "window_hours": _HOURS_WINDOW,
        }
    except Exception as exc:
        logger.warning("_collect_jobs: blad: %s", exc)
        return {"error": str(exc)[:100]}


# =============================================================================
# 2. Synchronizacja zrodel
# =============================================================================

async def _collect_sources(db: Any, text: Any) -> dict[str, Any]:
    """
    Stan synchronizacji zrodel dokumentow.

    Zwraca:
      sources: lista aktywnych zrodel z ostatnim statusem sync
      summary: {"ok": N, "error": N, "never": N}
    """
    try:
        result = await db.execute(text(f"""
            SELECT
                [source_name],
                [source_type],
                [last_sync_status],
                [last_sync_at],
                [last_sync_message],
                [is_test_mode]
            FROM [{_SCHEMA}].[skw_document_sources]
            WHERE [is_active] = 1
            ORDER BY [last_sync_at] DESC
        """))
        cols = list(result.keys())
        sources = []
        summary = {"ok": 0, "error": 0, "partial": 0, "never": 0}

        for row in result.fetchall():
            r = dict(zip(cols, row))
            status = r.get("last_sync_status") or "never"
            if status in summary:
                summary[status] += 1
            else:
                summary["never"] += 1

            sources.append({
                "name":         r["source_name"],
                "type":         r["source_type"],
                "status":       status,
                "last_sync_at": r["last_sync_at"],
                "message":      (r.get("last_sync_message") or "")[:100],
                "test_mode":    bool(r.get("is_test_mode")),
            })

        return {
            "sources": sources,
            "summary": summary,
        }
    except Exception as exc:
        logger.warning("_collect_sources: blad: %s", exc)
        return {"error": str(exc)[:100]}


# =============================================================================
# 3. OCR pipeline
# =============================================================================

async def _collect_ocr(db: Any, text: Any) -> dict[str, Any]:
    """
    Wyniki OCR z ostatniej godziny (z skw_ArqJobRegistry, task_name='ocr_task').

    Zwraca:
      processed:         liczba przetworzonych dokumentow
      success:           liczba udanych
      failed:            liczba bledow
      avg_confidence:    srednia confidence z extra_data result_summary
      enabled:           czy OCR_ENABLED=true w SystemConfig
    """
    try:
        # Liczniki z rejestru zadan
        result = await db.execute(text(f"""
            SELECT [status], COUNT(*) AS cnt
            FROM [{_SCHEMA}].[skw_ArqJobRegistry]
            WHERE [task_name] = N'ocr_task'
              AND [enqueued_at] >= DATEADD(HOUR, -{_HOURS_WINDOW}, SYSUTCDATETIME())
            GROUP BY [status]
        """))
        by_status = {r[0]: r[1] for r in result.fetchall()}
        processed = sum(by_status.values())
        success   = by_status.get("success", 0)
        failed    = by_status.get("failed", 0)

        # Srednia confidence z result_summary JSON (tylko dla success)
        avg_confidence = None
        if success > 0:
            conf_result = await db.execute(text(f"""
                SELECT TOP 100 [result_summary]
                FROM [{_SCHEMA}].[skw_ArqJobRegistry]
                WHERE [task_name] = N'ocr_task'
                  AND [status] = N'success'
                  AND [enqueued_at] >= DATEADD(HOUR, -{_HOURS_WINDOW}, SYSUTCDATETIME())
                  AND [result_summary] IS NOT NULL
            """))
            confidences = []
            for row in conf_result.fetchall():
                try:
                    summary = json.loads(row[0])
                    conf = summary.get("confidence")
                    if conf is not None:
                        confidences.append(float(conf))
                except Exception:
                    pass
            if confidences:
                avg_confidence = round(sum(confidences) / len(confidences), 2)

        # Czy OCR wlaczony
        ocr_enabled_result = await db.execute(text(f"""
            SELECT [ConfigValue] FROM [{_SCHEMA}].[skw_SystemConfig]
            WHERE [ConfigKey] = N'OCR_ENABLED' AND [IsActive] = 1
        """))
        ocr_row = ocr_enabled_result.fetchone()
        ocr_enabled = str(ocr_row[0]).lower() == "true" if ocr_row else False

        return {
            "enabled":        ocr_enabled,
            "processed":      processed,
            "success":        success,
            "failed":         failed,
            "avg_confidence": avg_confidence,
            "window_hours":   _HOURS_WINDOW,
        }
    except Exception as exc:
        logger.warning("_collect_ocr: blad: %s", exc)
        return {"error": str(exc)[:100]}