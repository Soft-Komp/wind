# backend/app/services/job_registry_service.py
"""
Serwis odczytu rejestru zadan ARQ — F7.

Wylacznie odczyt — zapisy robi worker przez job_tracker.py.
Ten serwis dziala na backendzie (FastAPI), nie w workerze.

UWAGA: from __future__ import annotations — NIGDY w tym pliku (SQLAlchemy ORM
nie jest tu uzywany bezposrednio, ale raw SQL przez text() i konwencja
projektu jest konsekwentna — bez tej linii).
"""

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_SCHEMA = "dbo"


async def list_jobs(
    db: AsyncSession,
    *,
    page: int = 1,
    per_page: int = 50,
    task_name: str | None = None,
    status: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict[str, Any]:
    """
    Lista zadan z rejestru ARQ, najnowsze pierwsze.

    Filtry opcjonalne: task_name (exact match), status, zakres dat po enqueued_at.
    """
    where: list[str] = []
    params: dict[str, Any] = {}

    if task_name:
        where.append("[task_name] = :task_name")
        params["task_name"] = task_name
    if status:
        where.append("[status] = :status")
        params["status"] = status
    if date_from:
        where.append("[enqueued_at] >= :date_from")
        params["date_from"] = date_from
    if date_to:
        where.append("[enqueued_at] <= :date_to_end")
        params["date_to_end"] = f"{date_to} 23:59:59"

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    count_result = await db.execute(
        text(f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_ArqJobRegistry] {where_sql}"),
        params,
    )
    total = count_result.scalar() or 0

    params["offset"] = (page - 1) * per_page
    params["limit"] = per_page

    result = await db.execute(
        text(f"""
            SELECT [id_job], [job_id], [task_name], [status],
                   [enqueued_at], [started_at], [finished_at], [duration_ms],
                   [result_summary], [error_message], [triggered_by]
            FROM [{_SCHEMA}].[skw_ArqJobRegistry]
            {where_sql}
            ORDER BY [enqueued_at] DESC
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
        """),
        params,
    )
    cols = list(result.keys())
    items = [_row_to_dict(dict(zip(cols, r))) for r in result.fetchall()]

    return {"items": items, "total": total, "page": page, "per_page": per_page}


async def get_job(db: AsyncSession, id_job: int) -> dict[str, Any] | None:
    """Pobiera pojedynczy wpis z rejestru. None jesli nie istnieje."""
    result = await db.execute(
        text(f"""
            SELECT [id_job], [job_id], [task_name], [status],
                   [enqueued_at], [started_at], [finished_at], [duration_ms],
                   [result_summary], [error_message], [triggered_by]
            FROM [{_SCHEMA}].[skw_ArqJobRegistry]
            WHERE [id_job] = :id
        """),
        {"id": id_job},
    )
    cols = list(result.keys())
    row = result.fetchone()
    return _row_to_dict(dict(zip(cols, row))) if row else None


async def get_summary(db: AsyncSession, *, hours: int = 24) -> dict[str, Any]:
    """
    Podsumowanie ostatnich N godzin — szybki przeglad "czy wszystko OK".

    Zwraca liczby per status oraz liste task_name ktore mialy >=1 failed
    (do szybkiego wychwycenia "ta wysylka maili nie wyszla").
    """
    result = await db.execute(
        text(f"""
            SELECT [status], COUNT(*) AS cnt
            FROM [{_SCHEMA}].[skw_ArqJobRegistry]
            WHERE [enqueued_at] >= DATEADD(HOUR, -:hours, SYSUTCDATETIME())
            GROUP BY [status]
        """),
        {"hours": hours},
    )
    by_status = {r[0]: r[1] for r in result.fetchall()}

    failed_result = await db.execute(
        text(f"""
            SELECT [task_name], COUNT(*) AS cnt, MAX([enqueued_at]) AS last_failed_at
            FROM [{_SCHEMA}].[skw_ArqJobRegistry]
            WHERE [status] = N'failed'
              AND [enqueued_at] >= DATEADD(HOUR, -:hours, SYSUTCDATETIME())
            GROUP BY [task_name]
            ORDER BY last_failed_at DESC
        """),
        {"hours": hours},
    )
    failed_tasks = [
        {"task_name": r[0], "failed_count": r[1], "last_failed_at": r[2]}
        for r in failed_result.fetchall()
    ]

    return {
        "hours":        hours,
        "by_status":    by_status,
        "failed_tasks": failed_tasks,
        "checked_at":   datetime.now(timezone.utc),
    }


def _row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    """Parsuje result_summary z JSON stringa do dict dla odpowiedzi API."""
    import json
    summary = row.get("result_summary")
    if summary:
        try:
            row["result_summary"] = json.loads(summary)
        except (json.JSONDecodeError, TypeError):
            pass  # zostaw jako raw string jesli nie da się sparsować
    return row