# backend/app/api/approval/sources.py
"""
GET /approval/sources — lista zrodel dokumentow.

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging
from fastapi import APIRouter, Query
from sqlalchemy import text
from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.services.approval_service import _check_module_enabled
from app.schemas.common import BaseResponse, dt_utc

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sources")
_SCHEMA = "dbo"


@router.get(
    "",
    summary="Lista zrodel dokumentow",
    description=(
        "Zwraca aktywne zrodla dokumentow z liczba aktywnych instancji. "
        "Uzywane przez frontend do dropdownow: filtr kolejki, formularz dispatch, "
        "konfiguracja filtrow automatycznych. "
        "**Wymaga:** `approval.view_queue`."
    ),
    dependencies=[require_permission("approval.view_queue")],
)
async def list_sources(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    active_only: bool = Query(True),
):
    await _check_module_enabled(db, redis)
    where = "WHERE s.[is_active]=1" if active_only else ""
    rows = await db.execute(
        text(
            f"SELECT s.[id_source], s.[source_name], s.[description], s.[is_active], "
            f"  s.[created_at], "
            f"  COUNT(CASE WHEN i.[status] NOT IN (N'approved',N'cancelled') "
            f"             THEN 1 END) AS active_instances "
            f"FROM [{_SCHEMA}].[skw_document_sources] s "
            f"LEFT JOIN [{_SCHEMA}].[skw_document_approval_instances] i "
            f"       ON i.[id_source] = s.[id_source] "
            f"{where} "
            f"GROUP BY s.[id_source],s.[source_name],s.[description],"
            f"  s.[is_active],s.[created_at] "
            f"ORDER BY s.[source_name] ASC"
        )
    )
    return {
        "data": [
            {
                "id_source":        r[0],
                "source_name":      r[1],
                "description":      r[2],
                "is_active":        bool(r[3]),
                "created_at":       dt_utc(r[4]),
                "active_instances": r[5] or 0,
            }
            for r in rows.fetchall()
        ]
    }