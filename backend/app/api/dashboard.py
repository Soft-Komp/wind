"""
dashboard.py
============
Endpointy dashboardu — System Windykacja.

Endpointy (3):
    GET /dashboard/debt-stats   — agregaty zadłużenia + top dłużnicy
    GET /dashboard/monit-stats  — statystyki monitów (globalne, kanały, trend)
    GET /dashboard/activity     — oś czasu ostatniej aktywności

Wymagane uprawnienie: debtors.view_list (istniejące — bez nowych seedów)
Cache Redis: TTL per endpoint (30s–120s)
"""
from __future__ import annotations

import logging
from typing import Any

import orjson
from fastapi import APIRouter, Query

from app.core.dependencies import (
    DB,
    RedisClient,
    ClientIP,
    RequestID,
    require_permission,
)
from app.schemas.common import BaseResponse
from app.services import dashboard_service as svc

logger = logging.getLogger("app.api.dashboard")

router = APIRouter()


# ─────────────────────────────────────────────────────────────────────────────
# GET /dashboard/debt-stats
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/debt-stats",
    summary="Agregaty zadłużenia dla dashboardu",
    description=(
        "Zwraca: globalną sumę zadłużenia, podział na kategorie wiekowe, "
        "kwotę zagrożoną (>60 dni) oraz listę top 20 dłużników. "
        "Cache Redis 120s. "
        "**Wymaga uprawnienia:** `dashboard.view_debt_stats`"
    ),
    response_model=dict,
    dependencies=[require_permission("dashboard.view_debt_stats")],
)
async def get_debt_stats(
    redis:      RedisClient,
    request_id: RequestID,
) -> dict:
    result = await svc.get_debt_stats(redis=redis)

    logger.info(
        orjson.dumps({
            "event":      "dashboard_debt_stats",
            "request_id": request_id,
        }).decode()
    )

    return BaseResponse(
        code=200,
        app_code="dashboard.debt_stats",
        errors=[],
        data=result,
    ).model_dump(mode="json")


# ─────────────────────────────────────────────────────────────────────────────
# GET /dashboard/monit-stats
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/monit-stats",
    summary="Statystyki monitów dla dashboardu",
    description=(
        "Zwraca trzy zestawy danych: globalny agregat, per kanał (30 dni), "
        "trend miesięczny (6 miesięcy). "
        "Cache Redis 60s. "
        "**Wymaga uprawnienia:** `dashboard.view_monit_stats`"
    ),
    response_model=dict,
    dependencies=[require_permission("dashboard.view_monit_stats")],
)
async def get_monit_stats(
    redis:      RedisClient,
    request_id: RequestID,
) -> dict:
    result = await svc.get_monit_stats(redis=redis)

    logger.info(
        orjson.dumps({
            "event":      "dashboard_monit_stats",
            "request_id": request_id,
        }).decode()
    )

    return BaseResponse(
        code=200,
        app_code="dashboard.monit_stats",
        errors=[],
        data=result,
    ).model_dump(mode="json")


# ─────────────────────────────────────────────────────────────────────────────
# GET /dashboard/activity
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/activity",
    summary="Oś czasu ostatniej aktywności dla dashboardu",
    description=(
        "Zwraca ostatnie N zdarzeń: monity i komentarze. "
        "Cache Redis 30s. "
        "**Wymaga uprawnienia:** `dashboard.view_activity`"
    ),
    response_model=dict,
    dependencies=[require_permission("dashboard.view_activity")],
)
async def get_activity(
    redis:      RedisClient,
    request_id: RequestID,
    limit: int = Query(20, ge=1, le=100,
        description="Liczba ostatnich zdarzeń (max 100, domyślnie 20)."),
) -> dict:
    result = await svc.get_activity(redis=redis, limit=limit)

    logger.info(
        orjson.dumps({
            "event":      "dashboard_activity",
            "limit":      limit,
            "request_id": request_id,
        }).decode()
    )

    return BaseResponse(
        code=200,
        app_code="dashboard.activity",
        errors=[],
        data=result,
    ).model_dump(mode="json")