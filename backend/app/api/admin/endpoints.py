# backend/app/api/admin/endpoints.py
"""
Panel zarzadzania wlacznikami endpointow — rejestr i toggle.

Dolaczany do app/api/admin/__init__.py jako kolejny include_router.

4 endpointy:
  GET    /admin/endpoints              — lista wszystkich endpointow z filtrami
  GET    /admin/endpoints/{key}        — szczegoly jednego endpointu
  POST   /admin/endpoints/{key}/enable  — wlacz endpoint
  POST   /admin/endpoints/{key}/disable — wylacz endpoint (wymaga powodu)

endpoint_key jest URL-encoded w sciezce — "/" zastepowane przez "%2F"
np. GET:/documents/{id_instance}/status-summary
  → "GET:%2Fdocuments%2F%7Bid_instance%7D%2Fstatus-summary"

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Path, Query
from pydantic import BaseModel, ConfigDict, Field

from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.schemas.common import BaseResponse
from app.services import endpoint_registry_service as svc

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/endpoints")


class DisableBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reason: str = Field(..., min_length=5, max_length=500,
                        description="Powod wylaczenia — wyswietlany gdy ktos trafi na wylaczony endpoint.")


def _decode_key(raw: str) -> str:
    """URL-dekoduje endpoint_key z parametru sciezki."""
    from urllib.parse import unquote
    return unquote(raw)


def _raise_svc_error(exc: Exception) -> None:
    if isinstance(exc, ValueError):
        raise HTTPException(status_code=404, detail=str(exc))
    raise


# =============================================================================
# GET /admin/endpoints — lista
# =============================================================================

@router.get(
    "",
    summary="Lista endpointow z wlacznikami",
    description=(
        "Wszystkie endpointy API zarejestrowane w skw_EndpointRegistry. "
        "Sortowanie: wylaczone pierwsze, potem alfabetycznie po kluczu. "
        "**Wymaga:** `system.manage_endpoints`."
    ),
    dependencies=[require_permission("system.manage_endpoints")],
)
async def list_endpoints(
    current_user: CurrentUser,
    db: DB,
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=500),
    is_enabled: Optional[bool] = Query(None, description="True=tylko wlaczone, False=tylko wylaczone"),
    search: Optional[str] = Query(None, max_length=100, description="Szukaj w kluczu lub nazwie"),
):
    result = await svc.list_endpoints(
        db, page=page, per_page=per_page,
        is_enabled=is_enabled, search=search,
    )
    return BaseResponse.ok(data=result, app_code="endpoints.list")


# =============================================================================
# GET /admin/endpoints/{endpoint_key} — szczegoly
# =============================================================================

@router.get(
    "/{endpoint_key:path}",
    summary="Szczegoly endpointu",
    responses={404: {"description": "Endpoint nie istnieje w rejestrze"}},
    dependencies=[require_permission("system.manage_endpoints")],
)
async def get_endpoint(
    endpoint_key: str = Path(..., description="Klucz endpointu, URL-encoded"),
    current_user: CurrentUser = None,
    db: DB = None,
):
    key = _decode_key(endpoint_key)
    try:
        result = await svc._get_or_404(db, key)
        return BaseResponse.ok(data=svc._row_to_dict(result), app_code="endpoints.get")
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


# =============================================================================
# POST /admin/endpoints/{endpoint_key}/enable
# =============================================================================

@router.post(
    "/{endpoint_key:path}/enable",
    summary="Wlacz endpoint",
    description=(
        "Wlacza wczesniej wylaczony endpoint. Inwaliduje cache Redis natychmiast. "
        "**Wymaga:** `system.manage_endpoints`."
    ),
    responses={
        404: {"description": "Endpoint nie istnieje w rejestrze"},
        409: {"description": "Endpoint juz jest wlaczony"},
    },
    dependencies=[require_permission("system.manage_endpoints")],
)
async def enable_endpoint(
    endpoint_key: str = Path(...),
    current_user: CurrentUser = None,
    db: DB = None,
    redis: RedisClient = None,
):
    key = _decode_key(endpoint_key)
    try:
        result = await svc.enable_endpoint(
            db, redis, key,
            actor_id=current_user.id_user,
            actor_username=current_user.username,
        )
    except ValueError as exc:
        status_code = 409 if "juz jest wlaczony" in str(exc) else 404
        raise HTTPException(status_code=status_code, detail=str(exc))
    return BaseResponse.ok(data=result, app_code="endpoints.enabled")


# =============================================================================
# POST /admin/endpoints/{endpoint_key}/disable
# =============================================================================

@router.post(
    "/{endpoint_key:path}/disable",
    summary="Wylacz endpoint",
    description=(
        "Wylacza endpoint na tej instancji. Wymaga podania powodu — "
        "bedzie wyswietlany kazdemu kto trafi na wylaczony endpoint. "
        "Inwaliduje cache Redis natychmiast. "
        "**Wymaga:** `system.manage_endpoints`."
    ),
    responses={
        404: {"description": "Endpoint nie istnieje w rejestrze"},
        409: {"description": "Endpoint juz jest wylaczony"},
    },
    dependencies=[require_permission("system.manage_endpoints")],
)
async def disable_endpoint(
    body: DisableBody,
    endpoint_key: str = Path(...),
    current_user: CurrentUser = None,
    db: DB = None,
    redis: RedisClient = None,
):
    key = _decode_key(endpoint_key)
    try:
        result = await svc.disable_endpoint(
            db, redis, key,
            actor_id=current_user.id_user,
            actor_username=current_user.username,
            reason=body.reason,
        )
    except ValueError as exc:
        status_code = 409 if "juz jest wylaczony" in str(exc) else 404
        raise HTTPException(status_code=status_code, detail=str(exc))
    return BaseResponse.ok(data=result, app_code="endpoints.disabled")