"""
api/roles.py
═══════════════════════════════════════════════════════════════════════════════
Router ról — System Windykacja.

Prefix: /api/v1/roles  (zarejestrowany w api/router.py)

7 endpointów:
  GET    /roles                — lista ról (paginacja)
  GET    /roles/{id}           — szczegóły roli
  POST   /roles                — utwórz rolę
  PUT    /roles/{id}           — zaktualizuj rolę
  DELETE /roles/{id}           — krok 1: inicjuj usunięcie → token JWT (202)
  DELETE /roles/{id}/confirm   — krok 2: potwierdź usunięcie tokenem
  GET    /roles/{id}/users     — użytkownicy przypisani do roli (paginacja)

UWAGA: Endpointy /roles/{id}/permissions* są w roles_permissions.py.

Serwis: services/role_service.py
  - get_list(), get_by_id()
  - create(), update()
  - initiate_delete(), confirm_delete()
  - get_users_with_role()

Autor: System Windykacja
Wersja: 2.0.0
Data: 2026-02-27
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Optional

import orjson
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import (
    DB,
    ClientIP,
    CurrentUser,
    Pagination,
    RedisClient,
    RequestID,
    require_permission,
)
from app.db.session import get_db
from app.db.models.role import Role
from app.schemas.base import BaseResponse, PaginatedData, PaginationMeta
from app.schemas.common import BaseResponse as CommonBaseResponse
from app.schemas.roles import (
    RoleCreate,
    RoleDetail,
    RoleDetailResponse,
    RoleListItem,
    RoleListQuery,
    RoleListResponse,
    RoleRead,
    RoleResponse,
    RoleUpdate,
)

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Mapa błędów serwisu → HTTP
# ---------------------------------------------------------------------------

def _raise_from_role_error(exc: Exception) -> None:
    """Mapuje wyjątki role_service na HTTP responses."""
    exc_type = type(exc).__name__
    _MAP = {
        "RoleNotFoundError":        (404, "roles.not_found",       "Rola nie istnieje lub jest nieaktywna"),
        "RoleAlreadyExistsError":   (409, "roles.already_exists",  "Rola o podanej nazwie już istnieje"),
        "RoleProtectedError":       (403, "roles.protected",       "Nie można usunąć chronionej roli systemowej"),
        "RoleHasUsersError":        (409, "roles.has_users",       "Rola ma przypisanych użytkowników — najpierw zmień im rolę"),
        "RoleDeleteTokenError":     (400, "roles.invalid_token",   "Token usunięcia jest nieprawidłowy, wygasły lub już użyty"),
        "RoleValidationError":      (422, "roles.validation_error","Błąd walidacji danych roli"),
        "RolePermissionAssignError":(422, "roles.perm_assign_error","Błąd przypisywania uprawnień"),
    }
    if exc_type in _MAP:
        http_status, code, msg = _MAP[exc_type]
        # RoleHasUsersError ma dodatkowe atrybuty
        detail_msg = str(exc) if str(exc) else msg
        raise HTTPException(
            status_code=http_status,
            detail={
                "code": code,
                "message": detail_msg,
                "errors": [{"field": "_", "message": detail_msg}],
            },
        )
    raise


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 1: GET /roles
# ─────────────────────────────────────────────────────────────────────────────

@router.get("", response_model=RoleListResponse)
async def list_roles(
    q: RoleListQuery = Depends(),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Role).order_by(Role.id_role).offset(q.offset).limit(q.limit)
    result = await db.execute(stmt)
    items = result.scalars().all()

    total_stmt = select(func.count()).select_from(Role)
    total = (await db.execute(total_stmt)).scalar_one()

    total_pages = (total + q.limit - 1) // q.limit if q.limit else 0
    data = PaginatedData[RoleListItem](
        items=[RoleListItem.model_validate(x) for x in items],
        pagination=PaginationMeta(
            page=q.page,
            limit=q.limit,
            total=total,
            pages=total_pages,
            has_next=q.page < total_pages,
            has_prev=q.page > 1,
        ),
    )
    return BaseResponse(code=200, data=data)


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 2: GET /roles/{id}
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/{item_id}", response_model=RoleDetailResponse)
async def get_roles(
    item_id: int,
    db: AsyncSession = Depends(get_db),
):
    obj = (await db.execute(select(Role).where(Role.id_role == item_id))).scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Role not found")
    return BaseResponse(code=200, data=RoleDetail.model_validate(obj))


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 3: POST /roles
# ─────────────────────────────────────────────────────────────────────────────

@router.post("", response_model=RoleResponse, status_code=status.HTTP_201_CREATED)
async def create_roles(
    payload: RoleCreate,
    db: AsyncSession = Depends(get_db),
):
    obj = Role(**payload.model_dump(exclude_none=True))
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    return BaseResponse(code=201, data=RoleRead.model_validate(obj))


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 4: PUT /roles/{id}
# ─────────────────────────────────────────────────────────────────────────────

@router.put("/{item_id}", response_model=RoleResponse)
async def update_roles(
    item_id: int,
    payload: RoleUpdate,
    db: AsyncSession = Depends(get_db),
):
    obj = (await db.execute(select(Role).where(Role.id_role == item_id))).scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Role not found")

    for k, v in payload.model_dump(exclude_unset=True, exclude_none=True).items():
        setattr(obj, k, v)

    await db.commit()
    await db.refresh(obj)
    return BaseResponse(code=200, data=RoleRead.model_validate(obj))


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 5: DELETE /roles/{id}   — KROK 1: inicjuj usunięcie
# ─────────────────────────────────────────────────────────────────────────────

@router.delete(
    "/{role_id}",
    summary="Krok 1/2 — Inicjuj usunięcie roli",
    description=(
        "Pierwszy krok dwuetapowego usuwania roli. "
        "Weryfikuje czy rola może być usunięta (nie jest chroniona, nie ma użytkowników). "
        "Zwraca JWT `delete_token` ważny przez skonfigurowany TTL (domyślnie 300s). "
        "\n\nToken użyj w `DELETE /roles/{id}/confirm` z nagłówkiem `X-Delete-Token`. "
        "**Wymaga uprawnienia:** `roles.delete`"
    ),
    response_description="Token potwierdzający usunięcie",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[require_permission("roles.delete")],
    responses={
        403: {"description": "Rola chroniona lub brak uprawnienia"},
        404: {"description": "Rola nie istnieje"},
        409: {"description": "Rola ma przypisanych użytkowników"},
    },
)
async def delete_role_initiate(
    role_id: int = Path(..., ge=1, description="ID roli do usunięcia"),
    current_user: CurrentUser = None,
    db: DB = None,
    redis: RedisClient = None,
    client_ip: ClientIP = None,
    request_id: RequestID = None,
):
    from app.services import role_service

    logger.warning(
        orjson.dumps({
            "event":       "api_role_delete_initiated",
            "role_id":     role_id,
            "initiated_by": current_user.id_user,
            "request_id":  request_id,
            "ip":          client_ip,
            "ts":          datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    try:
        result = await role_service.initiate_delete(
            db=db,
            redis=redis,
            role_id=role_id,
            initiated_by_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_role_error(exc)

    # result to dataclass DeleteConfirmData — konwertujemy ręcznie
    import dataclasses
    result_dict = dataclasses.asdict(result) if dataclasses.is_dataclass(result) else dict(result)

    return CommonBaseResponse.ok(
        data={
            "delete_token": result_dict.get("token"),
            "expires_in":   result_dict.get("expires_in"),
            "role_id":      role_id,
            "role_name":    result_dict.get("role_name"),
            "warning":      result_dict.get("warning"),
            "message": (
                f"Token usunięcia wygenerowany. "
                f"Użyj go w DELETE /roles/{role_id}/confirm "
                f"z tokenem."
            ),
        },
        app_code="roles.delete_initiated",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 6: DELETE /roles/{id}/confirm   — KROK 2: potwierdź usunięcie
# ─────────────────────────────────────────────────────────────────────────────

@router.delete(
    "/{role_id}/confirm",
    summary="Krok 2/2 — Potwierdź usunięcie roli",
    description=(
        "Drugi krok dwuetapowego usuwania roli. "
        "Wymaga `delete_token` z kroku 1 w body JSON: `{\"delete_token\": \"eyJ...\"}`. "
        "\n\nAkcje przy usunięciu:\n"
        "① Archiwizacja roli + uprawnień → `/app/archives/YYYY-MM-DD/` (JSON.gz)\n"
        "② Usunięcie wszystkich RolePermissions (fizyczny DELETE)\n"
        "③ Soft-delete roli (is_active=False)\n"
        "④ Inwalidacja cache Redis\n"
        "⑤ AuditLog `role_deleted`\n"
        "\n**Wymaga uprawnienia:** `roles.delete`"
    ),
    response_description="Potwierdzenie usunięcia roli",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("roles.delete")],
    responses={
        400: {"description": "Token nieprawidłowy, wygasły lub już użyty"},
        404: {"description": "Rola nie istnieje"},
        409: {"description": "Rola zyskała użytkowników od czasu kroku 1"},
    },
)
async def delete_role_confirm(
    request: Request,
    role_id: int = Path(..., ge=1, description="ID roli do usunięcia"),
    current_user: CurrentUser = None,
    db: DB = None,
    redis: RedisClient = None,
    client_ip: ClientIP = None,
    request_id: RequestID = None,
):
    from app.services import role_service

    # Pobierz token z body JSON: {"delete_token": "eyJ..."}
    try:
        raw_body = await request.body()
        body_data = orjson.loads(raw_body) if raw_body else {}
        delete_token = (body_data.get("delete_token") or "").strip()
    except Exception:
        delete_token = ""

    if not delete_token:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code": "roles.missing_token",
                "message": "Wymagane pole 'delete_token' w body JSON (token z kroku 1)",
                "errors": [{"field": "delete_token", "message": "Pole wymagane"}],
            },
        )

    logger.warning(
        orjson.dumps({
            "event":       "api_role_delete_confirmed",
            "role_id":     role_id,
            "confirmed_by": current_user.id_user,
            "request_id":  request_id,
            "ip":          client_ip,
            "ts":          datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    try:
        result = await role_service.confirm_delete(
            db=db,
            redis=redis,
            role_id=role_id,
            confirm_token=delete_token,
            initiated_by_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_role_error(exc)

    logger.warning(
        orjson.dumps({
            "event":       "api_role_deleted",
            "role_id":     role_id,
            "deleted_by":  current_user.id_user,
            "request_id":  request_id,
            "ip":          client_ip,
            "ts":          datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return CommonBaseResponse.ok(
        data=result,
        app_code="roles.deleted",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 7: GET /roles/{id}/users
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/{role_id}/users",
    summary="Użytkownicy przypisani do roli",
    description=(
        "Zwraca paginowaną listę aktywnych użytkowników przypisanych do danej roli. "
        "Sortowanie: `username` ASC. "
        "**Wymaga uprawnienia:** `roles.view_users`"
    ),
    response_description="Paginowana lista użytkowników z daną rolą",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("roles.view_users")],
    responses={
        404: {"description": "Rola nie istnieje"},
    },
)
async def get_role_users(
    role_id: int = Path(..., ge=1, description="ID roli"),
    current_user: CurrentUser = None,
    db: DB = None,
    request_id: RequestID = None,
    page: int = Query(1, ge=1, description="Numer strony"),
    page_size: int = Query(50, ge=1, le=200, description="Rozmiar strony (max 200)"),
):
    from app.services import role_service

    try:
        result = await role_service.get_users_with_role(
            db=db,
            role_id=role_id,
            page=page,
            page_size=page_size,
        )
    except Exception as exc:
        _raise_from_role_error(exc)

    return CommonBaseResponse.ok(
        data=result,
        app_code="roles.users_list",
    )