"""
api/permissions.py
═══════════════════════════════════════════════════════════════════════════════
Router uprawnień — System Windykacja.

5 endpointów:
  GET  /permissions           — lista wszystkich uprawnień (pogrupowana po kategorii)
  GET  /permissions/{id}      — szczegóły uprawnienia
  POST /permissions/check     — sprawdź czy bieżący user ma uprawnienie (Redis cache)
  POST /permissions/check-many — batch check listy uprawnień → mapa {perm: bool}
  GET  /permissions/my        — lista wszystkich uprawnień bieżącego użytkownika

Uwagi:
  • /check i /check-many nie wymagają żadnego RBAC — każdy zalogowany może sprawdzać
    swoje własne uprawnienia (używane przez frontend do renderowania UI)
  • Wyniki z cache Redis — zerowa latencja dla sprawdzenia uprawnień
  • Kolejność ścieżek ważna: /check-many i /my PRZED /{id}
    (FastAPI dopasowuje w kolejności — „my" i „check-many" to literały, nie ID)

Serwis: services/permission_service.py

Autor: System Windykacja
Wersja: 1.0.0
Data: 2026-02-20
"""
from __future__ import annotations

import logging
from typing import Optional

import orjson
from fastapi import APIRouter, HTTPException, Query, Request, status

from app.core.dependencies import (
    DB,
    ClientIP,
    CurrentUser,
    RedisClient,
    RequestID,
    require_permission,
    _get_role_permissions,
)
from app.schemas.common import BaseResponse

logger = logging.getLogger(__name__)
router = APIRouter()


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 1: GET /permissions
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "",
    summary="Lista wszystkich uprawnień",
    description=(
        "Zwraca pełną listę uprawnień systemu pogrupowanych według kategorii. "
        "Każde uprawnienie zawiera: ID, nazwy (code), opis po polsku, kategorię. "
        "Wyniki z cache Redis (`permissions:list` TTL 600s — uprawnienia rzadko się zmieniają). "
        "**Wymaga uprawnienia:** `permissions.list`"
    ),
    response_description="Lista uprawnień pogrupowanych po kategorii",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("permissions.list")],
)
async def list_permissions(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request_id: RequestID,
    category: Optional[str] = Query(None, description="Filtr po kategorii (np. auth, users, debtors)"),
):
    from app.services import permission_service

    permissions = await permission_service.get_list(db=db, redis=redis, category=category)

    return BaseResponse.ok(
        data={
            "items": permissions,
            "total": sum(len(v) if isinstance(v, list) else 1 for v in
                        (permissions.values() if isinstance(permissions, dict) else [permissions])),
            "grouped_by_category": isinstance(permissions, dict),
        },
        code="permissions.list",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 2: GET /permissions/my
# (PRZED /{id} — literal musi być dopasowany przed parametrem)
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/my",
    summary="Moje uprawnienia",
    description=(
        "Zwraca kompletną listę uprawnień aktualnie zalogowanego użytkownika. "
        "Nie wymaga żadnego specjalnego uprawnienia — każdy może sprawdzić swoje. "
        "Wyniki z cache Redis (`role_perms:{role_id}` TTL 300s). "
        "Używane przez frontend do renderowania menu i przycisków."
    ),
    response_description="Lista uprawnień bieżącego użytkownika",
    status_code=status.HTTP_200_OK,
)
async def my_permissions(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request_id: RequestID,
):
    # Używa tej samej funkcji co dependencies.py — spójne z check w JWT
    permissions = await _get_role_permissions(current_user.RoleID, db, redis)

    return BaseResponse.ok(
        data={
            "user_id": current_user.ID_USER,
            "role_id": current_user.RoleID,
            "permissions": sorted(permissions),
            "total": len(permissions),
        },
        code="permissions.my",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 3: POST /permissions/check
# (PRZED /{id})
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/check",
    summary="Sprawdzenie pojedynczego uprawnienia",
    description=(
        "Sprawdza czy bieżący użytkownik posiada wskazane uprawnienie. "
        "Nie wymaga żadnego RBAC — każdy zalogowany może sprawdzać swoje uprawnienia. "
        "Wynik z cache Redis (L1: `perm:{user_id}:{perm}`, L2: `role_perms:{role_id}`). "
        "Przydatne dla frontendu przy dynamicznym renderowaniu UI."
    ),
    response_description="Wynik sprawdzenia uprawnienia",
    status_code=status.HTTP_200_OK,
)
async def check_permission(
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request_id: RequestID,
):
    try:
        body = await request.json()
        permission = (body.get("permission") or "").strip()
    except Exception:
        permission = ""

    if not permission:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Wymagane pole permission",
                "errors": [{"field": "permission", "message": "Pole wymagane — np. 'debtors.view_list'"}],
            },
        )

    from app.services import permission_service

    has_perm = await permission_service.check(
        redis=redis,
        db=db,
        user_id=current_user.ID_USER,
        role_id=current_user.RoleID,
        permission_name=permission,
    )

    return BaseResponse.ok(
        data={
            "permission": permission,
            "granted": has_perm,
            "user_id": current_user.ID_USER,
        },
        code="permissions.check",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 4: POST /permissions/check-many
# (PRZED /{id})
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/check-many",
    summary="Batch sprawdzenie listy uprawnień",
    description=(
        "Sprawdza listę uprawnień naraz i zwraca mapę `{permission: bool}`. "
        "Wydajniejsze niż wielokrotne wywoływanie /check. "
        "Implementacja: `redis.mget()` dla batch lookup — jedna operacja Redis. "
        "Maksymalnie 50 uprawnień w jednym żądaniu. "
        "Nie wymaga żadnego RBAC — każdy zalogowany może sprawdzać swoje uprawnienia."
    ),
    response_description="Mapa uprawnień {permission_name: bool}",
    status_code=status.HTTP_200_OK,
)
async def check_many_permissions(
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request_id: RequestID,
):
    try:
        body = await request.json()
        permissions = body.get("permissions") or []
    except Exception:
        permissions = []

    if not permissions or not isinstance(permissions, list):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Wymagane pole permissions jako tablica stringów",
                "errors": [{"field": "permissions", "message": "Pole wymagane, format: [\"perm1\", \"perm2\"]"}],
            },
        )

    if len(permissions) > 50:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.too_many_permissions",
                "message": "Maksymalnie 50 uprawnień w jednym żądaniu",
                "errors": [{"field": "permissions", "message": f"Podano {len(permissions)}, limit = 50"}],
            },
        )

    # Walidacja: każdy element to string
    invalid = [p for p in permissions if not isinstance(p, str) or not p.strip()]
    if invalid:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Każdy element musi być niepustym stringiem",
                "errors": [{"field": "permissions", "message": "Znaleziono nieprawidłowe wartości"}],
            },
        )

    # Deduplikacja (zachowaj kolejność)
    seen: set[str] = set()
    unique_perms = [p.strip() for p in permissions if p.strip() not in seen and not seen.add(p.strip())]

    from app.services import permission_service

    result_map = await permission_service.check_many(
        redis=redis,
        db=db,
        user_id=current_user.ID_USER,
        role_id=current_user.RoleID,
        permission_names=unique_perms,
    )

    return BaseResponse.ok(
        data={
            "permissions": result_map,
            "user_id": current_user.ID_USER,
            "checked": len(unique_perms),
            "granted_count": sum(1 for v in result_map.values() if v),
        },
        code="permissions.check_many",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 5: GET /permissions/{id}
# (NA KOŃCU — literal routes muszą być wyżej)
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/{permission_id}",
    summary="Szczegóły uprawnienia",
    description=(
        "Zwraca szczegóły wybranego uprawnienia: ID, kod, opis, kategorię "
        "oraz listę ról, które mają to uprawnienie przypisane. "
        "**Wymaga uprawnienia:** `permissions.view`"
    ),
    response_description="Szczegóły uprawnienia",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("permissions.view")],
    responses={
        404: {"description": "Uprawnienie nie istnieje"},
    },
)
async def get_permission(
    permission_id: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request_id: RequestID,
):
    from app.services import permission_service

    try:
        perm = await permission_service.get_by_id(db=db, permission_id=permission_id)
    except Exception as exc:
        _raise_from_perm_error(exc)

    return BaseResponse.ok(data=perm, code="permissions.detail")


# ─────────────────────────────────────────────────────────────────────────────
# POMOCNICZE
# ─────────────────────────────────────────────────────────────────────────────

def _raise_from_perm_error(exc: Exception) -> None:
    """Konwertuje wyjątki z permission_service na HTTPException."""
    exc_type = type(exc).__name__

    _MAP: dict[str, tuple[int, str, str]] = {
        "PermissionNotFoundError": (404, "permissions.not_found", "Uprawnienie nie istnieje"),
        "PermissionServiceError":  (400, "permissions.service_error", "Błąd operacji na uprawnieniu"),
    }

    if exc_type in _MAP:
        http_status, code, msg = _MAP[exc_type]
        raise HTTPException(
            status_code=http_status,
            detail={
                "code": code,
                "message": msg,
                "errors": [{"field": "_", "message": str(exc) or msg}],
            },
        )
    raise