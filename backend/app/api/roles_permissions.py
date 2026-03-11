"""
api/roles_permissions.py
═══════════════════════════════════════════════════════════════════════════════
Router macierzy uprawnień ról — System Windykacja.

Prefix: /api/v1/roles-permissions  (zarejestrowany w api/router.py)

7 endpointów:
  GET    /roles-permissions/matrix           — macierz wszystkich ról × uprawnień
  PUT    /roles-permissions/matrix           — aktualizacja całej macierzy (batch)
  POST   /roles-permissions/bulk-assign      — masowe przypisanie/odebranie uprawnień

  GET    /roles/{role_id}/permissions        — uprawnienia konkretnej roli
  POST   /roles/{role_id}/permissions        — przypisz uprawnienia do roli (ADD)
  PUT    /roles/{role_id}/permissions        — nadpisz uprawnienia roli (REPLACE ALL)
  DELETE /roles/{role_id}/permissions/{pid}  — usuń jedno uprawnienie z roli

UWAGA ARCHITEKTONICZNA:
  Endpointy /roles/{role_id}/permissions są obsługiwane przez TEN plik
  ale rejestrowane z prefixem /roles w api/router.py jako osobny include_router.
  Dlatego w router.py należy dodać:

    _register_router(
        api_router,
        module_path="app.api.roles_permissions",
        attr="roles_router",          ← osobny router z prefixem /roles
        prefix="/roles",
        tags=["Role — Uprawnienia"],
    )

Serwis: services/role_service.py
  - assign_permissions(db, redis, role_id, permission_ids, ...) → atomowy DELETE+INSERT
  - get_permissions_matrix(db, redis) → {role_name: [permission_name, ...]}

Wzorce:
  - Każda mutacja → AuditLog (przez role_service fire-and-forget)
  - Inwalidacja cache Redis po każdej mutacji (przez role_service)
  - SSE event permissions_updated po zmianie (przez role_service)
  - Walidacja permission_ids: tylko istniejące aktywne IDs
  - extra='forbid' na wszystkich schematach wejściowych

Autor: System Windykacja
Wersja: 1.0.0
Data: 2026-02-24
"""
from __future__ import annotations

import logging
import unicodedata
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import orjson
from fastapi import APIRouter, HTTPException, Path, Request, status
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.core.dependencies import (
    DB,
    ClientIP,
    CurrentUser,
    RedisClient,
    RequestID,
    require_permission,
)
from app.schemas.common import BaseResponse
from app.core.config import get_settings
from jose import jwt as jose_jwt

# ─────────────────────────────────────────────────────────────────────────────
# Logger
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Routery — DWA osobne obiekty:
#   1. router      → prefix /roles-permissions  (macierz + bulk)
#   2. roles_router → prefix /roles             (per-role CRUD uprawnień)
# ─────────────────────────────────────────────────────────────────────────────

router = APIRouter()          # /roles-permissions/*
roles_router = APIRouter()    # /roles/{role_id}/permissions/*


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMATY WEJŚCIOWE — Pydantic v2
# ─────────────────────────────────────────────────────────────────────────────

def _sanitize_str(v: Optional[str], max_len: int = 200) -> Optional[str]:
    """NFC normalizacja + strip + truncate."""
    if v is None:
        return None
    v = unicodedata.normalize("NFC", v.strip())
    return v[:max_len] if v else None


class PermissionIdsRequest(BaseModel):
    """
    Żądanie zawierające listę ID uprawnień.
    Używane przez POST i PUT /roles/{id}/permissions.
    """
    model_config = ConfigDict(extra="forbid")

    permission_ids: List[int] = Field(
        ...,
        description="Lista ID uprawnień. Pusta lista = usuń wszystkie uprawnienia.",
        min_length=0,
        max_length=500,
    )

    @field_validator("permission_ids", mode="before")
    @classmethod
    def validate_ids(cls, v: list) -> list:
        if not isinstance(v, list):
            raise ValueError("permission_ids musi być tablicą.")
        invalid = [x for x in v if not isinstance(x, int) or x <= 0]
        if invalid:
            raise ValueError(
                f"Wszystkie ID muszą być pozytywnymi liczbami całkowitymi. "
                f"Nieprawidłowe: {invalid[:10]}"
            )
        # Deduplikacja z zachowaniem kolejności
        seen: set[int] = set()
        deduped = [x for x in v if not (x in seen or seen.add(x))]  # type: ignore
        return deduped


class MatrixUpdateEntry(BaseModel):
    """Pojedynczy wpis aktualizacji macierzy."""
    model_config = ConfigDict(extra="forbid")

    role_id: int = Field(..., ge=1, description="ID roli.")
    permission_id: int = Field(..., ge=1, description="ID uprawnienia.")
    assigned: bool = Field(..., description="True = przypisz, False = odbierz.")


class MatrixUpdateRequest(BaseModel):
    """
    Żądanie aktualizacji całej macierzy uprawnień.
    PUT /roles-permissions/matrix
    """
    model_config = ConfigDict(extra="forbid")

    updates: List[MatrixUpdateEntry] = Field(
        ...,
        description="Lista operacji na macierzy.",
        min_length=1,
        max_length=1000,
    )


class BulkAssignRequest(BaseModel):
    """
    Masowe przypisanie/odebranie uprawnień jednej roli.
    POST /roles-permissions/bulk-assign
    """
    model_config = ConfigDict(extra="forbid")

    role_id: int = Field(..., ge=1, description="ID roli.")
    permission_ids: List[int] = Field(
        ...,
        description="Lista ID uprawnień.",
        min_length=1,
        max_length=500,
    )
    action: str = Field(
        ...,
        description="Operacja: 'add' (dodaj) lub 'remove' (odbierz).",
        pattern=r"^(add|remove)$",
    )

    @field_validator("permission_ids", mode="before")
    @classmethod
    def validate_ids(cls, v: list) -> list:
        if not isinstance(v, list):
            raise ValueError("permission_ids musi być tablicą.")
        invalid = [x for x in v if not isinstance(x, int) or x <= 0]
        if invalid:
            raise ValueError(f"Nieprawidłowe ID: {invalid[:10]}")
        seen: set[int] = set()
        return [x for x in v if not (x in seen or seen.add(x))]  # type: ignore


# ─────────────────────────────────────────────────────────────────────────────
# POMOCNICZE — konwersja błędów serwisu
# ─────────────────────────────────────────────────────────────────────────────

def _raise_from_role_error(exc: Exception) -> None:
    """Konwertuje wyjątki z role_service na HTTPException."""
    exc_type = type(exc).__name__

    _MAP: dict[str, tuple[int, str, str]] = {
        "RoleNotFoundError":         (404, "roles.not_found",           "Rola nie istnieje."),
        "RolePermissionAssignError": (422, "roles.invalid_permissions",  "Nieprawidłowe ID uprawnień."),
        "RoleProtectedError":        (403, "roles.protected",            "Nie można modyfikować uprawnień tej roli."),
        "RoleValidationError":       (422, "roles.validation_error",     "Błąd walidacji danych."),
    }

    if exc_type in _MAP:
        http_status, code, msg = _MAP[exc_type]
        raise HTTPException(
            status_code=http_status,
            detail={
                "code": code,
                "message": str(exc) or msg,
                "errors": [{"field": "_", "message": str(exc) or msg}],
            },
        )
    # Nieznany wyjątek — re-raise
    logger.exception(
        "Nieobsłużony wyjątek w roles_permissions",
        extra={"exc_type": exc_type, "exc": str(exc)},
    )
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail={"code": "internal_error", "message": "Wewnętrzny błąd serwera."},
    )


# ═════════════════════════════════════════════════════════════════════════════
# ROUTER 1: /roles-permissions/*
# ═════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 1: GET /roles-permissions/matrix
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/matrix",
    summary="Macierz uprawnień ról",
    description=(
        "Zwraca kompletną macierz: {role_name: [permission_name, ...]}. "
        "Używana przez frontend do renderowania UI kafelkowego. "
        "Wyniki z cache Redis (`roles:matrix` TTL 300s). "
        "**Wymaga uprawnienia:** `permissions.view_matrix`"
    ),
    response_description="Macierz ról i uprawnień",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("permissions.view_matrix")],
)
async def get_matrix(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request_id: RequestID,
):
    from app.services import role_service

    matrix = await role_service.get_permissions_matrix(db=db, redis=redis)

    total_assignments = sum(len(v) for v in matrix.values())

    logger.debug(
        orjson.dumps({
            "event": "api_matrix_fetched",
            "roles_count": len(matrix),
            "total_assignments": total_assignments,
            "requested_by": current_user.id_user,
            "request_id": request_id,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={
            "matrix": matrix,
            "roles_count": len(matrix),
            "total_assignments": total_assignments,
        },
        app_code="roles_permissions.matrix",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 2: PUT /roles-permissions/matrix
# ─────────────────────────────────────────────────────────────────────────────

@router.put(
    "/matrix",
    summary="Aktualizacja macierzy uprawnień",
    description=(
        "Aktualizuje macierz uprawnień według listy operacji. "
        "Każda operacja: {role_id, permission_id, assigned: true/false}. "
        "Wykonywane atomowo — rollback przy błędzie. "
        "Po operacji: inwalidacja cache + SSE event permissions_updated. "
        "**Wymaga uprawnienia:** `permissions.edit_matrix`"
    ),
    response_description="Liczba zaktualizowanych przypisań",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("permissions.edit_matrix")],
)
async def update_matrix(
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import role_service

    try:
        body = await request.json()
        payload = MatrixUpdateRequest(**body)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Nieprawidłowe dane żądania.",
                "errors": [{"field": "_", "message": str(exc)}],
            },
        )

    # Grupujemy operacje po role_id — dla każdej roli robimy assign_permissions
    # Strategu: najpierw pobieramy obecne uprawnienia, potem aplikujemy diff
    from sqlalchemy import select
    from app.db.models.role_permission import RolePermission

    # Zbierz unikalne role_id z żądania
    role_ids_in_request = {entry.role_id for entry in payload.updates}
    updated_count = 0

    for role_id in role_ids_in_request:
        # Aktualne uprawnienia tej roli
        result = await db.execute(
            select(RolePermission.id_permission).where(
                RolePermission.id_role == role_id
            )
        )
        current_perm_ids: set[int] = {row[0] for row in result.fetchall()}

        # Aplikuj operacje dla tej roli
        target_perm_ids = set(current_perm_ids)
        for entry in payload.updates:
            if entry.role_id != role_id:
                continue
            if entry.assigned:
                target_perm_ids.add(entry.permission_id)
            else:
                target_perm_ids.discard(entry.permission_id)

        # Jeśli bez zmian — pomiń
        if target_perm_ids == current_perm_ids:
            continue

        try:
            await role_service.assign_permissions(
                db=db,
                redis=redis,
                role_id=role_id,
                permission_ids=sorted(target_perm_ids),
                updated_by_id=current_user.id_user,
                ip_address=client_ip,
            )
            updated_count += 1
        except Exception as exc:
            _raise_from_role_error(exc)

    logger.info(
        orjson.dumps({
            "event": "api_matrix_updated",
            "operations": len(payload.updates),
            "roles_affected": updated_count,
            "updated_by": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={
            "updated": updated_count,
            "operations": len(payload.updates),
            "message": f"Zaktualizowano {updated_count} ról.",
        },
        app_code="roles_permissions.matrix_updated",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 3: POST /roles-permissions/bulk-assign
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/bulk-assign",
    summary="Masowe przypisanie/odebranie uprawnień",
    description=(
        "Masowo dodaje lub odbiera uprawnienia jednej roli. "
        "action='add'    → dodaje podane uprawnienia (zachowuje istniejące). "
        "action='remove' → odbiera podane uprawnienia (zachowuje pozostałe). "
        "Po operacji: inwalidacja cache + SSE event permissions_updated. "
        "**Wymaga uprawnienia:** `permissions.assign_to_role`"
    ),
    response_description="Liczba zmienionych przypisań",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("permissions.assign_to_role")],
)
async def bulk_assign(
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import role_service
    from sqlalchemy import select
    from app.db.models.role_permission import RolePermission

    try:
        body = await request.json()
        payload = BulkAssignRequest(**body)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Nieprawidłowe dane żądania.",
                "errors": [{"field": "_", "message": str(exc)}],
            },
        )

    # Pobierz aktualne uprawnienia roli
    result = await db.execute(
        select(RolePermission.id_permission).where(
            RolePermission.id_role == payload.role_id
        )
    )
    current_perm_ids: set[int] = {row[0] for row in result.fetchall()}

    # Aplikuj operację
    target_perm_ids = set(current_perm_ids)
    if payload.action == "add":
        target_perm_ids.update(payload.permission_ids)
    else:  # remove
        target_perm_ids.difference_update(payload.permission_ids)

    affected = abs(len(target_perm_ids) - len(current_perm_ids))

    # Jeśli bez zmian — zwróć 200 bez operacji DB
    if target_perm_ids == current_perm_ids:
        return BaseResponse.ok(
            data={
                "affected": 0,
                "role_id": payload.role_id,
                "action": payload.action,
                "message": "Brak zmian do zastosowania.",
            },
            app_code="roles_permissions.bulk_assign_no_changes",
        )

    try:
        await role_service.assign_permissions(
            db=db,
            redis=redis,
            role_id=payload.role_id,
            permission_ids=sorted(target_perm_ids),
            updated_by_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_role_error(exc)

    logger.info(
        orjson.dumps({
            "event": "api_bulk_assign",
            "role_id": payload.role_id,
            "action": payload.action,
            "permission_ids": payload.permission_ids,
            "affected": affected,
            "updated_by": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={
            "affected": affected,
            "role_id": payload.role_id,
            "action": payload.action,
            "message": (
                f"Dodano {affected} uprawnień."
                if payload.action == "add"
                else f"Odebrano {affected} uprawnień."
            ),
        },
        app_code="roles_permissions.bulk_assign",
    )


# ═════════════════════════════════════════════════════════════════════════════
# ROUTER 2: /roles/{role_id}/permissions/*
# ═════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 4: GET /roles/{role_id}/permissions
# ─────────────────────────────────────────────────────────────────────────────

@roles_router.get(
    "/{role_id}/permissions",
    summary="Uprawnienia przypisane do roli",
    description=(
        "Zwraca listę uprawnień przypisanych do konkretnej roli. "
        "Każde uprawnienie zawiera: ID, kod, opis, kategorię. "
        "**Wymaga uprawnienia:** `permissions.view_list`"
    ),
    response_description="Lista uprawnień roli",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("permissions.view_list")],
)
async def get_role_permissions(
    role_id: int = Path(..., ge=1, description="ID roli."),
    current_user: CurrentUser = None,
    db: DB = None,
    redis: RedisClient = None,
    request_id: RequestID = None,
):
    from app.services import role_service

    try:
        role = await role_service.get_by_id(db=db, redis=redis, role_id=role_id)
    except Exception as exc:
        _raise_from_role_error(exc)

    permissions = role.get("permissions", [])

    return BaseResponse.ok(
        data={
            "role_id": role_id,
            "role_name": role.get("role_name"),
            "permissions": permissions,
            "total": len(permissions),
        },
        app_code="roles_permissions.role_permissions",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 5: POST /roles/{role_id}/permissions  (ADD — zachowuje istniejące)
# ─────────────────────────────────────────────────────────────────────────────

@roles_router.post(
    "/{role_id}/permissions",
    summary="Przypisanie uprawnień do roli",
    description=(
        "Dodaje uprawnienia do roli — zachowuje istniejące. "
        "Duplikaty ignorowane. "
        "Po operacji: inwalidacja cache Redis + SSE event permissions_updated. "
        "**Wymaga uprawnienia:** `permissions.assign_to_role`"
    ),
    response_description="Wynik przypisania uprawnień",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("permissions.assign_to_role")],
)
async def add_role_permissions(
    request: Request,
    role_id: int = Path(..., ge=1, description="ID roli."),
    current_user: CurrentUser = None,
    db: DB = None,
    redis: RedisClient = None,
    client_ip: ClientIP = None,
    request_id: RequestID = None,
):
    from app.services import role_service
    from sqlalchemy import select
    from app.db.models.role_permission import RolePermission

    try:
        body = await request.json()
        payload = PermissionIdsRequest(**body)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Nieprawidłowe dane żądania.",
                "errors": [{"field": "_", "message": str(exc)}],
            },
        )

    # Pobierz aktualne uprawnienia i dołącz nowe
    result = await db.execute(
        select(RolePermission.id_permission).where(
            RolePermission.id_role == role_id
        )
    )
    current_ids: set[int] = {row[0] for row in result.fetchall()}
    merged_ids = sorted(current_ids | set(payload.permission_ids))
    added_count = len(set(payload.permission_ids) - current_ids)

    try:
        await role_service.assign_permissions(
            db=db,
            redis=redis,
            role_id=role_id,
            permission_ids=merged_ids,
            updated_by_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_role_error(exc)

    logger.info(
        orjson.dumps({
            "event": "api_role_permissions_added",
            "role_id": role_id,
            "added": added_count,
            "permission_ids": payload.permission_ids,
            "updated_by": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={
            "role_id": role_id,
            "added": added_count,
            "total": len(merged_ids),
            "message": f"Dodano {added_count} uprawnień do roli.",
        },
        app_code="roles_permissions.added",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 6: PUT /roles/{role_id}/permissions  (REPLACE ALL)
# ─────────────────────────────────────────────────────────────────────────────

@roles_router.put(
    "/{role_id}/permissions",
    summary="Nadpisanie uprawnień roli",
    description=(
        "Zastępuje WSZYSTKIE uprawnienia roli podaną listą. "
        "⚠️ Operacja destrukcyjna — usuwa uprawnienia których nie ma w liście. "
        "Pusta lista = rola bez żadnych uprawnień. "
        "Po operacji: inwalidacja cache Redis + SSE event permissions_updated. "
        "**Wymaga uprawnienia:** `permissions.assign_to_role`"
    ),
    response_description="Wynik nadpisania uprawnień",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("permissions.assign_to_role")],
)
async def replace_role_permissions(
    request: Request,
    role_id: int = Path(..., ge=1, description="ID roli."),
    current_user: CurrentUser = None,
    db: DB = None,
    redis: RedisClient = None,
    client_ip: ClientIP = None,
    request_id: RequestID = None,
):
    from app.services import role_service

    try:
        body = await request.json()
        payload = PermissionIdsRequest(**body)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Nieprawidłowe dane żądania.",
                "errors": [{"field": "_", "message": str(exc)}],
            },
        )

    try:
        result = await role_service.assign_permissions(
            db=db,
            redis=redis,
            role_id=role_id,
            permission_ids=payload.permission_ids,
            updated_by_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_role_error(exc)

    logger.info(
        orjson.dumps({
            "event": "api_role_permissions_replaced",
            "role_id": role_id,
            "total": len(payload.permission_ids),
            "updated_by": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={
            "role_id": role_id,
            "total": len(payload.permission_ids),
            "message": f"Nadpisano uprawnienia roli — łącznie {len(payload.permission_ids)} uprawnień.",
        },
        app_code="roles_permissions.replaced",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 7: DELETE /roles/{role_id}/permissions/{permission_id}
# ─────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 7: DELETE /roles/{role_id}/permissions/{permission_id}/initiate
#             Krok 1/2 — inicjuj odebranie uprawnienia roli
# ─────────────────────────────────────────────────────────────────────────────

@roles_router.delete(
    "/{role_id}/permissions/{permission_id}/initiate",
    summary="Krok 1/2 — Inicjuj odebranie uprawnienia roli",
    description=(
        "Pierwszy krok dwuetapowego odbierania uprawnienia od roli. "
        "Sprawdza czy uprawnienie jest przypisane do roli. "
        "Zwraca jednorazowy token JWT ważny 60 sekund. "
        "Token wymagany w kroku 2: DELETE /{role_id}/permissions/{permission_id}/confirm. "
        "**Wymaga uprawnienia:** `permissions.revoke_from_role`"
    ),
    response_description="Token potwierdzający odebranie uprawnienia",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[require_permission("permissions.revoke_from_role")],
)
async def initiate_remove_role_permission(
    role_id: int = Path(..., ge=1, description="ID roli."),
    permission_id: int = Path(..., ge=1, description="ID uprawnienia do odebrania."),
    current_user: CurrentUser = None,
    db: DB = None,
    redis: RedisClient = None,
    client_ip: ClientIP = None,
    request_id: RequestID = None,
):
    from app.services import role_service
    from sqlalchemy import select
    from app.db.models.role_permission import RolePermission

    # Sprawdź czy uprawnienie jest przypisane do roli
    result = await db.execute(
        select(RolePermission.id_permission).where(
            RolePermission.id_role == role_id
        )
    )
    current_ids: set[int] = {row[0] for row in result.fetchall()}

    if permission_id not in current_ids:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "code": "roles.permission_not_assigned",
                "message": f"Uprawnienie ID={permission_id} nie jest przypisane do roli ID={role_id}.",
                "errors": [{"field": "permission_id", "message": "Uprawnienie nie jest przypisane do tej roli."}],
            },
        )

    # Wygeneruj jednorazowy token JWT
    settings = get_settings()
    ttl = 60
    jti = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    token_payload = {
        "scope":         "revoke_permission",
        "role_id":       role_id,
        "permission_id": permission_id,
        "initiated_by":  current_user.id_user,
        "jti":           jti,
        "iat":           int(now.timestamp()),
        "exp":           int((now + timedelta(seconds=ttl)).timestamp()),
    }
    token = jose_jwt.encode(
        token_payload,
        settings.secret_key.get_secret_value(),
        algorithm="HS256",
    )

    # Zapisz JTI w Redis — jednorazowość gwarantowana przez TTL
    await redis.setex(f"revoke_perm_jti:{jti}", ttl, "1")

    logger.warning(
        orjson.dumps({
            "event":         "api_role_permission_revoke_initiated",
            "role_id":       role_id,
            "permission_id": permission_id,
            "initiated_by":  current_user.id_user,
            "jti":           jti,
            "ttl_seconds":   ttl,
            "request_id":    request_id,
            "ip":            client_ip,
            "ts":            now.isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={
            "delete_token":  token,
            "expires_in":    ttl,
            "role_id":       role_id,
            "permission_id": permission_id,
            "message":       (
                f"Token ważny {ttl}s. "
                f"Użyj w DELETE /roles/{role_id}/permissions/{permission_id}/confirm."
            ),
        },
        app_code="roles_permissions.revoke_initiated",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 8: DELETE /roles/{role_id}/permissions/{permission_id}/confirm
#             Krok 2/2 — potwierdź odebranie uprawnienia roli
# ─────────────────────────────────────────────────────────────────────────────

@roles_router.delete(
    "/{role_id}/permissions/{permission_id}/confirm",
    summary="Krok 2/2 — Potwierdź odebranie uprawnienia roli",
    description=(
        "Drugi krok dwuetapowego odbierania uprawnienia od roli. "
        "Wymaga `delete_token` z kroku 1 w body JSON: `{\"delete_token\": \"eyJ...\"}`. "
        "Token jednorazowy — po użyciu wygasa natychmiast (JTI blacklista Redis). "
        "Po operacji: inwalidacja cache Redis + AuditLog. "
        "**Wymaga uprawnienia:** `permissions.revoke_from_role`"
    ),
    response_description="Potwierdzenie odebrania uprawnienia",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("permissions.revoke_from_role")],
    responses={
        400: {"description": "Token nieprawidłowy, wygasły lub już użyty"},
        404: {"description": "Uprawnienie nie jest przypisane do roli"},
    },
)
async def confirm_remove_role_permission(
    request: Request,
    role_id: int = Path(..., ge=1, description="ID roli."),
    permission_id: int = Path(..., ge=1, description="ID uprawnienia."),
    current_user: CurrentUser = None,
    db: DB = None,
    redis: RedisClient = None,
    client_ip: ClientIP = None,
    request_id: RequestID = None,
):
    from app.services import role_service
    from sqlalchemy import select
    from app.db.models.role_permission import RolePermission

    # Pobierz token z body JSON
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
                "code":    "roles_permissions.missing_token",
                "message": "Wymagane pole 'delete_token' w body JSON.",
                "errors":  [{"field": "delete_token", "message": "Pole wymagane."}],
            },
        )

    # Zweryfikuj podpis i expiry tokenu
    settings = get_settings()
    try:
        token_payload = jose_jwt.decode(
            delete_token,
            settings.secret_key.get_secret_value(),
            algorithms=["HS256"],
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code":    "roles_permissions.invalid_token",
                "message": "Token nieprawidłowy lub wygasł.",
                "errors":  [{"field": "delete_token", "message": "Token nieprawidłowy lub wygasł."}],
            },
        )

    # Sprawdź scope i zgodność z URL
    if (
        token_payload.get("scope")         != "revoke_permission"
        or token_payload.get("role_id")       != role_id
        or token_payload.get("permission_id") != permission_id
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code":    "roles_permissions.token_mismatch",
                "message": "Token nie dotyczy tej operacji.",
                "errors":  [{"field": "delete_token", "message": "Niezgodność role_id lub permission_id w tokenie."}],
            },
        )

    # Sprawdź jednorazowość — JTI musi istnieć w Redis
    jti = token_payload.get("jti", "")
    jti_key = f"revoke_perm_jti:{jti}"
    if not await redis.exists(jti_key):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code":    "roles_permissions.token_used",
                "message": "Token już został użyty lub wygasł.",
                "errors":  [{"field": "delete_token", "message": "Token jednorazowy — już wykorzystany."}],
            },
        )

    # Unieważnij JTI natychmiast (przed operacją — fail-safe)
    await redis.delete(jti_key)

    # Wykonaj odebranie uprawnienia
    result = await db.execute(
        select(RolePermission.id_permission).where(
            RolePermission.id_role == role_id
        )
    )
    current_ids: set[int] = {row[0] for row in result.fetchall()}
    new_ids = sorted(current_ids - {permission_id})

    try:
        await role_service.assign_permissions(
            db=db,
            redis=redis,
            role_id=role_id,
            permission_ids=new_ids,
            updated_by_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_role_error(exc)

    logger.warning(
        orjson.dumps({
            "event":         "api_role_permission_revoked",
            "role_id":       role_id,
            "permission_id": permission_id,
            "confirmed_by":  current_user.id_user,
            "jti":           jti,
            "request_id":    request_id,
            "ip":            client_ip,
            "ts":            datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={
            "role_id":       role_id,
            "permission_id": permission_id,
            "message":       f"Uprawnienie ID={permission_id} odebrane roli ID={role_id}.",
        },
        app_code="roles_permissions.revoked",
    )