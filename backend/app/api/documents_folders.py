# backend/app/api/documents_folders.py
"""
Teczki dokumentow — F6 (sekcja 4.15).

NOWY plik, NOWY router. Rejestrowany pod prefixem /documents w
backend/app/api/router.py.

7 endpointow:
  GET    /documents/folders                          — lista teczek widocznych dla usera
  POST   /documents/folders                            — utworz teczke
  GET    /documents/folders/{id_folder}                — szczegoly teczki
  PUT    /documents/folders/{id_folder}                — aktualizuj
  DELETE /documents/folders/{id_folder}                — usun
  GET    /documents/folders/{id_folder}/items           — lista dokumentow w teczce
  POST   /documents/{id_instance}/folders/{id_folder}   — dodaj dokument do teczki
  DELETE /documents/{id_instance}/folders/{id_folder}   — usun dokument z teczki

Uprawnienie bazowe: documents.manage_folders (CRUD teczek wlasnych/zespolowych).
documents.view_all daje dostep do WSZYSTKICH teczek niezaleznie od wlasciciela
(uzywane przez supervisorow/adminow do audytu).

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from app.core.dependencies import DB, CurrentUser, require_permission
from app.schemas.common import BaseResponse
from app.services import document_folder_service as svc
from app.services.document_folder_service import (
    FolderNotFoundError,
    FolderAccessDeniedError,
    FolderValidationError,
    InstanceNotFoundError,
)

logger = logging.getLogger(__name__)
router = APIRouter()


# =============================================================================
# Schematy Pydantic
# =============================================================================

class FolderCreateBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder_name:  str = Field(..., min_length=2, max_length=200)
    description:  Optional[str] = Field(default=None, max_length=500)
    color:        Optional[str] = Field(default=None, pattern=r"^#[0-9A-Fa-f]{6}$")
    folder_type:  str = Field(..., pattern=r"^(private|team)$")
    owner_user:   Optional[int] = Field(default=None, gt=0)
    owner_group:  Optional[int] = Field(default=None, gt=0)


class FolderUpdateBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder_name:  Optional[str] = Field(default=None, min_length=2, max_length=200)
    description:  Optional[str] = Field(default=None, max_length=500)
    color:        Optional[str] = Field(default=None, pattern=r"^#[0-9A-Fa-f]{6}$")
    is_active:    Optional[bool] = None


def _raise_folder_error(exc: Exception) -> None:
    if isinstance(exc, FolderNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, FolderAccessDeniedError):
        raise HTTPException(status_code=403, detail=str(exc))
    if isinstance(exc, FolderValidationError):
        raise HTTPException(status_code=422, detail=str(exc))
    if isinstance(exc, InstanceNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc))
    raise


async def _can_view_all(current_user: CurrentUser, db: DB) -> bool:
    """Sprawdza czy user ma documents.view_all (override widocznosci)."""
    from app.core.dependencies import _get_role_permissions
    from app.core.dependencies import get_redis
    # Uzywamy juz zaladowanych uprawnien przez request — fallback przez bezposrednie zapytanie
    try:
        from sqlalchemy import text as _text
        result = await db.execute(
            _text(
                "SELECT COUNT(*) FROM dbo.skw_UserRoles ur "
                "JOIN dbo.skw_RolePermissions rp ON rp.ID_ROLE = ur.ID_ROLE "
                "JOIN dbo.skw_Permissions p ON p.ID_PERMISSION = rp.ID_PERMISSION "
                "WHERE ur.ID_USER = :uid AND p.PermissionName = 'documents.view_all' AND p.IsActive = 1"
            ),
            {"uid": current_user.id_user},
        )
        return (result.scalar() or 0) > 0
    except Exception:
        return False


# =============================================================================
# CRUD TECZEK
# =============================================================================

@router.get(
    "/folders",
    summary="Lista teczek widocznych dla uzytkownika",
    description=(
        "Zwraca teczki prywatne nalezace do uzytkownika oraz teczki zespolowe "
        "grup do ktorych nalezy. Uzytkownik z documents.view_all widzi wszystkie. "
        "**Wymaga:** `documents.manage_folders`."
    ),
    dependencies=[require_permission("documents.manage_folders")],
)
async def list_folders_endpoint(current_user: CurrentUser, db: DB):
    can_view_all = await _can_view_all(current_user, db)
    folders = await svc.list_folders(db, actor_id=current_user.id_user, can_view_all=can_view_all)
    return BaseResponse.ok(data={"items": folders, "total": len(folders)}, app_code="folders.list")


@router.post(
    "/folders",
    status_code=201,
    summary="Utworz teczke",
    description=(
        "folder_type='private' wymaga owner_user (zazwyczaj wlasny ID), "
        "folder_type='team' wymaga owner_group (musisz byc czlonkiem tej grupy). "
        "**Wymaga:** `documents.manage_folders`."
    ),
    responses={422: {"description": "Walidacja nie powiodla sie"}},
    dependencies=[require_permission("documents.manage_folders")],
)
async def create_folder_endpoint(
    body: FolderCreateBody,
    current_user: CurrentUser,
    db: DB,
):
    try:
        folder = await svc.create_folder(
            db,
            folder_name=body.folder_name,
            description=body.description,
            color=body.color,
            folder_type=body.folder_type,
            owner_user=body.owner_user,
            owner_group=body.owner_group,
            actor_id=current_user.id_user,
        )
    except FolderValidationError as exc:
        _raise_folder_error(exc)
    return BaseResponse.ok(data=folder, app_code="folders.created")


@router.get(
    "/folders/{id_folder}",
    summary="Szczegoly teczki",
    responses={404: {"description": "Teczka nie istnieje"}, 403: {"description": "Brak dostepu"}},
    dependencies=[require_permission("documents.manage_folders")],
)
async def get_folder_endpoint(id_folder: int, current_user: CurrentUser, db: DB):
    can_view_all = await _can_view_all(current_user, db)
    try:
        folder = await svc.get_folder(
            db, id_folder, actor_id=current_user.id_user, can_view_all=can_view_all,
        )
    except (FolderNotFoundError, FolderAccessDeniedError) as exc:
        _raise_folder_error(exc)
    return BaseResponse.ok(data=folder, app_code="folders.get")


@router.put(
    "/folders/{id_folder}",
    summary="Aktualizuj teczke (partial update)",
    description=(
        "owner_user/owner_group/folder_type NIE sa edytowalne — usun i stworz "
        "nowa teczke jesli trzeba zmienic wlasciciela. "
        "**Wymaga:** `documents.manage_folders`."
    ),
    responses={404: {"description": "Teczka nie istnieje"}, 403: {"description": "Brak dostepu"}},
    dependencies=[require_permission("documents.manage_folders")],
)
async def update_folder_endpoint(
    id_folder: int,
    body: FolderUpdateBody,
    current_user: CurrentUser,
    db: DB,
):
    can_view_all = await _can_view_all(current_user, db)
    try:
        folder = await svc.update_folder(
            db, id_folder,
            actor_id=current_user.id_user,
            can_view_all=can_view_all,
            folder_name=body.folder_name,
            description=body.description,
            color=body.color,
            is_active=body.is_active,
        )
    except (FolderNotFoundError, FolderAccessDeniedError, FolderValidationError) as exc:
        _raise_folder_error(exc)
    return BaseResponse.ok(data=folder, app_code="folders.updated")


@router.delete(
    "/folders/{id_folder}",
    summary="Usun teczke",
    description=(
        "CASCADE DELETE usuwa wszystkie przypisania dokumentow do tej teczki "
        "(skw_document_folder_items). Same dokumenty/instancje obiegu NIE sa usuwane. "
        "**Wymaga:** `documents.manage_folders`."
    ),
    responses={404: {"description": "Teczka nie istnieje"}, 403: {"description": "Brak dostepu"}},
    dependencies=[require_permission("documents.manage_folders")],
)
async def delete_folder_endpoint(id_folder: int, current_user: CurrentUser, db: DB):
    can_view_all = await _can_view_all(current_user, db)
    try:
        await svc.delete_folder(
            db, id_folder, actor_id=current_user.id_user, can_view_all=can_view_all,
        )
    except (FolderNotFoundError, FolderAccessDeniedError) as exc:
        _raise_folder_error(exc)
    return BaseResponse.ok(data={"id_folder": id_folder, "deleted": True}, app_code="folders.deleted")


@router.get(
    "/folders/{id_folder}/items",
    summary="Lista dokumentow w teczce",
    responses={404: {"description": "Teczka nie istnieje"}, 403: {"description": "Brak dostepu"}},
    dependencies=[require_permission("documents.manage_folders")],
)
async def list_folder_items_endpoint(
    id_folder: int,
    current_user: CurrentUser,
    db: DB,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    can_view_all = await _can_view_all(current_user, db)
    try:
        result = await svc.list_documents_in_folder(
            db, id_folder,
            actor_id=current_user.id_user, can_view_all=can_view_all,
            page=page, per_page=per_page,
        )
    except (FolderNotFoundError, FolderAccessDeniedError) as exc:
        _raise_folder_error(exc)
    return BaseResponse.ok(data=result, app_code="folders.items_list")


# =============================================================================
# PRZYPISYWANIE DOKUMENTOW DO TECZEK
# =============================================================================

@router.post(
    "/{id_instance}/folders/{id_folder}",
    summary="Dodaj dokument do teczki",
    description=(
        "Idempotentne — jesli dokument juz jest w teczce, zwraca "
        "already_in_folder=true bez bledu. Dostep do teczki weryfikowany "
        "w warstwie serwisu (private->owner, team->czlonek grupy). "
        "**Wymaga:** `documents.assign_folder`."
    ),
    responses={
        404: {"description": "Teczka lub instancja nie istnieje"},
        403: {"description": "Brak dostepu do teczki"},
    },
    dependencies=[require_permission("documents.assign_folder")],
)
async def add_to_folder_endpoint(
    id_instance: int,
    id_folder: int,
    current_user: CurrentUser,
    db: DB,
):
    can_view_all = await _can_view_all(current_user, db)
    try:
        result = await svc.add_document_to_folder(
            db, id_instance, id_folder,
            actor_id=current_user.id_user, can_view_all=can_view_all,
        )
    except (FolderNotFoundError, FolderAccessDeniedError, InstanceNotFoundError) as exc:
        _raise_folder_error(exc)
    return BaseResponse.ok(data=result, app_code="folders.document_added")


@router.delete(
    "/{id_instance}/folders/{id_folder}",
    summary="Usun dokument z teczki",
    description=(
        "Idempotentne — brak bledu jesli dokument nie byl w teczce. "
        "**Wymaga:** `documents.assign_folder`."
    ),
    responses={404: {"description": "Teczka nie istnieje"}, 403: {"description": "Brak dostepu"}},
    dependencies=[require_permission("documents.assign_folder")],
)
async def remove_from_folder_endpoint(
    id_instance: int,
    id_folder: int,
    current_user: CurrentUser,
    db: DB,
):
    can_view_all = await _can_view_all(current_user, db)
    try:
        await svc.remove_document_from_folder(
            db, id_instance, id_folder,
            actor_id=current_user.id_user, can_view_all=can_view_all,
        )
    except (FolderNotFoundError, FolderAccessDeniedError) as exc:
        _raise_folder_error(exc)
    return BaseResponse.ok(
        data={"id_instance": id_instance, "id_folder": id_folder, "removed": True},
        app_code="folders.document_removed",
    )