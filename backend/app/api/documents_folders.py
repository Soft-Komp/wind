# backend/app/api/documents_folders.py
"""
Teczki dokumentow — F6 (sekcja 4.15) + widocznosc wielogrupowa (0074)
+ obejscie administracyjne (0075) + soft-delete/migawki/bulk (0076).

NOWY plik, NOWY router. Rejestrowany pod prefixem /documents w
backend/app/api/router.py.

13 endpointow:
  GET    /documents/folders                              — lista teczek widocznych
  POST   /documents/folders                                — utworz teczke
  GET    /documents/folders/{id_folder}                    — szczegoly teczki
  PUT    /documents/folders/{id_folder}                    — aktualizuj
  DELETE /documents/folders/{id_folder}                    — usun
  GET    /documents/folders/{id_folder}/items               — lista dokumentow (aktualna)
  POST   /documents/{id_instance}/folders/{id_folder}       — dodaj dokument do teczki
  DELETE /documents/{id_instance}/folders/{id_folder}       — usun dokument z teczki
  GET    /documents/folders/{id_folder}/groups               — lista grup wspoldzielonych
  POST   /documents/folders/{id_folder}/groups/{id_group}     — przypisz grupe
  DELETE /documents/folders/{id_folder}/groups/{id_group}     — usun przypisanie (soft-delete)
  POST   /documents/folders/{id_folder}/groups/bulk            — przypisz wiele grup naraz
  [PLACEHOLDER, HTTP 501] POST /documents/folders/{id_folder}/transfer-ownership
  [PLACEHOLDER, HTTP 501] GET  /documents/folders/orphaned
  [PLACEHOLDER, HTTP 501] GET  /documents/folders/{id_folder}/items/historical

Uprawnienia:
  documents.manage_folders       — CRUD teczek, bazowe (na wszystkich endpointach).
  documents.assign_folder        — dodawanie/usuwanie DOKUMENTU z teczki (JEDNO,
      wspolne uprawnienie, niezaleznie od folder_type — sprostowanie 06.08.2026,
      wczesniejsze wzmianki w dokumentacji roboczej o dwoch osobnych
      uprawnieniach assign_own_folder/assign_team_folder byly bledne).
  documents.assign_shared_folder — dodatkowo wymagane, gdy dostep do teczki
      jest WYLACZNIE przez grupe wspoldzielona (sprawdzane w serwisie).
  documents.manage_all_folders   — pelne obejscie czlonkostwa dla operacji
      zespolowych (0075) — sprawdzane WYLACZNIE przez skw_RolePermissions,
      NIGDY przez RoleName='Admin'.
  documents.view_all             — dostep do wszystkich teczek.

ZLECENIE FRONTU (06.08.2026, punkt 2) — ZARZADZANIE GRUPAMI WSPOLDZIELONYMI
(GET/POST/DELETE/bulk .../groups...) jest zarezerwowane dla wlasciciela
teczki lub administratora z documents.manage_all_folders. can_view_all
NIE jest tu sprawdzane — swiadome zawezenie, gosc i "zwykly" supervisor
bez manage_all_folders NIE widza i nie zarzadzaja ta lista.

ZNANA NIESPOJNOSC (zgloszona 06.08.2026, NIE naprawiana w tym pliku bez
osobnej decyzji): _can_view_all() w tym routerze sprawdza WYLACZNIE
documents.view_all — w przeciwienstwie do analogicznej funkcji w
documents.py, ktora sprawdza TAKZE approval.supervise.

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
    GroupNotFoundError,
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


class BulkGroupsBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id_groups: list[int] = Field(..., min_length=1)


class TransferOwnershipBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    new_owner_user: int = Field(..., gt=0)


def _raise_folder_error(exc: Exception) -> None:
    if isinstance(exc, FolderNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, FolderAccessDeniedError):
        raise HTTPException(status_code=403, detail=str(exc))
    if isinstance(exc, FolderValidationError):
        raise HTTPException(status_code=422, detail=str(exc))
    if isinstance(exc, InstanceNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, GroupNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc))
    raise


async def _can_view_all(current_user: CurrentUser, db: DB) -> bool:
    """
    Sprawdza czy user ma documents.view_all (override widocznosci).

    UWAGA — niespojnosc, patrz naglowek pliku: nie sprawdza approval.supervise,
    w przeciwienstwie do documents.py::_can_view_all(). Zgloszone, nie
    naprawiane tutaj bez osobnej decyzji.
    """
    try:
        from sqlalchemy import text as _text
        result = await db.execute(
            _text(
                "SELECT COUNT(*) FROM dbo.skw_Users u "
                "JOIN dbo.skw_RolePermissions rp ON rp.ID_ROLE = u.RoleID "
                "JOIN dbo.skw_Permissions p ON p.ID_PERMISSION = rp.ID_PERMISSION "
                "WHERE u.ID_USER = :uid AND p.PermissionName = 'documents.view_all' AND p.IsActive = 1"
            ),
            {"uid": current_user.id_user},
        )
        return (result.scalar() or 0) > 0
    except Exception as exc:
        logger.error(
            "_can_view_all: blad sprawdzania documents.view_all dla user_id=%s: %s",
            current_user.id_user, exc,
        )
        return False


async def _can_manage_all_folders(current_user: CurrentUser, db: DB) -> bool:
    """
    Sprawdza czy user ma documents.manage_all_folders — pelne obejscie
    sprawdzania czlonkostwa w grupie dla operacji na teczkach zespolowych.

    SWIADOMIE sprawdzane WYLACZNIE przez wpis w skw_RolePermissions —
    NIGDY przez skw_Roles.RoleName='Admin'. Administrator "z nazwy roli"
    i administrator "z uprawnien" to dwa niezalezne zbiory — macierz
    RolePermissions jest edytowalna niezaleznie od nazwy roli (patrz
    ODMIENNY wzorzec w worker/tasks/auto_dispatch_task.py::
    _get_admin_user_ids(), ktory filtruje po RoleName — to jest WYJATEK
    do wewnetrznego uzytku workera, nie konwencja do powielania).
    """
    try:
        from sqlalchemy import text as _text
        result = await db.execute(
            _text(
                "SELECT COUNT(*) FROM dbo.skw_Users u "
                "JOIN dbo.skw_RolePermissions rp ON rp.ID_ROLE = u.RoleID "
                "JOIN dbo.skw_Permissions p ON p.ID_PERMISSION = rp.ID_PERMISSION "
                "WHERE u.ID_USER = :uid AND p.PermissionName = 'documents.manage_all_folders' "
                "  AND p.IsActive = 1"
            ),
            {"uid": current_user.id_user},
        )
        return (result.scalar() or 0) > 0
    except Exception as exc:
        logger.error(
            "_can_manage_all_folders: blad sprawdzania documents.manage_all_folders "
            "dla user_id=%s: %s", current_user.id_user, exc,
        )
        return False


async def _can_manage_shared_items(current_user: CurrentUser, db: DB) -> bool:
    """Sprawdza czy user ma documents.assign_shared_folder."""
    try:
        from sqlalchemy import text as _text
        result = await db.execute(
            _text(
                "SELECT COUNT(*) FROM dbo.skw_Users u "
                "JOIN dbo.skw_RolePermissions rp ON rp.ID_ROLE = u.RoleID "
                "JOIN dbo.skw_Permissions p ON p.ID_PERMISSION = rp.ID_PERMISSION "
                "WHERE u.ID_USER = :uid AND p.PermissionName = 'documents.assign_shared_folder' "
                "  AND p.IsActive = 1"
            ),
            {"uid": current_user.id_user},
        )
        return (result.scalar() or 0) > 0
    except Exception as exc:
        logger.error(
            "_can_manage_shared_items: blad sprawdzania documents.assign_shared_folder "
            "dla user_id=%s: %s", current_user.id_user, exc,
        )
        return False


# =============================================================================
# CRUD TECZEK
# =============================================================================

@router.get(
    "/folders",
    summary="Lista teczek widocznych dla uzytkownika",
    description=(
        "Zwraca teczki prywatne nalezace do uzytkownika, teczki zespolowe "
        "grup do ktorych nalezy, oraz teczki przypisane jako wspoldzielone. "
        "documents.view_all LUB documents.manage_all_folders widzi wszystkie. "
        "**Wymaga:** `documents.manage_folders`."
    ),
    dependencies=[require_permission("documents.manage_folders")],
)
async def list_folders_endpoint(current_user: CurrentUser, db: DB):
    can_view_all = await _can_view_all(current_user, db)
    can_manage_all = await _can_manage_all_folders(current_user, db)
    folders = await svc.list_folders(
        db, actor_id=current_user.id_user, can_view_all=can_view_all,
        can_manage_all_folders=can_manage_all,
    )
    return BaseResponse.ok(data={"items": folders, "total": len(folders)}, app_code="folders.list")


@router.post(
    "/folders",
    status_code=201,
    summary="Utworz teczke",
    description=(
        "folder_type='private' wymaga owner_user, folder_type='team' wymaga "
        "owner_group (musisz byc czlonkiem tej grupy, ALBO miec "
        "documents.manage_all_folders). "
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
    can_manage_all = await _can_manage_all_folders(current_user, db)
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
            can_manage_all_folders=can_manage_all,
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
    can_manage_all = await _can_manage_all_folders(current_user, db)
    try:
        folder = await svc.get_folder(
            db, id_folder, actor_id=current_user.id_user, can_view_all=can_view_all,
            can_manage_all_folders=can_manage_all,
        )
    except (FolderNotFoundError, FolderAccessDeniedError) as exc:
        _raise_folder_error(exc)
    return BaseResponse.ok(data=folder, app_code="folders.get")


@router.put(
    "/folders/{id_folder}",
    summary="Aktualizuj teczke (partial update)",
    description=(
        "owner_user/owner_group/folder_type NIE sa edytowalne. "
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
    can_manage_all = await _can_manage_all_folders(current_user, db)
    try:
        folder = await svc.update_folder(
            db, id_folder,
            actor_id=current_user.id_user,
            can_view_all=can_view_all,
            can_manage_all_folders=can_manage_all,
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
        "CASCADE DELETE usuwa przypisania dokumentow ORAZ grup wspoldzielonych. "
        "Same dokumenty/instancje obiegu NIE sa usuwane. "
        "**Wymaga:** `documents.manage_folders`."
    ),
    responses={404: {"description": "Teczka nie istnieje"}, 403: {"description": "Brak dostepu"}},
    dependencies=[require_permission("documents.manage_folders")],
)
async def delete_folder_endpoint(id_folder: int, current_user: CurrentUser, db: DB):
    can_view_all = await _can_view_all(current_user, db)
    can_manage_all = await _can_manage_all_folders(current_user, db)
    try:
        await svc.delete_folder(
            db, id_folder, actor_id=current_user.id_user, can_view_all=can_view_all,
            can_manage_all_folders=can_manage_all,
        )
    except (FolderNotFoundError, FolderAccessDeniedError) as exc:
        _raise_folder_error(exc)
    return BaseResponse.ok(data={"id_folder": id_folder, "deleted": True}, app_code="folders.deleted")


@router.get(
    "/folders/{id_folder}/items",
    summary="Lista dokumentow w teczce (aktualna zawartosc)",
    description=(
        "Aktualna zawartosc — byli czlonkowie usunietej grupy wspoldzielonej "
        "NIE MAJA tu dostepu (patrz /items/historical, placeholder). "
        "**Wymaga:** `documents.manage_folders`."
    ),
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
    can_manage_all = await _can_manage_all_folders(current_user, db)
    try:
        result = await svc.list_documents_in_folder(
            db, id_folder,
            actor_id=current_user.id_user, can_view_all=can_view_all,
            can_manage_all_folders=can_manage_all,
            page=page, per_page=per_page,
        )
    except (FolderNotFoundError, FolderAccessDeniedError) as exc:
        _raise_folder_error(exc)
    return BaseResponse.ok(data=result, app_code="folders.items_list")


# =============================================================================
# ZARZADZANIE GRUPAMI WSPOLDZIELONYMI (0074/0076)
# ZLECENIE FRONTU (punkt 2): wylacznie wlasciciel/administrator, nigdy gosc.
# =============================================================================

@router.get(
    "/folders/{id_folder}/groups",
    summary="Lista AKTYWNYCH grup wspoldzielonych przypisanych do teczki",
    description=(
        "ZLECENIE FRONTU (06.08.2026, punkt 2): dostep WYLACZNIE dla "
        "wlasciciela teczki lub administratora z documents.manage_all_folders. "
        "**Wymaga:** `documents.manage_folders`."
    ),
    responses={404: {"description": "Teczka nie istnieje"}, 403: {"description": "Brak dostepu"}},
    dependencies=[require_permission("documents.manage_folders")],
)
async def list_folder_groups_endpoint(id_folder: int, current_user: CurrentUser, db: DB):
    can_manage_all = await _can_manage_all_folders(current_user, db)
    try:
        groups = await svc.list_folder_groups(
            db, id_folder, actor_id=current_user.id_user,
            can_manage_all_folders=can_manage_all,
        )
    except (FolderNotFoundError, FolderAccessDeniedError) as exc:
        _raise_folder_error(exc)
    return BaseResponse.ok(data={"items": groups, "total": len(groups)}, app_code="folders.groups_list")


@router.post(
    "/folders/{id_folder}/groups/bulk",
    status_code=201,
    summary="Przypisz wiele grup jako wspoldzielone jedna operacja",
    description=(
        "ZLECENIE FRONTU (06.08.2026, punkt 7): atomowe — jesli KTORAKOLWIEK "
        "grupa z listy nie istnieje, ZERO zmian w bazie, blad wskazuje ktore "
        "id_group sa problemem. Brak limitu liczby elementow (jawna decyzja "
        "frontu). Dostep: wlasciciel/administrator, nie gosc. "
        "**Wymaga:** `documents.manage_folders`."
    ),
    responses={
        404: {"description": "Teczka lub jedna z grup nie istnieje — ZERO zmian zapisanych"},
        403: {"description": "Brak dostepu — zarezerwowane dla wlasciciela/administratora"},
        422: {"description": "Pusta lista id_groups"},
    },
    dependencies=[require_permission("documents.manage_folders")],
)
async def add_folder_groups_bulk_endpoint(
    id_folder: int,
    body: BulkGroupsBody,
    current_user: CurrentUser,
    db: DB,
):
    can_manage_all = await _can_manage_all_folders(current_user, db)
    try:
        result = await svc.add_groups_to_folder_bulk(
            db, id_folder, body.id_groups,
            actor_id=current_user.id_user, can_manage_all_folders=can_manage_all,
        )
    except (FolderNotFoundError, FolderAccessDeniedError, GroupNotFoundError, FolderValidationError) as exc:
        _raise_folder_error(exc)
    return BaseResponse.ok(data=result, app_code="folders.groups_bulk_added")


@router.post(
    "/folders/{id_folder}/groups/{id_group}",
    status_code=201,
    summary="Przypisz grupe jako wspoldzielona dla teczki",
    description=(
        "Idempotentne. Daje grupie WYLACZNIE widocznosc — prawo modyfikacji "
        "zawartosci wymaga dodatkowo documents.assign_shared_folder. "
        "Dostep do TEGO endpointu: wlasciciel/administrator (punkt 2). "
        "**Wymaga:** `documents.manage_folders`."
    ),
    responses={
        404: {"description": "Teczka lub grupa nie istnieje"},
        403: {"description": "Brak dostepu — zarezerwowane dla wlasciciela/administratora"},
    },
    dependencies=[require_permission("documents.manage_folders")],
)
async def add_folder_group_endpoint(
    id_folder: int,
    id_group: int,
    current_user: CurrentUser,
    db: DB,
):
    can_manage_all = await _can_manage_all_folders(current_user, db)
    try:
        result = await svc.add_group_to_folder(
            db, id_folder, id_group,
            actor_id=current_user.id_user,
            can_manage_all_folders=can_manage_all,
        )
    except (FolderNotFoundError, FolderAccessDeniedError, GroupNotFoundError) as exc:
        _raise_folder_error(exc)
    return BaseResponse.ok(data=result, app_code="folders.group_added")
    try:
        result = await svc.add_groups_to_folder_bulk(
            db, id_folder, body.id_groups,
            actor_id=current_user.id_user, can_manage_all_folders=can_manage_all,
        )
    except (FolderNotFoundError, FolderAccessDeniedError, GroupNotFoundError, FolderValidationError) as exc:
        _raise_folder_error(exc)
    return BaseResponse.ok(data=result, app_code="folders.groups_bulk_added")


@router.delete(
    "/folders/{id_folder}/groups/{id_group}",
    summary="Usun przypisanie grupy wspoldzielonej z teczki (soft-delete)",
    description=(
        "ZLECENIE FRONTU (06.08.2026, 'Opcja A'): soft-delete — tworzy "
        "migawke aktualnej zawartosci teczki, dostepna (docelowo) dla "
        "bylych czlonkow przez /items/historical (placeholder). "
        "Idempotentne. Dostep: wlasciciel/administrator (punkt 2), nie gosc. "
        "**Wymaga:** `documents.manage_folders`."
    ),
    responses={404: {"description": "Teczka nie istnieje"}, 403: {"description": "Brak dostepu"}},
    dependencies=[require_permission("documents.manage_folders")],
)
async def remove_folder_group_endpoint(
    id_folder: int,
    id_group: int,
    current_user: CurrentUser,
    db: DB,
):
    can_manage_all = await _can_manage_all_folders(current_user, db)
    try:
        await svc.remove_group_from_folder(
            db, id_folder, id_group,
            actor_id=current_user.id_user,
            can_manage_all_folders=can_manage_all,
        )
    except (FolderNotFoundError, FolderAccessDeniedError) as exc:
        _raise_folder_error(exc)
    return BaseResponse.ok(
        data={"id_folder": id_folder, "id_group": id_group, "removed": True},
        app_code="folders.group_removed",
    )


# =============================================================================
# PRZYPISYWANIE DOKUMENTOW DO TECZEK
# =============================================================================

@router.post(
    "/{id_instance}/folders/{id_folder}",
    summary="Dodaj dokument do teczki",
    description=(
        "Idempotentne. Dostep do teczki weryfikowany w warstwie serwisu "
        "(wlasciciel, grupa wspoldzielona z documents.assign_shared_folder, "
        "lub documents.manage_all_folders). "
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
    can_manage_all = await _can_manage_all_folders(current_user, db)
    can_manage_shared_items = await _can_manage_shared_items(current_user, db)
    try:
        result = await svc.add_document_to_folder(
            db, id_instance, id_folder,
            actor_id=current_user.id_user, can_view_all=can_view_all,
            can_manage_shared_items=can_manage_shared_items,
            can_manage_all_folders=can_manage_all,
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
    can_manage_all = await _can_manage_all_folders(current_user, db)
    can_manage_shared_items = await _can_manage_shared_items(current_user, db)
    try:
        await svc.remove_document_from_folder(
            db, id_instance, id_folder,
            actor_id=current_user.id_user, can_view_all=can_view_all,
            can_manage_shared_items=can_manage_shared_items,
            can_manage_all_folders=can_manage_all,
        )
    except (FolderNotFoundError, FolderAccessDeniedError) as exc:
        _raise_folder_error(exc)
    return BaseResponse.ok(
        data={"id_instance": id_instance, "id_folder": id_folder, "removed": True},
        app_code="folders.document_removed",
    )


# =============================================================================
# PLACEHOLDER (0076, ZLECENIE FRONTU — punkty 1 i 4) — HTTP 501
# =============================================================================

@router.post(
    "/folders/{id_folder}/transfer-ownership",
    summary="[PLACEHOLDER] Przenies wlasnosc teczki prywatnej",
    description=(
        "NIEZAIMPLEMENTOWANE — patrz TODO w document_folder_service.py::"
        "transfer_folder_ownership(). ZLECENIE FRONTU (06.08.2026): jedyny "
        "mechanizm reagowania na dezaktywacje wlasciciela prywatnej teczki. "
        "**Wymaga:** `documents.manage_all_folders`."
    ),
    responses={501: {"description": "Nie zaimplementowane — placeholder"}},
    dependencies=[require_permission("documents.manage_all_folders")],
)
async def transfer_folder_ownership_endpoint(
    id_folder: int,
    body: TransferOwnershipBody,
    current_user: CurrentUser,
    db: DB,
):
    raise HTTPException(status_code=501, detail="transfer-ownership: placeholder, niezaimplementowane (0076)")


@router.get(
    "/folders/orphaned",
    summary="[PLACEHOLDER] Lista teczek prywatnych z nieaktywnym wlascicielem",
    description=(
        "NIEZAIMPLEMENTOWANE — patrz TODO w document_folder_service.py::"
        "list_orphaned_folders(). **Wymaga:** `documents.manage_all_folders`."
    ),
    responses={501: {"description": "Nie zaimplementowane — placeholder"}},
    dependencies=[require_permission("documents.manage_all_folders")],
)
async def list_orphaned_folders_endpoint(current_user: CurrentUser, db: DB):
    raise HTTPException(status_code=501, detail="orphaned: placeholder, niezaimplementowane (0076)")


@router.get(
    "/folders/{id_folder}/items/historical",
    summary="[PLACEHOLDER] Zamrozona zawartosc teczki (byli czlonkowie grupy)",
    description=(
        "NIEZAIMPLEMENTOWANE — patrz TODO w document_folder_service.py::"
        "list_documents_in_folder_historical(). Wymaga rozstrzygniecia "
        "nierozwiazanego problemu historii czlonkostwa w grupie PRZED "
        "implementacja (system nie rejestruje kiedy user byl czlonkiem "
        "grupy w przeszlosci). "
        "**Wymaga:** `documents.manage_folders` (NAPRAWA 06.08.2026 — "
        "endpoint pierwotnie NIE MIAL zadnej bramki uprawnien, kazdy "
        "zalogowany user mogl trafic do tego placeholdera; poprawione "
        "przed odpaleniem jakiejkolwiek realnej implementacji, zeby "
        "wzorzec bramki byl juz na miejscu)."
    ),
    responses={
        403: {"description": "Brak uprawnienia documents.manage_folders"},
        501: {"description": "Nie zaimplementowane — placeholder"},
    },
    dependencies=[require_permission("documents.manage_folders")],
)
async def list_folder_items_historical_endpoint(
    id_folder: int,
    current_user: CurrentUser,
    db: DB,
    id_group: int = Query(..., description="Grupa, dla ktorej sprawdzamy historyczny dostep"),
):
    raise HTTPException(status_code=501, detail="items/historical: placeholder, niezaimplementowane (0076)")