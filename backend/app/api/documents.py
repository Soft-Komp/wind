# backend/app/api/documents.py
"""
Uniwersalny widok dokumentow — F6 (sekcje 4.14, 4.17, 7.12).

NOWY plik, NOWY router. Rejestrowany pod prefixem /documents w
backend/app/api/router.py — UWAGA: musi byc zarejestrowany z innym
plikiem app/api/documents_folders.py (rowniez prefix /documents) —
FastAPI laczy oba routery pod tym samym prefixem bez konfliktu,
o ile sciezki sa rozne (sprawdzone: /folders vs /unassigned vs /{id}).

8 endpointow:
  GET  /documents                                — lista (filtr widocznosci)  [documents.view]
  GET  /documents/unassigned                       — PRZED /{id}              [documents.view]
  GET  /documents/duplicate-pending                — PRZED /{id}              [documents.manage_duplicates]
  POST /documents/{id}/duplicate-pending/resolve                              [documents.manage_duplicates]
  GET  /documents/{id}/status-summary                                         [documents.view]
  GET  /documents/{id}/actions/available                                      [documents.view]
  GET  /documents/{id}/timeline                                               [documents.view]
  POST /documents/{id}/actions/{id_action}        — wykonanie akcji zrodlowej [sources.execute_action]

Wszystkie nazwy uprawnien zgodne z faktyczna lista zasiana przez migracje 0039
(kod _krok11_seed_permissions) — NIE z dokumentacja projektowa PDF, ktora w
kilku miejscach (4.20) wymienia inne nazwy niz to co trafilo do kodu. Kod 0039
jest tu autorytatywny.

KRYTYCZNE — kolejnosc routingu FastAPI: /unassigned i /duplicate-pending MUSZA
byc zarejestrowane PRZED /{id_instance}/... inaczej FastAPI dopasuje "unassigned"
jako wartosc {id_instance} i zwroci 422 (nie da sie skonwertowac na int).

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.schemas.common import BaseResponse
from app.services import documents_service as svc
from app.services.documents_service import (
    DocumentNotFoundError,
    DuplicateResolveError,
)

logger = logging.getLogger(__name__)
router = APIRouter()


class DuplicateResolveBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision: str = Field(..., pattern=r"^(confirm|dismiss)$")


def _raise_doc_error(exc: Exception) -> None:
    if isinstance(exc, DocumentNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, DuplicateResolveError):
        raise HTTPException(status_code=409, detail=str(exc))
    raise


async def _can_view_all(current_user: CurrentUser, db: DB) -> bool:
    """Sprawdza czy user ma documents.view_all lub approval.supervise (override widocznosci)."""
    try:
        from sqlalchemy import text as _text
        result = await db.execute(
            _text(
                "SELECT COUNT(*) FROM dbo.skw_UserRoles ur "
                "JOIN dbo.skw_RolePermissions rp ON rp.ID_ROLE = ur.ID_ROLE "
                "JOIN dbo.skw_Permissions p ON p.ID_PERMISSION = rp.ID_PERMISSION "
                "WHERE ur.ID_USER = :uid "
                "  AND p.PermissionName IN ('documents.view_all', 'approval.supervise') "
                "  AND p.IsActive = 1"
            ),
            {"uid": current_user.id_user},
        )
        return (result.scalar() or 0) > 0
    except Exception:
        return False


# =============================================================================
# GET /documents — lista
# =============================================================================

@router.get(
    "",
    summary="Lista dokumentow ze wszystkich zrodel",
    description=(
        "Uniwersalny widok wszystkich instancji obiegu niezaleznie od zrodla. "
        "Filtr widocznosci: dokumenty objete restricted filtrem (sekcja 4.14) "
        "sa widoczne tylko gdy uzytkownik (lub jedna z jego grup) ma wpis "
        "w skw_approval_filter_visibility. documents.view_all/approval.supervise "
        "widza wszystko. "
        "\n\nid_folder dopuszcza wiele wartosci jednoczesnie (wielowymiarowosc teczek) "
        "— dokument widoczny jesli jest w KTOREJKOLWIEK z podanych teczek. "
        "**Wymaga:** `documents.view`."
    ),
    dependencies=[require_permission("documents.view")],
)
async def list_documents_endpoint(
    current_user: CurrentUser,
    db: DB,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    id_source: Optional[int] = Query(None),
    id_folder: Optional[list[int]] = Query(None),
    id_category: Optional[int] = Query(None),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None, max_length=100),
):
    can_view_all = await _can_view_all(current_user, db)
    result = await svc.list_documents(
        db,
        actor_id=current_user.id_user, can_view_all=can_view_all,
        page=page, per_page=per_page,
        id_source=id_source, id_folder=id_folder, id_category=id_category,
        status=status, search=search,
    )
    return BaseResponse.ok(data=result, app_code="documents.list")


# =============================================================================
# GET /documents/unassigned — PRZED /{id}
# =============================================================================

@router.get(
    "/unassigned",
    summary="Lista dokumentow nieprzypisanych do sciezki obiegu",
    description=(
        "status='unassigned' — auto_dispatch_task nie znalazl odpowiedniej "
        "sciezki po przekroczeniu progu prob (AUTO_DISPATCH_MAX_ATTEMPTS). "
        "Uzywane jako badge w nawigacji. "
        "**Wymaga:** `documents.view`."
    ),
    dependencies=[require_permission("documents.view")],
)
async def list_unassigned_endpoint(
    current_user: CurrentUser,
    db: DB,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    can_view_all = await _can_view_all(current_user, db)
    result = await svc.list_unassigned(
        db, actor_id=current_user.id_user, can_view_all=can_view_all,
        page=page, per_page=per_page,
    )
    return BaseResponse.ok(data=result, app_code="documents.unassigned_list")


# =============================================================================
# GET /documents/duplicate-pending — PRZED /{id}
# =============================================================================

@router.get(
    "/duplicate-pending",
    summary="Lista potencjalnych duplikatow",
    description=(
        "status='duplicate_pending' — DuplicateDetectionService wykryl "
        "podobienstwo do istniejacego dokumentu. Wymaga rozstrzygniecia "
        "przez POST /documents/{id}/duplicate-pending/resolve. "
        "**Wymaga:** `documents.manage_duplicates`."
    ),
    dependencies=[require_permission("documents.manage_duplicates")],
)
async def list_duplicate_pending_endpoint(
    current_user: CurrentUser,
    db: DB,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    can_view_all = await _can_view_all(current_user, db)
    result = await svc.list_duplicate_pending(
        db, actor_id=current_user.id_user, can_view_all=can_view_all,
        page=page, per_page=per_page,
    )
    return BaseResponse.ok(data=result, app_code="documents.duplicate_pending_list")


# =============================================================================
# POST /documents/{id_instance}/duplicate-pending/resolve
# =============================================================================

@router.post(
    "/{id_instance}/duplicate-pending/resolve",
    summary="Rozstrzygnij potencjalny duplikat",
    description=(
        "decision='confirm' — to faktycznie duplikat, dokument -> status=cancelled. "
        "decision='dismiss' — to NIE duplikat, dokument wpuszczany normalnie "
        "(status=pending_dispatch, dalej przez auto_dispatch_task). "
        "**Wymaga:** `documents.manage_duplicates`."
    ),
    responses={
        404: {"description": "Dokument nie istnieje"},
        409: {"description": "Dokument nie jest w stanie duplicate_pending"},
    },
    dependencies=[require_permission("documents.manage_duplicates")],
)
async def resolve_duplicate_endpoint(
    id_instance: int,
    body: DuplicateResolveBody,
    current_user: CurrentUser,
    db: DB,
):
    can_view_all = await _can_view_all(current_user, db)
    try:
        result = await svc.resolve_duplicate(
            db, id_instance,
            decision=body.decision,
            actor_id=current_user.id_user, can_view_all=can_view_all,
        )
    except (DocumentNotFoundError, DuplicateResolveError) as exc:
        _raise_doc_error(exc)
    return BaseResponse.ok(data=result, app_code="documents.duplicate_resolved")


# =============================================================================
# GET /documents/{id_instance}/status-summary
# =============================================================================

@router.get(
    "/{id_instance}/status-summary",
    summary="Kompletny stan dokumentu",
    description=(
        "Eliminuje potrzebe 3-4 osobnych requestow: status, biezacy etap obiegu, "
        "nazwa grupy, deadline, liczba dostepnych akcji, pilnosc, teczki. "
        "Preferowany endpoint dla widoku szczegolow dokumentu. "
        "**Wymaga:** `documents.view`."
    ),
    responses={404: {"description": "Dokument nie istnieje"}, 403: {"description": "Brak dostepu (filtr restricted)"}},
    dependencies=[require_permission("documents.view")],
)
async def get_status_summary_endpoint(id_instance: int, current_user: CurrentUser, db: DB):
    can_view_all = await _can_view_all(current_user, db)
    try:
        result = await svc.get_status_summary(
            db, id_instance, actor_id=current_user.id_user, can_view_all=can_view_all,
        )
    except DocumentNotFoundError as exc:
        _raise_doc_error(exc)
    return BaseResponse.ok(data=result, app_code="documents.status_summary")


# =============================================================================
# GET /documents/{id_instance}/actions/available
# =============================================================================

@router.get(
    "/{id_instance}/actions/available",
    summary="Lista akcji zrodlowych dostepnych dla uzytkownika",
    description=(
        "Frontend renderuje przyciski na podstawie tej listy. Kazda akcja "
        "ma pole 'available' — false jesli uzytkownik nie ma required_permission "
        "(przycisk wyswietlany jako wylaczony/niedostepny, nie skryty — transparentnosc). "
        "**Wymaga:** `documents.view`."
    ),
    responses={404: {"description": "Dokument nie istnieje"}},
    dependencies=[require_permission("documents.view")],
)
async def get_available_actions_endpoint(id_instance: int, current_user: CurrentUser, db: DB):
    can_view_all = await _can_view_all(current_user, db)
    try:
        actions = await svc.get_available_actions(
            db, id_instance, actor_id=current_user.id_user, can_view_all=can_view_all,
        )
    except DocumentNotFoundError as exc:
        _raise_doc_error(exc)
    return BaseResponse.ok(data={"items": actions, "total": len(actions)}, app_code="documents.actions_available")


# =============================================================================
# GET /documents/{id_instance}/timeline
# =============================================================================

@router.get(
    "/{id_instance}/timeline",
    summary="Zunifikowana os czasu dokumentu",
    description=(
        "Zdarzenia obiegu (approval_log) + komentarze, posortowane chronologicznie, "
        "niezaleznie od tego czy dokument ma aktywna instancje. "
        "**Wymaga:** `documents.view`."
    ),
    responses={404: {"description": "Dokument nie istnieje"}},
    dependencies=[require_permission("documents.view")],
)
async def get_timeline_endpoint(id_instance: int, current_user: CurrentUser, db: DB):
    can_view_all = await _can_view_all(current_user, db)
    try:
        timeline = await svc.get_timeline(
            db, id_instance, actor_id=current_user.id_user, can_view_all=can_view_all,
        )
    except DocumentNotFoundError as exc:
        _raise_doc_error(exc)
    return BaseResponse.ok(data={"items": timeline, "total": len(timeline)}, app_code="documents.timeline")


# =============================================================================
# POST /documents/{id_instance}/actions/{id_action} — wykonanie akcji (4.17)
# =============================================================================

@router.post(
    "/{id_instance}/actions/{id_action}",
    summary="Wykonaj akcje zrodlowa na dokumencie",
    description=(
        "Sprawdza required_permission akcji, wywoluje ActionService.execute() "
        "(ten sam mechanizm co HookService — sql_procedure/api_call z placeholderami), "
        "zapisuje wynik do skw_source_action_log, zwraca ustandaryzowana odpowiedz "
        "{status, message, refresh_document}. "
        "\n\nJesli refresh_document=true, frontend powinien odswiezyc dane dokumentu "
        "przez GET /documents/{id}/status-summary. "
        "**Wymaga:** `sources.execute_action` (uprawnienie bazowe — per-akcja "
        "required_permission weryfikowany dodatkowo wewnatrz ActionService)."
    ),
    responses={
        403: {"description": "Brak required_permission akcji"},
        404: {"description": "Dokument lub akcja nie istnieje"},
        422: {"description": "Blad wykonania akcji"},
    },
    dependencies=[require_permission("sources.execute_action")],
)
async def execute_action_endpoint(
    id_instance: int,
    id_action: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    from app.services.action_service import ActionService

    result = await ActionService.execute(
        id_instance=id_instance,
        id_action=id_action,
        db=db,
        redis=redis,
        id_user=current_user.id_user,
    )

    return BaseResponse.ok(
        data={
            "status":           result.status,
            "message":          result.message,
            "refresh_document": result.refresh_document,
            "execution_ms":     result.execution_ms,
        },
        app_code="documents.action_executed",
    )