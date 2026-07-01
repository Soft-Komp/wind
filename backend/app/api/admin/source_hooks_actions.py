# backend/app/api/admin/source_hooks_actions.py
"""
CRUD hookow i akcji zrodlowych — F6.

Plik DOLĄCZONY do tego samego routera admin co sources.py — rejestrowany
w app/api/admin/__init__.py jako kolejny include_router.

8 endpointow (zgnieżdżone pod /admin/sources/{id_source}):
  GET    /admin/sources/{id_source}/hooks                — lista       [sources.view_log]
  POST   /admin/sources/{id_source}/hooks                 — utworz      [sources.manage_hooks]
  PUT    /admin/sources/{id_source}/hooks/{id_hook}       — aktualizuj  [sources.manage_hooks]
  DELETE /admin/sources/{id_source}/hooks/{id_hook}       — usun        [sources.manage_hooks]
  GET    /admin/sources/{id_source}/actions               — lista       [sources.view_log]
  POST   /admin/sources/{id_source}/actions                — utworz      [sources.manage_actions]
  PUT    /admin/sources/{id_source}/actions/{id_action}   — aktualizuj  [sources.manage_actions]
  DELETE /admin/sources/{id_source}/actions/{id_action}   — usun        [sources.manage_actions]

Wszystkie nazwy uprawnien dokladnie zgodne z migracja 0039 (sources.manage_hooks,
sources.manage_actions, sources.view_log juz istnieja — zero nowych uprawnien
wymaganych dla tego pliku).

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging

from fastapi import APIRouter, HTTPException

from app.core.dependencies import DB, CurrentUser, require_permission
from app.schemas.common import BaseResponse
from app.schemas.sources import HookCreate, HookUpdate, ActionCreate, ActionUpdate
from app.services import source_hook_action_service as svc
from app.services.source_hook_action_service import (
    HookNotFoundError,
    HookConflictError,
    ActionNotFoundError,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sources/{id_source}")


def _raise_hook_error(exc: Exception) -> None:
    if isinstance(exc, HookNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, HookConflictError):
        raise HTTPException(status_code=409, detail=str(exc))
    raise


def _raise_action_error(exc: Exception) -> None:
    if isinstance(exc, ActionNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc))
    raise


# =============================================================================
# HOOKS
# =============================================================================

@router.get(
    "/hooks",
    summary="Lista hookow zrodla",
    description=(
        "Hooki wykonywane automatycznie po akcjach obiegowych (accepted/rejected). "
        "Max 1 aktywny hook per trigger_action — wymuszone na poziomie DB. "
        "**Wymaga:** `sources.view_log`."
    ),
    responses={404: {"description": "Zrodlo nie istnieje"}},
    dependencies=[require_permission("sources.view_log")],
)
async def list_hooks_admin(id_source: int, current_user: CurrentUser, db: DB):
    try:
        hooks = await svc.list_hooks(db, id_source)
    except HookNotFoundError as exc:
        _raise_hook_error(exc)
    return BaseResponse.ok(data={"items": hooks, "total": len(hooks)}, app_code="source_hooks.list")


@router.post(
    "/hooks",
    status_code=201,
    summary="Utworz hook",
    description=(
        "Tworzy nowy hook dla zrodla. Jesli is_active=true i juz istnieje "
        "aktywny hook dla tego trigger_action — 409 (dezaktywuj istniejacy "
        "lub edytuj go zamiast tworzyc nowy). "
        "**Wymaga:** `sources.manage_hooks`."
    ),
    responses={
        404: {"description": "Zrodlo nie istnieje"},
        409: {"description": "Aktywny hook dla trigger_action juz istnieje"},
        422: {"description": "Walidacja operation_config nie powiodla sie"},
    },
    dependencies=[require_permission("sources.manage_hooks")],
)
async def create_hook_admin(
    id_source: int,
    body: HookCreate,
    current_user: CurrentUser,
    db: DB,
):
    try:
        hook = await svc.create_hook(
            db, id_source,
            trigger_action=body.trigger_action,
            operation_type=body.operation_type,
            operation_config=body.operation_config,
            severity=body.severity,
            is_active=body.is_active,
            actor_id=current_user.id_user,
        )
    except (HookNotFoundError, HookConflictError) as exc:
        _raise_hook_error(exc)
    return BaseResponse.ok(data=hook, app_code="source_hooks.created")


@router.put(
    "/hooks/{id_hook}",
    summary="Aktualizuj hook (partial update)",
    description=(
        "trigger_action NIE jest edytowalny — usun hook i stworz nowy jesli "
        "trzeba zmienic akcje wyzwalajaca. "
        "Aktywacja (is_active: false->true) sprawdza konflikt z innymi hookami. "
        "**Wymaga:** `sources.manage_hooks`."
    ),
    responses={
        404: {"description": "Hook nie istnieje"},
        409: {"description": "Konflikt aktywacji — inny aktywny hook juz istnieje"},
    },
    dependencies=[require_permission("sources.manage_hooks")],
)
async def update_hook_admin(
    id_source: int,
    id_hook: int,
    body: HookUpdate,
    current_user: CurrentUser,
    db: DB,
):
    try:
        hook = await svc.update_hook(
            db, id_source, id_hook,
            actor_id=current_user.id_user,
            operation_type=body.operation_type,
            operation_config=body.operation_config,
            severity=body.severity,
            is_active=body.is_active,
        )
    except (HookNotFoundError, HookConflictError) as exc:
        _raise_hook_error(exc)
    return BaseResponse.ok(data=hook, app_code="source_hooks.updated")


@router.delete(
    "/hooks/{id_hook}",
    summary="Usun hook",
    description=(
        "Historia w skw_source_action_log POZOSTAJE po usunieciu hooka "
        "(diagnostyka przeszlych zdarzen jest dostepna nawet po usunieciu "
        "konfiguracji). "
        "**Wymaga:** `sources.manage_hooks`."
    ),
    responses={404: {"description": "Hook nie istnieje"}},
    dependencies=[require_permission("sources.manage_hooks")],
)
async def delete_hook_admin(
    id_source: int,
    id_hook: int,
    current_user: CurrentUser,
    db: DB,
):
    try:
        await svc.delete_hook(db, id_source, id_hook, actor_id=current_user.id_user)
    except HookNotFoundError as exc:
        _raise_hook_error(exc)
    return BaseResponse.ok(data={"id_hook": id_hook, "deleted": True}, app_code="source_hooks.deleted")


# =============================================================================
# ACTIONS
# =============================================================================

@router.get(
    "/actions",
    summary="Lista akcji zrodlowych",
    description=(
        "Akcje to przyciski kontekstowe dla dokumentu — NIE przesuwaja go po obiegu. "
        "Posortowane po sort_order ASC (kolejnosc w UI). "
        "**Wymaga:** `sources.view_log`."
    ),
    responses={404: {"description": "Zrodlo nie istnieje"}},
    dependencies=[require_permission("sources.view_log")],
)
async def list_actions_admin(id_source: int, current_user: CurrentUser, db: DB):
    try:
        actions = await svc.list_actions(db, id_source)
    except ActionNotFoundError as exc:
        _raise_action_error(exc)
    return BaseResponse.ok(data={"items": actions, "total": len(actions)}, app_code="source_actions.list")


@router.post(
    "/actions",
    status_code=201,
    summary="Utworz akcje zrodlowa",
    description=(
        "action_label jest wymagane — to etykieta wyswietlana uzytkownikowi "
        "(odrebna od action_name, ktora jest nazwa techniczna). "
        "Dla operation_type='file_move'/'file_delete' wymagane 'path_template' "
        "w operation_config. "
        "**Wymaga:** `sources.manage_actions`."
    ),
    responses={
        404: {"description": "Zrodlo nie istnieje"},
        422: {"description": "Walidacja operation_config nie powiodla sie"},
    },
    dependencies=[require_permission("sources.manage_actions")],
)
async def create_action_admin(
    id_source: int,
    body: ActionCreate,
    current_user: CurrentUser,
    db: DB,
):
    try:
        action = await svc.create_action(
            db, id_source,
            action_name=body.action_name,
            action_label=body.action_label,
            operation_type=body.operation_type,
            operation_config=body.operation_config,
            required_permission=body.required_permission,
            is_predefined=body.is_predefined,
            is_active=body.is_active,
            sort_order=body.sort_order,
            actor_id=current_user.id_user,
        )
    except ActionNotFoundError as exc:
        _raise_action_error(exc)
    return BaseResponse.ok(data=action, app_code="source_actions.created")


@router.put(
    "/actions/{id_action}",
    summary="Aktualizuj akcje (partial update)",
    responses={404: {"description": "Akcja nie istnieje"}},
    dependencies=[require_permission("sources.manage_actions")],
)
async def update_action_admin(
    id_source: int,
    id_action: int,
    body: ActionUpdate,
    current_user: CurrentUser,
    db: DB,
):
    try:
        action = await svc.update_action(
            db, id_source, id_action,
            actor_id=current_user.id_user,
            action_name=body.action_name,
            action_label=body.action_label,
            operation_type=body.operation_type,
            operation_config=body.operation_config,
            required_permission=body.required_permission,
            is_predefined=body.is_predefined,
            is_active=body.is_active,
            sort_order=body.sort_order,
        )
    except ActionNotFoundError as exc:
        _raise_action_error(exc)
    return BaseResponse.ok(data=action, app_code="source_actions.updated")


@router.delete(
    "/actions/{id_action}",
    summary="Usun akcje",
    description=(
        "Historia w skw_source_action_log POZOSTAJE po usunieciu akcji. "
        "**Wymaga:** `sources.manage_actions`."
    ),
    responses={404: {"description": "Akcja nie istnieje"}},
    dependencies=[require_permission("sources.manage_actions")],
)
async def delete_action_admin(
    id_source: int,
    id_action: int,
    current_user: CurrentUser,
    db: DB,
):
    try:
        await svc.delete_action(db, id_source, id_action, actor_id=current_user.id_user)
    except ActionNotFoundError as exc:
        _raise_action_error(exc)
    return BaseResponse.ok(data={"id_action": id_action, "deleted": True}, app_code="source_actions.deleted")