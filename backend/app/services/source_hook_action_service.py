# backend/app/services/source_hook_action_service.py
"""
Serwis CRUD dla hookow (skw_source_hooks) i akcji zrodlowych (skw_source_actions) — F6.

Pokrywa logike biznesowa dla 8 endpointow:
  list_hooks / get_hook / create_hook / update_hook / delete_hook
  list_actions / get_action / create_action / update_action / delete_action

Kluczowe reguly biznesowe:
  - Max 1 aktywny hook per (id_source, trigger_action) — wymuszone przez
    UNIQUE filtered index w DB (UQ_skw_sh_source_action_active).
    Serwis sprawdza to PRZED INSERT zeby zwrocic czytelny blad 409
    zamiast nieprzyjaznego IntegrityError.
  - operation_config zapisywany jako JSON string (NVARCHAR(MAX) w DB).
  - Kazda zmiana hooka/akcji loguje sie do AuditLog.
  - Usuniecie hooka/akcji NIE usuwa historii w skw_source_action_log
    (FK ON DELETE NO ACTION) — diagnostyka pozostaje dostepna.

UWAGA: from __future__ import annotations — NIGDY w tym pliku (SQLAlchemy ORM).
"""

import json
import logging
from datetime import datetime
from typing import Any

from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.approval.source_hook import SourceHook
from app.db.models.approval.source_action import SourceAction
from app.db.models.approval.document_source import DocumentSource

logger = logging.getLogger(__name__)

_SCHEMA = "dbo"


class HookNotFoundError(Exception):
    """Hook o podanym ID nie istnieje (lub nie nalezy do podanego zrodla)."""


class HookConflictError(Exception):
    """Juz istnieje aktywny hook dla (id_source, trigger_action)."""


class ActionNotFoundError(Exception):
    """Akcja o podanym ID nie istnieje (lub nie nalezy do podanego zrodla)."""


# =============================================================================
# HOOKS — CRUD
# =============================================================================

async def list_hooks(db: AsyncSession, id_source: int) -> list[dict[str, Any]]:
    """Lista hookow dla zrodla, posortowana po id_hook."""
    await _ensure_source_exists(db, id_source)
    result = await db.execute(
        select(SourceHook)
        .where(SourceHook.id_source == id_source)
        .order_by(SourceHook.id_hook.asc())
    )
    hooks = list(result.scalars().all())
    return [_hook_to_dict(h) for h in hooks]


async def get_hook(db: AsyncSession, id_source: int, id_hook: int) -> dict[str, Any]:
    """Pobiera pojedynczy hook. Weryfikuje przynaleznosc do id_source."""
    hook = await _get_hook_or_404(db, id_source, id_hook)
    return _hook_to_dict(hook)


async def create_hook(
    db: AsyncSession,
    id_source: int,
    *,
    trigger_action: str,
    operation_type: str,
    operation_config: dict[str, Any],
    severity: str,
    is_active: bool,
    actor_id: int,
) -> dict[str, Any]:
    """
    Tworzy nowy hook.

    Sprawdza PRZED INSERT czy istnieje juz aktywny hook dla tej kombinacji
    (id_source, trigger_action) — DB ma UNIQUE filtered index ale wolimy
    zwrocic czytelny 409 niz nieprzyjazny IntegrityError.

    Raises:
        HookConflictError: aktywny hook dla (id_source, trigger_action) juz istnieje.
    """
    await _ensure_source_exists(db, id_source)

    if is_active:
        existing = await db.execute(
            text(
                f"SELECT [id_hook] FROM [{_SCHEMA}].[skw_source_hooks] "
                f"WHERE [id_source] = :src AND [trigger_action] = :ta AND [is_active] = 1"
            ),
            {"src": id_source, "ta": trigger_action},
        )
        if existing.fetchone():
            raise HookConflictError(
                f"Aktywny hook dla trigger_action='{trigger_action}' juz istnieje "
                f"dla tego zrodla. Dezaktywuj go najpierw lub edytuj istniejacy."
            )

    hook = SourceHook(
        id_source=id_source,
        trigger_action=trigger_action,
        operation_type=operation_type,
        severity=severity,
        is_active=is_active,
    )
    hook.set_operation_config(operation_config)

    errors = hook.validate()
    if errors:
        raise HTTPException(status_code=422, detail="; ".join(errors))

    db.add(hook)
    await db.flush()

    await _audit_log(
        db, actor_id=actor_id, action="source_hook.created",
        entity_id=hook.id_hook,
        details={"id_source": id_source, "trigger_action": trigger_action, "severity": severity},
    )
    await db.commit()

    logger.info(
        "Hook utworzony | id_hook=%s id_source=%s trigger=%s severity=%s actor=%s",
        hook.id_hook, id_source, trigger_action, severity, actor_id,
    )
    return _hook_to_dict(hook)


async def update_hook(
    db: AsyncSession,
    id_source: int,
    id_hook: int,
    *,
    actor_id: int,
    operation_type: str | None = None,
    operation_config: dict[str, Any] | None = None,
    severity: str | None = None,
    is_active: bool | None = None,
) -> dict[str, Any]:
    """Aktualizuje hook (partial update). trigger_action NIE jest edytowalny — usun i stworz nowy."""
    hook = await _get_hook_or_404(db, id_source, id_hook)

    # Jesli wlaczamy hook (is_active False->True) — sprawdz konflikt
    if is_active is True and not hook.is_active:
        existing = await db.execute(
            text(
                f"SELECT [id_hook] FROM [{_SCHEMA}].[skw_source_hooks] "
                f"WHERE [id_source] = :src AND [trigger_action] = :ta "
                f"  AND [is_active] = 1 AND [id_hook] != :hid"
            ),
            {"src": id_source, "ta": hook.trigger_action, "hid": id_hook},
        )
        if existing.fetchone():
            raise HookConflictError(
                f"Nie mozna aktywowac — inny aktywny hook dla "
                f"trigger_action='{hook.trigger_action}' juz istnieje."
            )

    changes: dict[str, Any] = {}
    if operation_type is not None:
        hook.operation_type = operation_type
        changes["operation_type"] = operation_type
    if operation_config is not None:
        hook.set_operation_config(operation_config)
        changes["operation_config"] = "<zmieniono>"
    if severity is not None:
        hook.severity = severity
        changes["severity"] = severity
    if is_active is not None:
        hook.is_active = is_active
        changes["is_active"] = is_active

    errors = hook.validate()
    if errors:
        raise HTTPException(status_code=422, detail="; ".join(errors))

    if changes:
        await _audit_log(
            db, actor_id=actor_id, action="source_hook.updated",
            entity_id=id_hook, details=changes,
        )

    await db.commit()
    logger.info("Hook zaktualizowany | id_hook=%s changes=%s actor=%s", id_hook, list(changes), actor_id)
    return _hook_to_dict(hook)


async def delete_hook(db: AsyncSession, id_source: int, id_hook: int, *, actor_id: int) -> None:
    """
    Usuwa hook.

    Historia w skw_source_action_log POZOSTAJE (FK ON DELETE NO ACTION) —
    jesli ten hook mial wpisy w logu, beda nadal widoczne z id_hook
    wskazujacym na juz nieistniejacy wiersz. To zamierzone — diagnostyka
    przeszlych zdarzen jest wazniejsza niz czystosc FK.
    """
    hook = await _get_hook_or_404(db, id_source, id_hook)

    await _audit_log(
        db, actor_id=actor_id, action="source_hook.deleted",
        entity_id=id_hook,
        details={"id_source": id_source, "trigger_action": hook.trigger_action},
    )

    await db.delete(hook)
    await db.commit()

    logger.warning(
        "Hook usuniety | id_hook=%s id_source=%s trigger=%s actor=%s",
        id_hook, id_source, hook.trigger_action, actor_id,
    )


def _hook_to_dict(hook: SourceHook) -> dict[str, Any]:
    return {
        "id_hook":          hook.id_hook,
        "id_source":        hook.id_source,
        "trigger_action":   hook.trigger_action,
        "operation_type":   hook.operation_type,
        "operation_config": hook.get_operation_config(),
        "severity":         hook.severity,
        "is_active":        hook.is_active,
        "created_at":       hook.created_at,
    }


async def _get_hook_or_404(db: AsyncSession, id_source: int, id_hook: int) -> SourceHook:
    result = await db.execute(
        select(SourceHook).where(
            SourceHook.id_hook == id_hook,
            SourceHook.id_source == id_source,
        )
    )
    hook = result.scalar_one_or_none()
    if hook is None:
        raise HookNotFoundError(
            f"Hook ID={id_hook} nie istnieje dla zrodla ID={id_source}."
        )
    return hook


# =============================================================================
# ACTIONS — CRUD
# =============================================================================

async def list_actions(db: AsyncSession, id_source: int) -> list[dict[str, Any]]:
    """Lista akcji dla zrodla, posortowana po sort_order ASC."""
    await _ensure_source_exists(db, id_source)
    result = await db.execute(
        select(SourceAction)
        .where(SourceAction.id_source == id_source)
        .order_by(SourceAction.sort_order.asc(), SourceAction.id_action.asc())
    )
    actions = list(result.scalars().all())
    return [_action_to_dict(a) for a in actions]


async def get_action(db: AsyncSession, id_source: int, id_action: int) -> dict[str, Any]:
    """Pobiera pojedyncza akcje. Weryfikuje przynaleznosc do id_source."""
    action = await _get_action_or_404(db, id_source, id_action)
    return _action_to_dict(action)


async def create_action(
    db: AsyncSession,
    id_source: int,
    *,
    action_name: str,
    action_label: str,
    operation_type: str,
    operation_config: dict[str, Any],
    required_permission: str | None,
    is_predefined: bool,
    is_active: bool,
    sort_order: int,
    actor_id: int,
) -> dict[str, Any]:
    """Tworzy nowa akcje zrodlowa."""
    await _ensure_source_exists(db, id_source)

    action = SourceAction(
        id_source=id_source,
        action_name=action_name,
        action_label=action_label,
        operation_type=operation_type,
        required_permission=required_permission,
        is_predefined=is_predefined,
        is_active=is_active,
        sort_order=sort_order,
    )
    action.set_operation_config(operation_config)

    errors = action.validate()
    if errors:
        raise HTTPException(status_code=422, detail="; ".join(errors))

    db.add(action)
    await db.flush()

    await _audit_log(
        db, actor_id=actor_id, action="source_action.created",
        entity_id=action.id_action,
        details={"id_source": id_source, "action_name": action_name},
    )
    await db.commit()

    logger.info(
        "Akcja utworzona | id_action=%s id_source=%s name=%s actor=%s",
        action.id_action, id_source, action_name, actor_id,
    )
    return _action_to_dict(action)


async def update_action(
    db: AsyncSession,
    id_source: int,
    id_action: int,
    *,
    actor_id: int,
    action_name: str | None = None,
    action_label: str | None = None,
    operation_type: str | None = None,
    operation_config: dict[str, Any] | None = None,
    required_permission: str | None = None,
    is_predefined: bool | None = None,
    is_active: bool | None = None,
    sort_order: int | None = None,
) -> dict[str, Any]:
    """Aktualizuje akcje (partial update)."""
    action = await _get_action_or_404(db, id_source, id_action)

    changes: dict[str, Any] = {}
    if action_name is not None:
        action.action_name = action_name
        changes["action_name"] = action_name
    if action_label is not None:
        action.action_label = action_label
        changes["action_label"] = action_label
    if operation_type is not None:
        action.operation_type = operation_type
        changes["operation_type"] = operation_type
    if operation_config is not None:
        action.set_operation_config(operation_config)
        changes["operation_config"] = "<zmieniono>"
    if required_permission is not None:
        action.required_permission = required_permission
        changes["required_permission"] = required_permission
    if is_predefined is not None:
        action.is_predefined = is_predefined
        changes["is_predefined"] = is_predefined
    if is_active is not None:
        action.is_active = is_active
        changes["is_active"] = is_active
    if sort_order is not None:
        action.sort_order = sort_order
        changes["sort_order"] = sort_order

    errors = action.validate()
    if errors:
        raise HTTPException(status_code=422, detail="; ".join(errors))

    if changes:
        await _audit_log(
            db, actor_id=actor_id, action="source_action.updated",
            entity_id=id_action, details=changes,
        )

    await db.commit()
    logger.info("Akcja zaktualizowana | id_action=%s changes=%s actor=%s", id_action, list(changes), actor_id)
    return _action_to_dict(action)


async def delete_action(db: AsyncSession, id_source: int, id_action: int, *, actor_id: int) -> None:
    """Usuwa akcje. Historia w skw_source_action_log pozostaje (FK ON DELETE NO ACTION)."""
    action = await _get_action_or_404(db, id_source, id_action)

    await _audit_log(
        db, actor_id=actor_id, action="source_action.deleted",
        entity_id=id_action,
        details={"id_source": id_source, "action_name": action.action_name},
    )

    await db.delete(action)
    await db.commit()

    logger.warning(
        "Akcja usunieta | id_action=%s id_source=%s name=%s actor=%s",
        id_action, id_source, action.action_name, actor_id,
    )


def _action_to_dict(action: SourceAction) -> dict[str, Any]:
    return {
        "id_action":            action.id_action,
        "id_source":            action.id_source,
        "action_name":          action.action_name,
        "action_label":         action.action_label,
        "operation_type":       action.operation_type,
        "operation_config":     action.get_operation_config(),
        "required_permission":  action.required_permission,
        "is_predefined":        action.is_predefined,
        "is_active":            action.is_active,
        "sort_order":           action.sort_order,
        "created_at":           action.created_at,
    }


async def _get_action_or_404(db: AsyncSession, id_source: int, id_action: int) -> SourceAction:
    result = await db.execute(
        select(SourceAction).where(
            SourceAction.id_action == id_action,
            SourceAction.id_source == id_source,
        )
    )
    action = result.scalar_one_or_none()
    if action is None:
        raise ActionNotFoundError(
            f"Akcja ID={id_action} nie istnieje dla zrodla ID={id_source}."
        )
    return action


# =============================================================================
# Pomocnicze
# =============================================================================

async def _ensure_source_exists(db: AsyncSession, id_source: int) -> None:
    """Sprawdza istnienie zrodla. Rzuca 404 jesli nie istnieje (uzywane przed list/create)."""
    result = await db.execute(
        select(DocumentSource.id_source).where(DocumentSource.id_source == id_source)
    )
    if result.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=404,
            detail=f"Zrodlo ID={id_source} nie istnieje.",
        )


async def _audit_log(
    db: AsyncSession,
    *,
    actor_id: int,
    action: str,
    entity_id: int,
    details: dict[str, Any],
) -> None:
    """Zapisuje wpis do AuditLog. Blad zapisu nie przerywa operacji."""
    try:
        await db.execute(
            text(
                f"INSERT INTO [{_SCHEMA}].[skw_AuditLog] "
                f"([ID_USER], [Action], [EntityType], [EntityID], [NewValue], [Success], [Timestamp]) "
                f"VALUES (:uid, :action, N'SourceHookAction', :eid, :details, 1, SYSUTCDATETIME())"
            ),
            {
                "uid":     actor_id,
                "action":  action,
                "eid":     str(entity_id),
                "details": json.dumps(details, ensure_ascii=False, default=str),
            },
        )
    except Exception as exc:
        logger.error("_audit_log: blad zapisu dla action=%s: %s", action, exc)