# backend/app/services/document_folder_service.py
"""
Serwis teczek dokumentow — F6 (sekcja 4.15).

Teczki to WYLACZNIE mechanizm filtrowania — nie wplywaja na obieg dokumentu.
Jeden dokument moze byc w wielu teczkach jednoczesnie (wielowymiarowosc).

Pokrywa:
  list_folders / get_folder / create_folder / update_folder / delete_folder
  add_document_to_folder / remove_document_from_folder
  list_documents_in_folder

Reguly widocznosci:
  - Teczka private: widoczna tylko dla owner_user
  - Teczka team: widoczna dla czlonkow owner_group (przez skw_approval_group_members)
  - Uzytkownik z documents.view_all widzi wszystkie teczki

UWAGA: from __future__ import annotations — NIGDY w tym pliku (SQLAlchemy ORM).
"""

import logging
import re
from datetime import datetime
from typing import Any

from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.approval.document_folder import DocumentFolder, VALID_FOLDER_TYPES
from app.db.models.approval.document_folder_item import DocumentFolderItem

logger = logging.getLogger(__name__)

_SCHEMA = "dbo"
_HEX_COLOR_RE = re.compile(r'^#[0-9A-Fa-f]{6}$')


class FolderNotFoundError(Exception):
    """Teczka o podanym ID nie istnieje."""


class FolderAccessDeniedError(Exception):
    """Uzytkownik nie ma dostepu do tej teczki (nie jest ownerem/czlonkiem grupy)."""


class FolderValidationError(Exception):
    """Walidacja teczki nie powiodla sie."""


class InstanceNotFoundError(Exception):
    """Instancja obiegu o podanym ID nie istnieje."""


# =============================================================================
# CRUD — lista i odczyt
# =============================================================================

async def list_folders(
    db: AsyncSession,
    *,
    actor_id: int,
    can_view_all: bool,
) -> list[dict[str, Any]]:
    """
    Lista teczek widocznych dla uzytkownika.

    Widoczne:
      - Wlasne teczki prywatne (owner_user = actor_id)
      - Teczki zespolowe grup do ktorych nalezy actor_id
      - Wszystkie (gdy can_view_all=True, np. documents.view_all)
    """
    if can_view_all:
        result = await db.execute(
            select(DocumentFolder)
            .where(DocumentFolder.is_active == True)  # noqa: E712
            .order_by(DocumentFolder.folder_name.asc())
        )
        folders = list(result.scalars().all())
    else:
        result = await db.execute(
            text(f"""
                SELECT f.[id_folder]
                FROM [{_SCHEMA}].[skw_document_folders] f
                WHERE f.[is_active] = 1
                  AND (
                      f.[owner_user] = :uid
                      OR f.[owner_group] IN (
                          SELECT [id_group] FROM [{_SCHEMA}].[skw_approval_group_members]
                          WHERE [id_user] = :uid
                      )
                  )
                ORDER BY f.[folder_name] ASC
            """),
            {"uid": actor_id},
        )
        ids = [r[0] for r in result.fetchall()]
        if not ids:
            return []
        folders_result = await db.execute(
            select(DocumentFolder)
            .where(DocumentFolder.id_folder.in_(ids))
            .order_by(DocumentFolder.folder_name.asc())
        )
        folders = list(folders_result.scalars().all())

    return [_folder_to_dict(f) for f in folders]


async def get_folder(
    db: AsyncSession,
    id_folder: int,
    *,
    actor_id: int,
    can_view_all: bool,
) -> dict[str, Any]:
    """Pobiera teczke. Weryfikuje dostep (private->owner, team->czlonek grupy)."""
    folder = await _get_folder_or_404(db, id_folder)
    await _ensure_access(db, folder, actor_id=actor_id, can_view_all=can_view_all)
    return _folder_to_dict(folder)


def _folder_to_dict(folder: DocumentFolder) -> dict[str, Any]:
    return {
        "id_folder":    folder.id_folder,
        "folder_name":  folder.folder_name,
        "description":  folder.description,
        "color":        folder.color,
        "folder_type":  folder.folder_type,
        "owner_user":   folder.owner_user,
        "owner_group":  folder.owner_group,
        "is_active":    folder.is_active,
        "created_at":   folder.created_at,
        "updated_at":   folder.updated_at,
    }


async def _get_folder_or_404(db: AsyncSession, id_folder: int) -> DocumentFolder:
    result = await db.execute(
        select(DocumentFolder).where(DocumentFolder.id_folder == id_folder)
    )
    folder = result.scalar_one_or_none()
    if folder is None:
        raise FolderNotFoundError(f"Teczka ID={id_folder} nie istnieje.")
    return folder


async def _ensure_access(
    db: AsyncSession,
    folder: DocumentFolder,
    *,
    actor_id: int,
    can_view_all: bool,
) -> None:
    """Rzuca FolderAccessDeniedError jesli uzytkownik nie ma dostepu do teczki."""
    if can_view_all:
        return
    if folder.folder_type == "private":
        if folder.owner_user != actor_id:
            raise FolderAccessDeniedError(
                f"Teczka ID={folder.id_folder} jest prywatna i nie nalezy do Ciebie."
            )
        return
    # team
    result = await db.execute(
        text(
            f"SELECT 1 FROM [{_SCHEMA}].[skw_approval_group_members] "
            f"WHERE [id_group] = :gid AND [id_user] = :uid"
        ),
        {"gid": folder.owner_group, "uid": actor_id},
    )
    if not result.fetchone():
        raise FolderAccessDeniedError(
            f"Nie jestes czlonkiem grupy bedacej wlascicielem teczki ID={folder.id_folder}."
        )


# =============================================================================
# CRUD — create / update / delete
# =============================================================================

async def create_folder(
    db: AsyncSession,
    *,
    folder_name: str,
    description: str | None,
    color: str | None,
    folder_type: str,
    owner_user: int | None,
    owner_group: int | None,
    actor_id: int,
) -> dict[str, Any]:
    """
    Tworzy nowa teczke.

    Walidacja zgodna z CHECK constraintami DB:
      - dokladnie jedno z owner_user/owner_group (zalezne od folder_type)
      - private -> owner_user, team -> owner_group
      - color: format #RRGGBB lub None
    """
    folder = DocumentFolder(
        folder_name=folder_name,
        description=description,
        color=color,
        folder_type=folder_type,
        owner_user=owner_user,
        owner_group=owner_group,
        is_active=True,
    )

    errors = folder.validate()
    if errors:
        raise FolderValidationError("; ".join(errors))

    # team: owner musi byc czlonkiem grupy ktora tworzy (sam siebie dodaje jako wlasciciel grupy)
    if folder_type == "team" and owner_group is not None:
        is_member = await db.execute(
            text(
                f"SELECT 1 FROM [{_SCHEMA}].[skw_approval_group_members] "
                f"WHERE [id_group] = :gid AND [id_user] = :uid"
            ),
            {"gid": owner_group, "uid": actor_id},
        )
        if not is_member.fetchone():
            raise FolderValidationError(
                "Nie mozesz utworzyc teczki zespolowej dla grupy do ktorej nie nalezysz."
            )

    db.add(folder)
    await db.flush()

    await _audit_log(
        db, actor_id=actor_id, action="document_folder.created",
        entity_id=folder.id_folder,
        details={"folder_name": folder_name, "folder_type": folder_type},
    )
    await db.commit()

    logger.info(
        "Teczka utworzona | id_folder=%s name=%r type=%r actor=%s",
        folder.id_folder, folder_name, folder_type, actor_id,
    )
    return _folder_to_dict(folder)


async def update_folder(
    db: AsyncSession,
    id_folder: int,
    *,
    actor_id: int,
    can_view_all: bool,
    folder_name: str | None = None,
    description: str | None = None,
    color: str | None = None,
    is_active: bool | None = None,
) -> dict[str, Any]:
    """
    Aktualizuje teczke (partial update).

    owner_user/owner_group/folder_type NIE sa edytowalne — usun i stworz
    nowa teczke jesli trzeba zmienic wlasciciela (unika niezgodnosci stanu
    z elementami juz przypisanymi).
    """
    folder = await _get_folder_or_404(db, id_folder)
    await _ensure_access(db, folder, actor_id=actor_id, can_view_all=can_view_all)

    changes: dict[str, Any] = {}
    if folder_name is not None:
        folder.folder_name = folder_name
        changes["folder_name"] = folder_name
    if description is not None:
        folder.description = description
        changes["description"] = description
    if color is not None:
        if color and not _HEX_COLOR_RE.match(color):
            raise FolderValidationError(f"color='{color}' nieprawidlowy. Wymagany format: #RRGGBB")
        folder.color = color
        changes["color"] = color
    if is_active is not None:
        folder.is_active = is_active
        changes["is_active"] = is_active

    errors = folder.validate()
    if errors:
        raise FolderValidationError("; ".join(errors))

    if changes:
        await _audit_log(
            db, actor_id=actor_id, action="document_folder.updated",
            entity_id=id_folder, details=changes,
        )

    await db.commit()
    logger.info("Teczka zaktualizowana | id_folder=%s changes=%s actor=%s", id_folder, list(changes), actor_id)
    return _folder_to_dict(folder)


async def delete_folder(
    db: AsyncSession,
    id_folder: int,
    *,
    actor_id: int,
    can_view_all: bool,
) -> None:
    """
    Usuwa teczke. CASCADE DELETE usuwa wszystkie wpisy w skw_document_folder_items
    (przypisania dokumentow) — same dokumenty/instancje obiegu NIE sa usuwane.
    """
    folder = await _get_folder_or_404(db, id_folder)
    await _ensure_access(db, folder, actor_id=actor_id, can_view_all=can_view_all)

    await _audit_log(
        db, actor_id=actor_id, action="document_folder.deleted",
        entity_id=id_folder, details={"folder_name": folder.folder_name},
    )

    await db.delete(folder)
    await db.commit()

    logger.warning(
        "Teczka usunieta | id_folder=%s name=%r actor=%s",
        id_folder, folder.folder_name, actor_id,
    )


# =============================================================================
# Przypisywanie dokumentow do teczek
# =============================================================================

async def add_document_to_folder(
    db: AsyncSession,
    id_instance: int,
    id_folder: int,
    *,
    actor_id: int,
    can_view_all: bool,
) -> dict[str, Any]:
    """
    Dodaje dokument do teczki. Idempotentne — jesli juz jest w teczce,
    zwraca istniejacy wpis bez bledu (PK kompozytowy zapobiega duplikatom).

    Raises:
        FolderNotFoundError: teczka nie istnieje.
        InstanceNotFoundError: instancja obiegu nie istnieje.
        FolderAccessDeniedError: brak dostepu do teczki.
    """
    folder = await _get_folder_or_404(db, id_folder)
    await _ensure_access(db, folder, actor_id=actor_id, can_view_all=can_view_all)

    inst_check = await db.execute(
        text(
            f"SELECT 1 FROM [{_SCHEMA}].[skw_document_approval_instances] "
            f"WHERE [id_instance] = :i"
        ),
        {"i": id_instance},
    )
    if not inst_check.fetchone():
        raise InstanceNotFoundError(f"Instancja obiegu ID={id_instance} nie istnieje.")

    existing = await db.execute(
        text(
            f"SELECT 1 FROM [{_SCHEMA}].[skw_document_folder_items] "
            f"WHERE [id_folder] = :f AND [id_instance] = :i"
        ),
        {"f": id_folder, "i": id_instance},
    )
    already_in_folder = bool(existing.fetchone())

    if not already_in_folder:
        item = DocumentFolderItem(
            id_folder=id_folder,
            id_instance=id_instance,
            added_by=actor_id,
        )
        db.add(item)
        await db.flush()

        await _audit_log(
            db, actor_id=actor_id, action="document_folder.document_added",
            entity_id=id_folder,
            details={"id_instance": id_instance},
        )
        await db.commit()
        logger.info(
            "Dokument dodany do teczki | id_folder=%s id_instance=%s actor=%s",
            id_folder, id_instance, actor_id,
        )

    return {
        "id_folder":      id_folder,
        "id_instance":    id_instance,
        "already_in_folder": already_in_folder,
    }


async def remove_document_from_folder(
    db: AsyncSession,
    id_instance: int,
    id_folder: int,
    *,
    actor_id: int,
    can_view_all: bool,
) -> None:
    """Usuwa dokument z teczki. Idempotentne — brak bledu jesli wpis nie istnieje."""
    folder = await _get_folder_or_404(db, id_folder)
    await _ensure_access(db, folder, actor_id=actor_id, can_view_all=can_view_all)

    result = await db.execute(
        select(DocumentFolderItem).where(
            DocumentFolderItem.id_folder == id_folder,
            DocumentFolderItem.id_instance == id_instance,
        )
    )
    item = result.scalar_one_or_none()
    if item is None:
        return  # idempotentne

    await db.delete(item)

    await _audit_log(
        db, actor_id=actor_id, action="document_folder.document_removed",
        entity_id=id_folder,
        details={"id_instance": id_instance},
    )
    await db.commit()

    logger.info(
        "Dokument usuniety z teczki | id_folder=%s id_instance=%s actor=%s",
        id_folder, id_instance, actor_id,
    )


async def list_documents_in_folder(
    db: AsyncSession,
    id_folder: int,
    *,
    actor_id: int,
    can_view_all: bool,
    page: int = 1,
    per_page: int = 50,
) -> dict[str, Any]:
    """Lista dokumentow (instancji obiegu) przypisanych do teczki, z paginacja."""
    folder = await _get_folder_or_404(db, id_folder)
    await _ensure_access(db, folder, actor_id=actor_id, can_view_all=can_view_all)

    count_result = await db.execute(
        text(
            f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_document_folder_items] "
            f"WHERE [id_folder] = :f"
        ),
        {"f": id_folder},
    )
    total = count_result.scalar() or 0

    result = await db.execute(
        text(f"""
            SELECT
                i.[id_instance], i.[id_document], i.[status],
                i.[document_title], i.[document_amount],
                fi.[added_by], fi.[added_at]
            FROM [{_SCHEMA}].[skw_document_folder_items] fi
            JOIN [{_SCHEMA}].[skw_document_approval_instances] i
              ON i.[id_instance] = fi.[id_instance]
            WHERE fi.[id_folder] = :f
            ORDER BY fi.[added_at] DESC
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
        """),
        {"f": id_folder, "offset": (page - 1) * per_page, "limit": per_page},
    )
    cols = list(result.keys())
    items = [dict(zip(cols, r)) for r in result.fetchall()]

    return {"items": items, "total": total, "page": page, "per_page": per_page}


# =============================================================================
# Pomocnicze
# =============================================================================

async def _audit_log(
    db: AsyncSession,
    *,
    actor_id: int,
    action: str,
    entity_id: int,
    details: dict[str, Any],
) -> None:
    """Zapisuje wpis do AuditLog. Blad zapisu nie przerywa operacji."""
    import json
    try:
        await db.execute(
            text(
                f"INSERT INTO [{_SCHEMA}].[skw_AuditLog] "
                f"([ID_USER], [Action], [EntityType], [EntityID], [NewValue], [Success], [Timestamp]) "
                f"VALUES (:uid, :action, N'DocumentFolder', :eid, :details, 1, SYSUTCDATETIME())"
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