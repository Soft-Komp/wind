# backend/app/services/document_folder_service.py
"""
Serwis teczek dokumentow — F6 (sekcja 4.15) + widocznosc wielogrupowa (0074)
+ obejscie administracyjne (0075) + soft-delete/migawki/bulk (0076).

Teczki to WYLACZNIE mechanizm filtrowania — nie wplywaja na obieg dokumentu.
Jeden dokument moze byc w wielu teczkach jednoczesnie (wielowymiarowosc).

Reguly widocznosci (ODCZYT teczki i jej zawartosci — GET folder, GET items):
  - Teczka private: widoczna tylko dla owner_user
  - Teczka team: widoczna dla czlonkow owner_group
  - Widoczna TAKZE dla czlonkow KAZDEJ AKTYWNEJ (removed_at IS NULL) grupy
    przypisanej przez skw_document_folder_groups (0074/0076), niezaleznie
    od folder_type.
  - documents.view_all / approval.supervise -> widzi wszystko (can_view_all).
  - documents.manage_all_folders -> widzi wszystko (can_manage_all_folders).

Reguly MODYFIKACJI ZAWARTOSCI (dodanie/usuniecie DOKUMENTU z teczki):
  - Wlasciciel — bez zmian.
  - can_manage_all_folders (0075) — pelne obejscie.
  - Grupa wspoldzielona (0074) — wymaga DODATKOWO can_manage_shared_items
    (documents.assign_shared_folder).
  - can_view_all — przepuszcza jak dotychczas.

Reguly ZARZADZANIA LISTA GRUP WSPOLDZIELONYCH (odczyt/dodanie/usuniecie
samych GRUP, nie dokumentow) — ZLECENIE FRONTU (06.08.2026, punkt 2):
  - WYLACZNIE wlasciciel LUB administrator z can_manage_all_folders.
  - Gosc (dostep tylko przez shared_group) NIE MA tu dostepu, nawet jesli
    widzi zawartosc teczki. can_view_all NIE wystarcza — to jest swiadome
    zawezenie wzgledem reszty operacji, wynikajace z decyzji frontu.

RETROAKTYWNOSC (0076, ZLECENIE FRONTU — "Opcja A"):
  - Usuniecie grupy wspoldzielonej jest SOFT-DELETE (removed_at), nie
    fizyczny DELETE.
  - Przy usunieciu tworzona jest MIGAWKA aktualnej zawartosci teczki
    (skw_document_folder_snapshots) — bedaca podstawa dla przyszlego
    widoku historycznego (patrz PLACEHOLDER nizej).
  - Aktywna zawartosc (GET items) jest odtad niedostepna dla bylych
    czlonkow — korzystaja z osobnego (jeszcze niezaimplementowanego)
    widoku historycznego.

BULK (0076, ZLECENIE FRONTU — punkt 7):
  - add_groups_to_folder_bulk: atomowe, bez limitu liczby elementow.
    Jesli KTORAKOLWIEK grupa nie istnieje — ZERO zmian w bazie.

PLACEHOLDERY (0076, punkt 1 i 4) — jawnie NIEZAIMPLEMENTOWANE:
  - transfer_folder_ownership
  - list_orphaned_folders
  - list_documents_in_folder_historical (dodatkowo: wymaga rozstrzygniecia
    NIEROZWIAZANEGO problemu — system nie rejestruje historii czlonkostwa
    w grupie, skw_approval_group_members nie ma added_at/removed_at;
    migawka zawartosci teczki sama nie odpowiada na pytanie "czy TEN user
    byl czlonkiem TEJ grupy W MOMENCIE usuniecia")

UWAGA: from __future__ import annotations — NIGDY w tym pliku (SQLAlchemy ORM).
"""

import json
import logging
import re
from datetime import datetime
from typing import Any, Literal

from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.approval.document_folder import DocumentFolder, VALID_FOLDER_TYPES
from app.db.models.approval.document_folder_item import DocumentFolderItem
from app.db.models.approval.document_folder_group import DocumentFolderGroup

logger = logging.getLogger(__name__)

_SCHEMA = "dbo"
_HEX_COLOR_RE = re.compile(r'^#[0-9A-Fa-f]{6}$')

AccessVia = Literal["view_all", "owner", "shared_group", "admin_override"]


class FolderNotFoundError(Exception):
    """Teczka o podanym ID nie istnieje."""


class FolderAccessDeniedError(Exception):
    """Uzytkownik nie ma dostepu do tej teczki (nie jest ownerem/czlonkiem grupy)."""


class FolderValidationError(Exception):
    """Walidacja teczki nie powiodla sie."""


class InstanceNotFoundError(Exception):
    """Instancja obiegu o podanym ID nie istnieje."""


class GroupNotFoundError(Exception):
    """Grupa o podanym ID nie istnieje."""


# =============================================================================
# CRUD — lista i odczyt
# =============================================================================

async def list_folders(
    db: AsyncSession,
    *,
    actor_id: int,
    can_view_all: bool,
    can_manage_all_folders: bool = False,
) -> list[dict[str, Any]]:
    """
    Lista teczek widocznych dla uzytkownika.

    can_manage_all_folders dziala jak can_view_all dla PURPOSE tej funkcji
    ("widz wszystko") — ale to dwa NIEZALEZNE uprawnienia, stad OR, nie
    zastapienie jednego przez drugie.
    """
    if can_view_all or can_manage_all_folders:
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
                      OR f.[id_folder] IN (
                          SELECT fg.[id_folder]
                          FROM [{_SCHEMA}].[skw_document_folder_groups] fg
                          JOIN [{_SCHEMA}].[skw_approval_group_members] gm
                            ON gm.[id_group] = fg.[id_group]
                          WHERE gm.[id_user] = :uid AND fg.[removed_at] IS NULL
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
    can_manage_all_folders: bool = False,
) -> dict[str, Any]:
    """Pobiera teczke. Weryfikuje dostep."""
    folder = await _get_folder_or_404(db, id_folder)
    await _ensure_access(
        db, folder, actor_id=actor_id, can_view_all=can_view_all,
        can_manage_all_folders=can_manage_all_folders,
    )
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


async def _is_owner_access(
    db: AsyncSession,
    folder: DocumentFolder,
    *,
    actor_id: int,
) -> bool:
    """True jesli actor_id ma dostep do teczki przez wlasciciela."""
    if folder.folder_type == "private":
        return folder.owner_user == actor_id
    result = await db.execute(
        text(
            f"SELECT 1 FROM [{_SCHEMA}].[skw_approval_group_members] "
            f"WHERE [id_group] = :gid AND [id_user] = :uid"
        ),
        {"gid": folder.owner_group, "uid": actor_id},
    )
    return bool(result.fetchone())


async def _is_shared_group_access(
    db: AsyncSession,
    folder: DocumentFolder,
    *,
    actor_id: int,
) -> bool:
    """
    True jesli actor_id ma dostep do teczki przez AKTYWNE (removed_at IS NULL)
    przypisanie w skw_document_folder_groups (0074/0076).
    """
    result = await db.execute(
        text(f"""
            SELECT 1
            FROM [{_SCHEMA}].[skw_document_folder_groups] fg
            JOIN [{_SCHEMA}].[skw_approval_group_members] gm
              ON gm.[id_group] = fg.[id_group]
            WHERE fg.[id_folder] = :f AND gm.[id_user] = :uid
              AND fg.[removed_at] IS NULL
        """),
        {"f": folder.id_folder, "uid": actor_id},
    )
    return bool(result.fetchone())


async def _resolve_access_via(
    db: AsyncSession,
    folder: DocumentFolder,
    *,
    actor_id: int,
    can_view_all: bool,
    can_manage_all_folders: bool,
) -> AccessVia | None:
    """
    Rozstrzyga, PRZEZ JAKA sciezke actor_id ma dostep do teczki — do
    audytu ORAZ logiki autoryzacji. Zwraca None jesli brak dostepu.

    Priorytet (dla audytu — najbardziej swoista przyczyna wygrywa):
      1. owner, 2. shared_group, 3. view_all, 4. admin_override
    """
    if await _is_owner_access(db, folder, actor_id=actor_id):
        return "owner"
    if await _is_shared_group_access(db, folder, actor_id=actor_id):
        return "shared_group"
    if can_view_all:
        return "view_all"
    if can_manage_all_folders:
        return "admin_override"
    return None


async def _ensure_access(
    db: AsyncSession,
    folder: DocumentFolder,
    *,
    actor_id: int,
    can_view_all: bool,
    can_manage_all_folders: bool = False,
) -> AccessVia:
    """Rzuca FolderAccessDeniedError jesli brak ZADNEGO dostepu (odczyt). Zwraca AccessVia."""
    access_via = await _resolve_access_via(
        db, folder, actor_id=actor_id, can_view_all=can_view_all,
        can_manage_all_folders=can_manage_all_folders,
    )
    if access_via is None:
        raise FolderAccessDeniedError(
            f"Brak dostepu do teczki ID={folder.id_folder} — nie jestes "
            f"wlascicielem ani czlonkiem grupy wspoldzielonej."
        )
    return access_via


async def _ensure_write_access(
    db: AsyncSession,
    folder: DocumentFolder,
    *,
    actor_id: int,
    can_view_all: bool,
    can_manage_shared_items: bool,
    can_manage_all_folders: bool = False,
) -> AccessVia:
    """
    Rzuca FolderAccessDeniedError jesli uzytkownik nie moze MODYFIKOWAC
    zawartosci teczki (dodac/usunac DOKUMENT — nie grupe, patrz
    _ensure_group_management_access dla tamtej operacji).
    """
    if await _is_owner_access(db, folder, actor_id=actor_id):
        return "owner"

    if can_manage_all_folders:
        return "admin_override"

    if await _is_shared_group_access(db, folder, actor_id=actor_id):
        if can_manage_shared_items:
            return "shared_group"
        raise FolderAccessDeniedError(
            f"Masz dostep do teczki ID={folder.id_folder} przez grupe "
            f"wspoldzielona, ale brak Ci uprawnienia "
            f"'documents.assign_shared_folder' do modyfikacji jej zawartosci."
        )

    if can_view_all:
        return "view_all"

    raise FolderAccessDeniedError(
        f"Brak dostepu do teczki ID={folder.id_folder} — nie jestes "
        f"wlascicielem ani czlonkiem grupy wspoldzielonej."
    )


async def _ensure_group_management_access(
    db: AsyncSession,
    folder: DocumentFolder,
    *,
    actor_id: int,
    can_manage_all_folders: bool,
) -> AccessVia:
    """
    ZLECENIE FRONTU (06.08.2026, punkt 2): zarzadzanie lista grup
    wspoldzielonych (odczyt GET .../groups ORAZ modyfikacja POST/DELETE/
    bulk) jest zarezerwowane dla WLASCICIELA lub administratora z
    can_manage_all_folders — NIE dla goscia, i NIE wystarcza samo
    can_view_all (to swiadome zawezenie wzgledem innych operacji na
    teczce, wynikajace z decyzji frontu "tylko wlasciciel").

    Decyzja Claude (na wyrazne polecenie frontu "podejmij odpowiednia
    decyzje"): administrator z can_manage_all_folders WLICZA SIE do
    "tylko wlasciciel", zgodnie z wczesniejsza decyzja frontu, ze to
    uprawnienie jest PELNYM obejsciem dla WSZYSTKICH operacji zespolowych.
    """
    if await _is_owner_access(db, folder, actor_id=actor_id):
        return "owner"
    if can_manage_all_folders:
        return "admin_override"
    raise FolderAccessDeniedError(
        f"Zarzadzanie grupami wspoldzielonymi teczki ID={folder.id_folder} "
        f"jest zarezerwowane dla wlasciciela lub administratora."
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
    can_manage_all_folders: bool = False,
) -> dict[str, Any]:
    """
    Tworzy nowa teczke.

    can_manage_all_folders (0075) pomija walidacje czlonkostwa w
    owner_group dla folder_type='team'. Bez tego uprawnienia zachowanie
    jest DOKLADNIE jak przed 0075.
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

    access_via: AccessVia = "owner"

    if folder_type == "team" and owner_group is not None:
        is_member = await db.execute(
            text(
                f"SELECT 1 FROM [{_SCHEMA}].[skw_approval_group_members] "
                f"WHERE [id_group] = :gid AND [id_user] = :uid"
            ),
            {"gid": owner_group, "uid": actor_id},
        )
        if is_member.fetchone():
            access_via = "owner"
        elif can_manage_all_folders:
            access_via = "admin_override"
        else:
            raise FolderValidationError(
                "Nie mozesz utworzyc teczki zespolowej dla grupy do ktorej nie nalezysz."
            )

    db.add(folder)
    await db.flush()

    await _audit_log(
        db, actor_id=actor_id, action="document_folder.created",
        entity_id=folder.id_folder,
        details={"folder_name": folder_name, "folder_type": folder_type},
        access_via=access_via,
    )
    await db.commit()

    logger.info(
        "Teczka utworzona | id_folder=%s name=%r type=%r actor=%s access_via=%s",
        folder.id_folder, folder_name, folder_type, actor_id, access_via,
    )
    return _folder_to_dict(folder)


async def update_folder(
    db: AsyncSession,
    id_folder: int,
    *,
    actor_id: int,
    can_view_all: bool,
    can_manage_all_folders: bool = False,
    folder_name: str | None = None,
    description: str | None = None,
    color: str | None = None,
    is_active: bool | None = None,
) -> dict[str, Any]:
    """Aktualizuje teczke (partial update). owner_user/owner_group/folder_type NIE sa edytowalne."""
    folder = await _get_folder_or_404(db, id_folder)
    access_via = await _ensure_access(
        db, folder, actor_id=actor_id, can_view_all=can_view_all,
        can_manage_all_folders=can_manage_all_folders,
    )

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
            entity_id=id_folder, details=changes, access_via=access_via,
        )

    await db.commit()
    logger.info(
        "Teczka zaktualizowana | id_folder=%s changes=%s actor=%s access_via=%s",
        id_folder, list(changes), actor_id, access_via,
    )
    return _folder_to_dict(folder)


async def delete_folder(
    db: AsyncSession,
    id_folder: int,
    *,
    actor_id: int,
    can_view_all: bool,
    can_manage_all_folders: bool = False,
) -> None:
    """Usuwa teczke. CASCADE DELETE usuwa wpisy w skw_document_folder_items i skw_document_folder_groups."""
    folder = await _get_folder_or_404(db, id_folder)
    access_via = await _ensure_access(
        db, folder, actor_id=actor_id, can_view_all=can_view_all,
        can_manage_all_folders=can_manage_all_folders,
    )

    await _audit_log(
        db, actor_id=actor_id, action="document_folder.deleted",
        entity_id=id_folder, details={"folder_name": folder.folder_name},
        access_via=access_via,
    )

    await db.delete(folder)
    await db.commit()

    logger.warning(
        "Teczka usunieta | id_folder=%s name=%r actor=%s access_via=%s",
        id_folder, folder.folder_name, actor_id, access_via,
    )


# =============================================================================
# PLACEHOLDER (0076, ZLECENIE FRONTU — punkt 1)
# =============================================================================

async def transfer_folder_ownership(
    db: AsyncSession,
    id_folder: int,
    *,
    new_owner_user: int,
    actor_id: int,
    can_manage_all_folders: bool,
) -> dict[str, Any]:
    """
    PLACEHOLDER. ZLECENIE FRONTU (06.08.2026): jedyny mechanizm reagowania
    na dezaktywacje wlasciciela prywatnej teczki — WYLACZNIE reczny.
    Front potwierdzil: "te dwa endpointy wystarczaja jako rozwiazanie" —
    brak automatyki przy samej dezaktywacji konta.

    DO ZAIMPLEMENTOWANIA:
      - walidacja: folder.folder_type == 'private' (inaczej FolderValidationError)
      - walidacja: new_owner_user istnieje i jest aktywny (skw_Users.IsActive=1)
      - wymaga can_manage_all_folders bezwzglednie (stary wlasciciel z
        definicji moze byc juz dezaktywowany, nie moze sam siebie zmienic)
      - audit_log z access_via="admin_override" zawsze
    """
    raise NotImplementedError(
        "transfer_folder_ownership: placeholder, do zaimplementowania po "
        "potwierdzeniu walidacji new_owner_user (0076)"
    )


async def list_orphaned_folders(
    db: AsyncSession,
    *,
    can_manage_all_folders: bool,
) -> list[dict[str, Any]]:
    """
    PLACEHOLDER. ZLECENIE FRONTU (06.08.2026): lista teczek prywatnych,
    ktorych owner_user jest dzis nieaktywny — zrodlo kandydatow do
    transfer_folder_ownership().

    DO ZAIMPLEMENTOWANIA:
      - JOIN skw_document_folders + skw_Users, WHERE folder_type='private'
        AND owner_user IS NOT NULL AND u.IsActive = 0 AND f.is_active = 1
      - wymaga can_manage_all_folders (widok administracyjny)
    """
    raise NotImplementedError(
        "list_orphaned_folders: placeholder, do zaimplementowania (0076)"
    )


# =============================================================================
# Zarzadzanie grupami wspoldzielonymi (0074/0076)
# =============================================================================

async def list_folder_groups(
    db: AsyncSession,
    id_folder: int,
    *,
    actor_id: int,
    can_manage_all_folders: bool = False,
) -> list[dict[str, Any]]:
    """
    Lista AKTYWNYCH (removed_at IS NULL) grup przypisanych do teczki.

    ZLECENIE FRONTU (punkt 2): dostep WYLACZNIE wlasciciel/administrator —
    patrz _ensure_group_management_access. can_view_all NIE jest tu
    przyjmowane — swiadome zawezenie, gosc i "zwykly" supervisor bez
    can_manage_all_folders NIE widza tej listy.
    """
    folder = await _get_folder_or_404(db, id_folder)
    await _ensure_group_management_access(
        db, folder, actor_id=actor_id, can_manage_all_folders=can_manage_all_folders,
    )

    result = await db.execute(
        text(f"""
            SELECT fg.[id_folder_group], fg.[id_group], g.[group_name],
                   fg.[added_by], fg.[added_at]
            FROM [{_SCHEMA}].[skw_document_folder_groups] fg
            JOIN [{_SCHEMA}].[skw_approval_groups] g ON g.[id_group] = fg.[id_group]
            WHERE fg.[id_folder] = :f AND fg.[removed_at] IS NULL
            ORDER BY g.[group_name] ASC
        """),
        {"f": id_folder},
    )
    cols = list(result.keys())
    return [dict(zip(cols, r)) for r in result.fetchall()]


async def add_group_to_folder(
    db: AsyncSession,
    id_folder: int,
    id_group: int,
    *,
    actor_id: int,
    can_manage_all_folders: bool = False,
) -> dict[str, Any]:
    """
    Przypisuje grupe jako dodatkowego "gościa" teczki. Idempotentne
    (jesli AKTYWNE przypisanie juz istnieje).

    ZLECENIE FRONTU (punkt 2): dostep WYLACZNIE wlasciciel/administrator.
    """
    folder = await _get_folder_or_404(db, id_folder)
    access_via = await _ensure_group_management_access(
        db, folder, actor_id=actor_id, can_manage_all_folders=can_manage_all_folders,
    )

    group_check = await db.execute(
        text(f"SELECT 1 FROM [{_SCHEMA}].[skw_approval_groups] WHERE [id_group] = :g"),
        {"g": id_group},
    )
    if not group_check.fetchone():
        raise GroupNotFoundError(f"Grupa ID={id_group} nie istnieje.")

    existing = await db.execute(
        text(
            f"SELECT 1 FROM [{_SCHEMA}].[skw_document_folder_groups] "
            f"WHERE [id_folder] = :f AND [id_group] = :g AND [removed_at] IS NULL"
        ),
        {"f": id_folder, "g": id_group},
    )
    already_assigned = bool(existing.fetchone())

    if not already_assigned:
        assignment = DocumentFolderGroup(
            id_folder=id_folder,
            id_group=id_group,
            added_by=actor_id,
        )
        db.add(assignment)
        await db.flush()

        await _audit_log(
            db, actor_id=actor_id, action="document_folder.group_added",
            entity_id=id_folder, details={"id_group": id_group},
            access_via=access_via,
        )
        await db.commit()
        logger.info(
            "Grupa przypisana do teczki | id_folder=%s id_group=%s actor=%s access_via=%s",
            id_folder, id_group, actor_id, access_via,
        )

    return {
        "id_folder":         id_folder,
        "id_group":          id_group,
        "already_assigned":  already_assigned,
    }


async def remove_group_from_folder(
    db: AsyncSession,
    id_folder: int,
    id_group: int,
    *,
    actor_id: int,
    can_manage_all_folders: bool = False,
) -> None:
    """
    ZLECENIE FRONTU (0076, "Opcja A"): usuwa przypisanie PRZEZ SOFT-DELETE
    (removed_at = teraz), nie fizyczny DELETE. Przed oznaczeniem tworzy
    MIGAWKE aktualnej zawartosci teczki (skw_document_folder_snapshots) —
    to jest dane, ktore byli czlonkowie beda widziec przez (jeszcze
    niezaimplementowany) list_documents_in_folder_historical().

    Idempotentne — brak bledu jesli aktywne przypisanie nie istnieje.
    Dostep WYLACZNIE wlasciciel/administrator (punkt 2) — can_view_all
    NIE jest tu przyjmowane, swiadomie.
    """
    folder = await _get_folder_or_404(db, id_folder)
    access_via = await _ensure_group_management_access(
        db, folder, actor_id=actor_id, can_manage_all_folders=can_manage_all_folders,
    )

    result = await db.execute(
        select(DocumentFolderGroup).where(
            DocumentFolderGroup.id_folder == id_folder,
            DocumentFolderGroup.id_group == id_group,
            DocumentFolderGroup.removed_at.is_(None),
        )
    )
    assignment = result.scalar_one_or_none()
    if assignment is None:
        return  # idempotentne — juz usuniete albo nigdy nie istnialo

    # Migawka PRZED oznaczeniem removed_at — zawartosc TERAZ.
    items_result = await db.execute(
        text(
            f"SELECT [id_instance] FROM [{_SCHEMA}].[skw_document_folder_items] "
            f"WHERE [id_folder] = :f"
        ),
        {"f": id_folder},
    )
    instance_ids = [r[0] for r in items_result.fetchall()]

    now = datetime.utcnow()
    assignment.removed_at = now
    await db.flush()

    await db.execute(
        text(
            f"INSERT INTO [{_SCHEMA}].[skw_document_folder_snapshots] "
            f"([id_folder_group],[id_folder],[id_group],[snapshot_at],[instance_ids_json]) "
            f"VALUES (:fg,:f,:g,:now,:items)"
        ),
        {
            "fg": assignment.id_folder_group, "f": id_folder, "g": id_group,
            "now": now, "items": json.dumps(instance_ids),
        },
    )

    await _audit_log(
        db, actor_id=actor_id, action="document_folder.group_removed",
        entity_id=id_folder,
        details={"id_group": id_group, "snapshot_items_count": len(instance_ids)},
        access_via=access_via,
    )
    await db.commit()

    logger.info(
        "Grupa usunieta z teczki (soft-delete + migawka %d pozycji) | "
        "id_folder=%s id_group=%s actor=%s access_via=%s",
        len(instance_ids), id_folder, id_group, actor_id, access_via,
    )


async def add_groups_to_folder_bulk(
    db: AsyncSession,
    id_folder: int,
    id_groups: list[int],
    *,
    actor_id: int,
    can_manage_all_folders: bool,
) -> dict[str, Any]:
    """
    ZLECENIE FRONTU (0076, punkt 7): "nie wykonuje sie, zwraca blad do
    poprawy" + "bez limitu". Cala operacja w JEDNEJ transakcji — jesli
    KTORAKOLWIEK grupa z listy nie istnieje, ZERO zmian w bazie, blad
    wskazuje ktore id_group sa problemem. Brak MAX_ITEMS — jawna decyzja
    frontu, nie przeoczenie.

    Dostep: _ensure_group_management_access — wlasciciel/admin, NIE gosc.
    """
    folder = await _get_folder_or_404(db, id_folder)
    access_via = await _ensure_group_management_access(
        db, folder, actor_id=actor_id, can_manage_all_folders=can_manage_all_folders,
    )

    if not id_groups:
        raise FolderValidationError("Lista id_groups nie moze byc pusta.")

    missing: list[int] = []
    for gid in id_groups:
        exists = await db.execute(
            text(f"SELECT 1 FROM [{_SCHEMA}].[skw_approval_groups] WHERE [id_group] = :g"),
            {"g": gid},
        )
        if not exists.fetchone():
            missing.append(gid)
    if missing:
        raise GroupNotFoundError(
            f"Grupy nie istnieja: {missing}. Zadna zmiana nie zostala zapisana."
        )

    added: list[int] = []
    already: list[int] = []
    for gid in id_groups:
        existing = await db.execute(
            text(
                f"SELECT 1 FROM [{_SCHEMA}].[skw_document_folder_groups] "
                f"WHERE [id_folder] = :f AND [id_group] = :g AND [removed_at] IS NULL"
            ),
            {"f": id_folder, "g": gid},
        )
        if existing.fetchone():
            already.append(gid)
            continue
        db.add(DocumentFolderGroup(id_folder=id_folder, id_group=gid, added_by=actor_id))
        added.append(gid)

    await db.flush()
    await _audit_log(
        db, actor_id=actor_id, action="document_folder.groups_bulk_added",
        entity_id=id_folder,
        details={"added": added, "already_assigned": already},
        access_via=access_via,
    )
    await db.commit()

    logger.info(
        "Bulk dodanie grup do teczki | id_folder=%s added=%s already=%s actor=%s access_via=%s",
        id_folder, added, already, actor_id, access_via,
    )
    return {"id_folder": id_folder, "added": added, "already_assigned": already}


# =============================================================================
# Przypisywanie DOKUMENTOW do teczek
# =============================================================================

async def add_document_to_folder(
    db: AsyncSession,
    id_instance: int,
    id_folder: int,
    *,
    actor_id: int,
    can_view_all: bool,
    can_manage_shared_items: bool = False,
    can_manage_all_folders: bool = False,
) -> dict[str, Any]:
    """Dodaje dokument do teczki. Idempotentne."""
    folder = await _get_folder_or_404(db, id_folder)
    access_via = await _ensure_write_access(
        db, folder, actor_id=actor_id, can_view_all=can_view_all,
        can_manage_shared_items=can_manage_shared_items,
        can_manage_all_folders=can_manage_all_folders,
    )

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
            entity_id=id_folder, details={"id_instance": id_instance},
            access_via=access_via,
        )
        await db.commit()
        logger.info(
            "Dokument dodany do teczki | id_folder=%s id_instance=%s actor=%s access_via=%s",
            id_folder, id_instance, actor_id, access_via,
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
    can_manage_shared_items: bool = False,
    can_manage_all_folders: bool = False,
) -> None:
    """Usuwa dokument z teczki. Idempotentne."""
    folder = await _get_folder_or_404(db, id_folder)
    access_via = await _ensure_write_access(
        db, folder, actor_id=actor_id, can_view_all=can_view_all,
        can_manage_shared_items=can_manage_shared_items,
        can_manage_all_folders=can_manage_all_folders,
    )

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
        entity_id=id_folder, details={"id_instance": id_instance},
        access_via=access_via,
    )
    await db.commit()

    logger.info(
        "Dokument usuniety z teczki | id_folder=%s id_instance=%s actor=%s access_via=%s",
        id_folder, id_instance, actor_id, access_via,
    )


async def list_documents_in_folder(
    db: AsyncSession,
    id_folder: int,
    *,
    actor_id: int,
    can_view_all: bool,
    can_manage_all_folders: bool = False,
    page: int = 1,
    per_page: int = 50,
) -> dict[str, Any]:
    """
    Lista dokumentow (instancji obiegu) przypisanych do teczki — AKTUALNA
    zawartosc. Byli czlonkowie usunietej grupy wspoldzielonej NIE MAJA
    tu dostepu (patrz _is_shared_group_access, filtr removed_at) —
    korzystaja z (jeszcze niezaimplementowanego) widoku historycznego.
    """
    folder = await _get_folder_or_404(db, id_folder)
    await _ensure_access(
        db, folder, actor_id=actor_id, can_view_all=can_view_all,
        can_manage_all_folders=can_manage_all_folders,
    )

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


async def list_documents_in_folder_historical(
    db: AsyncSession,
    id_folder: int,
    id_group: int,
    *,
    actor_id: int,
) -> dict[str, Any]:
    """
    PLACEHOLDER. ZLECENIE FRONTU — Opcja A: zamrozona zawartosc teczki
    z MOMENTU usuniecia przypisania grupy (id_group), widoczna WYLACZNIE
    dla userow, ktorzy BYLI czlonkiem tej grupy W CHWILI removed_at.

    NIEROZWIAZANY PROBLEM (nie tylko brak implementacji): weryfikacja
    "byl czlonkiem W MOMENCIE removed_at" wymaga historii czlonkostwa w
    grupie, ktorej system DZIS NIE REJESTRUJE — skw_approval_group_members
    nie ma kolumn added_at/removed_at, tylko aktualny sklad. Migawka
    zawartosci teczki (skw_document_folder_snapshots, juz zaimplementowana
    w remove_group_from_folder) odpowiada na "co bylo w teczce", ale NIE
    na "kto mial prawo to widziec" — to jest odrebna, nierozstrzygnieta
    kwestia do zgloszenia frontowi PRZED implementacja tego endpointu.

    DO ZAIMPLEMENTOWANIA (po rozstrzygnieciu powyzszego):
      - znajdz skw_document_folder_groups (id_folder, id_group,
        removed_at IS NOT NULL) — MOZE BYC WIELE wpisow (grupa dodawana/
        usuwana wielokrotnie) — ktora migawke pokazac? NIEROZSTRZYGNIETE.
      - odczyt instance_ids_json z skw_document_folder_snapshots,
        JOIN do skw_document_approval_instances po id_instance
    """
    raise NotImplementedError(
        "list_documents_in_folder_historical: placeholder — wymaga "
        "rozstrzygniecia problemu historii czlonkostwa w grupie, patrz "
        "docstring (0076)"
    )


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
    access_via: AccessVia | None = None,
) -> None:
    """
    Zapisuje wpis do AuditLog. Blad zapisu nie przerywa operacji.

    access_via, jesli podane, jest zawsze dopisywane do details jako pole
    "access_via" — niezaleznie od tego, czy front prosil o to explicite.
    """
    full_details = dict(details)
    if access_via is not None:
        full_details["access_via"] = access_via

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
                "details": json.dumps(full_details, ensure_ascii=False, default=str),
            },
        )
    except Exception as exc:
        logger.error("_audit_log: blad zapisu dla action=%s: %s", action, exc)