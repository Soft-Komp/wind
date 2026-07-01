# backend/app/services/documents_service.py
"""
Serwis uniwersalnego widoku dokumentow — F6 (sekcje 4.14, 7.12).

Pokrywa logike dla:
  list_documents          — GET /documents (filtr widoczności restricted)
  get_status_summary      — GET /documents/{id}/status-summary
  get_available_actions   — GET /documents/{id}/actions/available
  list_unassigned         — GET /documents/unassigned
  list_duplicate_pending  — GET /documents/duplicate-pending
  resolve_duplicate       — POST /documents/{id}/duplicate-pending/resolve
  get_timeline            — GET /documents/{id}/timeline

Logika widocznosci (sekcja 4.14):
  - documents.view_all lub approval.supervise -> widzi WSZYSTKO
  - W przeciwnym razie: dokument jest widoczny gdy
      a) jego id_source nie ma ZADNEGO aktywnego filtru z visibility_mode='restricted'
      LUB
      b) ma taki filtr, ale uzytkownik (przez id_user) lub jedna z jego grup
         (przez id_group) jest wpisany w skw_approval_filter_visibility
         dla TEGO filtru.

  Filtr jest "dotyczacy" dokumentu gdy filter_type/warunki by go dopasowaly —
  ale dla widocznosci uzywamy uproszczenia: sprawdzamy WSZYSTKIE aktywne
  filtry restricted dla id_source dokumentu (niezaleznie od tego czy faktycznie
  dopasowuja warunki) — to jest bezpieczniejsze nadmiarowo (whitelist) niz
  przepuszczanie czegokolwiek przez przypadek.

UWAGA: from __future__ import annotations — NIGDY w tym pliku (SQLAlchemy ORM).
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_SCHEMA = "dbo"

_STATUS_DISPLAY = {
    "pending_dispatch":  "Nowy — czeka na przypisanie",
    "in_progress":       "W obiegu",
    "approved":          "Zaakceptowany",
    "cancelled":         "Anulowany",
    "rejected":          "Odrzucony",
    "unassigned":        "Nieprzypisany",
    "duplicate_pending":  "Mozliwy duplikat",
    "source_orphaned":   "Zniknal ze zrodla",
}


class DocumentNotFoundError(Exception):
    """Instancja obiegu o podanym ID nie istnieje."""


class DuplicateResolveError(Exception):
    """Blad przy rozstrzyganiu duplikatu (np. dokument nie jest w stanie duplicate_pending)."""


# =============================================================================
# GET /documents — lista z filtrem widoczności
# =============================================================================

async def list_documents(
    db: AsyncSession,
    *,
    actor_id: int,
    can_view_all: bool,
    page: int = 1,
    per_page: int = 50,
    id_source: int | None = None,
    id_folder: list[int] | None = None,
    id_category: int | None = None,
    status: str | None = None,
    search: str | None = None,
) -> dict[str, Any]:
    """
    Lista dokumentow (instancji obiegu) ze wszystkich zrodel, z filtrem widocznosci.

    id_folder: dopuszczalne wiele wartosci jednoczesnie (wielowymiarowosc teczek) —
    dokument widoczny jesli jest w KTOREJKOLWIEK z podanych teczek.
    """
    where: list[str] = []
    params: dict[str, Any] = {}

    if not can_view_all:
        visibility_clause = await _build_visibility_clause(db, actor_id)
        where.append(visibility_clause)

    if id_source is not None:
        where.append("i.[id_source] = :id_source")
        params["id_source"] = id_source
    if id_category is not None:
        where.append("i.[id_category] = :id_category")
        params["id_category"] = id_category
    if status is not None:
        where.append("i.[status] = :status")
        params["status"] = status
    if id_folder:
        ph = ",".join(f":folder_{j}" for j in range(len(id_folder)))
        where.append(
            f"i.[id_instance] IN ("
            f"  SELECT [id_instance] FROM [{_SCHEMA}].[skw_document_folder_items] "
            f"  WHERE [id_folder] IN ({ph})"
            f")"
        )
        for j, fid in enumerate(id_folder):
            params[f"folder_{j}"] = fid
    if search:
        safe_search = search.replace("'", "''")[:100]
        where.append(
            "(i.[document_title] LIKE :search "
            " OR i.[id_document] LIKE :search)"
        )
        params["search"] = f"%{safe_search}%"

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    count_result = await db.execute(
        text(f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_document_approval_instances] i {where_sql}"),
        params,
    )
    total = count_result.scalar() or 0

    params["offset"] = (page - 1) * per_page
    params["limit"] = per_page

    result = await db.execute(
        text(f"""
            SELECT
                i.[id_instance], i.[id_source], i.[id_document], i.[status],
                i.[document_title], i.[document_amount], i.[is_urgent],
                i.[created_at], i.[updated_at],
                s.[source_name]
            FROM [{_SCHEMA}].[skw_document_approval_instances] i
            JOIN [{_SCHEMA}].[skw_document_sources] s ON s.[id_source] = i.[id_source]
            {where_sql}
            ORDER BY i.[is_urgent] DESC, i.[created_at] DESC
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
        """),
        params,
    )
    cols = list(result.keys())
    items = []
    for row in result.fetchall():
        r = dict(zip(cols, row))
        r["status_display"] = _STATUS_DISPLAY.get(r["status"], r["status"])
        items.append(r)

    return {"items": items, "total": total, "page": page, "per_page": per_page}


async def _build_visibility_clause(db: AsyncSession, actor_id: int) -> str:
    """
    Buduje fragment WHERE ograniczajacy widocznosc do dokumentow
    nieobjetych restricted filtrami (lub objetych, ale z dostepem).

    Logika: dokument jest WIDOCZNY gdy:
      NOT EXISTS aktywny filtr restricted dla i.id_source
      OR
      EXISTS taki filtr ALE actor ma wpis w filter_visibility (user lub jedna z jego grup)
    """
    # Pobierz grupy actora raz — uzyte jako subquery
    return (
        f"NOT EXISTS ("
        f"    SELECT 1 FROM [{_SCHEMA}].[skw_approval_filters] f "
        f"    WHERE f.[id_source] = i.[id_source] "
        f"      AND f.[is_active] = 1 "
        f"      AND f.[visibility_mode] = N'restricted' "
        f"      AND NOT EXISTS ("
        f"          SELECT 1 FROM [{_SCHEMA}].[skw_approval_filter_visibility] v "
        f"          WHERE v.[id_filter] = f.[id_filter] "
        f"            AND ("
        f"                v.[id_user] = {actor_id} "
        f"                OR v.[id_group] IN ("
        f"                    SELECT [id_group] FROM [{_SCHEMA}].[skw_approval_group_members] "
        f"                    WHERE [id_user] = {actor_id}"
        f"                )"
        f"            )"
        f"      )"
        f")"
    )


# =============================================================================
# GET /documents/{id}/status-summary
# =============================================================================

async def get_status_summary(
    db: AsyncSession,
    id_instance: int,
    *,
    actor_id: int,
    can_view_all: bool,
) -> dict[str, Any]:
    """
    Kompletny stan dokumentu — eliminuje potrzebe 3-4 osobnych requestow.

    Zawiera: status, etap obiegu, nazwa grupy biezacego kroku, deadline,
    lista dostepnych akcji (obiegowych + zrodlowych), pilnosc, teczki.
    """
    instance = await _get_instance_or_404(db, id_instance)
    await _ensure_visibility(db, instance, actor_id=actor_id, can_view_all=can_view_all)

    # Biezacy krok obiegu (jesli in_progress)
    current_step_info = None
    if instance["status"] == "in_progress" and instance["current_step"]:
        step_result = await db.execute(
            text(f"""
                SELECT s.[step_order], s.[id_group], g.[group_name],
                       s.[deadline_at], s.[votes_required], s.[votes_cast]
                FROM [{_SCHEMA}].[skw_document_approval_snapshot_steps] s
                LEFT JOIN [{_SCHEMA}].[skw_approval_groups] g ON g.[id_group] = s.[id_group]
                WHERE s.[id_instance] = :i AND s.[step_order] = :step
            """),
            {"i": id_instance, "step": instance["current_step"]},
        )
        row = step_result.fetchone()
        if row:
            current_step_info = {
                "step_order":      row[0],
                "id_group":        row[1],
                "group_name":      row[2],
                "deadline_at":     row[3],
                "votes_required":  row[4],
                "votes_cast":      row[5],
            }

    # Akcje zrodlowe dostepne (bez weryfikacji required_permission — to robi /actions/available)
    actions_result = await db.execute(
        text(f"""
            SELECT COUNT(*) FROM [{_SCHEMA}].[skw_source_actions]
            WHERE [id_source] = :s AND [is_active] = 1
        """),
        {"s": instance["id_source"]},
    )
    available_actions_count = actions_result.scalar() or 0

    # Teczki zawierajace ten dokument
    folders_result = await db.execute(
        text(f"""
            SELECT f.[id_folder], f.[folder_name], f.[color]
            FROM [{_SCHEMA}].[skw_document_folder_items] fi
            JOIN [{_SCHEMA}].[skw_document_folders] f ON f.[id_folder] = fi.[id_folder]
            WHERE fi.[id_instance] = :i
        """),
        {"i": id_instance},
    )
    folders = [{"id_folder": r[0], "folder_name": r[1], "color": r[2]} for r in folders_result.fetchall()]

    return {
        "id_instance":             id_instance,
        "id_document":             instance["id_document"],
        "status":                  instance["status"],
        "status_display":          _STATUS_DISPLAY.get(instance["status"], instance["status"]),
        "document_title":          instance["document_title"],
        "document_amount":         instance["document_amount"],
        "is_urgent":               instance["is_urgent"],
        "current_step":            current_step_info,
        "available_actions_count": available_actions_count,
        "folders":                 folders,
        "created_at":              instance["created_at"],
        "updated_at":              instance["updated_at"],
    }


# =============================================================================
# GET /documents/{id}/actions/available
# =============================================================================

async def get_available_actions(
    db: AsyncSession,
    id_instance: int,
    *,
    actor_id: int,
    can_view_all: bool,
) -> list[dict[str, Any]]:
    """
    Lista akcji zrodlowych dostepnych dla zalogowanego uzytkownika,
    z uwzglednieniem required_permission. Frontend renderuje przyciski
    na podstawie tej listy.
    """
    instance = await _get_instance_or_404(db, id_instance)
    await _ensure_visibility(db, instance, actor_id=actor_id, can_view_all=can_view_all)

    result = await db.execute(
        text(f"""
            SELECT a.[id_action], a.[action_name], a.[action_label],
                   a.[required_permission], a.[sort_order]
            FROM [{_SCHEMA}].[skw_source_actions] a
            WHERE a.[id_source] = :s AND a.[is_active] = 1
            ORDER BY a.[sort_order] ASC, a.[id_action] ASC
        """),
        {"s": instance["id_source"]},
    )

    actions = []
    for id_action, action_name, action_label, required_perm, sort_order in result.fetchall():
        has_permission = True
        if required_perm:
            has_permission = await _check_user_permission(db, actor_id, required_perm)
        actions.append({
            "id_action":     id_action,
            "action_name":   action_name,
            "action_label":  action_label,
            "available":     has_permission,
            "sort_order":    sort_order,
        })

    return actions


# =============================================================================
# GET /documents/unassigned
# =============================================================================

async def list_unassigned(
    db: AsyncSession,
    *,
    actor_id: int,
    can_view_all: bool,
    page: int = 1,
    per_page: int = 50,
) -> dict[str, Any]:
    """Lista dokumentow status=unassigned z licznikiem (badge w nawigacji)."""
    return await list_documents(
        db, actor_id=actor_id, can_view_all=can_view_all,
        page=page, per_page=per_page, status="unassigned",
    )


# =============================================================================
# GET /documents/duplicate-pending + POST resolve
# =============================================================================

async def list_duplicate_pending(
    db: AsyncSession,
    *,
    actor_id: int,
    can_view_all: bool,
    page: int = 1,
    per_page: int = 50,
) -> dict[str, Any]:
    """Lista potencjalnych duplikatow czekajacych na potwierdzenie referenta."""
    return await list_documents(
        db, actor_id=actor_id, can_view_all=can_view_all,
        page=page, per_page=per_page, status="duplicate_pending",
    )


async def resolve_duplicate(
    db: AsyncSession,
    id_instance: int,
    *,
    decision: str,
    actor_id: int,
    can_view_all: bool,
) -> dict[str, Any]:
    """
    Rozstrzyga duplikat.

    decision='confirm': to faktycznie duplikat -> status=cancelled + adnotacja w extra_data
    decision='dismiss': to NIE duplikat -> status=pending_dispatch, wpuszcza normalnie do obiegu

    Raises:
        DuplicateResolveError: instancja nie jest w stanie duplicate_pending.
    """
    instance = await _get_instance_or_404(db, id_instance)
    await _ensure_visibility(db, instance, actor_id=actor_id, can_view_all=can_view_all)

    if instance["status"] != "duplicate_pending":
        raise DuplicateResolveError(
            f"Instancja ID={id_instance} ma status='{instance['status']}', "
            f"oczekiwano 'duplicate_pending'."
        )

    extra: dict = {}
    if instance.get("extra_data"):
        try:
            extra = json.loads(instance["extra_data"])
        except Exception:
            pass

    now = datetime.now(timezone.utc).replace(tzinfo=None)

    if decision == "confirm":
        extra["duplicate_resolution"] = "confirmed"
        extra["duplicate_resolved_by"] = actor_id
        extra["duplicate_resolved_at"] = now.isoformat()
        new_status = "cancelled"
    elif decision == "dismiss":
        extra["duplicate_resolution"] = "dismissed"
        extra["duplicate_resolved_by"] = actor_id
        extra["duplicate_resolved_at"] = now.isoformat()
        new_status = "pending_dispatch"
    else:
        raise DuplicateResolveError(f"decision='{decision}' nieprawidlowa. Dozwolone: confirm, dismiss.")

    await db.execute(
        text(f"""
            UPDATE [{_SCHEMA}].[skw_document_approval_instances]
            SET [status] = :status, [extra_data] = :extra, [updated_at] = SYSUTCDATETIME()
            WHERE [id_instance] = :i
        """),
        {
            "status": new_status,
            "extra":  json.dumps(extra, ensure_ascii=False, default=str),
            "i":      id_instance,
        },
    )

    try:
        await db.execute(
            text(
                f"INSERT INTO [{_SCHEMA}].[skw_AuditLog] "
                f"([ID_USER], [Action], [EntityType], [EntityID], [NewValue], [Success], [Timestamp]) "
                f"VALUES (:uid, N'document.duplicate_resolved', N'DocumentApprovalInstance', :eid, :details, 1, SYSUTCDATETIME())"
            ),
            {
                "uid":     actor_id,
                "eid":     str(id_instance),
                "details": json.dumps({"decision": decision, "new_status": new_status}, ensure_ascii=False),
            },
        )
    except Exception as exc:
        logger.error("resolve_duplicate: blad zapisu AuditLog: %s", exc)

    await db.commit()

    logger.info(
        "Duplikat rozstrzygniety | id_instance=%s decision=%s new_status=%s actor=%s",
        id_instance, decision, new_status, actor_id,
    )

    return {"id_instance": id_instance, "decision": decision, "status": new_status}


# =============================================================================
# GET /documents/{id}/timeline
# =============================================================================

async def get_timeline(
    db: AsyncSession,
    id_instance: int,
    *,
    actor_id: int,
    can_view_all: bool,
) -> list[dict[str, Any]]:
    """
    Zunifikowana os czasu: zdarzenia obiegu (approval_log) + komentarze
    (approval_comments) posortowane chronologicznie.
    """
    instance = await _get_instance_or_404(db, id_instance)
    await _ensure_visibility(db, instance, actor_id=actor_id, can_view_all=can_view_all)

    timeline: list[dict[str, Any]] = []

    log_result = await db.execute(
        text(f"""
            SELECT al.[id_log], al.[action], al.[username_snapshot],
                   al.[logged_at], al.[details]
            FROM [{_SCHEMA}].[skw_approval_log] al
            WHERE al.[id_instance] = :i AND al.[is_voided] = 0
            ORDER BY al.[logged_at] ASC
        """),
        {"i": id_instance},
    )
    for id_log, action, username, logged_at, details in log_result.fetchall():
        timeline.append({
            "type":      "approval_log",
            "id":        id_log,
            "action":    action,
            "actor":     username,
            "timestamp": logged_at,
            "details":   details,
        })

    try:
        comments_result = await db.execute(
            text(f"""
                SELECT c.[id_comment], u.[Username], c.[content], c.[created_at]
                FROM [{_SCHEMA}].[skw_approval_comments] c
                LEFT JOIN [{_SCHEMA}].[skw_Users] u ON u.[ID_USER] = c.[id_user]
                WHERE c.[id_instance] = :i AND c.[is_deleted] = 0
                ORDER BY c.[created_at] ASC
            """),
            {"i": id_instance},
        )
        for id_comment, username, content, created_at in comments_result.fetchall():
            timeline.append({
                "type":      "comment",
                "id":        id_comment,
                "actor":     username,
                "content":   content,
                "timestamp": created_at,
            })
    except Exception as exc:
        logger.warning("get_timeline: blad pobierania komentarzy (modul moze byc wylaczony): %s", exc)

    timeline.sort(key=lambda x: x["timestamp"] or datetime.min)
    return timeline


# =============================================================================
# Pomocnicze
# =============================================================================

async def _get_instance_or_404(db: AsyncSession, id_instance: int) -> dict[str, Any]:
    result = await db.execute(
        text(f"""
            SELECT [id_instance], [id_source], [id_document], [id_category],
                   [status], [current_step], [document_title], [document_amount],
                   [extra_data], [is_urgent], [created_at], [updated_at]
            FROM [{_SCHEMA}].[skw_document_approval_instances]
            WHERE [id_instance] = :i
        """),
        {"i": id_instance},
    )
    cols = list(result.keys())
    row = result.fetchone()
    if row is None:
        raise DocumentNotFoundError(f"Dokument (instancja obiegu) ID={id_instance} nie istnieje.")
    return dict(zip(cols, row))


async def _ensure_visibility(
    db: AsyncSession,
    instance: dict[str, Any],
    *,
    actor_id: int,
    can_view_all: bool,
) -> None:
    """Rzuca HTTPException(403) jesli dokument jest objety restricted filtrem bez dostepu."""
    if can_view_all:
        return

    result = await db.execute(
        text(f"""
            SELECT 1
            FROM [{_SCHEMA}].[skw_approval_filters] f
            WHERE f.[id_source] = :s
              AND f.[is_active] = 1
              AND f.[visibility_mode] = N'restricted'
              AND NOT EXISTS (
                  SELECT 1 FROM [{_SCHEMA}].[skw_approval_filter_visibility] v
                  WHERE v.[id_filter] = f.[id_filter]
                    AND (
                        v.[id_user] = :uid
                        OR v.[id_group] IN (
                            SELECT [id_group] FROM [{_SCHEMA}].[skw_approval_group_members]
                            WHERE [id_user] = :uid
                        )
                    )
              )
        """),
        {"s": instance["id_source"], "uid": actor_id},
    )
    if result.fetchone():
        raise HTTPException(
            status_code=403,
            detail=f"Brak dostepu do dokumentu ID={instance['id_instance']} (filtr widocznosci restricted).",
        )


async def _check_user_permission(db: AsyncSession, actor_id: int, permission_name: str) -> bool:
    result = await db.execute(
        text(f"""
            SELECT COUNT(*)
            FROM [{_SCHEMA}].[skw_UserRoles] ur
            JOIN [{_SCHEMA}].[skw_RolePermissions] rp ON rp.[ID_ROLE] = ur.[ID_ROLE]
            JOIN [{_SCHEMA}].[skw_Permissions] p ON p.[ID_PERMISSION] = rp.[ID_PERMISSION]
            WHERE ur.[ID_USER] = :u AND p.[PermissionName] = :perm AND p.[IsActive] = 1
        """),
        {"u": actor_id, "perm": permission_name},
    )
    return (result.scalar() or 0) > 0