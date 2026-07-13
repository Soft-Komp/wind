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
from datetime import date, datetime, timezone
from typing import Any
import os
import uuid

from pathlib import Path
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.event_service import _build_event_envelope, _append_event_to_log, _publish_to_channel

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

# Whitelist sortowania — analogiczna do faktura_akceptacja_service.get_faktury_list_new.
# NIGDY nie interpoluj order_by bezposrednio do SQL — tylko przez ten slownik.
_DOCUMENTS_SORT_MAP: dict[str, str] = {
    "created_at":      "i.[created_at]",
    "updated_at":       "i.[updated_at]",
    "document_title":   "i.[document_title]",
    "document_amount":  "i.[document_amount]",
    "status":           "i.[status]",
    "priority":         "i.[priority]",
}


async def list_documents(
    db: AsyncSession,
    *,
    actor_id: int,
    can_view_all: bool,
    accessible_source_ids: list[int] | None = None,
    page: int = 1,
    per_page: int = 50,
    id_source: int | None = None,
    id_folder: list[int] | None = None,
    id_category: int | None = None,
    status: str | None = None,
    search: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    priority: int | None = None,
    order_by: str = "created_at",
    order_dir: str = "desc",
) -> dict[str, Any]:
    """
    Lista dokumentow z filtrem widocznosci (poziom 2) i filtrem dostepu do zrodla (poziom 1).

    accessible_source_ids: None = brak filtru (supervisor), [] = brak dostepu, [1,2] = filtr.
    search: szuka po document_title, id_document ORAZ extra_data.ocr_text (TODO-06/F7).
    date_from/date_to: filtr po i.created_at (data wejscia dokumentu do systemu,
        NIE data samego dokumentu zrodlowego — skw_document_approval_instances
        nie ma osobnej kolumny na date dokumentu).
    order_by/order_dir: sortowanie przez whitelist _DOCUMENTS_SORT_MAP — wartosc
        spoza listy cicho spada na domyslne 'created_at' (bez bledu, zgodnie
        z istniejacym wzorcem w projekcie).
    """
    where: list[str] = []
    params: dict[str, Any] = {}

    # Poziom 1 — filtr dostępu do źródeł (skw_source_role_access)
    if not can_view_all and accessible_source_ids is not None:
        if len(accessible_source_ids) == 0:
            # Brak dostępu do żadnego źródła — zwróć pustą listę
            return {"items": [], "total": 0, "page": page, "per_page": per_page}
        ph = ",".join(f":src_{j}" for j in range(len(accessible_source_ids)))
        where.append(f"i.[id_source] IN ({ph})")
        for j, sid in enumerate(accessible_source_ids):
            params[f"src_{j}"] = sid

    # Poziom 2 — filtr widoczności (skw_approval_filter_visibility)
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
    if priority is not None:
        where.append("i.[priority] = :priority")
        params["priority"] = priority
    if date_from is not None:
        where.append("i.[created_at] >= :date_from")
        params["date_from"] = date_from
    if date_to is not None:
        where.append("i.[created_at] <= :date_to_end")
        params["date_to_end"] = f"{date_to} 23:59:59"
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
        # Wyszukiwanie po tytule, numerze dokumentu ORAZ po tekście OCR (F7)
        where.append(
            "(i.[document_title] LIKE :search "
            " OR i.[id_document] LIKE :search"
            " OR JSON_VALUE(i.[extra_data], '$.ocr_text') LIKE :search)"
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

    sort_col = _DOCUMENTS_SORT_MAP.get(order_by, "i.[created_at]")
    sort_dir_sql = "ASC" if order_dir.lower() == "asc" else "DESC"

    result = await db.execute(
        text(f"""
            SELECT
                i.[id_instance], i.[id_source], i.[id_document], i.[status],
                i.[document_title], i.[document_amount], i.[is_urgent],
                i.[created_at], i.[updated_at],
                s.[source_name],
                p.[path_name],
                g.[group_name] AS current_group_name
            FROM [{_SCHEMA}].[skw_document_approval_instances] i
            JOIN [{_SCHEMA}].[skw_document_sources] s
              ON s.[id_source] = i.[id_source]
            LEFT JOIN [{_SCHEMA}].[skw_approval_paths] p
              ON p.[id_path] = i.[id_path]
            LEFT JOIN [{_SCHEMA}].[skw_document_approval_snapshot_steps] ss
              ON ss.[id_instance] = i.[id_instance]
             AND ss.[step_order]  = i.[current_step]
            LEFT JOIN [{_SCHEMA}].[skw_approval_groups] g
              ON g.[id_group] = ss.[id_group]
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
        if r.get("document_amount") is not None:
            r["document_amount"] = float(r["document_amount"])
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

    ocr_data = None
    try:
        extra = json.loads(instance.get("extra_data") or "{}")
    except Exception:
        extra = {}
    if "ocr_confidence" in extra or "ocr_requires_review" in extra or "ocr_error" in extra:
        ocr_data = {
            "requires_review": extra.get("ocr_requires_review", False),
            "review_reasons":  extra.get("ocr_review_reasons", []),
            "confidence":      extra.get("ocr_confidence"),
            "doc_number":      extra.get("ocr_doc_number"),
            "nip":             extra.get("ocr_nip"),
            "doc_date":        extra.get("ocr_doc_date"),
            "amount_gross":    extra.get("ocr_amount_gross"),
            "contractor":      extra.get("ocr_contractor"),
            "pages":           extra.get("ocr_pages"),
            "error":           extra.get("ocr_error"),
            "raw_text":        extra.get("ocr_text"),
        }

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
        "ocr_data":                ocr_data,
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

_ALLOWED_UPLOAD_MIME = frozenset({
    "application/pdf",
    "image/png",
    "image/jpeg",
    "image/tiff",
    "image/bmp",
})


class OcrReviewStateError(Exception):
    """Instancja nie jest w stanie ocr_review_pending — nie mozna rozstrzygnac."""


async def upload_document(
    db: AsyncSession,
    redis: Any,
    *,
    file: Any,
    actor_id: int,
) -> dict[str, Any]:
    """
    Reczne wgranie dokumentu PDF -> nowa instancja (status=ocr_review_pending),
    kolejkowanie OCR w tle. Wymaga istniejacego zrodla 'manual_upload'
    (zakladane migracja 0053).
    """
    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=422, detail="Plik jest pusty.")

    max_mb_raw = await redis.get("syscfg:APPROVAL_MAX_ATTACHMENT_MB")
    max_mb = int(max_mb_raw.decode() if isinstance(max_mb_raw, bytes) else max_mb_raw) if max_mb_raw else 20
    if len(content) > max_mb * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"Plik za duzy. Max: {max_mb} MB.")

    try:
        import magic
        detected_mime = magic.from_buffer(content, mime=True)
    except ImportError:
        detected_mime = getattr(file, "content_type", None) or "application/octet-stream"

    if detected_mime not in _ALLOWED_UPLOAD_MIME:
        raise HTTPException(
            status_code=415,
            detail=f"Dozwolone sa wylacznie pliki PDF. Wykryto: {detected_mime}.",
        )

    src_row = (await db.execute(
        text(f"SELECT [id_source] FROM [{_SCHEMA}].[skw_document_sources] WHERE [source_name]=N'manual_upload'")
    )).fetchone()
    if not src_row:
        raise HTTPException(
            status_code=500,
            detail="Brak skonfigurowanego zrodla 'manual_upload'. Uruchom migracje 0053 lub skontaktuj sie z administratorem.",
        )
    id_source = src_row[0]

    def _sanitize_filename(name: str) -> str:
        import re as _re
        base = Path(name).stem
        ext  = Path(name).suffix.lower()[:10]
        safe = _re.sub(r"[^a-zA-Z0-9._\-]", "_", base)[:100]
        return f"{safe}{ext}" or f"file{ext}"

    uploads_dir = Path(os.environ.get("MANUAL_UPLOADS_DIR", "/data/manual_uploads"))
    uploads_dir.mkdir(parents=True, exist_ok=True)
    safe_name = _sanitize_filename(getattr(file, "filename", None) or "dokument.pdf")
    file_path = uploads_dir / f"{uuid.uuid4().hex}_{safe_name}"
    with open(file_path, "wb") as f:
        f.write(content)

    id_document = f"manual_{uuid.uuid4().hex}"
    now = datetime.now(timezone.utc)
    extra_data = {
        "file_path": str(file_path),
        "uploaded_by": actor_id,
        "original_filename": getattr(file, "filename", None),
    }

    insert_result = await db.execute(
        text(f"""
            INSERT INTO [{_SCHEMA}].[skw_document_approval_instances]
                ([id_source],[id_document],[status],[document_title],[document_amount],
                 [extra_data],[dispatch_attempts],[created_at],[updated_at])
            OUTPUT INSERTED.[id_instance]
            VALUES (:src,:doc,N'ocr_review_pending',:title,NULL,:extra,0,:now,:now)
        """),
        {
            "src": id_source, "doc": id_document,
            "title": safe_name, "extra": json.dumps(extra_data, ensure_ascii=False),
            "now": now,
        },
    )
    id_instance = insert_result.scalar_one()
    await db.commit()

    ocr_queued = False
    try:
        from app.core.arq_pool import get_arq_pool
        arq_pool = get_arq_pool()
        await arq_pool.enqueue_job("ocr_task", id_instance=id_instance, file_path=str(file_path))
        ocr_queued = True
    except Exception as exc:
        logger.error("upload_document: blad enqueue ocr_task dla id_instance=%s: %s", id_instance, exc)

    await _audit_log(
        db, actor_id=actor_id, action="document.manual_upload",
        entity_id=id_instance,
        details={"original_filename": getattr(file, "filename", None), "ocr_queued": ocr_queued},
    )
    await db.commit()

    logger.info(
        "upload_document: nowy dokument | id_instance=%s file=%s ocr_queued=%s actor=%s",
        id_instance, safe_name, ocr_queued, actor_id,
    )

    return {
        "id_instance":  id_instance,
        "id_document":  id_document,
        "status":       "ocr_review_pending",
        "ocr_queued":   ocr_queued,
        "message": (
            "Dokument przyjety. Trwa automatyczne rozpoznawanie danych (OCR) w tle. "
            "Sprawdz status przez GET /documents/{id}/status-summary za chwile."
            if ocr_queued else
            "Dokument przyjety, ale nie udalo sie zakolejkowac OCR — wymaga recznej weryfikacji."
        ),
    }


async def resolve_ocr_review(
    db: AsyncSession,
    id_instance: int,
    *,
    decision: str,
    document_title: str | None,
    document_amount: float | None,
    comment: str | None,
    actor_id: int,
    can_view_all: bool,
    redis: Any = None,
) -> dict[str, Any]:
    """
    Rozstrzyga dokument oczekujacy na reczna weryfikacje OCR.

    decision='confirm': operator potwierdza/poprawia pola -> status=pending_dispatch
                          (wchodzi w normalny automatyczny obieg przez auto_dispatch_task)
    decision='reject':   dokument odrzucony -> status=cancelled
    """
    instance = await _get_instance_or_404(db, id_instance)
    await _ensure_visibility(db, instance, actor_id=actor_id, can_view_all=can_view_all)

    if instance["status"] != "ocr_review_pending":
        raise OcrReviewStateError(
            f"Instancja ID={id_instance} nie jest w stanie ocr_review_pending "
            f"(aktualnie: {instance['status']})."
        )

    now = datetime.now(timezone.utc)

    if decision == "confirm":
        set_clauses = ["[status]=N'pending_dispatch'", "[updated_at]=:now"]
        params: dict[str, Any] = {"i": id_instance, "now": now}
        if document_title is not None:
            set_clauses.append("[document_title]=:title")
            params["title"] = document_title[:500]
        if document_amount is not None:
            set_clauses.append("[document_amount]=:amount")
            params["amount"] = document_amount
        await db.execute(
            text(f"""
                UPDATE [{_SCHEMA}].[skw_document_approval_instances]
                SET {", ".join(set_clauses)}
                WHERE [id_instance]=:i
            """),
            params,
        )
        new_status = "pending_dispatch"
    else:
        await db.execute(
            text(f"""
                UPDATE [{_SCHEMA}].[skw_document_approval_instances]
                SET [status]=N'cancelled', [updated_at]=:now
                WHERE [id_instance]=:i
            """),
            {"i": id_instance, "now": now},
        )
        new_status = "cancelled"

    await _audit_log(
        db, actor_id=actor_id, action="document.ocr_review_resolved",
        entity_id=id_instance,
        details={"decision": decision, "comment": comment},
    )
    await db.commit()

    logger.info(
        "resolve_ocr_review: id_instance=%s decision=%s new_status=%s actor=%s",
        id_instance, decision, new_status, actor_id,
    )

    # SSE — tylko przy potwierdzeniu (odrzucenie to nie "weryfikacja").
    if decision == "confirm" and redis is not None:
        try:
            uploaded_by = None
            try:
                extra = json.loads(instance.get("extra_data") or "{}")
                uploaded_by = extra.get("uploaded_by")
            except Exception:
                pass

            if uploaded_by:
                envelope = _build_event_envelope(
                    "document_ocr_verified",
                    {
                        "instance_id":         id_instance,
                        "confidence":          None,
                        "verified_by":         "human",
                        "verified_by_user_id": actor_id,
                    },
                    actor_id,
                )
                _append_event_to_log(envelope)
                await _publish_to_channel(redis, f"channel:user:{uploaded_by}", envelope)
                await _publish_to_channel(redis, "channel:admins", envelope)
        except Exception as exc:
            logger.warning("resolve_ocr_review: SSE publish blad | id_instance=%s: %s", id_instance, exc)

    return {"id_instance": id_instance, "status": new_status, "decision": decision}

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
                f"VALUES (:uid, :action, N'DocumentInstance', :eid, :details, 1, SYSUTCDATETIME())"
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