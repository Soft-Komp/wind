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

import hashlib
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
from app.services.duplicate_detection_service import DuplicateDetectionService
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
    id_path: list[int] | None = None,
    path_name: str | None = None,
    filter_mode: str = "AND",
    order_by: str = "created_at",
    order_dir: str = "desc",
    include_superseded: bool = False,
    include_resolved_duplicates: bool = False,
    include_duplicate_pending: bool = False,
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

    id_path: filtr po ID sciezki decyzyjnej — wiele wartosci = IN (...).
    path_name: filtr tekstowy LIKE po nazwie sciezki (np. 'firanki'), realizowany
        przez subquery na skw_approval_paths (a NIE przez JOIN w glownym query),
        zeby nie modyfikowac zapytania COUNT(*) i uniknac ryzyka duplikacji
        wierszy przy ewentualnej przyszlej zmianie kardynalnosci relacji.

    filter_mode: 'AND' (domyslnie, pelna wsteczna kompatybilnosc) lub 'OR'.
        Dotyczy WYLACZNIE grupy filtrow standardowych: id_source, id_category,
        status, priority, id_folder, id_path, path_name.

        BEZPIECZENSTWO — KRYTYCZNE: filtry poziomu 1 (dostep do zrodel) i
        poziomu 2 (widocznosc restricted) sa budowane w OSOBNEJ liscie
        (`mandatory_where`) i ZAWSZE laczone przez AND, niezaleznie od
        filter_mode. Rowniez search/date_from/date_to zostaja w AND — OR
        na wyszukiwaniu pelnotekstowym wzglendem filtrow rownosciowych
        prowadzi do zapytan zwracajacych nieoczekiwanie szeroki zakres
        wynikow. NIE przenosic tych warunkow do grupy OR bez swiadomej,
        oddzielnej decyzji projektowej.
    """
    filter_mode_clean = filter_mode.strip().upper()
    if filter_mode_clean not in ("AND", "OR"):
        # Redundantna walidacja — Query(pattern=...) w routerze juz to lapie,
        # ale funkcja serwisowa moze byc wywolana i z innych miejsc (np. eksport,
        # zadania w tle) — nigdy nie ufamy samemu warstwy API.
        logger.warning(
            "list_documents: niedozwolone filter_mode=%r — fallback na 'AND'",
            filter_mode,
        )
        filter_mode_clean = "AND"

    mandatory_where: list[str] = []
    group_where: list[str] = []
    params: dict[str, Any] = {}

    # WARIANT A (2026-07-22) — domyslnie ukrywamy instancje zastapione
    # (superseded_by_instance_id NOT NULL), np. stara cancelled instancja
    # po procedurze cancel+redispatch dla unassigned. ZAWSZE w mandatory_where
    # — nigdy nie wchodzi do grupy OR, analogicznie do filtrow bezpieczenstwa.
    if not include_superseded:
        mandatory_where.append("i.[superseded_by_instance_id] IS NULL")

    # NOWE (2026-07-28, na wniosek frontu) — domyslnie ukrywamy dokumenty
    # cancelled, ktore byly rozstrzygnietymi duplikatami (matched_instance_id
    # IS NOT NULL). SWIADOMIE nie wszystkie 'cancelled' — dokument mogl
    # zostac anulowany z zupelnie innego powodu, niezwiazanego z duplikatami,
    # i taki NIE powinien znikac z listy. Zero zmian w schemacie: obie
    # kolumny (status, matched_instance_id) istnieja od migracji 0068.
    # Dane fizycznie NIE sa usuwane — to wylacznie filtr widocznosci,
    # analogiczny do include_superseded powyzej. ZAWSZE w mandatory_where.
    if not include_resolved_duplicates:
        mandatory_where.append(
            "NOT (i.[status] = N'cancelled' AND i.[matched_instance_id] IS NOT NULL)"
        )

    # NOWE (2026-07-28, na wniosek frontu) — domyslnie ukrywamy TAKZE
    # dokumenty jeszcze NIEROZSTRZYGNIETE (status='duplicate_pending'),
    # nie tylko juz anulowane duplikaty. Dane pozostaja w systemie
    # (wymog: musza tam byc), znika wylacznie z listy ogolnej. Ekran
    # rozstrzygania duplikatow (list_duplicate_pending ponizej) MUSI
    # przekazywac include_duplicate_pending=True, inaczej wlasny filtr
    # status='duplicate_pending' zderzy sie z tym wykluczeniem i zwroci
    # zero wynikow — nie przeoczyc tego przy jakiejkolwiek dalszej zmianie.
    if not include_duplicate_pending:
        mandatory_where.append("i.[status] <> N'duplicate_pending'")

    # Poziom 1 — filtr dostępu do źródeł (skw_source_role_access)
    # ZAWSZE w mandatory_where — nigdy nie wchodzi do grupy OR.
    if not can_view_all and accessible_source_ids is not None:
        if len(accessible_source_ids) == 0:
            # Brak dostępu do żadnego źródła — zwróć pustą listę
            return {"items": [], "total": 0, "page": page, "per_page": per_page}
        ph = ",".join(f":src_{j}" for j in range(len(accessible_source_ids)))
        mandatory_where.append(f"i.[id_source] IN ({ph})")
        for j, sid in enumerate(accessible_source_ids):
            params[f"src_{j}"] = sid

    # Poziom 2 — filtr widoczności (skw_approval_filter_visibility)
    # ZAWSZE w mandatory_where — nigdy nie wchodzi do grupy OR.
    if not can_view_all:
        visibility_clause = await _build_visibility_clause(db, actor_id)
        mandatory_where.append(visibility_clause)

    # --- Grupa filtrow standardowych — podlega filter_mode (AND/OR) ---
    if id_source is not None:
        group_where.append("i.[id_source] = :id_source")
        params["id_source"] = id_source
    if id_category is not None:
        group_where.append("i.[id_category] = :id_category")
        params["id_category"] = id_category
    if status is not None:
        group_where.append("i.[status] = :status")
        params["status"] = status
    if priority is not None:
        group_where.append("i.[priority] = :priority")
        params["priority"] = priority
    if id_folder:
        ph = ",".join(f":folder_{j}" for j in range(len(id_folder)))
        group_where.append(
            f"i.[id_instance] IN ("
            f"  SELECT [id_instance] FROM [{_SCHEMA}].[skw_document_folder_items] "
            f"  WHERE [id_folder] IN ({ph})"
            f")"
        )
        for j, fid in enumerate(id_folder):
            params[f"folder_{j}"] = fid
    if id_path:
        ph = ",".join(f":path_id_{j}" for j in range(len(id_path)))
        group_where.append(f"i.[id_path] IN ({ph})")
        for j, pid in enumerate(id_path):
            params[f"path_id_{j}"] = pid
    if path_name:
        # Sanityzacja analogiczna do 'search' ponizej + escapowanie wildcardow
        # LIKE (redundancja celowa — obrona w glab, wzorzec z schemas/users.py).
        safe_path_name = path_name.replace("'", "''")[:100]
        safe_path_name = (
            safe_path_name.replace("[", "[[]").replace("%", "[%]").replace("_", "[_]")
        )
        group_where.append(
            f"i.[id_path] IN ("
            f"  SELECT [id_path] FROM [{_SCHEMA}].[skw_approval_paths] "
            f"  WHERE [path_name] LIKE :path_name"
            f")"
        )
        params["path_name"] = f"%{safe_path_name}%"

    # --- Refinement — ZAWSZE w mandatory_where, niezaleznie od filter_mode ---
    if date_from is not None:
        mandatory_where.append("i.[created_at] >= :date_from")
        params["date_from"] = date_from
    if date_to is not None:
        mandatory_where.append("i.[created_at] <= :date_to_end")
        params["date_to_end"] = f"{date_to} 23:59:59"
    if search:
        safe_search = search.replace("'", "''")[:100]
        # NAPRAWA (2026-07-23): usunieto JSON_VALUE(...'$.ocr_text'...) —
        # surowy tekst OCR nie jest juz przeszukiwany w zwyklym widoku
        # (decyzja produktowa: ocr_* to dane niezweryfikowane, przeszukiwanie
        # po nich stwarzalo posredni wyciek tresci sekcji technicznej dla
        # userow bez documents.view_extra_data). Dodano wyszukiwanie po
        # kanonicznych polach (doc_number, contractor, nip) ORAZ po polach
        # verified_* (zweryfikowanych przez operatora przy confirm OCR) —
        # oba zestawy traktowane jak dane pewne, zgodnie z ta sama zasada
        # co w widoku skw_v_approval_instance_detail.
        mandatory_where.append(
            "(i.[document_title] LIKE :search "
            " OR i.[id_document] LIKE :search"
            " OR JSON_VALUE(i.[extra_data], '$.doc_number') LIKE :search"
            " OR JSON_VALUE(i.[extra_data], '$.contractor') LIKE :search"
            " OR JSON_VALUE(i.[extra_data], '$.nip') LIKE :search"
            " OR JSON_VALUE(i.[extra_data], '$.verified_doc_number') LIKE :search"
            " OR JSON_VALUE(i.[extra_data], '$.verified_contractor') LIKE :search"
            " OR JSON_VALUE(i.[extra_data], '$.verified_nip') LIKE :search)"
        )
        params["search"] = f"%{safe_search}%"

    where_parts: list[str] = []
    if mandatory_where:
        where_parts.append(" AND ".join(mandatory_where))
    if group_where:
        joiner = " OR " if filter_mode_clean == "OR" else " AND "
        where_parts.append(f"({joiner.join(group_where)})")
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    # Absurdalnie szczegolowe logowanie zastosowanych filtrow — traceability
    # przy jakimkolwiek zgloszeniu "zle dane / zla lista" (JSONL-friendly).
    logger.info(
        "list_documents | audit=%s",
        json.dumps(
            {
                "actor_id": actor_id,
                "can_view_all": can_view_all,
                "filter_mode": filter_mode_clean,
                "id_source": id_source,
                "id_category": id_category,
                "status": status,
                "priority": priority,
                "id_folder": id_folder,
                "id_path": id_path,
                "path_name": path_name,
                "search_present": bool(search),
                "date_from": str(date_from) if date_from else None,
                "date_to": str(date_to) if date_to else None,
                "order_by": order_by,
                "order_dir": order_dir,
                "page": page,
                "per_page": per_page,
                "group_conditions_count": len(group_where),
                "mandatory_conditions_count": len(mandatory_where),
            },
            ensure_ascii=False,
            default=str,
        ),
    )

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
                i.[superseded_by_instance_id],
                i.[matched_instance_id], i.[match_type], i.[match_reason],
                s.[source_name],
                p.[path_name],
                g.[group_name] AS current_group_name,
                -- NOWE (2026-07-29, na wniosek frontu): data wystawienia dokumentu
                -- (NIE data wpiecia do obiegu — ta pozostaje w created_at bez zmian).
                -- Ten sam 3-poziomowy fallback co w skw_v_approval_instance_detail
                -- (migracja 0066) i skw_v_approval_my_queue (migracja 0067):
                -- fakir -> extra_data.doc_date -> extra_data.verified_doc_date.
                COALESCE(
                    fah.[DataWystawienia],
                    TRY_CONVERT(DATE, JSON_VALUE(i.[extra_data], '$.doc_date')),
                    TRY_CONVERT(DATE, JSON_VALUE(i.[extra_data], '$.verified_doc_date'))
                ) AS document_date
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
            -- NOWE — ten sam warunek JOIN co w migracjach 0066/0067 (tylko
            -- zrodlo 'fakir'; 'ksef_fakir' i pozostale maja doc_date juz
            -- bezposrednio w extra_data, nie potrzebuja tego JOIN-a).
            LEFT JOIN [{_SCHEMA}].[skw_faktury_akceptacja_naglowek] fah
              ON s.[source_name] = N'fakir' AND fah.[KSEF_ID] = i.[id_document]
            {where_sql}
            ORDER BY
                CASE WHEN i.[status] = N'unassigned' THEN 0 ELSE 1 END ASC,
                {sort_col} {sort_dir_sql},
                i.[id_instance] DESC
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
        # NOWE — date -> ISO string, spojnie z reszta API (front nie dostaje
        # surowego obiektu date z pyodbc).
        if r.get("document_date") is not None:
            r["document_date"] = r["document_date"].isoformat()
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

    # ── NOWE — pod-obiekt kontrahenta ("kontrahent": {"nazwa", "nip"}) ───────
    # Wyrownane z GET /approval/my-queue (widok skw_v_approval_my_queue,
    # migracja 0067) oraz z widokiem skw_v_approval_instance_detail
    # (migracja 0066): identyczny 3-poziomowy fallback
    #   fah.NazwaKontrahenta -> extra_data.contractor -> extra_data.verified_contractor
    #   extra_data.nip -> extra_data.verified_nip
    # Ten endpoint czyta instancje bezposrednio z tabeli (nie z widoku),
    # wiec fallback jest wykonany w osobnym, celowo minimalnym zapytaniu SQL
    # (COALESCE po stronie bazy — nie duplikujemy logiki JSON w Pythonie).
    kontrahent_result = await db.execute(
        text(f"""
            SELECT
                COALESCE(
                    fah.[NazwaKontrahenta],
                    JSON_VALUE(dai.[extra_data], '$.contractor'),
                    JSON_VALUE(dai.[extra_data], '$.verified_contractor')
                ) AS kontrahent_nazwa,
                COALESCE(
                    JSON_VALUE(dai.[extra_data], '$.nip'),
                    JSON_VALUE(dai.[extra_data], '$.verified_nip')
                ) AS kontrahent_nip
            FROM [{_SCHEMA}].[skw_document_approval_instances] dai
            JOIN [{_SCHEMA}].[skw_document_sources] ds
              ON ds.[id_source] = dai.[id_source]
            LEFT JOIN [{_SCHEMA}].[skw_faktury_akceptacja_naglowek] fah
                   ON  ds.[source_name] = N'fakir'
                   AND fah.[KSEF_ID]    = dai.[id_document]
            WHERE dai.[id_instance] = :i
        """),
        {"i": id_instance},
    )
    kontrahent_row = kontrahent_result.fetchone()
    kontrahent = {
        "nazwa": kontrahent_row[0] if kontrahent_row else None,
        "nip":   kontrahent_row[1] if kontrahent_row else None,
    }
    if kontrahent["nazwa"] is None:
        logger.info(
            "get_status_summary: kontrahent=null dla id_instance=%s — "
            "brak danych zarowno w fah.NazwaKontrahenta jak i extra_data.contractor/verified_contractor",
            id_instance,
        )

    return {
        "id_instance":             id_instance,
        "id_document":             instance["id_document"],
        "status":                  instance["status"],
        "status_display":          _STATUS_DISPLAY.get(instance["status"], instance["status"]),
        "document_title":          instance["document_title"],
        "document_amount":         instance["document_amount"],
        "kontrahent":              kontrahent,
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
    db: AsyncSession, *, actor_id: int, can_view_all: bool, page: int = 1, per_page: int = 25,
) -> dict[str, Any]:
    # include_duplicate_pending=True WYMAGANE — bez tego wlaczylby sie
    # nowy domyslny filtr wykluczajacy 'duplicate_pending' z listy_documents
    # i ten ekran zawsze zwracalby pusta liste.
    return await list_documents(
        db, actor_id=actor_id, can_view_all=can_view_all,
        page=page, per_page=per_page, status="duplicate_pending",
        include_duplicate_pending=True,
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

    PRZEBUDOWA (2026-07-28) — porzuca model D-09 (adnotacja w extra_data):
    matched_instance_id/match_type/match_reason (migracja 0068) sa juz
    ustawione przez DuplicateDetectionService w momencie wykrycia — ta
    funkcja ich NIE nadpisuje przy confirm (to fakt historyczny "kto
    wskazal na kogo"), tylko CZYSCI je przy dismiss (skoro to jednak NIE
    duplikat, trzymanie martwego wskazania myliloby przy dalszej analizie).

    decision='confirm': to faktycznie duplikat -> nowy=cancelled (matched_*
        zostaje jako slad), STARA (wskazana) instancja dostaje adnotacje
        w SWOIM extra_data (has_duplicate_attempt) — jedyne miejsce, gdzie
        extra_data jest tu nadal uzywane: to jednorazowy fakt historyczny
        na oryginale, nie stan operacyjny nowej instancji.
    decision='dismiss': to NIE duplikat -> pending_dispatch, matched_instance_id/
        match_type/match_reason wyzerowane.

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

    if decision not in ("confirm", "dismiss"):
        raise DuplicateResolveError(f"decision='{decision}' nieprawidlowa. Dozwolone: confirm, dismiss.")

    matched_instance_id = instance.get("matched_instance_id")
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    if decision == "confirm":
        new_status = "cancelled"
        await db.execute(
            text(f"""
                UPDATE [{_SCHEMA}].[skw_document_approval_instances]
                SET [status] = N'cancelled', [updated_at] = :now
                WHERE [id_instance] = :i
            """),
            {"now": now, "i": id_instance},
        )

        # NOWE (2026-07-28): adnotacja na STAREJ (oryginalnej) instancji —
        # jednorazowy zapis faktu historycznego, nie stan operacyjny.
        # Best-effort: blad tutaj NIE przerywa rozstrzygniecia duplikatu.
        if matched_instance_id:
            try:
                orig_row = (await db.execute(
                    text(f"""
                        SELECT [extra_data] FROM [{_SCHEMA}].[skw_document_approval_instances]
                        WHERE [id_instance] = :m
                    """),
                    {"m": matched_instance_id},
                )).fetchone()
                orig_extra: dict = {}
                if orig_row and orig_row[0]:
                    try:
                        orig_extra = json.loads(orig_row[0])
                    except Exception:
                        orig_extra = {}
                flagged_by = orig_extra.get("has_duplicate_attempt_from") or []
                if id_instance not in flagged_by:
                    flagged_by.append(id_instance)
                orig_extra["has_duplicate_attempt_from"] = flagged_by
                await db.execute(
                    text(f"""
                        UPDATE [{_SCHEMA}].[skw_document_approval_instances]
                        SET [extra_data] = :extra, [updated_at] = :now
                        WHERE [id_instance] = :m
                    """),
                    {
                        "extra": json.dumps(orig_extra, ensure_ascii=False, default=str),
                        "now": now, "m": matched_instance_id,
                    },
                )
            except Exception as exc:
                logger.error(
                    "resolve_duplicate: blad adnotacji na oryginalnej instancji "
                    "#%s (nieblokujacy) | id_instance=%s: %s",
                    matched_instance_id, id_instance, exc,
                )

    else:  # dismiss
        new_status = "pending_dispatch"
        await db.execute(
            text(f"""
                UPDATE [{_SCHEMA}].[skw_document_approval_instances]
                SET [status] = N'pending_dispatch',
                    [matched_instance_id] = NULL,
                    [match_type] = NULL,
                    [match_reason] = NULL,
                    [updated_at] = :now
                WHERE [id_instance] = :i
            """),
            {"now": now, "i": id_instance},
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
                "details": json.dumps({
                    "decision": decision, "new_status": new_status,
                    "matched_instance_id": matched_instance_id,
                }, ensure_ascii=False),
            },
        )
    except Exception as exc:
        logger.error("resolve_duplicate: blad zapisu AuditLog: %s", exc)

    await db.commit()

    logger.info(
        "Duplikat rozstrzygniety | id_instance=%s decision=%s new_status=%s "
        "matched_instance_id=%s actor=%s",
        id_instance, decision, new_status, matched_instance_id, actor_id,
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
                   [extra_data], [is_urgent], [created_at], [updated_at],
                   [matched_instance_id], [match_type], [match_reason]
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
    """
    UWAGA (naprawa 2026-07-16): pierwotna wersja odpytywala nieistniejaca
    tabele posredniczaca dbo.skw_UserRoles (many-to-many User<->Rola),
    ktora nigdy nie zostala utworzona w tym projekcie — brak DDL, brak
    migracji. Realna architektura: skw_Users.RoleID (FK bezposredni,
    jedna rola na usera), potwierdzone w database/ddl/003_users.sql.
    Kazde wywolanie tej funkcji konczylo sie pyodbc.ProgrammingError
    42S02 "Invalid object name" — patrz incydent 2026-07-16,
    request_id=c5478023-0f42-4e1d-80c4-c1626ec1b109.
    """
    result = await db.execute(
        text(f"""
            SELECT COUNT(*)
            FROM [{_SCHEMA}].[skw_Users] u
            JOIN [{_SCHEMA}].[skw_RolePermissions] rp ON rp.[ID_ROLE] = u.[RoleID]
            JOIN [{_SCHEMA}].[skw_Permissions] p ON p.[ID_PERMISSION] = rp.[ID_PERMISSION]
            WHERE u.[ID_USER] = :u AND p.[PermissionName] = :perm AND p.[IsActive] = 1
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

    # NOWE (2026-07-28): SHA-256 CALEGO pliku, PRZED czymkolwiek innym —
    # zgodnie z pkt. 3 specyfikacji ("przy recznym uploadzie — najpierw po
    # hash pliku"). Liczone raz, z bajtow juz w pamieci.
    file_sha256 = hashlib.sha256(content).hexdigest()

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
                 [extra_data],[dispatch_attempts],[file_sha256],[created_at],[updated_at])
            OUTPUT INSERTED.[id_instance]
            VALUES (:src,:doc,N'ocr_review_pending',:title,NULL,:extra,0,:hash,:now,:now)
        """),
        {
            "src": id_source, "doc": id_document,
            "title": safe_name, "extra": json.dumps(extra_data, ensure_ascii=False),
            "hash": file_sha256,
            "now": now,
        },
    )
    id_instance = insert_result.scalar_one()
    await db.commit()

    # NOWE (2026-07-28): sprawdzenie duplikatu PO hashu, PRZED OCR — pkt. 3
    # specyfikacji. Na tym etapie numer/NIP/kwota jeszcze nieznane (przed OCR),
    # wiec kaskada zadziala tu WYLACZNIE metoda file_sha256 (identyczny plik
    # juz kiedys wgrany/pobrany) — pozostale metody zadzialaja PONOWNIE po OCR
    # (patrz worker/tasks/ocr_task.py) i po recznej korekcie (patrz
    # resolve_ocr_review nizej), kiedy te dane juz beda dostepne.
    try:
        is_duplicate = await DuplicateDetectionService.check_and_mark(
            db, id_instance=id_instance, id_source=id_source, id_document=id_document,
        )
        await db.commit()
    except Exception as exc:
        logger.error(
            "upload_document: blad sprawdzania duplikatow (fail-safe, dokument "
            "mimo to zostaje przyjety) | id_instance=%s: %s", id_instance, exc, exc_info=True,
        )
        is_duplicate = False

    ocr_queued = False
    if not is_duplicate:
        try:
            from app.core.arq_pool import get_arq_pool
            arq_pool = get_arq_pool()
            await arq_pool.enqueue_job("ocr_task", id_instance=id_instance, file_path=str(file_path))
            ocr_queued = True
        except Exception as exc:
            logger.error("upload_document: blad enqueue ocr_task dla id_instance=%s: %s", id_instance, exc)
    else:
        logger.info(
            "upload_document: dokument oznaczony jako duplicate_pending (SHA-256) "
            "PRZED OCR — OCR pominiety, referent rozstrzygnie najpierw duplikat | "
            "id_instance=%s", id_instance,
        )

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
        "status":       "duplicate_pending" if is_duplicate else "ocr_review_pending",
        "ocr_queued":   ocr_queued,
        "is_duplicate": is_duplicate,
        "message": (
            "Wykryto mozliwy duplikat (identyczny plik) — sprawdz "
            "GET /documents/duplicate-pending."
            if is_duplicate else
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
    verified_doc_number: str | None = None,
    verified_contractor: str | None = None,
    verified_nip: str | None = None,
    verified_doc_date: date | None = None,
    comment: str | None,
    actor_id: int,
    can_view_all: bool,
    redis: Any = None,
) -> dict[str, Any]:
    """
    Rozstrzyga dokument oczekujacy na reczna weryfikacje OCR.

    decision='confirm': operator potwierdza/poprawia pola -> status=pending_dispatch
                          (wchodzi w normalny automatyczny obieg przez auto_dispatch_task).
                          Dodatkowo (2026-07-23) zapisuje verified_doc_number/
                          verified_contractor/verified_nip/verified_doc_date/
                          verified_by/verified_at do extra_data — oddzielne od
                          surowych ocr_* (ktore pozostaja bez zmian jako slad
                          techniczny). Widoki/wyszukiwanie czytaja WYLACZNIE
                          verified_*, nigdy ocr_* bezposrednio (decyzja
                          produktowa 2026-07-23 — patrz rozmowa robocza:
                          "surowe ocr_* nie sa fallbackiem w zwyklym widoku").
                          Pola moga zostac puste (null) mimo confirm — brak
                          wymogu niepustosci, sam zapis verified_by/verified_at
                          oznacza swiadome sprawdzenie przez operatora.
    decision='reject':   dokument odrzucony -> status=cancelled, ZERO zapisu
                          verified_* (odrzucenie to nie weryfikacja).
    """
    instance = await _get_instance_or_404(db, id_instance)
    await _ensure_visibility(db, instance, actor_id=actor_id, can_view_all=can_view_all)

    if instance["status"] != "ocr_review_pending":
        raise OcrReviewStateError(
            f"Instancja ID={id_instance} nie jest w stanie ocr_review_pending "
            f"(aktualnie: {instance['status']})."
        )

    now = datetime.now(timezone.utc)
    verified_payload: dict[str, Any] = {}

    if decision == "confirm":
        set_clauses = ["[status]=N'pending_dispatch'", "[updated_at]=:now"]
        params: dict[str, Any] = {"i": id_instance, "now": now}
        if document_title is not None:
            set_clauses.append("[document_title]=:title")
            params["title"] = document_title[:500]
        if document_amount is not None:
            set_clauses.append("[document_amount]=:amount")
            params["amount"] = document_amount

        # NOWE (2026-07-23): zapis verified_* do extra_data — merge nad
        # istniejaca zawartoscia (zachowuje ocr_* i inne klucze bez zmian).
        try:
            current_extra = json.loads(instance.get("extra_data") or "{}")
        except Exception:
            current_extra = {}

        verified_payload = {
            "verified_doc_number": verified_doc_number,
            "verified_contractor": verified_contractor,
            "verified_nip":        verified_nip,
            "verified_doc_date":   verified_doc_date.isoformat() if verified_doc_date else None,
            "verified_by":         actor_id,
            "verified_at":         now.isoformat(),
        }
        current_extra.update(verified_payload)
        set_clauses.append("[extra_data]=:extra")
        params["extra"] = json.dumps(current_extra, ensure_ascii=False, default=str)

        await db.execute(
            text(f"""
                UPDATE [{_SCHEMA}].[skw_document_approval_instances]
                SET {", ".join(set_clauses)}
                WHERE [id_instance]=:i
            """),
            params,
        )
        new_status = "pending_dispatch"

        # NOWE (2026-07-28): ponowne sprawdzenie duplikatu — pkt. 3 specyfikacji
        # ("ponownie po recznej korekcie"). Dopiero teraz numer/NIP/data/kwota
        # sa znane z pewnoscia (operator je zatwierdzil/poprawil). Jesli
        # kaskada znajdzie duplikat, PRZESLANIA new_status na duplicate_pending
        # — dokument NIE moze wejsc do auto-dispatchu mimo confirm.
        #
        # NAPRAWA (2026-07-28, self-review): is_dup zainicjalizowane PRZED
        # try — jest teraz uzywane rowniez nizej, w bramce bloku SSE. Bez
        # tego drugi UPDATE w check_and_mark() poprawnie nadpisuje status
        # w bazie na duplicate_pending, ALE blok SSE ponizej i tak wysylal
        # "document_ocr_verified" bezwarunkowo przy kazdym confirm — referent
        # dostawal falszywe powiadomienie "zweryfikowano" dla dokumentu,
        # ktory w rzeczywistosci czeka teraz na rozstrzygniecie duplikatu.
        is_dup = False
        try:
            is_dup = await DuplicateDetectionService.check_and_mark(
                db,
                id_instance=id_instance,
                id_source=instance["id_source"],
                id_document=instance["id_document"],
            )
            if is_dup:
                new_status = "duplicate_pending"
        except Exception as exc:
            logger.error(
                "resolve_ocr_review: blad sprawdzania duplikatow po weryfikacji "
                "(fail-safe) | id_instance=%s: %s", id_instance, exc, exc_info=True,
            )
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

    # NOWE (2026-07-23): audyt rozszerzony o pelny payload weryfikacji —
    # wczesniej zapisywano tylko {decision, comment}, co uniemozliwialo
    # rekonstrukcje co operator faktycznie zatwierdzil (zgloszenie: instancja 787).
    audit_details: dict[str, Any] = {"decision": decision, "comment": comment}
    if decision == "confirm":
        audit_details.update({
            "document_amount": document_amount,
            "duplicate_detected": is_dup,
            **verified_payload,
        })

    await _audit_log(
        db, actor_id=actor_id, action="document.ocr_review_resolved",
        entity_id=id_instance,
        details=audit_details,
    )
    await db.commit()

    logger.info(
        "resolve_ocr_review: id_instance=%s decision=%s new_status=%s actor=%s",
        id_instance, decision, new_status, actor_id,
    )

    # SSE — tylko przy potwierdzeniu (odrzucenie to nie "weryfikacja") ORAZ
    # tylko gdy dokument FAKTYCZNIE wszedl do pending_dispatch, nie gdy
    # ponowne sprawdzenie po korekcie oznaczylo go jako duplicate_pending
    # (NAPRAWA 2026-07-28 — patrz komentarz przy is_dup powyzej).
    if decision == "confirm" and not is_dup and redis is not None:
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


async def get_line_items(
    db: AsyncSession,
    id_instance: int,
    *,
    actor_id: int,
    can_view_all: bool,
) -> dict[str, Any]:
    """
    Pozycje dokumentu — dispatch po source_type zrodla.

    Kontrakt odpowiedzi (2026-07-15, ustalone w rozmowie roboczej,
    NIEUDOKUMENTOWANE w Etap2_Instrukcja_Techniczna — wymaga formalnego
    spisania po wdrozeniu):
        {"format": "structured", "items": [...]}   — database, api
        {"format": "ksef_xml", "raw": "<xml>"}      — ksef20
        {"format": "not_applicable"}                — ftp, email, manual
                                                        (zwrocone z HTTP 409,
                                                        spojnie z field-preview)
    """
    instance = await _get_instance_or_404(db, id_instance)
    await _ensure_visibility(db, instance, actor_id=actor_id, can_view_all=can_view_all)

    if not await _check_user_permission(db, actor_id, "documents.view_line_items"):
        raise HTTPException(
            status_code=403,
            detail="Brak uprawnienia do podgladu pozycji dokumentu.",
        )

    id_source   = instance["id_source"]
    id_document = instance["id_document"]

    src_result = await db.execute(
        text(
            f"SELECT [source_type], [connection_mode] "
            f"FROM [{_SCHEMA}].[skw_document_sources] WHERE [id_source] = :s"
        ),
        {"s": id_source},
    )
    src_row = src_result.fetchone()
    source_type = src_row[0] if src_row else None
    connection_mode = src_row[1] if src_row else None

    if source_type in ("ftp", "email", "manual"):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "line_items.not_applicable",
                "message": f"Zrodlo typu '{source_type}' nie obsluguje pozycji dokumentu.",
            },
        )

    if source_type == "ksef20":
        extra = json.loads(instance.get("extra_data") or "{}")
        xml_raw = extra.get("xml")
        if not xml_raw:
            raise HTTPException(status_code=404, detail="Brak surowego XML dla tego dokumentu KSeF.")
        return {"format": "ksef_xml", "raw": xml_raw}

    if source_type == "api" and connection_mode == "push":
        # NAPRAWA 2026-07-16 (Tier 1c, Recenzja Krytyczna Tier1/Tier2,
        # Rekomendacja D.1): zrodla push maja pozycje zapisane bezposrednio
        # w skw_document_push_items (Tier 1b), nie przez RestApiAdapter
        # (ktory zaklada wychodzace polaczenie HTTP — pull, kierunek
        # odwrotny niz push). Omijamy fabryke adapterow CALKOWICIE dla tej
        # kombinacji source_type+connection_mode, zamiast ja latac —
        # nizsze ryzyko niz modyfikacja get_adapter_by_source_id/_build_adapter
        # uzywanych tez przez inne, niezweryfikowane dzis sciezki wywolania.
        items_result = await db.execute(
            text(f"""
                SELECT [item_data]
                FROM [{_SCHEMA}].[skw_document_push_items]
                WHERE [id_instance] = :i
                ORDER BY [item_order] ASC
            """),
            {"i": id_instance},
        )
        push_items = [json.loads(r[0]) for r in items_result.fetchall()]
        # Brak pozycji = 200 + [] (Rozstrzygniecia Koncowe #13) — spojne
        # z precedensem juz istniejacym dla database+pull (brak wiersza w
        # widoku naglowka -> pusta lista, NIE 409 not_configured), bo brak
        # pozycji push to cecha konkretnego dokumentu, nie brak konfiguracji.
        return {"format": "structured", "items": push_items}

    if source_type in ("database", "api"):
        # Cache Redis TTL 60s — patrz uzasadnienie w rozmowie roboczej
        # (wielu userow otwierajacych ten sam dokument w krotkim czasie).
        cache_key = f"line_items:{id_source}:{id_document}"
        # UWAGA: redis nie jest dzis parametrem tej funkcji — wymaga
        # dopisania do sygnatury i przekazania z routera, jesli cache
        # ma byc uzyty. Poki co pomijam cache w tym szkielecie, zaznaczam
        # jako TODO jawnie, nie ukrywam.
        from app.schemas.unified_document import get_adapter_by_source_id
        adapter = await get_adapter_by_source_id(db, id_source)
        if adapter is None or not hasattr(adapter, "get_line_items"):
            raise HTTPException(status_code=409, detail={
                "code": "line_items.not_applicable",
                "message": "Adapter tego zrodla nie obsluguje pozycji.",
            })
        try:
            items = await adapter.get_line_items(id_document)
        except Exception as exc:
            # NAPRAWA 2026-07-16 (incydent request_id=68f6de26-0248-4bb7-a5e4-6fc3656d4722):
            # Adaptery (DatabaseAdapter, RestApiAdapter) celowo re-raise'uja
            # wyjatki po zalogowaniu (nie tlumia ich cicho) — ale bez obslugi
            # tutaj kazda awaria zewnetrznej infrastruktury (baza SQL
            # nieosiagalna, zewnetrzne API nie odpowiada) konczyla sie
            # nieopisanym 500 (server.internal_error) zamiast czytelnego
            # komunikatu. Rozrozniamy tu WYLACZNIE awarie transportowe/
            # polaczenia — NIE lapiemy httpx.HTTPStatusError (to znaczy ze
            # zewnetrzne API odpowiedzialo, tylko blednym kodem — inny
            # przypadek, celowo zostawiony jako 500 do dalszej analizy).
            import pyodbc
            import httpx

            if isinstance(exc, (pyodbc.Error, httpx.RequestError)):
                logger.error(
                    "get_line_items: zewnetrzne zrodlo danych niedostepne | "
                    "id_source=%s id_document=%s source_type=%s exc_type=%s error=%s",
                    id_source, id_document, source_type, type(exc).__name__, exc,
                )
                raise HTTPException(status_code=503, detail={
                    "code": "line_items.source_unavailable",
                    "message": "Zrodlo danych pozycji jest chwilowo niedostepne. Sprobuj ponownie za chwile.",
                }) from exc
            # Nieprzewidziany blad logiki aplikacji — propagujemy dalej jako
            # 500, nie maskujemy go pod "niedostepnosc zrodla zewnetrznego".
            raise

        if items is None:
            raise HTTPException(status_code=409, detail={
                "code": "line_items.not_configured",
                "message": "To zrodlo nie ma skonfigurowanych pozycji (pole opcjonalne, puste).",
            })
        return {"format": "structured", "items": items}

    raise HTTPException(status_code=409, detail={"code": "line_items.not_applicable", "message": f"Nieznany source_type '{source_type}'."})