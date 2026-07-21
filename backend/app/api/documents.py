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
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, UploadFile, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.schemas.common import BaseResponse
from app.services import documents_service as svc
from app.services.documents_service import (
    DocumentNotFoundError,
    DuplicateResolveError,
)
from app.services.documents_service import OcrReviewStateError

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
        "\n\nid_source rowniez dopuszcza wiele wartosci jednoczesnie (2026-07-21) "
        "— dokument widoczny jesli pochodzi z KTOREGOKOLWIEK z podanych zrodel "
        "(np. ?id_source=1&id_source=6). "
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
    date_from: Optional[date] = Query(None, description="Filtr: created_at >= date_from"),
    date_to: Optional[date] = Query(None, description="Filtr: created_at <= date_to (cały dzień)"),
    priority: Optional[int] = Query(None, description="Filtr po priorytecie dokumentu"),
    id_path: Optional[list[int]] = Query(
        None, description="Filtr po ID sciezki decyzyjnej. Wiele wartosci = dopasowanie IN (...)."
    ),
    path_name: Optional[str] = Query(
        None, max_length=100,
        description="Filtr tekstowy po nazwie sciezki decyzyjnej (LIKE '%fragment%'), np. 'firanki'.",
    ),
    filter_mode: str = Query(
        "AND", pattern="^(AND|OR|and|or)$",
        description=(
            "AND (domyslnie) lub OR — sposob laczenia filtrow standardowych "
            "(id_source, id_category, status, priority, id_folder, id_path, path_name). "
            "Filtry bezpieczenstwa (dostep do zrodel, widocznosc restricted) oraz "
            "search/date_from/date_to ZAWSZE pozostaja w AND, niezaleznie od tego parametru."
        ),
    ),
    order_by: str = Query(
        "created_at",
        description="Dozwolone: created_at | updated_at | document_title | document_amount | status | priority",
    ),
    order_dir: str = Query("desc", pattern="^(asc|desc)$"),
):
    can_view_all = await _can_view_all(current_user, db)
    result = await svc.list_documents(
        db,
        actor_id=current_user.id_user, can_view_all=can_view_all,
        page=page, per_page=per_page,
        id_source=id_source, id_folder=id_folder, id_category=id_category,
        status=status, search=search,
        date_from=date_from, date_to=date_to, priority=priority,
        id_path=id_path, path_name=path_name, filter_mode=filter_mode,
        order_by=order_by, order_dir=order_dir,
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



@router.post(
    "/upload",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Reczne wgranie dokumentu PDF do listy obiegowej",
    description=(
        "Zapisuje plik, tworzy nowa instancje (status=ocr_review_pending, "
        "zrodlo='manual_upload') i kolejkuje OCR w tle. Po zakonczeniu OCR: "
        "wysoka pewnosc + numer dokumentu i kwota znalezione -> "
        "status=pending_dispatch (normalny automatyczny obieg dalej). "
        "Niska pewnosc lub brak wymaganych pol -> status pozostaje "
        "ocr_review_pending, wymaga POST /documents/{id}/ocr-review/resolve. "
        "**Wymaga:** `documents.upload`."
    ),
    responses={
        413: {"description": "Plik za duzy"},
        415: {"description": "Niedozwolony typ pliku (tylko PDF)"},
        422: {"description": "Plik pusty"},
        500: {"description": "Brak skonfigurowanego zrodla manual_upload"},
    },
    dependencies=[require_permission("documents.upload")],
)
async def upload_document_endpoint(
    file: UploadFile,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    result = await svc.upload_document(db, redis, file=file, actor_id=current_user.id_user)
    return BaseResponse.ok(data=result, app_code="documents.uploaded")


@router.get(
    "/ocr-review-pending",
    summary="Lista dokumentow oczekujacych na reczna weryfikacje OCR",
    description=(
        "Dokumenty recznie wgrane, ktorych OCR nie znalazl wymaganych pol "
        "(numer, kwota) z wystarczajaca pewnoscia. "
        "**Wymaga:** `documents.upload`."
    ),
    dependencies=[require_permission("documents.upload")],
)
async def list_ocr_review_pending_endpoint(
    current_user: CurrentUser,
    db: DB,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    can_view_all = await _can_view_all(current_user, db)
    result = await svc.list_documents(
        db, actor_id=current_user.id_user, can_view_all=can_view_all,
        page=page, per_page=per_page, status="ocr_review_pending",
    )
    return BaseResponse.ok(data=result, app_code="documents.ocr_review_pending_list")


class OcrReviewResolveBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    decision: str = Field(..., pattern=r"^(confirm|reject)$")
    document_title: Optional[str] = Field(None, max_length=500)
    document_amount: Optional[float] = Field(None, ge=0)
    comment: Optional[str] = Field(None, max_length=1000)


@router.post(
    "/{id_instance}/ocr-review/resolve",
    summary="Rozstrzygniecie recznej weryfikacji OCR",
    description=(
        "decision='confirm' — operator potwierdza/poprawia numer i kwote, "
        "dokument wchodzi w normalny obieg (status=pending_dispatch). "
        "decision='reject' — dokument odrzucony (status=cancelled). "
        "**Wymaga:** `documents.upload`."
    ),
    responses={
        404: {"description": "Dokument nie istnieje"},
        409: {"description": "Dokument nie jest w stanie ocr_review_pending"},
    },
    dependencies=[require_permission("documents.upload")],
)
async def resolve_ocr_review_endpoint(
    id_instance: int,
    body: OcrReviewResolveBody,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    can_view_all = await _can_view_all(current_user, db)
    try:
        result = await svc.resolve_ocr_review(
            db, id_instance,
            decision=body.decision,
            document_title=body.document_title,
            document_amount=body.document_amount,
            comment=body.comment,
            actor_id=current_user.id_user, can_view_all=can_view_all,
            redis=redis,
        )
    except DocumentNotFoundError as exc:
        _raise_doc_error(exc)
    except OcrReviewStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return BaseResponse.ok(data=result, app_code="documents.ocr_review_resolved")

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


@router.get(
    "/{id_instance}/pdf",
    summary="Podglad PDF dokumentu (tylko jesli zrodlo go udostepnia)",
    description=(
        "Dla zrodla 'fakir' deleguje do istniejacej logiki generowania PDF "
        "modulu faktur (identyczny dokument, nowy adres URL, zero duplikacji "
        "kodu generowania). Dla pozostalych zrodel (ksef, ftp, email, manual) "
        "zwraca 404 z jasnym komunikatem — brak PDF nie jest bledem serwera, "
        "tylko informacja ze to zrodlo nie udostepnia jeszcze podgladu. "
        "**Wymaga:** `pdf.download`."
    ),
    responses={
        403: {"description": "Brak dostepu (filtr restricted) lub brak uprawnienia pdf.download"},
        404: {"description": "Dokument nie istnieje, brak odpowiadajacego rekordu, lub zrodlo nie udostepnia PDF"},
    },
    response_class=StreamingResponse,
    dependencies=[require_permission("pdf.download")],
)

async def get_document_pdf(
    id_instance: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
) -> StreamingResponse:
    can_view_all = await _can_view_all(current_user, db)
    try:
        instance = await svc._get_instance_or_404(db, id_instance)
        await svc._ensure_visibility(db, instance, actor_id=current_user.id_user, can_view_all=can_view_all)
    except DocumentNotFoundError as exc:
        _raise_doc_error(exc)

    # _get_instance_or_404 nie zwraca source_name (tylko id_source) —
    # osobny lookup, zamiast rozszerzac wspoldzielona funkcje uzywana
    # tez przez inne endpointy.
    from sqlalchemy import text as _text
    source_name_result = await db.execute(
        _text("SELECT [source_name] FROM [dbo].[skw_document_sources] WHERE [id_source] = :s"),
        {"s": instance["id_source"]},
    )
    source_name = source_name_result.scalar_one_or_none()

    import json as _json
    import mimetypes
    from pathlib import Path
    from fastapi.responses import FileResponse

    try:
        extra = _json.loads(instance.get("extra_data") or "{}")
    except Exception:
        extra = {}
    file_path = extra.get("file_path")

    if file_path:
        if not Path(file_path).exists():
            raise HTTPException(
                status_code=404,
                detail="Plik zrodlowy nie istnieje juz na dysku (usuniety recznie lub blad pobrania).",
            )
        media_type, _ = mimetypes.guess_type(file_path)
        return FileResponse(
            path=file_path,
            media_type=media_type or "application/octet-stream",
            filename=Path(file_path).name,
        )


    if source_name == "ksef20":
        # Brak wiernego renderera FA(3)->PDF (stary mechanizm z Etapu 1 nieznaleziony).
        # Reuzywamy ta sama sciezke co manual_upload — PDF z danych instancji,
        # nie z surowego XML. XML pozostaje w extra_data.xml na przyszlosc.
        pdf_bytes = await fak_svc.get_faktura_pdf_from_instance(
            db=db, redis=redis, id_instance=id_instance, actor_id=current_user.id_user,
        )
        return StreamingResponse(
            content=iter([pdf_bytes]), media_type="application/pdf",
            headers={"Content-Disposition": f'inline; filename="dokument_{id_instance}.pdf"'},
        )

    if source_name != "fakir":
        raise HTTPException(
            status_code=404,
            detail=f"Zrodlo '{source_name}' nie udostepnia jeszcze podgladu PDF.",
        )

    # id_document dla fakir == numer_ksef (KSEF_ID) — znajdz odpowiadajacy
    # rekord w legacy tabeli skw_faktura_akceptacja, ktorej PK oczekuje
    # istniejaca funkcja get_faktura_pdf().
    from sqlalchemy import select as _select
    from app.db.models.faktura_akceptacja import FakturaAkceptacja
    from app.services import faktura_akceptacja_service as fak_svc

    result = await db.execute(
        _select(FakturaAkceptacja.id).where(
            FakturaAkceptacja.numer_ksef == instance["id_document"]
        )
    )
    faktura_id = result.scalar_one_or_none()

    if faktura_id is not None:
        # Dokument zmigrowany Krokiem 0 — legacy sciezka (bez zmian)
        pdf_bytes = await fak_svc.get_faktura_pdf(
            db=db, redis=redis, faktura_id=faktura_id, actor_id=current_user.id_user,
        )
    else:
        # Dokument z nowego modelu (Etap 2) — nowa sciezka
        pdf_bytes = await fak_svc.get_faktura_pdf_from_instance(
            db=db, redis=redis, id_instance=id_instance, actor_id=current_user.id_user,
        )

    return StreamingResponse(
        content=iter([pdf_bytes]),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="dokument_{id_instance}.pdf"',
            "Cache-Control": "no-store",
        },
    )

@router.get(
    "/{id_instance}/line-items",
    summary="Pozycje (linie) dokumentu",
    description=(
        "Zwraca pozycje faktury — format zalezny od source_type (structured/ksef_xml). "
        "409 jesli zrodlo nie obsluguje pozycji. **Wymaga:** `documents.view` + `documents.view_line_items`."
    ),
    responses={
        403: {"description": "Brak uprawnienia documents.view_line_items"},
        404: {"description": "Dokument nie istnieje"},
        409: {"description": "Zrodlo nie obsluguje pozycji"},
    },
    dependencies=[require_permission("documents.view")],
)
async def get_line_items_endpoint(id_instance: int, current_user: CurrentUser, db: DB):
    can_view_all = await _can_view_all(current_user, db)
    result = await svc.get_line_items(
        db, id_instance, actor_id=current_user.id_user, can_view_all=can_view_all,
    )
    return BaseResponse.ok(data=result, app_code="documents.line_items")