"""
api/debtors.py
═══════════════════════════════════════════════════════════════════════════════
Router dłużników — System Windykacja.

10 endpointów:
  GET    /debtors                    — lista dłużników (filtry, paginacja) [WAPRO]
  GET    /debtors/stats              — statystyki zbiorcze [WAPRO]
  POST   /debtors/validate-bulk      — walidacja listy ID przed wysyłką ★
  POST   /debtors/send-bulk          — masowa wysyłka monitów → ARQ 202 Accepted ★
  GET    /debtors/{id}               — szczegóły dłużnika [WAPRO]
  GET    /debtors/{id}/invoices      — faktury/rozrachunki [WAPRO] ★
  GET    /debtors/{id}/monit-history — historia monitów
  GET    /debtors/{id}/comments      — komentarze ★
  POST   /debtors/{id}/preview-pdf   — podgląd PDF monitu → StreamingResponse ★
  POST   /debtors/{id}/send          — wyślij monit do jednego dłużnika → ARQ 202

Ważna kolejność rejestracji ścieżek (FastAPI FIFO):
  • /stats, /validate-bulk, /send-bulk — PRZED /{id}
    (literały muszą być dopasowane przed parametrami)

Serwisy:
  • debtor_service  — lista, szczegóły, faktury, historia, stats, walidacja
  • monit_service   — wysyłka (pojedyncza i bulk), podgląd PDF
  • comment_service — komentarze per-dłużnik

ARQ: endpointy send i send-bulk zwracają HTTP 202 Accepted.
PDF: StreamingResponse z Content-Type: application/pdf.

Autor: System Windykacja
Wersja: 1.0.0
Data: 2026-02-20
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import orjson
from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse

from app.core.dependencies import (
    DB,
    ClientIP,
    CurrentUser,
    Pagination,
    RedisClient,
    RequestID,
    WaproDB,
    require_permission,
)
from app.schemas.common import BaseResponse

logger = logging.getLogger(__name__)
router = APIRouter()


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 1: GET /debtors
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "",
    summary="Lista dłużników",
    description=(
        "Zwraca paginowaną listę dłużników z widoków WAPRO. "
        "Filtry: min_debt, max_debt, overdue_only, search (nazwa / NIP). "
        "Sortowanie: sort_by + sort_dir (domyślnie: total_debt DESC). "
        "Cache Redis TTL 60s. AuditLog fire-and-forget. "
        "**Wymaga uprawnienia:** `debtors.view_list`"
    ),
    response_description="Paginowana lista dłużników",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("debtors.view_list")],
)
async def list_debtors(
    current_user: CurrentUser,
    db: DB,
    wapro: WaproDB,
    redis: RedisClient,
    pagination: Pagination,
    request_id: RequestID,
    search: Optional[str] = Query(None, max_length=100, description="Wyszukiwanie po nazwie lub NIP"),
    min_debt: Optional[float] = Query(None, ge=0, description="Minimalne zadłużenie (PLN)"),
    max_debt: Optional[float] = Query(None, ge=0, description="Maksymalne zadłużenie (PLN)"),
    overdue_only: bool = Query(False, description="Tylko przeterminowani dłużnicy"),
    sort_by: str = Query("total_debt", description="Pole sortowania: total_debt | name | overdue_days"),
    sort_dir: str = Query("desc", pattern="^(asc|desc)$", description="Kierunek: asc | desc"),
):
    from app.services import debtor_service
    from app.services.debtor_service import DebtorListParams

    result = await debtor_service.get_list(
        wapro=wapro,
        redis=redis,
        db=db,
        params=DebtorListParams(
            search=search,
            min_debt=min_debt,
            max_debt=max_debt,
            overdue_min_days=None,
            overdue_max_days=None,
            has_active_monit=None,
            page=pagination.page,
            page_size=pagination.per_page,
            sort_by=sort_by,
            sort_desc=(sort_dir == "desc"),
        ),
        requesting_user_id=current_user.id_user,
        ip_address=None,
    )

    return BaseResponse.ok(
        data={
            "items": result["items"],
            "total": result["total"],
            "page": pagination.page,
            "per_page": pagination.per_page,
            "pages": _pages(result["total"], pagination.per_page),
        },
        app_code="debtors.list",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 2: GET /debtors/stats
# (PRZED /{id} — literal musi być przed parametrem)
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/stats",
    summary="Statystyki dłużników",
    description=(
        "Zwraca zagregowane statystyki z widoków WAPRO: łączne zadłużenie, "
        "liczba dłużników, liczba przeterminowanych, średnie zadłużenie, "
        "najstarszy dług (dni). Cache Redis TTL 120s. "
        "**Wymaga uprawnienia:** `debtors.stats`"
    ),
    response_description="Statystyki zbiorcze",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("debtors.view_list")],
)
async def get_debtors_stats(
    current_user: CurrentUser,
    wapro: WaproDB,
    redis: RedisClient,
    request_id: RequestID,
):
    from app.services import debtor_service

    stats = await debtor_service.get_stats(wapro=wapro, redis=redis)

    return BaseResponse.ok(data=stats, app_code="debtors.stats")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 3: POST /debtors/validate-bulk ★
# (PRZED /{id})
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/validate-bulk",
    summary="Walidacja listy dłużników przed wysyłką",
    description=(
        "Sprawdza listę ID pod kątem możliwości wysłania monitu: "
        "istnienie w WAPRO, dane kontaktowe dla wybranego kanału, cooldown (z config). "
        "Zwraca podział na: valid / invalid (z powodem). "
        "Maksymalnie 500 ID w jednym żądaniu. "
        "**Wymaga uprawnienia:** `monits.send`"
    ),
    response_description="Wynik walidacji: valid i invalid z powodami",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("debtors.view_list")],
)
async def validate_bulk_debtors(
    request: Request,
    current_user: CurrentUser,
    wapro: WaproDB,
    redis: RedisClient,
    db: DB,
    request_id: RequestID,
):
    from app.services import debtor_service

    body = await _parse_body(request)
    debtor_ids = body.get("debtor_ids") or []
    channel = (body.get("channel") or "email").strip().lower()
    template_id = body.get("template_id")

    _errors = _validate_debtor_ids(debtor_ids)
    if channel not in ("email", "sms", "letter"):
        _errors.append({"field": "channel", "message": "Dozwolone wartości: email, sms, letter"})
    if _errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "validation.error", "message": "Błąd walidacji", "errors": _errors},
        )

    valid_ids = await debtor_service.validate_ids(
        wapro=wapro,
        ids=debtor_ids,
    )

    all_ids_set = set(debtor_ids)
    valid_set = set(valid_ids)
    invalid_ids = sorted(all_ids_set - valid_set)

    return BaseResponse.ok(
        data={
            "valid":         sorted(valid_ids),
            "invalid":       invalid_ids,
            "valid_count":   len(valid_ids),
            "invalid_count": len(invalid_ids),
            "channel":       channel,
        },
        app_code="debtors.validate_bulk",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 4: POST /debtors/send-bulk ★
# (PRZED /{id})
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/send-bulk",
    summary="Masowa wysyłka monitów",
    description=(
        "Kolejkuje masową wysyłkę monitów do listy dłużników w ARQ. "
        "Odpowiedź natychmiastowa **HTTP 202 Accepted** — wysyłka asynchroniczna. "
        "Zalecane: najpierw /validate-bulk. Maksymalnie 500 dłużników. "
        "AuditLog: `monit_bulk_sent` z listą ID (fire-and-forget). "
        "**Wymaga uprawnienia:** `monits.send_bulk`"
    ),
    response_description="Potwierdzenie kolejkowania (ARQ job IDs)",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[require_permission("monits.send_bulk")],
)
async def send_bulk_monits(
    request: Request,
    current_user: CurrentUser,
    db: DB,
    wapro: WaproDB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import monit_service

    body = await _parse_body(request)
    debtor_ids = body.get("debtor_ids") or []
    channel = (body.get("channel") or "email").strip().lower()
    template_id = body.get("template_id")

    _errors = _validate_debtor_ids(debtor_ids)
    if channel not in ("email", "sms", "letter"):
        _errors.append({"field": "channel", "message": "Dozwolone: email, sms, letter"})
    if not template_id:
        _errors.append({"field": "template_id", "message": "Pole wymagane"})
    if _errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "validation.error", "message": "Błąd walidacji", "errors": _errors},
        )

    try:
        result = await monit_service.send_bulk(
            db=db,
            wapro=wapro,
            redis=redis,
            debtor_ids=debtor_ids,
            channel=channel,
            template_id=template_id,
            sent_by_id=current_user.id_user,
            ip=client_ip,
        )
    except Exception as exc:
        _raise_from_monit_error(exc)

    logger.warning(
        orjson.dumps({
            "event": "api_monit_bulk_queued",
            "debtor_count": len(debtor_ids),
            "channel": channel,
            "template_id": template_id,
            "sent_by": current_user.id_user,
            "job_count": result.get("job_count", 0),
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={
            "message": "Zlecenia wysyłki dodane do kolejki ARQ.",
            "job_count": result.get("job_count", 0),
            "job_ids": result.get("job_ids", []),
            "debtor_count": len(debtor_ids),
            "channel": channel,
        },
        app_code="debtors.send_bulk_queued",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 5: GET /debtors/{id}
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/{debtor_id}",
    summary="Szczegóły dłużnika",
    description=(
        "Zwraca pełne dane dłużnika z WAPRO: kontrahent, łączne zadłużenie, "
        "faktury z terminami, dni przeterminowania. "
        "Dodatkowe z dbo_ext: ostatnie 10 monitów. Cache TTL 120s. "
        "**Wymaga uprawnienia:** `debtors.view`"
    ),
    response_description="Szczegóły dłużnika z danymi WAPRO",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("debtors.view_details")],
    responses={404: {"description": "Dłużnik nie istnieje w WAPRO"}},
)
async def get_debtor(
    debtor_id: int,
    current_user: CurrentUser,
    db: DB,
    wapro: WaproDB,
    redis: RedisClient,
    request_id: RequestID,
):
    from app.services import debtor_service

    try:
        debtor = await debtor_service.get_by_id(
            wapro=wapro,
            db=db,
            redis=redis,
            debtor_id=debtor_id,
        )
    except Exception as exc:
        _raise_from_debtor_error(exc)

    return BaseResponse.ok(data=debtor, app_code="debtors.detail")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 6: GET /debtors/{id}/invoices ★
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/{debtor_id}/invoices",
    summary="Faktury/rozrachunki dłużnika",
    description=(
        "Lista faktur i rozrachunków dłużnika z WAPRO. "
        "Każda faktura: numer, daty, kwota brutto, kwota do zapłaty, dni przeterminowania. "
        "Cache Redis: `debtor:{id}:invoices` TTL 120s. "
        "**Wymaga uprawnienia:** `debtors.view`"
    ),
    response_description="Lista faktur dłużnika",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("debtors.view_invoices")],
)
async def get_debtor_invoices(
    debtor_id: int,
    current_user: CurrentUser,
    wapro: WaproDB,
    redis: RedisClient,
    db: DB,
    request_id: RequestID,
):
    from app.services import debtor_service

    try:
        invoices = await debtor_service.get_invoices(
            wapro=wapro,
            redis=redis,
            db=db,
            debtor_id=debtor_id,
        )
    except Exception as exc:
        _raise_from_debtor_error(exc)

    return BaseResponse.ok(
        data=invoices,
        app_code="debtors.invoices",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 7: GET /debtors/{id}/monit-history
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/{debtor_id}/monit-history",
    summary="Historia monitów dłużnika",
    description=(
        "Historia monitów wysłanych do dłużnika "
        "(z dbo_ext.MonitHistory — nie z WAPRO). "
        "Zawiera: datę, kanał, status, operatora, ewentualny błąd wysyłki. "
        "**Wymaga uprawnienia:** `debtors.view`"
    ),
    response_description="Historia monitów dłużnika",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("debtors.view_monit_history")],
)
async def get_debtor_monit_history(
    debtor_id: int,
    current_user: CurrentUser,
    db: DB,
    request_id: RequestID,
):
    from app.services import debtor_service

    from app.services.debtor_service import MonitHistoryParams

    history = await debtor_service.get_monit_history(
        db=db,
        params=MonitHistoryParams(debtor_id=debtor_id),
    )
    return BaseResponse.ok(
        data=history,
        app_code="debtors.monit_history",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 8: GET /debtors/{id}/comments ★
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/{debtor_id}/comments",
    summary="Komentarze do dłużnika",
    description=(
        "Komentarze przypisane do konkretnego dłużnika (skrót do /comments?debtor_id={id}). "
        "Sortowanie: CreatedAt DESC. "
        "**Wymaga uprawnienia:** `debtors.view`"
    ),
    response_description="Lista komentarzy dłużnika",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("comments.view")],
)
async def get_debtor_comments(
    debtor_id: int,
    current_user: CurrentUser,
    db: DB,
    request_id: RequestID,
):
    from app.services import comment_service

    comments = await comment_service.get_list(db=db, debtor_id=debtor_id)

    return BaseResponse.ok(
        data=comments,
        app_code="debtors.comments",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 9: POST /debtors/{id}/preview-pdf ★
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/{debtor_id}/preview-pdf",
    summary="Podgląd PDF monitu",
    description=(
        "Generuje podgląd PDF monitu bez zapisu (in-memory). "
        "Używane przed wysyłką do weryfikacji treści. "
        "Wymagane pola body: template_id, channel. "
        "Odpowiedź: **StreamingResponse** (Content-Type: application/pdf). "
        "**Wymaga uprawnienia:** `monits.send`"
    ),
    response_description="Strumień PDF monitu",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("pdf.generate")],
    responses={
        200: {"content": {"application/pdf": {}}, "description": "Strumień PDF"},
        404: {"description": "Dłużnik lub szablon nie istnieje"},
        422: {"description": "Brak template_id"},
    },
)
async def preview_monit_pdf(
    debtor_id: int,
    request: Request,
    current_user: CurrentUser,
    db: DB,
    wapro: WaproDB,
    redis: RedisClient,
    request_id: RequestID,
):
    from app.services import monit_service

    body = await _parse_body(request)
    template_id = body.get("template_id")
    channel = (body.get("channel") or "email").strip().lower()

    if not template_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "validation.error", "message": "Brak wymaganych pól",
                    "errors": [{"field": "template_id", "message": "Pole wymagane"}]},
        )

    try:
        pdf_bytes = await monit_service.generate_pdf_preview(
            db=db,
            wapro=wapro,
            redis=redis,
            debtor_id=debtor_id,
            template_id=template_id,
            channel=channel,
        )
    except Exception as exc:
        _raise_from_monit_error(exc)

    filename = f"monit_preview_{debtor_id}_{template_id}.pdf"

    return StreamingResponse(
        content=iter([pdf_bytes]),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="{filename}"',
            "X-Debtor-ID": str(debtor_id),
            "X-Template-ID": str(template_id),
            "X-Request-ID": request_id or "",
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 10: POST /debtors/{id}/send
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/{debtor_id}/send",
    summary="Wyślij monit do dłużnika",
    description=(
        "Kolejkuje wysyłkę monitu do jednego dłużnika w ARQ. "
        "Odpowiedź natychmiastowa **HTTP 202 Accepted**. "
        "Wymagane: template_id, channel. "
        "**Wymaga uprawnienia:** `monits.send`"
    ),
    response_description="Potwierdzenie kolejkowania (ARQ job ID)",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[require_permission("monits.send")],
    responses={202: {"description": "Monit w kolejce ARQ"}},
)
async def send_monit(
    debtor_id: int,
    request: Request,
    current_user: CurrentUser,
    db: DB,
    wapro: WaproDB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import monit_service

    body = await _parse_body(request)
    template_id = body.get("template_id")
    channel = (body.get("channel") or "email").strip().lower()
    note = str(body.get("note") or "")[:500]

    _errors = []
    if not template_id:
        _errors.append({"field": "template_id", "message": "Pole wymagane"})
    if channel not in ("email", "sms", "letter"):
        _errors.append({"field": "channel", "message": "Dozwolone: email, sms, letter"})
    if _errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "validation.error", "message": "Błąd walidacji", "errors": _errors},
        )

    try:
        result = await monit_service.send_single(
            db=db,
            wapro=wapro,
            redis=redis,
            debtor_id=debtor_id,
            template_id=template_id,
            channel=channel,
            note=note,
            sent_by_id=current_user.id_user,
            ip=client_ip,
        )
    except Exception as exc:
        _raise_from_monit_error(exc)

    logger.info(
        orjson.dumps({
            "event": "api_monit_queued",
            "debtor_id": debtor_id,
            "channel": channel,
            "template_id": template_id,
            "job_id": result.get("job_id"),
            "sent_by": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={
            "message": "Monit dodany do kolejki ARQ.",
            "job_id": result.get("job_id"),
            "debtor_id": debtor_id,
            "channel": channel,
            "monit_id": result.get("monit_id"),
        },
        app_code="debtors.monit_queued",
    )


# ─────────────────────────────────────────────────────────────────────────────
# POMOCNICZE
# ─────────────────────────────────────────────────────────────────────────────

async def _parse_body(request: Request) -> dict:
    try:
        return await request.json()
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "validation.error", "message": "Nieprawidłowy format JSON",
                    "errors": [{"field": "_", "message": "Wymagany body JSON"}]},
        )


def _validate_debtor_ids(debtor_ids: object) -> list[dict]:
    """Waliduje pole debtor_ids. Zwraca listę błędów (pusta = OK)."""
    if not isinstance(debtor_ids, list) or not debtor_ids:
        return [{"field": "debtor_ids", "message": "Wymagana niepusta tablica ID"}]
    if len(debtor_ids) > 500:
        return [{"field": "debtor_ids", "message": f"Maksymalnie 500 ID (podano {len(debtor_ids)})"}]
    if not all(isinstance(i, int) and i > 0 for i in debtor_ids):
        return [{"field": "debtor_ids", "message": "Każdy element musi być dodatnią liczbą całkowitą"}]
    return []


def _raise_from_debtor_error(exc: Exception) -> None:
    exc_type = type(exc).__name__
    _MAP: dict[str, tuple[int, str, str]] = {
        "DebtorNotFoundError":  (404, "debtors.not_found",    "Dłużnik nie istnieje w WAPRO"),
        "WaproConnectionError": (503, "wapro.unavailable",    "Baza WAPRO niedostępna — spróbuj ponownie"),
        "DebtorServiceError":   (400, "debtors.service_error","Błąd operacji na dłużniku"),
    }
    if exc_type in _MAP:
        http_status, code, msg = _MAP[exc_type]
        raise HTTPException(
            status_code=http_status,
            detail={"code": code, "message": msg,
                    "errors": [{"field": "_", "message": str(exc) or msg}]},
        )
    raise


def _raise_from_monit_error(exc: Exception) -> None:
    exc_type = type(exc).__name__
    _MAP: dict[str, tuple[int, str, str]] = {
        "DebtorNotFoundError":   (404, "debtors.not_found",       "Dłużnik nie istnieje w WAPRO"),
        "TemplateNotFoundError": (404, "monits.template_not_found","Szablon monitu nie istnieje"),
        "MonitCooldownError":    (429, "monits.cooldown",          "Dłużnik był monitorowany zbyt niedawno"),
        "MissingContactError":   (422, "monits.missing_contact",   "Brak danych kontaktowych dla wybranego kanału"),
        "WaproConnectionError":  (503, "wapro.unavailable",        "Baza WAPRO niedostępna"),
        "MonitServiceError":     (400, "monits.service_error",     "Błąd operacji wysyłki"),
    }
    if exc_type in _MAP:
        http_status, code, msg = _MAP[exc_type]
        raise HTTPException(
            status_code=http_status,
            detail={"code": code, "message": msg,
                    "errors": [{"field": "_", "message": str(exc) or msg}]},
        )
    raise


def _pages(total: int, per_page: int) -> int:
    if per_page <= 0:
        return 0
    return (total + per_page - 1) // per_page