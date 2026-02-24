"""
api/monits.py
═══════════════════════════════════════════════════════════════════════════════
Router monitów — System Windykacja. ★ NOWY plik (AUDIT R7/R12)

8 endpointów:
  GET    /monits              — historia monitów (paginacja, filtry)
  GET    /monits/stats        — statystyki wysyłek per kanał/status
  GET    /monits/queue        — status kolejki ARQ
  GET    /monits/{id}         — szczegóły monitu
  GET    /monits/{id}/pdf     — pobierz PDF monitu → StreamingResponse
  POST   /monits/{id}/retry   — ponów wysyłkę → ARQ 202 Accepted
  PUT    /monits/{id}/status  — zmiana statusu (np. DELIVERED → PAID)
  DELETE /monits/{id}/cancel  — anuluj zadanie z kolejki (tylko PENDING)

Ważna kolejność ścieżek (FastAPI FIFO):
  /stats i /queue PRZED /{id}

Serwis: services/monit_service.py

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
    require_permission,
)
from app.schemas.common import BaseResponse

logger = logging.getLogger(__name__)
router = APIRouter()

# Dozwolone statusy monitu
_VALID_STATUSES = frozenset({"PENDING", "SENT", "DELIVERED", "FAILED", "PAID", "CANCELLED"})

# Dozwolone przejścia statusów (manualna zmiana przez operatora)
_ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    "SENT":      frozenset({"DELIVERED", "FAILED", "PAID"}),
    "DELIVERED": frozenset({"PAID"}),
    "FAILED":    frozenset({"PENDING"}),
}


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 1: GET /monits
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "",
    summary="Historia monitów",
    description=(
        "Paginowana historia wszystkich monitów z systemu. "
        "Filtry: debtor_id, status, channel (email|sms|letter), "
        "date_from, date_to, sent_by (ID operatora). "
        "Sortowanie: SendDate DESC. "
        "**Wymaga uprawnienia:** `monits.list`"
    ),
    response_description="Paginowana lista monitów",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("monits.list")],
)
async def list_monits(
    current_user: CurrentUser,
    db: DB,
    pagination: Pagination,
    request_id: RequestID,
    debtor_id: Optional[int] = Query(None, description="Filtr po ID dłużnika"),
    status_filter: Optional[str] = Query(None, alias="status", description="Filtr po statusie"),
    channel: Optional[str] = Query(None, description="Filtr: email | sms | letter"),
    date_from: Optional[str] = Query(None, description="Data od (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="Data do (YYYY-MM-DD)"),
    sent_by: Optional[int] = Query(None, description="Filtr po ID operatora"),
):
    from app.services import monit_service

    if status_filter and status_filter.upper() not in _VALID_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": f"Nieprawidłowy status: {status_filter}",
                "errors": [{"field": "status", "message": f"Dozwolone: {', '.join(sorted(_VALID_STATUSES))}"}],
            },
        )

    result = await monit_service.get_list(
        db=db,
        offset=pagination.offset,
        limit=pagination.per_page,
        debtor_id=debtor_id,
        status=status_filter.upper() if status_filter else None,
        channel=channel,
        date_from=date_from,
        date_to=date_to,
        sent_by=sent_by,
    )

    return BaseResponse.ok(
        data={
            "items": result["items"],
            "total": result["total"],
            "page": pagination.page,
            "per_page": pagination.per_page,
            "pages": _pages(result["total"], pagination.per_page),
        },
        app_code="monits.list",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 2: GET /monits/stats
# (PRZED /{id})
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/stats",
    summary="Statystyki wysyłek monitów",
    description=(
        "Zagregowane statystyki: liczba wysłanych/dostarczonych/nieudanych per kanał, "
        "skuteczność (%) per kanał, zestawienie tygodniowe/miesięczne. "
        "Parametr period: week | month | year. "
        "**Wymaga uprawnienia:** `monits.stats`"
    ),
    response_description="Statystyki wysyłek",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("monits.stats")],
)
async def get_monits_stats(
    current_user: CurrentUser,
    db: DB,
    request_id: RequestID,
    period: str = Query("month", pattern="^(week|month|year)$", description="Okres: week | month | year"),
):
    from app.services import monit_service

    stats = await monit_service.get_stats(db=db, period=period)

    return BaseResponse.ok(data=stats, app_code="monits.stats")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 3: GET /monits/queue
# (PRZED /{id})
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/queue",
    summary="Status kolejki ARQ",
    description=(
        "Stan kolejki ARQ: zadania PENDING, IN_PROGRESS, zakończone w ciągu ostatniej godziny. "
        "Dane z Redis (klucze ARQ: `arq:queue:default` + `arq:results:*`). "
        "**Wymaga uprawnienia:** `monits.view_queue`"
    ),
    response_description="Stan kolejki ARQ",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("monits.view_queue")],
)
async def get_queue_status(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request_id: RequestID,
):
    from app.services import monit_service

    queue_status = await monit_service.get_queue_status(redis=redis, db=db)

    return BaseResponse.ok(data=queue_status, app_code="monits.queue_status")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 4: GET /monits/{id}
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/{monit_id}",
    summary="Szczegóły monitu",
    description=(
        "Pełne dane monitu: dłużnik, data wysyłki, kanał, status, "
        "treść (body z szablonu), błąd (jeśli FAILED), operator. "
        "**Wymaga uprawnienia:** `monits.view`"
    ),
    response_description="Szczegóły monitu",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("monits.view")],
    responses={404: {"description": "Monit nie istnieje"}},
)
async def get_monit(
    monit_id: int,
    current_user: CurrentUser,
    db: DB,
    request_id: RequestID,
):
    from app.services import monit_service

    try:
        monit = await monit_service.get_by_id(db=db, monit_id=monit_id)
    except Exception as exc:
        _raise_from_monit_error(exc)

    return BaseResponse.ok(data=monit, app_code="monits.detail")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 5: GET /monits/{id}/pdf
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/{monit_id}/pdf",
    summary="Pobierz PDF monitu",
    description=(
        "Zwraca zapisany PDF monitu jako **StreamingResponse**. "
        "PDF z dysku (`/app/monit_pdfs/`) lub regenerowany jeśli brak. "
        "Content-Disposition: `attachment` — przeglądarka proponuje zapis. "
        "**Wymaga uprawnienia:** `monits.view`"
    ),
    response_description="Strumień PDF monitu",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("monits.view")],
    responses={
        200: {"content": {"application/pdf": {}}, "description": "Strumień PDF"},
        404: {"description": "Monit lub plik PDF nie istnieje"},
    },
)
async def download_monit_pdf(
    monit_id: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request_id: RequestID,
):
    from app.services import monit_service

    try:
        result = await monit_service.get_pdf(db=db, redis=redis, monit_id=monit_id)
    except Exception as exc:
        _raise_from_monit_error(exc)

    pdf_bytes: bytes = result["pdf_bytes"]
    filename: str = result.get("filename", f"monit_{monit_id}.pdf")

    return StreamingResponse(
        content=iter([pdf_bytes]),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(pdf_bytes)),
            "X-Monit-ID": str(monit_id),
            "X-Request-ID": request_id or "",
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 6: POST /monits/{id}/retry
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/{monit_id}/retry",
    summary="Ponów wysyłkę monitu",
    description=(
        "Ponawia wysyłkę nieudanego monitu przez ARQ. "
        "Dozwolone tylko dla statusu FAILED. "
        "Odpowiedź: **HTTP 202 Accepted**. "
        "Status → PENDING, nowy job_id. "
        "**Wymaga uprawnienia:** `monits.retry`"
    ),
    response_description="Potwierdzenie kolejkowania retry",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[require_permission("monits.retry")],
    responses={
        202: {"description": "Retry w kolejce ARQ"},
        400: {"description": "Monit nie ma statusu FAILED"},
        404: {"description": "Monit nie istnieje"},
    },
)
async def retry_monit(
    monit_id: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import monit_service

    try:
        result = await monit_service.retry(
            db=db,
            redis=redis,
            monit_id=monit_id,
            retried_by_id=current_user.id_user,
            ip=client_ip,
        )
    except Exception as exc:
        _raise_from_monit_error(exc)

    logger.info(
        orjson.dumps({
            "event": "api_monit_retry_queued",
            "monit_id": monit_id,
            "job_id": result.get("job_id"),
            "retried_by": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={
            "message": "Ponowna wysyłka dodana do kolejki ARQ.",
            "monit_id": monit_id,
            "job_id": result.get("job_id"),
        },
        app_code="monits.retry_queued",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 7: PUT /monits/{id}/status
# ─────────────────────────────────────────────────────────────────────────────

@router.put(
    "/{monit_id}/status",
    summary="Zmiana statusu monitu",
    description=(
        "Manualna zmiana statusu monitu przez operatora. "
        "Dozwolone przejścia: SENT → DELIVERED|FAILED|PAID, "
        "DELIVERED → PAID, FAILED → PENDING. "
        "AuditLog: `monit_status_changed` z old_status/new_status. "
        "**Wymaga uprawnienia:** `monits.update_status`"
    ),
    response_description="Monit z zaktualizowanym statusem",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("monits.update_status")],
    responses={
        400: {"description": "Niedozwolone przejście statusu"},
        404: {"description": "Monit nie istnieje"},
        422: {"description": "Brak lub nieprawidłowy new_status"},
    },
)
async def update_monit_status(
    monit_id: int,
    request: Request,
    current_user: CurrentUser,
    db: DB,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import monit_service

    body = await _parse_body(request)
    new_status = (body.get("new_status") or "").strip().upper()
    note = str(body.get("note") or "")[:200]

    if not new_status:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "validation.error", "message": "Wymagane pole new_status",
                    "errors": [{"field": "new_status", "message": f"Dozwolone: {', '.join(sorted(_VALID_STATUSES))}"}]},
        )

    if new_status not in _VALID_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "validation.error",
                    "message": f"Nieprawidłowy status: {new_status}",
                    "errors": [{"field": "new_status", "message": f"Dozwolone: {', '.join(sorted(_VALID_STATUSES))}"}]},
        )

    try:
        result = await monit_service.update_status(
            db=db,
            monit_id=monit_id,
            new_status=new_status,
            changed_by_id=current_user.id_user,
            note=note,
            allowed_transitions=_ALLOWED_TRANSITIONS,
        )
    except Exception as exc:
        _raise_from_monit_error(exc)

    logger.info(
        orjson.dumps({
            "event": "api_monit_status_changed",
            "monit_id": monit_id,
            "new_status": new_status,
            "changed_by": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(data=result, app_code="monits.status_updated")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 8: DELETE /monits/{id}/cancel
# ─────────────────────────────────────────────────────────────────────────────

@router.delete(
    "/{monit_id}/cancel",
    summary="Anulowanie zadania z kolejki ARQ",
    description=(
        "Anuluje wysyłkę monitu PENDING z kolejki ARQ. "
        "Status → CANCELLED, zadanie usunięte z Redis. "
        "Jeśli wysyłka już IN_PROGRESS — błąd 409 (za późno). "
        "AuditLog: `monit_cancelled`. "
        "**Wymaga uprawnienia:** `monits.cancel`"
    ),
    response_description="Potwierdzenie anulowania",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("monits.cancel")],
    responses={
        400: {"description": "Monit nie ma statusu PENDING"},
        404: {"description": "Monit nie istnieje"},
        409: {"description": "Wysyłka już IN_PROGRESS — za późno"},
    },
)
async def cancel_monit(
    monit_id: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import monit_service

    try:
        result = await monit_service.cancel(
            db=db,
            redis=redis,
            monit_id=monit_id,
            cancelled_by_id=current_user.id_user,
            ip=client_ip,
        )
    except Exception as exc:
        _raise_from_monit_error(exc)

    logger.warning(
        orjson.dumps({
            "event": "api_monit_cancelled",
            "monit_id": monit_id,
            "cancelled_by": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={
            "message": "Monit anulowany. Zadanie usunięte z kolejki ARQ.",
            "monit_id": monit_id,
            "new_status": "CANCELLED",
        },
        app_code="monits.cancelled",
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


def _raise_from_monit_error(exc: Exception) -> None:
    """Konwertuje wyjątki z monit_service na HTTPException."""
    exc_type = type(exc).__name__

    _MAP: dict[str, tuple[int, str, str]] = {
        "MonitNotFoundError":         (404, "monits.not_found",          "Monit nie istnieje"),
        "TemplateNotFoundError":      (404, "monits.template_not_found", "Szablon monitu nie istnieje"),
        "MonitNotFailedError":        (400, "monits.not_failed",         "Retry możliwy tylko dla FAILED"),
        "MonitNotPendingError":       (400, "monits.not_pending",        "Anulowanie możliwe tylko dla PENDING"),
        "MonitInProgressError":       (409, "monits.in_progress",        "Wysyłka już IN_PROGRESS — za późno"),
        "MonitStatusTransitionError": (400, "monits.invalid_transition", "Niedozwolone przejście statusu"),
        "MonitCooldownError":         (429, "monits.cooldown",           "Dłużnik był monitorowany zbyt niedawno"),
        "MissingContactError":        (422, "monits.missing_contact",    "Brak danych kontaktowych"),
        "MonitPDFNotFoundError":      (404, "monits.pdf_not_found",      "Plik PDF nie istnieje"),
        "WaproConnectionError":       (503, "wapro.unavailable",         "Baza WAPRO niedostępna"),
        "MonitServiceError":          (400, "monits.service_error",      "Błąd operacji na monicie"),
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