"""
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

"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Optional

import orjson
from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from app.db.wapro import _FUNC_ODSETKI

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


# =============================================================================
# Pomocniki sanityzacji danych wejściowych
# =============================================================================

import re as _re
import orjson as _orjson

# Regex dla numerów faktur: litery, cyfry, /, -, \, spacja, kropka, podkreślenie
_INVOICE_NUMBER_RE = _re.compile(r"^[\w\s/\\.,-]{1,100}$", _re.UNICODE)
_MAX_INVOICE_NUMBERS = 100   # limit per żądanie


def _sanitize_invoice_numbers(raw_list: list) -> tuple[list[str], list[str]]:
    """
    Sanityzuje i waliduje listę numerów faktur z frontendu.

    Returns:
        (clean_list, rejected_list)
        - clean_list:    zaakceptowane, oczyszczone numery
        - rejected_list: odrzucone (puste, za długie, niedozwolone znaki)
    """
    clean: list[str] = []
    rejected: list[str] = []

    for item in raw_list:
        if not isinstance(item, (str, int, float)):
            rejected.append(str(item)[:50])
            continue
        s = str(item).strip()
        if not s:
            continue  # pomiń puste cichy
        if len(s) > 100:
            rejected.append(s[:50] + "…")
            continue
        if not _INVOICE_NUMBER_RE.match(s):
            rejected.append(s[:50])
            continue
        clean.append(s)

    # Deduplikacja — zachowaj kolejność
    seen: set[str] = set()
    deduped: list[str] = []
    for n in clean:
        if n not in seen:
            seen.add(n)
            deduped.append(n)

    return deduped, rejected


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
    min_days_overdue: Optional[int] = Query(
        None, ge=0, le=3650,
        description="Minimalna liczba dni po terminie (DniPrzeterminowania >= N).",
    ),
    max_last_monit_days_ago: Optional[int] = Query(
        None, ge=1, le=3650,
        description="Ostatni monit do rozrachunku starszy niż N dni.",
    ),
    due_date: Optional[date] = Query(
        None,
        description=(
            "Filtr po terminie płatności rozrachunków (TerminPlatnosci). "
            "Format ISO: YYYY-MM-DD. None = brak filtra. "
            "Pokaż tylko kontrahentów mających min. 1 rozrachunek spełniający kryterium."
        ),
    ),
    due_date_mode: str = Query(
        "up_to",
        pattern="^(exact|up_to)$",
        description=(
            "'exact' → TerminPlatnosci = due_date; "
            "'up_to' → TerminPlatnosci ≤ due_date."
        ),
    ),
    paid_filter: str = Query(
        "unpaid_only",
        pattern="^(unpaid_only|all)$",
        description=(
            "'unpaid_only' → tylko niezapłacone (CZY_ROZLICZONY <> 2); "
            "'all' → zapłacone i niezapłacone."
        ),
    ),
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
            min_days_overdue=min_days_overdue,
            max_last_monit_days_ago=max_last_monit_days_ago,
            due_date=due_date,
            due_date_mode=due_date_mode,
            paid_filter=paid_filter,
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
    if channel not in ("email", "sms", "print"):
        _errors.append({"field": "channel", "message": "Dozwolone: email, sms, print"})
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
    dependencies=[require_permission("monits.send_email_bulk")],
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
    if channel not in ("email", "sms", "print"):
        _errors.append({"field": "channel", "message": "Dozwolone: email, sms, print"})
    if not template_id:
        _errors.append({"field": "template_id", "message": "Pole wymagane"})
    if _errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "validation.error", "message": "Błąd walidacji", "errors": _errors},
        )

    # ── Obsługa invoice_numbers_per_debtor (alternatywa dla invoice_ids_per_debtor) ──
    _raw_ids_per_debtor    = body.get("invoice_ids_per_debtor") or None
    _raw_nums_per_debtor   = body.get("invoice_numbers_per_debtor")
    _final_ids_per_debtor  = _raw_ids_per_debtor  # domyślnie — może zostać None

    if _raw_nums_per_debtor is not None:
        if _raw_ids_per_debtor:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code":    "validation.conflict",
                    "message": "Nie można podać jednocześnie invoice_ids_per_debtor "
                               "i invoice_numbers_per_debtor.",
                },
            )
        if not isinstance(_raw_nums_per_debtor, dict):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code":    "validation.error",
                    "message": "invoice_numbers_per_debtor musi być słownikiem "
                               "{debtor_id: [NumerFaktury, ...]}",
                },
            )

        # Normalizuj klucze str → int + sanityzuj numery
        _nums_map: dict[int, list[str]] = {}
        try:
            for _k, _v in _raw_nums_per_debtor.items():
                _did = int(_k)
                if isinstance(_v, list):
                    _clean, _rej = _sanitize_invoice_numbers(_v)
                    if _rej:
                        logger.warning(
                            "send_bulk: odrzucone invoice_numbers dla dłużnika",
                            extra={"debtor_id": _did, "rejected": _rej},
                        )
                    if _clean:
                        _nums_map[_did] = _clean
        except (ValueError, TypeError) as _ke:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code":    "validation.error",
                    "message": f"invoice_numbers_per_debtor: nieprawidłowy klucz: {_ke}",
                },
            )

        if not _nums_map:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code":    "validation.error",
                    "message": "invoice_numbers_per_debtor nie zawiera prawidłowych danych",
                },
            )

        # Rozwiąż wszystkich dłużników współbieżnie
        from app.db.wapro import resolve_invoice_numbers as _resolve_nums
        import asyncio as _asyncio

        _gather_results = await _asyncio.gather(
            *[
                _resolve_nums(debtor_id=_did, invoice_numbers=_nums)
                for _did, _nums in _nums_map.items()
            ],
            return_exceptions=True,
        )

        _resolved_map: dict[int, list[int]] = {}
        _resolution_errors: list[dict] = []

        for _did, _res in zip(_nums_map.keys(), _gather_results):
            if isinstance(_res, Exception):
                logger.error(
                    "send_bulk: resolve_invoice_numbers błąd dla dłużnika",
                    extra={"debtor_id": _did, "error": str(_res)},
                )
                _resolution_errors.append({"debtor_id": _did, "error": str(_res)})
                continue
            _r_ids, _r_not_found, _ = _res
            if not _r_ids:
                _resolution_errors.append({
                    "debtor_id":       _did,
                    "error":           "Żadna faktura nie znaleziona",
                    "invoice_numbers": _nums_map[_did],
                })
                continue
            if _r_not_found:
                logger.warning(
                    "send_bulk: część invoice_numbers nie znaleziona",
                    extra={"debtor_id": _did, "not_found": _r_not_found},
                )
            _resolved_map[_did] = _r_ids

        if _resolution_errors:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code":    "send_bulk.invoice_resolution_failed",
                    "message": "Część faktur nie mogła zostać rozwiązana.",
                    "errors":  _resolution_errors,
                },
            )

        _final_ids_per_debtor = _resolved_map
        logger.info(
            "send_bulk: invoice_numbers_per_debtor rozwiązane",
            extra={
                "debtors_resolved": len(_resolved_map),
                "total_ids":        sum(len(v) for v in _resolved_map.values()),
            },
        )

    try:
        # Parsuj flagi opcjonalne
        _inc_odsetki_bulk = bool(body.get("include_odsetki", True))
        _inc_koszty_bulk  = bool(body.get("include_koszty", True))
        _do_daty_bulk: "date | None" = None
        _do_daty_raw_bulk = body.get("do_daty")
        if _do_daty_raw_bulk:
            try:
                from datetime import date as _date_cls
                _do_daty_bulk = _date_cls.fromisoformat(str(_do_daty_raw_bulk))
            except (ValueError, TypeError):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail={
                        "code":    "validation.error",
                        "message": "Nieprawidłowy format do_daty — oczekiwany ISO: YYYY-MM-DD",
                        "errors":  [{"field": "do_daty", "message": "Format: YYYY-MM-DD"}],
                    },
                )

        bulk_request = monit_service.MonitBulkRequest(
            debtor_ids=debtor_ids,
            monit_type=channel,
            template_id=template_id,
            invoice_ids_per_debtor=_final_ids_per_debtor,
            include_odsetki=_inc_odsetki_bulk,
            include_koszty=_inc_koszty_bulk,
            do_daty=_do_daty_bulk,
        )
        result = await monit_service.send_bulk(
            db=db,
            wapro=wapro,
            redis=redis,
            request=bulk_request,
            triggered_by_user_id=current_user.id_user,
            ip_address=client_ip,
        )
    except monit_service.MonitIntervalBlockedError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code":                "monit.interval_blocked",
                "message":             "Wysylka zablokowana — naruszenie interwalu monitow",
                "blocked_invoice_ids": exc.blocked_invoice_ids,
                "blocked_debtor_ids":  exc.blocked_debtor_ids,
                "interval_days":       exc.interval_days,
                "details":             exc.details,
            },
        )
    except Exception as exc:
        _raise_from_monit_error(exc)

    logger.warning(
        orjson.dumps({
            "event":        "api_monit_bulk_queued",
            "debtor_count": len(debtor_ids),
            "channel":      channel,
            "template_id":  template_id,
            "sent_by":      current_user.id_user,
            "queued_count": result.queued_count,
            "task_id":      result.task_id,
            "request_id":   request_id,
            "ip":           client_ip,
            "ts":           datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={
            "message":            "Zlecenia wysylki dodane do kolejki ARQ.",
            "queued_count":       result.queued_count,
            "monit_ids":          result.monit_ids,
            "task_id":            result.task_id,
            "valid_debtor_count": result.valid_debtor_count,
            "invalid_debtor_ids": result.invalid_debtor_ids,
            "debtor_count":       len(debtor_ids),
            "channel":            channel,
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
    min_days_overdue: int = Query(0, ge=0, le=3650,
        description="Tylko faktury >= N dni po terminie. 0 = wszystkie."),
    page: int = Query(1, ge=1,
        description="Numer strony (1-based)."),
    page_size: int = Query(50, ge=1, le=200,
        description="Liczba faktur na stronie (max 200)."),
    order_by: str = Query(
        "DataWystawienia",
        description="Sortowanie: DataWystawienia | TerminPlatnosci | KwotaBrutto | KwotaPozostala | DniPo",
    ),
    order_dir: str = Query(
        "desc",
        pattern="^(asc|desc)$",
        description="Kierunek: asc | desc.",
    ),
    due_date: Optional[date] = Query(
        None,
        description=(
            "Filtr po terminie płatności (TerminPlatnosci). "
            "Format ISO: YYYY-MM-DD. None = brak filtra."
        ),
    ),
    due_date_mode: str = Query(
        "up_to",
        pattern="^(exact|up_to)$",
        description="'exact' → TerminPlatnosci = due_date; 'up_to' → TerminPlatnosci ≤ due_date.",
    ),
    paid_filter: str = Query(
        "unpaid_only",
        pattern="^(unpaid_only|all)$",
        description="'unpaid_only' → tylko niezapłacone; 'all' → wszystkie faktury.",
    ),
    overdue_filter: str = Query(
        "all",
        pattern="^(all|overdue_only|not_overdue)$",
        description=(
            "'all' → wszystkie (domyślnie); "
            "'overdue_only' → tylko przeterminowane (DniPo > 0); "
            "'not_overdue' → tylko nieprzeteminowane (DniPo = 0). "
            "Błąd 422 przy jednoczesnym overdue_filter='not_overdue' i min_days_overdue > 0."
        ),
    ),
):
    from app.services import debtor_service

    # Walidacja konfliktu parametrów — zwracamy 422 zanim trafi do serwisu
    if overdue_filter == "not_overdue" and min_days_overdue > 0:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=422,
            detail={
                "error": "konflikt_parametrow",
                "message": (
                    f"overdue_filter='not_overdue' jest sprzeczny z "
                    f"min_days_overdue={min_days_overdue}. "
                    "Usuń min_days_overdue lub zmień overdue_filter."
                ),
                "fields": ["overdue_filter", "min_days_overdue"],
            },
        )

    try:
        invoices = await debtor_service.get_invoices(
            wapro=wapro,
            redis=redis,
            db=db,
            debtor_id=debtor_id,
            min_days_overdue=min_days_overdue,
            page=page,
            page_size=page_size,
            order_by=order_by,
            order_dir=order_dir,
            due_date=due_date,
            due_date_mode=due_date_mode,
            paid_filter=paid_filter,
            overdue_filter=overdue_filter,
        )
    except Exception as exc:
        _raise_from_debtor_error(exc)

    return BaseResponse.ok(
        data=invoices,
        app_code="debtors.invoices",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT: POST /debtors/{id}/monit-cost-preview
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/{debtor_id}/monit-cost-preview",
    summary="Kalkulacja kosztów monitu przed wysyłką",
    description=(
        "Zwraca pełne podsumowanie finansowe przed wysłaniem monitu: "
        "kwoty faktur, odsetki per faktura, koszty dodatkowe dla kanału, "
        "łączna kwota do żądania. "
        "Wymaga podania invoice_ids (min. 1). "
        "**Wymaga uprawnienia:** `monits.preview`"
    ),
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("monits.preview")],
)
async def monit_cost_preview(
    request: Request,
    debtor_id: int,
    current_user: CurrentUser,
    wapro: WaproDB,
    redis: RedisClient,
    db: DB,
    request_id: RequestID,
):
    from app.services import koszt_service
    from app.db.wapro import get_odsetki_for_rozrachunki, get_invoices_for_debtor, InvoiceFilterParams

    body = await _parse_body(request)

    channel     = (body.get("channel") or "").strip().lower()
    invoice_ids = body.get("invoice_ids") or []

    # Opcjonalna data końcowa odsetek — None = do dziś
    do_daty_raw = body.get("do_daty")
    do_daty: "date | None" = None
    if do_daty_raw:
        try:
            from datetime import date as _date
            do_daty = _date.fromisoformat(str(do_daty_raw))
        except (ValueError, TypeError):
            raise HTTPException(
                status_code=422,
                detail={
                    "code":    "validation.error",
                    "message": "Nieprawidłowy format do_daty — oczekiwany ISO: YYYY-MM-DD",
                    "errors":  [{"field": "do_daty", "message": "Format: YYYY-MM-DD"}],
                },
            )

    # Flagi — czy uwzględniać odsetki i koszty w kalkulacji
    _include_odsetki = bool(body.get("include_odsetki", True))
    _include_koszty  = bool(body.get("include_koszty", True))

    # ── Walidacja wejścia ────────────────────────────────────────────────────
    _errors = []
    if channel not in ("email", "sms", "print"):
        _errors.append({"field": "channel", "message": "Dozwolone: email, sms, print"})
    if not invoice_ids:
        _errors.append({"field": "invoice_ids", "message": "Wymagana niepusta lista faktur"})
    if _errors:
        raise HTTPException(
            status_code=422,
            detail={"code": "validation.error", "message": "Błąd walidacji", "errors": _errors},
        )

    # Sanityzacja invoice_ids
    clean_ids: list[int] = []
    for raw in invoice_ids:
        try:
            v = int(raw)
            if v > 0:
                clean_ids.append(v)
        except (TypeError, ValueError):
            pass
    clean_ids = list(dict.fromkeys(clean_ids))  # deduplikacja

    if not clean_ids:
        raise HTTPException(
            status_code=422,
            detail={"code": "validation.error", "message": "Brak prawidłowych ID faktur",
                    "errors": [{"field": "invoice_ids", "message": "Wszystkie ID muszą być > 0"}]},
        )

    try:
        # ── Pobierz faktury dłużnika z WAPRO ─────────────────────────────────
        params = InvoiceFilterParams(
            kontrahent_id=debtor_id,
            paid_filter="all",
            limit=500,
            offset=0,
        )
        wapro_result = await get_invoices_for_debtor(params)

        # Zbuduj słownik ID_ROZRACHUNKU → dane faktury
        faktura_map = {
            row["ID_ROZRACHUNKU"]: row
            for row in wapro_result.rows
            if row.get("ID_ROZRACHUNKU") in clean_ids
        }

        # Sprawdź czy wszystkie żądane faktury należą do tego dłużnika
        nieznane = [i for i in clean_ids if i not in faktura_map]
        if nieznane:
            raise HTTPException(
                status_code=422,
                detail={
                    "code":    "preview.invoice_not_found",
                    "message": f"Faktury nie należą do dłużnika {debtor_id} lub nie istnieją.",
                    "errors":  [{"field": "invoice_ids",
                                 "message": f"Nieznane ID: {nieznane}"}],
                },
            )

        # ── Oblicz odsetki per faktura (tylko gdy flaga włączona) ─────────────
        if _include_odsetki:
            odsetki_map = await get_odsetki_for_rozrachunki(clean_ids, do_daty=do_daty)
        else:
            from decimal import Decimal as _Dec
            odsetki_map = {id_: _Dec("0.00") for id_ in clean_ids}

        # ── Pobierz aktywne koszty dodatkowe dla kanału ───────────────────────
        if _include_koszty:
            koszty_dodatkowe = await koszt_service.get_active_for_channel(db, redis, channel)
        else:
            koszty_dodatkowe = []

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(
            "Błąd monit-cost-preview debtor_id=%d: %s",
            debtor_id, exc,
            extra={"debtor_id": debtor_id, "error": str(exc)},
        )
        raise HTTPException(
            status_code=500,
            detail={"code": "preview.error", "message": f"Błąd kalkulacji: {exc}"},
        )

    # ── Buduj pozycje faktur ──────────────────────────────────────────────────
    from decimal import Decimal as D

    pozycje = []
    suma_dlugu    = D("0.00")
    suma_odsetek  = D("0.00")

    for id_roz in clean_ids:
        row      = faktura_map[id_roz]
        pozostala = D(str(row.get("KwotaPozostala") or "0"))
        odsetki   = odsetki_map.get(id_roz, D("0.00"))

        suma_dlugu   += pozostala
        suma_odsetek += odsetki

        pozycje.append({
            "id_rozrachunku": id_roz,
            "numer_faktury":  row.get("NumerFaktury"),
            "data_wystawienia": (
                row["DataWystawienia"].isoformat()
                if row.get("DataWystawienia") else None
            ),
            "termin_platnosci": (
                row["TerminPlatnosci"].isoformat()
                if row.get("TerminPlatnosci") else None
            ),
            "kwota_brutto":   float(row.get("KwotaBrutto") or 0),
            "kwota_pozostala": float(pozostala),
            "dni_po":         row.get("DniPo"),
            "odsetki":        float(odsetki),
        })

    suma_kosztow    = D(str(sum(k["kwota"] for k in koszty_dodatkowe)))
    kwota_calkowita = suma_dlugu + suma_odsetek + suma_kosztow

    logger.info(
        "monit-cost-preview: debtor=%d channel=%s faktury=%d "
        "dlug=%.2f odsetki=%.2f koszty=%.2f calkowita=%.2f "
        "include_odsetki=%s include_koszty=%s",
        debtor_id, channel, len(clean_ids),
        suma_dlugu, suma_odsetek, suma_kosztow, kwota_calkowita,
        _include_odsetki, _include_koszty,
        extra={
            "debtor_id":       debtor_id,
            "channel":         channel,
            "invoice_count":   len(clean_ids),
            "suma_dlugu":      float(suma_dlugu),
            "suma_odsetek":    float(suma_odsetek),
            "suma_kosztow":    float(suma_kosztow),
            "kwota_calkowita": float(kwota_calkowita),
            "include_odsetki": _include_odsetki,
            "include_koszty":  _include_koszty,
            "do_daty":         str(do_daty) if do_daty else None,
            "user_id":         current_user.id_user,
        },
    )

    return BaseResponse.ok(
        data={
            "debtor_id":   debtor_id,
            "channel":     channel,
            "obliczono_at": datetime.now(timezone.utc).isoformat(),
            # Parametry kalkulacji
            "parametry": {
                "include_odsetki": _include_odsetki,
                "include_koszty":  _include_koszty,
                "do_daty":         do_daty.isoformat() if do_daty else None,
            },
            # Pozycje faktur ze szczegółami
            "pozycje": pozycje,
            # Koszty dodatkowe kanału (pusta lista gdy include_koszty=false)
            "koszty_dodatkowe": [
                {
                    "nazwa": k.get("nazwa") or k.get("NazwaKosztu") or "",
                    "kwota": float(k["kwota"]),
                    "typ":   k.get("typ_monitu") or k.get("TypMonitu") or channel,
                }
                for k in koszty_dodatkowe
            ],
            # Podsumowanie finansowe
            "podsumowanie": {
                "suma_dlugu":      float(suma_dlugu),
                "suma_odsetek":    float(suma_odsetek) if _include_odsetki else None,
                "suma_kosztow":    float(suma_kosztow) if _include_koszty  else None,
                "kwota_calkowita": float(kwota_calkowita),
            },
        },
        app_code="debtors.monit_cost_preview",
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

    # ── Walidacja pól wymaganych ──────────────────────────────────────────────
    template_id = body.get("template_id")
    channel     = (body.get("channel") or "email").strip().lower()

    _errors: list[dict] = []
    if not template_id:
        _errors.append({"field": "template_id", "message": "Pole wymagane"})
    if channel not in ("email", "sms", "print"):
        _errors.append({"field": "channel",
                        "message": "Dozwolone: email, sms, print"})
    if _errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "validation.error",
                    "message": "Brak lub błędne wymagane pola",
                    "errors": _errors},
        )

    # ── Sanityzacja invoice_numbers (opcjonalne) ──────────────────────────────
    _raw_numbers = body.get("invoice_numbers")
    _invoice_numbers: list[str] | None = None

    if _raw_numbers is not None:
        if not isinstance(_raw_numbers, list):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code":    "validation.error",
                    "message": "invoice_numbers musi być listą",
                    "errors":  [{"field": "invoice_numbers",
                                 "message": "Oczekiwana lista stringów"}],
                },
            )
        if len(_raw_numbers) > _MAX_INVOICE_NUMBERS:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code":    "validation.error",
                    "message": f"invoice_numbers: max {_MAX_INVOICE_NUMBERS} elementów",
                    "errors":  [{"field": "invoice_numbers",
                                 "message": f"Podano {len(_raw_numbers)}, "
                                            f"limit: {_MAX_INVOICE_NUMBERS}"}],
                },
            )
        _clean, _rejected = _sanitize_invoice_numbers(_raw_numbers)

        if _rejected:
            logger.warning(
                "preview_monit_pdf: odrzucone invoice_numbers po sanityzacji",
                extra={
                    "debtor_id":  debtor_id,
                    "rejected":   _rejected,
                    "accepted":   _clean,
                    "request_id": request_id,
                },
            )

        if not _clean and _raw_numbers:
            # Podano listę, ale wszystkie elementy były nieprawidłowe
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code":    "validation.invoice_numbers_invalid",
                    "message": "Wszystkie podane numery faktur mają nieprawidłowy format",
                    "errors":  [{"field": "invoice_numbers",
                                 "message": "Dozwolone znaki: litery, cyfry, "
                                            "/, -, \\, spacja, kropka. Max 100 znaków."}],
                    "rejected": _rejected,
                },
            )

        _invoice_numbers = _clean if _clean else None

    # ── Parsowanie flag opcjonalnych ──────────────────────────────────────────
    _include_odsetki = bool(body.get("include_odsetki", True))
    _include_koszty  = bool(body.get("include_koszty", True))

    # data końcowa odsetek — format ISO: "2026-05-01"
    _do_daty_preview: "date | None" = None
    _do_daty_raw = body.get("do_daty")
    if _do_daty_raw:
        try:
            from datetime import date as _date_cls
            _do_daty_preview = _date_cls.fromisoformat(str(_do_daty_raw))
        except (ValueError, TypeError):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code":    "validation.error",
                    "message": "Nieprawidłowy format do_daty — oczekiwany ISO: YYYY-MM-DD",
                    "errors":  [{"field": "do_daty",
                                 "message": "Format: YYYY-MM-DD"}],
                },
            )

    logger.info(
        "preview_monit_pdf żądanie",
        extra={
            "debtor_id":        debtor_id,
            "template_id":      template_id,
            "channel":          channel,
            "invoice_numbers":  _invoice_numbers,
            "invoice_count":    len(_invoice_numbers) if _invoice_numbers else None,
            "include_odsetki":  _include_odsetki,
            "include_koszty":   _include_koszty,
            "do_daty":          str(_do_daty_preview) if _do_daty_preview else None,
            "user_id":          current_user.id if hasattr(current_user, "id") else None,
            "request_id":       request_id,
        },
    )

    try:
        pdf_bytes = await monit_service.generate_pdf_preview(
            db=db,
            wapro=wapro,
            redis=redis,
            debtor_id=debtor_id,
            template_id=template_id,
            channel=channel,
            invoice_numbers=_invoice_numbers,
            include_odsetki=_include_odsetki,
            include_koszty=_include_koszty,
            do_daty=_do_daty_preview,
        )
    except Exception as exc:
        # MonitValidationError z powodu invoice_numbers → 422 ze szczegółami
        from app.services.monit_service import MonitValidationError
        if isinstance(exc, MonitValidationError) and _invoice_numbers:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code":            "preview.invoice_not_found",
                    "message":         str(exc),
                    "invoice_numbers": _invoice_numbers,
                    "debtor_id":       debtor_id,
                },
            )
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
    dependencies=[require_permission("monits.send_email_single")],
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
    invoice_ids_raw = body.get("invoice_ids") or []
    # Walidacja i sanityzacja invoice_ids
    invoice_ids: list[int] = []
    for _iv in invoice_ids_raw:
        try:
            _iv_int = int(_iv)
            if _iv_int > 0:
                invoice_ids.append(_iv_int)
        except (TypeError, ValueError):
            pass
    invoice_ids = list(dict.fromkeys(invoice_ids))  # deduplikacja z zachowaniem kolejności

    # ── Alternatywa: invoice_numbers (NumerFaktury jako stringi) ─────────────
    # Nie można podać obu jednocześnie. Jeśli invoice_numbers podane,
    # rozwiązujemy je do ID_ROZRACHUNKU i nadpisujemy invoice_ids.
    _raw_invoice_numbers = body.get("invoice_numbers")
    if _raw_invoice_numbers is not None:
        if invoice_ids:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code":    "validation.conflict",
                    "message": "Nie można podać jednocześnie invoice_ids i invoice_numbers.",
                },
            )
        if not isinstance(_raw_invoice_numbers, list):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code":    "validation.error",
                    "message": "invoice_numbers musi być listą stringów",
                    "errors":  [{"field": "invoice_numbers",
                                 "message": "Oczekiwana lista NumerFaktury"}],
                },
            )

        _clean_nums, _rejected_nums = _sanitize_invoice_numbers(_raw_invoice_numbers)

        if _rejected_nums:
            logger.warning(
                "send_monit: odrzucone invoice_numbers po sanityzacji",
                extra={
                    "debtor_id": debtor_id,
                    "rejected":  _rejected_nums,
                    "accepted":  _clean_nums,
                    "request_id": request_id,
                },
            )

        if not _clean_nums:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code":     "validation.invoice_numbers_invalid",
                    "message":  "Brak prawidłowych numerów faktur po sanityzacji",
                    "rejected": _rejected_nums,
                    "errors":   [{"field": "invoice_numbers",
                                  "message": "Dozwolone: litery, cyfry, "
                                             "/, -, \\, spacja, kropka. Max 100 znaków."}],
                },
            )

        try:
            from app.db.wapro import resolve_invoice_numbers as _resolve_nums
            _resolved_ids, _not_found, _resolved_rows = await _resolve_nums(
                debtor_id=debtor_id,
                invoice_numbers=_clean_nums,
            )
        except Exception as _res_exc:
            logger.error(
                "send_monit: błąd resolve_invoice_numbers",
                extra={"debtor_id": debtor_id, "error": str(_res_exc)},
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail={
                    "code":    "wapro.resolve_error",
                    "message": f"Błąd weryfikacji faktur w WAPRO: {_res_exc}",
                },
            )

        if not _resolved_ids:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code":            "send.invoice_not_found",
                    "message":         f"Żadna z podanych faktur nie należy do "
                                       f"dłużnika ID={debtor_id}.",
                    "invoice_numbers": _clean_nums,
                    "not_found":       _not_found,
                },
            )

        if _not_found:
            logger.warning(
                "send_monit: część invoice_numbers nie znaleziona",
                extra={
                    "debtor_id": debtor_id,
                    "not_found": _not_found,
                    "found":     len(_resolved_ids),
                },
            )

        invoice_ids = _resolved_ids
        logger.info(
            "send_monit: invoice_numbers rozwiązane do invoice_ids",
            extra={
                "debtor_id":       debtor_id,
                "invoice_numbers": _clean_nums,
                "resolved_ids":    _resolved_ids,
                "not_found":       _not_found,
            },
        )

    _errors = []
    if not template_id:
        _errors.append({"field": "template_id", "message": "Pole wymagane"})
    if channel not in ("email", "sms", "print"):
        _errors.append({"field": "channel", "message": "Dozwolone: email, sms, print"})
    if _errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "validation.error", "message": "Błąd walidacji", "errors": _errors},
        )

    try:
        from app.services.monit_service import MonitBulkRequest, MonitIntervalBlockedError
        # Parsuj flagi opcjonalne
        _inc_odsetki = bool(body.get("include_odsetki", True))
        _inc_koszty  = bool(body.get("include_koszty", True))
        _do_daty_send: "date | None" = None
        _do_daty_raw_send = body.get("do_daty")
        if _do_daty_raw_send:
            try:
                from datetime import date as _date_cls
                _do_daty_send = _date_cls.fromisoformat(str(_do_daty_raw_send))
            except (ValueError, TypeError):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail={
                        "code":    "validation.error",
                        "message": "Nieprawidłowy format do_daty — oczekiwany ISO: YYYY-MM-DD",
                        "errors":  [{"field": "do_daty", "message": "Format: YYYY-MM-DD"}],
                    },
                )

        result_bulk = await monit_service.send_bulk(
            db=db,
            wapro=wapro,
            redis=redis,
            request=MonitBulkRequest(
                debtor_ids=[debtor_id],
                monit_type=channel,
                template_id=template_id,
                invoice_ids_per_debtor={debtor_id: invoice_ids} if invoice_ids else None,
                include_odsetki=_inc_odsetki,
                include_koszty=_inc_koszty,
                do_daty=_do_daty_send,
            ),
            triggered_by_user_id=current_user.id_user,
            ip_address=client_ip,
        )
        result = {
            "job_id": result_bulk.task_id if result_bulk else None,
            "monit_id": result_bulk.monit_ids[0] if result_bulk and result_bulk.monit_ids else None,
        }
    except monit_service.MonitIntervalBlockedError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code":                "monit.interval_blocked",
                "message":             "Wysylka zablokowana — naruszenie interwalu monitow",
                "blocked_invoice_ids": exc.blocked_invoice_ids,
                "blocked_debtor_ids":  exc.blocked_debtor_ids,
                "interval_days":       exc.interval_days,
                "details":             exc.details,
            },
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
        # ── monit_service — rzeczywiste klasy ────────────────────────────
        "MonitError":                (403, "monits.blocked",            "Operacja wysyłki zablokowana"),
        "MonitValidationError":      (422, "monits.validation_error",   "Błąd walidacji danych monitu"),
        "MonitNotFoundError":        (404, "monits.not_found",          "Monit nie istnieje"),
        "MonitTemplateNotFoundError":(404, "monits.template_not_found", "Szablon monitu nie istnieje"),
        "MonitStatusTransitionError":(409, "monits.invalid_status",     "Niedozwolona zmiana statusu monitu"),
        "MonitRetryError":           (422, "monits.retry_error",        "Nie można ponowić wysyłki monitu"),
        # ── debtor_service — rzeczywiste klasy ───────────────────────────
        "DebtorError":               (400, "debtors.error",             "Błąd operacji na dłużniku"),
        "DebtorValidationError":     (422, "debtors.validation_error",  "Nieprawidłowe dane dłużnika"),
        "DebtorNotFoundError":       (404, "debtors.not_found",         "Dłużnik nie istnieje w WAPRO"),
        "DebtorWaproError":          (503, "wapro.unavailable",         "Baza WAPRO niedostępna"),
        "DebtorBatchValidationError":(422, "debtors.batch_invalid",     "Część podanych ID dłużników jest nieprawidłowa"),
        # ── stare wpisy zachowane dla kompatybilności ────────────────────
        "TemplateNotFoundError":     (404, "monits.template_not_found", "Szablon monitu nie istnieje"),
        "MonitCooldownError":        (429, "monits.cooldown",           "Dłużnik był monitorowany zbyt niedawno"),
        "MissingContactError":       (422, "monits.missing_contact",    "Brak danych kontaktowych dla wybranego kanału"),
        "WaproConnectionError":      (503, "wapro.unavailable",         "Baza WAPRO niedostępna"),
        "MonitServiceError":         (400, "monits.service_error",      "Błąd operacji wysyłki"),
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

# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT: POST /debtors/{debtor_id}/comments
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/{debtor_id}/comments",
    summary="Dodaj komentarz do dłużnika",
    description=(
        "Tworzy nowy komentarz przypisany do dłużnika. "
        "Właścicielem komentarza zostaje zalogowany użytkownik. "
        "Treść: NFC, max 10 000 znaków. "
        "**Wymaga uprawnienia:** `comments.create`"
    ),
    response_description="Nowo utworzony komentarz",
    status_code=status.HTTP_201_CREATED,
    dependencies=[require_permission("comments.create")],
)
async def create_comment(
    debtor_id: int,
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import comment_service
    from app.services.comment_service import (
        CommentCreateData,
        CommentNotFoundError,
        CommentPermissionError,
        CommentValidationError,
    )

    try:
        body = await request.json()
    except Exception:
        body = {}

    tresc_raw = (body.get("tresc") or "").strip()
    if not tresc_raw:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Błąd walidacji",
                "errors": [{"field": "tresc", "message": "Pole wymagane"}],
            },
        )

    try:
        data = CommentCreateData(debtor_id=debtor_id, tresc=tresc_raw)
    except CommentValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Błąd walidacji",
                "errors": [{"field": "tresc", "message": str(exc)}],
            },
        )

    try:
        result = await comment_service.create(
            db=db,
            data=data,
            author_user_id=current_user.id_user,
            ip_address=client_ip,
        )
    except CommentValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "comments.validation_error",
                "message": str(exc),
                "errors": [{"field": "tresc", "message": str(exc)}],
            },
        )
    except Exception as exc:
        logger.error(
            orjson.dumps({
                "event": "create_comment_error",
                "debtor_id": debtor_id,
                "user_id": current_user.id_user,
                "error": str(exc),
                "request_id": request_id,
                "ip": client_ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"code": "server.internal_error", "message": "Błąd serwera", "errors": []},
        )

    logger.info(
        orjson.dumps({
            "event": "comment_created",
            "comment_id": result.get("id_comment"),
            "debtor_id": debtor_id,
            "user_id": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data=result,
        app_code="comments.created",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT: PUT /debtors/{debtor_id}/comments/{comment_id}
# ─────────────────────────────────────────────────────────────────────────────

@router.put(
    "/{debtor_id}/comments/{comment_id:int}",
    summary="Edytuj komentarz",
    description=(
        "Aktualizuje treść komentarza. "
        "Właściciel może edytować własny komentarz (`comments.edit_own`). "
        "Inni użytkownicy wymagają `comments.edit_any`. "
        "**Wymaga uprawnienia:** `comments.edit_own` lub `comments.edit_any`"
    ),
    response_description="Zaktualizowany komentarz",
    status_code=status.HTTP_200_OK,
)
async def update_comment(
    debtor_id: int,
    comment_id: int,
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import comment_service
    from app.services.comment_service import (
        CommentNotFoundError,
        CommentPermissionError,
        CommentUpdateData,
        CommentValidationError,
    )

    # Sprawdź uprawnienia — user musi mieć edit_own LUB edit_any
    from app.core.dependencies import _get_role_permissions
    user_perms = await _get_role_permissions(current_user.role_id, db, redis)
    has_edit_own = "comments.edit_own" in user_perms
    has_edit_any = "comments.edit_any" in user_perms

    if not has_edit_own and not has_edit_any:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "code": "auth.permission_denied",
                "message": "Wymagane uprawnienie: comments.edit_own lub comments.edit_any",
                "errors": [{"field": "permission", "message": "Brak wymaganego uprawnienia"}],
            },
        )

    try:
        body = await request.json()
    except Exception:
        body = {}

    tresc_raw = (body.get("tresc") or "").strip()
    if not tresc_raw:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Błąd walidacji",
                "errors": [{"field": "tresc", "message": "Pole wymagane"}],
            },
        )

    try:
        data = CommentUpdateData(tresc=tresc_raw)
    except CommentValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Błąd walidacji",
                "errors": [{"field": "tresc", "message": str(exc)}],
            },
        )

    try:
        result = await comment_service.update(
            db=db,
            comment_id=comment_id,
            data=data,
            requesting_user_id=current_user.id_user,
            has_edit_any=has_edit_any,
            ip_address=client_ip,
        )
    except CommentNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "comments.not_found", "message": str(exc), "errors": []},
        )
    except CommentPermissionError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"code": "auth.permission_denied", "message": str(exc), "errors": []},
        )
    except CommentValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "comments.validation_error",
                "message": str(exc),
                "errors": [{"field": "tresc", "message": str(exc)}],
            },
        )
    except Exception as exc:
        logger.error(
            orjson.dumps({
                "event": "update_comment_error",
                "comment_id": comment_id,
                "debtor_id": debtor_id,
                "user_id": current_user.id_user,
                "error": str(exc),
                "request_id": request_id,
                "ip": client_ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"code": "server.internal_error", "message": "Błąd serwera", "errors": []},
        )

    logger.info(
        orjson.dumps({
            "event": "comment_updated",
            "comment_id": comment_id,
            "debtor_id": debtor_id,
            "user_id": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data=result,
        app_code="comments.updated",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT: DELETE /debtors/{debtor_id}/comments/{comment_id}
# Krok 1 — inicjacja usunięcia, zwraca token potwierdzający
# ─────────────────────────────────────────────────────────────────────────────

@router.delete(
    "/{debtor_id}/comments/{comment_id:int}/initiate",
    summary="Inicjuj usunięcie komentarza (krok 1/2)",
    description=(
        "Krok 1 dwuetapowego usunięcia komentarza. "
        "Zwraca token potwierdzający (JWT) ważny przez TTL z konfiguracji. "
        "Właściciel może usunąć własny (`comments.delete_own`). "
        "Inni wymagają `comments.delete_any`. "
        "**Wymaga uprawnienia:** `comments.delete_own` lub `comments.delete_any`"
    ),
    response_description="Token potwierdzający usunięcie",
    status_code=status.HTTP_202_ACCEPTED,
)
async def initiate_delete_comment(
    debtor_id: int,
    comment_id: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import comment_service
    from app.services.comment_service import (
        CommentNotFoundError,
        CommentPermissionError,
    )

    from app.core.dependencies import _get_role_permissions
    user_perms = await _get_role_permissions(current_user.role_id, db, redis)
    has_delete_own = "comments.delete_own" in user_perms
    has_delete_any = "comments.delete_any" in user_perms

    if not has_delete_own and not has_delete_any:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "code": "auth.permission_denied",
                "message": "Wymagane uprawnienie: comments.delete_own lub comments.delete_any",
                "errors": [{"field": "permission", "message": "Brak wymaganego uprawnienia"}],
            },
        )

    try:
        result = await comment_service.initiate_delete(
            db=db,
            redis=redis,
            comment_id=comment_id,
            requesting_user_id=current_user.id_user,
            has_delete_any=has_delete_any,
            ip_address=client_ip,
        )
    except CommentNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "comments.not_found", "message": str(exc), "errors": []},
        )
    except CommentPermissionError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"code": "auth.permission_denied", "message": str(exc), "errors": []},
        )
    except Exception as exc:
        logger.error(
            orjson.dumps({
                "event": "initiate_delete_comment_error",
                "comment_id": comment_id,
                "debtor_id": debtor_id,
                "user_id": current_user.id_user,
                "error": str(exc),
                "request_id": request_id,
                "ip": client_ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"code": "server.internal_error", "message": "Błąd serwera", "errors": []},
        )

    logger.warning(
        orjson.dumps({
            "event": "comment_delete_initiated",
            "comment_id": comment_id,
            "debtor_id": debtor_id,
            "user_id": current_user.id_user,
            "expires_in": result.expires_in,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={
            "token": result.token,
            "expires_in": result.expires_in,
            "comment_id": result.comment_id,
            "debtor_id": result.debtor_id,
            "tresc_preview": result.tresc_preview,
            "message": (
                f"Potwierdź usunięcie wysyłając token na "
                f"DELETE /debtors/{debtor_id}/comments/{comment_id}/confirm"
            ),
        },
        app_code="comments.delete_initiated",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT: DELETE /debtors/{debtor_id}/comments/{comment_id}/confirm
# Krok 2 — potwierdzenie usunięcia tokenem
# ─────────────────────────────────────────────────────────────────────────────

@router.delete(
    "/{debtor_id}/comments/{comment_id:int}/confirm",
    summary="Potwierdź usunięcie komentarza (krok 2/2)",
    description=(
        "Krok 2 dwuetapowego usunięcia. "
        "Wymaga tokenu z kroku 1 w nagłówku `X-Confirm-Token` lub body `{confirm_token}`. "
        "Wykonuje soft-delete (IsActive=0). "
        "**Wymaga uprawnienia:** `comments.delete_own` lub `comments.delete_any`"
    ),
    response_description="Potwierdzenie usunięcia",
    status_code=status.HTTP_200_OK,
)
async def confirm_delete_comment(
    debtor_id: int,
    comment_id: int,
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import comment_service
    from app.services.comment_service import (
        CommentDeleteTokenError,
        CommentNotFoundError,
        CommentPermissionError,
    )

    from app.core.dependencies import _get_role_permissions
    user_perms = await _get_role_permissions(current_user.role_id, db, redis)
    has_delete_own = "comments.delete_own" in user_perms
    has_delete_any = "comments.delete_any" in user_perms

    if not has_delete_own and not has_delete_any:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "code": "auth.permission_denied",
                "message": "Wymagane uprawnienie: comments.delete_own lub comments.delete_any",
                "errors": [{"field": "permission", "message": "Brak wymaganego uprawnienia"}],
            },
        )

    # Token z nagłówka lub body
    confirm_token = request.headers.get("X-Confirm-Token", "").strip()
    if not confirm_token:
        try:
            body = await request.json()
            confirm_token = (body.get("confirm_token") or "").strip()
        except Exception:
            confirm_token = ""

    if not confirm_token:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Błąd walidacji",
                "errors": [{"field": "confirm_token", "message": "Token potwierdzający wymagany"}],
            },
        )

    try:
        result = await comment_service.confirm_delete(
            db=db,
            redis=redis,
            comment_id=comment_id,
            confirm_token=confirm_token,
            requesting_user_id=current_user.id_user,
            ip_address=client_ip,
        )
    except CommentDeleteTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "comments.invalid_token", "message": str(exc), "errors": []},
        )
    except CommentNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "comments.not_found", "message": str(exc), "errors": []},
        )
    except CommentPermissionError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"code": "auth.permission_denied", "message": str(exc), "errors": []},
        )
    except Exception as exc:
        logger.error(
            orjson.dumps({
                "event": "confirm_delete_comment_error",
                "comment_id": comment_id,
                "debtor_id": debtor_id,
                "user_id": current_user.id_user,
                "error": str(exc),
                "request_id": request_id,
                "ip": client_ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"code": "server.internal_error", "message": "Błąd serwera", "errors": []},
        )

    logger.warning(
        orjson.dumps({
            "event": "comment_deleted",
            "comment_id": comment_id,
            "debtor_id": debtor_id,
            "user_id": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data=result,
        app_code="comments.deleted",
    )    