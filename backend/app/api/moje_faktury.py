"""
app/api/moje_faktury.py
=======================
Router endpointów pracownika — moduł Akceptacji Faktur KSeF.

Endpointy (4):
    GET   /moje-faktury             — lista faktur przypisanych do mnie
    GET   /moje-faktury/{id}        — szczegóły przypisanej faktury
    POST  /moje-faktury/{id}/decyzja — akceptacja / odrzucenie / nie_moje
    GET   /moje-faktury/{id}/pdf    — wizualizacja PDF (identyczna jak referent)

Każdy endpoint sprawdza:
    1. modul_akceptacji_faktur_enabled → 403 jeśli false
    2. Uprawnienie faktury.akceptant (rola modułowa)
    3. Uprawnienie akcji (faktury.moje_view, faktury.moje_decyzja etc.)
    4. Czy faktura NAPRAWDĘ jest przypisana do zalogowanego usera

REJESTRACJA w main.py:
    from app.api import moje_faktury
    app.include_router(
        moje_faktury.router,
        prefix="/api/v1",
        tags=["Faktury — Pracownik"],
    )
"""

from __future__ import annotations

import logging
from typing import Annotated

import orjson
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, status
from fastapi.responses import StreamingResponse
from redis.asyncio import Redis

from app.core.dependencies import (
    DB,
    RedisClient,
    Pagination,
    ClientIP,
    RequestID,
    require_permission,
)
from app.core.idempotency import IdempotencyResult, decyzja_guard
from app.schemas.faktura_akceptacja import (
    DecyzjaRequest,
    DecyzjaResponse,
    FakturaDetailResponse,
    MojeFakturyFilter,
)
from app.services import moje_faktury_service as svc
from app.services.config_service import get_config_value

logger = logging.getLogger("app.api.moje_faktury")

router = APIRouter()


# ─────────────────────────────────────────────────────────────────────────────
# Helper: włącznik modułu (DRY — identyczny jak w faktury_akceptacja.py)
# ─────────────────────────────────────────────────────────────────────────────

async def _require_module_enabled(redis: Redis) -> None:
    """403 jeśli moduł wyłączony."""
    try:
        enabled = await get_config_value(
            redis=redis,
            key="modul_akceptacji_faktur_enabled",
            default="false",
        )
        if str(enabled).lower() != "true":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Moduł akceptacji faktur jest wyłączony.",
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(f"Błąd odczytu modul_akceptacji_faktur_enabled: {exc}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Nie można zweryfikować stanu modułu.",
        )


def _require_akceptant(current_user) -> None:
    """Sprawdza rolę modułową faktury.akceptant."""
    if not current_user.has_permission("faktury.akceptant"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Brak uprawnienia: faktury.akceptant",
        )


# ─────────────────────────────────────────────────────────────────────────────
# GET /moje-faktury — lista przypisanych faktur
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/moje-faktury",
    summary="Lista faktur przypisanych do zalogowanego pracownika",
    description=(
        "Domyślnie: tylko oczekujące (is_active=1, status=oczekuje). "
        "Param ?status=archiwum: pokaż wszystkie (w tym zdecydowane). "
        "Cache Redis 60s per user."
    ),
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def list_moje_faktury(
    request:      Request,
    db:           DB,
    redis:        RedisClient,
    pagination:   Pagination,
    client_ip:    ClientIP,
    request_id:   RequestID,
    current_user  = Depends(require_permission("faktury.moje_view")),
    status_param: str | None = Query(default=None, alias="status"),
) -> dict:
    _require_akceptant(current_user)
    await _require_module_enabled(redis)

    logger.info(
        orjson.dumps({
            "event":      "moje_faktury_list",
            "user_id":    current_user.user_id,
            "status":     status_param,
            "page":       pagination.page,
            "limit":      pagination.limit,
            "ip":         client_ip,
            "request_id": request_id,
        }).decode()
    )

    # Walidacja status_param
    if status_param is not None and status_param not in ("archiwum",):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Parametr 'status' dopuszcza tylko wartość 'archiwum'.",
        )

    result = await svc.get_moje_faktury_list(
        db=db,
        redis=redis,
        user_id=current_user.user_id,
        page=pagination.page,
        limit=pagination.limit,
        archiwum=(status_param == "archiwum"),
    )

    return {
        "data":  result["items"],
        "total": result["total"],
        "page":  pagination.page,
        "limit": pagination.limit,
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /moje-faktury/{id} — szczegóły przypisanej faktury
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/moje-faktury/{faktura_id}",
    summary="Szczegóły faktury przypisanej do pracownika",
    description=(
        "Dane z widoku WAPRO + opis/uwagi referenta + pozycje faktury + status przypisania. "
        "Cache Redis 120s. Zwraca 403 jeśli faktura nie jest przypisana do zalogowanego usera."
    ),
    response_model=FakturaDetailResponse,
    status_code=status.HTTP_200_OK,
)
async def get_moja_faktura_detail(
    faktura_id:  Annotated[int, Path(gt=0)],
    db:          DB,
    redis:       RedisClient,
    client_ip:   ClientIP,
    request_id:  RequestID,
    current_user = Depends(require_permission("faktury.moje_details")),
) -> FakturaDetailResponse:
    _require_akceptant(current_user)
    await _require_module_enabled(redis)

    logger.info(
        orjson.dumps({
            "event":      "moje_faktury_detail",
            "user_id":    current_user.user_id,
            "faktura_id": faktura_id,
            "ip":         client_ip,
            "request_id": request_id,
        }).decode()
    )

    return await svc.get_moja_faktura_detail(
        db=db,
        redis=redis,
        faktura_id=faktura_id,
        user_id=current_user.user_id,
    )


# ─────────────────────────────────────────────────────────────────────────────
# POST /moje-faktury/{id}/decyzja — decyzja pracownika
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/moje-faktury/{faktura_id}/decyzja",
    summary="Podjęcie decyzji: akceptacja / odrzucenie / nie_moje",
    description=(
        "Body: {status: 'zaakceptowane'|'odrzucone'|'nie_moje', komentarz: '...'}. "
        "Komentarz wymagany przy odrzuceniu i nie_moje. "
        "Komentarz NIE trafia do AuditLog — tylko SHA256 hash. "
        "Idempotentny: duplikat w 10s → X-Idempotency-Replayed: true. "
        "Przy ostatnim zaakceptowaniu → trigger sagi Fakira."
    ),
    response_model=DecyzjaResponse,
    status_code=status.HTTP_200_OK,
)
async def zapisz_decyzje(
    faktura_id:  Annotated[int, Path(gt=0)],
    body:        DecyzjaRequest,
    request:     Request,
    db:          DB,
    redis:       RedisClient,
    client_ip:   ClientIP,
    request_id:  RequestID,
    current_user = Depends(require_permission("faktury.moje_decyzja")),
    idem:        IdempotencyResult = Depends(decyzja_guard),
) -> DecyzjaResponse:
    _require_akceptant(current_user)
    await _require_module_enabled(redis)

    # Idempotency replay
    if idem.is_replay:
        return idem.cached_response

    logger.info(
        orjson.dumps({
            "event":            "moje_faktury_decyzja",
            "user_id":          current_user.user_id,
            "faktura_id":       faktura_id,
            "status":           body.status,
            "has_komentarz":    bool(body.komentarz),
            "komentarz_hash":   body.komentarz_hash(),
            "ip":               client_ip,
            "request_id":       request_id,
        }).decode()
    )

    result = await svc.zapisz_decyzje(
        db=db,
        redis=redis,
        faktura_id=faktura_id,
        body=body,
        actor_id=current_user.user_id,
        actor_name=current_user.username,
        actor_ip=client_ip,
        request_id=request_id,
    )

    await idem.store_result(result.model_dump())
    return result


# ─────────────────────────────────────────────────────────────────────────────
# GET /moje-faktury/{id}/pdf — wizualizacja PDF
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/moje-faktury/{faktura_id}/pdf",
    summary="Wizualizacja PDF faktury przypisanej do pracownika",
    description=(
        "Identyczny PDF jak dla referenta. "
        "Cache Redis 300s. Zwraca 403 jeśli faktura nie jest przypisana do usera."
    ),
    status_code=status.HTTP_200_OK,
    response_class=StreamingResponse,
)
async def get_pdf_pracownik(
    faktura_id:  Annotated[int, Path(gt=0)],
    db:          DB,
    redis:       RedisClient,
    client_ip:   ClientIP,
    request_id:  RequestID,
    current_user = Depends(require_permission("faktury.view_pdf")),
) -> StreamingResponse:
    _require_akceptant(current_user)
    await _require_module_enabled(redis)

    # Sprawdź włącznik PDF
    pdf_enabled = await get_config_value(
        redis=redis, key="faktury.pdf_enabled", default="true"
    )
    if str(pdf_enabled).lower() != "true":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Generowanie PDF jest tymczasowo wyłączone.",
        )

    # Weryfikacja przypisania (nie może pobrać PDF cudzej faktury)
    is_assigned = await svc.check_przypisanie(
        db=db,
        faktura_id=faktura_id,
        user_id=current_user.user_id,
    )
    if not is_assigned:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Faktura nie jest przypisana do Ciebie.",
        )

    from app.services import faktura_akceptacja_service as ref_svc
    pdf_bytes = await ref_svc.get_faktura_pdf(
        db=db,
        redis=redis,
        faktura_id=faktura_id,
        actor_id=current_user.user_id,
    )

    return StreamingResponse(
        content=iter([pdf_bytes]),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="faktura_{faktura_id}.pdf"',
            "Cache-Control": "no-store",
        },
    )