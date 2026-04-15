"""
app/api/faktury_akceptacja.py
=============================
Router endpointów referenta — moduł Akceptacji Faktur KSeF.

Endpointy (7):
    GET    /faktury-akceptacja                      — lista faktur
    POST   /faktury-akceptacja                      — wpuszczenie do obiegu
    PATCH  /faktury-akceptacja/{id}                 — edycja priorytetu/opisu
    POST   /faktury-akceptacja/{id}/reset           — krok 1: inicjacja resetu
    POST   /faktury-akceptacja/{id}/reset/confirm   — krok 2: potwierdzenie resetu
    PATCH  /faktury-akceptacja/{id}/status          — krok 1: force_status
    POST   /faktury-akceptacja/{id}/status/confirm  — krok 2: potwierdzenie force
    GET    /faktury-akceptacja/{id}/historia        — historia zdarzeń
    GET    /faktury-akceptacja/{id}/pdf             — wizualizacja PDF

Każdy endpoint sprawdza:
    1. modul_akceptacji_faktur_enabled (cache Redis 300s) → 403 jeśli false
    2. Wymagane uprawnienia (faktury.view_list + faktury.referent etc.)
    3. Walidację body przez Pydantic schemas
    4. Idempotentność (POST create + oba confirm)

UWAGA: Rejestracja w main.py:
    from app.api import faktury_akceptacja
    app.include_router(
        faktury_akceptacja.router,
        prefix="/api/v1",
        tags=["Faktury — Referent"],
    )
"""

import logging
import uuid
from typing import Annotated, Any, Optional

import orjson
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, status
from fastapi.responses import StreamingResponse
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import (
    DB,
    RedisClient,
    Pagination,
    ClientIP,
    RequestID,
    get_db,
    get_redis,
    get_client_ip,
    get_request_id,
    require_permission,
)
from app.core.idempotency import (
    IdempotencyResult,
    faktury_create_guard,
    reset_confirm_guard,
    force_status_confirm_guard,
)
from app.schemas.faktura_akceptacja import (
    ConfirmTokenResponse,
    DecyzjaRequest,
    FakturaCreateRequest,
    FakturaCreateResponse,
    FakturaDetailResponse,
    FakturaForceStatusConfirmRequest,
    FakturaForceStatusRequest,
    FakturaHistoriaResponse,
    FakturaListFilter,
    FakturaListItem,
    FakturaPatchRequest,
    FakturaResetConfirmRequest,
    FakturaResetRequest,
    FakturaResetResponse,
)
from app.schemas.common import BaseResponse
from app.services import faktura_akceptacja_service as svc
from app.services.config_service import get_config_value

logger = logging.getLogger("app.api.faktury_akceptacja")

router = APIRouter()


# ─────────────────────────────────────────────────────────────────────────────
# Helper: sprawdzenie włącznika modułu
# ─────────────────────────────────────────────────────────────────────────────

async def _require_referent(current_user, db: AsyncSession, redis: Redis) -> None:
    """Sprawdza uprawnienie faktury.referent przez cache/bazę."""
    from app.core.dependencies import _get_role_permissions
    perms = await _get_role_permissions(current_user.role_id, db, redis)
    if "faktury.referent" not in perms:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Brak uprawnienia: faktury.referent",
        )


async def _require_module_enabled(redis: Redis, db=None) -> None:
    """
    Sprawdza czy moduł faktur jest włączony (SystemConfig, cache 300s).
    Rzuca HTTP 403 jeśli wyłączony.
    """
    try:
        enabled = await get_config_value(
            redis=redis,
            key="modul_akceptacji_faktur_enabled",
            default="false",
            db=db,
        )
        if str(enabled).lower() != "true":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    "Moduł akceptacji faktur jest wyłączony. "
                    "Skontaktuj się z administratorem."
                ),
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(f"Błąd odczytu modul_akceptacji_faktur_enabled: {exc}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Nie można zweryfikować stanu modułu — spróbuj ponownie.",
        )


# ─────────────────────────────────────────────────────────────────────────────
# GET /faktury-akceptacja — lista faktur
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "",
    summary="Lista faktur w obiegu akceptacji",
    description=(
        "Zwraca listę faktur: NOWE (z WAPRO bez obiegu) + W_TOKU (w naszej tabeli). "
        "Merge w pamięci Python. Paginacja. Filtrowanie: priorytet, status, data, search."
    ),
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def list_faktury(
    request:      Request,
    db:           DB,
    redis:        RedisClient,
    pagination:   Pagination,
    client_ip:    ClientIP,
    request_id:   RequestID,
    current_user: Annotated[Any, require_permission("faktury.view_list")],
    # Filtry query params
    priorytet:    Optional[str] = Query(default=None),
    status_f:     Optional[str] = Query(default=None, alias="status"),
    search:       Optional[str] = Query(default=None, max_length=100),
    date_from:    Optional[str] = Query(default=None),
    date_to:      Optional[str] = Query(default=None),
) -> dict:
    # Sprawdź uprawnienie referenta
    await _require_referent(current_user, db, redis)

    await _require_module_enabled(redis, db)

    logger.info(
        orjson.dumps({
            "event":      "faktury_list_request",
            "user_id":    current_user.id_user,
            "page":       pagination.page,
            "limit":      pagination.per_page,
            "filters":    {"priorytet": priorytet, "status": status_f, "search": search},
            "ip":         client_ip,
            "request_id": request_id,
            "ts":         __import__("datetime").datetime.utcnow().isoformat(),
        }).decode()
    )

    result = await svc.get_faktury_list(
        db=db,
        redis=redis,
        page=pagination.page,
        limit=pagination.per_page,
        priorytet=priorytet,
        status=status_f,
        search=search,
        date_from=date_from,
        date_to=date_to,
    )

    logger.debug(
        orjson.dumps({
            "event":       "faktury_list_response",
            "user_id":     current_user.id_user,
            "total":       result["total"],
            "page":        pagination.page,
            "items_count": len(result["items"]),
            "request_id":  request_id,
        }).decode()
    )

    return BaseResponse(
        code=200,
        app_code="faktury.view_list",
        errors=[],
        data={
            "data":  result["items"],
            "total": result["total"],
            "page":  pagination.page,
            "limit": pagination.per_page,
        },
    ).model_dump(mode="json")


# ─────────────────────────────────────────────────────────────────────────────
# POST /faktury-akceptacja — wpuszczenie faktury do obiegu
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "",
    summary="Wpuszczenie faktury do obiegu akceptacji",
    description=(
        "Tworzy wpis w skw_faktura_akceptacja + przypisania w skw_faktura_przypisanie. "
        "SSE push (nowa_faktura) do przypisanych pracowników. "
        "Idempotentny: dwa identyczne requesty w 10s zwracają ten sam wynik."
    ),
    response_model=dict,
    status_code=status.HTTP_201_CREATED,
)
async def create_faktura(
    request:      Request,
    body:         FakturaCreateRequest,
    db:           DB,
    redis:        RedisClient,
    client_ip:    ClientIP,
    request_id:   RequestID,
    current_user: Annotated[Any, require_permission("faktury.create")],
    idem:         IdempotencyResult = Depends(faktury_create_guard),
) -> dict:
    await _require_referent(current_user, db, redis)

    await _require_module_enabled(redis, db)

    # Idempotency replay
    if idem.is_replay:
        return BaseResponse(
            code=201,
            app_code="faktury.create",
            errors=[],
            data=idem.cached_response,
        ).model_dump(mode="json")

    logger.info(
        orjson.dumps({
            "event":       "faktura_create_request",
            "user_id":     current_user.id_user,
            "numer_ksef":  body.numer_ksef,
            "priorytet":   body.priorytet,
            "user_ids":    body.user_ids,
            "ip":          client_ip,
            "request_id":  request_id,
        }).decode()
    )

    result = await svc.create_faktura_akceptacja(
        db=db,
        redis=redis,
        body=body,
        actor_id=current_user.id_user,
        actor_name=current_user.username,
        actor_full_name=current_user.full_name or "",
        actor_ip=client_ip,
        request_id=request_id,
    )

    result_dict = result.model_dump(mode="json")
    await idem.store_result(result_dict, status_code=201)
    return BaseResponse(
        code=201,
        app_code="faktury.create",
        errors=[],
        data=result_dict,
    ).model_dump(mode="json")


@router.get(
    "/{faktura_id}",
    summary="Szczegóły faktury w obiegu akceptacji",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def get_faktura_detail(
    faktura_id:   Annotated[int, Path(gt=0)],
    db:           DB,
    redis:        RedisClient,
    client_ip:    ClientIP,
    request_id:   RequestID,
    current_user: Annotated[Any, require_permission("faktury.view_details")],
) -> dict:
    await _require_referent(current_user, db, redis)
    await _require_module_enabled(redis, db)

    result = await svc.get_faktura_detail(
        db=db,
        redis=redis,
        faktura_id=faktura_id,
        actor_id=current_user.id_user,
    )
    data = result.model_dump(mode="json") if hasattr(result, "model_dump") else result
    return BaseResponse(
        code=200,
        app_code="faktury.view_details",
        errors=[],
        data=data,
    ).model_dump(mode="json")



# ─────────────────────────────────────────────────────────────────────────────
# PATCH /faktury-akceptacja/{id} — edycja priorytetu/opisu/uwag
# ─────────────────────────────────────────────────────────────────────────────

@router.patch(
    "/{faktura_id}",
    summary="Edycja priorytetu, opisu i uwag faktury",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def patch_faktura(
    faktura_id:   Annotated[int, Path(gt=0)],
    body:         FakturaPatchRequest,
    db:           DB,
    redis:        RedisClient,
    client_ip:    ClientIP,
    request_id:   RequestID,
    current_user: Annotated[Any, require_permission("faktury.edit")],
) -> dict:
    await _require_referent(current_user, db, redis)

    await _require_module_enabled(redis, db)

    result = await svc.patch_faktura(
        db=db,
        redis=redis,
        faktura_id=faktura_id,
        body=body,
        actor_id=current_user.id_user,
        actor_name=current_user.username,
        actor_full_name=current_user.full_name or "",
        actor_ip=client_ip,
        request_id=request_id,
    )
    data = result.model_dump(mode="json") if hasattr(result, "model_dump") else result
    return BaseResponse(
        code=200,
        app_code="faktury.edit",
        errors=[],
        data=data,
    ).model_dump(mode="json")


# ─────────────────────────────────────────────────────────────────────────────
# POST /faktury-akceptacja/{id}/reset — krok 1: inicjacja resetu
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/{faktura_id}/reset",
    summary="Inicjacja resetu przypisań (krok 1 — zwraca confirm_token)",
    response_model=dict,
    status_code=status.HTTP_202_ACCEPTED,
)
async def initiate_reset(
    faktura_id:   Annotated[int, Path(gt=0)],
    body:         FakturaResetRequest,
    db:           DB,
    redis:        RedisClient,
    client_ip:    ClientIP,
    request_id:   RequestID,
    current_user: Annotated[Any, require_permission("faktury.reset")],
) -> dict:
    await _require_referent(current_user, db, redis)

    await _require_module_enabled(redis, db)

    # Sprawdź włącznik resetu
    reset_enabled = await get_config_value(
        redis=redis, key="faktury.reset_przypisania_enabled", default="true"
    )
    if str(reset_enabled).lower() != "true":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Reset przypisań jest tymczasowo wyłączony przez administratora.",
        )

    result = await svc.initiate_reset_przypisania(
        db=db,
        redis=redis,
        faktura_id=faktura_id,
        body=body,
        actor_id=current_user.id_user,
        actor_name=current_user.username,
        actor_ip=client_ip,
        request_id=request_id,
    )
    data = result.model_dump(mode="json") if hasattr(result, "model_dump") else result
    return BaseResponse(
        code=202,
        app_code="faktury.reset",
        errors=[],
        data=data,
    ).model_dump(mode="json")


# ─────────────────────────────────────────────────────────────────────────────
# POST /faktury-akceptacja/{id}/reset/confirm — krok 2
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/{faktura_id}/reset/confirm",
    summary="Potwierdzenie resetu przypisań (krok 2)",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def confirm_reset(
    faktura_id:   Annotated[int, Path(gt=0)],
    body:         FakturaResetConfirmRequest,
    request:      Request,
    db:           DB,
    redis:        RedisClient,
    client_ip:    ClientIP,
    request_id:   RequestID,
    current_user: Annotated[Any, require_permission("faktury.reset")],
    idem:         IdempotencyResult = Depends(reset_confirm_guard),
)  -> dict:
    await _require_referent(current_user, db, redis)

    await _require_module_enabled(redis, db)

    if idem.is_replay:
        return BaseResponse(
            code=200,
            app_code="faktury.reset",
            errors=[],
            data=idem.cached_response,
        ).model_dump(mode="json")

    result = await svc.confirm_reset_przypisania(
        db=db,
        redis=redis,
        faktura_id=faktura_id,
        confirm_token=body.confirm_token,
        actor_id=current_user.id_user,
        actor_name=current_user.username,
        actor_full_name=current_user.full_name or "",
        actor_ip=client_ip,
        request_id=request_id,
    )
    result_dict = result.model_dump(mode="json")
    await idem.store_result(result_dict)
    return BaseResponse(
        code=200,
        app_code="faktury.reset",
        errors=[],
        data=result_dict,
    ).model_dump(mode="json")


# ─────────────────────────────────────────────────────────────────────────────
# PATCH /faktury-akceptacja/{id}/status — krok 1: force_status
# ─────────────────────────────────────────────────────────────────────────────

@router.patch(
    "/{faktura_id}/status",
    summary="Wymuszenie zmiany statusu faktury (krok 1 — zwraca confirm_token)",
    response_model=dict,
    status_code=status.HTTP_202_ACCEPTED,
)
async def initiate_force_status(
    faktura_id:   Annotated[int, Path(gt=0)],
    body:         FakturaForceStatusRequest,
    db:           DB,
    redis:        RedisClient,
    client_ip:    ClientIP,
    request_id:   RequestID,
    current_user: Annotated[Any, require_permission("faktury.force_status")],
) -> dict:
    await _require_referent(current_user, db, redis)

    await _require_module_enabled(redis, db)

    force_enabled = await get_config_value(
        redis=redis, key="faktury.force_status_enabled", default="true"
    )
    if str(force_enabled).lower() != "true":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Force-akceptacja jest tymczasowo wyłączona przez administratora.",
        )

    result = await svc.initiate_force_status(
        db=db,
        redis=redis,
        faktura_id=faktura_id,
        body=body,
        actor_id=current_user.id_user,        
        actor_name=current_user.username, 
        actor_ip=client_ip,
        request_id=request_id
    )
    data = result.model_dump(mode="json") if hasattr(result, "model_dump") else result
    return BaseResponse(
        code=202,
        app_code="faktury.force_status",
        errors=[],
        data=data,
    ).model_dump(mode="json")


# ─────────────────────────────────────────────────────────────────────────────
# POST /faktury-akceptacja/{id}/status/confirm — krok 2
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/{faktura_id}/status/confirm",
    summary="Potwierdzenie zmiany statusu (krok 2)",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def confirm_force_status(
    faktura_id:   Annotated[int, Path(gt=0)],
    body:         FakturaForceStatusConfirmRequest,
    request:      Request,
    db:           DB,
    redis:        RedisClient,
    client_ip:    ClientIP,
    request_id:   RequestID,
    current_user  = require_permission("faktury.force_status"),
    idem:         IdempotencyResult = Depends(force_status_confirm_guard),
) -> dict:
    await _require_referent(current_user, db, redis)

    await _require_module_enabled(redis, db)

    if idem.is_replay:
        return BaseResponse(
            code=200,
            app_code="faktury.force_status",
            errors=[],
            data=idem.cached_response,
        ).model_dump(mode="json")

    result = await svc.confirm_force_status(
        db=db,
        redis=redis,
        faktura_id=faktura_id,
        confirm_token=body.confirm_token,  # 1. Zmień "token" na "confirm_token"
        actor_id=current_user.id_user,    # 2. Upewnij się, że pole to id_user lub user_id
        actor_name=current_user.username,  # 3. DODAJ ten brakujący argument
        actor_full_name=current_user.full_name or "",
        actor_ip=client_ip,               # 4. DODAJ ten brakujący argument
        request_id=request_id
    )

    await idem.store_result(result)
    return BaseResponse(
        code=200,
        app_code="faktury.force_status",
        errors=[],
        data=result,
    ).model_dump(mode="json")


# ─────────────────────────────────────────────────────────────────────────────
# GET /faktury-akceptacja/{id}/historia — historia zdarzeń
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/{faktura_id}/historia",
    summary="Historia zdarzeń faktury (skw_faktura_log)",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def get_historia(
    faktura_id:   Annotated[int, Path(gt=0)],
    db:           DB,
    redis:        RedisClient,
    client_ip:    ClientIP,
    request_id:   RequestID,
    current_user: Annotated[Any, require_permission("faktury.view_historia")],
) -> dict:
    await _require_referent(current_user, db, redis)
    await _require_module_enabled(redis, db)

    result = await svc.get_historia(db=db, redis=redis, faktura_id=faktura_id)
    data = result.model_dump(mode="json") if hasattr(result, "model_dump") else result
    return BaseResponse(
        code=200,
        app_code="faktury.view_historia",
        errors=[],
        data=data,
    ).model_dump(mode="json")


# ─────────────────────────────────────────────────────────────────────────────
# GET /faktury-akceptacja/{id}/pdf — wizualizacja PDF
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/{faktura_id}/pdf",
    summary="Wizualizacja PDF faktury (karta akceptacji)",
    status_code=status.HTTP_200_OK,
    response_class=StreamingResponse,
)
async def get_pdf_referent(
    faktura_id:   Annotated[int, Path(gt=0)],
    db:           DB,
    redis:        RedisClient,
    client_ip:    ClientIP,
    request_id:   RequestID,
    current_user: Annotated[Any, require_permission("faktury.view_pdf")],
) -> StreamingResponse:
    await _require_module_enabled(redis, db)

    # Sprawdź włącznik PDF
    pdf_enabled = await get_config_value(
        redis=redis, key="faktury.pdf_enabled", default="true"
    )
    if str(pdf_enabled).lower() != "true":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Generowanie PDF jest tymczasowo wyłączone.",
        )

    pdf_bytes = await svc.get_faktura_pdf(
        db=db,
        redis=redis,
        faktura_id=faktura_id,
        actor_id=current_user.id_user,
    )

    return StreamingResponse(
        content=iter([pdf_bytes]),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="faktura_{faktura_id}.pdf"',
            "Cache-Control": "no-store",
        },
    )

# ─────────────────────────────────────────────────────────────────────────────
# GET /faktury-akceptacja/ksef/{ksef_id} — szczegóły faktury po KSEF_ID
# z granularnym maskowaniem pól (whitelist faktury.pole.*)
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/ksef/{ksef_id}",
    summary="Szczegóły faktury po KSEF_ID z granularnym maskowaniem pól",
    description=(
        "Zwraca nagłówek i pozycje faktury z WAPRO. "
        "Widoczność każdego pola kontrolowana osobnym uprawnieniem faktury.pole.*. "
        "Logika whitelist: pole widoczne tylko gdy rola MA uprawnienie. "
        "Zawsze widoczne: numer_ksef, numer, id_buf_dokument, numer_pozycji. "
        "Wymaga: faktury.view_details + faktury.referent."
    ),
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def get_faktura_by_ksef(
    ksef_id:      Annotated[str, Path(
        min_length=10,
        max_length=100,
        description="Identyfikator KSeF faktury",
    )],
    db:           DB,
    redis:        RedisClient,
    client_ip:    ClientIP,
    request_id:   RequestID,
    current_user: Annotated[Any, require_permission("faktury.view_details")],
) -> dict:
    await _require_referent(current_user, db, redis)
    await _require_module_enabled(redis, db)

    # Pobierz uprawnienia roli z cache Redis / bazy
    # current_user to ORM User — nie ma .permissions
    # używamy tego samego helpera co require_permission()
    from app.core.dependencies import _get_role_permissions
    role_permissions: set[str] = await _get_role_permissions(
        current_user.role_id, db, redis
    )

    logger.info(
        orjson.dumps({
            "event":      "ksef_detail_request",
            "user_id":    current_user.id_user,
            "ksef_id":    ksef_id,
            "ip":         client_ip,
            "request_id": request_id,
            "pole_perms": [
                p for p in role_permissions
                if p.startswith("faktury.pole.")
            ],
        }).decode()
    )

    result = await svc.get_faktura_ksef_detail(
        db=db,
        redis=redis,
        ksef_id=ksef_id,
        permissions=list(role_permissions),
    )

    return BaseResponse(
        code=200,
        app_code="faktury.view_details",
        errors=[],
        data=result,
    ).model_dump(mode="json")