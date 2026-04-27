# backend/app/api/koszty.py
"""
Router kosztów dodatkowych monitów.

Endpointy:
  GET    /koszty-dodatkowe              — lista (opcjonalny ?typ_monitu=)
  GET    /koszty-dodatkowe/{id}         — szczegóły
  POST   /koszty-dodatkowe              — tworzenie
  PUT    /koszty-dodatkowe/{id}         — edycja
  DELETE /koszty-dodatkowe/{id}         — krok 1 (token)
  DELETE /koszty-dodatkowe/{id}/confirm — krok 2 (wykonanie)
"""

import logging
from datetime import datetime, timezone

import orjson
from fastapi import APIRouter, HTTPException, Path, Query, Request, status

from app.core.dependencies import (
    DB, ClientIP, CurrentUser, RedisClient, RequestID, require_permission,
)
from app.schemas.common import BaseResponse

logger = logging.getLogger(__name__)
router = APIRouter()


def _raise_from_koszt_error(exc: Exception) -> None:
    from app.services.koszt_service import (
        KosztNotFoundError, KosztValidationError, KosztDeleteTokenError,
    )
    if isinstance(exc, KosztNotFoundError):
        raise HTTPException(status_code=404, detail={"code": "koszt.not_found", "message": str(exc)})
    if isinstance(exc, KosztValidationError):
        raise HTTPException(
            status_code=422,
            detail={"code": "koszt.validation_error", "message": str(exc),
                    "errors": [{"field": "_", "message": str(exc)}]},
        )
    if isinstance(exc, KosztDeleteTokenError):
        raise HTTPException(status_code=400, detail={"code": "koszt.invalid_token", "message": str(exc)})
    raise HTTPException(status_code=500, detail={"code": "koszt.error", "message": str(exc)})


async def _parse_body(request: Request) -> dict:
    try:
        return await request.json()
    except Exception:
        raise HTTPException(
            status_code=422,
            detail={"code": "validation.error", "message": "Nieprawidłowy format JSON"},
        )


# ─── GET /koszty-dodatkowe ────────────────────────────────────────────────────

@router.get(
    "",
    summary="Lista kosztów dodatkowych",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("koszty.view_list")],
)
async def lista_kosztow(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request_id: RequestID,
    typ_monitu: str = Query(
        None,
        pattern="^(email|sms|print)$",
        description="Filtr po typie monitu: email | sms | print",
    ),
    tylko_aktywne: bool = Query(True, description="True = tylko aktywne koszty"),
):
    from app.services import koszt_service
    items = await koszt_service.get_list(db, redis, typ_monitu=typ_monitu, tylko_aktywne=tylko_aktywne)
    return BaseResponse.ok(data={"items": items, "total": len(items)}, app_code="koszty.list")


# ─── GET /koszty-dodatkowe/{id} ───────────────────────────────────────────────

@router.get(
    "/{id_kosztu}",
    summary="Szczegóły kosztu dodatkowego",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("koszty.view_details")],
)
async def szczegoly_kosztu(
    id_kosztu: int = Path(..., ge=1),
    current_user: CurrentUser = None,
    db: DB = None,
    redis: RedisClient = None,
    request_id: RequestID = None,
):
    from app.services import koszt_service
    try:
        koszt = await koszt_service.get_by_id(db, redis, id_kosztu)
    except Exception as exc:
        _raise_from_koszt_error(exc)
    return BaseResponse.ok(data=koszt, app_code="koszty.detail")


# ─── POST /koszty-dodatkowe ───────────────────────────────────────────────────

@router.post(
    "",
    summary="Tworzenie kosztu dodatkowego",
    status_code=status.HTTP_201_CREATED,
    dependencies=[require_permission("koszty.create")],
)
async def utworz_koszt(
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import koszt_service
    body = await _parse_body(request)

    _errors = []
    if not body.get("nazwa"):
        _errors.append({"field": "nazwa", "message": "Pole wymagane"})
    if body.get("kwota") is None:
        _errors.append({"field": "kwota", "message": "Pole wymagane"})
    if not body.get("typ_monitu"):
        _errors.append({"field": "typ_monitu", "message": "Pole wymagane"})
    if _errors:
        raise HTTPException(
            status_code=422,
            detail={"code": "validation.error", "message": "Błąd walidacji", "errors": _errors},
        )

    try:
        koszt = await koszt_service.create(
            db, redis,
            raw_nazwa=body.get("nazwa"),
            raw_kwota=body.get("kwota"),
            raw_typ_monitu=body.get("typ_monitu"),
            raw_opis=body.get("opis"),
            created_by_user_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_koszt_error(exc)

    logger.warning(
        orjson.dumps({
            "event": "api_koszt_created", "id": koszt["id_kosztu"],
            "user": current_user.id_user, "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )
    return BaseResponse.ok(data=koszt, app_code="koszty.created", code=201)


# ─── PUT /koszty-dodatkowe/{id} ───────────────────────────────────────────────

@router.put(
    "/{id_kosztu}",
    summary="Edycja kosztu dodatkowego",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("koszty.edit")],
)
async def edytuj_koszt(
    request: Request,
    id_kosztu: int = Path(..., ge=1),
    current_user: CurrentUser = None,
    db: DB = None,
    redis: RedisClient = None,
    client_ip: ClientIP = None,
    request_id: RequestID = None,
):
    from app.services import koszt_service
    body = await _parse_body(request)

    try:
        koszt = await koszt_service.update(
            db, redis, id_kosztu,
            raw_nazwa=body.get("nazwa"),
            raw_kwota=body.get("kwota"),
            raw_typ_monitu=body.get("typ_monitu"),
            raw_opis=body.get("opis"),
            raw_is_active=body.get("is_active"),
            updated_by_user_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_koszt_error(exc)

    return BaseResponse.ok(data=koszt, app_code="koszty.updated")


# ─── DELETE /koszty-dodatkowe/{id} — KROK 1 ──────────────────────────────────

@router.delete(
    "/{id_kosztu}",
    summary="Krok 1/2 — Inicjacja usunięcia kosztu",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[require_permission("koszty.delete")],
)
async def inicjuj_usuniecie_kosztu(
    id_kosztu: int = Path(..., ge=1),
    current_user: CurrentUser = None,
    db: DB = None,
    redis: RedisClient = None,
    client_ip: ClientIP = None,
    request_id: RequestID = None,
):
    from app.services import koszt_service
    try:
        result = await koszt_service.initiate_delete(
            db, redis, id_kosztu,
            initiated_by_user_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_koszt_error(exc)

    logger.warning(
        orjson.dumps({
            "event": "api_koszt_delete_initiated", "id": id_kosztu,
            "user": current_user.id_user, "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )
    return BaseResponse.ok(data=result, app_code="koszty.delete_initiated", code=202)


# ─── DELETE /koszty-dodatkowe/{id}/confirm — KROK 2 ──────────────────────────

@router.delete(
    "/{id_kosztu}/confirm",
    summary="Krok 2/2 — Potwierdzenie usunięcia kosztu",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("koszty.delete")],
)
async def potwierdz_usuniecie_kosztu(
    request: Request,
    id_kosztu: int = Path(..., ge=1),
    current_user: CurrentUser = None,
    db: DB = None,
    redis: RedisClient = None,
    client_ip: ClientIP = None,
    request_id: RequestID = None,
):
    from app.services import koszt_service

    try:
        raw_body = await request.body()
        body = orjson.loads(raw_body) if raw_body else {}
        delete_token = (body.get("delete_token") or "").strip()
    except Exception:
        delete_token = ""

    if not delete_token:
        raise HTTPException(
            status_code=400,
            detail={"code": "koszty.missing_token", "message": "Wymagane pole 'delete_token' w body."},
        )

    try:
        result = await koszt_service.confirm_delete(
            db, redis, id_kosztu, delete_token,
            confirmed_by_user_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_koszt_error(exc)

    logger.warning(
        orjson.dumps({
            "event": "api_koszt_deleted", "id": id_kosztu,
            "user": current_user.id_user, "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )
    return BaseResponse.ok(data=result, app_code="koszty.deleted")