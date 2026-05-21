# backend/app/api/approval/delegations.py
"""
5+2 endpointow — delegacje (anulowanie dwuetapowe: initiate/confirm).

  GET    /approval/delegations
  GET    /approval/delegations/all
  POST   /approval/delegations
  DELETE /approval/delegations/{id_delegation}/initiate   — krok 1/2
  DELETE /approval/delegations/{id_delegation}/confirm    — krok 2/2

503 jesli APPROVAL_DELEGATIONS_ENABLED=false.
UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging
import orjson
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text

from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.services.approval_service import _check_module_enabled, _check_feature_flag
from app.services.approval_service_ext import validate_delegation_create, invalidate_group_cache
from app.api.approval._delete_helpers import generate_delete_token, verify_delete_token

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/delegations")
_SCHEMA = "dbo"
_SCOPE = "cancel_delegation"


class DelegationCreateBody(BaseModel):
    id_user_to:  int           = Field(..., gt=0)
    id_group:    Optional[int] = Field(None, gt=0)
    valid_from:  datetime      = Field(...)
    valid_to:    datetime      = Field(...)
    reason:      Optional[str] = Field(None, max_length=500)

    @field_validator("valid_to")
    @classmethod
    def check_dates(cls, v: datetime, info) -> datetime:
        if hasattr(info, "data") and "valid_from" in info.data:
            if v <= info.data["valid_from"]:
                raise ValueError("valid_to musi byc pozniejsze niz valid_from")
        return v


class ConfirmDeleteBody(BaseModel):
    delete_token: str = Field(..., min_length=1)


def _row_to_dict(r) -> dict:
    return {
        "id_delegation": r[0], "id_user_from": r[1], "id_user_to": r[2],
        "id_group": r[3],
        "valid_from": r[4].isoformat() if r[4] else None,
        "valid_to":   r[5].isoformat() if r[5] else None,
        "reason": r[6], "is_active": bool(r[7]),
        "created_at": r[8].isoformat() if r[8] else None,
        "to_username": r[9], "to_fullname": r[10], "group_name": r[11],
    }


@router.get("", summary="Moje delegacje",
            dependencies=[require_permission("approval.manage_delegations")])
async def list_my_delegations(current_user: CurrentUser, db: DB, redis: RedisClient,
                              active_only: bool = Query(True)):
    await _check_module_enabled(db, redis)
    await _check_feature_flag(db, redis, "APPROVAL_DELEGATIONS_ENABLED",
                               error_msg="Delegacje sa wylaczone.")
    where = "AND d.[is_active]=1" if active_only else ""
    rows = await db.execute(
        text(f"SELECT d.[id_delegation],d.[id_user_from],d.[id_user_to],d.[id_group],"
             f"  d.[valid_from],d.[valid_to],d.[reason],d.[is_active],d.[created_at],"
             f"  ut.[Username],ut.[FullName],g.[group_name] "
             f"FROM [{_SCHEMA}].[skw_approval_delegations] d "
             f"LEFT JOIN [{_SCHEMA}].[skw_Users] ut ON ut.[ID_USER]=d.[id_user_to] "
             f"LEFT JOIN [{_SCHEMA}].[skw_approval_groups] g ON g.[id_group]=d.[id_group] "
             f"WHERE d.[id_user_from]=:u {where} ORDER BY d.[valid_from] DESC"),
        {"u": current_user.ID_USER},
    )
    return {"data": [_row_to_dict(r) for r in rows.fetchall()]}


@router.get("/all", summary="Wszystkie delegacje (nadzor)",
            dependencies=[require_permission("approval.supervise")])
async def list_all_delegations(current_user: CurrentUser, db: DB, redis: RedisClient,
                               id_group: Optional[int] = Query(None),
                               active_only: bool = Query(True),
                               page: int = Query(1, ge=1),
                               per_page: int = Query(50, ge=1, le=200)):
    await _check_module_enabled(db, redis)
    await _check_feature_flag(db, redis, "APPROVAL_DELEGATIONS_ENABLED",
                               error_msg="Delegacje sa wylaczone.")
    where = ["1=1"]; params: dict = {"offset": (page-1)*per_page, "limit": per_page}
    if active_only: where.append("d.[is_active]=1")
    if id_group is not None: where.append("d.[id_group]=:g"); params["g"] = id_group
    rows = await db.execute(
        text(f"SELECT d.[id_delegation],d.[id_user_from],d.[id_user_to],d.[id_group],"
             f"  d.[valid_from],d.[valid_to],d.[reason],d.[is_active],d.[created_at],"
             f"  ut.[Username],ut.[FullName],g.[group_name] "
             f"FROM [{_SCHEMA}].[skw_approval_delegations] d "
             f"LEFT JOIN [{_SCHEMA}].[skw_Users] ut ON ut.[ID_USER]=d.[id_user_to] "
             f"LEFT JOIN [{_SCHEMA}].[skw_approval_groups] g ON g.[id_group]=d.[id_group] "
             f"WHERE {' AND '.join(where)} ORDER BY d.[created_at] DESC "
             f"OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY"),
        params,
    )
    return {"data": [_row_to_dict(r) for r in rows.fetchall()], "page": page}


@router.post("", status_code=status.HTTP_201_CREATED,
             summary="Utworz delegacje uprawnien",
             responses={409: {"description": "Konflikt"}, 422: {"description": "Blad walidacji"}},
             dependencies=[require_permission("approval.manage_delegations")])
async def create_delegation(body: DelegationCreateBody, current_user: CurrentUser,
                            db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    await _check_feature_flag(db, redis, "APPROVAL_DELEGATIONS_ENABLED",
                               error_msg="Delegacje sa wylaczone.")
    valid_from = body.valid_from.replace(tzinfo=None)
    valid_to   = body.valid_to.replace(tzinfo=None)
    await validate_delegation_create(
        db, id_user_from=current_user.ID_USER, id_user_to=body.id_user_to,
        id_group=body.id_group, valid_from=valid_from, valid_to=valid_to,
    )
    result = await db.execute(
        text(f"INSERT INTO [{_SCHEMA}].[skw_approval_delegations] "
             f"([id_user_from],[id_user_to],[id_group],[valid_from],[valid_to],[reason],[created_by]) "
             f"OUTPUT INSERTED.[id_delegation] VALUES (:uf,:ut,:g,:vf,:vt,:r,:by)"),
        {"uf": current_user.ID_USER, "ut": body.id_user_to, "g": body.id_group,
         "vf": valid_from, "vt": valid_to, "r": body.reason, "by": current_user.ID_USER},
    )
    new_id = result.fetchone()[0]
    await db.commit()
    if body.id_group:
        await invalidate_group_cache(redis, body.id_group)
    return {"id_delegation": new_id, "message": "Delegacja utworzona."}


@router.delete(
    "/{id_delegation}/initiate",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Krok 1/2 — Inicjacja anulowania delegacji",
    description="Generuje token JWT (TTL 60s). Wlasciciel lub approval.supervise.",
    dependencies=[require_permission("approval.manage_delegations")],
)
async def initiate_cancel_delegation(
    id_delegation: int, current_user: CurrentUser,
    db: DB, redis: RedisClient, request: Request,
):
    await _check_module_enabled(db, redis)
    await _check_feature_flag(db, redis, "APPROVAL_DELEGATIONS_ENABLED",
                               error_msg="Delegacje sa wylaczone.")
    r = (await db.execute(
        text(f"SELECT [id_user_from],[is_active],[id_group] "
             f"FROM [{_SCHEMA}].[skw_approval_delegations] WHERE [id_delegation]=:d"),
        {"d": id_delegation},
    )).fetchone()
    if not r: raise HTTPException(status_code=404, detail="Delegacja nie istnieje.")
    if not bool(r[1]): raise HTTPException(status_code=409, detail="Delegacja juz nieaktywna.")
    from app.core.dependencies import _get_role_permissions
    perms = await _get_role_permissions(current_user.role_id, db, redis)
    if r[0] != current_user.ID_USER and "approval.supervise" not in perms:
        raise HTTPException(status_code=403, detail="Mozesz anulowac tylko swoje delegacje.")
    token, ttl = await generate_delete_token(
        redis, entity_id=id_delegation, scope=_SCOPE,
        initiated_by=current_user.ID_USER,
        extra={"id_group": r[2]},
    )
    logger.warning(orjson.dumps({
        "event": "approval_delegation_cancel_initiated",
        "id_delegation": id_delegation, "initiated_by": current_user.ID_USER,
        "ip": request.headers.get("X-Forwarded-For", getattr(request.client, "host", None)),
        "ts": datetime.now(timezone.utc).isoformat(),
    }).decode())
    return {"delete_token": token, "expires_in": ttl, "id_delegation": id_delegation,
            "message": f"Token wazny {ttl}s. Uzyj w DELETE /approval/delegations/{id_delegation}/confirm."}


@router.delete(
    "/{id_delegation}/confirm",
    status_code=status.HTTP_200_OK,
    summary="Krok 2/2 — Potwierdzenie anulowania delegacji",
    description="Dezaktywuje delegacje (is_active=0). Invaliduje cache grupy. Wymaga `delete_token` w body JSON.",
    responses={400: {"description": "Nieprawidlowy lub wygasly token"}},
    dependencies=[require_permission("approval.manage_delegations")],
)
async def confirm_cancel_delegation(
    id_delegation: int, body: ConfirmDeleteBody,
    current_user: CurrentUser, db: DB, redis: RedisClient, request: Request,
):
    await _check_module_enabled(db, redis)
    payload = await verify_delete_token(redis, token=body.delete_token,
                                        entity_id=id_delegation, scope=_SCOPE)
    id_group = payload.get("id_group")
    await db.execute(
        text(f"UPDATE [{_SCHEMA}].[skw_approval_delegations] "
             f"SET [is_active]=0 WHERE [id_delegation]=:d"),
        {"d": id_delegation},
    )
    await db.commit()
    if id_group:
        await invalidate_group_cache(redis, int(id_group))
    logger.warning(orjson.dumps({
        "event": "approval_delegation_cancelled",
        "id_delegation": id_delegation, "cancelled_by": current_user.ID_USER,
        "ip": request.headers.get("X-Forwarded-For", getattr(request.client, "host", None)),
        "ts": datetime.now(timezone.utc).isoformat(),
    }).decode())
    return {"id_delegation": id_delegation, "is_active": False, "message": "Delegacja anulowana."}