# backend/app/api/approval/categories.py
"""
5+2 endpointow — kategorie dokumentow (DELETE dwuetapowy).

DELETE /{id_category}/initiate  — krok 1/2
DELETE /{id_category}/confirm   — krok 2/2

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging
import orjson
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import text
from app.schemas.common import BaseResponse, dt_utc

from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.services.approval_service import _check_module_enabled
from app.api.approval._delete_helpers import generate_delete_token, verify_delete_token

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/categories")
_SCHEMA = "dbo"
_SCOPE = "delete_category"


class CategoryBody(BaseModel):
    category_name: str           = Field(..., min_length=2, max_length=100)
    description:   Optional[str] = Field(None, max_length=500)


class CategoryPatchBody(BaseModel):
    category_name: Optional[str]  = Field(None, min_length=2, max_length=100)
    description:   Optional[str]  = None
    is_active:     Optional[bool] = None


class ConfirmDeleteBody(BaseModel):
    delete_token: str = Field(..., min_length=1)


@router.get("", summary="Lista kategorii dokumentow",
            dependencies=[require_permission("approval.manage_paths")])
async def list_categories(current_user: CurrentUser, db: DB, redis: RedisClient,
                          active_only: bool = True):
    await _check_module_enabled(db, redis)
    where = "WHERE [is_active]=1" if active_only else ""
    rows = await db.execute(
        text(f"SELECT [id_category],[category_name],[description],[is_active],[created_at] "
             f"FROM [{_SCHEMA}].[skw_document_categories] {where} ORDER BY [category_name] ASC"),
    )
    return {"data": [{"id_category": r[0], "category_name": r[1], "description": r[2],
                      "is_active": bool(r[3]), "created_at": dt_utc(r[4])}
                     for r in rows.fetchall()]}


@router.post("", status_code=status.HTTP_201_CREATED, summary="Utworz kategorie",
             responses={409: {"description": "Nazwa juz istnieje"}},
             dependencies=[require_permission("approval.manage_paths")])
async def create_category(body: CategoryBody, current_user: CurrentUser,
                          db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    if (await db.execute(
        text(f"SELECT [id_category] FROM [{_SCHEMA}].[skw_document_categories] "
             f"WHERE [category_name]=:n"), {"n": body.category_name},
    )).fetchone():
        raise HTTPException(status_code=409,
            detail=f"Kategoria '{body.category_name}' juz istnieje.")
    result = await db.execute(
        text(f"INSERT INTO [{_SCHEMA}].[skw_document_categories] "
             f"([category_name],[description]) OUTPUT INSERTED.[id_category] VALUES (:n,:d)"),
        {"n": body.category_name, "d": body.description},
    )
    new_id = result.fetchone()[0]
    await db.commit()
    return {"id_category": new_id, "category_name": body.category_name}


@router.get("/{id_category}", summary="Szczegoly kategorii",
            dependencies=[require_permission("approval.manage_paths")])
async def get_category(id_category: int, current_user: CurrentUser,
                       db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    r = (await db.execute(
        text(f"SELECT [id_category],[category_name],[description],[is_active],"
             f"[created_at],[updated_at] FROM [{_SCHEMA}].[skw_document_categories] "
             f"WHERE [id_category]=:c"), {"c": id_category},
    )).fetchone()
    if not r:
        raise HTTPException(status_code=404, detail="Kategoria nie istnieje.")
    return {"id_category": r[0], "category_name": r[1], "description": r[2],
            "is_active": bool(r[3]),
            "created_at": dt_utc(r[4]),
            "updated_at": dt_utc(r[5])}


@router.patch("/{id_category}", summary="Aktualizacja kategorii",
              dependencies=[require_permission("approval.manage_paths")])
async def update_category(id_category: int, body: CategoryPatchBody,
                          current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    sets = []; params: dict = {"c": id_category}
    if body.category_name is not None:
        sets.append("[category_name]=:n"); params["n"] = body.category_name
    if body.description is not None:
        sets.append("[description]=:d"); params["d"] = body.description
    if body.is_active is not None:
        sets.append("[is_active]=:a"); params["a"] = 1 if body.is_active else 0
    if not sets: raise HTTPException(status_code=422, detail="Brak pol.")
    sets.append("[updated_at]=SYSUTCDATETIME()")
    await db.execute(
        text(f"UPDATE [{_SCHEMA}].[skw_document_categories] SET {','.join(sets)} "
             f"WHERE [id_category]=:c"), params,
    )
    await db.commit()
    return {"id_category": id_category, "updated": True}


@router.delete(
    "/{id_category}/initiate",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Krok 1/2 — Inicjacja dezaktywacji kategorii",
    description="Generuje token JWT (TTL 60s). Uzyj w DELETE /{id_category}/confirm.",
    responses={202: {"description": "Token wygenerowany"}, 404: {"description": "Nie istnieje"}},
    dependencies=[require_permission("approval.manage_paths")],
)
async def initiate_delete_category(
    id_category: int, current_user: CurrentUser,
    db: DB, redis: RedisClient, request: Request,
):
    await _check_module_enabled(db, redis)
    r = (await db.execute(
        text(f"SELECT [category_name] FROM [{_SCHEMA}].[skw_document_categories] "
             f"WHERE [id_category]=:c"), {"c": id_category},
    )).fetchone()
    if not r:
        raise HTTPException(status_code=404, detail="Kategoria nie istnieje.")
    token, ttl = await generate_delete_token(
        redis, entity_id=id_category, scope=_SCOPE,
        initiated_by=current_user.id_user, extra={"category_name": r[0]},
    )
    logger.warning(orjson.dumps({
        "event": "approval_category_delete_initiated", "id_category": id_category,
        "category_name": r[0], "initiated_by": current_user.id_user,
        "ip": request.headers.get("X-Forwarded-For", getattr(request.client, "host", None)),
        "ts": datetime.now(timezone.utc).isoformat(),
    }).decode())
    return {"delete_token": token, "expires_in": ttl,
            "id_category": id_category, "category_name": r[0],
            "message": f"Token wazny {ttl}s. Uzyj w DELETE /approval/categories/{id_category}/confirm."}


@router.delete(
    "/{id_category}/confirm",
    status_code=status.HTTP_200_OK,
    summary="Krok 2/2 — Potwierdzenie dezaktywacji kategorii",
    description="Soft-delete. Wymaga `delete_token` w body JSON.",
    responses={400: {"description": "Nieprawidlowy lub wygasly token"}},
    dependencies=[require_permission("approval.manage_paths")],
)
async def confirm_delete_category(
    id_category: int, body: ConfirmDeleteBody,
    current_user: CurrentUser, db: DB, redis: RedisClient, request: Request,
):
    await _check_module_enabled(db, redis)
    await verify_delete_token(redis, token=body.delete_token,
                              entity_id=id_category, scope=_SCOPE)
    await db.execute(
        text(f"UPDATE [{_SCHEMA}].[skw_document_categories] "
             f"SET [is_active]=0,[updated_at]=SYSUTCDATETIME() WHERE [id_category]=:c"),
        {"c": id_category},
    )
    await db.commit()
    logger.warning(orjson.dumps({
        "event": "approval_category_deleted", "id_category": id_category,
        "deleted_by": current_user.id_user,
        "ip": request.headers.get("X-Forwarded-For", getattr(request.client, "host", None)),
        "ts": datetime.now(timezone.utc).isoformat(),
    }).decode())
    return {"id_category": id_category, "is_active": False, "message": "Kategoria zdezaktywowana."}