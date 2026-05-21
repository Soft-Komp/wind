# backend/app/api/approval/filters.py
"""
8+2 endpointow — filtry automatyczne (DELETE filtru dwuetapowy).

KRYTYCZNE kolejnosci FastAPI:
  PATCH  /reorder          PRZED  PATCH /{id_filter}
  DELETE /{id_filter}/initiate i /{id_filter}/confirm — nie koliduja z innymi
  DELETE /{id_filter}/conditions/{id_condition} — bezposredni (sub-zasob)

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging
import orjson
import re
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text

from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.services.approval_service import _check_module_enabled
from app.api.approval._delete_helpers import generate_delete_token, verify_delete_token

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/filters")
_SCHEMA = "dbo"
_SCOPE = "delete_filter"
_VALID_OPERATORS = frozenset({"eq", "neq", "contains", "gt", "lt", "gte", "lte"})
_FUNC_RE = re.compile(r"^[a-zA-Z0-9_]+$")


class FilterCreateBody(BaseModel):
    filter_name:        str           = Field(..., min_length=2, max_length=200)
    filter_type:        str           = Field(..., pattern="^(standard|universal)$")
    id_path:            int           = Field(..., gt=0)
    id_source:          Optional[int] = Field(None, gt=0)
    priority:           int           = Field(100, ge=1, le=9999)
    universal_function: Optional[str] = Field(None, max_length=128)

    @field_validator("universal_function")
    @classmethod
    def validate_func(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not _FUNC_RE.match(v):
            raise ValueError("Nazwa funkcji SQL: ^[a-zA-Z0-9_]+$")
        return v


class FilterPatchBody(BaseModel):
    filter_name:        Optional[str]  = Field(None, min_length=2, max_length=200)
    priority:           Optional[int]  = Field(None, ge=1, le=9999)
    is_active:          Optional[bool] = None
    universal_function: Optional[str]  = Field(None, max_length=128)

    @field_validator("universal_function")
    @classmethod
    def validate_func(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not _FUNC_RE.match(v):
            raise ValueError("Niepoprawna nazwa funkcji SQL.")
        return v


class ReorderItem(BaseModel):
    id_filter: int = Field(..., gt=0)
    priority:  int = Field(..., ge=1, le=9999)


class ConditionCreateBody(BaseModel):
    field_name:  str = Field(..., min_length=1, max_length=100)
    operator:    str = Field(...)
    field_value: str = Field(..., max_length=500)

    @field_validator("operator")
    @classmethod
    def validate_op(cls, v: str) -> str:
        if v not in _VALID_OPERATORS:
            raise ValueError(f"Operator musi byc jednym z: {sorted(_VALID_OPERATORS)}")
        return v


class ConfirmDeleteBody(BaseModel):
    delete_token: str = Field(..., min_length=1)


@router.get("", summary="Lista filtrow automatycznych",
            description="Zwraca filtry posortowane priority DESC.",
            dependencies=[require_permission("approval.manage_filters")])
async def list_filters(current_user: CurrentUser, db: DB, redis: RedisClient,
                       id_source: Optional[int] = None, active_only: bool = True):
    await _check_module_enabled(db, redis)
    where = ["1=1"]; params: dict = {}
    if active_only: where.append("[is_active]=1")
    if id_source is not None:
        where.append("([id_source]=:src OR [id_source] IS NULL)"); params["src"] = id_source
    rows = await db.execute(
        text(
            f"SELECT f.[id_filter],f.[filter_name],f.[filter_type],f.[id_path],"
            f"  f.[id_source],f.[priority],f.[is_active],f.[universal_function],"
            f"  p.[path_name],"
            f"  (SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_filter_conditions] c "
            f"   WHERE c.[id_filter]=f.[id_filter]) AS condition_count "
            f"FROM [{_SCHEMA}].[skw_approval_filters] f "
            f"LEFT JOIN [{_SCHEMA}].[skw_approval_paths] p ON p.[id_path]=f.[id_path] "
            f"WHERE {' AND '.join(where)} ORDER BY f.[priority] DESC"
        ), params,
    )
    return {"data": [{"id_filter": r[0], "filter_name": r[1], "filter_type": r[2],
                      "id_path": r[3], "id_source": r[4], "priority": r[5],
                      "is_active": bool(r[6]), "universal_function": r[7],
                      "path_name": r[8], "condition_count": r[9]}
                     for r in rows.fetchall()]}


@router.post("", status_code=status.HTTP_201_CREATED, summary="Utworz filtr automatyczny",
             dependencies=[require_permission("approval.manage_filters")])
async def create_filter(body: FilterCreateBody, current_user: CurrentUser,
                        db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    if body.filter_type == "universal" and not body.universal_function:
        raise HTTPException(status_code=422, detail="Dla universal wymagane universal_function.")
    result = await db.execute(
        text(f"INSERT INTO [{_SCHEMA}].[skw_approval_filters] "
             f"([filter_name],[filter_type],[id_path],[id_source],[priority],[universal_function]) "
             f"OUTPUT INSERTED.[id_filter] VALUES (:fn,:ft,:ip,:is,:pr,:uf)"),
        {"fn": body.filter_name, "ft": body.filter_type, "ip": body.id_path,
         "is": body.id_source, "pr": body.priority, "uf": body.universal_function},
    )
    new_id = result.fetchone()[0]
    await db.commit()
    return {"id_filter": new_id, "filter_name": body.filter_name, "priority": body.priority}


# KRYTYCZNE: /reorder PRZED /{id_filter}
@router.patch("/reorder", summary="Zmien kolejnosc filtrow (bulk priority)",
              dependencies=[require_permission("approval.manage_filters")])
async def reorder_filters(items: list[ReorderItem], current_user: CurrentUser,
                          db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    if not items: raise HTTPException(status_code=422, detail="Lista nie moze byc pusta.")
    updated = 0
    for item in items:
        result = await db.execute(
            text(f"UPDATE [{_SCHEMA}].[skw_approval_filters] "
                 f"SET [priority]=:pr,[updated_at]=SYSUTCDATETIME() WHERE [id_filter]=:f"),
            {"pr": item.priority, "f": item.id_filter},
        )
        updated += result.rowcount
    await db.commit()
    return {"updated": updated}


@router.get("/{id_filter}", summary="Szczegoly filtru z warunkami",
            dependencies=[require_permission("approval.manage_filters")])
async def get_filter(id_filter: int, current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    r = (await db.execute(
        text(f"SELECT [id_filter],[filter_name],[filter_type],[id_path],[id_source],"
             f"[priority],[is_active],[universal_function],[created_at],[updated_at] "
             f"FROM [{_SCHEMA}].[skw_approval_filters] WHERE [id_filter]=:f"),
        {"f": id_filter},
    )).fetchone()
    if not r: raise HTTPException(status_code=404, detail="Filtr nie istnieje.")
    cond_rows = await db.execute(
        text(f"SELECT [id_condition],[field_name],[operator],[field_value] "
             f"FROM [{_SCHEMA}].[skw_approval_filter_conditions] WHERE [id_filter]=:f "
             f"ORDER BY [id_condition] ASC"), {"f": id_filter},
    )
    return {"id_filter": r[0], "filter_name": r[1], "filter_type": r[2],
            "id_path": r[3], "id_source": r[4], "priority": r[5],
            "is_active": bool(r[6]), "universal_function": r[7],
            "created_at": r[8].isoformat() if r[8] else None,
            "conditions": [{"id_condition": cr[0], "field_name": cr[1],
                             "operator": cr[2], "field_value": cr[3]}
                            for cr in cond_rows.fetchall()]}


@router.patch("/{id_filter}", summary="Aktualizacja filtru",
              dependencies=[require_permission("approval.manage_filters")])
async def update_filter(id_filter: int, body: FilterPatchBody,
                        current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    sets = []; params: dict = {"f": id_filter}
    if body.filter_name is not None: sets.append("[filter_name]=:fn"); params["fn"] = body.filter_name
    if body.priority is not None: sets.append("[priority]=:pr"); params["pr"] = body.priority
    if body.is_active is not None: sets.append("[is_active]=:a"); params["a"] = 1 if body.is_active else 0
    if body.universal_function is not None: sets.append("[universal_function]=:uf"); params["uf"] = body.universal_function
    if not sets: raise HTTPException(status_code=422, detail="Brak pol.")
    sets.append("[updated_at]=SYSUTCDATETIME()")
    await db.execute(
        text(f"UPDATE [{_SCHEMA}].[skw_approval_filters] SET {','.join(sets)} WHERE [id_filter]=:f"),
        params,
    )
    await db.commit()
    return {"id_filter": id_filter, "updated": True}


@router.delete(
    "/{id_filter}/initiate",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Krok 1/2 — Inicjacja usuniecia filtru",
    description="Generuje token JWT (TTL 60s). Uzyj w DELETE /{id_filter}/confirm.",
    responses={202: {"description": "Token wygenerowany"}, 404: {"description": "Nie istnieje"}},
    dependencies=[require_permission("approval.manage_filters")],
)
async def initiate_delete_filter(
    id_filter: int, current_user: CurrentUser,
    db: DB, redis: RedisClient, request: Request,
):
    await _check_module_enabled(db, redis)
    r = (await db.execute(
        text(f"SELECT [filter_name] FROM [{_SCHEMA}].[skw_approval_filters] WHERE [id_filter]=:f"),
        {"f": id_filter},
    )).fetchone()
    if not r: raise HTTPException(status_code=404, detail="Filtr nie istnieje.")
    token, ttl = await generate_delete_token(
        redis, entity_id=id_filter, scope=_SCOPE,
        initiated_by=current_user.ID_USER, extra={"filter_name": r[0]},
    )
    logger.warning(orjson.dumps({
        "event": "approval_filter_delete_initiated", "id_filter": id_filter,
        "filter_name": r[0], "initiated_by": current_user.ID_USER,
        "ip": request.headers.get("X-Forwarded-For", getattr(request.client, "host", None)),
        "ts": datetime.now(timezone.utc).isoformat(),
    }).decode())
    return {"delete_token": token, "expires_in": ttl, "id_filter": id_filter,
            "filter_name": r[0],
            "message": f"Token wazny {ttl}s. Uzyj w DELETE /approval/filters/{id_filter}/confirm."}


@router.delete(
    "/{id_filter}/confirm",
    status_code=status.HTTP_200_OK,
    summary="Krok 2/2 — Potwierdzenie usuniecia filtru",
    description="Usuwa filtr (hard delete). Wymaga `delete_token` w body JSON.",
    responses={400: {"description": "Nieprawidlowy lub wygasly token"}},
    dependencies=[require_permission("approval.manage_filters")],
)
async def confirm_delete_filter(
    id_filter: int, body: ConfirmDeleteBody,
    current_user: CurrentUser, db: DB, redis: RedisClient, request: Request,
):
    await _check_module_enabled(db, redis)
    await verify_delete_token(redis, token=body.delete_token, entity_id=id_filter, scope=_SCOPE)
    await db.execute(
        text(f"DELETE FROM [{_SCHEMA}].[skw_approval_filters] WHERE [id_filter]=:f"),
        {"f": id_filter},
    )
    await db.commit()
    logger.warning(orjson.dumps({
        "event": "approval_filter_deleted", "id_filter": id_filter,
        "deleted_by": current_user.ID_USER,
        "ip": request.headers.get("X-Forwarded-For", getattr(request.client, "host", None)),
        "ts": datetime.now(timezone.utc).isoformat(),
    }).decode())
    return {"id_filter": id_filter, "deleted": True}


@router.post("/{id_filter}/conditions", status_code=status.HTTP_201_CREATED,
             summary="Dodaj warunek do filtru standard",
             responses={422: {"description": "Niepoprawny operator"}},
             dependencies=[require_permission("approval.manage_filters")])
async def add_condition(id_filter: int, body: ConditionCreateBody,
                        current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    result = await db.execute(
        text(f"INSERT INTO [{_SCHEMA}].[skw_approval_filter_conditions] "
             f"([id_filter],[field_name],[operator],[field_value]) "
             f"OUTPUT INSERTED.[id_condition] VALUES (:f,:fn,:op,:fv)"),
        {"f": id_filter, "fn": body.field_name, "op": body.operator, "fv": body.field_value},
    )
    new_id = result.fetchone()[0]
    await db.commit()
    return {"id_condition": new_id, "id_filter": id_filter,
            "field_name": body.field_name, "operator": body.operator}


@router.delete("/{id_filter}/conditions/{id_condition}",
               summary="Usun warunek filtru",
               dependencies=[require_permission("approval.manage_filters")])
async def delete_condition(id_filter: int, id_condition: int,
                           current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    result = await db.execute(
        text(f"DELETE FROM [{_SCHEMA}].[skw_approval_filter_conditions] "
             f"WHERE [id_condition]=:c AND [id_filter]=:f"),
        {"c": id_condition, "f": id_filter},
    )
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Warunek nie istnieje.")
    await db.commit()
    return {"id_condition": id_condition, "deleted": True}