# backend/app/api/approval/paths.py
"""
9+2 endpointow — sciezki akceptacyjne (DELETE dwuetapowy).

DELETE /{id_path}/initiate   — krok 1/2
DELETE /{id_path}/confirm    — krok 2/2 (token w body)
DELETE /{id_path}/steps/{id_step} — bezposredni (sub-zasob)

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import json
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
router = APIRouter(prefix="/paths")
_SCHEMA = "dbo"
_SCOPE = "delete_path"


class PathCreateBody(BaseModel):
    path_name:   str           = Field(..., min_length=2, max_length=200)
    description: Optional[str] = Field(None, max_length=500)


class PathPatchBody(BaseModel):
    path_name:   Optional[str]  = Field(None, min_length=2, max_length=200)
    description: Optional[str]  = None
    is_active:   Optional[bool] = None


class StepCreateBody(BaseModel):
    step_order:     int           = Field(..., gt=0, le=50)
    id_group:       int           = Field(..., gt=0)
    deadline_hours: Optional[int] = Field(None, gt=0, le=8760)


class StepPatchBody(BaseModel):
    id_group:       Optional[int] = Field(None, gt=0)
    deadline_hours: Optional[int] = Field(None, ge=0, le=8760)


class ConfirmDeleteBody(BaseModel):
    delete_token: str = Field(..., min_length=1)


@router.get("", summary="Lista sciezek akceptacyjnych",
            dependencies=[require_permission("approval.manage_paths")])
async def list_paths(current_user: CurrentUser, db: DB, redis: RedisClient,
                     active_only: bool = True):
    await _check_module_enabled(db, redis)
    where = "WHERE p.[is_active]=1" if active_only else ""
    rows = await db.execute(
        text(
            f"SELECT p.[id_path],p.[path_name],p.[description],p.[is_active],"
            f"  p.[created_at],p.[updated_at],COUNT(s.[id_step]) AS step_count "
            f"FROM [{_SCHEMA}].[skw_approval_paths] p "
            f"LEFT JOIN [{_SCHEMA}].[skw_approval_path_steps] s ON s.[id_path]=p.[id_path] "
            f"{where} GROUP BY p.[id_path],p.[path_name],p.[description],p.[is_active],"
            f"  p.[created_at],p.[updated_at] ORDER BY p.[path_name] ASC"
        ),
    )
    return {"data": [{"id_path": r[0], "path_name": r[1], "description": r[2],
                      "is_active": bool(r[3]),
                      "created_at": dt_utc(r[4]),
                      "updated_at": dt_utc(r[5]),
                      "step_count": r[6]} for r in rows.fetchall()]}


@router.post("", status_code=status.HTTP_201_CREATED, summary="Utworz sciezke",
             responses={409: {"description": "Nazwa juz istnieje"}},
             dependencies=[require_permission("approval.manage_paths")])
async def create_path(body: PathCreateBody, current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    if (await db.execute(
        text(f"SELECT [id_path] FROM [{_SCHEMA}].[skw_approval_paths] WHERE [path_name]=:n"),
        {"n": body.path_name},
    )).fetchone():
        raise HTTPException(status_code=409, detail=f"Sciezka '{body.path_name}' juz istnieje.")
    result = await db.execute(
        text(f"INSERT INTO [{_SCHEMA}].[skw_approval_paths] ([path_name],[description],[created_by]) "
             f"OUTPUT INSERTED.[id_path] VALUES (:n,:d,:by)"),
        {"n": body.path_name, "d": body.description, "by": current_user.id_user},
    )
    new_id = result.fetchone()[0]
    await db.commit()
    return {"id_path": new_id, "path_name": body.path_name}


@router.get("/{id_path}", summary="Szczegoly sciezki z krokami",
            dependencies=[require_permission("approval.manage_paths")])
async def get_path(id_path: int, current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    r = (await db.execute(
        text(f"SELECT [id_path],[path_name],[description],[is_active],[created_at],[updated_at] "
             f"FROM [{_SCHEMA}].[skw_approval_paths] WHERE [id_path]=:p"),
        {"p": id_path},
    )).fetchone()
    if not r:
        raise HTTPException(status_code=404, detail=f"Sciezka {id_path} nie istnieje.")
    steps_rows = await db.execute(
        text(f"SELECT s.[id_step],s.[step_order],s.[id_group],g.[group_name],"
             f"  g.[consensus_type],s.[deadline_hours] "
             f"FROM [{_SCHEMA}].[skw_approval_path_steps] s "
             f"JOIN [{_SCHEMA}].[skw_approval_groups] g ON g.[id_group]=s.[id_group] "
             f"WHERE s.[id_path]=:p ORDER BY s.[step_order] ASC"),
        {"p": id_path},
    )
    return {"id_path": r[0], "path_name": r[1], "description": r[2], "is_active": bool(r[3]),
            "created_at": dt_utc(r[4]),
            "steps": [{"id_step": sr[0], "step_order": sr[1], "id_group": sr[2],
                        "group_name": sr[3], "consensus_type": sr[4], "deadline_hours": sr[5]}
                       for sr in steps_rows.fetchall()]}


@router.patch("/{id_path}", summary="Aktualizacja sciezki",
              responses={409: {"description": "Nowa nazwa juz istnieje"}},
              dependencies=[require_permission("approval.manage_paths")])
async def update_path(id_path: int, body: PathPatchBody,
                      current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    sets = []; params: dict = {"p": id_path}
    if body.path_name is not None:
        if (await db.execute(
            text(f"SELECT [id_path] FROM [{_SCHEMA}].[skw_approval_paths] "
                 f"WHERE [path_name]=:n AND [id_path]<>:p"),
            {"n": body.path_name, "p": id_path},
        )).fetchone():
            raise HTTPException(status_code=409, detail="Nazwa sciezki juz istnieje.")
        sets.append("[path_name]=:n"); params["n"] = body.path_name
    if body.description is not None:
        sets.append("[description]=:d"); params["d"] = body.description
    if body.is_active is not None:
        sets.append("[is_active]=:a"); params["a"] = 1 if body.is_active else 0
    if not sets:
        raise HTTPException(status_code=422, detail="Brak pol.")
    sets.append("[updated_at]=SYSUTCDATETIME()")
    await db.execute(
        text(f"UPDATE [{_SCHEMA}].[skw_approval_paths] SET {','.join(sets)} WHERE [id_path]=:p"),
        params,
    )
    await db.execute(
        text(f"INSERT INTO [{_SCHEMA}].[skw_approval_path_change_log] "
             f"([id_path],[changed_by],[change_type],[new_value]) VALUES (:p,:by,N'meta_updated',:nv)"),
        {"p": id_path, "by": current_user.id_user,
         "nv": json.dumps({k: str(v) for k, v in params.items() if k != "p"})},
    )
    await db.commit()
    return {"id_path": id_path, "updated": True}


@router.delete(
    "/{id_path}/initiate",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Krok 1/2 — Inicjacja dezaktywacji sciezki",
    description=(
        "Generuje token JWT (TTL 60s). "
        "Warunek: sciezka nie moze byc uzywana w aktywnym obiegu. "
        "Uzyj tokenu w DELETE /{id_path}/confirm."
    ),
    responses={
        202: {"description": "Token wygenerowany"},
        409: {"description": "Sciezka uzywana w aktywnym obiegu"},
    },
    dependencies=[require_permission("approval.manage_paths")],
)
async def initiate_delete_path(
    id_path: int, current_user: CurrentUser, db: DB, redis: RedisClient, request: Request,
):
    await _check_module_enabled(db, redis)
    r = (await db.execute(
        text(f"SELECT [path_name] FROM [{_SCHEMA}].[skw_approval_paths] WHERE [id_path]=:p"),
        {"p": id_path},
    )).fetchone()
    if not r:
        raise HTTPException(status_code=404, detail=f"Sciezka {id_path} nie istnieje.")
    cnt = (await db.execute(
        text(f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_document_approval_instances] "
             f"WHERE [id_path]=:p AND [status]<>N'approved' AND [status]<>N'cancelled'"),
        {"p": id_path},
    )).scalar()
    if cnt > 0:
        raise HTTPException(status_code=409,
            detail="Sciezka jest uzywana w aktywnych obiegach. Zakoncz obiegi przed dezaktywacja.")
    token, ttl = await generate_delete_token(
        redis, entity_id=id_path, scope=_SCOPE,
        initiated_by=current_user.id_user, extra={"path_name": r[0]},
    )
    logger.warning(orjson.dumps({
        "event": "approval_path_delete_initiated", "id_path": id_path,
        "path_name": r[0], "initiated_by": current_user.id_user,
        "ip": request.headers.get("X-Forwarded-For", getattr(request.client, "host", None)),
        "ts": datetime.now(timezone.utc).isoformat(),
    }).decode())
    return {"delete_token": token, "expires_in": ttl, "id_path": id_path, "path_name": r[0],
            "message": f"Token wazny {ttl}s. Uzyj w DELETE /approval/paths/{id_path}/confirm."}


@router.delete(
    "/{id_path}/confirm",
    status_code=status.HTTP_200_OK,
    summary="Krok 2/2 — Potwierdzenie dezaktywacji sciezki",
    description="Wykonuje soft-delete. Wymaga `delete_token` w body JSON.",
    responses={400: {"description": "Nieprawidlowy lub wygasly token"}},
    dependencies=[require_permission("approval.manage_paths")],
)
async def confirm_delete_path(
    id_path: int, body: ConfirmDeleteBody,
    current_user: CurrentUser, db: DB, redis: RedisClient, request: Request,
):
    await _check_module_enabled(db, redis)
    await verify_delete_token(redis, token=body.delete_token, entity_id=id_path, scope=_SCOPE)
    await db.execute(
        text(f"UPDATE [{_SCHEMA}].[skw_approval_paths] "
             f"SET [is_active]=0,[updated_at]=SYSUTCDATETIME() WHERE [id_path]=:p"),
        {"p": id_path},
    )
    await db.commit()
    logger.warning(orjson.dumps({
        "event": "approval_path_deleted", "id_path": id_path,
        "deleted_by": current_user.id_user,
        "ip": request.headers.get("X-Forwarded-For", getattr(request.client, "host", None)),
        "ts": datetime.now(timezone.utc).isoformat(),
    }).decode())
    return {"id_path": id_path, "is_active": False, "message": "Sciezka zdezaktywowana."}


@router.get("/{id_path}/steps", summary="Lista krokow sciezki",
            dependencies=[require_permission("approval.manage_paths")])
async def list_steps(id_path: int, current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    rows = await db.execute(
        text(f"SELECT s.[id_step],s.[step_order],s.[id_group],g.[group_name],"
             f"  g.[consensus_type],s.[deadline_hours],s.[created_at] "
             f"FROM [{_SCHEMA}].[skw_approval_path_steps] s "
             f"JOIN [{_SCHEMA}].[skw_approval_groups] g ON g.[id_group]=s.[id_group] "
             f"WHERE s.[id_path]=:p ORDER BY s.[step_order] ASC"),
        {"p": id_path},
    )
    return {"id_path": id_path, "steps": [
        {"id_step": r[0], "step_order": r[1], "id_group": r[2],
         "group_name": r[3], "consensus_type": r[4], "deadline_hours": r[5],
         "created_at": dt_utc(r[6])}
        for r in rows.fetchall()
    ]}


@router.post("/{id_path}/steps", status_code=status.HTTP_201_CREATED,
             summary="Dodaj krok do sciezki",
             responses={409: {"description": "step_order juz istnieje"}},
             dependencies=[require_permission("approval.manage_paths")])
async def add_step(id_path: int, body: StepCreateBody,
                   current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    if (await db.execute(
        text(f"SELECT [id_step] FROM [{_SCHEMA}].[skw_approval_path_steps] "
             f"WHERE [id_path]=:p AND [step_order]=:o"),
        {"p": id_path, "o": body.step_order},
    )).fetchone():
        raise HTTPException(status_code=409,
            detail=f"Krok step_order={body.step_order} juz istnieje.")
    result = await db.execute(
        text(f"INSERT INTO [{_SCHEMA}].[skw_approval_path_steps] "
             f"([id_path],[step_order],[id_group],[deadline_hours]) "
             f"OUTPUT INSERTED.[id_step] VALUES (:p,:o,:g,:dh)"),
        {"p": id_path, "o": body.step_order, "g": body.id_group, "dh": body.deadline_hours},
    )
    new_id = result.fetchone()[0]
    await db.commit()
    return {"id_step": new_id, "id_path": id_path, "step_order": body.step_order}

class StepReorderItem(BaseModel):
    id_step:    int = Field(..., gt=0)
    step_order: int = Field(..., gt=0, le=50)

@router.patch(
    "/{id_path}/steps/reorder",
    summary="Zmien kolejnosc krokow sciezki (drag & drop)",
    description=(
        "Aktualizuje `step_order` wielu krokow jednoczesnie. "
        "Body: lista `[{id_step, step_order}]`. "
        "Walidacja: step_order musi byc unikalny w ramach sciezki po aktualizacji. "
        "**Uzycie:** po drag & drop w UI konfiguracji sciezki."
    ),
    responses={
        200: {"description": "Kroki przestawione"},
        409: {"description": "Duplikat step_order po aktualizacji"},
        422: {"description": "Pusta lista lub zly format"},
    },
    dependencies=[require_permission("approval.manage_paths")],
)
async def reorder_steps(
    id_path: int,
    items: list[StepReorderItem],
    current_user: CurrentUser, db: DB, redis: RedisClient,
):
    await _check_module_enabled(db, redis)
    if not items:
        raise HTTPException(status_code=422, detail="Lista nie moze byc pusta.")

    # Walidacja unikalnosci step_order w body
    orders = [i.step_order for i in items]
    if len(orders) != len(set(orders)):
        raise HTTPException(
            status_code=409,
            detail="Duplikat step_order w body — kazdy krok musi miec unikalny numer.",
        )

    # Faza 1: ustaw tymczasowe wartości ujemne żeby uniknąć kolizji UNIQUE
    # podczas zmiany kolejności (np. zamiana 1↔2 bez fazy tymczasowej → duplikat)
    for item in items:
        await db.execute(
            text(
                f"UPDATE [{_SCHEMA}].[skw_approval_path_steps] "
                f"SET [step_order] = -[step_order] "
                f"WHERE [id_step]=:s AND [id_path]=:p"
            ),
            {"s": item.id_step, "p": id_path},
        )

    # Faza 2: ustaw docelowe wartości
    updated = 0
    for item in items:
        result = await db.execute(
            text(
                f"UPDATE [{_SCHEMA}].[skw_approval_path_steps] "
                f"SET [step_order]=:o "
                f"WHERE [id_step]=:s AND [id_path]=:p"
            ),
            {"o": item.step_order, "s": item.id_step, "p": id_path},
        )
        updated += result.rowcount

    await db.commit()
    return {
        "id_path":  id_path,
        "updated":  updated,
        "message":  f"Zaktualizowano kolejnosc {updated} krokow.",
    }

@router.patch("/{id_path}/steps/{id_step}", summary="Aktualizacja kroku",
              dependencies=[require_permission("approval.manage_paths")])
async def update_step(id_path: int, id_step: int, body: StepPatchBody,
                      current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    sets = []; params: dict = {"s": id_step, "p": id_path}
    if body.id_group is not None: sets.append("[id_group]=:g"); params["g"] = body.id_group
    if body.deadline_hours is not None:
        dh = body.deadline_hours if body.deadline_hours > 0 else None
        sets.append("[deadline_hours]=:dh"); params["dh"] = dh
    if not sets: raise HTTPException(status_code=422, detail="Brak pol.")
    await db.execute(
        text(f"UPDATE [{_SCHEMA}].[skw_approval_path_steps] SET {','.join(sets)} "
             f"WHERE [id_step]=:s AND [id_path]=:p"), params,
    )
    await db.commit()
    return {"id_step": id_step, "updated": True}


@router.delete("/{id_path}/steps/{id_step}", summary="Usun krok ze sciezki",
               dependencies=[require_permission("approval.manage_paths")])
async def delete_step(id_path: int, id_step: int,
                      current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    result = await db.execute(
        text(f"DELETE FROM [{_SCHEMA}].[skw_approval_path_steps] "
             f"WHERE [id_step]=:s AND [id_path]=:p"),
        {"s": id_step, "p": id_path},
    )
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Krok nie istnieje.")
    await db.commit()
    return {"id_step": id_step, "deleted": True}

