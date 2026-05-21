# backend/app/api/approval/groups.py
"""
8 endpointow — grupy akceptacyjne i czlonkowie.

  GET    /approval/groups
  POST   /approval/groups
  GET    /approval/groups/{id_group}
  PATCH  /approval/groups/{id_group}
  DELETE /approval/groups/{id_group}/initiate    — krok 1/2
  DELETE /approval/groups/{id_group}/confirm     — krok 2/2 (token w body)
  GET    /approval/groups/{id_group}/members
  POST   /approval/groups/{id_group}/members
  DELETE /approval/groups/{id_group}/members/{id_user}  — bezposredni

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging
import orjson
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import text

from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.services.approval_service import _check_module_enabled
from app.services.approval_service_ext import invalidate_group_cache
from app.api.approval._delete_helpers import generate_delete_token, verify_delete_token, DELETE_TOKEN_TTL

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/groups")
_SCHEMA = "dbo"
_SCOPE = "delete_group"


class GroupCreateBody(BaseModel):
    group_name:     str           = Field(..., min_length=2, max_length=100)
    consensus_type: str           = Field("OR", pattern="^(AND|OR)$")
    description:    Optional[str] = Field(None, max_length=500)


class GroupPatchBody(BaseModel):
    group_name:     Optional[str]  = Field(None, min_length=2, max_length=100)
    consensus_type: Optional[str]  = Field(None, pattern="^(AND|OR)$")
    description:    Optional[str]  = None
    is_active:      Optional[bool] = None


class MemberAddBody(BaseModel):
    id_user: int = Field(..., gt=0)


class ConfirmDeleteBody(BaseModel):
    delete_token: str = Field(..., min_length=1)


@router.get(
    "",
    summary="Lista grup akceptacyjnych",
    dependencies=[require_permission("approval.manage_groups")],
)
async def list_groups(
    current_user: CurrentUser, db: DB, redis: RedisClient,
    active_only: bool = Query(True),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    await _check_module_enabled(db, redis)
    offset = (page - 1) * per_page
    where = "WHERE g.[is_active]=1" if active_only else ""
    rows = await db.execute(
        text(
            f"SELECT g.[id_group],g.[group_name],g.[consensus_type],"
            f"  g.[description],g.[is_active],g.[created_at],g.[updated_at],"
            f"  COUNT(m.[id_user]) AS member_count "
            f"FROM [{_SCHEMA}].[skw_approval_groups] g "
            f"LEFT JOIN [{_SCHEMA}].[skw_approval_group_members] m ON m.[id_group]=g.[id_group] "
            f"{where} "
            f"GROUP BY g.[id_group],g.[group_name],g.[consensus_type],"
            f"  g.[description],g.[is_active],g.[created_at],g.[updated_at] "
            f"ORDER BY g.[group_name] ASC "
            f"OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY"
        ),
        {"offset": offset, "limit": per_page},
    )
    return {
        "data": [
            {"id_group": r[0], "group_name": r[1], "consensus_type": r[2],
             "description": r[3], "is_active": bool(r[4]),
             "created_at": r[5].isoformat() if r[5] else None,
             "updated_at": r[6].isoformat() if r[6] else None,
             "member_count": r[7]}
            for r in rows.fetchall()
        ],
        "page": page, "per_page": per_page,
    }


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    summary="Utworz grupe akceptacyjna",
    responses={409: {"description": "Nazwa grupy juz istnieje"}},
    dependencies=[require_permission("approval.manage_groups")],
)
async def create_group(
    body: GroupCreateBody, current_user: CurrentUser, db: DB, redis: RedisClient,
):
    await _check_module_enabled(db, redis)
    if (await db.execute(
        text(f"SELECT [id_group] FROM [{_SCHEMA}].[skw_approval_groups] WHERE [group_name]=:n"),
        {"n": body.group_name},
    )).fetchone():
        raise HTTPException(status_code=409, detail=f"Grupa '{body.group_name}' juz istnieje.")
    result = await db.execute(
        text(
            f"INSERT INTO [{_SCHEMA}].[skw_approval_groups] "
            f"([group_name],[consensus_type],[description]) "
            f"OUTPUT INSERTED.[id_group] VALUES (:n,:c,:d)"
        ),
        {"n": body.group_name, "c": body.consensus_type, "d": body.description},
    )
    new_id = result.fetchone()[0]
    await db.commit()
    logger.info("group.create | id=%d name=%r by=%d", new_id, body.group_name, current_user.ID_USER)
    return {"id_group": new_id, "group_name": body.group_name, "consensus_type": body.consensus_type}


@router.get(
    "/{id_group}",
    summary="Szczegoly grupy",
    dependencies=[require_permission("approval.manage_groups")],
)
async def get_group(id_group: int, current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    r = (await db.execute(
        text(f"SELECT [id_group],[group_name],[consensus_type],[description],[is_active],"
             f"[created_at],[updated_at] FROM [{_SCHEMA}].[skw_approval_groups] WHERE [id_group]=:g"),
        {"g": id_group},
    )).fetchone()
    if not r:
        raise HTTPException(status_code=404, detail=f"Grupa {id_group} nie istnieje.")
    return {
        "id_group": r[0], "group_name": r[1], "consensus_type": r[2],
        "description": r[3], "is_active": bool(r[4]),
        "created_at": r[5].isoformat() if r[5] else None,
        "updated_at": r[6].isoformat() if r[6] else None,
    }


@router.patch(
    "/{id_group}",
    summary="Aktualizacja grupy",
    responses={409: {"description": "Nowa nazwa juz istnieje"}},
    dependencies=[require_permission("approval.manage_groups")],
)
async def update_group(
    id_group: int, body: GroupPatchBody,
    current_user: CurrentUser, db: DB, redis: RedisClient,
):
    await _check_module_enabled(db, redis)
    sets = []; params: dict = {"g": id_group}
    if body.group_name is not None:
        if (await db.execute(
            text(f"SELECT [id_group] FROM [{_SCHEMA}].[skw_approval_groups] "
                 f"WHERE [group_name]=:n AND [id_group]<>:g"),
            {"n": body.group_name, "g": id_group},
        )).fetchone():
            raise HTTPException(status_code=409, detail="Nazwa grupy juz istnieje.")
        sets.append("[group_name]=:n"); params["n"] = body.group_name
    if body.consensus_type is not None:
        sets.append("[consensus_type]=:c"); params["c"] = body.consensus_type
    if body.description is not None:
        sets.append("[description]=:d"); params["d"] = body.description
    if body.is_active is not None:
        sets.append("[is_active]=:a"); params["a"] = 1 if body.is_active else 0
    if not sets:
        raise HTTPException(status_code=422, detail="Brak pol do aktualizacji.")
    sets.append("[updated_at]=SYSUTCDATETIME()")
    await db.execute(
        text(f"UPDATE [{_SCHEMA}].[skw_approval_groups] SET {','.join(sets)} WHERE [id_group]=:g"),
        params,
    )
    await db.commit()
    await invalidate_group_cache(redis, id_group)
    return {"id_group": id_group, "updated": True}


# =============================================================================
# DELETE DWUETAPOWY — DEZAKTYWACJA GRUPY
# =============================================================================

@router.delete(
    "/{id_group}/initiate",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Krok 1/2 — Inicjacja dezaktywacji grupy",
    description=(
        "Generuje jednorazowy token JWT (TTL 60s). "
        "Warunek: grupa nie moze byc biezacym krokiem aktywnego obiegu. "
        "Uzyj tokenu w DELETE /{id_group}/confirm."
    ),
    responses={
        202: {"description": "Token wygenerowany"},
        404: {"description": "Grupa nie istnieje"},
        409: {"description": "Grupa uzywana w aktywnym obiegu"},
    },
    dependencies=[require_permission("approval.manage_groups")],
)
async def initiate_delete_group(
    id_group: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request: Request,
):
    await _check_module_enabled(db, redis)
    r = (await db.execute(
        text(f"SELECT [group_name],[is_active] FROM [{_SCHEMA}].[skw_approval_groups] "
             f"WHERE [id_group]=:g"),
        {"g": id_group},
    )).fetchone()
    if not r:
        raise HTTPException(status_code=404, detail=f"Grupa {id_group} nie istnieje.")
    cnt = (await db.execute(
        text(
            f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_document_approval_snapshot_steps] s "
            f"JOIN [{_SCHEMA}].[skw_document_approval_instances] i ON i.[id_instance]=s.[id_instance] "
            f"WHERE s.[id_group]=:g AND s.[status]=N'in_progress' AND i.[status]=N'in_progress'"
        ),
        {"g": id_group},
    )).scalar()
    if cnt > 0:
        raise HTTPException(status_code=409,
            detail="Grupa jest biezacym etapem aktywnego obiegu. Zakoncz obiegi przed dezaktywacja.")

    token, ttl = await generate_delete_token(
        redis, entity_id=id_group, scope=_SCOPE,
        initiated_by=current_user.ID_USER,
        extra={"group_name": r[0]},
    )
    logger.warning(orjson.dumps({
        "event": "approval_group_delete_initiated", "id_group": id_group,
        "group_name": r[0], "initiated_by": current_user.ID_USER,
        "ip": request.headers.get("X-Forwarded-For", getattr(request.client, "host", None)),
        "ts": datetime.now(timezone.utc).isoformat(),
    }).decode())
    return {
        "delete_token": token,
        "expires_in":   ttl,
        "id_group":     id_group,
        "group_name":   r[0],
        "message":      f"Token wazny {ttl}s. Uzyj w DELETE /approval/groups/{id_group}/confirm.",
    }


@router.delete(
    "/{id_group}/confirm",
    status_code=status.HTTP_200_OK,
    summary="Krok 2/2 — Potwierdzenie dezaktywacji grupy",
    description=(
        "Wykonuje soft-delete (is_active=0). "
        "Wymaga `delete_token` z kroku 1 w body JSON: `{\"delete_token\": \"eyJ...\"}`."
    ),
    responses={
        200: {"description": "Grupa zdezaktywowana"},
        400: {"description": "Nieprawidlowy lub wygasly token"},
    },
    dependencies=[require_permission("approval.manage_groups")],
)
async def confirm_delete_group(
    id_group: int,
    body: ConfirmDeleteBody,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request: Request,
):
    await _check_module_enabled(db, redis)
    await verify_delete_token(redis, token=body.delete_token, entity_id=id_group, scope=_SCOPE)
    await db.execute(
        text(f"UPDATE [{_SCHEMA}].[skw_approval_groups] "
             f"SET [is_active]=0,[updated_at]=SYSUTCDATETIME() WHERE [id_group]=:g"),
        {"g": id_group},
    )
    await db.commit()
    await invalidate_group_cache(redis, id_group)
    logger.warning(orjson.dumps({
        "event": "approval_group_deleted", "id_group": id_group,
        "deleted_by": current_user.ID_USER,
        "ip": request.headers.get("X-Forwarded-For", getattr(request.client, "host", None)),
        "ts": datetime.now(timezone.utc).isoformat(),
    }).decode())
    return {"id_group": id_group, "is_active": False, "message": "Grupa zdezaktywowana."}


# =============================================================================
# MEMBERS
# =============================================================================

@router.get(
    "/{id_group}/members",
    summary="Lista czlonkow grupy",
    dependencies=[require_permission("approval.manage_groups")],
)
async def list_members(id_group: int, current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    rows = await db.execute(
        text(
            f"SELECT m.[id],m.[id_user],u.[Username],u.[FullName],m.[assigned_at] "
            f"FROM [{_SCHEMA}].[skw_approval_group_members] m "
            f"JOIN [{_SCHEMA}].[skw_Users] u ON u.[ID_USER]=m.[id_user] "
            f"WHERE m.[id_group]=:g ORDER BY u.[FullName] ASC"
        ),
        {"g": id_group},
    )
    members = [
        {"id": r[0], "id_user": r[1], "username": r[2], "full_name": r[3],
         "assigned_at": r[4].isoformat() if r[4] else None}
        for r in rows.fetchall()
    ]
    return {"id_group": id_group, "members": members, "count": len(members)}


@router.post(
    "/{id_group}/members",
    status_code=status.HTTP_201_CREATED,
    summary="Dodaj czlonka do grupy",
    responses={
        409: {"description": "Uzytkownik juz jest czlonkiem"},
        404: {"description": "Uzytkownik nie istnieje"},
    },
    dependencies=[require_permission("approval.manage_groups")],
)
async def add_member(
    id_group: int, body: MemberAddBody,
    current_user: CurrentUser, db: DB, redis: RedisClient,
):
    await _check_module_enabled(db, redis)
    if not (await db.execute(
        text(f"SELECT [ID_USER] FROM [{_SCHEMA}].[skw_Users] WHERE [ID_USER]=:u"),
        {"u": body.id_user},
    )).fetchone():
        raise HTTPException(status_code=404, detail=f"Uzytkownik {body.id_user} nie istnieje.")
    if (await db.execute(
        text(f"SELECT [id] FROM [{_SCHEMA}].[skw_approval_group_members] "
             f"WHERE [id_group]=:g AND [id_user]=:u"),
        {"g": id_group, "u": body.id_user},
    )).fetchone():
        raise HTTPException(status_code=409, detail="Uzytkownik juz jest czlonkiem tej grupy.")
    result = await db.execute(
        text(
            f"INSERT INTO [{_SCHEMA}].[skw_approval_group_members] "
            f"([id_group],[id_user],[assigned_by]) OUTPUT INSERTED.[id] VALUES (:g,:u,:by)"
        ),
        {"g": id_group, "u": body.id_user, "by": current_user.ID_USER},
    )
    new_id = result.fetchone()[0]
    await db.commit()
    await invalidate_group_cache(redis, id_group)
    return {"id": new_id, "id_group": id_group, "id_user": body.id_user}


@router.delete(
    "/{id_group}/members/{id_user}",
    summary="Usun czlonka z grupy",
    description="Bezposrednie usuniecie (odwracalne — mozna dodac ponownie). **409:** aktywna delegacja.",
    responses={409: {"description": "Aktywna delegacja dla tego czlonka"}},
    dependencies=[require_permission("approval.manage_groups")],
)
async def remove_member(
    id_group: int, id_user: int,
    current_user: CurrentUser, db: DB, redis: RedisClient,
):
    await _check_module_enabled(db, redis)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    if (await db.execute(
        text(f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_delegations] "
             f"WHERE [id_user_from]=:u AND [id_group]=:g AND [is_active]=1 AND [valid_to]>=:now"),
        {"u": id_user, "g": id_group, "now": now},
    )).scalar() > 0:
        raise HTTPException(status_code=409,
            detail="Czlonek ma aktywna delegacje. Anuluj delegacje przed usunieciem.")
    result = await db.execute(
        text(f"DELETE FROM [{_SCHEMA}].[skw_approval_group_members] "
             f"WHERE [id_group]=:g AND [id_user]=:u"),
        {"g": id_group, "u": id_user},
    )
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Czlonek nie nalezy do tej grupy.")
    await db.commit()
    await invalidate_group_cache(redis, id_group)
    return {"message": "Czlonek usuniety.", "id_group": id_group, "id_user": id_user}