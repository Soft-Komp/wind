# backend/app/api/approval/comments.py
"""
5+2 endpointow — komentarze (DELETE dwuetapowy: initiate/confirm).

  GET   /approval/instances/{id}/comments
  POST  /approval/instances/{id}/comments
  PATCH /approval/instances/{id}/comments/{id_comment}
  DELETE /approval/instances/{id}/comments/{id_comment}/initiate   — krok 1/2
  DELETE /approval/instances/{id}/comments/{id_comment}/confirm    — krok 2/2

503 jesli APPROVAL_COMMENTS_ENABLED=false.
UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging
import orjson
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import text

from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.services.approval_service import _check_module_enabled, _check_feature_flag
from app.api.approval._delete_helpers import generate_delete_token, verify_delete_token

logger = logging.getLogger(__name__)
router = APIRouter()
_SCHEMA = "dbo"
_SCOPE = "delete_comment"


class CommentCreateBody(BaseModel):
    content:   str           = Field(..., min_length=1, max_length=4000)
    parent_id: Optional[int] = Field(None, gt=0)


class CommentPatchBody(BaseModel):
    content: str = Field(..., min_length=1, max_length=4000)


class ConfirmDeleteBody(BaseModel):
    delete_token: str = Field(..., min_length=1)


@router.get("/instances/{id_instance}/comments",
            summary="Lista komentarzy instancji",
            description="Hierarchia. is_deleted=true: tresc [usunieto]. **503** jesli APPROVAL_COMMENTS_ENABLED=false.",
            dependencies=[require_permission("approval.view_queue")])
async def list_comments(id_instance: int, current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    await _check_feature_flag(db, redis, "APPROVAL_COMMENTS_ENABLED",
                               error_msg="Komentarze sa wylaczone.")
    rows = await db.execute(
        text(f"SELECT c.[id_comment],c.[parent_id],c.[id_user],u.[Username],u.[FullName],"
             f"  c.[content],c.[is_deleted],c.[created_at],c.[updated_at] "
             f"FROM [{_SCHEMA}].[skw_approval_comments] c "
             f"LEFT JOIN [{_SCHEMA}].[skw_Users] u ON u.[ID_USER]=c.[id_user] "
             f"WHERE c.[id_instance]=:i ORDER BY c.[created_at] ASC"),
        {"i": id_instance},
    )
    items = []
    for r in rows.fetchall():
        is_del = bool(r[6])
        items.append({
            "id_comment": r[0], "parent_id": r[1], "id_user": r[2],
            "username": r[3], "full_name": r[4],
            "content": "[usunieto]" if is_del else r[5], "is_deleted": is_del,
            "created_at": r[7].isoformat() if r[7] else None,
            "updated_at": r[8].isoformat() if r[8] else None,
            "can_edit": (r[2] == current_user.ID_USER) and not is_del,
        })
    return {"id_instance": id_instance, "comments": items, "total": len(items)}


@router.post("/instances/{id_instance}/comments",
             status_code=status.HTTP_201_CREATED,
             summary="Dodaj komentarz",
             dependencies=[require_permission("approval.accept")])
async def create_comment(id_instance: int, body: CommentCreateBody,
                         current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    await _check_feature_flag(db, redis, "APPROVAL_COMMENTS_ENABLED",
                               error_msg="Komentarze sa wylaczone.")
    if body.parent_id is not None and not (await db.execute(
        text(f"SELECT [id_comment] FROM [{_SCHEMA}].[skw_approval_comments] "
             f"WHERE [id_comment]=:p AND [id_instance]=:i"),
        {"p": body.parent_id, "i": id_instance},
    )).fetchone():
        raise HTTPException(status_code=404, detail="Komentarz-rodzic nie istnieje.")
    result = await db.execute(
        text(f"INSERT INTO [{_SCHEMA}].[skw_approval_comments] "
             f"([id_instance],[id_user],[parent_id],[content]) "
             f"OUTPUT INSERTED.[id_comment] VALUES (:i,:u,:p,:c)"),
        {"i": id_instance, "u": current_user.ID_USER,
         "p": body.parent_id, "c": body.content.strip()},
    )
    new_id = result.fetchone()[0]
    await db.commit()
    return {"id_comment": new_id, "id_instance": id_instance}


@router.patch("/instances/{id_instance}/comments/{id_comment}",
              summary="Edytuj komentarz",
              responses={403: {"description": "Mozesz edytowac tylko swoje komentarze"}},
              dependencies=[require_permission("approval.accept")])
async def update_comment(id_instance: int, id_comment: int, body: CommentPatchBody,
                         current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    await _check_feature_flag(db, redis, "APPROVAL_COMMENTS_ENABLED",
                               error_msg="Komentarze sa wylaczone.")
    r = (await db.execute(
        text(f"SELECT [id_user],[is_deleted] FROM [{_SCHEMA}].[skw_approval_comments] "
             f"WHERE [id_comment]=:c AND [id_instance]=:i"),
        {"c": id_comment, "i": id_instance},
    )).fetchone()
    if not r: raise HTTPException(status_code=404, detail="Komentarz nie istnieje.")
    if r[0] != current_user.ID_USER:
        raise HTTPException(status_code=403, detail="Mozesz edytowac tylko swoje komentarze.")
    if bool(r[1]):
        raise HTTPException(status_code=409, detail="Nie mozna edytowac usunietego komentarza.")
    await db.execute(
        text(f"UPDATE [{_SCHEMA}].[skw_approval_comments] "
             f"SET [content]=:c,[updated_at]=SYSUTCDATETIME() WHERE [id_comment]=:id"),
        {"c": body.content.strip(), "id": id_comment},
    )
    await db.commit()
    return {"id_comment": id_comment, "updated": True}


@router.delete(
    "/instances/{id_instance}/comments/{id_comment}/initiate",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Krok 1/2 — Inicjacja usuniecia komentarza",
    description="Generuje token JWT (TTL 60s). Wlasciciel lub approval.supervise.",
    dependencies=[require_permission("approval.accept")],
)
async def initiate_delete_comment(
    id_instance: int, id_comment: int,
    current_user: CurrentUser, db: DB, redis: RedisClient, request: Request,
):
    await _check_module_enabled(db, redis)
    await _check_feature_flag(db, redis, "APPROVAL_COMMENTS_ENABLED",
                               error_msg="Komentarze sa wylaczone.")
    r = (await db.execute(
        text(f"SELECT [id_user],[is_deleted] FROM [{_SCHEMA}].[skw_approval_comments] "
             f"WHERE [id_comment]=:c AND [id_instance]=:i"),
        {"c": id_comment, "i": id_instance},
    )).fetchone()
    if not r: raise HTTPException(status_code=404, detail="Komentarz nie istnieje.")
    if bool(r[1]): raise HTTPException(status_code=409, detail="Komentarz juz usuniety.")
    from app.core.dependencies import _get_role_permissions
    perms = await _get_role_permissions(current_user.role_id, db, redis)
    if r[0] != current_user.ID_USER and "approval.supervise" not in perms:
        raise HTTPException(status_code=403, detail="Mozesz usunac tylko swoje komentarze.")
    token, ttl = await generate_delete_token(
        redis, entity_id=id_comment, scope=_SCOPE,
        initiated_by=current_user.ID_USER,
        extra={"id_instance": id_instance},
    )
    logger.warning(orjson.dumps({
        "event": "approval_comment_delete_initiated",
        "id_comment": id_comment, "id_instance": id_instance,
        "initiated_by": current_user.ID_USER,
        "ip": request.headers.get("X-Forwarded-For", getattr(request.client, "host", None)),
        "ts": datetime.now(timezone.utc).isoformat(),
    }).decode())
    return {"delete_token": token, "expires_in": ttl,
            "id_comment": id_comment, "id_instance": id_instance,
            "message": f"Token wazny {ttl}s. Uzyj w DELETE .../comments/{id_comment}/confirm."}


@router.delete(
    "/instances/{id_instance}/comments/{id_comment}/confirm",
    status_code=status.HTTP_200_OK,
    summary="Krok 2/2 — Potwierdzenie usuniecia komentarza",
    description="Soft-delete. Wymaga `delete_token` w body JSON: `{\"delete_token\": \"eyJ...\"}`.",
    responses={400: {"description": "Nieprawidlowy lub wygasly token"}},
    dependencies=[require_permission("approval.accept")],
)
async def confirm_delete_comment(
    id_instance: int, id_comment: int, body: ConfirmDeleteBody,
    current_user: CurrentUser, db: DB, redis: RedisClient, request: Request,
):
    await _check_module_enabled(db, redis)
    await verify_delete_token(redis, token=body.delete_token,
                              entity_id=id_comment, scope=_SCOPE)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    await db.execute(
        text(f"UPDATE [{_SCHEMA}].[skw_approval_comments] "
             f"SET [is_deleted]=1,[deleted_at]=:now,[updated_at]=:now "
             f"WHERE [id_comment]=:c AND [id_instance]=:i"),
        {"now": now, "c": id_comment, "i": id_instance},
    )
    await db.commit()
    logger.warning(orjson.dumps({
        "event": "approval_comment_deleted",
        "id_comment": id_comment, "id_instance": id_instance,
        "deleted_by": current_user.ID_USER,
        "ip": request.headers.get("X-Forwarded-For", getattr(request.client, "host", None)),
        "ts": datetime.now(timezone.utc).isoformat(),
    }).decode())
    return {"id_comment": id_comment, "is_deleted": True}