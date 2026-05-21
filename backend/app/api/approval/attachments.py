# backend/app/api/approval/attachments.py
"""
5+2 endpointow — zalaczniki (DELETE dwuetapowy: initiate/confirm).

  GET    /approval/instances/{id}/attachments
  POST   /approval/instances/{id}/attachments
  GET    /approval/instances/{id}/attachments/{id}/download
  DELETE /approval/instances/{id}/attachments/{id}/initiate  — krok 1/2
  DELETE /approval/instances/{id}/attachments/{id}/confirm   — krok 2/2

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging
import orjson
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, UploadFile, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import text

from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.services.approval_service import _check_module_enabled, _check_feature_flag
from app.api.approval._delete_helpers import generate_delete_token, verify_delete_token

logger = logging.getLogger(__name__)
router = APIRouter()
_SCHEMA = "dbo"
_SCOPE = "delete_attachment"

_ALLOWED_MIME: frozenset[str] = frozenset({
    "application/pdf",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "text/plain", "text/csv", "image/jpeg", "image/png", "image/tiff",
})
_SAFE_FILENAME_RE = re.compile(r"[^a-zA-Z0-9._\-]")


def _sanitize_filename(name: str) -> str:
    base = Path(name).stem; ext = Path(name).suffix.lower()[:10]
    return f"{_SAFE_FILENAME_RE.sub('_', base)[:100]}{ext}" or f"file{ext}"


class ConfirmDeleteBody(BaseModel):
    delete_token: str = Field(..., min_length=1)


@router.get("/instances/{id_instance}/attachments",
            summary="Lista zalacznikow instancji",
            dependencies=[require_permission("approval.view_queue")])
async def list_attachments(id_instance: int, current_user: CurrentUser,
                           db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    await _check_feature_flag(db, redis, "APPROVAL_ATTACHMENTS_ENABLED",
                               error_msg="Zalaczniki sa wylaczone.")
    rows = await db.execute(
        text(f"SELECT a.[id_attachment],a.[id_user],u.[Username],"
             f"  a.[file_name],a.[file_size],a.[mime_type],a.[created_at] "
             f"FROM [{_SCHEMA}].[skw_approval_attachments] a "
             f"LEFT JOIN [{_SCHEMA}].[skw_Users] u ON u.[ID_USER]=a.[id_user] "
             f"WHERE a.[id_instance]=:i AND a.[is_deleted]=0 ORDER BY a.[created_at] ASC"),
        {"i": id_instance},
    )
    return {"id_instance": id_instance, "attachments": [
        {"id_attachment": r[0], "id_user": r[1], "username": r[2],
         "file_name": r[3], "file_size": r[4], "mime_type": r[5],
         "created_at": r[6].isoformat() if r[6] else None}
        for r in rows.fetchall()
    ]}


@router.post("/instances/{id_instance}/attachments",
             status_code=status.HTTP_201_CREATED,
             summary="Dodaj zalacznik (multipart)",
             responses={413: {"description": "Plik za duzy"}, 415: {"description": "Niedozwolony MIME"},
                        404: {"description": "Instancja nie istnieje"}, 409: {"description": "Instancja zamknieta"}},
             dependencies=[require_permission("approval.accept")])
async def upload_attachment(id_instance: int, file: UploadFile,
                            current_user: CurrentUser, db: DB, redis: RedisClient,
                            request: Request):
    await _check_module_enabled(db, redis)
    await _check_feature_flag(db, redis, "APPROVAL_ATTACHMENTS_ENABLED",
                               error_msg="Zalaczniki sa wylaczone.")
    inst = (await db.execute(
        text(f"SELECT [status] FROM [{_SCHEMA}].[skw_document_approval_instances] "
             f"WHERE [id_instance]=:i"), {"i": id_instance},
    )).fetchone()
    if not inst:
        raise HTTPException(status_code=404, detail=f"Instancja {id_instance} nie istnieje.")
    if inst[0] in ("approved", "cancelled"):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
            detail=f"Instancja zamknieta (status='{inst[0]}'). Nie mozna dodawac zalacznikow.")
    max_mb_raw = await redis.get("syscfg:APPROVAL_MAX_ATTACHMENT_MB")
    max_mb = int(max_mb_raw.decode() if isinstance(max_mb_raw, bytes) else max_mb_raw) if max_mb_raw else 20
    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=422, detail="Plik jest pusty.")
    if len(content) > max_mb * 1024 * 1024:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Plik za duzy. Max: {max_mb} MB.")
    try:
        import magic; detected_mime = magic.from_buffer(content, mime=True)
    except ImportError:
        detected_mime = file.content_type or "application/octet-stream"
        logger.warning("upload_attachment | python-magic niedostepne — fallback")
    if detected_mime not in _ALLOWED_MIME:
        raise HTTPException(status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=f"Niedozwolony typ: {detected_mime}.")
    attachments_dir = Path(os.environ.get("APPROVAL_ATTACHMENTS_DIR", "/data/approval_attachments"))
    instance_dir = attachments_dir / str(id_instance); instance_dir.mkdir(parents=True, exist_ok=True)
    safe_name = _sanitize_filename(file.filename or "attachment")
    file_path = instance_dir / f"{uuid.uuid4().hex}_{safe_name}"
    with open(file_path, "wb") as f: f.write(content)
    result = await db.execute(
        text(f"INSERT INTO [{_SCHEMA}].[skw_approval_attachments] "
             f"([id_instance],[id_user],[file_name],[file_path],[file_size],[mime_type]) "
             f"OUTPUT INSERTED.[id_attachment] VALUES (:i,:u,:fn,:fp,:fs,:mt)"),
        {"i": id_instance, "u": current_user.ID_USER,
         "fn": file.filename or safe_name, "fp": str(file_path),
         "fs": len(content), "mt": detected_mime},
    )
    new_id = result.fetchone()[0]
    await db.commit()
    logger.warning(orjson.dumps({
        "event": "approval_attachment_uploaded", "id_attachment": new_id,
        "id_instance": id_instance, "file_size": len(content), "mime": detected_mime,
        "uploaded_by": current_user.ID_USER, "ts": datetime.now(timezone.utc).isoformat(),
    }).decode())
    return {"id_attachment": new_id, "file_name": file.filename,
            "file_size": len(content), "mime_type": detected_mime}


@router.get("/instances/{id_instance}/attachments/{id_attachment}/download",
            summary="Pobierz zalacznik",
            responses={404: {"description": "Nie istnieje lub usuniety"},
                       410: {"description": "Plik nie istnieje na dysku"}},
            dependencies=[require_permission("approval.view_queue")])
async def download_attachment(id_instance: int, id_attachment: int,
                              current_user: CurrentUser, db: DB, redis: RedisClient):
    await _check_module_enabled(db, redis)
    await _check_feature_flag(db, redis, "APPROVAL_ATTACHMENTS_ENABLED",
                               error_msg="Zalaczniki sa wylaczone.")
    r = (await db.execute(
        text(f"SELECT [file_name],[file_path],[mime_type],[is_deleted] "
             f"FROM [{_SCHEMA}].[skw_approval_attachments] "
             f"WHERE [id_attachment]=:a AND [id_instance]=:i"),
        {"a": id_attachment, "i": id_instance},
    )).fetchone()
    if not r or bool(r[3]):
        raise HTTPException(status_code=404, detail="Zalacznik nie istnieje lub usuniety.")
    fp = Path(r[1])
    if not fp.exists():
        raise HTTPException(status_code=410, detail="Plik nie istnieje na dysku.")
    return FileResponse(path=str(fp), filename=r[0], media_type=r[2])


@router.delete(
    "/instances/{id_instance}/attachments/{id_attachment}/initiate",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Krok 1/2 — Inicjacja usuniecia zalacznika",
    description="Generuje token JWT (TTL 60s). Wlasciciel lub approval.supervise.",
    dependencies=[require_permission("approval.accept")],
)
async def initiate_delete_attachment(
    id_instance: int, id_attachment: int,
    current_user: CurrentUser, db: DB, redis: RedisClient, request: Request,
):
    await _check_module_enabled(db, redis)
    await _check_feature_flag(db, redis, "APPROVAL_ATTACHMENTS_ENABLED",
                               error_msg="Zalaczniki sa wylaczone.")
    r = (await db.execute(
        text(f"SELECT [id_user],[is_deleted],[file_name] FROM [{_SCHEMA}].[skw_approval_attachments] "
             f"WHERE [id_attachment]=:a AND [id_instance]=:i"),
        {"a": id_attachment, "i": id_instance},
    )).fetchone()
    if not r: raise HTTPException(status_code=404, detail="Zalacznik nie istnieje.")
    if bool(r[1]): raise HTTPException(status_code=409, detail="Zalacznik juz usuniety.")
    from app.core.dependencies import _get_role_permissions
    perms = await _get_role_permissions(current_user.role_id, db, redis)
    if r[0] != current_user.ID_USER and "approval.supervise" not in perms:
        raise HTTPException(status_code=403, detail="Mozesz usunac tylko swoje zalaczniki.")
    token, ttl = await generate_delete_token(
        redis, entity_id=id_attachment, scope=_SCOPE,
        initiated_by=current_user.ID_USER,
        extra={"id_instance": id_instance, "file_name": r[2]},
    )
    logger.warning(orjson.dumps({
        "event": "approval_attachment_delete_initiated",
        "id_attachment": id_attachment, "id_instance": id_instance,
        "file_name": r[2], "initiated_by": current_user.ID_USER,
        "ip": request.headers.get("X-Forwarded-For", getattr(request.client, "host", None)),
        "ts": datetime.now(timezone.utc).isoformat(),
    }).decode())
    return {"delete_token": token, "expires_in": ttl,
            "id_attachment": id_attachment, "file_name": r[2],
            "message": f"Token wazny {ttl}s. Uzyj w DELETE .../attachments/{id_attachment}/confirm."}


@router.delete(
    "/instances/{id_instance}/attachments/{id_attachment}/confirm",
    status_code=status.HTTP_200_OK,
    summary="Krok 2/2 — Potwierdzenie usuniecia zalacznika",
    description="Soft-delete metadanych. Plik fizyczny usuwany asynchronicznie. Wymaga `delete_token` w body JSON.",
    responses={400: {"description": "Nieprawidlowy lub wygasly token"}},
    dependencies=[require_permission("approval.accept")],
)
async def confirm_delete_attachment(
    id_instance: int, id_attachment: int, body: ConfirmDeleteBody,
    current_user: CurrentUser, db: DB, redis: RedisClient, request: Request,
):
    await _check_module_enabled(db, redis)
    await verify_delete_token(redis, token=body.delete_token,
                              entity_id=id_attachment, scope=_SCOPE)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    await db.execute(
        text(f"UPDATE [{_SCHEMA}].[skw_approval_attachments] "
             f"SET [is_deleted]=1,[deleted_at]=:now,[deleted_by]=:by "
             f"WHERE [id_attachment]=:a AND [id_instance]=:i"),
        {"now": now, "by": current_user.ID_USER, "a": id_attachment, "i": id_instance},
    )
    await db.commit()
    logger.warning(orjson.dumps({
        "event": "approval_attachment_deleted",
        "id_attachment": id_attachment, "id_instance": id_instance,
        "deleted_by": current_user.ID_USER,
        "ip": request.headers.get("X-Forwarded-For", getattr(request.client, "host", None)),
        "ts": datetime.now(timezone.utc).isoformat(),
    }).decode())
    return {"id_attachment": id_attachment, "is_deleted": True,
            "note": "Plik fizyczny zostanie usuniety przez serwis czyszczacy."}