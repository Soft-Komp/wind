# backend/app/api/approval/notifications.py
"""
4 endpointy — persystentne powiadomienia uzytkownika.

  GET  /approval/notifications
  GET  /approval/notifications/unread-count   ← MUSI byc PRZED /{id_notification}/read
  POST /approval/notifications/{id_notification}/read
  POST /approval/notifications/read-all

notif_unread:{id_user} — Redis TTL 24h, INCR przy nowym, DECR przy read, 0 przy read-all.
UWAGA: from __future__ import annotations NIGDY w tym pliku.
UWAGA: /unread-count MUSI byc przed /{id_notification}/read (FastAPI routing order).
"""
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy import text

from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.services.approval_service import _check_module_enabled

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/notifications")
_SCHEMA = "dbo"
_NOTIF_TTL = 86400  # 24h


@router.get(
    "",
    summary="Lista powiadomien uzytkownika",
    description=(
        "Powiadomienia posortowane: `is_read ASC, created_at DESC` (nieprzeczytane na gorze). "
        "Filtr: `unread_only=true`."
    ),
    dependencies=[require_permission("approval.view_queue")],
)
async def list_notifications(
    current_user: CurrentUser, db: DB, redis: RedisClient,
    unread_only: bool = Query(False),
    page:        int  = Query(1, ge=1),
    per_page:    int  = Query(25, ge=1, le=100),
):
    await _check_module_enabled(db, redis)
    offset = (page - 1) * per_page
    where = "AND [is_read]=0" if unread_only else ""

    total = (await db.execute(
        text(f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_user_notifications] "
             f"WHERE [id_user]=:u {where}"),
        {"u": current_user.ID_USER},
    )).scalar() or 0

    rows = await db.execute(
        text(
            f"SELECT [id_notification],[notification_type],[id_instance],"
            f"  [title],[message],[is_read],[read_at],[created_at] "
            f"FROM [{_SCHEMA}].[skw_user_notifications] "
            f"WHERE [id_user]=:u {where} "
            f"ORDER BY [is_read] ASC,[created_at] DESC "
            f"OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY"
        ),
        {"u": current_user.ID_USER, "offset": offset, "limit": per_page},
    )
    return {
        "total": total, "page": page, "per_page": per_page,
        "data": [
            {
                "id_notification":    r[0],
                "notification_type":  r[1],
                "id_instance":        r[2],
                "title":              r[3],
                "message":            r[4],
                "is_read":            bool(r[5]),
                "read_at":            r[6].isoformat() if r[6] else None,
                "created_at":         r[7].isoformat() if r[7] else None,
            }
            for r in rows.fetchall()
        ],
    }


# KRYTYCZNE: /unread-count MUSI byc PRZED /{id_notification}/read
@router.get(
    "/unread-count",
    summary="Liczba nieprzeczytanych powiadomien",
    description=(
        "Serwowany z Redis (`notif_unread:{id_user}`, TTL 24h). "
        "Cache miss: fallback do bazy + zapis w Redis. "
        "Uzyc do badge/ikony w naglowku UI."
    ),
    dependencies=[require_permission("approval.view_queue")],
)
async def get_unread_count(
    current_user: CurrentUser, db: DB, redis: RedisClient,
):
    await _check_module_enabled(db, redis)
    cache_key = f"notif_unread:{current_user.ID_USER}"
    cached = await redis.get(cache_key)
    if cached is not None:
        count = int(cached)
    else:
        count = (await db.execute(
            text(f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_user_notifications] "
                 f"WHERE [id_user]=:u AND [is_read]=0"),
            {"u": current_user.ID_USER},
        )).scalar() or 0
        await redis.set(cache_key, count, ex=_NOTIF_TTL)
    return {"id_user": current_user.ID_USER, "unread_count": max(0, count)}


@router.post(
    "/{id_notification}/read",
    summary="Oznacz powiadomienie jako przeczytane",
    description=(
        "Ustawia is_read=1, read_at=now. "
        "Dekrementuje `notif_unread:{id_user}` w Redis. "
        "**404** jesli nie nalezy do zalogowanego uzytkownika."
    ),
    dependencies=[require_permission("approval.view_queue")],
)
async def mark_read(
    id_notification: int,
    current_user:    CurrentUser,
    db:              DB,
    redis:           RedisClient,
):
    await _check_module_enabled(db, redis)
    r = (await db.execute(
        text(f"SELECT [is_read] FROM [{_SCHEMA}].[skw_user_notifications] "
             f"WHERE [id_notification]=:n AND [id_user]=:u"),
        {"n": id_notification, "u": current_user.ID_USER},
    )).fetchone()
    if not r:
        raise HTTPException(status_code=404, detail="Powiadomienie nie istnieje.")
    was_unread = not bool(r[0])
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    await db.execute(
        text(
            f"UPDATE [{_SCHEMA}].[skw_user_notifications] "
            f"SET [is_read]=1,[read_at]=:now "
            f"WHERE [id_notification]=:n AND [id_user]=:u AND [is_read]=0"
        ),
        {"now": now, "n": id_notification, "u": current_user.ID_USER},
    )
    await db.commit()
    if was_unread:
        cache_key = f"notif_unread:{current_user.ID_USER}"
        try:
            val = await redis.get(cache_key)
            if val is not None:
                await redis.set(cache_key, max(0, int(val)-1), ex=_NOTIF_TTL)
        except Exception as exc:
            logger.warning("mark_read | Redis DECR error: %s", exc)
    return {"id_notification": id_notification, "is_read": True}


@router.post(
    "/read-all",
    summary="Oznacz wszystkie powiadomienia jako przeczytane",
    description="Ustawia is_read=1 dla wszystkich nieprzeczytanych. Resetuje Redis do 0.",
    dependencies=[require_permission("approval.view_queue")],
)
async def mark_all_read(
    current_user: CurrentUser, db: DB, redis: RedisClient,
):
    await _check_module_enabled(db, redis)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    result = await db.execute(
        text(
            f"UPDATE [{_SCHEMA}].[skw_user_notifications] "
            f"SET [is_read]=1,[read_at]=:now WHERE [id_user]=:u AND [is_read]=0"
        ),
        {"now": now, "u": current_user.ID_USER},
    )
    updated = result.rowcount
    await db.commit()
    try:
        await redis.set(f"notif_unread:{current_user.ID_USER}", 0, ex=_NOTIF_TTL)
    except Exception as exc:
        logger.warning("mark_all_read | Redis SET error: %s", exc)
    logger.info("notifications.read_all | user=%d marked=%d", current_user.ID_USER, updated)
    return {"message": f"Oznaczono {updated} powiadomien.", "updated_count": updated, "unread_count": 0}