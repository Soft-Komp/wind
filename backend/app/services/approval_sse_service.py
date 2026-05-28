# backend/app/services/approval_sse_service.py
"""
SSE hooks dla Modułu Obiegu Dokumentów (Sprint 3).
======================================================

Wywoływane WYŁĄCZNIE jako background_tasks.add_task() z routera
instances.py i delegations.py — ZAWSZE po db.commit(), NIGDY przed.

Funkcje publiczne (on_*):
    on_dispatch()          — POST /approval/dispatch
    on_accept()            — POST /instances/{id}/accept
    on_rollback()          — POST /instances/{id}/rollback
    on_reject()            — POST /instances/{id}/reject
    on_cancel()            — POST /instances/{id}/cancel
    on_forward()           — POST /instances/{id}/forward
    on_send_to_group()     — POST /instances/{id}/send-to-group
    on_mark_urgent()       — POST /instances/{id}/mark-urgent
    on_delegation_change() — POST/DELETE /approval/delegations

Hierarchia eventów per akcja:
    dispatch      → dispatch_ack    (dyspozytor + admins)
                    document_waiting (członkowie grupy kroku 1)
    accept        → approval_update  (wszyscy uczestnicy + admins)
                    document_waiting (nowa grupa, jeśli step++)
                    document_approved (wszyscy + admins, jeśli terminal)
    rollback      → document_rollback (wszyscy uczestnicy + admins)
                    document_waiting  (target group, jeśli to_step >= 1)
    reject        → document_rejected  (wszyscy uczestnicy + admins)
    cancel        → document_cancelled (wszyscy uczestnicy + admins)
    forward       → approval_update   (wszyscy uczestnicy + admins)
                    document_waiting  (target group)
    send_to_group → approval_update   (wszyscy uczestnicy + admins)
                    document_waiting  (target group)
    mark_urgent   → approval_update   (wszyscy uczestnicy + admins)
    delegation    → delegation_update (delegujący + delegat + admins)

Zasady operacyjne:
    - NIGDY from __future__ import annotations w tym pliku
    - Błąd Redis/DB NIE rzuca wyjątku — loguje WARNING, kontynuuje
    - Każdy event zapisywany do JSONL niezależnie od Redis (dual-write)
    - event_id (UUID4) per envelope — frontend może deduplicować
    - _fetch_group_members() korzysta z cache Redis group_members:{id}
      (TTL 300s) identycznie jak approval_service.py
    - Każda on_*() jest idempotentna — bezpieczna przy retry FastAPI
    - Deduplikacja user_ids w _publish_to_users() — set() przed pętlą
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import orjson
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.event_service import (
    _append_event_to_log,
    _build_event_envelope,
    _publish_to_channel,
)

logger = logging.getLogger(__name__)

_SCHEMA         = "dbo"
_CHANNEL_ADMINS = "channel:admins"
_CHANNEL_USER   = "channel:user:{uid}"

# TTL cache członków grup — spójny z approval_service.py
_CACHE_MEMBERS_TTL = 300


# =============================================================================
# HELPERS PRYWATNE
# =============================================================================

async def _fetch_instance_meta(
    db: AsyncSession,
    id_instance: int,
) -> Optional[dict]:
    """
    Pobiera metadane instancji obiegu z DB.

    Wykonuje LEFT JOIN na skw_document_approval_snapshot_steps
    żeby dostać current_group_id bez osobnego query.

    Returns dict z kluczami:
        id_instance, dispatched_by, id_document, id_source,
        status, current_step, current_group_id, document_title

    Returns None jeśli instancja nie istnieje lub błąd DB.
    """
    try:
        row = (await db.execute(
            text(
                f"SELECT "
                f"  i.[id_instance], "
                f"  i.[dispatched_by_user_id], "
                f"  i.[id_document], "
                f"  i.[id_source], "
                f"  i.[status], "
                f"  i.[current_step], "
                f"  s.[id_group]    AS current_group_id, "
                f"  i.[id_document] AS document_title "
                f"FROM [{_SCHEMA}].[skw_document_approval_instances] i "
                f"LEFT JOIN [{_SCHEMA}].[skw_document_approval_snapshot_steps] s "
                f"  ON  s.[id_instance] = i.[id_instance] "
                f"  AND s.[step_order]  = i.[current_step] "
                f"  AND s.[is_active]   = 1 "
                f"WHERE i.[id_instance] = :iid"
            ),
            {"iid": id_instance},
        )).fetchone()

        if row is None:
            logger.warning(
                orjson.dumps({
                    "event":       "sse_meta_not_found",
                    "id_instance": id_instance,
                }).decode()
            )
            return None

        return {
            "id_instance":      row[0],
            "dispatched_by":    row[1],
            "id_document":      str(row[2]) if row[2] else f"DOC#{id_instance}",
            "id_source":        row[3],
            "status":           row[4],
            "current_step":     row[5],
            "current_group_id": row[6],
            # document_title: używamy id_document jako fallback —
            # widok skw_v_approval_instance_detail ma pełny tytuł,
            # ale dodatkowy JOIN jest zbędny dla SSE payload
            "document_title":   str(row[7]) if row[7] else f"Dokument #{id_instance}",
        }

    except Exception as exc:
        logger.warning(
            orjson.dumps({
                "event":       "sse_meta_fetch_error",
                "id_instance": id_instance,
                "error":       str(exc),
            }).decode()
        )
        return None


async def _fetch_all_participants(
    db: AsyncSession,
    id_instance: int,
) -> list[int]:
    """
    Pobiera WSZYSTKICH potencjalnych uczestników instancji.

    Definicja uczestnika: aktywny członek dowolnej grupy
    w snapshocie tej instancji (is_active=1 na obu poziomach).

    Używane do broad broadcast: approved, rejected, cancelled, rollback, update.
    Deduplikacja przez DISTINCT w SQL.

    Returns [] przy błędzie DB — non-blocking.
    """
    try:
        rows = (await db.execute(
            text(
                f"SELECT DISTINCT gm.[id_user] "
                f"FROM [{_SCHEMA}].[skw_document_approval_snapshot_steps] s "
                f"JOIN [{_SCHEMA}].[skw_approval_group_members] gm "
                f"  ON  gm.[id_group]  = s.[id_group] "
                f"  AND gm.[is_active] = 1 "
                f"WHERE s.[id_instance] = :iid "
                f"  AND s.[is_active]   = 1"
            ),
            {"iid": id_instance},
        )).fetchall()

        return [r[0] for r in rows]

    except Exception as exc:
        logger.warning(
            orjson.dumps({
                "event":       "sse_participants_fetch_error",
                "id_instance": id_instance,
                "error":       str(exc),
            }).decode()
        )
        return []


async def _fetch_group_members(
    db: AsyncSession,
    redis: Redis,
    id_group: int,
) -> list[int]:
    """
    Pobiera aktywnych członków grupy.

    Kolejność: Redis cache (group_members:{id_group}, TTL 300s) → DB.
    Spójne z _get_group_members_cached() w approval_service.py.

    Returns [] przy błędzie — non-blocking.
    """
    cache_key = f"group_members:{id_group}"

    # Próba cache
    try:
        raw = await redis.get(cache_key)
        if raw:
            members = orjson.loads(raw)
            logger.debug(
                "sse group_members cache hit | id_group=%d count=%d",
                id_group, len(members),
            )
            return members
    except Exception:
        pass

    # Fallback: DB
    try:
        rows = (await db.execute(
            text(
                f"SELECT [id_user] "
                f"FROM [{_SCHEMA}].[skw_approval_group_members] "
                f"WHERE [id_group] = :g AND [is_active] = 1"
            ),
            {"g": id_group},
        )).fetchall()

        members = [r[0] for r in rows]

        # Zapisz do cache — bez rzucania wyjątku przy błędzie Redis
        try:
            await redis.setex(cache_key, _CACHE_MEMBERS_TTL, orjson.dumps(members))
        except Exception:
            pass

        return members

    except Exception as exc:
        logger.warning(
            orjson.dumps({
                "event":    "sse_group_members_fetch_error",
                "id_group": id_group,
                "error":    str(exc),
            }).decode()
        )
        return []


async def _publish_to_users(
    redis: Redis,
    user_ids: list[int],
    event_type: str,
    data: dict,
    actor_user_id: Optional[int] = None,
    include_admins: bool = True,
) -> dict:
    """
    Publikuje jeden event do wielu użytkowników + opcjonalnie channel:admins.

    Każda wiadomość ma ten SAM event_id (jedna koperta, wielu odbiorców).
    JSONL zapisywany RAZ — nie per user (unikamy duplikatów w logach).
    Błąd publish do jednego usera NIE przerywa pętli.

    Args:
        user_ids:       Lista ID odbiorców (deduplikowana wewnętrznie).
        event_type:     Typ eventu (musi być w EventType Literal).
        data:           Payload eventu — dowolne dane serializowalne do JSON.
        actor_user_id:  Kto wygenerował event (metadane logu).
        include_admins: Czy wysłać też na channel:admins.

    Returns:
        {"sent": int, "failed": int, "admins_ok": bool}
    """
    envelope = _build_event_envelope(event_type, data, actor_user_id)

    # Zapis do JSONL — jeden wpis niezależnie od liczby odbiorców
    _append_event_to_log(envelope)

    # Deduplikacja i odfiltrowanie None
    unique_users = list({uid for uid in user_ids if uid is not None})

    sent   = 0
    failed = 0

    for uid in unique_users:
        channel = _CHANNEL_USER.format(uid=uid)
        ok = await _publish_to_channel(redis, channel, envelope)
        if ok:
            sent += 1
        else:
            failed += 1

    admins_ok = False
    if include_admins:
        admins_ok = await _publish_to_channel(redis, _CHANNEL_ADMINS, envelope)

    logger.info(
        orjson.dumps({
            "event":      "sse_approval_published",
            "event_type": event_type,
            "event_id":   envelope.get("event_id"),
            "recipients": unique_users,
            "sent":       sent,
            "failed":     failed,
            "admins_ok":  admins_ok,
            "actor":      actor_user_id,
        }).decode()
    )

    return {"sent": sent, "failed": failed, "admins_ok": admins_ok}


def _build_base_data(meta: dict, ts: str) -> dict:
    """Wspólne pola payload dla większości eventów obiegu."""
    return {
        "id_instance":    meta["id_instance"],
        "id_document":    meta["id_document"],
        "document_title": meta["document_title"],
        "status":         meta["status"],
        "current_step":   meta["current_step"],
        "ts":             ts,
    }


def _recipient_list(meta: dict, extra: list[int]) -> list[int]:
    """
    Łączy dyspozytor + extra lista.
    Bezpieczne: dispatched_by może być None.
    """
    base = list(extra)
    if meta.get("dispatched_by"):
        base.append(meta["dispatched_by"])
    return base


# =============================================================================
# PUBLIC: on_dispatch
# =============================================================================

async def on_dispatch(
    redis: Redis,
    db: AsyncSession,
    id_instance: int,
    id_dispatched_by: int,
) -> None:
    """
    SSE po POST /approval/dispatch. Wywołaj z bg.add_task().

    Wysyła:
      1. dispatch_ack      → dyspozytor + channel:admins
         (typ A: potwierdzenie commit; typ B: payload z id_instance;
          typ C: lekki ping że połączenie żyje i akcja przeszła)
      2. document_waiting  → członkowie grupy kroku 1
         (frontend powinien odświeżyć "Moja kolejka" natychmiast)
    """
    try:
        meta = await _fetch_instance_meta(db, id_instance)
        if not meta:
            return

        ts = datetime.now(timezone.utc).isoformat()
        base = _build_base_data(meta, ts)

        # 1 ── dispatch_ack → dyspozytor + admins ─────────────────────────────
        await _publish_to_users(
            redis,
            user_ids=[id_dispatched_by],
            event_type="dispatch_ack",
            data={
                **base,
                "message": "Dokument przekazany do obiegu.",
            },
            actor_user_id=id_dispatched_by,
            include_admins=True,
        )

        # 2 ── document_waiting → członkowie grupy 1 ──────────────────────────
        current_group = meta.get("current_group_id")
        if current_group:
            members = await _fetch_group_members(db, redis, current_group)
            if members:
                await _publish_to_users(
                    redis,
                    user_ids=members,
                    event_type="document_waiting",
                    data={
                        **base,
                        "id_group":  current_group,
                        "is_urgent": False,
                    },
                    actor_user_id=id_dispatched_by,
                    include_admins=False,   # admins dostali już w dispatch_ack
                )

        logger.info(
            orjson.dumps({
                "event":       "on_dispatch_sse_ok",
                "id_instance": id_instance,
                "group_step1": current_group,
                "ts":          ts,
            }).decode()
        )

    except Exception as exc:
        logger.warning(
            orjson.dumps({
                "event":       "on_dispatch_sse_error",
                "id_instance": id_instance,
                "error":       str(exc),
            }).decode()
        )


# =============================================================================
# PUBLIC: on_accept
# =============================================================================

async def on_accept(
    redis: Redis,
    db: AsyncSession,
    id_instance: int,
    id_user: int,
    step_complete: bool,
    approved_terminal: bool,
    next_step: Optional[int] = None,
    next_group_id: Optional[int] = None,
) -> None:
    """
    SSE po POST /instances/{id}/accept. Wywołaj z bg.add_task().

    Przypadki:
      - Głos oddany, etap NIE zakończony:
          approval_update → wszyscy uczestnicy + admins
      - Głos oddany, etap zakończony, NIE terminal:
          approval_update → wszyscy uczestnicy + admins
          document_waiting → nowa grupa (next_group_id)
      - Głos oddany, etap zakończony, terminal (approved):
          document_approved → dyspozytor + wszyscy uczestnicy + admins

    Args:
        step_complete:    Czy etap został zaliczony (wszyscy wymagani głosowali).
        approved_terminal: Czy to ostatni etap → dokument zaakceptowany.
        next_step:        Numer następnego etapu (jeśli step_complete i nie terminal).
        next_group_id:    ID grupy następnego etapu (jeśli step_complete i nie terminal).
    """
    try:
        meta = await _fetch_instance_meta(db, id_instance)
        if not meta:
            return

        ts            = datetime.now(timezone.utc).isoformat()
        base          = _build_base_data(meta, ts)
        all_parts     = await _fetch_all_participants(db, id_instance)
        recipients    = _recipient_list(meta, all_parts)

        if approved_terminal:
            # ── Terminal: dokument w pełni zaakceptowany ───────────────────
            await _publish_to_users(
                redis,
                user_ids=recipients,
                event_type="document_approved",
                data={**base, "approved": True},
                actor_user_id=id_user,
                include_admins=True,
            )
        else:
            # ── Normalny głos / przejście etapu ───────────────────────────
            await _publish_to_users(
                redis,
                user_ids=recipients,
                event_type="approval_update",
                data={
                    **base,
                    "action":        "accepted",
                    "step_complete": step_complete,
                },
                actor_user_id=id_user,
                include_admins=True,
            )

            # ── Powiadomienie nowej grupy (przejście etapu) ────────────────
            # next_group_id=None oznacza że router nie podał wartości —
            # fallback na current_group_id z meta (po commit DB zawiera już
            # nowy krok i grupę, więc current_group_id = nowa grupa)
            effective_next_group = next_group_id or meta.get("current_group_id")
            effective_next_step  = next_step or meta.get("current_step")

            if step_complete and effective_next_group:
                next_members = await _fetch_group_members(
                    db, redis, effective_next_group
                )
                if next_members:
                    await _publish_to_users(
                        redis,
                        user_ids=next_members,
                        event_type="document_waiting",
                        data={
                            **base,
                            "step_order": effective_next_step,
                            "id_group":   effective_next_group,
                        },
                        actor_user_id=id_user,
                        include_admins=False,
                    )

    except Exception as exc:
        logger.warning(
            orjson.dumps({
                "event":       "on_accept_sse_error",
                "id_instance": id_instance,
                "error":       str(exc),
            }).decode()
        )


# =============================================================================
# PUBLIC: on_rollback
# =============================================================================

async def on_rollback(
    redis: Redis,
    db: AsyncSession,
    id_instance: int,
    id_user: int,
    to_step: int,
    new_status: str,
) -> None:
    """
    SSE po POST /instances/{id}/rollback. Wywołaj z bg.add_task().

    Wysyła:
      1. document_rollback → dyspozytor + wszyscy uczestnicy + admins
      2. document_waiting  → target group (to_step >= 1)
         POMINIĘTE jeśli to_step == 0 (pending_dispatch — brak grupy docelowej)
    """
    try:
        meta = await _fetch_instance_meta(db, id_instance)
        if not meta:
            return

        ts         = datetime.now(timezone.utc).isoformat()
        base       = _build_base_data(meta, ts)
        all_parts  = await _fetch_all_participants(db, id_instance)
        recipients = _recipient_list(meta, all_parts)

        # 1 ── document_rollback → wszyscy ────────────────────────────────────
        await _publish_to_users(
            redis,
            user_ids=recipients,
            event_type="document_rollback",
            data={
                **base,
                "to_step":    to_step,
                "new_status": new_status,
            },
            actor_user_id=id_user,
            include_admins=True,
        )

        # 2 ── document_waiting → target group (jeśli jest etap docelowy) ─────
        # Po rollback _fetch_instance_meta zwraca już nowy current_step i group,
        # bo query jest po commit(). Sprawdzamy to_step >= 1.
        if to_step >= 1 and meta.get("current_group_id"):
            target_members = await _fetch_group_members(
                db, redis, meta["current_group_id"]
            )
            if target_members:
                await _publish_to_users(
                    redis,
                    user_ids=target_members,
                    event_type="document_waiting",
                    data={
                        **base,
                        "step_order": to_step,
                        "id_group":   meta["current_group_id"],
                    },
                    actor_user_id=id_user,
                    include_admins=False,
                )

    except Exception as exc:
        logger.warning(
            orjson.dumps({
                "event":       "on_rollback_sse_error",
                "id_instance": id_instance,
                "error":       str(exc),
            }).decode()
        )


# =============================================================================
# PUBLIC: on_reject
# =============================================================================

async def on_reject(
    redis: Redis,
    db: AsyncSession,
    id_instance: int,
    id_user: int,
) -> None:
    """
    SSE po POST /instances/{id}/reject. Wywołaj z bg.add_task().
    Terminal: document_rejected → dyspozytor + wszyscy uczestnicy + admins.
    """
    try:
        meta = await _fetch_instance_meta(db, id_instance)
        if not meta:
            return

        all_parts  = await _fetch_all_participants(db, id_instance)
        recipients = _recipient_list(meta, all_parts)

        await _publish_to_users(
            redis,
            user_ids=recipients,
            event_type="document_rejected",
            data={
                "id_instance":    id_instance,
                "id_document":    meta["id_document"],
                "document_title": meta["document_title"],
                "status":         "rejected",
                "ts":             datetime.now(timezone.utc).isoformat(),
            },
            actor_user_id=id_user,
            include_admins=True,
        )

    except Exception as exc:
        logger.warning(
            orjson.dumps({
                "event":       "on_reject_sse_error",
                "id_instance": id_instance,
                "error":       str(exc),
            }).decode()
        )


# =============================================================================
# PUBLIC: on_cancel
# =============================================================================

async def on_cancel(
    redis: Redis,
    db: AsyncSession,
    id_instance: int,
    id_user: int,
) -> None:
    """
    SSE po POST /instances/{id}/cancel. Wywołaj z bg.add_task().
    Terminal: document_cancelled → dyspozytor + wszyscy uczestnicy + admins.
    """
    try:
        meta = await _fetch_instance_meta(db, id_instance)
        if not meta:
            return

        all_parts  = await _fetch_all_participants(db, id_instance)
        recipients = _recipient_list(meta, all_parts)

        await _publish_to_users(
            redis,
            user_ids=recipients,
            event_type="document_cancelled",
            data={
                "id_instance":    id_instance,
                "id_document":    meta["id_document"],
                "document_title": meta["document_title"],
                "status":         "cancelled",
                "ts":             datetime.now(timezone.utc).isoformat(),
            },
            actor_user_id=id_user,
            include_admins=True,
        )

    except Exception as exc:
        logger.warning(
            orjson.dumps({
                "event":       "on_cancel_sse_error",
                "id_instance": id_instance,
                "error":       str(exc),
            }).decode()
        )


# =============================================================================
# PUBLIC: on_forward
# =============================================================================

async def on_forward(
    redis: Redis,
    db: AsyncSession,
    id_instance: int,
    id_user: int,
    id_target_group: int,
) -> None:
    """
    SSE po POST /instances/{id}/forward. Wywołaj z bg.add_task().

    Wysyła:
      1. approval_update  → wszyscy uczestnicy + admins
      2. document_waiting → członkowie target group
    """
    try:
        meta = await _fetch_instance_meta(db, id_instance)
        if not meta:
            return

        ts         = datetime.now(timezone.utc).isoformat()
        base       = _build_base_data(meta, ts)
        all_parts  = await _fetch_all_participants(db, id_instance)
        recipients = _recipient_list(meta, all_parts)

        # 1 ── approval_update → wszyscy ──────────────────────────────────────
        await _publish_to_users(
            redis,
            user_ids=recipients,
            event_type="approval_update",
            data={
                **base,
                "action":          "forwarded",
                "id_target_group": id_target_group,
            },
            actor_user_id=id_user,
            include_admins=True,
        )

        # 2 ── document_waiting → target group ────────────────────────────────
        target_members = await _fetch_group_members(db, redis, id_target_group)
        if target_members:
            await _publish_to_users(
                redis,
                user_ids=target_members,
                event_type="document_waiting",
                data={
                    **base,
                    "step_order": meta["current_step"],
                    "id_group":   id_target_group,
                },
                actor_user_id=id_user,
                include_admins=False,
            )

    except Exception as exc:
        logger.warning(
            orjson.dumps({
                "event":       "on_forward_sse_error",
                "id_instance": id_instance,
                "error":       str(exc),
            }).decode()
        )


# =============================================================================
# PUBLIC: on_send_to_group
# =============================================================================

async def on_send_to_group(
    redis: Redis,
    db: AsyncSession,
    id_instance: int,
    id_user: int,
    id_target_group: int,
) -> None:
    """
    SSE po POST /instances/{id}/send-to-group. Wywołaj z bg.add_task().
    Identyczna logika jak on_forward — różni się action w payloadzie.

    Wysyła:
      1. approval_update  → wszyscy uczestnicy + admins
      2. document_waiting → członkowie target group
    """
    try:
        meta = await _fetch_instance_meta(db, id_instance)
        if not meta:
            return

        ts         = datetime.now(timezone.utc).isoformat()
        base       = _build_base_data(meta, ts)
        all_parts  = await _fetch_all_participants(db, id_instance)
        recipients = _recipient_list(meta, all_parts)

        await _publish_to_users(
            redis,
            user_ids=recipients,
            event_type="approval_update",
            data={
                **base,
                "action":          "sent_to_group",
                "id_target_group": id_target_group,
            },
            actor_user_id=id_user,
            include_admins=True,
        )

        target_members = await _fetch_group_members(db, redis, id_target_group)
        if target_members:
            await _publish_to_users(
                redis,
                user_ids=target_members,
                event_type="document_waiting",
                data={
                    **base,
                    "step_order": meta["current_step"],
                    "id_group":   id_target_group,
                },
                actor_user_id=id_user,
                include_admins=False,
            )

    except Exception as exc:
        logger.warning(
            orjson.dumps({
                "event":       "on_send_to_group_sse_error",
                "id_instance": id_instance,
                "error":       str(exc),
            }).decode()
        )


# =============================================================================
# PUBLIC: on_mark_urgent
# =============================================================================

async def on_mark_urgent(
    redis: Redis,
    db: AsyncSession,
    id_instance: int,
    id_user: int,
    is_urgent: bool,
) -> None:
    """
    SSE po POST /instances/{id}/mark-urgent. Wywołaj z bg.add_task().
    approval_update → wszyscy uczestnicy + admins.
    Frontend odświeża listy i drawer (ikona pilności).
    """
    try:
        meta = await _fetch_instance_meta(db, id_instance)
        if not meta:
            return

        all_parts  = await _fetch_all_participants(db, id_instance)
        recipients = _recipient_list(meta, all_parts)

        await _publish_to_users(
            redis,
            user_ids=recipients,
            event_type="approval_update",
            data={
                "id_instance":    id_instance,
                "id_document":    meta["id_document"],
                "document_title": meta["document_title"],
                "action":         "mark_urgent",
                "is_urgent":      is_urgent,
                "status":         meta["status"],
                "ts":             datetime.now(timezone.utc).isoformat(),
            },
            actor_user_id=id_user,
            include_admins=True,
        )

    except Exception as exc:
        logger.warning(
            orjson.dumps({
                "event":       "on_mark_urgent_sse_error",
                "id_instance": id_instance,
                "error":       str(exc),
            }).decode()
        )


# =============================================================================
# PUBLIC: on_delegation_change
# =============================================================================

async def on_delegation_change(
    redis: Redis,
    id_user_from: int,
    id_user_to: int,
    action: str,
    id_group: Optional[int] = None,
    valid_from: Optional[str] = None,
    valid_to: Optional[str] = None,
) -> None:
    """
    SSE po create/cancel delegacji. Wywołaj z bg.add_task().

    Nie potrzebuje db — nie ma instancji do odpytania.
    Wysyła: delegation_update → delegujący + delegat + channel:admins.

    Args:
        action:     "created" | "cancelled"
        id_group:   ID grupy dla której delegacja (None = globalna dla wszystkich grup)
        valid_from: ISO timestamp początku delegacji (do payloadu)
        valid_to:   ISO timestamp końca delegacji (do payloadu)
    """
    try:
        envelope = _build_event_envelope(
            "delegation_update",
            {
                "action":       action,
                "id_user_from": id_user_from,
                "id_user_to":   id_user_to,
                "id_group":     id_group,
                "valid_from":   valid_from,
                "valid_to":     valid_to,
                "ts":           datetime.now(timezone.utc).isoformat(),
            },
            id_user_from,
        )

        # Zapis do JSONL
        _append_event_to_log(envelope)

        # Wysyłka do obu stron delegacji (deduplikacja jeśli from == to)
        for uid in {id_user_from, id_user_to}:
            channel = _CHANNEL_USER.format(uid=uid)
            await _publish_to_channel(redis, channel, envelope)

        # Broadcast do adminów
        await _publish_to_channel(redis, _CHANNEL_ADMINS, envelope)

        logger.info(
            orjson.dumps({
                "event":        "on_delegation_sse_ok",
                "action":       action,
                "id_user_from": id_user_from,
                "id_user_to":   id_user_to,
                "id_group":     id_group,
                "event_id":     envelope.get("event_id"),
            }).decode()
        )

    except Exception as exc:
        logger.warning(
            orjson.dumps({
                "event":        "on_delegation_sse_error",
                "action":       action,
                "id_user_from": id_user_from,
                "id_user_to":   id_user_to,
                "error":        str(exc),
            }).decode()
        )