"""
Router SSE (Server-Sent Events) — System Windykacja.

2 endpointy [SSE]:
  GET /events/stream         — SSE stream per-user (channel:user:{id})
  GET /events/stream/admins  — SSE stream dla adminów (channel:admins)

Implementacja:
  • Redis Pub/Sub — subskrypcja kanału, yielding wiadomości
  • Heartbeat ping co 30 sekund (keepalive — zapobiega timeout nginx/proxy)
  • Cleanup przy disconnect (anulowanie subskrypcji Redis)
  • Format SSE: `data: {json}\n\n`
  • Każda wiadomość logowana do logs/events_YYYY-MM-DD.jsonl

Format wiadomości SSE (JSON):
  {
    "event": "permissions_updated",   # typ zdarzenia
    "data": { ... },                  # payload
    "ts": "2026-02-20T10:00:00Z"     # timestamp
  }

EventSource (frontend):
  const es = new EventSource('/api/v1/events/stream', {
    headers: { Authorization: 'Bearer ...' }
  });
  es.onmessage = (e) => console.log(JSON.parse(e.data));

"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator

import orjson
from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import StreamingResponse

from app.core.dependencies import (
    CurrentUser,
    RedisClient,
    RequestID,
    require_permission,
)

logger = logging.getLogger(__name__)
router = APIRouter()

# Heartbeat interval — co tyle sekund wysyłamy ping
_HEARTBEAT_INTERVAL = 30

# Maksymalny czas jednej sesji SSE (po tym kliet musi się reconnectować)
# Zabezpieczenie przed zombie connections
_MAX_SESSION_SECONDS = 3600  # 1 godzina


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 1: GET /events/stream
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/stream",
    summary="SSE stream per-user",
    description=(
        "Strumień Server-Sent Events dla aktualnie zalogowanego użytkownika. "
        "Subskrybuje kanał Redis `channel:user:{user_id}`. "
        "\n\n**Typy zdarzeń:**\n"
        "- `task_completed` — zakończenie zadania ARQ (wysyłka monitów, snapshot)\n"
        "- `permissions_updated` — zmiana uprawnień roli użytkownika\n"
        "- `system_notification` — powiadomienie systemowe\n"
        "\n**Heartbeat:** ping co 30s (keepalive). "
        "**Max czas sesji:** 1 godzina (potem wymagane reconnect). "
        "Brak uprawnień RBAC — każdy zalogowany może subskrybować własny kanał."
    ),
    response_description="Strumień SSE (text/event-stream)",
    responses={
        200: {
            "content": {"text/event-stream": {}},
            "description": "SSE stream — połączenie utrzymywane do rozłączenia",
        }
    },
)
async def sse_user_stream(
    request: Request,
    current_user: CurrentUser,
    redis: RedisClient,
    request_id: RequestID,
):
    channel = f"channel:user:{current_user.id_user}"

    logger.info(
        orjson.dumps({
            "event": "sse_connected",
            "channel": channel,
            "user_id": current_user.id_user,
            "request_id": request_id,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return StreamingResponse(
        content=_sse_generator(
            request=request,
            redis=redis,
            channel=channel,
            user_id=current_user.id_user,
            request_id=request_id,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # wyłącz buforowanie nginx
            "Connection": "keep-alive",
            "X-Request-ID": request_id,
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 2: GET /events/stream/admins
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/stream/admins",
    summary="SSE stream dla adminów",
    description=(
        "Strumień Server-Sent Events dla administratorów. "
        "Subskrybuje kanał Redis `channel:admins` — broadcast do wszystkich adminów. "
        "\n\n**Typy zdarzeń:**\n"
        "- `schema_tamper_detected` — naruszenie integralności schematu DB\n"
        "- `permissions_updated` — zmiana macierzy uprawnień\n"
        "- `snapshot_completed` / `snapshot_failed` — wynik snapshotu\n"
        "- `system_notification` — powiadomienia systemowe\n"
        "- `master_key_used` — użycie Master Key (CRITICAL alert)\n"
        "\n**Wymaga uprawnienia:** `system.view_admin_events`"
    ),
    response_description="Strumień SSE adminów (text/event-stream)",
    dependencies=[require_permission("system.view_admin_events")],
    responses={
        200: {
            "content": {"text/event-stream": {}},
            "description": "SSE stream adminów",
        }
    },
)
async def sse_admins_stream(
    request: Request,
    current_user: CurrentUser,
    redis: RedisClient,
    request_id: RequestID,
):
    channel = "channel:admins"

    logger.info(
        orjson.dumps({
            "event": "sse_admin_connected",
            "channel": channel,
            "user_id": current_user.id_user,
            "request_id": request_id,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return StreamingResponse(
        content=_sse_generator(
            request=request,
            redis=redis,
            channel=channel,
            user_id=current_user.id_user,
            request_id=request_id,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
            "X-Request-ID": request_id,
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# GENERATOR SSE
# ─────────────────────────────────────────────────────────────────────────────

async def _sse_generator(
    request: Request,
    redis: RedisClient,
    channel: str,
    user_id: int,
    request_id: str,
) -> AsyncGenerator[bytes, None]:
    """
    Asynchroniczny generator SSE.

    Subskrybuje kanał Redis Pub/Sub.
    Wysyła heartbeat ping co _HEARTBEAT_INTERVAL sekund.
    Kończy przy:
      - rozłączeniu klienta (request.is_disconnected())
      - upłynięciu _MAX_SESSION_SECONDS
      - błędzie Redis
    """
    pubsub = redis.pubsub()
    session_start = asyncio.get_event_loop().time()
    connected = True

    try:
        await pubsub.subscribe(channel)

        # Potwierdzenie połączenia
        yield _sse_event("connected", {
            "channel": channel,
            "message": "Połączenie SSE aktywne",
            "heartbeat_interval": _HEARTBEAT_INTERVAL,
        })

        last_heartbeat = asyncio.get_event_loop().time()

        while connected:
            # Sprawdź timeout sesji
            elapsed = asyncio.get_event_loop().time() - session_start
            if elapsed > _MAX_SESSION_SECONDS:
                yield _sse_event("session_expired", {
                    "message": "Sesja SSE wygasła — połącz ponownie",
                })
                break

            # Sprawdź czy klient się rozłączył
            if await request.is_disconnected():
                logger.info(
                    orjson.dumps({
                        "event": "sse_disconnected",
                        "channel": channel,
                        "user_id": user_id,
                        "session_seconds": int(elapsed),
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }).decode()
                )
                break

            # Heartbeat jeśli minął interwał
            now = asyncio.get_event_loop().time()
            if now - last_heartbeat >= _HEARTBEAT_INTERVAL:
                yield _sse_event("ping", {"ts": datetime.now(timezone.utc).isoformat()})
                last_heartbeat = now

            # Odczytaj wiadomość z Redis (nieblokujące, timeout 1s)
            try:
                message = await asyncio.wait_for(
                    pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0),
                    timeout=2.0,
                )
            except asyncio.TimeoutError:
                message = None
            except Exception as redis_exc:
                logger.error(
                    orjson.dumps({
                        "event": "sse_redis_error",
                        "channel": channel,
                        "error": str(redis_exc),
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }).decode()
                )
                yield _sse_event("error", {"message": "Błąd połączenia z Redis — reconnect"})
                break

            if message and message.get("type") == "message":
                raw_data = message.get("data", b"")
                try:
                    payload = orjson.loads(raw_data)
                    # envelope z event_service.py używa klucza "type", nie "event"
                    event_type = payload.get("type", "message")
                    yield _sse_event(event_type, payload)
                except (orjson.JSONDecodeError, Exception):
                    # Wiadomość nie-JSON — pomiń
                    pass

            # Krótka pauza żeby nie pętlić za szybko
            await asyncio.sleep(0.05)

    except asyncio.CancelledError:
        # Klient rozłączył się nagle
        logger.info(
            orjson.dumps({
                "event": "sse_cancelled",
                "channel": channel,
                "user_id": user_id,
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
    finally:
        # Zawsze odsubskrybuj — zapobiega memory leaks w Redis
        try:
            await pubsub.unsubscribe(channel)
            await pubsub.close()
        except Exception:
            pass


def _sse_event(event_type: str, data: dict) -> bytes:
    """
    Formatuje zdarzenie SSE wg specyfikacji W3C EventSource.

    Format:
        event: {event_type}\n
        data: {json}\n
        \n
    """
    if "ts" not in data:
        data["ts"] = datetime.now(timezone.utc).isoformat()

    payload = orjson.dumps(data).decode()
    return f"event: {event_type}\ndata: {payload}\n\n".encode()