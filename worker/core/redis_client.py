# =============================================================================
# worker/core/redis_client.py — Klient Redis dla workera
# =============================================================================
# Redis używany przez workera do:
#   - Pub/Sub SSE eventów (publish → API odbiera i streamuje)
#   - Dead Letter Queue (DLQ)
#   - Health-check (arq:health-check key)
#   - Odczyt stanu kolejki ARQ
# =============================================================================

from __future__ import annotations

import json
import logging
from typing import Any, Optional

import redis.asyncio as aioredis
from redis.asyncio import Redis

from worker.settings import get_settings

logger = logging.getLogger("worker.redis")

# ── Klucze Redis ──────────────────────────────────────────────────────────────
CHANNEL_ADMINS       = "channel:admins"           # broadcast do adminów
CHANNEL_USER_PATTERN = "channel:user:{user_id}"   # per-user channel
KEY_DLQ = "windykacja:dlq"                   # Dead Letter Queue (ZSET score=timestamp)
KEY_TASK_RESULTS = "windykacja:task_results" # Hash: task_id → result JSON
KEY_WORKER_HEALTH = "arq:health-check"       # ARQ health check key
ARQ_QUEUE_KEY = "arq:queue:default"          # ARQ job queue (ZSET)

_redis_instance: Optional[Redis] = None


async def init_redis() -> Redis:
    """
    Tworzy i testuje połączenie Redis.
    Wywoływana w ARQ on_startup + FastAPI startup.
    """
    global _redis_instance
    settings = get_settings()

    logger.info(
        "Inicjalizacja Redis",
        extra={
            "host": settings.REDIS_HOST,
            "port": settings.REDIS_PORT,
            "db": settings.REDIS_DB,
        },
    )

    pool = aioredis.ConnectionPool.from_url(
        settings.redis_dsn,
        max_connections=20,
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5,
        retry_on_timeout=True,
    )
    _redis_instance = aioredis.Redis(connection_pool=pool)

    # Test połączenia
    try:
        pong = await _redis_instance.ping()
        logger.info("Połączenie Redis OK", extra={"pong": pong})
    except Exception as exc:
        logger.error(
            "Błąd połączenia Redis",
            extra={"error": str(exc)},
            exc_info=True,
        )
        raise

    return _redis_instance


async def close_redis() -> None:
    global _redis_instance
    if _redis_instance:
        await _redis_instance.aclose()
        logger.info("Połączenie Redis zamknięte")
        _redis_instance = None


def get_redis() -> Redis:
    if _redis_instance is None:
        raise RuntimeError("Redis nie zainicjalizowany — wywołaj init_redis() najpierw")
    return _redis_instance


# =============================================================================
# SSE Publisher
# =============================================================================

async def publish_sse_event(
    event_type: str,
    data: dict[str, Any],
    user_id: Optional[int] = None,
    target_user_id: Optional[int] = None,
) -> None:
    """
    Publikuje event SSE do Redis pub/sub.
    API odbiera i streamuje do podłączonych klientów.

    Args:
        event_type:     Typ eventu (task_completed, system_notification, ...)
        data:           Dane eventu
        user_id:        ID usera który wywołał akcję (opcjonalne)
        target_user_id: Jeśli podane — event tylko dla tego usera (opcjonalne)
    """
    from datetime import datetime, timezone

    payload = {
        "event_type": event_type,
        "ts": datetime.now(timezone.utc).isoformat(),
        "source": "worker",
        "user_id": user_id,
        "target_user_id": target_user_id,
        "data": data,
    }

    redis = get_redis()
    published_to: list[str] = []

    # Publikacja do kanału per-user
    if target_user_id is not None:
        user_channel = CHANNEL_USER_PATTERN.format(user_id=target_user_id)
        try:
            count = await redis.publish(
                user_channel,
                json.dumps(payload, ensure_ascii=False, default=str),
            )
            published_to.append(user_channel)
            logger.debug(
                "SSE event → per-user channel",
                extra={
                    "event_type":  event_type,
                    "channel":     user_channel,
                    "subscribers": count,
                },
            )
        except Exception as exc:
            logger.error(
                "Błąd publikacji SSE event do per-user channel",
                extra={"event_type": event_type, "channel": user_channel, "error": str(exc)},
            )
        # Nie rzucaj — SSE failure nie może blokować głównego taska

    # Broadcast do adminów (zawsze, niezależnie od target_user_id)
    try:
        count = await redis.publish(
            CHANNEL_ADMINS,
            json.dumps(payload, ensure_ascii=False, default=str),
        )
        published_to.append(CHANNEL_ADMINS)
        logger.debug(
            "SSE event → admins channel",
            extra={
                "event_type":  event_type,
                "channel":     CHANNEL_ADMINS,
                "subscribers": count,
            },
        )
    except Exception as exc:
        logger.error(
            "Błąd publikacji SSE event do admins channel",
            extra={"event_type": event_type, "channel": CHANNEL_ADMINS, "error": str(exc)},
        )

async def publish_task_completed(
    task_name: str,
    success_count: int,
    failed_count: int,
    message: str,
    user_id: Optional[int] = None,
    extra: Optional[dict] = None,
) -> None:
    """Shortcut dla task_completed SSE event."""
    await publish_sse_event(
        event_type="task_completed",
        data={
            "task": task_name,
            "success": success_count,
            "failed": failed_count,
            "message": message,
            **(extra or {}),
        },
        user_id=user_id,
    )