# =============================================================================
# backend/app/core/arq_pool.py — ARQ pool dla backendu
# =============================================================================
# Backend używa tej puli do enqueue tasków do ARQ workera.
# Inicjalizowana przy starcie FastAPI, zamykana przy shutdown.
# =============================================================================

from __future__ import annotations

import logging
from typing import Optional

from arq import create_pool
from arq.connections import ArqRedis, RedisSettings

logger = logging.getLogger("app.arq_pool")

_arq_pool: Optional[ArqRedis] = None


def _get_redis_settings() -> RedisSettings:
    """Buduje RedisSettings z app settings."""
    from app.core.config import settings
    return RedisSettings(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        password=settings.REDIS_PASSWORD or None,
        database=settings.REDIS_DB,
        conn_timeout=10,
        conn_retries=5,
        conn_retry_delay=2,
    )


async def init_arq_pool() -> ArqRedis:
    """
    Inicjalizuje pulę ARQ.
    Wywoływana w FastAPI lifespan/startup.
    """
    global _arq_pool
    redis_settings = _get_redis_settings()

    logger.info(
        "Inicjalizacja ARQ pool",
        extra={
            "host": redis_settings.host,
            "port": redis_settings.port,
        },
    )

    _arq_pool = await create_pool(redis_settings)

    # Test połączenia
    try:
        await _arq_pool.ping()
        logger.info("ARQ pool gotowy")
    except Exception as exc:
        logger.error("Błąd testu ARQ pool", extra={"error": str(exc)})
        raise

    return _arq_pool


async def close_arq_pool() -> None:
    """Zamyka pulę ARQ. Wywoływana w FastAPI shutdown."""
    global _arq_pool
    if _arq_pool:
        await _arq_pool.aclose()
        _arq_pool = None
        logger.info("ARQ pool zamknięta")


def get_arq_pool() -> ArqRedis:
    """
    Zwraca aktywną pulę ARQ.
    Używana jako FastAPI Dependency.

    Raises:
        RuntimeError: Jeśli pool nie zainicjalizowany.
    """
    if _arq_pool is None:
        raise RuntimeError("ARQ pool nie zainicjalizowana — init_arq_pool() nie wywołane")
    return _arq_pool


# FastAPI Dependency
async def get_arq() -> ArqRedis:
    """Dependency dla endpointów które enqueue taski."""
    return get_arq_pool()