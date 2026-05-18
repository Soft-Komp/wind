"""
Distributed lock oparty na Redis SET NX PX.

Wzorzec: SET key value NX PX ttl_ms
  NX  — ustaw tylko jesli klucz nie istnieje
  PX  — TTL w milisekundach (automatyczne wygasniecie — bez ryzyka deadlocka)

Uzycie w approval_service.py:
    async with approval_lock(redis, instance_id):
        # krytyczna sekcja — max jeden worker naraz

Jezeli lock niedostepny → HTTP 423 Locked (zgodnie z RFC 4918).
Timeout i TTL konfigurowalne.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import HTTPException, status
from redis.asyncio import Redis

logger = logging.getLogger(__name__)

# ── Domyslne parametry ────────────────────────────────────────────────────────
_DEFAULT_TTL_MS: int   = 10_000   # 10 sekund — max czas trzymania locka
_DEFAULT_TIMEOUT: float = 5.0     # 5 sekund — max czas oczekiwania na lock
_POLL_INTERVAL: float  = 0.05     # 50ms — czestotliwosc odpytywania

_KEY_PREFIX = "approval:lock:"


def _lock_key(instance_id: int) -> str:
    return f"{_KEY_PREFIX}{instance_id}"


@asynccontextmanager
async def approval_lock(
    redis: Redis,
    instance_id: int,
    *,
    ttl_ms: int = _DEFAULT_TTL_MS,
    timeout_s: float = _DEFAULT_TIMEOUT,
) -> AsyncGenerator[None, None]:
    """
    Asynchroniczny context manager — distributed lock dla instancji obiegu.

    Args:
        redis:       Klient Redis.
        instance_id: ID instancji — klucz locka to 'approval:lock:<id>'.
        ttl_ms:      TTL locka w ms. Po uplywie lock automatycznie wygasa.
        timeout_s:   Max czas oczekiwania na lock w sekundach.

    Raises:
        HTTPException(423): Lock niedostepny po uplywie timeout_s.
        HTTPException(500): Blad Redis.

    Przyklad:
        async with approval_lock(redis, instance_id=42):
            await approval_service.accept(...)
    """
    key   = _lock_key(instance_id)
    token = str(uuid.uuid4())  # Unikalny token — owner locka

    acquired = False
    deadline = asyncio.get_event_loop().time() + timeout_s

    try:
        # ── Probuj zdobyc lock ────────────────────────────────────────────────
        while asyncio.get_event_loop().time() < deadline:
            try:
                result = await redis.set(key, token, nx=True, px=ttl_ms)
            except Exception as exc:
                logger.error(
                    "approval_lock | Redis error przy SET NX | "
                    "instance=%d key=%s error=%s",
                    instance_id, key, exc,
                )
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Blad wewnetrzny — lock niedostepny (Redis error)",
                ) from exc

            if result:
                acquired = True
                logger.debug(
                    "approval_lock | ACQUIRED | instance=%d token=%s ttl_ms=%d",
                    instance_id, token[:8], ttl_ms,
                )
                break

            await asyncio.sleep(_POLL_INTERVAL)

        if not acquired:
            logger.warning(
                "approval_lock | TIMEOUT | instance=%d timeout=%.1fs",
                instance_id, timeout_s,
            )
            raise HTTPException(
                status_code=status.HTTP_423_LOCKED,
                detail=(
                    f"Dokument (instancja {instance_id}) jest aktualnie "
                    "przetwarzany. Sprobuj za chwile."
                ),
            )

        # ── Sekcja krytyczna ──────────────────────────────────────────────────
        yield

    finally:
        if acquired:
            # Zwolnij lock TYLKO jesli nadal nalezy do nas (token match).
            # Unika sytuacji: TTL wygas, inny worker przejal lock,
            # a my go zwalniamy przez pomylke.
            try:
                current = await redis.get(key)
                if current and current.decode() == token:
                    await redis.delete(key)
                    logger.debug(
                        "approval_lock | RELEASED | instance=%d token=%s",
                        instance_id, token[:8],
                    )
                else:
                    logger.warning(
                        "approval_lock | TTL EXPIRED before release | "
                        "instance=%d token=%s",
                        instance_id, token[:8],
                    )
            except Exception as exc:
                logger.error(
                    "approval_lock | Blad przy zwolnieniu locka | "
                    "instance=%d error=%s",
                    instance_id, exc,
                )
                # Nie rzucamy — lock wygasnie sam przez TTL