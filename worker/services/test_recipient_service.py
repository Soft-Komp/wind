# =============================================================================
# Serwis odczytu konfiguracji trybu testowego wysyłki.
#
# Priorytet źródeł (od najwyższego):
#   1. skw_SystemConfig w bazie (cachowane Redis TTL 300s)
#   2. Zmienne środowiskowe (.env) — fallback gdy DB niedostępna
#
# Klucze w skw_SystemConfig:
#   test_mode.enabled  → "true" / "false"
#   test_mode.email    → adres email
#   test_mode.phone    → numer telefonu
#
# WAŻNE: używa ctx["worker_redis"] — NIE ctx["redis"] (to ARQ's własny Redis)
# =============================================================================

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import text

from worker.core.db import get_session
from worker.settings import get_settings

logger = logging.getLogger("worker.test_recipient")

_CACHE_TTL_SECONDS = 300
_CACHE_KEY = "worker:test_recipient_config"

_DB_KEYS = (
    "test_mode.enabled",
    "test_mode.email",
    "test_mode.phone",
)


@dataclass(frozen=True)
class TestRecipientConfig:
    """Konfiguracja trybu testowego. Immutable."""
    enabled:    bool
    test_email: str
    test_phone: str
    source:     str  # "database" | "env_fallback" | "cache"


async def get_test_recipient_config(redis) -> TestRecipientConfig:
    """
    Pobiera konfigurację trybu testowego.

    Kolejność:
        1. Redis cache (TTL 300s)
        2. Baza danych skw_SystemConfig
        3. Zmienne środowiskowe .env — fallback awaryjny

    Nigdy nie rzuca wyjątku — zawsze zwraca obiekt.
    W najgorszym przypadku zwraca enabled=False (bezpieczna wartość).
    """
    settings = get_settings()

    # ── 0. Sprawdź bypass cache ───────────────────────────────────────────────
    bypass = await _is_cache_bypassed_worker()

    # ── 1. Redis cache — pomijany gdy bypass aktywny ──────────────────────────
    if not bypass and redis is not None:
        try:
            cached_raw = await redis.get(_CACHE_KEY)
            if cached_raw:
                cached = json.loads(cached_raw)
                logger.debug(
                    "TestRecipientConfig: cache HIT",
                    extra={"enabled": cached.get("enabled"), "source": "cache"},
                )
                return TestRecipientConfig(
                    enabled=bool(cached.get("enabled", False)),
                    test_email=str(cached.get("test_email", "")),
                    test_phone=str(cached.get("test_phone", "")),
                    source="cache",
                )
        except Exception as exc:
            logger.warning(
                "TestRecipientConfig: błąd Redis cache — pomijam",
                extra={"error": str(exc)},
            )
    elif bypass:
        logger.debug(
            "TestRecipientConfig: BYPASS aktywny — idę prosto do DB",
            extra={"source": "db_bypass"},
        )
    else:
        logger.warning("TestRecipientConfig: redis=None — pomijam cache")

    # ── 2. Baza danych ────────────────────────────────────────────────────────
    try:
        config = await _load_from_db()
        if config is not None:
            # Zapisz do Redis cache
            if redis is not None:
                try:
                    await redis.setex(
                        _CACHE_KEY,
                        _CACHE_TTL_SECONDS,
                        json.dumps({
                            "enabled":    config.enabled,
                            "test_email": config.test_email,
                            "test_phone": config.test_phone,
                            "source":     "database",
                            "cached_at":  datetime.now(timezone.utc).isoformat(),
                        }),
                    )
                except Exception as exc:
                    logger.warning(
                        "TestRecipientConfig: błąd zapisu Redis cache",
                        extra={"error": str(exc)},
                    )

            logger.info(
                "TestRecipientConfig: załadowano z bazy",
                extra={
                    "enabled":   config.enabled,
                    "has_email": bool(config.test_email),
                    "has_phone": bool(config.test_phone),
                },
            )
            return config

    except Exception as exc:
        logger.error(
            "TestRecipientConfig: błąd odczytu z bazy — używam .env fallback",
            extra={"error": str(exc), "error_type": type(exc).__name__},
        )

    # ── 3. Fallback .env ──────────────────────────────────────────────────────
    fallback = TestRecipientConfig(
        enabled=settings.TEST_MODE_ENABLED,
        test_email=settings.TEST_MODE_EMAIL,
        test_phone=settings.TEST_MODE_PHONE,
        source="env_fallback",
    )
    logger.warning(
        "TestRecipientConfig: używam wartości z .env (fallback awaryjny)",
        extra={
            "enabled":   fallback.enabled,
            "has_email": bool(fallback.test_email),
            "has_phone": bool(fallback.test_phone),
        },
    )
    return fallback


async def invalidate_cache(redis) -> None:
    """Usuwa cache z Redis — wywoływany po PUT /system/config/test_mode.*"""
    if redis is None:
        return
    try:
        await redis.delete(_CACHE_KEY)
        logger.info("TestRecipientConfig: cache unieważniony")
    except Exception as exc:
        logger.warning(
            "TestRecipientConfig: błąd unieważniania cache",
            extra={"error": str(exc)},
        )


async def _load_from_db() -> TestRecipientConfig | None:
    """
    Wczytuje 3 klucze z skw_SystemConfig.
    Zwraca None gdy brak kluczy lub niepełna konfiguracja.
    """
    values: dict[str, str] = {}

    async with get_session() as db:
        result = await db.execute(
            text("""
                SELECT ConfigKey, ConfigValue
                FROM [dbo_ext].[skw_SystemConfig]
                WHERE ConfigKey IN (
                    'test_mode.enabled',
                    'test_mode.email',
                    'test_mode.phone'
                )
                AND IsActive = 1
            """)
        )
        rows = result.fetchall()

    if not rows:
        logger.warning("TestRecipientConfig: brak kluczy test_mode.* w skw_SystemConfig")
        return None

    for row in rows:
        values[row[0]] = row[1] or ""

    # Sprawdź kompletność
    missing = [k for k in _DB_KEYS if k not in values]
    if missing:
        logger.warning(
            "TestRecipientConfig: brakujące klucze w skw_SystemConfig",
            extra={"missing": missing, "found": list(values.keys())},
        )
        return None

    return TestRecipientConfig(
        enabled=values["test_mode.enabled"].strip().lower() == "true",
        test_email=values["test_mode.email"].strip(),
        test_phone=values["test_mode.phone"].strip(),
        source="database",
    )

async def _is_cache_bypassed_worker() -> bool:
    """
    Sprawdza cache.bypass_enabled bezpośrednio z DB — wersja dla workera.
    Worker nie używa FastAPI DI, więc otwiera własną sesję.
    Nigdy nie rzuca wyjątku — przy błędzie zwraca False (bezpieczna wartość).
    """
    try:
        async with get_session() as db:
            result = await db.execute(
                text("""
                    SELECT [ConfigValue]
                    FROM [dbo_ext].[skw_SystemConfig]
                    WHERE [ConfigKey] = N'cache.bypass_enabled'
                      AND [IsActive] = 1
                """)
            )
            row = result.fetchone()
            if row is None:
                return False
            return str(row[0]).strip().lower() in ("true", "1", "yes", "tak")
    except Exception as exc:
        logger.warning(
            "Worker: błąd sprawdzenia cache bypass — domyślnie False",
            extra={"error": str(exc), "error_type": type(exc).__name__},
        )
        return False