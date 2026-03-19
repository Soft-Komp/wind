# =============================================================================
# worker/services/bcc_service.py
# =============================================================================
# Serwis konfiguracji UDW (BCC) dla wysyłki email.
#
# Priorytet źródeł:
#   1. Redis cache (TTL 300s)
#   2. skw_SystemConfig w bazie
#   3. Zmienne środowiskowe (.env) — fallback awaryjny
#
# Klucze w skw_SystemConfig:
#   bcc.enabled  → "true" / "false"
#   bcc.emails   → "szef@firma.pl,archiwum@firma.pl"
#
# Format listy: adresy oddzielone przecinkami, białe znaki ignorowane.
# Max adresów: 10 (ochrona przed błędną konfiguracją).
#
# Wersja: 1.0.0 | Data: 2026-03-19
# =============================================================================

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import text

from worker.core.db import get_session
from worker.settings import get_settings

logger = logging.getLogger("worker.bcc")

_CACHE_TTL_SECONDS = 300
_CACHE_KEY = "worker:bcc_config"
_MAX_BCC_ADDRESSES = 10
_DB_KEYS = ("bcc.enabled", "bcc.emails")

# Prosty regex walidacji email — nie musi być RFC-kompletny
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@dataclass(frozen=True)
class BccConfig:
    """
    Konfiguracja UDW. Immutable.

    emails: lista już zwalidowanych adresów — gotowa do użycia.
           Pusta gdy bcc.enabled=false lub brak adresów.
    """
    enabled:  bool
    emails:   tuple[str, ...]  # tuple — immutable, hashable
    source:   str              # "database" | "cache" | "env_fallback"

    @property
    def is_active(self) -> bool:
        """True gdy włączone I jest co najmniej jeden adres."""
        return self.enabled and len(self.emails) > 0


def _parse_emails(raw: str) -> tuple[str, ...]:
    """
    Parsuje string z adresami rozdzielonymi przecinkami.
    Waliduje format, ignoruje puste i nieprawidłowe.
    Maksymalnie _MAX_BCC_ADDRESSES adresów.
    """
    if not raw or not raw.strip():
        return ()

    parsed = []
    for part in raw.split(","):
        email = part.strip().lower()
        if not email:
            continue
        if not _EMAIL_RE.match(email):
            logger.warning(
                "BCC: nieprawidłowy adres email — pomijam",
                extra={"email": email},
            )
            continue
        parsed.append(email)
        if len(parsed) >= _MAX_BCC_ADDRESSES:
            logger.warning(
                "BCC: przekroczono limit adresów — obcinam do %d",
                _MAX_BCC_ADDRESSES,
                extra={"total_parsed": len(raw.split(","))},
            )
            break

    return tuple(parsed)


async def get_bcc_config(redis) -> BccConfig:
    """
    Pobiera konfigurację BCC.

    Kolejność:
        1. Redis cache (TTL 300s)
        2. Baza danych skw_SystemConfig
        3. Zmienne środowiskowe .env — fallback awaryjny

    Nigdy nie rzuca wyjątku — zawsze zwraca obiekt.
    W najgorszym przypadku zwraca enabled=False (bezpieczna wartość).
    """
    settings = get_settings()

    # ── 1. Redis cache ────────────────────────────────────────────────────────
    if redis is not None:
        try:
            cached_raw = await redis.get(_CACHE_KEY)
            if cached_raw:
                cached = json.loads(cached_raw)
                config = BccConfig(
                    enabled=bool(cached.get("enabled", False)),
                    emails=tuple(cached.get("emails", [])),
                    source="cache",
                )
                logger.debug(
                    "BccConfig: cache HIT",
                    extra={
                        "enabled":     config.enabled,
                        "email_count": len(config.emails),
                        "is_active":   config.is_active,
                    },
                )
                return config
        except Exception as exc:
            logger.warning(
                "BccConfig: błąd Redis cache — pomijam",
                extra={"error": str(exc)},
            )
    else:
        logger.warning("BccConfig: redis=None — pomijam cache")

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
                            "enabled":   config.enabled,
                            "emails":    list(config.emails),
                            "source":    "database",
                            "cached_at": datetime.now(timezone.utc).isoformat(),
                        }),
                    )
                except Exception as exc:
                    logger.warning(
                        "BccConfig: błąd zapisu Redis cache",
                        extra={"error": str(exc)},
                    )

            logger.info(
                "BccConfig: załadowano z bazy",
                extra={
                    "enabled":     config.enabled,
                    "email_count": len(config.emails),
                    "is_active":   config.is_active,
                },
            )
            return config

    except Exception as exc:
        logger.error(
            "BccConfig: błąd odczytu z bazy — używam .env fallback",
            extra={"error": str(exc), "error_type": type(exc).__name__},
        )

    # ── 3. Fallback .env ──────────────────────────────────────────────────────
    emails = _parse_emails(settings.BCC_EMAILS)
    fallback = BccConfig(
        enabled=settings.BCC_ENABLED,
        emails=emails,
        source="env_fallback",
    )
    logger.warning(
        "BccConfig: używam wartości z .env (fallback awaryjny)",
        extra={
            "enabled":     fallback.enabled,
            "email_count": len(fallback.emails),
            "is_active":   fallback.is_active,
        },
    )
    return fallback


async def invalidate_cache(redis) -> None:
    """Usuwa cache z Redis — wywoływany po PUT /system/config/bcc.*"""
    if redis is None:
        return
    try:
        await redis.delete(_CACHE_KEY)
        logger.info("BccConfig: cache unieważniony")
    except Exception as exc:
        logger.warning(
            "BccConfig: błąd unieważniania cache",
            extra={"error": str(exc)},
        )


async def _load_from_db() -> BccConfig | None:
    """
    Wczytuje klucze bcc.* z skw_SystemConfig.
    Zwraca None gdy brak kluczy.
    """
    values: dict[str, str] = {}

    async with get_session() as db:
        result = await db.execute(
            text("""
                SELECT ConfigKey, ConfigValue
                FROM [dbo_ext].[skw_SystemConfig]
                WHERE ConfigKey IN ('bcc.enabled', 'bcc.emails')
                AND IsActive = 1
            """)
        )
        rows = result.fetchall()

    if not rows:
        logger.warning("BccConfig: brak kluczy bcc.* w skw_SystemConfig")
        return None

    for row in rows:
        values[row[0]] = row[1] or ""

    missing = [k for k in _DB_KEYS if k not in values]
    if missing:
        logger.warning(
            "BccConfig: brakujące klucze w skw_SystemConfig",
            extra={"missing": missing, "found": list(values.keys())},
        )
        return None

    emails = _parse_emails(values["bcc.emails"])

    return BccConfig(
        enabled=values["bcc.enabled"].strip().lower() == "true",
        emails=emails,
        source="database",
    )