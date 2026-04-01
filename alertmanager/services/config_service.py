# =============================================================================
# alertmanager/services/config_service.py
# System Windykacja — Alert Manager — Serwis konfiguracji
#
# Ładuje RuntimeConfig z tabeli dbo_ext.skw_SystemConfig.
# Uruchamiany przy starcie i co config_reload_interval_seconds sekund.
#
# Priorytet:
#   1. SystemConfig (DB) — wartości operacyjne
#   2. .env fallback (settings) — gdy DB niedostępna lub klucz brakuje
# =============================================================================

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from functools import partial
from typing import Any, Optional

import pyodbc

from models.alert import RuntimeConfig

logger = logging.getLogger("alertmanager.services.config_service")

# Klucze SystemConfig których szukamy w bazie
_CONFIG_KEYS = [
    "alerts.enabled",
    "alerts.recipients",
    "alerts.cooldown_minutes",
    "alerts.brute_force_threshold",
    "alerts.worker_heartbeat_timeout_seconds",
    "alerts.db_latency_warn_ms",
    "alerts.dlq_overflow_threshold",
    "alerts.snapshot_expected_hour",
]


def _sync_load_config(connection_string: str) -> dict[str, str]:
    """
    Synchroniczne ładowanie SystemConfig z MSSQL — uruchamiane w executor.

    Returns:
        Słownik {ConfigKey: ConfigValue} — wszystkie aktywne wpisy alerts.*
    """
    logger.debug("Ładowanie SystemConfig z bazy...")
    result: dict[str, str] = {}

    try:
        conn = pyodbc.connect(connection_string, autocommit=True)
        try:
            cursor = conn.cursor()
            # Ładuj tylko aktywne klucze z prefiksem alerts.*
            cursor.execute(
                """
                SELECT [ConfigKey], [ConfigValue]
                FROM [dbo_ext].[skw_SystemConfig]
                WHERE [IsActive] = 1
                  AND [ConfigKey] LIKE 'alerts.%'
                ORDER BY [ConfigKey]
                """
            )
            rows = cursor.fetchall()
            for row in rows:
                key = str(row.ConfigKey).strip()
                val = str(row.ConfigValue).strip() if row.ConfigValue is not None else ""
                result[key] = val

            logger.debug(
                "SystemConfig załadowany: %d kluczy alerts.*",
                len(result),
            )
        finally:
            conn.close()

    except pyodbc.Error as exc:
        logger.error(
            "Błąd MSSQL podczas ładowania SystemConfig: %s", exc
        )
        raise
    except Exception as exc:
        logger.error(
            "Nieoczekiwany błąd podczas ładowania SystemConfig: %s",
            exc, exc_info=True,
        )
        raise

    return result


def _parse_bool(value: str, default: bool = True) -> bool:
    """Parsuj wartość tekstową jako bool."""
    return value.lower() in ("true", "1", "yes", "tak")


def _parse_int(value: str, default: int) -> int:
    """Parsuj wartość tekstową jako int."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _parse_float(value: str, default: float) -> float:
    """Parsuj wartość tekstową jako float."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _parse_email_list(value: str) -> list[str]:
    """
    Parsuj listę emaili rozdzielonych przecinkami.
    Filtruje puste stringi i waliduje minimalny format (@).
    """
    if not value:
        return []
    emails = []
    for email in value.split(","):
        email = email.strip().lower()
        if email and "@" in email and "." in email.split("@")[-1]:
            emails.append(email)
        elif email:
            logger.warning("Niepoprawny adres email w konfiguracji: '%s'", email)
    return emails


async def load_runtime_config(
    connection_string: str,
    fallback_recipients: str,
) -> RuntimeConfig:
    """
    Asynchronicznie ładuje RuntimeConfig z SystemConfig.

    Args:
        connection_string: pyodbc connection string do MSSQL
        fallback_recipients: adresy z .env (jeśli DB pusta)

    Returns:
        RuntimeConfig z załadowanymi wartościami.
        Przy błędzie DB — zwraca RuntimeConfig z wartościami domyślnymi
        i ustawionym load_error.
    """
    now = datetime.now(timezone.utc)

    try:
        loop = asyncio.get_event_loop()
        raw = await loop.run_in_executor(
            None,
            partial(_sync_load_config, connection_string),
        )
    except Exception as exc:
        logger.warning(
            "Nie udało się załadować SystemConfig — używam wartości domyślnych: %s",
            exc,
        )
        # Użyj fallbacku z .env
        fallback = _parse_email_list(fallback_recipients)
        config = RuntimeConfig(
            alerts_enabled=True,        # zakładamy włączone
            recipients=fallback,
            load_error=str(exc),
            loaded_at=now,
        )
        logger.info(
            "RuntimeConfig (fallback): alerts_enabled=%s, recipients=%d",
            config.alerts_enabled,
            len(config.recipients),
        )
        return config

    # ── Parsowanie załadowanych wartości ──────────────────────────────────
    recipients_str = raw.get("alerts.recipients", "")
    recipients = _parse_email_list(recipients_str)

    # Fallback z .env jeśli SystemConfig pusty
    if not recipients and fallback_recipients:
        recipients = _parse_email_list(fallback_recipients)
        logger.info(
            "alerts.recipients puste w SystemConfig — używam fallbacku z .env: %s",
            fallback_recipients[:100],
        )

    config = RuntimeConfig(
        alerts_enabled=_parse_bool(
            raw.get("alerts.enabled", "true"), default=True
        ),
        recipients=recipients,
        cooldown_minutes=_parse_int(
            raw.get("alerts.cooldown_minutes", "15"), default=15
        ),
        brute_force_threshold=_parse_int(
            raw.get("alerts.brute_force_threshold", "10"), default=10
        ),
        worker_heartbeat_timeout_seconds=_parse_int(
            raw.get("alerts.worker_heartbeat_timeout_seconds", "120"), default=120
        ),
        db_latency_warn_ms=_parse_float(
            raw.get("alerts.db_latency_warn_ms", "500"), default=500.0
        ),
        dlq_overflow_threshold=_parse_int(
            raw.get("alerts.dlq_overflow_threshold", "10"), default=10
        ),
        snapshot_expected_hour=_parse_int(
            raw.get("alerts.snapshot_expected_hour", "3"), default=3
        ),
        loaded_at=now,
        load_error=None,
    )

    logger.info(
        "RuntimeConfig załadowany z SystemConfig: "
        "enabled=%s, recipients=%d, cooldown=%dmin",
        config.alerts_enabled,
        len(config.recipients),
        config.cooldown_minutes,
        extra=config.to_dict(),
    )

    return config