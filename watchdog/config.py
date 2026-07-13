# =============================================================================
# watchdog/config.py — Konfiguracja Watchdoga (kolekcje ustawień ENV/DB)
# =============================================================================
# Nazwy zmiennych DB_*/REDIS_*/SMTP_* zgodne 1:1 z worker/settings.py — ten
# sam .env, współdzielony przez docker-compose (decyzja: wspólny env_file).
# =============================================================================
from __future__ import annotations

import socket
from functools import lru_cache
from typing import Optional

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class WatchdogSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8",
        case_sensitive=False, extra="ignore",
    )

    # ── DB — te same nazwy co worker/settings.py, ten sam user (decyzja) ────
    DB_HOST: str = Field(..., description="Host MSSQL")
    DB_PORT: int = Field(1433)
    DB_NAME: str = Field(...)
    DB_USER: str = Field(...)
    DB_PASSWORD: SecretStr = Field(...)
    ODBC_DRIVER: str = Field("ODBC Driver 18 for SQL Server")

    # ── Redis ─────────────────────────────────────────────────────────────
    REDIS_HOST: str = Field("redis")
    REDIS_PORT: int = Field(6379)
    REDIS_PASSWORD: Optional[SecretStr] = Field(None)
    REDIS_DB: int = Field(0)

    # ── Watchdog — parametry własne ───────────────────────────────────────
    WATCHDOG_CHECK_INTERVAL_SECONDS: int = Field(default=180, ge=30, le=1800)
    WATCHDOG_INSTANCE_ID: str = Field(default_factory=socket.gethostname)
    WATCHDOG_ALERT_EMAIL: str = Field(..., description="Adres odbiorcy alertów Watchdoga.")

    # ── SMTP — TA SAMA konfiguracja co worker (SMTP_CONFIGS_JSON, aiosmtplib) ──
    SMTP_CONFIGS_JSON: str = Field(default="[]", alias="SMTP_CONFIGS")

    LOG_DIR: str = Field(default="/app/logs")

    @property
    def db_connection_string(self) -> str:
        return (
            f"DRIVER={{{self.ODBC_DRIVER}}};"
            f"SERVER={self.DB_HOST},{self.DB_PORT};DATABASE={self.DB_NAME};"
            f"UID={self.DB_USER};PWD={self.DB_PASSWORD.get_secret_value()};"
            "TrustServerCertificate=yes;Encrypt=yes;"
        )

    @property
    def redis_dsn(self) -> str:
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD.get_secret_value()}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    @property
    def smtp_configs(self) -> list[dict]:
        import json
        return json.loads(self.SMTP_CONFIGS_JSON)


@lru_cache(maxsize=1)
def get_settings() -> WatchdogSettings:
    return WatchdogSettings()