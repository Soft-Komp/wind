# =============================================================================
# worker/settings.py — Konfiguracja workera
# =============================================================================
# Wszystkie dane z .env — identyczne jak backend, plus WORKER_* specyficzne.
# Pydantic v2 z validacją typów.
# =============================================================================

from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any, Optional

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings


class SMTPConfig(BaseSettings):
    """Konfiguracja jednej skrzynki SMTP."""
    host: str
    port: int = 587
    user: str
    password: str
    from_email: str
    from_name: str = "System Windykacja"
    use_tls: bool = True
    use_ssl: bool = False
    timeout: int = 30

    model_config = {"extra": "ignore"}


class Settings(BaseSettings):
    """
    Główna konfiguracja workera.
    Wczytywana z .env (ten sam plik co backend).
    """

    # ── Database ──────────────────────────────────────────────────────────────
    DB_HOST: str = Field(..., description="Host MSSQL")
    DB_PORT: int = Field(1433, description="Port MSSQL")
    DB_NAME: str = Field(..., description="Nazwa bazy MSSQL")
    DB_USER: str = Field(..., description="Użytkownik DB (worker)")
    DB_PASSWORD: str = Field(..., description="Hasło DB (worker)")
    ODBC_DRIVER: str = Field("ODBC Driver 18 for SQL Server")

    # Worker może mieć osobne credentials — fallback na DB_* jeśli nie podano
    WORKER_DB_USER: Optional[str] = Field(None)
    WORKER_DB_PASSWORD: Optional[str] = Field(None)

    # ── Redis ─────────────────────────────────────────────────────────────────
    REDIS_HOST: str = Field("redis")
    REDIS_PORT: int = Field(6379)
    REDIS_PASSWORD: Optional[str] = Field(None)
    REDIS_DB: int = Field(0)

    # ── Worker REST API ───────────────────────────────────────────────────────
    WORKER_PORT: int = Field(8001)
    WORKER_SECRET_KEY: str = Field(..., description="X-Worker-Key header secret")
    WORKER_API_TITLE: str = "Windykacja Worker API"

    # ── SMTP (JSON array) ──────────────────────────────────────────────────────
    # Format: '[{"host":"smtp1.com","port":587,"user":"x","password":"y","from_email":"z@x.com","from_name":"Windykacja"}]'
    SMTP_CONFIGS_JSON: str = Field(
        default="[]",
        alias="SMTP_CONFIGS",
        description="JSON array konfiguracji SMTP (failover chain)",
    )

    # ── SMSAPI ────────────────────────────────────────────────────────────────
    SMSAPI_TOKEN: str = Field(default="", description="Token API SMSAPI.pl")
    SMSAPI_SENDER: str = Field(default="Windykacja", max_length=11)
    SMSAPI_URL: str = Field(default="https://api.smsapi.pl/sms.do")
    SMSAPI_TEST_MODE: bool = Field(default=False, description="True = nie wysyła SMS")

    # ── Tryb demonstracyjny ───────────────────────────────────────────────────
    DEMO_MODE: bool = Field(
        default=True,
        description=(
            "Tryb demonstracyjny — blokuje wysyłkę email/SMS/PDF. "
            "True = klient ogląda system, wysyłka zablokowana. "
            "False = tryb produkcyjny, wysyłka aktywna. "
            "Domyślnie True — bezpieczna wartość dla nowych wdrożeń."
        ),
    )

    # ── Tryb testowy wysyłki (fallback — baza ma pierwszeństwo) ──────────────
    # Wartości z skw_SystemConfig nadpisują te zmienne przy każdym tasku.
    # Te wartości używane tylko gdy DB niedostępna lub klucz nie istnieje w DB.
    TEST_MODE_ENABLED: bool = Field(
        default=False,
        description=(
            "Fallback: czy przekierowywać wysyłkę na adresy testowe. "
            "Nadpisywane przez skw_SystemConfig['test_mode.enabled']."
        ),
    )
    TEST_MODE_EMAIL: str = Field(
        default="",
        description=(
            "Fallback: testowy adres email. "
            "Nadpisywany przez skw_SystemConfig['test_mode.email']."
        ),
    )
    TEST_MODE_PHONE: str = Field(
        default="",
        description=(
            "Fallback: testowy numer telefonu. "
            "Nadpisywany przez skw_SystemConfig['test_mode.phone']."
        ),
    )

    # ── UDW / BCC (fallback — baza ma pierwszeństwo) ──────────────────────────
    BCC_ENABLED: bool = Field(
        default=False,
        description=(
            "Fallback: czy dodawać UDW do emaili. "
            "Nadpisywany przez skw_SystemConfig['bcc.enabled']."
        ),
    )
    BCC_EMAILS: str = Field(
        default="",
        description=(
            "Fallback: lista adresów UDW oddzielona przecinkami. "
            "Nadpisywana przez skw_SystemConfig['bcc.emails']."
        ),
    )
    SMSAPI_FALLBACK_PHONE: str = Field(
        default="",
        description=(
            "Numer testowy — używany gdy Recipient jest pusty. "
            "Format: 48XXXXXXXXX lub XXXXXXXXX. "
            "Pusty = brak fallbacku, monit z pustym numerem trafia do failed."
        ),
    )
    
    # ── Paths ─────────────────────────────────────────────────────────────────
    LOG_DIR: str = Field(default="/app/logs")
    SNAPSHOT_DIR: str = Field(default="/app/snapshots")
    ARCHIVE_DIR: str = Field(default="/app/archives")
    PDF_CACHE_DIR: str = Field(default="/app/pdf_cache")

    # ── Retry policy ─────────────────────────────────────────────────────────
    TASK_MAX_RETRIES: int = Field(default=3, ge=1, le=10)
    # Opóźnienia w sekundach: [10, 60, 300] = 10s → 1min → 5min
    RETRY_DELAYS_JSON: str = Field(default="[10, 60, 300]", alias="RETRY_DELAYS")

    # ── ARQ worker ────────────────────────────────────────────────────────────
    ARQ_MAX_JOBS: int = Field(default=10, ge=1, le=100)
    ARQ_JOB_TIMEOUT: int = Field(default=300, description="Timeout jobu w sekundach")
    ARQ_HEALTH_CHECK_INTERVAL: int = Field(default=30)
    ARQ_QUEUE_NAME: str = Field(default="arq:queue:default")

    # ── Firma (PDF) ───────────────────────────────────────────────────────────
    COMPANY_NAME: str = Field(default="Firma Sp. z o.o.")
    COMPANY_NIP: str = Field(default="")
    COMPANY_REGON: str = Field(default="")
    COMPANY_ADDRESS: str = Field(default="")
    COMPANY_PHONE: str = Field(default="")
    COMPANY_EMAIL: str = Field(default="")
    LOGO_PATH: Optional[str] = Field(default=None)

    # ── Snapshot ─────────────────────────────────────────────────────────────
    SNAPSHOT_RETENTION_DAYS: int = Field(default=30, ge=1)

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
        "populate_by_name": True,
    }

    # ── Validators ────────────────────────────────────────────────────────────

    @field_validator("SMTP_CONFIGS_JSON", mode="before")
    @classmethod
    def validate_smtp_json(cls, v: Any) -> str:
        if isinstance(v, list):
            return json.dumps(v)
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if not isinstance(parsed, list):
                    raise ValueError("SMTP_CONFIGS musi być listą JSON")
            except json.JSONDecodeError as e:
                raise ValueError(f"Nieprawidłowy JSON w SMTP_CONFIGS: {e}")
        return str(v)

    @field_validator("RETRY_DELAYS_JSON", mode="before")
    @classmethod
    def validate_retry_delays(cls, v: Any) -> str:
        if isinstance(v, list):
            return json.dumps(v)
        return str(v)

    @field_validator("WORKER_SECRET_KEY")
    @classmethod
    def validate_secret_key(cls, v: str) -> str:
        if len(v) < 32:
            raise ValueError("WORKER_SECRET_KEY musi mieć min. 32 znaki")
        return v

    # ── Computed properties ───────────────────────────────────────────────────

    @property
    def smtp_configs(self) -> list[SMTPConfig]:
        """Parsuje SMTP_CONFIGS_JSON → lista SMTPConfig."""
        raw = json.loads(self.SMTP_CONFIGS_JSON)
        return [SMTPConfig(**cfg) for cfg in raw]

    @property
    def retry_delays(self) -> list[int]:
        """Parsuje RETRY_DELAYS_JSON → lista intów."""
        return json.loads(self.RETRY_DELAYS_JSON)

    @property
    def effective_db_user(self) -> str:
        return self.WORKER_DB_USER or self.DB_USER

    @property
    def effective_db_password(self) -> str:
        return self.WORKER_DB_PASSWORD or self.DB_PASSWORD

    @property
    def db_connection_string(self) -> str:
        return (
            f"DRIVER={{{self.ODBC_DRIVER}}};"
            f"SERVER={self.DB_HOST},{self.DB_PORT};"
            f"DATABASE={self.DB_NAME};"
            f"UID={self.effective_db_user};"
            f"PWD={self.effective_db_password};"
            "TrustServerCertificate=yes;"
            "Encrypt=yes;"
        )

    @property
    def redis_dsn(self) -> str:
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Singleton ustawień — wczytuje raz, cache'uje."""
    return Settings()