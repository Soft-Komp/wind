# =============================================================================
# alertmanager/config.py
# System Windykacja — Alert Manager — Konfiguracja
#
# Ładuje zmienne środowiskowe z .env / docker-compose environment.
# SystemConfig (DB) jest ładowany osobno przez config_service.py.
#
# PRIORYTET:
#   1. SystemConfig (DB) — konfiguracja operacyjna (admini mogą zmieniać)
#   2. .env / environment — fallback + sekrety (hasła SMTP)
# =============================================================================

from __future__ import annotations

import logging
from typing import Optional

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class AlertManagerSettings(BaseSettings):
    """
    Konfiguracja Alert Managera z .env / zmiennych środowiskowych.

    Sekrety (hasła SMTP) NIE trafiają do SystemConfig — zostają w .env.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",          # ignoruj nieznane zmienne z .env
    )

    # -----------------------------------------------------------------------
    # Identyfikacja serwisu
    # -----------------------------------------------------------------------
    service_name: str = Field(
        default="windykacja_alertmanager",
        description="Nazwa serwisu — pojawia się w logach i emailach.",
    )

    environment: str = Field(
        default="production",
        description="Środowisko: development / staging / production.",
        pattern=r"^(development|staging|production)$",
    )

    # -----------------------------------------------------------------------
    # Połączenie z MSSQL (ta sama baza co główny backend)
    # -----------------------------------------------------------------------
    db_host: str = Field(
        description="Host MSSQL — np. 192.168.0.50 lub GPGKJASLO.",
    )
    db_port: int = Field(
        default=1433,
        ge=1,
        le=65535,
        description="Port MSSQL.",
    )
    db_name: str = Field(
        description="Nazwa bazy danych — np. GPGKJASLO.",
    )
    db_user: str = Field(
        description="Login SQL Server dla alertmanagera.",
    )
    db_password: SecretStr = Field(
        description="Hasło SQL Server dla alertmanagera.",
    )
    odbc_driver: str = Field(
        default="ODBC Driver 18 for SQL Server",
        description="Nazwa sterownika ODBC.",
    )

    # -----------------------------------------------------------------------
    # Połączenie z Redis
    # -----------------------------------------------------------------------
    redis_url: str = Field(
        default="redis://redis:6379/0",
        description="URL Redis — redis://host:port/db.",
    )
    redis_password: Optional[SecretStr] = Field(
        default=None,
        description="Hasło Redis (jeśli wymagane).",
    )

    # -----------------------------------------------------------------------
    # SMTP ALERTÓW — własny (dedykowany serwer dla alertów systemowych)
    # -----------------------------------------------------------------------
    alert_smtp_host: Optional[str] = Field(
        default=None,
        description="Host SMTP dla alertów. None = używaj fallbacku z głównego SMTP.",
    )
    alert_smtp_port: int = Field(
        default=587,
        ge=1,
        le=65535,
        description="Port SMTP alertów.",
    )
    alert_smtp_user: Optional[str] = Field(
        default=None,
        description="Login SMTP alertów.",
    )
    alert_smtp_password: Optional[SecretStr] = Field(
        default=None,
        description="Hasło SMTP alertów.",
    )
    alert_smtp_from: Optional[str] = Field(
        default=None,
        description="Adres nadawcy alertów — np. alerty@gpgk.pl.",
    )
    alert_smtp_use_tls: bool = Field(
        default=True,
        description="STARTTLS dla SMTP alertów.",
    )
    alert_smtp_use_ssl: bool = Field(
        default=False,
        description="SSL (port 465) dla SMTP alertów.",
    )
    alert_smtp_timeout: int = Field(
        default=30,
        ge=5,
        le=120,
        description="Timeout połączenia SMTP alertów w sekundach.",
    )

    # -----------------------------------------------------------------------
    # SMTP FALLBACK — główny serwer systemu (z głównego backendu)
    # -----------------------------------------------------------------------
    smtp_host: Optional[str] = Field(
        default=None,
        description="Fallback SMTP host (główny serwer systemu).",
    )
    smtp_port: int = Field(
        default=587,
        ge=1,
        le=65535,
    )
    smtp_user: Optional[str] = Field(default=None)
    smtp_password: Optional[SecretStr] = Field(default=None)
    smtp_from: Optional[str] = Field(default=None)
    smtp_use_tls: bool = Field(default=True)
    smtp_use_ssl: bool = Field(default=False)
    smtp_timeout: int = Field(default=30, ge=5, le=120)

    # -----------------------------------------------------------------------
    # Fallback odbiorcy alertów (z .env, jeśli SystemConfig pusty)
    # -----------------------------------------------------------------------
    alert_recipients_fallback: str = Field(
        default="",
        description=(
            "Fallback: adresy email odbiorców alertów, przecinkiem. "
            "Używany gdy klucz SystemConfig 'alerts.recipients' jest pusty. "
            "Przykład: admin@gpgk.pl,it@gpgk.pl"
        ),
    )

    # -----------------------------------------------------------------------
    # Parametry operacyjne (wartości domyślne — nadpisywane przez SystemConfig)
    # -----------------------------------------------------------------------
    check_interval_seconds: int = Field(
        default=30,
        ge=10,
        le=300,
        description="Co ile sekund wykonywać sprawdzenia health.",
    )
    config_reload_interval_seconds: int = Field(
        default=300,
        ge=60,
        description="Co ile sekund przeładować SystemConfig z bazy danych.",
    )
    log_dir: str = Field(
        default="/app/logs",
        description="Katalog plików logów JSON.",
    )
    log_max_bytes: int = Field(
        default=50 * 1024 * 1024,  # 50 MB
        description="Maksymalny rozmiar jednego pliku logu przed rotacją.",
    )
    log_backup_count: int = Field(
        default=10,
        description="Liczba zachowanych plików archiwalnych logów.",
    )

    # -----------------------------------------------------------------------
    # Kanały Redis pub/sub
    # -----------------------------------------------------------------------
    redis_channel_watchdog_tamper: str = Field(
        default="channel:system:watchdog_tamper",
        description="Kanał Redis — publikowany przez integrity_watchdog przy tamper detection.",
    )
    redis_channel_security_alert: str = Field(
        default="channel:system:security_alert",
        description="Kanał Redis — publikowany przez auth przy brute-force.",
    )

    # -----------------------------------------------------------------------
    # Klucze Redis do sprawdzania stanu
    # -----------------------------------------------------------------------
    redis_key_arq_heartbeat: str = Field(
        default="arq:health:{worker_name}",
        description="Wzorzec klucza Redis heartbeatu workera ARQ.",
    )
    redis_key_brute_force: str = Field(
        default="auth:failures:{identifier}",
        description="Wzorzec klucza Redis licznika błędów logowania.",
    )

    @field_validator("alert_recipients_fallback", mode="before")
    @classmethod
    def strip_recipients(cls, v: str) -> str:
        return v.strip() if v else ""

    @model_validator(mode="after")
    def validate_smtp_config(self) -> "AlertManagerSettings":
        """Ostrzeż jeśli żaden SMTP nie jest skonfigurowany."""
        has_alert_smtp = bool(self.alert_smtp_host and self.alert_smtp_user)
        has_fallback_smtp = bool(self.smtp_host and self.smtp_user)
        if not has_alert_smtp and not has_fallback_smtp:
            logger.warning(
                "[config] UWAGA: Brak konfiguracji SMTP! "
                "Alerty NIE będą wysyłane emailem. "
                "Ustaw ALERT_SMTP_HOST lub SMTP_HOST w .env."
            )
        return self

    @property
    def db_connection_string(self) -> str:
        """Zwraca connection string pyodbc."""
        pwd = self.db_password.get_secret_value()
        return (
            f"DRIVER={{{self.odbc_driver}}};"
            f"SERVER={self.db_host},{self.db_port};"
            f"DATABASE={self.db_name};"
            f"UID={self.db_user};"
            f"PWD={pwd};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=10;"
        )

    @property
    def redis_connection_kwargs(self) -> dict:
        """Parametry połączenia Redis."""
        kwargs = {"url": self.redis_url, "decode_responses": True}
        if self.redis_password:
            kwargs["password"] = self.redis_password.get_secret_value()
        return kwargs