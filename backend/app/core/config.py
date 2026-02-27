"""
Konfiguracja aplikacji — centralne źródło prawdy dla wszystkich ustawień.

Zasady:
  1. WSZYSTKIE zmienne środowiskowe wczytywane przez ten moduł — nigdzie indziej
  2. Sekrety (hasła, klucze) jako SecretStr — nie pojawiają się w logach
  3. Walidacja przy starcie — aplikacja nie ruszy z brakującymi/błędnymi wartościami
  4. Singleton — jeden obiekt `settings` importowany wszędzie
  5. Metoda `get_odbc_dsn()` buduje connection string bez ujawniania hasła w logach

Użycie:
    from app.core.config import settings

    print(settings.db_host)
    print(settings.get_odbc_dsn())  # Bezpieczny DSN dla aioodbc
"""

from __future__ import annotations

import logging
import os
import sys
from enum import Enum
from pathlib import Path
from typing import List, Optional
from urllib.parse import quote_plus

from pydantic import (
    AnyHttpUrl,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Typy pomocnicze
# ---------------------------------------------------------------------------

class AppEnvironment(str, Enum):
    """Środowisko uruchomienia aplikacji."""
    DEVELOPMENT = "development"
    STAGING     = "staging"
    PRODUCTION  = "production"


class SchemaIntegrityReaction(str, Enum):
    """Reakcja na wykrytą zmianę checksumu schematu DB."""
    WARN  = "WARN"   # Tylko log ostrzeżenia
    ALERT = "ALERT"  # Log + SSE system_notification
    BLOCK = "BLOCK"  # Zatrzymaj aplikację — SystemExit(1)


# ---------------------------------------------------------------------------
# Główna klasa ustawień
# ---------------------------------------------------------------------------

class Settings(BaseSettings):
    """
    Centralna konfiguracja aplikacji wczytywana z .env i zmiennych środowiskowych.

    Pydantic-settings automatycznie:
      - Wczytuje z pliku .env (jeśli istnieje)
      - Nadpisuje zmiennymi środowiskowymi (pierwszeństwo)
      - Waliduje typy i zakresy przy starcie
      - Ukrywa SecretStr w repr/logach
    """


    model_config = SettingsConfigDict(
        # Plik .env w katalogu roboczym lub w /app (Docker)
        env_file=(".env", "/app/.env"),
        env_file_encoding="utf-8",
        # Zmienne środowiskowe nadpisują plik .env
        env_nested_delimiter="__",
        # Ignorujemy dodatkowe zmienne — nie rzucamy błędu za np. HOSTNAME
        extra="ignore",
        # Case-insensitive nazwy zmiennych
        case_sensitive=False,
    )

    # -----------------------------------------------------------------------
    # Sekcja: BAZA DANYCH (MSSQL)
    # -----------------------------------------------------------------------

    db_host: str = Field(
        ...,
        description="Hostname serwera MSSQL. W Dockerze: host.docker.internal.",
        examples=["host.docker.internal", "0.53", "192.168.1.10"],
    )
    db_port: int = Field(
        default=1433,
        ge=1,
        le=65535,
        description="Port MSSQL. Domyślnie 1433.",
    )
    db_name: str = Field(
        ...,
        description="Nazwa bazy danych.",
        examples=["WAPRO"],
    )
    db_user: str = Field(
        ...,
        description="Login SQL Server do bazy.",
    )
    db_password: SecretStr = Field(
        ...,
        description="Hasło SQL Server. Nigdy nie pojawia się w logach.",
    )
    odbc_driver: str = Field(
        default="ODBC Driver 18 for SQL Server",
        description="Nazwa sterownika ODBC. Musi być zainstalowany w kontenerze.",
    )

    # Dodatkowe opcje connection stringa
    db_encrypt: bool = Field(
        default=True,
        description="TrustServerCertificate=no gdy True (zalecane produkcyjnie).",
    )
    db_trust_server_certificate: bool = Field(
        default=False,
        description=(
            "TrustServerCertificate=yes — TYLKO dev/staging z self-signed cert. "
            "Na produkcji musi być False."
        ),
    )
    db_connection_timeout: int = Field(
        default=30,
        ge=5,
        le=300,
        description="Timeout nawiązania połączenia w sekundach.",
    )
    db_pool_size: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Rozmiar puli połączeń SQLAlchemy.",
    )
    db_pool_max_overflow: int = Field(
        default=20,
        ge=0,
        le=100,
        description="Maksymalna liczba połączeń ponad pool_size.",
    )
    db_pool_timeout: int = Field(
        default=30,
        ge=5,
        le=120,
        description="Czas oczekiwania na wolne połączenie z puli (sekundy).",
    )
    db_pool_recycle: int = Field(
        default=1800,
        ge=60,
        description="Co ile sekund SQLAlchemy recyklinguje połączenie (ochrona przed MSSQL timeout).",
    )
    # Schemat custom tabel — NIGDY nie zmieniaj na dbo
    db_schema: str = Field(
        default="dbo_ext",
        description="Schemat custom tabel systemu windykacji. Musi być 'dbo_ext'.",
    )

    # -----------------------------------------------------------------------
    # Sekcja: JWT
    # -----------------------------------------------------------------------

    secret_key: SecretStr = Field(
        ...,
        description="Klucz do podpisywania JWT. Min. 32 znaki losowe.",
        min_length=32,
    )
    algorithm: str = Field(
        default="HS256",
        description="Algorytm podpisywania JWT.",
        pattern=r"^(HS256|HS384|HS512|RS256)$",
    )
    access_token_expire_hours: int = Field(
        default=24,
        ge=1,
        le=24,
        description="Ważność access tokenu w godzinach. Max 24h (zgodnie z ustaleniami).",
    )
    refresh_token_expire_days: int = Field(
        default=30,
        ge=1,
        le=90,
        description="Ważność refresh tokenu w dniach.",
    )

    # -----------------------------------------------------------------------
    # Sekcja: MASTER KEY
    # -----------------------------------------------------------------------

    master_key: SecretStr = Field(
        ...,
        description=(
            "64-znakowy klucz serwisowy. TYLKO w .env — nigdy w bazie danych. "
            "Przy wycieku: zmień wartość + restart."
        ),
        min_length=64,
        max_length=64,
    )

    # -----------------------------------------------------------------------
    # Sekcja: REDIS
    # -----------------------------------------------------------------------

    redis_url: str = Field(
        default="redis://redis:6379",
        description="URL Redis. Format: redis://[user:pass@]host:port[/db]",
        examples=["redis://redis:6379", "redis://localhost:6379/0"],
    )
    redis_max_connections: int = Field(
        default=20,
        ge=5,
        le=100,
        description="Maksymalna liczba połączeń w puli Redis.",
    )

    # -----------------------------------------------------------------------
    # Sekcja: CORS (fallback — główne wartości w SystemConfig w DB)
    # -----------------------------------------------------------------------

    cors_origins_fallback: str = Field(
        default="http://localhost:3000",
        description=(
            "Fallback CORS gdy baza niedostępna przy starcie. "
            "Główne wartości przechowywane w SystemConfig.cors.allowed_origins."
        ),
    )

    @property
    def cors_origins_fallback_list(self) -> List[str]:
        """Parsuje cors_origins_fallback jako listę URL-i."""
        return [
            origin.strip()
            for origin in self.cors_origins_fallback.split(",")
            if origin.strip()
        ]

# -----------------------------------------------------------------------
    # Sekcja: COOKIE — HttpOnly refresh token
    # -----------------------------------------------------------------------
    # Dokumentacja bezpieczeństwa:
    #   HttpOnly  → JS nie czyta cookie (ochrona XSS)
    #   Secure    → tylko HTTPS (True na prod, False na dev/localhost)
    #   SameSite  → "lax" chroni przed CSRF bez blokowania UX
    #               "strict" blokuje cookie przy nawigacji z zewnętrznych linków
    #               "none" wymaga Secure=True i otwiera wektory CSRF
    #   Path      → ogranicza cookie do /api/v1/auth/* (minimalizacja ekspozycji)
    #   Domain    → jawnie ustaw na prod (np. "api.app.pl")
    #               None = browser default (host API)
    #
    # Dla architektury: app.pl (frontend) + api.app.pl (backend)
    #   → SameSite=Lax działa (same-site = ta sama domena nadrzędna)
    #   → Frontend robi fetch z credentials: 'include' do api.app.pl
    #   → Backend ustawia cookie na api.app.pl
    #   → Przeglądarka automatycznie dołącza cookie do requestów do api.app.pl
    # -----------------------------------------------------------------------

    cookie_name: str = Field(
        default="refresh_token",
        description=(
            "Nazwa HttpOnly cookie przechowującego refresh token. "
            "Zmiana nazwy invaliduje wszystkie istniejące sesje!"
        ),
        pattern=r"^[a-zA-Z0-9_\\-]+$",
        min_length=1,
        max_length=64,
    )

    cookie_secure: bool = Field(
        default=True,
        description=(
            "Czy cookie wymaga HTTPS (Secure flag). "
            "ZAWSZE True na produkcji. "
            "Ustaw False TYLKO na localhost (dev) — .env.docker: COOKIE_SECURE=false."
        ),
    )

    cookie_samesite: str = Field(
        default="strict",
        description=(
            "Polityka SameSite cookie. "
            "Dozwolone: 'strict' (rekomendowane — max ochrona CSRF dla SPA), "
            "'lax' (cross-site navigation), "
            "'none' (wymaga Secure=True, ryzyko CSRF). "
            "REKOMENDACJA frontendu: 'strict' dla architektury app.pl + api.app.pl."
        ),
        pattern=r"^(lax|strict|none)$",
    )

    cookie_path: str = Field(
        default="/api/v1/auth",
        description=(
            "Ścieżka do której ograniczony jest scope cookie. "
            "'/api/v1/auth' = cookie wysyłane TYLKO do endpointów auth. "
            "Minimalizuje ekspozycję — cookie nie idzie do /api/v1/users/ itp."
        ),
        min_length=1,
        max_length=256,
    )

    cookie_domain: Optional[str] = Field(
        default=None,
        description=(
            "Domena cookie. None = host który ustawił cookie (rekomendowane). "
            "Ustaw jawnie tylko gdy potrzebujesz współdzielenia między subdomenami. "
            "Przykład: 'api.app.pl' lub '.app.pl' (z kropką = subdomeny)."
        ),
        max_length=253,
    )

    # -----------------------------------------------------------------------
    # Sekcja: EMAIL (SMTP)
    # -----------------------------------------------------------------------

    smtp_host: Optional[str] = Field(
        default=None,
        description="Hostname serwera SMTP.",
    )
    smtp_port: int = Field(
        default=587,
        ge=1,
        le=65535,
        description="Port SMTP. 587 = STARTTLS, 465 = SSL, 25 = plain.",
    )
    smtp_user: Optional[str] = Field(
        default=None,
        description="Login SMTP.",
    )
    smtp_password: Optional[SecretStr] = Field(
        default=None,
        description="Hasło SMTP.",
    )
    smtp_from: Optional[str] = Field(
        default=None,
        description="Adres nadawcy emaili (From:).",
        examples=["windykacja@firma.pl"],
    )
    smtp_use_tls: bool = Field(
        default=True,
        description="Czy używać STARTTLS.",
    )

    # -----------------------------------------------------------------------
    # Sekcja: SMS (SMSAPI)
    # -----------------------------------------------------------------------

    smsapi_token: Optional[SecretStr] = Field(
        default=None,
        description="Token API do serwisu SMS.",
    )
    smsapi_sender: Optional[str] = Field(
        default=None,
        description="Nazwa nadawcy SMS (max 11 znaków).",
        max_length=11,
    )

    # -----------------------------------------------------------------------
    # Sekcja: ŚCIEŻKI
    # -----------------------------------------------------------------------

    snapshot_dir: Path = Field(
        default=Path("/app/snapshots"),
        description="Katalog na snapshoty bazy danych (cron 02:00).",
    )
    archive_dir: Path = Field(
        default=Path("/app/archives"),
        description="Katalog na archiwa soft-delete (JSON dump przy DELETE).",
    )
    log_dir: Path = Field(
        default=Path("/app/logs"),
        description="Katalog na pliki logów aplikacji.",
    )

    # -----------------------------------------------------------------------
    # Sekcja: APLIKACJA
    # -----------------------------------------------------------------------

    app_env: AppEnvironment = Field(
        default=AppEnvironment.DEVELOPMENT,
        description="Środowisko uruchomienia: development / staging / production.",
    )
    debug: bool = Field(
        default=False,
        description=(
            "Tryb debug. NA PRODUKCJI MUSI BYĆ FALSE — "
            "w trybie debug stack trace trafia do response."
        ),
    )
    app_name: str = Field(
        default="System Windykacja",
        description="Nazwa aplikacji — widoczna w Swagger UI.",
    )
    app_version: str = Field(
        default="1.0.0",
        description="Wersja aplikacji — widoczna w Swagger UI i logach.",
    )
    api_prefix: str = Field(
        default="/api/v1",
        description="Prefix dla wszystkich endpointów API.",
    )

    # -----------------------------------------------------------------------
    # Walidatory
    # -----------------------------------------------------------------------

    @field_validator("db_schema")
    @classmethod
    def validate_db_schema(cls, v: str) -> str:
        """Schema MUSI być dbo_ext — ochrona przed przypadkowym pisaniem do WAPRO."""
        if v != "dbo_ext":
            raise ValueError(
                f"db_schema musi być 'dbo_ext', otrzymano '{v}'. "
                "Zmiana schematu grozi zapisem do tabel WAPRO!"
            )
        return v

    @field_validator("log_dir", "snapshot_dir", "archive_dir", mode="after")
    @classmethod
    def ensure_directory_exists(cls, v: Path) -> Path:
        """Tworzy katalog jeśli nie istnieje."""
        try:
            v.mkdir(parents=True, exist_ok=True)
        except PermissionError as exc:
            raise ValueError(
                f"Brak uprawnień do tworzenia katalogu: {v}"
            ) from exc
        return v

    @model_validator(mode="after")
    def validate_production_settings(self) -> "Settings":
        """
        Dodatkowe walidacje dla środowiska produkcyjnego.
        Aplikacja nie wystartuje w produkcji z niebezpiecznymi ustawieniami.
        """
        if self.app_env == AppEnvironment.PRODUCTION:
            if self.debug:
                raise ValueError(
                    "BŁĄD BEZPIECZEŃSTWA: debug=True niedozwolone na produkcji!"
                )
            if self.db_trust_server_certificate:
                raise ValueError(
                    "BŁĄD BEZPIECZEŃSTWA: "
                    "db_trust_server_certificate=True niedozwolone na produkcji!"
                )
            if self.access_token_expire_hours > 8:
                logger.warning(
                    "OSTRZEŻENIE BEZPIECZEŃSTWA: access_token_expire_hours=%d "
                    "na produkcji jest powyżej zalecanego limitu 8h.",
                    self.access_token_expire_hours,
                )
        return self

    # -----------------------------------------------------------------------
    # Metody pomocnicze
    # -----------------------------------------------------------------------

    def get_odbc_dsn(self) -> str:
        """
        Buduje ODBC DSN string dla aioodbc/pyodbc.

        UWAGA: Hasło jest bezpośrednio w DSN — to standardowy format ODBC.
        DSN nie jest nigdy logowany — tylko przekazywany do create_async_engine.

        Returns:
            str: Pełny ODBC connection string z hasłem.
        """
        password = self.db_password.get_secret_value()

        dsn_parts = [
            f"DRIVER={{{self.odbc_driver}}}",
            f"SERVER={self.db_host},{self.db_port}",
            f"DATABASE={self.db_name}",
            f"UID={self.db_user}",
            f"PWD={password}",
            f"Encrypt={'yes' if self.db_encrypt else 'no'}",
            f"TrustServerCertificate={'yes' if self.db_trust_server_certificate else 'no'}",
            f"Connection Timeout={self.db_connection_timeout}",
            # MARS potrzebny dla niektórych async operacji na MSSQL
            "MARS_Connection=yes",
        ]

        return ";".join(dsn_parts)

    def get_sqlalchemy_url(self) -> str:
        """
        Buduje URL dla SQLAlchemy async engine (aioodbc).

        Używa quote_plus dla hasła — obsługuje znaki specjalne.
        Format: mssql+aioodbc://?odbc_connect=<encoded_dsn>

        Returns:
            str: SQLAlchemy database URL.
        """
        # Budujemy minimalny DSN dla URL (pełny DSN przez keyword argument)
        password = quote_plus(self.db_password.get_secret_value())

        # aioodbc wymaga odbc_connect= w URL lub osobnego parametru
        # Używamy formatu z encoded DSN — bezpieczniejszy dla znaków specjalnych
        raw_dsn = self.get_odbc_dsn()
        encoded_dsn = quote_plus(raw_dsn)

        return f"mssql+aioodbc:///?odbc_connect={encoded_dsn}"

    def get_safe_repr(self) -> dict:
        """
        Słownik ustawień BEZ sekretów — bezpieczny do logowania przy starcie.

        Returns:
            dict: Konfiguracja z zamaskowanymi sekretami.
        """
        return {
            "app_name":                     self.app_name,
            "app_version":                  self.app_version,
            "app_env":                      self.app_env.value,
            "debug":                        self.debug,
            "api_prefix":                   self.api_prefix,
            # DB — bez hasła
            "db_host":                      self.db_host,
            "db_port":                      self.db_port,
            "db_name":                      self.db_name,
            "db_user":                      self.db_user,
            "db_password":                  "**REDACTED**",
            "db_schema":                    self.db_schema,
            "db_encrypt":                   self.db_encrypt,
            "db_trust_server_certificate":  self.db_trust_server_certificate,
            "db_pool_size":                 self.db_pool_size,
            "db_pool_max_overflow":         self.db_pool_max_overflow,
            "db_pool_recycle":              self.db_pool_recycle,
            # JWT — bez klucza
            "algorithm":                    self.algorithm,
            "access_token_expire_hours":    self.access_token_expire_hours,
            "refresh_token_expire_days":    self.refresh_token_expire_days,
            "secret_key":                   "**REDACTED**",
            # Master key — tylko flaga istnienia
            "master_key":                   "**REDACTED**",
            # Redis — może zawierać hasło, maskujemy credentials
            "redis_url":                    self._mask_redis_url(),
            # SMTP — bez hasła
            "smtp_host":                    self.smtp_host,
            "smtp_port":                    self.smtp_port,
            "smtp_user":                    self.smtp_user,
            "smtp_from":                    self.smtp_from,
            "smtp_password":                "**REDACTED**" if self.smtp_password else None,
            # SMS
            "smsapi_sender":                self.smsapi_sender,
            "smsapi_token":                 "**REDACTED**" if self.smsapi_token else None,
            # Ścieżki
            "log_dir":                      str(self.log_dir),
            "snapshot_dir":                 str(self.snapshot_dir),
            "archive_dir":                  str(self.archive_dir),
            # CORS fallback
            "cors_origins_fallback":        self.cors_origins_fallback,
        }

    def _mask_redis_url(self) -> str:
        """Maskuje credentials w Redis URL jeśli istnieją."""
        from urllib.parse import urlparse, urlunparse
        try:
            parsed = urlparse(self.redis_url)
            if parsed.password:
                masked = parsed._replace(
                    netloc=f"{parsed.username}:**REDACTED**@{parsed.hostname}:{parsed.port}"
                )
                return urlunparse(masked)
        except Exception:
            pass
        return self.redis_url

    @property
    def is_development(self) -> bool:
        return self.app_env == AppEnvironment.DEVELOPMENT

    @property
    def is_production(self) -> bool:
        return self.app_env == AppEnvironment.PRODUCTION


# ---------------------------------------------------------------------------
# Singleton — jedyna instancja Settings w całej aplikacji
# ---------------------------------------------------------------------------

def _create_settings() -> Settings:
    """
    Tworzy i waliduje instancję Settings przy starcie.

    Przy błędzie walidacji (brakujące/nieprawidłowe zmienne):
      - Loguje czytelny komunikat błędu
      - Zatrzymuje aplikację (SystemExit)

    Aplikacja NIE może działać z niekompletną konfiguracją.
    """
    try:
        instance = Settings()  # type: ignore[call-arg]

        # Logujemy konfigurację przy starcie (bez sekretów)
        safe = instance.get_safe_repr()
        logger.info(
            "Konfiguracja załadowana pomyślnie. Środowisko: %s | "
            "DB: %s:%d/%s | Redis: %s | Debug: %s",
            safe["app_env"],
            safe["db_host"],
            safe["db_port"],
            safe["db_name"],
            safe["redis_url"],
            safe["debug"],
        )

        # Ostrzeżenie dla development z domyślnymi wartościami
        if instance.is_development:
            logger.debug(
                "Pełna konfiguracja (dev): %s",
                safe,
            )

        return instance

    except Exception as exc:
        # Formatujemy błąd walidacji pydantic czytelnie
        logger.critical(
            "KRYTYCZNY BŁĄD: Nie można załadować konfiguracji aplikacji!\n"
            "Sprawdź plik .env i zmienne środowiskowe.\n"
            "Szczegóły: %s",
            str(exc),
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Singleton / lazy loading – jedyna instancja Settings w całej aplikacji
# ---------------------------------------------------------------------------

from functools import lru_cache

@lru_cache
def get_settings() -> Settings:
    """
    Główna funkcja do pobierania konfiguracji.
    Używana w całej aplikacji: from app.core.config import get_settings
    """
    return _create_settings()

# Dla miejsc, które importują `settings` bezpośrednio:
settings: Settings = get_settings()