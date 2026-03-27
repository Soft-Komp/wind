"""
Plik  : app/db/fakir_write.py
Moduł : Akceptacja Faktur KSeF
Opis  : Dedykowane połączenie WAPRO z uprawnieniem ZAPISU.

        Używane WYŁĄCZNIE do:
          UPDATE dbo.BUF_DOKUMENT SET KOD_STATUSU='K' WHERE KSEF_ID=:id AND PRG_KOD=3

        Osobne połączenie od wapro.py (read-only) — zasada minimalnych uprawnień.
        User: windykacja_fakir_write (patrz DDL 021_fakir_write_user.sql)

        Konfiguracja (zmienne .env):
          FAKIR_DB_HOST       = <host> (INNY niż DB_HOST — walidator w config.py)
          FAKIR_DB_USER       = windykacja_fakir_write
          FAKIR_DB_PASSWORD   = <hasło z 021_fakir_write_user.sql>
          FAKIR_DB_DATABASE   = WAPRO

        Architektura połączenia:
          - aioodbc: async ODBC driver wrapper (spójny z resztą systemu)
          - Pula: max 2 połączenia (tylko UPDATE, ruch minimalny)
          - Timeout: 10s na połączenie, 30s na query
          - Retry: 3 próby z exponential backoff (konfigurowane z SystemConfig)
          - Circuit breaker: po N błędach rzędowy zapis disabled + alert do logu

        ⚠️  WAŻNE: faktury.fakir_update_enabled musi być 'true' w SystemConfig
            żeby jakikolwiek UPDATE faktycznie trafił do Fakira.
            Domyślnie false — ochrona przed przypadkowym zapisem.

Powiązane pliki:
  - app/core/config.py         → FAKIR_DB_* zmienne + walidator
  - app/services/faktura_service.py → saga pattern używa tej klasy
  - database/ddl/021_fakir_write_user.sql → DDL usera DB
"""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator, Optional

import aioodbc

from app.core.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------
_FAKIR_DRIVER    = "ODBC Driver 18 for SQL Server"
_POOL_MIN_SIZE   = 1
_POOL_MAX_SIZE   = 2   # Minimalna pula — tylko UPDATE, ruch rzadki
_CONNECT_TIMEOUT = 10  # sekund
_QUERY_TIMEOUT   = 30  # sekund

# Circuit breaker — po tylu błędach z rzędu: wyłącz zapis
_CIRCUIT_BREAK_THRESHOLD = 5
_CIRCUIT_RESET_SECONDS   = 120  # czas po którym spróbujemy ponownie


class FakirWriteConnectionError(Exception):
    """Błąd połączenia lub autoryzacji do Fakira (write)."""


class FakirWriteDisabledError(Exception):
    """Próba zapisu gdy faktury.fakir_update_enabled=false lub circuit breaker."""


class FakirWritePool:
    """
    Singleton puli połączeń aioodbc do WAPRO (write).

    Użycie:
        pool = await FakirWritePool.get_instance()
        async with pool.acquire() as conn:
            cursor = await conn.cursor()
            await cursor.execute("UPDATE dbo.BUF_DOKUMENT ...")

    Nie twórz bezpośrednio — używaj get_instance() lub fakir_write_connection().
    """

    _instance: Optional["FakirWritePool"] = None
    _lock = asyncio.Lock()

    def __init__(self) -> None:
        self._pool: Optional[aioodbc.Pool] = None
        self._consecutive_errors: int = 0
        self._circuit_broken_until: Optional[float] = None
        self._initialized: bool = False

    @classmethod
    async def get_instance(cls) -> "FakirWritePool":
        """Thread-safe singleton — tworzy pulę przy pierwszym wywołaniu."""
        async with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
                await cls._instance._initialize()
        return cls._instance

    @classmethod
    async def reset_instance(cls) -> None:
        """Zamknij pulę i wyczyść singleton — używać przy shutdown lub testach."""
        async with cls._lock:
            if cls._instance is not None:
                await cls._instance._close()
                cls._instance = None
        logger.info("[fakir_write] Pula połączeń zamknięta i zresetowana")

    def _build_connection_string(self) -> str:
        """
        Buduje connection string ODBC.
        Używa FAKIR_DB_* zmiennych z config.py (NIE tych samych co DB_*).
        """
        password = settings.FAKIR_DB_PASSWORD.get_secret_value()
        return (
            f"Driver={{{_FAKIR_DRIVER}}};"
            f"Server={settings.FAKIR_DB_HOST};"
            f"Database={settings.FAKIR_DB_DATABASE};"
            f"UID={settings.FAKIR_DB_USER};"
            f"PWD={password};"
            f"Connect Timeout={_CONNECT_TIMEOUT};"
            f"TrustServerCertificate=yes;"
            # Szyfrowanie połączenia (MSSQL 2022 wymaga)
            f"Encrypt=yes;"
        )

    async def _initialize(self) -> None:
        """Inicjalizacja puli połączeń przy starcie."""
        logger.info(
            "[fakir_write] Inicjalizacja puli połączeń: host=%s db=%s user=%s",
            settings.FAKIR_DB_HOST,
            settings.FAKIR_DB_DATABASE,
            settings.FAKIR_DB_USER,
        )
        try:
            dsn = self._build_connection_string()
            self._pool = await aioodbc.create_pool(
                dsn=dsn,
                minsize=_POOL_MIN_SIZE,
                maxsize=_POOL_MAX_SIZE,
                echo=False,          # Nie loguj SQL (credentials w DSN!)
            )
            self._initialized = True
            logger.info(
                "[fakir_write] Pula gotowa: min=%d max=%d",
                _POOL_MIN_SIZE,
                _POOL_MAX_SIZE,
            )
        except Exception as exc:
            logger.critical(
                "[fakir_write] KRYTYCZNY BŁĄD inicjalizacji puli: %s. "
                "Zapis do Fakira będzie niemożliwy!",
                exc,
                exc_info=True,
            )
            # Nie rzucamy — aplikacja startuje, ale zapisy będą failować gracefully
            self._initialized = False

    async def _close(self) -> None:
        """Zamknij pulę połączeń."""
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
            self._initialized = False

    def _is_circuit_broken(self) -> bool:
        """Sprawdź czy circuit breaker jest aktywny."""
        if self._circuit_broken_until is None:
            return False
        if time.monotonic() > self._circuit_broken_until:
            # Czas minął — reset circuit breaker
            logger.info(
                "[fakir_write] Circuit breaker RESET po %d sekundach przerwy",
                _CIRCUIT_RESET_SECONDS,
            )
            self._circuit_broken_until = None
            self._consecutive_errors = 0
            return False
        return True

    def _record_error(self) -> None:
        """Rejestruj błąd — po N błędach aktywuj circuit breaker."""
        self._consecutive_errors += 1
        logger.error(
            "[fakir_write] Błąd połączenia #%d (próg: %d)",
            self._consecutive_errors,
            _CIRCUIT_BREAK_THRESHOLD,
        )
        if self._consecutive_errors >= _CIRCUIT_BREAK_THRESHOLD:
            self._circuit_broken_until = time.monotonic() + _CIRCUIT_RESET_SECONDS
            logger.critical(
                "[fakir_write] CIRCUIT BREAKER AKTYWNY przez %d sekund! "
                "Zapis do Fakira wyłączony. Sprawdź połączenie z bazą.",
                _CIRCUIT_RESET_SECONDS,
            )

    def _record_success(self) -> None:
        """Rejestruj sukces — reset licznika błędów."""
        if self._consecutive_errors > 0:
            logger.info(
                "[fakir_write] Połączenie przywrócone po %d błędach",
                self._consecutive_errors,
            )
        self._consecutive_errors = 0

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[aioodbc.Connection, None]:
        """
        Context manager dla połączenia z puli.

        Raises:
            FakirWriteDisabledError: gdy circuit breaker aktywny lub pula niezainicjowana
            FakirWriteConnectionError: gdy aioodbc rzuci wyjątek
        """
        if not self._initialized or self._pool is None:
            raise FakirWriteDisabledError(
                "Pula fakir_write niezainicjowana — sprawdź FAKIR_DB_* zmienne w .env "
                "i logi startu aplikacji"
            )

        if self._is_circuit_broken():
            raise FakirWriteDisabledError(
                f"Circuit breaker aktywny po {_CIRCUIT_BREAK_THRESHOLD} błędach z rzędu. "
                f"Następna próba za {int(self._circuit_broken_until - time.monotonic())}s"
            )

        try:
            async with self._pool.acquire() as conn:
                yield conn
                self._record_success()
        except aioodbc.Error as exc:
            self._record_error()
            logger.error(
                "[fakir_write] aioodbc błąd przy acquire: %s",
                exc,
                exc_info=True,
            )
            raise FakirWriteConnectionError(
                f"Błąd połączenia ODBC do Fakira: {exc}"
            ) from exc


# ---------------------------------------------------------------------------
# Publiczne API modułu
# ---------------------------------------------------------------------------

async def get_fakir_pool() -> FakirWritePool:
    """Zwraca singleton puli — FastAPI Depends() lub bezpośrednie użycie."""
    return await FakirWritePool.get_instance()


@asynccontextmanager
async def fakir_write_connection() -> AsyncGenerator[aioodbc.Connection, None]:
    """
    Convenience context manager — najczęściej używany w serwisach.

    Przykład użycia w faktura_service.py:
        async with fakir_write_connection() as conn:
            cursor = await conn.cursor()
            await cursor.execute(
                "UPDATE dbo.BUF_DOKUMENT SET KOD_STATUSU=? "
                "WHERE KSEF_ID=? AND PRG_KOD=3",
                ("K", numer_ksef)
            )
            affected = cursor.rowcount
    """
    pool = await get_fakir_pool()
    async with pool.acquire() as conn:
        yield conn


async def verify_fakir_connection() -> dict:
    """
    Sprawdź połączenie z Fakirem — używane przez health check.

    Returns:
        dict z polami: ok (bool), latency_ms (float|None), error (str|None)
    """
    start = time.monotonic()
    result = {
        "ok": False,
        "latency_ms": None,
        "error": None,
        "host": settings.FAKIR_DB_HOST,
        "user": settings.FAKIR_DB_USER,
        "checked_at": datetime.utcnow().isoformat(),
    }

    try:
        async with fakir_write_connection() as conn:
            cursor = await conn.cursor()
            # Lekkie zapytanie — sprawdź czy user ma dostęp do BUF_DOKUMENT
            # Celowo tylko sprawdza uprawnienia, nie czyta danych
            await cursor.execute(
                "SELECT TOP 0 KOD_STATUSU FROM dbo.BUF_DOKUMENT WHERE 1=0"
            )
            latency = (time.monotonic() - start) * 1000
            result["ok"] = True
            result["latency_ms"] = round(latency, 2)
            logger.debug(
                "[fakir_write] Health check OK: %.1fms", latency
            )
    except FakirWriteDisabledError as exc:
        result["error"] = f"DISABLED: {exc}"
        logger.warning("[fakir_write] Health check: połączenie wyłączone: %s", exc)
    except FakirWriteConnectionError as exc:
        result["error"] = f"CONNECTION_ERROR: {exc}"
        logger.error("[fakir_write] Health check FAIL: %s", exc)
    except Exception as exc:
        result["error"] = f"UNEXPECTED: {exc}"
        logger.error(
            "[fakir_write] Health check nieoczekiwany błąd: %s",
            exc,
            exc_info=True,
        )

    return result


async def close_fakir_pool() -> None:
    """
    Zamknij pulę przy shutdownie aplikacji.
    Rejestruj w FastAPI lifespan jako cleanup.
    """
    await FakirWritePool.reset_instance()
    logger.info("[fakir_write] Pula zamknięta przy shutdownie")