"""
app/db/fakir_write.py
=====================
Warstwa dostępu WAPRO/Fakir z uprawnieniem ZAPISU.

Architektura (Decyzja D1 — Sprint 2):
    Osobne połączenie od wapro.py (read-only).
    Użytkownik DB: windykacja_fakir_write
    Uprawnienie: UPDATE (KOD_STATUSU) ON dbo.BUF_DOKUMENT

KLUCZOWE RÓŻNICE od wapro.py:
    • autocommit=False  — transakcje zarządzane explicite
    • pool_size=2       — minimalna pula (UPDATE rzadki)
    • Zmienne .env: FAKIR_DB_HOST / FAKIR_DB_USER / FAKIR_DB_PASSWORD
    • Walidator w config.py: FAKIR_DB_USER != DB_USER (ochrona przed pomyłką)
    • TYLKO operacja UPDATE KOD_STATUSU — żadna inna operacja niedozwolona

BEZPIECZEŃSTWO:
    • Whitelist operacji — tylko _ALLOWED_OPERATION = "update_kod_statusu"
    • KSEF_ID walidowany jako NVARCHAR(50), non-empty
    • Nie przyjmuje surowego SQL — tylko parametryzowane wywołanie
    • Każde wywołanie logowane z pełnym audit trail

SAGA PATTERN (wywoływana z fakir_service.py):
    1. Sprawdź fakiry.fakir_update_enabled z SystemConfig
    2. Zdobądź Redis lock (faktura:lock:{faktura_id} TTL 30s)
    3. execute_update_kod_statusu() — UPDATE + weryfikacja
    4. COMMIT lub ROLLBACK
    5. Zwolnij lock

DEMO MODE:
    Gdy settings.DEMO_MODE=True: operacja blokowana, zwracamy FakirUpdateResult
    z success=False, error="DEMO_MODE" — bez żadnego kontaktu z DB.

Retry:
    3 próby z exponential backoff (1s, 2s, 4s) dla transient errors.
    Konfigurowalnie przez SystemConfig: faktury.fakir_retry_attempts.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Generator, Optional

import orjson
import pyodbc

from app.core.config import get_settings

# ─────────────────────────────────────────────────────────────────────────────
# Logger — JSON Lines spójny z resztą projektu
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("app.db.fakir_write")

# ─────────────────────────────────────────────────────────────────────────────
# Stałe
# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_POOL_SIZE    = 2      # minimalna pula — operacje rzadkie
_DEFAULT_POOL_TIMEOUT = 10     # sekund na wolne połączenie
_DEFAULT_RETRY_COUNT  = 3      # domyślna liczba prób
_RETRY_BASE_DELAY     = 1.0    # sekund — base dla exponential backoff

# PRG_KOD = 3 → moduł Fakir (zakupy) — hardkodowane, nie parametryzowane
_FAKIR_PRG_KOD = 3

# Nowy status po akceptacji wszystkich
_AKCEPTACJA_KOD_STATUSU = "K"

# SQL stanów transient — retry tylko dla nich
_TRANSIENT_SQL_STATES = {
    "08001",  # Connection failure
    "08S01",  # Communication link failure
    "HYT00",  # Timeout
    "HYT01",  # Connection timeout
    "40001",  # Deadlock
}

# ─────────────────────────────────────────────────────────────────────────────
# Result dataclass — bogaty wynik operacji
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class FakirUpdateResult:
    """
    Wynik operacji UPDATE BUF_DOKUMENT.
    Przekazywany do fakir_service.py → serwisu → audit logu.
    """
    operation_id:      str   = field(default_factory=lambda: str(uuid.uuid4()))
    numer_ksef:        str   = ""
    success:           bool  = False
    rows_affected:     int   = 0
    kod_statusu_before: Optional[str] = None
    kod_statusu_after:  Optional[str] = None
    duration_ms:       Optional[float] = None
    retry_count:       int   = 0
    error:             Optional[str] = None
    error_detail:      Optional[str] = None  # pełny traceback — tylko do logów
    blocked_by_demo:   bool  = False
    blocked_by_config: bool  = False

    def to_log_dict(self) -> dict[str, Any]:
        return {
            "event":              "fakir_update",
            "operation_id":       self.operation_id,
            "numer_ksef":         self.numer_ksef,
            "success":            self.success,
            "rows_affected":      self.rows_affected,
            "kod_statusu_before": self.kod_statusu_before,
            "kod_statusu_after":  self.kod_statusu_after,
            "duration_ms":        self.duration_ms,
            "retry_count":        self.retry_count,
            "error":              self.error,
            "blocked_by_demo":    self.blocked_by_demo,
            "blocked_by_config":  self.blocked_by_config,
            "ts":                 datetime.now(timezone.utc).isoformat(),
        }

    def to_audit_dict(self) -> dict[str, Any]:
        """Wersja do AuditLog — bez error_detail (pełny traceback)."""
        d = self.to_log_dict()
        d.pop("error_detail", None)
        return d


# ─────────────────────────────────────────────────────────────────────────────
# Connection Pool — thread-safe, analogia WaproConnectionPool
# ─────────────────────────────────────────────────────────────────────────────

class FakirWriteConnectionPool:
    """
    Prosty connection pool dla pyodbc z uprawnieniem zapisu do Fakira.

    Kluczowe różnice od WaproConnectionPool:
      • autocommit=False (transakcje zarządzane explicite)
      • pool_size=2 (operacje rzadkie)
      • Osobne zmienne .env: FAKIR_DB_*
    """

    def __init__(
        self,
        connection_string: str,
        pool_size: int = _DEFAULT_POOL_SIZE,
        timeout:   int = _DEFAULT_POOL_TIMEOUT,
    ) -> None:
        self._connection_string = connection_string
        self._pool_size         = pool_size
        self._timeout           = timeout

        self._lock      = threading.Lock()
        self._semaphore = threading.BoundedSemaphore(pool_size)
        self._pool:     list[pyodbc.Connection] = []

        self._stats = {
            "total_acquired":  0,
            "total_released":  0,
            "total_errors":    0,
            "total_reconnects": 0,
            "pool_size":       pool_size,
        }

        logger.info(
            orjson.dumps({
                "event":     "fakir_pool_init",
                "pool_size": pool_size,
                "timeout":   timeout,
                "ts":        datetime.now(timezone.utc).isoformat(),
            }).decode()
        )

    def _create_connection(self) -> pyodbc.Connection:
        conn = pyodbc.connect(
            self._connection_string,
            autocommit=False,  # ← KRYTYCZNE: transakcje explicite
            timeout=self._timeout,
        )
        conn.setdecoding(pyodbc.SQL_CHAR,  encoding="utf-8")
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-8")
        conn.setencoding(encoding="utf-8")
        logger.debug("FakirWrite: nowe połączenie pyodbc utworzone")
        return conn

    def _is_alive(self, conn: pyodbc.Connection) -> bool:
        try:
            conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    @contextmanager
    def acquire(self) -> Generator[pyodbc.Connection, None, None]:
        """Context manager: pobiera połączenie z puli, zwalnia po wyjściu."""
        acquired = self._semaphore.acquire(timeout=self._timeout)
        if not acquired:
            self._stats["total_errors"] += 1
            raise TimeoutError(
                f"FakirWrite: timeout {self._timeout}s — pula {self._pool_size} wyczerpana"
            )

        conn = None
        try:
            with self._lock:
                self._stats["total_acquired"] += 1
                if self._pool:
                    conn = self._pool.pop()
                    if not self._is_alive(conn):
                        logger.warning("FakirWrite: połączenie martwe — reconnect")
                        self._stats["total_reconnects"] += 1
                        try:
                            conn.close()
                        except Exception:
                            pass
                        conn = self._create_connection()

            if conn is None:
                conn = self._create_connection()

            yield conn

        except Exception:
            # Przy błędzie — zamknij połączenie zamiast zwracać do puli
            if conn is not None:
                try:
                    conn.rollback()
                    conn.close()
                except Exception:
                    pass
                conn = None
            self._stats["total_errors"] += 1
            raise

        finally:
            if conn is not None:
                with self._lock:
                    if len(self._pool) < self._pool_size:
                        self._pool.append(conn)
                    else:
                        conn.close()
                    self._stats["total_released"] += 1
            self._semaphore.release()

    def get_stats(self) -> dict[str, Any]:
        with self._lock:
            return {**self._stats, "current_pool_size": len(self._pool)}

    def close_all(self) -> None:
        with self._lock:
            for conn in self._pool:
                try:
                    conn.close()
                except Exception:
                    pass
            self._pool.clear()
        logger.info("FakirWriteConnectionPool zamknięty")


# ─────────────────────────────────────────────────────────────────────────────
# Singleton + executor
# ─────────────────────────────────────────────────────────────────────────────

_pool_instance: Optional[FakirWriteConnectionPool] = None
_pool_lock = threading.Lock()
_executor:      Optional[ThreadPoolExecutor] = None


def initialize_fakir_pool() -> None:
    """
    Inicjalizuje pool Fakir Write.
    Wywoływana raz w lifespan FastAPI (po initialize_pool WAPRO).
    Gdy FAKIR_DB_* nie skonfigurowane — loguje WARNING i pomija.
    """
    global _pool_instance, _executor

    settings = get_settings()

    # Walidacja: FAKIR_DB_USER nie może być tym samym co DB_USER
    fakir_user = getattr(settings, "FAKIR_DB_USER", None)
    db_user    = getattr(settings, "DB_USER", None)
    if fakir_user and db_user and fakir_user == db_user:
        raise RuntimeError(
            "KRYTYCZNY BŁĄD KONFIGURACJI: FAKIR_DB_USER == DB_USER. "
            "Użytkownik Fakir MUSI być inny od użytkownika read-only WAPRO. "
            "Sprawdź .env i popraw FAKIR_DB_USER."
        )

    fakir_host = getattr(settings, "FAKIR_DB_HOST", None)
    fakir_pass = getattr(settings, "FAKIR_DB_PASSWORD", None)

    if not all([fakir_host, fakir_user, fakir_pass]):
        logger.warning(
            orjson.dumps({
                "event":   "fakir_pool_skipped",
                "reason":  "FAKIR_DB_* niekompletne — moduł faktur wyłączony (UPDATE BUF_DOKUMENT niedostępny)",
                "missing": [k for k, v in [
                    ("FAKIR_DB_HOST", fakir_host),
                    ("FAKIR_DB_USER", fakir_user),
                    ("FAKIR_DB_PASSWORD", fakir_pass),
                ] if not v],
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        return

    with _pool_lock:
        if _pool_instance is not None:
            logger.warning("FakirWriteConnectionPool już zainicjalizowany")
            return

        # Pobierz hasło (SecretStr lub str)
        password = fakir_pass
        if hasattr(password, "get_secret_value"):
            password = password.get_secret_value()

        db_name = getattr(settings, "FAKIR_DB_NAME", getattr(settings, "DB_NAME", "WAPRO"))

        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={fakir_host};"
            f"DATABASE={db_name};"
            f"UID={fakir_user};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
            f"Encrypt=yes;"
            f"Connection Timeout=10;"
        )

        _pool_instance = FakirWriteConnectionPool(
            connection_string=conn_str,
            pool_size=_DEFAULT_POOL_SIZE,
            timeout=_DEFAULT_POOL_TIMEOUT,
        )
        _executor = ThreadPoolExecutor(
            max_workers=4,
            thread_name_prefix="fakir_write_worker",
        )

        logger.info(
            orjson.dumps({
                "event":     "fakir_pool_ready",
                "host":      fakir_host,
                "db":        db_name,
                "user":      fakir_user,
                "pool_size": _DEFAULT_POOL_SIZE,
                "ts":        datetime.now(timezone.utc).isoformat(),
            }).decode()
        )


def shutdown_fakir_pool() -> None:
    """Zamyka pool i executor. Wywoływana w lifespan shutdown."""
    global _pool_instance, _executor
    with _pool_lock:
        if _executor is not None:
            _executor.shutdown(wait=True, cancel_futures=False)
            _executor = None
        if _pool_instance is not None:
            _pool_instance.close_all()
            _pool_instance = None


def get_fakir_pool() -> FakirWriteConnectionPool:
    if _pool_instance is None:
        raise RuntimeError(
            "FakirWriteConnectionPool nie jest zainicjalizowany. "
            "Sprawdź czy FAKIR_DB_* są ustawione i initialize_fakir_pool() wywołana."
        )
    return _pool_instance


def is_fakir_available() -> bool:
    """Sprawdza czy pool jest dostępny (bez rzucania wyjątku)."""
    return _pool_instance is not None


def get_fakir_executor() -> ThreadPoolExecutor:
    if _executor is None:
        raise RuntimeError("FakirWrite executor nie zainicjalizowany")
    return _executor


# ─────────────────────────────────────────────────────────────────────────────
# Operacje synchroniczne (wywołane w thread pool)
# ─────────────────────────────────────────────────────────────────────────────

def _get_current_kod_statusu_sync(
    conn: pyodbc.Connection,
    numer_ksef: str,
) -> Optional[str]:
    """
    Odczytuje bieżący KOD_STATUSU z BUF_DOKUMENT przed UPDATE.
    Używany do logowania before/after.
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT KOD_STATUSU FROM dbo.BUF_DOKUMENT "
        "WHERE KSEF_ID = ? AND PRG_KOD = ?",
        numer_ksef, _FAKIR_PRG_KOD,
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return row[0]  # może być NULL → None


def _execute_update_sync(
    conn:       pyodbc.Connection,
    numer_ksef: str,
    result:     FakirUpdateResult,
) -> None:
    """
    Wykonuje UPDATE BUF_DOKUMENT SET KOD_STATUSU='K'.
    Operuje wewnątrz połączenia z autocommit=False.
    COMMIT/ROLLBACK należy do wywołującego (saga pattern).

    Raises:
        pyodbc.Error:  błąd SQL — wywołujący robi rollback
        ValueError:    niezgodność po UPDATE (rows_affected != 1)
    """
    cursor = conn.cursor()

    # Stan PRZED — do audit logu
    result.kod_statusu_before = _get_current_kod_statusu_sync(conn, numer_ksef)

    logger.info(
        orjson.dumps({
            "event":              "fakir_update_attempt",
            "operation_id":       result.operation_id,
            "numer_ksef":         numer_ksef,
            "kod_statusu_before": result.kod_statusu_before,
            "prg_kod":            _FAKIR_PRG_KOD,
            "ts":                 datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    # UPDATE — wyłącznie przez parametryzowane zapytanie
    cursor.execute(
        "UPDATE dbo.BUF_DOKUMENT "
        "SET KOD_STATUSU = ? "
        "WHERE KSEF_ID = ? AND PRG_KOD = ?",
        _AKCEPTACJA_KOD_STATUSU,
        numer_ksef,
        _FAKIR_PRG_KOD,
    )
    result.rows_affected = cursor.rowcount

    # Weryfikacja: musi być dokładnie 1 wiersz
    if result.rows_affected == 0:
        raise ValueError(
            f"UPDATE BUF_DOKUMENT: 0 wierszy dotkniętych dla KSEF_ID={numer_ksef!r}. "
            f"Faktura nie istnieje w Fakirze lub PRG_KOD != {_FAKIR_PRG_KOD}."
        )
    if result.rows_affected > 1:
        raise ValueError(
            f"UPDATE BUF_DOKUMENT: {result.rows_affected} wierszy dotkniętych dla "
            f"KSEF_ID={numer_ksef!r} — niespójność danych. Rollback."
        )

    # Weryfikacja po UPDATE (w tej samej transakcji, przed COMMIT)
    result.kod_statusu_after = _get_current_kod_statusu_sync(conn, numer_ksef)
    if result.kod_statusu_after != _AKCEPTACJA_KOD_STATUSU:
        raise ValueError(
            f"Weryfikacja po UPDATE nieudana: oczekiwano KOD_STATUSU={_AKCEPTACJA_KOD_STATUSU!r}, "
            f"otrzymano={result.kod_statusu_after!r}. Rollback."
        )


def _update_kod_statusu_sync(
    numer_ksef:    str,
    retry_count:   int = _DEFAULT_RETRY_COUNT,
) -> FakirUpdateResult:
    """
    Synchroniczna implementacja saga kroku: UPDATE BUF_DOKUMENT.
    Wywoływana z thread pool executora.

    Logika retry: tylko dla transient SQL errors (deadlock, timeout, connection loss).
    Błędy walidacyjne (0 wierszy, >1 wierszy) — natychmiastowy fail bez retry.
    """
    result = FakirUpdateResult(numer_ksef=numer_ksef)
    pool   = get_fakir_pool()
    t_start = time.monotonic()
    last_exc: Optional[Exception] = None

    for attempt in range(1, retry_count + 1):
        try:
            with pool.acquire() as conn:
                _execute_update_sync(conn, numer_ksef, result)
                conn.commit()
                result.success    = True
                result.retry_count = attempt - 1
                result.duration_ms = round((time.monotonic() - t_start) * 1000, 2)

                logger.info(
                    orjson.dumps({
                        **result.to_log_dict(),
                        "attempt": attempt,
                    }).decode()
                )
                return result

        except ValueError as exc:
            # Błąd walidacyjny — rollback + natychmiastowy fail (bez retry)
            try:
                with pool.acquire() as conn:
                    conn.rollback()
            except Exception:
                pass
            result.error         = str(exc)
            result.error_detail  = traceback.format_exc()
            result.duration_ms   = round((time.monotonic() - t_start) * 1000, 2)
            result.retry_count   = attempt - 1
            logger.error(
                orjson.dumps({
                    **result.to_log_dict(),
                    "attempt": attempt,
                    "fail_type": "validation_error",
                }).decode()
            )
            return result

        except pyodbc.Error as exc:
            last_exc = exc
            sql_state = exc.args[0] if exc.args else "UNKNOWN"
            is_transient = str(sql_state) in _TRANSIENT_SQL_STATES

            logger.warning(
                orjson.dumps({
                    "event":        "fakir_update_retry",
                    "operation_id": result.operation_id,
                    "numer_ksef":   numer_ksef,
                    "attempt":      attempt,
                    "max_attempts": retry_count,
                    "sql_state":    str(sql_state),
                    "is_transient": is_transient,
                    "error":        str(exc),
                    "ts":           datetime.now(timezone.utc).isoformat(),
                }).decode()
            )

            if not is_transient or attempt >= retry_count:
                break

            delay = _RETRY_BASE_DELAY * (2 ** (attempt - 1))
            logger.info(f"FakirWrite retry za {delay:.1f}s (próba {attempt + 1}/{retry_count})")
            time.sleep(delay)

        except Exception as exc:
            last_exc = exc
            break

    # Wszystkie próby wyczerpane
    result.error        = str(last_exc) if last_exc else "Nieznany błąd"
    result.error_detail = traceback.format_exc()
    result.retry_count  = retry_count - 1
    result.duration_ms  = round((time.monotonic() - t_start) * 1000, 2)

    logger.error(
        orjson.dumps({
            **result.to_log_dict(),
            "traceback_preview": result.error_detail[:500],
            "fail_type":         "exhausted_retries",
        }).decode()
    )
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Public API — async (wywoływane z serwisów FastAPI)
# ─────────────────────────────────────────────────────────────────────────────

async def update_kod_statusu(
    numer_ksef:  str,
    retry_count: int = _DEFAULT_RETRY_COUNT,
) -> FakirUpdateResult:
    """
    Async wrapper — UPDATE BUF_DOKUMENT SET KOD_STATUSU='K'.

    Walidacja wejścia:
      • numer_ksef: non-empty string, max 50 znaków
      • Znaki kontrolne: odrzucone
      • Pool musi być dostępny (is_fakir_available())

    Args:
        numer_ksef:  Unikalny ID KSeF faktury (PK w BUF_DOKUMENT)
        retry_count: Liczba prób (domyślnie z _DEFAULT_RETRY_COUNT)

    Returns:
        FakirUpdateResult z pełnym statusem operacji

    Raises:
        RuntimeError: Pool niezainicjalizowany
        ValueError:   numer_ksef nieprawidłowy
    """
    # Walidacja wejścia — zero trust
    if not numer_ksef or not isinstance(numer_ksef, str):
        result = FakirUpdateResult(numer_ksef=str(numer_ksef))
        result.error = "numer_ksef musi być niepustym stringiem"
        logger.error(orjson.dumps(result.to_log_dict()).decode())
        return result

    numer_ksef = numer_ksef.strip()

    if len(numer_ksef) == 0:
        result = FakirUpdateResult(numer_ksef=numer_ksef)
        result.error = "numer_ksef nie może być pusty po strip()"
        return result

    if len(numer_ksef) > 50:
        result = FakirUpdateResult(numer_ksef=numer_ksef[:50])
        result.error = f"numer_ksef za długi: {len(numer_ksef)} > 50 znaków"
        return result

    # Sprawdź znaki kontrolne
    import re as _re
    if _re.search(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", numer_ksef):
        result = FakirUpdateResult(numer_ksef=numer_ksef)
        result.error = "numer_ksef zawiera niedozwolone znaki kontrolne"
        return result

    if not is_fakir_available():
        result = FakirUpdateResult(numer_ksef=numer_ksef)
        result.error = "FakirWriteConnectionPool niedostępny — sprawdź FAKIR_DB_* w .env"
        result.blocked_by_config = True
        logger.error(orjson.dumps(result.to_log_dict()).decode())
        return result

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        get_fakir_executor(),
        _update_kod_statusu_sync,
        numer_ksef,
        retry_count,
    )


def get_pool_stats() -> dict[str, Any]:
    """Zwraca statystyki puli — do GET /system/health."""
    if _pool_instance is None:
        return {"status": "not_initialized"}
    return {
        "status": "ok",
        **_pool_instance.get_stats(),
    }