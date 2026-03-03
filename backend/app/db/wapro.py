"""
backend/app/db/wapro.py
========================
Warstwa dostępu do danych WAPRO przez pyodbc + widoki SQL.

Architektura:
    - TYLKO ODCZYT — żaden endpoint NIE pisze do tabel WAPRO
    - Dostęp wyłącznie przez widoki: dbo.VIEW_kontrahenci, dbo.VIEW_rozrachunki_faktur
    - pyodbc z thread-pool executor (asyncio-friendly)
    - Connection pooling z limitem i timeout
    - Retry z exponential backoff dla transient errors
    - Wszystkie zapytania parametryzowane (zero SQL injection)
    - Sanityzacja i walidacja wszystkich parametrów wejściowych

Widoki (schemat dbo):
    dbo.VIEW_kontrahenci         — lista/szczegóły dłużników (1 wiersz/kontrahent)
    dbo.VIEW_rozrachunki_faktur  — faktury per kontrahent (1 wiersz/faktura)

Konwencja dat WAPRO:
    DATA_DOK i TERMIN_PLATNOSCI to INT (dni od 1899-12-30).
    Konwersja w widokach SQL: CAST(DATEADD(DAY, kolumna, '18991230') AS DATE)
    W Python: daty przychodzą już jako date/datetime (po konwersji w widoku).

Wersja: 1.0.0
Data:   2026-02-18
Autor:  System Windykacja
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
import time
import traceback
import unicodedata
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Generator, Iterator, Optional

import pyodbc

# ---------------------------------------------------------------------------
# Logger modułu
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stałe — nazwy widoków (NIGDY nie zmieniać bez migracji Alembic)
# ---------------------------------------------------------------------------
VIEW_KONTRAHENCI: str = "dbo.skw_kontrahenci"
VIEW_ROZRACHUNKI_FAKTUR: str = "dbo.skw_rozrachunki_faktur"

# Maksymalna długość parametrów tekstowych (ochrona przed payload injection)
_MAX_SEARCH_LEN: int = 200
_MAX_PARAM_LEN: int = 500

# Retry config dla transient DB errors
_MAX_RETRIES: int = 3
_RETRY_BASE_DELAY: float = 0.5   # sekundy — podwaja się przy każdej próbie

# Pool config
_DEFAULT_POOL_SIZE: int = 5
_DEFAULT_POOL_TIMEOUT: int = 30  # sekundy na oczekiwanie na wolne połączenie

# WAPRO transient error codes (08001=connection fail, 08S01=communication link)
_TRANSIENT_SQL_STATES: frozenset[str] = frozenset({"08001", "08S01", "HYT00", "HY000"})

# Kolumny SELECT dla VIEW_kontrahenci — jawna lista (nigdy SELECT *)
_COLS_KONTRAHENCI = """
    ID_KONTRAHENTA,
    NazwaKontrahenta,
    Email,
    Telefon,
    SumaDlugu,
    LiczbaFaktur,
    NajstarszaFaktura,
    DniPrzeterminowania,
    OstatniMonitData,
    OstatniMonitTyp,
    LiczbaMonitow
"""

# Kolumny SELECT dla VIEW_rozrachunki_faktur — jawna lista
_COLS_ROZRACHUNKI = """
    ID_KONTRAHENTA,
    NumerFaktury,
    DataWystawienia,
    TerminPlatnosci,
    KwotaBrutto,
    KwotaPozostala,
    KwotaZaplacona,
    CzyZaplacona,
    CzyPrzeterminowana,
    DniPrzeterminowania,
    MetodaPlatnosci,
    KategoriaWieku
"""


# ---------------------------------------------------------------------------
# Typy wejściowe dla zapytań — immutable dataclasses
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DebtorFilterParams:
    """
    Parametry filtrowania listy dłużników.
    Wszystkie pola opcjonalne — None = brak filtru.
    Walidacja i sanityzacja w __post_init__.
    """
    search_query: Optional[str] = None          # NazwaKontrahenta LIKE %q%
    min_debt_amount: Optional[Decimal] = None   # SumaDlugu >= X
    max_debt_amount: Optional[Decimal] = None   # SumaDlugu <= X
    last_contact_days: Optional[int] = None     # OstatniMonitData < DATEADD(DAY,-X,GETDATE())
    no_contact_ever: Optional[bool] = None      # LiczbaMonitow = 0
    overdue_days_min: Optional[int] = None      # MaxDniPrzeterminowania >= X
    overdue_days_max: Optional[int] = None      # MaxDniPrzeterminowania <= X
    age_category: Optional[str] = None          # KategoriaWieku = X
    has_email: Optional[bool] = None            # Email IS NOT NULL AND Email != ''
    has_phone: Optional[bool] = None            # Telefon IS NOT NULL AND Telefon != ''
    # Paginacja
    limit: int = 50
    offset: int = 0
    # Sortowanie
    order_by: str = "SumaDlugu"
    order_dir: str = "DESC"

    # Dozwolone kolumny sortowania — whitelist (nigdy interpolacja string z frontendu!)
    _ALLOWED_ORDER_BY: frozenset[str] = field(
        default=frozenset({
            "SumaDlugu", "NazwaKontrahenta", "DniPrzeterminowania",
            "OstatniMonitData", "LiczbaFaktur", "LiczbaMonitow",
            "NajstarszaFaktura",
        }),
        init=False, repr=False, compare=False,
    )
    _ALLOWED_AGE_CATEGORIES: frozenset[str] = field(
        default=frozenset({
            "biezace", "do_30_dni", "31_60_dni", "61_90_dni", "powyzej_90_dni",
        }),
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        # search_query: sanityzacja NFC + strip + długość
        if self.search_query is not None:
            sanitized = _sanitize_text(self.search_query)
            if len(sanitized) > _MAX_SEARCH_LEN:
                raise ValueError(
                    f"search_query przekracza limit {_MAX_SEARCH_LEN} znaków "
                    f"(po sanityzacji: {len(sanitized)})"
                )
            # Frozen dataclass — używamy object.__setattr__
            object.__setattr__(self, "search_query", sanitized)

        # Kwoty — muszą być >= 0
        if self.min_debt_amount is not None and self.min_debt_amount < 0:
            raise ValueError("min_debt_amount nie może być ujemna")
        if self.max_debt_amount is not None and self.max_debt_amount < 0:
            raise ValueError("max_debt_amount nie może być ujemna")
        if (
            self.min_debt_amount is not None
            and self.max_debt_amount is not None
            and self.min_debt_amount > self.max_debt_amount
        ):
            raise ValueError("min_debt_amount > max_debt_amount")

        # Dni — muszą być >= 0
        for field_name in ("last_contact_days", "overdue_days_min", "overdue_days_max"):
            val = getattr(self, field_name)
            if val is not None and val < 0:
                raise ValueError(f"{field_name} nie może być ujemna")

        # age_category — whitelist
        if self.age_category is not None:
            if self.age_category not in self._ALLOWED_AGE_CATEGORIES:
                raise ValueError(
                    f"Niedozwolona age_category: {self.age_category!r}. "
                    f"Dozwolone: {sorted(self._ALLOWED_AGE_CATEGORIES)}"
                )

        # Paginacja
        if self.limit < 1 or self.limit > 200:
            raise ValueError(f"limit musi być między 1 a 200, otrzymano: {self.limit}")
        if self.offset < 0:
            raise ValueError(f"offset nie może być ujemny: {self.offset}")

        # Sortowanie — whitelist (SQL injection protection!)
        if self.order_by not in self._ALLOWED_ORDER_BY:
            raise ValueError(
                f"Niedozwolone order_by: {self.order_by!r}. "
                f"Dozwolone: {sorted(self._ALLOWED_ORDER_BY)}"
            )
        if self.order_dir.upper() not in ("ASC", "DESC"):
            raise ValueError(
                f"Niedozwolone order_dir: {self.order_dir!r}. Dozwolone: ASC, DESC"
            )
        object.__setattr__(self, "order_dir", self.order_dir.upper())


@dataclass(frozen=True)
class InvoiceFilterParams:
    """Parametry filtrowania faktur dla konkretnego kontrahenta."""
    kontrahent_id: int
    include_paid: bool = False      # False = tylko nieopłacone (główny przypadek)
    limit: int = 100
    offset: int = 0
    order_by: str = "TerminPlatnosci"
    order_dir: str = "ASC"

    _ALLOWED_ORDER_BY: frozenset[str] = field(
        default=frozenset({
            "TerminPlatnosci", "DataWystawienia", "KwotaBrutto",
            "KwotaPozostala", "DniPrzeterminowania",
        }),
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if self.kontrahent_id <= 0:
            raise ValueError(f"kontrahent_id musi być > 0, otrzymano: {self.kontrahent_id}")
        if self.limit < 1 or self.limit > 500:
            raise ValueError(f"limit musi być między 1 a 500, otrzymano: {self.limit}")
        if self.offset < 0:
            raise ValueError(f"offset nie może być ujemny: {self.offset}")
        if self.order_by not in self._ALLOWED_ORDER_BY:
            raise ValueError(
                f"Niedozwolone order_by: {self.order_by!r}. "
                f"Dozwolone: {sorted(self._ALLOWED_ORDER_BY)}"
            )
        if self.order_dir.upper() not in ("ASC", "DESC"):
            raise ValueError(f"Niedozwolone order_dir: {self.order_dir!r}")
        object.__setattr__(self, "order_dir", self.order_dir.upper())


# ---------------------------------------------------------------------------
# Wyniki zapytań — typowane słowniki
# ---------------------------------------------------------------------------

DebtorRow = dict[str, Any]
InvoiceRow = dict[str, Any]


@dataclass
class QueryResult:
    """Wynik zapytania z pełnymi metadanymi diagnostycznymi."""
    query_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    rows: list[dict[str, Any]] = field(default_factory=list)
    total_count: Optional[int] = None
    duration_ms: Optional[float] = None
    query_type: str = ""
    params_summary: dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    from_cache: bool = False
    error: Optional[str] = None
    success: bool = False

    def to_log_dict(self) -> dict[str, Any]:
        return {
            "event": "wapro_query",
            "query_id": self.query_id,
            "query_type": self.query_type,
            "success": self.success,
            "rows_returned": len(self.rows),
            "total_count": self.total_count,
            "duration_ms": self.duration_ms,
            "retry_count": self.retry_count,
            "from_cache": self.from_cache,
            "params_summary": self.params_summary,
            "error": self.error,
        }


# ---------------------------------------------------------------------------
# Sanityzacja parametrów — linia obrony #1
# ---------------------------------------------------------------------------

def _sanitize_text(value: str) -> str:
    """
    Sanityzacja tekstu wejściowego:
    1. Unicode NFC normalization (zapobiega homoglyph attacks)
    2. Strip whitespace
    3. Usunięcie znaków kontrolnych (oprócz \t, \n)
    4. Escape specjalnych znaków LIKE: %, _, [, ]
    """
    # NFC normalization
    normalized = unicodedata.normalize("NFC", value)
    # Strip
    stripped = normalized.strip()
    # Usunięcie znaków kontrolnych (U+0000–U+001F oprócz \t i \n, plus U+007F)
    cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", stripped)
    return cleaned


def _escape_like(value: str) -> str:
    """
    Escape znaków specjalnych SQL LIKE: %, _, [, ]
    Używane gdy chcemy literalnego wyszukiwania tych znaków.
    """
    return (
        value
        .replace("[", "[[]")
        .replace("%", "[%]")
        .replace("_", "[_]")
    )


def _validate_kontrahent_id(value: Any) -> int:
    """Walidacja ID kontrahenta — musi być dodatnim intem."""
    try:
        int_val = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Nieprawidłowy kontrahent_id: {value!r}") from exc
    if int_val <= 0:
        raise ValueError(f"kontrahent_id musi być > 0, otrzymano: {int_val}")
    return int_val


# ---------------------------------------------------------------------------
# Connection Pool (thread-safe, singleton per process)
# ---------------------------------------------------------------------------

class WaproConnectionPool:
    """
    Prosty connection pool dla pyodbc.
    Thread-safe. Singleton per process (lazy init).

    Pool zarządza:
    - Limitem otwartych połączeń (max_size)
    - Timeoutem oczekiwania na wolne połączenie
    - Automatycznym reconnect przy błędzie
    - Statystykami diagnostycznymi (w logach)
    """

    def __init__(
        self,
        connection_string: str,
        pool_size: int = _DEFAULT_POOL_SIZE,
        timeout: int = _DEFAULT_POOL_TIMEOUT,
    ) -> None:
        self._connection_string = connection_string
        self._pool_size = pool_size
        self._timeout = timeout

        self._lock = threading.Lock()
        self._semaphore = threading.BoundedSemaphore(pool_size)
        self._pool: list[pyodbc.Connection] = []

        # Statystyki
        self._stats = {
            "total_acquired": 0,
            "total_released": 0,
            "total_errors": 0,
            "total_reconnects": 0,
            "pool_size": pool_size,
            "timeout_seconds": timeout,
        }

        logger.info(
            "WaproConnectionPool zainicjalizowany: pool_size=%d, timeout=%ds",
            pool_size,
            timeout,
        )

    def _create_connection(self) -> pyodbc.Connection:
        conn = pyodbc.connect(
            self._connection_string,
            autocommit=True,
            timeout=self._timeout,
        )
        # Encoding dla poprawnej obsługi polskich znaków NVARCHAR
        conn.setdecoding(pyodbc.SQL_CHAR,  encoding="utf-8")
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-8")
        conn.setencoding(encoding="utf-8")
        logger.debug("Nowe połączenie pyodbc utworzone")
        return conn

    def _is_connection_alive(self, conn: pyodbc.Connection) -> bool:
        """Sprawdza czy połączenie jest aktywne (lekki ping)."""
        try:
            conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    @contextmanager
    def acquire(self) -> Generator[pyodbc.Connection, None, None]:
        """
        Context manager: pobiera połączenie z puli, zwalnia po wyjściu.

        Usage:
            with pool.acquire() as conn:
                cursor = conn.cursor()
                ...
        """
        acquired = self._semaphore.acquire(timeout=self._timeout)
        if not acquired:
            self._stats["total_errors"] += 1
            raise TimeoutError(
                f"Timeout oczekiwania na połączenie WAPRO ({self._timeout}s). "
                f"Pool wyczerpany ({self._pool_size} połączeń w użyciu)."
            )

        conn = None
        try:
            with self._lock:
                self._stats["total_acquired"] += 1
                if self._pool:
                    conn = self._pool.pop()
                    # Weryfikacja czy połączenie wciąż żyje
                    if not self._is_connection_alive(conn):
                        logger.warning("Martwe połączenie w puli — reconnect")
                        self._stats["total_reconnects"] += 1
                        try:
                            conn.close()
                        except Exception:
                            pass
                        conn = None

            if conn is None:
                conn = self._create_connection()

            yield conn

        except Exception as exc:
            self._stats["total_errors"] += 1
            logger.error(
                "Błąd podczas użycia połączenia WAPRO: %s",
                exc,
                extra={"traceback": traceback.format_exc()},
            )
            # Nie zwracamy błędnego połączenia do puli
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None
            raise

        finally:
            with self._lock:
                self._stats["total_released"] += 1
                if conn is not None:
                    self._pool.append(conn)
            self._semaphore.release()

    def get_stats(self) -> dict[str, Any]:
        """Zwraca statystyki puli — do health endpoint."""
        with self._lock:
            return {
                **self._stats,
                "current_pool_size": len(self._pool),
            }

    def close_all(self) -> None:
        """Zamyka wszystkie połączenia w puli (shutdown)."""
        with self._lock:
            for conn in self._pool:
                try:
                    conn.close()
                except Exception:
                    pass
            self._pool.clear()
        logger.info("WaproConnectionPool zamknięty")


# ---------------------------------------------------------------------------
# Singleton pool + thread pool executor (singleton per process)
# ---------------------------------------------------------------------------

_pool_instance: Optional[WaproConnectionPool] = None
_pool_lock = threading.Lock()
_executor: Optional[ThreadPoolExecutor] = None


def initialize_pool(
    connection_string: str,
    pool_size: int = _DEFAULT_POOL_SIZE,
    timeout: int = _DEFAULT_POOL_TIMEOUT,
    executor_workers: int = 10,
) -> None:
    """
    Inicjalizuje pool połączeń i thread executor.
    Wywoływana raz przy starcie aplikacji (lifespan).

    Args:
        connection_string: Pełny pyodbc connection string (z .env)
        pool_size:         Maks. liczba równoległych połączeń WAPRO
        timeout:           Timeout na oczekiwanie na wolne połączenie [s]
        executor_workers:  Rozmiar thread pool (musi być >= pool_size)
    """
    global _pool_instance, _executor

    with _pool_lock:
        if _pool_instance is not None:
            logger.warning("WaproConnectionPool już zainicjalizowany — pomijam")
            return

        logger.info(
            "Inicjalizacja WaproConnectionPool: pool_size=%d, workers=%d",
            pool_size, executor_workers,
        )
        _pool_instance = WaproConnectionPool(
            connection_string=connection_string,
            pool_size=pool_size,
            timeout=timeout,
        )
        _executor = ThreadPoolExecutor(
            max_workers=executor_workers,
            thread_name_prefix="wapro_worker",
        )
        logger.info("WAPRO pool i executor gotowe")


def shutdown_pool() -> None:
    """
    Zamyka pool i executor.
    Wywoływana przy shutdown aplikacji (lifespan).
    """
    global _pool_instance, _executor

    with _pool_lock:
        if _executor is not None:
            _executor.shutdown(wait=True, cancel_futures=False)
            _executor = None
            logger.info("WAPRO ThreadPoolExecutor zamknięty")

        if _pool_instance is not None:
            _pool_instance.close_all()
            _pool_instance = None
            logger.info("WAPRO ConnectionPool zamknięty")


def get_pool() -> WaproConnectionPool:
    """Zwraca singleton pool. Rzuca RuntimeError jeśli nie zainicjalizowany."""
    if _pool_instance is None:
        raise RuntimeError(
            "WaproConnectionPool nie jest zainicjalizowany. "
            "Wywołaj initialize_pool() w lifespan FastAPI."
        )
    return _pool_instance


def get_executor() -> ThreadPoolExecutor:
    """Zwraca singleton executor."""
    if _executor is None:
        raise RuntimeError(
            "ThreadPoolExecutor nie jest zainicjalizowany. "
            "Wywołaj initialize_pool() w lifespan FastAPI."
        )
    return _executor


# ---------------------------------------------------------------------------
# Pomocnik: synchroniczne wykonanie zapytania z retry
# ---------------------------------------------------------------------------

def _execute_query_sync(
    sql: str,
    params: tuple[Any, ...],
    query_id: str,
    query_type: str,
) -> list[dict[str, Any]]:
    """
    Synchroniczne wykonanie zapytania pyodbc z retry (exponential backoff).
    Wywoływane w thread pool executor — NIE w event loop.

    Returns:
        Lista wierszy jako dict[kolumna → wartość]

    Raises:
        pyodbc.Error: Przy nierecoverable błędzie po wyczerpaniu retry
    """
    pool = get_pool()
    last_exc: Optional[Exception] = None

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            t_start = time.monotonic()

            with pool.acquire() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                columns = [col[0] for col in cursor.description]
                rows_raw = cursor.fetchall()
                cursor.close()

            duration_ms = (time.monotonic() - t_start) * 1000

            # Konwersja do list[dict] z typowaniem Decimal/date/datetime
            rows = []
            for raw_row in rows_raw:
                row_dict: dict[str, Any] = {}
                for col, val in zip(columns, raw_row):
                    row_dict[col] = _coerce_value(val)
                rows.append(row_dict)

            logger.debug(
                "Zapytanie %s wykonane: attempt=%d, rows=%d, %.1fms",
                query_id, attempt, len(rows), duration_ms,
                extra={
                    "query_id": query_id,
                    "query_type": query_type,
                    "attempt": attempt,
                    "rows": len(rows),
                    "duration_ms": duration_ms,
                },
            )
            return rows

        except TimeoutError:
            # Pool timeout — nie retry (problem systemowy)
            raise

        except pyodbc.Error as exc:
            last_exc = exc
            sql_state = exc.args[0] if exc.args else "UNKNOWN"
            is_transient = str(sql_state) in _TRANSIENT_SQL_STATES

            logger.warning(
                "pyodbc error w %s (attempt %d/%d, state=%s, transient=%s): %s",
                query_id, attempt, _MAX_RETRIES, sql_state, is_transient, exc,
                extra={
                    "query_id": query_id,
                    "attempt": attempt,
                    "sql_state": sql_state,
                    "is_transient": is_transient,
                    "traceback": traceback.format_exc(),
                },
            )

            if not is_transient or attempt >= _MAX_RETRIES:
                break

            delay = _RETRY_BASE_DELAY * (2 ** (attempt - 1))
            logger.info(
                "Retry za %.1fs (attempt %d/%d)",
                delay, attempt + 1, _MAX_RETRIES,
                extra={"query_id": query_id},
            )
            time.sleep(delay)

    raise last_exc or RuntimeError(f"Nieznany błąd w _execute_query_sync [{query_id}]")


def _coerce_value(value: Any) -> Any:
    """
    Konwersja typów pyodbc → Python:
    - Decimal: zostawiamy jako Decimal (pydantic obsługuje)
    - datetime: zostawiamy (Pydantic v2 obsługuje)
    - bytes: dekoduj jako UTF-8 (MSSQL NVARCHAR edge case)
    - None: zostawiamy
    """
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


# ---------------------------------------------------------------------------
# Async wrappers — API publiczne (wywoływane z coroutines FastAPI)
# ---------------------------------------------------------------------------

async def _run_in_executor(
    func,
    *args: Any,
) -> Any:
    """Uruchamia synchroniczną funkcję w thread pool executor."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(get_executor(), func, *args)


# ---------------------------------------------------------------------------
# ZAPYTANIE 1: Lista dłużników (VIEW_kontrahenci)
# ---------------------------------------------------------------------------

def _build_debtors_query(
    params: DebtorFilterParams,
) -> tuple[str, tuple[Any, ...]]:
    """
    Buduje SQL + parametry dla zapytania listy dłużników.
    Używa jawnej listy kolumn (nigdy SELECT *).
    Sortowanie: whitelist — bezpieczne interpolacja tylko z zatwierdzonej listy.

    Returns:
        (sql_string, params_tuple)
    """
    conditions: list[str] = []
    query_params: list[Any] = []

    # search_query — LIKE z escape specjalnych znaków
    if params.search_query:
        escaped = _escape_like(params.search_query)
        conditions.append("NazwaKontrahenta LIKE ?")
        like_val = f"%{escaped}%"
        query_params.append(like_val)

    # Kwoty
    if params.min_debt_amount is not None:
        conditions.append("SumaDlugu >= ?")
        query_params.append(params.min_debt_amount)

    if params.max_debt_amount is not None:
        conditions.append("SumaDlugu <= ?")
        query_params.append(params.max_debt_amount)

    # Ostatni kontakt
    if params.last_contact_days is not None:
        conditions.append(
            "(OstatniMonitData < DATEADD(DAY, ?, GETDATE()) OR OstatniMonitData IS NULL)"
        )
        query_params.append(-abs(params.last_contact_days))

    # Nigdy nie kontaktowany
    if params.no_contact_ever is True:
        conditions.append("LiczbaMonitow = 0")
    elif params.no_contact_ever is False:
        conditions.append("LiczbaMonitow > 0")

    # Dni przeterminowania
    if params.overdue_days_min is not None:
        conditions.append("DniPrzeterminowania >= ?")   # było: MaxDniPrzeterminowania
        query_params.append(params.overdue_days_min)

    if params.overdue_days_max is not None:
        conditions.append("DniPrzeterminowania <= ?")   # było: MaxDniPrzeterminowania
        query_params.append(params.overdue_days_max)

    # Email / telefon
    if params.has_email is True:
        conditions.append("Email IS NOT NULL AND Email <> ''")
    elif params.has_email is False:
        conditions.append("(Email IS NULL OR Email = '')")

    if params.has_phone is True:
        conditions.append("Telefon IS NOT NULL AND Telefon <> ''")
    elif params.has_phone is False:
        conditions.append("(Telefon IS NULL OR Telefon = '')")

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    # Sortowanie — bezpieczna interpolacja tylko z whitelisted kolumny
    order_clause = f"ORDER BY {params.order_by} {params.order_dir}"

    sql = f"""
        SELECT {_COLS_KONTRAHENCI}
        FROM {VIEW_KONTRAHENCI}
        {where_clause}
        {order_clause}
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
    """
    query_params.extend([params.offset, params.limit])

    return sql.strip(), tuple(query_params)


def _build_debtors_count_query(
    params: DebtorFilterParams,
) -> tuple[str, tuple[Any, ...]]:
    """
    Buduje zapytanie COUNT(*) dla paginacji (te same warunki co list query).
    Osobna funkcja dla przejrzystości i niezależności testów.
    """
    data_sql, data_params = _build_debtors_query(params)

    # Podmień SELECT lista + ORDER BY + OFFSET na COUNT(*)
    # Bezpieczny sposób: wrap w subquery
    count_sql = f"""
        SELECT COUNT(*) AS TotalCount
        FROM (
            SELECT ID_KONTRAHENTA
            FROM {VIEW_KONTRAHENCI}
            {_extract_where_clause(data_sql)}
        ) AS cnt_sub
    """
    # Params bez OFFSET i LIMIT (ostatnie 2 elementy)
    count_params = data_params[:-2]
    return count_sql.strip(), count_params


def _extract_where_clause(full_sql: str) -> str:
    """Wyciąga klauzulę WHERE z pełnego SQL (do COUNT query)."""
    upper = full_sql.upper()
    where_idx = upper.find("WHERE")
    order_idx = upper.find("ORDER BY")

    if where_idx == -1:
        return ""
    if order_idx != -1:
        return full_sql[where_idx:order_idx].strip()
    return full_sql[where_idx:].strip()


async def get_debtors(
    params: DebtorFilterParams,
    *,
    include_total_count: bool = True,
) -> QueryResult:
    """
    Pobiera listę dłużników z VIEW_kontrahenci z filtrowaniem i paginacją.

    Args:
        params:              Parametry filtrowania (już zwalidowane)
        include_total_count: Czy pobrać COUNT(*) dla paginacji (2 zapytania)

    Returns:
        QueryResult z rows + total_count
    """
    query_id = str(uuid.uuid4())
    result = QueryResult(
        query_id=query_id,
        query_type="debtors_list",
        params_summary={
            "search_query": bool(params.search_query),
            "has_amount_filter": (
                params.min_debt_amount is not None
                or params.max_debt_amount is not None
            ),
            "has_overdue_filter": (
                params.overdue_days_min is not None
                or params.overdue_days_max is not None
            ),
            "limit": params.limit,
            "offset": params.offset,
            "order_by": params.order_by,
            "order_dir": params.order_dir,
        },
    )

    logger.info(
        "get_debtors start [%s]: limit=%d, offset=%d, order=%s %s",
        query_id, params.limit, params.offset, params.order_by, params.order_dir,
        extra={"query_id": query_id, "params_summary": result.params_summary},
    )

    t_start = time.monotonic()

    try:
        data_sql, data_params = _build_debtors_query(params)

        # Uruchom zapytanie danych w thread pool
        rows_task = _run_in_executor(
            _execute_query_sync,
            data_sql, data_params, query_id, "debtors_data",
        )

        # Opcjonalnie: COUNT w osobnym zapytaniu (współbieżnie)
        if include_total_count:
            count_sql, count_params = _build_debtors_count_query(params)
            count_task = _run_in_executor(
                _execute_query_sync,
                count_sql, count_params, f"{query_id}_count", "debtors_count",
            )
            rows_raw, count_raw = await asyncio.gather(rows_task, count_task)
            result.total_count = count_raw[0]["TotalCount"] if count_raw else 0
        else:
            rows_raw = await rows_task

        result.rows = rows_raw
        result.duration_ms = (time.monotonic() - t_start) * 1000
        result.success = True

        logger.info(
            "get_debtors OK [%s]: rows=%d, total=%s, %.1fms",
            query_id, len(result.rows), result.total_count, result.duration_ms,
            extra=result.to_log_dict(),
        )

    except Exception as exc:
        result.duration_ms = (time.monotonic() - t_start) * 1000
        result.error = str(exc)
        result.success = False

        logger.error(
            "get_debtors BŁĄD [%s]: %s (%.1fms)",
            query_id, exc, result.duration_ms,
            extra={**result.to_log_dict(), "traceback": traceback.format_exc()},
        )
        raise

    return result


# ---------------------------------------------------------------------------
# ZAPYTANIE 2: Szczegóły jednego dłużnika (VIEW_kontrahenci)
# ---------------------------------------------------------------------------

async def get_debtor_by_id(kontrahent_id: int) -> QueryResult:
    """
    Pobiera pełne dane jednego kontrahenta z VIEW_kontrahenci.

    Args:
        kontrahent_id: ID_KONTRAHENTA (walidowany)

    Returns:
        QueryResult z 0 lub 1 wierszem

    Raises:
        ValueError: Przy nieprawidłowym kontrahent_id
    """
    kontrahent_id = _validate_kontrahent_id(kontrahent_id)
    query_id = str(uuid.uuid4())

    result = QueryResult(
        query_id=query_id,
        query_type="debtor_detail",
        params_summary={"kontrahent_id": kontrahent_id},
    )

    logger.info(
        "get_debtor_by_id start [%s]: kontrahent_id=%d",
        query_id, kontrahent_id,
        extra={"query_id": query_id, "kontrahent_id": kontrahent_id},
    )

    t_start = time.monotonic()

    try:
        sql = f"""
            SELECT {_COLS_KONTRAHENCI}
            FROM {VIEW_KONTRAHENCI}
            WHERE ID_KONTRAHENTA = ?
        """
        rows_raw = await _run_in_executor(
            _execute_query_sync,
            sql.strip(), (kontrahent_id,), query_id, "debtor_detail",
        )

        result.rows = rows_raw
        result.total_count = len(rows_raw)
        result.duration_ms = (time.monotonic() - t_start) * 1000
        result.success = True

        logger.info(
            "get_debtor_by_id OK [%s]: kontrahent_id=%d, found=%s, %.1fms",
            query_id, kontrahent_id, bool(rows_raw), result.duration_ms,
            extra=result.to_log_dict(),
        )

    except Exception as exc:
        result.duration_ms = (time.monotonic() - t_start) * 1000
        result.error = str(exc)
        result.success = False

        logger.error(
            "get_debtor_by_id BŁĄD [%s]: kontrahent_id=%d, %s",
            query_id, kontrahent_id, exc,
            extra={**result.to_log_dict(), "traceback": traceback.format_exc()},
        )
        raise

    return result


# ---------------------------------------------------------------------------
# ZAPYTANIE 3: Faktury kontrahenta (VIEW_rozrachunki_faktur)
# ---------------------------------------------------------------------------

def _build_invoices_query(
    params: InvoiceFilterParams,
) -> tuple[str, tuple[Any, ...]]:
    """
    Buduje SQL + parametry dla zapytania faktur kontrahenta.
    Filtry: kontrahent_id (wymagany) + opcjonalnie include_paid.
    """
    conditions = ["ID_KONTRAHENTA = ?"]
    query_params: list[Any] = [params.kontrahent_id]

    if not params.include_paid:
        conditions.append("CzyZaplacona = 0")

    where_clause = "WHERE " + " AND ".join(conditions)
    order_clause = f"ORDER BY {params.order_by} {params.order_dir}"

    sql = f"""
        SELECT {_COLS_ROZRACHUNKI}
        FROM {VIEW_ROZRACHUNKI_FAKTUR}
        {where_clause}
        {order_clause}
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
    """
    query_params.extend([params.offset, params.limit])
    return sql.strip(), tuple(query_params)


async def get_invoices_for_debtor(params: InvoiceFilterParams) -> QueryResult:
    """
    Pobiera faktury kontrahenta z VIEW_rozrachunki_faktur.

    Args:
        params: InvoiceFilterParams (już zwalidowane)

    Returns:
        QueryResult z listą faktur
    """
    query_id = str(uuid.uuid4())
    result = QueryResult(
        query_id=query_id,
        query_type="invoices_for_debtor",
        params_summary={
            "kontrahent_id": params.kontrahent_id,
            "include_paid": params.include_paid,
            "limit": params.limit,
            "offset": params.offset,
        },
    )

    logger.info(
        "get_invoices_for_debtor start [%s]: kontrahent_id=%d, include_paid=%s",
        query_id, params.kontrahent_id, params.include_paid,
        extra={"query_id": query_id, "params_summary": result.params_summary},
    )

    t_start = time.monotonic()

    try:
        data_sql, data_params = _build_invoices_query(params)

        # COUNT zapytanie — te same warunki bez OFFSET/LIMIT
        count_sql = f"""
            SELECT COUNT(*) AS TotalCount
            FROM {VIEW_ROZRACHUNKI_FAKTUR}
            WHERE ID_KONTRAHENTA = ?
            {' AND CzyZaplacona = 0' if not params.include_paid else ''}
        """

        rows_task = _run_in_executor(
            _execute_query_sync,
            data_sql, data_params, query_id, "invoices_data",
        )
        count_task = _run_in_executor(
            _execute_query_sync,
            count_sql.strip(),
            (params.kontrahent_id,),
            f"{query_id}_count",
            "invoices_count",
        )

        rows_raw, count_raw = await asyncio.gather(rows_task, count_task)

        result.rows = rows_raw
        result.total_count = count_raw[0]["TotalCount"] if count_raw else 0
        result.duration_ms = (time.monotonic() - t_start) * 1000
        result.success = True

        logger.info(
            "get_invoices_for_debtor OK [%s]: kontrahent_id=%d, "
            "rows=%d, total=%d, %.1fms",
            query_id, params.kontrahent_id,
            len(result.rows), result.total_count or 0, result.duration_ms,
            extra=result.to_log_dict(),
        )

    except Exception as exc:
        result.duration_ms = (time.monotonic() - t_start) * 1000
        result.error = str(exc)
        result.success = False

        logger.error(
            "get_invoices_for_debtor BŁĄD [%s]: kontrahent_id=%d, %s",
            query_id, params.kontrahent_id, exc,
            extra={**result.to_log_dict(), "traceback": traceback.format_exc()},
        )
        raise

    return result


# ---------------------------------------------------------------------------
# ZAPYTANIE 4: Walidacja bulk — czy lista ID_KONTRAHENTA istnieje w WAPRO
# ---------------------------------------------------------------------------

async def validate_kontrahent_ids(
    ids: list[int],
) -> dict[int, bool]:
    """
    Sprawdza które z podanych ID_KONTRAHENTA istnieją w WAPRO.
    Używane przez endpoint POST /debtors/validate-bulk.

    Ograniczenie: max 500 ID w jednym wywołaniu (ochrona przed payload injection).

    Returns:
        dict {id: exists_in_wapro}
    """
    if len(ids) > 500:
        raise ValueError(
            f"Zbyt wiele ID do walidacji: {len(ids)}. Maksimum: 500."
        )
    if not ids:
        return {}

    # Walidacja każdego ID
    validated_ids = []
    for raw_id in ids:
        try:
            v = _validate_kontrahent_id(raw_id)
            validated_ids.append(v)
        except ValueError as exc:
            logger.warning("Nieprawidłowy ID podczas bulk validate: %s", exc)
            # Uznajemy za nieistniejący
            continue

    if not validated_ids:
        return {id_: False for id_ in ids}

    query_id = str(uuid.uuid4())

    logger.info(
        "validate_kontrahent_ids [%s]: sprawdzam %d ID",
        query_id, len(validated_ids),
        extra={"query_id": query_id, "id_count": len(validated_ids)},
    )

    # Używamy parametryzowanego IN z listą
    placeholders = ", ".join("?" * len(validated_ids))
    sql = f"""
        SELECT ID_KONTRAHENTA
        FROM {VIEW_KONTRAHENCI}
        WHERE ID_KONTRAHENTA IN ({placeholders})
    """

    def _run_validate() -> list[dict[str, Any]]:
        return _execute_query_sync(
            sql.strip(),
            tuple(validated_ids),
            query_id,
            "bulk_validate",
        )

    try:
        rows = await _run_in_executor(_run_validate)
        existing_ids = {row["ID_KONTRAHENTA"] for row in rows}

        result = {id_: (id_ in existing_ids) for id_ in validated_ids}
        # Dodaj False dla nieprzeszłych walidacji
        for raw_id in ids:
            if raw_id not in result:
                result[raw_id] = False

        found_count = sum(1 for v in result.values() if v)
        logger.info(
            "validate_kontrahent_ids OK [%s]: %d/%d znalezionych",
            query_id, found_count, len(ids),
            extra={
                "query_id": query_id,
                "total": len(ids),
                "found": found_count,
                "missing": len(ids) - found_count,
            },
        )
        return result

    except Exception as exc:
        logger.error(
            "validate_kontrahent_ids BŁĄD [%s]: %s",
            query_id, exc,
            extra={"query_id": query_id, "traceback": traceback.format_exc()},
        )
        raise


# ---------------------------------------------------------------------------
# ZAPYTANIE 5: Health check — test połączenia WAPRO
# ---------------------------------------------------------------------------

async def ping() -> dict[str, Any]:
    """
    Test połączenia z bazą WAPRO.
    Używany przez GET /system/health.

    Returns:
        {"status": "ok"/"error", "latency_ms": float, "details": ...}
    """
    query_id = str(uuid.uuid4())
    t_start = time.monotonic()

    try:
        def _ping() -> list[dict[str, Any]]:
            return _execute_query_sync(
                "SELECT 1 AS ping, GETDATE() AS server_time, @@VERSION AS sql_version",
                (),
                query_id,
                "health_ping",
            )

        rows = await _run_in_executor(_ping)
        latency_ms = (time.monotonic() - t_start) * 1000
        server_time = rows[0].get("server_time") if rows else None

        pool_stats = get_pool().get_stats()

        result = {
            "status": "ok",
            "latency_ms": round(latency_ms, 2),
            "server_time": server_time.isoformat() if server_time else None,
            "pool": pool_stats,
            "views": {
                "VIEW_kontrahenci": VIEW_KONTRAHENCI,
                "VIEW_rozrachunki_faktur": VIEW_ROZRACHUNKI_FAKTUR,
            },
        }

        logger.debug(
            "WAPRO ping OK [%s]: %.1fms",
            query_id, latency_ms,
            extra={"query_id": query_id, "latency_ms": latency_ms},
        )
        return result

    except Exception as exc:
        latency_ms = (time.monotonic() - t_start) * 1000
        logger.error(
            "WAPRO ping BŁĄD [%s]: %s",
            query_id, exc,
            extra={
                "query_id": query_id,
                "latency_ms": latency_ms,
                "traceback": traceback.format_exc(),
            },
        )
        return {
            "status": "error",
            "latency_ms": round(latency_ms, 2),
            "error": str(exc),
            "pool": get_pool().get_stats() if _pool_instance else {},
        }


# ---------------------------------------------------------------------------
# Eksport publicznego API
# ---------------------------------------------------------------------------

__all__ = [
    # Inicjalizacja
    "initialize_pool",
    "shutdown_pool",
    "get_pool",
    # Typy parametrów
    "DebtorFilterParams",
    "InvoiceFilterParams",
    # Wyniki
    "QueryResult",
    # Zapytania
    "get_debtors",
    "get_debtor_by_id",
    "get_invoices_for_debtor",
    "validate_kontrahent_ids",
    "ping",
    # Stałe — nazwy widoków (do referencji w serwisach)
    "VIEW_KONTRAHENCI",
    "VIEW_ROZRACHUNKI_FAKTUR",
]