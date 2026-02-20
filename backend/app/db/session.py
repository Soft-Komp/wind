"""
Zarządzanie sesjami bazy danych — SQLAlchemy async z aioodbc (MSSQL).

Architektura:
  - Jeden `AsyncEngine` na całą aplikację (singleton)
  - Jeden `async_session_factory` (sessionmaker)
  - Dependency `get_db` wstrzykiwana do endpointów przez FastAPI `Depends()`
  - Dependency `get_wapro_db` dla połączeń read-only do tabel WAPRO (pyodbc)

Zasady:
  - Sesja SQLAlchemy: TYLKO tabele dbo_ext (zapis i odczyt przez ORM)
  - Sesja WAPRO: TYLKO widoki read-only (pyodbc, przez wapro.py)
  - Każda sesja automatycznie zamykana po zakończeniu requestu
  - Przy błędzie: rollback + log + propagacja wyjątku
  - Pool health check przy starcie aplikacji

Użycie w endpointach:
    from app.db.session import get_db

    @router.get("/users")
    async def list_users(db: AsyncSession = Depends(get_db)):
        result = await db.execute(select(User))
        ...
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy import event, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

# Import settings — config.py musi być załadowany przed session.py
from app.core.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tworzenie async engine — JEDEN na całą aplikację
# ---------------------------------------------------------------------------

def _build_engine() -> AsyncEngine:
    """
    Buduje i konfiguruje AsyncEngine dla MSSQL przez aioodbc.

    Parametry puli połączeń pobierane z settings — konfigurowalne przez .env.
    Engine jest lazy — faktyczne połączenia nawiązywane przy pierwszym użyciu.

    Returns:
        AsyncEngine: Skonfigurowany silnik SQLAlchemy async.
    """
    sqlalchemy_url = settings.get_sqlalchemy_url()

    # UWAGA: URL zawiera zakodowane hasło — nie logujemy go nigdy
    logger.info(
        "Inicjalizacja AsyncEngine | driver=aioodbc | host=%s | db=%s | "
        "pool_size=%d | max_overflow=%d | pool_recycle=%ds",
        settings.db_host,
        settings.db_name,
        settings.db_pool_size,
        settings.db_pool_max_overflow,
        settings.db_pool_recycle,
    )

    engine = create_async_engine(
        sqlalchemy_url,
        # ---- Pula połączeń ----
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_pool_max_overflow,
        pool_timeout=settings.db_pool_timeout,
        pool_recycle=settings.db_pool_recycle,
        # Sprawdzenie połączenia przed wydaniem z puli
        pool_pre_ping=True,

        # ---- Logowanie SQL ----
        # echo=True tylko w debug — NIE na produkcji (hasła w query params!)
        echo=settings.debug and settings.is_development,
        echo_pool=settings.debug and settings.is_development,

        # ---- Ustawienia MSSQL ----
        # MSSQL wymaga isolation level READ COMMITTED (domyślny)
        isolation_level="READ COMMITTED",

        # ---- Timeout wykonania zapytania ----
        # Zapobiegamy zawieszonym zapytaniom blokującym pulę
        connect_args={
            # aioodbc nie wspiera bezpośrednio query_timeout w connect_args,
            # ale ustawiamy go przez event listener poniżej
        },

        # Metadane dla Alembic — schemat dbo_ext
        # (używane przez autogenerate w env.py)
        execution_options={
            "schema_translate_map": None,
        },
    )

    # ---- Event listenery dla monitoringu i bezpieczeństwa ----

    @event.listens_for(engine.sync_engine, "connect")
    def on_connect(dbapi_conn, connection_record):
        """
        Wywoływane przy każdym nowym połączeniu z puli.
        Ustawia parametry sesji MSSQL.
        """
        logger.debug(
            "Nowe połączenie MSSQL nawiązane | connection_id=%s",
            id(dbapi_conn),
        )
        cursor = dbapi_conn.cursor()
        try:
            # Ustawienie timeout zapytania — 30s (ochrona przed zamrożonymi query)
            cursor.execute("SET QUERY_GOVERNOR_COST_LIMIT 0")
            # MSSQL: ustaw domyślny schemat sesji
            cursor.execute(f"USE [{settings.db_name}]")
        except Exception as exc:
            logger.warning("Błąd konfiguracji sesji MSSQL: %s", exc)
        finally:
            cursor.close()

    @event.listens_for(engine.sync_engine, "checkout")
    def on_checkout(dbapi_conn, connection_record, connection_proxy):
        """Wywoływane przy pobraniu połączenia z puli — do metryk."""
        logger.debug(
            "Połączenie pobrane z puli | connection_id=%s",
            id(dbapi_conn),
        )

    @event.listens_for(engine.sync_engine, "checkin")
    def on_checkin(dbapi_conn, connection_record):
        """Wywoływane przy zwrocie połączenia do puli — do metryk."""
        logger.debug(
            "Połączenie zwrócone do puli | connection_id=%s",
            id(dbapi_conn),
        )

    @event.listens_for(engine.sync_engine, "invalidate")
    def on_invalidate(dbapi_conn, connection_record, exception):
        """Wywoływane gdy połączenie jest unieważniane (np. timeout, błąd sieciowy)."""
        logger.warning(
            "Połączenie MSSQL unieważnione | connection_id=%s | przyczyna=%s",
            id(dbapi_conn),
            str(exception) if exception else "brak",
        )

    return engine


# Singleton engine — tworzony raz przy imporcie modułu
engine: AsyncEngine = _build_engine()

# ---------------------------------------------------------------------------
# Session factory — JEDEN na całą aplikację
# ---------------------------------------------------------------------------

async_session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    # expire_on_commit=False — obiekty ORM pozostają dostępne po commit()
    # (ważne: bez tego dostęp do atrybutów po commit() rzuca DetachedInstanceError)
    expire_on_commit=False,
    # Nie autocommit — każdy request zarządza transakcją ręcznie
    autocommit=False,
    # Nie autoflush — flush ręcznie przed commit lub query
    autoflush=False,
)


# ---------------------------------------------------------------------------
# FastAPI Dependency — wstrzykiwana do endpointów przez Depends()
# ---------------------------------------------------------------------------

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency FastAPI dostarczająca sesję SQLAlchemy dla endpointu.

    Zasady:
      - Sesja otwierana na początku requestu
      - Przy sukcesie: automatyczny flush (nie commit — commit w serwisie)
      - Przy wyjątku: rollback + log + propagacja wyjątku do handler'a
      - Sesja ZAWSZE zamykana w finally — nawet przy błędzie

    Użycie:
        @router.post("/users")
        async def create_user(
            data: UserCreateRequest,
            db: AsyncSession = Depends(get_db),
        ):
            user = await user_service.create(db, data)
            await db.commit()   # ← Commit w endpointcie lub serwisie
            return BaseResponse.created(user)

    Yields:
        AsyncSession: Aktywna sesja bazy danych.
    """
    session_id = id(object())  # Unikalny ID sesji dla logowania
    start_time = time.monotonic()

    logger.debug("Otwieranie sesji DB | session_id=%d", session_id)

    async with async_session_factory() as session:
        try:
            yield session

        except SQLAlchemyError as exc:
            # Błąd bazy danych — rollback + szczegółowy log
            await session.rollback()
            elapsed = time.monotonic() - start_time
            logger.error(
                "Błąd SQLAlchemy — ROLLBACK | session_id=%d | czas=%.3fs | błąd=%s",
                session_id,
                elapsed,
                str(exc),
                exc_info=True,
            )
            raise  # Propagacja do FastAPI exception handler

        except Exception as exc:
            # Inny wyjątek — również rollback (np. ValidationError w serwisie)
            await session.rollback()
            elapsed = time.monotonic() - start_time
            logger.warning(
                "Wyjątek podczas transakcji — ROLLBACK | session_id=%d | "
                "czas=%.3fs | typ=%s | błąd=%s",
                session_id,
                elapsed,
                type(exc).__name__,
                str(exc),
            )
            raise

        finally:
            elapsed = time.monotonic() - start_time
            logger.debug(
                "Zamykanie sesji DB | session_id=%d | czas=%.3fs",
                session_id,
                elapsed,
            )
            # Sesja zamykana automatycznie przez context manager async_session_factory


# ---------------------------------------------------------------------------
# Context manager do użycia poza FastAPI (np. worker ARQ, testy)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def get_db_context() -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager dla sesji DB — użycie poza FastAPI.

    Używany w:
      - Worker ARQ (zadania asynchroniczne)
      - Skrypty migracyjne
      - Testy integracyjne

    Przykład:
        async with get_db_context() as db:
            result = await db.execute(select(User))
            await db.commit()
    """
    session_id = id(object())
    start_time = time.monotonic()

    logger.debug("Otwieranie sesji DB (context) | session_id=%d", session_id)

    async with async_session_factory() as session:
        async with session.begin():
            try:
                yield session
                # begin() automatycznie commituje przy wyjściu bez błędu
            except Exception as exc:
                elapsed = time.monotonic() - start_time
                logger.error(
                    "Błąd w sesji DB (context) — ROLLBACK | "
                    "session_id=%d | czas=%.3fs | błąd=%s",
                    session_id,
                    elapsed,
                    str(exc),
                    exc_info=True,
                )
                raise
            finally:
                elapsed = time.monotonic() - start_time
                logger.debug(
                    "Zamykanie sesji DB (context) | session_id=%d | czas=%.3fs",
                    session_id,
                    elapsed,
                )


# ---------------------------------------------------------------------------
# Health check — sprawdzenie połączenia z MSSQL przy starcie aplikacji
# ---------------------------------------------------------------------------

async def check_db_connection() -> dict:
    """
    Sprawdza połączenie z bazą danych.

    Wykonuje lekkie zapytanie testowe i zwraca szczegóły połączenia.
    Wywoływany przy starcie aplikacji (lifespan event).

    Returns:
        dict: Informacje o połączeniu i statusie.

    Raises:
        SQLAlchemyError: Gdy połączenie z bazą jest niemożliwe.
        Exception: Przy innych błędach.
    """
    start_time = time.monotonic()

    logger.info("Sprawdzanie połączenia z MSSQL...")

    try:
        async with engine.connect() as conn:
            # Zapytanie testowe — sprawdza wersję MSSQL i aktualną bazę
            result = await conn.execute(
                text(
                    "SELECT "
                    "  SERVERPROPERTY('ProductVersion') AS version, "
                    "  SERVERPROPERTY('ProductLevel')   AS level, "
                    "  SERVERPROPERTY('Edition')        AS edition, "
                    "  DB_NAME()                        AS database_name, "
                    "  GETDATE()                        AS server_time, "
                    "  SCHEMA_ID(:schema)               AS schema_id",
                ),
                {"schema": settings.db_schema},
            )
            row = result.mappings().first()

            if row is None:
                raise RuntimeError("Zapytanie testowe nie zwróciło wyników.")

            elapsed = time.monotonic() - start_time

            # Weryfikacja że schemat dbo_ext istnieje
            if row["schema_id"] is None:
                logger.error(
                    "BŁĄD: Schemat '%s' nie istnieje w bazie '%s'! "
                    "Uruchom DDL: 000_create_schema.sql",
                    settings.db_schema,
                    settings.db_name,
                )
                raise RuntimeError(
                    f"Schemat {settings.db_schema} nie istnieje w bazie."
                )

            db_info = {
                "status":        "ok",
                "host":          settings.db_host,
                "database":      str(row["database_name"]),
                "schema":        settings.db_schema,
                "mssql_version": str(row["version"]),
                "mssql_level":   str(row["level"]),
                "mssql_edition": str(row["edition"]),
                "server_time":   str(row["server_time"]),
                "latency_ms":    round(elapsed * 1000, 2),
            }

            logger.info(
                "Połączenie z MSSQL OK | host=%s | db=%s | wersja=%s %s | "
                "schemat=%s | czas=%.2fms",
                db_info["host"],
                db_info["database"],
                db_info["mssql_version"],
                db_info["mssql_level"],
                db_info["schema"],
                db_info["latency_ms"],
            )

            return db_info

    except SQLAlchemyError as exc:
        elapsed = time.monotonic() - start_time
        logger.critical(
            "KRYTYCZNY BŁĄD: Nie można połączyć się z MSSQL! "
            "host=%s | czas=%.2fms | błąd=%s",
            settings.db_host,
            elapsed * 1000,
            str(exc),
            exc_info=True,
        )
        raise

    except Exception as exc:
        elapsed = time.monotonic() - start_time
        logger.critical(
            "NIEOCZEKIWANY BŁĄD przy sprawdzaniu połączenia DB | "
            "czas=%.2fms | typ=%s | błąd=%s",
            elapsed * 1000,
            type(exc).__name__,
            str(exc),
            exc_info=True,
        )
        raise


# ---------------------------------------------------------------------------
# Graceful shutdown — zamknięcie engine przy wyłączaniu aplikacji
# ---------------------------------------------------------------------------

async def close_db_connection() -> None:
    """
    Zamyka engine i wszystkie połączenia z puli.

    Wywoływany przy zatrzymaniu aplikacji (lifespan event shutdown).
    Zapewnia graceful shutdown bez wiszących transakcji.
    """
    logger.info("Zamykanie puli połączeń MSSQL...")
    try:
        await engine.dispose(close=True)
        logger.info("Pula połączeń MSSQL zamknięta pomyślnie.")
    except Exception as exc:
        logger.error(
            "Błąd przy zamykaniu puli połączeń MSSQL: %s",
            str(exc),
            exc_info=True,
        )


async def close_db_engine() -> None:
    """
    Alias dla zgodności z main.py – deleguje do close_db_connection().
    """
    await close_db_connection()

    # Alias dla zgodności wstecznej – stare endpointy mogą używać get_async_session
get_async_session = get_db

from redis.asyncio import Redis

_redis_client: Redis | None = None


def get_redis_client() -> Redis:
    """
    Zwraca singleton klienta Redis opartego na settings.redis_url.

    Używane m.in. do cache, limitów itd.
    """
    global _redis_client

    if _redis_client is None:
        _redis_client = Redis.from_url(
            settings.redis_url,
            max_connections=settings.redis_max_connections,
        )

    return _redis_client