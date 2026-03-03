# =============================================================================
# backend/alembic/env.py
# Konfiguracja środowiska Alembic — projekt Windykacja
#
# ARCHITEKTURA:
#   - Async engine (aioodbc) — wymagany przez SQLAlchemy async
#   - Dwa tryby:
#       offline → generuje SQL bez połączenia z DB (alembic upgrade --sql)
#       online  → wykonuje migracje bezpośrednio przez async engine
#   - include_schemas=True → śledzi schemat dbo_ext
#   - include_object()     → wyklucza tabele WAPRO (schemat dbo i inne)
#   - version_table_schema → tabela alembic_version w dbo_ext
#
# WYMAGANE ZMIENNE ŚRODOWISKOWE:
#   DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
#   Opcjonalne: ODBC_DRIVER (domyślnie: ODBC Driver 18 for SQL Server)
#
# MODELE (skw_ prefix — wszystkie tabele systemu Windykacja):
#   skw_Roles, skw_Permissions, skw_RolePermissions
#   skw_Users, skw_RefreshTokens, skw_OtpCodes
#   skw_AuditLog, skw_MasterAccessLog
#   skw_Templates, skw_MonitHistory
#   skw_SystemConfig, skw_SchemaChecksums
#   skw_Comments
#
# WAŻNE: jeśli tabele tworzone są ręcznie przez DDL (sqlcmd),
#   po wykonaniu DDL uruchom: alembic stamp head
#   Dzięki temu Alembic wie, że schema jest aktualna.
#
# Wersja: 2.0.0 | Data: 2026-03-02
# =============================================================================

from __future__ import annotations

import asyncio
import logging
import os
import sys
from logging.config import fileConfig
from urllib.parse import quote_plus

from alembic import context
from sqlalchemy import pool, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

# ---------------------------------------------------------------------------
# Importuj Base i WSZYSTKIE modele — Alembic musi widzieć każdy model
# dla poprawnego autogenerate. Jeśli model nie jest zaimportowany,
# Alembic po cichu pominie tę tabelę przy generowaniu migracji.
#
# KOLEJNOŚĆ IMPORTU:
#   1. Base (metadane) — zawsze pierwszy
#   2. Modele bez FK
#   3. User — centralny model
#   4. Modele zależne od User
#   5. Modele zależne od wielu tabel
# ---------------------------------------------------------------------------
try:
    # ── 1. Base ──────────────────────────────────────────────────────────────
    from app.db.models.base import Base  # noqa: F401

    # ── 2. Modele bez FK (lub tylko wewnętrzne) ───────────────────────────────
    from app.db.models.role import Role  # noqa: F401
    from app.db.models.permission import Permission  # noqa: F401
    from app.db.models.system_config import SystemConfig  # noqa: F401
    from app.db.models.schema_checksums import SchemaChecksums  # noqa: F401
    from app.db.models.template import Template  # noqa: F401

    # ── 3. User — centralny (FK → Role) ──────────────────────────────────────
    from app.db.models.user import User  # noqa: F401

    # ── 4. Modele zależne od User ─────────────────────────────────────────────
    from app.db.models.refresh_token import RefreshToken  # noqa: F401
    from app.db.models.otp_code import OtpCode  # noqa: F401
    from app.db.models.audit_log import AuditLog  # noqa: F401
    from app.db.models.master_access_log import MasterAccessLog  # noqa: F401

    # ── 5. Modele zależne od wielu tabel ──────────────────────────────────────
    from app.db.models.role_permission import RolePermission  # noqa: F401
    from app.db.models.monit_history import MonitHistory  # noqa: F401
    from app.db.models.comment import Comment  # noqa: F401

except ImportError as exc:
    # Nie pozwól Alembicowi startować bez kompletnych modeli.
    # Brakujący model = brakująca migracja = niedostrzegalny problem w prod.
    print(
        f"\n[ALEMBIC ENV] KRYTYCZNY BŁĄD IMPORTU MODELI:\n"
        f"  {exc}\n\n"
        f"  Sprawdź czy:\n"
        f"    1. Uruchamiasz alembic z katalogu backend/\n"
        f"    2. prepend_sys_path = . jest ustawione w alembic.ini\n"
        f"    3. Plik modelu istnieje i nie ma błędów składni\n"
        f"    4. Virtualenv jest aktywowany\n",
        file=sys.stderr,
    )
    raise SystemExit(1) from exc


# ---------------------------------------------------------------------------
# Konfiguracja Alembic z pliku alembic.ini
# ---------------------------------------------------------------------------
config = context.config

# Załaduj konfigurację logowania z alembic.ini ([loggers], [handlers] itp.)
# fileConfig konfiguruje standardowy logging.Logger dla całego procesu Alembic.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

logger = logging.getLogger("alembic.env")

# Metadane SQLAlchemy — wszystkie modele są już zaimportowane powyżej
target_metadata = Base.metadata

# Schemat dla tabeli wersji Alembic (alembic_version)
# Musi być w dbo_ext — tam mamy PEŁNY CRUD
_VERSION_TABLE_SCHEMA = "dbo"

# Monitorowany schemat — tylko dbo_ext śledzony przez ORM
_TRACKED_SCHEMA = "dbo_ext"


# =============================================================================
# BUDOWANIE URL POŁĄCZENIA
# =============================================================================

def _require_env(key: str) -> str:
    """Pobiera zmienną środowiskową lub rzuca czytelny błąd."""
    value = os.environ.get(key)
    if not value:
        logger.critical(
            "Brak wymaganej zmiennej środowiskowej: %s\n"
            "Ustaw ją w pliku .env lub docker-compose.yml",
            key,
        )
        raise SystemExit(f"[ALEMBIC ENV] Brak zmiennej: {key}")
    return value


def build_connection_url() -> str:
    """
    Buduje URL połączenia dla SQLAlchemy async z MSSQL przez aioodbc.

    Format: mssql+aioodbc:///?odbc_connect=<URL-encoded ODBC string>

    UWAGA: pyodbc jest SYNCHRONICZNY — do pracy async wymagany aioodbc.
      pip install aioodbc  (jest już w requirements.txt)

    Środowisko:
      DB_HOST      → adres serwera MSSQL  (wymagane)
      DB_PORT      → port MSSQL           (domyślnie: 1433)
      DB_NAME      → nazwa bazy           (wymagane)
      DB_USER      → login SQL            (wymagane)
      DB_PASSWORD  → hasło SQL            (wymagane)
      ODBC_DRIVER  → sterownik ODBC       (domyślnie: ODBC Driver 18 for SQL Server)

    Returns:
        str: Pełny URL SQLAlchemy gotowy do użycia.

    Raises:
        SystemExit: Gdy brak wymaganej zmiennej środowiskowej.
    """
    db_host    = _require_env("DB_HOST")
    db_port    = os.environ.get("DB_PORT", "1433")
    db_name    = _require_env("DB_NAME")
    db_user    = _require_env("DB_USER")
    db_password = _require_env("DB_PASSWORD")
    odbc_driver = os.environ.get(
        "ODBC_DRIVER",
        "ODBC Driver 18 for SQL Server",
    )

    odbc_str = (
        f"DRIVER={{{odbc_driver}}};"
        f"SERVER={db_host},{db_port};"
        f"DATABASE={db_name};"
        f"UID={db_user};"
        f"PWD={db_password};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=yes;"
        # MSSQL specyfika async — wymagane przez aioodbc
        f"MARS_Connection=yes;"
    )

    url = f"mssql+aioodbc:///?odbc_connect={quote_plus(odbc_str)}"

    # Loguj bez hasła (maskuj DB_PASSWORD)
    masked_odbc = (
        f"DRIVER={{{odbc_driver}}};"
        f"SERVER={db_host},{db_port};"
        f"DATABASE={db_name};"
        f"UID={db_user};"
        f"PWD=***MASKED***;"
        f"TrustServerCertificate=yes;"
        f"Encrypt=yes;"
        f"MARS_Connection=yes;"
    )
    logger.debug(
        "Zbudowano URL połączenia | odbc=%s",
        masked_odbc,
    )

    return url


# =============================================================================
# FILTR OBIEKTÓW — co Alembic ma śledzić
# =============================================================================

def include_object(
    object,   # noqa: A002  (shadow builtin — wymagane przez Alembic API)
    name: str,
    type_: str,
    reflected: bool,
    compare_to: object,
) -> bool:
    """
    Filtr autogenerate — decyduje co Alembic śledzi.

    Reguły:
      ŚLEDŹ:
        → Tabele w schemacie dbo_ext (tabele skw_*)
      IGNORUJ:
        → Tabele WAPRO (schemat dbo i inne)
        → Indeksy    — zarządzamy ręcznie w plikach DDL .sql
        → Triggery   — zarządzamy ręcznie w 014_triggers_updated_at.sql
        → Widoki     — zarządzamy ręcznie w database/views/

    Args:
        object:     Obiekt SQLAlchemy (Table, Index itp.)
        name:       Nazwa obiektu
        type_:      Typ: 'table', 'column', 'index', 'unique_constraint' itp.
        reflected:  True jeśli obiekt odczytany z DB (nie z modelu)
        compare_to: Drugi obiekt do porównania (może być None)

    Returns:
        bool: True = śledź, False = ignoruj
    """
    if type_ == "table":
        schema = getattr(object, "schema", None)
        tracked = schema == _TRACKED_SCHEMA

        if not tracked:
            logger.debug(
                "Pomijam tabelę poza śledzonym schematem | tabela=%s | schemat=%s",
                name,
                schema,
            )
        else:
            logger.debug(
                "Śledzę tabelę | tabela=%s | schemat=%s",
                name,
                schema,
            )

        return tracked

    if type_ == "index":
        # Indeksy zarządzamy w plikach DDL (IX_skw_* w 001-013)
        # Alembic nie powinien ich tykać
        return False

    # Wszystko inne (kolumny, FK, constrainty) — śledź
    return True


# =============================================================================
# TRYB OFFLINE — generowanie SQL bez połączenia z DB
# (używany przez: alembic upgrade --sql  lub  alembic downgrade --sql)
# =============================================================================

def run_migrations_offline() -> None:
    """
    Generuje SQL migracji bez aktywnego połączenia z bazą.

    Użyteczne gdy chcesz przejrzeć SQL przed wykonaniem,
    lub gdy środowisko docelowe nie ma bezpośredniego połączenia.

    Wyjście trafia na stdout (można przekierować do pliku):
      alembic upgrade head --sql > migration.sql
    """
    url = build_connection_url()

    logger.info(
        "Tryb OFFLINE | target_schema=%s | version_table_schema=%s",
        _TRACKED_SCHEMA,
        _VERSION_TABLE_SCHEMA,
    )

    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        # Śledź schemat dbo_ext
        include_schemas=True,
        include_object=include_object,
        # Wykrywaj zmiany typów kolumn (np. NVARCHAR(50) → NVARCHAR(100))
        compare_type=True,
        # Wykrywaj zmiany wartości domyślnych (DEFAULT)
        compare_server_default=True,
        # Tabela wersji Alembic w dbo_ext
        version_table="alembic_version",
        version_table_schema=_VERSION_TABLE_SCHEMA,
    )

    with context.begin_transaction():
        context.run_migrations()


# =============================================================================
# TRYB ONLINE — wykonywanie migracji przez async engine
# =============================================================================

def _configure_context_for_migration(connection: Connection) -> None:
    """
    Konfiguruje kontekst Alembic dla aktywnego połączenia.

    Wydzielona funkcja — wywoływana zarówno przez tryb async online,
    jak i potencjalnie przez tryb sync (testy).
    """
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        # Śledź schemat dbo_ext
        include_schemas=True,
        include_object=include_object,
        # Wykrywaj zmiany typów i wartości domyślnych
        compare_type=True,
        compare_server_default=True,
        # Tabela wersji Alembic w schemacie dbo_ext
        version_table="alembic_version",
        version_table_schema=_VERSION_TABLE_SCHEMA,
        # MSSQL specyfika: transakcja DDL (domyślnie True dla MSSQL)
        # Gwarantuje atomowość każdej migracji
        transaction_per_migration=False,
    )


async def _verify_connection(connection: Connection) -> None:
    """
    Weryfikuje połączenie z bazą danych przed uruchomieniem migracji.

    ZAKRES WERYFIKACJI:
      ✅ Dostępność bazy danych (SELECT 1 + DB_NAME + @@VERSION)
      ✅ Informacja o istnieniu schematu dbo_ext (log INFO/WARNING)
      ❌ NIE blokuje startu gdy dbo_ext nie istnieje —
         schemat tworzy migracja 0001_skw_initial_schema.

    KIEDY BLOKUJE (raise SystemExit):
      - Baza danych jest całkowicie niedostępna (connection error)
      - Zapytanie weryfikacyjne nie zwróciło wyników

    Args:
        connection: Aktywne połączenie SQLAlchemy async.

    Raises:
        SystemExit: Wyłącznie gdy baza jest niedostępna.
    """
    try:
        # ── Krok 1: Podstawowa weryfikacja połączenia ─────────────────────────
        result = await connection.execute(
            text(
                "SELECT "
                "  1                    AS probe, "
                "  DB_NAME()            AS db_name, "
                "  SCHEMA_ID(:schema)   AS schema_id, "
                "  SUBSTRING(@@VERSION, 1, 80) AS sql_version"
            ),
            {"schema": _TRACKED_SCHEMA},
        )
        row = result.mappings().first()

        if row is None:
            # Zapytanie poszło ale nic nie zwróciło — bardzo dziwne
            raise RuntimeError(
                "Zapytanie weryfikacyjne SELECT 1 nie zwróciło wierszy. "
                "Sprawdź uprawnienia użytkownika DB."
            )

        db_name    = row["db_name"]
        schema_id  = row["schema_id"]
        sql_ver    = row["sql_version"]

        logger.info(
            "Połączenie z bazą OK | db=%s | sql_server=%s",
            db_name,
            sql_ver.strip() if sql_ver else "?",
        )

        # ── Krok 2: Informacja o schemacie (NIE blokuje) ─────────────────────
        if schema_id is not None:
            logger.info(
                "Schemat '%s' ISTNIEJE w bazie '%s' (schema_id=%s) — "
                "migracje będą aktualizować istniejące tabele.",
                _TRACKED_SCHEMA,
                db_name,
                schema_id,
            )
        else:
            # Schemat nie istnieje — to normalne przy pierwszym uruchomieniu.
            # Migracja 0001 go stworzy. NIE przerywamy.
            logger.warning(
                "Schemat '%s' NIE ISTNIEJE w bazie '%s'. "
                "Migracja 0001_skw_initial_schema utworzy go automatycznie. "
                "To jest oczekiwane przy pierwszym uruchomieniu systemu.",
                _TRACKED_SCHEMA,
                db_name,
            )
            # Drukuj też na stdout żeby było widać w docker logs
            print(
                f"[ALEMBIC ENV] INFO: Schemat {_TRACKED_SCHEMA} nie istnieje — "
                f"zostanie utworzony przez migrację 0001.",
                flush=True,
            )

    except SystemExit:
        # Przepuść SystemExit bez modyfikacji (własny raise z tej funkcji)
        raise

    except Exception as exc:
        # Każdy inny błąd = baza niedostępna lub problem z połączeniem
        logger.critical(
            "Nie można zweryfikować połączenia z bazą danych: %s | "
            "Sprawdź DB_HOST=%s DB_PORT=%s DB_NAME=%s DB_USER=%s",
            exc,
            os.environ.get("DB_HOST", "?"),
            os.environ.get("DB_PORT", "?"),
            os.environ.get("DB_NAME", "?"),
            os.environ.get("DB_USER", "?"),
            exc_info=True,
        )
        raise SystemExit(
            f"[ALEMBIC ENV] Baza danych niedostępna: {exc}"
        ) from exc

async def _run_migrations_online() -> None:
    """
    Uruchamia migracje przez async engine (aioodbc).

    Sekwencja:
      1. Zbuduj URL z env vars
      2. Utwórz async engine (NullPool — brak poolingu w migracjach)
      3. Otwórz połączenie
      4. Zweryfikuj bazę i schemat
      5. Skonfiguruj kontekst Alembic
      6. Wykonaj migracje w transakcji
      7. Zamknij engine
    """
    logger.info(
        "Tryb ONLINE | target_schema=%s | version_table_schema=%s",
        _TRACKED_SCHEMA,
        _VERSION_TABLE_SCHEMA,
    )

    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = build_connection_url()

    # NullPool — migracje jednorazowe, pooling niepotrzebny i szkodliwy
    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    try:
        async with connectable.connect() as connection:
            # MSSQL + SQLAlchemy 2.0: wyłącz autobegin żeby nie
            # kolidował z zarządzaniem transakcjami przez Alembic
            await connection.execute(text("SET XACT_ABORT OFF"))

            # Weryfikacja połączenia i schematu przed migracjami
            await _verify_connection(connection)

            # Konfiguracja kontekstu Alembic
            await connection.run_sync(_configure_context_for_migration)

            # Wykonaj migracje
            logger.info("Uruchamianie migracji...")
            await connection.run_sync(lambda conn: context.run_migrations())

            # KRYTYCZNE dla MSSQL + async: jawny commit
            # Bez tego SQLAlchemy 2.0 robi rollback całego DDL przy zamknięciu
            await connection.commit()
            logger.info("Migracje zakończone pomyślnie — COMMIT wykonany.")

    except Exception as exc:
        if isinstance(exc, SystemExit):
            raise
        logger.critical(
            "Błąd podczas wykonywania migracji: %s",
            exc,
            exc_info=True,
        )
        raise
    finally:
        # Zawsze zamknij engine — nawet przy błędzie
        await connectable.dispose()
        logger.debug("Engine zamknięty.")


def run_migrations_online() -> None:
    """Entry point dla trybu online — uruchamia async event loop."""
    try:
        asyncio.run(_run_migrations_online())
    except KeyboardInterrupt:
        logger.warning("Migracje przerwane przez użytkownika (Ctrl+C)")
        raise SystemExit(130)


# =============================================================================
# ENTRY POINT — decyzja offline / online
# =============================================================================

logger.debug(
    "Alembic env.py załadowany | offline_mode=%s | models_count=%d",
    context.is_offline_mode(),
    len(target_metadata.tables),
)

if context.is_offline_mode():
    logger.info("Uruchamianie migracji w trybie OFFLINE (generowanie SQL)")
    run_migrations_offline()
else:
    logger.info("Uruchamianie migracji w trybie ONLINE (async aioodbc)")
    run_migrations_online()