"""
Konfiguracja Alembic dla projektu windykacja.

Kluczowe ustawienia:
  - include_schemas=True   → śledzi obiekty w schemacie dbo_ext
  - include_object()       → wyklucza tabele WAPRO (schema != dbo_ext)
  - Async engine           → aioodbc + run_async_migrations()
  - compare_type=True      → wykrywa zmiany typów kolumn
"""

import asyncio
import logging
import os
from logging.config import fileConfig
from urllib.parse import quote_plus

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

# Importuj WSZYSTKIE modele — Alembic musi je "widzieć" dla autogenerate
from app.db.models import Base  # noqa: F401 — import wymagany dla metadanych

logger = logging.getLogger("alembic.env")

# ---------------------------------------------------------------------------
# Konfiguracja Alembic z pliku alembic.ini
# ---------------------------------------------------------------------------
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


# ---------------------------------------------------------------------------
# Budowanie connection string dla aioodbc (async MSSQL)
# ---------------------------------------------------------------------------

def build_connection_url() -> str:
    """
    Buduje URL połączenia dla SQLAlchemy async z MSSQL przez aioodbc.

    UWAGA: pyodbc jest SYNCHRONICZNY — do async potrzebny aioodbc.
    pip install aioodbc sqlalchemy[asyncio]
    """
    db_host = os.environ["DB_HOST"]
    db_port = os.environ.get("DB_PORT", "1433")
    db_name = os.environ["DB_NAME"]
    db_user = os.environ["DB_USER"]
    db_password = os.environ["DB_PASSWORD"]
    odbc_driver = os.environ.get("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

    odbc_str = (
        f"DRIVER={{{odbc_driver}}};"
        f"SERVER={db_host},{db_port};"
        f"DATABASE={db_name};"
        f"UID={db_user};"
        f"PWD={db_password};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=yes;"
    )

    return f"mssql+aioodbc:///?odbc_connect={quote_plus(odbc_str)}"


# ---------------------------------------------------------------------------
# Filtr obiektów — pomijamy wszystko spoza dbo_ext
# ---------------------------------------------------------------------------

def include_object(object, name, type_, reflected, compare_to) -> bool:
    """
    Alembic autogenerate filtr:
      - Śledź: tabele w schemacie dbo_ext
      - Ignoruj: tabele WAPRO (dbo i inne schematy)
      - Ignoruj: indeksy i triggery — zarządzane przez osobne pliki .sql
    """
    if type_ == "table":
        # Śledź tylko tabele w schemacie dbo_ext
        return object.schema == "dbo_ext"

    if type_ == "index":
        # Indeksy zarządzamy ręcznie w plikach .sql
        return False

    return True


# ---------------------------------------------------------------------------
# Migracje synchroniczne (Alembic offline — generowanie SQL bez połączenia)
# ---------------------------------------------------------------------------

def run_migrations_offline() -> None:
    """Generuje SQL migracji bez aktywnego połączenia z bazą."""
    url = build_connection_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        include_object=include_object,
        compare_type=True,
        compare_server_default=True,
        # MSSQL specyfika — pomijaj zmiany kolejności kolumn
        render_item=None,
    )

    with context.begin_transaction():
        context.run_migrations()


# ---------------------------------------------------------------------------
# Migracje asynchroniczne (online)
# ---------------------------------------------------------------------------

def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
        include_object=include_object,
        compare_type=True,
        compare_server_default=True,
        # Schemat dla tabeli wersji Alembic
        version_table_schema="dbo_ext",
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Uruchamia migracje przez async engine (aioodbc)."""
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = build_connection_url()

    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,  # Brak poolingu w migracjach
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if context.is_offline_mode():
    logger.info("Uruchamianie migracji w trybie offline (bez połączenia DB)")
    run_migrations_offline()
else:
    logger.info("Uruchamianie migracji w trybie online (async aioodbc)")
    run_migrations_online()