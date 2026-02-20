# app/init_db.py

import os
from sqlalchemy import create_engine, text

DB_NAME = os.getenv("DB_NAME", "Windykacja")
DB_USER = os.getenv("DB_USER", "sa")
DB_PASSWORD = os.getenv("DB_PASSWORD", "YourStrong!Passw0rd")
DB_HOST = os.getenv("DB_HOST", "mssql")
DB_PORT = os.getenv("DB_PORT", "1433")

# połączenie do master
MASTER_CONN_STR = (
    f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/master"
    "?driver=ODBC+Driver+18+for+SQL+Server"
    "&Encrypt=no"
    "&TrustServerCertificate=yes"
)

# połączenie do docelowej bazy
TARGET_CONN_STR = (
    f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    "?driver=ODBC+Driver+18+for+SQL+Server"
    "&Encrypt=no"
    "&TrustServerCertificate=yes"
)

master_engine = create_engine(MASTER_CONN_STR, isolation_level="AUTOCOMMIT")
target_engine = create_engine(TARGET_CONN_STR, isolation_level="AUTOCOMMIT")


def ensure_database() -> None:
    """Tworzy bazę DB_NAME, jeśli nie istnieje."""
    with master_engine.connect() as conn:
        conn.execute(
            text(
                f"IF DB_ID('{DB_NAME}') IS NULL "
                f"BEGIN "
                f"    CREATE DATABASE [{DB_NAME}]; "
                f"END"
            )
        )
        print(f"✅ Baza '{DB_NAME}' istnieje (utworzona lub już była).")


def ensure_schema() -> None:
    """Tworzy schema dbo_ext w bazie DB_NAME, jeśli nie istnieje."""
    with target_engine.connect() as conn:
        conn.execute(
            text(
                """
                IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dbo_ext')
                BEGIN
                    EXEC('CREATE SCHEMA dbo_ext');
                END
                """
            )
        )
        print("✅ Schemat 'dbo_ext' istnieje (utworzony lub już był).")


if __name__ == "__main__":
    ensure_database()
    ensure_schema()