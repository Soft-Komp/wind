# backend/alembic/versions/0044_maintenance_mode_config.py
"""0044 — Seed maintenance_mode.enabled w skw_SystemConfig

Klucz maintenance_mode.enabled istnieje w database/seeds/05_system_config.sql
ale NIE w zadnej migracji Alembic — na instancjach gdzie seed nie byl
uruchamiany moze go brakować.

Ta migracja gwarantuje ze klucz istnieje na kazdej instancji.
MERGE WHEN NOT MATCHED — nie nadpisuje wartosci zmienionej przez admina.

Revision ID: 0044
Revises:     0043
Create Date: 2026-06-30
"""

from alembic import op
from sqlalchemy import text

revision      = "0044"
down_revision = "0043"
branch_labels = None
depends_on    = None

SCHEMA = "dbo"


def upgrade() -> None:
    op.execute(text(f"""
        MERGE [{SCHEMA}].[skw_SystemConfig] AS target
        USING (VALUES
            (N'maintenance_mode.enabled', N'false',
             N'Tryb serwisowy API. true = kazda odpowiedz JSON dostaje sekcje maintenance:{{}}. '
             + N'Mozna tez wlaczyc przez zmienna ENV MAINTENANCE_MODE=on (priorytet nad baza).'),
            (N'maintenance_mode.message',  N'',
             N'Opcjonalny komunikat serwisowy widoczny w sekcji maintenance (przyszle uzycie).')
        ) AS source ([ConfigKey], [ConfigValue], [Description])
        ON target.[ConfigKey] = source.[ConfigKey]
        WHEN NOT MATCHED THEN
            INSERT ([ConfigKey], [ConfigValue], [Description], [IsActive])
            VALUES (source.[ConfigKey], source.[ConfigValue], source.[Description], 1);
    """))


def downgrade() -> None:
    # Nie usuwamy — klucz moze byc uzywany przez inne czesci systemu
    pass