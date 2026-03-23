# =============================================================================
# backend/alembic/versions/0003_add_monit_history_invoices.py
#
# ZASADA: Zero conn.execute() — całość przez op.execute() z IF NOT EXISTS w SQL.
# Dzięki temu Alembic ma pełną kontrolę nad transakcją (MSSQL transactional DDL).
#
# Poprzednia: 0002_update_view_skw_rozrachunki_faktur
# Następna:   0004_update_views_dniPo
# =============================================================================

from __future__ import annotations

import logging
from datetime import datetime

import sqlalchemy as sa
from alembic import op

logger = logging.getLogger("alembic.migration.0003")

revision      = "0003"
down_revision = "0002"
branch_labels = None
depends_on    = None


def upgrade() -> None:
    logger.info("0003 upgrade: START")

    # ------------------------------------------------------------------
    # 1. Tabela — cały IF NOT EXISTS w jednym op.execute()
    # ------------------------------------------------------------------
    logger.info("0003: CREATE TABLE IF NOT EXISTS skw_MonitHistory_Invoices")
    op.execute(sa.text("""
        IF NOT EXISTS (
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'dbo_ext'
              AND TABLE_NAME   = 'skw_MonitHistory_Invoices'
        )
        BEGIN
            CREATE TABLE [dbo_ext].[skw_MonitHistory_Invoices] (
                [ID_MONIT_INVOICE]  BIGINT    IDENTITY(1,1) NOT NULL,
                [ID_MONIT]          BIGINT                  NOT NULL,
                [ID_ROZRACHUNKU]    INT                     NOT NULL,
                [CreatedAt]         DATETIME                NOT NULL
                    CONSTRAINT [DF_skw_MonitHistory_Invoices_CreatedAt]
                    DEFAULT GETDATE(),
                CONSTRAINT [PK_skw_MonitHistory_Invoices]
                    PRIMARY KEY CLUSTERED ([ID_MONIT_INVOICE] ASC),
                CONSTRAINT [FK_skw_MonitHistory_Invoices_skw_MonitHistory]
                    FOREIGN KEY ([ID_MONIT])
                    REFERENCES [dbo_ext].[skw_MonitHistory]([ID_MONIT])
                    ON DELETE CASCADE
            )
            PRINT '0003: CREATE TABLE OK'
        END
        ELSE
            PRINT '0003: tabela juz istnieje - pomijam'
    """))
    logger.info("0003: tabela OK")

    # ------------------------------------------------------------------
    # 2. Indeksy — każdy IF NOT EXISTS
    # ------------------------------------------------------------------
    op.execute(sa.text("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes si
            JOIN sys.objects  so ON si.object_id = so.object_id
            JOIN sys.schemas  ss ON so.schema_id = ss.schema_id
            WHERE ss.name = 'dbo_ext'
              AND so.name = 'skw_MonitHistory_Invoices'
              AND si.name = 'IX_skw_MonitHistory_Invoices_ID_MONIT'
        )
        CREATE INDEX [IX_skw_MonitHistory_Invoices_ID_MONIT]
            ON [dbo_ext].[skw_MonitHistory_Invoices] ([ID_MONIT] ASC)
    """))

    op.execute(sa.text("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes si
            JOIN sys.objects  so ON si.object_id = so.object_id
            JOIN sys.schemas  ss ON so.schema_id = ss.schema_id
            WHERE ss.name = 'dbo_ext'
              AND so.name = 'skw_MonitHistory_Invoices'
              AND si.name = 'IX_skw_MonitHistory_Invoices_ID_ROZRACHUNKU'
        )
        CREATE INDEX [IX_skw_MonitHistory_Invoices_ID_ROZRACHUNKU]
            ON [dbo_ext].[skw_MonitHistory_Invoices] ([ID_ROZRACHUNKU] ASC)
    """))

    op.execute(sa.text("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes si
            JOIN sys.objects  so ON si.object_id = so.object_id
            JOIN sys.schemas  ss ON so.schema_id = ss.schema_id
            WHERE ss.name = 'dbo_ext'
              AND so.name = 'skw_MonitHistory_Invoices'
              AND si.name = 'IX_skw_MonitHistory_Invoices_ROZR_DATE'
        )
        CREATE INDEX [IX_skw_MonitHistory_Invoices_ROZR_DATE]
            ON [dbo_ext].[skw_MonitHistory_Invoices]
            ([ID_ROZRACHUNKU] ASC, [CreatedAt] DESC)
    """))

    logger.info("0003: indeksy OK")

    # ------------------------------------------------------------------
    # 3. Seed SystemConfig — IF NOT EXISTS per klucz
    # ------------------------------------------------------------------
    now = datetime.now().replace(tzinfo=None)

    op.execute(
        sa.text("""
            IF NOT EXISTS (
                SELECT 1 FROM [dbo_ext].[skw_SystemConfig]
                WHERE [ConfigKey] = 'monit.interval_days'
            )
            INSERT INTO [dbo_ext].[skw_SystemConfig]
                ([ConfigKey], [ConfigValue], [Description], [IsActive], [CreatedAt])
            VALUES (
                'monit.interval_days',
                '14',
                'Minimalny interwal miedzy monitami w dniach',
                1,
                :now
            )
        """).bindparams(now=now)
    )

    op.execute(
        sa.text("""
            IF NOT EXISTS (
                SELECT 1 FROM [dbo_ext].[skw_SystemConfig]
                WHERE [ConfigKey] = 'monit.block_mode'
            )
            INSERT INTO [dbo_ext].[skw_SystemConfig]
                ([ConfigKey], [ConfigValue], [Description], [IsActive], [CreatedAt])
            VALUES (
                'monit.block_mode',
                'block',
                'Tryb blokady wysylki monitow: block | warn',
                1,
                :now
            )
        """).bindparams(now=now)
    )

    op.execute(
        sa.text("""
            IF NOT EXISTS (
                SELECT 1 FROM [dbo_ext].[skw_SystemConfig]
                WHERE [ConfigKey] = 'monit.min_days_overdue'
            )
            INSERT INTO [dbo_ext].[skw_SystemConfig]
                ([ConfigKey], [ConfigValue], [Description], [IsActive], [CreatedAt])
            VALUES (
                'monit.min_days_overdue',
                '1',
                'Domyslny filtr GET /debtors - DniPo >= N',
                1,
                :now
            )
        """).bindparams(now=now)
    )

    logger.info("0003: SystemConfig seed OK")
    logger.info("0003 upgrade: ZAKOŃCZONY POMYŚLNIE")


def downgrade() -> None:
    logger.info("0003 downgrade: START")

    op.execute(sa.text("""
        DELETE FROM [dbo_ext].[skw_SystemConfig]
        WHERE [ConfigKey] IN (
            'monit.interval_days',
            'monit.block_mode',
            'monit.min_days_overdue'
        )
    """))

    op.execute(sa.text("""
        IF OBJECT_ID('dbo_ext.skw_MonitHistory_Invoices', 'U') IS NOT NULL
            DROP TABLE [dbo_ext].[skw_MonitHistory_Invoices]
    """))

    logger.info("0003 downgrade: ZAKOŃCZONY POMYŚLNIE")