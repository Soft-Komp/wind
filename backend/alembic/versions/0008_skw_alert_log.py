"""0008_skw_alert_log

Migracja: tabela historii alertów systemowych + klucze SystemConfig.

Odpowiada plikom DDL:
  - database/ddl/022_skw_alert_log.sql     (DDL tabeli — dla świeżej instalacji)
  - database/seeds/11_alert_config.sql     (seed konfiguracji — dla świeżej instalacji)

Ta migracja jest ścieżką dla AKTUALIZACJI istniejącego systemu:
  ALEMBIC_MODE=upgrade → docker compose up → alembic upgrade 0008

Revision ID: 0008
Revises: 0007
Create Date: 2026-04-01
"""

from __future__ import annotations

import logging

from alembic import op

revision: str = "0008"
down_revision: str = "0007"
branch_labels = None
depends_on = None

logger = logging.getLogger(f"alembic.migration.{revision}")

SCHEMA = "dbo_ext"


def upgrade() -> None:
    logger.info("[0008] START upgrade — skw_AlertLog + alerts.* SystemConfig")

    # =========================================================================
    # TABELA: dbo_ext.skw_AlertLog
    # =========================================================================
    op.execute(f"""
    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = '{SCHEMA}' AND t.name = 'skw_AlertLog'
    )
    BEGIN
        CREATE TABLE [{SCHEMA}].[skw_AlertLog] (
            [ID]              BIGINT         NOT NULL IDENTITY(1,1),
            [AlertType]       NVARCHAR(100)  NOT NULL,
            [Level]           NVARCHAR(20)   NOT NULL,
            [Title]           NVARCHAR(500)  NOT NULL,
            [Message]         NVARCHAR(4000) NOT NULL,
            [Details]         NVARCHAR(MAX)  NULL,
            [EmailSent]       BIT            NOT NULL DEFAULT 0,
            [EmailRecipients] NVARCHAR(1000) NULL,
            [EmailError]      NVARCHAR(500)  NULL,
            [IsRecovery]      BIT            NOT NULL DEFAULT 0,
            [IncidentId]      NVARCHAR(36)   NOT NULL,
            [CheckedAt]       DATETIME       NOT NULL,
            [CreatedAt]       DATETIME       NOT NULL DEFAULT GETDATE(),
            CONSTRAINT [PK_skw_AlertLog]
                PRIMARY KEY CLUSTERED ([ID] ASC),
            CONSTRAINT [CK_skw_AlertLog_Level]
                CHECK ([Level] IN (N'INFO', N'WARNING', N'SECURITY', N'CRITICAL')),
            CONSTRAINT [CK_skw_AlertLog_AlertType]
                CHECK (LEN([AlertType]) > 0)
        )
    END
    """)

    # =========================================================================
    # INDEKSY
    # =========================================================================
    for idx_name, idx_sql in [
        (
            "IX_skw_AlertLog_AlertType",
            f"CREATE NONCLUSTERED INDEX [IX_skw_AlertLog_AlertType] "
            f"ON [{SCHEMA}].[skw_AlertLog] ([AlertType] ASC) "
            f"INCLUDE ([Level], [EmailSent], [CreatedAt])"
        ),
        (
            "IX_skw_AlertLog_Level",
            f"CREATE NONCLUSTERED INDEX [IX_skw_AlertLog_Level] "
            f"ON [{SCHEMA}].[skw_AlertLog] ([Level] ASC, [CreatedAt] DESC)"
        ),
        (
            "IX_skw_AlertLog_IncidentId",
            f"CREATE NONCLUSTERED INDEX [IX_skw_AlertLog_IncidentId] "
            f"ON [{SCHEMA}].[skw_AlertLog] ([IncidentId] ASC)"
        ),
        (
            "IX_skw_AlertLog_IsRecovery",
            f"CREATE NONCLUSTERED INDEX [IX_skw_AlertLog_IsRecovery] "
            f"ON [{SCHEMA}].[skw_AlertLog] ([IsRecovery] ASC, [CreatedAt] DESC)"
        ),
        (
            "IX_skw_AlertLog_CreatedAt",
            f"CREATE NONCLUSTERED INDEX [IX_skw_AlertLog_CreatedAt] "
            f"ON [{SCHEMA}].[skw_AlertLog] ([CreatedAt] DESC)"
        ),
    ]:
        op.execute(f"""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{idx_name}')
        BEGIN
            {idx_sql}
        END
        """)

    # =========================================================================
    # SEED: klucze SystemConfig dla alertmanagera (MERGE — idempotentne)
    # =========================================================================
    op.execute(f"""
    MERGE [{SCHEMA}].[skw_SystemConfig] AS target
    USING (VALUES
        (N'alerts.enabled',                        N'true',  N'Alert Manager: master switch.'),
        (N'alerts.recipients',                     N'',      N'Alert Manager: odbiorcy emaili, przecinkami.'),
        (N'alerts.cooldown_minutes',               N'15',    N'Alert Manager: cooldown między alertami tego samego typu (minuty).'),
        (N'alerts.brute_force_threshold',          N'10',    N'Alert Manager: próg błędów logowania dla alertu SECURITY.'),
        (N'alerts.worker_heartbeat_timeout_seconds', N'120', N'Alert Manager: timeout heartbeatu workera ARQ (sekundy).'),
        (N'alerts.db_latency_warn_ms',             N'500',   N'Alert Manager: próg latencji MSSQL dla alertu WARNING (ms).'),
        (N'alerts.dlq_overflow_threshold',         N'10',    N'Alert Manager: próg DLQ dla alertu WARNING.'),
        (N'alerts.snapshot_expected_hour',         N'3',     N'Alert Manager: godzina UTC oczekiwanego snapshotu dziennego.')
    ) AS source ([ConfigKey], [ConfigValue], [Description])
    ON target.[ConfigKey] = source.[ConfigKey]
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ([ConfigKey], [ConfigValue], [Description], [IsActive], [CreatedAt])
        VALUES (source.[ConfigKey], source.[ConfigValue], source.[Description], 1, GETDATE())
    WHEN MATCHED THEN
        UPDATE SET [Description] = source.[Description];
    """)

    logger.info("[0008] DONE upgrade — skw_AlertLog gotowy")


def downgrade() -> None:
    raise NotImplementedError(
        "Migracja 0008 jest nieodwracalna automatycznie. "
        "Aby cofnąć: DROP TABLE [dbo_ext].[skw_AlertLog] w SSMS + "
        "usuń klucze alerts.* z skw_SystemConfig. "
        "Dane historii alertów zostaną utracone."
    )