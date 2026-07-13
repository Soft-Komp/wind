# backend/alembic/versions/0048_koszty_jednorazowe_monitu.py
"""0048_koszty_jednorazowe_monitu

Nowa tabela skw_MonitKosztyJednorazowe — ręcznie wpisywane, jednorazowe
koszty dodatkowe przypisane do konkretnego monitu. Append-only (trigger
blokuje UPDATE/DELETE poza is_voided=1), analogicznie do skw_approval_log.

Revision ID: 0048
Revises:     0047
Create Date: 2026-07-06
"""
from __future__ import annotations
import logging
from typing import Final
from alembic import op
import sqlalchemy as sa

revision:      str = "0048"
down_revision: str = "0047"
branch_labels       = None
depends_on          = None

TABLE: Final[str] = "skw_MonitKosztyJednorazowe"
logger = logging.getLogger(f"alembic.migration.{revision}")

_CREATE_TABLE: Final[str] = f"""
IF OBJECT_ID(N'[dbo].[{TABLE}]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[{TABLE}] (
        [id_koszt]        INT IDENTITY(1,1) PRIMARY KEY,
        [id_monit]        BIGINT          NOT NULL,
        [opis]            NVARCHAR(200)   NOT NULL,
        [kwota]           DECIMAL(15,2)   NOT NULL CHECK ([kwota] > 0 AND [kwota] <= 100000),
        [id_user_dodal]   INT             NOT NULL,
        [ip_address]      NVARCHAR(45)    NULL,
        [request_id]      NVARCHAR(64)    NULL,
        [created_at]      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        [is_voided]       BIT             NOT NULL DEFAULT 0,
        [voided_at]       DATETIME2       NULL,
        [voided_by]       INT             NULL,
        [voided_reason]   NVARCHAR(200)   NULL,
        CONSTRAINT [FK_{TABLE}_MonitHistory]
            FOREIGN KEY ([id_monit]) REFERENCES [dbo].[skw_MonitHistory]([id_monit])
    );
    CREATE INDEX [IX_{TABLE}_id_monit] ON [dbo].[{TABLE}]([id_monit]);
END
"""

# Append-only: blokuje UPDATE/DELETE poza ustawieniem is_voided=1
_CREATE_TRIGGER: Final[str] = f"""
CREATE OR ALTER TRIGGER [dbo].[TRG_{TABLE}_AppendOnly]
ON [dbo].[{TABLE}]
INSTEAD OF UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (SELECT 1 FROM deleted)
       AND NOT EXISTS (
           SELECT 1 FROM inserted i
           JOIN deleted d ON d.id_koszt = i.id_koszt
           WHERE i.is_voided = 1 AND d.is_voided = 0
       )
    BEGIN
        RAISERROR(
            N'{TABLE} jest append-only — dozwolone wyłącznie ustawienie is_voided=1.',
            16, 1
        );
        RETURN;
    END

    UPDATE t SET
        is_voided     = i.is_voided,
        voided_at     = i.voided_at,
        voided_by     = i.voided_by,
        voided_reason = i.voided_reason
    FROM [dbo].[{TABLE}] t
    JOIN inserted i ON i.id_koszt = t.id_koszt;
END
"""

def upgrade() -> None:
    logger.info("[%s] UPGRADE — tworzę %s + trigger append-only", revision, TABLE)
    bind = op.get_bind()
    bind.execute(sa.text(_CREATE_TABLE))
    bind.execute(sa.text(_CREATE_TRIGGER))
    logger.info("[%s] UPGRADE OK", revision)

def downgrade() -> None:
    logger.warning("[%s] DOWNGRADE — usuwam trigger + tabelę %s", revision, TABLE)
    bind = op.get_bind()
    bind.execute(sa.text(f"DROP TRIGGER IF EXISTS [dbo].[TRG_{TABLE}_AppendOnly]"))
    bind.execute(sa.text(f"DROP TABLE IF EXISTS [dbo].[{TABLE}]"))
    logger.warning("[%s] DOWNGRADE OK", revision)