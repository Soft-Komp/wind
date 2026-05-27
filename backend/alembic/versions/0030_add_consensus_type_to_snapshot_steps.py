"""
0030_add_consensus_type_to_snapshot_steps
═════════════════════════════════════════
Dodaje brakującą kolumnę consensus_type do tabeli
skw_document_approval_snapshot_steps.

Kolumna była używana przez kod (can-act, accept) ale nie istniała
w DDL migracji 0028 — przeoczona przy tworzeniu tabeli.

Kroki:
  1. Dodaj kolumnę consensus_type NVARCHAR(10) NULL (idempotentne)
  2. Uzupełnij istniejące wiersze z skw_approval_groups
  3. (downgrade) Usuń kolumnę jeśli istnieje

Revision ID : 0030
Revises     : 0029
"""

import logging

from alembic import op

revision      = "0030"
down_revision = "0029"
branch_labels = None
depends_on    = None

SCHEMA = "dbo"
TABLE  = "skw_document_approval_snapshot_steps"

logger = logging.getLogger(f"alembic.migration.{revision}")


def upgrade() -> None:
    logger.info("0030 upgrade — dodawanie consensus_type do %s.%s", SCHEMA, TABLE)

    # Krok 1: dodaj kolumnę (idempotentne — IF NOT EXISTS)
    op.execute(f"""
        IF NOT EXISTS (
            SELECT 1
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = N'{SCHEMA}'
              AND TABLE_NAME   = N'{TABLE}'
              AND COLUMN_NAME  = N'consensus_type'
        )
        BEGIN
            ALTER TABLE [{SCHEMA}].[{TABLE}]
            ADD [consensus_type] NVARCHAR(10) NULL;
        END
    """)

    # Krok 2: uzupełnij istniejące wiersze na podstawie grupy
    op.execute(f"""
        UPDATE s
        SET s.[consensus_type] = g.[consensus_type]
        FROM [{SCHEMA}].[{TABLE}] s
        JOIN [{SCHEMA}].[skw_approval_groups] g
          ON g.[id_group] = s.[id_group]
        WHERE s.[consensus_type] IS NULL;
    """)

    logger.info("0030 upgrade — zakończono")


def downgrade() -> None:
    logger.info("0030 downgrade — usuwanie consensus_type z %s.%s", SCHEMA, TABLE)

    op.execute(f"""
        IF EXISTS (
            SELECT 1
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = N'{SCHEMA}'
              AND TABLE_NAME   = N'{TABLE}'
              AND COLUMN_NAME  = N'consensus_type'
        )
        BEGIN
            ALTER TABLE [{SCHEMA}].[{TABLE}]
            DROP COLUMN [consensus_type];
        END
    """)

    logger.info("0030 downgrade — zakończono")