"""0019_add_orphaned_status

Dodaje wartość 'orphaned' do CHECK constraint CHK_sfa_status_wewnetrzny
w tabeli dbo_ext.skw_faktura_akceptacja.

POWÓD:
    ET-01 (_handle_orphan_if_needed) ustawia status='orphaned' gdy faktura
    zniknie z widoku WAPRO. Constraint z migracji 0006 nie zawierał tej wartości
    → IntegrityError przy pierwszym GET szczegółów faktury orphaned.

    StatusWewnetrzny.ORPHANED = "orphaned" istnieje w schemacie Pydantic
    od początku — brakuje tylko po stronie DB.

Revision ID: 0019
Revises:     0018
Create Date: 2026-04-22
"""

from __future__ import annotations

import logging
from alembic import op

revision:      str = "0019"
down_revision: str = "0018"
branch_labels       = None
depends_on          = None

logger = logging.getLogger(f"alembic.migration.{revision}")

SCHEMA = "dbo_ext"
TABLE  = "skw_faktura_akceptacja"
CK_OLD = "CHK_sfa_status_wewnetrzny"
CK_NEW = "CHK_sfa_status_wewnetrzny"


def upgrade() -> None:
    logger.info("[%s] UPGRADE — dodanie 'orphaned' do %s", revision, CK_OLD)

    op.execute(f"""
        IF EXISTS (
            SELECT 1
            FROM sys.check_constraints cc
            JOIN sys.tables  t ON cc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name  = N'{SCHEMA}'
              AND t.name  = N'{TABLE}'
              AND cc.name = N'{CK_OLD}'
              AND cc.definition NOT LIKE N'%orphaned%'
        )
        BEGIN
            ALTER TABLE [{SCHEMA}].[{TABLE}]
                DROP CONSTRAINT [{CK_OLD}];

            ALTER TABLE [{SCHEMA}].[{TABLE}]
                ADD CONSTRAINT [{CK_NEW}] CHECK (
                    status_wewnetrzny IN (
                        N'nowe',
                        N'w_toku',
                        N'zaakceptowana',
                        N'anulowana',
                        N'orphaned'
                    )
                );

            PRINT N'[0019] CHK_sfa_status_wewnetrzny zaktualizowany — orphaned dodany.';
        END
        ELSE
        BEGIN
            PRINT N'[0019] Constraint już zawiera orphaned — pomijam.';
        END
    """)

    logger.info("[%s] UPGRADE OK", revision)


def downgrade() -> None:
    logger.warning("[%s] DOWNGRADE — usunięcie 'orphaned' z %s", revision, CK_OLD)

    op.execute(f"""
        IF EXISTS (
            SELECT 1
            FROM sys.check_constraints cc
            JOIN sys.tables  t ON cc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name  = N'{SCHEMA}'
              AND t.name  = N'{TABLE}'
              AND cc.name = N'{CK_OLD}'
              AND cc.definition LIKE N'%orphaned%'
        )
        BEGIN
            ALTER TABLE [{SCHEMA}].[{TABLE}]
                DROP CONSTRAINT [{CK_OLD}];

            ALTER TABLE [{SCHEMA}].[{TABLE}]
                ADD CONSTRAINT [{CK_NEW}] CHECK (
                    status_wewnetrzny IN (
                        N'nowe',
                        N'w_toku',
                        N'zaakceptowana',
                        N'anulowana'
                    )
                );

            PRINT N'[0019] CHK_sfa_status_wewnetrzny przywrócony — orphaned usunięty.';
        END
    """)

    logger.warning("[%s] DOWNGRADE OK", revision)