"""
Migracja  : 0007_add_faktury_category
Opis       : Rozszerzenie CHECK constraint CK_skw_Permissions_Category
             o kategorię 'faktury' (moduł Akceptacji Faktur KSeF).
             Idempotentna — sprawdza czy constraint już zawiera 'faktury'
             przed wykonaniem ALTER TABLE.
down_revision: 0006
revision     : 0007
"""

from __future__ import annotations

import logging

from alembic import op

logger = logging.getLogger(__name__)
revision       = "0007"
down_revision  = "0006"
branch_labels  = None
depends_on     = None


def upgrade() -> None:
    logger.info("[0007] START upgrade — rozszerzenie CK_skw_Permissions_Category")

    # ── Krok 1: Sprawdź czy 'faktury' już jest w constraint ──────────────────
    # Jeśli tak — pomiń (idempotentność)
    op.execute("""
        IF NOT EXISTS (
            SELECT 1
            FROM sys.check_constraints cc
            JOIN sys.tables t  ON cc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name  = N'dbo_ext'
              AND t.name  = N'skw_Permissions'
              AND cc.name = N'CK_skw_Permissions_Category'
              AND cc.definition LIKE N'%faktury%'
        )
        BEGIN
            -- ── Krok 2: Usuń stary constraint ────────────────────────────────
            IF EXISTS (
                SELECT 1
                FROM sys.check_constraints cc
                JOIN sys.tables t  ON cc.parent_object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name  = N'dbo_ext'
                  AND t.name  = N'skw_Permissions'
                  AND cc.name = N'CK_skw_Permissions_Category'
            )
            BEGIN
                ALTER TABLE [dbo_ext].[skw_Permissions]
                    DROP CONSTRAINT [CK_skw_Permissions_Category];
                PRINT N'[0007] Stary constraint CK_skw_Permissions_Category usunięty.';
            END

            -- ── Krok 3: Dodaj nowy constraint z faktury ───────────────────────
            ALTER TABLE [dbo_ext].[skw_Permissions]
                ADD CONSTRAINT [CK_skw_Permissions_Category] CHECK (
                    [Category] IN (
                        N'auth',        N'users',     N'roles',
                        N'permissions', N'debtors',   N'monits',
                        N'comments',    N'pdf',        N'reports',
                        N'snapshots',   N'audit',      N'system',
                        N'templates',   N'faktury'
                    )
                );
            PRINT N'[0007] Nowy constraint CK_skw_Permissions_Category dodany (z faktury).';
        END
        ELSE
        BEGIN
            PRINT N'[0007] Constraint już zawiera faktury — pomijam.';
        END
    """)

    logger.info("[0007] DONE upgrade — CK_skw_Permissions_Category zaktualizowany")


def downgrade() -> None:
    logger.info("[0007] START downgrade — usuwam faktury z CK_skw_Permissions_Category")

    op.execute("""
        IF EXISTS (
            SELECT 1
            FROM sys.check_constraints cc
            JOIN sys.tables t  ON cc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name  = N'dbo_ext'
              AND t.name  = N'skw_Permissions'
              AND cc.name = N'CK_skw_Permissions_Category'
        )
        BEGIN
            ALTER TABLE [dbo_ext].[skw_Permissions]
                DROP CONSTRAINT [CK_skw_Permissions_Category];

            ALTER TABLE [dbo_ext].[skw_Permissions]
                ADD CONSTRAINT [CK_skw_Permissions_Category] CHECK (
                    [Category] IN (
                        N'auth',        N'users',     N'roles',
                        N'permissions', N'debtors',   N'monits',
                        N'comments',    N'pdf',        N'reports',
                        N'snapshots',   N'audit',      N'system',
                        N'templates'
                    )
                );
            PRINT N'[0007] Downgrade: usunięto faktury z CK_skw_Permissions_Category.';
        END
    """)

    logger.info("[0007] DONE downgrade")