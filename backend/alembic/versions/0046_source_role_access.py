# backend/alembic/versions/0046_source_role_access.py
"""0046 — skw_source_role_access: dwupoziomowa kontrola dostepu do zrodel (TODO-05)

Model zamkniety: nowe zrodlo domyslnie niewidoczne dla zwyklych uzytkownikow
dopoki admin nie przypisze roli. To swiadoma decyzja bezpieczenstwa.

Regula: uzytkownik widzi dokumenty ze zrodla X jesli:
  1. Ma role przypisana do zrodla X w skw_source_role_access, LUB
  2. Posiada uprawnienie approval.supervise lub documents.view_all

Cache Redis:
  source_roles:{id_source} TTL 300s — lista id_role z dostepem
  user_sources:{id_user}   TTL 300s — lista id_source dostepnych dla usera

Invalidacja cache przy POST/DELETE /sources/{id}/roles przez scan_iter.

Revision ID: 0046
Revises:     0045
Create Date: 2026-07-02
"""

from alembic import op
from sqlalchemy import text

revision      = "0046"
down_revision = "0045"
branch_labels = None
depends_on    = None

SCHEMA = "dbo"


def upgrade() -> None:
    # ── 1. Tabela skw_source_role_access ─────────────────────────────────────
    op.execute(text(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_source_role_access'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_source_role_access] (
                [id_source]   INT          NOT NULL,
                [id_role]     INT          NOT NULL,
                [created_at]  DATETIME2(7) NOT NULL
                              CONSTRAINT [DF_skw_sra_created_at] DEFAULT SYSUTCDATETIME(),
                [created_by]  INT          NULL,

                CONSTRAINT [PK_skw_source_role_access]
                    PRIMARY KEY CLUSTERED ([id_source] ASC, [id_role] ASC),

                CONSTRAINT [FK_skw_sra_source]
                    FOREIGN KEY ([id_source])
                    REFERENCES [{SCHEMA}].[skw_document_sources] ([id_source])
                    ON DELETE CASCADE ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_sra_role]
                    FOREIGN KEY ([id_role])
                    REFERENCES [{SCHEMA}].[skw_Roles] ([ID_ROLE])
                    ON DELETE CASCADE ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_sra_created_by]
                    FOREIGN KEY ([created_by])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE SET NULL ON UPDATE NO ACTION
            );
            PRINT N'[0046] Tabela skw_source_role_access utworzona.'
        END
        ELSE
            PRINT N'[0046] Tabela skw_source_role_access juz istnieje — pomijam.'
    """))

    # Indeks odwrotny — szybkie "jakie zrodla widzi ta rola?"
    op.execute(text(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_source_role_access]')
              AND name = N'IX_skw_sra_role'
        )
        CREATE NONCLUSTERED INDEX [IX_skw_sra_role]
            ON [{SCHEMA}].[skw_source_role_access] ([id_role], [id_source])
    """))

    # ── 2. Uprawnienia do zarzadzania dostepem do zrodel ─────────────────────
    op.execute(text(f"""
        MERGE [{SCHEMA}].[skw_Permissions] AS target
        USING (VALUES
            (N'sources.manage_access',
             N'Zarzadzanie dostepem rol do zrodel dokumentow (GET/POST/DELETE /sources/{{id}}/roles)',
             N'sources')
        ) AS source ([PermissionName], [Description], [Category])
        ON target.[PermissionName] = source.[PermissionName]
        WHEN NOT MATCHED THEN
            INSERT ([PermissionName], [Description], [Category], [IsActive])
            VALUES (source.[PermissionName], source.[Description], source.[Category], 1);
    """))

    op.execute(text(f"""
        INSERT INTO [{SCHEMA}].[skw_RolePermissions] ([ID_ROLE], [ID_PERMISSION])
        SELECT r.[ID_ROLE], p.[ID_PERMISSION]
        FROM [{SCHEMA}].[skw_Roles] r
        CROSS JOIN [{SCHEMA}].[skw_Permissions] p
        WHERE r.[RoleName] = N'admin'
          AND p.[PermissionName] = N'sources.manage_access'
          AND NOT EXISTS (
              SELECT 1 FROM [{SCHEMA}].[skw_RolePermissions] rp
              WHERE rp.[ID_ROLE] = r.[ID_ROLE]
                AND rp.[ID_PERMISSION] = p.[ID_PERMISSION]
          );
    """))


def downgrade() -> None:
    op.execute(text(f"""
        DELETE rp FROM [{SCHEMA}].[skw_RolePermissions] rp
        JOIN [{SCHEMA}].[skw_Permissions] p ON p.[ID_PERMISSION] = rp.[ID_PERMISSION]
        WHERE p.[PermissionName] = N'sources.manage_access';
    """))
    op.execute(text(f"""
        DELETE FROM [{SCHEMA}].[skw_Permissions]
        WHERE [PermissionName] = N'sources.manage_access';
    """))
    op.execute(text(f"""
        IF EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_source_role_access'
        )
        DROP TABLE [{SCHEMA}].[skw_source_role_access]
    """))