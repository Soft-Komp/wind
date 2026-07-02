# backend/alembic/versions/0045_endpoint_registry.py
"""0045 — skw_EndpointRegistry: rejestr wlacznikow endpointow

Tabela przechowuje stan wlacznikow dla wszystkich endpointow API.
Endpointy sa rejestrowane automatycznie przy pierwszym wywolaniu
(lazy registration) lub przy starcie aplikacji (eager scan).

Domyslnie wszystkie endpointy sa wlaczone (is_enabled=1).
Wylaczenie jest zawsze reczna, swiadoma akcja admina.

Kolumny:
  endpoint_key   NVARCHAR(200) PK — klucz unikalny, format: "METHOD:/sciezka"
                 np. "GET:/documents/{id_instance}/status-summary"
  label          NVARCHAR(200) NULL — czytelna nazwa (z dekoratora @endpoint_toggle
                 lub NULL = uzyj endpoint_key jako etykiety)
  is_enabled     BIT NOT NULL DEFAULT 1
  disabled_by    INT NULL FK → skw_Users (kto wylaczyl)
  disabled_at    DATETIME2 NULL
  disabled_reason NVARCHAR(500) NULL — powod wylaczenia (wymagany przy off)
  created_at     DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
  updated_at     DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()

Revision ID: 0045
Revises:     0044
Create Date: 2026-06-30
"""

from alembic import op
from sqlalchemy import text

revision      = "0045"
down_revision = "0044"
branch_labels = None
depends_on    = None

SCHEMA = "dbo"


def upgrade() -> None:
    # ── 1. Tabela skw_EndpointRegistry ───────────────────────────────────────
    op.execute(text(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_EndpointRegistry'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_EndpointRegistry] (
                [endpoint_key]    NVARCHAR(200)  NOT NULL,
                [label]           NVARCHAR(200)  NULL,
                [is_enabled]      BIT            NOT NULL
                                  CONSTRAINT [DF_skw_er_is_enabled] DEFAULT 1,
                [disabled_by]     INT            NULL,
                [disabled_at]     DATETIME2(7)   NULL,
                [disabled_reason] NVARCHAR(500)  NULL,
                [created_at]      DATETIME2(7)   NOT NULL
                                  CONSTRAINT [DF_skw_er_created_at] DEFAULT SYSUTCDATETIME(),
                [updated_at]      DATETIME2(7)   NOT NULL
                                  CONSTRAINT [DF_skw_er_updated_at] DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_EndpointRegistry]
                    PRIMARY KEY CLUSTERED ([endpoint_key] ASC),

                CONSTRAINT [FK_skw_er_disabled_by]
                    FOREIGN KEY ([disabled_by])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE SET NULL ON UPDATE NO ACTION
            );
            PRINT N'[0045] Tabela skw_EndpointRegistry utworzona.'
        END
        ELSE
            PRINT N'[0045] Tabela skw_EndpointRegistry juz istnieje — pomijam.'
    """))

    # Indeks dla szybkiego sprawdzania is_enabled (hot path middleware)
    op.execute(text(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_EndpointRegistry]')
              AND name = N'IX_skw_er_enabled'
        )
        CREATE NONCLUSTERED INDEX [IX_skw_er_enabled]
            ON [{SCHEMA}].[skw_EndpointRegistry] ([is_enabled])
            INCLUDE ([endpoint_key], [label])
    """))

    # ── 2. Uprawnienie system.manage_endpoints ────────────────────────────────
    op.execute(text(f"""
        MERGE [{SCHEMA}].[skw_Permissions] AS target
        USING (
            SELECT
                N'system.manage_endpoints' AS PermissionName,
                N'Zarzadzanie wlacznikami endpointow API (wlacz/wylacz per instancja)' AS Description,
                N'system' AS Category
        ) AS source
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
          AND p.[PermissionName] = N'system.manage_endpoints'
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
        WHERE p.[PermissionName] = N'system.manage_endpoints';
    """))
    op.execute(text(f"""
        DELETE FROM [{SCHEMA}].[skw_Permissions]
        WHERE [PermissionName] = N'system.manage_endpoints';
    """))
    op.execute(text(f"""
        IF EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_EndpointRegistry'
        )
        DROP TABLE [{SCHEMA}].[skw_EndpointRegistry]
    """))