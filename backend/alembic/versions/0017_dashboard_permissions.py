"""0017_dashboard_permissions

Dodaje kategorię 'dashboard' do CK_skw_Permissions_Category,
wstawia 3 uprawnienia i przypisuje je do ról.

Uprawnienia:
  - dashboard.view_debt_stats   → agregaty zadłużenia + top dłużnicy
  - dashboard.view_monit_stats  → statystyki monitów (kanały, trend)
  - dashboard.view_activity     → oś czasu aktywności

Przypisanie do ról:
  Admin    → wszystkie 3
  Manager  → wszystkie 3
  User     → tylko dashboard.view_activity (bez danych finansowych)
  ReadOnly → wszystkie 3

Revision ID: 0017
Revises:     0016
Create Date: 2026-04-15
"""

from __future__ import annotations

import logging
from typing import Final

from alembic import op

revision:      str = "0017"
down_revision: str = "0016"
branch_labels       = None
depends_on          = None

logger = logging.getLogger(f"alembic.migration.{revision}")

SCHEMA_EXT: Final[str] = "dbo_ext"

_PERMISSIONS: Final[list[tuple[str, str]]] = [
    (
        "dashboard.view_debt_stats",
        "Podgląd dashboardu: agregaty zadłużenia i top dłużnicy",
    ),
    (
        "dashboard.view_monit_stats",
        "Podgląd dashboardu: statystyki monitów (kanały, trend miesięczny)",
    ),
    (
        "dashboard.view_activity",
        "Podgląd dashboardu: oś czasu ostatniej aktywności",
    ),
]

_ROLE_PERMISSIONS: Final[dict[str, list[str]]] = {
    "Admin":    [
        "dashboard.view_debt_stats",
        "dashboard.view_monit_stats",
        "dashboard.view_activity",
    ],
    "Manager":  [
        "dashboard.view_debt_stats",
        "dashboard.view_monit_stats",
        "dashboard.view_activity",
    ],
    "User":     [
        "dashboard.view_activity",
    ],
    "ReadOnly": [
        "dashboard.view_debt_stats",
        "dashboard.view_monit_stats",
        "dashboard.view_activity",
    ],
}


# ===========================================================================
# UPGRADE
# ===========================================================================

def upgrade() -> None:
    logger.info("[%s] UPGRADE START", revision)

    # ── Krok 1: Rozszerz constraint o kategorię dashboard ─────────────────────
    logger.info("[%s] Krok 1/3 — rozszerzenie CK_skw_Permissions_Category", revision)
    op.execute("""
        IF NOT EXISTS (
            SELECT 1
            FROM sys.check_constraints cc
            JOIN sys.tables  t ON cc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id          = s.schema_id
            WHERE s.name  = N'dbo_ext'
              AND t.name  = N'skw_Permissions'
              AND cc.name = N'CK_skw_Permissions_Category'
              AND cc.definition LIKE N'%dashboard%'
        )
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM sys.check_constraints cc
                JOIN sys.tables  t ON cc.parent_object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id          = s.schema_id
                WHERE s.name  = N'dbo_ext'
                  AND t.name  = N'skw_Permissions'
                  AND cc.name = N'CK_skw_Permissions_Category'
            )
            BEGIN
                ALTER TABLE [dbo_ext].[skw_Permissions]
                    DROP CONSTRAINT [CK_skw_Permissions_Category];
                PRINT N'[0017] Stary constraint usunięty.';
            END

            ALTER TABLE [dbo_ext].[skw_Permissions]
                ADD CONSTRAINT [CK_skw_Permissions_Category] CHECK (
                    [Category] IN (
                        N'faktury',   N'templates', N'auth',       N'users',
                        N'roles',     N'permissions', N'debtors',  N'monits',
                        N'comments',  N'pdf',        N'reports',   N'snapshots',
                        N'audit',     N'system',     N'dashboard'
                    )
                );
            PRINT N'[0017] Nowy constraint z dashboard dodany.';
        END
        ELSE
        BEGIN
            PRINT N'[0017] Constraint juz zawiera dashboard — pomijam.';
        END
    """)
    logger.info("[%s] Krok 1/3 — OK", revision)

    # ── Krok 2: Wstaw uprawnienia (INSERT-only MERGE) ─────────────────────────
    logger.info("[%s] Krok 2/3 — INSERT uprawnienia dashboard.*", revision)
    for perm_name, description in _PERMISSIONS:
        op.execute(f"""
            MERGE [{SCHEMA_EXT}].[skw_Permissions] AS target
            USING (
                SELECT
                    N'{perm_name}'    AS PermissionName,
                    N'{description}'  AS Description,
                    N'dashboard'      AS Category
            ) AS source
                ON target.[PermissionName] = source.[PermissionName]
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ([PermissionName], [Description], [Category], [IsActive], [CreatedAt])
                VALUES (
                    source.[PermissionName],
                    source.[Description],
                    source.[Category],
                    1,
                    GETDATE()
                );
        """)
        logger.info("[%s]   INSERT: %s", revision, perm_name)
    logger.info("[%s] Krok 2/3 — OK", revision)

    # ── Krok 3: Przypisz do ról (INSERT-only MERGE) ───────────────────────────
    logger.info("[%s] Krok 3/3 — przypisanie uprawnień do ról", revision)
    for role_name, perms in _ROLE_PERMISSIONS.items():
        for perm_name in perms:
            op.execute(f"""
                MERGE [{SCHEMA_EXT}].[skw_RolePermissions] AS target
                USING (
                    SELECT r.[ID_ROLE], p.[ID_PERMISSION]
                    FROM   [{SCHEMA_EXT}].[skw_Roles]       r
                    CROSS JOIN [{SCHEMA_EXT}].[skw_Permissions] p
                    WHERE  r.[RoleName]       = N'{role_name}'
                      AND  p.[PermissionName] = N'{perm_name}'
                      AND  p.[IsActive]       = 1
                ) AS source
                    ON  target.[ID_ROLE]       = source.[ID_ROLE]
                    AND target.[ID_PERMISSION] = source.[ID_PERMISSION]
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ([ID_ROLE], [ID_PERMISSION], [CreatedAt])
                    VALUES (source.[ID_ROLE], source.[ID_PERMISSION], GETDATE());
            """)
            logger.info("[%s]   ASSIGN: %s → %s", revision, role_name, perm_name)
    logger.info("[%s] Krok 3/3 — OK", revision)

    logger.info(
        "[%s] UPGRADE zakończony — %d uprawnień, %d przypisań",
        revision,
        len(_PERMISSIONS),
        sum(len(p) for p in _ROLE_PERMISSIONS.values()),
    )


# ===========================================================================
# DOWNGRADE
# ===========================================================================

def downgrade() -> None:
    logger.warning("[%s] DOWNGRADE START — usuwanie uprawnień dashboard.*", revision)

    # Usuń przypisania (najpierw — FK constraint)
    op.execute(f"""
        DELETE rp
        FROM [{SCHEMA_EXT}].[skw_RolePermissions] rp
        INNER JOIN [{SCHEMA_EXT}].[skw_Permissions] p
            ON rp.[ID_PERMISSION] = p.[ID_PERMISSION]
        WHERE p.[PermissionName] LIKE N'dashboard.%';
    """)
    logger.warning("[%s] Przypisania dashboard.* usunięte", revision)

    # Usuń uprawnienia
    op.execute(f"""
        DELETE FROM [{SCHEMA_EXT}].[skw_Permissions]
        WHERE [PermissionName] LIKE N'dashboard.%';
    """)
    logger.warning("[%s] Uprawnienia dashboard.* usunięte", revision)

    # Cofnij constraint — usuń dashboard z listy dozwolonych
    op.execute("""
        IF EXISTS (
            SELECT 1
            FROM sys.check_constraints cc
            JOIN sys.tables  t ON cc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id          = s.schema_id
            WHERE s.name  = N'dbo_ext'
              AND t.name  = N'skw_Permissions'
              AND cc.name = N'CK_skw_Permissions_Category'
              AND cc.definition LIKE N'%dashboard%'
        )
        BEGIN
            ALTER TABLE [dbo_ext].[skw_Permissions]
                DROP CONSTRAINT [CK_skw_Permissions_Category];

            ALTER TABLE [dbo_ext].[skw_Permissions]
                ADD CONSTRAINT [CK_skw_Permissions_Category] CHECK (
                    [Category] IN (
                        N'faktury',   N'templates', N'auth',      N'users',
                        N'roles',     N'permissions', N'debtors', N'monits',
                        N'comments',  N'pdf',        N'reports',  N'snapshots',
                        N'audit',     N'system'
                    )
                );
            PRINT N'[0017] DOWNGRADE: usunięto dashboard z constraint.';
        END
    """)
    logger.warning("[%s] DOWNGRADE zakończony", revision)