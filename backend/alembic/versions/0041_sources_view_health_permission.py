# backend/alembic/versions/0041_sources_view_health_permission.py
"""0041 — Seed uprawnienia sources.view_health (F6, dashboard zdrowia zrodel)

Migracja 0039 zasiala 11 uprawnien sources.* (manage, view, sync, view_log,
manage_hooks, manage_actions, execute_action, test_connection,
toggle_test_mode, view_config, manage_ksef) ale NIE zasiala sources.view_health
— uprawnienie wymagane przez GET /admin/sources/health (F6).

Ta migracja dodaje WYLACZNIE to jedno, brakujace uprawnienie. Nie duplikuje
zadnego z uprawnien juz wstawionych przez 0039 (MERGE z WHEN NOT MATCHED
jest bezpieczny nawet gdyby ktos pomyslowo wpisal je rowniez tutaj, ale
celowo ograniczam zakres do minimum).

Przypisuje uprawnienie do roli 'admin'.

Revision ID: 0041
Revises:     0040
Create Date: 2026-06-30
"""

from alembic import op
from sqlalchemy import text

revision      = "0041"
down_revision = "0040"
branch_labels = None
depends_on    = None

SCHEMA = "dbo"


def upgrade() -> None:
    # ── 1. Seed jedynego brakujacego uprawnienia ─────────────────────────────
    # Kategoria 'sources' jest juz dozwolona w CK_skw_Permissions_Category
    # (rozszerzona przez migracje 0039 krok 11a) — brak potrzeby ALTER TABLE.
    op.execute(text(f"""
        MERGE [{SCHEMA}].[skw_Permissions] AS target
        USING (
            SELECT
                N'sources.view_health' AS PermissionName,
                N'Dashboard zdrowia wszystkich zrodel dokumentow (GET /admin/sources/health)' AS Description,
                N'sources' AS Category
        ) AS source
        ON target.[PermissionName] = source.[PermissionName]
        WHEN NOT MATCHED THEN
            INSERT ([PermissionName], [Description], [Category], [IsActive])
            VALUES (source.[PermissionName], source.[Description], source.[Category], 1);
    """))

    # ── 2. Przypisanie do roli 'admin' ────────────────────────────────────────
    op.execute(text(f"""
        INSERT INTO [{SCHEMA}].[skw_RolePermissions] ([ID_ROLE], [ID_PERMISSION])
        SELECT r.[ID_ROLE], p.[ID_PERMISSION]
        FROM [{SCHEMA}].[skw_Roles] r
        CROSS JOIN [{SCHEMA}].[skw_Permissions] p
        WHERE r.[RoleName] = N'admin'
          AND p.[PermissionName] = N'sources.view_health'
          AND NOT EXISTS (
              SELECT 1 FROM [{SCHEMA}].[skw_RolePermissions] rp
              WHERE rp.[ID_ROLE] = r.[ID_ROLE]
                AND rp.[ID_PERMISSION] = p.[ID_PERMISSION]
          );
    """))


def downgrade() -> None:
    op.execute(text(f"""
        DELETE rp
        FROM [{SCHEMA}].[skw_RolePermissions] rp
        JOIN [{SCHEMA}].[skw_Permissions] p ON p.[ID_PERMISSION] = rp.[ID_PERMISSION]
        WHERE p.[PermissionName] = N'sources.view_health';
    """))

    op.execute(text(f"""
        DELETE FROM [{SCHEMA}].[skw_Permissions]
        WHERE [PermissionName] = N'sources.view_health';
    """))