# backend/alembic/versions/0021_koszty_dodatkowe.py
"""0021_koszty_dodatkowe

Nowy moduł kosztów dodatkowych monitów.

════════════════════════════════════════════════════════════════
Kroki upgrade:
  1. CREATE TABLE dbo_ext.skw_KosztyDodatkowe
  2. ADD COLUMNS do skw_MonitHistory
       (OdsetkiTotal, KosztyDodatkoweTotal, KwotaCalkowita)
  3. Rozszerzenie CK_skw_Permissions_Category o 'koszty'
  4. MERGE uprawnień: koszty.* + monits.preview
  5. MERGE przypisań do ról

Revision ID: 0021
Revises:     0020
Create Date: 2026-04-24
"""

from __future__ import annotations

import logging
from typing import Final

from alembic import op

revision:      str = "0021"
down_revision: str = "0020"
branch_labels       = None
depends_on          = None

SCHEMA_EXT: Final[str] = "dbo_ext"

logger = logging.getLogger(f"alembic.migration.{revision}")

# ─────────────────────────────────────────────────────────────────────────────
# Uprawnienia nowego modułu
# ─────────────────────────────────────────────────────────────────────────────

_PERMISSIONS: Final[list[tuple[str, str, str]]] = [
    ("koszty.view_list",    "Lista kosztów dodatkowych monitów",         "koszty"),
    ("koszty.view_details", "Szczegóły kosztu dodatkowego",              "koszty"),
    ("koszty.create",       "Tworzenie nowego kosztu dodatkowego",       "koszty"),
    ("koszty.edit",         "Edycja istniejącego kosztu dodatkowego",    "koszty"),
    ("koszty.delete",       "Dezaktywacja kosztu dodatkowego",           "koszty"),
    ("monits.preview",      "Podgląd kalkulacji kosztu przed wysyłką",   "monits"),
]

_ROLE_PERMISSIONS: Final[dict[str, list[str]]] = {
    "Admin": [
        "koszty.view_list", "koszty.view_details",
        "koszty.create", "koszty.edit", "koszty.delete",
        "monits.preview",
    ],
    "Manager": [
        "koszty.view_list", "koszty.view_details",
        "koszty.create", "koszty.edit",
        "monits.preview",
    ],
    "User": [
        "koszty.view_list", "koszty.view_details",
        "monits.preview",
    ],
    "ReadOnly": [
        "koszty.view_list", "koszty.view_details",
    ],
}

# Pełna lista kategorii po tej migracji (do constraintu)
_KATEGORIE_PO: Final[str] = (
    "N'auth', N'users', N'roles', N'permissions', N'debtors', N'monits', "
    "N'comments', N'pdf', N'reports', N'snapshots', N'audit', N'system', "
    "N'templates', N'faktury', N'dashboard', N'koszty'"
)

# Pełna lista kategorii przed tą migracją (do downgrade)
_KATEGORIE_PRZED: Final[str] = (
    "N'auth', N'users', N'roles', N'permissions', N'debtors', N'monits', "
    "N'comments', N'pdf', N'reports', N'snapshots', N'audit', N'system', "
    "N'templates', N'faktury', N'dashboard'"
)


# =============================================================================
# UPGRADE
# =============================================================================

def upgrade() -> None:
    logger.info("[%s] ── UPGRADE START ──", revision)

    _krok1_tabela()
    _krok2_kolumny_monit_history()
    _krok3_constraint_kategoria()
    _krok4_uprawnienia()
    _krok5_przypisania_ról()

    logger.info("[%s] ── UPGRADE OK ──", revision)


def _krok1_tabela() -> None:
    logger.info("[%s] Krok 1/5 — CREATE TABLE skw_KosztyDodatkowe", revision)
    op.execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA_EXT}' AND t.name = N'skw_KosztyDodatkowe'
        )
        BEGIN
            CREATE TABLE [{SCHEMA_EXT}].[skw_KosztyDodatkowe] (
                [ID_KOSZTU]   BIGINT         NOT NULL IDENTITY(1,1),
                [Nazwa]       NVARCHAR(200)  NOT NULL,
                [Kwota]       DECIMAL(15,2)  NOT NULL,
                [TypMonitu]   NVARCHAR(20)   NOT NULL,
                [Opis]        NVARCHAR(500)      NULL,
                [IsActive]    BIT            NOT NULL
                              CONSTRAINT [DF_skw_KosztyDodatkowe_IsActive]  DEFAULT (1),
                [CreatedAt]   DATETIME       NOT NULL
                              CONSTRAINT [DF_skw_KosztyDodatkowe_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]   DATETIME           NULL,

                CONSTRAINT [PK_skw_KosztyDodatkowe]
                    PRIMARY KEY CLUSTERED ([ID_KOSZTU] ASC),
                CONSTRAINT [CK_skw_KosztyDodatkowe_TypMonitu]
                    CHECK ([TypMonitu] IN (N'email', N'sms', N'print')),
                CONSTRAINT [CK_skw_KosztyDodatkowe_Kwota]
                    CHECK ([Kwota] > 0),
                CONSTRAINT [CK_skw_KosztyDodatkowe_Nazwa]
                    CHECK (LEN(LTRIM(RTRIM([Nazwa]))) > 0)
            );
            CREATE NONCLUSTERED INDEX [IX_skw_KosztyDodatkowe_TypMonitu_Active]
                ON [{SCHEMA_EXT}].[skw_KosztyDodatkowe] ([TypMonitu], [IsActive])
                INCLUDE ([Kwota], [Nazwa]);
            PRINT N'[0021] Tabela skw_KosztyDodatkowe utworzona.';
        END
        ELSE
            PRINT N'[0021] Tabela skw_KosztyDodatkowe już istnieje — pomijam.';
    """)
    logger.info("[%s] Krok 1/5 — OK", revision)


def _krok2_kolumny_monit_history() -> None:
    logger.info("[%s] Krok 2/5 — ADD COLUMNS do skw_MonitHistory", revision)

    kolumny = [
        (
            "OdsetkiTotal",
            "DECIMAL(18,2)",
            "CK_skw_MonitHistory_OdsetkiTotal",
            "[OdsetkiTotal] IS NULL OR [OdsetkiTotal] >= 0",
        ),
        (
            "KosztyDodatkoweTotal",
            "DECIMAL(18,2)",
            "CK_skw_MonitHistory_KosztyDodatkoweTotal",
            "[KosztyDodatkoweTotal] IS NULL OR [KosztyDodatkoweTotal] >= 0",
        ),
        (
            "KwotaCalkowita",
            "DECIMAL(18,2)",
            "CK_skw_MonitHistory_KwotaCalkowita",
            "[KwotaCalkowita] IS NULL OR [KwotaCalkowita] >= 0",
        ),
    ]

    for col_name, col_type, ck_name, ck_def in kolumny:
        # Dodaj kolumnę
        op.execute(f"""
            IF NOT EXISTS (
                SELECT 1 FROM sys.columns c
                JOIN sys.tables  t ON c.object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = N'{SCHEMA_EXT}'
                  AND t.name = N'skw_MonitHistory'
                  AND c.name = N'{col_name}'
            )
            BEGIN
                ALTER TABLE [{SCHEMA_EXT}].[skw_MonitHistory]
                    ADD [{col_name}] {col_type} NULL;
                PRINT N'[0021] Kolumna {col_name} dodana.';
            END
            ELSE
                PRINT N'[0021] Kolumna {col_name} już istnieje — pomijam.';
        """)
        # Dodaj CHECK constraint
        op.execute(f"""
            IF NOT EXISTS (
                SELECT 1 FROM sys.check_constraints cc
                JOIN sys.tables  t ON cc.parent_object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name  = N'{SCHEMA_EXT}'
                  AND t.name  = N'skw_MonitHistory'
                  AND cc.name = N'{ck_name}'
            )
            BEGIN
                ALTER TABLE [{SCHEMA_EXT}].[skw_MonitHistory]
                    ADD CONSTRAINT [{ck_name}] CHECK ({ck_def});
                PRINT N'[0021] Constraint {ck_name} dodany.';
            END
        """)
        logger.info("[%s]   Kolumna %s — OK", revision, col_name)

    logger.info("[%s] Krok 2/5 — OK", revision)


def _krok3_constraint_kategoria() -> None:
    logger.info("[%s] Krok 3/5 — rozszerzenie CK_skw_Permissions_Category o 'koszty'", revision)
    op.execute(f"""
        IF NOT EXISTS (
            SELECT 1
            FROM sys.check_constraints cc
            JOIN sys.tables  t ON cc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name  = N'{SCHEMA_EXT}'
              AND t.name  = N'skw_Permissions'
              AND cc.name = N'CK_skw_Permissions_Category'
              AND cc.definition LIKE N'%koszty%'
        )
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM sys.check_constraints cc
                JOIN sys.tables  t ON cc.parent_object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name  = N'{SCHEMA_EXT}'
                  AND t.name  = N'skw_Permissions'
                  AND cc.name = N'CK_skw_Permissions_Category'
            )
            BEGIN
                ALTER TABLE [{SCHEMA_EXT}].[skw_Permissions]
                    DROP CONSTRAINT [CK_skw_Permissions_Category];
                PRINT N'[0021] Stary constraint usunięty.';
            END

            ALTER TABLE [{SCHEMA_EXT}].[skw_Permissions]
                ADD CONSTRAINT [CK_skw_Permissions_Category] CHECK (
                    [Category] IN ({_KATEGORIE_PO})
                );
            PRINT N'[0021] Constraint z koszty dodany.';
        END
        ELSE
            PRINT N'[0021] Constraint już zawiera koszty — pomijam.';
    """)
    logger.info("[%s] Krok 3/5 — OK", revision)


def _krok4_uprawnienia() -> None:
    logger.info("[%s] Krok 4/5 — INSERT uprawnienia (%d)", revision, len(_PERMISSIONS))
    for perm_name, description, category in _PERMISSIONS:
        op.execute(f"""
            MERGE [{SCHEMA_EXT}].[skw_Permissions] AS tgt
            USING (
                SELECT
                    N'{perm_name}'   AS PermissionName,
                    N'{description}' AS Description,
                    N'{category}'    AS Category
            ) AS src ON tgt.[PermissionName] = src.[PermissionName]
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ([PermissionName], [Description], [Category], [IsActive], [CreatedAt])
                VALUES (src.[PermissionName], src.[Description], src.[Category], 1, GETDATE());
        """)
        logger.info("[%s]   INSERT: %s", revision, perm_name)
    logger.info("[%s] Krok 4/5 — OK", revision)


def _krok5_przypisania_ról() -> None:
    logger.info("[%s] Krok 5/5 — przypisanie uprawnień do ról", revision)
    for role_name, perms in _ROLE_PERMISSIONS.items():
        for perm_name in perms:
            op.execute(f"""
                MERGE [{SCHEMA_EXT}].[skw_RolePermissions] AS tgt
                USING (
                    SELECT r.[ID_ROLE], p.[ID_PERMISSION]
                    FROM   [{SCHEMA_EXT}].[skw_Roles]       r
                    CROSS JOIN [{SCHEMA_EXT}].[skw_Permissions] p
                    WHERE  r.[RoleName]       = N'{role_name}'
                      AND  p.[PermissionName] = N'{perm_name}'
                      AND  p.[IsActive]       = 1
                ) AS src
                ON  tgt.[ID_ROLE]       = src.[ID_ROLE]
                AND tgt.[ID_PERMISSION] = src.[ID_PERMISSION]
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ([ID_ROLE], [ID_PERMISSION], [CreatedAt])
                    VALUES (src.[ID_ROLE], src.[ID_PERMISSION], GETDATE());
            """)
            logger.info("[%s]   ASSIGN: %s → %s", revision, role_name, perm_name)
    logger.info("[%s] Krok 5/5 — OK", revision)


# =============================================================================
# DOWNGRADE
# =============================================================================

def downgrade() -> None:
    logger.warning("[%s] ── DOWNGRADE START ──", revision)

    # Usuń przypisania ról
    all_perms = [p[0] for p in _PERMISSIONS]
    perms_sql = ", ".join(f"N'{p}'" for p in all_perms)
    op.execute(f"""
        DELETE rp
        FROM [{SCHEMA_EXT}].[skw_RolePermissions] rp
        INNER JOIN [{SCHEMA_EXT}].[skw_Permissions] p
            ON rp.[ID_PERMISSION] = p.[ID_PERMISSION]
        WHERE p.[PermissionName] IN ({perms_sql});
    """)

    # Usuń uprawnienia
    op.execute(f"""
        DELETE FROM [{SCHEMA_EXT}].[skw_Permissions]
        WHERE [PermissionName] IN ({perms_sql});
    """)

    # Cofnij constraint
    op.execute(f"""
        IF EXISTS (
            SELECT 1
            FROM sys.check_constraints cc
            JOIN sys.tables  t ON cc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name  = N'{SCHEMA_EXT}'
              AND t.name  = N'skw_Permissions'
              AND cc.name = N'CK_skw_Permissions_Category'
              AND cc.definition LIKE N'%koszty%'
        )
        BEGIN
            ALTER TABLE [{SCHEMA_EXT}].[skw_Permissions]
                DROP CONSTRAINT [CK_skw_Permissions_Category];
            ALTER TABLE [{SCHEMA_EXT}].[skw_Permissions]
                ADD CONSTRAINT [CK_skw_Permissions_Category]
                CHECK ([Category] IN ({_KATEGORIE_PRZED}));
            PRINT N'[0021] DOWNGRADE: koszty usunięte z constraint.';
        END
    """)

    # Usuń CHECK constraints i kolumny z MonitHistory
    for ck_name, col_name in [
        ("CK_skw_MonitHistory_KwotaCalkowita",       "KwotaCalkowita"),
        ("CK_skw_MonitHistory_KosztyDodatkoweTotal", "KosztyDodatkoweTotal"),
        ("CK_skw_MonitHistory_OdsetkiTotal",         "OdsetkiTotal"),
    ]:
        op.execute(f"""
            IF EXISTS (
                SELECT 1 FROM sys.check_constraints cc
                JOIN sys.tables  t ON cc.parent_object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = N'{SCHEMA_EXT}'
                  AND t.name = N'skw_MonitHistory'
                  AND cc.name = N'{ck_name}'
            )
            BEGIN
                ALTER TABLE [{SCHEMA_EXT}].[skw_MonitHistory]
                    DROP CONSTRAINT [{ck_name}];
            END
        """)
        op.execute(f"""
            IF EXISTS (
                SELECT 1 FROM sys.columns c
                JOIN sys.tables  t ON c.object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = N'{SCHEMA_EXT}'
                  AND t.name = N'skw_MonitHistory'
                  AND c.name = N'{col_name}'
            )
            BEGIN
                ALTER TABLE [{SCHEMA_EXT}].[skw_MonitHistory]
                    DROP COLUMN [{col_name}];
                PRINT N'[0021] DOWNGRADE: kolumna {col_name} usunięta.';
            END
        """)

    # Usuń tabelę
    op.execute(f"""
        IF EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA_EXT}' AND t.name = N'skw_KosztyDodatkowe'
        )
        BEGIN
            DROP TABLE [{SCHEMA_EXT}].[skw_KosztyDodatkowe];
            PRINT N'[0021] DOWNGRADE: tabela skw_KosztyDodatkowe usunięta.';
        END
    """)

    logger.warning("[%s] ── DOWNGRADE OK ──", revision)