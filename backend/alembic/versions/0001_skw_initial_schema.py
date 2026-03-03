"""Inicjalna migracja schematu dbo_ext — wszystkie tabele skw_*.

Revision ID:  0001
Revises:      (brak — to jest pierwsza migracja)
Create Date:  2026-03-02 00:00:00.000000 UTC

OPIS:
  Tworzy kompletny schemat systemu Windykacja (prefiks skw_).
  Zastępuje stare migracje 001-004 + 20260223 + e5f6a7b8c9d0
  które używały tabel bez prefiksu.

TABELE (13 tabel, kolejność respektuje zależności FK):
   1. dbo_ext.skw_Roles              — brak FK
   2. dbo_ext.skw_Permissions        — brak FK
   3. dbo_ext.skw_Templates          — brak FK (wymagane przez MonitHistory)
   4. dbo_ext.skw_SystemConfig       — brak FK
   5. dbo_ext.skw_SchemaChecksums    — brak FK
   6. dbo_ext.skw_Users              — FK → skw_Roles
   7. dbo_ext.skw_RolePermissions    — FK → skw_Roles, skw_Permissions
   8. dbo_ext.skw_RefreshTokens      — FK → skw_Users (CASCADE DELETE)
   9. dbo_ext.skw_OtpCodes           — FK → skw_Users (CASCADE DELETE)
  10. dbo_ext.skw_AuditLog           — FK → skw_Users (SET NULL)
  11. dbo_ext.skw_MonitHistory       — FK → skw_Users (SET NULL), skw_Templates (SET NULL)
  12. dbo_ext.skw_MasterAccessLog    — FK → skw_Users (SET NULL)
  13. dbo_ext.skw_Comments           — FK → skw_Users (NO ACTION / RESTRICT)

WAŻNE — SPOSÓB UŻYCIA:
  Migracja używa op.execute() z surowym SQL (IF NOT EXISTS) — jest idempotentna.
  Można ją uruchomić na pustej bazie LUB na bazie gdzie DDL był już wykonany
  ręcznie przez sqlcmd.

  Dla nowych środowisk (fresh install):
    Opcja A — Alembic tworzy tabele (zamiast sqlcmd):
      cd backend/
      alembic upgrade head
      alembic stamp head  ← jeśli tabele już istniały

    Opcja B — DDL ręcznie + oznaczenie wersji (ZALECANE):
      sqlcmd -S ... -i database/ddl/000_create_schema.sql
      sqlcmd -S ... -i database/ddl/001_roles.sql
      ... (wszystkie DDL 000-014)
      cd backend/
      alembic stamp head

DOWNGRADE:
  Usuwa wszystkie tabele skw_* w odwrotnej kolejności FK.
  DESTRUKTYWNE — traci wszystkie dane.
  Używaj TYLKO na środowiskach testowych.
"""

from __future__ import annotations

import logging

from alembic import op

# =============================================================================
# Identyfikatory rewizji
# =============================================================================

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None

# =============================================================================
# Logger
# =============================================================================

logger = logging.getLogger("alembic.migration.0001_skw_initial_schema")

# =============================================================================
# Stała — schemat (jedna zmiana = zmiana wszędzie)
# =============================================================================

SCHEMA: str = "dbo_ext"


# =============================================================================
# UPGRADE
# =============================================================================

def upgrade() -> None:
    """
    Tworzy kompletny schemat skw_* w dbo_ext.

    Kolejność: respektuje zależności FK.
    Każda funkcja jest idempotentna (IF NOT EXISTS / IF OBJECT_ID IS NULL).
    """
    logger.info("=== MIGRACJA 0001 skw_initial_schema — UPGRADE START ===")

    # ── Krok 1: Schemat ───────────────────────────────────────────────────────
    _create_schema()

    # ── Krok 2: Tabele bez zależności (w kolejności) ──────────────────────────
    _create_skw_roles()
    _create_skw_permissions()
    _create_skw_templates()
    _create_skw_system_config()
    _create_skw_schema_checksums()

    # ── Krok 3: Users (FK → Roles) ────────────────────────────────────────────
    _create_skw_users()

    # ── Krok 4: Tabele zależne od Users + Roles/Permissions ───────────────────
    _create_skw_role_permissions()
    _create_skw_refresh_tokens()
    _create_skw_otp_codes()
    _create_skw_audit_log()
    _create_skw_monit_history()
    _create_skw_master_access_log()
    _create_skw_comments()

    logger.info("=== MIGRACJA 0001 skw_initial_schema — UPGRADE ZAKOŃCZONY ===")


# =============================================================================
# DOWNGRADE
# =============================================================================

def downgrade() -> None:
    """
    Usuwa wszystkie tabele skw_* w odwrotnej kolejności FK.

    ⚠️ DESTRUKTYWNE — traci WSZYSTKIE dane systemu Windykacja.
    Wywoływać TYLKO na środowiskach testowych / deweloperskich.
    Na produkcji: alembic downgrade base jest zablokowane przez runbook.
    """
    logger.warning(
        "=== MIGRACJA 0001 skw_initial_schema — DOWNGRADE START "
        "(DESTRUKTYWNE — usuwam wszystkie tabele skw_*) ===",
    )

    # Odwrotna kolejność FK — najpierw tabele z referencjami do innych
    _tables_ordered_for_drop = [
        "skw_Comments",
        "skw_MasterAccessLog",
        "skw_MonitHistory",
        "skw_AuditLog",
        "skw_OtpCodes",
        "skw_RefreshTokens",
        "skw_RolePermissions",
        "skw_Users",
        "skw_SchemaChecksums",
        "skw_SystemConfig",
        "skw_Templates",
        "skw_Permissions",
        "skw_Roles",
    ]

    for table_name in _tables_ordered_for_drop:
        op.execute(
            f"IF OBJECT_ID(N'[{SCHEMA}].[{table_name}]', N'U') IS NOT NULL "
            f"    DROP TABLE [{SCHEMA}].[{table_name}];"
        )
        logger.info("Usunięto tabelę: %s.%s", SCHEMA, table_name)

    logger.warning(
        "=== MIGRACJA 0001 skw_initial_schema — DOWNGRADE ZAKOŃCZONY ==="
    )


# =============================================================================
# SCHEMAT
# =============================================================================

def _create_schema() -> None:
    """Tworzy schemat dbo_ext jeśli nie istnieje."""
    op.execute(
        """
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dbo_ext')
        BEGIN
            EXEC sp_executesql N'CREATE SCHEMA [dbo_ext] AUTHORIZATION dbo';
            PRINT '[0001] Schemat dbo_ext utworzony.';
        END
        ELSE
            PRINT '[0001] Schemat dbo_ext już istnieje — pominięto.';
        """
    )
    logger.info("[0001] Schemat dbo_ext: OK")


# =============================================================================
# TABELE — IMPLEMENTACJA
# =============================================================================

def _create_skw_roles() -> None:
    """
    dbo_ext.skw_Roles — role użytkowników systemu.

    Seed: database/seeds/01_roles.sql → Admin, Manager, User, ReadOnly
    Powiązania: ← skw_Users.RoleID (FK RESTRICT)
                ← skw_RolePermissions.ID_ROLE (FK CASCADE DELETE)
    """
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_Roles]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_Roles] (
                [ID_ROLE]     INT           IDENTITY(1,1) NOT NULL,
                [RoleName]    NVARCHAR(50)                NOT NULL,
                [Description] NVARCHAR(200)                   NULL,
                [IsActive]    BIT           NOT NULL
                              CONSTRAINT [DF_skw_Roles_IsActive]  DEFAULT (1),
                [CreatedAt]   DATETIME      NOT NULL
                              CONSTRAINT [DF_skw_Roles_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]   DATETIME                        NULL,

                CONSTRAINT [PK_skw_Roles]
                    PRIMARY KEY CLUSTERED ([ID_ROLE] ASC),
                CONSTRAINT [UQ_skw_Roles_RoleName]
                    UNIQUE ([RoleName])
            );
            CREATE NONCLUSTERED INDEX [IX_skw_Roles_IsActive]
                ON [{SCHEMA}].[skw_Roles] ([IsActive] ASC);
            PRINT '[0001] Tabela skw_Roles: OK';
        END
        ELSE
            PRINT '[0001] Tabela skw_Roles: już istnieje';
        """
    )
    logger.info("[0001] skw_Roles: OK")


def _create_skw_permissions() -> None:
    """
    dbo_ext.skw_Permissions — granularne uprawnienia (format: kategoria.akcja).

    Seed: database/seeds/02_permissions.sql → 83 uprawnienia w 11 kategoriach
    """
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_Permissions]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_Permissions] (
                [ID_PERMISSION]  INT            IDENTITY(1,1) NOT NULL,
                [PermissionName] NVARCHAR(100)                NOT NULL,
                [Description]    NVARCHAR(200)                    NULL,
                [Category]       NVARCHAR(50)                     NULL,
                [IsActive]       BIT            NOT NULL
                                 CONSTRAINT [DF_skw_Permissions_IsActive]  DEFAULT (1),
                [CreatedAt]      DATETIME       NOT NULL
                                 CONSTRAINT [DF_skw_Permissions_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]      DATETIME                         NULL,

                CONSTRAINT [PK_skw_Permissions]
                    PRIMARY KEY CLUSTERED ([ID_PERMISSION] ASC),
                CONSTRAINT [UQ_skw_Permissions_PermissionName]
                    UNIQUE ([PermissionName]),
                CONSTRAINT [CK_skw_Permissions_Category]
                    CHECK ([Category] IN (
                        N'auth', N'users', N'roles', N'permissions', N'debtors', N'monits',
                        N'comments', N'pdf', N'reports', N'snapshots',
                        N'audit', N'system'
                    ))
            );
            CREATE NONCLUSTERED INDEX [IX_skw_Permissions_Category]
                ON [{SCHEMA}].[skw_Permissions] ([Category] ASC, [IsActive] ASC);
            PRINT '[0001] Tabela skw_Permissions: OK';
        END
        ELSE
            PRINT '[0001] Tabela skw_Permissions: już istnieje';
        """
    )
    logger.info("[0001] skw_Permissions: OK")


def _create_skw_templates() -> None:
    """
    dbo_ext.skw_Templates — szablony wiadomości Jinja2 (email/sms/print).

    UWAGA: musi być PRZED skw_MonitHistory (FK dependency).
    """
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_Templates]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_Templates] (
                [ID_TEMPLATE]  INT            IDENTITY(1,1) NOT NULL,
                [TemplateName] NVARCHAR(100)                NOT NULL,
                [TemplateType] NVARCHAR(20)                 NOT NULL,
                [Subject]      NVARCHAR(200)                    NULL,
                [Body]         NVARCHAR(MAX)                NOT NULL,
                [IsActive]     BIT            NOT NULL
                               CONSTRAINT [DF_skw_Templates_IsActive]  DEFAULT (1),
                [CreatedAt]    DATETIME       NOT NULL
                               CONSTRAINT [DF_skw_Templates_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]    DATETIME                         NULL,

                CONSTRAINT [PK_skw_Templates]
                    PRIMARY KEY CLUSTERED ([ID_TEMPLATE] ASC),
                CONSTRAINT [UQ_skw_Templates_TemplateName]
                    UNIQUE ([TemplateName]),
                CONSTRAINT [CK_skw_Templates_TemplateType]
                    CHECK ([TemplateType] IN (N'email', N'sms', N'print')),
                CONSTRAINT [CK_skw_Templates_Subject_Email]
                    CHECK (
                        ([TemplateType] = N'email' AND [Subject] IS NOT NULL)
                        OR [TemplateType] <> N'email'
                    )
            );
            CREATE NONCLUSTERED INDEX [IX_skw_Templates_Type_Active]
                ON [{SCHEMA}].[skw_Templates] ([TemplateType] ASC, [IsActive] ASC);
            PRINT '[0001] Tabela skw_Templates: OK';
        END
        ELSE
            PRINT '[0001] Tabela skw_Templates: już istnieje';
        """
    )
    logger.info("[0001] skw_Templates: OK")


def _create_skw_system_config() -> None:
    """
    dbo_ext.skw_SystemConfig — dynamiczna konfiguracja aplikacji.

    Cachowana w Redis (TTL: 5 min). Seed: database/seeds/05_system_config.sql → 8 kluczy.
    """
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_SystemConfig]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_SystemConfig] (
                [ID_CONFIG]   INT            IDENTITY(1,1) NOT NULL,
                [ConfigKey]   NVARCHAR(100)                NOT NULL,
                [ConfigValue] NVARCHAR(MAX)                    NULL,
                [Description] NVARCHAR(500)                    NULL,
                [IsActive]    BIT            NOT NULL
                              CONSTRAINT [DF_skw_SystemConfig_IsActive]  DEFAULT (1),
                [CreatedAt]   DATETIME       NOT NULL
                              CONSTRAINT [DF_skw_SystemConfig_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]   DATETIME                         NULL,

                CONSTRAINT [PK_skw_SystemConfig]
                    PRIMARY KEY CLUSTERED ([ID_CONFIG] ASC),
                CONSTRAINT [UQ_skw_SystemConfig_ConfigKey]
                    UNIQUE ([ConfigKey])
            );
            CREATE NONCLUSTERED INDEX [IX_skw_SystemConfig_IsActive]
                ON [{SCHEMA}].[skw_SystemConfig] ([IsActive] ASC, [ConfigKey] ASC);
            PRINT '[0001] Tabela skw_SystemConfig: OK';
        END
        ELSE
            PRINT '[0001] Tabela skw_SystemConfig: już istnieje';
        """
    )
    logger.info("[0001] skw_SystemConfig: OK")


def _create_skw_schema_checksums() -> None:
    """
    dbo_ext.skw_SchemaChecksums — rejestr sum kontrolnych widoków i procedur.

    Weryfikacja przy starcie aplikacji. Niezgodność → BLOCK (SystemExit).
    """
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_SchemaChecksums]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_SchemaChecksums] (
                [ID_CHECKSUM]     INT            IDENTITY(1,1) NOT NULL,
                [ObjectName]      NVARCHAR(200)                NOT NULL,
                [ObjectType]      NVARCHAR(50)                 NOT NULL,
                [SchemaName]      NVARCHAR(20)                 NOT NULL
                                  CONSTRAINT [DF_skw_SchemaChecksums_SchemaName]
                                  DEFAULT (N'dbo_ext'),
                [Checksum]        INT                          NOT NULL,
                [AlembicRevision] NVARCHAR(50)                     NULL,
                [LastVerifiedAt]  DATETIME                         NULL,
                [CreatedAt]       DATETIME       NOT NULL
                                  CONSTRAINT [DF_skw_SchemaChecksums_CreatedAt]
                                  DEFAULT (GETDATE()),
                [UpdatedAt]       DATETIME                         NULL,

                CONSTRAINT [PK_skw_SchemaChecksums]
                    PRIMARY KEY CLUSTERED ([ID_CHECKSUM] ASC),
                CONSTRAINT [CK_skw_SchemaChecksums_ObjectType]
                    CHECK ([ObjectType] IN (N'VIEW', N'PROCEDURE', N'INDEX')),
                CONSTRAINT [CK_skw_SchemaChecksums_SchemaName]
                    CHECK ([SchemaName] IN (N'dbo', N'dbo_ext')),
                CONSTRAINT [UQ_skw_SchemaChecksums_Object]
                    UNIQUE ([ObjectName], [SchemaName], [ObjectType])
            );
            PRINT '[0001] Tabela skw_SchemaChecksums: OK';
        END
        ELSE
            PRINT '[0001] Tabela skw_SchemaChecksums: już istnieje';
        """
    )
    logger.info("[0001] skw_SchemaChecksums: OK")


def _create_skw_users() -> None:
    """
    dbo_ext.skw_Users — użytkownicy systemu.

    Hasła: argon2id (argon2-cffi) — NIGDY plain text.
    Blokada konta: FailedLoginAttempts + LockedUntil.
    FK: → skw_Roles (RESTRICT — nie można usunąć roli z userami)
    """
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_Users]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_Users] (
                [ID_USER]              INT            IDENTITY(1,1) NOT NULL,
                [Username]             NVARCHAR(50)                 NOT NULL,
                [Email]                NVARCHAR(100)                NOT NULL,
                [PasswordHash]         NVARCHAR(255)                NOT NULL,
                [FullName]             NVARCHAR(100)                    NULL,
                [RoleID]               INT                          NOT NULL,
                [IsActive]             BIT            NOT NULL
                                       CONSTRAINT [DF_skw_Users_IsActive]  DEFAULT (1),
                [LastLoginAt]          DATETIME                         NULL,
                [FailedLoginAttempts]  INT            NOT NULL
                                       CONSTRAINT [DF_skw_Users_FailedLoginAttempts] DEFAULT (0),
                [LockedUntil]          DATETIME                         NULL,
                [CreatedAt]            DATETIME       NOT NULL
                                       CONSTRAINT [DF_skw_Users_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]            DATETIME                         NULL,

                CONSTRAINT [PK_skw_Users]
                    PRIMARY KEY CLUSTERED ([ID_USER] ASC),
                CONSTRAINT [UQ_skw_Users_Username]
                    UNIQUE ([Username]),
                CONSTRAINT [UQ_skw_Users_Email]
                    UNIQUE ([Email]),
                CONSTRAINT [CK_skw_Users_FailedLoginAttempts]
                    CHECK ([FailedLoginAttempts] >= 0),
                CONSTRAINT [FK_skw_Users_RoleID]
                    FOREIGN KEY ([RoleID])
                    REFERENCES [{SCHEMA}].[skw_Roles] ([ID_ROLE])
                    ON DELETE NO ACTION
                    ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_skw_Users_RoleID]
                ON [{SCHEMA}].[skw_Users] ([RoleID] ASC);
            CREATE NONCLUSTERED INDEX [IX_skw_Users_IsActive]
                ON [{SCHEMA}].[skw_Users] ([IsActive] ASC);
            CREATE NONCLUSTERED INDEX [IX_skw_Users_LockedUntil]
                ON [{SCHEMA}].[skw_Users] ([LockedUntil] ASC)
                WHERE [LockedUntil] IS NOT NULL;
            PRINT '[0001] Tabela skw_Users: OK';
        END
        ELSE
            PRINT '[0001] Tabela skw_Users: już istnieje';
        """
    )
    logger.info("[0001] skw_Users: OK")


def _create_skw_role_permissions() -> None:
    """
    dbo_ext.skw_RolePermissions — tabela łącząca role z uprawnieniami.

    Operacja przypisania: zawsze DELETE + INSERT.
    FK: → skw_Roles (CASCADE DELETE), → skw_Permissions (CASCADE DELETE)
    """
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_RolePermissions]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_RolePermissions] (
                [ID_ROLE]       INT      NOT NULL,
                [ID_PERMISSION] INT      NOT NULL,
                [CreatedAt]     DATETIME NOT NULL
                                CONSTRAINT [DF_skw_RolePermissions_CreatedAt]
                                DEFAULT (GETDATE()),

                CONSTRAINT [PK_skw_RolePermissions]
                    PRIMARY KEY CLUSTERED ([ID_ROLE] ASC, [ID_PERMISSION] ASC),
                CONSTRAINT [FK_skw_RolePermissions_RoleID]
                    FOREIGN KEY ([ID_ROLE])
                    REFERENCES [{SCHEMA}].[skw_Roles] ([ID_ROLE])
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION,
                CONSTRAINT [FK_skw_RolePermissions_PermissionID]
                    FOREIGN KEY ([ID_PERMISSION])
                    REFERENCES [{SCHEMA}].[skw_Permissions] ([ID_PERMISSION])
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_skw_RolePermissions_PermissionID]
                ON [{SCHEMA}].[skw_RolePermissions]
                ([ID_PERMISSION] ASC, [ID_ROLE] ASC);
            PRINT '[0001] Tabela skw_RolePermissions: OK';
        END
        ELSE
            PRINT '[0001] Tabela skw_RolePermissions: już istnieje';
        """
    )
    logger.info("[0001] skw_RolePermissions: OK")


def _create_skw_refresh_tokens() -> None:
    """
    dbo_ext.skw_RefreshTokens — tokeny odświeżania JWT (HttpOnly cookie).

    Token: SHA-256 hash — NIGDY plain JWT.
    Immutable: revoke = IsRevoked=1 + RevokedAt=GETDATE().
    FK: → skw_Users (CASCADE DELETE — usunięcie usera usuwa tokeny)
    """
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_RefreshTokens]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_RefreshTokens] (
                [ID_TOKEN]  INT            IDENTITY(1,1) NOT NULL,
                [ID_USER]   INT                          NOT NULL,
                [Token]     NVARCHAR(500)                NOT NULL,
                [ExpiresAt] DATETIME                     NOT NULL,
                [IsRevoked] BIT            NOT NULL
                            CONSTRAINT [DF_skw_RefreshTokens_IsRevoked] DEFAULT (0),
                [RevokedAt] DATETIME                         NULL,
                [IPAddress] NVARCHAR(45)                     NULL,
                [UserAgent] NVARCHAR(500)                    NULL,
                [CreatedAt] DATETIME       NOT NULL
                            CONSTRAINT [DF_skw_RefreshTokens_CreatedAt] DEFAULT (GETDATE()),

                CONSTRAINT [PK_skw_RefreshTokens]
                    PRIMARY KEY CLUSTERED ([ID_TOKEN] ASC),
                CONSTRAINT [UQ_skw_RefreshTokens_Token]
                    UNIQUE ([Token]),
                CONSTRAINT [FK_skw_RefreshTokens_UserID]
                    FOREIGN KEY ([ID_USER])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_skw_RefreshTokens_UserID]
                ON [{SCHEMA}].[skw_RefreshTokens] ([ID_USER] ASC, [IsRevoked] ASC);
            CREATE NONCLUSTERED INDEX [IX_skw_RefreshTokens_ExpiresAt]
                ON [{SCHEMA}].[skw_RefreshTokens] ([ExpiresAt] ASC)
                WHERE [IsRevoked] = 0;
            PRINT '[0001] Tabela skw_RefreshTokens: OK';
        END
        ELSE
            PRINT '[0001] Tabela skw_RefreshTokens: już istnieje';
        """
    )
    logger.info("[0001] skw_RefreshTokens: OK")


def _create_skw_otp_codes() -> None:
    """
    dbo_ext.skw_OtpCodes — jednorazowe kody OTP (reset hasła, 2FA).

    Kod: hash bcrypt — NIGDY plain 6-cyfr.
    TTL: z SystemConfig: otp.expiry_minutes (domyślnie 15 min).
    FK: → skw_Users (CASCADE DELETE)
    """
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_OtpCodes]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_OtpCodes] (
                [ID_OTP]    INT          IDENTITY(1,1) NOT NULL,
                [ID_USER]   INT                        NOT NULL,
                [Code]      NVARCHAR(10)               NOT NULL,
                [Purpose]   NVARCHAR(20)               NOT NULL,
                [ExpiresAt] DATETIME                   NOT NULL,
                [IsUsed]    BIT          NOT NULL
                            CONSTRAINT [DF_skw_OtpCodes_IsUsed]    DEFAULT (0),
                [IPAddress] NVARCHAR(45)                    NULL,
                [CreatedAt] DATETIME     NOT NULL
                            CONSTRAINT [DF_skw_OtpCodes_CreatedAt] DEFAULT (GETDATE()),

                CONSTRAINT [PK_skw_OtpCodes]
                    PRIMARY KEY CLUSTERED ([ID_OTP] ASC),
                CONSTRAINT [CK_skw_OtpCodes_Purpose]
                    CHECK ([Purpose] IN (N'password_reset', N'2fa')),
                CONSTRAINT [FK_skw_OtpCodes_UserID]
                    FOREIGN KEY ([ID_USER])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_skw_OtpCodes_UserID_Purpose]
                ON [{SCHEMA}].[skw_OtpCodes]
                ([ID_USER] ASC, [Purpose] ASC, [IsUsed] ASC)
                WHERE [IsUsed] = 0;
            CREATE NONCLUSTERED INDEX [IX_skw_OtpCodes_ExpiresAt]
                ON [{SCHEMA}].[skw_OtpCodes] ([ExpiresAt] ASC)
                WHERE [IsUsed] = 0;
            PRINT '[0001] Tabela skw_OtpCodes: OK';
        END
        ELSE
            PRINT '[0001] Tabela skw_OtpCodes: już istnieje';
        """
    )
    logger.info("[0001] skw_OtpCodes: OK")


def _create_skw_audit_log() -> None:
    """
    dbo_ext.skw_AuditLog — pełny audit trail wszystkich operacji.

    ⚠️ IMMUTABLE — tylko INSERT. Nigdy UPDATE ani DELETE.
    OldValue/NewValue/Details: JSON (NVARCHAR MAX).
    Zapis asynchroniczny — nie blokuje HTTP response.
    FK: → skw_Users (SET NULL — log zostaje po usunięciu usera)
    """
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_AuditLog]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_AuditLog] (
                [ID_LOG]          BIGINT         IDENTITY(1,1) NOT NULL,
                [ID_USER]         INT                              NULL,
                [Username]        NVARCHAR(50)                     NULL,
                [Action]          NVARCHAR(100)                NOT NULL,
                [ActionCategory]  NVARCHAR(50)                     NULL,
                [EntityType]      NVARCHAR(50)                     NULL,
                [EntityID]        INT                              NULL,
                [OldValue]        NVARCHAR(MAX)                    NULL,
                [NewValue]        NVARCHAR(MAX)                    NULL,
                [Details]         NVARCHAR(MAX)                    NULL,
                [IPAddress]       NVARCHAR(45)                     NULL,
                [UserAgent]       NVARCHAR(500)                    NULL,
                [RequestURL]      NVARCHAR(500)                    NULL,
                [RequestMethod]   NVARCHAR(10)                     NULL,
                [Success]         BIT            NOT NULL
                                  CONSTRAINT [DF_skw_AuditLog_Success]    DEFAULT (1),
                [ErrorMessage]    NVARCHAR(500)                    NULL,
                [Timestamp]       DATETIME       NOT NULL
                                  CONSTRAINT [DF_skw_AuditLog_Timestamp]  DEFAULT (GETDATE()),

                CONSTRAINT [PK_skw_AuditLog]
                    PRIMARY KEY CLUSTERED ([ID_LOG] ASC),
                CONSTRAINT [CK_skw_AuditLog_RequestMethod]
                    CHECK ([RequestMethod] IN (
                        N'GET', N'POST', N'PUT', N'DELETE', N'PATCH'
                    ) OR [RequestMethod] IS NULL),
                CONSTRAINT [CK_skw_AuditLog_ActionCategory]
                    CHECK ([ActionCategory] IN (
                        N'Auth', N'Users', N'Roles', N'Debtors', N'Monits',
                        N'Comments', N'System', N'Snapshots', N'Audit'
                    ) OR [ActionCategory] IS NULL),
                CONSTRAINT [FK_skw_AuditLog_UserID]
                    FOREIGN KEY ([ID_USER])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE SET NULL
                    ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_skw_AuditLog_Timestamp]
                ON [{SCHEMA}].[skw_AuditLog] ([Timestamp] DESC);
            CREATE NONCLUSTERED INDEX [IX_skw_AuditLog_UserID]
                ON [{SCHEMA}].[skw_AuditLog] ([ID_USER] ASC, [Timestamp] DESC)
                WHERE [ID_USER] IS NOT NULL;
            CREATE NONCLUSTERED INDEX [IX_skw_AuditLog_ActionCategory]
                ON [{SCHEMA}].[skw_AuditLog] ([ActionCategory] ASC, [Timestamp] DESC);
            CREATE NONCLUSTERED INDEX [IX_skw_AuditLog_EntityType_ID]
                ON [{SCHEMA}].[skw_AuditLog]
                ([EntityType] ASC, [EntityID] ASC, [Timestamp] DESC)
                WHERE [EntityType] IS NOT NULL;
            -- Indeks na nieudane operacje (Success=0) — do monitoringu bezpieczeństwa
            CREATE NONCLUSTERED INDEX [IX_skw_AuditLog_Failures]
                ON [{SCHEMA}].[skw_AuditLog] ([Timestamp] DESC)
                WHERE [Success] = 0;
            PRINT '[0001] Tabela skw_AuditLog: OK';
        END
        ELSE
            PRINT '[0001] Tabela skw_AuditLog: już istnieje';
        """
    )
    logger.info("[0001] skw_AuditLog: OK")


def _create_skw_monit_history() -> None:
    """
    dbo_ext.skw_MonitHistory — historia wysłanych monitów (email/sms/print).

    ID_KONTRAHENTA: klucz z WAPRO (BEZ FK constraint — inna baza logiczna).
    InvoiceNumbers: JSON array, np. ["FV/001/2026","FV/002/2026"]
    Statusy: pending → sent → delivered → bounced/failed/opened/clicked
    FK: → skw_Users (SET NULL), → skw_Templates (SET NULL)
    """
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_MonitHistory]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_MonitHistory] (
                [ID_MONIT]        BIGINT          IDENTITY(1,1) NOT NULL,
                [ID_KONTRAHENTA]  INT                           NOT NULL,
                [ID_USER]         INT                               NULL,
                [MonitType]       NVARCHAR(20)                  NOT NULL,
                [TemplateID]      INT                               NULL,
                [Status]          NVARCHAR(20)                  NOT NULL
                                  CONSTRAINT [DF_skw_MonitHistory_Status]
                                  DEFAULT (N'pending'),
                [Recipient]       NVARCHAR(100)                     NULL,
                [Subject]         NVARCHAR(200)                     NULL,
                [MessageBody]     NVARCHAR(MAX)                     NULL,
                [TotalDebt]       DECIMAL(18,2)                     NULL,
                [InvoiceNumbers]  NVARCHAR(500)                     NULL,
                [PDFPath]         NVARCHAR(500)                     NULL,
                [ExternalID]      NVARCHAR(100)                     NULL,
                [ScheduledAt]     DATETIME                          NULL,
                [SentAt]          DATETIME                          NULL,
                [DeliveredAt]     DATETIME                          NULL,
                [OpenedAt]        DATETIME                          NULL,
                [ClickedAt]       DATETIME                          NULL,
                [ErrorMessage]    NVARCHAR(500)                     NULL,
                [RetryCount]      INT            NOT NULL
                                  CONSTRAINT [DF_skw_MonitHistory_RetryCount] DEFAULT (0),
                [Cost]            DECIMAL(10,4)                     NULL,
                [IsActive]        BIT            NOT NULL
                                  CONSTRAINT [DF_skw_MonitHistory_IsActive]  DEFAULT (1),
                [CreatedAt]       DATETIME       NOT NULL
                                  CONSTRAINT [DF_skw_MonitHistory_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]       DATETIME                          NULL,

                CONSTRAINT [PK_skw_MonitHistory]
                    PRIMARY KEY CLUSTERED ([ID_MONIT] ASC),
                CONSTRAINT [CK_skw_MonitHistory_MonitType]
                    CHECK ([MonitType] IN (N'email', N'sms', N'print')),
                CONSTRAINT [CK_skw_MonitHistory_Status]
                    CHECK ([Status] IN (
                        N'pending', N'sent', N'delivered',
                        N'bounced', N'failed', N'opened', N'clicked'
                    )),
                CONSTRAINT [CK_skw_MonitHistory_RetryCount]
                    CHECK ([RetryCount] >= 0),
                CONSTRAINT [CK_skw_MonitHistory_TotalDebt]
                    CHECK ([TotalDebt] IS NULL OR [TotalDebt] >= 0),
                CONSTRAINT [FK_skw_MonitHistory_UserID]
                    FOREIGN KEY ([ID_USER])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE SET NULL
                    ON UPDATE NO ACTION,
                CONSTRAINT [FK_skw_MonitHistory_TemplateID]
                    FOREIGN KEY ([TemplateID])
                    REFERENCES [{SCHEMA}].[skw_Templates] ([ID_TEMPLATE])
                    ON DELETE SET NULL
                    ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_skw_MonitHistory_Kontrahent]
                ON [{SCHEMA}].[skw_MonitHistory]
                ([ID_KONTRAHENTA] ASC, [CreatedAt] DESC);
            CREATE NONCLUSTERED INDEX [IX_skw_MonitHistory_Status]
                ON [{SCHEMA}].[skw_MonitHistory] ([Status] ASC, [CreatedAt] DESC);
            CREATE NONCLUSTERED INDEX [IX_skw_MonitHistory_UserID]
                ON [{SCHEMA}].[skw_MonitHistory] ([ID_USER] ASC, [CreatedAt] DESC)
                WHERE [ID_USER] IS NOT NULL;
            PRINT '[0001] Tabela skw_MonitHistory: OK';
        END
        ELSE
            PRINT '[0001] Tabela skw_MonitHistory: już istnieje';
        """
    )
    logger.info("[0001] skw_MonitHistory: OK")


def _create_skw_master_access_log() -> None:
    """
    dbo_ext.skw_MasterAccessLog — log dostępu przez Master Key.

    ⚠️ IMMUTABLE — tylko INSERT. Nigdy UPDATE ani DELETE.
    ⚠️ Brak endpointu API — dostęp wyłącznie przez SSMS (DBA).
    App user: tylko INSERT.
    FK: → skw_Users (SET NULL — log zostaje po usunięciu usera)
    """
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_MasterAccessLog]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_MasterAccessLog] (
                [ID_LOG]          BIGINT         IDENTITY(1,1) NOT NULL,
                [TargetUserID]    INT                              NULL,
                [TargetUsername]  NVARCHAR(50)                 NOT NULL,
                [IPAddress]       NVARCHAR(45)                 NOT NULL,
                [UserAgent]       NVARCHAR(500)                    NULL,
                [AccessedAt]      DATETIME       NOT NULL
                                  CONSTRAINT [DF_skw_MasterAccessLog_AccessedAt]
                                  DEFAULT (GETDATE()),
                [SessionEndedAt]  DATETIME                         NULL,
                [Notes]           NVARCHAR(500)                    NULL,

                CONSTRAINT [PK_skw_MasterAccessLog]
                    PRIMARY KEY CLUSTERED ([ID_LOG] ASC),
                CONSTRAINT [FK_skw_MasterAccessLog_TargetUserID]
                    FOREIGN KEY ([TargetUserID])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE SET NULL
                    ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_skw_MasterAccessLog_AccessedAt]
                ON [{SCHEMA}].[skw_MasterAccessLog] ([AccessedAt] DESC);
            CREATE NONCLUSTERED INDEX [IX_skw_MasterAccessLog_TargetUserID]
                ON [{SCHEMA}].[skw_MasterAccessLog]
                ([TargetUserID] ASC, [AccessedAt] DESC)
                WHERE [TargetUserID] IS NOT NULL;
            PRINT '[0001] Tabela skw_MasterAccessLog: OK';
        END
        ELSE
            PRINT '[0001] Tabela skw_MasterAccessLog: już istnieje';
        """
    )
    logger.info("[0001] skw_MasterAccessLog: OK")


def _create_skw_comments() -> None:
    """
    dbo_ext.skw_Comments — komentarze do kontrahentów (dłużników).

    ID_KONTRAHENTA: klucz z WAPRO (BEZ FK constraint — inna baza logiczna).
    Usunięcie: dwuetapowe (token potwierdzający, TTL z SystemConfig).
    FK: → skw_Users przez UzytkownikID (NO ACTION / RESTRICT)
        ⚠️ Nie można usunąć usera z komentarzami!
    """
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_Comments]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_Comments] (
                [ID_COMMENT]      INT            IDENTITY(1,1) NOT NULL,
                [ID_KONTRAHENTA]  INT                          NOT NULL,
                [Tresc]           NVARCHAR(MAX)                NOT NULL,
                [UzytkownikID]    INT                          NOT NULL,
                [IsActive]        BIT            NOT NULL
                                  CONSTRAINT [DF_skw_Comments_IsActive]  DEFAULT (1),
                [CreatedAt]       DATETIME       NOT NULL
                                  CONSTRAINT [DF_skw_Comments_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]       DATETIME                         NULL,

                CONSTRAINT [PK_skw_Comments]
                    PRIMARY KEY CLUSTERED ([ID_COMMENT] ASC),
                CONSTRAINT [FK_skw_Comments_UzytkownikID]
                    FOREIGN KEY ([UzytkownikID])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE NO ACTION
                    ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_skw_Comments_Kontrahent]
                ON [{SCHEMA}].[skw_Comments]
                ([ID_KONTRAHENTA] ASC, [IsActive] ASC, [CreatedAt] DESC);
            CREATE NONCLUSTERED INDEX [IX_skw_Comments_UzytkownikID]
                ON [{SCHEMA}].[skw_Comments]
                ([UzytkownikID] ASC, [CreatedAt] DESC);
            PRINT '[0001] Tabela skw_Comments: OK';
        END
        ELSE
            PRINT '[0001] Tabela skw_Comments: już istnieje';
        """
    )
    logger.info("[0001] skw_Comments: OK")