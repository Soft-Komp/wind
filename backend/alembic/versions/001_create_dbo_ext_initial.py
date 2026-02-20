"""Tworzenie wszystkich tabel schematu dbo_ext — migracja inicjalna.

Revision ID: a1b2c3d4e5f6
Revises: (brak poprzedniej)
Create Date: 2026-02-18

UWAGA: Migracja ręczna (brak live DB podczas generowania).
Używa surowego SQL przez op.execute() dla pełnej kontroli nad typami MSSQL.

Kolejność tworzenia tabel (respektuje zależności FK):
  1.  dbo_ext.Roles              — brak zależności
  2.  dbo_ext.Permissions        — brak zależności
  3.  dbo_ext.Users              — FK → Roles
  4.  dbo_ext.RolePermissions    — FK → Roles, Permissions
  5.  dbo_ext.RefreshTokens      — FK → Users CASCADE DELETE
  6.  dbo_ext.OtpCodes           — FK → Users CASCADE DELETE
  7.  dbo_ext.Templates          — brak zależności (wymagane przez MonitHistory)
  8.  dbo_ext.AuditLog           — FK → Users SET NULL
  9.  dbo_ext.MonitHistory       — FK → Users SET NULL, Templates SET NULL
  10. dbo_ext.SystemConfig        — brak zależności
  11. dbo_ext.SchemaChecksums     — brak zależności
  12. dbo_ext.MasterAccessLog     — FK → Users SET NULL
  13. dbo_ext.Comments            — FK → Users RESTRICT (NO ACTION)

Downgrade: usuwa tabele w odwrotnej kolejności (szanuje FK).
"""

from __future__ import annotations

import logging

from alembic import op

# ─── Identyfikatory rewizji ───────────────────────────────────────────────────
revision: str = "a1b2c3d4e5f6"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None

logger = logging.getLogger("alembic.migration.001")

# ─── Nazwa schematu (jedna stała — łatwa zmiana) ──────────────────────────────
SCHEMA = "dbo_ext"


# ─── UPGRADE ─────────────────────────────────────────────────────────────────


def upgrade() -> None:
    """Tworzy schemat dbo_ext i wszystkie tabele własne systemu."""
    logger.info("=== MIGRACJA 001 — UPGRADE START ===")

    _create_schema()

    # Tabele bez zależności FK (najpierw)
    _create_roles()
    _create_permissions()
    _create_templates()
    _create_system_config()
    _create_schema_checksums()

    # Tabele zależne od Roles
    _create_users()

    # Tabele zależne od Roles + Permissions
    _create_role_permissions()

    # Tabele zależne od Users
    _create_refresh_tokens()
    _create_otp_codes()
    _create_audit_log()
    _create_monit_history()
    _create_master_access_log()
    _create_comments()

    # Indeksy wydajnościowe na tabelach własnych
    _create_indexes()

    logger.info("=== MIGRACJA 001 — UPGRADE ZAKOŃCZONY ===")


# ─── DOWNGRADE ───────────────────────────────────────────────────────────────


def downgrade() -> None:
    """Usuwa wszystkie tabele dbo_ext w odwrotnej kolejności FK."""
    logger.warning("=== MIGRACJA 001 — DOWNGRADE START (DESTRUKTYWNE!) ===")

    tables_in_reverse = [
        "Comments",
        "MasterAccessLog",
        "MonitHistory",
        "AuditLog",
        "OtpCodes",
        "RefreshTokens",
        "RolePermissions",
        "Users",
        "SchemaChecksums",
        "SystemConfig",
        "Templates",
        "Permissions",
        "Roles",
    ]

    for table in tables_in_reverse:
        op.execute(
            f"IF OBJECT_ID(N'[{SCHEMA}].[{table}]', N'U') IS NOT NULL "
            f"DROP TABLE [{SCHEMA}].[{table}];"
        )
        logger.info("Usunięto tabelę: %s.%s", SCHEMA, table)

    logger.warning("=== MIGRACJA 001 — DOWNGRADE ZAKOŃCZONY ===")


# ─── SCHEMAT ─────────────────────────────────────────────────────────────────


def _create_schema() -> None:
    op.execute(
        """
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dbo_ext')
        BEGIN
            EXEC sp_executesql N'CREATE SCHEMA [dbo_ext]';
            PRINT 'Schemat dbo_ext utworzony.';
        END
        ELSE
        BEGIN
            PRINT 'Schemat dbo_ext już istnieje — pominięto.';
        END
        """
    )
    logger.info("Schemat dbo_ext: OK")


# ─── TABELE ──────────────────────────────────────────────────────────────────


def _create_roles() -> None:
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[Roles]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[Roles] (
                [ID_ROLE]     INT           IDENTITY(1,1) NOT NULL,
                [RoleName]    NVARCHAR(50)  NOT NULL,
                [Description] NVARCHAR(200) NULL,
                [IsActive]    BIT           NOT NULL CONSTRAINT [DF_Roles_IsActive] DEFAULT (1),
                [CreatedAt]   DATETIME      NOT NULL CONSTRAINT [DF_Roles_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]   DATETIME      NULL,

                CONSTRAINT [PK_Roles]         PRIMARY KEY CLUSTERED ([ID_ROLE] ASC),
                CONSTRAINT [UQ_Roles_RoleName] UNIQUE NONCLUSTERED ([RoleName] ASC)
            );
            PRINT 'Tabela [{SCHEMA}].[Roles] utworzona.';
        END
        ELSE
            PRINT 'Tabela [{SCHEMA}].[Roles] już istnieje — pominięto.';
        """
    )
    logger.info("Tabela Roles: OK")


def _create_permissions() -> None:
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[Permissions]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[Permissions] (
                [ID_PERMISSION]  INT           IDENTITY(1,1) NOT NULL,
                [PermissionName] NVARCHAR(100) NOT NULL,
                [Description]    NVARCHAR(200) NULL,
                [Category]       NVARCHAR(50)  NOT NULL,
                [IsActive]       BIT           NOT NULL CONSTRAINT [DF_Permissions_IsActive]  DEFAULT (1),
                [CreatedAt]      DATETIME      NOT NULL CONSTRAINT [DF_Permissions_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]      DATETIME      NULL,

                CONSTRAINT [PK_Permissions]              PRIMARY KEY CLUSTERED ([ID_PERMISSION] ASC),
                CONSTRAINT [UQ_Permissions_PermissionName] UNIQUE NONCLUSTERED ([PermissionName] ASC)
            );
            CREATE NONCLUSTERED INDEX [IX_Permissions_Category]
                ON [{SCHEMA}].[Permissions] ([Category] ASC)
                INCLUDE ([PermissionName], [IsActive]);
            PRINT 'Tabela [{SCHEMA}].[Permissions] utworzona.';
        END
        ELSE
            PRINT 'Tabela [{SCHEMA}].[Permissions] już istnieje — pominięto.';
        """
    )
    logger.info("Tabela Permissions: OK")


def _create_users() -> None:
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[Users]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[Users] (
                [ID_USER]              INT           IDENTITY(1,1) NOT NULL,
                [Username]             NVARCHAR(50)  NOT NULL,
                [Email]                NVARCHAR(100) NOT NULL,
                [PasswordHash]         NVARCHAR(255) NOT NULL,
                [FullName]             NVARCHAR(100) NULL,
                [IsActive]             BIT           NOT NULL CONSTRAINT [DF_Users_IsActive]             DEFAULT (1),
                [RoleID]               INT           NOT NULL,
                [CreatedAt]            DATETIME      NOT NULL CONSTRAINT [DF_Users_CreatedAt]            DEFAULT (GETDATE()),
                [UpdatedAt]            DATETIME      NULL,
                [LastLoginAt]          DATETIME      NULL,
                [FailedLoginAttempts]  INT           NOT NULL CONSTRAINT [DF_Users_FailedLoginAttempts] DEFAULT (0),
                [LockedUntil]          DATETIME      NULL,

                CONSTRAINT [PK_Users]           PRIMARY KEY CLUSTERED ([ID_USER] ASC),
                CONSTRAINT [UQ_Users_Username]  UNIQUE NONCLUSTERED ([Username] ASC),
                CONSTRAINT [UQ_Users_Email]     UNIQUE NONCLUSTERED ([Email] ASC),
                CONSTRAINT [FK_Users_Roles]     FOREIGN KEY ([RoleID])
                    REFERENCES [{SCHEMA}].[Roles] ([ID_ROLE])
                    ON DELETE NO ACTION
                    ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_Users_RoleID]
                ON [{SCHEMA}].[Users] ([RoleID] ASC)
                INCLUDE ([Username], [IsActive]);
            CREATE NONCLUSTERED INDEX [IX_Users_IsActive_CreatedAt]
                ON [{SCHEMA}].[Users] ([IsActive] ASC, [CreatedAt] DESC);
            PRINT 'Tabela [{SCHEMA}].[Users] utworzona.';
        END
        ELSE
            PRINT 'Tabela [{SCHEMA}].[Users] już istnieje — pominięto.';
        """
    )
    logger.info("Tabela Users: OK")


def _create_role_permissions() -> None:
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[RolePermissions]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[RolePermissions] (
                [ID_ROLE]       INT      NOT NULL,
                [ID_PERMISSION] INT      NOT NULL,
                [CreatedAt]     DATETIME NOT NULL CONSTRAINT [DF_RolePermissions_CreatedAt] DEFAULT (GETDATE()),

                CONSTRAINT [PK_RolePermissions] PRIMARY KEY CLUSTERED ([ID_ROLE] ASC, [ID_PERMISSION] ASC),
                CONSTRAINT [FK_RolePermissions_Roles]
                    FOREIGN KEY ([ID_ROLE])
                    REFERENCES [{SCHEMA}].[Roles] ([ID_ROLE])
                    ON DELETE CASCADE
                    ON UPDATE CASCADE,
                CONSTRAINT [FK_RolePermissions_Permissions]
                    FOREIGN KEY ([ID_PERMISSION])
                    REFERENCES [{SCHEMA}].[Permissions] ([ID_PERMISSION])
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
            );
            CREATE NONCLUSTERED INDEX [IX_RolePermissions_PermissionID]
                ON [{SCHEMA}].[RolePermissions] ([ID_PERMISSION] ASC);
            PRINT 'Tabela [{SCHEMA}].[RolePermissions] utworzona.';
        END
        ELSE
            PRINT 'Tabela [{SCHEMA}].[RolePermissions] już istnieje — pominięto.';
        """
    )
    logger.info("Tabela RolePermissions: OK")


def _create_refresh_tokens() -> None:
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[RefreshTokens]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[RefreshTokens] (
                [ID_TOKEN]  INT           IDENTITY(1,1) NOT NULL,
                [ID_USER]   INT           NOT NULL,
                [Token]     NVARCHAR(500) NOT NULL,
                [ExpiresAt] DATETIME      NOT NULL,
                [CreatedAt] DATETIME      NOT NULL CONSTRAINT [DF_RefreshTokens_CreatedAt] DEFAULT (GETDATE()),
                [IsRevoked] BIT           NOT NULL CONSTRAINT [DF_RefreshTokens_IsRevoked] DEFAULT (0),
                [RevokedAt] DATETIME      NULL,
                [IPAddress] NVARCHAR(45)  NULL,
                [UserAgent] NVARCHAR(500) NULL,

                CONSTRAINT [PK_RefreshTokens]    PRIMARY KEY CLUSTERED ([ID_TOKEN] ASC),
                CONSTRAINT [FK_RefreshTokens_Users]
                    FOREIGN KEY ([ID_USER])
                    REFERENCES [{SCHEMA}].[Users] ([ID_USER])
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
            );
            CREATE NONCLUSTERED INDEX [IX_RefreshTokens_UserID]
                ON [{SCHEMA}].[RefreshTokens] ([ID_USER] ASC)
                INCLUDE ([IsRevoked], [ExpiresAt]);
            CREATE NONCLUSTERED INDEX [IX_RefreshTokens_Token]
                ON [{SCHEMA}].[RefreshTokens] ([Token] ASC);
            CREATE NONCLUSTERED INDEX [IX_RefreshTokens_ExpiresAt]
                ON [{SCHEMA}].[RefreshTokens] ([ExpiresAt] ASC)
                WHERE [IsRevoked] = 0;
            PRINT 'Tabela [{SCHEMA}].[RefreshTokens] utworzona.';
        END
        ELSE
            PRINT 'Tabela [{SCHEMA}].[RefreshTokens] już istnieje — pominięto.';
        """
    )
    logger.info("Tabela RefreshTokens: OK")


def _create_otp_codes() -> None:
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[OtpCodes]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[OtpCodes] (
                [ID_OTP]    INT          IDENTITY(1,1) NOT NULL,
                [ID_USER]   INT          NOT NULL,
                [Code]      NVARCHAR(10) NOT NULL,
                [Purpose]   NVARCHAR(20) NOT NULL
                                CONSTRAINT [CK_OtpCodes_Purpose]
                                CHECK ([Purpose] IN (N'password_reset', N'2fa')),
                [ExpiresAt] DATETIME     NOT NULL,
                [IsUsed]    BIT          NOT NULL CONSTRAINT [DF_OtpCodes_IsUsed]    DEFAULT (0),
                [CreatedAt] DATETIME     NOT NULL CONSTRAINT [DF_OtpCodes_CreatedAt] DEFAULT (GETDATE()),
                [IPAddress] NVARCHAR(45) NULL,

                CONSTRAINT [PK_OtpCodes] PRIMARY KEY CLUSTERED ([ID_OTP] ASC),
                CONSTRAINT [FK_OtpCodes_Users]
                    FOREIGN KEY ([ID_USER])
                    REFERENCES [{SCHEMA}].[Users] ([ID_USER])
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
            );
            CREATE NONCLUSTERED INDEX [IX_OtpCodes_UserID_IsUsed]
                ON [{SCHEMA}].[OtpCodes] ([ID_USER] ASC, [IsUsed] ASC)
                INCLUDE ([ExpiresAt], [Purpose]);
            CREATE NONCLUSTERED INDEX [IX_OtpCodes_ExpiresAt]
                ON [{SCHEMA}].[OtpCodes] ([ExpiresAt] ASC)
                WHERE [IsUsed] = 0;
            PRINT 'Tabela [{SCHEMA}].[OtpCodes] utworzona.';
        END
        ELSE
            PRINT 'Tabela [{SCHEMA}].[OtpCodes] już istnieje — pominięto.';
        """
    )
    logger.info("Tabela OtpCodes: OK")


def _create_templates() -> None:
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[Templates]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[Templates] (
                [ID_TEMPLATE]    INT           IDENTITY(1,1) NOT NULL,
                [TemplateName]   NVARCHAR(100) NOT NULL,
                [TemplateType]   NVARCHAR(20)  NOT NULL
                                     CONSTRAINT [CK_Templates_TemplateType]
                                     CHECK ([TemplateType] IN (N'email', N'sms', N'print')),
                [Subject]        NVARCHAR(200) NULL,
                [Body]           NVARCHAR(MAX) NOT NULL,
                [IsActive]       BIT           NOT NULL CONSTRAINT [DF_Templates_IsActive]  DEFAULT (1),
                [CreatedAt]      DATETIME      NOT NULL CONSTRAINT [DF_Templates_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]      DATETIME      NULL,

                CONSTRAINT [PK_Templates]             PRIMARY KEY CLUSTERED ([ID_TEMPLATE] ASC),
                CONSTRAINT [UQ_Templates_TemplateName] UNIQUE NONCLUSTERED ([TemplateName] ASC)
            );
            CREATE NONCLUSTERED INDEX [IX_Templates_TemplateType_IsActive]
                ON [{SCHEMA}].[Templates] ([TemplateType] ASC, [IsActive] ASC);
            PRINT 'Tabela [{SCHEMA}].[Templates] utworzona.';
        END
        ELSE
            PRINT 'Tabela [{SCHEMA}].[Templates] już istnieje — pominięto.';
        """
    )
    logger.info("Tabela Templates: OK")


def _create_audit_log() -> None:
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[AuditLog]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[AuditLog] (
                [ID_LOG]         BIGINT        IDENTITY(1,1) NOT NULL,
                [ID_USER]        INT           NULL,
                [Username]       NVARCHAR(50)  NULL,
                [Action]         NVARCHAR(100) NOT NULL,
                [ActionCategory] NVARCHAR(50)  NULL,
                [EntityType]     NVARCHAR(50)  NULL,
                [EntityID]       INT           NULL,
                [OldValue]       NVARCHAR(MAX) NULL,
                [NewValue]       NVARCHAR(MAX) NULL,
                [Details]        NVARCHAR(MAX) NULL,
                [IPAddress]      NVARCHAR(45)  NULL,
                [UserAgent]      NVARCHAR(500) NULL,
                [RequestURL]     NVARCHAR(500) NULL,
                [RequestMethod]  NVARCHAR(10)  NULL,
                [Timestamp]      DATETIME      NOT NULL CONSTRAINT [DF_AuditLog_Timestamp] DEFAULT (GETDATE()),
                [Success]        BIT           NOT NULL CONSTRAINT [DF_AuditLog_Success]   DEFAULT (1),
                [ErrorMessage]   NVARCHAR(500) NULL,

                CONSTRAINT [PK_AuditLog] PRIMARY KEY CLUSTERED ([ID_LOG] ASC),
                -- SET NULL przy usunięciu usera — rekord zostaje, Username zachowany
                CONSTRAINT [FK_AuditLog_Users]
                    FOREIGN KEY ([ID_USER])
                    REFERENCES [{SCHEMA}].[Users] ([ID_USER])
                    ON DELETE SET NULL
                    ON UPDATE NO ACTION
            );
            -- Indeks czasowy — najczęstsze zapytania to "logi z ostatnich X dni"
            CREATE NONCLUSTERED INDEX [IX_AuditLog_Timestamp]
                ON [{SCHEMA}].[AuditLog] ([Timestamp] DESC);
            CREATE NONCLUSTERED INDEX [IX_AuditLog_UserID_Timestamp]
                ON [{SCHEMA}].[AuditLog] ([ID_USER] ASC, [Timestamp] DESC)
                WHERE [ID_USER] IS NOT NULL;
            CREATE NONCLUSTERED INDEX [IX_AuditLog_Action_Category]
                ON [{SCHEMA}].[AuditLog] ([ActionCategory] ASC, [Action] ASC);
            CREATE NONCLUSTERED INDEX [IX_AuditLog_EntityType_EntityID]
                ON [{SCHEMA}].[AuditLog] ([EntityType] ASC, [EntityID] ASC)
                WHERE [EntityType] IS NOT NULL AND [EntityID] IS NOT NULL;
            PRINT 'Tabela [{SCHEMA}].[AuditLog] utworzona.';
        END
        ELSE
            PRINT 'Tabela [{SCHEMA}].[AuditLog] już istnieje — pominięto.';
        """
    )
    logger.info("Tabela AuditLog: OK")


def _create_monit_history() -> None:
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[MonitHistory]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[MonitHistory] (
                [ID_MONIT]        BIGINT         IDENTITY(1,1) NOT NULL,
                [ID_KONTRAHENTA]  INT            NOT NULL,
                [ID_USER]         INT            NULL,
                [MonitType]       NVARCHAR(20)   NOT NULL
                                      CONSTRAINT [CK_MonitHistory_MonitType]
                                      CHECK ([MonitType] IN (N'email', N'sms', N'print')),
                [TemplateID]      INT            NULL,
                [Status]          NVARCHAR(20)   NOT NULL
                                      CONSTRAINT [CK_MonitHistory_Status]
                                      CHECK ([Status] IN (N'pending', N'sent', N'delivered',
                                                          N'bounced', N'failed', N'opened', N'clicked')),
                [Recipient]       NVARCHAR(100)  NULL,
                [Subject]         NVARCHAR(200)  NULL,
                [MessageBody]     NVARCHAR(MAX)  NULL,
                [TotalDebt]       DECIMAL(18, 2) NULL,
                [InvoiceNumbers]  NVARCHAR(500)  NULL,
                [PDFPath]         NVARCHAR(500)  NULL,
                [ExternalID]      NVARCHAR(100)  NULL,
                [ScheduledAt]     DATETIME       NULL,
                [SentAt]          DATETIME       NULL,
                [DeliveredAt]     DATETIME       NULL,
                [OpenedAt]        DATETIME       NULL,
                [ClickedAt]       DATETIME       NULL,
                [ErrorMessage]    NVARCHAR(500)  NULL,
                [RetryCount]      INT            NOT NULL CONSTRAINT [DF_MonitHistory_RetryCount] DEFAULT (0),
                [Cost]            DECIMAL(10, 4) NULL,
                [IsActive]        BIT            NOT NULL CONSTRAINT [DF_MonitHistory_IsActive]   DEFAULT (1),
                [CreatedAt]       DATETIME       NOT NULL CONSTRAINT [DF_MonitHistory_CreatedAt]  DEFAULT (GETDATE()),
                [UpdatedAt]       DATETIME       NULL,

                CONSTRAINT [PK_MonitHistory] PRIMARY KEY CLUSTERED ([ID_MONIT] ASC),
                CONSTRAINT [FK_MonitHistory_Users]
                    FOREIGN KEY ([ID_USER])
                    REFERENCES [{SCHEMA}].[Users] ([ID_USER])
                    ON DELETE SET NULL
                    ON UPDATE NO ACTION,
                CONSTRAINT [FK_MonitHistory_Templates]
                    FOREIGN KEY ([TemplateID])
                    REFERENCES [{SCHEMA}].[Templates] ([ID_TEMPLATE])
                    ON DELETE SET NULL
                    ON UPDATE NO ACTION
            );
            -- Indeks pokrywający typowe zapytania: historia dla kontrahenta
            CREATE NONCLUSTERED INDEX [IX_MonitHistory_Kontrahent_CreatedAt]
                ON [{SCHEMA}].[MonitHistory] ([ID_KONTRAHENTA] ASC, [CreatedAt] DESC)
                INCLUDE ([MonitType], [Status], [ID_USER]);
            CREATE NONCLUSTERED INDEX [IX_MonitHistory_Status_CreatedAt]
                ON [{SCHEMA}].[MonitHistory] ([Status] ASC, [CreatedAt] DESC)
                WHERE [IsActive] = 1;
            CREATE NONCLUSTERED INDEX [IX_MonitHistory_UserID_CreatedAt]
                ON [{SCHEMA}].[MonitHistory] ([ID_USER] ASC, [CreatedAt] DESC)
                WHERE [ID_USER] IS NOT NULL;
            PRINT 'Tabela [{SCHEMA}].[MonitHistory] utworzona.';
        END
        ELSE
            PRINT 'Tabela [{SCHEMA}].[MonitHistory] już istnieje — pominięto.';
        """
    )
    logger.info("Tabela MonitHistory: OK")


def _create_system_config() -> None:
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[SystemConfig]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[SystemConfig] (
                [ID_CONFIG]    INT           IDENTITY(1,1) NOT NULL,
                [ConfigKey]    NVARCHAR(100) NOT NULL,
                [ConfigValue]  NVARCHAR(MAX) NOT NULL,
                [Description]  NVARCHAR(500) NULL,
                [IsActive]     BIT           NOT NULL CONSTRAINT [DF_SystemConfig_IsActive]  DEFAULT (1),
                [CreatedAt]    DATETIME      NOT NULL CONSTRAINT [DF_SystemConfig_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]    DATETIME      NULL,

                CONSTRAINT [PK_SystemConfig]          PRIMARY KEY CLUSTERED ([ID_CONFIG] ASC),
                CONSTRAINT [UQ_SystemConfig_ConfigKey] UNIQUE NONCLUSTERED ([ConfigKey] ASC)
            );
            PRINT 'Tabela [{SCHEMA}].[SystemConfig] utworzona.';
        END
        ELSE
            PRINT 'Tabela [{SCHEMA}].[SystemConfig] już istnieje — pominięto.';
        """
    )
    logger.info("Tabela SystemConfig: OK")


def _create_schema_checksums() -> None:
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[SchemaChecksums]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[SchemaChecksums] (
                [ID_CHECKSUM]      INT           IDENTITY(1,1) NOT NULL,
                [ObjectName]       NVARCHAR(200) NOT NULL,
                [SchemaName]       NVARCHAR(50)  NOT NULL
                                       CONSTRAINT [CK_SchemaChecksums_SchemaName]
                                       CHECK ([SchemaName] IN (N'dbo', N'dbo_ext')),
                [ObjectType]       NVARCHAR(50)  NOT NULL
                                       CONSTRAINT [CK_SchemaChecksums_ObjectType]
                                       CHECK ([ObjectType] IN (N'VIEW', N'PROCEDURE', N'INDEX')),
                [Checksum]         INT           NOT NULL,
                [AlembicRevision]  NVARCHAR(50)  NULL,
                [LastVerifiedAt]   DATETIME      NULL,
                [CreatedAt]        DATETIME      NOT NULL CONSTRAINT [DF_SchemaChecksums_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]        DATETIME      NULL,

                CONSTRAINT [PK_SchemaChecksums] PRIMARY KEY CLUSTERED ([ID_CHECKSUM] ASC),
                CONSTRAINT [UQ_SchemaChecksums_Object]
                    UNIQUE NONCLUSTERED ([ObjectName] ASC, [SchemaName] ASC, [ObjectType] ASC)
            );
            PRINT 'Tabela [{SCHEMA}].[SchemaChecksums] utworzona.';
        END
        ELSE
            PRINT 'Tabela [{SCHEMA}].[SchemaChecksums] już istnieje — pominięto.';
        """
    )
    logger.info("Tabela SchemaChecksums: OK")


def _create_master_access_log() -> None:
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[MasterAccessLog]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[MasterAccessLog] (
                [ID_LOG]         BIGINT        IDENTITY(1,1) NOT NULL,
                [TargetUserID]   INT           NULL,
                [TargetUsername] NVARCHAR(50)  NOT NULL,
                [IPAddress]      NVARCHAR(45)  NOT NULL,
                [UserAgent]      NVARCHAR(500) NULL,
                [AccessedAt]     DATETIME      NOT NULL CONSTRAINT [DF_MasterAccessLog_AccessedAt] DEFAULT (GETDATE()),
                [SessionEndedAt] DATETIME      NULL,
                [Notes]          NVARCHAR(500) NULL,

                CONSTRAINT [PK_MasterAccessLog] PRIMARY KEY CLUSTERED ([ID_LOG] ASC),
                CONSTRAINT [FK_MasterAccessLog_Users]
                    FOREIGN KEY ([TargetUserID])
                    REFERENCES [{SCHEMA}].[Users] ([ID_USER])
                    ON DELETE SET NULL
                    ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_MasterAccessLog_AccessedAt]
                ON [{SCHEMA}].[MasterAccessLog] ([AccessedAt] DESC);
            CREATE NONCLUSTERED INDEX [IX_MasterAccessLog_TargetUserID]
                ON [{SCHEMA}].[MasterAccessLog] ([TargetUserID] ASC)
                WHERE [TargetUserID] IS NOT NULL;
            PRINT 'Tabela [{SCHEMA}].[MasterAccessLog] utworzona.';
        END
        ELSE
            PRINT 'Tabela [{SCHEMA}].[MasterAccessLog] już istnieje — pominięto.';
        """
    )
    logger.info("Tabela MasterAccessLog: OK")


def _create_comments() -> None:
    op.execute(
        f"""
        IF OBJECT_ID(N'[{SCHEMA}].[Comments]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[Comments] (
                [ID_COMMENT]     INT           IDENTITY(1,1) NOT NULL,
                [ID_KONTRAHENTA] INT           NOT NULL,
                [Tresc]          NVARCHAR(MAX) NOT NULL,
                [UzytkownikID]   INT           NOT NULL,
                [IsActive]       BIT           NOT NULL CONSTRAINT [DF_Comments_IsActive]  DEFAULT (1),
                [CreatedAt]      DATETIME      NOT NULL CONSTRAINT [DF_Comments_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]      DATETIME      NULL,

                CONSTRAINT [PK_Comments] PRIMARY KEY CLUSTERED ([ID_COMMENT] ASC),
                -- RESTRICT: nie można usunąć usera który ma komentarze
                CONSTRAINT [FK_Comments_Users]
                    FOREIGN KEY ([UzytkownikID])
                    REFERENCES [{SCHEMA}].[Users] ([ID_USER])
                    ON DELETE NO ACTION
                    ON UPDATE NO ACTION
            );
            -- Indeks pokrywający listę komentarzy dla dłużnika (najczęstsze zapytanie)
            CREATE NONCLUSTERED INDEX [IX_Comments_Kontrahent_CreatedAt]
                ON [{SCHEMA}].[Comments] ([ID_KONTRAHENTA] ASC, [CreatedAt] DESC)
                INCLUDE ([UzytkownikID], [IsActive]);
            CREATE NONCLUSTERED INDEX [IX_Comments_UzytkownikID]
                ON [{SCHEMA}].[Comments] ([UzytkownikID] ASC)
                WHERE [IsActive] = 1;
            PRINT 'Tabela [{SCHEMA}].[Comments] utworzona.';
        END
        ELSE
            PRINT 'Tabela [{SCHEMA}].[Comments] już istnieje — pominięto.';
        """
    )
    logger.info("Tabela Comments: OK")


# ─── INDEKSY WYDAJNOŚCIOWE (własne tabele) ────────────────────────────────────


def _create_indexes() -> None:
    """Dodatkowe indeksy złożone — ponad te tworzone inline przy CREATE TABLE."""

    # AuditLog: zapytanie "co zrobił user X w categorii Y"
    op.execute(
        f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[AuditLog]')
              AND name = N'IX_AuditLog_Success_Timestamp'
        )
        CREATE NONCLUSTERED INDEX [IX_AuditLog_Success_Timestamp]
            ON [{SCHEMA}].[AuditLog] ([Success] ASC, [Timestamp] DESC)
            WHERE [Success] = 0;
        """
    )
    logger.info("Indeks IX_AuditLog_Success_Timestamp: OK")