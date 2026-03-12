"""initial_schema

Revision ID:  f124fa9b58eb
Revises:      
Create Date:  2026-03-12 11:25:32.875502+00:00

INSTRUKCJA:
  upgrade:   alembic upgrade f124fa9b58eb
  downgrade: alembic downgrade 

WAŻNE dla MSSQL:
  - Każda operacja DDL w MSSQL jest auto-transakcyjna.
  - Jeśli coś się wysypie, transakcja jest automatycznie rollbackowana.
  - NIGDY nie mieszaj operacji DDL i DML w jednej migracji na MSSQL —
    może to powodować problemy z transakcjami.
  - Dla zmiany kolumny NOT NULL: kolejność ma znaczenie
      1. Dodaj kolumnę jako NULL
      2. Wypełnij dane (UPDATE)
      3. Zmień na NOT NULL (ALTER COLUMN)

KONWENCJA NAZEWNICTWA:
  Nazwa migracji w -m "..." powinna być:
    - snake_case
    - opisowa: "add_skw_documents_table" nie "update2"
    - czas przeszły: "added_column_x" lub rzeczownik: "skw_initial_schema"

PRZYKŁADY OPERACJI:
  # Dodaj kolumnę
  op.add_column('skw_Users', sa.Column('PhoneNumber', sa.String(20), nullable=True), schema='dbo_ext')

  # Zmień typ kolumny
  op.alter_column('skw_Users', 'Email', existing_type=sa.String(100), type_=sa.String(200), schema='dbo_ext')

  # Dodaj indeks
  op.create_index('IX_skw_Users_Email', 'skw_Users', ['Email'], schema='dbo_ext')

  # Wykonaj surowy SQL (np. INSERT seed danych)
  op.execute("INSERT INTO [dbo_ext].[skw_SystemConfig] ...")

  # Warunkowe — sprawdź czy kolumna istnieje zanim dodasz
  # (przydatne gdy DDL i Alembic mogą być niezsynch.)
  from alembic import op as alembic_op
  bind = op.get_bind()
  inspector = sa.inspect(bind)
  columns = [c['name'] for c in inspector.get_columns('skw_Users', schema='dbo_ext')]
  if 'NewColumn' not in columns:
      op.add_column(...)
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mssql

# revision identifiers, used by Alembic.
revision: str = 'f124fa9b58eb'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA: str = "dbo_ext"


def upgrade() -> None:
    _create_schema()
    _create_skw_roles()
    _create_skw_permissions()
    _create_skw_templates()
    _create_skw_system_config()
    _create_skw_schema_checksums()
    _create_skw_users()
    _create_skw_role_permissions()
    _create_skw_refresh_tokens()
    _create_skw_otp_codes()
    _create_skw_audit_log()
    _create_skw_monit_history()
    _create_skw_master_access_log()
    _create_skw_comments()


def downgrade() -> None:
    for table in [
        "skw_Comments", "skw_MasterAccessLog", "skw_MonitHistory",
        "skw_AuditLog", "skw_OtpCodes", "skw_RefreshTokens",
        "skw_RolePermissions", "skw_Users", "skw_SchemaChecksums",
        "skw_SystemConfig", "skw_Templates", "skw_Permissions", "skw_Roles",
    ]:
        op.execute(
            f"IF OBJECT_ID(N'[{SCHEMA}].[{table}]', N'U') IS NOT NULL "
            f"DROP TABLE [{SCHEMA}].[{table}];"
        )


def _create_schema() -> None:
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dbo_ext')
        BEGIN
            EXEC sp_executesql N'CREATE SCHEMA [dbo_ext] AUTHORIZATION dbo';
        END
    """)


def _create_skw_roles() -> None:
    op.execute(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_Roles]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_Roles] (
                [ID_ROLE]     INT           IDENTITY(1,1) NOT NULL,
                [RoleName]    NVARCHAR(50)                NOT NULL,
                [Description] NVARCHAR(200)                   NULL,
                [IsActive]    BIT           NOT NULL CONSTRAINT [DF_skw_Roles_IsActive]  DEFAULT (1),
                [CreatedAt]   DATETIME      NOT NULL CONSTRAINT [DF_skw_Roles_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]   DATETIME                        NULL,
                CONSTRAINT [PK_skw_Roles]          PRIMARY KEY CLUSTERED ([ID_ROLE] ASC),
                CONSTRAINT [UQ_skw_Roles_RoleName] UNIQUE ([RoleName])
            );
            CREATE NONCLUSTERED INDEX [IX_skw_Roles_IsActive]
                ON [{SCHEMA}].[skw_Roles] ([IsActive] ASC);
        END
    """)


def _create_skw_permissions() -> None:
    op.execute(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_Permissions]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_Permissions] (
                [ID_PERMISSION]  INT            IDENTITY(1,1) NOT NULL,
                [PermissionName] NVARCHAR(100)                NOT NULL,
                [Description]    NVARCHAR(200)                    NULL,
                [Category]       NVARCHAR(50)                     NULL,
                [IsActive]       BIT            NOT NULL CONSTRAINT [DF_skw_Permissions_IsActive]  DEFAULT (1),
                [CreatedAt]      DATETIME       NOT NULL CONSTRAINT [DF_skw_Permissions_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]      DATETIME                         NULL,
                CONSTRAINT [PK_skw_Permissions]
                    PRIMARY KEY CLUSTERED ([ID_PERMISSION] ASC),
                CONSTRAINT [UQ_skw_Permissions_PermissionName]
                    UNIQUE ([PermissionName]),
                CONSTRAINT [CK_skw_Permissions_Category]
                    CHECK ([Category] IN (
                        N'auth', N'users', N'roles', N'permissions', N'debtors',
                        N'monits', N'comments', N'pdf', N'reports',
                        N'snapshots', N'audit', N'system'
                    ))
            );
            CREATE NONCLUSTERED INDEX [IX_skw_Permissions_Category]
                ON [{SCHEMA}].[skw_Permissions] ([Category] ASC, [IsActive] ASC);
        END
    """)


def _create_skw_templates() -> None:
    op.execute(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_Templates]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_Templates] (
                [ID_TEMPLATE]  INT            IDENTITY(1,1) NOT NULL,
                [TemplateName] NVARCHAR(100)                NOT NULL,
                [TemplateType] NVARCHAR(20)                 NOT NULL,
                [Subject]      NVARCHAR(200)                    NULL,
                [Body]         NVARCHAR(MAX)                NOT NULL,
                [IsActive]     BIT            NOT NULL CONSTRAINT [DF_skw_Templates_IsActive]  DEFAULT (1),
                [CreatedAt]    DATETIME       NOT NULL CONSTRAINT [DF_skw_Templates_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]    DATETIME                         NULL,
                CONSTRAINT [PK_skw_Templates]
                    PRIMARY KEY CLUSTERED ([ID_TEMPLATE] ASC),
                CONSTRAINT [UQ_skw_Templates_TemplateName]
                    UNIQUE ([TemplateName]),
                CONSTRAINT [CK_skw_Templates_TemplateType]
                    CHECK ([TemplateType] IN (N'email', N'sms', N'print'))
            );
            CREATE NONCLUSTERED INDEX [IX_skw_Templates_Type_Active]
                ON [{SCHEMA}].[skw_Templates] ([TemplateType] ASC, [IsActive] ASC);
        END
    """)


def _create_skw_system_config() -> None:
    op.execute(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_SystemConfig]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_SystemConfig] (
                [ID_CONFIG]   INT            IDENTITY(1,1) NOT NULL,
                [ConfigKey]   NVARCHAR(100)                NOT NULL,
                [ConfigValue] NVARCHAR(MAX)                    NULL,
                [Description] NVARCHAR(500)                    NULL,
                [IsActive]    BIT            NOT NULL CONSTRAINT [DF_skw_SystemConfig_IsActive]  DEFAULT (1),
                [CreatedAt]   DATETIME       NOT NULL CONSTRAINT [DF_skw_SystemConfig_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]   DATETIME                         NULL,
                CONSTRAINT [PK_skw_SystemConfig]         PRIMARY KEY CLUSTERED ([ID_CONFIG] ASC),
                CONSTRAINT [UQ_skw_SystemConfig_ConfigKey] UNIQUE ([ConfigKey])
            );
        END
    """)


def _create_skw_schema_checksums() -> None:
    op.execute(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_SchemaChecksums]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_SchemaChecksums] (
                [ID_CHECKSUM]     INT            IDENTITY(1,1) NOT NULL,
                [ObjectName]      NVARCHAR(200)                NOT NULL,
                [ObjectType]      NVARCHAR(50)                 NOT NULL,
                [SchemaName]      NVARCHAR(20)                 NOT NULL
                                  CONSTRAINT [DF_skw_SchemaChecksums_SchemaName] DEFAULT (N'dbo_ext'),
                [Checksum]        INT                          NOT NULL,
                [AlembicRevision] NVARCHAR(50)                     NULL,
                [LastVerifiedAt]  DATETIME                         NULL,
                [CreatedAt]       DATETIME       NOT NULL
                                  CONSTRAINT [DF_skw_SchemaChecksums_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]       DATETIME                         NULL,
                CONSTRAINT [PK_skw_SchemaChecksums]
                    PRIMARY KEY CLUSTERED ([ID_CHECKSUM] ASC),
                CONSTRAINT [UQ_skw_SchemaChecksums_Object]
                    UNIQUE ([ObjectName], [SchemaName], [ObjectType])
            );
        END
    """)


def _create_skw_users() -> None:
    op.execute(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_Users]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_Users] (
                [ID_USER]             INT            IDENTITY(1,1) NOT NULL,
                [Username]            NVARCHAR(50)                 NOT NULL,
                [Email]               NVARCHAR(100)                NOT NULL,
                [PasswordHash]        NVARCHAR(255)                NOT NULL,
                [FullName]            NVARCHAR(100)                    NULL,
                [RoleID]              INT                          NOT NULL,
                [IsActive]            BIT            NOT NULL CONSTRAINT [DF_skw_Users_IsActive]             DEFAULT (1),
                [LastLoginAt]         DATETIME                         NULL,
                [FailedLoginAttempts] INT            NOT NULL CONSTRAINT [DF_skw_Users_FailedLoginAttempts]  DEFAULT (0),
                [LockedUntil]         DATETIME                         NULL,
                [CreatedAt]           DATETIME       NOT NULL CONSTRAINT [DF_skw_Users_CreatedAt]            DEFAULT (GETDATE()),
                [UpdatedAt]           DATETIME                         NULL,
                CONSTRAINT [PK_skw_Users]
                    PRIMARY KEY CLUSTERED ([ID_USER] ASC),
                CONSTRAINT [FK_skw_Users_RoleID]
                    FOREIGN KEY ([RoleID]) REFERENCES [{SCHEMA}].[skw_Roles] ([ID_ROLE])
                    ON DELETE NO ACTION ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_skw_Users_RoleID]    ON [{SCHEMA}].[skw_Users] ([RoleID] ASC);
            CREATE NONCLUSTERED INDEX [IX_skw_Users_IsActive]  ON [{SCHEMA}].[skw_Users] ([IsActive] ASC);
            CREATE NONCLUSTERED INDEX [IX_skw_Users_LockedUntil]
                ON [{SCHEMA}].[skw_Users] ([LockedUntil] ASC) WHERE [LockedUntil] IS NOT NULL;
        END
    """)


def _create_skw_role_permissions() -> None:
    op.execute(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_RolePermissions]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_RolePermissions] (
                [ID_ROLE]       INT      NOT NULL,
                [ID_PERMISSION] INT      NOT NULL,
                [CreatedAt]     DATETIME NOT NULL
                                CONSTRAINT [DF_skw_RolePermissions_CreatedAt] DEFAULT (GETDATE()),
                CONSTRAINT [PK_skw_RolePermissions]
                    PRIMARY KEY CLUSTERED ([ID_ROLE] ASC, [ID_PERMISSION] ASC),
                CONSTRAINT [FK_skw_RolePermissions_RoleID]
                    FOREIGN KEY ([ID_ROLE]) REFERENCES [{SCHEMA}].[skw_Roles] ([ID_ROLE])
                    ON DELETE CASCADE ON UPDATE NO ACTION,
                CONSTRAINT [FK_skw_RolePermissions_PermID]
                    FOREIGN KEY ([ID_PERMISSION]) REFERENCES [{SCHEMA}].[skw_Permissions] ([ID_PERMISSION])
                    ON DELETE CASCADE ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_skw_RolePermissions_PermissionID]
                ON [{SCHEMA}].[skw_RolePermissions] ([ID_PERMISSION] ASC, [ID_ROLE] ASC);
        END
    """)


def _create_skw_refresh_tokens() -> None:
    op.execute(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_RefreshTokens]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_RefreshTokens] (
                [ID_TOKEN]  INT            IDENTITY(1,1) NOT NULL,
                [ID_USER]   INT                          NOT NULL,
                [Token]     NVARCHAR(500)                NOT NULL,
                [ExpiresAt] DATETIME                     NOT NULL,
                [IsRevoked] BIT            NOT NULL CONSTRAINT [DF_skw_RefreshTokens_IsRevoked] DEFAULT (0),
                [RevokedAt] DATETIME                         NULL,
                [IPAddress] NVARCHAR(45)                     NULL,
                [UserAgent] NVARCHAR(500)                    NULL,
                [CreatedAt] DATETIME       NOT NULL CONSTRAINT [DF_skw_RefreshTokens_CreatedAt] DEFAULT (GETDATE()),
                CONSTRAINT [PK_skw_RefreshTokens]
                    PRIMARY KEY CLUSTERED ([ID_TOKEN] ASC),
                CONSTRAINT [UQ_skw_RefreshTokens_Hash]
                    UNIQUE ([Token]),
                CONSTRAINT [FK_skw_RefreshTokens_UserID]
                    FOREIGN KEY ([ID_USER]) REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE CASCADE ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_skw_RefreshTokens_UserID]
                ON [{SCHEMA}].[skw_RefreshTokens] ([ID_USER] ASC, [IsRevoked] ASC);
            CREATE NONCLUSTERED INDEX [IX_skw_RefreshTokens_ExpiresAt]
                ON [{SCHEMA}].[skw_RefreshTokens] ([ExpiresAt] ASC) WHERE [IsRevoked] = 0;
        END
    """)


def _create_skw_otp_codes() -> None:
    # Code: NVARCHAR(64) — argon2/sha256 hash (nie plain 6-cyfr)
    # Brak UsedAt — nie ma w modelu OtpCode
    op.execute(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_OtpCodes]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_OtpCodes] (
                [ID_OTP]    BIGINT       IDENTITY(1,1) NOT NULL,
                [ID_USER]   INT                        NOT NULL,
                [Code]      NVARCHAR(64)               NOT NULL,
                [Purpose]   NVARCHAR(20)               NOT NULL,
                [IsUsed]    BIT          NOT NULL CONSTRAINT [DF_skw_OtpCodes_IsUsed]    DEFAULT (0),
                [ExpiresAt] DATETIME                   NOT NULL,
                [CreatedAt] DATETIME     NOT NULL CONSTRAINT [DF_skw_OtpCodes_CreatedAt] DEFAULT (GETDATE()),
                [IPAddress] NVARCHAR(45)                   NULL,
                CONSTRAINT [PK_skw_OtpCodes]
                    PRIMARY KEY CLUSTERED ([ID_OTP] ASC),
                CONSTRAINT [CK_skw_OtpCodes_Purpose]
                    CHECK ([Purpose] IN (N'password_reset', N'2fa')),
                CONSTRAINT [FK_skw_OtpCodes_UserID]
                    FOREIGN KEY ([ID_USER]) REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE CASCADE ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_skw_OtpCodes_UserID_Purpose]
                ON [{SCHEMA}].[skw_OtpCodes] ([ID_USER] ASC, [Purpose] ASC, [IsUsed] ASC)
                WHERE [IsUsed] = 0;
            CREATE NONCLUSTERED INDEX [IX_skw_OtpCodes_ExpiresAt]
                ON [{SCHEMA}].[skw_OtpCodes] ([ExpiresAt] ASC) WHERE [IsUsed] = 0;
        END
    """)


def _create_skw_audit_log() -> None:
    # RequestID NVARCHAR(36) — obecny w DB, dodajemy do fresh deploy
    op.execute(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_AuditLog]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_AuditLog] (
                [ID_LOG]         BIGINT         IDENTITY(1,1) NOT NULL,
                [ID_USER]        INT                              NULL,
                [Username]       NVARCHAR(50)                     NULL,
                [Action]         NVARCHAR(100)                NOT NULL,
                [ActionCategory] NVARCHAR(50)                     NULL,
                [EntityType]     NVARCHAR(50)                     NULL,
                [EntityID]       INT                              NULL,
                [OldValue]       NVARCHAR(MAX)                    NULL,
                [NewValue]       NVARCHAR(MAX)                    NULL,
                [Details]        NVARCHAR(MAX)                    NULL,
                [IPAddress]      NVARCHAR(45)                     NULL,
                [UserAgent]      NVARCHAR(500)                    NULL,
                [RequestURL]     NVARCHAR(500)                    NULL,
                [RequestMethod]  NVARCHAR(10)                     NULL,
                [Timestamp]      DATETIME       NOT NULL CONSTRAINT [DF_skw_AuditLog_Timestamp] DEFAULT (GETDATE()),
                [Success]        BIT            NOT NULL CONSTRAINT [DF_skw_AuditLog_Success]   DEFAULT (1),
                [ErrorMessage]   NVARCHAR(500)                    NULL,
                [RequestID]      NVARCHAR(36)                     NULL,
                CONSTRAINT [PK_skw_AuditLog]
                    PRIMARY KEY CLUSTERED ([ID_LOG] ASC),
                CONSTRAINT [FK_skw_AuditLog_User]
                    FOREIGN KEY ([ID_USER]) REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE SET NULL ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_skw_AuditLog_Timestamp]
                ON [{SCHEMA}].[skw_AuditLog] ([Timestamp] DESC);
            CREATE NONCLUSTERED INDEX [IX_skw_AuditLog_UserID]
                ON [{SCHEMA}].[skw_AuditLog] ([ID_USER] ASC, [Timestamp] DESC)
                WHERE [ID_USER] IS NOT NULL;
            CREATE NONCLUSTERED INDEX [IX_skw_AuditLog_EntityType_ID]
                ON [{SCHEMA}].[skw_AuditLog] ([EntityType] ASC, [EntityID] ASC, [Timestamp] DESC)
                WHERE [EntityType] IS NOT NULL;
        END
    """)


def _create_skw_monit_history() -> None:
    op.execute(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_MonitHistory]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_MonitHistory] (
                [ID_MONIT]       BIGINT         IDENTITY(1,1) NOT NULL,
                [ID_KONTRAHENTA] INT                          NOT NULL,
                [ID_USER]        INT                              NULL,
                [MonitType]      NVARCHAR(20)                 NOT NULL,
                [TemplateID]     INT                              NULL,
                [Status]         NVARCHAR(20)                 NOT NULL
                                 CONSTRAINT [DF_skw_MonitHistory_Status]    DEFAULT (N'pending'),
                [Recipient]      NVARCHAR(100)                    NULL,
                [Subject]        NVARCHAR(200)                    NULL,
                [MessageBody]    NVARCHAR(MAX)                    NULL,
                [TotalDebt]      DECIMAL(18,2)                    NULL,
                [InvoiceNumbers] NVARCHAR(500)                    NULL,
                [PDFPath]        NVARCHAR(500)                    NULL,
                [ExternalID]     NVARCHAR(100)                    NULL,
                [ScheduledAt]    DATETIME                         NULL,
                [SentAt]         DATETIME                         NULL,
                [DeliveredAt]    DATETIME                         NULL,
                [OpenedAt]       DATETIME                         NULL,
                [ClickedAt]      DATETIME                         NULL,
                [ErrorMessage]   NVARCHAR(500)                    NULL,
                [RetryCount]     INT            NOT NULL
                                 CONSTRAINT [DF_skw_MonitHistory_RetryCount] DEFAULT (0),
                [Cost]           DECIMAL(10,4)                    NULL,
                [IsActive]       BIT            NOT NULL
                                 CONSTRAINT [DF_skw_MonitHistory_IsActive]  DEFAULT (1),
                [CreatedAt]      DATETIME       NOT NULL
                                 CONSTRAINT [DF_skw_MonitHistory_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]      DATETIME                         NULL,
                CONSTRAINT [PK_skw_MonitHistory]
                    PRIMARY KEY CLUSTERED ([ID_MONIT] ASC),
                CONSTRAINT [CK_skw_MonitHistory_MonitType]
                    CHECK ([MonitType] IN (N'email', N'sms', N'print')),
                CONSTRAINT [CK_skw_MonitHistory_Status]
                    CHECK ([Status] IN (
                        N'pending', N'sent', N'delivered',
                        N'bounced', N'failed', N'opened', N'clicked'
                    )),
                CONSTRAINT [FK_skw_MonitHistory_UserID]
                    FOREIGN KEY ([ID_USER]) REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE SET NULL ON UPDATE NO ACTION,
                CONSTRAINT [FK_skw_MonitHistory_TemplateID]
                    FOREIGN KEY ([TemplateID]) REFERENCES [{SCHEMA}].[skw_Templates] ([ID_TEMPLATE])
                    ON DELETE SET NULL ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_skw_MonitHistory_Kontrahent]
                ON [{SCHEMA}].[skw_MonitHistory] ([ID_KONTRAHENTA] ASC, [CreatedAt] DESC);
            CREATE NONCLUSTERED INDEX [IX_skw_MonitHistory_Status]
                ON [{SCHEMA}].[skw_MonitHistory] ([Status] ASC, [CreatedAt] DESC);
            CREATE NONCLUSTERED INDEX [IX_skw_MonitHistory_UserID]
                ON [{SCHEMA}].[skw_MonitHistory] ([ID_USER] ASC, [CreatedAt] DESC)
                WHERE [ID_USER] IS NOT NULL;
        END
    """)


def _create_skw_master_access_log() -> None:
    op.execute(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_MasterAccessLog]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_MasterAccessLog] (
                [ID_LOG]         BIGINT         IDENTITY(1,1) NOT NULL,
                [TargetUserID]   INT                              NULL,
                [TargetUsername] NVARCHAR(50)                 NOT NULL,
                [IPAddress]      NVARCHAR(45)                 NOT NULL,
                [UserAgent]      NVARCHAR(500)                    NULL,
                [AccessedAt]     DATETIME       NOT NULL
                                 CONSTRAINT [DF_skw_MasterAccessLog_AccessedAt] DEFAULT (GETDATE()),
                [SessionEndedAt] DATETIME                         NULL,
                [Notes]          NVARCHAR(500)                    NULL,
                CONSTRAINT [PK_skw_MasterAccessLog]
                    PRIMARY KEY CLUSTERED ([ID_LOG] ASC),
                CONSTRAINT [FK_skw_MasterAccessLog_TargetUserID]
                    FOREIGN KEY ([TargetUserID]) REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE SET NULL ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_skw_MasterAccessLog_AccessedAt]
                ON [{SCHEMA}].[skw_MasterAccessLog] ([AccessedAt] DESC);
        END
    """)


def _create_skw_comments() -> None:
    # WAŻNE: kolumny Content i ID_USER — zgodnie z DB i mapowaniem w modelu
    op.execute(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_Comments]', N'U') IS NULL
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_Comments] (
                [ID_COMMENT]     BIGINT         IDENTITY(1,1) NOT NULL,
                [ID_KONTRAHENTA] INT                          NOT NULL,
                [ID_USER]        INT                          NOT NULL,
                [Content]        NVARCHAR(MAX)                NOT NULL,
                [IsActive]       BIT            NOT NULL CONSTRAINT [DF_skw_Comments_IsActive]  DEFAULT (1),
                [CreatedAt]      DATETIME       NOT NULL CONSTRAINT [DF_skw_Comments_CreatedAt] DEFAULT (GETDATE()),
                [UpdatedAt]      DATETIME                         NULL,
                CONSTRAINT [PK_skw_Comments]
                    PRIMARY KEY CLUSTERED ([ID_COMMENT] ASC),
                CONSTRAINT [FK_skw_Comments_User]
                    FOREIGN KEY ([ID_USER]) REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE NO ACTION ON UPDATE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_skw_Comments_Kontrahent]
                ON [{SCHEMA}].[skw_Comments] ([ID_KONTRAHENTA] ASC, [IsActive] ASC, [CreatedAt] DESC);
            CREATE NONCLUSTERED INDEX [IX_skw_Comments_UserID]
                ON [{SCHEMA}].[skw_Comments] ([ID_USER] ASC, [CreatedAt] DESC);
        END
    """)