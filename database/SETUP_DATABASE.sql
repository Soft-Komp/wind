-- =============================================================================
-- SETUP_DATABASE.sql
-- System Windykacja — jednorazowa inicjalizacja bazy PRZED pierwszym
-- uruchomieniem kontenera Docker.
--
-- URUCHOM TEN SKRYPT RAZ przez SSMS / DBeaver / Azure Data Studio
-- jako użytkownik SA lub DBA z prawem CREATE SCHEMA.
--
-- Skrypt jest IDEMPOTENTNY — bezpieczne uruchomienie wielokrotnie.
-- Każda operacja sprawdza IF NOT EXISTS przed wykonaniem.
--
-- Po wykonaniu tego skryptu:
--   1. Ustaw w .env:  ALEMBIC_MODE=stamp
--   2. Uruchom:       docker compose up
--   Alembic oznaczy bazę jako aktualną bez próby tworzenia tabel ponownie.
--
-- Wersja: 1.0.0 | Data: 2026-03-03
-- =============================================================================

USE [GPGKJASLO];  -- ← zmień na nazwę swojej bazy jeśli inna
GO

SET NOCOUNT ON;
PRINT '============================================================';
PRINT ' System Windykacja — inicjalizacja schematu dbo_ext';
PRINT ' Baza:  ' + DB_NAME();
PRINT ' Czas:  ' + CONVERT(NVARCHAR, GETDATE(), 120);
PRINT '============================================================';
GO

-- =============================================================================
-- KROK 1: Schemat dbo_ext
-- Musi być pierwszy — wszystkie tabele skw_* są w tym schemacie.
-- CREATE SCHEMA wymaga osobnego batcha (sp_executesql).
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dbo_ext')
BEGIN
    EXEC sp_executesql N'CREATE SCHEMA [dbo_ext] AUTHORIZATION dbo';
    PRINT '[KROK 1] Schemat dbo_ext: UTWORZONY';
END
ELSE
    PRINT '[KROK 1] Schemat dbo_ext: już istnieje — pominięto';
GO

-- Weryfikacja
IF SCHEMA_ID(N'dbo_ext') IS NULL
BEGIN
    PRINT 'BŁĄD KRYTYCZNY: Nie można utworzyć schematu dbo_ext!';
    THROW 50001, 'Schemat dbo_ext nie istnieje po próbie utworzenia.', 1;
END
GO

-- =============================================================================
-- KROK 2: skw_Roles
-- =============================================================================
BEGIN TRANSACTION;
BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo_ext' AND t.name='skw_Roles')
    BEGIN
        CREATE TABLE [dbo_ext].[skw_Roles] (
            [ID_ROLE]     INT           IDENTITY(1,1) NOT NULL,
            [RoleName]    NVARCHAR(50)                NOT NULL,
            [Description] NVARCHAR(200)                   NULL,
            [IsActive]    BIT           NOT NULL CONSTRAINT [DF_skw_Roles_IsActive]  DEFAULT (1),
            [CreatedAt]   DATETIME      NOT NULL CONSTRAINT [DF_skw_Roles_CreatedAt] DEFAULT (GETDATE()),
            [UpdatedAt]   DATETIME                        NULL,
            CONSTRAINT [PK_skw_Roles]         PRIMARY KEY CLUSTERED ([ID_ROLE] ASC),
            CONSTRAINT [UQ_skw_Roles_RoleName] UNIQUE ([RoleName])
        );
        CREATE NONCLUSTERED INDEX [IX_skw_Roles_IsActive] ON [dbo_ext].[skw_Roles] ([IsActive] ASC);
        PRINT '[KROK 2] skw_Roles: UTWORZONO';
    END
    ELSE PRINT '[KROK 2] skw_Roles: już istnieje';
    COMMIT;
END TRY
BEGIN CATCH ROLLBACK; PRINT '[KROK 2] BŁĄD: ' + ERROR_MESSAGE(); THROW; END CATCH
GO

-- =============================================================================
-- KROK 3: skw_Permissions
-- =============================================================================
BEGIN TRANSACTION;
BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo_ext' AND t.name='skw_Permissions')
    BEGIN
        CREATE TABLE [dbo_ext].[skw_Permissions] (
            [ID_PERMISSION]  INT            IDENTITY(1,1) NOT NULL,
            [PermissionName] NVARCHAR(100)                NOT NULL,
            [Description]    NVARCHAR(200)                    NULL,
            [Category]       NVARCHAR(50)                     NULL,
            [IsActive]       BIT            NOT NULL CONSTRAINT [DF_skw_Permissions_IsActive]  DEFAULT (1),
            [CreatedAt]      DATETIME       NOT NULL CONSTRAINT [DF_skw_Permissions_CreatedAt] DEFAULT (GETDATE()),
            [UpdatedAt]      DATETIME                         NULL,
            CONSTRAINT [PK_skw_Permissions]              PRIMARY KEY CLUSTERED ([ID_PERMISSION] ASC),
            CONSTRAINT [UQ_skw_Permissions_PermissionName] UNIQUE ([PermissionName]),
            CONSTRAINT [CK_skw_Permissions_Category]       CHECK ([Category] IN (
                N'auth', N'users', N'roles', N'debtors', N'monits',
                N'comments', N'pdf', N'reports', N'snapshots', N'audit', N'system'
            ))
        );
        CREATE NONCLUSTERED INDEX [IX_skw_Permissions_Category] ON [dbo_ext].[skw_Permissions] ([Category] ASC, [IsActive] ASC);
        PRINT '[KROK 3] skw_Permissions: UTWORZONO';
    END
    ELSE PRINT '[KROK 3] skw_Permissions: już istnieje';
    COMMIT;
END TRY
BEGIN CATCH ROLLBACK; PRINT '[KROK 3] BŁĄD: ' + ERROR_MESSAGE(); THROW; END CATCH
GO

-- =============================================================================
-- KROK 4: skw_Templates
-- (musi być przed skw_MonitHistory — FK dependency)
-- =============================================================================
BEGIN TRANSACTION;
BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo_ext' AND t.name='skw_Templates')
    BEGIN
        CREATE TABLE [dbo_ext].[skw_Templates] (
            [ID_TEMPLATE]   INT            IDENTITY(1,1) NOT NULL,
            [TemplateName]  NVARCHAR(100)                NOT NULL,
            [TemplateType]  NVARCHAR(20)                 NOT NULL,
            [Subject]       NVARCHAR(200)                    NULL,
            [Body]          NVARCHAR(MAX)                NOT NULL,
            [IsActive]      BIT            NOT NULL CONSTRAINT [DF_skw_Templates_IsActive]  DEFAULT (1),
            [CreatedAt]     DATETIME       NOT NULL CONSTRAINT [DF_skw_Templates_CreatedAt] DEFAULT (GETDATE()),
            [UpdatedAt]     DATETIME                         NULL,
            CONSTRAINT [PK_skw_Templates]              PRIMARY KEY CLUSTERED ([ID_TEMPLATE] ASC),
            CONSTRAINT [UQ_skw_Templates_TemplateName]  UNIQUE ([TemplateName]),
            CONSTRAINT [CK_skw_Templates_TemplateType]  CHECK ([TemplateType] IN (N'email', N'sms', N'print'))
        );
        CREATE NONCLUSTERED INDEX [IX_skw_Templates_Type_Active] ON [dbo_ext].[skw_Templates] ([TemplateType] ASC, [IsActive] ASC);
        PRINT '[KROK 4] skw_Templates: UTWORZONO';
    END
    ELSE PRINT '[KROK 4] skw_Templates: już istnieje';
    COMMIT;
END TRY
BEGIN CATCH ROLLBACK; PRINT '[KROK 4] BŁĄD: ' + ERROR_MESSAGE(); THROW; END CATCH
GO

-- =============================================================================
-- KROK 5: skw_SystemConfig
-- =============================================================================
BEGIN TRANSACTION;
BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo_ext' AND t.name='skw_SystemConfig')
    BEGIN
        CREATE TABLE [dbo_ext].[skw_SystemConfig] (
            [ID_CONFIG]   INT            IDENTITY(1,1) NOT NULL,
            [ConfigKey]   NVARCHAR(100)                NOT NULL,
            [ConfigValue] NVARCHAR(MAX)                    NULL,
            [Description] NVARCHAR(500)                    NULL,
            [IsActive]    BIT            NOT NULL CONSTRAINT [DF_skw_SystemConfig_IsActive]  DEFAULT (1),
            [CreatedAt]   DATETIME       NOT NULL CONSTRAINT [DF_skw_SystemConfig_CreatedAt] DEFAULT (GETDATE()),
            [UpdatedAt]   DATETIME                         NULL,
            CONSTRAINT [PK_skw_SystemConfig]          PRIMARY KEY CLUSTERED ([ID_CONFIG] ASC),
            CONSTRAINT [UQ_skw_SystemConfig_ConfigKey] UNIQUE ([ConfigKey])
        );
        CREATE NONCLUSTERED INDEX [IX_skw_SystemConfig_IsActive] ON [dbo_ext].[skw_SystemConfig] ([IsActive] ASC, [ConfigKey] ASC);
        PRINT '[KROK 5] skw_SystemConfig: UTWORZONO';
    END
    ELSE PRINT '[KROK 5] skw_SystemConfig: już istnieje';
    COMMIT;
END TRY
BEGIN CATCH ROLLBACK; PRINT '[KROK 5] BŁĄD: ' + ERROR_MESSAGE(); THROW; END CATCH
GO

-- =============================================================================
-- KROK 6: skw_SchemaChecksums
-- =============================================================================
BEGIN TRANSACTION;
BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo_ext' AND t.name='skw_SchemaChecksums')
    BEGIN
        CREATE TABLE [dbo_ext].[skw_SchemaChecksums] (
            [ID_CHECKSUM]     INT           IDENTITY(1,1) NOT NULL,
            [ObjectName]      NVARCHAR(200)               NOT NULL,
            [ObjectType]      NVARCHAR(50)                NOT NULL,
            [SchemaName]      NVARCHAR(20)                NOT NULL CONSTRAINT [DF_skw_SchemaChecksums_SchemaName] DEFAULT ('dbo_ext'),
            [Checksum]        INT                         NOT NULL,
            [AlembicRevision] NVARCHAR(50)                    NULL,
            [LastVerifiedAt]  DATETIME                        NULL,
            [CreatedAt]       DATETIME      NOT NULL CONSTRAINT [DF_skw_SchemaChecksums_CreatedAt] DEFAULT (GETDATE()),
            [UpdatedAt]       DATETIME                        NULL,
            CONSTRAINT [PK_skw_SchemaChecksums]    PRIMARY KEY CLUSTERED ([ID_CHECKSUM] ASC),
            CONSTRAINT [CK_skw_SchemaChecksums_ObjectType]  CHECK ([ObjectType] IN ('VIEW', 'PROCEDURE', 'INDEX')),
            CONSTRAINT [CK_skw_SchemaChecksums_SchemaName]  CHECK ([SchemaName] IN ('dbo', 'dbo_ext')),
            CONSTRAINT [UQ_skw_SchemaChecksums_Object]      UNIQUE ([ObjectName], [SchemaName], [ObjectType])
        );
        PRINT '[KROK 6] skw_SchemaChecksums: UTWORZONO';
    END
    ELSE PRINT '[KROK 6] skw_SchemaChecksums: już istnieje';
    COMMIT;
END TRY
BEGIN CATCH ROLLBACK; PRINT '[KROK 6] BŁĄD: ' + ERROR_MESSAGE(); THROW; END CATCH
GO

-- =============================================================================
-- KROK 7: skw_Users (FK → skw_Roles)
-- =============================================================================
BEGIN TRANSACTION;
BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo_ext' AND t.name='skw_Users')
    BEGIN
        CREATE TABLE [dbo_ext].[skw_Users] (
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
            CONSTRAINT [PK_skw_Users]                   PRIMARY KEY CLUSTERED ([ID_USER] ASC),
            CONSTRAINT [UQ_skw_Users_Username]           UNIQUE ([Username]),
            CONSTRAINT [UQ_skw_Users_Email]              UNIQUE ([Email]),
            CONSTRAINT [CK_skw_Users_FailedLoginAttempts] CHECK ([FailedLoginAttempts] >= 0),
            CONSTRAINT [FK_skw_Users_RoleID]             FOREIGN KEY ([RoleID])
                REFERENCES [dbo_ext].[skw_Roles] ([ID_ROLE]) ON DELETE NO ACTION ON UPDATE NO ACTION
        );
        CREATE NONCLUSTERED INDEX [IX_skw_Users_RoleID]    ON [dbo_ext].[skw_Users] ([RoleID] ASC);
        CREATE NONCLUSTERED INDEX [IX_skw_Users_IsActive]  ON [dbo_ext].[skw_Users] ([IsActive] ASC);
        CREATE NONCLUSTERED INDEX [IX_skw_Users_LockedUntil] ON [dbo_ext].[skw_Users] ([LockedUntil] ASC) WHERE [LockedUntil] IS NOT NULL;
        PRINT '[KROK 7] skw_Users: UTWORZONO';
    END
    ELSE PRINT '[KROK 7] skw_Users: już istnieje';
    COMMIT;
END TRY
BEGIN CATCH ROLLBACK; PRINT '[KROK 7] BŁĄD: ' + ERROR_MESSAGE(); THROW; END CATCH
GO

-- =============================================================================
-- KROK 8: skw_RolePermissions (FK → skw_Roles, skw_Permissions)
-- =============================================================================
BEGIN TRANSACTION;
BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo_ext' AND t.name='skw_RolePermissions')
    BEGIN
        CREATE TABLE [dbo_ext].[skw_RolePermissions] (
            [ID_ROLE]       INT      NOT NULL,
            [ID_PERMISSION] INT      NOT NULL,
            [CreatedAt]     DATETIME NOT NULL CONSTRAINT [DF_skw_RolePermissions_CreatedAt] DEFAULT (GETDATE()),
            CONSTRAINT [PK_skw_RolePermissions]          PRIMARY KEY CLUSTERED ([ID_ROLE] ASC, [ID_PERMISSION] ASC),
            CONSTRAINT [FK_skw_RolePermissions_RoleID]   FOREIGN KEY ([ID_ROLE])       REFERENCES [dbo_ext].[skw_Roles]       ([ID_ROLE])       ON DELETE CASCADE,
            CONSTRAINT [FK_skw_RolePermissions_PermID]   FOREIGN KEY ([ID_PERMISSION]) REFERENCES [dbo_ext].[skw_Permissions] ([ID_PERMISSION]) ON DELETE CASCADE
        );
        CREATE NONCLUSTERED INDEX [IX_skw_RolePermissions_PermissionID] ON [dbo_ext].[skw_RolePermissions] ([ID_PERMISSION] ASC, [ID_ROLE] ASC);
        PRINT '[KROK 8] skw_RolePermissions: UTWORZONO';
    END
    ELSE PRINT '[KROK 8] skw_RolePermissions: już istnieje';
    COMMIT;
END TRY
BEGIN CATCH ROLLBACK; PRINT '[KROK 8] BŁĄD: ' + ERROR_MESSAGE(); THROW; END CATCH
GO

-- =============================================================================
-- KROK 9: skw_RefreshTokens (FK → skw_Users CASCADE DELETE)
-- =============================================================================
BEGIN TRANSACTION;
BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo_ext' AND t.name='skw_RefreshTokens')
    BEGIN
        CREATE TABLE [dbo_ext].[skw_RefreshTokens] (
            [ID_TOKEN]    BIGINT         IDENTITY(1,1) NOT NULL,
            [ID_USER]     INT                          NOT NULL,
            [TokenHash]   NVARCHAR(255)                NOT NULL,
            [IsRevoked]   BIT            NOT NULL CONSTRAINT [DF_skw_RefreshTokens_IsRevoked]  DEFAULT (0),
            [ExpiresAt]   DATETIME                     NOT NULL,
            [RevokedAt]   DATETIME                         NULL,
            [UserAgent]   NVARCHAR(500)                    NULL,
            [IPAddress]   NVARCHAR(45)                     NULL,
            [CreatedAt]   DATETIME       NOT NULL CONSTRAINT [DF_skw_RefreshTokens_CreatedAt] DEFAULT (GETDATE()),
            CONSTRAINT [PK_skw_RefreshTokens]         PRIMARY KEY CLUSTERED ([ID_TOKEN] ASC),
            CONSTRAINT [UQ_skw_RefreshTokens_Hash]     UNIQUE ([TokenHash]),
            CONSTRAINT [FK_skw_RefreshTokens_UserID]   FOREIGN KEY ([ID_USER]) REFERENCES [dbo_ext].[skw_Users] ([ID_USER]) ON DELETE CASCADE
        );
        CREATE NONCLUSTERED INDEX [IX_skw_RefreshTokens_UserID]    ON [dbo_ext].[skw_RefreshTokens] ([ID_USER] ASC, [IsRevoked] ASC);
        CREATE NONCLUSTERED INDEX [IX_skw_RefreshTokens_ExpiresAt] ON [dbo_ext].[skw_RefreshTokens] ([ExpiresAt] ASC) WHERE [IsRevoked] = 0;
        PRINT '[KROK 9] skw_RefreshTokens: UTWORZONO';
    END
    ELSE PRINT '[KROK 9] skw_RefreshTokens: już istnieje';
    COMMIT;
END TRY
BEGIN CATCH ROLLBACK; PRINT '[KROK 9] BŁĄD: ' + ERROR_MESSAGE(); THROW; END CATCH
GO

-- =============================================================================
-- KROK 10: skw_OtpCodes (FK → skw_Users CASCADE DELETE)
-- =============================================================================
BEGIN TRANSACTION;
BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo_ext' AND t.name='skw_OtpCodes')
    BEGIN
        CREATE TABLE [dbo_ext].[skw_OtpCodes] (
            [ID_OTP]    BIGINT         IDENTITY(1,1) NOT NULL,
            [ID_USER]   INT                          NOT NULL,
            [Code]      NVARCHAR(64)                 NOT NULL,
            [Purpose]   NVARCHAR(50)                 NOT NULL,
            [IsUsed]    BIT            NOT NULL CONSTRAINT [DF_skw_OtpCodes_IsUsed]    DEFAULT (0),
            [ExpiresAt] DATETIME                     NOT NULL,
            [UsedAt]    DATETIME                         NULL,
            [CreatedAt] DATETIME       NOT NULL CONSTRAINT [DF_skw_OtpCodes_CreatedAt] DEFAULT (GETDATE()),
            CONSTRAINT [PK_skw_OtpCodes]       PRIMARY KEY CLUSTERED ([ID_OTP] ASC),
            CONSTRAINT [FK_skw_OtpCodes_UserID] FOREIGN KEY ([ID_USER]) REFERENCES [dbo_ext].[skw_Users] ([ID_USER]) ON DELETE CASCADE
        );
        CREATE NONCLUSTERED INDEX [IX_skw_OtpCodes_UserID_Purpose] ON [dbo_ext].[skw_OtpCodes] ([ID_USER] ASC, [Purpose] ASC, [IsUsed] ASC) WHERE [IsUsed] = 0;
        CREATE NONCLUSTERED INDEX [IX_skw_OtpCodes_ExpiresAt]      ON [dbo_ext].[skw_OtpCodes] ([ExpiresAt] ASC) WHERE [IsUsed] = 0;
        PRINT '[KROK 10] skw_OtpCodes: UTWORZONO';
    END
    ELSE PRINT '[KROK 10] skw_OtpCodes: już istnieje';
    COMMIT;
END TRY
BEGIN CATCH ROLLBACK; PRINT '[KROK 10] BŁĄD: ' + ERROR_MESSAGE(); THROW; END CATCH
GO

-- =============================================================================
-- KROK 11: skw_AuditLog (FK → skw_Users SET NULL)
-- =============================================================================
BEGIN TRANSACTION;
BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo_ext' AND t.name='skw_AuditLog')
    BEGIN
        CREATE TABLE [dbo_ext].[skw_AuditLog] (
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
            CONSTRAINT [PK_skw_AuditLog]      PRIMARY KEY CLUSTERED ([ID_LOG] ASC),
            CONSTRAINT [FK_skw_AuditLog_User]  FOREIGN KEY ([ID_USER]) REFERENCES [dbo_ext].[skw_Users] ([ID_USER]) ON DELETE SET NULL
        );
        CREATE NONCLUSTERED INDEX [IX_skw_AuditLog_UserID]     ON [dbo_ext].[skw_AuditLog] ([ID_USER] ASC);
        CREATE NONCLUSTERED INDEX [IX_skw_AuditLog_Timestamp]  ON [dbo_ext].[skw_AuditLog] ([Timestamp] DESC);
        CREATE NONCLUSTERED INDEX [IX_skw_AuditLog_Action]     ON [dbo_ext].[skw_AuditLog] ([Action] ASC);
        CREATE NONCLUSTERED INDEX [IX_skw_AuditLog_EntityType] ON [dbo_ext].[skw_AuditLog] ([EntityType] ASC, [EntityID] ASC);
        CREATE NONCLUSTERED INDEX [IX_skw_AuditLog_Success]    ON [dbo_ext].[skw_AuditLog] ([Success] ASC);
        PRINT '[KROK 11] skw_AuditLog: UTWORZONO';
    END
    ELSE PRINT '[KROK 11] skw_AuditLog: już istnieje';
    COMMIT;
END TRY
BEGIN CATCH ROLLBACK; PRINT '[KROK 11] BŁĄD: ' + ERROR_MESSAGE(); THROW; END CATCH
GO

-- =============================================================================
-- KROK 12: skw_MonitHistory (FK → skw_Users SET NULL, skw_Templates SET NULL)
-- =============================================================================
BEGIN TRANSACTION;
BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo_ext' AND t.name='skw_MonitHistory')
    BEGIN
        CREATE TABLE [dbo_ext].[skw_MonitHistory] (
            [ID_MONIT]       BIGINT          IDENTITY(1,1) NOT NULL,
            [ID_KONTRAHENTA] INT                           NOT NULL,
            [ID_USER]        INT                               NULL,
            [MonitType]      NVARCHAR(20)                  NOT NULL,
            [TemplateID]     INT                               NULL,
            [Status]         NVARCHAR(20)   NOT NULL CONSTRAINT [DF_skw_MonitHistory_Status] DEFAULT ('pending'),
            [Recipient]      NVARCHAR(100)                     NULL,
            [Subject]        NVARCHAR(200)                     NULL,
            [MessageBody]    NVARCHAR(MAX)                     NULL,
            [TotalDebt]      DECIMAL(18,2)                     NULL,
            [InvoiceNumbers] NVARCHAR(500)                     NULL,
            [PDFPath]        NVARCHAR(500)                     NULL,
            [ExternalID]     NVARCHAR(100)                     NULL,
            [ScheduledAt]    DATETIME                          NULL,
            [SentAt]         DATETIME                          NULL,
            [DeliveredAt]    DATETIME                          NULL,
            [OpenedAt]       DATETIME                          NULL,
            [ClickedAt]      DATETIME                          NULL,
            [ErrorMessage]   NVARCHAR(500)                     NULL,
            [RetryCount]     INT            NOT NULL CONSTRAINT [DF_skw_MonitHistory_RetryCount] DEFAULT (0),
            [Cost]           DECIMAL(10,4)                     NULL,
            [CreatedAt]      DATETIME       NOT NULL CONSTRAINT [DF_skw_MonitHistory_CreatedAt]  DEFAULT (GETDATE()),
            [UpdatedAt]      DATETIME                          NULL,
            CONSTRAINT [PK_skw_MonitHistory]              PRIMARY KEY CLUSTERED ([ID_MONIT] ASC),
            CONSTRAINT [CK_skw_MonitHistory_MonitType]    CHECK ([MonitType] IN (N'email', N'sms', N'print')),
            CONSTRAINT [CK_skw_MonitHistory_Status]       CHECK ([Status] IN (N'pending',N'sent',N'delivered',N'bounced',N'failed',N'opened',N'clicked')),
            CONSTRAINT [FK_skw_MonitHistory_UserID]       FOREIGN KEY ([ID_USER])     REFERENCES [dbo_ext].[skw_Users]     ([ID_USER])     ON DELETE SET NULL,
            CONSTRAINT [FK_skw_MonitHistory_TemplateID]   FOREIGN KEY ([TemplateID])  REFERENCES [dbo_ext].[skw_Templates] ([ID_TEMPLATE]) ON DELETE SET NULL
        );
        CREATE NONCLUSTERED INDEX [IX_skw_MonitHistory_Kontrahent] ON [dbo_ext].[skw_MonitHistory] ([ID_KONTRAHENTA] ASC);
        CREATE NONCLUSTERED INDEX [IX_skw_MonitHistory_UserID]     ON [dbo_ext].[skw_MonitHistory] ([ID_USER] ASC);
        CREATE NONCLUSTERED INDEX [IX_skw_MonitHistory_MonitType]  ON [dbo_ext].[skw_MonitHistory] ([MonitType] ASC, [Status] ASC);
        CREATE NONCLUSTERED INDEX [IX_skw_MonitHistory_Status]     ON [dbo_ext].[skw_MonitHistory] ([Status] ASC);
        CREATE NONCLUSTERED INDEX [IX_skw_MonitHistory_SentAt]     ON [dbo_ext].[skw_MonitHistory] ([SentAt] DESC);
        CREATE NONCLUSTERED INDEX [IX_skw_MonitHistory_CreatedAt]  ON [dbo_ext].[skw_MonitHistory] ([CreatedAt] DESC);
        PRINT '[KROK 12] skw_MonitHistory: UTWORZONO';
    END
    ELSE PRINT '[KROK 12] skw_MonitHistory: już istnieje';
    COMMIT;
END TRY
BEGIN CATCH ROLLBACK; PRINT '[KROK 12] BŁĄD: ' + ERROR_MESSAGE(); THROW; END CATCH
GO

-- =============================================================================
-- KROK 13: skw_MasterAccessLog (FK → skw_Users SET NULL)
-- =============================================================================
BEGIN TRANSACTION;
BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo_ext' AND t.name='skw_MasterAccessLog')
    BEGIN
        CREATE TABLE [dbo_ext].[skw_MasterAccessLog] (
            [ID_ACCESS]     BIGINT         IDENTITY(1,1) NOT NULL,
            [ID_USER]       INT                              NULL,
            [Username]      NVARCHAR(50)                     NULL,
            [Action]        NVARCHAR(100)                NOT NULL,
            [EntityType]    NVARCHAR(50)                     NULL,
            [EntityID]      INT                              NULL,
            [IPAddress]     NVARCHAR(45)                     NULL,
            [UserAgent]     NVARCHAR(500)                    NULL,
            [RequestURL]    NVARCHAR(500)                    NULL,
            [RequestMethod] NVARCHAR(10)                     NULL,
            [Timestamp]     DATETIME       NOT NULL CONSTRAINT [DF_skw_MasterAccessLog_Timestamp] DEFAULT (GETDATE()),
            [Success]       BIT            NOT NULL CONSTRAINT [DF_skw_MasterAccessLog_Success]   DEFAULT (1),
            CONSTRAINT [PK_skw_MasterAccessLog]     PRIMARY KEY CLUSTERED ([ID_ACCESS] ASC),
            CONSTRAINT [FK_skw_MasterAccessLog_User] FOREIGN KEY ([ID_USER]) REFERENCES [dbo_ext].[skw_Users] ([ID_USER]) ON DELETE SET NULL
        );
        CREATE NONCLUSTERED INDEX [IX_skw_MasterAccessLog_UserID]    ON [dbo_ext].[skw_MasterAccessLog] ([ID_USER] ASC);
        CREATE NONCLUSTERED INDEX [IX_skw_MasterAccessLog_Timestamp] ON [dbo_ext].[skw_MasterAccessLog] ([Timestamp] DESC);
        CREATE NONCLUSTERED INDEX [IX_skw_MasterAccessLog_Action]    ON [dbo_ext].[skw_MasterAccessLog] ([Action] ASC);
        PRINT '[KROK 13] skw_MasterAccessLog: UTWORZONO';
    END
    ELSE PRINT '[KROK 13] skw_MasterAccessLog: już istnieje';
    COMMIT;
END TRY
BEGIN CATCH ROLLBACK; PRINT '[KROK 13] BŁĄD: ' + ERROR_MESSAGE(); THROW; END CATCH
GO

-- =============================================================================
-- KROK 14: skw_Comments (FK → skw_Users NO ACTION)
-- =============================================================================
BEGIN TRANSACTION;
BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo_ext' AND t.name='skw_Comments')
    BEGIN
        CREATE TABLE [dbo_ext].[skw_Comments] (
            [ID_COMMENT]     BIGINT         IDENTITY(1,1) NOT NULL,
            [ID_KONTRAHENTA] INT                          NOT NULL,
            [ID_USER]        INT                          NOT NULL,
            [Content]        NVARCHAR(MAX)                NOT NULL,
            [IsActive]       BIT            NOT NULL CONSTRAINT [DF_skw_Comments_IsActive]  DEFAULT (1),
            [CreatedAt]      DATETIME       NOT NULL CONSTRAINT [DF_skw_Comments_CreatedAt] DEFAULT (GETDATE()),
            [UpdatedAt]      DATETIME                         NULL,
            CONSTRAINT [PK_skw_Comments]      PRIMARY KEY CLUSTERED ([ID_COMMENT] ASC),
            CONSTRAINT [FK_skw_Comments_User]  FOREIGN KEY ([ID_USER]) REFERENCES [dbo_ext].[skw_Users] ([ID_USER]) ON DELETE NO ACTION
        );
        CREATE NONCLUSTERED INDEX [IX_skw_Comments_Kontrahent] ON [dbo_ext].[skw_Comments] ([ID_KONTRAHENTA] ASC);
        CREATE NONCLUSTERED INDEX [IX_skw_Comments_UserID]     ON [dbo_ext].[skw_Comments] ([ID_USER] ASC);
        PRINT '[KROK 14] skw_Comments: UTWORZONO';
    END
    ELSE PRINT '[KROK 14] skw_Comments: już istnieje';
    COMMIT;
END TRY
BEGIN CATCH ROLLBACK; PRINT '[KROK 14] BŁĄD: ' + ERROR_MESSAGE(); THROW; END CATCH
GO

-- =============================================================================
-- WERYFIKACJA KOŃCOWA
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT ' WERYFIKACJA: tabele skw_* w schemacie dbo_ext';
PRINT '============================================================';

SELECT
    t.name          AS [Tabela],
    p.rows          AS [Wiersze],
    CONVERT(NVARCHAR, o.create_date, 120) AS [Utworzono]
FROM sys.tables     t
JOIN sys.schemas    s ON t.schema_id = s.schema_id
JOIN sys.objects    o ON t.object_id = o.object_id
JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
WHERE s.name = 'dbo_ext'
  AND t.name LIKE 'skw_%'
ORDER BY o.create_date;
GO

DECLARE @cnt INT = (
    SELECT COUNT(*) FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'dbo_ext' AND t.name LIKE 'skw_%'
);
PRINT '';
IF @cnt = 13
    PRINT '✅ SUKCES: Wszystkie 13 tabel skw_* istnieją. Możesz uruchomić docker compose.';
ELSE
    PRINT '⚠️  UWAGA: Znaleziono ' + CAST(@cnt AS NVARCHAR) + '/13 tabel. Sprawdź błędy powyżej.';
GO