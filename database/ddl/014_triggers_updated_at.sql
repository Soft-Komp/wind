-- =============================================================================
-- DDL 014 — Triggery UpdatedAt (AFTER UPDATE)
-- =============================================================================
-- Plik:    database/ddl/014_triggers_updated_at.sql
-- Wersja:  1.0.0
-- Data:    2026-02-18
-- Zgodny:  TABELE_REFERENCJA v1.0 §TRIGGERY UpdatedAt
--
-- Tworzy triggery AFTER UPDATE dla 8 tabel w schemacie dbo_ext.
-- Cel: redundantne aktualizowanie UpdatedAt nawet gdy SQLAlchemy onupdate zawiedzie.
-- Logika: IF NOT UPDATE(UpdatedAt) → SET UpdatedAt = GETDATE()
-- (Nie nadpisuje jeśli kolumna była jawnie ustawiona w UPDATE)
--
-- Tabele z triggerami:
--   1. Roles
--   2. Permissions
--   3. Users
--   4. Templates
--   5. MonitHistory
--   6. SystemConfig
--   7. SchemaChecksums
--   8. Comments
--
-- Tabele BEZ triggerów (immutable lub inna logika):
--   - AuditLog        — tylko INSERT
--   - RefreshTokens   — brak UpdatedAt
--   - OtpCodes        — brak UpdatedAt
--   - RolePermissions — brak UpdatedAt (delete+insert)
--   - MasterAccessLog — tylko INSERT
--
-- IDEMPOTENTNY — DROP IF EXISTS + CREATE (bezpieczne wielokrotne uruchamianie).
-- =============================================================================

USE [WAPRO];
GO

SET NOCOUNT ON;
GO

PRINT '=== DDL 014: Triggery UpdatedAt — START ===';
PRINT 'Czas: ' + CONVERT(NVARCHAR, GETDATE(), 120);

-- =============================================================================
-- 1. Roles
-- =============================================================================
IF OBJECT_ID(N'[dbo_ext].[TR_Roles_UpdatedAt]', N'TR') IS NOT NULL
    DROP TRIGGER [dbo_ext].[TR_Roles_UpdatedAt];
GO

CREATE TRIGGER [dbo_ext].[TR_Roles_UpdatedAt]
ON [dbo_ext].[Roles]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    -- Tylko jeśli UpdatedAt nie był jawnie ustawiony w UPDATE
    IF NOT UPDATE([UpdatedAt])
    BEGIN
        UPDATE r
        SET [UpdatedAt] = GETDATE()
        FROM [dbo_ext].[Roles] r
        INNER JOIN inserted i ON r.[ID_ROLE] = i.[ID_ROLE];
    END
END;
GO

PRINT 'Trigger TR_Roles_UpdatedAt: OK';

-- =============================================================================
-- 2. Permissions
-- =============================================================================
IF OBJECT_ID(N'[dbo_ext].[TR_Permissions_UpdatedAt]', N'TR') IS NOT NULL
    DROP TRIGGER [dbo_ext].[TR_Permissions_UpdatedAt];
GO

CREATE TRIGGER [dbo_ext].[TR_Permissions_UpdatedAt]
ON [dbo_ext].[Permissions]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE([UpdatedAt])
    BEGIN
        UPDATE p
        SET [UpdatedAt] = GETDATE()
        FROM [dbo_ext].[Permissions] p
        INNER JOIN inserted i ON p.[ID_PERMISSION] = i.[ID_PERMISSION];
    END
END;
GO

PRINT 'Trigger TR_Permissions_UpdatedAt: OK';

-- =============================================================================
-- 3. Users
-- =============================================================================
IF OBJECT_ID(N'[dbo_ext].[TR_Users_UpdatedAt]', N'TR') IS NOT NULL
    DROP TRIGGER [dbo_ext].[TR_Users_UpdatedAt];
GO

CREATE TRIGGER [dbo_ext].[TR_Users_UpdatedAt]
ON [dbo_ext].[Users]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    -- Wyjątek: aktualizacja FailedLoginAttempts/LastLoginAt/LockedUntil
    -- też powinna zaktualizować UpdatedAt — dlatego warunek na NOT UPDATE(UpdatedAt)
    IF NOT UPDATE([UpdatedAt])
    BEGIN
        UPDATE u
        SET [UpdatedAt] = GETDATE()
        FROM [dbo_ext].[Users] u
        INNER JOIN inserted i ON u.[ID_USER] = i.[ID_USER];
    END
END;
GO

PRINT 'Trigger TR_Users_UpdatedAt: OK';

-- =============================================================================
-- 4. Templates
-- =============================================================================
IF OBJECT_ID(N'[dbo_ext].[TR_Templates_UpdatedAt]', N'TR') IS NOT NULL
    DROP TRIGGER [dbo_ext].[TR_Templates_UpdatedAt];
GO

CREATE TRIGGER [dbo_ext].[TR_Templates_UpdatedAt]
ON [dbo_ext].[Templates]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE([UpdatedAt])
    BEGIN
        UPDATE t
        SET [UpdatedAt] = GETDATE()
        FROM [dbo_ext].[Templates] t
        INNER JOIN inserted i ON t.[ID_TEMPLATE] = i.[ID_TEMPLATE];
    END
END;
GO

PRINT 'Trigger TR_Templates_UpdatedAt: OK';

-- =============================================================================
-- 5. MonitHistory
-- =============================================================================
IF OBJECT_ID(N'[dbo_ext].[TR_MonitHistory_UpdatedAt]', N'TR') IS NOT NULL
    DROP TRIGGER [dbo_ext].[TR_MonitHistory_UpdatedAt];
GO

CREATE TRIGGER [dbo_ext].[TR_MonitHistory_UpdatedAt]
ON [dbo_ext].[MonitHistory]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE([UpdatedAt])
    BEGIN
        UPDATE mh
        SET [UpdatedAt] = GETDATE()
        FROM [dbo_ext].[MonitHistory] mh
        INNER JOIN inserted i ON mh.[ID_MONIT] = i.[ID_MONIT];
    END
END;
GO

PRINT 'Trigger TR_MonitHistory_UpdatedAt: OK';

-- =============================================================================
-- 6. SystemConfig
-- =============================================================================
IF OBJECT_ID(N'[dbo_ext].[TR_SystemConfig_UpdatedAt]', N'TR') IS NOT NULL
    DROP TRIGGER [dbo_ext].[TR_SystemConfig_UpdatedAt];
GO

CREATE TRIGGER [dbo_ext].[TR_SystemConfig_UpdatedAt]
ON [dbo_ext].[SystemConfig]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE([UpdatedAt])
    BEGIN
        UPDATE sc
        SET [UpdatedAt] = GETDATE()
        FROM [dbo_ext].[SystemConfig] sc
        INNER JOIN inserted i ON sc.[ID_CONFIG] = i.[ID_CONFIG];
    END
END;
GO

PRINT 'Trigger TR_SystemConfig_UpdatedAt: OK';

-- =============================================================================
-- 7. SchemaChecksums
-- =============================================================================
IF OBJECT_ID(N'[dbo_ext].[TR_SchemaChecksums_UpdatedAt]', N'TR') IS NOT NULL
    DROP TRIGGER [dbo_ext].[TR_SchemaChecksums_UpdatedAt];
GO

CREATE TRIGGER [dbo_ext].[TR_SchemaChecksums_UpdatedAt]
ON [dbo_ext].[SchemaChecksums]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE([UpdatedAt])
    BEGIN
        UPDATE sck
        SET [UpdatedAt] = GETDATE()
        FROM [dbo_ext].[SchemaChecksums] sck
        INNER JOIN inserted i ON sck.[ID_CHECKSUM] = i.[ID_CHECKSUM];
    END
END;
GO

PRINT 'Trigger TR_SchemaChecksums_UpdatedAt: OK';

-- =============================================================================
-- 8. Comments
-- =============================================================================
IF OBJECT_ID(N'[dbo_ext].[TR_Comments_UpdatedAt]', N'TR') IS NOT NULL
    DROP TRIGGER [dbo_ext].[TR_Comments_UpdatedAt];
GO

CREATE TRIGGER [dbo_ext].[TR_Comments_UpdatedAt]
ON [dbo_ext].[Comments]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE([UpdatedAt])
    BEGIN
        UPDATE c
        SET [UpdatedAt] = GETDATE()
        FROM [dbo_ext].[Comments] c
        INNER JOIN inserted i ON c.[ID_COMMENT] = i.[ID_COMMENT];
    END
END;
GO

PRINT 'Trigger TR_Comments_UpdatedAt: OK';
GO

-- =============================================================================
-- Weryfikacja — lista wszystkich triggerów dbo_ext
-- =============================================================================
PRINT '';
PRINT 'Triggery w schemacie dbo_ext:';
SELECT
    SCHEMA_NAME(o.schema_id)    AS [Schema],
    o.[name]                    AS [TriggerName],
    OBJECT_NAME(t.parent_id)    AS [Tabela],
    t.is_disabled               AS [Wyłączony],
    CONVERT(NVARCHAR, o.create_date, 120) AS [Utworzony],
    CONVERT(NVARCHAR, o.modify_date, 120) AS [Zmodyfikowany]
FROM sys.triggers t
JOIN sys.objects   o ON t.object_id = o.object_id
WHERE SCHEMA_NAME(o.schema_id) = N'dbo_ext'
ORDER BY OBJECT_NAME(t.parent_id), o.[name];
GO

PRINT '=== DDL 014: Triggery UpdatedAt — OK ===';