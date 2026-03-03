-- ============================================================
-- database/ddl/014_triggers_updated_at.sql
-- Triggery AFTER UPDATE — automatyczna aktualizacja UpdatedAt
--
-- Tabele z triggerem (mają UpdatedAt):
--   skw_Roles, skw_Permissions, skw_Users, skw_Templates
--   skw_MonitHistory, skw_SystemConfig, skw_SchemaChecksums, skw_Comments
--
-- Tabele BEZ triggera (immutable lub inna logika):
--   skw_AuditLog          — tylko INSERT, nigdy UPDATE
--   skw_RefreshTokens     — brak UpdatedAt (immutable)
--   skw_OtpCodes          — brak UpdatedAt (jednorazowe)
--   skw_RolePermissions   — brak UpdatedAt (delete + insert)
--   skw_MasterAccessLog   — tylko INSERT
--
-- Logika: aktualizuje UpdatedAt = GETDATE() tylko gdy
--   kolumna UpdatedAt NIE była ręcznie ustawiana w tym UPDATE.
--   Redundantne z SQLAlchemy onupdate=datetime.now(timezone.utc).
--
-- Idempotentny — bezpiecznie uruchamiany wielokrotnie.
--
-- Wersja: 2.0.0 | Data: 2026-03-02
-- ============================================================

USE [WAPRO];
GO

SET NOCOUNT ON;
GO

-- ═════════════════════════════════════════════════════════════════════════════
-- TR_skw_Roles_UpdatedAt
-- ═════════════════════════════════════════════════════════════════════════════

IF EXISTS (
    SELECT 1 FROM sys.triggers
    WHERE name = 'TR_skw_Roles_UpdatedAt'
      AND parent_id = OBJECT_ID('dbo_ext.skw_Roles')
)
    DROP TRIGGER [dbo_ext].[TR_skw_Roles_UpdatedAt];
GO

CREATE TRIGGER [dbo_ext].[TR_skw_Roles_UpdatedAt]
ON [dbo_ext].[skw_Roles]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(UpdatedAt)
    BEGIN
        UPDATE r
        SET    r.[UpdatedAt] = GETDATE()
        FROM   [dbo_ext].[skw_Roles] r
        INNER JOIN inserted i ON r.[ID_ROLE] = i.[ID_ROLE];
    END
END
GO

PRINT '[014] Trigger TR_skw_Roles_UpdatedAt — OK';
GO

-- ═════════════════════════════════════════════════════════════════════════════
-- TR_skw_Permissions_UpdatedAt
-- ═════════════════════════════════════════════════════════════════════════════

IF EXISTS (
    SELECT 1 FROM sys.triggers
    WHERE name = 'TR_skw_Permissions_UpdatedAt'
      AND parent_id = OBJECT_ID('dbo_ext.skw_Permissions')
)
    DROP TRIGGER [dbo_ext].[TR_skw_Permissions_UpdatedAt];
GO

CREATE TRIGGER [dbo_ext].[TR_skw_Permissions_UpdatedAt]
ON [dbo_ext].[skw_Permissions]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(UpdatedAt)
    BEGIN
        UPDATE p
        SET    p.[UpdatedAt] = GETDATE()
        FROM   [dbo_ext].[skw_Permissions] p
        INNER JOIN inserted i ON p.[ID_PERMISSION] = i.[ID_PERMISSION];
    END
END
GO

PRINT '[014] Trigger TR_skw_Permissions_UpdatedAt — OK';
GO

-- ═════════════════════════════════════════════════════════════════════════════
-- TR_skw_Users_UpdatedAt
-- ═════════════════════════════════════════════════════════════════════════════

IF EXISTS (
    SELECT 1 FROM sys.triggers
    WHERE name = 'TR_skw_Users_UpdatedAt'
      AND parent_id = OBJECT_ID('dbo_ext.skw_Users')
)
    DROP TRIGGER [dbo_ext].[TR_skw_Users_UpdatedAt];
GO

CREATE TRIGGER [dbo_ext].[TR_skw_Users_UpdatedAt]
ON [dbo_ext].[skw_Users]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(UpdatedAt)
    BEGIN
        UPDATE u
        SET    u.[UpdatedAt] = GETDATE()
        FROM   [dbo_ext].[skw_Users] u
        INNER JOIN inserted i ON u.[ID_USER] = i.[ID_USER];
    END
END
GO

PRINT '[014] Trigger TR_skw_Users_UpdatedAt — OK';
GO

-- ═════════════════════════════════════════════════════════════════════════════
-- TR_skw_Templates_UpdatedAt
-- ═════════════════════════════════════════════════════════════════════════════

IF EXISTS (
    SELECT 1 FROM sys.triggers
    WHERE name = 'TR_skw_Templates_UpdatedAt'
      AND parent_id = OBJECT_ID('dbo_ext.skw_Templates')
)
    DROP TRIGGER [dbo_ext].[TR_skw_Templates_UpdatedAt];
GO

CREATE TRIGGER [dbo_ext].[TR_skw_Templates_UpdatedAt]
ON [dbo_ext].[skw_Templates]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(UpdatedAt)
    BEGIN
        UPDATE t
        SET    t.[UpdatedAt] = GETDATE()
        FROM   [dbo_ext].[skw_Templates] t
        INNER JOIN inserted i ON t.[ID_TEMPLATE] = i.[ID_TEMPLATE];
    END
END
GO

PRINT '[014] Trigger TR_skw_Templates_UpdatedAt — OK';
GO

-- ═════════════════════════════════════════════════════════════════════════════
-- TR_skw_MonitHistory_UpdatedAt
-- ═════════════════════════════════════════════════════════════════════════════

IF EXISTS (
    SELECT 1 FROM sys.triggers
    WHERE name = 'TR_skw_MonitHistory_UpdatedAt'
      AND parent_id = OBJECT_ID('dbo_ext.skw_MonitHistory')
)
    DROP TRIGGER [dbo_ext].[TR_skw_MonitHistory_UpdatedAt];
GO

CREATE TRIGGER [dbo_ext].[TR_skw_MonitHistory_UpdatedAt]
ON [dbo_ext].[skw_MonitHistory]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(UpdatedAt)
    BEGIN
        UPDATE m
        SET    m.[UpdatedAt] = GETDATE()
        FROM   [dbo_ext].[skw_MonitHistory] m
        INNER JOIN inserted i ON m.[ID_MONIT] = i.[ID_MONIT];
    END
END
GO

PRINT '[014] Trigger TR_skw_MonitHistory_UpdatedAt — OK';
GO

-- ═════════════════════════════════════════════════════════════════════════════
-- TR_skw_SystemConfig_UpdatedAt
-- ═════════════════════════════════════════════════════════════════════════════

IF EXISTS (
    SELECT 1 FROM sys.triggers
    WHERE name = 'TR_skw_SystemConfig_UpdatedAt'
      AND parent_id = OBJECT_ID('dbo_ext.skw_SystemConfig')
)
    DROP TRIGGER [dbo_ext].[TR_skw_SystemConfig_UpdatedAt];
GO

CREATE TRIGGER [dbo_ext].[TR_skw_SystemConfig_UpdatedAt]
ON [dbo_ext].[skw_SystemConfig]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(UpdatedAt)
    BEGIN
        UPDATE c
        SET    c.[UpdatedAt] = GETDATE()
        FROM   [dbo_ext].[skw_SystemConfig] c
        INNER JOIN inserted i ON c.[ID_CONFIG] = i.[ID_CONFIG];
    END
END
GO

PRINT '[014] Trigger TR_skw_SystemConfig_UpdatedAt — OK';
GO

-- ═════════════════════════════════════════════════════════════════════════════
-- TR_skw_SchemaChecksums_UpdatedAt
-- ═════════════════════════════════════════════════════════════════════════════

IF EXISTS (
    SELECT 1 FROM sys.triggers
    WHERE name = 'TR_skw_SchemaChecksums_UpdatedAt'
      AND parent_id = OBJECT_ID('dbo_ext.skw_SchemaChecksums')
)
    DROP TRIGGER [dbo_ext].[TR_skw_SchemaChecksums_UpdatedAt];
GO

CREATE TRIGGER [dbo_ext].[TR_skw_SchemaChecksums_UpdatedAt]
ON [dbo_ext].[skw_SchemaChecksums]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(UpdatedAt)
    BEGIN
        UPDATE sc
        SET    sc.[UpdatedAt] = GETDATE()
        FROM   [dbo_ext].[skw_SchemaChecksums] sc
        INNER JOIN inserted i ON sc.[ID_CHECKSUM] = i.[ID_CHECKSUM];
    END
END
GO

PRINT '[014] Trigger TR_skw_SchemaChecksums_UpdatedAt — OK';
GO

-- ═════════════════════════════════════════════════════════════════════════════
-- TR_skw_Comments_UpdatedAt
-- ═════════════════════════════════════════════════════════════════════════════

IF EXISTS (
    SELECT 1 FROM sys.triggers
    WHERE name = 'TR_skw_Comments_UpdatedAt'
      AND parent_id = OBJECT_ID('dbo_ext.skw_Comments')
)
    DROP TRIGGER [dbo_ext].[TR_skw_Comments_UpdatedAt];
GO

CREATE TRIGGER [dbo_ext].[TR_skw_Comments_UpdatedAt]
ON [dbo_ext].[skw_Comments]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(UpdatedAt)
    BEGIN
        UPDATE c
        SET    c.[UpdatedAt] = GETDATE()
        FROM   [dbo_ext].[skw_Comments] c
        INNER JOIN inserted i ON c.[ID_COMMENT] = i.[ID_COMMENT];
    END
END
GO

PRINT '[014] Trigger TR_skw_Comments_UpdatedAt — OK';
GO

-- ═════════════════════════════════════════════════════════════════════════════
-- Weryfikacja — lista triggerów po instalacji
-- ═════════════════════════════════════════════════════════════════════════════

SELECT
    t.[name]                    AS [TriggerName],
    OBJECT_NAME(t.parent_id)    AS [Tabela],
    t.is_disabled               AS [Wyłączony],
    CONVERT(NVARCHAR, o.create_date, 120) AS [Utworzony],
    CONVERT(NVARCHAR, o.modify_date, 120) AS [Zmodyfikowany]
FROM sys.triggers  t
JOIN sys.objects   o ON t.object_id = o.object_id
WHERE SCHEMA_NAME(o.schema_id) = N'dbo_ext'
  AND t.[name] LIKE 'TR_skw_%'
ORDER BY OBJECT_NAME(t.parent_id), o.[name];
GO

PRINT '[014] === DDL 014: Triggery UpdatedAt — OK ===';
GO