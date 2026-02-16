-- ============================================================
-- Triggery UpdatedAt dla wszystkich tabel biznesowych.
-- Redundancja z SQLAlchemy onupdate — gwarantuje aktualizację
-- nawet przy bezpośrednich zapytaniach SQL poza ORM.
-- ============================================================

USE [WAPRO];
GO

-- ---- Roles ----
CREATE OR ALTER TRIGGER dbo_ext.trg_Roles_UpdatedAt
ON dbo_ext.Roles
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(UpdatedAt)  -- nie aktualizuj jeśli ktoś ręcznie ustawił UpdatedAt
    BEGIN
        UPDATE dbo_ext.Roles
        SET UpdatedAt = GETDATE()
        WHERE ID_ROLE IN (SELECT ID_ROLE FROM inserted);
    END
END;
GO

-- ---- Permissions ----
CREATE OR ALTER TRIGGER dbo_ext.trg_Permissions_UpdatedAt
ON dbo_ext.Permissions
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(UpdatedAt)
    BEGIN
        UPDATE dbo_ext.Permissions
        SET UpdatedAt = GETDATE()
        WHERE ID_PERMISSION IN (SELECT ID_PERMISSION FROM inserted);
    END
END;
GO

-- ---- Users ----
CREATE OR ALTER TRIGGER dbo_ext.trg_Users_UpdatedAt
ON dbo_ext.Users
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(UpdatedAt)
    BEGIN
        UPDATE dbo_ext.Users
        SET UpdatedAt = GETDATE()
        WHERE ID_USER IN (SELECT ID_USER FROM inserted);
    END
END;
GO

-- ---- Templates ----
CREATE OR ALTER TRIGGER dbo_ext.trg_Templates_UpdatedAt
ON dbo_ext.Templates
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(UpdatedAt)
    BEGIN
        UPDATE dbo_ext.Templates
        SET UpdatedAt = GETDATE()
        WHERE ID_TEMPLATE IN (SELECT ID_TEMPLATE FROM inserted);
    END
END;
GO

-- ---- MonitHistory ----
CREATE OR ALTER TRIGGER dbo_ext.trg_MonitHistory_UpdatedAt
ON dbo_ext.MonitHistory
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(UpdatedAt)
    BEGIN
        UPDATE dbo_ext.MonitHistory
        SET UpdatedAt = GETDATE()
        WHERE ID_MONIT IN (SELECT ID_MONIT FROM inserted);
    END
END;
GO

-- ---- SystemConfig ----
CREATE OR ALTER TRIGGER dbo_ext.trg_SystemConfig_UpdatedAt
ON dbo_ext.SystemConfig
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(UpdatedAt)
    BEGIN
        UPDATE dbo_ext.SystemConfig
        SET UpdatedAt = GETDATE()
        WHERE ID_CONFIG IN (SELECT ID_CONFIG FROM inserted);
    END
END;
GO

-- ---- SchemaChecksums ----
CREATE OR ALTER TRIGGER dbo_ext.trg_SchemaChecksums_UpdatedAt
ON dbo_ext.SchemaChecksums
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(UpdatedAt)
    BEGIN
        UPDATE dbo_ext.SchemaChecksums
        SET UpdatedAt = GETDATE()
        WHERE ID_CHECKSUM IN (SELECT ID_CHECKSUM FROM inserted);
    END
END;
GO

-- ---- Comments ----
CREATE OR ALTER TRIGGER dbo_ext.trg_Comments_UpdatedAt
ON dbo_ext.Comments
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(UpdatedAt)
    BEGIN
        UPDATE dbo_ext.Comments
        SET UpdatedAt = GETDATE()
        WHERE ID_COMMENT IN (SELECT ID_COMMENT FROM inserted);
    END
END;
GO

PRINT 'Wszystkie triggery UpdatedAt utworzone/zaktualizowane.';
GO