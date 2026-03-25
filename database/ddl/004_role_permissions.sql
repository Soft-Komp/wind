-- ============================================================
-- database/ddl/004_role_permissions.sql
-- Tabela: dbo_ext.skw_RolePermissions
--
-- Tabela łącząca role z uprawnieniami (many-to-many).
-- Operacja przypisania: zawsze DELETE + INSERT (brak UPDATE).
-- Brak kolumn IsActive i UpdatedAt — immutable per wiersz.
-- Seed: database/seeds/03_role_permissions.sql
--
-- Powiązania:
--   → skw_Roles.ID_ROLE               (FK CASCADE DELETE)
--   → skw_Permissions.ID_PERMISSION   (FK CASCADE DELETE)
-- ============================================================

USE [WAPRO];
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

BEGIN TRANSACTION;
BEGIN TRY

    IF NOT EXISTS (
        SELECT 1
        FROM   sys.tables  t
        JOIN   sys.schemas s ON t.schema_id = s.schema_id
        WHERE  s.name = 'dbo_ext'
          AND  t.name = 'skw_RolePermissions'
    )
    BEGIN
        PRINT '[004] Tworzenie tabeli dbo_ext.skw_RolePermissions...';

        CREATE TABLE [dbo_ext].[skw_RolePermissions] (

            -- ── Klucz kompozytowy (PK) ────────────────────────────────────────
            [ID_ROLE]       INT      NOT NULL,
            [ID_PERMISSION] INT      NOT NULL,

            -- ── Timestamp ────────────────────────────────────────────────────
            [CreatedAt]     DATETIME NOT NULL
                            CONSTRAINT [DF_skw_RolePermissions_CreatedAt] DEFAULT (GETDATE()),

            -- ── Constraints ──────────────────────────────────────────────────
            CONSTRAINT [PK_skw_RolePermissions]
                PRIMARY KEY CLUSTERED ([ID_ROLE] ASC, [ID_PERMISSION] ASC),

            -- FK → skw_Roles (usunięcie roli = usunięcie wszystkich przypisań)
            CONSTRAINT [FK_skw_RolePermissions_RoleID]
                FOREIGN KEY ([ID_ROLE])
                REFERENCES [dbo_ext].[skw_Roles] ([ID_ROLE])
                ON DELETE CASCADE
                ON UPDATE NO ACTION,

            -- FK → skw_Permissions (usunięcie uprawnienia = usunięcie przypisań)
            CONSTRAINT [FK_skw_RolePermissions_PermissionID]
                FOREIGN KEY ([ID_PERMISSION])
                REFERENCES [dbo_ext].[skw_Permissions] ([ID_PERMISSION])
                ON DELETE CASCADE
                ON UPDATE NO ACTION
        );

        PRINT '[004] Tabela dbo_ext.skw_RolePermissions utworzona.';
    END
    ELSE
    BEGIN
        PRINT '[004] Tabela dbo_ext.skw_RolePermissions już istnieje — pominięto.';
    END

    -- ── Indeks odwrotny (szybkie wyszukiwanie uprawnień dla roli) ─────────────

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_RolePermissions')
          AND  name      = 'IX_skw_RolePermissions_PermissionID'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_RolePermissions_PermissionID]
            ON [dbo_ext].[skw_RolePermissions] ([ID_PERMISSION] ASC, [ID_ROLE] ASC);
        PRINT '[004] Indeks IX_skw_RolePermissions_PermissionID utworzony.';
    END

    COMMIT TRANSACTION;
    PRINT '[004] === DDL 004: skw_RolePermissions — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT '[004] BŁĄD: ' + ERROR_MESSAGE();
    THROW;
END CATCH
GO