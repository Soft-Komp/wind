-- ============================================================
-- database/ddl/002_permissions.sql
-- Tabela: dbo_ext.skw_Permissions
--
-- Granularne uprawnienia systemu (format: kategoria.akcja).
-- Seed: database/seeds/02_permissions.sql  → 83 uprawnienia
--
-- Kategorie uprawnień:
--   auth / users / roles / debtors / monits / comments /
--   pdf  / reports / snapshots / audit / system
--
-- Powiązania:
--   ← skw_RolePermissions.ID_PERMISSION (FK CASCADE DELETE)
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
          AND  t.name = 'skw_Permissions'
    )
    BEGIN
        PRINT '[002] Tworzenie tabeli dbo_ext.skw_Permissions...';

        CREATE TABLE [dbo_ext].[skw_Permissions] (

            -- ── Klucz główny ──────────────────────────────────────────────────
            [ID_PERMISSION]  INT           IDENTITY(1,1)  NOT NULL,

            -- ── Dane uprawnienia ─────────────────────────────────────────────
            [PermissionName] NVARCHAR(100)                NOT NULL,
            [Description]    NVARCHAR(200)                    NULL,
            [Category]       NVARCHAR(50)                     NULL,

            -- ── Status ───────────────────────────────────────────────────────
            [IsActive]       BIT                          NOT NULL
                             CONSTRAINT [DF_skw_Permissions_IsActive]  DEFAULT (1),

            -- ── Timestampy ───────────────────────────────────────────────────
            [CreatedAt]      DATETIME                     NOT NULL
                             CONSTRAINT [DF_skw_Permissions_CreatedAt] DEFAULT (GETDATE()),
            [UpdatedAt]      DATETIME                         NULL,

            -- ── Constraints ──────────────────────────────────────────────────
            CONSTRAINT [PK_skw_Permissions]
                PRIMARY KEY CLUSTERED ([ID_PERMISSION] ASC),

            CONSTRAINT [UQ_skw_Permissions_PermissionName]
                UNIQUE ([PermissionName]),

            -- Kategoria musi być jedną z dozwolonych wartości
            CONSTRAINT [CK_skw_Permissions_Category]
                CHECK ([Category] IN (
                    'auth', 'users', 'roles', 'permissions', 'debtors', 'monits',
                    'comments', 'pdf', 'reports', 'snapshots',
                    'audit', 'system', 'templates', 'faktury'
                ))
        );

        PRINT '[002] Tabela dbo_ext.skw_Permissions utworzona.';
    END
    ELSE
    BEGIN
        PRINT '[002] Tabela dbo_ext.skw_Permissions już istnieje — pominięto.';
    END

    -- ── Indeksy ───────────────────────────────────────────────────────────────

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_Permissions')
          AND  name      = 'IX_skw_Permissions_Category'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_Permissions_Category]
            ON [dbo_ext].[skw_Permissions] ([Category] ASC, [IsActive] ASC);
        PRINT '[002] Indeks IX_skw_Permissions_Category utworzony.';
    END

    COMMIT TRANSACTION;
    PRINT '[002] === DDL 002: skw_Permissions — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT '[002] BŁĄD: ' + ERROR_MESSAGE();
    THROW;
END CATCH
GO