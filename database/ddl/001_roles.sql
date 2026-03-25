-- ============================================================
-- database/ddl/001_roles.sql
-- Tabela: dbo_ext.skw_Roles
--
-- Przechowuje role użytkowników systemu Windykacja.
-- Seed: database/seeds/01_roles.sql
--       → Admin, Manager, User, ReadOnly
--
-- Powiązania:
--   ← skw_Users.RoleID         (FK RESTRICT)
--   ← skw_RolePermissions.ID_ROLE (FK CASCADE DELETE)
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
          AND  t.name = 'skw_Roles'
    )
    BEGIN
        PRINT '[001] Tworzenie tabeli dbo_ext.skw_Roles...';

        CREATE TABLE [dbo_ext].[skw_Roles] (

            -- ── Klucz główny ──────────────────────────────────────────────────
            [ID_ROLE]     INT           IDENTITY(1,1)  NOT NULL,

            -- ── Dane roli ────────────────────────────────────────────────────
            [RoleName]    NVARCHAR(50)                 NOT NULL,
            [Description] NVARCHAR(200)                    NULL,

            -- ── Status ───────────────────────────────────────────────────────
            [IsActive]    BIT                          NOT NULL
                          CONSTRAINT [DF_skw_Roles_IsActive]  DEFAULT (1),

            -- ── Timestampy ───────────────────────────────────────────────────
            [CreatedAt]   DATETIME                     NOT NULL
                          CONSTRAINT [DF_skw_Roles_CreatedAt] DEFAULT (GETDATE()),
            [UpdatedAt]   DATETIME                         NULL,

            -- ── Constraints ──────────────────────────────────────────────────
            CONSTRAINT [PK_skw_Roles]
                PRIMARY KEY CLUSTERED ([ID_ROLE] ASC),

            CONSTRAINT [UQ_skw_Roles_RoleName]
                UNIQUE ([RoleName])
        );

        PRINT '[001] Tabela dbo_ext.skw_Roles utworzona.';
    END
    ELSE
    BEGIN
        PRINT '[001] Tabela dbo_ext.skw_Roles już istnieje — pominięto.';
    END

    -- ── Indeksy ───────────────────────────────────────────────────────────────

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_Roles')
          AND  name      = 'IX_skw_Roles_IsActive'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_Roles_IsActive]
            ON [dbo_ext].[skw_Roles] ([IsActive] ASC);
        PRINT '[001] Indeks IX_skw_Roles_IsActive utworzony.';
    END

    COMMIT TRANSACTION;
    PRINT '[001] === DDL 001: skw_Roles — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT '[001] BŁĄD: ' + ERROR_MESSAGE();
    THROW;
END CATCH
GO