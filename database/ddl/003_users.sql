-- ============================================================
-- database/ddl/003_users.sql
-- Tabela: dbo_ext.skw_Users
--
-- Użytkownicy systemu Windykacja.
-- Hasła: argon2id (argon2-cffi) — NIGDY plain text.
-- Blokada konta: FailedLoginAttempts + LockedUntil.
-- Seed: database/seeds/04_admin_user.sql
--
-- Powiązania:
--   → skw_Roles.ID_ROLE                     (FK RESTRICT)
--   ← skw_RefreshTokens.ID_USER             (FK CASCADE DELETE)
--   ← skw_OtpCodes.ID_USER                  (FK CASCADE DELETE)
--   ← skw_AuditLog.ID_USER                  (FK SET NULL)
--   ← skw_MonitHistory.ID_USER              (FK SET NULL)
--   ← skw_MasterAccessLog.TargetUserID      (FK SET NULL)
--   ← skw_Comments.UzytkownikID             (FK RESTRICT)
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
          AND  t.name = 'skw_Users'
    )
    BEGIN
        PRINT '[003] Tworzenie tabeli dbo_ext.skw_Users...';

        CREATE TABLE [dbo_ext].[skw_Users] (

            -- ── Klucz główny ──────────────────────────────────────────────────
            [ID_USER]              INT           IDENTITY(1,1)  NOT NULL,

            -- ── Dane uwierzytelniania ────────────────────────────────────────
            [Username]             NVARCHAR(50)                 NOT NULL,
            [Email]                NVARCHAR(100)                NOT NULL,
            [PasswordHash]         NVARCHAR(255)                NOT NULL,

            -- ── Dane personalne ──────────────────────────────────────────────
            [FullName]             NVARCHAR(100)                    NULL,

            -- ── Rola (RBAC) ───────────────────────────────────────────────────
            [RoleID]               INT                          NOT NULL,

            -- ── Status konta ─────────────────────────────────────────────────
            [IsActive]             BIT                          NOT NULL
                                   CONSTRAINT [DF_skw_Users_IsActive]  DEFAULT (1),

            -- ── Bezpieczeństwo logowania ─────────────────────────────────────
            [LastLoginAt]          DATETIME                         NULL,
            [FailedLoginAttempts]  INT                          NOT NULL
                                   CONSTRAINT [DF_skw_Users_FailedLoginAttempts] DEFAULT (0),
            [LockedUntil]          DATETIME                         NULL,

            -- ── Timestampy ───────────────────────────────────────────────────
            [CreatedAt]            DATETIME                     NOT NULL
                                   CONSTRAINT [DF_skw_Users_CreatedAt] DEFAULT (GETDATE()),
            [UpdatedAt]            DATETIME                         NULL,

            -- ── Constraints ──────────────────────────────────────────────────
            CONSTRAINT [PK_skw_Users]
                PRIMARY KEY CLUSTERED ([ID_USER] ASC),

            -- CONSTRAINT [UQ_skw_Users_Username]
            --    UNIQUE ([Username]),

            -- CONSTRAINT [UQ_skw_Users_Email]
            --    UNIQUE ([Email]),

            -- FailedLoginAttempts nie może być ujemna
            CONSTRAINT [CK_skw_Users_FailedLoginAttempts]
                CHECK ([FailedLoginAttempts] >= 0),

            -- FK → skw_Roles (RESTRICT — nie można usunąć roli z przypisanymi userami)
            CONSTRAINT [FK_skw_Users_RoleID]
                FOREIGN KEY ([RoleID])
                REFERENCES [dbo_ext].[skw_Roles] ([ID_ROLE])
                ON DELETE NO ACTION
                ON UPDATE NO ACTION
        );

        CREATE UNIQUE NONCLUSTERED INDEX [UQ_skw_Users_Email_Active]
        ON dbo_ext.skw_Users (Email)
        WHERE IsActive = 1;

        CREATE UNIQUE NONCLUSTERED INDEX [UQ_skw_Users_Username_Active]
        ON dbo_ext.skw_Users (Username)
        WHERE IsActive = 1;

        PRINT '[003] Tabela dbo_ext.skw_Users utworzona.';
    END
    ELSE
    BEGIN
        PRINT '[003] Tabela dbo_ext.skw_Users już istnieje — pominięto.';
    END

    -- ── Indeksy ───────────────────────────────────────────────────────────────

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_Users')
          AND  name      = 'IX_skw_Users_RoleID'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_Users_RoleID]
            ON [dbo_ext].[skw_Users] ([RoleID] ASC);
        PRINT '[003] Indeks IX_skw_Users_RoleID utworzony.';
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_Users')
          AND  name      = 'IX_skw_Users_IsActive'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_Users_IsActive]
            ON [dbo_ext].[skw_Users] ([IsActive] ASC);
        PRINT '[003] Indeks IX_skw_Users_IsActive utworzony.';
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_Users')
          AND  name      = 'IX_skw_Users_LockedUntil'
    )
    BEGIN
        -- Filtrowany — tylko konta aktualnie zablokowane (NULL = brak blokady)
        CREATE NONCLUSTERED INDEX [IX_skw_Users_LockedUntil]
            ON [dbo_ext].[skw_Users] ([LockedUntil] ASC)
            WHERE [LockedUntil] IS NOT NULL;
        PRINT '[003] Indeks IX_skw_Users_LockedUntil utworzony.';
    END

    COMMIT TRANSACTION;
    PRINT '[003] === DDL 003: skw_Users — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT '[003] BŁĄD: ' + ERROR_MESSAGE();
    THROW;
END CATCH
GO