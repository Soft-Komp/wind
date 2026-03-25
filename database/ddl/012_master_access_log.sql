-- ============================================================
-- database/ddl/012_master_access_log.sql
-- Tabela: dbo_ext.skw_MasterAccessLog
--
-- ⚠️ IMMUTABLE — tylko INSERT. Nigdy UPDATE ani DELETE.
-- ⚠️ BRAK endpointu API. Dostęp wyłącznie przez SSMS (DBA).
--
-- Rejestr dostępu przez Master Key (impersonacja administracyjna).
-- App user: tylko INSERT.
--   DENY  SELECT, UPDATE, DELETE ON dbo_ext.skw_MasterAccessLog TO [windykacja_app_user];
--   GRANT INSERT                 ON dbo_ext.skw_MasterAccessLog TO [windykacja_app_user];
--
-- Powiązania:
--   → skw_Users.TargetUserID (FK SET NULL — log zostaje po usunięciu usera)
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
          AND  t.name = 'skw_MasterAccessLog'
    )
    BEGIN
        PRINT '[012] Tworzenie tabeli dbo_ext.skw_MasterAccessLog...';

        CREATE TABLE [dbo_ext].[skw_MasterAccessLog] (

            -- ── Klucz główny ──────────────────────────────────────────────────
            [ID_LOG]          BIGINT         IDENTITY(1,1)  NOT NULL,

            -- ── Cel impersonacji ─────────────────────────────────────────────
            [TargetUserID]    INT                               NULL,
            [TargetUsername]  NVARCHAR(50)                  NOT NULL,  -- kopia nazwy

            -- ── Kontekst HTTP ────────────────────────────────────────────────
            [IPAddress]       NVARCHAR(45)                  NOT NULL,
            [UserAgent]       NVARCHAR(500)                     NULL,

            -- ── Czas dostępu ─────────────────────────────────────────────────
            [AccessedAt]      DATETIME                      NOT NULL
                              CONSTRAINT [DF_skw_MasterAccessLog_AccessedAt]
                              DEFAULT (GETDATE()),
            [SessionEndedAt]  DATETIME                          NULL,

            -- ── Notatki (DBA) ────────────────────────────────────────────────
            [Notes]           NVARCHAR(500)                     NULL,

            -- ── Constraints ──────────────────────────────────────────────────
            CONSTRAINT [PK_skw_MasterAccessLog]
                PRIMARY KEY CLUSTERED ([ID_LOG] ASC),

            -- FK → skw_Users (SET NULL — log zostaje po usunięciu usera)
            CONSTRAINT [FK_skw_MasterAccessLog_TargetUserID]
                FOREIGN KEY ([TargetUserID])
                REFERENCES [dbo_ext].[skw_Users] ([ID_USER])
                ON DELETE SET NULL
                ON UPDATE NO ACTION
        );

        PRINT '[012] Tabela dbo_ext.skw_MasterAccessLog utworzona.';
    END
    ELSE
    BEGIN
        PRINT '[012] Tabela dbo_ext.skw_MasterAccessLog już istnieje — pominięto.';
    END

    -- ── Indeksy ───────────────────────────────────────────────────────────────

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_MasterAccessLog')
          AND  name      = 'IX_skw_MasterAccessLog_AccessedAt'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_MasterAccessLog_AccessedAt]
            ON [dbo_ext].[skw_MasterAccessLog] ([AccessedAt] DESC);
        PRINT '[012] Indeks IX_skw_MasterAccessLog_AccessedAt utworzony.';
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_MasterAccessLog')
          AND  name      = 'IX_skw_MasterAccessLog_TargetUserID'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_MasterAccessLog_TargetUserID]
            ON [dbo_ext].[skw_MasterAccessLog] ([TargetUserID] ASC, [AccessedAt] DESC)
            WHERE [TargetUserID] IS NOT NULL;
        PRINT '[012] Indeks IX_skw_MasterAccessLog_TargetUserID utworzony.';
    END

    COMMIT TRANSACTION;
    PRINT '[012] === DDL 012: skw_MasterAccessLog — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT '[012] BŁĄD: ' + ERROR_MESSAGE();
    THROW;
END CATCH
GO