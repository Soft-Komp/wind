-- ============================================================
-- database/ddl/007_audit_log.sql
-- Tabela: dbo_ext.skw_AuditLog
--
-- ⚠️ IMMUTABLE — tylko INSERT. Nigdy UPDATE ani DELETE.
--
-- Pełny audit trail wszystkich operacji w systemie.
-- OldValue / NewValue / Details: JSON (NVARCHAR(MAX)).
-- Zapis asynchroniczny — nie blokuje response HTTP.
--
-- Powiązania:
--   → skw_Users.ID_USER (FK SET NULL — log zostaje po usunięciu usera)
--
-- Wersja: 2.0.0 | Data: 2026-03-02
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
          AND  t.name = 'skw_AuditLog'
    )
    BEGIN
        PRINT '[007] Tworzenie tabeli dbo_ext.skw_AuditLog...';

        CREATE TABLE [dbo_ext].[skw_AuditLog] (

            -- ── Klucz główny ──────────────────────────────────────────────────
            [ID_LOG]          BIGINT         IDENTITY(1,1)  NOT NULL,

            -- ── Aktor (kto) ──────────────────────────────────────────────────
            [ID_USER]         INT                               NULL,  -- NULL = system/anon
            [Username]        NVARCHAR(50)                      NULL,  -- kopia na wypadek usunięcia

            -- ── Akcja (co) ───────────────────────────────────────────────────
            [Action]          NVARCHAR(100)                 NOT NULL,  -- snake_case: user_created
            [ActionCategory]  NVARCHAR(50)                      NULL,  -- Auth/Users/Roles/...

            -- ── Obiekt akcji (na czym) ────────────────────────────────────────
            [EntityType]      NVARCHAR(50)                      NULL,  -- User/Debtor/Monit/Role
            [EntityID]        INT                               NULL,

            -- ── Stan przed i po (JSON) ────────────────────────────────────────
            [OldValue]        NVARCHAR(MAX)                     NULL,  -- JSON stan PRZED
            [NewValue]        NVARCHAR(MAX)                     NULL,  -- JSON stan PO
            [Details]         NVARCHAR(MAX)                     NULL,  -- JSON dodatkowe info

            -- ── Kontekst HTTP ────────────────────────────────────────────────
            [IPAddress]       NVARCHAR(45)                      NULL,
            [UserAgent]       NVARCHAR(500)                     NULL,
            [RequestURL]      NVARCHAR(500)                     NULL,
            [RequestMethod]   NVARCHAR(10)                      NULL,
            [RequestID]       NVARCHAR(36)                      NULL,  

            -- ── Wynik ────────────────────────────────────────────────────────
            [Success]         BIT                           NOT NULL
                              CONSTRAINT [DF_skw_AuditLog_Success] DEFAULT (1),
            [ErrorMessage]    NVARCHAR(500)                     NULL,

            -- ── Timestamp ────────────────────────────────────────────────────
            -- Brak UpdatedAt — tabela immutable (tylko INSERT)
            [Timestamp]       DATETIME                      NOT NULL
                              CONSTRAINT [DF_skw_AuditLog_Timestamp] DEFAULT (GETDATE()),

            -- ── Constraints ──────────────────────────────────────────────────
            CONSTRAINT [PK_skw_AuditLog]
                PRIMARY KEY CLUSTERED ([ID_LOG] ASC),

            CONSTRAINT [CK_skw_AuditLog_RequestMethod]
                CHECK ([RequestMethod] IN ('GET','POST','PUT','DELETE','PATCH') OR [RequestMethod] IS NULL),

            CONSTRAINT [CK_skw_AuditLog_ActionCategory]
                CHECK ([ActionCategory] IN (
                    'Auth','Users','Roles','Debtors','Monits',
                    'Comments','System','Snapshots','Audit'
                ) OR [ActionCategory] IS NULL),

            -- FK → skw_Users (SET NULL — log zostaje po usunięciu usera)
            CONSTRAINT [FK_skw_AuditLog_UserID]
                FOREIGN KEY ([ID_USER])
                REFERENCES [dbo_ext].[skw_Users] ([ID_USER])
                ON DELETE SET NULL
                ON UPDATE NO ACTION
        );

        PRINT '[007] Tabela dbo_ext.skw_AuditLog utworzona.';
    END
    ELSE
    BEGIN
        PRINT '[007] Tabela dbo_ext.skw_AuditLog już istnieje — pominięto.';
    END

    -- ── Indeksy ───────────────────────────────────────────────────────────────

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_AuditLog')
          AND  name      = 'IX_skw_AuditLog_Timestamp'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_AuditLog_Timestamp]
            ON [dbo_ext].[skw_AuditLog] ([Timestamp] DESC);
        PRINT '[007] Indeks IX_skw_AuditLog_Timestamp utworzony.';
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_AuditLog')
          AND  name      = 'IX_skw_AuditLog_UserID'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_AuditLog_UserID]
            ON [dbo_ext].[skw_AuditLog] ([ID_USER] ASC, [Timestamp] DESC)
            WHERE [ID_USER] IS NOT NULL;
        PRINT '[007] Indeks IX_skw_AuditLog_UserID utworzony.';
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_AuditLog')
          AND  name      = 'IX_skw_AuditLog_ActionCategory'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_AuditLog_ActionCategory]
            ON [dbo_ext].[skw_AuditLog] ([ActionCategory] ASC, [Timestamp] DESC);
        PRINT '[007] Indeks IX_skw_AuditLog_ActionCategory utworzony.';
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_AuditLog')
          AND  name      = 'IX_skw_AuditLog_EntityType_ID'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_AuditLog_EntityType_ID]
            ON [dbo_ext].[skw_AuditLog] ([EntityType] ASC, [EntityID] ASC, [Timestamp] DESC)
            WHERE [EntityType] IS NOT NULL;
        PRINT '[007] Indeks IX_skw_AuditLog_EntityType_ID utworzony.';
    END

    COMMIT TRANSACTION;
    PRINT '[007] === DDL 007: skw_AuditLog — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT '[007] BŁĄD: ' + ERROR_MESSAGE();
    THROW;
END CATCH
GO