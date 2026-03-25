-- ============================================================
-- database/ddl/009_monit_history.sql
-- Tabela: dbo_ext.skw_MonitHistory
--
-- ⚠️ Wymaga wcześniejszego uruchomienia 008_templates.sql
--
-- Historia wysłanych monitów (email/sms/print).
-- ID_KONTRAHENTA = klucz z WAPRO (BEZ FK constraint — inna baza logiczna).
-- InvoiceNumbers: JSON array faktur, np. ["FV/001/2026","FV/002/2026"]
-- Statusy: pending → sent → delivered → bounced/failed/opened/clicked
--
-- Powiązania:
--   → skw_Users.ID_USER          (FK SET NULL)
--   → skw_Templates.ID_TEMPLATE  (FK SET NULL)
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
          AND  t.name = 'skw_MonitHistory'
    )
    BEGIN
        PRINT '[009] Tworzenie tabeli dbo_ext.skw_MonitHistory...';

        CREATE TABLE [dbo_ext].[skw_MonitHistory] (

            -- ── Klucz główny ──────────────────────────────────────────────────
            [ID_MONIT]        BIGINT          IDENTITY(1,1)  NOT NULL,

            -- ── Powiązanie z WAPRO (bez FK — inna baza logiczna) ─────────────
            [ID_KONTRAHENTA]  INT                            NOT NULL,

            -- ── Kto zlecił ───────────────────────────────────────────────────
            [ID_USER]         INT                                NULL,

            -- ── Typ i szablon ────────────────────────────────────────────────
            [MonitType]       NVARCHAR(20)                   NOT NULL,
            [TemplateID]      INT                                NULL,

            -- ── Status wysyłki ───────────────────────────────────────────────
            [Status]          NVARCHAR(20)                   NOT NULL
                              CONSTRAINT [DF_skw_MonitHistory_Status] DEFAULT ('pending'),

            -- ── Adresowanie ──────────────────────────────────────────────────
            [Recipient]       NVARCHAR(100)                      NULL,  -- email lub tel

            -- ── Treść (kopia w momencie wysyłki) ────────────────────────────
            [Subject]         NVARCHAR(200)                      NULL,
            [MessageBody]     NVARCHAR(MAX)                      NULL,

            -- ── Dane finansowe (snapshot w momencie wysyłki) ─────────────────
            [TotalDebt]       DECIMAL(18,2)                      NULL,
            [InvoiceNumbers]  NVARCHAR(500)                      NULL,  -- JSON array

            -- ── Plik PDF ─────────────────────────────────────────────────────
            [PDFPath]         NVARCHAR(500)                      NULL,  -- NULL = blob on-demand

            -- ── Integracja zewnętrzna ─────────────────────────────────────────
            [ExternalID]      NVARCHAR(100)                      NULL,  -- ID z bramki SMS/Email

            -- ── Harmonogram i historia statusów ──────────────────────────────
            [ScheduledAt]     DATETIME                           NULL,
            [SentAt]          DATETIME                           NULL,
            [DeliveredAt]     DATETIME                           NULL,
            [OpenedAt]        DATETIME                           NULL,
            [ClickedAt]       DATETIME                           NULL,

            -- ── Błędy i retry ────────────────────────────────────────────────
            [ErrorMessage]    NVARCHAR(500)                      NULL,
            [RetryCount]      INT                            NOT NULL
                              CONSTRAINT [DF_skw_MonitHistory_RetryCount] DEFAULT (0),

            -- ── Koszty ───────────────────────────────────────────────────────
            [Cost]            DECIMAL(10,4)                      NULL,

            -- ── Status rekordu ───────────────────────────────────────────────
            [IsActive]        BIT                            NOT NULL
                              CONSTRAINT [DF_skw_MonitHistory_IsActive]  DEFAULT (1),

            -- ── Timestampy ───────────────────────────────────────────────────
            [CreatedAt]       DATETIME                       NOT NULL
                              CONSTRAINT [DF_skw_MonitHistory_CreatedAt] DEFAULT (GETDATE()),
            [UpdatedAt]       DATETIME                           NULL,

            -- ── Constraints ──────────────────────────────────────────────────
            CONSTRAINT [PK_skw_MonitHistory]
                PRIMARY KEY CLUSTERED ([ID_MONIT] ASC),

            CONSTRAINT [CK_skw_MonitHistory_MonitType]
                CHECK ([MonitType] IN ('email', 'sms', 'print')),

            CONSTRAINT [CK_skw_MonitHistory_Status]
                CHECK ([Status] IN (
                    'pending','sent','delivered',
                    'bounced','failed','opened','clicked'
                )),

            CONSTRAINT [CK_skw_MonitHistory_RetryCount]
                CHECK ([RetryCount] >= 0),

            CONSTRAINT [CK_skw_MonitHistory_TotalDebt]
                CHECK ([TotalDebt] IS NULL OR [TotalDebt] >= 0),

            -- FK → skw_Users (SET NULL — historia zostaje po usunięciu usera)
            CONSTRAINT [FK_skw_MonitHistory_UserID]
                FOREIGN KEY ([ID_USER])
                REFERENCES [dbo_ext].[skw_Users] ([ID_USER])
                ON DELETE SET NULL
                ON UPDATE NO ACTION,

            -- FK → skw_Templates (SET NULL — historia zostaje po usunięciu szablonu)
            CONSTRAINT [FK_skw_MonitHistory_TemplateID]
                FOREIGN KEY ([TemplateID])
                REFERENCES [dbo_ext].[skw_Templates] ([ID_TEMPLATE])
                ON DELETE SET NULL
                ON UPDATE NO ACTION
        );

        PRINT '[009] Tabela dbo_ext.skw_MonitHistory utworzona.';
    END
    ELSE
    BEGIN
        PRINT '[009] Tabela dbo_ext.skw_MonitHistory już istnieje — pominięto.';
    END

    -- ── Indeksy ───────────────────────────────────────────────────────────────

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_MonitHistory')
          AND  name      = 'IX_skw_MonitHistory_Kontrahent'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_MonitHistory_Kontrahent]
            ON [dbo_ext].[skw_MonitHistory] ([ID_KONTRAHENTA] ASC, [CreatedAt] DESC);
        PRINT '[009] Indeks IX_skw_MonitHistory_Kontrahent utworzony.';
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_MonitHistory')
          AND  name      = 'IX_skw_MonitHistory_Status'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_MonitHistory_Status]
            ON [dbo_ext].[skw_MonitHistory] ([Status] ASC, [CreatedAt] DESC);
        PRINT '[009] Indeks IX_skw_MonitHistory_Status utworzony.';
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_MonitHistory')
          AND  name      = 'IX_skw_MonitHistory_UserID'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_MonitHistory_UserID]
            ON [dbo_ext].[skw_MonitHistory] ([ID_USER] ASC, [CreatedAt] DESC)
            WHERE [ID_USER] IS NOT NULL;
        PRINT '[009] Indeks IX_skw_MonitHistory_UserID utworzony.';
    END

    COMMIT TRANSACTION;
    PRINT '[009] === DDL 009: skw_MonitHistory — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT '[009] BŁĄD: ' + ERROR_MESSAGE();
    THROW;
END CATCH
GO