-- ============================================================
-- database/ddl/013_comments.sql
-- Tabela: dbo_ext.skw_Comments
--
-- Komentarze do kontrahentów (dłużników).
-- ID_KONTRAHENTA: klucz z WAPRO (BEZ FK constraint — inna baza logiczna).
-- Usunięcie: dwuetapowe (token potwierdzający, TTL z SystemConfig).
-- Autor: UzytkownikID (NIE ID_USER) — zachowanie zgodności nazewniczej.
--
-- ⚠️ WAŻNE: Nie można usunąć usera, który ma komentarze (FK RESTRICT).
--
-- Powiązania:
--   → skw_Users.ID_USER jako UzytkownikID (FK RESTRICT)
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
          AND  t.name = 'skw_Comments'
    )
    BEGIN
        PRINT '[013] Tworzenie tabeli dbo_ext.skw_Comments...';

        CREATE TABLE [dbo_ext].[skw_Comments] (

            -- ── Klucz główny ──────────────────────────────────────────────────
            [ID_COMMENT]      INT            IDENTITY(1,1)  NOT NULL,

            -- ── Powiązanie z WAPRO (bez FK — inna baza logiczna) ─────────────
            [ID_KONTRAHENTA]  INT                           NOT NULL,

            -- ── Treść komentarza ─────────────────────────────────────────────
            [Tresc]           NVARCHAR(MAX)                 NOT NULL,

            -- ── Autor (RESTRICT — nie można usunąć usera z komentarzami) ──────
            [UzytkownikID]    INT                           NOT NULL,

            -- ── Status (soft-delete) ─────────────────────────────────────────
            [IsActive]        BIT                           NOT NULL
                              CONSTRAINT [DF_skw_Comments_IsActive]  DEFAULT (1),

            -- ── Timestampy ───────────────────────────────────────────────────
            [CreatedAt]       DATETIME                      NOT NULL
                              CONSTRAINT [DF_skw_Comments_CreatedAt] DEFAULT (GETDATE()),
            [UpdatedAt]       DATETIME                          NULL,

            -- ── Constraints ──────────────────────────────────────────────────
            CONSTRAINT [PK_skw_Comments]
                PRIMARY KEY CLUSTERED ([ID_COMMENT] ASC),

            -- FK → skw_Users przez UzytkownikID (RESTRICT — blokuje usunięcie usera)
            CONSTRAINT [FK_skw_Comments_UzytkownikID]
                FOREIGN KEY ([UzytkownikID])
                REFERENCES [dbo_ext].[skw_Users] ([ID_USER])
                ON DELETE NO ACTION
                ON UPDATE NO ACTION
        );

        PRINT '[013] Tabela dbo_ext.skw_Comments utworzona.';
    END
    ELSE
    BEGIN
        PRINT '[013] Tabela dbo_ext.skw_Comments już istnieje — pominięto.';
    END

    -- ── Indeksy ───────────────────────────────────────────────────────────────

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_Comments')
          AND  name      = 'IX_skw_Comments_Kontrahent'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_Comments_Kontrahent]
            ON [dbo_ext].[skw_Comments] ([ID_KONTRAHENTA] ASC, [IsActive] ASC, [CreatedAt] DESC);
        PRINT '[013] Indeks IX_skw_Comments_Kontrahent utworzony.';
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_Comments')
          AND  name      = 'IX_skw_Comments_UzytkownikID'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_Comments_UzytkownikID]
            ON [dbo_ext].[skw_Comments] ([UzytkownikID] ASC, [CreatedAt] DESC);
        PRINT '[013] Indeks IX_skw_Comments_UzytkownikID utworzony.';
    END

    COMMIT TRANSACTION;
    PRINT '[013] === DDL 013: skw_Comments — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT '[013] BŁĄD: ' + ERROR_MESSAGE();
    THROW;
END CATCH
GO