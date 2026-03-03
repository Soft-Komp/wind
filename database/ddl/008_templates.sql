-- ============================================================
-- database/ddl/008_templates.sql
-- Tabela: dbo_ext.skw_Templates
--
-- ⚠️ MUSI być uruchomiony PRZED 009_monit_history.sql
--    (FK skw_MonitHistory.TemplateID → skw_Templates.ID_TEMPLATE)
--
-- Szablony wiadomości Jinja2:
--   email  → Subject + Body z HTML
--   sms    → Body (plain text, max ~160 znaków)
--   print  → Body z HTML do PDF
--
-- Zmienne Jinja2: {{ debtor_name }}, {{ total_debt }}, {{ invoice_list }}
--
-- Powiązania:
--   ← skw_MonitHistory.TemplateID (FK SET NULL)
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
          AND  t.name = 'skw_Templates'
    )
    BEGIN
        PRINT '[008] Tworzenie tabeli dbo_ext.skw_Templates...';

        CREATE TABLE [dbo_ext].[skw_Templates] (

            -- ── Klucz główny ──────────────────────────────────────────────────
            [ID_TEMPLATE]  INT            IDENTITY(1,1)  NOT NULL,

            -- ── Identyfikacja szablonu ───────────────────────────────────────
            [TemplateName] NVARCHAR(100)                 NOT NULL,
            [TemplateType] NVARCHAR(20)                  NOT NULL,

            -- ── Treść ────────────────────────────────────────────────────────
            [Subject]      NVARCHAR(200)                     NULL,  -- tylko dla email
            [Body]         NVARCHAR(MAX)                 NOT NULL,

            -- ── Status ───────────────────────────────────────────────────────
            [IsActive]     BIT                           NOT NULL
                           CONSTRAINT [DF_skw_Templates_IsActive]  DEFAULT (1),

            -- ── Timestampy ───────────────────────────────────────────────────
            [CreatedAt]    DATETIME                      NOT NULL
                           CONSTRAINT [DF_skw_Templates_CreatedAt] DEFAULT (GETDATE()),
            [UpdatedAt]    DATETIME                          NULL,

            -- ── Constraints ──────────────────────────────────────────────────
            CONSTRAINT [PK_skw_Templates]
                PRIMARY KEY CLUSTERED ([ID_TEMPLATE] ASC),

            CONSTRAINT [UQ_skw_Templates_TemplateName]
                UNIQUE ([TemplateName]),

            CONSTRAINT [CK_skw_Templates_TemplateType]
                CHECK ([TemplateType] IN ('email', 'sms', 'print')),

            -- Subject wymagany tylko dla email, brak dla sms/print
            CONSTRAINT [CK_skw_Templates_Subject_Email]
                CHECK (
                    ([TemplateType] = 'email' AND [Subject] IS NOT NULL)
                    OR [TemplateType] <> 'email'
                )
        );

        PRINT '[008] Tabela dbo_ext.skw_Templates utworzona.';
    END
    ELSE
    BEGIN
        PRINT '[008] Tabela dbo_ext.skw_Templates już istnieje — pominięto.';
    END

    -- ── Indeksy ───────────────────────────────────────────────────────────────

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_Templates')
          AND  name      = 'IX_skw_Templates_Type_Active'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_Templates_Type_Active]
            ON [dbo_ext].[skw_Templates] ([TemplateType] ASC, [IsActive] ASC);
        PRINT '[008] Indeks IX_skw_Templates_Type_Active utworzony.';
    END

    COMMIT TRANSACTION;
    PRINT '[008] === DDL 008: skw_Templates — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT '[008] BŁĄD: ' + ERROR_MESSAGE();
    THROW;
END CATCH
GO