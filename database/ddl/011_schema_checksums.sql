-- ============================================================
-- database/ddl/011_schema_checksums.sql
-- Tabela: dbo_ext.skw_SchemaChecksums
--
-- Rejestr sum kontrolnych widoków i procedur składowanych.
-- Ochrona integralności schematu — weryfikacja przy każdym starcie.
--
-- Mechanizm:
--   startup FastAPI → schema_integrity.verify()
--     → pobierz stored checksums z skw_SchemaChecksums
--     → oblicz aktualne z sys.sql_modules
--     → porównaj każdy obiekt
--     → MISMATCH → reakcja wg SystemConfig: schema_integrity.reaction
--       WARN  → log WARNING, start OK
--       ALERT → log CRITICAL + SSE system_notification, start OK
--       BLOCK → log CRITICAL + SystemExit(1)  ← domyślne
--
-- ObjectType: VIEW | PROCEDURE | INDEX
-- SchemaName: dbo (widoki WAPRO) | dbo_ext (custom)
--
-- Idempotentny — bezpiecznie uruchamiany wielokrotnie.
-- ============================================================

USE [WAPRO];
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

BEGIN TRANSACTION;
BEGIN TRY

    -- ── Tworzenie tabeli (pierwsza instalacja) ────────────────────────────────

    IF NOT EXISTS (
        SELECT 1
        FROM   sys.tables  t
        JOIN   sys.schemas s ON t.schema_id = s.schema_id
        WHERE  s.name = 'dbo_ext'
          AND  t.name = 'skw_SchemaChecksums'
    )
    BEGIN
        PRINT '[011] Tworzenie tabeli dbo_ext.skw_SchemaChecksums v2.0.0...';

        CREATE TABLE [dbo_ext].[skw_SchemaChecksums] (

            -- ── Klucz główny ──────────────────────────────────────────────────
            [ID_CHECKSUM]      INT            IDENTITY(1,1)  NOT NULL,

            -- ── Identyfikacja obiektu ────────────────────────────────────────
            [ObjectName]       NVARCHAR(200)                 NOT NULL,
            [ObjectType]       NVARCHAR(50)                  NOT NULL,
            [SchemaName]       NVARCHAR(20)                  NOT NULL
                               CONSTRAINT [DF_skw_SchemaChecksums_SchemaName]
                               DEFAULT ('dbo_ext'),

            -- ── Suma kontrolna ───────────────────────────────────────────────
            [Checksum]         INT                           NOT NULL,

            -- ── Wersja Alembic ───────────────────────────────────────────────
            [AlembicRevision]  NVARCHAR(50)                      NULL,

            -- ── Weryfikacja ──────────────────────────────────────────────────
            [LastVerifiedAt]   DATETIME                          NULL,

            -- ── Timestampy ───────────────────────────────────────────────────
            [CreatedAt]        DATETIME                      NOT NULL
                               CONSTRAINT [DF_skw_SchemaChecksums_CreatedAt]
                               DEFAULT (GETDATE()),
            [UpdatedAt]        DATETIME                          NULL,

            -- ── Constraints ──────────────────────────────────────────────────
            CONSTRAINT [PK_skw_SchemaChecksums]
                PRIMARY KEY CLUSTERED ([ID_CHECKSUM] ASC),

            CONSTRAINT [CK_skw_SchemaChecksums_ObjectType]
                CHECK ([ObjectType] IN ('VIEW', 'PROCEDURE', 'INDEX')),

            CONSTRAINT [CK_skw_SchemaChecksums_SchemaName]
                CHECK ([SchemaName] IN ('dbo', 'dbo_ext')),

            -- Jeden checksum per obiekt (schemat + nazwa + typ)
            CONSTRAINT [UQ_skw_SchemaChecksums_Object]
                UNIQUE ([ObjectName], [SchemaName], [ObjectType])
        );

        PRINT '[011] Tabela dbo_ext.skw_SchemaChecksums utworzona (v2.0.0).';
    END
    ELSE
    BEGIN
        PRINT '[011] Tabela dbo_ext.skw_SchemaChecksums już istnieje — pominięto.';
    END

    COMMIT TRANSACTION;
    PRINT '[011] === DDL 011: skw_SchemaChecksums — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT '[011] BŁĄD: ' + ERROR_MESSAGE();
    THROW;
END CATCH
GO