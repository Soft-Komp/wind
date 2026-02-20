-- =============================================================================
-- database/ddl/011_schema_checksums.sql
-- =============================================================================
-- Tabela: dbo_ext.SchemaChecksums
-- Cel: Rejestr sum kontrolnych widoków i procedur — ochrona integralności schematu
--
-- WERSJA 1.1.0 — AUDIT_ZGODNOSCI R5:
--   + Kolumna SchemaName NVARCHAR(20) NOT NULL DEFAULT 'dbo_ext'
--   + CHECK constraint na ObjectType: VIEW / PROCEDURE / INDEX
--   + CHECK constraint na SchemaName: dbo / dbo_ext
--   + UNIQUE constraint: (ObjectName, SchemaName, ObjectType)
--
-- WERSJA 1.0.0 — wersja bazowa
--   Kolumny: ID_CHECKSUM, ObjectName, ObjectType, Checksum,
--            AlembicRevision, LastVerifiedAt, CreatedAt, UpdatedAt
--
-- WAŻNE: skrypt jest idempotentny (IF NOT EXISTS / IF COL_LENGTH)
--   Bezpiecznie uruchamialny wielokrotnie — nie nadpisze danych.
--   Przy pierwszym uruchomieniu: tworzy tabelę od zera.
--   Przy kolejnych: sprawdza czy kolumna SchemaName istnieje i ją dodaje.
--
-- Kolejność w DDL: po 010_system_config.sql, przed 012_master_access_log.sql
--
-- Data: 2026-02-17 | Faza: 0 — naprawa R5
-- =============================================================================

SET NOCOUNT ON;
SET XACT_ABORT ON;  -- rollback całej transakcji przy błędzie

BEGIN TRANSACTION;

BEGIN TRY

    -- =========================================================================
    -- KROK 1: Utwórz tabelę jeśli nie istnieje (pierwsza instalacja)
    -- =========================================================================

    IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo_ext'
          AND t.name = 'SchemaChecksums'
    )
    BEGIN
        PRINT '[011] Tworzenie tabeli dbo_ext.SchemaChecksums v1.1.0...';

        CREATE TABLE [dbo_ext].[SchemaChecksums] (
            -- Klucz główny
            [ID_CHECKSUM]       INT IDENTITY(1,1)   NOT NULL,

            -- Identyfikacja obiektu
            [ObjectName]        NVARCHAR(200)        NOT NULL,
            [ObjectType]        NVARCHAR(50)         NOT NULL,
            [SchemaName]        NVARCHAR(20)         NOT NULL    -- [R5] NOWA
                                CONSTRAINT [DF_SchemaChecksums_SchemaName]
                                DEFAULT ('dbo_ext'),

            -- Suma kontrolna
            [Checksum]          INT                  NOT NULL,

            -- Wersja migracji
            [AlembicRevision]   NVARCHAR(50)         NULL,

            -- Weryfikacja
            [LastVerifiedAt]    DATETIME             NULL,

            -- Timestamps
            [CreatedAt]         DATETIME             NOT NULL
                                CONSTRAINT [DF_SchemaChecksums_CreatedAt]
                                DEFAULT (GETDATE()),
            [UpdatedAt]         DATETIME             NULL,

            -- ── Constraints ──────────────────────────────────────────────────

            CONSTRAINT [PK_SchemaChecksums]
                PRIMARY KEY CLUSTERED ([ID_CHECKSUM] ASC),

            -- [R5] CHECK: tylko dozwolone typy obiektów
            CONSTRAINT [CK_SchemaChecksums_ObjectType]
                CHECK ([ObjectType] IN ('VIEW', 'PROCEDURE', 'INDEX')),

            -- [R5] CHECK: tylko monitorowane schematy
            CONSTRAINT [CK_SchemaChecksums_SchemaName]
                CHECK ([SchemaName] IN ('dbo', 'dbo_ext')),

            -- [R5] UNIQUE: jeden checksum per obiekt (schemat + nazwa + typ)
            --   Bez tego constraint można mieć duplikaty przy reinsercie.
            CONSTRAINT [UQ_SchemaChecksums_Object]
                UNIQUE ([ObjectName], [SchemaName], [ObjectType])
        );

        PRINT '[011] Tabela dbo_ext.SchemaChecksums utworzona pomyślnie (v1.1.0).';
    END
    ELSE
    BEGIN
        PRINT '[011] Tabela dbo_ext.SchemaChecksums już istnieje — sprawdzam migrację do v1.1.0...';

        -- =====================================================================
        -- KROK 2: Migracja v1.0 → v1.1 — dodaj SchemaName jeśli nie istnieje
        -- Bezpieczne dla istniejącej tabeli z danymi.
        -- =====================================================================

        -- 2a. Dodaj kolumnę SchemaName
        IF COL_LENGTH('[dbo_ext].[SchemaChecksums]', 'SchemaName') IS NULL
        BEGIN
            PRINT '[011] Migracja v1.0→v1.1: Dodawanie kolumny SchemaName...';

            ALTER TABLE [dbo_ext].[SchemaChecksums]
                ADD [SchemaName] NVARCHAR(20) NOT NULL
                    CONSTRAINT [DF_SchemaChecksums_SchemaName]
                    DEFAULT ('dbo_ext');

            -- Istniejące wiersze dostaną wartość 'dbo_ext' (default)
            -- Wiersze dla widoków dbo.VIEW_* należy zaktualizować ręcznie:
            -- UPDATE [dbo_ext].[SchemaChecksums]
            --   SET [SchemaName] = 'dbo'
            --   WHERE [ObjectName] LIKE 'VIEW_%';

            PRINT '[011] Kolumna SchemaName dodana. UWAGA: Zaktualizuj SchemaName=''dbo'' dla widoków WAPRO.';
        END
        ELSE
        BEGIN
            PRINT '[011] Kolumna SchemaName już istnieje — pomijam.';
        END

        -- 2b. Dodaj CHECK constraint na ObjectType (jeśli brak)
        IF NOT EXISTS (
            SELECT 1
            FROM sys.check_constraints cc
            JOIN sys.tables t ON cc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'dbo_ext'
              AND t.name = 'SchemaChecksums'
              AND cc.name = 'CK_SchemaChecksums_ObjectType'
        )
        BEGIN
            PRINT '[011] Dodawanie CHECK constraint CK_SchemaChecksums_ObjectType...';

            -- Najpierw upewnij się że istniejące dane są zgodne
            IF EXISTS (
                SELECT 1
                FROM [dbo_ext].[SchemaChecksums]
                WHERE [ObjectType] NOT IN ('VIEW', 'PROCEDURE', 'INDEX')
            )
            BEGIN
                RAISERROR(
                    '[011] BŁĄD: Istniejące dane zawierają niedozwolone wartości ObjectType. '
                    'Sprawdź i popraw przed dodaniem CHECK constraint.',
                    16, 1
                );
            END

            ALTER TABLE [dbo_ext].[SchemaChecksums]
                ADD CONSTRAINT [CK_SchemaChecksums_ObjectType]
                CHECK ([ObjectType] IN ('VIEW', 'PROCEDURE', 'INDEX'));

            PRINT '[011] CHECK constraint CK_SchemaChecksums_ObjectType dodany.';
        END

        -- 2c. Dodaj CHECK constraint na SchemaName (jeśli brak)
        IF NOT EXISTS (
            SELECT 1
            FROM sys.check_constraints cc
            JOIN sys.tables t ON cc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'dbo_ext'
              AND t.name = 'SchemaChecksums'
              AND cc.name = 'CK_SchemaChecksums_SchemaName'
        )
        BEGIN
            PRINT '[011] Dodawanie CHECK constraint CK_SchemaChecksums_SchemaName...';

            ALTER TABLE [dbo_ext].[SchemaChecksums]
                ADD CONSTRAINT [CK_SchemaChecksums_SchemaName]
                CHECK ([SchemaName] IN ('dbo', 'dbo_ext'));

            PRINT '[011] CHECK constraint CK_SchemaChecksums_SchemaName dodany.';
        END

        -- 2d. Dodaj UNIQUE constraint na (ObjectName, SchemaName, ObjectType) jeśli brak
        IF NOT EXISTS (
            SELECT 1
            FROM sys.key_constraints kc
            JOIN sys.tables t ON kc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'dbo_ext'
              AND t.name = 'SchemaChecksums'
              AND kc.name = 'UQ_SchemaChecksums_Object'
        )
        BEGIN
            PRINT '[011] Dodawanie UNIQUE constraint UQ_SchemaChecksums_Object...';

            -- Sprawdź duplikaty przed dodaniem UNIQUE
            IF EXISTS (
                SELECT [ObjectName], [SchemaName], [ObjectType], COUNT(*) AS cnt
                FROM [dbo_ext].[SchemaChecksums]
                GROUP BY [ObjectName], [SchemaName], [ObjectType]
                HAVING COUNT(*) > 1
            )
            BEGIN
                RAISERROR(
                    '[011] BŁĄD: Znaleziono duplikaty (ObjectName, SchemaName, ObjectType). '
                    'Usuń duplikaty przed dodaniem UNIQUE constraint.',
                    16, 1
                );
            END

            ALTER TABLE [dbo_ext].[SchemaChecksums]
                ADD CONSTRAINT [UQ_SchemaChecksums_Object]
                UNIQUE ([ObjectName], [SchemaName], [ObjectType]);

            PRINT '[011] UNIQUE constraint UQ_SchemaChecksums_Object dodany.';
        END

        PRINT '[011] Migracja v1.0→v1.1 zakończona pomyślnie.';
    END

    -- =========================================================================
    -- KROK 3: Indeks wydajnościowy na LastVerifiedAt
    -- Używany przez schema_integrity.py przy każdym starcie (ORDER BY LastVerifiedAt)
    -- =========================================================================

    IF NOT EXISTS (
        SELECT 1
        FROM sys.indexes i
        JOIN sys.tables t ON i.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo_ext'
          AND t.name = 'SchemaChecksums'
          AND i.name = 'IX_SchemaChecksums_LastVerifiedAt'
    )
    BEGIN
        PRINT '[011] Tworzenie indeksu IX_SchemaChecksums_LastVerifiedAt...';

        CREATE NONCLUSTERED INDEX [IX_SchemaChecksums_LastVerifiedAt]
            ON [dbo_ext].[SchemaChecksums] ([LastVerifiedAt] ASC)
            INCLUDE ([ObjectName], [SchemaName], [ObjectType], [Checksum]);

        PRINT '[011] Indeks IX_SchemaChecksums_LastVerifiedAt utworzony.';
    END

    COMMIT TRANSACTION;
    PRINT '[011] === Skrypt 011_schema_checksums.sql zakończony sukcesem ===';

END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine    INT            = ERROR_LINE();
    DECLARE @ErrorSeverity INT           = ERROR_SEVERITY();

    PRINT '[011] BŁĄD KRYTYCZNY w linii ' + CAST(@ErrorLine AS NVARCHAR) + ': ' + @ErrorMessage;

    RAISERROR(
        '[011_schema_checksums.sql] Błąd: %s (linia: %d)',
        @ErrorSeverity,
        1,
        @ErrorMessage,
        @ErrorLine
    );
END CATCH;

-- =============================================================================
-- WERYFIKACJA — uruchom po skrypcie żeby potwierdzić stan tabeli
-- =============================================================================
/*
SELECT
    t.name                          AS TableName,
    s.name                          AS SchemaName,
    c.name                          AS ColumnName,
    tp.name                         AS DataType,
    c.max_length                    AS MaxLength,
    c.is_nullable                   AS IsNullable,
    dc.definition                   AS DefaultValue
FROM sys.columns c
JOIN sys.tables t  ON c.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.types tp  ON c.user_type_id = tp.user_type_id
LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
WHERE s.name = 'dbo_ext'
  AND t.name = 'SchemaChecksums'
ORDER BY c.column_id;

-- Sprawdź constraints:
SELECT cc.name, cc.definition
FROM sys.check_constraints cc
JOIN sys.tables t ON cc.parent_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'dbo_ext' AND t.name = 'SchemaChecksums';
*/