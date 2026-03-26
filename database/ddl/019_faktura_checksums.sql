-- =============================================================================
-- PLIK:    019_faktura_checksums.sql
-- MODUŁ:   Akceptacja Faktur KSeF — Sprint 2 / Sesja 2
-- SERWER:  GPGKJASLO (192.168.0.50) | BAZA: GPGKJASLO
-- AUTOR:   Windykacja-gpgk-backend
-- DATA:    2026-03-26
--
-- ZAWARTOŚĆ:
--   Rejestracja checksumów dwóch nowych widoków w dbo_ext.skw_SchemaChecksums.
--   Checksuma obliczana DYNAMICZNIE z sys.sql_modules — zawsze odpowiada
--   rzeczywistej definicji widoku w bazie, bez ręcznego wpisywania wartości.
--
-- KOLEJNOŚĆ URUCHOMIENIA:
--   ⚠️  MUSI być uruchomiony PO 018_faktura_widoki_dbo.sql
--   Jeśli widoki nie istnieją, MERGE nic nie wstawi (ostrzeżenie na końcu).
--
-- IDEMPOTENTNOŚĆ:
--   MERGE — bezpieczne wielokrotne uruchomienie.
--   Jeśli checksum istnieje → UPDATE wartości.
--   Jeśli nie istnieje → INSERT.
--
-- STRUKTURA skw_SchemaChecksums (z TABELE_REFERENCJA.md):
--   ID_CHECKSUM    INT IDENTITY PK
--   ObjectName     NVARCHAR(200) NOT NULL
--   ObjectType     NVARCHAR(50)  ('VIEW' / 'PROCEDURE')
--   Checksum       INT NOT NULL  (CHECKSUM z sys.sql_modules)
--   AlembicRevision NVARCHAR(50) NULL
--   LastVerifiedAt  DATETIME NULL
--   CreatedAt      DATETIME DEFAULT GETDATE()
--   UpdatedAt      DATETIME NULL
-- =============================================================================

USE [GPGKJASLO];
GO

-- ===========================================================================
-- KROK 1: Weryfikacja — czy widoki faktycznie istnieją?
-- Jeśli poniższe zapytanie zwróci 0 wierszy dla któregoś widoku,
-- ZATRZYMAJ się i uruchom najpierw 018_faktura_widoki_dbo.sql.
-- ===========================================================================
DECLARE @views_count INT;

SELECT @views_count = COUNT(*)
FROM sys.objects o
JOIN sys.sql_modules m ON m.object_id = o.object_id
WHERE
    SCHEMA_NAME(o.schema_id) = 'dbo'
    AND o.name IN (
        'skw_faktury_akceptacja_naglowek',
        'skw_faktury_akceptacja_pozycje'
    )
    AND o.type = 'V';

IF @views_count < 2
BEGIN
    RAISERROR(
        N'[BŁĄD] Znaleziono tylko %d z 2 wymaganych widoków w schemacie dbo. '
        N'Uruchom najpierw 018_faktura_widoki_dbo.sql.',
        16, 1, @views_count
    );
    RETURN;
END

PRINT N'[OK] Oba widoki istnieją w schemacie dbo. Rejestruję checksums...';
GO

-- ===========================================================================
-- KROK 2: MERGE checksumów — wstaw lub zaktualizuj
-- ===========================================================================

-- Źródło: dynamicznie obliczone checksums z sys.sql_modules
-- CHECKSUM() wbudowana funkcja MSSQL — oblicza hash z definicji SQL widoku.
-- Wynik jest INT, może być ujemny — to normalne zachowanie CHECKSUM().

MERGE dbo_ext.skw_SchemaChecksums AS target

USING (
    -- Dynamicznie pobierz definicje widoków i oblicz checksums
    SELECT
        o.name                         AS ObjectName,
        N'VIEW'                        AS ObjectType,
        CHECKSUM(m.definition)         AS Checksum,
        N'007'                         AS AlembicRevision,
        -- Dodatkowe metadane dla diagnostyki
        o.modify_date                  AS LastModified,
        SCHEMA_NAME(o.schema_id)       AS SchemaName
    FROM sys.sql_modules m
    JOIN sys.objects o ON m.object_id = o.object_id
    WHERE
        SCHEMA_NAME(o.schema_id) = 'dbo'
        AND o.name IN (
            'skw_faktury_akceptacja_naglowek',
            'skw_faktury_akceptacja_pozycje'
        )
        AND o.type = 'V'
) AS source
    ON  target.ObjectName = source.ObjectName
    AND target.ObjectType = source.ObjectType

-- Widok już zarejestrowany → zaktualizuj checksum i znacznik weryfikacji
WHEN MATCHED THEN
    UPDATE SET
        target.Checksum        = source.Checksum,
        target.AlembicRevision = source.AlembicRevision,
        target.LastVerifiedAt  = GETDATE(),
        target.UpdatedAt       = GETDATE()

-- Nowy widok → wstaw nowy rekord
WHEN NOT MATCHED THEN
    INSERT (
        ObjectName,
        ObjectType,
        Checksum,
        AlembicRevision,
        LastVerifiedAt,
        CreatedAt
    )
    VALUES (
        source.ObjectName,
        source.ObjectType,
        source.Checksum,
        source.AlembicRevision,
        GETDATE(),
        GETDATE()
    );

GO

-- ===========================================================================
-- KROK 3: Weryfikacja po MERGE
-- ===========================================================================
SELECT
    ID_CHECKSUM,
    ObjectName,
    ObjectType,
    Checksum,
    AlembicRevision,
    LastVerifiedAt,
    CreatedAt,
    UpdatedAt
FROM dbo_ext.skw_SchemaChecksums
WHERE ObjectName IN (
    'skw_faktury_akceptacja_naglowek',
    'skw_faktury_akceptacja_pozycje'
)
ORDER BY CreatedAt;

-- Oczekiwany wynik: 2 wiersze, ObjectType='VIEW', AlembicRevision='007',
-- Checksum ≠ 0, LastVerifiedAt ≈ TERAZ
GO

-- ===========================================================================
-- KROK 4: Pełny stan tabeli skw_SchemaChecksums po aktualizacji
-- (diagnostyka — ile widoków jest łącznie zarejestrowanych)
-- ===========================================================================
SELECT
    COUNT(*) AS LacznaLiczbaZarejestrowanychObiektow,
    SUM(CASE WHEN ObjectType = 'VIEW'      THEN 1 ELSE 0 END) AS LiczbaWidokow,
    SUM(CASE WHEN ObjectType = 'PROCEDURE' THEN 1 ELSE 0 END) AS LiczbaProcedur
FROM dbo_ext.skw_SchemaChecksums;
GO

-- =============================================================================
-- NASTĘPNY KROK: uruchom 020_faktura_triggers_updated_at.sql
-- =============================================================================