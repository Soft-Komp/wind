-- ============================================================
-- FILE:    database/ddl/015_faktura_akceptacja.sql
-- MODUŁ:   Akceptacja Faktur KSeF
-- TABELA:  dbo_ext.skw_faktura_akceptacja
-- WERSJA:  1.0
-- DATA:    2026-03-26
-- SCHEMAT: dbo_ext
-- ============================================================
-- OPIS:
--   Główna tabela modułu Akceptacji Faktur.
--   Jedna faktura KSeF = jeden wiersz w tej tabeli.
--   numer_ksef = unikalny identyfikator KSeF (twarda referencja).
--
-- IDEMPOTENTNY: TAK — IF NOT EXISTS, nazwy constraints są unikalne
-- WYKONANIE:    SSMS (raz, przy świeżej instalacji LUB aktualizacji)
--
-- POWIĄZANA MIGRACJA:
--   backend/alembic/versions/0006_faktura_akceptacja.py
--
-- ZALEŻNOŚCI (muszą istnieć PRZED uruchomieniem):
--   ✅ dbo_ext.skw_Users (FK: utworzony_przez → ID_USER)
--
-- NASTĘPNY PLIK:
--   016_faktura_przypisanie.sql (FK: faktura_id → id tej tabeli)
--
-- STATUS CYCLE:
--   nowe → w_toku → zaakceptowana
--                 ↘ anulowana
--
-- SOFT-DELETE:
--   IsActive = 0 przy anulowaniu (NIE DELETE).
--   Archiwum JSON.gz tworzone przez application layer.
-- ============================================================

USE [WAPRO];
GO

PRINT N'';
PRINT N'=== [015] START: dbo_ext.skw_faktura_akceptacja ===';
PRINT N'=== Timestamp: ' + CONVERT(NVARCHAR(30), GETDATE(), 126) + N' ===';
PRINT N'';
GO

-- ── Główna tabela ────────────────────────────────────────────────────────────
IF NOT EXISTS (
    SELECT 1
    FROM   sys.tables  t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE  s.name = N'dbo_ext'
      AND  t.name = N'skw_faktura_akceptacja'
)
BEGIN
    PRINT N'[015] Tworzenie tabeli dbo_ext.skw_faktura_akceptacja...';

    CREATE TABLE [dbo_ext].[skw_faktura_akceptacja] (

        -- ── Identyfikacja ────────────────────────────────────────────────────
        id                  INT          IDENTITY(1,1) NOT NULL,
        numer_ksef          NVARCHAR(50)               NOT NULL,

        -- ── Status i priorytet ───────────────────────────────────────────────
        -- Dozwolone wartości chronione przez CHECK constraints poniżej
        status_wewnetrzny   NVARCHAR(20)               NOT NULL,
        priorytet           NVARCHAR(15)               NOT NULL
            CONSTRAINT [DF_sfa_priorytet]   DEFAULT N'normalny',

        -- ── Opis dokumentu ───────────────────────────────────────────────────
        opis_dokumentu      NVARCHAR(MAX)                  NULL,   -- formalny opis (referent)
        uwagi               NVARCHAR(MAX)                  NULL,   -- nieformalne uwagi

        -- ── Metadane ─────────────────────────────────────────────────────────
        utworzony_przez     INT                        NOT NULL,   -- FK → skw_Users
        IsActive            BIT                        NOT NULL
            CONSTRAINT [DF_sfa_IsActive]    DEFAULT 1,
        CreatedAt           DATETIME2(7)               NOT NULL
            CONSTRAINT [DF_sfa_CreatedAt]   DEFAULT GETDATE(),
        UpdatedAt           DATETIME2(7)                   NULL,   -- trigger: 020_faktura_triggers_updated_at.sql

        -- ══════════════════════════════════════════════════════════════════════
        -- CONSTRAINTS
        -- ══════════════════════════════════════════════════════════════════════

        -- Klucz główny
        CONSTRAINT [PK_skw_faktura_akceptacja]
            PRIMARY KEY CLUSTERED (id ASC)
            WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
                  ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON),

        -- Unikalność KSeF — jedna faktura = jeden obieg akceptacji
        CONSTRAINT [UQ_skw_faktura_akceptacja_numer_ksef]
            UNIQUE NONCLUSTERED (numer_ksef),

        -- FK: kto wpuścił fakturę do obiegu
        CONSTRAINT [FK_sfa_utworzony_przez]
            FOREIGN KEY (utworzony_przez)
            REFERENCES [dbo_ext].[skw_Users] (ID_USER),

        -- Walidacja statusu wewnętrznego
        CONSTRAINT [CHK_sfa_status_wewnetrzny] CHECK (
            status_wewnetrzny IN (
                N'nowe',
                N'w_toku',
                N'zaakceptowana',
                N'anulowana'
            )
        ),

        -- Walidacja priorytetu
        CONSTRAINT [CHK_sfa_priorytet] CHECK (
            priorytet IN (
                N'normalny',
                N'pilny',
                N'bardzo_pilny'
            )
        )
    );

    PRINT N'[015] ✓ Tabela dbo_ext.skw_faktura_akceptacja — UTWORZONA pomyślnie.';
    PRINT N'[015]   Constraints: PK + UQ(numer_ksef) + FK(utworzony_przez) + 2x CHK';
END
ELSE
BEGIN
    PRINT N'[015] ~ Tabela dbo_ext.skw_faktura_akceptacja — już istnieje, pomijam.';
END
GO

-- ── Weryfikacja ───────────────────────────────────────────────────────────────
PRINT N'';
PRINT N'[015] Weryfikacja struktury:';
SELECT
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.IS_NULLABLE,
    c.COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = N'dbo_ext'
  AND c.TABLE_NAME   = N'skw_faktura_akceptacja'
ORDER BY c.ORDINAL_POSITION;
GO

PRINT N'';
PRINT N'=== [015] DONE: ' + CONVERT(NVARCHAR(30), GETDATE(), 126) + N' ===';
PRINT N'';
GO