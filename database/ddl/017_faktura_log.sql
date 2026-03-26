-- ============================================================
-- FILE:    database/ddl/017_faktura_log.sql
-- MODUŁ:   Akceptacja Faktur KSeF
-- TABELA:  dbo_ext.skw_faktura_log
-- WERSJA:  1.0
-- DATA:    2026-03-26
-- SCHEMAT: dbo_ext
-- ============================================================
-- OPIS:
--   Immutable audit trail modułu Akceptacji Faktur.
--   TYLKO INSERT — nigdy UPDATE ani DELETE (analogia: skw_AuditLog).
--   Dlatego BRAK kolumny UpdatedAt i BRAK triggera UpdatedAt.
--
--   szczegoly: JSON serializowany przez model FakturaLogDetails (Sesja 4).
--              Bezpośrednie wstawianie raw dict jest ZABRONIONE przez konwencję.
--
--   user_id = NULL → akcja systemowa (auto-akceptacja, force, timeout).
--
-- IDEMPOTENTNY: TAK — IF NOT EXISTS
-- WYKONANIE:    SSMS (po 015 i 016)
--
-- POWIĄZANA MIGRACJA:
--   backend/alembic/versions/007_faktura_akceptacja.py
--
-- ZALEŻNOŚCI:
--   ✅ dbo_ext.skw_faktura_akceptacja (FK: faktura_id → id)
--   ✅ dbo_ext.skw_Users              (FK: user_id → ID_USER, ON DELETE SET NULL)
--
-- AKCJE DOZWOLONE (CHK_sfl_akcja):
--   przypisano, zaakceptowano, odrzucono, zresetowano,
--   status_zmieniony, priorytet_zmieniony,
--   fakir_update, fakir_update_failed,
--   nie_moje, force_akceptacja, anulowano
--
-- INDEKS:
--   IX_sfl_faktura_created → GET /{id}/historia (ORDER BY CreatedAt DESC)
-- ============================================================

USE [WAPRO];
GO

PRINT N'';
PRINT N'=== [017] START: dbo_ext.skw_faktura_log ===';
PRINT N'=== Timestamp: ' + CONVERT(NVARCHAR(30), GETDATE(), 126) + N' ===';
PRINT N'';
GO

-- ── Główna tabela ────────────────────────────────────────────────────────────
IF NOT EXISTS (
    SELECT 1
    FROM   sys.tables  t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE  s.name = N'dbo_ext'
      AND  t.name = N'skw_faktura_log'
)
BEGIN
    PRINT N'[017] Tworzenie tabeli dbo_ext.skw_faktura_log...';

    CREATE TABLE [dbo_ext].[skw_faktura_log] (

        -- ── Identyfikacja ────────────────────────────────────────────────────
        id          INT          IDENTITY(1,1) NOT NULL,

        -- ── Powiązania ───────────────────────────────────────────────────────
        faktura_id  INT                        NOT NULL,   -- FK → skw_faktura_akceptacja (NOT NULL)
        user_id     INT                            NULL,   -- FK → skw_Users (NULL = akcja systemowa)

        -- ── Treść logu ───────────────────────────────────────────────────────
        akcja       NVARCHAR(50)               NOT NULL,   -- typ zdarzenia (patrz CHK poniżej)
        szczegoly   NVARCHAR(MAX)                  NULL,   -- JSON (model FakturaLogDetails — Sesja 4)

        -- ── Timestamp ────────────────────────────────────────────────────────
        -- ⚠ BRAK UpdatedAt — tabela IMMUTABLE, analogia do skw_AuditLog
        -- ⚠ BRAK triggera UpdatedAt — nie dodawać do 020_faktura_triggers_updated_at.sql
        CreatedAt   DATETIME2(7)               NOT NULL
            CONSTRAINT [DF_sfl_CreatedAt] DEFAULT GETDATE(),

        -- ══════════════════════════════════════════════════════════════════════
        -- CONSTRAINTS
        -- ══════════════════════════════════════════════════════════════════════

        CONSTRAINT [PK_skw_faktura_log]
            PRIMARY KEY CLUSTERED (id ASC)
            WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF,
                  ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON),

        -- FK: do której faktury należy ten wpis logu
        CONSTRAINT [FK_sfl_faktura_id]
            FOREIGN KEY (faktura_id)
            REFERENCES [dbo_ext].[skw_faktura_akceptacja] (id),

        -- FK: kto wykonał akcję
        -- ON DELETE SET NULL: historia przeżywa dezaktywację/usunięcie pracownika
        CONSTRAINT [FK_sfl_user_id]
            FOREIGN KEY (user_id)
            REFERENCES [dbo_ext].[skw_Users] (ID_USER)
            ON DELETE SET NULL,

        -- Walidacja typów akcji — wszystkie możliwe zdarzenia w module
        CONSTRAINT [CHK_sfl_akcja] CHECK (
            akcja IN (
                N'przypisano',          -- referent przypisał pracownika(ów)
                N'zaakceptowano',       -- pracownik zaakceptował fakturę
                N'odrzucono',           -- pracownik odrzucił fakturę
                N'zresetowano',         -- referent zresetował przypisania
                N'status_zmieniony',    -- referent wymusił zmianę statusu
                N'priorytet_zmieniony', -- referent zmienił priorytet
                N'fakir_update',        -- sukces UPDATE BUF_DOKUMENT
                N'fakir_update_failed', -- błąd UPDATE BUF_DOKUMENT (saga rollback)
                N'nie_moje',            -- pracownik oznaczył "nie moja faktura"
                N'force_akceptacja',    -- admin/manager wymusił akceptację
                N'anulowano'            -- referent anulował fakturę (IsActive=0)
            )
        )
    );

    PRINT N'[017] ✓ Tabela dbo_ext.skw_faktura_log — UTWORZONA.';
    PRINT N'[017]   Constraints: PK + 2x FK + CHK(akcja — 11 wartości)';
    PRINT N'[017]   ⚠ IMMUTABLE: tylko INSERT, nigdy UPDATE/DELETE';
END
ELSE
BEGIN
    PRINT N'[017] ~ Tabela dbo_ext.skw_faktura_log — już istnieje, pomijam.';
END
GO

-- ── Indeks: historia faktury ─────────────────────────────────────────────────
-- Używany przez: GET /faktury-akceptacja/{id}/historia
-- Zapytanie:     WHERE faktura_id = @fid ORDER BY CreatedAt DESC
-- Bez indeksu:   full scan przy każdym odczycie historii (niezależnie od rozmiaru tabeli)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name      = N'IX_sfl_faktura_created'
      AND object_id = OBJECT_ID(N'dbo_ext.skw_faktura_log')
)
BEGIN
    PRINT N'[017] Tworzenie indeksu IX_sfl_faktura_created...';

    CREATE NONCLUSTERED INDEX [IX_sfl_faktura_created]
        ON [dbo_ext].[skw_faktura_log]
            (faktura_id ASC, CreatedAt DESC)
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF,
              SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF,
              ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);

    PRINT N'[017] ✓ Indeks IX_sfl_faktura_created — UTWORZONY.';
    PRINT N'[017]   Kolumny: (faktura_id ASC, CreatedAt DESC)';
END
ELSE
BEGIN
    PRINT N'[017] ~ Indeks IX_sfl_faktura_created — już istnieje, pomijam.';
END
GO

-- ── Weryfikacja ───────────────────────────────────────────────────────────────
PRINT N'';
PRINT N'[017] Weryfikacja struktury tabeli:';
SELECT
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.IS_NULLABLE,
    c.COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = N'dbo_ext'
  AND c.TABLE_NAME   = N'skw_faktura_log'
ORDER BY c.ORDINAL_POSITION;
GO

PRINT N'[017] Weryfikacja constraint akcji:';
SELECT
    cc.CONSTRAINT_NAME,
    cc.CHECK_CLAUSE
FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc
INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
    ON cc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
WHERE ccu.TABLE_SCHEMA  = N'dbo_ext'
  AND ccu.TABLE_NAME    = N'skw_faktura_log'
  AND ccu.COLUMN_NAME   = N'akcja';
GO

PRINT N'';
PRINT N'=== [017] DONE: ' + CONVERT(NVARCHAR(30), GETDATE(), 126) + N' ===';
PRINT N'';
GO

-- ============================================================
-- POST-INSTALL CHECKLIST (SSMS):
--
--   Po uruchomieniu 015, 016, 017 wykonaj weryfikację:
--
--   SELECT t.name, s.name AS [schema]
--   FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
--   WHERE s.name = 'dbo_ext' AND t.name LIKE 'skw_faktura%'
--   ORDER BY t.name;
--   -- Oczekiwany wynik: 3 wiersze
--
--   Następnie uruchom migrację Alembic:
--   ALEMBIC_MODE=upgrade → docker compose up → alembic upgrade 0006
-- ============================================================