-- ============================================================
-- FILE:    database/ddl/016_faktura_przypisanie.sql
-- MODUŁ:   Akceptacja Faktur KSeF
-- TABELA:  dbo_ext.skw_faktura_przypisanie
-- WERSJA:  1.0
-- DATA:    2026-03-26
-- SCHEMAT: dbo_ext
-- ============================================================
-- OPIS:
--   Tabela przypisań pracowników do faktur.
--   Jeden wiersz = jeden pracownik przypisany do jednej faktury.
--
--   is_active = 0 → dezaktywowane przez reset referenta (NIE DELETE).
--   decided_at    → timestamp podjęcia decyzji przez pracownika.
--
-- IDEMPOTENTNY: TAK — IF NOT EXISTS, idempotentne tworzenie indeksów
-- WYKONANIE:    SSMS (po 015_faktura_akceptacja.sql)
--
-- POWIĄZANA MIGRACJA:
--   backend/alembic/versions/007_faktura_akceptacja.py
--
-- ZALEŻNOŚCI (muszą istnieć PRZED uruchomieniem):
--   ✅ dbo_ext.skw_faktura_akceptacja (FK: faktura_id → id)
--   ✅ dbo_ext.skw_Users              (FK: user_id → ID_USER)
--
-- INDEKSY KRYTYCZNE (Sprint_2, Sekcja 3.1.2):
--   IX_sfp_user_active   → endpoint GET /moje-faktury (pracownik)
--   IX_sfp_faktura_active → sprawdzenie kompletności akceptacji (saga)
--
-- STATUS CYCLE:
--   oczekuje → zaakceptowane
--            → odrzucone
--            → nie_moje
-- ============================================================

USE [WAPRO];
GO

PRINT N'';
PRINT N'=== [016] START: dbo_ext.skw_faktura_przypisanie ===';
PRINT N'=== Timestamp: ' + CONVERT(NVARCHAR(30), GETDATE(), 126) + N' ===';
PRINT N'';
GO

-- ── Główna tabela ────────────────────────────────────────────────────────────
IF NOT EXISTS (
    SELECT 1
    FROM   sys.tables  t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE  s.name = N'dbo_ext'
      AND  t.name = N'skw_faktura_przypisanie'
)
BEGIN
    PRINT N'[016] Tworzenie tabeli dbo_ext.skw_faktura_przypisanie...';

    CREATE TABLE [dbo_ext].[skw_faktura_przypisanie] (

        -- ── Identyfikacja ────────────────────────────────────────────────────
        id          INT          IDENTITY(1,1) NOT NULL,

        -- ── Relacje ──────────────────────────────────────────────────────────
        faktura_id  INT                        NOT NULL,   -- FK → skw_faktura_akceptacja
        user_id     INT                        NOT NULL,   -- FK → skw_Users

        -- ── Stan przypisania ─────────────────────────────────────────────────
        status      NVARCHAR(20)               NOT NULL
            CONSTRAINT [DF_sfp_status]    DEFAULT N'oczekuje',
        komentarz   NVARCHAR(MAX)                  NULL,   -- komentarz pracownika przy decyzji
        is_active   BIT                        NOT NULL
            CONSTRAINT [DF_sfp_is_active] DEFAULT 1,      -- 0 = dezaktywowane przez reset

        -- ── Timestampy ───────────────────────────────────────────────────────
        CreatedAt   DATETIME2(7)               NOT NULL
            CONSTRAINT [DF_sfp_CreatedAt] DEFAULT GETDATE(),
        UpdatedAt   DATETIME2(7)                   NULL,   -- trigger: 020_faktura_triggers_updated_at.sql
        decided_at  DATETIME2(7)                   NULL,   -- kiedy pracownik podjął decyzję

        -- ══════════════════════════════════════════════════════════════════════
        -- CONSTRAINTS
        -- ══════════════════════════════════════════════════════════════════════

        CONSTRAINT [PK_skw_faktura_przypisanie]
            PRIMARY KEY CLUSTERED (id ASC)
            WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF,
                  ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON),

        -- FK: do której faktury
        CONSTRAINT [FK_sfp_faktura_id]
            FOREIGN KEY (faktura_id)
            REFERENCES [dbo_ext].[skw_faktura_akceptacja] (id),

        -- FK: który pracownik
        CONSTRAINT [FK_sfp_user_id]
            FOREIGN KEY (user_id)
            REFERENCES [dbo_ext].[skw_Users] (ID_USER),

        -- Walidacja statusu decyzji
        CONSTRAINT [CHK_sfp_status] CHECK (
            status IN (
                N'oczekuje',
                N'zaakceptowane',
                N'odrzucone',
                N'nie_moje'
            )
        )
    );

    PRINT N'[016] ✓ Tabela dbo_ext.skw_faktura_przypisanie — UTWORZONA.';
END
ELSE
BEGIN
    PRINT N'[016] ~ Tabela dbo_ext.skw_faktura_przypisanie — już istnieje, pomijam.';
END
GO

-- ── Indeks krytyczny 1: moje faktury (endpoint pracownika) ───────────────────
-- Zapytanie: WHERE user_id = @uid AND is_active = 1 ORDER BY CreatedAt DESC
-- Bez tego indeksu: full scan tabeli przy każdym otwarciu "moich faktur"
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name        = N'IX_sfp_user_active'
      AND object_id   = OBJECT_ID(N'dbo_ext.skw_faktura_przypisanie')
)
BEGIN
    PRINT N'[016] Tworzenie indeksu IX_sfp_user_active...';

    CREATE NONCLUSTERED INDEX [IX_sfp_user_active]
        ON [dbo_ext].[skw_faktura_przypisanie]
            (user_id ASC, is_active ASC, status ASC)
        INCLUDE (faktura_id, CreatedAt)
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF,
              SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF,
              ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);

    PRINT N'[016] ✓ Indeks IX_sfp_user_active — UTWORZONY.';
    PRINT N'[016]   Kolumny: (user_id, is_active, status) INCLUDE (faktura_id, CreatedAt)';
END
ELSE
BEGIN
    PRINT N'[016] ~ Indeks IX_sfp_user_active — już istnieje, pomijam.';
END
GO

-- ── Indeks krytyczny 2: kompletność akceptacji (saga trigger Fakira) ─────────
-- Zapytanie: WHERE faktura_id = @fid AND is_active = 1
-- Używany przez: moje_faktury_service.py → sprawdzenie "czy wszyscy zaakceptowali"
-- Krytyczny dla wydajności triggera zapisu do BUF_DOKUMENT
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name      = N'IX_sfp_faktura_active'
      AND object_id = OBJECT_ID(N'dbo_ext.skw_faktura_przypisanie')
)
BEGIN
    PRINT N'[016] Tworzenie indeksu IX_sfp_faktura_active...';

    CREATE NONCLUSTERED INDEX [IX_sfp_faktura_active]
        ON [dbo_ext].[skw_faktura_przypisanie]
            (faktura_id ASC, is_active ASC)
        INCLUDE (user_id, status)
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF,
              SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF,
              ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);

    PRINT N'[016] ✓ Indeks IX_sfp_faktura_active — UTWORZONY.';
    PRINT N'[016]   Kolumny: (faktura_id, is_active) INCLUDE (user_id, status)';
END
ELSE
BEGIN
    PRINT N'[016] ~ Indeks IX_sfp_faktura_active — już istnieje, pomijam.';
END
GO

-- ── Weryfikacja ───────────────────────────────────────────────────────────────
PRINT N'';
PRINT N'[016] Weryfikacja struktury tabeli:';
SELECT
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.IS_NULLABLE,
    c.COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = N'dbo_ext'
  AND c.TABLE_NAME   = N'skw_faktura_przypisanie'
ORDER BY c.ORDINAL_POSITION;
GO

PRINT N'[016] Weryfikacja indeksów:';
SELECT
    i.name        AS IndexName,
    i.type_desc   AS IndexType,
    c.name        AS ColumnName,
    ic.is_included_column AS IsIncluded
FROM sys.indexes i
INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
INNER JOIN sys.columns       c  ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.object_id = OBJECT_ID(N'dbo_ext.skw_faktura_przypisanie')
  AND i.name IN (N'IX_sfp_user_active', N'IX_sfp_faktura_active')
ORDER BY i.name, ic.key_ordinal, ic.is_included_column;
GO

PRINT N'';
PRINT N'=== [016] DONE: ' + CONVERT(NVARCHAR(30), GETDATE(), 126) + N' ===';
PRINT N'';
GO