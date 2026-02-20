-- =============================================================================
-- database/views/VIEW_kontrahenci.sql
-- =============================================================================
-- Widok: dbo.VIEW_kontrahenci
-- Cel:   Zagregowane dane dłużników z tabel WAPRO do systemu windykacyjnego.
--        Enkapsuluje logikę biznesową — żaden endpoint nie odpytuje tabel WAPRO
--        bezpośrednio. Wszystkie filtry biznesowe są tutaj.
--
-- NAPRAWY (AUDIT_ZGODNOSCI R1, R2, R3, R4):
--   [R1] Schemat: dbo_ext → dbo  (widoki WAPRO są w schemacie dbo, nie dbo_ext)
--   [R2] Nazwa: vw_Kontrahenci → VIEW_kontrahenci  (nowa konwencja nazewnictwa)
--   [R3] Tabela WAPRO: dbo.Rozrachunek (NIE: ROZRACHUNKI)
--        Kolumny: POZOSTALO, CZY_ROZLICZONY, STRONA, TYP_DOK, NR_DOK (NIE: KWOTA_ZAPLACONA, TYP=1)
--   [R4] Daty WAPRO: DATA_DOK i TERMIN_PLATNOSCI to INT (dni od 1899-12-30) — NIE DATETIME
--        Konwersja: CAST(DATEADD(DAY, DATA_DOK, '18991230') AS DATE)
--
-- Tabele źródłowe (WAPRO, tylko odczyt):
--   dbo.KONTRAHENT    — dane kontrahenta (nazwa, email, telefon, RODO, zablokowany)
--   dbo.Rozrachunek   — rozrachunki faktur (UWAGA: Rozrachunek, nie ROZRACHUNKI!)
--
-- Schemat widoku: dbo (ten sam schemat co tabele WAPRO)
-- Odpytywany przez: db/wapro.py (pyodbc, NIE SQLAlchemy ORM)
-- Checksum śledzony w: dbo_ext.SchemaChecksums (SchemaName='dbo', ObjectType='VIEW')
--
-- Wersja: 1.0.0 | Data: 2026-02-17 | Faza: 0 — naprawa R1/R2/R3/R4
-- =============================================================================

-- Idempotentny: CREATE OR ALTER — bezpieczne przy wielokrotnym wdrożeniu
-- Wymaga SQL Server 2016 SP1+ (nasza wersja: MSSQL 2022 — OK)
CREATE OR ALTER VIEW [dbo].[VIEW_kontrahenci]
AS
-- =============================================================================
-- CTE 1: Agregacja rozrachunków — suma długu i przeterminowania
--
-- KLUCZOWE FILTRY BIZNESOWE (zawarte tutaj, nie w API):
--   STRONA = 'WN'       → tylko należności (Winien), pomijamy Ma (zobowiązania)
--   TYP_DOK = 'F'       → tylko faktury (F), pomijamy inne dokumenty
--   CZY_ROZLICZONY = 0  → tylko niezapłacone (0=nie, 1=tak)
--
-- UWAGA na daty:
--   DATA_DOK i TERMIN_PLATNOSCI w dbo.Rozrachunek to INT (liczba dni od 1899-12-30)
--   Konwersja: CAST(DATEADD(DAY, kolumna_int, '18991230') AS DATE)
--   Błędne założenie v1.4 że to DATETIME — powodowało błędy przy porównaniach
-- =============================================================================
WITH cte_rozrachunki AS (
    SELECT
        r.ID_KONTRAHENTA,

        -- Suma całkowitego długu (kwota pozostała do zapłaty)
        -- POZOSTALO = kwota brutto faktury minus ewentualne częściowe płatności
        SUM(
            CASE
                WHEN r.CZY_ROZLICZONY = 0
                 AND r.STRONA = 'WN'
                 AND r.TYP_DOK = 'F'
                THEN ISNULL(r.POZOSTALO, 0)
                ELSE 0
            END
        )                                                   AS SumaDlugu,

        -- Suma długu przeterminowanego (po terminie płatności)
        -- [R4] TERMIN_PLATNOSCI to INT — konwertujemy do DATE przed porównaniem z GETDATE()
        SUM(
            CASE
                WHEN r.CZY_ROZLICZONY = 0
                 AND r.STRONA = 'WN'
                 AND r.TYP_DOK = 'F'
                 AND CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE) < CAST(GETDATE() AS DATE)
                THEN ISNULL(r.POZOSTALO, 0)
                ELSE 0
            END
        )                                                   AS SumaDlinguPrzeterminowanego,

        -- Liczba faktur niezapłaconych
        COUNT(
            CASE
                WHEN r.CZY_ROZLICZONY = 0
                 AND r.STRONA = 'WN'
                 AND r.TYP_DOK = 'F'
                THEN r.ID_KONTRAHENTA
            END
        )                                                   AS LiczbaFakturNiezaplaconych,

        -- Najstarszy termin płatności (najdłużej zaległa faktura)
        -- [R4] Konwersja INT → DATE
        MIN(
            CASE
                WHEN r.CZY_ROZLICZONY = 0
                 AND r.STRONA = 'WN'
                 AND r.TYP_DOK = 'F'
                THEN CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE)
            END
        )                                                   AS NajstarszyTerminPlatnosci,

        -- Najnowsza data wystawienia faktury (ostatnia aktywność)
        -- [R4] Konwersja INT → DATE
        MAX(
            CASE
                WHEN r.CZY_ROZLICZONY = 0
                 AND r.STRONA = 'WN'
                 AND r.TYP_DOK = 'F'
                THEN CAST(DATEADD(DAY, r.DATA_DOK, '18991230') AS DATE)
            END
        )                                                   AS DataOstatniejFaktury,

        -- Liczba dni od najstarszego terminu płatności (wiek długu w dniach)
        DATEDIFF(
            DAY,
            MIN(
                CASE
                    WHEN r.CZY_ROZLICZONY = 0
                     AND r.STRONA = 'WN'
                     AND r.TYP_DOK = 'F'
                    THEN CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE)
                END
            ),
            CAST(GETDATE() AS DATE)
        )                                                   AS MaxDniPrzeterminowania,

        -- Czy JAKIKOLWIEK rozrachunek jest przeterminowany (BIT flag dla szybkiego filtrowania)
        CAST(
            MAX(
                CASE
                    WHEN r.CZY_ROZLICZONY = 0
                     AND r.STRONA = 'WN'
                     AND r.TYP_DOK = 'F'
                     AND CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE) < CAST(GETDATE() AS DATE)
                    THEN 1
                    ELSE 0
                END
            )
        AS BIT)                                             AS MaPrzeterminowane

    FROM dbo.Rozrachunek AS r
    -- Filtrujemy już w GROUP BY żeby nie liczyć zbędnych kontrahentów
    -- (kontrahenci bez faktur WN są odfiltrowani przez HAVING)
    GROUP BY r.ID_KONTRAHENTA
    -- HAVING: tylko kontrahenci z niezerowym długiem
    -- Bez tego widok zwraca wszystkich kontrahentów WAPRO (zbyt dużo)
    HAVING SUM(
        CASE
            WHEN r.CZY_ROZLICZONY = 0
             AND r.STRONA = 'WN'
             AND r.TYP_DOK = 'F'
            THEN ISNULL(r.POZOSTALO, 0)
            ELSE 0
        END
    ) > 0
),

-- =============================================================================
-- CTE 2: Dane kontrahenta z filtrami RODO i blokady
--
-- FILTRY:
--   RODO_ZANONIMIZOWANY = 0  → pomijamy zanonimizowanych (RODO compliance)
--   ZABLOKOWANY = 0          → pomijamy zablokowanych (klientów wyłączonych z windykacji)
--
-- KOLUMNY (poprawione w R3):
--   ADRES_EMAIL     (NIE: EMAIL)
--   TELEFON_FIRMOWY (NIE: TELEFON)
-- =============================================================================
cte_kontrahenci AS (
    SELECT
        k.ID_KONTRAHENTA,
        k.NAZWA                                             AS NazwaKontrahenta,
        k.KOD                                               AS KodKontrahenta,
        k.NIP                                               AS NIP,
        -- [R3] Poprawne nazwy kolumn WAPRO:
        k.ADRES_EMAIL                                       AS Email,
        k.TELEFON_FIRMOWY                                   AS Telefon,
        k.MIEJSCOWOSC                                       AS Miejscowosc,
        k.ULICA                                             AS Ulica,
        k.KOD_POCZTOWY                                      AS KodPocztowy
    FROM dbo.KONTRAHENT AS k
    WHERE
        -- [R3] RODO compliance: pomijamy zanonimizowanych kontrahentów
        k.RODO_ZANONIMIZOWANY = 0
        -- Pomijamy zablokowanych (np. wyłączonych z windykacji ręcznie)
        AND k.ZABLOKOWANY = 0
)

-- =============================================================================
-- Finalne SELECT — join CTE na ID_KONTRAHENTA
-- Tylko kontrahenci którzy mają dług (INNER JOIN z cte_rozrachunki)
-- =============================================================================
SELECT
    -- ── Identyfikacja ─────────────────────────────────────────────────────────
    k.ID_KONTRAHENTA                                        AS IdKontrahenta,
    k.KodKontrahenta,
    k.NazwaKontrahenta,
    k.NIP,

    -- ── Dane kontaktowe ────────────────────────────────────────────────────────
    -- Mogą być NULL jeśli WAPRO nie ma danych kontaktowych
    k.Email,
    k.Telefon,

    -- ── Adres ─────────────────────────────────────────────────────────────────
    k.Ulica,
    k.KodPocztowy,
    k.Miejscowosc,

    -- ── Dane długu ────────────────────────────────────────────────────────────
    r.SumaDlugu,
    r.SumaDlinguPrzeterminowanego,
    r.LiczbaFakturNiezaplaconych,
    r.MaPrzeterminowane,

    -- ── Daty (już skonwertowane z INT w CTE — tutaj są DATE) ────────────────
    r.NajstarszyTerminPlatnosci,
    r.DataOstatniejFaktury,
    r.MaxDniPrzeterminowania,

    -- ── Kolumna pomocnicza dla UI ──────────────────────────────────────────────
    -- Kategoria wiekowa długu (używana do sortowania i kolorowania w UI)
    CASE
        WHEN r.MaxDniPrzeterminowania IS NULL OR r.MaxDniPrzeterminowania <= 0
            THEN N'biezace'          -- nie przeterminowane
        WHEN r.MaxDniPrzeterminowania <= 30
            THEN N'do_30_dni'
        WHEN r.MaxDniPrzeterminowania <= 60
            THEN N'31_60_dni'
        WHEN r.MaxDniPrzeterminowania <= 90
            THEN N'61_90_dni'
        ELSE N'powyzej_90_dni'
    END                                                     AS KategoriaWieku

FROM cte_kontrahenci AS k
    -- INNER JOIN: wyklucza kontrahentów bez żadnego długu
    -- (cte_rozrachunki już ma HAVING SumaDlugu > 0)
INNER JOIN cte_rozrachunki AS r
    ON k.ID_KONTRAHENTA = r.ID_KONTRAHENTA;

-- =============================================================================
-- UWAGI DLA DBA:
--
-- 1. Wydajność:
--    Widok wykonuje GROUP BY na całej tabeli Rozrachunek.
--    Dla dużych baz (>100k rekordów) wymagane indeksy (AUDIT R9):
--      IX_Roz_Kontrahent_Dlugi     → (ID_KONTRAHENTA, CZY_ROZLICZONY, STRONA, TYP_DOK)
--      IX_Roz_Faktura_Kontrahent   → (ID_KONTRAHENTA, TERMIN_PLATNOSCI, POZOSTALO)
--    Pliki: database/indexes/IX_Roz_Kontrahent_Dlugi.sql
--
-- 2. RODO:
--    Widok automatycznie wyklucza RODO_ZANONIMIZOWANY=1.
--    Po anonimizacji kontrahenta znika z listy dłużników — bez restartu aplikacji.
--
-- 3. Checksum:
--    Po każdej zmianie tego widoku zaktualizuj dbo_ext.SchemaChecksums:
--    UPDATE [dbo_ext].[SchemaChecksums]
--       SET [Checksum]         = (SELECT CHECKSUM(m.definition)
--                                 FROM sys.sql_modules m
--                                 JOIN sys.objects o ON m.object_id = o.object_id
--                                 WHERE o.name = 'VIEW_kontrahenci'
--                                   AND SCHEMA_NAME(o.schema_id) = 'dbo'),
--           [AlembicRevision]  = '<numer_rewizji>',
--           [LastVerifiedAt]   = NULL  -- wymusi re-weryfikację przy starcie
--    WHERE [ObjectName] = 'VIEW_kontrahenci'
--      AND [SchemaName] = 'dbo'
--      AND [ObjectType] = 'VIEW';
--
-- 4. Zmienna WAPRO:
--    Sprawdź czy Twoja wersja WAPRO używa dokładnie tych nazw tabel i kolumn.
--    Możliwe różnice między wersjami WAPRO — zweryfikuj z DBA przed wdrożeniem:
--      SELECT TOP 1 * FROM dbo.Rozrachunek  -- sprawdź kolumny
--      SELECT TOP 1 * FROM dbo.KONTRAHENT  -- sprawdź kolumny
-- =============================================================================