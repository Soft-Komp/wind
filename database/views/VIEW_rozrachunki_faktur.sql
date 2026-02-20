-- =============================================================================
-- database/views/VIEW_rozrachunki_faktur.sql
-- =============================================================================
-- Widok: dbo.VIEW_rozrachunki_faktur
-- Cel:   Szczegółowe dane poszczególnych faktur (rozrachunków) dla dłużnika.
--        Używany przez endpoint GET /api/v1/debtors/{id}/invoices
--        oraz przy generowaniu PDF wezwania do zapłaty (lista faktur).
--
-- NAPRAWY (AUDIT_ZGODNOSCI R2, R3, R4):
--   [R2] Nazwa: vw_FakturyDetal → VIEW_rozrachunki_faktur  (nowa konwencja)
--   [R3] Tabela: dbo.Rozrachunek (nie ROZRACHUNKI!)
--        Kolumny wg rzeczywistej struktury WAPRO:
--          NR_DOK         → numer faktury (nie NUMER_FAKTURY)
--          DATA_DOK       → INT (dni od 1899-12-30) — NIE DATETIME
--          TERMIN_PLATNOSCI → INT (dni od 1899-12-30) — NIE DATETIME
--          POZOSTALO      → kwota do zapłaty (nie KWOTA_ZAPLACONA)
--          CZY_ROZLICZONY → BIT 0/1 (nie STATUS)
--          FORMA_PLATNOSCI → metoda płatności
--   [R4] Konwersja dat INT → DATE:
--        CAST(DATEADD(DAY, DATA_DOK, '18991230') AS DATE) AS DataWystawienia
--        CAST(DATEADD(DAY, TERMIN_PLATNOSCI, '18991230') AS DATE) AS TerminPlatnosci
--
-- Tabele źródłowe (WAPRO, tylko odczyt):
--   dbo.Rozrachunek  — faktury i rozrachunki
--   dbo.KONTRAHENT   — dane kontrahenta (join dla nazwy)
--
-- Schemat widoku: dbo
-- Odpytywany przez: db/wapro.py (pyodbc)
-- Parametr wejściowy: ID_KONTRAHENTA (filtrowany w WHERE przez wapro.py)
-- Checksum śledzony w: dbo_ext.SchemaChecksums (SchemaName='dbo', ObjectType='VIEW')
--
-- Wersja: 1.0.0 | Data: 2026-02-17 | Faza: 0 — naprawa R2/R3/R4
-- =============================================================================

CREATE OR ALTER VIEW [dbo].[VIEW_rozrachunki_faktur]
AS
-- =============================================================================
-- CTE: Kontrahenci z filtrem RODO i blokady (identyczny jak w VIEW_kontrahenci)
-- Redundancja jest celowa — widoki są niezależne, nie zagnieżdżamy widoków w widokach
-- (zagnieżdżone widoki w MSSQL utrudniają optymalizację przez query planner)
-- =============================================================================
WITH cte_kontrahenci_aktywni AS (
    SELECT
        k.ID_KONTRAHENTA,
        k.NAZWA     AS NazwaKontrahenta,
        k.NIP       AS NIP
    FROM dbo.KONTRAHENT AS k
    WHERE
        k.RODO_ZANONIMIZOWANY = 0
        AND k.ZABLOKOWANY = 0
)

-- =============================================================================
-- Finalne SELECT — jedna faktura = jeden wiersz
--
-- Filtr główny (tutaj, nie w wapro.py):
--   STRONA = 'WN'       → tylko należności
--   TYP_DOK = 'F'       → tylko faktury
--
-- Filtr CZY_ROZLICZONY: celowo NIE filtrujemy tutaj — widok zwraca
-- zarówno zapłacone jak i niezapłacone. Filtr po stronie API (is_paid parameter).
-- Pozwala to na pokazanie historii wszystkich faktur danego dłużnika.
-- =============================================================================
SELECT
    -- ── Identyfikacja ─────────────────────────────────────────────────────────
    r.ID_KONTRAHENTA                                        AS IdKontrahenta,
    k.NazwaKontrahenta,
    k.NIP,

    -- ── Dane faktury ──────────────────────────────────────────────────────────
    -- [R3] NR_DOK to numer dokumentu w WAPRO (np. "FV/2024/01/0001")
    r.NR_DOK                                                AS NumerFaktury,

    -- [R4] Konwersja DATA_DOK (INT) → DATE
    -- Wzór: DATEADD(DAY, liczba_dni_od_1899-12-30, '1899-12-30')
    -- Przykład: DATA_DOK = 45292 → 2024-01-15
    CAST(
        DATEADD(DAY, r.DATA_DOK, '18991230')
    AS DATE)                                                AS DataWystawienia,

    -- [R4] Konwersja TERMIN_PLATNOSCI (INT) → DATE
    CAST(
        DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230')
    AS DATE)                                                AS TerminPlatnosci,

    -- ── Kwoty ─────────────────────────────────────────────────────────────────
    -- KWOTA = kwota brutto faktury (pełna wartość)
    ISNULL(r.KWOTA, 0)                                      AS KwotaBrutto,

    -- POZOSTALO = kwota jeszcze do zapłaty (po częściowych płatnościach)
    -- [R3] POZOSTALO, nie KWOTA_ZAPLACONA (błąd v1.4)
    ISNULL(r.POZOSTALO, 0)                                  AS KwotaPozostala,

    -- Kwota już zapłacona = KWOTA - POZOSTALO (obliczona, nie z kolumny)
    ISNULL(r.KWOTA, 0) - ISNULL(r.POZOSTALO, 0)            AS KwotaZaplacona,

    -- ── Status ────────────────────────────────────────────────────────────────
    -- [R3] CZY_ROZLICZONY to BIT (0=niezapłacona, 1=zapłacona)
    r.CZY_ROZLICZONY                                        AS CzyZaplacona,

    -- Czy faktura jest przeterminowana (obliczone w widoku dla wydajności)
    -- [R4] Porównanie z DATE, nie DATETIME
    CAST(
        CASE
            WHEN r.CZY_ROZLICZONY = 0
             AND CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE)
                 < CAST(GETDATE() AS DATE)
            THEN 1
            ELSE 0
        END
    AS BIT)                                                 AS CzyPrzeterminowana,

    -- Liczba dni przeterminowania (NULL jeśli nie przeterminowana lub zapłacona)
    CASE
        WHEN r.CZY_ROZLICZONY = 0
         AND CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE)
             < CAST(GETDATE() AS DATE)
        THEN DATEDIFF(
            DAY,
            CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE),
            CAST(GETDATE() AS DATE)
        )
        ELSE NULL
    END                                                     AS DniPrzeterminowania,

    -- ── Metadane dokumentu ────────────────────────────────────────────────────
    -- [R3] FORMA_PLATNOSCI — metoda płatności (przelew, gotówka itp.)
    r.FORMA_PLATNOSCI                                       AS FormaPlatnosci,

    -- Strona rozrachunku — zawsze 'WN' dla należności (filtr w WHERE)
    -- Zostawiamy w SELECT dla transparentności danych
    r.STRONA                                                AS StronaRozrachunku,

    -- Typ dokumentu — zawsze 'F' dla faktur (filtr w WHERE)
    r.TYP_DOK                                               AS TypDokumentu,

    -- ── Kolumna pomocnicza dla sortowania UI ──────────────────────────────────
    -- Kategoria przeterminowania (identyczna logika jak w VIEW_kontrahenci)
    CASE
        WHEN r.CZY_ROZLICZONY = 1
            THEN N'zaplacona'
        WHEN CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE)
             >= CAST(GETDATE() AS DATE)
            THEN N'biezaca'
        WHEN DATEDIFF(
                DAY,
                CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE),
                CAST(GETDATE() AS DATE)
             ) <= 30
            THEN N'do_30_dni'
        WHEN DATEDIFF(
                DAY,
                CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE),
                CAST(GETDATE() AS DATE)
             ) <= 60
            THEN N'31_60_dni'
        WHEN DATEDIFF(
                DAY,
                CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE),
                CAST(GETDATE() AS DATE)
             ) <= 90
            THEN N'61_90_dni'
        ELSE N'powyzej_90_dni'
    END                                                     AS KategoriaWieku

FROM dbo.Rozrachunek AS r
    -- INNER JOIN: wyklucza kontrahentów zanonimizowanych/zablokowanych (RODO)
INNER JOIN cte_kontrahenci_aktywni AS k
    ON r.ID_KONTRAHENTA = k.ID_KONTRAHENTA
WHERE
    -- Tylko należności (Winien)
    r.STRONA = 'WN'
    -- Tylko faktury (nie inne typy dokumentów)
    AND r.TYP_DOK = 'F';
-- Uwaga: celowo BEZ filtra CZY_ROZLICZONY — widok zwraca wszystkie faktury
-- (zarówno zapłacone jak i niezapłacone). Filtrowanie po CZY_ROZLICZONY
-- odbywa się w wapro.py przez parametr is_paid=True/False/None

-- =============================================================================
-- UWAGI DLA DBA:
--
-- 1. Wydajność:
--    Widok używany z filtrem ID_KONTRAHENTA (wapro.py zawsze dodaje WHERE).
--    Wymagany indeks (AUDIT R9):
--      IX_Roz_Faktura_Kontrahent → (ID_KONTRAHENTA, STRONA, TYP_DOK)
--                                   INCLUDE (NR_DOK, DATA_DOK, TERMIN_PLATNOSCI,
--                                            KWOTA, POZOSTALO, CZY_ROZLICZONY,
--                                            FORMA_PLATNOSCI)
--    Bez tego indeksu każde wywołanie GET /debtors/{id}/invoices → full scan.
--
-- 2. Kalibracja dat WAPRO:
--    Jeśli wyniki dat wyglądają nieprawidłowo, zweryfikuj bazę odniesienia:
--      SELECT TOP 5
--        DATA_DOK,
--        CAST(DATEADD(DAY, DATA_DOK, '18991230') AS DATE) AS DataSkonwertowana
--      FROM dbo.Rozrachunek
--      WHERE DATA_DOK IS NOT NULL
--    Oczekiwany wynik: DataSkonwertowana w zakresie 1990-2030.
--    Jeśli wyniki są poza zakresem — baza odniesienia może być inna ('19000101'?)
--    Skonsultuj z DBA przed wdrożeniem.
--
-- 3. Checksum:
--    Po każdej zmianie tego widoku zaktualizuj dbo_ext.SchemaChecksums:
--    UPDATE [dbo_ext].[SchemaChecksums]
--       SET [Checksum]        = (SELECT CHECKSUM(m.definition)
--                                FROM sys.sql_modules m
--                                JOIN sys.objects o ON m.object_id = o.object_id
--                                WHERE o.name = 'VIEW_rozrachunki_faktur'
--                                  AND SCHEMA_NAME(o.schema_id) = 'dbo'),
--           [LastVerifiedAt]  = NULL
--    WHERE [ObjectName] = 'VIEW_rozrachunki_faktur'
--      AND [SchemaName] = 'dbo'
--      AND [ObjectType] = 'VIEW';
--
-- 4. Test po wdrożeniu:
--    SELECT TOP 10 * FROM dbo.VIEW_rozrachunki_faktur WHERE IdKontrahenta = <id_testowy>
--    Sprawdź: DataWystawienia i TerminPlatnosci mają sensowne wartości (nie 1900-01-01)
-- =============================================================================