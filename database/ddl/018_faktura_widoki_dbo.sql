-- =============================================================================
-- PLIK:    018_faktura_widoki_dbo.sql
-- MODUŁ:   Akceptacja Faktur KSeF — Sprint 2 / Sesja 2
-- SERWER:  GPGKJASLO (192.168.0.50) | BAZA: GPGKJASLO
-- SCHEMAT: dbo  ← WYJĄTEK od reguły dbo_ext (konieczność dostępu do WAPRO)
-- AUTOR:   Windykacja-gpgk-backend
-- DATA:    2026-03-26
--
-- ZAWARTOŚĆ:
--   Widok 1: dbo.skw_faktury_akceptacja_naglowek
--            Źródło: dbo.BUF_DOKUMENT JOIN dbo.KONTRAHENT
--            Filtr:  PRG_KOD=3, KSEF_ID IS NOT NULL, TYP='Z'
--
--   Widok 2: dbo.skw_faktury_akceptacja_pozycje
--            Źródło: dbo.Api_V_BufferDocumentPosition
--            Użycie: tylko przy szczegółach faktury i PDF (nie przy liście)
--
-- KOLUMNY WAPRO ZWERYFIKOWANE W SSMS 2026-03-26:
--   BUF_DOKUMENT.KSEF_ID         = varchar
--   BUF_DOKUMENT.PRG_KOD         = tinyint
--   BUF_DOKUMENT.TYP             = varchar
--   BUF_DOKUMENT.KOD_STATUSU     = varchar  (NULL=NOWY, 'K'=ZATWIERDZONY, 'A'=ZAKSIEGOWANY)
--   BUF_DOKUMENT.DATA_WYSTAWIENIA = int (format Clarion → DATEADD(DAY, val, '18991230'))
--   BUF_DOKUMENT.DATA_OTRZYMANIA  = int (format Clarion)
--   BUF_DOKUMENT.TERMIN_PLATNOSCI = int (format Clarion)
--   BUF_DOKUMENT.ID_KONTRAHENTA   = numeric (JOIN z KONTRAHENT)
--   KONTRAHENT.ADRES_EMAIL        ← UWAGA: nie 'EMAIL', nie 'MAIL'
--   KONTRAHENT.TELEFON_FIRMOWY    ← UWAGA: nie 'TELEFON' (ZWERYFIKOWANE W SSMS!)
--
-- URUCHOMIENIE:
--   1. Podłącz SSMS do serwera GPGKJASLO, baza GPGKJASLO
--   2. Uruchom cały plik (F5)
--   3. Weryfikacja: SELECT TOP 5 * FROM dbo.skw_faktury_akceptacja_naglowek
--   4. Następnie uruchom 019_faktura_checksums.sql
--
-- IDEMPOTENTNOŚĆ: CREATE OR ALTER VIEW — bezpieczne wielokrotne uruchomienie
-- =============================================================================

GO

-- ===========================================================================
-- WIDOK 1: skw_faktury_akceptacja_naglowek
-- Nagłówki faktur zakupowych z KSeF oczekujących na akceptację lub w obiegu.
-- Jeden wiersz = jedna faktura w buforze WAPRO.
-- ===========================================================================
CREATE OR ALTER VIEW dbo.skw_faktury_akceptacja_naglowek
AS
/*
    LOGIKA BIZNESOWA:
    - PRG_KOD = 3  → faktury Fakira (moduł Fakir WAPRO)
    - KSEF_ID IS NOT NULL → tylko faktury z KSeF (mają elektroniczny identyfikator)
    - TYP = 'Z'    → faktury zakupowe (Z=zakup, S=sprzedaż)

    KONWERSJA DAT:
    WAPRO przechowuje daty jako INT w formacie Clarion:
    liczba dni od 1899-12-30. Konwersja: DATEADD(DAY, wartosc_int, '18991230')
    Wartość NULL lub 0 → zwracamy NULL (bezpieczna obsługa pustych dat).

    KONTRAHENT:
    LEFT JOIN — faktura może nie mieć przypisanego kontrahenta w WAPRO.
    W takim przypadku pola NazwaKontrahenta/Email/Telefon będą NULL.
    Kolumna telefonu zweryfikowana w SSMS: TELEFON_FIRMOWY (nie TELEFON).
*/
SELECT
    -- ── Identyfikatory ──────────────────────────────────────────────────────
    bd.ID_BUF_DOKUMENT,
    bd.KSEF_ID,
    bd.NUMER,

    -- ── Status faktury w WAPRO ──────────────────────────────────────────────
    bd.KOD_STATUSU,
    CASE
        WHEN bd.KOD_STATUSU IS NULL THEN N'NOWY'
        WHEN bd.KOD_STATUSU = 'K'   THEN N'ZATWIERDZONY'
        WHEN bd.KOD_STATUSU = 'A'   THEN N'ZAKSIEGOWANY'
        ELSE bd.KOD_STATUSU         -- inne kody przekazujemy verbatim
    END AS StatusOpis,

    -- ── Daty (Clarion INT → DATE) ───────────────────────────────────────────
    -- Format Clarion: liczba dni od 1899-12-30
    -- NULL/0 traktujemy jako brak daty (np. faktura bez daty otrzymania)
    CASE
        WHEN bd.DATA_WYSTAWIENIA IS NULL OR bd.DATA_WYSTAWIENIA = 0 THEN NULL
        ELSE CAST(DATEADD(DAY, bd.DATA_WYSTAWIENIA, '18991230') AS DATE)
    END AS DataWystawienia,

    CASE
        WHEN bd.DATA_OTRZYMANIA IS NULL OR bd.DATA_OTRZYMANIA = 0 THEN NULL
        ELSE CAST(DATEADD(DAY, bd.DATA_OTRZYMANIA, '18991230') AS DATE)
    END AS DataOtrzymania,

    CASE
        WHEN bd.TERMIN_PLATNOSCI IS NULL OR bd.TERMIN_PLATNOSCI = 0 THEN NULL
        ELSE CAST(DATEADD(DAY, bd.TERMIN_PLATNOSCI, '18991230') AS DATE)
    END AS TerminPlatnosci,

    -- ── Wartości finansowe ──────────────────────────────────────────────────
    -- ISNULL: jeśli WAPRO zwróci NULL (ET-13: WARTOSC_BRUTTO = NULL),
    -- odpowiedź serwisu ustawi 0.00 — widok zachowuje NULL dla walidacji w serwisie
    bd.WARTOSC_NETTO,
    bd.WARTOSC_BRUTTO,
    bd.KWOTA_VAT,

    -- ── Płatność ────────────────────────────────────────────────────────────
    bd.FORMA_PLATNOSCI,
    bd.UWAGI,

    -- ── Dane kontrahenta (LEFT JOIN — może być NULL) ─────────────────────────
    k.NAZWA            AS NazwaKontrahenta,
    -- ZWERYFIKOWANE: ADRES_EMAIL (nie EMAIL, nie MAIL)
    k.ADRES_EMAIL      AS EmailKontrahenta,
    -- ZWERYFIKOWANE: TELEFON_FIRMOWY (nie TELEFON — kolumna nie istnieje w KONTRAHENT!)
    k.TELEFON_FIRMOWY  AS TelefonKontrahenta

FROM dbo.BUF_DOKUMENT bd
LEFT JOIN dbo.KONTRAHENT k
    ON k.ID_KONTRAHENTA = bd.ID_KONTRAHENTA

WHERE
    bd.PRG_KOD    = 3             -- tylko Fakir
    AND bd.KSEF_ID IS NOT NULL    -- tylko faktury z KSeF
    AND bd.TYP    = 'Z';          -- tylko zakupowe
GO

-- ===========================================================================
-- WIDOK 2: skw_faktury_akceptacja_pozycje
-- Pozycje faktur zakupowych — używany TYLKO przy szczegółach faktury i PDF.
-- NIE używać przy liście faktur (zbyt ciężki join).
--
-- ŹRÓDŁO: dbo.Api_V_BufferDocumentPosition (widok WAPRO z API)
-- KOLUMNY ZWERYFIKOWANE W SSMS 2026-03-26:
--   BufferDocumentId          → ID_BUF_DOKUMENT (klucz łączący z naglowek)
--   BufferDocumentPositionIndex → NumerPozycji
--   ProductName               → NazwaTowaru
--   Quantity                  → Ilosc
--   Unit                      → Jednostka
--   NetPrice                  → CenaNetto
--   GrossPrice                → CenaBrutto
--   TotalNetAmount            → WartoscNetto
--   TotalGrossAmount          → WartoscBrutto
--   TaxCode                   → StawkaVAT
--   Description               → Opis
-- ===========================================================================
CREATE OR ALTER VIEW dbo.skw_faktury_akceptacja_pozycje
AS
/*
    UWAGI:
    - Widok renomuje angielskie kolumny WAPRO API na polskie nazwy
      zgodne z Pydantic Response Schema (FakturaPozycjaResponse)
    - Brak filtra — filtrowanie po ID_BUF_DOKUMENT odbywa się w serwisie
    - Wszystkie wartości numeryczne mogą być NULL → serwis Python obsłuży z domyślnym 0.00
*/
SELECT
    -- Klucz łączący z dbo.skw_faktury_akceptacja_naglowek
    p.BufferDocumentId              AS ID_BUF_DOKUMENT,

    -- Numer porządkowy pozycji na fakturze
    p.BufferDocumentPositionIndex   AS NumerPozycji,

    -- Opis towaru/usługi
    ISNULL(p.ProductName, N'')      AS NazwaTowaru,

    -- Ilość i jednostka miary
    p.Quantity                      AS Ilosc,
    ISNULL(p.Unit, N'')             AS Jednostka,

    -- Ceny jednostkowe
    p.NetPrice                      AS CenaNetto,
    p.GrossPrice                    AS CenaBrutto,

    -- Wartości sumaryczne pozycji
    p.TotalNetAmount                AS WartoscNetto,
    p.TotalGrossAmount              AS WartoscBrutto,

    -- Stawka VAT (string np. '23', '8', 'ZW', 'NP')
    ISNULL(p.TaxCode, N'')          AS StawkaVAT,

    -- Dodatkowy opis pozycji (może być NULL)
    p.Description                   AS Opis

FROM dbo.Api_V_BufferDocumentPosition p;
GO

-- =============================================================================
-- WERYFIKACJA — uruchom po stworzeniu widoków
-- =============================================================================
/*
-- Test 1: Widok naglowek — sprawdź czy zwraca dane
SELECT TOP 5
    ID_BUF_DOKUMENT,
    KSEF_ID,
    NUMER,
    KOD_STATUSU,
    StatusOpis,
    DataWystawienia,
    DataOtrzymania,
    TerminPlatnosci,
    WARTOSC_NETTO,
    WARTOSC_BRUTTO,
    NazwaKontrahenta,
    EmailKontrahenta,
    TelefonKontrahenta
FROM dbo.skw_faktury_akceptacja_naglowek
ORDER BY ID_BUF_DOKUMENT DESC;

-- Test 2: Widok pozycje — sprawdź czy zwraca dane
SELECT TOP 10
    ID_BUF_DOKUMENT,
    NumerPozycji,
    NazwaTowaru,
    Ilosc,
    Jednostka,
    CenaNetto,
    WartoscNetto,
    StawkaVAT
FROM dbo.skw_faktury_akceptacja_pozycje
ORDER BY ID_BUF_DOKUMENT DESC;

-- Test 3: JOIN obu widoków (symulacja GET /{id}/details)
SELECT
    n.KSEF_ID,
    n.NUMER,
    n.StatusOpis,
    n.NazwaKontrahenta,
    n.WARTOSC_BRUTTO,
    p.NazwaTowaru,
    p.Ilosc,
    p.WartoscNetto
FROM dbo.skw_faktury_akceptacja_naglowek n
JOIN dbo.skw_faktury_akceptacja_pozycje p
    ON p.ID_BUF_DOKUMENT = n.ID_BUF_DOKUMENT
WHERE n.KSEF_ID IS NOT NULL
ORDER BY n.ID_BUF_DOKUMENT DESC, p.NumerPozycji;

-- Test 4: Czy funkcja Clarion poprawnie konwertuje daty?
SELECT
    DATA_WYSTAWIENIA                                    AS ClarionInt,
    DATEADD(DAY, DATA_WYSTAWIENIA, '18991230')          AS DataDatetime,
    CAST(DATEADD(DAY, DATA_WYSTAWIENIA, '18991230') AS DATE) AS DataDate
FROM dbo.BUF_DOKUMENT
WHERE PRG_KOD = 3
  AND DATA_WYSTAWIENIA IS NOT NULL
  AND DATA_WYSTAWIENIA > 0
ORDER BY ID_BUF_DOKUMENT DESC;
*/

-- =============================================================================
-- NASTĘPNY KROK: uruchom 019_faktura_checksums.sql
-- =============================================================================