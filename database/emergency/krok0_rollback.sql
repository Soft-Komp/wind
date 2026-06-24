-- =============================================================================
-- database/emergency/krok0_rollback.sql
--
-- ARTEFAKT AWARYJNY — uzywany wylacznie gdy Krok 0 musi byc cofniety.
--
-- Co robi:
--   Usuwa triggery DENY z trzech starych tabel (skw_faktura_akceptacja,
--   skw_faktura_przypisanie, skw_faktura_log), przywracajac mozliwosc
--   zapisu. NIE przywraca danych — dane w nowych tabelach pozostaja.
--
-- Kiedy uzywac:
--   Tylko gdy Krok 0 zostal uruchomiony i potwierdzono koniecznosc rollbacku.
--   Po tym skrypcie system wraca do starego trybu pracy (stare tabele aktywne).
--   Endpoint /faktury-akceptacja musi miec ETAP2_FAKTURA_ENDPOINT_NEW_IMPL=false.
--
-- Kolejnosc dzialan po rollbacku:
--   1. Uruchom ten skrypt w SSMS
--   2. Upewnij sie ze ETAP2_FAKTURA_ENDPOINT_NEW_IMPL=false w skw_SystemConfig
--   3. Zrestartuj backend: docker restart windykacja_api
--   4. Zweryfikuj ze /faktury-akceptacja zwraca dane ze starych tabel
--
-- Skrypt jest IDEMPOTENTNY — bezpieczny do wielokrotnego uruchomienia.
-- =============================================================================

-- UWAGA: Przed uruchomieniem zmien nazwe bazy na właściwą instancję:
--   STOMIL (test):      USE [STOMIL];
--   GPGKJASLO (prod):   USE [GPGKJASLO];
-- Domyślnie poniżej wstawiona nazwa STOMIL — zmień ręcznie przed uruchomieniem na produkcji.
USE [STOMIL];
GO

PRINT N'';
PRINT N'=== [KROK0_ROLLBACK] START: usuwanie triggerow DENY ===';
PRINT N'=== Timestamp: ' + CONVERT(NVARCHAR(30), GETDATE(), 126) + N' ===';
PRINT N'';
GO

-- ── 1. skw_faktura_akceptacja ─────────────────────────────────────────────────

IF EXISTS (
    SELECT 1 FROM sys.triggers
    WHERE name = N'TR_skw_faktura_akceptacja_DENY'
      AND parent_id = OBJECT_ID(N'[dbo].[skw_faktura_akceptacja]')
)
BEGIN
    DROP TRIGGER [dbo].[TR_skw_faktura_akceptacja_DENY];
    PRINT N'[ROLLBACK] DROP TRIGGER TR_skw_faktura_akceptacja_DENY — OK';
END
ELSE
    PRINT N'[ROLLBACK] TR_skw_faktura_akceptacja_DENY nie istnieje — pomijam';
GO

-- ── 2. skw_faktura_przypisanie ────────────────────────────────────────────────

IF EXISTS (
    SELECT 1 FROM sys.triggers
    WHERE name = N'TR_skw_faktura_przypisanie_DENY'
      AND parent_id = OBJECT_ID(N'[dbo].[skw_faktura_przypisanie]')
)
BEGIN
    DROP TRIGGER [dbo].[TR_skw_faktura_przypisanie_DENY];
    PRINT N'[ROLLBACK] DROP TRIGGER TR_skw_faktura_przypisanie_DENY — OK';
END
ELSE
    PRINT N'[ROLLBACK] TR_skw_faktura_przypisanie_DENY nie istnieje — pomijam';
GO

-- ── 3. skw_faktura_log ────────────────────────────────────────────────────────

IF EXISTS (
    SELECT 1 FROM sys.triggers
    WHERE name = N'TR_skw_faktura_log_DENY'
      AND parent_id = OBJECT_ID(N'[dbo].[skw_faktura_log]')
)
BEGIN
    DROP TRIGGER [dbo].[TR_skw_faktura_log_DENY];
    PRINT N'[ROLLBACK] DROP TRIGGER TR_skw_faktura_log_DENY — OK';
END
ELSE
    PRINT N'[ROLLBACK] TR_skw_faktura_log_DENY nie istnieje — pomijam';
GO

-- ── Weryfikacja ───────────────────────────────────────────────────────────────

PRINT N'';
PRINT N'=== [ROLLBACK] Weryfikacja — czy triggery zostaly usuniete ===';

SELECT
    t.name          AS TriggerName,
    OBJECT_NAME(t.parent_id) AS Tabela,
    t.is_disabled   AS JestWylaczony
FROM sys.triggers t
WHERE t.name IN (
    N'TR_skw_faktura_akceptacja_DENY',
    N'TR_skw_faktura_przypisanie_DENY',
    N'TR_skw_faktura_log_DENY'
);

-- Oczekiwany wynik: 0 wierszy (wszystkie triggery usuniete)
PRINT N'';
PRINT N'Oczekiwany wynik powyzej: 0 wierszy';
PRINT N'Jesli widac wiersze — rollback nie powiodl sie dla tych tabel.';
PRINT N'';
PRINT N'=== [KROK0_ROLLBACK] ZAKONCZONE ===';
GO