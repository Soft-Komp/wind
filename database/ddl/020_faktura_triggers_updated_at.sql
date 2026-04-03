-- =============================================================================
-- PLIK:    020_faktura_triggers_updated_at.sql
-- MODUŁ:   Akceptacja Faktur KSeF — Sprint 2 / Sesja 2
-- SERWER:  GPGKJASLO (192.168.0.50) | BAZA: GPGKJASLO
-- AUTOR:   Windykacja-gpgk-backend
-- DATA:    2026-03-26
--
-- ZAWARTOŚĆ:
--   Triggery AFTER UPDATE dla tabel modułu akceptacji faktur.
--   Wzorzec identyczny z 014_triggers_updated_at.sql (istniejący plik projektu).
--
-- TABELE OBJĘTE TRIGGERAMI:
--   ✅ dbo_ext.skw_faktura_akceptacja  — ma kolumnę UpdatedAt
--   ✅ dbo_ext.skw_faktura_przypisanie — ma kolumnę UpdatedAt
--   ❌ dbo_ext.skw_faktura_log        — IMMUTABLE (tylko INSERT, nigdy UPDATE)
--
-- LOGIKA TRIGGERA:
--   IF NOT UPDATE(UpdatedAt) → SQL Server sprawdza czy kolumna UpdatedAt
--   była jawnie ustawiana w instrukcji UPDATE.
--   - Jeśli NIE → trigger ustawia UpdatedAt = GETDATE() (automatyczna aktualizacja)
--   - Jeśli TAK → trigger nie nadpisuje (szanuje wartość ustawioną ręcznie)
--   Redundantne z SQLAlchemy onupdate=datetime.utcnow — podwójne zabezpieczenie.
--
-- IDEMPOTENTNOŚĆ:
--   DROP TRIGGER IF EXISTS przed CREATE TRIGGER — bezpieczne wielokrotne uruchomienie.
--
-- ⚠️  UWAGA NA DATETIME2:
--   Nowe tabele (skw_faktura_*) używają DATETIME2 (nie DATETIME jak starsze tabele).
--   GETDATE() zwraca DATETIME — automatyczna konwersja do DATETIME2 jest bezpieczna.
-- =============================================================================

GO

-- ===========================================================================
-- TRIGGER 1: skw_faktura_akceptacja — UpdatedAt
-- Tabela główna modułu. Aktualizowana przy: PATCH priorytetu/opisu/uwag,
-- zmianie status_wewnetrzny, reset, force_status, archiwizacja (IsActive=0).
-- ===========================================================================

DROP TRIGGER IF EXISTS dbo_ext.TR_skw_faktura_akceptacja_UpdatedAt;
GO

CREATE TRIGGER dbo_ext.TR_skw_faktura_akceptacja_UpdatedAt
ON dbo_ext.skw_faktura_akceptacja
AFTER UPDATE
AS
BEGIN
    -- Nie przerywaj całej transakcji przy błędzie triggera
    SET NOCOUNT ON;

    -- Wejdź tylko jeśli aplikacja NIE ustawiła UpdatedAt jawnie.
    -- SQLAlchemy onupdate=datetime.utcnow ustawia UpdatedAt → UPDATE(UpdatedAt)=TRUE
    -- → trigger nie nadpisuje, bo SQLAlchemy już to zrobił.
    -- Bez SQLAlchemy (np. ręczny UPDATE przez SSMS) → trigger wkracza.
    IF NOT UPDATE(UpdatedAt)
    BEGIN
        UPDATE dbo_ext.skw_faktura_akceptacja
        SET    UpdatedAt = GETDATE()
        FROM   dbo_ext.skw_faktura_akceptacja fa
        -- INSERTED — wirtualna tabela z nowymi wartościami zaktualizowanych wierszy
        INNER JOIN INSERTED i ON fa.id = i.id;
    END
END;
GO

-- ===========================================================================
-- TRIGGER 2: skw_faktura_przypisanie — UpdatedAt
-- Tabela przypisań pracowników. Aktualizowana przy: decyzji pracownika
-- (status: oczekuje → zaakceptowane/odrzucone/nie_moje), reset referenta
-- (is_active: 1 → 0).
-- ===========================================================================

DROP TRIGGER IF EXISTS dbo_ext.TR_skw_faktura_przypisanie_UpdatedAt;
GO

CREATE TRIGGER dbo_ext.TR_skw_faktura_przypisanie_UpdatedAt
ON dbo_ext.skw_faktura_przypisanie
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT UPDATE(UpdatedAt)
    BEGIN
        UPDATE dbo_ext.skw_faktura_przypisanie
        SET    UpdatedAt = GETDATE()
        FROM   dbo_ext.skw_faktura_przypisanie fp
        INNER JOIN INSERTED i ON fp.id = i.id;
    END
END;
GO

-- ===========================================================================
-- WERYFIKACJA — triggery powinny być widoczne w sys.triggers
-- ===========================================================================
SELECT
    t.name           AS TriggerName,
    OBJECT_NAME(t.parent_id) AS Tabela,
    t.is_disabled,
    t.is_instead_of_trigger,
    t.create_date,
    t.modify_date
FROM sys.triggers t
JOIN sys.objects o ON t.parent_id = o.object_id
WHERE
    SCHEMA_NAME(o.schema_id) = 'dbo_ext'
    AND t.name IN (
        'TR_skw_faktura_akceptacja_UpdatedAt',
        'TR_skw_faktura_przypisanie_UpdatedAt'
    )
ORDER BY t.name;

-- Oczekiwany wynik: 2 wiersze, is_disabled=0, is_instead_of_trigger=0
GO

-- ===========================================================================
-- TEST TRIGGERA (opcjonalny — wykonaj ręcznie po wstawieniu danych testowych)
-- ===========================================================================
/*
-- 1. Sprawdź aktualny UpdatedAt przed UPDATE
SELECT id, status_wewnetrzny, UpdatedAt FROM dbo_ext.skw_faktura_akceptacja WHERE id = 1;

-- 2. Wykonaj UPDATE bez jawnego ustawiania UpdatedAt
UPDATE dbo_ext.skw_faktura_akceptacja
SET priorytet = 'pilny'
WHERE id = 1;

-- 3. Sprawdź czy trigger ustawił UpdatedAt
SELECT id, priorytet, UpdatedAt FROM dbo_ext.skw_faktura_akceptacja WHERE id = 1;
-- Oczekiwany wynik: UpdatedAt ≈ TERAZ

-- 4. Test: UPDATE z jawnym UpdatedAt (trigger NIE powinien nadpisać)
DECLARE @custom_dt DATETIME2 = '2025-01-01 00:00:00';
UPDATE dbo_ext.skw_faktura_akceptacja
SET priorytet = 'normalny', UpdatedAt = @custom_dt
WHERE id = 1;
SELECT id, priorytet, UpdatedAt FROM dbo_ext.skw_faktura_akceptacja WHERE id = 1;
-- Oczekiwany wynik: UpdatedAt = '2025-01-01 00:00:00' (trigger nie nadpisał)
*/

-- =============================================================================
-- NASTĘPNY KROK: uruchom 021_fakir_write_user.sql
-- =============================================================================