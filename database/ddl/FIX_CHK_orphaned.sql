-- =============================================================================
-- FIX_CHK_orphaned.sql
-- Dodanie statusu 'orphaned' do CHECK constraint skw_faktura_akceptacja.
--
-- PROBLEM:  CHK_sfa_status_wewnetrzny nie zawierał wartości 'orphaned'.
-- NAPRAWA:  DROP starego + ADD nowego z pełną listą wartości.
-- IDEMPOTENTNY: TAK — IF EXISTS przed DROP.
-- KIEDY:    Przed uruchomieniem migracji 0009 (lub niezależnie).
-- =============================================================================
GO

SET NOCOUNT ON;
PRINT '=== FIX: CHK_sfa_status_wewnetrzny + orphaned ===';
PRINT 'Czas: ' + CONVERT(NVARCHAR, GETDATE(), 120);

BEGIN TRANSACTION;
BEGIN TRY

    -- 1. Usuwamy stare ograniczenie (jeśli istnieje)
    IF EXISTS (
        SELECT 1 FROM sys.check_constraints
        WHERE name = 'CHK_sfa_status_wewnetrzny'
          AND parent_object_id = OBJECT_ID('dbo_ext.skw_faktura_akceptacja')
    )
    BEGIN
        ALTER TABLE [dbo_ext].[skw_faktura_akceptacja]
        DROP CONSTRAINT [CHK_sfa_status_wewnetrzny];
        PRINT '[FIX] Stary constraint usunięty.';
    END
    ELSE
    BEGIN
        PRINT '[FIX] Stary constraint nie istnieje — pomijam DROP.';
    END

    -- 2. Dodajemy nowe ograniczenie z 'orphaned'
    ALTER TABLE [dbo_ext].[skw_faktura_akceptacja]
    ADD CONSTRAINT [CHK_sfa_status_wewnetrzny]
    CHECK ([status_wewnetrzny] IN (
        N'anulowana',
        N'zaakceptowana',
        N'w_toku',
        N'nowe',
        N'orphaned'
    ));
    PRINT '[FIX] Nowy constraint CHK_sfa_status_wewnetrzny (z orphaned) dodany.';

    COMMIT TRANSACTION;
    PRINT '=== FIX: Zakończony pomyślnie ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT '[BŁĄD] ' + ERROR_MESSAGE();
    THROW;
END CATCH;
GO

-- Weryfikacja
SELECT
    cc.name        AS constraint_name,
    cc.definition  AS definition,
    GETDATE()      AS verified_at
FROM sys.check_constraints cc
WHERE cc.name = 'CHK_sfa_status_wewnetrzny';
GO