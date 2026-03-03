-- =============================================================================
-- SEED 01 — Roles (role systemowe)
-- =============================================================================
-- Plik:    database/seeds/01_roles.sql
-- Wersja:  1.0.0
-- Data:    2026-02-18
-- Zgodny:  USTALENIA_PROJEKTU v1.6 §5.5, TABELE_REFERENCJA v1.0
--
-- IDEMPOTENTNY — można uruchamiać wielokrotnie bez skutków ubocznych.
-- Mechanizm: MERGE (INSERT only — nigdy UPDATE istniejących ról).
-- Nie nadpisuje opisu ani IsActive jeśli rola już istnieje.
--
-- Kolejność: MUSI być wykonany przed 02_permissions.sql (brak FK, ale logika).
-- =============================================================================

SET NOCOUNT ON;
SET XACT_ABORT ON;  -- rollback całej transakcji przy błędzie
GO

PRINT '=== SEED 01: Roles — START ===';
PRINT 'Czas: ' + CONVERT(NVARCHAR, GETDATE(), 120);

BEGIN TRANSACTION;
BEGIN TRY

    -- -------------------------------------------------------------------------
    -- MERGE: wstawia rolę jeśli nie istnieje; nie modyfikuje istniejących
    -- -------------------------------------------------------------------------

    MERGE [dbo_ext].[skw_Roles] AS target
    USING (
        SELECT RoleName, Description FROM (VALUES
            -- Rola         | Opis
            (N'Admin',      N'Pełne uprawnienia administracyjne do całego systemu'),
            (N'Manager',    N'Zarządzanie windykacją — widzi wszystko, wysyła monity, nie modyfikuje konfiguracji systemu'),
            (N'User',       N'Podstawowy pracownik biurowy — widok dłużników, wysyłka pojedynczych monitów'),
            (N'ReadOnly',   N'Tylko podgląd — brak możliwości wysyłki lub modyfikacji danych')
        ) AS src (RoleName, Description)
    ) AS source
        ON target.[RoleName] = source.[RoleName]
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ([RoleName], [Description], [IsActive], [CreatedAt])
        VALUES (source.[RoleName], source.[Description], 1, GETDATE());

    PRINT 'Role: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' wstawione (istniejące pominięte).';

    -- -------------------------------------------------------------------------
    -- Weryfikacja — log stanu po operacji
    -- -------------------------------------------------------------------------
    SELECT
        [ID_ROLE],
        [RoleName],
        [Description],
        [IsActive],
        CONVERT(NVARCHAR, [CreatedAt], 120) AS CreatedAt
    FROM [dbo_ext].[skw_Roles]
    ORDER BY [ID_ROLE];

    COMMIT TRANSACTION;
    PRINT '=== SEED 01: Roles — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    DECLARE @msg  NVARCHAR(2048) = ERROR_MESSAGE();
    DECLARE @line INT            = ERROR_LINE();
    DECLARE @sev  INT            = ERROR_SEVERITY();
    PRINT '=== SEED 01: BŁĄD ===';
    PRINT 'Linia:    ' + CAST(@line AS NVARCHAR);
    PRINT 'Wiadomość: ' + @msg;
    RAISERROR(@msg, @sev, 1);
END CATCH
GO