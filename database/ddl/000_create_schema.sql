-- =============================================================================
-- database/ddl/000_create_schema.sql
-- Tworzenie schematu dbo_ext — KROK ZERO, infrastruktura
-- =============================================================================
--
-- WAŻNE: Ten plik jest uruchamiany przez entrypoint.sh PRZED Alembic.
--        Jest IDEMPOTENTNY — bezpieczne uruchamianie wielokrotnie.
--        Nie wymaga istniejącego schematu — tworzy go jeśli brak.
--
-- Uruchomienie ręczne (Windows PowerShell, jedna linia):
--   sqlcmd -S tcp:HOST,PORT -d DBNAME -U USER -P PASS -C -b -I -i database/ddl/000_create_schema.sql
--
-- Wersja: 2.1.0 | Data: 2026-03-03
-- =============================================================================

SET NOCOUNT ON;
GO

PRINT '=== DDL 000: Tworzenie schematu dbo_ext ===';
PRINT 'Czas: ' + CONVERT(NVARCHAR, GETDATE(), 120);
PRINT 'Baza: ' + DB_NAME();
GO

-- =============================================================================
-- SCHEMAT dbo_ext
-- Wymagany przez WSZYSTKIE tabele skw_* systemu Windykacja.
-- CREATE SCHEMA musi być jedyną instrukcją w batchu — stąd sp_executesql.
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dbo_ext')
BEGIN
    EXEC sp_executesql N'CREATE SCHEMA [dbo_ext] AUTHORIZATION dbo';
    PRINT '[000] Schemat dbo_ext: UTWORZONY.';
END
ELSE
BEGIN
    PRINT '[000] Schemat dbo_ext: już istnieje — pominięto.';
END
GO

-- =============================================================================
-- WERYFIKACJA — upewnij się że schemat rzeczywiście istnieje
-- =============================================================================

DECLARE @schema_id INT = SCHEMA_ID(N'dbo_ext');
IF @schema_id IS NULL
BEGIN
    PRINT '[000] BŁĄD KRYTYCZNY: Schemat dbo_ext nie został utworzony!';
    THROW 50001, 'Nie udało się utworzyć schematu dbo_ext.', 1;
END

PRINT '[000] Weryfikacja: schemat dbo_ext istnieje (schema_id=' + CAST(@schema_id AS NVARCHAR) + ').';
PRINT '[000] === DDL 000: OK ===';
GO