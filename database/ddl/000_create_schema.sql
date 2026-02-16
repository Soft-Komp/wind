-- ============================================================
-- Tworzenie schematu dbo_ext
-- Uruchom jako pierwszy, przed wszystkimi innymi plikami DDL.
-- Użytkownik aplikacji potrzebuje uprawnień do tego schematu.
-- ============================================================

USE [WAPRO];
GO

-- Schemat dla wszystkich custom tabel systemu windykacji
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dbo_ext')
BEGIN
    EXEC('CREATE SCHEMA dbo_ext AUTHORIZATION dbo');
    PRINT 'Schemat dbo_ext utworzony.';
END
ELSE
BEGIN
    PRINT 'Schemat dbo_ext już istnieje — pominięto.';
END
GO

-- Uprawnienia dla użytkownika aplikacji (ustaw właściwą nazwę usera)
-- GRANT EXECUTE ON SCHEMA::dbo_ext TO [windykacja_app_user];
-- GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::dbo_ext TO [windykacja_app_user];
-- 
-- UWAGA: MasterAccessLog — tylko INSERT dla app usera
-- DENY SELECT, UPDATE, DELETE ON dbo_ext.MasterAccessLog TO [windykacja_app_user];
-- GRANT INSERT ON dbo_ext.MasterAccessLog TO [windykacja_app_user];