-- ============================================================
-- database/ddl/000_create_schema.sql
-- Tworzenie schematu dbo_ext
-- Uruchom jako PIERWSZY, przed wszystkimi innymi plikami DDL.
--
-- WAŻNE: Wszystkie tabele systemu Windykacja mają prefiks skw_
--        np. dbo_ext.skw_Users, dbo_ext.skw_Roles itd.
--
-- Wersja: 2.0.0 | Data: 2026-03-02
-- ============================================================

USE [WAPRO];
GO

SET NOCOUNT ON;
GO

-- ── Schemat dbo_ext ───────────────────────────────────────────────────────────

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dbo_ext')
BEGIN
    EXEC('CREATE SCHEMA dbo_ext AUTHORIZATION dbo');
    PRINT '[000] Schemat dbo_ext utworzony.';
END
ELSE
BEGIN
    PRINT '[000] Schemat dbo_ext już istnieje — pominięto.';
END
GO

-- ── Uprawnienia dla użytkownika aplikacji ─────────────────────────────────────
-- Odkomentuj i ustaw właściwą nazwę użytkownika aplikacji:
--
-- GRANT EXECUTE ON SCHEMA::dbo_ext TO [windykacja_app_user];
-- GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::dbo_ext TO [windykacja_app_user];
--
-- skw_MasterAccessLog — tylko INSERT dla app usera:
-- DENY  SELECT, UPDATE, DELETE ON dbo_ext.skw_MasterAccessLog TO [windykacja_app_user];
-- GRANT INSERT                 ON dbo_ext.skw_MasterAccessLog TO [windykacja_app_user];

PRINT '[000] === DDL 000: Schemat — OK ===';
GO