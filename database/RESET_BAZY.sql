-- =============================================================================
-- PLIK  : 000_RESET_PELNY_RESET_BAZY.sql
-- OPIS  : Usuwa WSZYSTKIE obiekty projektu Windykacja z bazy danych.
--
-- ⚠⚠⚠  NIEODWRACALNE — tylko srodowisko testowe / developerskie  ⚠⚠⚠
--
-- STRATEGIA: Najpierw dynamicznie usuwa WSZYSTKIE FK constraints w dbo_ext,
-- potem usuwa tabele. Dzieki temu kolejnosc nie ma znaczenia i skrypt
-- jest odporny na nieznane lub niestandardowe FK.
--
-- BAZA: GPGKJASLO
-- =============================================================================

USE [GPGKJASLO];
GO

SET NOCOUNT ON;
PRINT '============================================================';
PRINT ' WINDYKACJA — PELNY RESET BAZY';
PRINT ' ' + CONVERT(NVARCHAR, GETDATE(), 120);
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- KROK 1 — Widoki projektu w schemacie dbo
-- (usuwamy przed reszta — referuja tabele WAPRO)
-- =============================================================================

PRINT '--- KROK 1: Widoki w schemacie dbo ---';

IF OBJECT_ID(N'[dbo].[skw_faktury_akceptacja_pozycje]', N'V') IS NOT NULL
BEGIN
    DROP VIEW [dbo].[skw_faktury_akceptacja_pozycje];
    PRINT '[OK] dbo.skw_faktury_akceptacja_pozycje';
END
ELSE PRINT '[--] dbo.skw_faktury_akceptacja_pozycje';

IF OBJECT_ID(N'[dbo].[skw_faktury_akceptacja_naglowek]', N'V') IS NOT NULL
BEGIN
    DROP VIEW [dbo].[skw_faktury_akceptacja_naglowek];
    PRINT '[OK] dbo.skw_faktury_akceptacja_naglowek';
END
ELSE PRINT '[--] dbo.skw_faktury_akceptacja_naglowek';

-- =============================================================================
-- KROK 2 — Dynamiczne usuniecie WSZYSTKICH FK constraints w schemacie dbo_ext
--
-- Dzieki temu tabele moga byc usuniete w dowolnej kolejnosci.
-- Nie trzeba znac pelnej mapy zaleznosci FK.
-- =============================================================================

PRINT '';
PRINT '--- KROK 2: Usuwanie wszystkich FK constraints w dbo_ext ---';

DECLARE @sql        NVARCHAR(500);
DECLARE @fk_name    NVARCHAR(200);
DECLARE @table_name NVARCHAR(200);
DECLARE @cnt        INT = 0;

DECLARE fk_cursor CURSOR FOR
    SELECT
        fk.name             AS fk_name,
        t.name              AS table_name
    FROM sys.foreign_keys fk
    JOIN sys.tables       t  ON fk.parent_object_id  = t.object_id
    JOIN sys.schemas      s  ON t.schema_id           = s.schema_id
    WHERE s.name = N'dbo_ext'
    ORDER BY fk.name;

OPEN fk_cursor;
FETCH NEXT FROM fk_cursor INTO @fk_name, @table_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'ALTER TABLE [dbo_ext].[' + @table_name + N'] DROP CONSTRAINT [' + @fk_name + N']';
    EXEC sp_executesql @sql;
    PRINT '[OK] FK: ' + @fk_name + ' (tabela: ' + @table_name + ')';
    SET @cnt = @cnt + 1;
    FETCH NEXT FROM fk_cursor INTO @fk_name, @table_name;
END

CLOSE fk_cursor;
DEALLOCATE fk_cursor;

PRINT '     Usunieto lacznie: ' + CAST(@cnt AS NVARCHAR) + ' FK constraints.';

-- =============================================================================
-- KROK 3 — Triggery
-- (usuwamy explicite; bez triggerow DROP TABLE jest czystszy)
-- =============================================================================

PRINT '';
PRINT '--- KROK 3: Triggery ---';

DECLARE @triggers_removed INT = 0;

DECLARE tr_cursor CURSOR FOR
    SELECT t.name, tb.name
    FROM sys.triggers t
    JOIN sys.tables tb  ON t.parent_id  = tb.object_id
    JOIN sys.schemas s  ON tb.schema_id = s.schema_id
    WHERE s.name = N'dbo_ext'
    ORDER BY t.name;

DECLARE @tr_name NVARCHAR(200), @tr_table NVARCHAR(200);
OPEN tr_cursor;
FETCH NEXT FROM tr_cursor INTO @tr_name, @tr_table;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'DROP TRIGGER [dbo_ext].[' + @tr_name + N']';
    EXEC sp_executesql @sql;
    PRINT '[OK] Trigger: ' + @tr_name;
    SET @triggers_removed = @triggers_removed + 1;
    FETCH NEXT FROM tr_cursor INTO @tr_name, @tr_table;
END

CLOSE tr_cursor;
DEALLOCATE tr_cursor;
PRINT '     Usunieto lacznie: ' + CAST(@triggers_removed AS NVARCHAR) + ' triggerow.';

-- =============================================================================
-- KROK 4 — Usuniecie wszystkich tabel skw_* i alembic_version w dbo_ext
--
-- Po usunieciu FK constraints w KROKU 2 kolejnosc jest dowolna.
-- Uzywamy dynamicznego kursora zeby objac rowniez tabele ktorych
-- nie ma na hardkodowanej liscie.
-- =============================================================================

PRINT '';
PRINT '--- KROK 4: Usuwanie wszystkich tabel w dbo_ext ---';

DECLARE @tables_removed INT = 0;

DECLARE tbl_cursor CURSOR FOR
    SELECT t.name
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = N'dbo_ext'
    ORDER BY t.name;

DECLARE @tbl_name NVARCHAR(200);
OPEN tbl_cursor;
FETCH NEXT FROM tbl_cursor INTO @tbl_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'DROP TABLE [dbo_ext].[' + @tbl_name + N']';
    BEGIN TRY
        EXEC sp_executesql @sql;
        PRINT '[OK] ' + @tbl_name;
        SET @tables_removed = @tables_removed + 1;
    END TRY
    BEGIN CATCH
        PRINT '[!!] BLAD przy ' + @tbl_name + ': ' + ERROR_MESSAGE();
    END CATCH
    FETCH NEXT FROM tbl_cursor INTO @tbl_name;
END

CLOSE tbl_cursor;
DEALLOCATE tbl_cursor;
PRINT '     Usunieto lacznie: ' + CAST(@tables_removed AS NVARCHAR) + ' tabel.';

-- =============================================================================
-- KROK 5 — Schemat dbo_ext (domyslnie POMIJANY)
-- Odkomentuj jezeli chcesz usunac rowniez schemat.
-- Po usunieciu schematu PRZED docker compose up uruchom recznie:
--   database/ddl/000_create_schema.sql
-- =============================================================================

PRINT '';
PRINT '--- KROK 5: Schemat dbo_ext (pomijany) ---';
PRINT '[--] dbo_ext — pomijam (odkomentuj ponizej aby usunac)';

/*
IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dbo_ext')
BEGIN
    DECLARE @remaining_obj INT;
    SELECT @remaining_obj = COUNT(*) FROM sys.objects o
    JOIN sys.schemas s ON o.schema_id = s.schema_id WHERE s.name = 'dbo_ext';
    IF @remaining_obj = 0
    BEGIN DROP SCHEMA [dbo_ext]; PRINT '[OK] Schemat dbo_ext — usuniety'; END
    ELSE PRINT '[!!] Schemat dbo_ext ma jeszcze ' + CAST(@remaining_obj AS NVARCHAR) + ' obiektow — nie usunieto';
END
*/

-- alembic_version w dbo_ext (starsze instalacje)
IF OBJECT_ID(N'[dbo_ext].[alembic_version]', N'U') IS NOT NULL
BEGIN DROP TABLE [dbo_ext].[alembic_version]; PRINT '[OK] dbo_ext.alembic_version'; END
ELSE PRINT '[--] dbo_ext.alembic_version';

-- alembic_version w dbo (aktualne instalacje — version_table_schema=dbo)
IF OBJECT_ID(N'[dbo].[alembic_version]', N'U') IS NOT NULL
BEGIN DELETE FROM [dbo].[alembic_version]; PRINT '[OK] dbo.alembic_version — wyczyszczona (' + CAST(@@ROWCOUNT AS NVARCHAR) + ' wierszy)'; END
ELSE PRINT '[--] dbo.alembic_version';

-- =============================================================================
-- WERYFIKACJA KONCOWA
-- =============================================================================

PRINT '';
PRINT '--- WERYFIKACJA: Pozostale obiekty w dbo_ext ---';

SELECT
    t.name    AS [Tabela],
    p.rows    AS [Wiersze]
FROM sys.tables t
JOIN sys.schemas   s ON t.schema_id  = s.schema_id
JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
WHERE s.name = N'dbo_ext'
ORDER BY t.name;

DECLARE @left INT;
SELECT @left = COUNT(*) FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = N'dbo_ext';

PRINT '';
PRINT '============================================================';
IF @left = 0
BEGIN
    PRINT ' SUKCES: Baza wyczyszczona. Brak tabel w dbo_ext.';
    PRINT '';
    PRINT ' Fresh install — co dalej:';
    PRINT '   .env: ALEMBIC_MODE=upgrade, RUN_SEEDS=auto';
    PRINT '   docker compose up -d';
    PRINT '   (po sukcesie: ALEMBIC_MODE=stamp)';
END
ELSE
    PRINT ' UWAGA: W dbo_ext pozostalo ' + CAST(@left AS NVARCHAR) + ' tabel — sprawdz bledy powyzej.';
PRINT '============================================================';
GO