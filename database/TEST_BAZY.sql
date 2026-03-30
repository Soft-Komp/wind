-- =============================================================================
-- PLIK  : 001_WERYFIKACJA_INSTALACJI.sql
-- OPIS  : Sprawdza czy instalacja systemu Windykacja jest kompletna.
--         Uruchamiaj po kazdej fresh install lub upgrade.
--
-- CO SPRAWDZA:
--   1. Tabele dbo_ext — czy wszystkie istnieja
--   2. Widoki dbo — modul faktur
--   3. Triggery — czy sa na tabelach
--   4. Dane seedow — role, uprawnienia, SystemConfig, admin user
--   5. Migracja Alembic — aktualna wersja
--   6. Podsumowanie — PASS / FAIL
--
-- BAZA: GPGKJASLO
-- =============================================================================

USE [GPGKJASLO];
GO

SET NOCOUNT ON;

DECLARE @pass  INT = 0;
DECLARE @fail  INT = 0;
DECLARE @warn  INT = 0;

PRINT '============================================================';
PRINT ' WINDYKACJA — WERYFIKACJA INSTALACJI';
PRINT ' ' + CONVERT(NVARCHAR, GETDATE(), 120);
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- 1. TABELE — czy wszystkie 19 tabel skw_* istnieja
-- =============================================================================

PRINT '--- 1. TABELE w dbo_ext ---';

DECLARE @expected_tables TABLE (nazwa NVARCHAR(100));
INSERT INTO @expected_tables VALUES
    ('skw_Roles'),
    ('skw_Permissions'),
    ('skw_RolePermissions'),
    ('skw_Users'),
    ('skw_RefreshTokens'),
    ('skw_OtpCodes'),
    ('skw_Templates'),
    ('skw_AuditLog'),
    ('skw_MonitHistory'),
    ('skw_MasterAccessLog'),
    ('skw_SystemConfig'),
    ('skw_SchemaChecksums'),
    ('skw_Comments'),
    ('skw_faktura_akceptacja'),
    ('skw_faktura_przypisanie'),
    ('skw_faktura_log');

DECLARE @tbl NVARCHAR(100);
DECLARE tbl_cur CURSOR FOR SELECT nazwa FROM @expected_tables ORDER BY nazwa;
OPEN tbl_cur;
FETCH NEXT FROM tbl_cur INTO @tbl;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID(N'[dbo_ext].[' + @tbl + N']', N'U') IS NOT NULL
    BEGIN PRINT '[OK] dbo_ext.' + @tbl; SET @pass = @pass + 1; END
    ELSE
    BEGIN PRINT '[FAIL] dbo_ext.' + @tbl + ' — BRAK!'; SET @fail = @fail + 1; END
    FETCH NEXT FROM tbl_cur INTO @tbl;
END
CLOSE tbl_cur; DEALLOCATE tbl_cur;

-- Ilosc tabel
DECLARE @tbl_count INT;
SELECT @tbl_count = COUNT(*) FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = N'dbo_ext';
PRINT '     Lacznie tabel w dbo_ext: ' + CAST(@tbl_count AS NVARCHAR);

-- =============================================================================
-- 2. WIDOKI w schemacie dbo (modul faktur)
-- =============================================================================

PRINT '';
PRINT '--- 2. WIDOKI w schemacie dbo ---';

IF OBJECT_ID(N'[dbo].[skw_faktury_akceptacja_naglowek]', N'V') IS NOT NULL
BEGIN PRINT '[OK] dbo.skw_faktury_akceptacja_naglowek'; SET @pass = @pass + 1; END
ELSE
BEGIN PRINT '[FAIL] dbo.skw_faktury_akceptacja_naglowek — BRAK!'; SET @fail = @fail + 1; END

IF OBJECT_ID(N'[dbo].[skw_faktury_akceptacja_pozycje]', N'V') IS NOT NULL
BEGIN PRINT '[OK] dbo.skw_faktury_akceptacja_pozycje'; SET @pass = @pass + 1; END
ELSE
BEGIN PRINT '[FAIL] dbo.skw_faktury_akceptacja_pozycje — BRAK!'; SET @fail = @fail + 1; END

-- Test czy widok dziala (SELECT bez bledu)
BEGIN TRY
    DECLARE @v_cnt INT;
    SELECT @v_cnt = COUNT(*) FROM [dbo].[skw_faktury_akceptacja_naglowek];
    PRINT '[OK] Widok naglowek dziala — ' + CAST(@v_cnt AS NVARCHAR) + ' wierszy z WAPRO';
    SET @pass = @pass + 1;
END TRY
BEGIN CATCH
    PRINT '[FAIL] Widok naglowek zwraca blad: ' + ERROR_MESSAGE();
    SET @fail = @fail + 1;
END CATCH

-- =============================================================================
-- 3. TRIGGERY — UpdatedAt na tabelach z UpdatedAt
-- =============================================================================

PRINT '';
PRINT '--- 3. TRIGGERY ---';

DECLARE @expected_triggers TABLE (trigger_name NVARCHAR(200), table_name NVARCHAR(200));
INSERT INTO @expected_triggers VALUES
    ('TR_skw_Roles_UpdatedAt',           'skw_Roles'),
    ('TR_skw_Permissions_UpdatedAt',     'skw_Permissions'),
    ('TR_skw_Users_UpdatedAt',           'skw_Users'),
    ('TR_skw_Templates_UpdatedAt',       'skw_Templates'),
    ('TR_skw_MonitHistory_UpdatedAt',    'skw_MonitHistory'),
    ('TR_skw_SystemConfig_UpdatedAt',    'skw_SystemConfig'),
    ('TR_skw_SchemaChecksums_UpdatedAt', 'skw_SchemaChecksums'),
    ('TR_skw_Comments_UpdatedAt',        'skw_Comments'),
    ('TR_skw_faktura_akceptacja_UpdatedAt',  'skw_faktura_akceptacja'),
    ('TR_skw_faktura_przypisanie_UpdatedAt', 'skw_faktura_przypisanie');

DECLARE @tr NVARCHAR(200), @tr_tbl NVARCHAR(200);
DECLARE tr_cur CURSOR FOR SELECT trigger_name, table_name FROM @expected_triggers ORDER BY trigger_name;
OPEN tr_cur;
FETCH NEXT FROM tr_cur INTO @tr, @tr_tbl;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF EXISTS (SELECT 1 FROM sys.triggers WHERE name = @tr)
    BEGIN PRINT '[OK] Trigger ' + @tr; SET @pass = @pass + 1; END
    ELSE
    BEGIN PRINT '[WARN] Trigger ' + @tr + ' — brak (tabela: ' + @tr_tbl + ')'; SET @warn = @warn + 1; END
    FETCH NEXT FROM tr_cur INTO @tr, @tr_tbl;
END
CLOSE tr_cur; DEALLOCATE tr_cur;

-- =============================================================================
-- 4. DANE SEEDOW
-- =============================================================================

PRINT '';
PRINT '--- 4. DANE SEEDOW ---';

-- 4.1 Role (oczekiwane: 4)
DECLARE @role_count INT;
SELECT @role_count = COUNT(*) FROM [dbo_ext].[skw_Roles] WHERE IsActive = 1;
IF @role_count >= 4
BEGIN PRINT '[OK] Role: ' + CAST(@role_count AS NVARCHAR) + ' (oczekiwane min 4)'; SET @pass = @pass + 1; END
ELSE
BEGIN PRINT '[FAIL] Role: ' + CAST(@role_count AS NVARCHAR) + ' (oczekiwane min 4)'; SET @fail = @fail + 1; END

-- Lista rol
SELECT '       Rola: ' + RoleName AS [Rola] FROM [dbo_ext].[skw_Roles] WHERE IsActive = 1;

-- 4.2 Uprawnienia — lacznie (oczekiwane: 91+ = 72 bazowe + 5 templates + 14 faktury)
DECLARE @perm_count INT;
SELECT @perm_count = COUNT(*) FROM [dbo_ext].[skw_Permissions] WHERE IsActive = 1;
IF @perm_count >= 91
BEGIN PRINT '[OK] Uprawnienia: ' + CAST(@perm_count AS NVARCHAR) + ' (oczekiwane min 91)'; SET @pass = @pass + 1; END
ELSE IF @perm_count >= 72
BEGIN PRINT '[WARN] Uprawnienia: ' + CAST(@perm_count AS NVARCHAR) + ' — brakuje templates/faktury (oczekiwane 91+)'; SET @warn = @warn + 1; END
ELSE
BEGIN PRINT '[FAIL] Uprawnienia: ' + CAST(@perm_count AS NVARCHAR) + ' (oczekiwane min 91)'; SET @fail = @fail + 1; END

-- Uprawnienia per kategoria
SELECT '       ' + ISNULL(Category, 'NULL') + ': ' + CAST(COUNT(*) AS NVARCHAR) + ' uprawnien'
FROM [dbo_ext].[skw_Permissions] WHERE IsActive = 1
GROUP BY Category ORDER BY Category;

-- 4.3 Uprawnienia kategorii 'faktury' (oczekiwane: 14)
DECLARE @fact_perm INT;
SELECT @fact_perm = COUNT(*) FROM [dbo_ext].[skw_Permissions] WHERE Category = N'faktury' AND IsActive = 1;
IF @fact_perm = 14
BEGIN PRINT '[OK] Uprawnienia faktury: 14'; SET @pass = @pass + 1; END
ELSE
BEGIN PRINT '[FAIL] Uprawnienia faktury: ' + CAST(@fact_perm AS NVARCHAR) + ' (oczekiwane 14)'; SET @fail = @fail + 1; END

-- 4.4 Uprawnienia kategorii 'templates' (oczekiwane: 5)
DECLARE @tmpl_perm INT;
SELECT @tmpl_perm = COUNT(*) FROM [dbo_ext].[skw_Permissions] WHERE Category = N'templates' AND IsActive = 1;
IF @tmpl_perm = 5
BEGIN PRINT '[OK] Uprawnienia templates: 5'; SET @pass = @pass + 1; END
ELSE
BEGIN PRINT '[FAIL] Uprawnienia templates: ' + CAST(@tmpl_perm AS NVARCHAR) + ' (oczekiwane 5)'; SET @fail = @fail + 1; END

-- 4.5 SystemConfig — klucze ogolne (oczekiwane: min 8)
DECLARE @cfg_count INT;
SELECT @cfg_count = COUNT(*) FROM [dbo_ext].[skw_SystemConfig] WHERE IsActive = 1;
IF @cfg_count >= 8
BEGIN PRINT '[OK] SystemConfig: ' + CAST(@cfg_count AS NVARCHAR) + ' kluczy (oczekiwane min 8)'; SET @pass = @pass + 1; END
ELSE
BEGIN PRINT '[FAIL] SystemConfig: ' + CAST(@cfg_count AS NVARCHAR) + ' kluczy (oczekiwane min 8)'; SET @fail = @fail + 1; END

-- 4.6 SystemConfig — klucz modul_akceptacji_faktur_enabled
IF EXISTS (SELECT 1 FROM [dbo_ext].[skw_SystemConfig] WHERE ConfigKey = N'modul_akceptacji_faktur_enabled')
BEGIN PRINT '[OK] SystemConfig: modul_akceptacji_faktur_enabled istnieje'; SET @pass = @pass + 1; END
ELSE
BEGIN PRINT '[FAIL] SystemConfig: modul_akceptacji_faktur_enabled — BRAK!'; SET @fail = @fail + 1; END

-- 4.7 Admin user (oczekiwane: przynajmniej 1 aktywny user)
DECLARE @user_count INT;
SELECT @user_count = COUNT(*) FROM [dbo_ext].[skw_Users] WHERE IsActive = 1;
IF @user_count >= 1
BEGIN PRINT '[OK] Uzytkownicy aktywni: ' + CAST(@user_count AS NVARCHAR); SET @pass = @pass + 1; END
ELSE
BEGIN PRINT '[FAIL] Brak aktywnych uzytkownikow!'; SET @fail = @fail + 1; END

-- Lista userow
SELECT '       User: ' + Username + ' (rola: ' + r.RoleName + ')'
FROM [dbo_ext].[skw_Users] u
JOIN [dbo_ext].[skw_Roles] r ON u.RoleID = r.ID_ROLE
WHERE u.IsActive = 1;

-- 4.8 Checksums widokow faktur
DECLARE @chk_count INT;
SELECT @chk_count = COUNT(*) FROM [dbo_ext].[skw_SchemaChecksums]
WHERE ObjectName IN (N'dbo.skw_faktury_akceptacja_naglowek', N'dbo.skw_faktury_akceptacja_pozycje');
IF @chk_count = 2
BEGIN PRINT '[OK] SchemaChecksums: 2 wpisy dla widokow faktur'; SET @pass = @pass + 1; END
ELSE
BEGIN PRINT '[WARN] SchemaChecksums: ' + CAST(@chk_count AS NVARCHAR) + '/2 wpisow dla widokow faktur'; SET @warn = @warn + 1; END

-- =============================================================================
-- 5. MIGRACJA ALEMBIC
-- =============================================================================

PRINT '';
PRINT '--- 5. WERSJA ALEMBIC ---';

IF OBJECT_ID(N'[dbo_ext].[alembic_version]', N'U') IS NOT NULL
BEGIN
    DECLARE @alembic_ver NVARCHAR(50);
    SELECT TOP 1 @alembic_ver = version_num FROM [dbo_ext].[alembic_version];
    IF @alembic_ver = N'0007'
    BEGIN PRINT '[OK] alembic_version = 0007 (head)'; SET @pass = @pass + 1; END
    ELSE
    BEGIN PRINT '[WARN] alembic_version = ' + ISNULL(@alembic_ver, 'NULL') + ' (oczekiwane: 0007)'; SET @warn = @warn + 1; END
END
ELSE
    PRINT '[WARN] Tabela alembic_version nie istnieje — Alembic nie byl uruchamiany';

-- =============================================================================
-- 6. INDEKSY KRYTYCZNE (modul faktur)
-- =============================================================================

PRINT '';
PRINT '--- 6. INDEKSY KRYTYCZNE ---';

DECLARE @idx_check TABLE (idx_name NVARCHAR(200), tbl_name NVARCHAR(200));
INSERT INTO @idx_check VALUES
    ('IX_sfp_user_active',    'skw_faktura_przypisanie'),
    ('IX_sfp_faktura_active', 'skw_faktura_przypisanie'),
    ('IX_sfl_faktura',        'skw_faktura_log');

DECLARE @idx NVARCHAR(200), @idx_tbl NVARCHAR(200);
DECLARE idx_cur CURSOR FOR SELECT idx_name, tbl_name FROM @idx_check;
OPEN idx_cur;
FETCH NEXT FROM idx_cur INTO @idx, @idx_tbl;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = @idx AND object_id = OBJECT_ID(N'dbo_ext.' + @idx_tbl))
    BEGIN PRINT '[OK] Indeks ' + @idx; SET @pass = @pass + 1; END
    ELSE
    BEGIN PRINT '[WARN] Indeks ' + @idx + ' na ' + @idx_tbl + ' — brak'; SET @warn = @warn + 1; END
    FETCH NEXT FROM idx_cur INTO @idx, @idx_tbl;
END
CLOSE idx_cur; DEALLOCATE idx_cur;

-- =============================================================================
-- 7. PODSUMOWANIE
-- =============================================================================

PRINT '';
PRINT '============================================================';
PRINT ' WYNIK WERYFIKACJI';
PRINT '   PASS : ' + CAST(@pass AS NVARCHAR);
PRINT '   WARN : ' + CAST(@warn AS NVARCHAR) + '  (nieblokujace — sprawdz)';
PRINT '   FAIL : ' + CAST(@fail AS NVARCHAR);
PRINT '';

IF @fail = 0 AND @warn = 0
    PRINT ' STATUS: ✓ INSTALACJA KOMPLETNA — system gotowy do pracy.';
ELSE IF @fail = 0
    PRINT ' STATUS: ~ INSTALACJA OK z ostrzezeniami — sprawdz WARN powyzej.';
ELSE
    PRINT ' STATUS: ✗ INSTALACJA NIEPELNA — ' + CAST(@fail AS NVARCHAR) + ' krytycznych bledow!';

PRINT '============================================================';
GO