-- =============================================================================
-- PLIK:    021_fakir_write_user.sql
-- MODUŁ:   Akceptacja Faktur KSeF — Sprint 2 / Sesja 2
-- SERWER:  GPGKJASLO (192.168.0.50) | BAZA: GPGKJASLO
-- AUTOR:   Windykacja-gpgk-backend
-- DATA:    2026-03-26
--
-- ZAWARTOŚĆ:
--   Tworzenie użytkownika SQL Server windykacja_fakir_write z MINIMALNYM
--   zestawem uprawnień wymaganych do zatwierdzania faktur w Fakirze.
--
-- ZASADA MINIMALNYCH UPRAWNIEŃ (Principle of Least Privilege):
--   Użytkownik może TYLKO:
--     ✅ UPDATE (KOD_STATUSU) ON dbo.BUF_DOKUMENT  ← cel biznesowy
--     ✅ SELECT (KSEF_ID, PRG_KOD, KOD_STATUSU) ON dbo.BUF_DOKUMENT
--        ← wymagane przez klauzulę WHERE przy UPDATE + weryfikacja po UPDATE
--
--   Użytkownik NIE może:
--     ❌ SELECT na całej tabeli / innych kolumnach
--     ❌ INSERT / DELETE na jakiejkolwiek tabeli
--     ❌ DDL (ALTER, DROP, CREATE)
--     ❌ Dostęp do innych tabel WAPRO
--
-- ⚠️  WYMAGANE DZIAŁANIA DBA PRZED URUCHOMIENIEM:
--   1. Zmień hasło w wierszu 'PASSWORD = ...' poniżej
--   2. Przechowaj hasło w bezpiecznym menedżerze haseł
--   3. Wpisz hasło do .env jako FAKIR_DB_PASSWORD
--   4. NIE commituj tego pliku z prawdziwym hasłem do repozytorium!
--
-- URUCHOMIENIE:
--   Wymagane uprawnienia DBA: sysadmin lub securityadmin + db_owner na GPGKJASLO
--   Podłącz SSMS do GPGKJASLO jako sa/administrator
--   Uruchom cały plik (F5)
--
-- IDEMPOTENTNOŚĆ:
--   IF NOT EXISTS dla LOGIN i USER — bezpieczne wielokrotne uruchomienie.
--   GRANT nie ma IF EXISTS — może wyrzucić "permission already exists" jako warning
--   (nie jest to błąd, skrypt kontynuuje).
-- =============================================================================

USE [master];
GO

-- ===========================================================================
-- KROK 1: SQL Server Login (poziom serwera)
-- ===========================================================================

-- ⚠️ ZMIEŃ HASŁO PONIŻEJ PRZED URUCHOMIENIEM
-- Wymagania hasła:
--   - Min 16 znaków
--   - Duże i małe litery + cyfry + znak specjalny
--   - CHECK_POLICY=ON — SQL Server weryfikuje zgodność z polityką Windows
-- Przykład silnego hasła: Fk!W1nd3k4cj4_2026#Srv
-- NIE używaj przykładowego hasła produkcyjnie!

IF NOT EXISTS (
    SELECT 1 FROM sys.server_principals
    WHERE name = N'windykacja_fakir_write'
      AND type = 'S'  -- SQL Login (nie Windows)
)
BEGIN
    -- ⚠️ ZMIEŃ HASŁO ↓
    CREATE LOGIN [windykacja_fakir_write]
        WITH PASSWORD   = N'$(FAKIR_PASSWORD)',
             -- CHECK_POLICY: weryfikacja siły hasła przez politykę Windows
             CHECK_POLICY      = ON,
             -- CHECK_EXPIRATION: wygasanie hasła (OFF dla service account)
             CHECK_EXPIRATION   = OFF,
             -- DEFAULT_DATABASE: baza startowa po połączeniu
             DEFAULT_DATABASE   = [GPGKJASLO],
             DEFAULT_LANGUAGE   = [Polish];

    PRINT N'[OK] Login windykacja_fakir_write UTWORZONY.';
END
ELSE
BEGIN
    PRINT N'[INFO] Login windykacja_fakir_write już istnieje — pomijam CREATE LOGIN.';
END
GO

-- ===========================================================================
-- KROK 2: Database User w bazie GPGKJASLO
-- ===========================================================================

GO

IF NOT EXISTS (
    SELECT 1 FROM sys.database_principals
    WHERE name = N'windykacja_fakir_write'
      AND type = 'S'
)
BEGIN
    CREATE USER [windykacja_fakir_write]
        FOR LOGIN [windykacja_fakir_write]
        WITH DEFAULT_SCHEMA = [dbo];

    PRINT N'[OK] User windykacja_fakir_write UTWORZONY w bazie GPGKJASLO.';
END
ELSE
BEGIN
    PRINT N'[INFO] User windykacja_fakir_write już istnieje w bazie GPGKJASLO — pomijam CREATE USER.';
END
GO

-- ===========================================================================
-- KROK 3: Uprawnienia — zasada minimalnych praw
-- ===========================================================================

-- GRANT 1: UPDATE tylko na kolumnie KOD_STATUSU
-- To jest uprawnienie kolumnowe — windykacja_fakir_write może zmieniać
-- WYŁĄCZNIE tę jedną kolumnę w tej jednej tabeli.
GRANT UPDATE (KOD_STATUSU)
    ON dbo.BUF_DOKUMENT
    TO [windykacja_fakir_write];
GO

-- GRANT 2: SELECT na kolumnach wymaganych przez WHERE w UPDATE
-- Bez tego SQL Server odmówi wykonania:
-- UPDATE BUF_DOKUMENT SET KOD_STATUSU='K' WHERE KSEF_ID=:id AND PRG_KOD=3
-- Minimalne kolumny: KSEF_ID (warunek WHERE), PRG_KOD (warunek WHERE),
-- KOD_STATUSU (weryfikacja po UPDATE: SELECT KOD_STATUSU WHERE KSEF_ID=:id)
GRANT SELECT (KSEF_ID, PRG_KOD, KOD_STATUSU)
    ON dbo.BUF_DOKUMENT
    TO [windykacja_fakir_write];
GO

-- DENY: jawne zablokowanie wszystkiego innego na tej tabeli
-- Defensywne — domyślnie i tak brak uprawnień, ale explicit DENY jest
-- odporny na przyszłe GRANT przez pomyłkę (DENY ma wyższy priorytet niż GRANT)
DENY INSERT  ON dbo.BUF_DOKUMENT TO [windykacja_fakir_write];
DENY DELETE  ON dbo.BUF_DOKUMENT TO [windykacja_fakir_write];
GO

-- ===========================================================================
-- KROK 4: Weryfikacja uprawnień
-- ===========================================================================

-- Sprawdź uprawnienia użytkownika na tabeli BUF_DOKUMENT
SELECT
    dp.name             AS PrincipalName,
    dp.type_desc        AS PrincipalType,
    op.permission_name  AS Permission,
    op.state_desc       AS State,           -- GRANT / DENY / REVOKE
    op.class_desc       AS ObjectClass,
    OBJECT_NAME(op.major_id) AS ObjectName,
    col.name            AS ColumnName       -- NULL = uprawnienie na całej tabeli
FROM sys.database_permissions op
JOIN sys.database_principals dp
    ON dp.principal_id = op.grantee_principal_id
LEFT JOIN sys.columns col
    ON col.object_id = op.major_id
    AND col.column_id = op.minor_id
WHERE
    dp.name = N'windykacja_fakir_write'
ORDER BY op.permission_name, col.name;
GO

-- Oczekiwany wynik (5 wierszy):
-- windykacja_fakir_write | SQL_USER | DELETE  | DENY  | OBJECT | BUF_DOKUMENT | NULL
-- windykacja_fakir_write | SQL_USER | INSERT  | DENY  | OBJECT | BUF_DOKUMENT | NULL
-- windykacja_fakir_write | SQL_USER | SELECT  | GRANT | COLUMN | BUF_DOKUMENT | KSEF_ID
-- windykacja_fakir_write | SQL_USER | SELECT  | GRANT | COLUMN | BUF_DOKUMENT | KOD_STATUSU
-- windykacja_fakir_write | SQL_USER | SELECT  | GRANT | COLUMN | BUF_DOKUMENT | PRG_KOD
-- windykacja_fakir_write | SQL_USER | UPDATE  | GRANT | COLUMN | BUF_DOKUMENT | KOD_STATUSU

-- ===========================================================================
-- KROK 5: Test funkcjonalny (uruchom ręcznie po dodaniu danych testowych)
-- ===========================================================================
/*
-- Zmień kontekst na windykacja_fakir_write i przetestuj:
EXECUTE AS USER = 'windykacja_fakir_write';

-- Test 1: UPDATE dozwolony (powinien działać)
UPDATE dbo.BUF_DOKUMENT
SET KOD_STATUSU = 'K'
WHERE KSEF_ID = 'TEST_KSEF_ID_123' AND PRG_KOD = 3;
-- Oczekiwany wynik: "0 rows affected" lub "1 rows affected" (nie: "permission denied")

-- Test 2: SELECT na zabronionych kolumnach (powinien FAIL)
SELECT NUMER, WARTOSC_BRUTTO FROM dbo.BUF_DOKUMENT WHERE PRG_KOD = 3;
-- Oczekiwany wynik: "The SELECT permission was denied on the column 'NUMER'"

-- Test 3: DELETE (powinien FAIL)
DELETE FROM dbo.BUF_DOKUMENT WHERE ID_BUF_DOKUMENT = 0;
-- Oczekiwany wynik: "The DELETE permission was denied"

REVERT;  -- Wróć do oryginalnego kontekstu!
*/

-- ===========================================================================
-- KROK 6: Zmienne środowiskowe dla .env.docker
-- Dodaj do .env NASTĘPUJĄCE ZMIENNE (wartości dostarcza DBA):
-- ===========================================================================
/*
# Fakir Write Connection — osobna pula do zapisu BUF_DOKUMENT
# UWAGA: FAKIR_DB_USER musi być RÓŻNY od DB_USER (walidator w config.py blokuje start!)
FAKIR_DB_HOST=host.docker.internal
FAKIR_DB_PORT=1433
FAKIR_DB_NAME=GPGKJASLO
FAKIR_DB_USER=windykacja_fakir_write
FAKIR_DB_PASSWORD=TUTAJ_WPISZ_HASLO_Z_KROKU_1
FAKIR_WRITE_POOL_SIZE=2
FAKIR_WRITE_TIMEOUT=30
*/

-- =============================================================================
-- SESJA 2 ZAKOŃCZONA — wszystkie 4 pliki DDL gotowe.
-- Następne kroki: Sesja 3 → Seeds (08, 09, 10) + naprawa 05_system_config.sql
-- =============================================================================