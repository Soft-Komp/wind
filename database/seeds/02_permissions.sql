-- =============================================================================
-- SEED 02 — Permissions (83 uprawnienia w 12 kategoriach)
-- =============================================================================
-- Plik:    database/seeds/02_permissions.sql
-- Wersja:  1.0.0
-- Data:    2026-02-18
-- Zgodny:  USTALENIA_PROJEKTU v1.6 §11 (pełna lista 83 uprawnień)
--
-- IDEMPOTENTNY — MERGE INSERT only.
-- Nie nadpisuje Description ani IsActive jeśli uprawnienie już istnieje.
-- Jeśli dodasz nowe uprawnienie — po prostu uruchom ponownie.
--
-- Kategorie (12):
--   auth (8), users (11), roles (7), permissions (9),
--   debtors (8), comments (6), monits (12), pdf (4),
--   reports (5), audit (4), snapshots (4), system (5)
-- SUMA: 83 uprawnienia
--
-- Wymaganie: 01_roles.sql musi być wykonany wcześniej (brak FK ale logika).
-- =============================================================================

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

PRINT '=== SEED 02: Permissions — START ===';
PRINT 'Czas: ' + CONVERT(NVARCHAR, GETDATE(), 120);

BEGIN TRANSACTION;
BEGIN TRY

    -- =========================================================================
    -- MERGE: 83 uprawnienia — INSERT jeśli nie istnieje
    -- =========================================================================
    MERGE [dbo_ext].[Permissions] AS target
    USING (
        SELECT PermissionName, Description, Category FROM (VALUES

            -- =================================================================
            -- AUTH (8) — uwierzytelnianie i sesje
            -- =================================================================
            (N'auth.login',
             N'Logowanie do systemu',
             N'auth'),
            (N'auth.logout',
             N'Wylogowanie z systemu',
             N'auth'),
            (N'auth.change_own_password',
             N'Zmiana własnego hasła',
             N'auth'),
            (N'auth.reset_password_any',
             N'Reset hasła dowolnego użytkownika (admin)',
             N'auth'),
            (N'auth.view_own_sessions',
             N'Podgląd własnych aktywnych sesji / refresh tokenów',
             N'auth'),
            (N'auth.revoke_own_sessions',
             N'Unieważnianie własnych sesji',
             N'auth'),
            (N'auth.revoke_any_sessions',
             N'Unieważnianie sesji dowolnego użytkownika',
             N'auth'),
            (N'auth.impersonate',
             N'Logowanie w kontekście innego użytkownika (impersonacja)',
             N'auth'),

            -- =================================================================
            -- USERS (11) — zarządzanie użytkownikami
            -- =================================================================
            (N'users.view_list',
             N'Lista wszystkich użytkowników systemu',
             N'users'),
            (N'users.view_details',
             N'Szczegóły dowolnego użytkownika',
             N'users'),
            (N'users.view_own_profile',
             N'Własny profil użytkownika',
             N'users'),
            (N'users.edit_own_profile',
             N'Edycja własnego profilu (FullName, Email)',
             N'users'),
            (N'users.create',
             N'Tworzenie nowego konta użytkownika',
             N'users'),
            (N'users.edit',
             N'Edycja danych dowolnego użytkownika',
             N'users'),
            (N'users.delete',
             N'Usuwanie konta użytkownika (soft-delete, dwuetapowe)',
             N'users'),
            (N'users.lock',
             N'Ręczne zablokowanie konta użytkownika',
             N'users'),
            (N'users.unlock',
             N'Odblokowywanie zablokowanego konta',
             N'users'),
            (N'users.change_role',
             N'Zmiana roli przypisanej do użytkownika',
             N'users'),
            (N'users.view_audit',
             N'Historia działań konkretnego użytkownika w AuditLog',
             N'users'),

            -- =================================================================
            -- ROLES (7) — zarządzanie rolami
            -- =================================================================
            (N'roles.view_list',
             N'Lista wszystkich ról systemowych',
             N'roles'),
            (N'roles.view_details',
             N'Szczegóły roli wraz z przypisanymi uprawnieniami',
             N'roles'),
            (N'roles.create',
             N'Tworzenie nowej roli',
             N'roles'),
            (N'roles.edit',
             N'Edycja nazwy i opisu roli',
             N'roles'),
            (N'roles.delete',
             N'Usuwanie roli (soft-delete, dwuetapowe)',
             N'roles'),
            (N'roles.assign_to_user',
             N'Przypisywanie roli do użytkownika',
             N'roles'),
            (N'roles.view_users',
             N'Lista użytkowników z daną rolą',
             N'roles'),

            -- =================================================================
            -- PERMISSIONS (9) — zarządzanie uprawnieniami
            -- =================================================================
            (N'permissions.view_list',
             N'Lista wszystkich uprawnień systemowych',
             N'permissions'),
            (N'permissions.view_details',
             N'Szczegóły pojedynczego uprawnienia',
             N'permissions'),
            (N'permissions.create',
             N'Tworzenie nowego uprawnienia',
             N'permissions'),
            (N'permissions.edit',
             N'Edycja opisu uprawnienia',
             N'permissions'),
            (N'permissions.delete',
             N'Usuwanie uprawnienia (soft-delete)',
             N'permissions'),
            (N'permissions.assign_to_role',
             N'Przypisywanie uprawnienia do roli',
             N'permissions'),
            (N'permissions.revoke_from_role',
             N'Odbieranie uprawnienia z roli',
             N'permissions'),
            (N'permissions.view_matrix',
             N'Podgląd macierzy ról i uprawnień (GET /roles-permissions/matrix)',
             N'permissions'),
            (N'permissions.edit_matrix',
             N'Edycja całej macierzy uprawnień (PUT /roles-permissions/matrix)',
             N'permissions'),

            -- =================================================================
            -- DEBTORS (8) — dłużnicy z WAPRO (read-only)
            -- =================================================================
            (N'debtors.view_list',
             N'Lista dłużników z filtrami',
             N'debtors'),
            (N'debtors.view_details',
             N'Szczegóły dłużnika (adres, NIP)',
             N'debtors'),
            (N'debtors.view_invoices',
             N'Faktury dłużnika (VIEW_rozrachunki_faktur)',
             N'debtors'),
            (N'debtors.view_contact_data',
             N'Dane kontaktowe dłużnika (email, telefon)',
             N'debtors'),
            (N'debtors.view_debt_amount',
             N'Kwoty zadłużenia dłużnika',
             N'debtors'),
            (N'debtors.filter_advanced',
             N'Zaawansowane filtry listy dłużników (data, kwota, kategoria)',
             N'debtors'),
            (N'debtors.export',
             N'Eksport listy dłużników do pliku',
             N'debtors'),
            (N'debtors.view_monit_history',
             N'Historia monitów wysłanych do dłużnika',
             N'debtors'),

            -- =================================================================
            -- COMMENTS (6) — komentarze do kontrahentów
            -- =================================================================
            (N'comments.view',
             N'Podgląd komentarzy do kontrahenta',
             N'comments'),
            (N'comments.create',
             N'Dodawanie nowego komentarza',
             N'comments'),
            (N'comments.edit_own',
             N'Edycja własnego komentarza',
             N'comments'),
            (N'comments.edit_any',
             N'Edycja dowolnego komentarza (admin/manager)',
             N'comments'),
            (N'comments.delete_own',
             N'Usuwanie własnego komentarza (dwuetapowe)',
             N'comments'),
            (N'comments.delete_any',
             N'Usuwanie dowolnego komentarza (dwuetapowe, admin)',
             N'comments'),

            -- =================================================================
            -- MONITS (12) — wysyłka monitów
            -- =================================================================
            (N'monits.send_email_single',
             N'Wysłanie emaila do jednego dłużnika',
             N'monits'),
            (N'monits.send_email_bulk',
             N'Masowa wysyłka emaili do wielu dłużników (ARQ)',
             N'monits'),
            (N'monits.send_sms_single',
             N'Wysłanie SMS do jednego dłużnika',
             N'monits'),
            (N'monits.send_sms_bulk',
             N'Masowa wysyłka SMS (ARQ)',
             N'monits'),
            (N'monits.send_print_single',
             N'Generowanie PDF/druku dla jednego dłużnika',
             N'monits'),
            (N'monits.send_print_bulk',
             N'Masowe generowanie PDF/druku (ARQ)',
             N'monits'),
            (N'monits.schedule',
             N'Planowanie wysyłki na przyszłą datę',
             N'monits'),
            (N'monits.cancel',
             N'Anulowanie zaplanowanego monitu (status: pending)',
             N'monits'),
            (N'monits.view_history_own',
             N'Historia własnych wysłanych monitów',
             N'monits'),
            (N'monits.view_history_all',
             N'Historia wszystkich monitów (wszystkich użytkowników)',
             N'monits'),
            (N'monits.view_cost',
             N'Podgląd kosztów wysyłki SMS/email',
             N'monits'),
            (N'monits.retry',
             N'Ponowna próba wysyłki po błędzie (status: failed)',
             N'monits'),

            -- =================================================================
            -- PDF (4) — generowanie i szablony PDF
            -- =================================================================
            (N'pdf.generate',
             N'Generowanie pliku PDF (ReportLab blob)',
             N'pdf'),
            (N'pdf.download',
             N'Pobieranie wygenerowanego PDF',
             N'pdf'),
            (N'pdf.view_templates',
             N'Podgląd listy i zawartości szablonów PDF/email/SMS',
             N'pdf'),
            (N'pdf.manage_templates',
             N'Tworzenie, edycja i usuwanie szablonów',
             N'pdf'),

            -- =================================================================
            -- REPORTS (5) — raporty i statystyki
            -- =================================================================
            (N'reports.view_dashboard',
             N'Dashboard / podsumowanie statystyk windykacji',
             N'reports'),
            (N'reports.view_monit_stats',
             N'Szczegółowe statystyki monitów (wysłane, dostarczone, błędy)',
             N'reports'),
            (N'reports.view_debt_stats',
             N'Statystyki zadłużeń (suma, wiek, kategorie)',
             N'reports'),
            (N'reports.export_excel',
             N'Eksport raportu do pliku Excel',
             N'reports'),
            (N'reports.export_pdf',
             N'Eksport raportu do pliku PDF',
             N'reports'),

            -- =================================================================
            -- AUDIT (4) — dostęp do logów AuditLog
            -- =================================================================
            (N'audit.view_own',
             N'Podgląd własnych wpisów w AuditLog',
             N'audit'),
            (N'audit.view_all',
             N'Podgląd wszystkich wpisów AuditLog',
             N'audit'),
            (N'audit.view_system',
             N'Podgląd logów systemowych (schema_tamper, startup)',
             N'audit'),
            (N'audit.export',
             N'Eksport logów AuditLog do pliku',
             N'audit'),

            -- =================================================================
            -- SNAPSHOTS (4) — kopie zapasowe
            -- =================================================================
            (N'snapshots.view_list',
             N'Lista dostępnych snapshotów (pliki .json.gz)',
             N'snapshots'),
            (N'snapshots.create_manual',
             N'Ręczne wyzwolenie snapshotu (POST /system/snapshots)',
             N'snapshots'),
            (N'snapshots.restore',
             N'Przywracanie danych z snapshotu',
             N'snapshots'),
            (N'snapshots.delete',
             N'Usuwanie snapshotu (permanentne — ostrożnie)',
             N'snapshots'),

            -- =================================================================
            -- SYSTEM (5) — konfiguracja systemowa
            -- =================================================================
            (N'system.config_view',
             N'Podgląd kluczy konfiguracyjnych SystemConfig',
             N'system'),
            (N'system.config_edit',
             N'Edycja wartości konfiguracyjnych (PUT /system/config/{key})',
             N'system'),
            (N'system.cors_manage',
             N'Zarządzanie listą dozwolonych origins CORS',
             N'system'),
            (N'system.view_health',
             N'Dostęp do endpointu health check (/system/health)',
             N'system'),
            (N'system.schema_integrity_view',
             N'Podgląd stanu checksumów schematu (/system/schema-integrity)',
             N'system')

        ) AS src (PermissionName, Description, Category)
    ) AS source
        ON target.[PermissionName] = source.[PermissionName]
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ([PermissionName], [Description], [Category], [IsActive], [CreatedAt])
        VALUES (source.[PermissionName], source.[Description], source.[Category], 1, GETDATE());

    DECLARE @inserted INT = @@ROWCOUNT;
    PRINT 'Uprawnienia: ' + CAST(@inserted AS NVARCHAR) + ' wstawione (istniejące pominięte).';

    -- -------------------------------------------------------------------------
    -- Weryfikacja — podsumowanie per kategoria
    -- -------------------------------------------------------------------------
    PRINT '';
    PRINT 'Stan po seedzie:';
    SELECT
        [Category],
        COUNT(*) AS Ilosc,
        SUM(CASE WHEN [IsActive] = 1 THEN 1 ELSE 0 END) AS Aktywne
    FROM [dbo_ext].[Permissions]
    GROUP BY [Category]
    ORDER BY [Category];

    -- Sprawdzenie sumy — powinno być 83
    DECLARE @total INT = (SELECT COUNT(*) FROM [dbo_ext].[Permissions] WHERE [IsActive] = 1);
    PRINT '';
    PRINT 'Łącznie aktywnych uprawnień: ' + CAST(@total AS NVARCHAR);
    IF @total < 83
    BEGIN
        PRINT 'UWAGA: Oczekiwano 83 uprawnień, znaleziono ' + CAST(@total AS NVARCHAR) + '.';
        PRINT 'Sprawdź czy poprzednie seedy nie usunęły żadnych wpisów.';
    END

    COMMIT TRANSACTION;
    PRINT '=== SEED 02: Permissions — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    DECLARE @msg  NVARCHAR(2048) = ERROR_MESSAGE();
    DECLARE @line INT            = ERROR_LINE();
    DECLARE @sev  INT            = ERROR_SEVERITY();
    PRINT '=== SEED 02: BŁĄD ===';
    PRINT 'Linia:     ' + CAST(@line AS NVARCHAR);
    PRINT 'Wiadomość: ' + @msg;
    RAISERROR(@msg, @sev, 1);
END CATCH
GO