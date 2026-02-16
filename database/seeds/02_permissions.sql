-- ============================================================
-- Seed: wszystkie uprawnienia systemu
-- Format: kategoria.akcja | Idempotentny (MERGE)
-- ============================================================

USE [WAPRO];
GO

MERGE dbo_ext.Permissions AS target
USING (VALUES
    -- AUTH (8 uprawnień)
    (N'auth.login',               N'Logowanie do systemu',                          N'auth'),
    (N'auth.logout',              N'Wylogowanie z systemu',                          N'auth'),
    (N'auth.change_own_password', N'Zmiana własnego hasła',                          N'auth'),
    (N'auth.reset_password_any',  N'Reset hasła dowolnego użytkownika',              N'auth'),
    (N'auth.view_own_sessions',   N'Podgląd własnych aktywnych sesji',               N'auth'),
    (N'auth.revoke_own_sessions', N'Unieważnianie własnych sesji',                   N'auth'),
    (N'auth.revoke_any_sessions', N'Unieważnianie sesji dowolnego użytkownika',      N'auth'),
    (N'auth.impersonate',         N'Logowanie w kontekście innego użytkownika',      N'auth'),

    -- USERS (11 uprawnień)
    (N'users.view_list',          N'Lista użytkowników systemu',                     N'users'),
    (N'users.view_details',       N'Szczegóły konkretnego użytkownika',              N'users'),
    (N'users.view_own_profile',   N'Podgląd własnego profilu',                       N'users'),
    (N'users.edit_own_profile',   N'Edycja własnego profilu',                        N'users'),
    (N'users.create',             N'Tworzenie nowego użytkownika',                   N'users'),
    (N'users.edit',               N'Edycja danych użytkownika',                      N'users'),
    (N'users.delete',             N'Usuwanie użytkownika (soft-delete)',              N'users'),
    (N'users.lock',               N'Blokowanie konta użytkownika',                   N'users'),
    (N'users.unlock',             N'Odblokowywanie konta użytkownika',               N'users'),
    (N'users.change_role',        N'Zmiana roli przypisanej do użytkownika',         N'users'),
    (N'users.view_audit',         N'Historia działań użytkownika w AuditLog',        N'users'),

    -- ROLES (7 uprawnień)
    (N'roles.view_list',          N'Lista ról systemu',                              N'roles'),
    (N'roles.view_details',       N'Szczegóły roli z listą uprawnień',               N'roles'),
    (N'roles.create',             N'Tworzenie nowej roli',                           N'roles'),
    (N'roles.edit',               N'Edycja nazwy i opisu roli',                      N'roles'),
    (N'roles.delete',             N'Usuwanie roli (tylko jeśli brak użytkowników)',  N'roles'),
    (N'roles.assign_to_user',     N'Przypisywanie roli do użytkownika',              N'roles'),
    (N'roles.view_users',         N'Lista użytkowników z daną rolą',                 N'roles'),

    -- PERMISSIONS (9 uprawnień)
    (N'permissions.view_list',      N'Lista wszystkich uprawnień',                   N'permissions'),
    (N'permissions.view_details',   N'Szczegóły uprawnienia',                        N'permissions'),
    (N'permissions.create',         N'Tworzenie nowego uprawnienia',                 N'permissions'),
    (N'permissions.edit',           N'Edycja opisu uprawnienia',                     N'permissions'),
    (N'permissions.delete',         N'Usuwanie uprawnienia',                         N'permissions'),
    (N'permissions.assign_to_role', N'Przypisywanie uprawnień do roli',              N'permissions'),
    (N'permissions.revoke_from_role',N'Odbieranie uprawnień z roli',                 N'permissions'),
    (N'permissions.view_matrix',    N'Podgląd macierzy ról i uprawnień',             N'permissions'),
    (N'permissions.edit_matrix',    N'Edycja całej macierzy uprawnień',              N'permissions'),

    -- DEBTORS (8 uprawnień)
    (N'debtors.view_list',          N'Lista dłużników (zagregowana po kontrahencie)',N'debtors'),
    (N'debtors.view_details',       N'Szczegóły dłużnika',                           N'debtors'),
    (N'debtors.view_invoices',      N'Faktury dłużnika z WAPRO',                     N'debtors'),
    (N'debtors.view_contact_data',  N'Dane kontaktowe dłużnika (email, telefon)',     N'debtors'),
    (N'debtors.view_debt_amount',   N'Kwoty zadłużenia',                             N'debtors'),
    (N'debtors.filter_advanced',    N'Zaawansowane filtry (min_debt, last_contact)',  N'debtors'),
    (N'debtors.export',             N'Eksport listy dłużników',                      N'debtors'),
    (N'debtors.view_monit_history', N'Historia monitów dla dłużnika',                N'debtors'),

    -- COMMENTS (6 uprawnień)
    (N'comments.view',              N'Podgląd komentarzy do dłużnika',               N'comments'),
    (N'comments.create',            N'Dodawanie komentarza',                         N'comments'),
    (N'comments.edit_own',          N'Edycja własnego komentarza',                   N'comments'),
    (N'comments.edit_any',          N'Edycja dowolnego komentarza',                  N'comments'),
    (N'comments.delete_own',        N'Usuwanie własnego komentarza',                 N'comments'),
    (N'comments.delete_any',        N'Usuwanie dowolnego komentarza',                N'comments'),

    -- MONITS (12 uprawnień)
    (N'monits.send_email_single',   N'Email do jednego dłużnika',                    N'monits'),
    (N'monits.send_email_bulk',     N'Masowa wysyłka email',                         N'monits'),
    (N'monits.send_sms_single',     N'SMS do jednego dłużnika',                      N'monits'),
    (N'monits.send_sms_bulk',       N'Masowa wysyłka SMS',                           N'monits'),
    (N'monits.send_print_single',   N'PDF/druk dla jednego dłużnika',                N'monits'),
    (N'monits.send_print_bulk',     N'Masowy PDF/druk',                              N'monits'),
    (N'monits.schedule',            N'Planowanie wysyłki monitów',                   N'monits'),
    (N'monits.cancel',              N'Anulowanie zaplanowanego monitu',               N'monits'),
    (N'monits.view_history_own',    N'Historia własnych wysłanych monitów',           N'monits'),
    (N'monits.view_history_all',    N'Historia wszystkich monitów w systemie',        N'monits'),
    (N'monits.view_cost',           N'Podgląd kosztów wysyłki',                      N'monits'),
    (N'monits.retry',               N'Ponowna próba wysyłki po błędzie',             N'monits'),

    -- PDF (4 uprawnienia)
    (N'pdf.generate',               N'Generowanie PDF wezwania do zapłaty',          N'pdf'),
    (N'pdf.download',               N'Pobieranie wygenerowanego PDF',                N'pdf'),
    (N'pdf.view_templates',         N'Podgląd szablonów PDF',                        N'pdf'),
    (N'pdf.manage_templates',       N'Zarządzanie szablonami PDF',                   N'pdf'),

    -- REPORTS (5 uprawnień)
    (N'reports.view_dashboard',     N'Dashboard z KPI windykacji',                   N'reports'),
    (N'reports.view_monit_stats',   N'Statystyki wysłanych monitów',                 N'reports'),
    (N'reports.view_debt_stats',    N'Statystyki zadłużeń kontrahentów',             N'reports'),
    (N'reports.export_excel',       N'Eksport danych do Excel',                      N'reports'),
    (N'reports.export_pdf',         N'Eksport raportów do PDF',                      N'reports'),

    -- AUDIT (4 uprawnienia)
    (N'audit.view_own',             N'Własne logi aktywności',                       N'audit'),
    (N'audit.view_all',             N'Wszystkie logi aktywności użytkowników',       N'audit'),
    (N'audit.view_system',          N'Logi systemowe (startup, cron, schema)',        N'audit'),
    (N'audit.export',               N'Eksport logów audytu',                         N'audit'),

    -- SNAPSHOTS (4 uprawnienia)
    (N'snapshots.view_list',        N'Lista dostępnych snapshotów',                  N'snapshots'),
    (N'snapshots.create_manual',    N'Ręczne tworzenie snapshotu',                   N'snapshots'),
    (N'snapshots.restore',          N'Przywracanie danych ze snapshotu',             N'snapshots'),
    (N'snapshots.delete',           N'Usuwanie snapshotu (nieodwracalne)',            N'snapshots'),

    -- SYSTEM (5 uprawnień)
    (N'system.config_view',         N'Podgląd konfiguracji SystemConfig',            N'system'),
    (N'system.config_edit',         N'Edycja konfiguracji SystemConfig',             N'system'),
    (N'system.cors_manage',         N'Zarządzanie białą listą CORS',                 N'system'),
    (N'system.view_health',         N'Health check i status systemu',                N'system'),
    (N'system.schema_integrity_view',N'Podgląd stanu checksumów schematu DB',        N'system')

) AS source (PermissionName, Description, Category)
ON target.PermissionName = source.PermissionName
WHEN NOT MATCHED THEN
    INSERT (PermissionName, Description, Category, IsActive, CreatedAt)
    VALUES (source.PermissionName, source.Description, source.Category, 1, GETDATE())
WHEN MATCHED THEN
    UPDATE SET
        Description = source.Description,
        Category    = source.Category;
GO

PRINT 'Seed uprawnień zakończony. Łącznie: 83 uprawnienia w 12 kategoriach.';
GO