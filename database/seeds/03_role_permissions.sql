-- =============================================================================
-- SEED 03 — RolePermissions (macierz ról i uprawnień)
-- =============================================================================
-- Plik:    database/seeds/03_role_permissions.sql
-- Wersja:  1.0.0
-- Data:    2026-02-18
-- Zgodny:  USTALENIA_PROJEKTU v1.6 §5.5, §11
--
-- IDEMPOTENTNY — MERGE (DELETE+INSERT per rola → czysta podmiana macierzy).
-- Strategia: DELETE istniejących przypisań dla roli → INSERT nowych.
-- Dzięki temu ponowne uruchomienie = dokładna synchronizacja macierzy.
--
-- Wymaganie: 01_roles.sql + 02_permissions.sql muszą być wykonane wcześniej.
--
-- MACIERZ UPRAWNIEŃ:
-- ┌─────────────────────────────────┬───────┬─────────┬──────┬──────────┐
-- │ Kategoria                       │ Admin │ Manager │ User │ ReadOnly │
-- ├─────────────────────────────────┼───────┼─────────┼──────┼──────────┤
-- │ auth (8)                        │  ALL  │   5/8   │  3/8 │   3/8   │
-- │ users (11)                      │  ALL  │   2/11  │ 2/11 │   2/11  │
-- │ roles (7)                       │  ALL  │   2/7   │  0   │   0     │
-- │ permissions (9)                 │  ALL  │   2/9   │  0   │   0     │
-- │ debtors (8)                     │  ALL  │   8/8   │  5/8 │   5/8   │
-- │ comments (6)                    │  ALL  │   5/6   │  3/6 │   1/6   │
-- │ monits (12)                     │  ALL  │  10/12  │  3/12│   0     │
-- │ pdf (4)                         │  ALL  │   3/4   │  2/4 │   0     │
-- │ reports (5)                     │  ALL  │   4/5   │  1/5 │   2/5   │
-- │ audit (4)                       │  ALL  │   3/4   │  1/4 │   1/4   │
-- │ snapshots (4)                   │  ALL  │   2/4   │  0   │   0     │
-- │ system (5)                      │  ALL  │   1/5   │  0   │   0     │
-- └─────────────────────────────────┴───────┴─────────┴──────┴──────────┘
-- =============================================================================

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

PRINT '=== SEED 03: RolePermissions — START ===';
PRINT 'Czas: ' + CONVERT(NVARCHAR, GETDATE(), 120);

BEGIN TRANSACTION;
BEGIN TRY

    -- =========================================================================
    -- Pomocnicza procedura: INSERT przypisania jeśli nie istnieje
    -- =========================================================================
    -- Używamy CTE + MERGE dla każdej roli osobno — przejrzyste i audytowalne.

    -- =========================================================================
    -- ADMIN — pełne uprawnienia (wszystkie 83)
    -- =========================================================================
    PRINT 'Przypisuję uprawnienia roli: Admin';

    ;WITH admin_perms AS (
        SELECT [ID_PERMISSION]
        FROM [dbo_ext].[Permissions]
        WHERE [IsActive] = 1
    )
    MERGE [dbo_ext].[RolePermissions] AS target
    USING (
        SELECT r.[ID_ROLE], p.[ID_PERMISSION]
        FROM [dbo_ext].[Roles] r
        CROSS JOIN admin_perms p
        WHERE r.[RoleName] = N'Admin'
    ) AS source
        ON target.[ID_ROLE]       = source.[ID_ROLE]
       AND target.[ID_PERMISSION] = source.[ID_PERMISSION]
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ([ID_ROLE], [ID_PERMISSION], [CreatedAt])
        VALUES (source.[ID_ROLE], source.[ID_PERMISSION], GETDATE());

    PRINT '  Admin: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' nowych przypisań.';

    -- =========================================================================
    -- MANAGER — zarządzanie windykacją, bez wrażliwych operacji systemowych
    -- =========================================================================
    PRINT 'Przypisuję uprawnienia roli: Manager';

    ;WITH manager_perms AS (
        SELECT [ID_PERMISSION] FROM [dbo_ext].[Permissions]
        WHERE [PermissionName] IN (
            -- AUTH (5/8) — bez: reset_password_any, revoke_any_sessions, impersonate
            N'auth.login',
            N'auth.logout',
            N'auth.change_own_password',
            N'auth.view_own_sessions',
            N'auth.revoke_own_sessions',

            -- USERS (2/11) — tylko własny profil
            N'users.view_own_profile',
            N'users.edit_own_profile',

            -- ROLES (2/7) — tylko podgląd
            N'roles.view_list',
            N'roles.view_details',

            -- PERMISSIONS (2/9) — tylko podgląd macierzy
            N'permissions.view_list',
            N'permissions.view_matrix',

            -- DEBTORS (8/8) — pełny dostęp
            N'debtors.view_list',
            N'debtors.view_details',
            N'debtors.view_invoices',
            N'debtors.view_contact_data',
            N'debtors.view_debt_amount',
            N'debtors.filter_advanced',
            N'debtors.export',
            N'debtors.view_monit_history',

            -- COMMENTS (5/6) — bez: delete_any (dwuetapowe — tylko admin)
            N'comments.view',
            N'comments.create',
            N'comments.edit_own',
            N'comments.edit_any',
            N'comments.delete_own',

            -- MONITS (10/12) — bez: schedule (zaawansowane), retry (techniczne)
            N'monits.send_email_single',
            N'monits.send_email_bulk',
            N'monits.send_sms_single',
            N'monits.send_sms_bulk',
            N'monits.send_print_single',
            N'monits.send_print_bulk',
            N'monits.cancel',
            N'monits.view_history_own',
            N'monits.view_history_all',
            N'monits.view_cost',

            -- PDF (3/4) — bez: manage_templates
            N'pdf.generate',
            N'pdf.download',
            N'pdf.view_templates',

            -- REPORTS (4/5) — bez: export_pdf (systemowe)
            N'reports.view_dashboard',
            N'reports.view_monit_stats',
            N'reports.view_debt_stats',
            N'reports.export_excel',

            -- AUDIT (3/4) — bez: export
            N'audit.view_own',
            N'audit.view_all',
            N'audit.view_system',

            -- SNAPSHOTS (2/4) — podgląd i tworzenie; bez restore i delete
            N'snapshots.view_list',
            N'snapshots.create_manual',

            -- SYSTEM (1/5) — tylko health check
            N'system.view_health'
        )
        AND [IsActive] = 1
    )
    MERGE [dbo_ext].[RolePermissions] AS target
    USING (
        SELECT r.[ID_ROLE], p.[ID_PERMISSION]
        FROM [dbo_ext].[Roles] r
        CROSS JOIN manager_perms p
        WHERE r.[RoleName] = N'Manager'
    ) AS source
        ON target.[ID_ROLE]       = source.[ID_ROLE]
       AND target.[ID_PERMISSION] = source.[ID_PERMISSION]
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ([ID_ROLE], [ID_PERMISSION], [CreatedAt])
        VALUES (source.[ID_ROLE], source.[ID_PERMISSION], GETDATE());

    PRINT '  Manager: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' nowych przypisań.';

    -- =========================================================================
    -- USER — podstawowy pracownik biurowy
    -- =========================================================================
    PRINT 'Przypisuję uprawnienia roli: User';

    ;WITH user_perms AS (
        SELECT [ID_PERMISSION] FROM [dbo_ext].[Permissions]
        WHERE [PermissionName] IN (
            -- AUTH (3/8)
            N'auth.login',
            N'auth.logout',
            N'auth.change_own_password',
            N'auth.view_own_sessions',
            N'auth.revoke_own_sessions',

            -- USERS — tylko własny profil
            N'users.view_own_profile',
            N'users.edit_own_profile',

            -- DEBTORS (5/8) — bez export, filter_advanced, view_monit_history
            N'debtors.view_list',
            N'debtors.view_details',
            N'debtors.view_invoices',
            N'debtors.view_contact_data',
            N'debtors.view_debt_amount',

            -- COMMENTS (3/6) — tylko własne
            N'comments.view',
            N'comments.create',
            N'comments.edit_own',
            N'comments.delete_own',

            -- MONITS (3/12) — tylko pojedyncze wysyłki
            N'monits.send_email_single',
            N'monits.send_sms_single',
            N'monits.send_print_single',
            N'monits.view_history_own',

            -- PDF (2/4)
            N'pdf.generate',
            N'pdf.download',

            -- REPORTS (1/5) — tylko dashboard
            N'reports.view_dashboard',

            -- AUDIT (1/4) — tylko własne
            N'audit.view_own'
        )
        AND [IsActive] = 1
    )
    MERGE [dbo_ext].[RolePermissions] AS target
    USING (
        SELECT r.[ID_ROLE], p.[ID_PERMISSION]
        FROM [dbo_ext].[Roles] r
        CROSS JOIN user_perms p
        WHERE r.[RoleName] = N'User'
    ) AS source
        ON target.[ID_ROLE]       = source.[ID_ROLE]
       AND target.[ID_PERMISSION] = source.[ID_PERMISSION]
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ([ID_ROLE], [ID_PERMISSION], [CreatedAt])
        VALUES (source.[ID_ROLE], source.[ID_PERMISSION], GETDATE());

    PRINT '  User: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' nowych przypisań.';

    -- =========================================================================
    -- READONLY — tylko podgląd, zero mutacji
    -- =========================================================================
    PRINT 'Przypisuję uprawnienia roli: ReadOnly';

    ;WITH readonly_perms AS (
        SELECT [ID_PERMISSION] FROM [dbo_ext].[Permissions]
        WHERE [PermissionName] IN (
            -- AUTH (3/8) — tylko własne
            N'auth.login',
            N'auth.logout',
            N'auth.change_own_password',
            N'auth.view_own_sessions',
            N'auth.revoke_own_sessions',

            -- USERS — tylko własny profil (bez edycji)
            N'users.view_own_profile',

            -- DEBTORS (5/8) — tylko widok
            N'debtors.view_list',
            N'debtors.view_details',
            N'debtors.view_invoices',
            N'debtors.view_contact_data',
            N'debtors.view_debt_amount',

            -- COMMENTS — tylko podgląd
            N'comments.view',

            -- REPORTS (2/5) — tylko podgląd
            N'reports.view_dashboard',
            N'reports.view_debt_stats',

            -- AUDIT (1/4) — tylko własne
            N'audit.view_own'
        )
        AND [IsActive] = 1
    )
    MERGE [dbo_ext].[RolePermissions] AS target
    USING (
        SELECT r.[ID_ROLE], p.[ID_PERMISSION]
        FROM [dbo_ext].[Roles] r
        CROSS JOIN readonly_perms p
        WHERE r.[RoleName] = N'ReadOnly'
    ) AS source
        ON target.[ID_ROLE]       = source.[ID_ROLE]
       AND target.[ID_PERMISSION] = source.[ID_PERMISSION]
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ([ID_ROLE], [ID_PERMISSION], [CreatedAt])
        VALUES (source.[ID_ROLE], source.[ID_PERMISSION], GETDATE());

    PRINT '  ReadOnly: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' nowych przypisań.';

    -- =========================================================================
    -- Weryfikacja — podsumowanie macierzy po operacji
    -- =========================================================================
    PRINT '';
    PRINT 'Macierz po seedzie:';
    SELECT
        r.[RoleName],
        COUNT(rp.[ID_PERMISSION]) AS LiczbaUprawnienia
    FROM [dbo_ext].[Roles] r
    LEFT JOIN [dbo_ext].[RolePermissions] rp ON r.[ID_ROLE] = rp.[ID_ROLE]
    GROUP BY r.[RoleName]
    ORDER BY r.[RoleName];

    COMMIT TRANSACTION;
    PRINT '=== SEED 03: RolePermissions — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    DECLARE @msg  NVARCHAR(2048) = ERROR_MESSAGE();
    DECLARE @line INT            = ERROR_LINE();
    DECLARE @sev  INT            = ERROR_SEVERITY();
    PRINT '=== SEED 03: BŁĄD ===';
    PRINT 'Linia:     ' + CAST(@line AS NVARCHAR);
    PRINT 'Wiadomość: ' + @msg;
    RAISERROR(@msg, @sev, 1);
END CATCH
GO