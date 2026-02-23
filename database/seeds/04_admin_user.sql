-- =============================================================================
-- SEED 04 — Admin User (domyślny administrator systemu)
-- =============================================================================
-- Plik:    database/seeds/04_admin_user.sql
-- Wersja:  1.0.0
-- Data:    2026-02-18
-- Zgodny:  USTALENIA_PROJEKTU v1.6 §5.4, TABELE_REFERENCJA v1.0
--
-- IDEMPOTENTNY — MERGE INSERT only (nie nadpisuje istniejącego admina).
-- Jeśli użytkownik 'admin' istnieje — pomija wstawienie.
--
-- ⚠️  KRYTYCZNE BEZPIECZEŃSTWO:
--     PasswordHash poniżej to PLACEHOLDER — musi być zastąpiony przed produkcją!
--     Uruchom: python database/setup.py --set-admin-password
--     Skrypt pobierze hasło z .env ADMIN_INITIAL_PASSWORD i haszuje argon2id.
--
-- Wymagania: 01_roles.sql musi być wykonany wcześniej (potrzebne ID_ROLE).
-- =============================================================================

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

PRINT '=== SEED 04: Admin User — START ===';
PRINT 'Czas: ' + CONVERT(NVARCHAR, GETDATE(), 120);

BEGIN TRANSACTION;
BEGIN TRY

    -- =========================================================================
    -- Walidacja zależności — Roles musi istnieć i mieć rolę Admin
    -- =========================================================================
    DECLARE @admin_role_id INT;
    SET @admin_role_id = (
        SELECT [ID_ROLE]
        FROM [dbo_ext].[Roles]
        WHERE [RoleName] = N'Admin'
          AND [IsActive] = 1
    );

    IF @admin_role_id IS NULL
    BEGIN
        RAISERROR(
            N'BŁĄD: Rola Admin nie istnieje w tabeli Roles. Uruchom najpierw 01_roles.sql.',
            16, 1
        );
    END

    PRINT 'Rola Admin: ID_ROLE = ' + CAST(@admin_role_id AS NVARCHAR);

    -- =========================================================================
    -- MERGE: wstaw admina jeśli nie istnieje
    -- =========================================================================
    -- ⚠️  PasswordHash = PLACEHOLDER (argon2id format, ale NIEPRAWIDŁOWE hasło)
    --     Wartość '$argon2id$v=19$placeholder...' NIE jest prawidłowym hashem.
    --     MUSI zostać zastąpiona przez: python database/setup.py --set-admin-password
    --
    --     Jeśli potrzebujesz tymczasowego hasła do testów DEV:
    --     1. python -c "from argon2 import PasswordHasher; print(PasswordHasher().hash('Admin123!'))"
    --     2. Zastąp wartość RĘCZNIE poniżej (NIE commituj do repozytorium!)
    -- =========================================================================
    MERGE [dbo_ext].[Users] AS target
    USING (
        SELECT
            N'admin'                               AS Username,
            N'admin@windykacja.local'              AS Email,
            N'$argon2id$v=19$PLACEHOLDER_REPLACE_BEFORE_PRODUCTION'
                                                   AS PasswordHash,
            N'Administrator Systemu'               AS FullName,
            1                                      AS IsActive,
            @admin_role_id                         AS RoleID
    ) AS source
        ON target.[Username] = source.[Username]
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            [Username], [Email], [PasswordHash], [FullName],
            [IsActive], [RoleID], [CreatedAt],
            [FailedLoginAttempts]
        )
        VALUES (
            source.[Username], source.[Email], source.[PasswordHash], source.[FullName],
            source.[IsActive], source.[RoleID], GETDATE(),
            0
        );

    DECLARE @inserted INT = @@ROWCOUNT;

    IF @inserted = 1
    BEGIN
        PRINT 'Admin: konto utworzone.';
        PRINT '⚠️  WAŻNE: Uruchom setup.py --set-admin-password przed produkcją!';
    END
    ELSE
    BEGIN
        PRINT 'Admin: konto już istnieje — pominięto (PasswordHash niezmieniony).';
    END

    -- =========================================================================
    -- Weryfikacja stanu konta
    -- =========================================================================
    SELECT
        u.[ID_USER],
        u.[Username],
        u.[Email],
        u.[FullName],
        u.[IsActive],
        r.[RoleName],
        CONVERT(NVARCHAR, u.[CreatedAt], 120) AS CreatedAt,
        CASE
            WHEN u.[PasswordHash] LIKE N'$argon2id$v=19$PLACEHOLDER%'
            THEN N'⚠️  PLACEHOLDER — wymagana zmiana!'
            ELSE N'✅ Hash ustawiony'
        END AS PasswordStatus
    FROM [dbo_ext].[Users] u
    JOIN [dbo_ext].[Roles]  r ON u.[RoleID] = r.[ID_ROLE]
    WHERE u.[Username] = N'admin';

    COMMIT TRANSACTION;
    PRINT '=== SEED 04: Admin User — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    DECLARE @msg  NVARCHAR(2048) = ERROR_MESSAGE();
    DECLARE @line INT            = ERROR_LINE();
    DECLARE @sev  INT            = ERROR_SEVERITY();
    PRINT '=== SEED 04: BŁĄD ===';
    PRINT 'Linia:     ' + CAST(@line AS NVARCHAR);
    PRINT 'Wiadomość: ' + @msg;
    RAISERROR(@msg, @sev, 1);
END CATCH
GO