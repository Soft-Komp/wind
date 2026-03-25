-- =============================================================================
-- SEED 05 — SystemConfig (8 kluczy konfiguracyjnych)
-- =============================================================================
--
-- IDEMPOTENTNY — MERGE:
--   INSERT jeśli klucz nie istnieje.
--   UPDATE jeśli klucz istnieje — ale NIE nadpisuje master_key.pin_hash
--   jeśli ma już niepustą wartość (chroni przed przypadkowym resetem).
--
-- ⚠️  master_key.pin_hash:
--     Po seedzie wartość = '' (pusty string — placeholder).
--     Uruchom: python database/setup.py --set-master-pin
--     Skrypt:
--       1. Pyta o PIN (bez echa terminala)
--       2. Hashuje bcrypt (rounds=12)
--       3. UPDATE SystemConfig SET ConfigValue=hash WHERE ConfigKey='master_key.pin_hash'
--     NIGDY nie commituj rzeczywistego hasha do repozytorium!
-- =============================================================================

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

PRINT '=== SEED 05: SystemConfig — START ===';
PRINT 'Czas: ' + CONVERT(NVARCHAR, GETDATE(), 120);

BEGIN TRANSACTION;
BEGIN TRY

    -- =========================================================================
    -- MERGE: 8 kluczy konfiguracyjnych
    -- Warunek UPDATE: nie nadpisuj master_key.pin_hash jeśli już ustawiony
    -- =========================================================================
    MERGE [dbo_ext].[skw_SystemConfig] AS target
    USING (
        SELECT ConfigKey, ConfigValue, Description FROM (VALUES

            (
                N'cors.allowed_origins',
                N'http://0.53:3000,http://localhost:3000',
                N'Lista dozwolonych origins CORS (przecinkami). Zmiana działa natychmiast po odświeżeniu cache Redis (TTL 5 min).'
            ),
            (
                N'otp.expiry_minutes',
                N'15',
                N'Czas ważności kodu OTP do resetu hasła (minuty). Zakres: 5–60.'
            ),
            (
                N'delete_token.ttl_seconds',
                N'60',
                N'Czas ważności tokenu potwierdzającego DELETE (sekundy). Po tym czasie token wygasa i operacja wymaga ponownego zainicjowania.'
            ),
            (
                N'impersonation.max_hours',
                N'4',
                N'Maksymalny czas trwania sesji impersonacji (godziny). Po tym czasie token impersonacji wygasa automatycznie.'
            ),
            (
                N'master_key.enabled',
                N'true',
                N'Czy dostęp przez Master Key jest aktywny (true/false). Wyłącz w środowiskach produkcyjnych gdy nie jest potrzebny.'
            ),
            (
                N'master_key.pin_hash',
                N'',
                N'Hash bcrypt (rounds=12) PIN-u do Master Key. WYMAGANE ustawienie przed produkcją: python database/setup.py --set-master-pin'
            ),
            (
                N'schema_integrity.reaction',
                N'BLOCK',
                N'Reakcja na wykrycie zmian schematu poza Alembic: WARN (log + kontynuuj) / ALERT (log + SSE) / BLOCK (SystemExit(1)).'
            ),
            (
                N'snapshot.retention_days',
                N'30',
                N'Liczba dni przechowywania snapshotów automatycznych. Starsze pliki usuwane przez ARQ cron podczas tworzenia nowego snapshotu.'
            )

        ) AS src (ConfigKey, ConfigValue, Description)
    ) AS source
        ON target.[ConfigKey] = source.[ConfigKey]
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ([ConfigKey], [ConfigValue], [Description], [IsActive], [CreatedAt])
        VALUES (source.[ConfigKey], source.[ConfigValue], source.[Description], 1, GETDATE())
    WHEN MATCHED THEN
        UPDATE SET
            -- Aktualizuj Description zawsze (może się zmienić w nowej wersji)
            [Description] = source.[Description],
            [UpdatedAt]   = GETDATE(),
            -- Aktualizuj ConfigValue tylko jeśli:
            -- (a) klucz NIE jest master_key.pin_hash (pin nigdy nie resetujemy)
            -- (b) LUB target.ConfigValue jest pusty (pierwsza inicjalizacja)
            [ConfigValue] = CASE
                WHEN target.[ConfigKey] = N'master_key.pin_hash'
                     AND LTRIM(RTRIM(target.[ConfigValue])) <> N''
                THEN target.[ConfigValue]   -- zachowaj istniejący hash PIN-u
                ELSE source.[ConfigValue]   -- aktualizuj pozostałe
            END;

    DECLARE @affected INT = @@ROWCOUNT;
    PRINT 'SystemConfig: ' + CAST(@affected AS NVARCHAR) + ' rekordów wstawionych/zaktualizowanych.';

    -- =========================================================================
    -- Weryfikacja — log stanu wszystkich kluczy
    -- =========================================================================
    PRINT '';
    PRINT 'Stan konfiguracji po seedzie:';
    SELECT
        [ConfigKey],
        -- Maskuj wartość PIN-u w logach
        CASE
            WHEN [ConfigKey] = N'master_key.pin_hash'
            THEN CASE
                     WHEN LTRIM(RTRIM([ConfigValue])) = N''
                     THEN N'[PUSTY — WYMAGANA KONFIGURACJA]'
                     ELSE N'[USTAWIONY — ' + CAST(LEN([ConfigValue]) AS NVARCHAR) + N' znaków]'
                 END
            ELSE [ConfigValue]
        END AS ConfigValue,
        [IsActive],
        CONVERT(NVARCHAR, [CreatedAt], 120) AS CreatedAt,
        CONVERT(NVARCHAR, [UpdatedAt], 120) AS UpdatedAt
    FROM [dbo_ext].[skw_SystemConfig]
    ORDER BY [ConfigKey];

    -- Ostrzeżenie jeśli pin_hash pusty
    DECLARE @pin_hash NVARCHAR(MAX);
    SET @pin_hash = (
        SELECT [ConfigValue]
        FROM [dbo_ext].[skw_SystemConfig]
        WHERE [ConfigKey] = N'master_key.pin_hash'
    );

    IF LTRIM(RTRIM(ISNULL(@pin_hash, N''))) = N''
    BEGIN
        PRINT '';
        PRINT '⚠️  UWAGA: master_key.pin_hash jest pusty!';
        PRINT '   Master Key nie będzie działał do czasu ustawienia PIN-u.';
        PRINT '   Uruchom: python database/setup.py --set-master-pin';
    END

    COMMIT TRANSACTION;
    PRINT '';
    PRINT '=== SEED 05: SystemConfig — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    DECLARE @msg  NVARCHAR(2048) = ERROR_MESSAGE();
    DECLARE @line INT            = ERROR_LINE();
    DECLARE @sev  INT            = ERROR_SEVERITY();
    PRINT '=== SEED 05: BŁĄD ===';
    PRINT 'Linia:     ' + CAST(@line AS NVARCHAR);
    PRINT 'Wiadomość: ' + @msg;
    RAISERROR(@msg, @sev, 1);
END CATCH
GO

INSERT INTO [dbo_ext].[skw_SystemConfig] 
    ([ConfigKey], [ConfigValue], [Description], [IsActive])
VALUES
    (
        'test_mode.enabled',
        'false',
        'Tryb testowy wysyłki — true = wszystkie email/SMS lecą na adresy testowe zamiast do dłużników.',
        1
    ),
    (
        'test_mode.email',
        '',
        'Testowy adres email — gdy test_mode.enabled=true, wszystkie maile lecą tutaj. Fallback z .env: TEST_MODE_EMAIL.',
        1
    ),
    (
        'test_mode.phone',
        '',
        'Testowy numer telefonu — gdy test_mode.enabled=true, wszystkie SMS lecą tutaj. Fallback z .env: TEST_MODE_PHONE.',
        1
    );
INSERT INTO [dbo_ext].[skw_SystemConfig]
    ([ConfigKey], [ConfigValue], [Description], [IsActive])
VALUES
    (
        'bcc.enabled',
        'false',
        'UDW — przełącznik (true/false). Gdy false — BCC nie jest dodawany niezależnie od bcc.emails.',
        1
    ),
    (
        'bcc.emails',
        '',
        'UDW — lista adresów email oddzielona przecinkami. Przykład: szef@firma.pl,archiwum@firma.pl. Fallback z .env: BCC_EMAILS.',
        1
    );