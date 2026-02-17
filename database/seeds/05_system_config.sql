-- ============================================================
-- Seed: domyślna konfiguracja SystemConfig
-- Wszystkie klucze opisane w USTALENIA_PROJEKTU v1.5
--
-- WAŻNE — logika MERGE:
--   ConfigValue  → INSERT przy nowym kluczu / NIE nadpisuje istniejącej
--   Description  → zawsze aktualizowana (opis może się zmienić w projekcie)
--
-- Idempotentny — bezpieczny do wielokrotnego uruchomienia
-- ============================================================

USE [WAPRO];
GO

MERGE dbo_ext.SystemConfig AS target
USING (VALUES
    (
        N'cors.allowed_origins',
        N'http://0.53:3000,http://localhost:3000',
        N'Biała lista domen CORS. Wartości oddzielone przecinkiem. Zarządzaj przez endpoint /system/cors.'
    ),
    (
        N'otp.expiry_minutes',
        N'15',
        N'TTL kodu OTP w minutach. Dotyczy: password_reset i 2fa.'
    ),
    (
        N'delete_token.ttl_seconds',
        N'60',
        N'TTL tokenu potwierdzającego usunięcie (dwuetapowe DELETE). Wartość w sekundach.'
    ),
    (
        N'impersonation.max_hours',
        N'4',
        N'Maksymalny czas trwania sesji impersonacji w godzinach. Uprawnienie: auth.impersonate.'
    ),
    (
        N'master_key.enabled',
        N'true',
        N'Czy MASTER_KEY jest aktywny. false = dostęp serwisowy wyłączony bez restartu.'
    ),
    (
        N'master_key.pin_hash',
        N'',
        N'Hash argon2 PINu wymaganego przy użyciu MASTER_KEY. Puste = PIN nieaktywny. NIGDY nie wpisuj plain PINu.'
    ),
    (
        N'schema_integrity.reaction',
        N'BLOCK',
        N'Reakcja na niezgodność checksumu schematu przy starcie. Wartości: WARN / ALERT / BLOCK.'
    ),
    (
        N'snapshot.retention_days',
        N'30',
        N'Liczba dni przechowywania snapshotów dbo_ext. Starsze usuwa cron.'
    )
) AS source (ConfigKey, ConfigValue, Description)
ON target.ConfigKey = source.ConfigKey
WHEN NOT MATCHED THEN
    INSERT (ConfigKey, ConfigValue, Description, IsActive, CreatedAt)
    VALUES (source.ConfigKey, source.ConfigValue, source.Description, 1, GETDATE())
WHEN MATCHED THEN
    -- Aktualizuj TYLKO opis — nie nadpisuj wartości zmienionej przez admina
    UPDATE SET Description = source.Description;
GO

PRINT 'Seed 05 zakończony. 8 kluczy konfiguracyjnych w SystemConfig.';
PRINT '';
PRINT 'UWAGA: master_key.pin_hash jest pusty.';
PRINT 'Jeśli PIN jest wymagany, ustaw go ręcznie:';
PRINT '  UPDATE dbo_ext.SystemConfig';
PRINT '    SET ConfigValue = ''<argon2_hash_pinu>''';
PRINT '  WHERE ConfigKey   = ''master_key.pin_hash'';';
GO