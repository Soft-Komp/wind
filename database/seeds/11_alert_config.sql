-- =============================================================================
-- database/seeds/06_alert_config.sql
-- System Windykacja — Klucze SystemConfig dla Alert Managera
--
-- Idempotentny MERGE — nigdy nie nadpisuje wartości admina,
-- tylko wstawia brakujące rekordy (konwencja projektu).
-- =============================================================================

USE GPGKJASLO;
GO

PRINT 'Seeder: 06_alert_config.sql — SystemConfig Alert Manager';
GO

MERGE [dbo_ext].[skw_SystemConfig] AS target
USING (VALUES
    -- ── Przełącznik główny ─────────────────────────────────────────────
    (
        N'alerts.enabled',
        N'true',
        N'Alert Manager: master switch. true = alerty aktywne, false = wyłączone. '
        + N'Wyłączenie zatrzymuje wysyłkę emaili, ale NIE zatrzymuje kontenera.'
    ),

    -- ── Odbiorcy alertów ───────────────────────────────────────────────
    (
        N'alerts.recipients',
        N'',
        N'Alert Manager: lista adresów email odbiorców alertów, przecinkami. '
        + N'Przykład: admin@gpgk.pl,it@gpgk.pl. '
        + N'Gdy puste — używany jest fallback ALERT_RECIPIENTS_FALLBACK z .env.'
    ),

    -- ── Throttling ─────────────────────────────────────────────────────
    (
        N'alerts.cooldown_minutes',
        N'15',
        N'Alert Manager: minimalny czas (w minutach) między powtórnym alertem '
        + N'tego samego typu. Zapobiega spamowaniu emailem podczas ciągłej awarii. '
        + N'Przykład: 15 = jeden email co 15 minut maksymalnie.'
    ),

    -- ── Brute-force ────────────────────────────────────────────────────
    (
        N'alerts.brute_force_threshold',
        N'10',
        N'Alert Manager: liczba błędnych prób logowania na jeden IP/konto '
        + N'po której wysyłany jest alert SECURITY. Domyślnie: 10.'
    ),

    -- ── Worker heartbeat ───────────────────────────────────────────────
    (
        N'alerts.worker_heartbeat_timeout_seconds',
        N'120',
        N'Alert Manager: jeśli klucz heartbeatu workera ARQ w Redis nie był '
        + N'odświeżony przez X sekund — worker uznany za martwy. Domyślnie: 120.'
    ),

    -- ── Latencja DB ────────────────────────────────────────────────────
    (
        N'alerts.db_latency_warn_ms',
        N'500',
        N'Alert Manager: czas odpowiedzi MSSQL powyżej którego wysyłany jest '
        + N'alert WARNING o wysokiej latencji. Wartość w milisekundach. '
        + N'Domyślnie: 500ms.'
    ),

    -- ── DLQ ────────────────────────────────────────────────────────────
    (
        N'alerts.dlq_overflow_threshold',
        N'10',
        N'Alert Manager: liczba nieudanych zadań w Dead Letter Queue (ARQ) '
        + N'powyżej której wysyłany jest alert WARNING. Domyślnie: 10.'
    ),

    -- ── Snapshot ───────────────────────────────────────────────────────
    (
        N'alerts.snapshot_expected_hour',
        N'3',
        N'Alert Manager: godzina UTC (0-23) o której powinien być wykonany '
        + N'snapshot dzienny. Jeśli snapshot nie nastąpił — alert WARNING. '
        + N'Domyślnie: 3 (3:00 UTC = 4:00 lub 5:00 CET/CEST).'
    )
) AS source ([ConfigKey], [ConfigValue], [Description])
ON target.[ConfigKey] = source.[ConfigKey]

-- Wstawiaj tylko NOWE klucze (NIE nadpisuj wartości ustawionych przez admina!)
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([ConfigKey], [ConfigValue], [Description], [IsActive], [CreatedAt])
    VALUES (source.[ConfigKey], source.[ConfigValue], source.[Description], 1, GETDATE())

-- Aktualizuj TYLKO opis (nie wartość!)
WHEN MATCHED THEN
    UPDATE SET [Description] = source.[Description];

PRINT 'SystemConfig (alert_config): ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' operacji MERGE.';
GO

-- =============================================================================
-- WERYFIKACJA
-- =============================================================================

SELECT
    [ConfigKey],
    [ConfigValue],
    [IsActive],
    [CreatedAt]
FROM [dbo_ext].[skw_SystemConfig]
WHERE [ConfigKey] LIKE 'alerts.%'
ORDER BY [ConfigKey];

PRINT '✅ Seeder 06_alert_config.sql — zakończony pomyślnie.';
GO