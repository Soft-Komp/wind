-- =============================================================================
-- database/seeds/12_cache_bypass_seed.sql
-- Seed: klucz cache.bypass_enabled w skw_SystemConfig
--
-- Zgodny z wzorcem MERGE NOT MATCHED — nie nadpisuje wartości admina.
-- Uruchamiany przez migrację Alembic (w łańcuchu po 0009).
-- =============================================================================

MERGE [dbo_ext].[skw_SystemConfig] AS [target]
USING (
    VALUES (
        N'cache.bypass_enabled',
        N'false',
        N'Globalny bypass cache Redis. Gdy true — wszystkie serwisy czytają '
        N'dane bezpośrednio z bazy (konfiguracja, monity, CORS). '
        N'Kolejki ARQ i SSE Pub/Sub pozostają nienaruszone. '
        N'Zmiana działa w ciągu 5 sekund bez restartu.',
        1
    )
) AS [source] ([ConfigKey], [ConfigValue], [Description], [IsActive])
ON [target].[ConfigKey] = [source].[ConfigKey]
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([ConfigKey], [ConfigValue], [Description], [IsActive])
    VALUES ([source].[ConfigKey], [source].[ConfigValue],
            [source].[Description], [source].[IsActive])
WHEN MATCHED THEN
    -- Celowo aktualizujemy TYLKO opis — wartość admina nigdy nie jest nadpisywana
    UPDATE SET [Description] = [source].[Description];