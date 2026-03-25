-- ============================================================
-- database/ddl/010_system_config.sql
-- Tabela: dbo_ext.skw_SystemConfig
--
-- Dynamiczna konfiguracja aplikacji.
-- Cachowana w Redis (TTL: 5 minut). Zmiana działa bez restartu.
-- ConfigValue: JSON lub plain string.
-- Seed: database/seeds/05_system_config.sql → 8 kluczy
--
-- Klucze konfiguracyjne:
--   cors.allowed_origins      → "http://0.53:3000,http://localhost:3000"
--   otp.expiry_minutes        → "15"
--   delete_token.ttl_seconds  → "60"
--   impersonation.max_hours   → "4"
--   master_key.enabled        → "true"
--   master_key.pin_hash       → "" (wypełnić ręcznie — argon2 hash)
--   schema_integrity.reaction → "BLOCK"   (WARN / ALERT / BLOCK)
--   snapshot.retention_days   → "30"
-- ============================================================

USE [WAPRO];
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

BEGIN TRANSACTION;
BEGIN TRY

    IF NOT EXISTS (
        SELECT 1
        FROM   sys.tables  t
        JOIN   sys.schemas s ON t.schema_id = s.schema_id
        WHERE  s.name = 'dbo_ext'
          AND  t.name = 'skw_SystemConfig'
    )
    BEGIN
        PRINT '[010] Tworzenie tabeli dbo_ext.skw_SystemConfig...';

        CREATE TABLE [dbo_ext].[skw_SystemConfig] (

            -- ── Klucz główny ──────────────────────────────────────────────────
            [ID_CONFIG]    INT            IDENTITY(1,1)  NOT NULL,

            -- ── Klucz konfiguracji ───────────────────────────────────────────
            [ConfigKey]    NVARCHAR(100)                 NOT NULL,

            -- ── Wartość (JSON lub plain string) ───────────────────────────────
            [ConfigValue]  NVARCHAR(MAX)                     NULL,

            -- ── Opis ─────────────────────────────────────────────────────────
            [Description]  NVARCHAR(500)                     NULL,

            -- ── Status ───────────────────────────────────────────────────────
            [IsActive]     BIT                           NOT NULL
                           CONSTRAINT [DF_skw_SystemConfig_IsActive]  DEFAULT (1),

            -- ── Timestampy ───────────────────────────────────────────────────
            [CreatedAt]    DATETIME                      NOT NULL
                           CONSTRAINT [DF_skw_SystemConfig_CreatedAt] DEFAULT (GETDATE()),
            [UpdatedAt]    DATETIME                          NULL,

            -- ── Constraints ──────────────────────────────────────────────────
            CONSTRAINT [PK_skw_SystemConfig]
                PRIMARY KEY CLUSTERED ([ID_CONFIG] ASC),

            CONSTRAINT [UQ_skw_SystemConfig_ConfigKey]
                UNIQUE ([ConfigKey])
        );

        PRINT '[010] Tabela dbo_ext.skw_SystemConfig utworzona.';
    END
    ELSE
    BEGIN
        PRINT '[010] Tabela dbo_ext.skw_SystemConfig już istnieje — pominięto.';
    END

    -- ── Indeksy ───────────────────────────────────────────────────────────────

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_SystemConfig')
          AND  name      = 'IX_skw_SystemConfig_IsActive'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_SystemConfig_IsActive]
            ON [dbo_ext].[skw_SystemConfig] ([IsActive] ASC, [ConfigKey] ASC);
        PRINT '[010] Indeks IX_skw_SystemConfig_IsActive utworzony.';
    END

    COMMIT TRANSACTION;
    PRINT '[010] === DDL 010: skw_SystemConfig — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT '[010] BŁĄD: ' + ERROR_MESSAGE();
    THROW;
END CATCH
GO