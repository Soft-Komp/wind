-- =============================================================================
-- database/migrations/022_skw_alert_log.sql
-- System Windykacja — Tabela historii alertów systemowych
--
-- Wykonaj w SSMS na bazie GPGKJASLO PRZED uruchomieniem alertmanagera.
-- Idempotentny — bezpieczny do ponownego wykonania.
--
-- Tabela: dbo_ext.skw_AlertLog
-- Cel: historia wszystkich alertów wysłanych przez Alert Manager
-- Reguła: immutable — tylko INSERT, brak UPDATE/DELETE (jak skw_AuditLog)
-- =============================================================================

USE GPGKJASLO;
GO

-- =============================================================================
-- TABELA: dbo_ext.skw_AlertLog
-- =============================================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'dbo_ext' AND t.name = 'skw_AlertLog'
)
BEGIN
    CREATE TABLE [dbo_ext].[skw_AlertLog] (
        -- Klucz główny
        [ID]              BIGINT          NOT NULL IDENTITY(1,1),

        -- Typ i poziom alertu
        [AlertType]       NVARCHAR(100)   NOT NULL,
        [Level]           NVARCHAR(20)    NOT NULL,  -- INFO / WARNING / SECURITY / CRITICAL

        -- Treść alertu
        [Title]           NVARCHAR(500)   NOT NULL,
        [Message]         NVARCHAR(4000)  NOT NULL,
        [Details]         NVARCHAR(MAX)   NULL,       -- JSON z danymi diagnostycznymi

        -- Dane email
        [EmailSent]       BIT             NOT NULL DEFAULT 0,
        [EmailRecipients] NVARCHAR(1000)  NULL,       -- lista emaili rozdzielona przecinkami
        [EmailError]      NVARCHAR(500)   NULL,       -- błąd wysyłki (jeśli EmailSent=0)

        -- Flagi
        [IsRecovery]      BIT             NOT NULL DEFAULT 0,  -- 1 = powiadomienie o odzyskaniu

        -- Identyfikacja
        [IncidentId]      NVARCHAR(36)    NOT NULL,   -- UUID z CheckResult.incident_id
        [CheckedAt]       DATETIME        NOT NULL,   -- kiedy checker wykonał sprawdzenie

        -- Audyt
        [CreatedAt]       DATETIME        NOT NULL DEFAULT GETDATE(),

        -- PK
        CONSTRAINT [PK_skw_AlertLog]
            PRIMARY KEY CLUSTERED ([ID] ASC),

        -- Ograniczenia
        CONSTRAINT [CK_skw_AlertLog_Level]
            CHECK ([Level] IN (N'INFO', N'WARNING', N'SECURITY', N'CRITICAL')),

        CONSTRAINT [CK_skw_AlertLog_AlertType]
            CHECK (LEN([AlertType]) > 0),
    );

    PRINT '✅ Tabela dbo_ext.skw_AlertLog utworzona.';
END
ELSE
BEGIN
    PRINT 'ℹ️  Tabela dbo_ext.skw_AlertLog już istnieje — pominięto.';
END
GO

-- =============================================================================
-- INDEKSY
-- =============================================================================

-- Szybkie filtrowanie po typie alertu (najczęstszy filtr)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_skw_AlertLog_AlertType'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_skw_AlertLog_AlertType]
        ON [dbo_ext].[skw_AlertLog] ([AlertType] ASC)
        INCLUDE ([Level], [EmailSent], [CreatedAt]);
    PRINT '✅ INDEX IX_skw_AlertLog_AlertType utworzony.';
END
GO

-- Szybkie filtrowanie po poziomie (np. pokaż tylko CRITICAL)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_skw_AlertLog_Level'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_skw_AlertLog_Level]
        ON [dbo_ext].[skw_AlertLog] ([Level] ASC, [CreatedAt] DESC);
    PRINT '✅ INDEX IX_skw_AlertLog_Level utworzony.';
END
GO

-- Szukanie po incident_id (debugowanie konkretnego incydentu)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_skw_AlertLog_IncidentId'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_skw_AlertLog_IncidentId]
        ON [dbo_ext].[skw_AlertLog] ([IncidentId] ASC);
    PRINT '✅ INDEX IX_skw_AlertLog_IncidentId utworzony.';
END
GO

-- Filtrowanie recovery vs problem
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_skw_AlertLog_IsRecovery'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_skw_AlertLog_IsRecovery]
        ON [dbo_ext].[skw_AlertLog] ([IsRecovery] ASC, [CreatedAt] DESC);
    PRINT '✅ INDEX IX_skw_AlertLog_IsRecovery utworzony.';
END
GO

-- Przeglądanie chronologiczne (domyślny widok historii)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_skw_AlertLog_CreatedAt'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_skw_AlertLog_CreatedAt]
        ON [dbo_ext].[skw_AlertLog] ([CreatedAt] DESC);
    PRINT '✅ INDEX IX_skw_AlertLog_CreatedAt utworzony.';
END
GO

-- =============================================================================
-- WERYFIKACJA
-- =============================================================================

SELECT
    'skw_AlertLog' AS tabela,
    COUNT(*) AS kolumn
FROM sys.columns c
JOIN sys.tables t ON c.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'dbo_ext' AND t.name = 'skw_AlertLog';

SELECT
    i.name AS indeks,
    i.type_desc AS typ,
    i.is_primary_key AS pk
FROM sys.indexes i
JOIN sys.tables t ON i.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'dbo_ext' AND t.name = 'skw_AlertLog';

PRINT '✅ SUKCES: skw_AlertLog gotowy do użycia.';
GO