-- =============================================================================
-- INDEX: IX_Mon_Kontrahent_Historia
-- =============================================================================
-- Plik:    database/indexes/IX_Mon_Kontrahent_Historia.sql
-- Wersja:  1.0.0
-- Data:    2026-02-18
-- Zgodny:  AUDIT_ZGODNOSCI.md §R9, USTALENIA_PROJEKTU v1.6
--
-- Cel: Optymalizacja endpointu GET /debtors/{id}/monit-history.
-- Pokrywa: historia monitów dla kontrahenta malejąco po dacie.
--
-- Tabela docelowa: dbo_ext.MonitHistory (własna — CRUD przez ORM)
-- Alembic: zarządzany przez migrację 002_add_wapro_performance_indexes.py
-- =============================================================================

USE [WAPRO];
GO

PRINT 'Tworzenie indeksu IX_Mon_Kontrahent_Historia...';

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[dbo_ext].[MonitHistory]')
      AND name = N'IX_Mon_Kontrahent_Historia'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Mon_Kontrahent_Historia]
        ON [dbo_ext].[MonitHistory] (
            [ID_KONTRAHENTA] ASC,
            [CreatedAt]      DESC
        )
        INCLUDE (
            [MonitType],
            [Status],
            [ID_USER],
            [Recipient],
            [TotalDebt],
            [IsActive]
        )
        WHERE [IsActive] = 1
    WITH (
        FILLFACTOR             = 85,
        ONLINE                 = OFF,
        STATISTICS_NORECOMPUTE = OFF,
        SORT_IN_TEMPDB         = ON
    );

    PRINT 'Indeks IX_Mon_Kontrahent_Historia: UTWORZONY';
END
ELSE
BEGIN
    PRINT 'Indeks IX_Mon_Kontrahent_Historia: już istnieje — pominięto.';
END
GO