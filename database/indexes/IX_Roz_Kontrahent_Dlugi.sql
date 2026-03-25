-- =============================================================================
-- INDEX: IX_Roz_Kontrahent_Dlugi
-- =============================================================================
--
-- Cel: Optymalizacja CTE cte_rozrachunki w skw_kontrahenci.
-- Pokrywa: SUM(POZOSTALO) per ID_KONTRAHENTA dla faktur niezapłaconych.
--
-- Tabela docelowa: dbo.Rozrachunek (WAPRO — TYLKO ODCZYT)
-- Alembic: zarządzany przez migrację 002_add_wapro_performance_indexes.py
-- =============================================================================

USE [WAPRO];
GO

PRINT 'Tworzenie indeksu IX_Roz_Kontrahent_Dlugi...';

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[dbo].[Rozrachunek]')
      AND name = N'IX_Roz_Kontrahent_Dlugi'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Roz_Kontrahent_Dlugi]
        ON [dbo].[Rozrachunek] (
            [ID_KONTRAHENTA] ASC,
            [CZY_ROZLICZONY] ASC
        )
        INCLUDE (
            [POZOSTALO],
            [KWOTA],
            [STRONA],
            [TERMIN_PLATNOSCI],
            [TYP_DOK]
        )
        WHERE [TYP_DOK] = N'F'
          AND [CZY_ROZLICZONY] = 0
    WITH (
        FILLFACTOR             = 85,
        ONLINE                 = OFF,
        STATISTICS_NORECOMPUTE = OFF,
        SORT_IN_TEMPDB         = ON
    );

    PRINT 'Indeks IX_Roz_Kontrahent_Dlugi: UTWORZONY';
END
ELSE
BEGIN
    PRINT 'Indeks IX_Roz_Kontrahent_Dlugi: już istnieje — pominięto.';
END
GO