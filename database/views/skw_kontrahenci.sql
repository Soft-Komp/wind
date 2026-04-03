-- =============================================================================
-- database/views/skw_kontrahenci.sql
-- =============================================================================
-- Widok: dbo.skw_kontrahenci
-- Cel:   Zagregowane dane dłużników z tabel WAPRO do systemu windykacyjnego.
--        Enkapsuluje logikę biznesową — żaden endpoint nie odpytuje tabel WAPRO
--        bezpośrednio. Wszystkie filtry biznesowe są tutaj.
--
--
-- Tabele źródłowe (WAPRO, tylko odczyt):
--   dbo.KONTRAHENT    — dane kontrahenta (nazwa, email, telefon, RODO, zablokowany)
--   dbo.Rozrachunek   — rozrachunki faktur (UWAGA: Rozrachunek, nie ROZRACHUNKI!)
--
-- Schemat widoku: dbo (ten sam schemat co tabele WAPRO)
-- Odpytywany przez: db/wapro.py (pyodbc, NIE SQLAlchemy ORM)
-- Checksum śledzony w: dbo_ext.SchemaChecksums (SchemaName='dbo', ObjectType='VIEW')
--
-- =============================================================================

-- Idempotentny: CREATE OR ALTER — bezpieczne przy wielokrotnym wdrożeniu
-- Wymaga SQL Server 2016 SP1+ (nasza wersja: MSSQL 2022 — OK)
CREATE OR ALTER VIEW dbo.skw_kontrahenci
AS
WITH cte_rozrachunki AS
(
    SELECT
        r.ID_KONTRAHENTA,
        SUM(ISNULL(r.POZOSTALO_WN, 0))                               AS SumaDlugu,
        COUNT(*)                                                      AS LiczbaFaktur,
        CAST(
            dbo.RM_Func_ClarionDateToDateTime(
                MIN(r.DATA_DOK)
            )
        AS DATE)                                                      AS NajstarszaFaktura,
        MAX(
            CASE
                WHEN r.TERMIN_PLATNOSCI IS NOT NULL AND r.TERMIN_PLATNOSCI > 0
                THEN DATEDIFF(
                    DAY,
                    CAST(dbo.RM_Func_ClarionDateToDateTime(r.TERMIN_PLATNOSCI) AS DATE),
                    CAST(GETDATE() AS DATE)
                )
                ELSE 0
            END
        )                                                             AS DniPrzeterminowania
    FROM dbo.ROZRACHUNEK_VIEW AS r
    WHERE r.ID_KONTRAHENTA   IS NOT NULL
      AND r.STRONA           = 'WN'
      AND r.CZY_ROZLICZONY   IN (0, 1)
    GROUP BY r.ID_KONTRAHENTA
),
cte_monity_ranked AS
(
    SELECT
        m.ID_KONTRAHENTA,
        m.SentAt,
        m.MonitType,
        COUNT(*)     OVER (PARTITION BY m.ID_KONTRAHENTA)            AS LiczbaMonitow,
        ROW_NUMBER() OVER (
            PARTITION BY m.ID_KONTRAHENTA
            ORDER BY ISNULL(m.SentAt, m.CreatedAt) DESC
        )                                                             AS rn
    FROM dbo_ext.skw_MonitHistory AS m
),
cte_monity AS
(
    SELECT
        ID_KONTRAHENTA,
        SentAt      AS OstatniMonitData,
        MonitType   AS OstatniMonitTyp,
        LiczbaMonitow
    FROM cte_monity_ranked
    WHERE rn = 1
),
cte_monity_rozrachunki AS
(
    SELECT
        mh.ID_KONTRAHENTA,
        MAX(mi.CreatedAt)                                             AS OstatniMonitRozrachunku
    FROM dbo_ext.skw_MonitHistory_Invoices AS mi
    JOIN dbo_ext.skw_MonitHistory          AS mh ON mh.ID_MONIT = mi.ID_MONIT
    GROUP BY mh.ID_KONTRAHENTA
)
SELECT
    k.ID_KONTRAHENTA,
    ISNULL(k.NAZWA_PELNA, k.NAZWA)          AS NazwaKontrahenta,
    k.ADRES_EMAIL                           AS Email,
    k.TELEFON_FIRMOWY                       AS Telefon,
    ISNULL(roz.SumaDlugu,          0)       AS SumaDlugu,
    ISNULL(roz.LiczbaFaktur,       0)       AS LiczbaFaktur,
    roz.NajstarszaFaktura,
    ISNULL(roz.DniPrzeterminowania, 0)      AS DniPrzeterminowania,
    mon.OstatniMonitData,
    mon.OstatniMonitTyp,
    ISNULL(mon.LiczbaMonitow,      0)       AS LiczbaMonitow,
    monr.OstatniMonitRozrachunku
FROM      dbo.KONTRAHENT           AS k
LEFT JOIN cte_rozrachunki          AS roz  ON roz.ID_KONTRAHENTA   = k.ID_KONTRAHENTA
LEFT JOIN cte_monity               AS mon  ON mon.ID_KONTRAHENTA   = k.ID_KONTRAHENTA
LEFT JOIN cte_monity_rozrachunki   AS monr ON monr.ID_KONTRAHENTA  = k.ID_KONTRAHENTA;