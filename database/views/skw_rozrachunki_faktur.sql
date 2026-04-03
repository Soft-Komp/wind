-- =============================================================================
-- database/views/skw_rozrachunki_faktur.sql
-- =============================================================================
CREATE OR ALTER VIEW dbo.skw_rozrachunki_faktur
AS
WITH cte_ostatni_monit AS
(
    SELECT
        mi.ID_ROZRACHUNKU,
        mi.CreatedAt                                            AS OstatniMonitRozrachunku,
        ROW_NUMBER() OVER (
            PARTITION BY mi.ID_ROZRACHUNKU
            ORDER BY mi.CreatedAt DESC
        )                                                       AS rn
    FROM dbo_ext.skw_MonitHistory_Invoices AS mi
)
SELECT
    r.ID_ROZRACHUNKU,
    r.ID_KONTRAHENTA,
    ISNULL(k.NAZWA_PELNA, k.NAZWA)                              AS NazwaKontrahenta,
    r.NR_DOK                                                    AS NumerFaktury,
    CAST(dbo.RM_Func_ClarionDateToDateTime(r.DATA_DOK)          AS DATE) AS DataWystawienia,
    CAST(dbo.RM_Func_ClarionDateToDateTime(r.TERMIN_PLATNOSCI)  AS DATE) AS TerminPlatnosci,
    r.KWOTA                                                     AS KwotaBrutto,
    CAST(
        CASE
            WHEN r.KWOTA - ISNULL(r.POZOSTALO_WN, 0) < 0
            THEN 0
            ELSE r.KWOTA - ISNULL(r.POZOSTALO_WN, 0)
        END
    AS DECIMAL(15,2))                                           AS KwotaZaplacona,
    ISNULL(r.POZOSTALO_WN, 0)                                   AS KwotaPozostala,
    r.FORMA_PLATNOSCI                                           AS MetodaPlatnosci,
    CASE
        WHEN r.TERMIN_PLATNOSCI IS NULL OR r.TERMIN_PLATNOSCI = 0
            THEN NULL
        WHEN DATEDIFF(
                DAY,
                CAST(dbo.RM_Func_ClarionDateToDateTime(r.TERMIN_PLATNOSCI) AS DATE),
                CAST(GETDATE() AS DATE)
             ) <= 0
            THEN 0
        ELSE
            DATEDIFF(
                DAY,
                CAST(dbo.RM_Func_ClarionDateToDateTime(r.TERMIN_PLATNOSCI) AS DATE),
                CAST(GETDATE() AS DATE)
            )
    END                                                         AS DniPo,
    r.CZY_ROZLICZONY,
    r.ID_TYP_DOK,
    mon.OstatniMonitRozrachunku
FROM dbo.ROZRACHUNEK_VIEW AS r
LEFT JOIN dbo.KONTRAHENT AS k
       ON k.ID_KONTRAHENTA = r.ID_KONTRAHENTA
LEFT JOIN cte_ostatni_monit AS mon
       ON mon.ID_ROZRACHUNKU = r.ID_ROZRACHUNKU
      AND mon.rn = 1
WHERE
    r.ID_KONTRAHENTA    IS NOT NULL
    AND r.STRONA        = 'WN'
    AND r.CZY_ROZLICZONY IN (0, 1);