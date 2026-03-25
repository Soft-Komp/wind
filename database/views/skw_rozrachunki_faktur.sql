-- =============================================================================
-- database/views/skw_rozrachunki_faktur.sql
-- =============================================================================
CREATE OR ALTER VIEW dbo.skw_rozrachunki_faktur
AS
SELECT
    r.id_platnika AS ID_KONTRAHENTA,
    ISNULL(k.NAZWA_PELNA, k.NAZWA) AS NazwaKontrahenta,
    r.numer AS NumerFaktury,
    CAST(dbo.RM_Func_ClarionDateToDateTime(r.data_wystawienia) AS DATE) AS DataWystawienia,
    CAST(dbo.RM_Func_ClarionDateToDateTime(r.termin_platnosci) AS DATE) AS TerminPlatnosci,
    ABS(r.wartosc_brutto) AS KwotaBrutto,
    CAST(
        CASE
            WHEN ABS(r.wartosc_brutto) - ABS(r.pozostalo) < 0
            THEN 0
            ELSE ABS(r.wartosc_brutto) - ABS(r.pozostalo)
        END
    AS DECIMAL(15,2)) AS KwotaZaplacona,
    ABS(r.pozostalo) AS KwotaPozostala,
    r.forma_platnosci AS MetodaPlatnosci,
    CASE
        WHEN r.rozliczony = 2
        THEN 0
        WHEN r.termin_platnosci IS NULL OR r.termin_platnosci = 0
        THEN NULL
        WHEN DATEDIFF(
            DAY,
            CAST(dbo.RM_Func_ClarionDateToDateTime(r.termin_platnosci) AS DATE),
            CAST(GETDATE() AS DATE)
        ) <= 0
        THEN 0
        ELSE
            DATEDIFF(
                DAY,
                CAST(dbo.RM_Func_ClarionDateToDateTime(r.termin_platnosci) AS DATE),
                CAST(GETDATE() AS DATE)
            )
    END AS DniPo,
    r.rozliczony,
    r.typ_dok
FROM ROZRACHUNEK_V AS r
LEFT JOIN dbo.KONTRAHENT AS k
    ON CAST(k.ID_KONTRAHENTA AS INT) = r.id_platnika
WHERE
    r.id_platnika IS NOT NULL
    AND r.pozostalo < 0;