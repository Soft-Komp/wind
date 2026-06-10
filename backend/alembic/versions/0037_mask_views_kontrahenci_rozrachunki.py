# backend/alembic/versions/0037_mask_views_kontrahenci_rozrachunki.py
"""0037_mask_views_kontrahenci_rozrachunki

Aktualizuje dwa widoki WAPRO — maskowanie danych wrażliwych.

WIDOKI OBJĘTE:
    1. dbo.skw_rozrachunki_faktur  (v aktualnej → v+1 z maskowaniem)
       Pola: NazwaKontrahenta, NumerFaktury

    2. dbo.skw_kontrahenci         (v aktualnej → v+1 z maskowaniem)
       Pola: NazwaKontrahenta

WZORZEC MASKOWANIA (identyczny jak 0036):
    NazwaKontrahenta → 'KONTRAHENT-[XXXXXXXX]'
        HASHBYTES('SHA2_256', ISNULL(nazwa, N'')) → 8 hex znaków

    NumerFaktury     → 'TST/RRRR/[XXXX]/0001'
        HASHBYTES('SHA2_256', ISNULL(numer, N'')) → 4 hex znaki

DOWNGRADE:
    Przywraca definicje z migracji 0020 (ostatnia przed maskowaniem).

Revision ID: 0037
Revises:     0036
Create Date: 2026-06-08
"""

from __future__ import annotations

import logging
import textwrap
from typing import Final

import sqlalchemy as sa
from alembic import op

revision:      str = "0037"
down_revision: str = "0036"
branch_labels       = None
depends_on          = None

logger = logging.getLogger(f"alembic.migration.{revision}")

SCHEMA_WAPRO: Final[str] = "dbo"
SCHEMA_EXT:   Final[str] = "dbo"


# =============================================================================
# Helper — MERGE SchemaChecksums (identyczny wzorzec jak 0036)
# =============================================================================

def _merge_checksum(view_name: str, alembic_revision: str) -> None:
    logger.info(
        "[%s] MERGE SchemaChecksums → %s.%s (revision=%s) …",
        revision, SCHEMA_WAPRO, view_name, alembic_revision,
    )
    op.execute(textwrap.dedent(f"""\
        MERGE [{SCHEMA_EXT}].[skw_SchemaChecksums] AS target
        USING (
            SELECT
                N'{view_name}'        AS ObjectName,
                N'{SCHEMA_WAPRO}'     AS SchemaName,
                N'VIEW'               AS ObjectType,
                (
                    SELECT CHECKSUM(m.definition)
                    FROM   sys.sql_modules AS m
                    JOIN   sys.objects     AS o ON m.object_id = o.object_id
                    WHERE  o.name                   = N'{view_name}'
                      AND  SCHEMA_NAME(o.schema_id) = N'{SCHEMA_WAPRO}'
                )                     AS Checksum,
                N'{alembic_revision}' AS AlembicRevision,
                NULL                  AS LastVerifiedAt,
                GETDATE()             AS Now
        ) AS source
        ON (
            target.[ObjectName] = source.[ObjectName]
            AND target.[SchemaName] = source.[SchemaName]
            AND target.[ObjectType] = source.[ObjectType]
        )
        WHEN MATCHED THEN
            UPDATE SET
                [Checksum]        = source.[Checksum],
                [AlembicRevision] = source.[AlembicRevision],
                [LastVerifiedAt]  = source.[LastVerifiedAt],
                [UpdatedAt]       = source.[Now]
        WHEN NOT MATCHED THEN
            INSERT (
                [ObjectName], [SchemaName], [ObjectType],
                [Checksum], [AlembicRevision], [LastVerifiedAt], [CreatedAt]
            )
            VALUES (
                source.[ObjectName], source.[SchemaName], source.[ObjectType],
                source.[Checksum], source.[AlembicRevision],
                source.[LastVerifiedAt], source.[Now]
            );
    """))
    logger.info("[%s] SchemaChecksums MERGE → OK (%s.%s)", revision, SCHEMA_WAPRO, view_name)


# =============================================================================
# DDL — skw_rozrachunki_faktur
# =============================================================================

# UPGRADE — z maskowaniem NazwaKontrahenta + NumerFaktury
_ROZRACHUNKI_V_MASKED: Final[str] = textwrap.dedent("""\
    CREATE OR ALTER VIEW dbo.skw_rozrachunki_faktur
    AS
    /*
        WERSJA : masked  (migracja 0037, 2026-06-08)
        POPRZ. : 0020

        ZMIANY:
          NazwaKontrahenta → 'KONTRAHENT-[XXXXXXXX]'  (SHA2_256, 8 hex)
          NumerFaktury     → 'TST/RRRR/[XXXX]/0001'   (SHA2_256, 4 hex)
    */
    SELECT
        r.ID_ROZRACHUNKU,
        r.ID_KONTRAHENTA,

        -- NazwaKontrahenta zamaskowana
        N'KONTRAHENT-['
        + LEFT(
            CONVERT(
                NVARCHAR(64),
                HASHBYTES('SHA2_256', ISNULL(ISNULL(k.NAZWA_PELNA, k.NAZWA), N'')),
                2
            ),
            8
          )
        + N']'                                          AS NazwaKontrahenta,

        -- NumerFaktury zamaskowany
        N'TST/'
        + CAST(YEAR(GETDATE()) AS NVARCHAR(4))
        + N'/['
        + LEFT(
            CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', ISNULL(r.NR_DOK, N'')), 2),
            4
          )
        + N']/0001'                                     AS NumerFaktury,

        CAST(DATEADD(DAY, r.DATA_DOK,         '18991230') AS DATE) AS DataWystawienia,
        CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE) AS TerminPlatnosci,
        r.KWOTA                                         AS KwotaBrutto,
        CAST(
            CASE
                WHEN r.KWOTA - ISNULL(r.POZOSTALO_WN, 0) < 0
                THEN 0
                ELSE r.KWOTA - ISNULL(r.POZOSTALO_WN, 0)
            END
        AS DECIMAL(15,2))                               AS KwotaZaplacona,
        ISNULL(r.POZOSTALO_WN, 0)                       AS KwotaPozostala,
        r.FORMA_PLATNOSCI                               AS MetodaPlatnosci,
        CASE
            WHEN r.TERMIN_PLATNOSCI IS NULL OR r.TERMIN_PLATNOSCI = 0
                THEN NULL
            WHEN DATEDIFF(
                    DAY,
                    CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE),
                    CAST(GETDATE() AS DATE)
                 ) <= 0
                THEN 0
            ELSE
                DATEDIFF(
                    DAY,
                    CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE),
                    CAST(GETDATE() AS DATE)
                )
        END                                             AS DniPo,
        r.CZY_ROZLICZONY,
        r.ID_TYP_DOK
    FROM dbo.ROZRACHUNEK_VIEW AS r
    LEFT JOIN dbo.KONTRAHENT AS k
           ON k.ID_KONTRAHENTA = r.ID_KONTRAHENTA
    WHERE
        r.ID_KONTRAHENTA    IS NOT NULL
        AND r.STRONA        = 'WN'
        AND r.CZY_ROZLICZONY IN (0, 1)
        AND r.RODZAJ        = 'N'
        AND r.ID_TYP_DOK   != 37
""")

# DOWNGRADE — przywrócenie wersji z migracji 0020 (bez maskowania)
_ROZRACHUNKI_V_ORIG: Final[str] = textwrap.dedent("""\
    CREATE OR ALTER VIEW dbo.skw_rozrachunki_faktur
    AS
    /*
        WERSJA : 0020 — przywrócona przez downgrade 0037
    */
    SELECT
        r.ID_ROZRACHUNKU,
        r.ID_KONTRAHENTA,
        ISNULL(k.NAZWA_PELNA, k.NAZWA)                              AS NazwaKontrahenta,
        r.NR_DOK                                                    AS NumerFaktury,
        CAST(DATEADD(DAY, r.DATA_DOK,         '18991230') AS DATE)  AS DataWystawienia,
        CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE)  AS TerminPlatnosci,
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
                    CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE),
                    CAST(GETDATE() AS DATE)
                 ) <= 0
                THEN 0
            ELSE
                DATEDIFF(
                    DAY,
                    CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE),
                    CAST(GETDATE() AS DATE)
                )
        END                                                         AS DniPo,
        r.CZY_ROZLICZONY,
        r.ID_TYP_DOK
    FROM dbo.ROZRACHUNEK_VIEW AS r
    LEFT JOIN dbo.KONTRAHENT AS k
           ON k.ID_KONTRAHENTA = r.ID_KONTRAHENTA
    WHERE
        r.ID_KONTRAHENTA    IS NOT NULL
        AND r.STRONA        = 'WN'
        AND r.CZY_ROZLICZONY IN (0, 1)
        AND r.RODZAJ        = 'N'
        AND r.ID_TYP_DOK   != 37
""")


# =============================================================================
# DDL — skw_kontrahenci
# =============================================================================

# UPGRADE — z maskowaniem NazwaKontrahenta
# Widok skw_kontrahenci jest bardzo długi (CTE) — maskujemy TYLKO kolumnę
# NazwaKontrahenta w finalnym SELECT. Reszta CTE bez zmian.
_KONTRAHENCI_V_MASKED: Final[str] = textwrap.dedent("""\
    CREATE OR ALTER VIEW dbo.skw_kontrahenci
    AS
    /*
        WERSJA : masked  (migracja 0037, 2026-06-08)
        POPRZ. : 0020

        ZMIANA: NazwaKontrahenta → 'KONTRAHENT-[XXXXXXXX]' (SHA2_256, 8 hex)
        CTE bez zmian — maskowanie tylko w finalnym SELECT.
    */
    WITH cte_rozrachunki AS
    (
        SELECT
            r.ID_KONTRAHENTA,
            SUM(ISNULL(r.POZOSTALO_WN, 0))              AS SumaDlugu,
            COUNT(*)                                     AS LiczbaFaktur,
            CAST(
                dbo.RM_Func_ClarionDateToDateTime(MIN(r.DATA_DOK))
            AS DATE)                                     AS NajstarszaFaktura,
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
            )                                            AS DniPrzeterminowania
        FROM dbo.ROZRACHUNEK_VIEW AS r
        WHERE r.ID_KONTRAHENTA IS NOT NULL
          AND r.STRONA          = 'WN'
          AND r.CZY_ROZLICZONY IN (0, 1)
        GROUP BY r.ID_KONTRAHENTA
    ),
    cte_monity_ranked AS
    (
        SELECT
            m.ID_KONTRAHENTA,
            m.SentAt,
            m.MonitType,
            COUNT(*) OVER (PARTITION BY m.ID_KONTRAHENTA) AS LiczbaMonitow,
            ROW_NUMBER() OVER (
                PARTITION BY m.ID_KONTRAHENTA
                ORDER BY ISNULL(m.SentAt, m.CreatedAt) DESC
            )                                              AS rn
        FROM dbo.skw_MonitHistory AS m
    ),
    cte_monity AS
    (
        SELECT ID_KONTRAHENTA, SentAt AS OstatniMonitData,
               MonitType AS OstatniMonitTyp, LiczbaMonitow
        FROM cte_monity_ranked
        WHERE rn = 1
    ),
    cte_monity_rozrachunki AS
    (
        SELECT
            mi.ID_ROZRACHUNKU,
            MAX(ISNULL(mh.SentAt, mh.CreatedAt)) AS OstatniMonitRozrachunku
        FROM dbo.skw_MonitHistory_Invoices AS mi
        JOIN dbo.skw_MonitHistory          AS mh ON mh.ID_MONIT = mi.ID_MONIT
        GROUP BY mi.ID_ROZRACHUNKU
    ),
    cte_ostatni_monit_rozrachunku AS
    (
        SELECT
            r.ID_KONTRAHENTA,
            MAX(mr.OstatniMonitRozrachunku) AS OstatniMonitRozrachunku
        FROM dbo.ROZRACHUNEK_VIEW               AS r
        JOIN cte_monity_rozrachunki             AS mr ON mr.ID_ROZRACHUNKU = r.ID_ROZRACHUNKU
        WHERE r.ID_KONTRAHENTA IS NOT NULL
          AND r.STRONA = 'WN'
        GROUP BY r.ID_KONTRAHENTA
    )
    SELECT
        k.ID_KONTRAHENTA                                AS IdKontrahenta,
        k.KLUCZ                                         AS KodKontrahenta,

        -- NazwaKontrahenta zamaskowana
        N'KONTRAHENT-['
        + LEFT(
            CONVERT(
                NVARCHAR(64),
                HASHBYTES('SHA2_256', ISNULL(ISNULL(k.NAZWA_PELNA, k.NAZWA), N'')),
                2
            ),
            8
          )
        + N']'                                          AS NazwaKontrahenta,

        k.NIP                                           AS NIP,
        k.ADRES_EMAIL                                   AS Email,
        k.TELEFON_FIRMOWY                               AS Telefon,
        NULL                                            AS Ulica,
        NULL                                            AS KodPocztowy,
        NULL                                            AS Miejscowosc,
        ISNULL(cr.SumaDlugu, 0)                         AS SumaDlugu,
        ISNULL(cr.LiczbaFaktur, 0)                      AS LiczbaFakturNiezaplaconych,
        CASE WHEN cr.DniPrzeterminowania > 0 THEN 1 ELSE 0 END AS MaPrzeterminowane,
        cr.NajstarszaFaktura                            AS NajstarszyTerminPlatnosci,
        cr.NajstarszaFaktura                            AS DataOstatniejFaktury,
        CASE WHEN cr.DniPrzeterminowania > 0
             THEN cr.DniPrzeterminowania ELSE NULL
        END                                             AS MaxDniPrzeterminowania,
        ISNULL(
            (
                SELECT SUM(ISNULL(r2.POZOSTALO_WN, 0))
                FROM dbo.ROZRACHUNEK_VIEW AS r2
                WHERE r2.ID_KONTRAHENTA = k.ID_KONTRAHENTA
                  AND r2.STRONA = 'WN'
                  AND r2.CZY_ROZLICZONY IN (0, 1)
                  AND r2.TERMIN_PLATNOSCI IS NOT NULL
                  AND r2.TERMIN_PLATNOSCI > 0
                  AND CAST(dbo.RM_Func_ClarionDateToDateTime(r2.TERMIN_PLATNOSCI) AS DATE)
                      < CAST(GETDATE() AS DATE)
            ), 0
        )                                               AS SumaDlinguPrzeterminowanego,
        cm.OstatniMonitData,
        cm.OstatniMonitTyp,
        ISNULL(cm.LiczbaMonitow, 0)                     AS LiczbaMonitow,
        omr.OstatniMonitRozrachunku,
        CASE
            WHEN cr.DniPrzeterminowania > 365 THEN N'powyzej_roku'
            WHEN cr.DniPrzeterminowania > 180 THEN N'powyzej_pol_roku'
            WHEN cr.DniPrzeterminowania > 90  THEN N'powyzej_3_miesiecy'
            WHEN cr.DniPrzeterminowania > 30  THEN N'powyzej_miesiaca'
            WHEN cr.DniPrzeterminowania > 0   THEN N'do_miesiaca'
            ELSE N'biezace'
        END                                             AS KategoriaWieku
    FROM dbo.KONTRAHENT AS k
    JOIN cte_rozrachunki              AS cr  ON cr.ID_KONTRAHENTA  = k.ID_KONTRAHENTA
    LEFT JOIN cte_monity              AS cm  ON cm.ID_KONTRAHENTA  = k.ID_KONTRAHENTA
    LEFT JOIN cte_ostatni_monit_rozrachunku AS omr
                                             ON omr.ID_KONTRAHENTA = k.ID_KONTRAHENTA
    WHERE k.RODO_ZANONIMIZOWANY = 0
      AND k.ZABLOKOWANY         = 0
""")

# DOWNGRADE — przywrócenie wersji 0020 (bez maskowania)
# Identyczna logika CTE, tylko NazwaKontrahenta bez HASHBYTES
_KONTRAHENCI_V_ORIG: Final[str] = textwrap.dedent("""\
    CREATE OR ALTER VIEW dbo.skw_kontrahenci
    AS
    /*
        WERSJA : 0020 — przywrócona przez downgrade 0037
    */
    WITH cte_rozrachunki AS
    (
        SELECT
            r.ID_KONTRAHENTA,
            SUM(ISNULL(r.POZOSTALO_WN, 0))              AS SumaDlugu,
            COUNT(*)                                     AS LiczbaFaktur,
            CAST(
                dbo.RM_Func_ClarionDateToDateTime(MIN(r.DATA_DOK))
            AS DATE)                                     AS NajstarszaFaktura,
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
            )                                            AS DniPrzeterminowania
        FROM dbo.ROZRACHUNEK_VIEW AS r
        WHERE r.ID_KONTRAHENTA IS NOT NULL
          AND r.STRONA          = 'WN'
          AND r.CZY_ROZLICZONY IN (0, 1)
        GROUP BY r.ID_KONTRAHENTA
    ),
    cte_monity_ranked AS
    (
        SELECT
            m.ID_KONTRAHENTA,
            m.SentAt,
            m.MonitType,
            COUNT(*) OVER (PARTITION BY m.ID_KONTRAHENTA) AS LiczbaMonitow,
            ROW_NUMBER() OVER (
                PARTITION BY m.ID_KONTRAHENTA
                ORDER BY ISNULL(m.SentAt, m.CreatedAt) DESC
            )                                              AS rn
        FROM dbo.skw_MonitHistory AS m
    ),
    cte_monity AS
    (
        SELECT ID_KONTRAHENTA, SentAt AS OstatniMonitData,
               MonitType AS OstatniMonitTyp, LiczbaMonitow
        FROM cte_monity_ranked
        WHERE rn = 1
    ),
    cte_monity_rozrachunki AS
    (
        SELECT
            mi.ID_ROZRACHUNKU,
            MAX(ISNULL(mh.SentAt, mh.CreatedAt)) AS OstatniMonitRozrachunku
        FROM dbo.skw_MonitHistory_Invoices AS mi
        JOIN dbo.skw_MonitHistory          AS mh ON mh.ID_MONIT = mi.ID_MONIT
        GROUP BY mi.ID_ROZRACHUNKU
    ),
    cte_ostatni_monit_rozrachunku AS
    (
        SELECT
            r.ID_KONTRAHENTA,
            MAX(mr.OstatniMonitRozrachunku) AS OstatniMonitRozrachunku
        FROM dbo.ROZRACHUNEK_VIEW               AS r
        JOIN cte_monity_rozrachunki             AS mr ON mr.ID_ROZRACHUNKU = r.ID_ROZRACHUNKU
        WHERE r.ID_KONTRAHENTA IS NOT NULL
          AND r.STRONA = 'WN'
        GROUP BY r.ID_KONTRAHENTA
    )
    SELECT
        k.ID_KONTRAHENTA                                AS IdKontrahenta,
        k.KLUCZ                                         AS KodKontrahenta,
        ISNULL(k.NAZWA_PELNA, k.NAZWA)                  AS NazwaKontrahenta,
        k.NIP                                           AS NIP,
        k.ADRES_EMAIL                                   AS Email,
        k.TELEFON_FIRMOWY                               AS Telefon,
        NULL                                            AS Ulica,
        NULL                                            AS KodPocztowy,
        NULL                                            AS Miejscowosc,
        ISNULL(cr.SumaDlugu, 0)                         AS SumaDlugu,
        ISNULL(cr.LiczbaFaktur, 0)                      AS LiczbaFakturNiezaplaconych,
        CASE WHEN cr.DniPrzeterminowania > 0 THEN 1 ELSE 0 END AS MaPrzeterminowane,
        cr.NajstarszaFaktura                            AS NajstarszyTerminPlatnosci,
        cr.NajstarszaFaktura                            AS DataOstatniejFaktury,
        CASE WHEN cr.DniPrzeterminowania > 0
             THEN cr.DniPrzeterminowania ELSE NULL
        END                                             AS MaxDniPrzeterminowania,
        ISNULL(
            (
                SELECT SUM(ISNULL(r2.POZOSTALO_WN, 0))
                FROM dbo.ROZRACHUNEK_VIEW AS r2
                WHERE r2.ID_KONTRAHENTA = k.ID_KONTRAHENTA
                  AND r2.STRONA = 'WN'
                  AND r2.CZY_ROZLICZONY IN (0, 1)
                  AND r2.TERMIN_PLATNOSCI IS NOT NULL
                  AND r2.TERMIN_PLATNOSCI > 0
                  AND CAST(dbo.RM_Func_ClarionDateToDateTime(r2.TERMIN_PLATNOSCI) AS DATE)
                      < CAST(GETDATE() AS DATE)
            ), 0
        )                                               AS SumaDlinguPrzeterminowanego,
        cm.OstatniMonitData,
        cm.OstatniMonitTyp,
        ISNULL(cm.LiczbaMonitow, 0)                     AS LiczbaMonitow,
        omr.OstatniMonitRozrachunku,
        CASE
            WHEN cr.DniPrzeterminowania > 365 THEN N'powyzej_roku'
            WHEN cr.DniPrzeterminowania > 180 THEN N'powyzej_pol_roku'
            WHEN cr.DniPrzeterminowania > 90  THEN N'powyzej_3_miesiecy'
            WHEN cr.DniPrzeterminowania > 30  THEN N'powyzej_miesiaca'
            WHEN cr.DniPrzeterminowania > 0   THEN N'do_miesiaca'
            ELSE N'biezace'
        END                                             AS KategoriaWieku
    FROM dbo.KONTRAHENT AS k
    JOIN cte_rozrachunki              AS cr  ON cr.ID_KONTRAHENTA  = k.ID_KONTRAHENTA
    LEFT JOIN cte_monity              AS cm  ON cm.ID_KONTRAHENTA  = k.ID_KONTRAHENTA
    LEFT JOIN cte_ostatni_monit_rozrachunku AS omr
                                             ON omr.ID_KONTRAHENTA = k.ID_KONTRAHENTA
    WHERE k.RODO_ZANONIMIZOWANY = 0
      AND k.ZABLOKOWANY         = 0
""")


# =============================================================================
# UPGRADE
# =============================================================================

def upgrade() -> None:
    logger.info("[%s] upgrade START — maskowanie NazwaKontrahenta + NumerFaktury", revision)
    bind = op.get_bind()

    logger.info("[%s] 1/4 CREATE OR ALTER VIEW skw_rozrachunki_faktur (masked) …", revision)
    bind.execute(sa.text(_ROZRACHUNKI_V_MASKED))
    logger.info("[%s] 2/4 skw_rozrachunki_faktur → OK", revision)
    _merge_checksum("skw_rozrachunki_faktur", revision)

    logger.info("[%s] 3/4 CREATE OR ALTER VIEW skw_kontrahenci (masked) …", revision)
    bind.execute(sa.text(_KONTRAHENCI_V_MASKED))
    logger.info("[%s] 4/4 skw_kontrahenci → OK", revision)
    _merge_checksum("skw_kontrahenci", revision)

    logger.info("[%s] upgrade ZAKOŃCZONY — oba widoki zamaskowane", revision)


# =============================================================================
# DOWNGRADE
# =============================================================================

def downgrade() -> None:
    logger.warning("[%s] downgrade START — przywracanie widoków bez maskowania", revision)
    bind = op.get_bind()

    bind.execute(sa.text(_ROZRACHUNKI_V_ORIG))
    _merge_checksum("skw_rozrachunki_faktur", "0020")

    bind.execute(sa.text(_KONTRAHENCI_V_ORIG))
    _merge_checksum("skw_kontrahenci", "0020")

    logger.warning("[%s] downgrade ZAKOŃCZONY — widoki przywrócone do wersji 0020", revision)