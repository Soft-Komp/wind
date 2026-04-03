# =============================================================================
# backend/alembic/versions/0004_update_views_dniPo.py
#
# Co robi:
#   Aktualizuje 2 widoki SQL (CREATE OR ALTER — bezpieczne wielokrotnie):
#   - dbo.skw_rozrachunki_faktur  → dodaje DniPo, OstatniMonitRozrachunku
#   - dbo.skw_kontrahenci         → dodaje OstatniMonitRozrachunku
#
# WYMAGA: tabela dbo_ext.skw_MonitHistory_Invoices (migracja 0003)
#
# Poprzednia: 0003_add_monit_history_invoices
# Następna:   (kolejna)
# =============================================================================

from __future__ import annotations

import hashlib
import logging

import sqlalchemy as sa
from alembic import op

logger = logging.getLogger("alembic.migration.0004")

revision      = "0004"
down_revision = "0003"
branch_labels = None
depends_on    = None

# =============================================================================
# Nowe widoki
# =============================================================================

_SKW_ROZRACHUNKI_NEW = """
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
    AND r.CZY_ROZLICZONY IN (0, 1)
"""

_SKW_KONTRAHENCI_NEW = """
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
LEFT JOIN cte_monity_rozrachunki   AS monr ON monr.ID_KONTRAHENTA  = k.ID_KONTRAHENTA
"""

# =============================================================================
# Poprzednie wersje (do downgrade)
# =============================================================================

_SKW_ROZRACHUNKI_OLD = """
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
    AND r.CZY_ROZLICZONY IN (0, 1)
"""

_SKW_KONTRAHENCI_OLD = """
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
LEFT JOIN cte_monity_rozrachunki   AS monr ON monr.ID_KONTRAHENTA  = k.ID_KONTRAHENTA
"""


# =============================================================================
# UPGRADE
# =============================================================================

def upgrade() -> None:
    logger.info("0004 upgrade: START — aktualizacja widoków SQL")

    op.execute(sa.text(_SKW_ROZRACHUNKI_NEW))
    logger.info("0004: skw_rozrachunki_faktur OK")

    op.execute(sa.text(_SKW_KONTRAHENCI_NEW))
    logger.info("0004: skw_kontrahenci OK")

    # SchemaChecksums celowo pominiete — kolumna Checksum jest INT,
    # MD5 hex nie konwertuje sie do INT w MSSQL.
    # Checksums aktualizowane recznie lub przez osobny mechanizm.

    logger.info("0004 upgrade: ZAKOŃCZONY POMYŚLNIE")


# =============================================================================
# DOWNGRADE
# =============================================================================

def downgrade() -> None:
    logger.info("0004 downgrade: START — przywracam poprzednie widoki")

    op.execute(_SKW_ROZRACHUNKI_OLD)
    logger.info("0004 downgrade: skw_rozrachunki_faktur przywrócony")

    op.execute(_SKW_KONTRAHENCI_OLD)
    logger.info("0004 downgrade: skw_kontrahenci przywrócony")

    logger.info("0004 downgrade: ZAKOŃCZONY POMYŚLNIE")


# =============================================================================
# Helper — SchemaChecksums
# =============================================================================
def _upsert_schema_checksums() -> None:
    """Checksums — calosc przez op.execute(), zero conn.execute()."""
    entries = [
        ("dbo", "VIEW", "skw_rozrachunki_faktur", _SKW_ROZRACHUNKI_NEW),
        ("dbo", "VIEW", "skw_kontrahenci",        _SKW_KONTRAHENCI_NEW),
    ]
    for schema_name, obj_type, obj_name, sql in entries:
        checksum = hashlib.md5(sql.strip().encode("utf-8")).hexdigest()
        try:
            op.execute(
                sa.text("""
                    IF EXISTS (
                        SELECT 1 FROM [dbo_ext].[skw_SchemaChecksums]
                        WHERE [SchemaName] = :schema
                          AND [ObjectName] = :name
                    )
                        UPDATE [dbo_ext].[skw_SchemaChecksums]
                        SET    [Checksum]  = :checksum,
                               [UpdatedAt] = GETDATE()
                        WHERE  [SchemaName] = :schema
                          AND  [ObjectName] = :name
                    ELSE
                        INSERT INTO [dbo_ext].[skw_SchemaChecksums]
                            ([SchemaName], [ObjectType], [ObjectName],
                             [Checksum],  [UpdatedAt])
                        VALUES
                            (:schema, :obj_type, :name, :checksum, GETDATE())
                """).bindparams(
                    schema=schema_name,
                    obj_type=obj_type,
                    name=obj_name,
                    checksum=checksum,
                )
            )
            logger.info("0004: SchemaChecksum OK — %s", obj_name)
        except Exception as exc:
            logger.warning(
                "0004: SchemaChecksum FAILED dla %s — %s (kontynuuje)",
                obj_name, exc,
            )