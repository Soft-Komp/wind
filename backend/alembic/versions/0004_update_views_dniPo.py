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

_VIEW_ROZRACHUNKI_NEW = """
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
    r.id_rozrachunku                                            AS ID_ROZRACHUNKU,
    r.id_platnika                                               AS ID_KONTRAHENTA,
    ISNULL(k.NAZWA_PELNA, k.NAZWA)                              AS NazwaKontrahenta,
    r.numer                                                     AS NumerFaktury,
    CAST(dbo.RM_Func_ClarionDateToDateTime(r.data_wystawienia)  AS DATE) AS DataWystawienia,
    CAST(dbo.RM_Func_ClarionDateToDateTime(r.termin_platnosci)  AS DATE) AS TerminPlatnosci,
    ABS(r.wartosc_brutto)                                       AS KwotaBrutto,
    CAST(
        CASE
            WHEN ABS(r.wartosc_brutto) - ABS(r.pozostalo) < 0 THEN 0
            ELSE ABS(r.wartosc_brutto) - ABS(r.pozostalo)
        END
    AS DECIMAL(15,2))                                           AS KwotaZaplacona,
    ABS(r.pozostalo)                                            AS KwotaPozostala,
    r.forma_platnosci                                           AS MetodaPlatnosci,
    CASE
        WHEN r.rozliczony = 2 THEN 0
        WHEN r.termin_platnosci IS NULL OR r.termin_platnosci = 0 THEN NULL
        WHEN DATEDIFF(DAY,
                CAST(dbo.RM_Func_ClarionDateToDateTime(r.termin_platnosci) AS DATE),
                CAST(GETDATE() AS DATE)) <= 0 THEN 0
        ELSE DATEDIFF(DAY,
                CAST(dbo.RM_Func_ClarionDateToDateTime(r.termin_platnosci) AS DATE),
                CAST(GETDATE() AS DATE))
    END                                                         AS DniPo,
    r.rozliczony,
    r.typ_dok,
    mon.OstatniMonitRozrachunku
FROM ROZRACHUNEK_V AS r
LEFT JOIN dbo.KONTRAHENT AS k
       ON CAST(k.ID_KONTRAHENTA AS INT) = r.id_platnika
LEFT JOIN cte_ostatni_monit AS mon
       ON mon.ID_ROZRACHUNKU = r.id_rozrachunku
      AND mon.rn = 1
WHERE
    r.id_platnika IS NOT NULL
    AND r.pozostalo < 0
"""

_SKW_KONTRAHENCI_NEW = """
CREATE OR ALTER VIEW dbo.skw_kontrahenci
AS
WITH cte_rozrachunki AS
(
    SELECT
        r.id_platnika,
        SUM(ABS(r.pozostalo))                                   AS SumaDlugu,
        COUNT(CASE WHEN r.typ_dok = 'h' THEN 1 END)             AS LiczbaFaktur,
        CAST(dbo.RM_Func_ClarionDateToDateTime(
            MIN(CASE WHEN r.typ_dok = 'h' THEN r.data_wystawienia END)
        ) AS DATE)                                              AS NajstarszaFaktura,
        MAX(CASE
            WHEN r.termin_platnosci IS NOT NULL AND r.termin_platnosci > 0
            THEN DATEDIFF(DAY,
                CAST(dbo.RM_Func_ClarionDateToDateTime(r.termin_platnosci) AS DATE),
                CAST(GETDATE() AS DATE))
            ELSE 0
        END)                                                    AS DniPrzeterminowania
    FROM dbo.ROZRACHUNEK_V AS r
    WHERE r.id_platnika IS NOT NULL
      AND r.rozliczony = 0
      AND r.pozostalo < 0
    GROUP BY r.id_platnika
),
cte_monity_ranked AS
(
    SELECT
        m.ID_KONTRAHENTA, m.SentAt, m.MonitType,
        COUNT(*) OVER (PARTITION BY m.ID_KONTRAHENTA)           AS LiczbaMonitow,
        ROW_NUMBER() OVER (
            PARTITION BY m.ID_KONTRAHENTA
            ORDER BY ISNULL(m.SentAt, m.CreatedAt) DESC
        )                                                       AS rn
    FROM dbo_ext.skw_MonitHistory AS m
),
cte_monity AS
(
    SELECT ID_KONTRAHENTA, SentAt AS OstatniMonitData,
           MonitType AS OstatniMonitTyp, LiczbaMonitow
    FROM cte_monity_ranked WHERE rn = 1
),
cte_monity_rozrachunki AS
(
    SELECT
        mh.ID_KONTRAHENTA,
        MAX(mi.CreatedAt)                                       AS OstatniMonitRozrachunku
    FROM dbo_ext.skw_MonitHistory_Invoices AS mi
    JOIN dbo_ext.skw_MonitHistory AS mh ON mh.ID_MONIT = mi.ID_MONIT
    GROUP BY mh.ID_KONTRAHENTA
)
SELECT
    k.ID_KONTRAHENTA,
    ISNULL(k.NAZWA_PELNA, k.NAZWA)              AS NazwaKontrahenta,
    k.ADRES_EMAIL                               AS Email,
    k.TELEFON_FIRMOWY                           AS Telefon,
    ISNULL(roz.SumaDlugu,           0)          AS SumaDlugu,
    ISNULL(roz.LiczbaFaktur,        0)          AS LiczbaFaktur,
    roz.NajstarszaFaktura,
    ISNULL(roz.DniPrzeterminowania, 0)          AS DniPrzeterminowania,
    mon.OstatniMonitData,
    mon.OstatniMonitTyp,
    ISNULL(mon.LiczbaMonitow,       0)          AS LiczbaMonitow,
    monr.OstatniMonitRozrachunku
FROM dbo.KONTRAHENT AS k
LEFT JOIN cte_rozrachunki        AS roz  ON roz.id_platnika    = CAST(k.ID_KONTRAHENTA AS INT)
LEFT JOIN cte_monity             AS mon  ON mon.ID_KONTRAHENTA = CAST(k.ID_KONTRAHENTA AS INT)
LEFT JOIN cte_monity_rozrachunki AS monr ON monr.ID_KONTRAHENTA = CAST(k.ID_KONTRAHENTA AS INT)
"""

# =============================================================================
# Poprzednie wersje (do downgrade)
# =============================================================================

_VIEW_ROZRACHUNKI_OLD = """
CREATE OR ALTER VIEW dbo.skw_rozrachunki_faktur
AS
SELECT
    r.id_rozrachunku        AS ID_ROZRACHUNKU,
    r.id_platnika           AS ID_KONTRAHENTA,
    ISNULL(k.NAZWA_PELNA, k.NAZWA) AS NazwaKontrahenta,
    r.numer                 AS NumerFaktury,
    CAST(dbo.RM_Func_ClarionDateToDateTime(r.data_wystawienia) AS DATE) AS DataWystawienia,
    CAST(dbo.RM_Func_ClarionDateToDateTime(r.termin_platnosci) AS DATE) AS TerminPlatnosci,
    ABS(r.wartosc_brutto)   AS KwotaBrutto,
    CAST(
        CASE
            WHEN ABS(r.wartosc_brutto) - ABS(r.pozostalo) < 0 THEN 0
            ELSE ABS(r.wartosc_brutto) - ABS(r.pozostalo)
        END
    AS DECIMAL(15,2))       AS KwotaZaplacona,
    ABS(r.pozostalo)        AS KwotaPozostala,
    r.forma_platnosci       AS MetodaPlatnosci,
    r.rozliczony,
    r.typ_dok
FROM ROZRACHUNEK_V AS r
LEFT JOIN dbo.KONTRAHENT AS k ON CAST(k.ID_KONTRAHENTA AS INT) = r.id_platnika
WHERE r.id_platnika IS NOT NULL AND r.pozostalo < 0
"""

_SKW_KONTRAHENCI_OLD = """
CREATE OR ALTER VIEW dbo.skw_kontrahenci
AS
WITH cte_rozrachunki AS
(
    SELECT
        r.id_platnika,
        SUM(ABS(r.pozostalo)) AS SumaDlugu,
        COUNT(CASE WHEN r.typ_dok = 'h' THEN 1 END) AS LiczbaFaktur,
        CAST(dbo.RM_Func_ClarionDateToDateTime(
            MIN(CASE WHEN r.typ_dok = 'h' THEN r.data_wystawienia END)
        ) AS DATE) AS NajstarszaFaktura,
        MAX(CASE
            WHEN r.termin_platnosci IS NOT NULL AND r.termin_platnosci > 0
            THEN DATEDIFF(DAY,
                CAST(dbo.RM_Func_ClarionDateToDateTime(r.termin_platnosci) AS DATE),
                CAST(GETDATE() AS DATE))
            ELSE 0
        END) AS DniPrzeterminowania
    FROM dbo.ROZRACHUNEK_V AS r
    WHERE r.id_platnika IS NOT NULL AND r.rozliczony = 0 AND r.pozostalo < 0
    GROUP BY r.id_platnika
),
cte_monity_ranked AS
(
    SELECT m.ID_KONTRAHENTA, m.SentAt, m.MonitType,
           COUNT(*) OVER (PARTITION BY m.ID_KONTRAHENTA) AS LiczbaMonitow,
           ROW_NUMBER() OVER (
               PARTITION BY m.ID_KONTRAHENTA
               ORDER BY ISNULL(m.SentAt, m.CreatedAt) DESC
           ) AS rn
    FROM dbo_ext.skw_MonitHistory AS m
),
cte_monity AS
(
    SELECT ID_KONTRAHENTA, SentAt AS OstatniMonitData,
           MonitType AS OstatniMonitTyp, LiczbaMonitow
    FROM cte_monity_ranked WHERE rn = 1
)
SELECT
    k.ID_KONTRAHENTA,
    ISNULL(k.NAZWA_PELNA, k.NAZWA) AS NazwaKontrahenta,
    k.ADRES_EMAIL AS Email, k.TELEFON_FIRMOWY AS Telefon,
    ISNULL(roz.SumaDlugu, 0) AS SumaDlugu,
    ISNULL(roz.LiczbaFaktur, 0) AS LiczbaFaktur,
    roz.NajstarszaFaktura,
    ISNULL(roz.DniPrzeterminowania, 0) AS DniPrzeterminowania,
    mon.OstatniMonitData, mon.OstatniMonitTyp,
    ISNULL(mon.LiczbaMonitow, 0) AS LiczbaMonitow
FROM dbo.KONTRAHENT AS k
LEFT JOIN cte_rozrachunki AS roz ON roz.id_platnika    = CAST(k.ID_KONTRAHENTA AS INT)
LEFT JOIN cte_monity      AS mon ON mon.ID_KONTRAHENTA = CAST(k.ID_KONTRAHENTA AS INT)
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