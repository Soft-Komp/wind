# backend/alembic/versions/0020_update_views_rodzaj_typ_dok.py
"""0020_update_views_rodzaj_typ_dok

Aktualizuje dwa widoki systemu windykacji — dodaje filtr
r.RODZAJ = 'N' AND r.ID_TYP_DOK != 37 do warunków WHERE.

════════════════════════════════════════════════════════════════
  1. dbo.skw_rozrachunki_faktur
════════════════════════════════════════════════════════════════
  ZMIANA względem migracji 0004:
    WHERE … dodano:
      AND r.RODZAJ     = 'N'       ← tylko rozrachunki typu 'N' (należności)
      AND r.ID_TYP_DOK != 37       ← wyklucza typ dok 37 (noty odsetkowe)

════════════════════════════════════════════════════════════════
  2. dbo.skw_kontrahenci
════════════════════════════════════════════════════════════════
  ZMIANA względem migracji 0011:
    cte_rozrachunki WHERE … dodano:
      AND r.RODZAJ     = 'N'
      AND r.ID_TYP_DOK != 37
    Efekt: SumaDlugu, LiczbaFaktur, DniPrzeterminowania pomijają
    noty odsetkowe — spójne z listą faktur na dłużniku.

DOWNGRADE:
  Przywraca poprzednie wersje obu widoków (bez nowych filtrów).

IDEMPOTENTNOŚĆ:
  CREATE OR ALTER VIEW + MERGE SchemaChecksums → bezpieczne przy re-run.

Revision ID: 0020
Revises:     0019
Create Date: 2026-04-24
"""

from __future__ import annotations

import logging
import textwrap
from typing import Any, Final

import sqlalchemy as sa
from alembic import op

# ─────────────────────────────────────────────────────────────────────────────
# Metadane Alembic
# ─────────────────────────────────────────────────────────────────────────────
revision:      str = "0020"
down_revision: str = "0019"
branch_labels       = None
depends_on          = None

# ─────────────────────────────────────────────────────────────────────────────
# Stałe
# ─────────────────────────────────────────────────────────────────────────────
SCHEMA_WAPRO: Final[str] = "dbo"
SCHEMA_EXT:   Final[str] = "dbo_ext"

VIEW_ROZRACHUNKI: Final[str] = "skw_rozrachunki_faktur"
VIEW_KONTRAHENCI: Final[str] = "skw_kontrahenci"

logger = logging.getLogger(f"alembic.migration.{revision}")


# =============================================================================
# NOWE definicje widoków (upgrade → te wersje)
# =============================================================================

_ROZRACHUNKI_NEW: Final[str] = textwrap.dedent("""\
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
        r.ID_KONTRAHENTA     IS NOT NULL
        AND r.STRONA         = 'WN'
        AND r.CZY_ROZLICZONY IN (0, 1)
        AND r.RODZAJ         = 'N'
        AND r.ID_TYP_DOK     != 37;
""")

_KONTRAHENCI_NEW: Final[str] = textwrap.dedent("""\
    CREATE OR ALTER VIEW dbo.skw_kontrahenci
    AS
    WITH cte_rozrachunki AS
    (
        SELECT
            r.ID_KONTRAHENTA,
            SUM(ISNULL(r.POZOSTALO_WN, 0))                          AS SumaDlugu,
            COUNT(*)                                                 AS LiczbaFaktur,
            CAST(
                dbo.RM_Func_ClarionDateToDateTime(
                    MIN(r.DATA_DOK)
                )
            AS DATE)                                                 AS NajstarszaFaktura,
            MAX(
                CASE
                    WHEN r.TERMIN_PLATNOSCI IS NOT NULL
                         AND r.TERMIN_PLATNOSCI > 0
                    THEN DATEDIFF(
                             DAY,
                             CAST(dbo.RM_Func_ClarionDateToDateTime(r.TERMIN_PLATNOSCI) AS DATE),
                             CAST(GETDATE() AS DATE)
                         )
                    ELSE 0
                END
            )                                                        AS DniPrzeterminowania
        FROM dbo.ROZRACHUNEK_VIEW AS r
        WHERE r.ID_KONTRAHENTA   IS NOT NULL
          AND r.STRONA           = 'WN'
          AND r.CZY_ROZLICZONY   IN (0, 1)
          AND r.RODZAJ           = 'N'
          AND r.ID_TYP_DOK       != 37
        GROUP BY r.ID_KONTRAHENTA
    ),
    cte_monity_ranked AS
    (
        SELECT
            m.ID_KONTRAHENTA,
            m.SentAt,
            m.MonitType,
            COUNT(*) OVER (
                PARTITION BY m.ID_KONTRAHENTA
            )                                                        AS LiczbaMonitow,
            ROW_NUMBER() OVER (
                PARTITION BY m.ID_KONTRAHENTA
                ORDER BY ISNULL(m.SentAt, m.CreatedAt) DESC
            )                                                        AS rn
        FROM dbo_ext.skw_MonitHistory AS m
    ),
    cte_monity AS
    (
        SELECT
            ID_KONTRAHENTA,
            SentAt          AS OstatniMonitData,
            MonitType       AS OstatniMonitTyp,
            LiczbaMonitow
        FROM cte_monity_ranked
        WHERE rn = 1
    ),
    cte_monity_rozrachunki AS
    (
        SELECT
            mh.ID_KONTRAHENTA,
            MAX(mi.CreatedAt)                                        AS OstatniMonitRozrachunku
        FROM dbo_ext.skw_MonitHistory_Invoices AS mi
        JOIN dbo_ext.skw_MonitHistory AS mh ON mh.ID_MONIT = mi.ID_MONIT
        GROUP BY mh.ID_KONTRAHENTA
    )
    SELECT
        k.ID_KONTRAHENTA,
        ISNULL(k.NAZWA_PELNA, k.NAZWA)          AS NazwaKontrahenta,
        k.ADRES_EMAIL                           AS Email,
        k.TELEFON_FIRMOWY                       AS Telefon,
        ISNULL(roz.SumaDlugu,           0)      AS SumaDlugu,
        ISNULL(roz.LiczbaFaktur,        0)      AS LiczbaFaktur,
        roz.NajstarszaFaktura,
        ISNULL(roz.DniPrzeterminowania, 0)      AS DniPrzeterminowania,
        mon.OstatniMonitData,
        mon.OstatniMonitTyp,
        ISNULL(mon.LiczbaMonitow,       0)      AS LiczbaMonitow,
        monr.OstatniMonitRozrachunku
    FROM      dbo.KONTRAHENT         AS k
    LEFT JOIN cte_rozrachunki        AS roz  ON roz.ID_KONTRAHENTA  = k.ID_KONTRAHENTA
    LEFT JOIN cte_monity             AS mon  ON mon.ID_KONTRAHENTA  = k.ID_KONTRAHENTA
    LEFT JOIN cte_monity_rozrachunki AS monr ON monr.ID_KONTRAHENTA = k.ID_KONTRAHENTA
    WHERE ISNULL(roz.SumaDlugu, 0) > 0;
""")


# =============================================================================
# STARE definicje widoków (downgrade → te wersje)
# =============================================================================

_ROZRACHUNKI_OLD: Final[str] = textwrap.dedent("""\
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
        r.ID_KONTRAHENTA     IS NOT NULL
        AND r.STRONA         = 'WN'
        AND r.CZY_ROZLICZONY IN (0, 1);
""")

_KONTRAHENCI_OLD: Final[str] = textwrap.dedent("""\
    CREATE OR ALTER VIEW dbo.skw_kontrahenci
    AS
    WITH cte_rozrachunki AS
    (
        SELECT
            r.ID_KONTRAHENTA,
            SUM(ISNULL(r.POZOSTALO_WN, 0))                          AS SumaDlugu,
            COUNT(*)                                                 AS LiczbaFaktur,
            CAST(
                dbo.RM_Func_ClarionDateToDateTime(
                    MIN(r.DATA_DOK)
                )
            AS DATE)                                                 AS NajstarszaFaktura,
            MAX(
                CASE
                    WHEN r.TERMIN_PLATNOSCI IS NOT NULL
                         AND r.TERMIN_PLATNOSCI > 0
                    THEN DATEDIFF(
                             DAY,
                             CAST(dbo.RM_Func_ClarionDateToDateTime(r.TERMIN_PLATNOSCI) AS DATE),
                             CAST(GETDATE() AS DATE)
                         )
                    ELSE 0
                END
            )                                                        AS DniPrzeterminowania
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
            COUNT(*) OVER (
                PARTITION BY m.ID_KONTRAHENTA
            )                                                        AS LiczbaMonitow,
            ROW_NUMBER() OVER (
                PARTITION BY m.ID_KONTRAHENTA
                ORDER BY ISNULL(m.SentAt, m.CreatedAt) DESC
            )                                                        AS rn
        FROM dbo_ext.skw_MonitHistory AS m
    ),
    cte_monity AS
    (
        SELECT
            ID_KONTRAHENTA,
            SentAt          AS OstatniMonitData,
            MonitType       AS OstatniMonitTyp,
            LiczbaMonitow
        FROM cte_monity_ranked
        WHERE rn = 1
    ),
    cte_monity_rozrachunki AS
    (
        SELECT
            mh.ID_KONTRAHENTA,
            MAX(mi.CreatedAt)                                        AS OstatniMonitRozrachunku
        FROM dbo_ext.skw_MonitHistory_Invoices AS mi
        JOIN dbo_ext.skw_MonitHistory AS mh ON mh.ID_MONIT = mi.ID_MONIT
        GROUP BY mh.ID_KONTRAHENTA
    )
    SELECT
        k.ID_KONTRAHENTA,
        ISNULL(k.NAZWA_PELNA, k.NAZWA)          AS NazwaKontrahenta,
        k.ADRES_EMAIL                           AS Email,
        k.TELEFON_FIRMOWY                       AS Telefon,
        ISNULL(roz.SumaDlugu,           0)      AS SumaDlugu,
        ISNULL(roz.LiczbaFaktur,        0)      AS LiczbaFaktur,
        roz.NajstarszaFaktura,
        ISNULL(roz.DniPrzeterminowania, 0)      AS DniPrzeterminowania,
        mon.OstatniMonitData,
        mon.OstatniMonitTyp,
        ISNULL(mon.LiczbaMonitow,       0)      AS LiczbaMonitow,
        monr.OstatniMonitRozrachunku
    FROM      dbo.KONTRAHENT         AS k
    LEFT JOIN cte_rozrachunki        AS roz  ON roz.ID_KONTRAHENTA  = k.ID_KONTRAHENTA
    LEFT JOIN cte_monity             AS mon  ON mon.ID_KONTRAHENTA  = k.ID_KONTRAHENTA
    LEFT JOIN cte_monity_rozrachunki AS monr ON monr.ID_KONTRAHENTA = k.ID_KONTRAHENTA
    WHERE ISNULL(roz.SumaDlugu, 0) > 0;
""")


# =============================================================================
# Helper prywatny — pobiera skalar z zapytania (wzorzec z 0011)
# =============================================================================

def _raw_scalar(bind: Any, sql: str) -> Any:
    """Zwraca pierwszy skalar z pierwszego wiersza wynikowego zapytania."""
    result = bind.execute(sa.text(sql))
    row = result.fetchone()
    return row[0] if row else None


# =============================================================================
# Helper — rejestracja/aktualizacja checksumu w skw_SchemaChecksums
# =============================================================================

def _register_checksum(bind: Any, view_name: str) -> None:
    """
    Pobiera CHECKSUM() widoku z sys.sql_modules i wykonuje MERGE
    do dbo_ext.skw_SchemaChecksums.

    Operacja idempotentna (MERGE → WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT).
    LastVerifiedAt = NULL → wymusza ponowną weryfikację przy starcie aplikacji.
    """
    logger.info("[%s] Rejestracja checksum dla %s.%s …", revision, SCHEMA_WAPRO, view_name)

    # Krok 1: pobierz INT checksum z sys.sql_modules
    checksum = _raw_scalar(bind, f"""
        SELECT CHECKSUM(m.definition)
        FROM   sys.sql_modules AS m
        JOIN   sys.objects     AS o ON o.object_id = m.object_id
        JOIN   sys.schemas     AS s ON s.schema_id = o.schema_id
        WHERE  s.name = N'{SCHEMA_WAPRO}'
          AND  o.name = N'{view_name}'
    """)

    if checksum is None:
        msg = (
            f"[{revision}] Nie można odczytać CHECKSUM widoku "
            f"{SCHEMA_WAPRO}.{view_name} z sys.sql_modules. "
            "Migracja nie może zarejestrować checksumu."
        )
        logger.critical(msg)
        raise RuntimeError(msg)

    logger.debug("[%s] CHECKSUM(%s) = %s", revision, view_name, checksum)

    # Krok 2: MERGE — nie mieszamy DDL z DML w jednym execute (pyodbc limit)
    bind.execute(sa.text(f"""
        MERGE [{SCHEMA_EXT}].[skw_SchemaChecksums] AS tgt
        USING (
            SELECT
                N'{SCHEMA_WAPRO}'  AS SchemaName,
                N'{view_name}'     AS ObjectName,
                N'VIEW'            AS ObjectType,
                {checksum}         AS Checksum,
                NULL               AS LastVerifiedAt,
                GETDATE()          AS UpdatedAt
        ) AS src
        ON  tgt.SchemaName = src.SchemaName
        AND tgt.ObjectName = src.ObjectName
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SchemaName, ObjectName, ObjectType, Checksum, LastVerifiedAt, UpdatedAt)
            VALUES (src.SchemaName, src.ObjectName, src.ObjectType,
                    src.Checksum,  src.LastVerifiedAt, src.UpdatedAt)
        WHEN MATCHED THEN
            UPDATE SET
                tgt.Checksum       = src.Checksum,
                tgt.LastVerifiedAt = src.LastVerifiedAt,
                tgt.UpdatedAt      = src.UpdatedAt;
    """))

    logger.info(
        "[%s] SchemaChecksums MERGE OK — %s.%s (checksum=%s)",
        revision, SCHEMA_WAPRO, view_name, checksum,
    )


# =============================================================================
# UPGRADE
# =============================================================================

def upgrade() -> None:
    """
    Aktualizuje oba widoki: dodaje filtr RODZAJ='N' AND ID_TYP_DOK!=37.
    Rejestruje nowe checksums w skw_SchemaChecksums.

    Kolejność:
        1. CREATE OR ALTER VIEW skw_rozrachunki_faktur
        2. MERGE checksum skw_rozrachunki_faktur
        3. CREATE OR ALTER VIEW skw_kontrahenci
        4. MERGE checksum skw_kontrahenci
    """
    logger.info("[%s] ── UPGRADE START ──", revision)
    bind = op.get_bind()

    # ── widok 1: skw_rozrachunki_faktur ──────────────────────────────────────
    logger.info("[%s] CREATE OR ALTER VIEW %s.%s …", revision, SCHEMA_WAPRO, VIEW_ROZRACHUNKI)
    bind.execute(sa.text(_ROZRACHUNKI_NEW))
    logger.info("[%s] %s.%s → OK", revision, SCHEMA_WAPRO, VIEW_ROZRACHUNKI)
    _register_checksum(bind, VIEW_ROZRACHUNKI)

    # ── widok 2: skw_kontrahenci ──────────────────────────────────────────────
    logger.info("[%s] CREATE OR ALTER VIEW %s.%s …", revision, SCHEMA_WAPRO, VIEW_KONTRAHENCI)
    bind.execute(sa.text(_KONTRAHENCI_NEW))
    logger.info("[%s] %s.%s → OK", revision, SCHEMA_WAPRO, VIEW_KONTRAHENCI)
    _register_checksum(bind, VIEW_KONTRAHENCI)

    logger.info("[%s] ── UPGRADE OK ──", revision)


# =============================================================================
# DOWNGRADE
# =============================================================================

def downgrade() -> None:
    """
    Przywraca wersje widoków bez filtrów RODZAJ='N' / ID_TYP_DOK!=37.
    Aktualizuje checksums do poprzednich wartości.
    """
    logger.warning("[%s] ── DOWNGRADE START ──", revision)
    bind = op.get_bind()

    logger.warning(
        "[%s] DOWNGRADE: przywracam %s.%s (bez RODZAJ/ID_TYP_DOK)",
        revision, SCHEMA_WAPRO, VIEW_ROZRACHUNKI,
    )
    bind.execute(sa.text(_ROZRACHUNKI_OLD))
    _register_checksum(bind, VIEW_ROZRACHUNKI)

    logger.warning(
        "[%s] DOWNGRADE: przywracam %s.%s (bez RODZAJ/ID_TYP_DOK w cte_rozrachunki)",
        revision, SCHEMA_WAPRO, VIEW_KONTRAHENCI,
    )
    bind.execute(sa.text(_KONTRAHENCI_OLD))
    _register_checksum(bind, VIEW_KONTRAHENCI)

    logger.warning("[%s] ── DOWNGRADE OK ──", revision)