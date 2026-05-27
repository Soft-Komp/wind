"""
0033_stomil_catchup
════════════════════
Migracja wyrównująca stan bazy STOMIL (środowisko testowe).

Problem: migracja 0011 (skw_kontrahenci) wymagała tabel
skw_MonitHistory i skw_MonitHistory_Invoices — prereq-check
przerwał DDL widoku ale Alembic zaliczył rewizję.

Na tej instalacji wszystkie tabele skw_* są w schemacie dbo
(przeniesione z dbo_ext). Widok skw_kontrahenci musi
joinować dbo.skw_MonitHistory, nie dbo_ext.

Kroki:
  01. SKIP — skw_MonitHistory już istnieje w dbo
  02. SKIP — skw_MonitHistory_Invoices już istnieje w dbo
  03. CREATE OR ALTER VIEW dbo.skw_kontrahenci
  04. MERGE skw_SchemaChecksums

Revision ID : 0033
Revises     : 0032
"""

import logging
from alembic import op

revision      = "0033"
down_revision = "0032"
branch_labels = None
depends_on    = None

SCHEMA     = "dbo"
SCHEMA_EXT = "dbo_ext"

logger = logging.getLogger(f"alembic.migration.{revision}")


def _log(krok: str, msg: str) -> None:
    logger.info("0033 [%s] %s", krok, msg)


def upgrade() -> None:
    logger.info("0033 upgrade — wyrównanie stanu STOMIL")

    # ── KROK 01 ───────────────────────────────────────────────────────────────
    _log("01", "SKIP — skw_MonitHistory juz istnieje w dbo")

    # ── KROK 02 ───────────────────────────────────────────────────────────────
    _log("02", "SKIP — skw_MonitHistory_Invoices juz istnieje w dbo")

    # ── KROK 03: skw_kontrahenci ──────────────────────────────────────────────
    _log("03", "CREATE OR ALTER VIEW dbo.skw_kontrahenci")
    op.execute(f"""
CREATE OR ALTER VIEW [{SCHEMA}].[skw_kontrahenci] AS
WITH cte_rozrachunki AS (
    SELECT
        r.[ID_KONTRAHENTA],
        SUM(ISNULL(r.[POZOSTALO_WN], 0))                        AS SumaDlugu,
        SUM(CASE
                WHEN r.[TERMIN_PLATNOSCI] IS NOT NULL
                     AND r.[TERMIN_PLATNOSCI] > 0
                     AND DATEDIFF(DAY,
                         CAST(dbo.RM_Func_ClarionDateToDateTime(r.[TERMIN_PLATNOSCI]) AS DATE),
                         CAST(GETDATE() AS DATE)) > 0
                THEN ISNULL(r.[POZOSTALO_WN], 0)
                ELSE 0
        END)                                                     AS SumaDliguPrzeterminowanego,
        COUNT(*)                                                 AS LiczbaFakturNiezaplaconych,
        MIN(CAST(dbo.RM_Func_ClarionDateToDateTime(r.[DATA_DOK]) AS DATE))
                                                                 AS DataOstatniejFaktury,
        MIN(CASE
                WHEN r.[TERMIN_PLATNOSCI] IS NOT NULL
                     AND r.[TERMIN_PLATNOSCI] > 0
                THEN CAST(dbo.RM_Func_ClarionDateToDateTime(r.[TERMIN_PLATNOSCI]) AS DATE)
                ELSE NULL
        END)                                                     AS NajstarszyTerminPlatnosci,
        MAX(CASE
                WHEN r.[TERMIN_PLATNOSCI] IS NOT NULL
                     AND r.[TERMIN_PLATNOSCI] > 0
                THEN DATEDIFF(DAY,
                         CAST(dbo.RM_Func_ClarionDateToDateTime(r.[TERMIN_PLATNOSCI]) AS DATE),
                         CAST(GETDATE() AS DATE))
                ELSE 0
        END)                                                     AS MaxDniPrzeterminowania
    FROM [{SCHEMA}].[ROZRACHUNEK_VIEW] AS r
    WHERE r.[ID_KONTRAHENTA] IS NOT NULL
      AND r.[STRONA]          = N'WN'
      AND r.[CZY_ROZLICZONY] IN (0, 1)
    GROUP BY r.[ID_KONTRAHENTA]
),
cte_monity AS (
    SELECT
        m.[ID_KONTRAHENTA],
        MAX(ISNULL(m.[SentAt], m.[CreatedAt]))  AS OstatniMonitData,
        COUNT(*)                                AS LiczbaMonitow
    FROM [{SCHEMA}].[skw_MonitHistory] AS m
    WHERE m.[IsActive] = 1
    GROUP BY m.[ID_KONTRAHENTA]
)
SELECT
    k.[ID_KONTRAHENTA]                          AS IdKontrahenta,
    k.[KOD_KONTRAHENTA]                         AS KodKontrahenta,
    ISNULL(k.[NAZWA_PELNA], k.[NAZWA])          AS NazwaKontrahenta,
    k.[NIP]                                     AS NIP,
    k.[ADRES_EMAIL]                             AS Email,
    k.[TELEFON_FIRMOWY]                         AS Telefon,
    CAST(NULL AS NVARCHAR(100))                 AS Ulica,
    k.[KOD_POCZTOWY]                            AS KodPocztowy,
    k.[MIEJSCOWOSC]                             AS Miejscowosc,
    ISNULL(roz.[SumaDlugu], 0)                  AS SumaDlugu,
    ISNULL(roz.[SumaDliguPrzeterminowanego], 0) AS SumaDlinguPrzeterminowanego,
    ISNULL(roz.[LiczbaFakturNiezaplaconych], 0) AS LiczbaFakturNiezaplaconych,
    CASE WHEN ISNULL(roz.[SumaDliguPrzeterminowanego], 0) > 0
         THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT)
    END                                         AS MaPrzeterminowane,
    roz.[NajstarszyTerminPlatnosci],
    roz.[DataOstatniejFaktury],
    CASE WHEN ISNULL(roz.[MaxDniPrzeterminowania], 0) > 0
         THEN roz.[MaxDniPrzeterminowania]
         ELSE NULL
    END                                         AS MaxDniPrzeterminowania,
    CASE
        WHEN ISNULL(roz.[MaxDniPrzeterminowania], 0) = 0    THEN N'biezace'
        WHEN roz.[MaxDniPrzeterminowania]            <= 30   THEN N'do30dni'
        WHEN roz.[MaxDniPrzeterminowania]            <= 90   THEN N'31_90dni'
        WHEN roz.[MaxDniPrzeterminowania]            <= 180  THEN N'91_180dni'
        ELSE                                                      N'powyzej180dni'
    END                                         AS KategoriaWieku,
    mon.[OstatniMonitData],
    ISNULL(mon.[LiczbaMonitow], 0)              AS LiczbaMonitow
FROM [{SCHEMA}].[KONTRAHENT] AS k
LEFT JOIN cte_rozrachunki AS roz ON roz.[ID_KONTRAHENTA] = k.[ID_KONTRAHENTA]
LEFT JOIN cte_monity       AS mon ON mon.[ID_KONTRAHENTA] = k.[ID_KONTRAHENTA]
WHERE ISNULL(roz.[SumaDlugu], 0) > 0
""")
    _log("03", "OK")

    # ── KROK 04: Aktualizacja SchemaChecksums ─────────────────────────────────
    _log("04", "MERGE skw_SchemaChecksums — skw_kontrahenci")
    op.execute(f"""
        MERGE [{SCHEMA}].[skw_SchemaChecksums] AS target
        USING (
            SELECT
                obj.[name]                  AS ObjectName,
                sch.[name]                  AS SchemaName,
                N'VIEW'                     AS ObjectType,
                CHECKSUM(mod.[definition])  AS Checksum
            FROM sys.objects obj
            JOIN sys.schemas sch ON sch.[schema_id] = obj.[schema_id]
            JOIN sys.sql_modules mod ON mod.[object_id] = obj.[object_id]
            WHERE obj.[type] = N'V'
              AND sch.[name] = N'{SCHEMA}'
              AND obj.[name] = N'skw_kontrahenci'
        ) AS source
            ON target.[ObjectName] = source.[ObjectName]
           AND target.[SchemaName] = source.[SchemaName]
        WHEN MATCHED THEN
            UPDATE SET
                [Checksum]  = source.[Checksum],
                [UpdatedAt] = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ([ObjectName],[SchemaName],[ObjectType],[Checksum],[CreatedAt],[UpdatedAt])
            VALUES (source.[ObjectName], source.[SchemaName], source.[ObjectType],
                    source.[Checksum], SYSUTCDATETIME(), SYSUTCDATETIME());
    """)
    _log("04", "OK")

    logger.info("0033 upgrade — zakończono")


def downgrade() -> None:
    raise NotImplementedError(
        "Downgrade 0033 nieodwracalny. Cofaj ręcznie jeśli konieczne."
    )