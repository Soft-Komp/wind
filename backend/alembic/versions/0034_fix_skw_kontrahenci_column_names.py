"""
0034_fix_skw_kontrahenci_column_names
══════════════════════════════════════
Widok skw_kontrahenci na STOMIL (0033) miał inne nazwy kolumn
niż oczekuje wapro.py (_COLS_KONTRAHENCI).

Mapowanie kolumn widoku → nazwy wymagane przez wapro.py:
  LiczbaFakturNiezaplaconych → LiczbaFaktur
  DataOstatniejFaktury       → NajstarszaFaktura
  MaxDniPrzeterminowania     → DniPrzeterminowania
  IdKontrahenta              → ID_KONTRAHENTA
  NULL                       → OstatniMonitTyp        (brak w widoku)
  NULL                       → OstatniMonitRozrachunku (brak w widoku)

Revision ID : 0034
Revises     : 0033
"""

import logging
from alembic import op

revision      = "0034"
down_revision = "0033"
branch_labels = None
depends_on    = None

SCHEMA = "dbo"

logger = logging.getLogger(f"alembic.migration.{revision}")


def upgrade() -> None:
    logger.info("0034 upgrade — naprawa nazw kolumn skw_kontrahenci")

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
        END)                                                     AS SumaDlinguPrzeterminowanego,
        COUNT(*)                                                 AS LiczbaFaktur,
        MIN(CAST(dbo.RM_Func_ClarionDateToDateTime(r.[DATA_DOK]) AS DATE))
                                                                 AS NajstarszaFaktura,
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
        END)                                                     AS DniPrzeterminowania
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
    k.[ID_KONTRAHENTA]                              AS ID_KONTRAHENTA,
    k.[KOD_KONTRAHENTA]                             AS KodKontrahenta,
    ISNULL(k.[NAZWA_PELNA], k.[NAZWA])              AS NazwaKontrahenta,
    k.[NIP]                                         AS NIP,
    k.[ADRES_EMAIL]                                 AS Email,
    k.[TELEFON_FIRMOWY]                             AS Telefon,
    CAST(NULL AS NVARCHAR(100))                     AS Ulica,
    k.[KOD_POCZTOWY]                                AS KodPocztowy,
    k.[MIEJSCOWOSC]                                 AS Miejscowosc,
    ISNULL(roz.[SumaDlugu], 0)                      AS SumaDlugu,
    ISNULL(roz.[SumaDlinguPrzeterminowanego], 0)    AS SumaDlinguPrzeterminowanego,
    ISNULL(roz.[LiczbaFaktur], 0)                   AS LiczbaFakturNiezaplaconych,
    ISNULL(roz.[LiczbaFaktur], 0)                   AS LiczbaFaktur,
    CASE WHEN ISNULL(roz.[SumaDlinguPrzeterminowanego], 0) > 0
         THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT)
    END                                             AS MaPrzeterminowane,
    roz.[NajstarszyTerminPlatnosci],
    roz.[NajstarszaFaktura]                         AS DataOstatniejFaktury,
    roz.[NajstarszaFaktura],
    CASE WHEN ISNULL(roz.[DniPrzeterminowania], 0) > 0
         THEN roz.[DniPrzeterminowania]
         ELSE NULL
    END                                             AS MaxDniPrzeterminowania,
    roz.[DniPrzeterminowania],
    CASE
        WHEN ISNULL(roz.[DniPrzeterminowania], 0) = 0   THEN N'biezace'
        WHEN roz.[DniPrzeterminowania]            <= 30  THEN N'do30dni'
        WHEN roz.[DniPrzeterminowania]            <= 90  THEN N'31_90dni'
        WHEN roz.[DniPrzeterminowania]            <= 180 THEN N'91_180dni'
        ELSE                                                  N'powyzej180dni'
    END                                             AS KategoriaWieku,
    mon.[OstatniMonitData],
    CAST(NULL AS NVARCHAR(50))                      AS OstatniMonitTyp,
    ISNULL(mon.[LiczbaMonitow], 0)                  AS LiczbaMonitow,
    CAST(NULL AS NVARCHAR(100))                     AS OstatniMonitRozrachunku
FROM [{SCHEMA}].[KONTRAHENT] AS k
LEFT JOIN cte_rozrachunki AS roz ON roz.[ID_KONTRAHENTA] = k.[ID_KONTRAHENTA]
LEFT JOIN cte_monity       AS mon ON mon.[ID_KONTRAHENTA] = k.[ID_KONTRAHENTA]
WHERE ISNULL(roz.[SumaDlugu], 0) > 0
""")

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
            UPDATE SET [Checksum] = source.[Checksum], [UpdatedAt] = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ([ObjectName],[SchemaName],[ObjectType],[Checksum],[CreatedAt],[UpdatedAt])
            VALUES (source.[ObjectName], source.[SchemaName], source.[ObjectType],
                    source.[Checksum], SYSUTCDATETIME(), SYSUTCDATETIME());
    """)

    logger.info("0034 upgrade — zakończono")


def downgrade() -> None:
    raise NotImplementedError("Downgrade 0034 nieodwracalny.")