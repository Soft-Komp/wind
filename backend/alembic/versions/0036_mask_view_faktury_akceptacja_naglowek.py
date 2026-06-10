# backend/alembic/versions/0036_mask_view_faktury_akceptacja_naglowek.py
"""0036_mask_view_faktury_akceptacja_naglowek

Aktualizuje widok dbo.skw_faktury_akceptacja_naglowek do wersji v4.

ZMIANY v3 → v4:
    NazwaKontrahenta i NUMER zastąpione deterministycznymi tokenami
    pseudonimizacyjnymi opartymi na HASHBYTES('SHA2_256', ...).

    Cel: maskowanie danych wrażliwych na potrzeby środowisk
    testowych, szkoleń i prezentacji systemu.

    Maskowanie stałe — brak flagi konfiguracyjnej.
    Powrót do danych oryginalnych: downgrade() tej migracji.

WZORZEC MASKOWNIA:
    NazwaKontrahenta → 'KONTRAHENT-[XXXXXXXX]'
        gdzie XXXXXXXX = pierwsze 8 znaków hex z HASHBYTES SHA2_256
        z NAZWA_PELNA kontrahenta (deterministyczne per kontrahent)

    NUMER → 'TST/RRRR/[XXXX]/0001'
        gdzie RRRR = rok bieżący z GETDATE()
              XXXX = pierwsze 4 znaki hex z HASHBYTES SHA2_256 z NUMER
        (deterministyczne per numer dokumentu)

IDEMPOTENTNOŚĆ:
    CREATE OR ALTER VIEW — bezpieczny re-run.
    MERGE SchemaChecksums — bezpieczny re-run.

DOWNGRADE:
    Przywraca v3 (definicja zakodowana na stałe poniżej w _VIEW_V3).
    Aktualizuje SchemaChecksums do revision 0018.

Revision ID: 0036
Revises:     0035
Create Date: 2026-06-08
"""

from __future__ import annotations

import logging
import textwrap
from typing import Final

from alembic import op

# ---------------------------------------------------------------------------
# Metadane Alembic
# ---------------------------------------------------------------------------
revision:      str = "0036"
down_revision: str = "0035"
branch_labels       = None
depends_on          = None

logger = logging.getLogger(f"alembic.migration.{revision}")

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------
SCHEMA_WAPRO: Final[str] = "dbo"
SCHEMA_EXT:   Final[str] = "dbo"
VIEW_NAME:    Final[str] = "skw_faktury_akceptacja_naglowek"
FQVIEW:       Final[str] = f"[{SCHEMA_WAPRO}].[{VIEW_NAME}]"


# =============================================================================
# DDL — v4 (z maskowaniem, UPGRADE target)
# =============================================================================

_VIEW_V4: Final[str] = textwrap.dedent("""\
    CREATE OR ALTER VIEW dbo.skw_faktury_akceptacja_naglowek
    AS
    /*
        WERSJA : v4  (migracja 0036, 2026-06-08)
        POPRZ. : v3  (migracja 0018, 2026-04-22)

        ZMIANY v3 → v4:
          Maskowanie danych wrażliwych — stałe, bez flagi konfiguracyjnej.

          NazwaKontrahenta:
            'KONTRAHENT-[' + LEFT(CONVERT(NVARCHAR(64),
              HASHBYTES('SHA2_256', ISNULL(k.NAZWA_PELNA, ISNULL(k.NAZWA, N''))), 2), 8) + ']'

          NUMER:
            'TST/' + CAST(YEAR(GETDATE()) AS NVARCHAR(4)) + '/['
            + LEFT(CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', ISNULL(bd.NUMER, N'')), 2), 4)
            + ']/0001'

          Maskowanie deterministyczne — ten sam input → zawsze ten sam token.
          HASHBYTES SHA2_256 — wbudowany w MSSQL, nie wymaga CLR ani UDF.

        POZOSTAŁE KOLUMNY: bez zmian względem v3.
        FILTRY: bez zmian względem v3 (PRG_KOD=1, KSEF_ID IS NOT NULL, KIERUNEK_SYS='Z').
        JOIN: bez zmian względem v3 (KONTRAHENT.KLUCZ = BUF_DOKUMENT.KONTRAHENT_KLUCZ).
    */
    SELECT
        bd.ID_BUF_DOKUMENT,
        bd.KSEF_ID,

        -- NUMER zamaskowany deterministycznie
        N'TST/'
        + CAST(YEAR(GETDATE()) AS NVARCHAR(4))
        + N'/['
        + LEFT(
            CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', ISNULL(bd.NUMER, N'')), 2),
            4
          )
        + N']/0001'                                     AS NUMER,

        bd.KOD_STATUSU,
        CASE
            WHEN bd.KOD_STATUSU IS NULL THEN N'NOWY'
            WHEN bd.KOD_STATUSU = N'K'  THEN N'ZATWIERDZONY'
            WHEN bd.KOD_STATUSU = N'A'  THEN N'ZAKSIEGOWANY'
            ELSE bd.KOD_STATUSU
        END                                             AS StatusOpis,

        CAST(dbo.RM_Func_ClarionDateToDateTime(bd.DATA_WYSTAWIENIA) AS DATE) AS DataWystawienia,
        CAST(dbo.RM_Func_ClarionDateToDateTime(bd.DATA_OTRZYMANIA)  AS DATE) AS DataOtrzymania,
        CAST(dbo.RM_Func_ClarionDateToDateTime(bd.TERMIN_PLATNOSCI) AS DATE) AS TerminPlatnosci,
        bd.WARTOSC_NETTO,
        bd.WARTOSC_BRUTTO,
        bd.KWOTA_VAT,
        bd.FORMA_PLATNOSCI,
        bd.UWAGI,
        k.ID_KONTRAHENTA,

        -- NazwaKontrahenta zamaskowana deterministycznie
        N'KONTRAHENT-['
        + LEFT(
            CONVERT(
                NVARCHAR(64),
                HASHBYTES('SHA2_256', ISNULL(k.NAZWA_PELNA, ISNULL(k.NAZWA, N''))),
                2
            ),
            8
          )
        + N']'                                          AS NazwaKontrahenta,

        k.ADRES_EMAIL     AS EmailKontrahenta,
        k.TELEFON_FIRMOWY AS TelefonKontrahenta

    FROM dbo.BUF_DOKUMENT AS bd
    LEFT JOIN dbo.KONTRAHENT AS k
        ON k.KLUCZ = bd.KONTRAHENT_KLUCZ
    WHERE bd.PRG_KOD     = 1
      AND bd.KSEF_ID      IS NOT NULL
      AND bd.KIERUNEK_SYS = N'Z'
""")


# =============================================================================
# DDL — v3 (oryginał, DOWNGRADE target)
# =============================================================================

_VIEW_V3: Final[str] = textwrap.dedent("""\
    CREATE OR ALTER VIEW dbo.skw_faktury_akceptacja_naglowek
    AS
    /*
        WERSJA : v3  (migracja 0018, 2026-04-22) — przywrócona przez downgrade 0036
    */
    SELECT
        bd.ID_BUF_DOKUMENT,
        bd.KSEF_ID,
        bd.NUMER,
        bd.KOD_STATUSU,
        CASE
            WHEN bd.KOD_STATUSU IS NULL THEN N'NOWY'
            WHEN bd.KOD_STATUSU = N'K'  THEN N'ZATWIERDZONY'
            WHEN bd.KOD_STATUSU = N'A'  THEN N'ZAKSIEGOWANY'
            ELSE bd.KOD_STATUSU
        END                                             AS StatusOpis,

        CAST(dbo.RM_Func_ClarionDateToDateTime(bd.DATA_WYSTAWIENIA) AS DATE) AS DataWystawienia,
        CAST(dbo.RM_Func_ClarionDateToDateTime(bd.DATA_OTRZYMANIA)  AS DATE) AS DataOtrzymania,
        CAST(dbo.RM_Func_ClarionDateToDateTime(bd.TERMIN_PLATNOSCI) AS DATE) AS TerminPlatnosci,
        bd.WARTOSC_NETTO,
        bd.WARTOSC_BRUTTO,
        bd.KWOTA_VAT,
        bd.FORMA_PLATNOSCI,
        bd.UWAGI,
        k.ID_KONTRAHENTA,
        CASE
            WHEN ISNULL(k.NAZWA_PELNA, N'') = N''
            THEN bd.KONTRAHENT_KLUCZ
            ELSE k.NAZWA_PELNA
        END                                             AS NazwaKontrahenta,

        k.ADRES_EMAIL     AS EmailKontrahenta,
        k.TELEFON_FIRMOWY AS TelefonKontrahenta

    FROM dbo.BUF_DOKUMENT AS bd
    LEFT JOIN dbo.KONTRAHENT AS k
        ON k.KLUCZ = bd.KONTRAHENT_KLUCZ
    WHERE bd.PRG_KOD     = 1
      AND bd.KSEF_ID      IS NOT NULL
      AND bd.KIERUNEK_SYS = N'Z'
""")


# =============================================================================
# Helper — MERGE SchemaChecksums
# =============================================================================

def _merge_checksum(alembic_revision: str) -> None:
    """
    Rejestruje aktualny checksum widoku w dbo_ext.skw_SchemaChecksums.

    CHECKSUM obliczany dynamicznie z sys.sql_modules — zawsze aktualny
    względem definicji widoku w tej samej transakcji.
    LastVerifiedAt = NULL — wymusi re-weryfikację przy następnym starcie.
    """
    logger.info(
        "[%s] MERGE SchemaChecksums → %s.%s (revision=%s) …",
        revision, SCHEMA_WAPRO, VIEW_NAME, alembic_revision,
    )

    op.execute(textwrap.dedent(f"""\
        MERGE [{SCHEMA_EXT}].[skw_SchemaChecksums] AS target
        USING (
            SELECT
                N'{VIEW_NAME}'        AS ObjectName,
                N'{SCHEMA_WAPRO}'     AS SchemaName,
                N'VIEW'               AS ObjectType,
                (
                    SELECT CHECKSUM(m.definition)
                    FROM   sys.sql_modules AS m
                    JOIN   sys.objects     AS o ON m.object_id = o.object_id
                    WHERE  o.name                   = N'{VIEW_NAME}'
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

    logger.info(
        "[%s] SchemaChecksums MERGE → OK (%s.%s)",
        revision, SCHEMA_WAPRO, VIEW_NAME,
    )


# =============================================================================
# UPGRADE
# =============================================================================

def upgrade() -> None:
    """
    Zastępuje widok skw_faktury_akceptacja_naglowek wersją v4 (z maskowaniem).
    Rejestruje nowy checksum w skw_SchemaChecksums.

    Kolejność:
        1. CREATE OR ALTER VIEW (v4 — dane zamaskowane)
        2. MERGE SchemaChecksums (revision=0036)
    """
    logger.info("[%s] upgrade START → %s (v3 → v4, maskowanie danych)", revision, FQVIEW)

    op.execute(textwrap.dedent(_VIEW_V4))
    logger.info("[%s] CREATE OR ALTER VIEW → OK", revision)

    _merge_checksum(alembic_revision=revision)

    logger.info("[%s] upgrade ZAKOŃCZONY — widok %s aktywny (v4)", revision, FQVIEW)


# =============================================================================
# DOWNGRADE
# =============================================================================

def downgrade() -> None:
    """
    Przywraca widok skw_faktury_akceptacja_naglowek do wersji v3 (bez maskowania).
    Aktualizuje SchemaChecksums do revision 0018.

    UWAGA: downgrade to operacja awaryjna.
    Po downgrade wykonaj: docker exec windykacja_redis redis-cli FLUSHDB
    """
    logger.info("[%s] downgrade START → %s (v4 → v3, przywrócenie danych oryginalnych)", revision, FQVIEW)

    op.execute(textwrap.dedent(_VIEW_V3))
    logger.info("[%s] CREATE OR ALTER VIEW (v3) → OK", revision)

    _merge_checksum(alembic_revision="0018")

    logger.info("[%s] downgrade ZAKOŃCZONY — widok %s przywrócony (v3)", revision, FQVIEW)