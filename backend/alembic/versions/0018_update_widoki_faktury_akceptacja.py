# backend/alembic/versions/0018_update_widoki_faktury_akceptacja.py
"""0018_update_widoki_faktury_akceptacja

Aktualizuje dwa widoki modułu Akceptacji Faktur KSeF do wersji v3:

════════════════════════════════════════════════════════════════
  1. dbo.skw_faktury_akceptacja_naglowek  (v2 → v3)
════════════════════════════════════════════════════════════════
  ZMIANY względem migracji 0014:
    a) JOIN:
         STARY: LEFT JOIN dbo.KONTRAHENT k ON k.ID_KONTRAHENTA = bd.ID_KONTRAHENTA
         NOWY:  LEFT JOIN dbo.KONTRAHENT k ON k.KLUCZ = bd.KONTRAHENT_KLUCZ
    b) NazwaKontrahenta:
         STARA: ISNULL(k.NAZWA_PELNA, k.NAZWA)
         NOWA:  CASE WHEN ISNULL(k.NAZWA_PELNA, '') = ''
                     THEN bd.KONTRAHENT_KLUCZ
                     ELSE k.NAZWA_PELNA END
         POWÓD: Jeśli kontrahent nie istnieje w KONTRAHENT (NULL z LEFT JOIN),
                fallback na klucz WAPRO który jest zawsze wypełniony na fakturze.
    c) WHERE — dodano filtr kierunku dokumentu:
         NOWY:  AND bd.KIERUNEK_SYS = 'Z'   (tylko zakupowe — przychodzące)

════════════════════════════════════════════════════════════════
  2. dbo.skw_faktury_akceptacja_pozycje  (v1/stub → v3)
════════════════════════════════════════════════════════════════
  ZMIANY względem migracji 0010:
    a) Źródło danych:
         STARE: dbo.Api_V_BufferDocumentPosition  (widok WAPRO — opcjonalny)
         NOWE:  dbo.BUF_MAPA JOIN dbo.BUF_DOKUMENT (tabele bazowe WAPRO)
    b) Filtr na poziomie widoku:
         NOWY:  bd.PRG_KOD = 1 AND bd.KSEF_ID IS NOT NULL
    c) Mapowanie kolumn bezpośrednio z BUF_MAPA:
         m.LP        → NumerPozycji
         m.NAZWA     → NazwaTowaru
         m.OPIS_POZYCJI → Opis  (zamiast Description z Api_V)

DOWNGRADE:
  naglowek → przywraca v2 (definicja z migracji 0014)
  pozycje  → przywraca wersję Api_V_BufferDocumentPosition (lub stub)

IDEMPOTENTNOŚĆ:
  CREATE OR ALTER VIEW → bezpieczny przy re-run
  MERGE SchemaChecksums → bezpieczny przy re-run

Revision ID: 0018
Revises:     0017
Create Date: 2026-04-22
"""

from __future__ import annotations

import logging
import textwrap
from typing import Final

from alembic import op

# ─────────────────────────────────────────────────────────────────────────────
# Metadane Alembic
# ─────────────────────────────────────────────────────────────────────────────
revision:      str = "0018"
down_revision: str = "0017"
branch_labels       = None
depends_on          = None

# ─────────────────────────────────────────────────────────────────────────────
# Stałe
# ─────────────────────────────────────────────────────────────────────────────
SCHEMA_WAPRO: Final[str] = "dbo"
SCHEMA_EXT:   Final[str] = "dbo_ext"

VIEW_NAGLOWEK: Final[str] = "skw_faktury_akceptacja_naglowek"
VIEW_POZYCJE:  Final[str] = "skw_faktury_akceptacja_pozycje"

FQ_NAGLOWEK: Final[str] = f"[{SCHEMA_WAPRO}].[{VIEW_NAGLOWEK}]"
FQ_POZYCJE:  Final[str] = f"[{SCHEMA_WAPRO}].[{VIEW_POZYCJE}]"

# ─────────────────────────────────────────────────────────────────────────────
# Logger
# ─────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger(f"alembic.migration.{revision}")


# =============================================================================
# WIDOKI v3 — UPGRADE
# =============================================================================

_NAGLOWEK_V3: Final[str] = textwrap.dedent("""\
    CREATE OR ALTER VIEW dbo.skw_faktury_akceptacja_naglowek
    AS
    /*
        WERSJA : v3  (migracja 0018, 2026-04-22)
        POPRZ. : v2  (migracja 0014, 2026-04-14)

        ZMIANY v2 → v3:
          1. JOIN zmieniony z ID_KONTRAHENTA → KLUCZ/KONTRAHENT_KLUCZ
          2. NazwaKontrahenta — fallback na bd.KONTRAHENT_KLUCZ gdy brak nazwy
          3. Dodano filtr bd.KIERUNEK_SYS = 'Z'

        KONWERSJA DAT: dbo.RM_Func_ClarionDateToDateTime (Clarion INT → DATE)
        KOLUMNY WAPRO: KONTRAHENT.KLUCZ, BUF_DOKUMENT.KONTRAHENT_KLUCZ,
                       BUF_DOKUMENT.KIERUNEK_SYS, KONTRAHENT.ADRES_EMAIL,
                       KONTRAHENT.TELEFON_FIRMOWY
    */
    SELECT
        bd.ID_BUF_DOKUMENT,
        bd.KSEF_ID,
        bd.NUMER,
        bd.KOD_STATUSU,
        CASE
            WHEN bd.KOD_STATUSU IS NULL THEN 'NOWY'
            WHEN bd.KOD_STATUSU = 'K'   THEN 'ZATWIERDZONY'
            WHEN bd.KOD_STATUSU = 'A'   THEN 'ZAKSIEGOWANY'
            ELSE bd.KOD_STATUSU
        END AS StatusOpis,
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
            WHEN ISNULL(k.NAZWA_PELNA, '') = ''
            THEN bd.KONTRAHENT_KLUCZ
            ELSE k.NAZWA_PELNA
        END AS NazwaKontrahenta,
        k.ADRES_EMAIL     AS EmailKontrahenta,
        k.TELEFON_FIRMOWY AS TelefonKontrahenta
    FROM dbo.BUF_DOKUMENT AS bd
    LEFT JOIN dbo.KONTRAHENT AS k
        ON k.KLUCZ = bd.KONTRAHENT_KLUCZ
    WHERE bd.PRG_KOD      = 1
      AND bd.KSEF_ID       IS NOT NULL
      AND bd.KIERUNEK_SYS  = 'Z'
""")

_POZYCJE_V3: Final[str] = textwrap.dedent("""\
    CREATE OR ALTER VIEW dbo.skw_faktury_akceptacja_pozycje
    AS
    /*
        WERSJA : v3  (migracja 0018, 2026-04-22)
        POPRZ. : v1  (migracja 0010 — Api_V_BufferDocumentPosition)

        Źródło zmienione na BUF_MAPA JOIN BUF_DOKUMENT.
        Api_V_BufferDocumentPosition nie istnieje w środowisku produkcyjnym GPGKJASLO.
        Filtr spójny z widokiem naglowek: PRG_KOD=1 AND KSEF_ID IS NOT NULL.
    */
    SELECT
        m.ID_BUF_DOKUMENT,
        m.LP              AS NumerPozycji,
        m.NAZWA           AS NazwaTowaru,
        m.ILOSC           AS Ilosc,
        m.JEDNOSTKA       AS Jednostka,
        m.CENA_NETTO      AS CenaNetto,
        m.CENA_BRUTTO     AS CenaBrutto,
        m.WARTOSC_NETTO   AS WartoscNetto,
        m.WARTOSC_BRUTTO  AS WartoscBrutto,
        m.STAWKA_VAT      AS StawkaVAT,
        m.OPIS_POZYCJI    AS Opis
    FROM dbo.BUF_MAPA AS m
    JOIN dbo.BUF_DOKUMENT AS bd
        ON bd.ID_BUF_DOKUMENT = m.ID_BUF_DOKUMENT
    WHERE bd.PRG_KOD    = 1
      AND bd.KSEF_ID     IS NOT NULL
""")


# =============================================================================
# WIDOKI — DOWNGRADE (przywrócenie stanu po 0017 / 0014)
# =============================================================================

# naglowek v2 — stan po migracji 0014
_NAGLOWEK_V2_DOWNGRADE: Final[str] = textwrap.dedent("""\
    CREATE OR ALTER VIEW dbo.skw_faktury_akceptacja_naglowek
    AS
    /* WERSJA v2 (migracja 0014) — przywrócona przez downgrade 0018 */
    SELECT
        bd.ID_BUF_DOKUMENT,
        bd.KSEF_ID,
        bd.NUMER,
        bd.KOD_STATUSU,
        CASE
            WHEN bd.KOD_STATUSU IS NULL THEN 'NOWY'
            WHEN bd.KOD_STATUSU = 'K'   THEN 'ZATWIERDZONY'
            WHEN bd.KOD_STATUSU = 'A'   THEN 'ZAKSIEGOWANY'
            ELSE bd.KOD_STATUSU
        END AS StatusOpis,
        CAST(dbo.RM_Func_ClarionDateToDateTime(bd.DATA_WYSTAWIENIA) AS DATE) AS DataWystawienia,
        CAST(dbo.RM_Func_ClarionDateToDateTime(bd.DATA_OTRZYMANIA)  AS DATE) AS DataOtrzymania,
        CAST(dbo.RM_Func_ClarionDateToDateTime(bd.TERMIN_PLATNOSCI) AS DATE) AS TerminPlatnosci,
        bd.WARTOSC_NETTO,
        bd.WARTOSC_BRUTTO,
        bd.KWOTA_VAT,
        bd.FORMA_PLATNOSCI,
        bd.UWAGI,
        k.ID_KONTRAHENTA,
        ISNULL(k.NAZWA_PELNA, k.NAZWA) AS NazwaKontrahenta,
        k.ADRES_EMAIL                  AS EmailKontrahenta,
        k.TELEFON_FIRMOWY              AS TelefonKontrahenta
    FROM dbo.BUF_DOKUMENT AS bd
    LEFT JOIN dbo.KONTRAHENT AS k
        ON k.ID_KONTRAHENTA = bd.ID_KONTRAHENTA
    WHERE bd.PRG_KOD  = 1
      AND bd.KSEF_ID   IS NOT NULL
""")

# pozycje v1 — stan po migracji 0010 (Api_V_BufferDocumentPosition lub stub)
_POZYCJE_V1_DOWNGRADE_PELNY: Final[str] = (
    "CREATE OR ALTER VIEW dbo.skw_faktury_akceptacja_pozycje "
    "AS "
    "/* WERSJA v1 — przywrócona przez downgrade 0018 */ "
    "SELECT "
    "    p.BufferDocumentId              AS ID_BUF_DOKUMENT, "
    "    p.BufferDocumentPositionIndex   AS NumerPozycji, "
    "    ISNULL(p.ProductName, N'')      AS NazwaTowaru, "
    "    p.Quantity                      AS Ilosc, "
    "    ISNULL(p.Unit, N'')             AS Jednostka, "
    "    p.NetPrice                      AS CenaNetto, "
    "    p.GrossPrice                    AS CenaBrutto, "
    "    p.TotalNetAmount                AS WartoscNetto, "
    "    p.TotalGrossAmount              AS WartoscBrutto, "
    "    ISNULL(p.TaxCode, N'')          AS StawkaVAT, "
    "    p.Description                   AS Opis "
    "FROM dbo.Api_V_BufferDocumentPosition p"
).replace("'", "''")

_POZYCJE_V1_DOWNGRADE_STUB: Final[str] = (
    "CREATE OR ALTER VIEW dbo.skw_faktury_akceptacja_pozycje "
    "AS "
    "/* WERSJA v1 stub — downgrade 0018 — brak Api_V_BufferDocumentPosition */ "
    "SELECT "
    "    CAST(NULL AS INT)            AS ID_BUF_DOKUMENT, "
    "    CAST(NULL AS INT)            AS NumerPozycji, "
    "    CAST(N'' AS NVARCHAR(500))   AS NazwaTowaru, "
    "    CAST(NULL AS DECIMAL(18,4))  AS Ilosc, "
    "    CAST(N'' AS NVARCHAR(50))    AS Jednostka, "
    "    CAST(NULL AS DECIMAL(18,4))  AS CenaNetto, "
    "    CAST(NULL AS DECIMAL(18,4))  AS CenaBrutto, "
    "    CAST(NULL AS DECIMAL(18,4))  AS WartoscNetto, "
    "    CAST(NULL AS DECIMAL(18,4))  AS WartoscBrutto, "
    "    CAST(N'' AS NVARCHAR(10))    AS StawkaVAT, "
    "    CAST(NULL AS NVARCHAR(MAX))  AS Opis "
    "WHERE 1 = 0"
).replace("'", "''")

_POZYCJE_V1_DOWNGRADE_SQL: Final[str] = f"""
IF OBJECT_ID(N'dbo.Api_V_BufferDocumentPosition', N'V') IS NOT NULL
BEGIN
    EXEC sp_executesql N'{_POZYCJE_V1_DOWNGRADE_PELNY}';
    PRINT '[0018-downgrade] pozycje — wersja pelna (Api_V_BufferDocumentPosition)';
END
ELSE
BEGIN
    EXEC sp_executesql N'{_POZYCJE_V1_DOWNGRADE_STUB}';
    PRINT '[0018-downgrade] pozycje — wersja stub (brak Api_V_BufferDocumentPosition)';
END
"""


# =============================================================================
# HELPERS
# =============================================================================

def _check_prerequisites() -> None:
    """
    Weryfikuje obecność obiektów wymaganych przez widoki v3.
    RAISERROR przerywa migrację jeśli któryś brakuje.
    """
    logger.info("[%s] Weryfikacja wymagań wstępnych...", revision)

    op.execute(textwrap.dedent("""\
        DECLARE @brak NVARCHAR(MAX) = N'';

        IF OBJECT_ID(N'[dbo].[BUF_DOKUMENT]', N'U') IS NULL
            SET @brak = @brak + N'  [BRAK] dbo.BUF_DOKUMENT' + CHAR(10);

        IF OBJECT_ID(N'[dbo].[BUF_MAPA]', N'U') IS NULL
            SET @brak = @brak + N'  [BRAK] dbo.BUF_MAPA' + CHAR(10);

        IF OBJECT_ID(N'[dbo].[KONTRAHENT]', N'U') IS NULL
            SET @brak = @brak + N'  [BRAK] dbo.KONTRAHENT' + CHAR(10);

        IF OBJECT_ID(N'[dbo].[RM_Func_ClarionDateToDateTime]', N'FN') IS NULL
            SET @brak = @brak + N'  [BRAK] dbo.RM_Func_ClarionDateToDateTime' + CHAR(10);

        IF OBJECT_ID(N'[dbo_ext].[skw_SchemaChecksums]', N'U') IS NULL
            SET @brak = @brak + N'  [BRAK] dbo_ext.skw_SchemaChecksums' + CHAR(10);

        IF LEN(@brak) > 0
        BEGIN
            DECLARE @err NVARCHAR(MAX) =
                N'[0018] Brakuje wymaganych obiektów bazy danych:' + CHAR(10) + @brak;
            RAISERROR(@err, 16, 1);
        END
        PRINT N'[0018] Weryfikacja wymagań wstępnych — OK.';
    """))

    logger.info("[%s] Weryfikacja wymagań wstępnych → OK", revision)


def _create_view(fq_name: str, ddl: str) -> None:
    """Wykonuje CREATE OR ALTER VIEW. Idempotentny."""
    logger.info("[%s] CREATE OR ALTER VIEW %s ...", revision, fq_name)
    op.execute(ddl)
    logger.info("[%s] CREATE OR ALTER VIEW %s → OK", revision, fq_name)


def _merge_checksum(view_name: str) -> None:
    """
    Rejestruje / aktualizuje checksum widoku w dbo_ext.skw_SchemaChecksums.

    Checksum pobierany subquery z sys.sql_modules w tym samym statement —
    pyodbc nie obsługuje multi-statement batchy (brak DECLARE/SET).
    LastVerifiedAt = NULL — wymusza re-weryfikację przy starcie aplikacji.
    """
    logger.info(
        "[%s] MERGE checksum [%s].[%s] → skw_SchemaChecksums ...",
        revision, SCHEMA_WAPRO, view_name,
    )

    op.execute(textwrap.dedent(f"""\
        MERGE [{SCHEMA_EXT}].[skw_SchemaChecksums] AS target
        USING (
            SELECT
                N'{view_name}'    AS ObjectName,
                N'{SCHEMA_WAPRO}' AS SchemaName,
                N'VIEW'           AS ObjectType,
                (
                    SELECT CHECKSUM(m.definition)
                    FROM   sys.sql_modules AS m
                    JOIN   sys.objects     AS o ON o.object_id = m.object_id
                    WHERE  o.name                    = N'{view_name}'
                      AND  SCHEMA_NAME(o.schema_id)  = N'{SCHEMA_WAPRO}'
                )                 AS Checksum,
                N'{revision}'     AS AlembicRevision,
                NULL              AS LastVerifiedAt,
                GETDATE()         AS Now
        ) AS source
        ON (
                target.[ObjectName]  = source.[ObjectName]
            AND target.[SchemaName]  = source.[SchemaName]
            AND target.[ObjectType]  = source.[ObjectType]
        )
        WHEN MATCHED THEN
            UPDATE SET
                [Checksum]        = source.[Checksum],
                [AlembicRevision] = source.[AlembicRevision],
                [LastVerifiedAt]  = source.[LastVerifiedAt],
                [UpdatedAt]       = source.[Now]
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                [ObjectName],  [SchemaName],  [ObjectType],
                [Checksum],    [AlembicRevision],
                [LastVerifiedAt], [CreatedAt]
            )
            VALUES (
                source.[ObjectName],  source.[SchemaName],  source.[ObjectType],
                source.[Checksum],    source.[AlembicRevision],
                source.[LastVerifiedAt], source.[Now]
            );
    """))

    logger.info(
        "[%s] MERGE checksum [%s].[%s] → OK (AlembicRevision=%s)",
        revision, SCHEMA_WAPRO, view_name, revision,
    )


# =============================================================================
# UPGRADE
# =============================================================================

def upgrade() -> None:
    """
    Aktualizuje oba widoki modułu Akceptacji Faktur do wersji v3.

    Kolejność:
      1. Weryfikacja wymagań wstępnych  ← RAISERROR jeśli brak obiektu
      2. CREATE OR ALTER VIEW naglowek  ← idempotentny
      3. MERGE checksum naglowek        ← LastVerifiedAt=NULL (re-weryfikacja)
      4. CREATE OR ALTER VIEW pozycje   ← idempotentny
      5. MERGE checksum pozycje
    """
    logger.info(
        "[%s] ══ UPGRADE START ══ aktualizacja %s i %s do v3",
        revision, FQ_NAGLOWEK, FQ_POZYCJE,
    )

    _check_prerequisites()

    _create_view(FQ_NAGLOWEK, _NAGLOWEK_V3)
    _merge_checksum(VIEW_NAGLOWEK)

    _create_view(FQ_POZYCJE, _POZYCJE_V3)
    _merge_checksum(VIEW_POZYCJE)

    logger.info(
        "[%s] ══ UPGRADE OK ══ oba widoki v3 aktywne, checksums zaktualizowane",
        revision,
    )


# =============================================================================
# DOWNGRADE
# =============================================================================

def downgrade() -> None:
    """
    Przywraca oba widoki do stanu sprzed tej migracji.

    naglowek → v2  (definicja z migracji 0014)
    pozycje  → v1  (Api_V_BufferDocumentPosition lub stub — logika z 0010)

    UWAGA: Downgrade jest operacją awaryjną.
    schema_integrity.py wymusi re-weryfikację checksumów przy następnym starcie.
    """
    logger.warning(
        "[%s] ══ DOWNGRADE START ══ przywracanie %s → v2, %s → v1",
        revision, FQ_NAGLOWEK, FQ_POZYCJE,
    )

    logger.warning("[%s] Przywracanie %s → v2 ...", revision, FQ_NAGLOWEK)
    op.execute(_NAGLOWEK_V2_DOWNGRADE)
    _merge_checksum(VIEW_NAGLOWEK)
    logger.warning("[%s] %s → v2 OK", revision, FQ_NAGLOWEK)

    logger.warning("[%s] Przywracanie %s → v1 ...", revision, FQ_POZYCJE)
    op.execute(_POZYCJE_V1_DOWNGRADE_SQL)
    _merge_checksum(VIEW_POZYCJE)
    logger.warning("[%s] %s → v1 OK", revision, FQ_POZYCJE)

    logger.warning(
        "[%s] ══ DOWNGRADE OK ══ widoki przywrócone. "
        "UWAGA: schema_integrity.py wymusi re-weryfikację przy następnym starcie.",
        revision,
    )