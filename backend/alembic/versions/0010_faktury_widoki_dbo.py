"""0010_faktury_widoki_dbo

Tworzy dwa widoki w schemacie dbo dla modułu Akceptacji Faktur KSeF:
  - dbo.skw_faktury_akceptacja_naglowek  — nagłówki faktur zakupowych z KSeF
  - dbo.skw_faktury_akceptacja_pozycje   — pozycje faktur (szczegóły i PDF)

Widok pozycji tworzony warunkowo:
  - Jeśli dbo.Api_V_BufferDocumentPosition istnieje → pełna wersja
  - Jeśli nie istnieje (np. środowisko testowe) → stub z poprawnymi kolumnami,
    WHERE 1=0 (zero wierszy, ale aplikacja startuje poprawnie)

Konwersja dat Clarion (INT → DATE): DATEADD(DAY, val, '18991230')

CREATE OR ALTER VIEW — idempotentne, bezpieczne przy re-run.

Revision ID: 0010
Revises: 0009
Create Date: 2026-04-03
"""

from __future__ import annotations

import logging

from alembic import op

revision      = "0010"
down_revision = "0009"
branch_labels = None
depends_on    = None

logger = logging.getLogger(f"alembic.migration.{revision}")


# =============================================================================
# WIDOK 1: dbo.skw_faktury_akceptacja_naglowek
# =============================================================================

_VIEW_NAGLOWEK = """
CREATE OR ALTER VIEW dbo.skw_faktury_akceptacja_naglowek
AS
SELECT
    bd.ID_BUF_DOKUMENT,
    bd.KSEF_ID,
    bd.NUMER,
    bd.KOD_STATUSU,
    CASE
        WHEN bd.KOD_STATUSU IS NULL THEN N'NOWY'
        WHEN bd.KOD_STATUSU = 'K'   THEN N'ZATWIERDZONY'
        WHEN bd.KOD_STATUSU = 'A'   THEN N'ZAKSIEGOWANY'
        ELSE bd.KOD_STATUSU
    END                             AS StatusOpis,
    CASE
        WHEN bd.DATA_WYSTAWIENIA IS NULL OR bd.DATA_WYSTAWIENIA = 0 THEN NULL
        ELSE CAST(DATEADD(DAY, bd.DATA_WYSTAWIENIA, '18991230') AS DATE)
    END                             AS DataWystawienia,
    CASE
        WHEN bd.DATA_OTRZYMANIA IS NULL OR bd.DATA_OTRZYMANIA = 0 THEN NULL
        ELSE CAST(DATEADD(DAY, bd.DATA_OTRZYMANIA, '18991230') AS DATE)
    END                             AS DataOtrzymania,
    CASE
        WHEN bd.TERMIN_PLATNOSCI IS NULL OR bd.TERMIN_PLATNOSCI = 0 THEN NULL
        ELSE CAST(DATEADD(DAY, bd.TERMIN_PLATNOSCI, '18991230') AS DATE)
    END                             AS TerminPlatnosci,
    bd.WARTOSC_NETTO,
    bd.WARTOSC_BRUTTO,
    bd.KWOTA_VAT,
    bd.FORMA_PLATNOSCI,
    bd.UWAGI,
    k.NAZWA                         AS NazwaKontrahenta,
    k.ADRES_EMAIL                   AS EmailKontrahenta,
    k.TELEFON_FIRMOWY               AS TelefonKontrahenta
FROM dbo.BUF_DOKUMENT bd
LEFT JOIN dbo.KONTRAHENT k
    ON k.ID_KONTRAHENTA = bd.ID_KONTRAHENTA
WHERE
    bd.PRG_KOD    = 3
    AND bd.KSEF_ID IS NOT NULL
    AND bd.TYP    = 'Z'
"""


# =============================================================================
# WIDOK 2: dbo.skw_faktury_akceptacja_pozycje — warunkowo
#
# PROBLEM: CREATE OR ALTER VIEW jest parsowany przez MSSQL w całości —
# jeśli Api_V_BufferDocumentPosition nie istnieje, parser odrzuca cały batch
# nawet jeśli opakowany w IF. Rozwiązanie: sp_executesql odkłada parsowanie
# do momentu wykonania, dzięki czemu IF działa prawidłowo.
#
# Escapowanie: .replace("'", "''") zamienia ' → '' dla osadzenia w N'...'
# =============================================================================

_POZYCJE_SQL_PELNY = (
    "CREATE OR ALTER VIEW dbo.skw_faktury_akceptacja_pozycje "
    "AS "
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

_POZYCJE_SQL_STUB = (
    "CREATE OR ALTER VIEW dbo.skw_faktury_akceptacja_pozycje "
    "AS "
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

_VIEW_POZYCJE = f"""
IF OBJECT_ID(N'dbo.Api_V_BufferDocumentPosition', N'V') IS NOT NULL
BEGIN
    EXEC sp_executesql N'{_POZYCJE_SQL_PELNY}';
    PRINT '[0010] skw_faktury_akceptacja_pozycje — wersja pelna';
END
ELSE
BEGIN
    EXEC sp_executesql N'{_POZYCJE_SQL_STUB}';
    PRINT '[0010] skw_faktury_akceptacja_pozycje — wersja stub (brak Api_V_BufferDocumentPosition)';
END
"""


# =============================================================================
# DOWNGRADE
# =============================================================================

_DROP_POZYCJE = """
IF OBJECT_ID(N'dbo.skw_faktury_akceptacja_pozycje', N'V') IS NOT NULL
    DROP VIEW dbo.skw_faktury_akceptacja_pozycje
"""

_DROP_NAGLOWEK = """
IF OBJECT_ID(N'dbo.skw_faktury_akceptacja_naglowek', N'V') IS NOT NULL
    DROP VIEW dbo.skw_faktury_akceptacja_naglowek
"""


# =============================================================================
# UPGRADE
# =============================================================================

def upgrade() -> None:
    logger.info("[0010] START upgrade — widoki dbo modułu faktur KSeF")

    logger.info("[0010] Tworzę: dbo.skw_faktury_akceptacja_naglowek")
    op.execute(_VIEW_NAGLOWEK)
    logger.info("[0010] dbo.skw_faktury_akceptacja_naglowek — OK")

    logger.info("[0010] Tworzę: dbo.skw_faktury_akceptacja_pozycje (warunkowo)")
    op.execute(_VIEW_POZYCJE)
    logger.info("[0010] dbo.skw_faktury_akceptacja_pozycje — OK")

    logger.info("[0010] upgrade ZAKOŃCZONY POMYŚLNIE")


# =============================================================================
# DOWNGRADE
# =============================================================================

def downgrade() -> None:
    logger.info("[0010] START downgrade — usuwam widoki dbo modułu faktur KSeF")

    op.execute(_DROP_POZYCJE)
    logger.info("[0010] dbo.skw_faktury_akceptacja_pozycje — usunięty")

    op.execute(_DROP_NAGLOWEK)
    logger.info("[0010] dbo.skw_faktury_akceptacja_naglowek — usunięty")

    logger.info("[0010] downgrade ZAKOŃCZONY POMYŚLNIE")