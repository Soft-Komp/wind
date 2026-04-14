"""0014_update_view_faktury_akceptacja_naglowek

Aktualizuje widok dbo.skw_faktury_akceptacja_naglowek do wersji v2.

ZMIANY v1 → v2:
  - PRG_KOD: 3 (Fakir) → 1
  - Usunięty filtr: AND bd.TYP = 'Z'  (zakupowe)
  - Dodana kolumna: k.ID_KONTRAHENTA   (brakowało w SELECT v1)
  - Kolumna NazwaKontrahenta bez zmian: ISNULL(k.NAZWA_PELNA, k.NAZWA)

POWÓD ZMIANY:
  Stary widok (PRG_KOD=3 + TYP='Z') zwracał pusty lub błędny zestaw danych
  w środowisku produkcyjnym GPGKJASLO. Zleceniodawca potwierdził
  konieczność zmiany warunków filtrowania.

WIDOK W SCHEMACIE dbo:
  Wyjątek od reguły dbo_ext — konieczne dla dostępu do obiektów WAPRO
  (BUF_DOKUMENT, KONTRAHENT, RM_Func_ClarionDateToDateTime).

SCHEMAT SchemaChecksums:
  Checksum widoku jest aktualizowany przez MERGE po recreate.
  AlembicRevision zmienia się z '007' → '0014'.

IDEMPOTENTNOŚĆ:
  - DROP VIEW IF EXISTS → bezpieczne przy re-run
  - CREATE OR ALTER VIEW → bezpieczne przy re-run
  - MERGE SchemaChecksums → bezpieczne przy re-run

Revision ID: 0014
Revises:     0013
Create Date: 2026-04-14
"""

from __future__ import annotations

import logging
import textwrap
from typing import Final

from alembic import op

# ---------------------------------------------------------------------------
# Metadane Alembic
# ---------------------------------------------------------------------------
revision:      str  = "0014"
down_revision: str  = "0013"
branch_labels       = None
depends_on          = None

# ---------------------------------------------------------------------------
# Stałe — nazwy obiektów
# ---------------------------------------------------------------------------
SCHEMA_WAPRO: Final[str] = "dbo"
SCHEMA_EXT:   Final[str] = "dbo_ext"
VIEW_NAME:    Final[str] = "skw_faktury_akceptacja_naglowek"

# Pełna kwalifikowana nazwa widoku (używana w logach i SQL)
FQVIEW: Final[str] = f"[{SCHEMA_WAPRO}].[{VIEW_NAME}]"

# Rewizja Alembic rejestrowana w skw_SchemaChecksums dla tej wersji widoku
CHECKSUM_REVISION_NEW: Final[str] = "0014"
# Rewizja poprzedniej wersji (zarejestrowana przez 019_faktura_checksums.sql)
CHECKSUM_REVISION_OLD: Final[str] = "007"

# ---------------------------------------------------------------------------
# Logger — spójny z pozostałymi migracjami projektu
# ---------------------------------------------------------------------------
logger = logging.getLogger(f"alembic.migration.{revision}")


# ===========================================================================
# DEFINICJA WIDOKU — wersja v2 (NOWA, po zmianie)
# ===========================================================================

_VIEW_DDL_V2: Final[str] = textwrap.dedent("""\
    CREATE OR ALTER VIEW dbo.skw_faktury_akceptacja_naglowek
    AS
    /*
        WERSJA:  v2  (migracja 0014, 2026-04-14)
        POPRZ.:  v1  (018_faktura_widoki_dbo.sql / checksum 007)

        LOGIKA BIZNESOWA:
          - PRG_KOD = 1          → zmieniony z 3 na żądanie zleceniodawcy
          - KSEF_ID IS NOT NULL  → tylko faktury z KSeF (elektroniczny identyfikator)
          - Brak filtra TYP      → usunięty filtr AND bd.TYP = 'Z' (zakupowe)

        KONWERSJA DAT:
          WAPRO przechowuje daty jako INT (format Clarion):
          liczba dni od 1899-12-30. Konwersja przez dbo.RM_Func_ClarionDateToDateTime.
          Wartość NULL lub 0 → funkcja zwraca NULL (obsługa po stronie serwisu Python).

        KONTRAHENT:
          LEFT JOIN — faktura może nie mieć przypisanego kontrahenta w WAPRO.
          W takim przypadku pola NazwaKontrahenta / Email / Telefon będą NULL.
          NazwaKontrahenta: preferuje NAZWA_PELNA, fallback na NAZWA.

        KOLUMNY ZWERYFIKOWANE W SSMS:
          KONTRAHENT.ADRES_EMAIL     (nie EMAIL, nie MAIL)
          KONTRAHENT.TELEFON_FIRMOWY (nie TELEFON)
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
        ISNULL(k.NAZWA_PELNA, k.NAZWA)  AS NazwaKontrahenta,
        k.ADRES_EMAIL                   AS EmailKontrahenta,
        k.TELEFON_FIRMOWY               AS TelefonKontrahenta
    FROM dbo.BUF_DOKUMENT AS bd
    LEFT JOIN dbo.KONTRAHENT AS k
        ON k.ID_KONTRAHENTA = bd.ID_KONTRAHENTA
    WHERE
        bd.PRG_KOD    = 1
        AND bd.KSEF_ID IS NOT NULL;
""")


# ===========================================================================
# DEFINICJA WIDOKU — wersja v1 (STARA, używana przy downgrade)
# Źródło: database/ddl/018_faktura_widoki_dbo.sql
# ===========================================================================

_VIEW_DDL_V1: Final[str] = textwrap.dedent("""\
    CREATE OR ALTER VIEW dbo.skw_faktury_akceptacja_naglowek
    AS
    /*
        WERSJA:  v1  (018_faktura_widoki_dbo.sql / checksum 007)
        PRZYWRÓCONA PRZEZ: migracja 0014 downgrade

        LOGIKA BIZNESOWA:
          - PRG_KOD = 3  → tylko Fakir
          - KSEF_ID IS NOT NULL
          - TYP = 'Z'    → tylko faktury zakupowe
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
        ISNULL(k.NAZWA_PELNA, k.NAZWA)  AS NazwaKontrahenta,
        k.ADRES_EMAIL                   AS EmailKontrahenta,
        k.TELEFON_FIRMOWY               AS TelefonKontrahenta
    FROM dbo.BUF_DOKUMENT AS bd
    LEFT JOIN dbo.KONTRAHENT AS k
        ON k.ID_KONTRAHENTA = bd.ID_KONTRAHENTA
    WHERE
        bd.PRG_KOD    = 3
        AND bd.KSEF_ID IS NOT NULL
        AND bd.TYP    = 'Z';
""")


# ===========================================================================
# SQL — sprawdzenie wymagań wstępnych
# ===========================================================================

_SQL_CHECK_PREREQUISITES: Final[str] = textwrap.dedent("""\
    -- Weryfikacja wymagań wstępnych migracji 0014
    DECLARE @missing NVARCHAR(MAX) = N'';

    -- 1. Tabela BUF_DOKUMENT (WAPRO)
    IF OBJECT_ID(N'[dbo].[BUF_DOKUMENT]', N'U') IS NULL
        SET @missing = @missing + N'  [BRAK] dbo.BUF_DOKUMENT' + CHAR(10);

    -- 2. Tabela KONTRAHENT (WAPRO)
    IF OBJECT_ID(N'[dbo].[KONTRAHENT]', N'U') IS NULL
        SET @missing = @missing + N'  [BRAK] dbo.KONTRAHENT' + CHAR(10);

    -- 3. Funkcja konwersji dat Clarion (WAPRO)
    IF OBJECT_ID(N'[dbo].[RM_Func_ClarionDateToDateTime]', N'FN') IS NULL
        SET @missing = @missing + N'  [BRAK] dbo.RM_Func_ClarionDateToDateTime' + CHAR(10);

    -- 4. Tabela SchemaChecksums (projekt)
    IF OBJECT_ID(N'[dbo_ext].[skw_SchemaChecksums]', N'U') IS NULL
        SET @missing = @missing + N'  [BRAK] dbo_ext.skw_SchemaChecksums' + CHAR(10);

    IF LEN(@missing) > 0
    BEGIN
        DECLARE @err NVARCHAR(MAX) =
            N'[0014] Brakuje wymaganych obiektów bazy danych:' + CHAR(10) + @missing;
        RAISERROR(@err, 16, 1);
    END
""")


# ===========================================================================
# SQL — usunięcie starego widoku (jeśli istnieje)
# ===========================================================================

_SQL_DROP_VIEW: Final[str] = textwrap.dedent("""\
    IF OBJECT_ID(N'[dbo].[skw_faktury_akceptacja_naglowek]', N'V') IS NOT NULL
    BEGIN
        DROP VIEW [dbo].[skw_faktury_akceptacja_naglowek];
    END
""")


# ===========================================================================
# SQL — aktualizacja SchemaChecksums po recreate widoku
# ===========================================================================

def _sql_merge_checksum(alembic_revision: str) -> str:
    """
    Zwraca SQL MERGE aktualizujący checksum widoku w skw_SchemaChecksums.

    Checksum obliczany dynamicznie z sys.sql_modules — zawsze aktualny
    względem faktycznie zainstalowanej definicji widoku.
    AlembicRevision ustawiana na przekazaną wartość revision.
    """
    return textwrap.dedent(f"""\
        MERGE [dbo_ext].[skw_SchemaChecksums] AS target
        USING (
            SELECT
                o.name                   AS ObjectName,
                N'VIEW'                  AS ObjectType,
                CHECKSUM(m.definition)   AS Checksum,
                N'{alembic_revision}'    AS AlembicRevision
            FROM sys.sql_modules  AS m
            JOIN sys.objects      AS o ON m.object_id = o.object_id
            WHERE
                SCHEMA_NAME(o.schema_id) = 'dbo'
                AND o.name               = 'skw_faktury_akceptacja_naglowek'
                AND o.type               = 'V'
        ) AS source
            ON  target.ObjectName = source.ObjectName
            AND target.ObjectType = source.ObjectType
        WHEN MATCHED THEN
            UPDATE SET
                target.Checksum        = source.Checksum,
                target.AlembicRevision = source.AlembicRevision,
                target.LastVerifiedAt  = GETDATE(),
                target.UpdatedAt       = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (ObjectName, ObjectType, Checksum, AlembicRevision, LastVerifiedAt, CreatedAt)
            VALUES (
                source.ObjectName,
                source.ObjectType,
                source.Checksum,
                source.AlembicRevision,
                GETDATE(),
                GETDATE()
            );
    """)


# ===========================================================================
# UPGRADE — v1 → v2
# ===========================================================================


def upgrade() -> None:
    """
    Aktualizuje dbo.skw_faktury_akceptacja_naglowek do wersji v2.

    Kroki:
      1. Weryfikacja wymagań wstępnych (RAISERROR jeśli brak)
      2. DROP VIEW IF EXISTS  (usunięcie v1)
      3. CREATE OR ALTER VIEW (instalacja v2)
      4. Weryfikacja rowcount  (SELECT COUNT(*) — ostrzeżenie przy 0 wierszach)
      5. MERGE SchemaChecksums (aktualizacja checksumu + AlembicRevision=0014)
    """
    logger.info(
        "[%s] UPGRADE → aktualizacja widoku %s do v2 "
        "(PRG_KOD=1, brak TYP='Z', dodano ID_KONTRAHENTA)",
        revision, FQVIEW,
    )

    # ------------------------------------------------------------------
    # KROK 1: Weryfikacja wymagań wstępnych
    # ------------------------------------------------------------------
    logger.info("[%s] Krok 1/5 — weryfikacja wymagań wstępnych", revision)
    op.execute(_SQL_CHECK_PREREQUISITES)
    logger.info("[%s] Krok 1/5 — OK (wszystkie obiekty WAPRO dostępne)", revision)

    # ------------------------------------------------------------------
    # KROK 2: Usunięcie starego widoku
    # ------------------------------------------------------------------
    logger.info("[%s] Krok 2/5 — DROP VIEW IF EXISTS %s", revision, FQVIEW)
    op.execute(_SQL_DROP_VIEW)
    logger.info("[%s] Krok 2/5 — OK (widok v1 usunięty lub nie istniał)", revision)

    # ------------------------------------------------------------------
    # KROK 3: Utworzenie nowego widoku (v2)
    # ------------------------------------------------------------------
    logger.info("[%s] Krok 3/5 — CREATE OR ALTER VIEW %s (v2)", revision, FQVIEW)
    op.execute(_VIEW_DDL_V2)
    logger.info("[%s] Krok 3/5 — OK (widok v2 zainstalowany)", revision)

    # ------------------------------------------------------------------
    # KROK 4: Weryfikacja — SELECT COUNT(*) jako smoke test
    # Alembic nie zwraca wyników, ale wyjątek SQL przerwie migrację
    # jeśli widok jest niepoprawny. Logujemy ostrzeżenie przy 0 wierszach
    # (może oznaczać brak danych z PRG_KOD=1 — wymaga weryfikacji w SSMS).
    # ------------------------------------------------------------------
    logger.info("[%s] Krok 4/5 — smoke test SELECT COUNT(*) FROM %s", revision, FQVIEW)
    op.execute(textwrap.dedent(f"""\
        DECLARE @cnt INT;
        SELECT @cnt = COUNT(*)
        FROM [{SCHEMA_WAPRO}].[{VIEW_NAME}];

        IF @cnt = 0
        BEGIN
            -- Nie przerywamy migracji — może być baza testowa bez danych
            -- Logujemy ostrzeżenie dla administratora
            PRINT N'[WARN][0014] Widok {FQVIEW} zwraca 0 wierszy. '
                + N'Sprawdź czy PRG_KOD=1 jest poprawne dla tej bazy.';
        END
        ELSE
        BEGIN
            PRINT N'[OK][0014] Widok {FQVIEW} zwraca ' + CAST(@cnt AS NVARCHAR) + N' wierszy.';
        END
    """))
    logger.info("[%s] Krok 4/5 — OK (widok odpowiada na zapytanie bez błędu)", revision)

    # ------------------------------------------------------------------
    # KROK 5: Aktualizacja SchemaChecksums
    # ------------------------------------------------------------------
    logger.info(
        "[%s] Krok 5/5 — MERGE SchemaChecksums (AlembicRevision=%s)",
        revision, CHECKSUM_REVISION_NEW,
    )
    op.execute(_sql_merge_checksum(CHECKSUM_REVISION_NEW))
    logger.info(
        "[%s] Krok 5/5 — OK (SchemaChecksums zaktualizowany, revision=%s)",
        revision, CHECKSUM_REVISION_NEW,
    )

    logger.info(
        "[%s] UPGRADE zakończony pomyślnie — %s v2 aktywny",
        revision, FQVIEW,
    )


# ===========================================================================
# DOWNGRADE — v2 → v1
# ===========================================================================


def downgrade() -> None:
    """
    Przywraca dbo.skw_faktury_akceptacja_naglowek do wersji v1.

    Kroki:
      1. DROP VIEW IF EXISTS  (usunięcie v2)
      2. CREATE OR ALTER VIEW (przywrócenie v1 z 018_faktura_widoki_dbo.sql)
      3. MERGE SchemaChecksums (przywrócenie AlembicRevision='007')

    UWAGA:
      Downgrade to operacja awaryjna — wykonuj tylko jeśli wiesz co robisz.
      Po downgrade aplikacja może zwracać błędne dane (PRG_KOD=3 + TYP='Z'
      były przyczyną problemu produkcyjnego).
    """
    logger.warning(
        "[%s] DOWNGRADE → przywracam %s do v1 (PRG_KOD=3, TYP='Z'). "
        "UWAGA: v1 zwracała błędne dane produkcyjne!",
        revision, FQVIEW,
    )

    # ------------------------------------------------------------------
    # KROK 1: Usunięcie widoku v2
    # ------------------------------------------------------------------
    logger.info("[%s] DOWNGRADE Krok 1/3 — DROP VIEW IF EXISTS %s", revision, FQVIEW)
    op.execute(_SQL_DROP_VIEW)
    logger.info("[%s] DOWNGRADE Krok 1/3 — OK", revision)

    # ------------------------------------------------------------------
    # KROK 2: Przywrócenie widoku v1
    # ------------------------------------------------------------------
    logger.info("[%s] DOWNGRADE Krok 2/3 — CREATE OR ALTER VIEW %s (v1)", revision, FQVIEW)
    op.execute(_VIEW_DDL_V1)
    logger.info("[%s] DOWNGRADE Krok 2/3 — OK (widok v1 przywrócony)", revision)

    # ------------------------------------------------------------------
    # KROK 3: Przywrócenie checksumu do revision '007'
    # ------------------------------------------------------------------
    logger.info(
        "[%s] DOWNGRADE Krok 3/3 — MERGE SchemaChecksums (AlembicRevision=%s)",
        revision, CHECKSUM_REVISION_OLD,
    )
    op.execute(_sql_merge_checksum(CHECKSUM_REVISION_OLD))
    logger.info(
        "[%s] DOWNGRADE Krok 3/3 — OK (revision przywrócona do %s)",
        revision, CHECKSUM_REVISION_OLD,
    )

    logger.warning(
        "[%s] DOWNGRADE zakończony — %s v1 aktywny. "
        "Aplikacja może zwracać błędne dane. Monitoruj logi!",
        revision, FQVIEW,
    )