# backend/alembic/versions/0022_func_odsetki_rozrachunku.py
"""0022_func_odsetki_rozrachunku

Tworzy funkcję skalarną dbo.skw_Func_OdsetkiRozrachunku.

════════════════════════════════════════════════════════════════
Kroki upgrade:
  1. Weryfikacja zależności (skw_rozrachunki_faktur, dbo.ODSETKI,
     dbo.RM_Func_ClarionDateToDateTime)
  2. Rozszerzenie CK_skw_SchemaChecksums_ObjectType o 'FUNCTION'
  3. CREATE OR ALTER FUNCTION dbo.skw_Func_OdsetkiRozrachunku
  4. MERGE checksumu do dbo_ext.skw_SchemaChecksums

════════════════════════════════════════════════════════════════
Funkcja:
  dbo.skw_Func_OdsetkiRozrachunku(@idrozrachunku NUMERIC(18,0),
                                   @do_daty DATE = NULL)
  RETURNS DECIMAL(15,2)

  Oblicza odsetki ustawowe dla jednego rozrachunku.
  Źródła danych:
    - dbo.skw_rozrachunki_faktur  (kwota, termin)
    - dbo.ODSETKI                 (stopy ustawowe, ID_TABELI_ODSETEK=1)
    - dbo.RM_Func_ClarionDateToDateTime (konwersja dat Clarion)

════════════════════════════════════════════════════════════════
Downgrade:
  DROP FUNCTION IF EXISTS + DELETE z skw_SchemaChecksums
  + przywrócenie constraint bez 'FUNCTION'

IDEMPOTENTNOŚĆ:
  CREATE OR ALTER FUNCTION — bezpieczny przy re-run.

Revision ID: 0022
Revises:     0021
Create Date: 2026-04-27
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
revision:      str = "0022"
down_revision: str = "0021"
branch_labels       = None
depends_on          = None

SCHEMA_WAPRO: Final[str] = "dbo"
SCHEMA_EXT:   Final[str] = "dbo_ext"
FUNC_NAME:    Final[str] = "skw_Func_OdsetkiRozrachunku"

logger = logging.getLogger(f"alembic.migration.{revision}")

# ─────────────────────────────────────────────────────────────────────────────
# Wartości constraint ObjectType
# ─────────────────────────────────────────────────────────────────────────────
_OBJECT_TYPES_PRZED: Final[str] = "N'VIEW', N'PROCEDURE', N'INDEX'"
_OBJECT_TYPES_PO:    Final[str] = "N'VIEW', N'PROCEDURE', N'INDEX', N'FUNCTION'"

# ─────────────────────────────────────────────────────────────────────────────
# DDL funkcji
# ─────────────────────────────────────────────────────────────────────────────
_FUNC_DDL: Final[str] = textwrap.dedent("""\
    CREATE OR ALTER FUNCTION dbo.skw_Func_OdsetkiRozrachunku
    (
        @idrozrachunku  NUMERIC(18,0),
        @do_daty        DATE = NULL
    )
    RETURNS DECIMAL(15,2)
    AS
    BEGIN
        DECLARE @wynik      DECIMAL(15,2) = 0
        DECLARE @do         DATE          = ISNULL(@do_daty, CAST(GETDATE() AS DATE))
        DECLARE @termin     DATE
        DECLARE @kwota      DECIMAL(15,2)

        -- Pobierz termin płatności i kwotę pozostałą do spłaty
        SELECT
            @termin = r.TerminPlatnosci,
            @kwota  = r.KwotaPozostala
        FROM dbo.skw_rozrachunki_faktur AS r
        WHERE r.ID_ROZRACHUNKU = @idrozrachunku

        -- Jeśli rozrachunek nie istnieje lub nie ma terminu — zwróć 0
        IF @termin IS NULL OR @kwota IS NULL OR @kwota <= 0 OR @termin >= @do
            RETURN 0

        -- Suma odsetek po wszystkich okresach stóp ustawowych
        SELECT @wynik = ISNULL(SUM(
            @kwota
            * (o.STOPA / 100.0)
            * (DATEDIFF(DAY,
                -- Początek okresu: max(termin płatności, początek okresu stopy)
                CASE
                    WHEN CAST(dbo.RM_Func_ClarionDateToDateTime(o.OD_DNIA) AS DATE) > @termin
                    THEN CAST(dbo.RM_Func_ClarionDateToDateTime(o.OD_DNIA) AS DATE)
                    ELSE @termin
                END,
                -- Koniec okresu: min(do_daty, koniec okresu stopy)
                CASE
                    WHEN o.DO_DNIA = 0
                    THEN @do
                    WHEN CAST(dbo.RM_Func_ClarionDateToDateTime(o.DO_DNIA) AS DATE) < @do
                    THEN CAST(dbo.RM_Func_ClarionDateToDateTime(o.DO_DNIA) AS DATE)
                    ELSE @do
                END
               ) / 365.0)
        ), 0)
        FROM dbo.ODSETKI AS o
        WHERE o.ID_TABELI_ODSETEK = 1
          -- Tylko okresy które nakładają się z czasem przeterminowania
          AND CAST(dbo.RM_Func_ClarionDateToDateTime(o.OD_DNIA) AS DATE) <= @do
          AND (
                o.DO_DNIA = 0
                OR CAST(dbo.RM_Func_ClarionDateToDateTime(o.DO_DNIA) AS DATE) >= @termin
              )
          -- Tylko gdy nakładający się okres jest dodatni
          AND DATEDIFF(DAY,
                CASE
                    WHEN CAST(dbo.RM_Func_ClarionDateToDateTime(o.OD_DNIA) AS DATE) > @termin
                    THEN CAST(dbo.RM_Func_ClarionDateToDateTime(o.OD_DNIA) AS DATE)
                    ELSE @termin
                END,
                CASE
                    WHEN o.DO_DNIA = 0
                    THEN @do
                    WHEN CAST(dbo.RM_Func_ClarionDateToDateTime(o.DO_DNIA) AS DATE) < @do
                    THEN CAST(dbo.RM_Func_ClarionDateToDateTime(o.DO_DNIA) AS DATE)
                    ELSE @do
                END
              ) > 0

        RETURN ISNULL(@wynik, 0)
    END
""")


# =============================================================================
# Helpers prywatne
# =============================================================================

def _raw_scalar(bind: Any, sql: str) -> Any:
    result = bind.execute(sa.text(sql))
    row = result.fetchone()
    return row[0] if row else None


def _krok1_weryfikacja_zaleznosci(bind: Any) -> None:
    """
    Sprawdza czy wszystkie zależności funkcji istnieją w bazie.
    Przerywa migrację z RuntimeError jeśli czegoś brakuje.
    """
    logger.info("[%s] Krok 1/4 — weryfikacja zależności", revision)

    braki: list[str] = []

    # Widok skw_rozrachunki_faktur
    wynik = _raw_scalar(bind, """
        SELECT COUNT(*)
        FROM sys.objects o
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE s.name = N'dbo'
          AND o.name = N'skw_rozrachunki_faktur'
          AND o.type = 'V'
    """)
    if not wynik:
        braki.append("VIEW dbo.skw_rozrachunki_faktur")

    # Tabela dbo.ODSETKI
    wynik = _raw_scalar(bind, """
        SELECT COUNT(*)
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = N'dbo' AND t.name = N'ODSETKI'
    """)
    if not wynik:
        braki.append("TABLE dbo.ODSETKI")

    # Funkcja RM_Func_ClarionDateToDateTime
    wynik = _raw_scalar(bind, """
        SELECT COUNT(*)
        FROM sys.objects o
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE s.name = N'dbo'
          AND o.name = N'RM_Func_ClarionDateToDateTime'
          AND o.type IN ('FN', 'TF', 'IF')
    """)
    if not wynik:
        braki.append("FUNCTION dbo.RM_Func_ClarionDateToDateTime")

    # Kolumna ID_TABELI_ODSETEK w dbo.ODSETKI
    wynik = _raw_scalar(bind, """
        SELECT COUNT(*)
        FROM sys.columns c
        JOIN sys.tables t  ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = N'dbo'
          AND t.name = N'ODSETKI'
          AND c.name = N'ID_TABELI_ODSETEK'
    """)
    if not wynik:
        braki.append("COLUMN dbo.ODSETKI.ID_TABELI_ODSETEK")

    if braki:
        msg = (
            f"[{revision}] Brakuje wymaganych obiektów bazy: "
            + ", ".join(braki)
        )
        logger.critical(msg)
        raise RuntimeError(msg)

    logger.info("[%s] Krok 1/4 — zależności OK", revision)


def _krok2_constraint_object_type(bind: Any) -> None:
    """
    Rozszerza CK_skw_SchemaChecksums_ObjectType o wartość 'FUNCTION'.
    Idempotentny — nie wykona nic jeśli constraint już zawiera 'FUNCTION'.
    """
    logger.info(
        "[%s] Krok 2/4 — rozszerzenie CK_skw_SchemaChecksums_ObjectType o 'FUNCTION'",
        revision,
    )

    # Sprawdź czy już zawiera FUNCTION
    juz_ma = _raw_scalar(bind, f"""
        SELECT COUNT(*)
        FROM sys.check_constraints cc
        JOIN sys.tables  t ON cc.parent_object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name  = N'{SCHEMA_EXT}'
          AND t.name  = N'skw_SchemaChecksums'
          AND cc.name = N'CK_skw_SchemaChecksums_ObjectType'
          AND cc.definition LIKE N'%FUNCTION%'
    """)

    if juz_ma:
        logger.info("[%s] Krok 2/4 — constraint już zawiera FUNCTION, pomijam", revision)
        return

    # Usuń stary constraint
    bind.execute(sa.text(f"""
        IF EXISTS (
            SELECT 1
            FROM sys.check_constraints cc
            JOIN sys.tables  t ON cc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name  = N'{SCHEMA_EXT}'
              AND t.name  = N'skw_SchemaChecksums'
              AND cc.name = N'CK_skw_SchemaChecksums_ObjectType'
        )
        BEGIN
            ALTER TABLE [{SCHEMA_EXT}].[skw_SchemaChecksums]
                DROP CONSTRAINT [CK_skw_SchemaChecksums_ObjectType];
            PRINT N'[0022] Stary CK_skw_SchemaChecksums_ObjectType usunięty.';
        END
    """))

    # Dodaj nowy constraint z FUNCTION
    bind.execute(sa.text(f"""
        ALTER TABLE [{SCHEMA_EXT}].[skw_SchemaChecksums]
            ADD CONSTRAINT [CK_skw_SchemaChecksums_ObjectType]
            CHECK ([ObjectType] IN ({_OBJECT_TYPES_PO}));
        PRINT N'[0022] Nowy CK_skw_SchemaChecksums_ObjectType z FUNCTION dodany.';
    """))

    logger.info("[%s] Krok 2/4 — constraint zaktualizowany", revision)


def _krok3_create_function(bind: Any) -> None:
    """Tworzy / aktualizuje funkcję. CREATE OR ALTER — idempotentny."""
    logger.info(
        "[%s] Krok 3/4 — CREATE OR ALTER FUNCTION %s.%s",
        revision, SCHEMA_WAPRO, FUNC_NAME,
    )
    bind.execute(sa.text(_FUNC_DDL))
    logger.info("[%s] Krok 3/4 — funkcja OK", revision)


def _krok4_register_checksum(bind: Any) -> None:
    """
    Rejestruje checksum funkcji w dbo_ext.skw_SchemaChecksums.
    ObjectType = 'FUNCTION'.
    LastVerifiedAt = NULL — watchdog zweryfikuje przy starcie (gdy zostanie
    rozszerzony o śledzenie funkcji; na razie tylko rejestracja).
    """
    logger.info(
        "[%s] Krok 4/4 — MERGE checksum %s.%s → skw_SchemaChecksums",
        revision, SCHEMA_WAPRO, FUNC_NAME,
    )

    checksum = _raw_scalar(bind, f"""
        SELECT CHECKSUM(m.definition)
        FROM   sys.sql_modules AS m
        JOIN   sys.objects     AS o ON o.object_id = m.object_id
        JOIN   sys.schemas     AS s ON s.schema_id = o.schema_id
        WHERE  s.name = N'{SCHEMA_WAPRO}'
          AND  o.name = N'{FUNC_NAME}'
    """)

    if checksum is None:
        msg = (
            f"[{revision}] Nie można odczytać CHECKSUM funkcji "
            f"{SCHEMA_WAPRO}.{FUNC_NAME} — funkcja nie istnieje w sys.sql_modules."
        )
        logger.critical(msg)
        raise RuntimeError(msg)

    logger.debug("[%s] CHECKSUM(%s) = %s", revision, FUNC_NAME, checksum)

    bind.execute(sa.text(f"""
        MERGE [{SCHEMA_EXT}].[skw_SchemaChecksums] AS tgt
        USING (
            SELECT
                N'{SCHEMA_WAPRO}'  AS SchemaName,
                N'{FUNC_NAME}'     AS ObjectName,
                N'FUNCTION'        AS ObjectType,
                {checksum}         AS Checksum,
                N'{revision}'      AS AlembicRevision,
                NULL               AS LastVerifiedAt,
                GETDATE()          AS UpdatedAt
        ) AS src
        ON  tgt.SchemaName = src.SchemaName
        AND tgt.ObjectName = src.ObjectName
        AND tgt.ObjectType = src.ObjectType
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SchemaName, ObjectName, ObjectType,
                    Checksum, AlembicRevision, LastVerifiedAt, UpdatedAt)
            VALUES (src.SchemaName, src.ObjectName, src.ObjectType,
                    src.Checksum,  src.AlembicRevision,
                    src.LastVerifiedAt, src.UpdatedAt)
        WHEN MATCHED THEN
            UPDATE SET
                tgt.Checksum         = src.Checksum,
                tgt.AlembicRevision  = src.AlembicRevision,
                tgt.LastVerifiedAt   = src.LastVerifiedAt,
                tgt.UpdatedAt        = src.UpdatedAt;
    """))

    logger.info(
        "[%s] SchemaChecksums MERGE OK — %s.%s FUNCTION (checksum=%s)",
        revision, SCHEMA_WAPRO, FUNC_NAME, checksum,
    )


# =============================================================================
# UPGRADE
# =============================================================================

def upgrade() -> None:
    logger.info("[%s] ── UPGRADE START ──", revision)
    bind = op.get_bind()

    _krok1_weryfikacja_zaleznosci(bind)
    _krok2_constraint_object_type(bind)
    _krok3_create_function(bind)
    _krok4_register_checksum(bind)

    logger.info("[%s] ── UPGRADE OK ──", revision)


# =============================================================================
# DOWNGRADE
# =============================================================================

def downgrade() -> None:
    logger.warning("[%s] ── DOWNGRADE START ──", revision)
    bind = op.get_bind()

    # Usuń checksum z rejestru
    bind.execute(sa.text(f"""
        DELETE FROM [{SCHEMA_EXT}].[skw_SchemaChecksums]
        WHERE  SchemaName = N'{SCHEMA_WAPRO}'
          AND  ObjectName = N'{FUNC_NAME}'
          AND  ObjectType = N'FUNCTION';
    """))
    logger.warning("[%s] DOWNGRADE: checksum usunięty", revision)

    # Usuń funkcję
    bind.execute(sa.text(f"""
        IF EXISTS (
            SELECT 1
            FROM sys.objects o
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA_WAPRO}'
              AND o.name = N'{FUNC_NAME}'
              AND o.type = 'FN'
        )
        BEGIN
            DROP FUNCTION [{SCHEMA_WAPRO}].[{FUNC_NAME}];
            PRINT N'[0022] DOWNGRADE: funkcja {FUNC_NAME} usunięta.';
        END
    """))
    logger.warning("[%s] DOWNGRADE: funkcja usunięta", revision)

    # Cofnij constraint do wersji bez FUNCTION
    bind.execute(sa.text(f"""
        IF EXISTS (
            SELECT 1
            FROM sys.check_constraints cc
            JOIN sys.tables  t ON cc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name  = N'{SCHEMA_EXT}'
              AND t.name  = N'skw_SchemaChecksums'
              AND cc.name = N'CK_skw_SchemaChecksums_ObjectType'
              AND cc.definition LIKE N'%FUNCTION%'
        )
        BEGIN
            ALTER TABLE [{SCHEMA_EXT}].[skw_SchemaChecksums]
                DROP CONSTRAINT [CK_skw_SchemaChecksums_ObjectType];
            ALTER TABLE [{SCHEMA_EXT}].[skw_SchemaChecksums]
                ADD CONSTRAINT [CK_skw_SchemaChecksums_ObjectType]
                CHECK ([ObjectType] IN ({_OBJECT_TYPES_PRZED}));
            PRINT N'[0022] DOWNGRADE: constraint przywrócony bez FUNCTION.';
        END
    """))
    logger.warning("[%s] DOWNGRADE: constraint przywrócony", revision)

    logger.warning("[%s] ── DOWNGRADE OK ──", revision)