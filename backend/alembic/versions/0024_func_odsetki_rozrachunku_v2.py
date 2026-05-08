# backend/alembic/versions/0023_func_odsetki_rozrachunku_v2.py
"""0023_func_odsetki_rozrachunku_v2

Aktualizuje funkcję skalarną dbo.skw_Func_OdsetkiRozrachunku.

════════════════════════════════════════════════════════════════
Powód zmiany:
  Poprzednia implementacja (0022) obliczała odsetki własną logiką
  na tabeli dbo.ODSETKI. Nowa wersja deleguje obliczenie do
  dbo.AP_Func_PodajKwoteOdsetek — funkcji WAPRO która jest
  autorytatywnym źródłem kalkulacji odsetek w systemie.

  Kolejność parametrów AP_Func_PodajKwoteOdsetek:
    1. @idrozrachunku  NUMERIC
    2. @ido            INT  (ID_TABELI_ODSETEK = 1)
    3. @today          INT  (data w formacie Clarion)
    4. @czy_wal        INT  (0 = PLN)
    5. @czy_naliczac_za_zaplaty INT (0)

Kroki upgrade:
  1. Weryfikacja zależności (AP_Func_PodajKwoteOdsetek)
  2. DROP FUNCTION IF EXISTS + CREATE FUNCTION
  3. MERGE checksumu do dbo_ext.skw_SchemaChecksums

Downgrade:
  DROP + przywrócenie poprzedniej implementacji (v1 z 0022).
  Aktualizuje checksum w skw_SchemaChecksums.

IDEMPOTENTNOŚĆ:
  DROP IF EXISTS + CREATE — bezpieczny przy re-run.

Revision ID: 0024
Revises:     0023
Create Date: 2026-04-29
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
revision:      str = "0024"
down_revision: str = "0023"
branch_labels       = None
depends_on          = None

SCHEMA_WAPRO: Final[str] = "dbo"
SCHEMA_EXT:   Final[str] = "dbo_ext"
FUNC_NAME:    Final[str] = "skw_Func_OdsetkiRozrachunku"

logger = logging.getLogger(f"alembic.migration.{revision}")

# ─────────────────────────────────────────────────────────────────────────────
# DDL — DROP (wspólny dla upgrade i downgrade)
# ─────────────────────────────────────────────────────────────────────────────
_FUNC_DROP: Final[str] = (
    f"DROP FUNCTION IF EXISTS [{SCHEMA_WAPRO}].[{FUNC_NAME}]"
)

# ─────────────────────────────────────────────────────────────────────────────
# DDL — nowa implementacja v2
# Deleguje do AP_Func_PodajKwoteOdsetek (autorytatywna funkcja WAPRO).
# Parametry w kolejności: @ido, @today (Clarion INT), @czy_wal, @czy_naliczac
# ─────────────────────────────────────────────────────────────────────────────
_FUNC_CREATE_V2: Final[str] = textwrap.dedent("""\
    CREATE FUNCTION dbo.skw_Func_OdsetkiRozrachunku
    (
        @idrozrachunku  NUMERIC(18,0),
        @do_daty        DATE = NULL
    )
    RETURNS DECIMAL(15,2)
    AS
    BEGIN
        -- Konwersja daty do formatu Clarion (INT).
        -- CONVERT(..., 112) → string 'YYYYMMDD' → DATETIME → +36163 → INT Clarion.
        DECLARE @dzis INT = CAST(
            CAST(CONVERT(VARCHAR, ISNULL(@do_daty, GETDATE()), 112) AS DATETIME) + 36163
            AS INT)

        RETURN CAST(
            dbo.AP_Func_PodajKwoteOdsetek(
                @idrozrachunku,
                1,      -- @ido = ID_TABELI_ODSETEK (1 = ustawowe)
                @dzis,  -- @today = data naliczenia w formacie Clarion
                0,      -- @czy_wal = 0 (PLN)
                0       -- @czy_naliczac_za_zaplaty = 0
            )
        AS DECIMAL(15,2))
    END
""")

# ─────────────────────────────────────────────────────────────────────────────
# DDL — poprzednia implementacja v1 (do downgrade)
# ─────────────────────────────────────────────────────────────────────────────
_FUNC_CREATE_V1: Final[str] = textwrap.dedent("""\
    CREATE FUNCTION dbo.skw_Func_OdsetkiRozrachunku
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

        SELECT
            @termin = r.TerminPlatnosci,
            @kwota  = r.KwotaPozostala
        FROM dbo.skw_rozrachunki_faktur AS r
        WHERE r.ID_ROZRACHUNKU = @idrozrachunku

        IF @termin IS NULL OR @kwota IS NULL OR @kwota <= 0 OR @termin >= @do
            RETURN 0

        SELECT @wynik = ISNULL(SUM(
            @kwota
            * (o.STOPA / 100.0)
            * (DATEDIFF(DAY,
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
               ) / 365.0)
        ), 0)
        FROM dbo.ODSETKI AS o
        WHERE o.ID_TABELI_ODSETEK = 1
          AND CAST(dbo.RM_Func_ClarionDateToDateTime(o.OD_DNIA) AS DATE) <= @do
          AND (
                o.DO_DNIA = 0
                OR CAST(dbo.RM_Func_ClarionDateToDateTime(o.DO_DNIA) AS DATE) >= @termin
              )
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


# ─────────────────────────────────────────────────────────────────────────────
# Helpery
# ─────────────────────────────────────────────────────────────────────────────

def _raw_scalar(bind: Any, sql: str) -> Any:
    """Wykonuje zapytanie i zwraca pierwszą kolumnę pierwszego wiersza."""
    result = bind.execute(sa.text(sql))
    row = result.fetchone()
    return row[0] if row else None


def _krok1_weryfikacja_zaleznosci(bind: Any) -> None:
    """
    Sprawdza że dbo.AP_Func_PodajKwoteOdsetek istnieje w bazie.
    Bez niej nowa implementacja nie zadziała.
    """
    logger.info("[%s] Krok 1/3 — weryfikacja zależności", revision)

    istnieje = _raw_scalar(bind, """
        SELECT COUNT(*)
        FROM   sys.objects AS o
        JOIN   sys.schemas AS s ON s.schema_id = o.schema_id
        WHERE  s.name = N'dbo'
          AND  o.name = N'AP_Func_PodajKwoteOdsetek'
          AND  o.type IN ('FN', 'TF', 'IF', 'FS', 'FT')
    """)

    if not istnieje:
        msg = (
            f"[{revision}] Brak wymaganej funkcji dbo.AP_Func_PodajKwoteOdsetek. "
            f"Migracja {revision} nie może zostać zastosowana."
        )
        logger.critical(msg)
        raise RuntimeError(msg)

    logger.info("[%s] Krok 1/3 — zależności OK", revision)


def _krok2_replace_function(bind: Any, create_ddl: str, wersja: str) -> None:
    """
    DROP FUNCTION IF EXISTS + CREATE FUNCTION.
    Dwa osobne execute — pyodbc nie obsługuje multi-statement w jednym call.
    """
    logger.info(
        "[%s] Krok 2/3 — DROP + CREATE FUNCTION %s.%s (%s)",
        revision, SCHEMA_WAPRO, FUNC_NAME, wersja,
    )

    bind.execute(sa.text(_FUNC_DROP))
    logger.debug("[%s] DROP IF EXISTS — OK", revision)

    bind.execute(sa.text(create_ddl))
    logger.info("[%s] Krok 2/3 — CREATE FUNCTION OK (%s)", revision, wersja)


def _krok3_register_checksum(bind: Any) -> None:
    """
    Aktualizuje checksum funkcji w dbo_ext.skw_SchemaChecksums.
    MERGE — idempotentny, aktualizuje istniejący wpis z 0022.
    LastVerifiedAt = NULL — watchdog zweryfikuje przy starcie.
    """
    logger.info(
        "[%s] Krok 3/3 — MERGE checksum %s.%s → skw_SchemaChecksums",
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
            f"{SCHEMA_WAPRO}.{FUNC_NAME} po CREATE — funkcja nie istnieje "
            f"w sys.sql_modules."
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
    _krok2_replace_function(bind, _FUNC_CREATE_V2, "v2")
    _krok3_register_checksum(bind)

    logger.info("[%s] ── UPGRADE OK ──", revision)


# =============================================================================
# DOWNGRADE
# =============================================================================

def downgrade() -> None:
    logger.warning("[%s] ── DOWNGRADE START ──", revision)
    bind = op.get_bind()

    # Przywróć poprzednią implementację (v1 z migracji 0022)
    _krok2_replace_function(bind, _FUNC_CREATE_V1, "v1")
    _krok3_register_checksum(bind)

    logger.warning("[%s] ── DOWNGRADE OK — przywrócono v1 ──", revision)