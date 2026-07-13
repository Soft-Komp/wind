# backend/alembic/versions/0047_func_odsetki_od_daty.py
"""0047_func_odsetki_od_daty

CREATE OR ALTER FUNCTION dbo.skw_Func_OdsetkiRozrachunku — v3.
Dodaje opcjonalny parametr @od_daty (przesunięcie punktu startowego
naliczania odsetek, np. data ugody).

Zachowanie:
    @od_daty IS NULL     → deleguje do AP_Func_PodajKwoteOdsetek (jak v2/0024,
                            ZERO zmiany zachowania domyślnego).
    @od_daty IS NOT NULL → własna formuła po tabeli dbo.ODSETKI, z @od_daty
                            jako punktem startowym zamiast TerminPlatnosci.

UWAGA — DECYZJA WYMAGA POTWIERDZENIA PRZED WDROŻENIEM NA GPGKJASLO:
    Gałąź @od_daty NOT NULL może dać wynik nieznacznie inny niż natywna
    funkcja WAPRO w analogicznym okresie — patrz komentarz w treści funkcji
    poniżej oraz notatka w MASTER_DOKUMENTACJA.md, sekcja Etap 2.1.

Revision ID: 0047
Revises:     0046
Create Date: 2026-07-06
"""

from __future__ import annotations

import logging
import textwrap
from typing import Final

from alembic import op
from sqlalchemy import text as sa_text

revision:      str = "0047"
down_revision: str = "0046"
branch_labels       = None
depends_on          = None

FUNC_NAME: Final[str] = "skw_Func_OdsetkiRozrachunku"
logger = logging.getLogger(f"alembic.migration.{revision}")

# ─────────────────────────────────────────────────────────────────────────────
# UWAGA KRYTYCZNA: treść _FUNC_V2_CURRENT poniżej odtwarza migrację 0024.
# PRZED URUCHOMIENIEM na STOMIL/GPGKJASLO należy potwierdzić w SSMS, że to
# wciąż DOKŁADNA aktualna treść funkcji (identycznie jak przy TODO-01 dla
# migracji 006 — funkcje/widoki bywały poprawiane ręcznie poza Alembikiem).
#   SELECT OBJECT_DEFINITION(OBJECT_ID('dbo.skw_Func_OdsetkiRozrachunku'));
# ─────────────────────────────────────────────────────────────────────────────

_FUNC_V3: Final[str] = textwrap.dedent("""\
    CREATE OR ALTER FUNCTION dbo.skw_Func_OdsetkiRozrachunku
    (
        @idrozrachunku  NUMERIC(18,0),
        @do_daty        DATE = NULL,
        @od_daty        DATE = NULL
    )
    RETURNS DECIMAL(15,2)
    AS
    BEGIN
        DECLARE @wynik DECIMAL(15,2) = 0

        -- ── Gałąź domyślna: bez zmian względem produkcji (v2 / migracja 0024) ──
        IF @od_daty IS NULL
        BEGIN
            DECLARE @dzis INT = CAST(
                CAST(CONVERT(VARCHAR, ISNULL(@do_daty, GETDATE()), 112) AS DATETIME) + 36163
                AS INT)

            SET @wynik = CAST(
                dbo.AP_Func_PodajKwoteOdsetek(
                    @idrozrachunku, 1, @dzis, 0, 0
                )
            AS DECIMAL(15,2))

            RETURN @wynik
        END

        -- ── Gałąź nowa: @od_daty podane — własna formuła, punkt startowy = @od_daty ──
        DECLARE @do     DATE = ISNULL(@do_daty, CAST(GETDATE() AS DATE))
        DECLARE @kwota  DECIMAL(15,2)

        SELECT @kwota = r.KwotaPozostala
        FROM dbo.skw_rozrachunki_faktur AS r
        WHERE r.ID_ROZRACHUNKU = @idrozrachunku

        IF @kwota IS NULL OR @kwota <= 0 OR @od_daty >= @do
            RETURN 0

        SELECT @wynik = ISNULL(SUM(
            @kwota
            * (o.STOPA / 100.0)
            * (DATEDIFF(DAY,
                CASE
                    WHEN CAST(dbo.RM_Func_ClarionDateToDateTime(o.OD_DNIA) AS DATE) > @od_daty
                    THEN CAST(dbo.RM_Func_ClarionDateToDateTime(o.OD_DNIA) AS DATE)
                    ELSE @od_daty
                END,
                CASE
                    WHEN o.DO_DNIA = 0 THEN @do
                    WHEN CAST(dbo.RM_Func_ClarionDateToDateTime(o.DO_DNIA) AS DATE) < @do
                    THEN CAST(dbo.RM_Func_ClarionDateToDateTime(o.DO_DNIA) AS DATE)
                    ELSE @do
                END
               ) / 365.0)
        ), 0)
        FROM dbo.ODSETKI AS o
        WHERE o.ID_TABELI_ODSETEK = 1
          AND CAST(dbo.RM_Func_ClarionDateToDateTime(o.OD_DNIA) AS DATE) <= @do
          AND (o.DO_DNIA = 0
               OR CAST(dbo.RM_Func_ClarionDateToDateTime(o.DO_DNIA) AS DATE) >= @od_daty)

        RETURN @wynik
    END
""")

_FUNC_V2_ROLLBACK: Final[str] = textwrap.dedent("""\
    CREATE OR ALTER FUNCTION dbo.skw_Func_OdsetkiRozrachunku
    (
        @idrozrachunku  NUMERIC(18,0),
        @do_daty        DATE = NULL
    )
    RETURNS DECIMAL(15,2)
    AS
    BEGIN
        DECLARE @dzis INT = CAST(
            CAST(CONVERT(VARCHAR, ISNULL(@do_daty, GETDATE()), 112) AS DATETIME) + 36163
            AS INT)

        RETURN CAST(
            dbo.AP_Func_PodajKwoteOdsetek(@idrozrachunku, 1, @dzis, 0, 0)
        AS DECIMAL(15,2))
    END
""")


def upgrade() -> None:
    logger.info("[%s] ── UPGRADE START — CREATE OR ALTER FUNCTION %s ──", revision, FUNC_NAME)
    bind = op.get_bind()
    bind.execute(sa_text(_FUNC_V3))
    _merge_checksum(bind, revision)
    logger.info("[%s] ── UPGRADE OK ──", revision)


def downgrade() -> None:
    logger.warning("[%s] ── DOWNGRADE — przywracam v2 (bez @od_daty) ──", revision)
    bind = op.get_bind()
    bind.execute(sa_text(_FUNC_V2_ROLLBACK))
    _merge_checksum(bind, down_revision)
    logger.warning("[%s] ── DOWNGRADE OK ──", revision)


# Pomocnicze — zgodnie z FAKTYCZNYM wzorcem checksumow z migracji 0022/0023/0024.
# UWAGA: Checksum w skw_SchemaChecksums jest typu INT, wypelniana przez
# CHECKSUM(definition) — NIE HASHBYTES/SHA2_256 (ktore zwraca string hex
# i powoduje pyodbc.DataError przy probie wstawienia do kolumny INT).
def _merge_checksum(bind, rev: str) -> None:
    import sqlalchemy as sa

    checksum_row = bind.execute(sa.text(f"""
        SELECT CHECKSUM(m.definition)
        FROM   sys.sql_modules AS m
        JOIN   sys.objects     AS o ON o.object_id = m.object_id
        WHERE  o.name = N'{FUNC_NAME}'
    """)).fetchone()

    if checksum_row is None or checksum_row[0] is None:
        msg = (
            f"[{rev}] Nie mozna odczytac CHECKSUM funkcji dbo.{FUNC_NAME} "
            f"po CREATE — funkcja nie istnieje w sys.sql_modules."
        )
        logger.critical(msg)
        raise RuntimeError(msg)

    checksum = checksum_row[0]
    logger.debug("[%s] CHECKSUM(%s) = %s", rev, FUNC_NAME, checksum)

    bind.execute(sa.text(f"""
        MERGE [dbo].[skw_SchemaChecksums] AS target
        USING (SELECT N'{FUNC_NAME}' AS ObjectName, N'FUNCTION' AS ObjectType) AS src
        ON target.ObjectName = src.ObjectName
        WHEN MATCHED THEN UPDATE SET
            Checksum = {checksum},
            AlembicRevision = N'{rev}',
            UpdatedAt = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (ObjectName, ObjectType, Checksum, AlembicRevision, UpdatedAt)
            VALUES (src.ObjectName, src.ObjectType, {checksum}, N'{rev}', SYSUTCDATETIME());
    """))

    logger.info(
        "[%s] SchemaChecksums MERGE OK — dbo.%s FUNCTION (checksum=%s)",
        rev, FUNC_NAME, checksum,
    )
