"""0011_kontrahenci_widok_dbo

Tworzy widok w schemacie dbo dla modułu listy dłużników:
  - dbo.skw_kontrahenci  — kontrahenci z aktywnymi należnościami,
    wzbogaceni o agregaty rozrachunków i historię monitów

Widok łączy dane z:
  - dbo.KONTRAHENT              (WAPRO, read-only)
  - dbo.ROZRACHUNEK_VIEW        (WAPRO, read-only)
  - dbo_ext.skw_MonitHistory    (projekt)
  - dbo_ext.skw_MonitHistory_Invoices (projekt)

Konwersja dat Clarion: dbo.RM_Func_ClarionDateToDateTime(val) → DATE
CREATE OR ALTER VIEW — idempotentne, bezpieczne przy re-run.

Wymagania wstępne (upgrade przerywa z błędem jeśli brakuje):
  - dbo.KONTRAHENT                      (tabela WAPRO)
  - dbo.ROZRACHUNEK_VIEW                (widok WAPRO)
  - dbo.RM_Func_ClarionDateToDateTime   (funkcja WAPRO)
  - dbo_ext.skw_MonitHistory            (projekt — migracja 0001+)
  - dbo_ext.skw_MonitHistory_Invoices   (projekt — migracja 0003+)

Revision ID: 0011
Revises: 0010
Create Date: 2026-04-07
"""

from __future__ import annotations

import logging
import textwrap
from typing import Final

from alembic import op

# ---------------------------------------------------------------------------
# Metadane Alembic
# ---------------------------------------------------------------------------
revision: str = "0011"
down_revision: str = "0010"
branch_labels = None
depends_on = None

# ---------------------------------------------------------------------------
# Stałe — nazwy obiektów
# ---------------------------------------------------------------------------
SCHEMA_WAPRO: Final[str] = "dbo"
SCHEMA_EXT:   Final[str] = "dbo_ext"
VIEW_NAME:    Final[str] = "skw_kontrahenci"

# Pełna kwalifikowana nazwa widoku (używana w logach i SQL)
FQVIEW: Final[str] = f"[{SCHEMA_WAPRO}].[{VIEW_NAME}]"

# ---------------------------------------------------------------------------
# Logger — spójny z innymi migracjami projektu
# ---------------------------------------------------------------------------
logger = logging.getLogger(f"alembic.migration.{revision}")


# ===========================================================================
# DEFINICJA WIDOKU
# ===========================================================================

_VIEW_DDL: Final[str] = """
CREATE OR ALTER VIEW dbo.skw_kontrahenci
AS
WITH cte_rozrachunki AS
(
    /*
     * Agregat długów aktywnych (nierozliczonych lub częściowo rozliczonych)
     * po stronie WN (należności). Jedna strona na kontrahenta.
     *
     * CZY_ROZLICZONY IN (0,1):
     *   0 = nierozliczony
     *   1 = częściowo rozliczony
     *   2 = rozliczony w całości — celowo POMINIĘTY
     */
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
    WHERE
        r.ID_KONTRAHENTA IS NOT NULL
        AND r.STRONA          = 'WN'
        AND r.CZY_ROZLICZONY IN (0, 1)
    GROUP BY
        r.ID_KONTRAHENTA
),
cte_monity_ranked AS
(
    /*
     * Ranking monitów per kontrahent — najnowszy na pozycji rn=1.
     * ISNULL(SentAt, CreatedAt): jeśli monit był w kolejce (nie wysłany),
     * używamy daty utworzenia jako przybliżenia.
     */
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
    /*
     * Ostatni monit na kontrahenta (rn=1 z rankingu powyżej).
     */
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
    /*
     * Data ostatniego monitu powiązanego z konkretnym rozrachunkiem
     * (przez skw_MonitHistory_Invoices). Może różnić się od OstatniMonitData
     * gdy kontrahent dostał globalny monit bez powiązania z fakturą.
     */
    SELECT
        mh.ID_KONTRAHENTA,
        MAX(mi.CreatedAt)   AS OstatniMonitRozrachunku
    FROM dbo_ext.skw_MonitHistory_Invoices AS mi
    JOIN dbo_ext.skw_MonitHistory AS mh
        ON mh.ID_MONIT = mi.ID_MONIT
    GROUP BY
        mh.ID_KONTRAHENTA
)
SELECT
    k.ID_KONTRAHENTA,
    ISNULL(k.NAZWA_PELNA, k.NAZWA)          AS NazwaKontrahenta,
    k.ADRES_EMAIL                           AS Email,
    k.TELEFON_FIRMOWY                       AS Telefon,
    ISNULL(roz.SumaDlugu,          0)       AS SumaDlugu,
    ISNULL(roz.LiczbaFaktur,       0)       AS LiczbaFaktur,
    roz.NajstarszaFaktura,
    ISNULL(roz.DniPrzeterminowania,0)       AS DniPrzeterminowania,
    mon.OstatniMonitData,
    mon.OstatniMonitTyp,
    ISNULL(mon.LiczbaMonitow,      0)       AS LiczbaMonitow,
    monr.OstatniMonitRozrachunku
FROM dbo.KONTRAHENT AS k
LEFT JOIN cte_rozrachunki       AS roz  ON roz.ID_KONTRAHENTA  = k.ID_KONTRAHENTA
LEFT JOIN cte_monity            AS mon  ON mon.ID_KONTRAHENTA  = k.ID_KONTRAHENTA
LEFT JOIN cte_monity_rozrachunki AS monr ON monr.ID_KONTRAHENTA = k.ID_KONTRAHENTA
WHERE ISNULL(roz.SumaDlugu, 0) > 0;
"""

# ---------------------------------------------------------------------------
# DROP używany przy downgrade
# ---------------------------------------------------------------------------
_DROP_VIEW: Final[str] = f"""
IF OBJECT_ID(N'{FQVIEW}', N'V') IS NOT NULL
    DROP VIEW [{SCHEMA_WAPRO}].[{VIEW_NAME}];
"""


# ===========================================================================
# PRYWATNE FUNKCJE POMOCNICZE
# ===========================================================================

def _assert_dependency(
    bind,
    object_name: str,
    schema: str,
    object_type: str,
    *,
    type_code: str,
) -> None:
    """
    Weryfikuje istnienie wymaganego obiektu bazy danych.

    Przerywa migrację wyjątkiem RuntimeError jeśli obiekt nie istnieje.

    Args:
        bind:        połączenie SQLAlchemy (op.get_bind())
        object_name: nazwa obiektu (bez schematu)
        schema:      schemat właściciela
        object_type: czytelna nazwa typu ('tabela', 'widok', 'funkcja')
        type_code:   kod MSSQL z sys.objects.type — np. 'U', 'V', 'FN', 'TF'
    """
    sql = textwrap.dedent(f"""
        SELECT COUNT(1)
        FROM   sys.objects  AS o
        JOIN   sys.schemas  AS s ON s.schema_id = o.schema_id
        WHERE  s.name    = '{schema}'
          AND  o.name    = '{object_name}'
          AND  o.type    = '{type_code}'
    """)
    result = bind.execute(op.inline_literal(sql) if False else
                          _raw_scalar(bind, sql))
    count = result if isinstance(result, int) else (result.scalar() or 0)

    fq = f"[{schema}].[{object_name}]"
    if count == 0:
        msg = (
            f"[{revision}] BRAK WYMAGANEJ ZALEŻNOŚCI — "
            f"{object_type} {fq} nie istnieje. "
            f"Migracja {revision} nie może zostać zastosowana."
        )
        logger.critical(msg)
        raise RuntimeError(msg)

    logger.debug(
        "[%s] zależność OK — %s %s (type=%s)",
        revision, object_type, fq, type_code,
    )


def _raw_scalar(bind, sql: str):
    """
    Wykonuje surowy SQL i zwraca wynik jako skalar.
    Kompatybilne z SQLAlchemy 1.x i 2.x (TextClause).
    """
    import sqlalchemy as sa  # import lokalny — nie zaśmieca namespace modułu
    return bind.execute(sa.text(sql)).scalar()


def _check_all_dependencies(bind) -> None:
    """
    Sprawdza wszystkie obiekty wymagane przez widok dbo.skw_kontrahenci.

    Zależności:
        [dbo].[KONTRAHENT]                  — tabela WAPRO
        [dbo].[ROZRACHUNEK_VIEW]            — widok WAPRO
        [dbo].[RM_Func_ClarionDateToDateTime] — funkcja WAPRO (skalar lub tabela)
        [dbo_ext].[skw_MonitHistory]        — tabela projektu (migracja 0001+)
        [dbo_ext].[skw_MonitHistory_Invoices] — tabela projektu (migracja 0003+)

    Przy każdym brakującym obiekcie rzuca RuntimeError i PRZERYWA migrację.
    """
    logger.info(
        "[%s] Weryfikacja zależności przed CREATE OR ALTER VIEW %s ...",
        revision, FQVIEW,
    )

    deps = [
        # (object_name,                    schema,     opis_ludzki,       type_code)
        ("KONTRAHENT",                     "dbo",      "tabela WAPRO",    "U"),
        ("ROZRACHUNEK_VIEW",               "dbo",      "widok WAPRO",     "V"),
        ("skw_MonitHistory",               "dbo_ext",  "tabela projektu", "U"),
        ("skw_MonitHistory_Invoices",      "dbo_ext",  "tabela projektu", "U"),
    ]

    for obj_name, schema, opis, tcode in deps:
        _assert_dependency(bind, obj_name, schema, opis, type_code=tcode)

    # Funkcja Clarion — może być FN (skalar) lub TF (tabela) — sprawdzamy oba
    func_name = "RM_Func_ClarionDateToDateTime"
    sql_func = textwrap.dedent(f"""
        SELECT COUNT(1)
        FROM   sys.objects  AS o
        JOIN   sys.schemas  AS s ON s.schema_id = o.schema_id
        WHERE  s.name  = 'dbo'
          AND  o.name  = '{func_name}'
          AND  o.type IN ('FN', 'TF', 'IF', 'FS', 'FT')
    """)
    count = _raw_scalar(bind, sql_func) or 0
    if count == 0:
        msg = (
            f"[{revision}] BRAK WYMAGANEJ ZALEŻNOŚCI — "
            f"funkcja [dbo].[{func_name}] nie istnieje (żaden typ FN/TF/IF/FS/FT). "
            f"Migracja {revision} nie może zostać zastosowana."
        )
        logger.critical(msg)
        raise RuntimeError(msg)

    logger.info(
        "[%s] Wszystkie zależności zweryfikowane — widok może zostać utworzony.",
        revision,
    )


def _create_or_alter_view(bind) -> None:
    """
    Wykonuje CREATE OR ALTER VIEW.

    Operacja jest idempotentna — bezpieczna przy ewentualnym re-run
    (np. po przerwaniu migracji i ponownym jej uruchomieniu).
    """
    logger.info("[%s] CREATE OR ALTER VIEW %s ...", revision, FQVIEW)
    import sqlalchemy as sa
    bind.execute(sa.text(_VIEW_DDL))
    logger.info("[%s] CREATE OR ALTER VIEW %s → OK", revision, FQVIEW)


def _register_checksum(bind) -> None:
    """
    Rejestruje / aktualizuje checksum widoku w dbo_ext.skw_SchemaChecksums.

    Checksum pobierany z sys.sql_modules BEZPOŚREDNIO po CREATE OR ALTER VIEW
    — musi być w tej samej sesji/transakcji, żeby był aktualny.

    Wzorzec MERGE (UPSERT):
        WHEN NOT MATCHED BY TARGET → INSERT  (nowy wpis)
        WHEN MATCHED               → UPDATE  (re-run migracji)

    LastVerifiedAt ustawione na NULL celowo:
        Wymusza re-weryfikację przy następnym starcie aplikacji
        (schema_integrity.py).

    Checksum jest INT obliczany przez CHECKSUM() w SQL Server.
    NIE wstawiamy tutaj MD5 / hex stringa — zgodnie z regułą projektu.
    """
    logger.info(
        "[%s] Rejestracja checksum widoku %s w [%s].[skw_SchemaChecksums] ...",
        revision, FQVIEW, SCHEMA_EXT,
    )

    import sqlalchemy as sa

    merge_sql = textwrap.dedent(f"""
        DECLARE @checksum INT;

        SELECT @checksum = CHECKSUM(definition)
        FROM   sys.sql_modules AS m
        JOIN   sys.objects     AS o ON o.object_id = m.object_id
        JOIN   sys.schemas     AS s ON s.schema_id = o.schema_id
        WHERE  s.name = N'{SCHEMA_WAPRO}'
          AND  o.name = N'{VIEW_NAME}';

        IF @checksum IS NULL
        BEGIN
            RAISERROR(
                N'[{revision}] Nie można odczytać definicji widoku {FQVIEW} '
                N'z sys.sql_modules — rejestracja checksum niemożliwa.',
                16, 1
            );
        END;

        MERGE [{SCHEMA_EXT}].[skw_SchemaChecksums] AS tgt
        USING (
            SELECT
                N'{SCHEMA_WAPRO}'  AS SchemaName,
                N'{VIEW_NAME}'     AS ObjectName,
                N'VIEW'            AS ObjectType,
                @checksum          AS Checksum,
                NULL               AS LastVerifiedAt,
                GETDATE()          AS UpdatedAt
        ) AS src
        ON  tgt.SchemaName = src.SchemaName
        AND tgt.ObjectName = src.ObjectName
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SchemaName, ObjectName, ObjectType,  Checksum,     LastVerifiedAt, UpdatedAt)
            VALUES (src.SchemaName, src.ObjectName, src.ObjectType,
                    src.Checksum,  src.LastVerifiedAt,  src.UpdatedAt)
        WHEN MATCHED THEN
            UPDATE SET
                tgt.Checksum        = src.Checksum,
                tgt.LastVerifiedAt  = src.LastVerifiedAt,
                tgt.UpdatedAt       = src.UpdatedAt;
    """)

    bind.execute(sa.text(merge_sql))

    logger.info(
        "[%s] SchemaChecksums MERGE → OK (widok %s zarejestrowany / zaktualizowany)",
        revision, FQVIEW,
    )


# ===========================================================================
# UPGRADE
# ===========================================================================

def upgrade() -> None:
    """
    Tworzy widok dbo.skw_kontrahenci i rejestruje jego checksum.

    Kolejność operacji:
        1. Weryfikacja zależności          ← przerywa z RuntimeError jeśli brakuje
        2. CREATE OR ALTER VIEW            ← idempotentny, bezpieczny przy re-run
        3. MERGE do skw_SchemaChecksums    ← rejestruje checksum po CREATE
    """
    logger.info(
        "[%s] ── UPGRADE START ── tworzenie widoku %s",
        revision, FQVIEW,
    )

    bind = op.get_bind()

    _check_all_dependencies(bind)
    _create_or_alter_view(bind)
    _register_checksum(bind)

    logger.info(
        "[%s] ── UPGRADE OK ── widok %s aktywny, checksum zarejestrowany",
        revision, FQVIEW,
    )


# ===========================================================================
# DOWNGRADE
# ===========================================================================

def downgrade() -> None:
    """
    Usuwa widok dbo.skw_kontrahenci i jego wpis w skw_SchemaChecksums.

    UWAGA: Downgrade jest operacją awaryjną.
    Po jej wykonaniu aplikacja może nie startować (schema_integrity.py
    wykryje brak wpisu checksumu dla widoku który istniał wcześniej).

    Kolejność operacji:
        1. DROP VIEW IF EXISTS             ← bezpieczny, nie rzuca błędu gdy brak
        2. DELETE z skw_SchemaChecksums    ← usuwa wpis monitoringu
    """
    logger.warning(
        "[%s] ── DOWNGRADE START ── usuwanie widoku %s",
        revision, FQVIEW,
    )

    import sqlalchemy as sa
    bind = op.get_bind()

    # 1. DROP VIEW
    logger.info("[%s] DROP VIEW %s ...", revision, FQVIEW)
    bind.execute(sa.text(_DROP_VIEW))
    logger.info("[%s] DROP VIEW %s → OK", revision, FQVIEW)

    # 2. Usuń wpis checksumu
    delete_sql = textwrap.dedent(f"""
        DELETE FROM [{SCHEMA_EXT}].[skw_SchemaChecksums]
        WHERE  SchemaName = N'{SCHEMA_WAPRO}'
          AND  ObjectName = N'{VIEW_NAME}';
    """)
    logger.info(
        "[%s] DELETE z [%s].[skw_SchemaChecksums] WHERE ObjectName = '%s' ...",
        revision, SCHEMA_EXT, VIEW_NAME,
    )
    bind.execute(sa.text(delete_sql))
    logger.info("[%s] DELETE SchemaChecksums → OK", revision, FQVIEW)

    logger.warning(
        "[%s] ── DOWNGRADE OK ── widok %s usunięty. "
        "WYMAGANA ręczna weryfikacja schema_integrity.py przed restartem aplikacji.",
        revision, FQVIEW,
    )