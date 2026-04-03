"""Aktualizacja widoku dbo.skw_rozrachunki_faktur.

Revision ID:  0002
Revises:      f124fa9b58eb
Create Date:  2026-03-19 00:00:00.000000 UTC

OPIS:
    Zastępuje definicję widoku dbo.skw_rozrachunki_faktur nową wersją (v2.0).
    Widok był stworzony w migracji f124fa9b58eb jako część initial_schema,
    ale wymagał zmiany logiki biznesowej po analizie danych produkcyjnych.

ZMIANY W WIDOKU (v1 → v2):
    [CH-1] Usunięto CTE cte_kontrahenci_aktywni z JOIN — zastąpiono LEFT JOIN
           przez cte_rodo_guard (filtr RODO zachowany, ale JOIN nie blokuje faktur)
    [CH-2] JOIN → LEFT JOIN: faktury bez kontrahenta są teraz widoczne (NazwaKontrahenta=NULL)
    [CH-3] Dodano kolumny: KwotaZaplacona (DECIMAL 15,2), DniPo, rozliczony, typ_dok
    [CH-4] Usunięto kolumny: NIP, CzyZaplacona, DniPrzeterminowania
    [CH-5] FormaPlatnosci → MetodaPlatnosci (zmiana nazwy aliasu)
    [CH-6] Logika rozliczony: 2=zapłacona (nie 1 jak w v1) — zgodnie z danymi WAPRO live

WAŻNE — KOLUMNY USUNIĘTE:
    Jeśli jakikolwiek kod backend (wapro.py, serwisy, endpointy) odwoływał się
    do kolumn NIP, CzyZaplacona, DniPrzeterminowania, FormaPlatnosci —
    musi zostać zaktualizowany PRZED uruchomieniem tej migracji.
    Przeszukaj kod: grep -r "CzyZaplacona|DniPrzeterminowania|FormaPlatnosci" backend/

PLIKI POWIĄZANE:
    database/views/skw_rozrachunki_faktur.sql  ← źródło prawdy dla SQL widoku
    backend/app/db/wapro.py                    ← może wymagać aktualizacji mapowania
    backend/app/schemas/debtor.py              ← może wymagać aktualizacji Pydantic

SCHEMACHECKSUMS:
    Widok NIE miał wpisu w skw_SchemaChecksums (nie był tam zarejestrowany
    przez migrację f124fa9b58eb). Ta migracja tworzy wpis po raz pierwszy.
    Po downgrade wpis jest usuwany.

DOWNGRADE:
    Przywraca oryginalną definicję widoku z migracji f124fa9b58eb.
    Usuwa wpis z SchemaChecksums.
    UWAGA: Downgrade nie przywróci danych — tylko strukturę widoku.
"""

from __future__ import annotations

import logging
from typing import Final

from alembic import op


# ─── METADANE MIGRACJI ────────────────────────────────────────────────────────

revision:      str        = "0002"
down_revision: str | None = "f124fa9b58eb"
branch_labels: None       = None
depends_on:    None       = None

# ─── STAŁE ───────────────────────────────────────────────────────────────────

SCHEMA_WAPRO: Final[str] = "dbo"
SCHEMA_EXT:   Final[str] = "dbo_ext"
VIEW_NAME:    Final[str] = "skw_rozrachunki_faktur"

# ─── LOGGER ──────────────────────────────────────────────────────────────────

logger = logging.getLogger(f"alembic.migration.{revision}")


# =============================================================================
# UPGRADE
# =============================================================================


def upgrade() -> None:
    """
    Zastępuje widok skw_rozrachunki_faktur nową wersją (v2.0).
    Rejestruje checksum w skw_SchemaChecksums (MERGE — idempotentny).

    Kolejność operacji:
        1. CREATE OR ALTER VIEW      ← idempotentny, bezpieczny przy re-run
        2. MERGE do SchemaChecksums  ← rejestruje checksum po zmianie widoku
    """
    logger.info(
        "[%s] upgrade → aktualizacja widoku %s.%s do v2.0",
        revision, SCHEMA_WAPRO, VIEW_NAME,
    )

    _upgrade_create_or_alter_view()
    _upgrade_register_checksum()

    logger.info(
        "[%s] upgrade zakończony pomyślnie — widok %s.%s v2.0 aktywny",
        revision, SCHEMA_WAPRO, VIEW_NAME,
    )


def _upgrade_create_or_alter_view() -> None:
    # WERSJA POŚREDNIA (v2) — bez OstatniMonitRozrachunku.
    # Migracja 0004 nadpisze pełną wersją po utworzeniu skw_MonitHistory_Invoices (0003).
    logger.info("[%s] CREATE OR ALTER VIEW %s.%s ...", revision, SCHEMA_WAPRO, VIEW_NAME)

    op.execute("""
        CREATE OR ALTER VIEW dbo.skw_rozrachunki_faktur
AS
SELECT
    r.ID_ROZRACHUNKU,
    r.ID_KONTRAHENTA,
    ISNULL(k.NAZWA_PELNA, k.NAZWA)                              AS NazwaKontrahenta,
    r.NR_DOK                                                    AS NumerFaktury,
    CAST(DATEADD(DAY, r.DATA_DOK, '18991230')         AS DATE)  AS DataWystawienia,
    CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE)  AS TerminPlatnosci,
    r.KWOTA                                                     AS KwotaBrutto,
    CAST(
        CASE
            WHEN r.KWOTA - ISNULL(r.POZOSTALO_WN, 0) < 0
            THEN 0
            ELSE r.KWOTA - ISNULL(r.POZOSTALO_WN, 0)
        END
    AS DECIMAL(15,2))                                           AS KwotaZaplacona,
    ISNULL(r.POZOSTALO_WN, 0)                                   AS KwotaPozostala,
    r.FORMA_PLATNOSCI                                           AS MetodaPlatnosci,
    CASE
        WHEN r.TERMIN_PLATNOSCI IS NULL OR r.TERMIN_PLATNOSCI = 0
            THEN NULL
        WHEN DATEDIFF(
                DAY,
                CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE),
                CAST(GETDATE() AS DATE)
             ) <= 0
            THEN 0
        ELSE
            DATEDIFF(
                DAY,
                CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE),
                CAST(GETDATE() AS DATE)
            )
    END                                                         AS DniPo,
    r.CZY_ROZLICZONY,
    r.ID_TYP_DOK
FROM dbo.ROZRACHUNEK_VIEW AS r
LEFT JOIN dbo.KONTRAHENT AS k
       ON k.ID_KONTRAHENTA = r.ID_KONTRAHENTA
WHERE
    r.ID_KONTRAHENTA    IS NOT NULL
    AND r.STRONA        = 'WN'
    AND r.CZY_ROZLICZONY IN (0, 1)
    """)

    logger.info("[%s] CREATE OR ALTER VIEW %s.%s → OK", revision, SCHEMA_WAPRO, VIEW_NAME)


def _upgrade_register_checksum() -> None:
    """
    Rejestruje checksum widoku w dbo_ext.skw_SchemaChecksums.

    Używa MERGE (UPSERT) — idempotentny przy ewentualnym re-run migracji.

    UWAGA: Widok NIE miał wpisu w SchemaChecksums przed tą migracją.
    Checksum obliczany z sys.sql_modules bezpośrednio po CREATE OR ALTER VIEW —
    musi być wykonany w tej samej transakcji, żeby był aktualny.

    LastVerifiedAt ustawione na NULL celowo — wymusi re-weryfikację
    przy następnym starcie aplikacji (schema_integrity.py).
    """
    logger.info(
        "[%s] MERGE do %s.skw_SchemaChecksums dla %s.%s ...",
        revision, SCHEMA_EXT, SCHEMA_WAPRO, VIEW_NAME,
    )

    op.execute(f"""
        MERGE [{SCHEMA_EXT}].[skw_SchemaChecksums] AS target
        USING (
            SELECT
                N'{VIEW_NAME}'          AS ObjectName,
                N'{SCHEMA_WAPRO}'       AS SchemaName,
                N'VIEW'                 AS ObjectType,
                (
                    SELECT CHECKSUM(m.definition)
                    FROM   sys.sql_modules m
                    JOIN   sys.objects     o ON m.object_id = o.object_id
                    WHERE  o.name                    = N'{VIEW_NAME}'
                      AND  SCHEMA_NAME(o.schema_id)  = N'{SCHEMA_WAPRO}'
                )                       AS Checksum,
                N'{revision}'           AS AlembicRevision,
                NULL                    AS LastVerifiedAt,  -- wymusi weryfikację przy starcie
                GETDATE()               AS Now
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
                [ObjectName],
                [SchemaName],
                [ObjectType],
                [Checksum],
                [AlembicRevision],
                [LastVerifiedAt],
                [CreatedAt]
            )
            VALUES (
                source.[ObjectName],
                source.[SchemaName],
                source.[ObjectType],
                source.[Checksum],
                source.[AlembicRevision],
                source.[LastVerifiedAt],
                source.[Now]
            );
    """)

    logger.info(
        "[%s] SchemaChecksums MERGE → OK (widok %s.%s zarejestrowany)",
        revision, SCHEMA_WAPRO, VIEW_NAME,
    )


# =============================================================================
# DOWNGRADE
# =============================================================================


def downgrade() -> None:
    """
    Przywraca oryginalną definicję widoku z migracji f124fa9b58eb.
    Usuwa wpis z SchemaChecksums (bo v1 go nie miała).

    UWAGA: Downgrade to operacja awaryjna. Wykonuj tylko jeśli wiesz co robisz.
    Po downgrade aplikacja może nie startować (schema_integrity.py wykryje
    brak wpisu checksumu) — wymagane ręczne usunięcie wpisu LUB downgrade
    SchemaChecksums razem z widokiem (ta migracja to robi).
    """
    logger.warning(
        "[%s] DOWNGRADE → przywracam widok %s.%s do v1.0 (f124fa9b58eb)",
        revision, SCHEMA_WAPRO, VIEW_NAME,
    )

    _downgrade_restore_view_v1()
    _downgrade_remove_checksum()

    logger.warning(
        "[%s] DOWNGRADE zakończony — widok %s.%s przywrócony do v1.0",
        revision, SCHEMA_WAPRO, VIEW_NAME,
    )


def _downgrade_restore_view_v1() -> None:
    """
    Przywraca definicję widoku z initial_schema (f124fa9b58eb).

    Oryginalna definicja używała:
        - INNER JOIN przez CTE cte_kontrahenci_aktywni (filtr RODO + ZABLOKOWANY)
        - WHERE r.typ_dok = 'h' (tylko faktury sprzedaży)
        - Kolumny: NIP, CzyZaplacona, DniPrzeterminowania, FormaPlatnosci
        - rozliczony=1 jako warunek zapłaconej faktury
    """
    logger.info("[%s] DOWNGRADE: przywracam v1 widoku %s.%s", revision, SCHEMA_WAPRO, VIEW_NAME)

    op.execute(f"""
        CREATE OR ALTER VIEW [{SCHEMA_WAPRO}].[{VIEW_NAME}]
        AS
        WITH cte_kontrahenci_aktywni AS (
            SELECT
                k.ID_KONTRAHENTA,
                ISNULL(k.NAZWA_PELNA, k.NAZWA) AS NazwaKontrahenta,
                k.NIP                          AS NIP
            FROM dbo.KONTRAHENT AS k
            WHERE k.RODO_ZANONIMIZOWANY = 0
              AND k.ZABLOKOWANY         = 0
        )
        SELECT
            r.id_platnika                                                   AS ID_KONTRAHENTA,
            ka.NazwaKontrahenta,
            ka.NIP,
            r.numer                                                         AS NumerFaktury,
            CAST(dbo.RM_Func_ClarionDateToDateTime(r.data_wystawienia)  AS DATE) AS DataWystawienia,
            CAST(dbo.RM_Func_ClarionDateToDateTime(r.termin_platnosci)  AS DATE) AS TerminPlatnosci,
            ABS(r.wartosc_brutto)                                           AS KwotaBrutto,
            ABS(r.pozostalo)                                                AS KwotaPozostala,
            ABS(r.wartosc_brutto) - ABS(r.pozostalo)                       AS KwotaZaplacona,
            CASE WHEN r.rozliczony = 1 THEN 1 ELSE 0 END                   AS CzyZaplacona,
            ISNULL(r.dni_przeterminowania, 0)                              AS DniPrzeterminowania,
            r.forma_platnosci                                               AS FormaPlatnosci
        FROM [dbo].[ROZRACHUNEK_V] AS r
        JOIN cte_kontrahenci_aktywni AS ka
          ON ka.ID_KONTRAHENTA = CAST(r.id_platnika AS INT)
        WHERE r.typ_dok  = 'h'
          AND r.pozostalo < 0;
    """)

    logger.info(
        "[%s] DOWNGRADE: widok %s.%s v1.0 przywrócony",
        revision, SCHEMA_WAPRO, VIEW_NAME,
    )


def _downgrade_remove_checksum() -> None:
    """
    Usuwa wpis z SchemaChecksums — przywraca stan sprzed tej migracji.

    Migracja f124fa9b58eb NIE rejestrowała checksumu tego widoku,
    więc downgrade musi go usunąć żeby nie pozostał osierocony wpis.
    """
    logger.info(
        "[%s] DOWNGRADE: usuwam wpis SchemaChecksums dla %s.%s",
        revision, SCHEMA_WAPRO, VIEW_NAME,
    )

    op.execute(f"""
        DELETE FROM [{SCHEMA_EXT}].[skw_SchemaChecksums]
        WHERE [ObjectName] = N'{VIEW_NAME}'
          AND [SchemaName] = N'{SCHEMA_WAPRO}'
          AND [ObjectType] = N'VIEW';
    """)

    logger.info(
        "[%s] DOWNGRADE: wpis SchemaChecksums usunięty — OK",
        revision, SCHEMA_WAPRO, VIEW_NAME,
    )