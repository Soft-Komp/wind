"""Indeksy wydajnościowe na tabelach WAPRO (dbo) — raw SQL.

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-02-18

Kontekst: Alembic NIE zarządza tabelami WAPRO (schemat dbo).
Indeksy tworzymy przez op.execute() — Alembic tylko śledzi rewizję.

Indeksy pokrywają CTE w VIEW_kontrahenci i VIEW_rozrachunki_faktur.
Bez nich każde zapytanie do widoków = full table scan na dbo.Rozrachunek
i dbo.KONTRAHENT (może być 100k+ wierszy w produkcji).

Indeksy opisane w AUDIT_ZGODNOSCI.md §R9:
  - IX_Roz_Kontrahent_Dlugi     → dbo.Rozrachunek (filtr WAPRO CTE)
  - IX_Mon_Kontrahent_Historia   → dbo_ext.MonitHistory (dla widoku historii)
  - IX_Roz_Faktura_Kontrahent    → dbo.Rozrachunek (faktury per kontrahent)

Po migracji: rejestracja checksumów w SchemaChecksums (ObjectType='INDEX').
"""

from __future__ import annotations

import logging

from alembic import op

# ─── Identyfikatory rewizji ───────────────────────────────────────────────────
revision: str = "b2c3d4e5f6a7"
down_revision: str = "a1b2c3d4e5f6"
branch_labels: str | None = None
depends_on: str | None = None

logger = logging.getLogger("alembic.migration.002")

# ─── Stałe ───────────────────────────────────────────────────────────────────
SCHEMA_WAPRO = "dbo"
SCHEMA_EXT = "dbo_ext"


# ─── UPGRADE ─────────────────────────────────────────────────────────────────


def upgrade() -> None:
    """Tworzy indeksy WAPRO i rejestruje je w SchemaChecksums."""
    logger.info("=== MIGRACJA 002 — UPGRADE START (indeksy WAPRO) ===")

    _create_ix_roz_kontrahent_dlugi()
    _create_ix_roz_faktura_kontrahent()
    _create_ix_mon_kontrahent_historia()

    # Rejestracja indeksów w SchemaChecksums (ObjectType='INDEX')
    # Checksum oparty na nazwie indeksu + target table — deterministyczny
    _register_index_checksums()

    logger.info("=== MIGRACJA 002 — UPGRADE ZAKOŃCZONY ===")


# ─── DOWNGRADE ───────────────────────────────────────────────────────────────


def downgrade() -> None:
    """Usuwa indeksy WAPRO (bezpieczne — dane WAPRO nieruszone)."""
    logger.warning("=== MIGRACJA 002 — DOWNGRADE START ===")

    _drop_if_exists(SCHEMA_WAPRO, "Rozrachunek", "IX_Roz_Kontrahent_Dlugi")
    _drop_if_exists(SCHEMA_WAPRO, "Rozrachunek", "IX_Roz_Faktura_Kontrahent")
    _drop_if_exists(SCHEMA_EXT, "MonitHistory", "IX_Mon_Kontrahent_Historia")

    # Usuń wpisy z SchemaChecksums
    op.execute(
        f"""
        DELETE FROM [{SCHEMA_EXT}].[SchemaChecksums]
        WHERE [ObjectType] = N'INDEX'
          AND [AlembicRevision] = N'{revision}';
        """
    )

    logger.warning("=== MIGRACJA 002 — DOWNGRADE ZAKOŃCZONY ===")


# ─── INDEKSY WAPRO ────────────────────────────────────────────────────────────


def _create_ix_roz_kontrahent_dlugi() -> None:
    """
    Indeks pokrywający CTE cte_rozrachunki w VIEW_kontrahenci.

    Zapytanie docelowe:
        SELECT r.ID_KONTRAHENTA,
               SUM(CASE WHEN r.CZY_ROZLICZONY = 0 AND r.STRONA = 'WN'
                        THEN ISNULL(r.POZOSTALO, 0) ELSE 0 END) AS SumaDlugu
        FROM dbo.Rozrachunek r
        WHERE r.TYP_DOK = 'F'
          AND r.CZY_ROZLICZONY = 0
        GROUP BY r.ID_KONTRAHENTA

    Strateguia: FILTERED index (TYP_DOK='F', CZY_ROZLICZONY=0) pokrywający
    wszystkie kolumny INCLUDE potrzebne CTE.
    """
    op.execute(
        f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA_WAPRO}].[Rozrachunek]')
              AND name = N'IX_Roz_Kontrahent_Dlugi'
        )
        BEGIN
            CREATE NONCLUSTERED INDEX [IX_Roz_Kontrahent_Dlugi]
                ON [{SCHEMA_WAPRO}].[Rozrachunek] (
                    [ID_KONTRAHENTA] ASC,
                    [CZY_ROZLICZONY] ASC
                )
                INCLUDE (
                    [POZOSTALO],
                    [KWOTA],
                    [STRONA],
                    [TERMIN_PLATNOSCI],
                    [TYP_DOK]
                )
                WHERE [TYP_DOK] = N'F'
                  AND [CZY_ROZLICZONY] = 0
            WITH (
                FILLFACTOR          = 85,
                ONLINE              = OFF,
                STATISTICS_NORECOMPUTE = OFF
            );
            PRINT 'Indeks IX_Roz_Kontrahent_Dlugi utworzony.';
        END
        ELSE
            PRINT 'Indeks IX_Roz_Kontrahent_Dlugi już istnieje — pominięto.';
        """
    )
    logger.info("Indeks IX_Roz_Kontrahent_Dlugi: OK")


def _create_ix_roz_faktura_kontrahent() -> None:
    """
    Indeks pokrywający VIEW_rozrachunki_faktur.

    Zapytanie docelowe:
        SELECT r.ID_KONTRAHENTA,
               r.NR_DOK,
               CAST(DATEADD(DAY, r.DATA_DOK, '18991230') AS DATE) AS DataWystawienia,
               CAST(DATEADD(DAY, r.TERMIN_PLATNOSCI, '18991230') AS DATE) AS TerminPlatnosci,
               r.KWOTA, r.POZOSTALO, r.CZY_ROZLICZONY, r.CZY_PRZETERMINOWANY,
               DATEDIFF(DAY, DATEADD(DAY, r.TERMIN_PLATNOSCI,'18991230'), GETDATE()) AS DniPrzeterminowania
        FROM dbo.Rozrachunek r
        WHERE r.TYP_DOK = 'F' AND r.STRONA = 'WN'
        ORDER BY r.ID_KONTRAHENTA, r.TERMIN_PLATNOSCI

    Strategia: klucz (ID_KONTRAHENTA, TERMIN_PLATNOSCI) + filtred TYP_DOK='F' STRONA='WN'
    """
    op.execute(
        f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA_WAPRO}].[Rozrachunek]')
              AND name = N'IX_Roz_Faktura_Kontrahent'
        )
        BEGIN
            CREATE NONCLUSTERED INDEX [IX_Roz_Faktura_Kontrahent]
                ON [{SCHEMA_WAPRO}].[Rozrachunek] (
                    [ID_KONTRAHENTA]   ASC,
                    [TERMIN_PLATNOSCI] ASC
                )
                INCLUDE (
                    [NR_DOK],
                    [DATA_DOK],
                    [KWOTA],
                    [POZOSTALO],
                    [CZY_ROZLICZONY],
                    [CZY_PRZETERMINOWANY],
                    [FORMA_PLATNOSCI]
                )
                WHERE [TYP_DOK] = N'F'
                  AND [STRONA]   = N'WN'
            WITH (
                FILLFACTOR          = 85,
                ONLINE              = OFF,
                STATISTICS_NORECOMPUTE = OFF
            );
            PRINT 'Indeks IX_Roz_Faktura_Kontrahent utworzony.';
        END
        ELSE
            PRINT 'Indeks IX_Roz_Faktura_Kontrahent już istnieje — pominięto.';
        """
    )
    logger.info("Indeks IX_Roz_Faktura_Kontrahent: OK")


def _create_ix_mon_kontrahent_historia() -> None:
    """
    Indeks na MonitHistory (dbo_ext) — historia monitów dla kontrahenta.

    Optymalizuje zapytanie: GET /api/v1/debtors/{id}/monit-history
    które pobiera historię per kontrahent sortowaną malejąco po dacie.
    """
    op.execute(
        f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA_EXT}].[MonitHistory]')
              AND name = N'IX_Mon_Kontrahent_Historia'
        )
        BEGIN
            CREATE NONCLUSTERED INDEX [IX_Mon_Kontrahent_Historia]
                ON [{SCHEMA_EXT}].[MonitHistory] (
                    [ID_KONTRAHENTA] ASC,
                    [CreatedAt]      DESC
                )
                INCLUDE (
                    [MonitType],
                    [Status],
                    [ID_USER],
                    [Recipient],
                    [TotalDebt],
                    [IsActive]
                )
                WHERE [IsActive] = 1
            WITH (
                FILLFACTOR          = 85,
                ONLINE              = OFF,
                STATISTICS_NORECOMPUTE = OFF
            );
            PRINT 'Indeks IX_Mon_Kontrahent_Historia utworzony.';
        END
        ELSE
            PRINT 'Indeks IX_Mon_Kontrahent_Historia już istnieje — pominięto.';
        """
    )
    logger.info("Indeks IX_Mon_Kontrahent_Historia: OK")


# ─── REJESTRACJA W SCHEMACHECKSUMS ───────────────────────────────────────────


def _register_index_checksums() -> None:
    """
    Rejestruje informacje o indeksach w tabeli SchemaChecksums.

    Checksum dla indeksów nie pochodzi z sys.sql_modules (bo indeksy tam nie są),
    lecz z deterministycznej funkcji CHECKSUM() na nazwie obiektu + tabeli.
    Przy weryfikacji startu aplikacja porównuje tę wartość z sys.indexes.
    """
    # Indeks 1: IX_Roz_Kontrahent_Dlugi
    op.execute(
        f"""
        MERGE [{SCHEMA_EXT}].[SchemaChecksums] AS target
        USING (
            SELECT
                N'IX_Roz_Kontrahent_Dlugi'              AS ObjectName,
                N'{SCHEMA_WAPRO}'                        AS SchemaName,
                N'INDEX'                                 AS ObjectType,
                CHECKSUM(N'IX_Roz_Kontrahent_Dlugi' +
                         N'Rozrachunek' +
                         N'{SCHEMA_WAPRO}')              AS Checksum,
                N'{revision}'                            AS AlembicRevision,
                GETDATE()                                AS LastVerifiedAt
        ) AS source
            ON target.[ObjectName] = source.[ObjectName]
           AND target.[SchemaName] = source.[SchemaName]
           AND target.[ObjectType] = source.[ObjectType]
        WHEN MATCHED THEN
            UPDATE SET
                [Checksum]        = source.[Checksum],
                [AlembicRevision] = source.[AlembicRevision],
                [LastVerifiedAt]  = source.[LastVerifiedAt],
                [UpdatedAt]       = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT ([ObjectName], [SchemaName], [ObjectType], [Checksum], [AlembicRevision], [LastVerifiedAt])
            VALUES (source.[ObjectName], source.[SchemaName], source.[ObjectType],
                    source.[Checksum], source.[AlembicRevision], source.[LastVerifiedAt]);
        """
    )

    # Indeks 2: IX_Roz_Faktura_Kontrahent
    op.execute(
        f"""
        MERGE [{SCHEMA_EXT}].[SchemaChecksums] AS target
        USING (
            SELECT
                N'IX_Roz_Faktura_Kontrahent'             AS ObjectName,
                N'{SCHEMA_WAPRO}'                        AS SchemaName,
                N'INDEX'                                  AS ObjectType,
                CHECKSUM(N'IX_Roz_Faktura_Kontrahent' +
                         N'Rozrachunek' +
                         N'{SCHEMA_WAPRO}')              AS Checksum,
                N'{revision}'                            AS AlembicRevision,
                GETDATE()                                AS LastVerifiedAt
        ) AS source
            ON target.[ObjectName] = source.[ObjectName]
           AND target.[SchemaName] = source.[SchemaName]
           AND target.[ObjectType] = source.[ObjectType]
        WHEN MATCHED THEN
            UPDATE SET
                [Checksum]        = source.[Checksum],
                [AlembicRevision] = source.[AlembicRevision],
                [LastVerifiedAt]  = source.[LastVerifiedAt],
                [UpdatedAt]       = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT ([ObjectName], [SchemaName], [ObjectType], [Checksum], [AlembicRevision], [LastVerifiedAt])
            VALUES (source.[ObjectName], source.[SchemaName], source.[ObjectType],
                    source.[Checksum], source.[AlembicRevision], source.[LastVerifiedAt]);
        """
    )

    # Indeks 3: IX_Mon_Kontrahent_Historia
    op.execute(
        f"""
        MERGE [{SCHEMA_EXT}].[SchemaChecksums] AS target
        USING (
            SELECT
                N'IX_Mon_Kontrahent_Historia'            AS ObjectName,
                N'{SCHEMA_EXT}'                          AS SchemaName,
                N'INDEX'                                  AS ObjectType,
                CHECKSUM(N'IX_Mon_Kontrahent_Historia' +
                         N'MonitHistory' +
                         N'{SCHEMA_EXT}')               AS Checksum,
                N'{revision}'                            AS AlembicRevision,
                GETDATE()                                AS LastVerifiedAt
        ) AS source
            ON target.[ObjectName] = source.[ObjectName]
           AND target.[SchemaName] = source.[SchemaName]
           AND target.[ObjectType] = source.[ObjectType]
        WHEN MATCHED THEN
            UPDATE SET
                [Checksum]        = source.[Checksum],
                [AlembicRevision] = source.[AlembicRevision],
                [LastVerifiedAt]  = source.[LastVerifiedAt],
                [UpdatedAt]       = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT ([ObjectName], [SchemaName], [ObjectType], [Checksum], [AlembicRevision], [LastVerifiedAt])
            VALUES (source.[ObjectName], source.[SchemaName], source.[ObjectType],
                    source.[Checksum], source.[AlembicRevision], source.[LastVerifiedAt]);
        """
    )

    logger.info("SchemaChecksums — wpisy dla 3 indeksów: OK")


# ─── HELPERS ─────────────────────────────────────────────────────────────────


def _drop_if_exists(schema: str, table: str, index_name: str) -> None:
    op.execute(
        f"""
        IF EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{schema}].[{table}]')
              AND name = N'{index_name}'
        )
        BEGIN
            DROP INDEX [{index_name}] ON [{schema}].[{table}];
            PRINT 'Indeks {index_name} usunięty.';
        END
        """
    )
    logger.info("Usunięto indeks: %s.%s.%s", schema, table, index_name)