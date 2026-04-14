# backend/alembic/versions/0013_schema_checksums_brakujace_widoki.py
"""schema_checksums_brakujace_widoki

Rejestruje checksums 3 widokow w dbo_ext.skw_SchemaChecksums
ktore istnieja w bazie ale nie maja wpisow checksumow:
  - dbo.skw_faktury_akceptacja_naglowek  (DDL z 018_faktura_widoki_dbo.sql)
  - dbo.skw_faktury_akceptacja_pozycje   (DDL z 018_faktura_widoki_dbo.sql)
  - dbo.skw_rozrachunki_faktur           (DDL z migracji 0002/0004)

Checksum obliczany dynamicznie z sys.sql_modules — zawsze aktualny.
MERGE idempotentny — bezpieczny przy re-run.

Revision ID: 0013
Revises: 0012
Create Date: 2026-04-13
"""

from __future__ import annotations

import logging
from typing import Final

from alembic import op

revision: str = "0013"
down_revision: str = "0012"
branch_labels = None
depends_on = None

logger = logging.getLogger(f"alembic.migration.{revision}")

SCHEMA_WAPRO: Final[str] = "dbo"
SCHEMA_EXT: Final[str] = "dbo_ext"

# Widoki do zarejestrowania: (nazwa, revision ktory je stworzyl)
_VIEWS: Final[tuple] = (
    ("skw_faktury_akceptacja_naglowek", "007"),
    ("skw_faktury_akceptacja_pozycje",  "007"),
    ("skw_rozrachunki_faktur",          "0004"),
)


def _merge_checksum(view_name: str, source_revision: str) -> None:
    """
    Rejestruje checksum jednego widoku w skw_SchemaChecksums.

    Uzywa subquery w MERGE zeby pobrac checksum bezposrednio z sys.sql_modules
    w jednym statement — bez DECLARE/SET (pyodbc nie obsluguje multi-batch).
    LastVerifiedAt = NULL celowo — wymusi weryfikacje przy nastepnym starcie.
    """
    logger.info(
        "[%s] MERGE checksum dla [%s].[%s] (source_revision=%s) ...",
        revision, SCHEMA_WAPRO, view_name, source_revision,
    )

    op.execute(
        f"""
        MERGE [{SCHEMA_EXT}].[skw_SchemaChecksums] AS target
        USING (
            SELECT
                N'{view_name}'        AS ObjectName,
                N'{SCHEMA_WAPRO}'     AS SchemaName,
                N'VIEW'               AS ObjectType,
                (
                    SELECT CHECKSUM(m.definition)
                    FROM   sys.sql_modules m
                    JOIN   sys.objects     o ON m.object_id = o.object_id
                    WHERE  o.name                   = N'{view_name}'
                      AND  SCHEMA_NAME(o.schema_id) = N'{SCHEMA_WAPRO}'
                )                     AS Checksum,
                N'{source_revision}'  AS AlembicRevision,
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
        """
    )

    logger.info(
        "[%s] MERGE checksum dla [%s].[%s] -> OK",
        revision, SCHEMA_WAPRO, view_name,
    )


def upgrade() -> None:
    logger.info(
        "[%s] -- UPGRADE START -- rejestracja %d brakujacych checksumow widokow",
        revision, len(_VIEWS),
    )

    for view_name, source_revision in _VIEWS:
        _merge_checksum(view_name, source_revision)

    logger.info(
        "[%s] -- UPGRADE OK -- %d checksumow zarejestrowanych w skw_SchemaChecksums",
        revision, len(_VIEWS),
    )


def downgrade() -> None:
    logger.warning(
        "[%s] -- DOWNGRADE -- usuwanie wpisow checksumow z skw_SchemaChecksums",
        revision,
    )

    for view_name, _ in _VIEWS:
        op.execute(
            f"""
            DELETE FROM [{SCHEMA_EXT}].[skw_SchemaChecksums]
            WHERE  [ObjectName]  = N'{view_name}'
              AND  [SchemaName]  = N'{SCHEMA_WAPRO}'
              AND  [ObjectType]  = N'VIEW';
            """
        )
        logger.info("[%s] DELETE checksum [%s].[%s] -> OK", revision, SCHEMA_WAPRO, view_name)

    logger.warning(
        "[%s] -- DOWNGRADE OK -- wpisy usuniete. "
        "Watchdog pominie weryfikacje tych widokow do nastepnego upgrade.",
        revision,
    )