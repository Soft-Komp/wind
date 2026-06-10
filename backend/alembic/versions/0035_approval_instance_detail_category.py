"""
0035_approval_instance_detail_category
═══════════════════════════════════════
Dodanie pól id_category i category_name do widoku
skw_v_approval_instance_detail poprzez JOIN na
skw_document_categories.

Widok był tworzony w migracji 0028/0029 bez tego JOINa.
Endpoint GET /approval/instances/{id} zwraca SELECT * z widoku
więc pola pojawią się automatycznie po aktualizacji widoku.

Revision ID : 0035
Revises     : 0034
"""

import logging

from alembic import op

logger = logging.getLogger(__name__)

revision      = "0035"
down_revision = "0034"
branch_labels = None
depends_on    = None

_SCHEMA = "dbo"
_VIEW   = "skw_v_approval_instance_detail"

_VIEW_DDL = f"""
CREATE OR ALTER VIEW [{_SCHEMA}].[{_VIEW}] AS
SELECT
    dai.[id_instance],
    dai.[id_document],
    dai.[id_source],
    ds.[source_name],
    dai.[status],
    dai.[current_step],
    dai.[is_urgent],
    dai.[document_title],
    dai.[document_amount],
    dai.[deadline_at],
    dai.[created_at]                    AS instance_created_at,
    dai.[updated_at]                    AS instance_updated_at,
    snap.[id_snapshot]                  AS snapshot_id,
    snap.[id_group],
    ag.[group_name],
    ag.[consensus_type],
    snap.[votes_cast],
    snap.[votes_required],
    snap.[deadline_at]                  AS step_deadline,
    u_disp.[Username]                   AS dispatched_by_username,
    dai.[id_category],
    dc.[category_name],
    fah.[NUMER]                         AS fakir_numer,
    fah.[WARTOSC_NETTO]                 AS fakir_wartosc_netto,
    fah.[WARTOSC_BRUTTO]                AS fakir_wartosc_brutto,
    fah.[KWOTA_VAT]                     AS fakir_kwota_vat,
    fah.[NazwaKontrahenta]              AS fakir_kontrahent,
    fah.[KOD_STATUSU]                   AS fakir_status_zewnetrzny,
    fah.[StatusOpis]                    AS fakir_status_opis,
    fah.[DataWystawienia]               AS fakir_data_wystawienia,
    fah.[TerminPlatnosci]               AS fakir_termin_platnosci,
    fah.[FORMA_PLATNOSCI]               AS fakir_forma_platnosci,
    fah.[UWAGI]                         AS fakir_uwagi
FROM [{_SCHEMA}].[skw_document_approval_instances] dai
INNER JOIN [{_SCHEMA}].[skw_document_sources] ds
       ON ds.[id_source] = dai.[id_source]
LEFT  JOIN [{_SCHEMA}].[skw_Users] u_disp
       ON u_disp.[ID_USER] = dai.[dispatched_by]
LEFT  JOIN [{_SCHEMA}].[skw_document_approval_snapshot_steps] snap
       ON  snap.[id_instance] = dai.[id_instance]
       AND snap.[step_order]  = dai.[current_step]
LEFT  JOIN [{_SCHEMA}].[skw_approval_groups] ag
       ON ag.[id_group] = snap.[id_group]
LEFT  JOIN [{_SCHEMA}].[skw_document_categories] dc
       ON dc.[id_category] = dai.[id_category]
LEFT  JOIN [{_SCHEMA}].[skw_faktury_akceptacja_naglowek] fah
       ON  ds.[source_name] = N'fakir'
       AND fah.[KSEF_ID]    = dai.[id_document]
"""

_CHECKSUM_MERGE = f"""
    MERGE [{_SCHEMA}].[skw_SchemaChecksums] AS target
    USING (
        SELECT
            obj.[name]                  AS ObjectName,
            sch.[name]                  AS SchemaName,
            N'VIEW'                     AS ObjectType,
            CHECKSUM(mod.[definition])  AS Checksum,
            N'0035'                     AS AlembicRevision,
            NULL                        AS LastVerifiedAt,
            SYSUTCDATETIME()            AS Now
        FROM sys.objects  obj
        JOIN sys.schemas  sch ON sch.[schema_id] = obj.[schema_id]
        JOIN sys.sql_modules mod ON mod.[object_id] = obj.[object_id]
        WHERE obj.[type] = N'V'
          AND sch.[name] = N'{_SCHEMA}'
          AND obj.[name] = N'{_VIEW}'
    ) AS source
        ON  target.[ObjectName] = source.[ObjectName]
        AND target.[SchemaName] = source.[SchemaName]
    WHEN MATCHED THEN
        UPDATE SET
            [Checksum]        = source.[Checksum],
            [AlembicRevision] = source.[AlembicRevision],
            [LastVerifiedAt]  = source.[LastVerifiedAt],
            [UpdatedAt]       = source.[Now]
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            [ObjectName], [SchemaName], [ObjectType],
            [Checksum], [AlembicRevision],
            [LastVerifiedAt], [CreatedAt]
        )
        VALUES (
            source.[ObjectName], source.[SchemaName], source.[ObjectType],
            source.[Checksum],   source.[AlembicRevision],
            source.[LastVerifiedAt], source.[Now]
        );
"""


def upgrade() -> None:
    logger.info("[0035] START — aktualizacja widoku %s.%s", _SCHEMA, _VIEW)

    op.execute(_VIEW_DDL)
    logger.info("[0035] CREATE OR ALTER VIEW %s.%s → OK", _SCHEMA, _VIEW)

    op.execute(_CHECKSUM_MERGE)
    logger.info("[0035] MERGE skw_SchemaChecksums → OK")

    logger.info("[0035] UPGRADE ZAKOŃCZONY POMYŚLNIE")


def downgrade() -> None:
    logger.warning(
        "[0035] DOWNGRADE — przywracanie widoku %s.%s bez id_category/category_name",
        _SCHEMA, _VIEW,
    )
    op.execute(f"""
        CREATE OR ALTER VIEW [{_SCHEMA}].[{_VIEW}] AS
        SELECT
            dai.[id_instance],
            dai.[id_document],
            dai.[id_source],
            ds.[source_name],
            dai.[status],
            dai.[current_step],
            dai.[is_urgent],
            dai.[document_title],
            dai.[document_amount],
            dai.[deadline_at],
            dai.[created_at]                    AS instance_created_at,
            dai.[updated_at]                    AS instance_updated_at,
            snap.[id_snapshot]                  AS snapshot_id,
            snap.[id_group],
            ag.[group_name],
            ag.[consensus_type],
            snap.[votes_cast],
            snap.[votes_required],
            snap.[deadline_at]                  AS step_deadline,
            u_disp.[Username]                   AS dispatched_by_username,
            fah.[NUMER]                         AS fakir_numer,
            fah.[WARTOSC_NETTO]                 AS fakir_wartosc_netto,
            fah.[WARTOSC_BRUTTO]                AS fakir_wartosc_brutto,
            fah.[KWOTA_VAT]                     AS fakir_kwota_vat,
            fah.[NazwaKontrahenta]              AS fakir_kontrahent,
            fah.[KOD_STATUSU]                   AS fakir_status_zewnetrzny,
            fah.[StatusOpis]                    AS fakir_status_opis,
            fah.[DataWystawienia]               AS fakir_data_wystawienia,
            fah.[TerminPlatnosci]               AS fakir_termin_platnosci,
            fah.[FORMA_PLATNOSCI]               AS fakir_forma_platnosci,
            fah.[UWAGI]                         AS fakir_uwagi
        FROM [{_SCHEMA}].[skw_document_approval_instances] dai
        INNER JOIN [{_SCHEMA}].[skw_document_sources] ds
               ON ds.[id_source] = dai.[id_source]
        LEFT  JOIN [{_SCHEMA}].[skw_Users] u_disp
               ON u_disp.[ID_USER] = dai.[dispatched_by]
        LEFT  JOIN [{_SCHEMA}].[skw_document_approval_snapshot_steps] snap
               ON  snap.[id_instance] = dai.[id_instance]
               AND snap.[step_order]  = dai.[current_step]
        LEFT  JOIN [{_SCHEMA}].[skw_approval_groups] ag
               ON ag.[id_group] = snap.[id_group]
        LEFT  JOIN [{_SCHEMA}].[skw_faktury_akceptacja_naglowek] fah
               ON  ds.[source_name] = N'fakir'
               AND fah.[KSEF_ID]    = dai.[id_document]
    """)

    op.execute(_CHECKSUM_MERGE)
    logger.warning("[0035] DOWNGRADE ZAKOŃCZONY")