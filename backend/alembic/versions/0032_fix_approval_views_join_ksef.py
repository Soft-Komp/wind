"""
0032_fix_approval_views_join_ksef
══════════════════════════════════
Poprawka JOIN w widokach approval — id_document to KSEF_ID (string),
nie ID_BUF_DOKUMENT (INT).

Wszystkie trzy widoki miały błędny warunek:
    fah.[ID_BUF_DOKUMENT] = TRY_CAST(dai.[id_document] AS INT)
co zawsze zwracało NULL bo id_document = '6840010009-20260410-...' (KSEF_ID).

Poprawny JOIN:
    fah.[KSEF_ID] = dai.[id_document]

Widoki do aktualizacji:
  - skw_v_approval_dispatch_queue
  - skw_v_approval_instance_detail
  - skw_v_approval_my_queue

Po zmianie widoków aktualizujemy skw_SchemaChecksums (MERGE).

Revision ID : 0032
Revises     : 0031
"""

import logging
from alembic import op

revision      = "0032"
down_revision = "0031"
branch_labels = None
depends_on    = None

SCHEMA = "dbo"

logger = logging.getLogger(f"alembic.migration.{revision}")


def _log(krok: str, msg: str) -> None:
    logger.info("0032 [%s] %s", krok, msg)


def _execute(sql: str) -> None:
    op.execute(sql)


def upgrade() -> None:
    logger.info("0032 upgrade — naprawa JOIN KSEF_ID w widokach approval")

    # ── KROK 01: skw_v_approval_dispatch_queue ────────────────────────────────
    _log("01", "CREATE OR ALTER VIEW skw_v_approval_dispatch_queue")
    _execute(f"""
CREATE OR ALTER VIEW [{SCHEMA}].[skw_v_approval_dispatch_queue] AS
SELECT
    dai.[id_instance],
    dai.[id_document],
    dai.[id_source],
    ds.[source_name],
    dai.[status],
    dai.[is_urgent],
    dai.[current_step],
    dai.[document_title],
    dai.[document_amount],
    dai.[extra_data],
    dai.[created_at]                    AS instance_created_at,
    dai.[updated_at]                    AS instance_updated_at,
    dai.[deadline_at],
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
FROM [{SCHEMA}].[skw_document_approval_instances] dai
INNER JOIN [{SCHEMA}].[skw_document_sources] ds
       ON ds.[id_source] = dai.[id_source]
LEFT  JOIN [{SCHEMA}].[skw_faktury_akceptacja_naglowek] fah
       ON  ds.[source_name] = N'fakir'
       AND fah.[KSEF_ID]    = dai.[id_document]
WHERE dai.[status] = N'pending_dispatch'
""")
    _log("01", "OK")

    # ── KROK 02: skw_v_approval_instance_detail ───────────────────────────────
    _log("02", "CREATE OR ALTER VIEW skw_v_approval_instance_detail")
    _execute(f"""
CREATE OR ALTER VIEW [{SCHEMA}].[skw_v_approval_instance_detail] AS
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
FROM [{SCHEMA}].[skw_document_approval_instances] dai
INNER JOIN [{SCHEMA}].[skw_document_sources] ds
       ON ds.[id_source] = dai.[id_source]
LEFT  JOIN [{SCHEMA}].[skw_Users] u_disp
       ON u_disp.[ID_USER] = dai.[dispatched_by]
LEFT  JOIN [{SCHEMA}].[skw_document_approval_snapshot_steps] snap
       ON  snap.[id_instance] = dai.[id_instance]
       AND snap.[step_order]  = dai.[current_step]
LEFT  JOIN [{SCHEMA}].[skw_approval_groups] ag
       ON ag.[id_group] = snap.[id_group]
LEFT  JOIN [{SCHEMA}].[skw_faktury_akceptacja_naglowek] fah
       ON  ds.[source_name] = N'fakir'
       AND fah.[KSEF_ID]    = dai.[id_document]
""")
    _log("02", "OK")

    # ── KROK 03: skw_v_approval_my_queue ─────────────────────────────────────
    _log("03", "CREATE OR ALTER VIEW skw_v_approval_my_queue")
    _execute(f"""
CREATE OR ALTER VIEW [{SCHEMA}].[skw_v_approval_my_queue] AS
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
    dai.[created_at]                AS instance_created_at,
    snap.[id_snapshot]              AS snapshot_id,
    snap.[id_group],
    ag.[group_name],
    ag.[consensus_type],
    snap.[votes_cast],
    snap.[votes_required],
    snap.[deadline_at]              AS step_deadline,
    gm.[id_user]                    AS member_id_user,
    del.[id_user_to]                AS delegate_id_user,
    COALESCE(del.[id_user_to], gm.[id_user]) AS authorized_id_user,
    CASE WHEN del.[id_delegation] IS NOT NULL THEN 1 ELSE 0 END AS via_delegation,
    del.[id_delegation],
    del.[id_user_from]              AS delegated_from_id,
    fah.[NUMER]                     AS fakir_numer,
    fah.[WARTOSC_BRUTTO]            AS fakir_wartosc_brutto,
    fah.[NazwaKontrahenta]          AS fakir_kontrahent
FROM [{SCHEMA}].[skw_document_approval_instances] dai
INNER JOIN [{SCHEMA}].[skw_document_sources] ds
       ON ds.[id_source] = dai.[id_source]
INNER JOIN [{SCHEMA}].[skw_document_approval_snapshot_steps] snap
       ON  snap.[id_instance] = dai.[id_instance]
       AND snap.[step_order]  = dai.[current_step]
       AND snap.[status]      = N'in_progress'
INNER JOIN [{SCHEMA}].[skw_approval_groups] ag
       ON ag.[id_group] = snap.[id_group]
INNER JOIN [{SCHEMA}].[skw_approval_group_members] gm
       ON gm.[id_group] = snap.[id_group]
LEFT  JOIN [{SCHEMA}].[skw_approval_delegations] del
       ON  del.[id_user_from] = gm.[id_user]
       AND del.[is_active]    = 1
       AND del.[valid_from]   <= SYSUTCDATETIME()
       AND del.[valid_to]     >= SYSUTCDATETIME()
       AND (del.[id_group] = snap.[id_group] OR del.[id_group] IS NULL)
LEFT  JOIN [{SCHEMA}].[skw_faktury_akceptacja_naglowek] fah
       ON  ds.[source_name] = N'fakir'
       AND fah.[KSEF_ID]    = dai.[id_document]
WHERE dai.[status] = N'in_progress'
""")
    _log("03", "OK")

    # ── KROK 04: Aktualizacja SchemaChecksums ─────────────────────────────────
    _log("04", "MERGE skw_SchemaChecksums")
    _execute(f"""
MERGE [{SCHEMA}].[skw_SchemaChecksums] AS target
USING (
    SELECT
        obj.[name]                  AS ObjectName,
        sch.[name]                  AS SchemaName,
        N'VIEW'                     AS ObjectType,
        CHECKSUM(mod.[definition])  AS Checksum
    FROM sys.objects obj
    JOIN sys.schemas sch ON sch.[schema_id] = obj.[schema_id]
    JOIN sys.sql_modules mod ON mod.[object_id] = obj.[object_id]
    WHERE obj.[type] = N'V'
      AND sch.[name] = N'{SCHEMA}'
      AND obj.[name] IN (
          N'skw_v_approval_dispatch_queue',
          N'skw_v_approval_instance_detail',
          N'skw_v_approval_my_queue'
      )
) AS source
    ON target.[ObjectName] = source.[ObjectName]
   AND target.[SchemaName] = source.[SchemaName]
WHEN MATCHED THEN
    UPDATE SET
        [Checksum]  = source.[Checksum],
        [UpdatedAt] = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([ObjectName],[SchemaName],[ObjectType],[Checksum],[CreatedAt],[UpdatedAt])
    VALUES (source.[ObjectName], source.[SchemaName], source.[ObjectType],
            source.[Checksum], SYSUTCDATETIME(), SYSUTCDATETIME());
""")
    _log("04", "OK")

    logger.info("0032 upgrade — zakończono")


def downgrade() -> None:
    # Downgrade przywraca błędny TRY_CAST — nie ma sensu produkcyjnie,
    # ale wymagany przez Alembic dla spójności łańcucha.
    raise NotImplementedError(
        "Downgrade 0032 nieodwracalny — przywrócenie błędnego JOIN "
        "TRY_CAST(id_document AS INT) byłoby regresją. "
        "W razie potrzeby cofnij ręcznie przez 0031."
    )