# backend/alembic/versions/0067_my_queue_kontrahent_object.py
"""0067_my_queue_kontrahent_object

Dodaje do widoku skw_v_approval_my_queue rozwiazane (COALESCE) pola
kontrahent_nazwa / kontrahent_nip, na wniosek frontu (2026-07-24):
GET /approval/my-queue ma zwracac kontrahenta jako pod-obiekt
{"kontrahent": {"nazwa": ..., "nip": ...}} w kazdej pozycji listy.

PROBLEM ZASTANY: pole fakir_kontrahent dziala WYLACZNIE dla
source_name='fakir' (JOIN po KSEF_ID trafia tylko dla tego zrodla).
Dla zrodel uniwersalnych (qa_b_api_push, qa_b_ftp, ksef20, manual_upload)
fakir_kontrahent jest zawsze NULL, mimo ze kontrahent jest zapisany
w extra_data (klucz 'contractor', patrz UnifiedDocument.to_extra_data_json()).

WYROWNANIE Z ISTNIEJACYM WZORCEM (2026-07-24): widok
skw_v_approval_instance_detail (migracja 0066) ustanowil juz identyczny
3-poziomowy fallback dla tego samego problemu:
    kanoniczne (fah.* / extra_data.*) -> extra_data.verified_*
Ta migracja stosuje DOKLADNIE ten sam wzorzec zamiast prostszego
2-poziomowego COALESCE — jeden spojny wzorzec fallbacku w calym kodzie
jest wart wiecej niz oszczednosc jednego poziomu.

Kolumny ISTNIEJACE (fakir_numer/fakir_wartosc_brutto/fakir_kontrahent)
NIE sa usuwane — endpoint GET /my-queue nadal ich uzywa jako plaskich
pol, kontrakt wsteczny zachowany. Dodane sa DWIE nowe, juz rozwiazane
kolumny:
    kontrahent_nazwa = COALESCE(fah.NazwaKontrahenta,
                                 extra_data.contractor,
                                 extra_data.verified_contractor)
    kontrahent_nip   = COALESCE(extra_data.nip,
                                 extra_data.verified_nip)
    (NIP nie ma odpowiednika w fah w tym widoku - skw_faktury_akceptacja_
    naglowek historycznie nie byl tu joinowany po NIP, wiec brak regresji
    wzgledem stanu poprzedniego)

Revision ID : 0067
Revises     : 0066
"""
from alembic import op

revision = "0067"
down_revision = "0066"
branch_labels = None
depends_on = None

SCHEMA = "dbo"
VIEW = "skw_v_approval_my_queue"

_VIEW_DDL = f"""
CREATE OR ALTER VIEW [{SCHEMA}].[{VIEW}] AS
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
    fah.[NazwaKontrahenta]          AS fakir_kontrahent,
    -- ── NOWE (migracja 0067) — pod-obiekt kontrahenta, wyrownany
    --    z 3-poziomowym wzorcem fallbacku z widoku
    --    skw_v_approval_instance_detail (migracja 0066). ────────────────
    COALESCE(
        fah.[NazwaKontrahenta],
        JSON_VALUE(dai.[extra_data], '$.contractor'),
        JSON_VALUE(dai.[extra_data], '$.verified_contractor')
    )                                AS kontrahent_nazwa,
    COALESCE(
        JSON_VALUE(dai.[extra_data], '$.nip'),
        JSON_VALUE(dai.[extra_data], '$.verified_nip')
    )                                AS kontrahent_nip
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
"""

# Definicja POPRZEDNIA (0032) — do downgrade
_VIEW_DDL_PREVIOUS = f"""
CREATE OR ALTER VIEW [{SCHEMA}].[{VIEW}] AS
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
"""


def _checksum_merge(revision_tag: str) -> str:
    """Wzorzec identyczny jak w migracjach 0061/0062/0066 — MERGE do skw_SchemaChecksums."""
    return f"""
MERGE [{SCHEMA}].[skw_SchemaChecksums] AS target
USING (
    SELECT
        obj.[name]                  AS ObjectName,
        sch.[name]                  AS SchemaName,
        N'VIEW'                     AS ObjectType,
        CHECKSUM(mod.[definition])  AS Checksum,
        N'{revision_tag}'           AS AlembicRevision,
        NULL                        AS LastVerifiedAt,
        SYSUTCDATETIME()            AS Now
    FROM sys.objects  obj
    JOIN sys.schemas  sch ON sch.[schema_id] = obj.[schema_id]
    JOIN sys.sql_modules mod ON mod.[object_id] = obj.[object_id]
    WHERE obj.[type] = N'V'
      AND sch.[name] = N'{SCHEMA}'
      AND obj.[name] = N'{VIEW}'
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
    op.execute(_VIEW_DDL)
    op.execute(_checksum_merge("0067"))


def downgrade() -> None:
    op.execute(_VIEW_DDL_PREVIOUS)
    op.execute(_checksum_merge("0066"))