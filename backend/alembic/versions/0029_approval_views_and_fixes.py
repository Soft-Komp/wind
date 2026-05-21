# backend/alembic/versions/0029_approval_views_and_fixes.py
"""
Migracja 0029 — Widoki SQL modulu Approval + brakujace kolumny.

Kroki:
  01 — ALTER TABLE skw_document_approval_instances:
       dodaj is_deadline_notified BIT NOT NULL DEFAULT 0
       dodaj marked_urgent_by     INT NULL (FK do skw_Users)
       dodaj marked_urgent_at     DATETIME NULL
  02 — CREATE OR ALTER VIEW dbo.skw_v_approval_dispatch_queue
  03 — CREATE OR ALTER VIEW dbo.skw_v_approval_instance_detail
  04 — CREATE OR ALTER VIEW dbo.skw_v_approval_my_queue
  05 — MERGE skw_SchemaChecksums (rejestracja 3 widokow)

UWAGI ODBC Driver 18:
  - CREATE OR ALTER VIEW musi byc jedyna instrukcja w batchu
    (osobny op.execute() per widok)
  - Brak polskich znakow w komentarzach wewnatrz SQL widoku
  - Brak implicit concatenation N'...' N'...'

Revision: 0029
Down revision: 0028
"""

from alembic import op
from sqlalchemy import text

revision      = "0029"
down_revision = "0028"
branch_labels = None
depends_on    = None

SCHEMA = "dbo"


def _log(step: str, msg: str) -> None:
    print(f"[0029] KROK {step} | {msg}")


def _execute(sql: str, params: dict | None = None) -> None:
    if params:
        op.execute(text(sql), params)
    else:
        op.execute(text(sql))


# =============================================================================
# UPGRADE
# =============================================================================

def upgrade() -> None:

    # ── KROK 01: Brakujace kolumny ────────────────────────────────────────────
    _log("01", "ALTER TABLE skw_document_approval_instances — brakujace kolumny")

    # is_deadline_notified — flaga uzywana przez deadline_task
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_approval_instances]')
              AND name = N'is_deadline_notified'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_approval_instances]
            ADD [is_deadline_notified] BIT NOT NULL DEFAULT 0
    """)

    # marked_urgent_by — kto oznaczyl jako pilny
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_approval_instances]')
              AND name = N'marked_urgent_by'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_approval_instances]
            ADD [marked_urgent_by] INT NULL
    """)

    # marked_urgent_at — kiedy oznaczono jako pilny
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_approval_instances]')
              AND name = N'marked_urgent_at'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_approval_instances]
            ADD [marked_urgent_at] DATETIME NULL
    """)

    _log("01", "OK")

    # ── KROK 02: VIEW skw_v_approval_dispatch_queue ───────────────────────────
    # Widok kolejki dyspozytora — dokumenty pending_dispatch z danymi Fakir.
    # UWAGA: CREATE OR ALTER VIEW musi byc jedyna instrukcja w batchu.
    _log("02", "CREATE OR ALTER VIEW dbo.skw_v_approval_dispatch_queue")
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
    dai.[deadline_at],
    dai.[created_at]                AS instance_created_at,
    dai.[updated_at]                AS instance_updated_at,
    dai.[dispatched_by],
    u.[Username]                    AS dispatched_by_username,
    u.[FullName]                    AS dispatched_by_fullname,
    p.[path_name],
    dc.[category_name],
    fah.[NUMER]                     AS fakir_numer,
    fah.[WARTOSC_BRUTTO]            AS fakir_wartosc_brutto,
    fah.[WARTOSC_NETTO]             AS fakir_wartosc_netto,
    fah.[NazwaKontrahenta]          AS fakir_kontrahent,
    fah.[DataWystawienia]           AS fakir_data_wystawienia,
    fah.[TerminPlatnosci]           AS fakir_termin_platnosci,
    fah.[FORMA_PLATNOSCI]           AS fakir_forma_platnosci,
    fah.[KOD_STATUSU]               AS fakir_status_zewnetrzny,
    fah.[StatusOpis]                AS fakir_status_opis
FROM [{SCHEMA}].[skw_document_approval_instances] dai
INNER JOIN [{SCHEMA}].[skw_document_sources] ds
       ON ds.[id_source] = dai.[id_source]
LEFT  JOIN [{SCHEMA}].[skw_Users] u
       ON u.[ID_USER] = dai.[dispatched_by]
LEFT  JOIN [{SCHEMA}].[skw_approval_paths] p
       ON p.[id_path] = dai.[id_path]
LEFT  JOIN [{SCHEMA}].[skw_document_categories] dc
       ON dc.[id_category] = dai.[id_category]
LEFT  JOIN [{SCHEMA}].[skw_faktury_akceptacja_naglowek] fah
       ON  ds.[source_name]      = N'fakir'
       AND fah.[ID_BUF_DOKUMENT] = TRY_CAST(dai.[id_document] AS INT)
WHERE dai.[status] = N'pending_dispatch'
""")
    _log("02", "OK")

    # ── KROK 03: VIEW skw_v_approval_instance_detail ──────────────────────────
    # Szczegoly instancji — jeden wiersz per instancja z biezacym krokiem.
    _log("03", "CREATE OR ALTER VIEW dbo.skw_v_approval_instance_detail")
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
    dai.[extra_data],
    dai.[deadline_at],
    dai.[created_at]                AS instance_created_at,
    dai.[updated_at]                AS instance_updated_at,
    dai.[dispatched_at],
    dai.[completed_at],
    dai.[dispatched_by],
    u_disp.[Username]               AS dispatched_by_username,
    u_disp.[FullName]               AS dispatched_by_fullname,
    dai.[is_deadline_notified],
    dai.[marked_urgent_by],
    dai.[marked_urgent_at],
    p.[id_path],
    p.[path_name],
    dc.[id_category],
    dc.[category_name],
    snap.[id_snapshot]              AS current_snapshot_id,
    snap.[id_group]                 AS current_id_group,
    ag.[group_name]                 AS current_group_name,
    ag.[consensus_type]             AS current_consensus_type,
    snap.[votes_cast]               AS current_votes_cast,
    snap.[votes_required]           AS current_votes_required,
    snap.[deadline_at]              AS step_deadline_at,
    snap.[status]                   AS step_status,
    fah.[NUMER]                     AS fakir_numer,
    fah.[WARTOSC_BRUTTO]            AS fakir_wartosc_brutto,
    fah.[WARTOSC_NETTO]             AS fakir_wartosc_netto,
    fah.[KWOTA_VAT]                 AS fakir_kwota_vat,
    fah.[NazwaKontrahenta]          AS fakir_kontrahent,
    fah.[EmailKontrahenta]          AS fakir_email_kontrahenta,
    fah.[DataWystawienia]           AS fakir_data_wystawienia,
    fah.[TerminPlatnosci]           AS fakir_termin_platnosci,
    fah.[FORMA_PLATNOSCI]           AS fakir_forma_platnosci,
    fah.[KOD_STATUSU]               AS fakir_status_zewnetrzny,
    fah.[StatusOpis]                AS fakir_status_opis,
    fah.[UWAGI]                     AS fakir_uwagi
FROM [{SCHEMA}].[skw_document_approval_instances] dai
INNER JOIN [{SCHEMA}].[skw_document_sources] ds
       ON ds.[id_source] = dai.[id_source]
LEFT  JOIN [{SCHEMA}].[skw_Users] u_disp
       ON u_disp.[ID_USER] = dai.[dispatched_by]
LEFT  JOIN [{SCHEMA}].[skw_approval_paths] p
       ON p.[id_path] = dai.[id_path]
LEFT  JOIN [{SCHEMA}].[skw_document_categories] dc
       ON dc.[id_category] = dai.[id_category]
LEFT  JOIN [{SCHEMA}].[skw_document_approval_snapshot_steps] snap
       ON  snap.[id_instance] = dai.[id_instance]
       AND snap.[step_order]  = dai.[current_step]
LEFT  JOIN [{SCHEMA}].[skw_approval_groups] ag
       ON ag.[id_group] = snap.[id_group]
LEFT  JOIN [{SCHEMA}].[skw_faktury_akceptacja_naglowek] fah
       ON  ds.[source_name]      = N'fakir'
       AND fah.[ID_BUF_DOKUMENT] = TRY_CAST(dai.[id_document] AS INT)
""")
    _log("03", "OK")

    # ── KROK 04: VIEW skw_v_approval_my_queue ────────────────────────────────
    # Moja kolejka — dokumenty czekajace na akcje usera (czlonek lub delegat).
    # Kazdy wiersz zawiera authorized_id_user = uzytkownik uprawniony do akcji.
    # Backend filtruje: WHERE authorized_id_user = current_user.ID_USER
    _log("04", "CREATE OR ALTER VIEW dbo.skw_v_approval_my_queue")
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
       ON  ds.[source_name]      = N'fakir'
       AND fah.[ID_BUF_DOKUMENT] = TRY_CAST(dai.[id_document] AS INT)
WHERE dai.[status] = N'in_progress'
""")
    _log("04", "OK")

    # ── KROK 05: MERGE skw_SchemaChecksums ───────────────────────────────────
    _log("05", "MERGE skw_SchemaChecksums — rejestracja 3 widokow approval")
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
                [Checksum]   = source.[Checksum],
                [UpdatedAt]  = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ([ObjectName],[SchemaName],[ObjectType],[Checksum],[CreatedAt],[UpdatedAt])
            VALUES (source.[ObjectName], source.[SchemaName], source.[ObjectType],
                    source.[Checksum], SYSUTCDATETIME(), SYSUTCDATETIME());
    """)
    _log("05", "OK")


# =============================================================================
# DOWNGRADE
# =============================================================================

def downgrade() -> None:
    _execute(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_v_approval_my_queue]', N'V') IS NOT NULL
            DROP VIEW [{SCHEMA}].[skw_v_approval_my_queue]
    """)
    _execute(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_v_approval_instance_detail]', N'V') IS NOT NULL
            DROP VIEW [{SCHEMA}].[skw_v_approval_instance_detail]
    """)
    _execute(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_v_approval_dispatch_queue]', N'V') IS NOT NULL
            DROP VIEW [{SCHEMA}].[skw_v_approval_dispatch_queue]
    """)
    _execute(f"""
        DELETE FROM [{SCHEMA}].[skw_SchemaChecksums]
        WHERE [ObjectName] IN (
            N'skw_v_approval_dispatch_queue',
            N'skw_v_approval_instance_detail',
            N'skw_v_approval_my_queue'
        )
    """)
    # Kolumny pozostaja — usuwanie kolumn z produkcyjnej tabeli jest ryzykowne
    # Jesli konieczne — wykonaj recznie w SSMS po upewnieniu sie ze nie ma danych