"""0053_manual_upload_ocr_review

Rozszerza CHECK constraint statusu o 'ocr_review_pending' (recznie wgrane
dokumenty PDF oczekujace na weryfikacje OCR), zaklada zrodlo 'manual_upload'
i seeduje uprawnienie 'documents.upload'.

WAZNE: sekcja seeda uprawnienia oznaczona do weryfikacji przed uruchomieniem
na produkcji — nazwy kolumn skw_Permissions/skw_RolePermissions przyjete
przez analogie, nie zweryfikowane bezposrednio w tej sesji.
"""
from alembic import op
from sqlalchemy import text

revision = "0053"
down_revision = "0052"
branch_labels = None
depends_on = None

SCHEMA = "dbo"


def _execute(sql: str) -> None:
    op.get_bind().execute(text(sql))


def _log(step: str, msg: str) -> None:
    print(f"[0053-{step}] {msg}")


def upgrade() -> None:
    # ── KROK 1: CHECK constraint status — dodanie 'ocr_review_pending' ───────
    _log("01", "CHECK constraint status — dodanie ocr_review_pending")

    conn = op.get_bind()
    result = conn.execute(text(f"""
        SELECT cc.name
          FROM sys.check_constraints cc
         WHERE cc.parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_approval_instances]')
           AND cc.definition LIKE N'%status%'
    """))
    row = result.fetchone()
    if row:
        old_constraint = row[0]
        _log("01a", f"Znaleziono stary constraint: {old_constraint} — DROP")
        _execute(
            f"ALTER TABLE [{SCHEMA}].[skw_document_approval_instances] "
            f"DROP CONSTRAINT [{old_constraint}]"
        )
    else:
        _log("01a", "Brak istniejacego CHECK constraint na status — pomijam DROP")

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_approval_instances]')
              AND name = N'CK_skw_dai_status_etap21'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_approval_instances]
            ADD CONSTRAINT [CK_skw_dai_status_etap21]
                CHECK ([status] IN (
                    N'pending_dispatch',
                    N'in_progress',
                    N'approved',
                    N'cancelled',
                    N'rejected',
                    N'unassigned',
                    N'duplicate_pending',
                    N'source_orphaned',
                    N'ocr_review_pending'
                ))
    """)
    _log("01b", "CREATE CK_skw_dai_status_etap21 (9 wartosci) OK")

    # ── KROK 2: zrodlo 'manual_upload' — idempotentny INSERT WHERE NOT EXISTS ─
    _log("02", "Seed zrodla manual_upload")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM [{SCHEMA}].[skw_document_sources]
            WHERE [source_name] = N'manual_upload'
        )
        INSERT INTO [{SCHEMA}].[skw_document_sources]
            ([source_name], [description], [source_type], [connection_mode],
             [sync_interval_minutes], [is_test_mode], [is_active],
             [created_at], [updated_at])
        VALUES
            (N'manual_upload', N'Recznie wgrywane dokumenty PDF (ksiegowosc)',
             N'manual', N'pull', 15, 0, 1, SYSUTCDATETIME(), SYSUTCDATETIME())
    """)
    _log("02", "OK")

    # ── KROK 3: uprawnienie 'documents.upload' + przypisanie do roli Ksiegowosc ─
    _log("03", "Seed uprawnienia documents.upload")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM [{SCHEMA}].[skw_Permissions]
            WHERE [PermissionName] = N'documents.upload'
        )
        INSERT INTO [{SCHEMA}].[skw_Permissions]
            ([PermissionName], [Description], [Category], [IsActive], [CreatedAt])
        VALUES
            (N'documents.upload', N'Reczne wgrywanie dokumentow PDF do listy obiegowej',
             N'documents', 1, GETDATE())
    """)

    conn = op.get_bind()
    perm_row = conn.execute(text(
        f"SELECT [ID_PERMISSION] FROM [{SCHEMA}].[skw_Permissions] WHERE [PermissionName] = N'documents.upload'"
    )).fetchone()
    role_row = conn.execute(text(
        f"SELECT [ID_ROLE] FROM [{SCHEMA}].[skw_Roles] WHERE [RoleName] = N'Księgowość'"
    )).fetchone()

    if perm_row and role_row:
        id_permission, id_role = perm_row[0], role_row[0]
        _execute(f"""
            IF NOT EXISTS (
                SELECT 1 FROM [{SCHEMA}].[skw_RolePermissions]
                WHERE [ID_ROLE] = {id_role} AND [ID_PERMISSION] = {id_permission}
            )
            INSERT INTO [{SCHEMA}].[skw_RolePermissions] ([ID_ROLE],[ID_PERMISSION],[CreatedAt])
            VALUES ({id_role}, {id_permission}, GETDATE())
        """)
        _log("03c", f"Przypisano documents.upload (id={id_permission}) do roli Ksiegowosc (id={id_role})")
    else:
        _log("03c", "UWAGA: brak roli 'Księgowość' lub permission po insert — sprawdz recznie w SSMS")

    _log("DONE", "Migracja 0053 zakonczona")


def downgrade() -> None:
    _execute(f"""
        IF EXISTS (
            SELECT 1 FROM [{SCHEMA}].[skw_Permissions]
            WHERE [PermissionName] = N'documents.upload'
        )
        DELETE FROM [{SCHEMA}].[skw_Permissions] WHERE [PermissionName] = N'documents.upload'
    """)
    _execute(f"""
        IF EXISTS (
            SELECT 1 FROM [{SCHEMA}].[skw_document_sources]
            WHERE [source_name] = N'manual_upload'
        )
        DELETE FROM [{SCHEMA}].[skw_document_sources] WHERE [source_name] = N'manual_upload'
    """)
    _execute(f"""
        IF EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_approval_instances]')
              AND name = N'CK_skw_dai_status_etap21'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_approval_instances]
            DROP CONSTRAINT [CK_skw_dai_status_etap21]
    """)
    _execute(f"""
        ALTER TABLE [{SCHEMA}].[skw_document_approval_instances]
            ADD CONSTRAINT [CK_skw_dai_status_etap2]
                CHECK ([status] IN (
                    N'pending_dispatch', N'in_progress', N'approved', N'cancelled',
                    N'rejected', N'unassigned', N'duplicate_pending', N'source_orphaned'
                ))
    """)