"""
0031_fix_approval_log_trigger
══════════════════════════════
Poprawka triggera trg_DenyModify_skw_approval_log.

Problem: trigger z migracji 0028 blokuje KAŻDY UPDATE włącznie
z legalnym UPDATE SET is_voided=1 używanym przez rollback.

Rozwiązanie: trigger przepuszcza UPDATE tylko gdy:
  - modyfikowana jest wyłącznie kolumna is_voided (UPDATE(is_voided))
  - żadna inna kolumna nie jest modyfikowana
  - wartość zmienia się z 0 → 1 (nie odwrotnie)
Każdy inny UPDATE oraz każdy DELETE nadal jest blokowany.

Revision ID : 0031
Revises     : 0030
"""

import logging
from alembic import op

revision      = "0031"
down_revision = "0030"
branch_labels = None
depends_on    = None

SCHEMA = "dbo"

logger = logging.getLogger(f"alembic.migration.{revision}")


def upgrade() -> None:
    logger.info("0031 upgrade — naprawa triggera trg_DenyModify_skw_approval_log")

    op.execute(
        f"CREATE OR ALTER TRIGGER [{SCHEMA}].[trg_DenyModify_skw_approval_log] "
        f"ON [{SCHEMA}].[skw_approval_log] "
        f"AFTER UPDATE, DELETE "
        f"AS "
        f"BEGIN "
        f"    SET NOCOUNT ON; "
        # Zezwól TYLKO na UPDATE kolumny is_voided z 0 → 1
        # UPDATE() sprawdza czy kolumna była w SET — nie jej wartość.
        # Dlatego dodatkowo sprawdzamy INSERTED/DELETED żeby upewnić się
        # że to jedyna zmiana i kierunek jest 0→1.
        f"    IF ( "
        f"        UPDATE([is_voided]) "
        # Żadna inna kolumna nie może być modyfikowana
        f"        AND NOT UPDATE([id_instance]) "
        f"        AND NOT UPDATE([id_user]) "
        f"        AND NOT UPDATE([username_snapshot]) "
        f"        AND NOT UPDATE([action]) "
        f"        AND NOT UPDATE([step_order_snapshot]) "
        f"        AND NOT UPDATE([id_group_snapshot]) "
        f"        AND NOT UPDATE([consensus_snapshot]) "
        f"        AND NOT UPDATE([votes_before]) "
        f"        AND NOT UPDATE([votes_after]) "
        f"        AND NOT UPDATE([details]) "
        f"        AND NOT UPDATE([ip_address]) "
        f"        AND NOT UPDATE([logged_at]) "
        # Kierunek tylko 0→1 — nigdy nie można cofnąć is_voided
        f"        AND NOT EXISTS ( "
        f"            SELECT 1 FROM INSERTED i "
        f"            JOIN DELETED d ON d.[id_log] = i.[id_log] "
        f"            WHERE i.[is_voided] = 0 OR d.[is_voided] = 1 "
        f"        ) "
        f"    ) "
        f"    BEGIN "
        f"        RETURN; "  # ← legalny UPDATE is_voided 0→1, przepuść
        f"    END; "
        # Wszystko inne: DELETE lub UPDATE innych kolumn → blokuj
        f"    RAISERROR( "
        f"        N'APPROVAL_LOG: Modyfikacja skw_approval_log jest ZABRONIONA. "
        f"Tabela jest APPEND-ONLY. Uzyj pola is_voided zamiast DELETE lub UPDATE.', "
        f"        16, 1 "
        f"    ); "
        f"    ROLLBACK TRANSACTION; "
        f"END"
    )

    logger.info("0031 upgrade — trigger zaktualizowany")


def downgrade() -> None:
    logger.info("0031 downgrade — przywracanie oryginalnego triggera (blokuje wszystko)")

    # Przywróć wersję z 0028 — blokuje każdy UPDATE i DELETE
    op.execute(
        f"CREATE OR ALTER TRIGGER [{SCHEMA}].[trg_DenyModify_skw_approval_log] "
        f"ON [{SCHEMA}].[skw_approval_log] "
        f"AFTER UPDATE, DELETE "
        f"AS "
        f"BEGIN "
        f"    SET NOCOUNT ON; "
        f"    RAISERROR( "
        f"        N'APPROVAL_LOG: Modyfikacja skw_approval_log jest ZABRONIONA. "
        f"Tabela jest APPEND-ONLY. Uzyj pola is_voided zamiast DELETE lub UPDATE.', "
        f"        16, 1 "
        f"    ); "
        f"    ROLLBACK TRANSACTION; "
        f"END"
    )

    logger.info("0031 downgrade — trigger przywrócony do wersji blokującej")