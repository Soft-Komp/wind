# backend/alembic/versions/0059_notification_type_check_constraint.py
"""0059_notification_type_check_constraint

NAPRAWA (sesja 2026-07-16, "dokoncz symetrie unassigned"): odkryto, ze
CHECK constraint na skw_user_notifications.notification_type NIGDY nie
zostal fizycznie utworzony w migracji 0028 — kolumna ma tylko
NVARCHAR(50) NOT NULL. Lista dozwolonych wartosci istniala WYLACZNIE
jako komentarz w ORM (user_notification.py) i docstring w
notification_task.py — umowna konwencja, nie wymuszenie SQL.

Ta migracja:
  1. Dodaje PRAWDZIWY CHECK constraint (CK_skw_un_notification_type),
     obejmujacy wszystkie 6 dotychczasowych wartosci + NOWA wartosc
     'approval_unassigned' (powiadomienie administratorow gdy
     auto_dispatch_task nie znajdzie sciezki obiegu dla dokumentu —
     worker/tasks/auto_dispatch_task.py, galaz "unassigned").
  2. Uzywa WITH NOCHECK przy ADD CONSTRAINT — jesli w tabeli istnieja
     juz wiersze z wartosciami spoza listy (nieprawdopodobne, bo caly
     kod zawsze uzywal tylko wartosci z docstringu, ale nie
     zweryfikowane empirycznie), migracja i tak przejdzie. Nowe INSERTy
     BEDA jednak wymuszane od tego momentu. Swiadomy kompromis —
     retrofitting constraintu na dzialajaca tabele bez ryzyka
     nieudanej migracji z powodu historycznych danych.

Revision ID : 0059
Revises     : 0058
"""
from alembic import op
from sqlalchemy import text

revision = "0059"
down_revision = "0058"
branch_labels = None
depends_on = None

SCHEMA = "dbo"

_ALLOWED_TYPES = (
    "approval_pending",
    "approval_accepted",
    "approval_rejected",
    "approval_deadline_warning",
    "approval_deadline_expired",
    "approval_escalated",
    "approval_unassigned",  # NOWY (sesja 2026-07-16)
)


def upgrade() -> None:
    bind = op.get_bind()

    values_sql = ", ".join(f"N'{v}'" for v in _ALLOWED_TYPES)

    bind.execute(text(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_user_notifications]')
              AND name = N'CK_skw_un_notification_type'
        )
        BEGIN
            ALTER TABLE [{SCHEMA}].[skw_user_notifications] WITH NOCHECK
                ADD CONSTRAINT [CK_skw_un_notification_type]
                    CHECK ([notification_type] IN ({values_sql}));
            PRINT N'[0059] CK_skw_un_notification_type dodany (WITH NOCHECK — '
                + N'nie wymusza na istniejacych wierszach, tylko od teraz).';
        END
        ELSE
            PRINT N'[0059] CK_skw_un_notification_type juz istnieje — pomijam.';
    """))


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(text(f"""
        IF EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_user_notifications]')
              AND name = N'CK_skw_un_notification_type'
        )
        ALTER TABLE [{SCHEMA}].[skw_user_notifications]
            DROP CONSTRAINT [CK_skw_un_notification_type]
    """))