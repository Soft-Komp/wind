# backend/alembic/versions/0042_deadline_sms_phone_number.py
"""0042 — phone_number w skw_Users (F7, sekcja 4.18, SMS deadline)

WAZNE: DEADLINE_REMINDER_HOURS_BEFORE, DEADLINE_REMINDER_INTERVAL_HOURS
i DEADLINE_SMS_ENABLED zostaly JUZ zasiane przez migracje 0039 (krok 12,
kategoria 'deadlines'). Ta migracja NIE duplikuje tej pracy — dotyczy
WYLACZNIE jedynego elementu ktory faktycznie brakuje: kolumny phone_number.

Bez tej kolumny SMS deadline (worker/tasks/deadline_task.py, funkcja
_send_sms_reminders) nie ma skad odczytac numeru telefonu uzytkownika —
zapytanie SELECT phone_number rzucilo by Invalid column name.

NULL = uzytkownik nie ma podanego telefonu, SMS po prostu nie jest
wysylany dla niego (fail-safe, nie blokuje pozostalych kanalow).

Revision ID: 0042
Revises:     0041
Create Date: 2026-06-30
"""

from alembic import op
from sqlalchemy import text

revision      = "0042"
down_revision = "0041"
branch_labels = None
depends_on    = None

SCHEMA = "dbo"


def upgrade() -> None:
    op.execute(text(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_Users]')
              AND name = N'phone_number'
        )
        ALTER TABLE [{SCHEMA}].[skw_Users]
            ADD [phone_number] NVARCHAR(20) NULL
    """))


def downgrade() -> None:
    op.execute(text(f"""
        IF EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_Users]')
              AND name = N'phone_number'
        )
        ALTER TABLE [{SCHEMA}].[skw_Users]
            DROP COLUMN [phone_number]
    """))