# backend/alembic/versions/0058_auto_dispatch_error_threshold.py
"""0058_auto_dispatch_error_threshold

Seed SystemConfig AUTO_DISPATCH_ERROR_ALERT_THRESHOLD — prog liczby bledow
w jednym cyklu auto_dispatch_task, po ktorym worker publikuje SSE
system_notification do adminow (worker/tasks/auto_dispatch_task.py,
_publish_dispatch_errors_alert).

Kontekst: naprawa brakujacego SSE document_waiting po automatycznym
dispatch (sesja 2026-07-16, self-review "publikuj wszystkie wazne rzeczy")
— przy okazji dodano tez alert bledow.

Revision ID : 0058
Revises     : 0057
"""
from alembic import op
from sqlalchemy import text

revision = "0058"
down_revision = "0057"
branch_labels = None
depends_on = None

SCHEMA = "dbo"


def upgrade() -> None:
    op.execute(text(f"""
        MERGE [{SCHEMA}].[skw_SystemConfig] AS target
        USING (SELECT
            N'AUTO_DISPATCH_ERROR_ALERT_THRESHOLD' AS ConfigKey,
            N'3' AS ConfigValue,
            N'Prog liczby bledow w jednym cyklu auto_dispatch_task (co 1 min), '
            + N'po ktorym worker publikuje SSE system_notification do adminow '
            + N'(channel:admins). Powtarzajace sie bledy moga oznaczac systemowy '
            + N'problem (filter_engine, baza), nie pojedynczy zly dokument.' AS Description
        ) AS source
        ON target.[ConfigKey] = source.ConfigKey
        WHEN NOT MATCHED THEN
            INSERT ([ConfigKey], [ConfigValue], [Description], [IsActive])
            VALUES (source.ConfigKey, source.ConfigValue, source.Description, 1);
    """))


def downgrade() -> None:
    op.execute(text(f"""
        DELETE FROM [{SCHEMA}].[skw_SystemConfig]
        WHERE [ConfigKey] = N'AUTO_DISPATCH_ERROR_ALERT_THRESHOLD'
    """))