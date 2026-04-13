# backend/alembic/versions/0012_cache_bypass_config.py
"""cache_bypass_config

Revision ID: 0012
Revises: 0011
Create Date: 2026-04-13
"""
from alembic import op

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        MERGE [dbo_ext].[skw_SystemConfig] AS [target]
        USING (
            VALUES (
                N'cache.bypass_enabled',
                N'false',
                N'Globalny bypass cache Redis — true = dane prosto z DB, '
                N'false = normalne cache. Kolejki ARQ/SSE nienaruszane. '
                N'Zmiana aktywna w ciągu 5 sekund.',
                1
            )
        ) AS [source] ([ConfigKey], [ConfigValue], [Description], [IsActive])
        ON [target].[ConfigKey] = [source].[ConfigKey]
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ([ConfigKey], [ConfigValue], [Description], [IsActive])
            VALUES ([source].[ConfigKey], [source].[ConfigValue],
                    [source].[Description], [source].[IsActive])
        WHEN MATCHED THEN
            UPDATE SET [Description] = [source].[Description];
    """)


def downgrade() -> None:
    op.execute("""
        DELETE FROM [dbo_ext].[skw_SystemConfig]
        WHERE [ConfigKey] = N'cache.bypass_enabled';
    """)