# backend/alembic/versions/0050_template_layout_engine.py
"""0050_template_layout_engine

Rozszerza skw_Templates o LayoutEngine — rozróżnienie:
    'jinja_text'            — wolny tekst z placeholderami (3 istniejące szablony)
    'structured_statement'   — strukturalny wyciąg zbiorczy (pkt 3.3): stała
                               tabela faktur renderowana kodem + edytowalne
                               akapity prawne (Jinja2) z template_body.

Seed z treścią ROBOCZĄ, is_active=0 — wymaga weryfikacji prawnej przed
włączeniem produkcyjnym.

Revision ID: 0050
Revises:     0049
Create Date: 2026-07-07
"""
from __future__ import annotations
import logging
from typing import Final
from alembic import op
import sqlalchemy as sa

revision:      str = "0050"
down_revision: str = "0049"
branch_labels       = None
depends_on          = None

logger = logging.getLogger(f"alembic.migration.{revision}")

_ADD_COLUMN: Final[str] = """
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.skw_Templates') AND name = 'LayoutEngine'
)
BEGIN
    ALTER TABLE dbo.skw_Templates
    ADD LayoutEngine NVARCHAR(30) NOT NULL
        CONSTRAINT DF_skw_Templates_LayoutEngine DEFAULT (N'jinja_text');
END
"""

_ADD_CHECK_CONSTRAINT: Final[str] = """
IF NOT EXISTS (
    SELECT 1 FROM sys.check_constraints WHERE name = 'CK_skw_Templates_LayoutEngine'
)
BEGIN
    ALTER TABLE dbo.skw_Templates
    ADD CONSTRAINT CK_skw_Templates_LayoutEngine
        CHECK (LayoutEngine IN (N'jinja_text', N'structured_statement'));
END
"""

_SEED_TEMPLATE: Final[str] = """
MERGE [dbo].[skw_Templates] AS target
USING (VALUES (
    N'Wezwanie do zapłaty — Wyciąg zbiorczy (druk + e-mail)',
    N'print',
    N'Wezwanie do zapłaty — {{ company_name }}',
    N'[TREŚĆ ROBOCZA — DO WERYFIKACJI PRAWNEJ]

Stosownie do art. 476 kodeksu cywilnego wzywamy Panią/Pana do dobrowolnego
dokonania zapłaty należności przypadającej nam z tytułu poniżej wymienionych
dokumentów.

Prosimy o dokonanie wpłaty w terminie 7 dni od daty otrzymania niniejszego
wezwania na konto: {{ payment_account }}.

Jeżeli powyższa kwota została uregulowana przed otrzymaniem niniejszego
wezwania, prosimy o potraktowanie go za nieważne. W przypadku nieuregulowania
należności w wyznaczonym terminie sprawę skierujemy bez ponownego wezwania
do sądu.',
    N'structured_statement',
    0
)) AS source ([TemplateName], [TemplateType], [Subject], [Body], [LayoutEngine], [IsActive])
ON target.[TemplateName] = source.[TemplateName]
WHEN NOT MATCHED THEN
    INSERT ([TemplateName], [TemplateType], [Subject], [Body], [LayoutEngine], [IsActive], [CreatedAt])
    VALUES (source.[TemplateName], source.[TemplateType], source.[Subject],
            source.[Body], source.[LayoutEngine], source.[IsActive], SYSUTCDATETIME());
"""

def upgrade() -> None:
    logger.info("[%s] UPGRADE — LayoutEngine + seed (is_active=0)", revision)
    bind = op.get_bind()
    bind.execute(sa.text(_ADD_COLUMN))
    bind.execute(sa.text(_ADD_CHECK_CONSTRAINT))
    bind.execute(sa.text(_SEED_TEMPLATE))

def downgrade() -> None:
    logger.warning("[%s] DOWNGRADE — usuwam CHECK + kolumnę LayoutEngine", revision)
    bind = op.get_bind()
    bind.execute(sa.text("""
        IF EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_skw_Templates_LayoutEngine')
            ALTER TABLE dbo.skw_Templates DROP CONSTRAINT CK_skw_Templates_LayoutEngine;
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.skw_Templates') AND name = 'LayoutEngine')
            ALTER TABLE dbo.skw_Templates DROP COLUMN LayoutEngine;
    """))