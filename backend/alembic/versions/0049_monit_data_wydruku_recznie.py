# backend/alembic/versions/0049_monit_data_wydruku_recznie.py
"""0049_monit_data_wydruku_recznie

Dodaje kolumnę DataWydrukuRecznie do skw_MonitHistory.
Operator może ustawić dowolną datę druku — BEZ limitu tolerancji
(decyzja: log audytowy w dbo.skw_AuditLog wystarcza jako zabezpieczenie).
Walidacja zakresu 1990-01-01..2100-12-31 to wyłącznie ochrona przed
błędem/overflow, nie reguła biznesowa.

Revision ID: 0049
Revises:     0048
Create Date: 2026-07-06
"""
from __future__ import annotations
import logging
from typing import Final
from alembic import op
import sqlalchemy as sa

revision:      str = "0049"
down_revision: str = "0048"
branch_labels       = None
depends_on          = None

logger = logging.getLogger(f"alembic.migration.{revision}")

_ADD_COLUMN: Final[str] = """
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.skw_MonitHistory')
      AND name = 'DataWydrukuRecznie'
)
BEGIN
    ALTER TABLE dbo.skw_MonitHistory
    ADD DataWydrukuRecznie DATE NULL;
    -- NULL = użyto daty systemowej w momencie generowania (zachowanie dotychczasowe)
    -- NOT NULL = operator ręcznie ustawił datę druku
END
"""

_DROP_COLUMN: Final[str] = """
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.skw_MonitHistory')
      AND name = 'DataWydrukuRecznie'
)
BEGIN
    ALTER TABLE dbo.skw_MonitHistory DROP COLUMN DataWydrukuRecznie;
END
"""

def upgrade() -> None:
    logger.info("[%s] UPGRADE — dodaję kolumnę DataWydrukuRecznie", revision)
    op.get_bind().execute(sa.text(_ADD_COLUMN))
    logger.info("[%s] UPGRADE OK", revision)

def downgrade() -> None:
    logger.warning("[%s] DOWNGRADE — usuwam kolumnę DataWydrukuRecznie", revision)
    op.get_bind().execute(sa.text(_DROP_COLUMN))
    logger.warning("[%s] DOWNGRADE OK", revision)