# backend/alembic/versions/0051_monit_kwoty_zbiorcze.py
"""0051_monit_kwoty_zbiorcze

Dodaje dwie kolumny do dbo.skw_MonitHistory, wymagane przez renderer
generate_pdf_structured_statement (Punkt 3.3):

    KwotaWplatySuma  — suma (KwotaBrutto - KwotaPozostala) po wszystkich
                       fakturach monitu, liczona RAZ w monit_service.send_bulk()
                       (WAPRO connection pool dostępny tam, NIE w workerze).
    SaldoSuma        — suma KwotaPozostala po wszystkich fakturach monitu.

Obie kolumny NULL dla monitów sprzed tej migracji i dla layout_engine
'jinja_text' (gdzie te wartości nie są używane w renderze) — brak wstecznego
przeliczania historii, zgodnie ze standardem projektu (nowe pola = NULL dla
danych historycznych, nigdy retroaktywne dociąganie z WAPRO).

CHECK >= 0 — ochrona przed ujemną sumą w razie błędu obliczeniowego
w warstwie aplikacji (redundancja: walidacja w Pythonie ORAZ w bazie).

Revision ID: 0051
Revises:     0050
Create Date: 2026-07-07
"""
from __future__ import annotations

import logging
from typing import Final

from alembic import op
from sqlalchemy import text as sa_text

revision:      str = "0051"
down_revision: str = "0050"
branch_labels       = None
depends_on          = None

logger = logging.getLogger(f"alembic.migration.{revision}")

TABLE: Final[str] = "skw_MonitHistory"

_ADD_COLUMNS: Final[str] = f"""
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.{TABLE}') AND name = 'KwotaWplatySuma'
)
BEGIN
    ALTER TABLE dbo.{TABLE}
    ADD KwotaWplatySuma DECIMAL(18,2) NULL
        CONSTRAINT CK_{TABLE}_KwotaWplatySuma_NonNeg CHECK (KwotaWplatySuma IS NULL OR KwotaWplatySuma >= 0);
END

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.{TABLE}') AND name = 'SaldoSuma'
)
BEGIN
    ALTER TABLE dbo.{TABLE}
    ADD SaldoSuma DECIMAL(18,2) NULL
        CONSTRAINT CK_{TABLE}_SaldoSuma_NonNeg CHECK (SaldoSuma IS NULL OR SaldoSuma >= 0);
END
"""

_DROP_COLUMNS: Final[str] = f"""
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.{TABLE}') AND name = 'KwotaWplatySuma')
BEGIN
    ALTER TABLE dbo.{TABLE} DROP CONSTRAINT CK_{TABLE}_KwotaWplatySuma_NonNeg;
    ALTER TABLE dbo.{TABLE} DROP COLUMN KwotaWplatySuma;
END
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.{TABLE}') AND name = 'SaldoSuma')
BEGIN
    ALTER TABLE dbo.{TABLE} DROP CONSTRAINT CK_{TABLE}_SaldoSuma_NonNeg;
    ALTER TABLE dbo.{TABLE} DROP COLUMN SaldoSuma;
END
"""


def upgrade() -> None:
    logger.info(
        "[%s] UPGRADE START — dodaję KwotaWplatySuma, SaldoSuma do dbo.%s",
        revision, TABLE,
        extra={"event": "migration.upgrade.start", "revision": revision, "table": TABLE},
    )
    bind = op.get_bind()
    bind.execute(sa_text(_ADD_COLUMNS))
    logger.info(
        "[%s] UPGRADE OK",
        revision,
        extra={"event": "migration.upgrade.ok", "revision": revision},
    )


def downgrade() -> None:
    logger.warning(
        "[%s] DOWNGRADE — usuwam KwotaWplatySuma, SaldoSuma z dbo.%s",
        revision, TABLE,
        extra={"event": "migration.downgrade.start", "revision": revision, "table": TABLE},
    )
    bind = op.get_bind()
    bind.execute(sa_text(_DROP_COLUMNS))
    logger.warning(
        "[%s] DOWNGRADE OK",
        revision,
        extra={"event": "migration.downgrade.ok", "revision": revision},
    )