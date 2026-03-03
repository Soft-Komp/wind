"""Fix OtpCodes.Code column length: NVARCHAR(10) → NVARCHAR(64)

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-02-26

Przyczyna: kolumna Code przechowuje hash SHA-256 (64 znaki hex),
           a została zdefiniowana jako NVARCHAR(10) — truncation error.
"""
from __future__ import annotations

import logging

from alembic import op

logger = logging.getLogger("alembic.migration.003")

revision = "c3d4e5f6a7b8"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None

SCHEMA = "dbo_ext"


def upgrade() -> None:
    logger.info("=== MIGRACJA 003 — UPGRADE START ===")

    op.execute(
        f"""
        ALTER TABLE [{SCHEMA}].[OtpCodes]
        ALTER COLUMN [Code] NVARCHAR(64) NOT NULL;
        """
    )
    logger.info("OtpCodes.Code: NVARCHAR(10) → NVARCHAR(64) — OK")

    logger.info("=== MIGRACJA 003 — UPGRADE ZAKOŃCZONY ===")


def downgrade() -> None:
    logger.info("=== MIGRACJA 003 — DOWNGRADE START ===")

    # Uwaga: downgrade może się nie udać jeśli w tabeli są już dane dłuższe niż 10 znaków
    op.execute(
        f"""
        ALTER TABLE [{SCHEMA}].[OtpCodes]
        ALTER COLUMN [Code] NVARCHAR(10) NOT NULL;
        """
    )
    logger.info("OtpCodes.Code: NVARCHAR(64) → NVARCHAR(10) — OK (rollback)")

    logger.info("=== MIGRACJA 003 — DOWNGRADE ZAKOŃCZONY ===")