"""Merge heads: c3d4e5f6a7b8 + r20260223_add_user_cols

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8, r20260223_add_user_cols
Create Date: 2026-02-26
"""
from __future__ import annotations

from alembic import op

revision = "d4e5f6a7b8c9"
down_revision = ("c3d4e5f6a7b8", "r20260223_add_user_cols")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass  # Migracja scalająca — brak zmian DDL


def downgrade() -> None:
    pass  # Migracja scalająca — brak zmian DDL