"""
Model ORM — dbo.skw_document_categories
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Identity, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.models.base import Base, _utcnow

SCHEMA = "dbo"


class DocumentCategory(Base):
    """Kategoria dokumentow (slownik)."""

    __tablename__ = "skw_document_categories"
    __table_args__ = {"schema": SCHEMA}

    id_category: Mapped[int] = mapped_column(
        "id_category", Integer, Identity(start=1, increment=1), primary_key=True,
    )
    category_name: Mapped[str] = mapped_column(
        "category_name", String(100), nullable=False, unique=True,
    )
    description: Mapped[str | None] = mapped_column(
        "description", String(500), nullable=True,
    )
    is_active: Mapped[bool] = mapped_column(
        "is_active", Boolean, nullable=False,
        server_default=text("1"), default=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        "created_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), default=_utcnow,
    )
    updated_at: Mapped[datetime] = mapped_column(
        "updated_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), onupdate=_utcnow, default=_utcnow,
    )

    def __repr__(self) -> str:
        return f"<DocumentCategory id={self.id_category} name={self.category_name!r}>"