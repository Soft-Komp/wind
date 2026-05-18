"""
Model ORM — dbo.skw_document_sources

Slownik zrodel dokumentow: fakir, ksef, manual.
Seed w migracji 0028: fakir + ksef.
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, Identity, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.document_approval_instance import DocumentApprovalInstance
    from app.db.models.approval.approval_filter import ApprovalFilter
    from app.db.models.approval.document_source_field_mapping import DocumentSourceFieldMapping

SCHEMA = "dbo"


class DocumentSource(Base):
    """
    Zrodlo dokumentow.

    Tabela: dbo.skw_document_sources
    Seed: fakir (BUF_DOKUMENT Fakir/WAPRO), ksef (KSeF)
    """

    __tablename__ = "skw_document_sources"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Slownik zrodel dokumentow wchodzacych do obiegu",
    }

    id_source: Mapped[int] = mapped_column(
        "id_source", Integer, Identity(start=1, increment=1), primary_key=True,
    )
    source_name: Mapped[str] = mapped_column(
        "source_name", String(50), nullable=False, unique=True,
        comment="Krotka nazwa: fakir / ksef / manual",
    )
    description: Mapped[str | None] = mapped_column(
        "description", String(200), nullable=True,
    )
    is_active: Mapped[bool] = mapped_column(
        "is_active", Boolean, nullable=False,
        server_default=text("1"), default=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        "created_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), default=_utcnow,
    )

    # ── Relacje ───────────────────────────────────────────────────────────────
    instances: Mapped[list[DocumentApprovalInstance]] = relationship(
        "DocumentApprovalInstance", back_populates="source", lazy="noload",
    )
    filters: Mapped[list[ApprovalFilter]] = relationship(
        "ApprovalFilter", back_populates="source", lazy="noload",
    )
    field_mappings: Mapped[list[DocumentSourceFieldMapping]] = relationship(
        "DocumentSourceFieldMapping",
        back_populates="source",
        cascade="all, delete-orphan",
        lazy="noload",
    )

    def __repr__(self) -> str:
        return f"<DocumentSource id={self.id_source} name={self.source_name!r}>"


# =============================================================================
# backend/app/db/models/approval/document_category.py
# =============================================================================