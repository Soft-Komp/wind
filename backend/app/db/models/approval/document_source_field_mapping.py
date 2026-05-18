"""
Model ORM — dbo.skw_document_source_field_mappings

Mapowanie pol miedzy zrodlem a UnifiedDocument.
common_field: wspolna nazwa w systemie (np. document_amount).
source_field:  nazwa pola w konkretnym zrodle (np. WARTOSC_BRUTTO).
transform_expression: opcjonalne wyrazenie SQL (np. konwersja daty Clarion).
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Identity, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.document_source import DocumentSource

SCHEMA = "dbo"


class DocumentSourceFieldMapping(Base):
    """
    Mapowanie pol zrodla do UnifiedDocument.

    Tabela: dbo.skw_document_source_field_mappings
    UNIQUE: (id_source, common_field)
    CASCADE DELETE z DocumentSource.
    """

    __tablename__ = "skw_document_source_field_mappings"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Mapowanie pol zrodel do wspolnego interfejsu UnifiedDocument",
    }

    id_mapping: Mapped[int] = mapped_column(
        "id_mapping", Integer, Identity(start=1, increment=1), primary_key=True,
    )
    id_source: Mapped[int] = mapped_column(
        "id_source",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_document_sources.id_source",
            ondelete="CASCADE",
        ),
        nullable=False,
        index=True,
    )
    common_field: Mapped[str] = mapped_column(
        "common_field", String(100), nullable=False,
        comment="Nazwa pola w UnifiedDocument",
    )
    source_field: Mapped[str] = mapped_column(
        "source_field", String(200), nullable=False,
        comment="Nazwa pola w zrodle (kolumna SQL lub klucz JSON)",
    )
    field_type: Mapped[str] = mapped_column(
        "field_type", String(20), nullable=False,
        comment="string | decimal | date | int",
    )
    transform_expression: Mapped[str | None] = mapped_column(
        "transform_expression", String(500), nullable=True,
        comment="Opcjonalne SQL (np. DATEADD(DAY,val,18991230) dla dat Clarion)",
    )
    created_at: Mapped[datetime] = mapped_column(
        "created_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), default=_utcnow,
    )

    # ── Relacje ───────────────────────────────────────────────────────────────
    source: Mapped[DocumentSource] = relationship(
        "DocumentSource", back_populates="field_mappings", lazy="noload",
    )

    def __repr__(self) -> str:
        return (
            f"<FieldMapping source={self.id_source} "
            f"{self.common_field} -> {self.source_field}>"
        )