"""
Model ORM — dbo.skw_approval_filters

Reguly automatycznego przydzialu sciezek do dokumentow.
filter_type:
    standard   — warunki w tabeli approval_filter_conditions
    universal  — wywolanie funkcji SQL (whitelist: ^[a-zA-Z0-9_]+$)

Wyzszy priority = sprawdzany pierwszy.
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, ForeignKey, Identity, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.approval_path import ApprovalPath
    from app.db.models.approval.document_source import DocumentSource
    from app.db.models.approval.approval_filter_condition import ApprovalFilterCondition

SCHEMA = "dbo"


class ApprovalFilter(Base):
    """
    Regula automatycznego przydzialu sciezki.

    Tabela: dbo.skw_approval_filters
    """

    __tablename__ = "skw_approval_filters"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Reguly automatycznego przydzialu sciezek do dokumentow",
    }

    id_filter: Mapped[int] = mapped_column(
        "id_filter", Integer, Identity(start=1, increment=1), primary_key=True,
    )
    filter_name: Mapped[str] = mapped_column(
        "filter_name", String(200), nullable=False,
    )
    filter_type: Mapped[str] = mapped_column(
        "filter_type", String(20), nullable=False,
        comment="standard | universal",
    )
    id_path: Mapped[int] = mapped_column(
        "id_path",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_approval_paths.id_path", ondelete="NO ACTION"),
        nullable=False,
    )
    id_source: Mapped[int | None] = mapped_column(
        "id_source",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_document_sources.id_source", ondelete="SET NULL"),
        nullable=True,
        comment="NULL = filtr dziala dla wszystkich zrodel",
    )
    priority: Mapped[int] = mapped_column(
        "priority", Integer, nullable=False,
        server_default=text("100"), default=100,
        comment="Wyzszy = sprawdzany pierwszy",
    )
    is_active: Mapped[bool] = mapped_column(
        "is_active", Boolean, nullable=False,
        server_default=text("1"), default=True,
    )
    universal_function: Mapped[str | None] = mapped_column(
        "universal_function", String(200), nullable=True,
        comment="Nazwa funkcji SQL (tylko dla filter_type=universal). "
                "Whitelist: ^[a-zA-Z0-9_]+$ — walidacja w filter_engine.py",
    )
    created_at: Mapped[datetime] = mapped_column(
        "created_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), default=_utcnow,
    )
    updated_at: Mapped[datetime] = mapped_column(
        "updated_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), onupdate=_utcnow, default=_utcnow,
    )

    # ── Relacje ───────────────────────────────────────────────────────────────
    path: Mapped[ApprovalPath] = relationship(
        "ApprovalPath", back_populates="filters", lazy="noload",
    )
    source: Mapped[DocumentSource | None] = relationship(
        "DocumentSource", back_populates="filters", lazy="noload",
    )
    conditions: Mapped[list[ApprovalFilterCondition]] = relationship(
        "ApprovalFilterCondition",
        back_populates="filter",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<ApprovalFilter id={self.id_filter} "
            f"name={self.filter_name!r} type={self.filter_type!r}>"
        )


# =============================================================================
# backend/app/db/models/approval/approval_filter_condition.py
# =============================================================================