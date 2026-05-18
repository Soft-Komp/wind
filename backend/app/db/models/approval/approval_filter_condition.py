
"""
Model ORM — dbo.skw_approval_filter_conditions

Warunki filtrow standardowych. Operator whitelist: eq/neq/contains/gt/lt/gte/lte.
CASCADE DELETE z filtru.
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Identity, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.approval_filter import ApprovalFilter

SCHEMA = "dbo"

# Whitelist operatorow — MUSI byc zsynchronizowana z CK_skw_afc_operator
# w bazie danych i z logika filter_engine.py.
VALID_OPERATORS: frozenset[str] = frozenset(
    {"eq", "neq", "contains", "gt", "lt", "gte", "lte"}
)


class ApprovalFilterCondition(Base):
    """
    Warunek filtru standardowego.

    Tabela: dbo.skw_approval_filter_conditions
    """

    __tablename__ = "skw_approval_filter_conditions"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Warunki filtrow automatycznego przydzialu sciezek",
    }

    id_condition: Mapped[int] = mapped_column(
        "id_condition", Integer, Identity(start=1, increment=1), primary_key=True,
    )
    id_filter: Mapped[int] = mapped_column(
        "id_filter",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_approval_filters.id_filter",
            ondelete="CASCADE",
        ),
        nullable=False,
        index=True,
    )
    field_name: Mapped[str] = mapped_column(
        "field_name", String(100), nullable=False,
        comment="Nazwa pola z UnifiedDocument (np. document_amount, supplier_name)",
    )
    operator: Mapped[str] = mapped_column(
        "operator", String(10), nullable=False,
        comment="Operator: eq|neq|contains|gt|lt|gte|lte",
    )
    field_value: Mapped[str] = mapped_column(
        "field_value", String(500), nullable=False,
        comment="Wartosc porownania (zawsze string — rzutowanie w filter_engine)",
    )
    created_at: Mapped[datetime] = mapped_column(
        "created_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), default=_utcnow,
    )

    # ── Relacje ───────────────────────────────────────────────────────────────
    filter: Mapped[ApprovalFilter] = relationship(
        "ApprovalFilter", back_populates="conditions", lazy="noload",
    )

    def __repr__(self) -> str:
        return (
            f"<ApprovalFilterCondition "
            f"filter={self.id_filter} "
            f"{self.field_name} {self.operator} {self.field_value!r}>"
        )