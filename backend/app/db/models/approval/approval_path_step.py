"""
Model ORM — dbo.skw_approval_path_steps

Kroki definicji sciezki. UNIQUE(id_path, step_order).
CASCADE DELETE: usuniecie sciezki usuwa jej kroki.
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Identity, Integer, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.approval_path import ApprovalPath
    from app.db.models.approval.approval_group import ApprovalGroup

SCHEMA = "dbo"


class ApprovalPathStep(Base):
    """
    Krok definicji sciezki akceptacyjnej.

    Tabela: dbo.skw_approval_path_steps
    UNIQUE: (id_path, step_order)
    deadline_hours: NULL = brak terminu dla tego kroku
    """

    __tablename__ = "skw_approval_path_steps"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Kroki definicji sciezek akceptacyjnych",
    }

    id_step: Mapped[int] = mapped_column(
        "id_step",
        Integer,
        Identity(start=1, increment=1),
        primary_key=True,
    )
    id_path: Mapped[int] = mapped_column(
        "id_path",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_approval_paths.id_path",
            ondelete="CASCADE",
        ),
        nullable=False,
        index=True,
    )
    step_order: Mapped[int] = mapped_column(
        "step_order",
        Integer,
        nullable=False,
        comment="Kolejnosc kroku — unikalny per sciezka",
    )
    id_group: Mapped[int] = mapped_column(
        "id_group",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_approval_groups.id_group",
            ondelete="NO ACTION",
        ),
        nullable=False,
    )
    deadline_hours: Mapped[int | None] = mapped_column(
        "deadline_hours",
        Integer,
        nullable=True,
        comment="Termin dla tego kroku w godzinach. NULL = brak terminu.",
    )
    created_at: Mapped[datetime] = mapped_column(
        "created_at",
        DateTime,
        nullable=False,
        server_default=text("SYSUTCDATETIME()"),
        default=_utcnow,
    )

    # ── Relacje ───────────────────────────────────────────────────────────────
    path: Mapped[ApprovalPath] = relationship(
        "ApprovalPath",
        back_populates="steps",
        lazy="noload",
    )
    group: Mapped[ApprovalGroup] = relationship(
        "ApprovalGroup",
        back_populates="path_steps",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<ApprovalPathStep path={self.id_path} "
            f"order={self.step_order} group={self.id_group}>"
        )


# =============================================================================
# backend/app/db/models/approval/approval_path_change_log.py
# =============================================================================