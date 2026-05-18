"""
Model ORM — dbo.skw_approval_paths

Definicja sciezki akceptacyjnej (szablon obiegu).
Kazdy dokument przy dispatch dostaje SNAPSHOT krokow z tej definicji —
pozniejsze zmiany sciezki nie wplywaja na trwajace obiegi.
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, ForeignKey, Identity, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.approval_path_step import ApprovalPathStep
    from app.db.models.approval.approval_path_change_log import ApprovalPathChangeLog
    from app.db.models.approval.approval_filter import ApprovalFilter
    from app.db.models.user import User

SCHEMA = "dbo"


class ApprovalPath(Base):
    """
    Sciezka akceptacyjna — definicja sekwencji krokow.

    Tabela: dbo.skw_approval_paths
    UNIQUE: path_name
    """

    __tablename__ = "skw_approval_paths"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Definicje sciezek akceptacyjnych",
    }

    id_path: Mapped[int] = mapped_column(
        "id_path",
        Integer,
        Identity(start=1, increment=1),
        primary_key=True,
    )
    path_name: Mapped[str] = mapped_column(
        "path_name",
        String(200),
        nullable=False,
        unique=True,
        comment="Unikalna nazwa sciezki",
    )
    description: Mapped[str | None] = mapped_column(
        "description", String(500), nullable=True,
    )
    is_active: Mapped[bool] = mapped_column(
        "is_active",
        Boolean,
        nullable=False,
        server_default=text("1"),
        default=True,
    )
    created_by: Mapped[int | None] = mapped_column(
        "created_by",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_Users.ID_USER", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        "created_at",
        DateTime,
        nullable=False,
        server_default=text("SYSUTCDATETIME()"),
        default=_utcnow,
    )
    updated_at: Mapped[datetime] = mapped_column(
        "updated_at",
        DateTime,
        nullable=False,
        server_default=text("SYSUTCDATETIME()"),
        onupdate=_utcnow,
        default=_utcnow,
    )

    # ── Relacje ───────────────────────────────────────────────────────────────
    steps: Mapped[list[ApprovalPathStep]] = relationship(
        "ApprovalPathStep",
        back_populates="path",
        cascade="all, delete-orphan",
        order_by="ApprovalPathStep.step_order",
        lazy="selectin",
    )
    change_logs: Mapped[list[ApprovalPathChangeLog]] = relationship(
        "ApprovalPathChangeLog",
        back_populates="path",
        lazy="noload",
    )
    filters: Mapped[list[ApprovalFilter]] = relationship(
        "ApprovalFilter",
        back_populates="path",
        lazy="noload",
    )
    creator: Mapped[User | None] = relationship(
        "User",
        foreign_keys=[created_by],
        lazy="noload",
    )

    def __repr__(self) -> str:
        return f"<ApprovalPath id={self.id_path} name={self.path_name!r}>"