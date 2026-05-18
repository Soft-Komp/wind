"""
Model ORM — dbo.skw_approval_delegations

Delegowanie uprawnien akceptacyjnych miedzy uzytkownikami.
valid_from/valid_to: okno czasowe delegacji.
id_group = NULL: delegacja globalna (wszystkie grupy).

CHECK: valid_to > valid_from, id_user_from != id_user_to
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, ForeignKey, Identity, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.approval_group import ApprovalGroup
    from app.db.models.user import User

SCHEMA = "dbo"


class ApprovalDelegation(Base):
    """
    Delegacja uprawnien akceptacyjnych.

    Tabela: dbo.skw_approval_delegations
    """

    __tablename__ = "skw_approval_delegations"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Delegowania uprawnien akceptacyjnych miedzy uzytkownikami",
    }

    id_delegation: Mapped[int] = mapped_column(
        "id_delegation", Integer, Identity(start=1, increment=1), primary_key=True,
    )
    id_user_from: Mapped[int] = mapped_column(
        "id_user_from",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_Users.ID_USER", ondelete="NO ACTION"),
        nullable=False,
        index=True,
    )
    id_user_to: Mapped[int] = mapped_column(
        "id_user_to",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_Users.ID_USER", ondelete="NO ACTION"),
        nullable=False,
    )
    id_group: Mapped[int | None] = mapped_column(
        "id_group",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_approval_groups.id_group", ondelete="SET NULL"),
        nullable=True,
        comment="NULL = delegacja globalna (wszystkie grupy)",
    )
    valid_from: Mapped[datetime] = mapped_column(
        "valid_from", DateTime, nullable=False,
    )
    valid_to: Mapped[datetime] = mapped_column(
        "valid_to", DateTime, nullable=False,
    )
    reason: Mapped[str | None] = mapped_column(
        "reason", String(500), nullable=True,
    )
    is_active: Mapped[bool] = mapped_column(
        "is_active", Boolean, nullable=False,
        server_default=text("1"), default=True,
    )
    created_by: Mapped[int | None] = mapped_column(
        "created_by",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_Users.ID_USER", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        "created_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), default=_utcnow,
    )

    # ── Relacje ───────────────────────────────────────────────────────────────
    user_from: Mapped[User] = relationship(
        "User", foreign_keys=[id_user_from], lazy="noload",
    )
    user_to: Mapped[User] = relationship(
        "User", foreign_keys=[id_user_to], lazy="noload",
    )
    group: Mapped[ApprovalGroup | None] = relationship(
        "ApprovalGroup", back_populates="delegations", lazy="noload",
    )

    def __repr__(self) -> str:
        return (
            f"<ApprovalDelegation "
            f"from={self.id_user_from} to={self.id_user_to} "
            f"group={self.id_group}>"
        )


# =============================================================================
# backend/app/db/models/approval/approval_comment.py
# =============================================================================