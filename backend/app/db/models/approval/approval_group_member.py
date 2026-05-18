"""
Model ORM — dbo.skw_approval_group_members

Czlonkowie grup akceptacyjnych. UNIQUE(id_group, id_user).
FK RESTRICT: nie mozna usunac grupy z czlonkami ani usera
bedacego w grupie z aktywnym obiegiem.
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Identity, Integer, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.approval_group import ApprovalGroup
    from app.db.models.user import User

SCHEMA = "dbo"


class ApprovalGroupMember(Base):
    """
    Czlonek grupy akceptacyjnej.

    Tabela: dbo.skw_approval_group_members
    UNIQUE: (id_group, id_user) — jeden user raz w grupie
    """

    __tablename__ = "skw_approval_group_members"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Czlonkowie grup akceptacyjnych",
    }

    id: Mapped[int] = mapped_column(
        "id",
        Integer,
        Identity(start=1, increment=1),
        primary_key=True,
    )
    id_group: Mapped[int] = mapped_column(
        "id_group",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_approval_groups.id_group", ondelete="NO ACTION"),
        nullable=False,
        index=True,
    )
    id_user: Mapped[int] = mapped_column(
        "id_user",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_Users.ID_USER", ondelete="NO ACTION"),
        nullable=False,
    )
    assigned_at: Mapped[datetime] = mapped_column(
        "assigned_at",
        DateTime,
        nullable=False,
        server_default=text("SYSUTCDATETIME()"),
        default=_utcnow,
    )
    assigned_by: Mapped[int | None] = mapped_column(
        "assigned_by",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_Users.ID_USER", ondelete="NO ACTION"),
        nullable=True,
    )

    # ── Relacje ───────────────────────────────────────────────────────────────
    group: Mapped[ApprovalGroup] = relationship(
        "ApprovalGroup",
        back_populates="members",
        lazy="selectin",
    )
    user: Mapped[User] = relationship(
        "User",
        foreign_keys=[id_user],
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<ApprovalGroupMember "
            f"group={self.id_group} user={self.id_user}>"
        )