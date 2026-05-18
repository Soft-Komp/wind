"""
Model ORM — dbo.skw_approval_path_change_log

Append-only historia zmian definicji sciezki.
old_value / new_value przechowywane jako JSON.
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, DateTime, ForeignKey, Identity, Integer, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.approval_path import ApprovalPath
    from app.db.models.user import User

SCHEMA = "dbo"


class ApprovalPathChangeLog(Base):
    """
    Historia zmian sciezki akceptacyjnej (append-only).

    Tabela: dbo.skw_approval_path_change_log
    """

    __tablename__ = "skw_approval_path_change_log"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Append-only historia zmian definicji sciezek",
    }

    id_change: Mapped[int] = mapped_column(
        "id_change",
        BigInteger,
        Identity(start=1, increment=1),
        primary_key=True,
    )
    id_path: Mapped[int] = mapped_column(
        "id_path",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_approval_paths.id_path", ondelete="NO ACTION"),
        nullable=False,
        index=True,
    )
    changed_by: Mapped[int | None] = mapped_column(
        "changed_by",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_Users.ID_USER", ondelete="SET NULL"),
        nullable=True,
    )
    changed_at: Mapped[datetime] = mapped_column(
        "changed_at",
        DateTime,
        nullable=False,
        server_default=text("SYSUTCDATETIME()"),
        default=_utcnow,
    )
    change_type: Mapped[str] = mapped_column(
        "change_type",
        String(50),
        nullable=False,
        comment="Typ zmiany: step_added / step_removed / step_reordered / meta_updated",
    )
    old_value: Mapped[str | None] = mapped_column(
        "old_value", Text, nullable=True, comment="Stan przed zmiana — JSON"
    )
    new_value: Mapped[str | None] = mapped_column(
        "new_value", Text, nullable=True, comment="Stan po zmianie — JSON"
    )

    # ── Relacje ───────────────────────────────────────────────────────────────
    path: Mapped[ApprovalPath] = relationship(
        "ApprovalPath", back_populates="change_logs", lazy="noload"
    )
    editor: Mapped[User | None] = relationship(
        "User", foreign_keys=[changed_by], lazy="noload"
    )

    def __repr__(self) -> str:
        return (
            f"<ApprovalPathChangeLog path={self.id_path} "
            f"type={self.change_type!r} at={self.changed_at}>"
        )