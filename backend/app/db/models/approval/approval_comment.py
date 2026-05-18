"""
Model ORM — dbo.skw_approval_comments

Komentarze wewnetrzne przy obiegu dokumentow.
Self-referencing FK: parent_id → id_comment (watki odpowiedzi).
is_deleted: soft-delete — tresc zamazana, rekord pozostaje dla spojnosci watku.
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, ForeignKey, Identity, Integer, Text, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.document_approval_instance import DocumentApprovalInstance
    from app.db.models.user import User

SCHEMA = "dbo"


class ApprovalComment(Base):
    """
    Komentarz przy obiegu dokumentu.

    Tabela: dbo.skw_approval_comments
    Self-ref: parent_id → id_comment (odpowiedz w watku)
    Usuniecie: is_deleted=True, deleted_at=now (soft delete)
    """

    __tablename__ = "skw_approval_comments"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Komentarze wewnetrzne przy obiegu dokumentow",
    }

    id_comment: Mapped[int] = mapped_column(
        "id_comment", Integer, Identity(start=1, increment=1), primary_key=True,
    )
    id_instance: Mapped[int] = mapped_column(
        "id_instance",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_document_approval_instances.id_instance",
            ondelete="CASCADE",
        ),
        nullable=False,
        index=True,
    )
    id_user: Mapped[int | None] = mapped_column(
        "id_user",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_Users.ID_USER", ondelete="SET NULL"),
        nullable=True,
    )
    parent_id: Mapped[int | None] = mapped_column(
        "parent_id",
        Integer,
        # Self-referencing FK
        ForeignKey(f"{SCHEMA}.skw_approval_comments.id_comment", ondelete="NO ACTION"),
        nullable=True,
        comment="NULL = komentarz glowny. Non-NULL = odpowiedz w watku.",
    )
    content: Mapped[str] = mapped_column(
        "content", Text, nullable=False,
    )
    is_deleted: Mapped[bool] = mapped_column(
        "is_deleted", Boolean, nullable=False,
        server_default=text("0"), default=False,
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        "deleted_at", DateTime, nullable=True,
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
    instance: Mapped[DocumentApprovalInstance] = relationship(
        "DocumentApprovalInstance", back_populates="comments", lazy="noload",
    )
    author: Mapped[User | None] = relationship(
        "User", foreign_keys=[id_user], lazy="selectin",
    )
    # Self-referencing: rodzic i dzieci
    parent: Mapped[ApprovalComment | None] = relationship(
        "ApprovalComment",
        remote_side="ApprovalComment.id_comment",
        foreign_keys=[parent_id],
        lazy="noload",
    )
    replies: Mapped[list[ApprovalComment]] = relationship(
        "ApprovalComment",
        foreign_keys=[parent_id],
        back_populates="parent",
        lazy="noload",
    )

    def __repr__(self) -> str:
        return (
            f"<ApprovalComment id={self.id_comment} "
            f"inst={self.id_instance} deleted={self.is_deleted}>"
        )