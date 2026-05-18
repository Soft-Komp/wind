"""
Model ORM — dbo.skw_user_notifications

Persystentne powiadomienia biznesowe (nie SSE — te sa ulotne).
Filtrowany index DB: (id_user, is_read) WHERE is_read=0
— szybki COUNT nieprzeczytanych.

Typy powiadomien:
    approval_pending          — dokument czeka na akcje usera
    approval_accepted         — dokument zaakceptowany
    approval_rejected         — dokument odrzucony
    approval_deadline_warning — deadline za X godzin
    approval_deadline_expired — przekroczony deadline
    approval_escalated        — eskalacja do nadrzednego
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, Identity, Integer, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.document_approval_instance import DocumentApprovalInstance
    from app.db.models.user import User

SCHEMA = "dbo"


class UserNotification(Base):
    """
    Persystentne powiadomienie dla uzytkownika.

    Tabela: dbo.skw_user_notifications
    Filtrowany index w DB: (id_user, is_read) WHERE is_read=0
    — nie definiujemy go w ORM (Alembic go nie tworzy ponownie).
    """

    __tablename__ = "skw_user_notifications"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Persystentne powiadomienia biznesowe dla uzytkownikow",
    }

    id_notification: Mapped[int] = mapped_column(
        "id_notification",
        BigInteger,
        Identity(start=1, increment=1),
        primary_key=True,
    )
    id_user: Mapped[int] = mapped_column(
        "id_user",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_Users.ID_USER", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    notification_type: Mapped[str] = mapped_column(
        "notification_type", String(50), nullable=False,
        comment=(
            "approval_pending | approval_accepted | approval_rejected | "
            "approval_deadline_warning | approval_deadline_expired | "
            "approval_escalated"
        ),
    )
    id_instance: Mapped[int | None] = mapped_column(
        "id_instance",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_document_approval_instances.id_instance",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    title: Mapped[str] = mapped_column(
        "title", String(200), nullable=False,
    )
    message: Mapped[str] = mapped_column(
        "message", Text, nullable=False,
    )
    is_read: Mapped[bool] = mapped_column(
        "is_read", Boolean, nullable=False,
        server_default=text("0"), default=False,
    )
    read_at: Mapped[datetime | None] = mapped_column(
        "read_at", DateTime, nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        "created_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), default=_utcnow,
    )

    # ── Relacje ───────────────────────────────────────────────────────────────
    user: Mapped[User] = relationship(
        "User", foreign_keys=[id_user], lazy="noload",
    )
    instance: Mapped[DocumentApprovalInstance | None] = relationship(
        "DocumentApprovalInstance", back_populates="notifications", lazy="noload",
    )

    def __repr__(self) -> str:
        return (
            f"<UserNotification id={self.id_notification} "
            f"user={self.id_user} type={self.notification_type!r} "
            f"read={self.is_read}>"
        )