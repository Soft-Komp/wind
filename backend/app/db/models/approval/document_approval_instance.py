"""
Model ORM — dbo.skw_document_approval_instances

Glowna tabela obiegu dokumentow. Jeden rekord = jeden aktywny
lub zakonczony obieg dokumentu.

KRYTYCZNE:
  - Filtrowany UNIQUE INDEX w DB: (id_document, id_source)
    WHERE status <> 'approved' AND status <> 'cancelled'
  - Backend musi uzywac TEGO SAMEGO filtru przy sprawdzaniu
    istnienia aktywnego obiegu — inaczej fałszywy 409 lub duplikat
  - Model ORM NIE definiuje UniqueConstraint (Alembic nie probuje
    tworzyc indeksu ponownie — jest juz w DB z migracji 0028)
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger, Boolean, DateTime, ForeignKey,
    Identity, Integer, Numeric, String, Text, text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.document_source import DocumentSource
    from app.db.models.approval.document_category import DocumentCategory
    from app.db.models.approval.approval_path import ApprovalPath
    from app.db.models.approval.document_approval_snapshot_step import (
        DocumentApprovalSnapshotStep,
    )
    from app.db.models.approval.approval_comment import ApprovalComment
    from app.db.models.approval.approval_attachment import ApprovalAttachment
    from app.db.models.approval.user_notification import UserNotification
    from app.db.models.user import User

SCHEMA = "dbo"

# Statusy aktywnego obiegu — MUSI byc zsynchronizowane z filtrem
# filtrowanego unique indexu w bazie (migracja 0028 krok 09).
# Zmiana tutaj wymaga ROWNOCZESNEJ zmiany logiki indeksu w nowej migracji.
ACTIVE_STATUSES: frozenset[str] = frozenset({
    "pending_dispatch",
    "in_progress",
    "rejected",
})
TERMINAL_STATUSES: frozenset[str] = frozenset({"approved", "cancelled"})


class DocumentApprovalInstance(Base):
    """
    Instancja obiegu dokumentu.

    Tabela: dbo.skw_document_approval_instances
    status moze byc:
        pending_dispatch — oczekuje na dyspozytora
        in_progress      — w toku akceptacji
        approved         — zaakceptowany (terminal)
        cancelled        — anulowany (terminal)
        rejected         — odrzucony (terminal)
    """

    __tablename__ = "skw_document_approval_instances"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Instancje obiegów dokumentów — jedna per aktywny obieg",
    }

    id_instance: Mapped[int] = mapped_column(
        "id_instance", Integer, Identity(start=1, increment=1), primary_key=True,
    )
    id_document: Mapped[str] = mapped_column(
        "id_document", String(100), nullable=False,
        comment="Zewnetrzny ID dokumentu (np. ID_BUF_DOKUMENT jako str)",
    )
    id_source: Mapped[int] = mapped_column(
        "id_source",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_document_sources.id_source", ondelete="NO ACTION"),
        nullable=False,
        index=True,
    )
    id_path: Mapped[int | None] = mapped_column(
        "id_path",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_approval_paths.id_path", ondelete="SET NULL"),
        nullable=True,
    )
    id_category: Mapped[int | None] = mapped_column(
        "id_category",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_document_categories.id_category", ondelete="SET NULL"),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        "status", String(30), nullable=False,
        server_default=text("'pending_dispatch'"),
        default="pending_dispatch",
        index=True,
        comment="pending_dispatch|in_progress|approved|cancelled|rejected",
    )
    current_step: Mapped[int] = mapped_column(
        "current_step", Integer, nullable=False,
        server_default=text("0"), default=0,
        comment="Aktualny krok snapshotu (0 = nierozpoczety)",
    )
    is_urgent: Mapped[bool] = mapped_column(
        "is_urgent", Boolean, nullable=False,
        server_default=text("0"), default=False, index=True,
    )
    dispatched_by: Mapped[int | None] = mapped_column(
        "dispatched_by",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_Users.ID_USER", ondelete="SET NULL"),
        nullable=True,
    )
    dispatched_at: Mapped[datetime | None] = mapped_column(
        "dispatched_at", DateTime, nullable=True,
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        "completed_at", DateTime, nullable=True,
    )
    deadline_at: Mapped[datetime | None] = mapped_column(
        "deadline_at", DateTime, nullable=True,
        comment="Globalny deadline instancji (obliczany przy dispatch)",
    )
    document_title: Mapped[str | None] = mapped_column(
        "document_title", String(500), nullable=True,
    )
    document_amount: Mapped[Decimal | None] = mapped_column(
        "document_amount", Numeric(18, 2), nullable=True,
    )
    extra_data: Mapped[str | None] = mapped_column(
        "extra_data", Text, nullable=True,
        comment="Dane zrodlowe dokumentu jako JSON (numer_ksef, typ_dok itp.)",
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
    source: Mapped[DocumentSource] = relationship(
        "DocumentSource", back_populates="instances", lazy="selectin",
    )
    path: Mapped[ApprovalPath | None] = relationship(
        "ApprovalPath", lazy="noload",
    )
    category: Mapped[DocumentCategory | None] = relationship(
        "DocumentCategory", lazy="noload",
    )
    dispatcher: Mapped[User | None] = relationship(
        "User", foreign_keys=[dispatched_by], lazy="noload",
    )
    snapshot_steps: Mapped[list[DocumentApprovalSnapshotStep]] = relationship(
        "DocumentApprovalSnapshotStep",
        back_populates="instance",
        cascade="all, delete-orphan",
        order_by="DocumentApprovalSnapshotStep.step_order",
        lazy="selectin",
    )
    comments: Mapped[list[ApprovalComment]] = relationship(
        "ApprovalComment",
        back_populates="instance",
        cascade="all, delete-orphan",
        lazy="noload",
    )
    attachments: Mapped[list[ApprovalAttachment]] = relationship(
        "ApprovalAttachment",
        back_populates="instance",
        cascade="all, delete-orphan",
        lazy="noload",
    )
    notifications: Mapped[list[UserNotification]] = relationship(
        "UserNotification",
        back_populates="instance",
        lazy="noload",
    )

    @property
    def is_active(self) -> bool:
        """True jesli obieg jest w toku (nie zakonczony)."""
        return self.status in ACTIVE_STATUSES

    @property
    def is_terminal(self) -> bool:
        """True jesli obieg jest zakonczony (approved/cancelled)."""
        return self.status in TERMINAL_STATUSES

    def __repr__(self) -> str:
        return (
            f"<DocumentApprovalInstance id={self.id_instance} "
            f"doc={self.id_document!r} status={self.status!r}>"
        )