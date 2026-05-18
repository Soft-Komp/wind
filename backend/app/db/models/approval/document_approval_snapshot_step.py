"""
Model ORM — dbo.skw_document_approval_snapshot_steps

Robocza kopia krokow sciezki dla konkretnej instancji.
System obiegu dziala WYLACZNIE na tym snapshocie — nie na definicji
sciezki. Zmiany sciezki nie wplywaja na trwajace obiegi.

UNIQUE: (id_instance, step_order) — CASCADE DELETE z instancji.
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Identity, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.document_approval_instance import DocumentApprovalInstance
    from app.db.models.approval.approval_group import ApprovalGroup

SCHEMA = "dbo"


class DocumentApprovalSnapshotStep(Base):
    """
    Krok roboczy instancji obiegu (snapshot sciezki).

    Tabela: dbo.skw_document_approval_snapshot_steps
    status: pending | in_progress | approved | skipped
    votes_required: dla AND = liczba czlonkow grupy, dla OR = 1
    """

    __tablename__ = "skw_document_approval_snapshot_steps"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Robocza kopia krokow obiegu — per instancja",
    }

    id_snapshot: Mapped[int] = mapped_column(
        "id_snapshot", Integer, Identity(start=1, increment=1), primary_key=True,
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
    step_order: Mapped[int] = mapped_column(
        "step_order", Integer, nullable=False,
    )
    id_group: Mapped[int] = mapped_column(
        "id_group",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_approval_groups.id_group", ondelete="NO ACTION"),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        "status", String(20), nullable=False,
        server_default=text("'pending'"), default="pending",
        comment="pending | in_progress | approved | skipped",
    )
    votes_required: Mapped[int] = mapped_column(
        "votes_required", Integer, nullable=False,
        server_default=text("1"), default=1,
        comment="Wymagana liczba glosow. AND=len(members), OR=1",
    )
    votes_cast: Mapped[int] = mapped_column(
        "votes_cast", Integer, nullable=False,
        server_default=text("0"), default=0,
    )
    deadline_at: Mapped[datetime | None] = mapped_column(
        "deadline_at", DateTime, nullable=True,
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        "completed_at", DateTime, nullable=True,
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
        "DocumentApprovalInstance", back_populates="snapshot_steps", lazy="noload",
    )
    group: Mapped[ApprovalGroup] = relationship(
        "ApprovalGroup", back_populates="snapshot_steps", lazy="selectin",
    )

    @property
    def is_complete(self) -> bool:
        return self.votes_cast >= self.votes_required

    def __repr__(self) -> str:
        return (
            f"<SnapshotStep inst={self.id_instance} "
            f"order={self.step_order} status={self.status!r}>"
        )