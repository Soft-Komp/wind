"""
Model ORM — dbo.skw_approval_groups

Grupy akceptacyjne. Consensus type AND/OR decyduje ile głosów
wystarczy do zaliczenia etapu.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Identity, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.approval_group_member import ApprovalGroupMember
    from app.db.models.approval.approval_path_step import ApprovalPathStep
    from app.db.models.approval.approval_delegation import ApprovalDelegation
    from app.db.models.approval.document_approval_snapshot_step import (
        DocumentApprovalSnapshotStep,
    )

SCHEMA = "dbo"


class ApprovalGroup(Base):
    """
    Grupa akceptacyjna.

    Tabela: dbo.skw_approval_groups
    consensus_type:
        AND — wszyscy aktywni czlonkowie musza zaakceptowac
        OR  — wystarczy jeden glos (domyslnie)
    """

    __tablename__ = "skw_approval_groups"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Grupy akceptacyjne modulu obiegu dokumentow",
    }

    id_group: Mapped[int] = mapped_column(
        "id_group",
        Integer,
        Identity(start=1, increment=1),
        primary_key=True,
        comment="Klucz glowny IDENTITY(1,1)",
    )
    group_name: Mapped[str] = mapped_column(
        "group_name",
        String(100),
        nullable=False,
        unique=True,
        comment="Unikalna nazwa grupy",
    )
    consensus_type: Mapped[str] = mapped_column(
        "consensus_type",
        String(3),
        nullable=False,
        server_default=text("'OR'"),
        default="OR",
        comment="AND = wszyscy, OR = jeden wystarczy",
    )
    description: Mapped[str | None] = mapped_column(
        "description",
        String(500),
        nullable=True,
    )
    is_active: Mapped[bool] = mapped_column(
        "is_active",
        Boolean,
        nullable=False,
        server_default=text("1"),
        default=True,
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
    members: Mapped[list[ApprovalGroupMember]] = relationship(
        "ApprovalGroupMember",
        back_populates="group",
        lazy="selectin",
    )
    path_steps: Mapped[list[ApprovalPathStep]] = relationship(
        "ApprovalPathStep",
        back_populates="group",
        lazy="noload",
    )
    delegations: Mapped[list[ApprovalDelegation]] = relationship(
        "ApprovalDelegation",
        back_populates="group",
        lazy="noload",
    )
    snapshot_steps: Mapped[list[DocumentApprovalSnapshotStep]] = relationship(
        "DocumentApprovalSnapshotStep",
        back_populates="group",
        lazy="noload",
    )

    def __repr__(self) -> str:
        return (
            f"<ApprovalGroup id={self.id_group} "
            f"name={self.group_name!r} "
            f"consensus={self.consensus_type}>"
        )