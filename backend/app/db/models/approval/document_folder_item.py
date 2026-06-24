# backend/app/db/models/approval/document_folder_item.py
"""
Model ORM — dbo.skw_document_folder_items

Relacja wiele-do-wielu: DocumentApprovalInstance <-> DocumentFolder.

PK kompozytowy (id_folder, id_instance) — dokument moze byc w teczce max raz.
added_by NULL = import zbiorczy (system), NOT NULL = konkretny uzytkownik.
CASCADE DELETE z obu stron (teczka lub instancja uswa -> wpis znika).

UWAGA: from __future__ import annotations — NIGDY w tym pliku.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Integer, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.document_folder import DocumentFolder
    from app.db.models.approval.document_approval_instance import DocumentApprovalInstance
    from app.db.models.user import User

SCHEMA = "dbo"


class DocumentFolderItem(Base):
    """
    Przypisanie dokumentu do teczki.

    Tabela: dbo.skw_document_folder_items
    PK: (id_folder, id_instance)
    """

    __tablename__ = "skw_document_folder_items"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Relacja wiele-do-wielu: dokument <-> teczka",
    }

    id_folder: Mapped[int] = mapped_column(
        "id_folder",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_document_folders.id_folder",
            ondelete="CASCADE",
        ),
        primary_key=True,
        comment="FK do teczki — CASCADE DELETE",
    )
    id_instance: Mapped[int] = mapped_column(
        "id_instance",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_document_approval_instances.id_instance",
            ondelete="CASCADE",
        ),
        primary_key=True,
        index=True,
        comment="FK do instancji obiegu — CASCADE DELETE",
    )
    added_by: Mapped[int | None] = mapped_column(
        "added_by",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_Users.ID_USER",
            ondelete="SET NULL",
        ),
        nullable=True,
        comment="Kto dodal dokument do teczki (NULL = import zbiorczy/system)",
    )
    added_at: Mapped[datetime] = mapped_column(
        "added_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), default=_utcnow,
    )

    # ── Relacje ───────────────────────────────────────────────────────────────

    folder: Mapped["DocumentFolder"] = relationship(
        "DocumentFolder", back_populates="items", lazy="noload",
    )
    instance: Mapped["DocumentApprovalInstance"] = relationship(
        "DocumentApprovalInstance",
        foreign_keys=[id_instance],
        lazy="noload",
    )
    user: Mapped["User | None"] = relationship(
        "User",
        foreign_keys=[added_by],
        lazy="noload",
    )

    def __repr__(self) -> str:
        return (
            f"<DocumentFolderItem folder={self.id_folder} "
            f"instance={self.id_instance} added_by={self.added_by}>"
        )