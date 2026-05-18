"""
Model ORM — dbo.skw_approval_attachments

Metadane zalacznikow. Plik fizycznie na dysku poza web root.
file_name: oryginalna nazwa (do wyswietlenia).
file_path: sciezka serwera po sanityzacji.
is_deleted: soft-delete (plik na dysku usuwany osobno przez serwis).
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, Identity, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.document_approval_instance import DocumentApprovalInstance
    from app.db.models.user import User

SCHEMA = "dbo"


class ApprovalAttachment(Base):
    """
    Metadane zalacznika do obiegu.

    Tabela: dbo.skw_approval_attachments
    Plik na dysku: /app/approval_attachments/<id_instance>/<file_path>
    """

    __tablename__ = "skw_approval_attachments"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Metadane zalacznikow do obiegow dokumentow",
    }

    id_attachment: Mapped[int] = mapped_column(
        "id_attachment", Integer, Identity(start=1, increment=1), primary_key=True,
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
    file_name: Mapped[str] = mapped_column(
        "file_name", String(255), nullable=False,
        comment="Oryginalna nazwa pliku (do wyswietlenia w UI)",
    )
    file_path: Mapped[str] = mapped_column(
        "file_path", String(1000), nullable=False,
        comment="Sciezka po sanityzacji — poza web root",
    )
    file_size: Mapped[int] = mapped_column(
        "file_size", BigInteger, nullable=False,
        comment="Rozmiar w bajtach (> 0)",
    )
    mime_type: Mapped[str] = mapped_column(
        "mime_type", String(200), nullable=False,
        comment="Wykryty MIME (python-magic) — nie ufamy naglowkowi z przegladarki",
    )
    is_deleted: Mapped[bool] = mapped_column(
        "is_deleted", Boolean, nullable=False,
        server_default=text("0"), default=False,
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        "deleted_at", DateTime, nullable=True,
    )
    deleted_by: Mapped[int | None] = mapped_column(
        "deleted_by",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_Users.ID_USER", ondelete="NO ACTION"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        "created_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), default=_utcnow,
    )

    # ── Relacje ───────────────────────────────────────────────────────────────
    instance: Mapped[DocumentApprovalInstance] = relationship(
        "DocumentApprovalInstance", back_populates="attachments", lazy="noload",
    )
    uploader: Mapped[User | None] = relationship(
        "User", foreign_keys=[id_user], lazy="noload",
    )

    def __repr__(self) -> str:
        return (
            f"<ApprovalAttachment id={self.id_attachment} "
            f"name={self.file_name!r} size={self.file_size}>"
        )