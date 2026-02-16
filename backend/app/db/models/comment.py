"""
Model tabeli dbo_ext.Comments.
Komentarze pracowników do dłużników (kontrahentów z WAPRO).
ID_KONTRAHENTA = ref do WAPRO.KONTRAHENT — bez FK constraint.
"""

from sqlalchemy import ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import AuditMixin, Base


class Comment(AuditMixin, Base):
    __tablename__ = "Comments"
    __table_args__ = {
        "schema": "dbo_ext",
        "comment": (
            "Komentarze do kontrahentów (dłużników). "
            "ID_KONTRAHENTA = ref do WAPRO (bez FK constraint)."
        ),
    }

    id_comment: Mapped[int] = mapped_column(
        "ID_COMMENT", Integer, primary_key=True, autoincrement=True,
    )
    id_kontrahenta: Mapped[int] = mapped_column(
        "ID_KONTRAHENTA", Integer, nullable=False,
        comment="Ref do WAPRO.KONTRAHENT — brak FK (WAPRO read-only)",
    )
    id_user: Mapped[int | None] = mapped_column(
        "ID_USER",
        Integer,
        ForeignKey("dbo_ext.Users.ID_USER", ondelete="SET NULL"),
        nullable=True,
        comment="Autor komentarza. NULL jeśli user usunięty.",
    )
    content: Mapped[str] = mapped_column(
        "Content", Text, nullable=False,
        comment="Treść komentarza. Np. 'Obiecał zapłacić do piątku.'",
    )

    # Relacje
    user: Mapped["User | None"] = relationship(  # type: ignore[name-defined]
        "User", back_populates="comments"
    )

    def __repr__(self) -> str:
        return (
            f"<Comment id={self.id_comment} "
            f"kontrahent={self.id_kontrahenta} "
            f"user={self.id_user}>"
        )