"""
Model tabeli dbo_ext.Comments.
Notatki pracowników do kontrahentów (dłużników z WAPRO).

UWAGA na nazewnictwo wg USTALENIA_PROJEKTU v1.5:
  - kolumna treści:   Tresc       (NIE Content)
  - kolumna autora:   UzytkownikID (NIE ID_USER)
  - UzytkownikID:     NOT NULL    (NIE nullable — komentarz MUSI mieć autora)

Przy usunięciu usera: komentarze NIE tracą autora (NOT NULL = nie można usunąć
usera który ma komentarze — RESTRICT). Najpierw trzeba przepisać komentarze.

WAŻNE — dwuetapowe usuwanie:
  Każde DELETE komentarza → token potwierdzający (TTL z delete_token.ttl_seconds)
  Każde DELETE/EDIT → AuditLog z OldValue JSON
"""

from sqlalchemy import ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import AuditMixin, Base


class Comment(AuditMixin, Base):
    __tablename__ = "Comments"
    __table_args__ = {
        "schema": "dbo_ext",
        "comment": (
            "Notatki pracowników do kontrahentów. "
            "ID_KONTRAHENTA = ref do WAPRO (bez FK constraint — WAPRO read-only). "
            "UzytkownikID NOT NULL — komentarz musi mieć autora. "
            "Soft-delete: IsActive = 0."
        ),
    }

    id_comment: Mapped[int] = mapped_column(
        "ID_COMMENT", Integer, primary_key=True, autoincrement=True,
        comment="Klucz główny",
    )
    id_kontrahenta: Mapped[int] = mapped_column(
        "ID_KONTRAHENTA", Integer, nullable=False,
        comment="Ref do WAPRO.KONTRAHENT — brak FK (WAPRO read-only, osobny schemat)",
    )
    tresc: Mapped[str] = mapped_column(
        "Tresc",        # ← polska nazwa kolumny zgodna z dokumentacją v1.5
        Text,
        nullable=False,
        comment="Treść komentarza. Np. 'Obiecał zapłacić do piątku.'",
    )
    uzytkownik_id: Mapped[int] = mapped_column(
        "UzytkownikID", # ← polska nazwa kolumny zgodna z dokumentacją v1.5
        Integer,
        ForeignKey(
            "dbo_ext.Users.ID_USER",
            ondelete="RESTRICT",  # NIE SET NULL — NOT NULL wymaga RESTRICT
        ),
        nullable=False,          # ← NOT NULL (poprawka względem v1.4)
        comment=(
            "FK → Users. NOT NULL — komentarz musi mieć autora. "
            "RESTRICT: nie można usunąć usera który ma komentarze."
        ),
    )

    # Relacje
    uzytkownik: Mapped["User"] = relationship(  # type: ignore[name-defined]
        "User",
        back_populates="comments",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<Comment id={self.id_comment} "
            f"kontrahent={self.id_kontrahenta} "
            f"uzytkownik={self.uzytkownik_id}>"
        )