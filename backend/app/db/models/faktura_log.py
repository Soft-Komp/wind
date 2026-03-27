"""
Plik   : app/db/models/faktura_przypisanie.py
Moduł  : Akceptacja Faktur KSeF
Model  : FakturaPrzypisanie → dbo_ext.skw_faktura_przypisanie

Jeden wiersz = jeden pracownik przypisany do jednej faktury.

Ważne:
  - is_active=0 przy resecie przypisań (NIE DELETE)
  - Jeden pracownik może być przypisany wielokrotnie (po resecie)
    — poprzednie wiersze is_active=0
  - komentarz przechowywany jawnie w TEJ tabeli
    DO AuditLog trafia wyłącznie SHA256(komentarz) — privacy by design
  - decided_at ustawiany TYLKO przy zmianie statusu z 'oczekuje'
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base

if TYPE_CHECKING:
    from app.db.models.faktura_akceptacja import FakturaAkceptacja
    from app.db.models.user import User

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stałe — dozwolone statusy decyzji
# ---------------------------------------------------------------------------
STATUS_PRZYPISANIA_VALUES = frozenset({
    "oczekuje",
    "zaakceptowane",
    "odrzucone",
    "nie_moje",
})

# Statusy które oznaczają podjętą decyzję (nie oczekuje)
STATUS_ZDECYDOWANE = frozenset({
    "zaakceptowane",
    "odrzucone",
    "nie_moje",
})

# Tylko ten status traktowany jako pozytywna akceptacja
STATUS_AKCEPTACJA = "zaakceptowane"


class FakturaPrzypisanie(Base):
    """
    Model ORM: dbo_ext.skw_faktura_przypisanie

    Przypisanie pracownika do faktury w obiegu akceptacji.

    Logika biznesowa:
        - Referent tworzy przypisania POST /faktury-akceptacja (nowe)
        - Pracownik zmienia status POST /moje-faktury/{id}/decyzja
        - Referent może dezaktywować (reset): is_active=0, create new
        - Saga pattern sprawdza: ile is_active=1 AND status='zaakceptowane'
          vs ile is_active=1 ogółem → jeśli wszystkie OK → UPDATE Fakira
    """

    __tablename__ = "skw_faktura_przypisanie"
    __table_args__ = (
        # Indeksy krytyczne z DDL 016
        Index(
            "IX_skw_faktura_przypisanie_user_active",
            "user_id",
            "is_active",
            "status",
        ),
        Index(
            "IX_skw_faktura_przypisanie_faktura_active",
            "faktura_id",
            "is_active",
        ),
        CheckConstraint(
            "status IN ('oczekuje','zaakceptowane','odrzucone','nie_moje')",
            name="CK_skw_faktura_przypisanie_status",
        ),
        {"schema": "dbo_ext"},
    )

    # ------------------------------------------------------------------
    # Kolumny
    # ------------------------------------------------------------------

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    faktura_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey(
            "dbo_ext.skw_faktura_akceptacja.id",
            ondelete="NO ACTION",
            name="FK_skw_faktura_przypisanie_faktura",
        ),
        nullable=False,
        index=False,  # Pokryte przez IX_skw_faktura_przypisanie_faktura_active
        comment="FK do skw_faktura_akceptacja",
    )

    user_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey(
            "dbo_ext.skw_Users.ID_USER",
            ondelete="NO ACTION",
            name="FK_skw_faktura_przypisanie_user",
        ),
        nullable=False,
        index=False,  # Pokryte przez IX_skw_faktura_przypisanie_user_active
        comment="FK do skw_Users — przypisany pracownik",
    )

    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="oczekuje",
        server_default="oczekuje",
        comment="oczekuje | zaakceptowane | odrzucone | nie_moje",
    )

    komentarz: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Komentarz przy decyzji — pełna treść. Do AuditLog idzie SHA256.",
    )

    is_active: Mapped[bool] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        server_default="1",
        comment="0 = dezaktywowane przez reset referenta",
    )

    CreatedAt: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.getdate(),
    )

    UpdatedAt: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True,
        default=None,
        onupdate=datetime.utcnow,
    )

    decided_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True,
        default=None,
        comment="Kiedy pracownik podjął decyzję (NULL dopóki status=oczekuje)",
    )

    # ------------------------------------------------------------------
    # Relacje ORM
    # ------------------------------------------------------------------

    faktura: Mapped["FakturaAkceptacja"] = relationship(
        "FakturaAkceptacja",
        back_populates="przypisania",
        lazy="select",
    )

    pracownik: Mapped["User"] = relationship(
        "User",
        foreign_keys=[user_id],
        lazy="select",
    )

    # ------------------------------------------------------------------
    # Metody pomocnicze
    # ------------------------------------------------------------------

    def is_decided(self) -> bool:
        """Czy pracownik podjął już decyzję."""
        return self.status in STATUS_ZDECYDOWANE

    def is_positive_acceptance(self) -> bool:
        """Czy decyzja była pozytywna (zaakceptował)."""
        return self.status == STATUS_AKCEPTACJA

    def to_log_dict(self) -> dict:
        """Słownik dla before/after w FakturaLogDetails."""
        return {
            "user_id":    self.user_id,
            "status":     self.status,
            "is_active":  int(self.is_active),
        }

    def __repr__(self) -> str:
        return (
            f"<FakturaPrzypisanie id={self.id!r} "
            f"faktura={self.faktura_id!r} "
            f"user={self.user_id!r} "
            f"status={self.status!r} "
            f"active={self.is_active!r}>"
        )