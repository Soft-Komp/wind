"""
Plik   : app/db/models/faktura_akceptacja.py
Moduł  : Akceptacja Faktur KSeF
Model  : FakturaAkceptacja → dbo_ext.skw_faktura_akceptacja

Odwzorowuje główną tabelę modułu: jeden wiersz na fakturę wpuszczoną do obiegu.

Zasady systemu:
  - Soft-delete: IsActive=0 przy anulowaniu (nigdy fizyczny DELETE)
  - UpdatedAt: redundantnie SQLAlchemy onupdate + trigger MSSQL
  - MSSQL wymaga naiwnych datetime (bez tzinfo) — używamy datetime.utcnow
  - FK do skw_Users.ID_USER z RESTRICT (NO ACTION) — spójne z DDL 015

Relacje ORM:
  - przypisania: List[FakturaPrzypisanie] (back_populates)
  - logi:        List[FakturaLog]         (back_populates)
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import (
    BigInteger,
    BitString,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base

if TYPE_CHECKING:
    from app.db.models.faktura_przypisanie import FakturaPrzypisanie
    from app.db.models.faktura_log import FakturaLog
    from app.db.models.user import User

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stałe — dozwolone wartości enum-like (walidacja na poziomie Pydantic + DB CHECK)
# ---------------------------------------------------------------------------
STATUS_WEWNETRZNY_VALUES = frozenset({
    "nowe",
    "w_toku",
    "zaakceptowana",
    "anulowana",
})

PRIORYTET_VALUES = frozenset({
    "normalny",
    "pilny",
    "bardzo_pilny",
})


class FakturaAkceptacja(Base):
    """
    Model ORM: dbo_ext.skw_faktura_akceptacja

    Jeden wiersz = jedna faktura zakupowa z KSeF wpuszczona do obiegu akceptacji.

    Ważne:
        - numer_ksef jest UNIKALNY i wskazuje na BUF_DOKUMENT.KSEF_ID w WAPRO.
          Nigdy nie edytować po INSERT.
        - status_wewnetrzny jest stanem w NASZYM systemie (nie w Fakirze).
          Stan w Fakirze (KOD_STATUSU) jest oddzielny i aktualizowany przez
          fakir_write po zakończeniu akceptacji.
    """

    __tablename__ = "skw_faktura_akceptacja"
    __table_args__ = (
        # Indeksy pokrywające zgodne z DDL 015
        Index(
            "IX_skw_faktura_akceptacja_status_active",
            "status_wewnetrzny",
            "IsActive",
        ),
        Index(
            "IX_skw_faktura_akceptacja_priorytet",
            "priorytet",
            "IsActive",
        ),
        Index(
            "IX_skw_faktura_akceptacja_utworzony_przez",
            "utworzony_przez",
        ),
        # Constraints walidacyjne — zwalidowane też na poziomie DB CHECK
        CheckConstraint(
            "status_wewnetrzny IN ('nowe','w_toku','zaakceptowana','anulowana')",
            name="CK_skw_faktura_akceptacja_status",
        ),
        CheckConstraint(
            "priorytet IN ('normalny','pilny','bardzo_pilny')",
            name="CK_skw_faktura_akceptacja_priorytet",
        ),
        UniqueConstraint("numer_ksef", name="UQ_skw_faktura_akceptacja_numer_ksef"),
        {"schema": "dbo_ext"},
    )

    # ------------------------------------------------------------------
    # Kolumny
    # ------------------------------------------------------------------

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Klucz główny IDENTITY",
    )

    numer_ksef: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="Unikalny ID KSeF = BUF_DOKUMENT.KSEF_ID. Niezmienny po INSERT.",
    )

    status_wewnetrzny: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        comment="nowe | w_toku | zaakceptowana | anulowana",
    )

    priorytet: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        default="normalny",
        server_default="normalny",
        comment="normalny | pilny | bardzo_pilny",
    )

    opis_dokumentu: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Formalny opis faktury wpisany przez referenta",
    )

    uwagi: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Nieformalne uwagi referenta (wewnętrzne)",
    )

    utworzony_przez: Mapped[int] = mapped_column(
        Integer,
        ForeignKey(
            "dbo_ext.skw_Users.ID_USER",
            ondelete="RESTRICT",
            name="FK_skw_faktura_akceptacja_utworzony_przez",
        ),
        nullable=False,
        comment="ID usera który wpuścił fakturę do obiegu (FK RESTRICT)",
    )

    IsActive: Mapped[bool] = mapped_column(
        Integer,  # BIT w MSSQL → Integer w SQLAlchemy
        nullable=False,
        default=1,
        server_default="1",
        comment="Soft-delete: 1=aktywna, 0=anulowana",
    )

    CreatedAt: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,          # SQLAlchemy side
        server_default=func.getdate(),    # DB side — fallback
        comment="Timestamp utworzenia (immutable)",
    )

    UpdatedAt: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True,
        default=None,
        onupdate=datetime.utcnow,         # SQLAlchemy side
        comment="Timestamp ostatniej modyfikacji (trigger + ORM onupdate)",
    )

    # ------------------------------------------------------------------
    # Relacje ORM
    # ------------------------------------------------------------------

    przypisania: Mapped[List["FakturaPrzypisanie"]] = relationship(
        "FakturaPrzypisanie",
        back_populates="faktura",
        lazy="select",
        cascade="save-update, merge",
    )

    logi: Mapped[List["FakturaLog"]] = relationship(
        "FakturaLog",
        back_populates="faktura",
        lazy="select",
        order_by="FakturaLog.CreatedAt.desc()",
    )

    autor: Mapped["User"] = relationship(
        "User",
        foreign_keys=[utworzony_przez],
        lazy="select",
    )

    # ------------------------------------------------------------------
    # Metody pomocnicze
    # ------------------------------------------------------------------

    def is_finalized(self) -> bool:
        """Czy faktura jest w stanie końcowym (nie można modyfikować)."""
        return self.status_wewnetrzny in ("zaakceptowana", "anulowana")

    def can_accept_decisions(self) -> bool:
        """Czy pracownicy mogą jeszcze podejmować decyzje."""
        return self.status_wewnetrzny == "w_toku" and bool(self.IsActive)

    def to_log_dict(self) -> dict:
        """
        Zwraca słownik do użycia w polu 'before'/'after' FakturaLogDetails.
        Zawiera TYLKO pola które mogą się zmieniać — nie ID, nie daty.
        """
        return {
            "status_wewnetrzny": self.status_wewnetrzny,
            "priorytet":         self.priorytet,
            "IsActive":          int(self.IsActive),
        }

    def __repr__(self) -> str:
        return (
            f"<FakturaAkceptacja id={self.id!r} "
            f"ksef={self.numer_ksef!r} "
            f"status={self.status_wewnetrzny!r} "
            f"active={self.IsActive!r}>"
        )