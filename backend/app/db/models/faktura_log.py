"""
Plik   : app/db/models/faktura_log.py
Moduł  : Akceptacja Faktur KSeF
Model  : FakturaLog → dbo_ext.skw_faktura_log

Immutable audit trail modułu faktur.
ZASADA: tylko INSERT — nigdy UPDATE ani DELETE.
Analogia do skw_AuditLog — stąd brak UpdatedAt.

user_id = NULL oznacza akcję systemową (auto-akceptacja, timeout, force).
FK user_id → ON DELETE SET NULL — historia przeżywa dezaktywację usera.
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
# Stałe — dozwolone wartości akcji (spójne z CHK_sfl_akcja w DDL 017)
# ---------------------------------------------------------------------------
AKCJA_VALUES = frozenset({
    "przypisano",
    "zaakceptowano",
    "odrzucono",
    "zresetowano",
    "status_zmieniony",
    "priorytet_zmieniony",
    "fakir_update",
    "fakir_update_failed",
    "nie_moje",
    "force_akceptacja",
    "anulowano",
})


class FakturaLog(Base):
    """
    Model ORM: dbo_ext.skw_faktura_log

    Immutable audit trail — każde zdarzenie w module faktur.
    Jeden wiersz = jedno zdarzenie. Nigdy nie modyfikować po INSERT.

    szczegoly: JSON serializowany przez FakturaLogDetails.
    Bezpośrednie wstawianie dict ZABRONIONE — zawsze używaj FakturaLogDetails.build().

    user_id = NULL → akcja systemowa (nie użytkownik).
    """

    __tablename__  = "skw_faktura_log"
    __table_args__ = (
        CheckConstraint(
            "akcja IN ("
            "'przypisano','zaakceptowano','odrzucono','zresetowano',"
            "'status_zmieniony','priorytet_zmieniony','fakir_update',"
            "'fakir_update_failed','nie_moje','force_akceptacja','anulowano'"
            ")",
            name="CHK_sfl_akcja",
        ),
        Index("IX_sfl_faktura_created", "faktura_id", "created_at"),
        {"schema": "dbo_ext"},
    )

    # ── Kolumny ──────────────────────────────────────────────────────────────

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Klucz główny IDENTITY(1,1)",
    )

    faktura_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("dbo_ext.skw_faktura_akceptacja.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="FK → skw_faktura_akceptacja",
    )

    user_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("dbo_ext.skw_Users.ID_USER", ondelete="SET NULL"),
        nullable=True,
        comment="Kto wykonał akcję — NULL = akcja systemowa",
    )

    akcja: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="Typ zdarzenia — patrz AKCJA_VALUES",
    )

    szczegoly: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="JSON (model FakturaLogDetails) — nigdy raw dict",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.getdate(),
        comment="Timestamp immutable — nigdy nie aktualizować",
    )

    # ── Relacje ───────────────────────────────────────────────────────────────

    faktura: Mapped["FakturaAkceptacja"] = relationship(
        "FakturaAkceptacja",
        back_populates="logi",
    )

    user: Mapped[Optional["User"]] = relationship(
        "User",
        foreign_keys=[user_id],
    )