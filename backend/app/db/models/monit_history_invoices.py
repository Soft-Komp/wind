# =============================================================================
# backend/app/db/models/monit_history_invoices.py
#
# Model SQLAlchemy ORM dla tabeli dbo_ext.skw_MonitHistory_Invoices
#
# Tabela łączy monit (skw_MonitHistory) z rozrachunkami WAPRO.
# Relacja N:M — jeden monit może dotyczyć wielu rozrachunków,
# jeden rozrachunek może mieć wiele monitów w historii.
#
# Używany przez:
#   - worker/tasks/email_task.py  (INSERT po wysyłce email)
#   - worker/tasks/sms_task.py    (INSERT po wysyłce SMS)
#
# NIE używany przez wapro.py — ten odczytuje dane przez pyodbc z widoków.
#
# Wersja: 1.0.0 | Data: 2026-03-23
# =============================================================================

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, Integer, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base

logger = logging.getLogger(__name__)


class MonitHistoryInvoice(Base):
    """
    Powiązanie monitu z rozrachunkiem WAPRO.

    Tabela: dbo_ext.skw_MonitHistory_Invoices
    Schemat: dbo_ext

    INSERT wykonywany przez worker po każdej pomyślnej wysyłce monitu.
    Jeden rekord = jeden rozrachunek w ramach jednego monitu.

    Przykład: monit ID=5 wysłany do kontrahenta który ma 3 przeterminowane
    rozrachunki → 3 rekordy w tej tabeli (ID_MONIT=5, ID_ROZRACHUNKU=101/102/103).
    """

    __tablename__ = "skw_MonitHistory_Invoices"
    __table_args__ = (
        # Indeks kompozytowy — używany przez check_interval():
        # WHERE ID_ROZRACHUNKU = ? ORDER BY CreatedAt DESC
        Index(
            "IX_skw_MonitHistory_Invoices_ROZR_DATE",
            "ID_ROZRACHUNKU",
            "CreatedAt",
            mssql_clustered=False,
        ),
        {
            "schema":  "dbo_ext",
            "comment": (
                "Powiązanie monitu z rozrachunkiem WAPRO. "
                "INSERT przez worker po pomyślnej wysyłce. "
                "Używany przez check_interval() do sprawdzenia interwału."
            ),
        },
    )

    # ── Klucz główny ──────────────────────────────────────────────────────────
    id_monit_invoice: Mapped[int] = mapped_column(
        "ID_MONIT_INVOICE",
        BigInteger,
        primary_key=True,
        autoincrement=True,
        comment="PK — IDENTITY(1,1)",
    )

    # ── FK → skw_MonitHistory ─────────────────────────────────────────────────
    id_monit: Mapped[int] = mapped_column(
        "ID_MONIT",
        BigInteger,
        ForeignKey(
            "dbo_ext.skw_MonitHistory.ID_MONIT",
            ondelete="CASCADE",
            name="FK_skw_MonitHistory_Invoices_skw_MonitHistory",
        ),
        nullable=False,
        index=True,
        comment="FK → skw_MonitHistory. CASCADE DELETE.",
    )

    # ── ID rozrachunku WAPRO ──────────────────────────────────────────────────
    id_rozrachunku: Mapped[int] = mapped_column(
        "ID_ROZRACHUNKU",
        Integer,
        nullable=False,
        index=True,
        comment=(
            "ID rozrachunku z WAPRO (ROZRACHUNEK_V.id_rozrachunku). "
            "Brak FK — WAPRO jest read-only przez pyodbc."
        ),
    )

    # ── Timestamp ─────────────────────────────────────────────────────────────
    created_at: Mapped[datetime] = mapped_column(
        "CreatedAt",
        DateTime,
        nullable=False,
        server_default=text("GETDATE()"),
        comment="Timestamp zapisu. Używany do wyznaczenia OstatniMonitRozrachunku.",
    )

    # ── Relacja do MonitHistory (opcjonalna — do backref) ─────────────────────
    # Uncomment jeśli potrzebujesz monit.invoice_links w ORM
    # monit: Mapped["MonitHistory"] = relationship(back_populates="invoice_links")

    # ── Repr ──────────────────────────────────────────────────────────────────
    def __repr__(self) -> str:
        return (
            f"MonitHistoryInvoice("
            f"id={self.id_monit_invoice}, "
            f"monit={self.id_monit}, "
            f"rozrachunek={self.id_rozrachunku}, "
            f"created={self.created_at})"
        )

    @classmethod
    def create(
        cls,
        id_monit: int,
        id_rozrachunku: int,
    ) -> "MonitHistoryInvoice":
        """
        Factory method — tworzy rekord powiązania monit↔rozrachunek.

        Args:
            id_monit:       ID monitu z skw_MonitHistory.
            id_rozrachunku: ID rozrachunku z WAPRO.

        Returns:
            Nowy obiekt MonitHistoryInvoice (niezapisany — wymaga session.add()).

        Przykład użycia w worker:
            for inv_id in invoice_ids:
                record = MonitHistoryInvoice.create(
                    id_monit=monit.id_monit,
                    id_rozrachunku=inv_id,
                )
                session.add(record)
            await session.commit()
        """
        return cls(
            id_monit=id_monit,
            id_rozrachunku=id_rozrachunku,
            created_at=datetime.now().replace(tzinfo=None),  # MSSQL: naive datetime
        )