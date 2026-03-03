"""
Model tabeli dbo_ext.MonitHistory.
Historia wszystkich wysłanych monitów (email, SMS, print).
ID_KONTRAHENTA to referencja do tabeli WAPRO.KONTRAHENT — read-only, bez FK constraint.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, Numeric, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

if TYPE_CHECKING:
    from app.db.models.user import User

MONIT_TYPES = frozenset({"email", "sms", "print"})
MONIT_STATUSES = frozenset({
    "pending", "sent", "delivered",
    "bounced", "failed", "opened", "clicked",
})


class MonitHistory(Base):
    __tablename__ = "skw_MonitHistory"
    __table_args__ = {
        "schema": "dbo_ext",
        "comment": (
            "Historia monitów windykacyjnych. "
            "ID_KONTRAHENTA = ref do WAPRO (bez FK — read-only)."
        ),
    }

    id_monit: Mapped[int] = mapped_column(
        "ID_MONIT", BigInteger, primary_key=True, autoincrement=True,
    )
    id_kontrahenta: Mapped[int] = mapped_column(
        "ID_KONTRAHENTA", Integer, nullable=False,
        comment="Ref do WAPRO.KONTRAHENT — brak FK constraint (WAPRO = read-only)",
    )
    id_user: Mapped[int | None] = mapped_column(
        "ID_USER",
        Integer,
        ForeignKey("dbo_ext.Users.ID_USER", ondelete="SET NULL"),
        nullable=True,
        comment="Kto zlecił wysyłkę. NULL = automatyczny task ARQ.",
    )
    monit_type: Mapped[str] = mapped_column(
        "MonitType", String(20), nullable=False,
        comment=f"Typ: {', '.join(sorted(MONIT_TYPES))}",
    )
    template_id: Mapped[int | None] = mapped_column(
        "TemplateID",
        Integer,
        ForeignKey("dbo_ext.Templates.ID_TEMPLATE", ondelete="SET NULL"),
        nullable=True,
        comment="FK → Templates. NULL jeśli wysłano bez szablonu.",
    )
    status: Mapped[str] = mapped_column(
        "Status", String(20), nullable=False, default="pending",
        server_default="pending",
        comment=f"Status: {', '.join(sorted(MONIT_STATUSES))}",
    )
    recipient: Mapped[str | None] = mapped_column(
        "Recipient", String(100), nullable=True,
        comment="Email lub numer telefonu odbiorcy",
    )
    subject: Mapped[str | None] = mapped_column(
        "Subject", String(200), nullable=True,
        comment="Temat wiadomości email",
    )
    message_body: Mapped[str | None] = mapped_column(
        "MessageBody", Text, nullable=True,
        comment="Treść wiadomości (email body lub treść SMS)",
    )
    total_debt: Mapped[Decimal | None] = mapped_column(
        "TotalDebt", Numeric(18, 2), nullable=True,
        comment="Kwota długu w momencie wysyłki",
    )
    invoice_numbers: Mapped[str | None] = mapped_column(
        "InvoiceNumbers", String(500), nullable=True,
        comment="JSON lista numerów faktur objętych monitem",
    )
    pdf_path: Mapped[str | None] = mapped_column(
        "PDFPath", String(500), nullable=True,
        comment="Ścieżka do PDF jeśli zapisany na dysk. NULL = blob on-demand.",
    )
    external_id: Mapped[str | None] = mapped_column(
        "ExternalID", String(100), nullable=True,
        comment="ID z bramki SMS/Email do trackingu statusu",
    )
    scheduled_at: Mapped[datetime | None] = mapped_column(
        "ScheduledAt", DateTime, nullable=True,
    )
    sent_at: Mapped[datetime | None] = mapped_column(
        "SentAt", DateTime, nullable=True,
    )
    delivered_at: Mapped[datetime | None] = mapped_column(
        "DeliveredAt", DateTime, nullable=True,
    )
    opened_at: Mapped[datetime | None] = mapped_column(
        "OpenedAt", DateTime, nullable=True,
    )
    clicked_at: Mapped[datetime | None] = mapped_column(
        "ClickedAt", DateTime, nullable=True,
    )
    error_message: Mapped[str | None] = mapped_column(
        "ErrorMessage", String(500), nullable=True,
    )
    retry_count: Mapped[int] = mapped_column(
        "RetryCount", Integer, nullable=False, default=0, server_default="0",
    )
    cost: Mapped[Decimal | None] = mapped_column(
        "Cost", Numeric(10, 4), nullable=True,
        comment="Koszt wysyłki (SMS = per message, Email = zwykle 0)",
    )
    is_active: Mapped[bool] = mapped_column(
        "IsActive", nullable=False, default=True, server_default=text("1"),
    )
    created_at: Mapped[datetime] = mapped_column(
        "CreatedAt", DateTime, nullable=False,
        default=datetime.utcnow, server_default=text("GETDATE()"),
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        "UpdatedAt", DateTime, nullable=True, onupdate=datetime.utcnow,
    )

    # ── Relacje ───────────────────────────────────────────────────────────────
    user: Mapped["User | None"] = relationship(  # type: ignore[name-defined]
        "User",
        back_populates="monit_history",
        foreign_keys=[id_user],
        lazy="select",
    )

    def __repr__(self) -> str:
        return (
            f"<MonitHistory id={self.id_monit} "
            f"kontrahent={self.id_kontrahenta} "
            f"type={self.monit_type!r} "
            f"status={self.status!r}>"
        )