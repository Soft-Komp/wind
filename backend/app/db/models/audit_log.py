"""
Model tabeli dbo_ext.AuditLog.
Immutable audit trail — NIGDY nie edytujemy ani nie usuwamy wpisów.
Każda akcja: kto, co, kiedy, IP, OldValue JSON, NewValue JSON.
Zapis asynchroniczny przez audit_service.py — nie blokuje response.
"""

from datetime import datetime

from sqlalchemy import BigInteger, Boolean, ForeignKey, Integer, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

# Dozwolone kategorie akcji
ACTION_CATEGORIES = frozenset({
    "Auth", "Debtors", "Monits", "Users", "Roles",
    "Permissions", "System", "Snapshots", "Comments",
})


class AuditLog(Base):
    __tablename__ = "skw_AuditLog"
    __table_args__ = {
        "schema": "dbo_ext",
        "comment": (
            "Immutable audit trail. "
            "Brak UPDATE/DELETE — tylko INSERT. "
            "OldValue/NewValue jako JSON."
        ),
    }

    id_log: Mapped[int] = mapped_column(
        "ID_LOG", BigInteger, primary_key=True, autoincrement=True,
        comment="Klucz główny — BigInt dla dużych wolumenów",
    )
    id_user: Mapped[int | None] = mapped_column(
        "ID_USER",
        Integer,
        ForeignKey("dbo_ext.skw_Users.ID_USER", ondelete="SET NULL"),
        nullable=True,
        comment="FK → Users. NULL dla akcji systemowych (cron, startup).",
    )
    username: Mapped[str | None] = mapped_column(
        "Username", String(50), nullable=True,
        comment="Kopia username — zachowana nawet po usunięciu usera",
    )
    action: Mapped[str] = mapped_column(
        "Action", String(100), nullable=False,
        comment="Nazwa akcji (snake_case). Np. user_created, monit_sent.",
    )
    action_category: Mapped[str | None] = mapped_column(
        "ActionCategory", String(50), nullable=True,
        comment=f"Kategoria akcji: {', '.join(sorted(ACTION_CATEGORIES))}",
    )
    entity_type: Mapped[str | None] = mapped_column(
        "EntityType", String(50), nullable=True,
        comment="Typ encji której dotyczy akcja (User/Debtor/Monit/Role)",
    )
    entity_id: Mapped[int | None] = mapped_column(
        "EntityID", Integer, nullable=True,
        comment="ID encji której dotyczy akcja",
    )
    old_value: Mapped[str | None] = mapped_column(
        "OldValue", Text, nullable=True,
        comment="Stan PRZED zmianą jako JSON. NULL dla CREATE.",
    )
    new_value: Mapped[str | None] = mapped_column(
        "NewValue", Text, nullable=True,
        comment="Stan PO zmianie jako JSON. NULL dla DELETE.",
    )
    details: Mapped[str | None] = mapped_column(
        "Details", Text, nullable=True,
        comment="Dodatkowe informacje jako JSON. Np. {impersonated_by: 1}",
    )
    ip_address: Mapped[str | None] = mapped_column(
        "IPAddress", String(45), nullable=True,
        comment="IP użytkownika (IPv4 lub IPv6)",
    )
    user_agent: Mapped[str | None] = mapped_column(
        "UserAgent", String(500), nullable=True,
    )
    request_url: Mapped[str | None] = mapped_column(
        "RequestURL", String(500), nullable=True,
        comment="Endpoint API który wywołał akcję",
    )
    request_method: Mapped[str | None] = mapped_column(
        "RequestMethod", String(10), nullable=True,
        comment="HTTP method: GET/POST/PUT/DELETE",
    )
    timestamp: Mapped[datetime] = mapped_column(
        "Timestamp", nullable=False,
        default=datetime.utcnow,
        server_default=text("GETDATE()"),
        comment="Czas zdarzenia (UTC). Nigdy nie modyfikowany.",
    )
    success: Mapped[bool] = mapped_column(
        "Success", Boolean, nullable=False, default=True,
        server_default="1",
        comment="Czy akcja zakończyła się sukcesem",
    )
    error_message: Mapped[str | None] = mapped_column(
        "ErrorMessage", String(500), nullable=True,
        comment="Komunikat błędu jeśli Success = 0",
    )

    # Relacje
    user: Mapped["User | None"] = relationship(  # type: ignore[name-defined]
        "User", back_populates="audit_logs"
    )

    def __repr__(self) -> str:
        return (
            f"<AuditLog id={self.id_log} "
            f"action={self.action!r} "
            f"user={self.id_user} "
            f"success={self.success}>"
        )