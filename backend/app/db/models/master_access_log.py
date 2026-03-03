"""
Model tabeli dbo_ext.MasterAccessLog.
TYLKO DBA — brak endpointu API. Dostęp wyłącznie przez SSMS.
Loguje użycie MASTER_KEY (dostęp serwisowy).

BEZPIECZEŃSTWO:
  - Tabela NIE pojawia się w żadnym endpoincie API
  - Dostęp DB user aplikacji: tylko INSERT (brak SELECT/UPDATE/DELETE)
  - MASTER_KEY nigdy nie trafia do tej tabeli (tylko fakt użycia)
  - Pełna historia: kto, skąd, kiedy, do którego konta
"""

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class MasterAccessLog(Base):
    __tablename__ = "skw_MasterAccessLog"
    __table_args__ = {
        "schema": "dbo_ext",
        "comment": (
            "Logi użycia MASTER_KEY. "
            "BRAK endpointu API. "
            "Dostęp TYLKO przez SSMS (DBA). "
            "App user: tylko INSERT."
        ),
    }

    id_log: Mapped[int] = mapped_column(
        "ID_LOG", BigInteger, primary_key=True, autoincrement=True,
    )
    target_user_id: Mapped[int | None] = mapped_column(
        "TargetUserID",
        Integer,
        ForeignKey("dbo_ext.Users.ID_USER", ondelete="SET NULL"),
        nullable=True,
        comment="ID konta do którego uzyskano dostęp przez MASTER_KEY",
    )
    target_username: Mapped[str] = mapped_column(
        "TargetUsername", String(50), nullable=False,
        comment="Kopia username — zachowana nawet po usunięciu usera",
    )
    ip_address: Mapped[str] = mapped_column(
        "IPAddress", String(45), nullable=False,
        comment="IP z którego użyto MASTER_KEY",
    )
    user_agent: Mapped[str | None] = mapped_column(
        "UserAgent", String(500), nullable=True,
    )
    accessed_at: Mapped[datetime] = mapped_column(
        "AccessedAt", DateTime, nullable=False,
        default=datetime.utcnow, server_default=text("GETDATE()"),
    )
    session_ended_at: Mapped[datetime | None] = mapped_column(
        "SessionEndedAt", DateTime, nullable=True,
        comment="Kiedy zakończono sesję impersonacji",
    )
    notes: Mapped[str | None] = mapped_column(
        "Notes", String(500), nullable=True,
        comment="Opcjonalna notatka serwisowa",
    )

    def __repr__(self) -> str:
        return (
            f"<MasterAccessLog id={self.id_log} "
            f"target={self.target_username!r} "
            f"ip={self.ip_address!r}>"
        )