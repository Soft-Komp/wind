"""
Model tabeli dbo_ext.RefreshTokens.
JWT refresh tokeny. Token przechowywany jako HASH (sha256) — nigdy plain.
Czas życia: 30 dni (REFRESH_TOKEN_EXPIRE_DAYS z .env).
"""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column, relationship, synonym

from .base import Base


class RefreshToken(Base):
    __tablename__ = "skw_RefreshTokens"
    __table_args__ = {
        "schema": "dbo_ext",
        "comment": "JWT Refresh tokeny. Token kolumna przechowuje HASH sha256 tokenu.",
    }

    id_token: Mapped[int] = mapped_column(
        "ID_TOKEN", Integer, primary_key=True, autoincrement=True,
        comment="Klucz główny",
    )
    id_user: Mapped[int] = mapped_column(
        "ID_USER",
        Integer,
        ForeignKey("dbo_ext.Users.ID_USER", ondelete="CASCADE"),
        nullable=False,
        comment="FK → Users",
    )
    user_id = synonym("id_user")
    token: Mapped[str] = mapped_column(
        "Token", String(500), nullable=False,
        comment="SHA-256 hash refresh tokenu — NIE plain token",
    )
    expires_at: Mapped[datetime] = mapped_column(
        "ExpiresAt", DateTime, nullable=False,
        comment="Data wygaśnięcia. Po tej dacie token jest nieważny.",
    )
    created_at: Mapped[datetime] = mapped_column(
        "CreatedAt", DateTime, nullable=False,
        default=datetime.utcnow,
        server_default=text("GETDATE()"),
        comment="Data utworzenia tokenu",
    )
    is_revoked: Mapped[bool] = mapped_column(
        "IsRevoked", Boolean, nullable=False, default=False,
        server_default="0",
        comment="Czy token został unieważniony przed wygaśnięciem",
    )
    revoked_at: Mapped[datetime | None] = mapped_column(
        "RevokedAt", DateTime, nullable=True,
        comment="Kiedy token został unieważniony",
    )
    ip_address: Mapped[str | None] = mapped_column(
        "IPAddress", String(45), nullable=True,
        comment="IP z którego token został wydany (IPv4 lub IPv6)",
    )
    user_agent: Mapped[str | None] = mapped_column(
        "UserAgent", String(500), nullable=True,
        comment="Przeglądarka/urządzenie z którego token został wydany",
    )

    # Relacje
    user: Mapped["User"] = relationship(  # type: ignore[name-defined]
        "User", back_populates="refresh_tokens"
    )

    def __repr__(self) -> str:
        return (
            f"<RefreshToken id={self.id_token} "
            f"user={self.id_user} revoked={self.is_revoked}>"
        )