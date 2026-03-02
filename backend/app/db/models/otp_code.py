"""
Model tabeli dbo_ext.OtpCodes.
Jednorazowe kody OTP do resetu hasła i 2FA.
Czas życia: 15 minut (otp.expiry_minutes z SystemConfig).
Kod hashowany — nigdy plain text w bazie.
"""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

# Dozwolone przeznaczenia kodu OTP
OTP_PURPOSES = frozenset({"password_reset", "2fa"})


class OtpCode(Base):
    __tablename__ = "OtpCodes"
    __table_args__ = {
        "schema": "dbo_ext",
        "comment": "Jednorazowe kody OTP. Kod kolumna zawiera HASH — nigdy plain.",
    }

    id_otp: Mapped[int] = mapped_column(
        "ID_OTP", Integer, primary_key=True, autoincrement=True,
        comment="Klucz główny",
    )
    id_user: Mapped[int] = mapped_column(
        "ID_USER",
        Integer,
        ForeignKey("dbo_ext.Users.ID_USER", ondelete="CASCADE"),
        nullable=False,
        comment="FK → Users",
    )
    code: Mapped[str] = mapped_column(
        "Code", String(64), nullable=False,
        comment="Hash kodu OTP (argon2 lub sha256) — NIE plain 6-cyfrowy kod",
    )
    purpose: Mapped[str] = mapped_column(
        "Purpose", String(20), nullable=False,
        comment=f"Przeznaczenie: {', '.join(sorted(OTP_PURPOSES))}",
    )
    expires_at: Mapped[datetime] = mapped_column(
        "ExpiresAt", DateTime, nullable=False,
        comment="Data wygaśnięcia. TTL: otp.expiry_minutes z SystemConfig (default 15 min)",
    )
    is_used: Mapped[bool] = mapped_column(
        "IsUsed", Boolean, nullable=False, default=False,
        server_default="0",
        comment="Czy kod został już wykorzystany. Jednorazowy.",
    )
    created_at: Mapped[datetime] = mapped_column(
        "CreatedAt", DateTime, nullable=False,
        default=datetime.utcnow,
        server_default=text("GETDATE()"),
    )
    ip_address: Mapped[str | None] = mapped_column(
        "IPAddress", String(45), nullable=True,
        comment="IP z którego zainicjowano reset",
    )

    # Relacje
    user: Mapped["User"] = relationship(  # type: ignore[name-defined]
        "User", back_populates="otp_codes"
    )

    def __repr__(self) -> str:
        return f"<OtpCode id={self.id_otp} user={self.id_user} purpose={self.purpose!r} used={self.is_used}>"