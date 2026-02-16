"""
Model tabeli dbo_ext.Users.
Użytkownicy systemu windykacji. Hasła hashowane przez argon2-cffi.

Bezpieczeństwo:
  - PasswordHash: argon2 (NIE bcrypt, NIE plain SHA)
  - FailedLoginAttempts: blokada po N nieudanych próbach
  - LockedUntil: konto zablokowane do określonego czasu
  - LastLoginAt: aktualizowany przy każdym udanym logowaniu
"""

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import AuditMixin, Base


class User(AuditMixin, Base):
    __tablename__ = "Users"
    __table_args__ = {
        "schema": "dbo_ext",
        "comment": "Użytkownicy systemu. Hasła hashowane argon2. RBAC przez RoleID.",
    }

    id_user: Mapped[int] = mapped_column(
        "ID_USER", Integer, primary_key=True, autoincrement=True,
        comment="Klucz główny",
    )
    username: Mapped[str] = mapped_column(
        "Username", String(50), nullable=False, unique=True,
        comment="Login użytkownika — unikalny, case-insensitive w MSSQL",
    )
    email: Mapped[str] = mapped_column(
        "Email", String(100), nullable=False, unique=True,
        comment="Adres email — unikalny, używany do OTP reset hasła",
    )
    password_hash: Mapped[str] = mapped_column(
        "PasswordHash", String(255), nullable=False,
        comment="Hash argon2 — NIE przechowujemy plain text ani MD5",
    )
    full_name: Mapped[str | None] = mapped_column(
        "FullName", String(100), nullable=True,
        comment="Imię i nazwisko — opcjonalne",
    )
    id_role: Mapped[int] = mapped_column(
        "RoleID",
        Integer,
        ForeignKey("dbo_ext.Roles.ID_ROLE", ondelete="RESTRICT"),
        nullable=False,
        comment="FK → Roles. RESTRICT — nie można usunąć roli z przypisanymi userami",
    )
    last_login_at: Mapped[datetime | None] = mapped_column(
        "LastLoginAt", DateTime, nullable=True,
        comment="Data ostatniego udanego logowania — aktualizowane przez auth_service",
    )
    failed_login_attempts: Mapped[int] = mapped_column(
        "FailedLoginAttempts", Integer, nullable=False, default=0,
        server_default="0",
        comment="Licznik nieudanych prób. Resetowany przy udanym logowaniu.",
    )
    locked_until: Mapped[datetime | None] = mapped_column(
        "LockedUntil", DateTime, nullable=True,
        comment="Konto zablokowane do tej daty. NULL = nie zablokowane.",
    )

    # Relacje
    role: Mapped["Role"] = relationship(  # type: ignore[name-defined]
        "Role", back_populates="users", lazy="selectin"
    )
    refresh_tokens: Mapped[list["RefreshToken"]] = relationship(  # type: ignore[name-defined]
        "RefreshToken", back_populates="user", cascade="all, delete-orphan"
    )
    otp_codes: Mapped[list["OtpCode"]] = relationship(  # type: ignore[name-defined]
        "OtpCode", back_populates="user", cascade="all, delete-orphan"
    )
    audit_logs: Mapped[list["AuditLog"]] = relationship(  # type: ignore[name-defined]
        "AuditLog", back_populates="user"
    )
    comments: Mapped[list["Comment"]] = relationship(  # type: ignore[name-defined]
        "Comment", back_populates="user"
    )

    def __repr__(self) -> str:
        return f"<User id={self.id_user} username={self.username!r} role={self.id_role}>"