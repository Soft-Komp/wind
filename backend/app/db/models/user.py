# =============================================================================
# backend/app/db/models/user.py
# =============================================================================
# Model SQLAlchemy dla tabeli dbo_ext.Users
#
# NAPRAWY W TEJ WERSJI:
#   [B1] KRYTYCZNY — relacja `comments` miała błędne wcięcie (była POZA klasą).
#        Naprawiono: relacja jest teraz wewnątrz klasy User z prawidłowym wcięciem.
#   [MODERNIZACJA] datetime.utcnow() → datetime.now(timezone.utc)
#        Python 3.12 emituje DeprecationWarning dla utcnow().
#        Usunięte w Python 3.14 — PLAN_PRAC.md §"Co WYMAGA UWAGI/MODERNIZACJI"
#
# Tabela wg: TABELE_REFERENCJA v1.0 §4 + USTALENIA_PROJEKTU v1.4 §5.4
# Schemat: dbo_ext (custom, zarządzany przez Alembic)
#
# Wersja: 1.1.0 | Data: 2026-02-17 | Faza: 0 — naprawa B1
# =============================================================================

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    BitString,
    DateTime,
    ForeignKey,
    Integer,
    String,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base

# TYPE_CHECKING — importy tylko dla anotacji typów, nie w runtime
# Zapobiega circular imports: user.py ↔ comment.py ↔ monit_history.py
if TYPE_CHECKING:
    from app.db.models.role import Role
    from app.db.models.comment import Comment
    from app.db.models.refresh_token import RefreshToken
    from app.db.models.otp_code import OtpCode
    from app.db.models.audit_log import AuditLog
    from app.db.models.monit_history import MonitHistory

logger = logging.getLogger(__name__)


class User(Base):
    """
    Model użytkownika systemu windykacyjnego.

    Tabela: dbo_ext.Users
    Schemat: dbo_ext (zarządzany przez Alembic, nie WAPRO)

    Zasady:
        - Soft delete: IsActive = 0 (nigdy fizyczny DELETE)
        - PasswordHash: argon2-cffi hash — nigdy plain text
        - Blokada konta: FailedLoginAttempts >= 5 → LockedUntil = now + 15min
        - LastLoginAt: aktualizowany przez auth_service przy każdym logowaniu
        - Relacje komentarzy i monitów: przez ID_KONTRAHENTA (WAPRO), nie FK

    Bezpieczeństwo:
        - __repr__ NIGDY nie wypisuje PasswordHash
        - Kolumna PasswordHash jest String(255) — argon2 hash mieści się w 255 znakach
    """

    __tablename__ = "Users"
    __table_args__ = {
        "schema": "dbo_ext",
        "comment": "Użytkownicy systemu windykacyjnego — zarządzani przez aplikację",
    }

    # ── Klucz główny ──────────────────────────────────────────────────────────
    id_user: Mapped[int] = mapped_column(
        "ID_USER",
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Klucz główny — IDENTITY(1,1)",
    )

    # ── Dane logowania ────────────────────────────────────────────────────────
    username: Mapped[str] = mapped_column(
        "Username",
        String(50),
        unique=True,
        nullable=False,
        comment="Nazwa użytkownika — unikalny login, min 3 znaki",
    )

    email: Mapped[str] = mapped_column(
        "Email",
        String(100),
        unique=True,
        nullable=False,
        comment="Adres email — unikalny, używany do OTP i powiadomień",
    )

    password_hash: Mapped[str] = mapped_column(
        "PasswordHash",
        String(255),
        nullable=False,
        comment="Hash hasła argon2-cffi — NIGDY plain text w logach ani response",
    )

    # ── Dane profilowe ────────────────────────────────────────────────────────
    full_name: Mapped[str | None] = mapped_column(
        "FullName",
        String(100),
        nullable=True,
        comment="Imię i nazwisko — opcjonalne, wyświetlane w UI",
    )

    # ── Rola i aktywność ──────────────────────────────────────────────────────
    role_id: Mapped[int] = mapped_column(
        "RoleID",
        Integer,
        ForeignKey("dbo_ext.Roles.ID_ROLE", ondelete="RESTRICT"),
        nullable=False,
        comment="FK → Roles.ID_ROLE — RESTRICT: nie można usunąć roli z userami",
    )

    is_active: Mapped[bool] = mapped_column(
        "IsActive",
        # MSSQL BIT — True/False mapowane jako 1/0
        Integer,
        nullable=False,
        server_default=text("1"),
        comment="Soft delete: 1=aktywny, 0=usunięty — nigdy fizyczny DELETE",
    )

    # ── Śledzenie sesji ───────────────────────────────────────────────────────
    last_login_at: Mapped[datetime | None] = mapped_column(
        "LastLoginAt",
        DateTime,
        nullable=True,
        comment="Ostatnie logowanie — aktualizowane przez auth_service.login()",
    )

    # ── Blokada konta (brute-force protection) ────────────────────────────────
    failed_login_attempts: Mapped[int] = mapped_column(
        "FailedLoginAttempts",
        Integer,
        nullable=False,
        server_default=text("0"),
        comment=(
            "Licznik nieudanych logowań. "
            "Po ≥5 próbach → LockedUntil = now + 15min. "
            "Reset do 0 przy pomyślnym logowaniu."
        ),
    )

    locked_until: Mapped[datetime | None] = mapped_column(
        "LockedUntil",
        DateTime,
        nullable=True,
        comment=(
            "Konto zablokowane do tej daty. "
            "NULL = nie zablokowane. "
            "Sprawdzane w auth_service przed każdą próbą logowania."
        ),
    )

    # ── Timestamps ────────────────────────────────────────────────────────────
    # MODERNIZACJA: datetime.now(timezone.utc) zamiast deprecated datetime.utcnow()
    # Python 3.12 DeprecationWarning → usunięte w 3.14 (PLAN_PRAC.md §modernizacja)
    created_at: Mapped[datetime] = mapped_column(
        "CreatedAt",
        DateTime,
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        server_default=text("GETDATE()"),
        comment="Data utworzenia rekordu — ustawiana raz, nigdy modyfikowana",
    )

    updated_at: Mapped[datetime | None] = mapped_column(
        "UpdatedAt",
        DateTime,
        nullable=True,
        onupdate=lambda: datetime.now(timezone.utc),
        comment=(
            "Data ostatniej modyfikacji. "
            "Aktualizowana redundantnie: SQLAlchemy onupdate + trigger MSSQL "
            "(014_triggers_updated_at.sql). NULL = nigdy modyfikowany."
        ),
    )

    # =========================================================================
    # RELACJE SQLAlchemy
    # =========================================================================
    # UWAGA NA WCIĘCIE: WSZYSTKIE relacje MUSZĄ być wewnątrz klasy (4 spacje).
    # Błąd B1 polegał na tym, że relacja `comments` była POZA klasą (0 spacji).
    # SQLAlchemy cicho ignoruje atrybut poza klasą — model był niekompletny.
    # =========================================================================

    # ── Rola (Many-to-One) ────────────────────────────────────────────────────
    role: Mapped["Role"] = relationship(
        "Role",
        back_populates="users",
        lazy="select",
        # innerjoin=True — każdy user MA rolę (nullable=False FK)
        innerjoin=True,
    )

    # ── Tokeny odświeżania (One-to-Many) ──────────────────────────────────────
    refresh_tokens: Mapped[list["RefreshToken"]] = relationship(
        "RefreshToken",
        back_populates="user",
        cascade="all, delete-orphan",
        lazy="select",
        order_by="RefreshToken.created_at.desc()",
    )

    # ── Kody OTP (One-to-Many) ────────────────────────────────────────────────
    otp_codes: Mapped[list["OtpCode"]] = relationship(
        "OtpCode",
        back_populates="user",
        cascade="all, delete-orphan",
        lazy="select",
    )

    # ── Wpisy audytu (One-to-Many) ────────────────────────────────────────────
    # viewonly=True — AuditLog jest append-only, nigdy modyfikowany przez ORM
    audit_logs: Mapped[list["AuditLog"]] = relationship(
        "AuditLog",
        back_populates="user",
        foreign_keys="[AuditLog.user_id]",
        viewonly=True,
        lazy="select",
    )

    # ── Historia monitów (One-to-Many) ────────────────────────────────────────
    # SET NULL przy usunięciu usera — MonitHistory.ID_USER może być NULL
    monit_history: Mapped[list["MonitHistory"]] = relationship(
        "MonitHistory",
        back_populates="user",
        foreign_keys="[MonitHistory.id_user]",
        lazy="select",
    )

    # ── Komentarze (One-to-Many) ──────────────────────────────────────────────
    # [B1] FIX: ta relacja była POZA klasą w poprzedniej wersji pliku.
    # Poprawka: 4 spacje wcięcia — relacja jest teraz wewnątrz klasy User.
    # RESTRICT przy usunięciu — nie można usunąć usera który ma komentarze.
    comments: Mapped[list["Comment"]] = relationship(
        "Comment",
        back_populates="uzytkownik",
        foreign_keys="[Comment.uzytkownik_id]",
        lazy="select",
        # Brak cascade — RESTRICT na FK: SQLAlchemy rzuci IntegrityError
        # zamiast cicho usuwać komentarze przy usunięciu usera
    )

    # =========================================================================
    # METODY POMOCNICZE
    # =========================================================================

    @property
    def is_locked(self) -> bool:
        """
        Sprawdza czy konto jest aktualnie zablokowane.

        Returns:
            True jeśli LockedUntil jest w przyszłości (konto zablokowane).
            False jeśli NULL lub data w przeszłości.

        Użycie w auth_service:
            if user.is_locked:
                raise AccountLockedException(until=user.locked_until)
        """
        if self.locked_until is None:
            return False
        # Porównanie timezone-aware — locked_until z DB może być naive
        # Konwertujemy do UTC dla bezpieczeństwa
        now = datetime.now(timezone.utc)
        locked = self.locked_until
        if locked.tzinfo is None:
            # DB zwraca naive datetime — traktujemy jako UTC
            locked = locked.replace(tzinfo=timezone.utc)
        return now < locked

    @property
    def display_name(self) -> str:
        """
        Nazwa do wyświetlenia w UI i logach.
        Preferuje FullName, fallback do Username.
        """
        return self.full_name or self.username

    def __repr__(self) -> str:
        """
        BEZPIECZEŃSTWO: PasswordHash jest ZAWSZE redagowany.
        Ten __repr__ może pojawić się w logach — nigdy nie wypisujemy hasła.
        """
        return (
            f"<User("
            f"id={self.id_user!r}, "
            f"username={self.username!r}, "
            f"email={self.email!r}, "
            f"role_id={self.role_id!r}, "
            f"is_active={self.is_active!r}, "
            f"password_hash='[REDACTED]'"
            f")>"
        )

    def __str__(self) -> str:
        return f"User({self.username!r}, id={self.id_user})"