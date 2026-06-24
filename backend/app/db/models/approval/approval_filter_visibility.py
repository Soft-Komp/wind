# backend/app/db/models/approval/approval_filter_visibility.py
"""
Model ORM — dbo.skw_approval_filter_visibility

Widocznosc filtrow ograniczonych (visibility_mode='restricted') per grupa/user.

Uzywana gdy ApprovalFilter.visibility_mode == 'restricted'.
Logika: uzytkownik widzi dokumenty filtra restricted TYLKO jesli
jego id_user lub id grupy do ktorej nalezy jest w tej tabeli.

CHECK constraint w DB: dokladnie jedno z id_group / id_user musi byc NOT NULL.
UNIQUE indeksy filtrowane w DB:
  (id_filter, id_group) WHERE id_group IS NOT NULL
  (id_filter, id_user)  WHERE id_user  IS NOT NULL

Uzytkownik z uprawnieniem documents.view_all lub approval.supervise
widzi wszystkie dokumenty niezaleznie od tej tabeli.

UWAGA: from __future__ import annotations — NIGDY w tym pliku.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Identity, Integer, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.approval_filter import ApprovalFilter
    from app.db.models.approval.approval_group import ApprovalGroup
    from app.db.models.user import User

SCHEMA = "dbo"


class ApprovalFilterVisibility(Base):
    """
    Uprawnienie do widocznosci filtra restricted.

    Tabela: dbo.skw_approval_filter_visibility
    CASCADE DELETE z ApprovalFilter.
    """

    __tablename__ = "skw_approval_filter_visibility"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Widocznosc filtrow restricted per grupa/uzytkownik",
    }

    id_visibility: Mapped[int] = mapped_column(
        "id_visibility", Integer, Identity(start=1, increment=1), primary_key=True,
    )
    id_filter: Mapped[int] = mapped_column(
        "id_filter",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_approval_filters.id_filter",
            ondelete="CASCADE",
        ),
        nullable=False,
        index=True,
        comment="FK do filtra — CASCADE DELETE",
    )
    id_group: Mapped[int | None] = mapped_column(
        "id_group",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_approval_groups.id_group",
            ondelete="NO ACTION",
        ),
        nullable=True,
        comment="Grupa majaca dostep do filtra (NULL jesli wpis dla usera)",
    )
    id_user: Mapped[int | None] = mapped_column(
        "id_user",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_Users.ID_USER",
            ondelete="NO ACTION",
        ),
        nullable=True,
        comment="Uzytkownik majacy dostep do filtra (NULL jesli wpis dla grupy)",
    )
    created_at: Mapped[datetime] = mapped_column(
        "created_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), default=_utcnow,
    )

    # ── Relacje ───────────────────────────────────────────────────────────────

    filter_rule: Mapped["ApprovalFilter"] = relationship(
        "ApprovalFilter",
        foreign_keys=[id_filter],
        lazy="noload",
    )
    group: Mapped["ApprovalGroup | None"] = relationship(
        "ApprovalGroup",
        foreign_keys=[id_group],
        lazy="noload",
    )
    user: Mapped["User | None"] = relationship(
        "User",
        foreign_keys=[id_user],
        lazy="noload",
    )

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def is_group_entry(self) -> bool:
        """True jesli wpis dotyczy grupy (nie konkretnego usera)."""
        return self.id_group is not None

    @property
    def is_user_entry(self) -> bool:
        """True jesli wpis dotyczy konkretnego uzytkownika."""
        return self.id_user is not None

    def validate(self) -> list[str]:
        """Waliduje obiekt. Zwraca liste bledow."""
        errors: list[str] = []

        if self.id_group is None and self.id_user is None:
            errors.append("Wymagane dokladnie jedno z: id_group lub id_user")

        if self.id_group is not None and self.id_user is not None:
            errors.append("Tylko jedno z id_group / id_user moze byc NOT NULL")

        return errors

    def __repr__(self) -> str:
        target = f"group={self.id_group}" if self.is_group_entry else f"user={self.id_user}"
        return (
            f"<ApprovalFilterVisibility id={self.id_visibility} "
            f"filter={self.id_filter} {target}>"
        )