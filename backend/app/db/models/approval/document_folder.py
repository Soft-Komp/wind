# backend/app/db/models/approval/document_folder.py
"""
Model ORM — dbo.skw_document_folders + dbo.skw_document_folder_items

Teczki dokumentow — mechanizm filtrowania, niezalezny od kategorii.

Jeden dokument moze byc w wielu teczkach jednoczesnie (wielowymiarowosc).
Teczki to WYLACZNIE filtr — nie wplywaja na obieg dokumentu.

folder_type:
  private — widoczna tylko dla wlasciciela (owner_user)
  team    — widoczna dla czlonkow grupy (owner_group)

Wlasciciel:
  Dokladnie jedno z owner_user / owner_group musi byc NOT NULL.
  CHECK constraint w DB egzekwuje tę zasade.

UWAGA: from __future__ import annotations — NIGDY w tym pliku.
"""

import re
import logging
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, ForeignKey, Identity, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.user import User
    from app.db.models.approval.approval_group import ApprovalGroup
    from app.db.models.approval.document_folder_item import DocumentFolderItem

logger = logging.getLogger(__name__)

SCHEMA = "dbo"

VALID_FOLDER_TYPES = frozenset({"private", "team"})

# Walidacja formatu koloru hex (#RRGGBB)
_HEX_COLOR_RE = re.compile(r'^#[0-9A-Fa-f]{6}$')


class DocumentFolder(Base):
    """
    Teczka dokumentow.

    Tabela: dbo.skw_document_folders
    """

    __tablename__ = "skw_document_folders"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Teczki dokumentow — wielowymiarowy filtr, niezalezny od kategorii",
    }

    id_folder: Mapped[int] = mapped_column(
        "id_folder", Integer, Identity(start=1, increment=1), primary_key=True,
    )
    folder_name: Mapped[str] = mapped_column(
        "folder_name", String(200), nullable=False,
        comment="Nazwa teczki wyswietlana w UI",
    )
    description: Mapped[str | None] = mapped_column(
        "description", String(500), nullable=True,
    )
    color: Mapped[str | None] = mapped_column(
        "color", String(7), nullable=True,
        comment="Kolor teczki w formacie #RRGGBB (opcjonalny, dla UI)",
    )
    folder_type: Mapped[str] = mapped_column(
        "folder_type", String(10), nullable=False,
        server_default=text("N'private'"), default="private",
        comment="private | team",
    )
    owner_user: Mapped[int | None] = mapped_column(
        "owner_user",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_Users.ID_USER",
            ondelete="NO ACTION",
        ),
        nullable=True,
        index=True,
        comment="Wlasciciel-uzytkownik (NULL gdy teczka zespolowa)",
    )
    owner_group: Mapped[int | None] = mapped_column(
        "owner_group",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_approval_groups.id_group",
            ondelete="NO ACTION",
        ),
        nullable=True,
        index=True,
        comment="Wlasciciel-grupa (NULL gdy teczka prywatna)",
    )
    is_active: Mapped[bool] = mapped_column(
        "is_active", Boolean, nullable=False,
        server_default=text("1"), default=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        "created_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), default=_utcnow,
    )
    updated_at: Mapped[datetime] = mapped_column(
        "updated_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), default=_utcnow, onupdate=_utcnow,
    )

    # ── Relacje ───────────────────────────────────────────────────────────────

    user_owner: Mapped["User | None"] = relationship(
        "User",
        foreign_keys=[owner_user],
        lazy="noload",
    )
    group_owner: Mapped["ApprovalGroup | None"] = relationship(
        "ApprovalGroup",
        foreign_keys=[owner_group],
        lazy="noload",
    )
    items: Mapped[list["DocumentFolderItem"]] = relationship(
        "DocumentFolderItem",
        back_populates="folder",
        cascade="all, delete-orphan",
        lazy="noload",
    )

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def is_private(self) -> bool:
        return self.folder_type == "private"

    @property
    def is_team(self) -> bool:
        return self.folder_type == "team"

    @property
    def document_count(self) -> int:
        """Liczba dokumentow w teczce. Wymaga zaladowania items."""
        return len(self.items) if self.items else 0

    def validate(self) -> list[str]:
        errors: list[str] = []

        if self.folder_type not in VALID_FOLDER_TYPES:
            errors.append(f"folder_type='{self.folder_type}' nieprawidlowy")

        if self.owner_user is None and self.owner_group is None:
            errors.append("Teczka musi miec wlasciciela: owner_user lub owner_group")

        if self.owner_user is not None and self.owner_group is not None:
            errors.append("Teczka moze miec tylko jednego wlasciciela (user LUB group)")

        if self.folder_type == "private" and self.owner_group is not None:
            errors.append("Teczka prywatna musi miec owner_user, nie owner_group")

        if self.folder_type == "team" and self.owner_user is not None:
            errors.append("Teczka zespolowa musi miec owner_group, nie owner_user")

        if self.color is not None and not _HEX_COLOR_RE.match(self.color):
            errors.append(f"color='{self.color}' nieprawidlowy. Wymagany format: #RRGGBB")

        if not self.folder_name or not self.folder_name.strip():
            errors.append("folder_name nie moze byc pusty")

        return errors

    def __repr__(self) -> str:
        owner = f"user={self.owner_user}" if self.owner_user else f"group={self.owner_group}"
        return (
            f"<DocumentFolder id={self.id_folder} name={self.folder_name!r} "
            f"type={self.folder_type!r} {owner} active={self.is_active}>"
        )