# backend/app/db/models/approval/document_folder_group.py
"""
Model ORM — dbo.skw_document_folder_groups

Dodatkowa widocznosc teczki dla grup spoza jej wlasciciela (owner_user/
owner_group na DocumentFolder pozostaje bez zmian — to nadal jeden,
jednoznaczny tworca/wlasciciel). Tabela PLASKA — brak kolumny poziomu
dostepu; prawo modyfikacji zawartosci teczki dla grup przypisanych tutaj
jest kontrolowane WYLACZNIE przez uprawnienie documents.assign_shared_folder,
sprawdzane w warstwie serwisu/routera, nie na tym modelu.

Wprowadzone migracja 0074, na wniosek frontu (05.08.2026).

NAPRAWA (06.08.2026, incydent request_id=f7ce8515-7209-462a-94a1-8bac8a56e7a3):
kolumna removed_at zostala dodana do bazy migracja 0076 (soft-delete przy
usunieciu przypisania grupy, patrz docstring document_folder_service.py),
ale NIGDY nie zostala dopisana do tej klasy ORM — SQLAlchemy nie mial
zadnej wiedzy o jej istnieniu, co dawalo AttributeError przy kazdym
wywolaniu remove_group_from_folder(). Ten plik byl opisany jako "do
zmiany" w PATCH_document_folder_service.py, ale nigdy nie zostal
faktycznie wygenerowany jako kompletny plik — dokladnie ten blad, przed
ktorym mial chronic standing rule "kazda zmiana kodu przez create_file".

UWAGA: from __future__ import annotations — NIGDY w tym pliku.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Identity, Integer, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.user import User
    from app.db.models.approval.approval_group import ApprovalGroup
    from app.db.models.approval.document_folder import DocumentFolder

SCHEMA = "dbo"


class DocumentFolderGroup(Base):
    """
    Przypisanie grupy jako dodatkowego, uprawnionego do widoku "goscia"
    teczki (poza jej wlascicielem).

    Tabela: dbo.skw_document_folder_groups
    """

    __tablename__ = "skw_document_folder_groups"
    __table_args__ = (
        UniqueConstraint("id_folder", "id_group", name="UQ_skw_document_folder_groups_folder_group"),
        {
            "schema": SCHEMA,
            "comment": "Dodatkowa widocznosc teczki dla grup spoza jej wlasciciela",
        },
    )

    id_folder_group: Mapped[int] = mapped_column(
        "id_folder_group", Integer, Identity(start=1, increment=1), primary_key=True,
    )
    id_folder: Mapped[int] = mapped_column(
        "id_folder",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_document_folders.id_folder", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    id_group: Mapped[int] = mapped_column(
        "id_group",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_approval_groups.id_group", ondelete="NO ACTION"),
        nullable=False,
        index=True,
    )
    added_by: Mapped[int | None] = mapped_column(
        "added_by",
        Integer,
        ForeignKey(f"{SCHEMA}.skw_Users.ID_USER", ondelete="NO ACTION"),
        nullable=True,
        comment="Kto przypisal te grupe do teczki (audyt)",
    )
    added_at: Mapped[datetime] = mapped_column(
        "added_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), default=_utcnow,
    )
    # NOWE (0076, ZLECENIE FRONTU 06.08.2026 — "Opcja A" retroaktywnosc):
    # NULL = przypisanie aktywne. Wypelnione = "usuniete", ale wiersz
    # ZOSTAJE w tabeli (soft-delete) — pozwala bylym czlonkom zachowac
    # (docelowo, przez jeszcze niezaimplementowany endpoint historyczny)
    # wglad w zamrozona zawartosc teczki z tego momentu, patrz
    # skw_document_folder_snapshots (migracja 0076).
    removed_at: Mapped[datetime | None] = mapped_column(
        "removed_at", DateTime, nullable=True,
    )

    # ── Relacje ───────────────────────────────────────────────────────────────

    folder: Mapped["DocumentFolder"] = relationship(
        "DocumentFolder",
        foreign_keys=[id_folder],
        lazy="noload",
    )
    group: Mapped["ApprovalGroup"] = relationship(
        "ApprovalGroup",
        foreign_keys=[id_group],
        lazy="noload",
    )
    added_by_user: Mapped["User | None"] = relationship(
        "User",
        foreign_keys=[added_by],
        lazy="noload",
    )

    @property
    def is_active(self) -> bool:
        """True jesli przypisanie nie zostalo (soft-)usuniete."""
        return self.removed_at is None

    def __repr__(self) -> str:
        status = "active" if self.removed_at is None else f"removed@{self.removed_at}"
        return (
            f"<DocumentFolderGroup id={self.id_folder_group} "
            f"folder={self.id_folder} group={self.id_group} {status}>"
        )