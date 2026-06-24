# backend/app/db/models/approval/source_action.py
"""
Model ORM — dbo.skw_source_actions

Akcje zrodlowe — dodatkowe przyciski kontekstowe dla dokumentu z danego zrodla.
NIE przesuwaja dokumentu po obiegu — sa zewnetrzna operacja.

is_predefined:
  True  — predeklarowany "klocek" aktywowany przez admina
  False — w pelni niestandardowa akcja skonfigurowana recznie

Wynik wykonania akcji zapisywany do skw_source_action_log.
Frontend obsluguje wynik przez generyczny komponent niezaleznie od zrodla.

UWAGA: from __future__ import annotations — NIGDY w tym pliku.
"""

import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import Boolean, DateTime, ForeignKey, Identity, Integer, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.document_source import DocumentSource
    from app.db.models.approval.source_action_log import SourceActionLog

logger = logging.getLogger(__name__)

SCHEMA = "dbo"

VALID_OPERATION_TYPES = frozenset({
    "sql_procedure", "api_call", "file_move", "file_delete",
})


class SourceAction(Base):
    """
    Akcja zrodlowa (przycisk kontekstowy dla dokumentu).

    Tabela: dbo.skw_source_actions
    CASCADE DELETE z DocumentSource.
    Posortowane po sort_order (ASC) — kolejnosc w UI.
    """

    __tablename__ = "skw_source_actions"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Akcje zrodlowe — dodatkowe operacje na dokumentach z danego zrodla",
    }

    id_action: Mapped[int] = mapped_column(
        "id_action", Integer, Identity(start=1, increment=1), primary_key=True,
    )
    id_source: Mapped[int] = mapped_column(
        "id_source",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_document_sources.id_source",
            ondelete="CASCADE",
        ),
        nullable=False,
        index=True,
        comment="FK do zrodla — CASCADE DELETE",
    )
    action_name: Mapped[str] = mapped_column(
        "action_name", String(100), nullable=False,
        comment="Nazwa techniczna: snake_case, unikalna per zrodlo (preferowany)",
    )
    action_label: Mapped[str] = mapped_column(
        "action_label", String(200), nullable=False,
        comment="Etykieta dla uzytkownika (wyswietlana w UI)",
    )
    operation_type: Mapped[str] = mapped_column(
        "operation_type", String(20), nullable=False,
        comment="sql_procedure | api_call | file_move | file_delete",
    )
    operation_config: Mapped[str | None] = mapped_column(
        "operation_config", Text, nullable=True,
        comment="JSON z parametrami operacji i placeholderami",
    )
    required_permission: Mapped[str | None] = mapped_column(
        "required_permission", String(100), nullable=True,
        comment="Uprawnienie wymagane do wykonania akcji (NULL = sources.execute_action wystarczy)",
    )
    is_predefined: Mapped[bool] = mapped_column(
        "is_predefined", Boolean, nullable=False,
        server_default=text("0"), default=False,
        comment="True = predeklarowany klocek | False = niestandardowa akcja",
    )
    is_active: Mapped[bool] = mapped_column(
        "is_active", Boolean, nullable=False,
        server_default=text("1"), default=True,
    )
    sort_order: Mapped[int] = mapped_column(
        "sort_order", Integer, nullable=False,
        server_default=text("0"), default=0,
        comment="Kolejnosc wyswietlania w UI (ASC)",
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

    source: Mapped["DocumentSource"] = relationship(
        "DocumentSource", back_populates="actions", lazy="noload",
    )
    logs: Mapped[list["SourceActionLog"]] = relationship(
        "SourceActionLog",
        primaryjoin="SourceActionLog.id_action == SourceAction.id_action",
        foreign_keys="SourceActionLog.id_action",
        back_populates="action",
        lazy="noload",
    )

    # ── Properties ────────────────────────────────────────────────────────────

    def get_operation_config(self) -> dict[str, Any]:
        """Zwraca operation_config jako slownik. Pusty dict jesli NULL/blad."""
        if not self.operation_config:
            return {}
        try:
            return json.loads(self.operation_config)
        except json.JSONDecodeError:
            logger.warning(
                "Niepoprawny JSON w operation_config akcji id=%s", self.id_action
            )
            return {}

    def set_operation_config(self, config: dict[str, Any]) -> None:
        """Zapisuje operation_config jako JSON string."""
        self.operation_config = json.dumps(config, ensure_ascii=False)

    def validate(self) -> list[str]:
        """Waliduje obiekt. Zwraca liste bledow."""
        errors: list[str] = []

        if self.operation_type not in VALID_OPERATION_TYPES:
            errors.append(
                f"operation_type='{self.operation_type}' nieprawidlowy. "
                f"Dozwolone: {sorted(VALID_OPERATION_TYPES)}"
            )

        if not self.action_name or not self.action_name.strip():
            errors.append("action_name nie moze byc pusty")

        if not self.action_label or not self.action_label.strip():
            errors.append("action_label nie moze byc pusty")

        return errors

    def __repr__(self) -> str:
        return (
            f"<SourceAction id={self.id_action} source={self.id_source} "
            f"name={self.action_name!r} type={self.operation_type!r} "
            f"active={self.is_active} order={self.sort_order}>"
        )