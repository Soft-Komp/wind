# backend/app/db/models/approval/source_hook.py
"""
Model ORM — dbo.skw_source_hooks

Hooki wykonywane automatycznie po akcjach obiegowych (accepted / rejected).

DECYZJA D-E02:
  trigger_action TYLKO 'accepted' i 'rejected'.
  Zewnetrzne systemy nie rozumieja semantyki akcji posrednich
  (rollback, forwarded) — CHECK constraint egzekwuje to na poziomie DB.

severity:
  critical      — blad hooka = rollback calej akcji obiegowej
  informational — blad hooka = log + ostrzezenie, akcja przechodzi

Unikalnosc: max 1 aktywny hook per (id_source, trigger_action).
Zrealizowane przez UNIQUE filtrowany indeks w DB (WHERE is_active = 1).

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

VALID_TRIGGER_ACTIONS = frozenset({"accepted", "rejected"})
VALID_OPERATION_TYPES  = frozenset({"sql_procedure", "api_call"})
VALID_SEVERITIES       = frozenset({"critical", "informational"})

# Domyslny timeout hooka w sekundach (konfigurowalny per hook w operation_config)
DEFAULT_HOOK_TIMEOUT_SECONDS = 30
MIN_HOOK_TIMEOUT_SECONDS     = 5
MAX_HOOK_TIMEOUT_SECONDS     = 120


class SourceHook(Base):
    """
    Hook po akcji obiegowej dla zrodla.

    Tabela: dbo.skw_source_hooks
    CASCADE DELETE z DocumentSource.

    Przyklady uzytku:
      - Hook 'accepted' severity='critical' type='sql_procedure'
        -> wywolyje procedure dbo.skw_AktualizujStatusFaktury(@ksef_id, @action)
        -> blad = rollback akceptacji w silniku obiegu
      - Hook 'rejected' severity='informational' type='api_call'
        -> powiadamia zewnetrzny system przez REST API
        -> blad = log + ostrzezenie, odrzucenie przechodzi
    """

    __tablename__ = "skw_source_hooks"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Hooki po akcjach obiegowych (accepted/rejected) dla zrodel",
    }

    id_hook: Mapped[int] = mapped_column(
        "id_hook", Integer, Identity(start=1, increment=1), primary_key=True,
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
    trigger_action: Mapped[str] = mapped_column(
        "trigger_action", String(30), nullable=False,
        comment="Akcja wyzwalajaca: accepted | rejected",
    )
    operation_type: Mapped[str] = mapped_column(
        "operation_type", String(20), nullable=False,
        comment="sql_procedure | api_call",
    )
    operation_config: Mapped[str | None] = mapped_column(
        "operation_config", Text, nullable=True,
        comment="JSON z parametrami operacji i placeholderami ({id_instance}, {ksef_id} itp.)",
    )
    severity: Mapped[str] = mapped_column(
        "severity", String(20), nullable=False,
        comment="critical = rollback na blad | informational = log i kontynuuj",
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

    source: Mapped["DocumentSource"] = relationship(
        "DocumentSource", back_populates="hooks", lazy="noload",
    )
    logs: Mapped[list["SourceActionLog"]] = relationship(
        "SourceActionLog",
        primaryjoin="SourceActionLog.id_hook == SourceHook.id_hook",
        foreign_keys="SourceActionLog.id_hook",
        back_populates="hook",
        lazy="noload",
    )

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def is_critical(self) -> bool:
        """True jesli blad hooka powoduje rollback akcji obiegowej."""
        return self.severity == "critical"

    @property
    def is_informational(self) -> bool:
        """True jesli blad hooka jest tylko logowany (akcja przechodzi)."""
        return self.severity == "informational"

    def get_operation_config(self) -> dict[str, Any]:
        """
        Zwraca operation_config jako slownik.
        Pusty dict jesli NULL lub niepoprawny JSON.
        """
        if not self.operation_config:
            return {}
        try:
            return json.loads(self.operation_config)
        except json.JSONDecodeError:
            logger.warning(
                "Niepoprawny JSON w operation_config hooka id=%s", self.id_hook
            )
            return {}

    def get_timeout_seconds(self) -> int:
        """
        Zwraca timeout hooka w sekundach.
        Konfigurowalny per hook przez klucz 'timeout_seconds' w operation_config.
        Min: 5s, Max: 120s, Default: 30s.
        """
        cfg = self.get_operation_config()
        raw = cfg.get("timeout_seconds", DEFAULT_HOOK_TIMEOUT_SECONDS)
        try:
            val = int(raw)
        except (TypeError, ValueError):
            val = DEFAULT_HOOK_TIMEOUT_SECONDS
        return max(MIN_HOOK_TIMEOUT_SECONDS, min(MAX_HOOK_TIMEOUT_SECONDS, val))

    def set_operation_config(self, config: dict[str, Any]) -> None:
        """Zapisuje operation_config jako JSON string."""
        self.operation_config = json.dumps(config, ensure_ascii=False)

    def validate(self) -> list[str]:
        """Waliduje obiekt. Zwraca liste bledow."""
        errors: list[str] = []

        if self.trigger_action not in VALID_TRIGGER_ACTIONS:
            errors.append(
                f"trigger_action='{self.trigger_action}' nieprawidlowy. "
                f"Dozwolone: {sorted(VALID_TRIGGER_ACTIONS)}"
            )

        if self.operation_type not in VALID_OPERATION_TYPES:
            errors.append(
                f"operation_type='{self.operation_type}' nieprawidlowy. "
                f"Dozwolone: {sorted(VALID_OPERATION_TYPES)}"
            )

        if self.severity not in VALID_SEVERITIES:
            errors.append(
                f"severity='{self.severity}' nieprawidlowy. "
                f"Dozwolone: {sorted(VALID_SEVERITIES)}"
            )

        return errors

    def __repr__(self) -> str:
        return (
            f"<SourceHook id={self.id_hook} source={self.id_source} "
            f"action={self.trigger_action!r} severity={self.severity!r} "
            f"type={self.operation_type!r} active={self.is_active}>"
        )