# backend/app/db/models/approval/source_action_log.py
"""
Model ORM — dbo.skw_source_action_log

Log wywolan hookow i akcji zrodlowych.

KLUCZOWY do diagnostyki: kazde wywolanie — niezaleznie od wyniku —
trafia do tej tabeli. Przy hooku krytycznym ktory zablokuje akceptacje,
ten log jest jedynym miejscem gdzie widac co sie stalo.

id_hook / id_action:
  Dokladnie jedno z nich musi byc NOT NULL (CHECK constraint w DB).
  id_hook  -> wywolanie przez HookService (automatyczne po akcji obiegowej)
  id_action -> wywolanie przez ActionService (reczna akcja uzytkownika)

id_user NULL = wywolanie systemowe (hook po akceptacji, bez inicjatora).

request_payload / response_payload:
  Surowe dane diagnostyczne — moga byc duze JSON-y.
  Logowanie kontrolowane przez HOOK_LOG_REQUEST_PAYLOAD / HOOK_LOG_RESPONSE_PAYLOAD
  w skw_SystemConfig (domyslnie true, mozna wylaczyd dla zrodel z danymi wrazliwymi).

UWAGA: from __future__ import annotations — NIGDY w tym pliku.
"""

import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, ForeignKey, Identity, Integer, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.source_hook import SourceHook
    from app.db.models.approval.source_action import SourceAction
    from app.db.models.approval.document_approval_instance import DocumentApprovalInstance
    from app.db.models.user import User

logger = logging.getLogger(__name__)

SCHEMA = "dbo"

VALID_STATUSES = frozenset({"success", "error", "warning"})


class SourceActionLog(Base):
    """
    Log wywolan hookow i akcji zrodlowych.

    Tabela: dbo.skw_source_action_log
    Append-only z perspektywy biznesowej — nie edytujemy rekordow po zapisie.
    (Brak triggera DENY jak w approval_log — HookService moze potrzebowac
    aktualizacji execution_ms po zakonczeniu operacji asynchronicznej.)
    """

    __tablename__ = "skw_source_action_log"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Log wywolan hookow i akcji zrodlowych — diagnostyka i audyt",
    }

    id_log: Mapped[int] = mapped_column(
        "id_log", Integer, Identity(start=1, increment=1), primary_key=True,
    )
    id_hook: Mapped[int | None] = mapped_column(
        "id_hook",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_source_hooks.id_hook",
            ondelete="NO ACTION",
        ),
        nullable=True,
        index=True,
        comment="FK do hooka (NULL jesli to wywolanie akcji recznej)",
    )
    id_action: Mapped[int | None] = mapped_column(
        "id_action",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_source_actions.id_action",
            ondelete="NO ACTION",
        ),
        nullable=True,
        index=True,
        comment="FK do akcji (NULL jesli to wywolanie hooka)",
    )
    id_instance: Mapped[int] = mapped_column(
        "id_instance",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_document_approval_instances.id_instance",
            ondelete="NO ACTION",
        ),
        nullable=False,
        index=True,
        comment="FK do instancji obiegu — glowny klucz diagnostyczny",
    )
    id_user: Mapped[int | None] = mapped_column(
        "id_user",
        Integer,
        ForeignKey(
            f"{SCHEMA}.skw_Users.ID_USER",
            ondelete="SET NULL",
        ),
        nullable=True,
        comment="Kto wywolal akcje (NULL = wywolanie systemowe przez hook)",
    )
    executed_at: Mapped[datetime] = mapped_column(
        "executed_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), default=_utcnow,
        comment="Timestamp wywolania (UTC)",
    )
    status: Mapped[str] = mapped_column(
        "status", String(20), nullable=False,
        comment="success | error | warning",
    )
    message: Mapped[str | None] = mapped_column(
        "message", String(500), nullable=True,
        comment="Komunikat zwrocony przez zewnetrzny system (dla uzytkownika)",
    )
    execution_ms: Mapped[int | None] = mapped_column(
        "execution_ms", Integer, nullable=True,
        comment="Czas wykonania w milisekundach",
    )
    request_payload: Mapped[str | None] = mapped_column(
        "request_payload", Text, nullable=True,
        comment="Dane wysylane do zewnetrznego systemu (diagnostyka)",
    )
    response_payload: Mapped[str | None] = mapped_column(
        "response_payload", Text, nullable=True,
        comment="Dane otrzymane od zewnetrznego systemu (diagnostyka)",
    )

    # ── Relacje ───────────────────────────────────────────────────────────────

    hook: Mapped["SourceHook | None"] = relationship(
        "SourceHook",
        primaryjoin="SourceActionLog.id_hook == SourceHook.id_hook",
        foreign_keys=[id_hook],
        back_populates="logs",
        lazy="noload",
    )
    action: Mapped["SourceAction | None"] = relationship(
        "SourceAction",
        primaryjoin="SourceActionLog.id_action == SourceAction.id_action",
        foreign_keys=[id_action],
        back_populates="logs",
        lazy="noload",
    )
    instance: Mapped["DocumentApprovalInstance"] = relationship(
        "DocumentApprovalInstance",
        foreign_keys=[id_instance],
        lazy="noload",
    )
    user: Mapped["User | None"] = relationship(
        "User",
        foreign_keys=[id_user],
        lazy="noload",
    )

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def is_hook_call(self) -> bool:
        """True jesli wpis pochodzi z wywolania hooka (automatycznego)."""
        return self.id_hook is not None

    @property
    def is_action_call(self) -> bool:
        """True jesli wpis pochodzi z recznej akcji uzytkownika."""
        return self.id_action is not None

    @property
    def succeeded(self) -> bool:
        return self.status == "success"

    @property
    def failed(self) -> bool:
        return self.status == "error"

    def get_request_payload(self) -> dict[str, Any] | None:
        """Parsuje request_payload jako slownik. None jesli brak lub blad."""
        if not self.request_payload:
            return None
        try:
            return json.loads(self.request_payload)
        except json.JSONDecodeError:
            return {"raw": self.request_payload}

    def get_response_payload(self) -> dict[str, Any] | None:
        """Parsuje response_payload jako slownik. None jesli brak lub blad."""
        if not self.response_payload:
            return None
        try:
            return json.loads(self.response_payload)
        except json.JSONDecodeError:
            return {"raw": self.response_payload}

    def set_request_payload(self, payload: dict[str, Any]) -> None:
        self.request_payload = json.dumps(payload, ensure_ascii=False, default=str)

    def set_response_payload(self, payload: dict[str, Any]) -> None:
        self.response_payload = json.dumps(payload, ensure_ascii=False, default=str)

    def __repr__(self) -> str:
        src = f"hook={self.id_hook}" if self.is_hook_call else f"action={self.id_action}"
        return (
            f"<SourceActionLog id={self.id_log} {src} "
            f"instance={self.id_instance} status={self.status!r} "
            f"ms={self.execution_ms}>"
        )