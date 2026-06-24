# backend/app/db/models/approval/document_source.py
"""
Model ORM — dbo.skw_document_sources

Slownik zrodel dokumentow z pelna konfiguracja Etapu 2.

ETAP 2 — nowe kolumny (migracja 0039):
  source_type          — database | api | ftp | email | manual | ksef20
  connection_mode      — pull | push
  connection_config    — JSON szyfrowany Fernet (hasla, tokeny, certyfikaty)
  sync_interval_minutes
  last_sync_at / last_sync_status / last_sync_message
  is_test_mode         — nowe zrodla startuja zawsze w trybie testowym
  webhook_token        — UNIQUE, tylko dla connection_mode=push
  updated_at

Properties:
  get_config() / set_config()  — transparentne szyfrowanie/deszyfrowanie
  is_webhook                   — True gdy connection_mode == 'push'
  needs_sync                   — True gdy pull i minal sync_interval_minutes

UWAGA: from __future__ import annotations — NIGDY w tym pliku.
SQLAlchemy lazy-eval typu Mapped[] wymaga resolved annotations.
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import Boolean, DateTime, Identity, Integer, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, _utcnow

if TYPE_CHECKING:
    from app.db.models.approval.document_approval_instance import DocumentApprovalInstance
    from app.db.models.approval.approval_filter import ApprovalFilter
    from app.db.models.approval.document_source_field_mapping import DocumentSourceFieldMapping
    from app.db.models.approval.source_hook import SourceHook
    from app.db.models.approval.source_action import SourceAction

logger = logging.getLogger(__name__)

SCHEMA = "dbo"

# Dozwolone wartosci — zsynchronizowane z CHECK constraintami w DB (migracja 0039)
SOURCE_TYPES    = frozenset({"database", "api", "ftp", "email", "manual", "ksef20"})
CONNECTION_MODES = frozenset({"pull", "push"})
SYNC_STATUSES   = frozenset({"ok", "error", "partial"})


class DocumentSource(Base):
    """
    Zrodlo dokumentow wchodzacych do obiegu akceptacji.

    Tabela: dbo.skw_document_sources

    Seed (migracja 0028 krok27):
      'fakir' — BUF_DOKUMENT z systemu Fakir/WAPRO
      'ksef'  — faktury z Krajowego Systemu e-Faktur

    connection_config przechowuje JSON zaszyfrowany Fernet.
    Uzyj get_config() / set_config() — nigdy nie czytaj surowego pola.
    """

    __tablename__ = "skw_document_sources"
    __table_args__ = {
        "schema": SCHEMA,
        "comment": "Slownik zrodel dokumentow wchodzacych do obiegu (Etap 2)",
    }

    # ── Kolumny istniejace (sprzed Etapu 2) ──────────────────────────────────

    id_source: Mapped[int] = mapped_column(
        "id_source", Integer, Identity(start=1, increment=1), primary_key=True,
    )
    source_name: Mapped[str] = mapped_column(
        "source_name", String(50), nullable=False, unique=True,
        comment="Krotka nazwa: fakir / ksef / manual / <custom>",
    )
    description: Mapped[str | None] = mapped_column(
        "description", String(200), nullable=True,
    )
    is_active: Mapped[bool] = mapped_column(
        "is_active", Boolean, nullable=False,
        server_default=text("1"), default=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        "created_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), default=_utcnow,
    )

    # ── Kolumny nowe (migracja 0039 — Etap 2) ────────────────────────────────

    source_type: Mapped[str] = mapped_column(
        "source_type", String(20), nullable=False,
        server_default=text("N'database'"), default="database",
        comment="Typ polaczenia: database | api | ftp | email | manual | ksef20",
    )
    connection_mode: Mapped[str] = mapped_column(
        "connection_mode", String(10), nullable=False,
        server_default=text("N'pull'"), default="pull",
        comment="pull = worker cykliczny | push = webhook",
    )
    connection_config: Mapped[str | None] = mapped_column(
        "connection_config", Text, nullable=True,
        comment="JSON zaszyfrowany Fernet — dane polaczenia i dane wrazliwe",
    )
    sync_interval_minutes: Mapped[int] = mapped_column(
        "sync_interval_minutes", Integer, nullable=False,
        server_default=text("15"), default=15,
        comment="Co ile minut worker sprawdza to zrodlo (pull only)",
    )
    last_sync_at: Mapped[datetime | None] = mapped_column(
        "last_sync_at", DateTime, nullable=True,
        comment="Timestamp ostatniej synchronizacji (UTC)",
    )
    last_sync_status: Mapped[str | None] = mapped_column(
        "last_sync_status", String(20), nullable=True,
        comment="ok | error | partial",
    )
    last_sync_message: Mapped[str | None] = mapped_column(
        "last_sync_message", String(500), nullable=True,
        comment="Komunikat ostatniej synchronizacji (skrocony, max 500 zn.)",
    )
    is_test_mode: Mapped[bool] = mapped_column(
        "is_test_mode", Boolean, nullable=False,
        server_default=text("1"), default=True,
        comment="Tryb testowy: dokumenty nie wchodza do rzeczywistego obiegu",
    )
    webhook_token: Mapped[str | None] = mapped_column(
        "webhook_token", String(100), nullable=True, unique=True,
        comment="Token URL webhookowego endpointu (NULL gdy connection_mode=pull)",
    )
    updated_at: Mapped[datetime] = mapped_column(
        "updated_at", DateTime, nullable=False,
        server_default=text("SYSUTCDATETIME()"), default=_utcnow, onupdate=_utcnow,
    )

    # ── Relacje ───────────────────────────────────────────────────────────────

    instances: Mapped[list["DocumentApprovalInstance"]] = relationship(
        "DocumentApprovalInstance", back_populates="source", lazy="noload",
    )
    filters: Mapped[list["ApprovalFilter"]] = relationship(
        "ApprovalFilter", back_populates="source", lazy="noload",
    )
    field_mappings: Mapped[list["DocumentSourceFieldMapping"]] = relationship(
        "DocumentSourceFieldMapping",
        back_populates="source",
        cascade="all, delete-orphan",
        lazy="noload",
    )
    hooks: Mapped[list["SourceHook"]] = relationship(
        "SourceHook",
        back_populates="source",
        cascade="all, delete-orphan",
        lazy="noload",
        order_by="SourceHook.id_hook",
    )
    actions: Mapped[list["SourceAction"]] = relationship(
        "SourceAction",
        back_populates="source",
        cascade="all, delete-orphan",
        lazy="noload",
        order_by="SourceAction.sort_order",
    )

    # ── Properties — connection_config (szyfrowanie transparentne) ───────────

    def get_config(self) -> dict[str, Any]:
        """
        Zwraca odszyfrowany connection_config jako slownik.

        Jesli connection_config jest NULL lub pusty — zwraca pusty dict.
        Jesli deszyfrowanie sie nie powiedzie — rzuca ValueError
        (nie logujemy samego bledu zeby nie wyciec danych).

        Uzycie:
            cfg = source.get_config()
            view_name = cfg.get("view_name")
        """
        if not self.connection_config:
            return {}

        try:
            from app.core.encryption import decrypt_value
            raw = decrypt_value(self.connection_config)
            return json.loads(raw)
        except Exception as exc:
            logger.error(
                "Nie mozna odszyfrować connection_config dla zrodla id=%s name=%r: %s",
                self.id_source, self.source_name, type(exc).__name__,
            )
            raise ValueError(
                f"Blad deszyfrowania connection_config dla zrodla '{self.source_name}'"
            ) from exc

    def set_config(self, config: dict[str, Any]) -> None:
        """
        Szyfruje i zapisuje connection_config.

        Uzycie:
            source.set_config({"view_name": "skw_faktury", "id_column": "KSEF_ID"})
            db.add(source)
            await db.commit()
        """
        try:
            from app.core.encryption import encrypt_value
            raw = json.dumps(config, ensure_ascii=False)
            self.connection_config = encrypt_value(raw)
        except Exception as exc:
            logger.error(
                "Nie mozna zaszyfrować connection_config dla zrodla id=%s: %s",
                self.id_source, type(exc).__name__,
            )
            raise ValueError("Blad szyfrowania connection_config") from exc

    def get_config_safe(self) -> dict[str, Any]:
        """
        Zwraca odszyfrowany config z wymazanymi polami wrazliwymi.
        Bezpieczne do zwrocenia w API (np. GET /sources/{id}).

        Pola wymazywane: password, key_content, cert_content, api_key,
        auth_config, key_password, connection_string (zawiera haslo).
        """
        try:
            cfg = self.get_config()
        except ValueError:
            return {}

        SENSITIVE = frozenset({
            "password", "key_content", "cert_content", "api_key",
            "auth_config", "key_password", "connection_string",
        })
        return {
            k: ("***" if k in SENSITIVE else v)
            for k, v in cfg.items()
        }

    # ── Properties — logika biznesowa ────────────────────────────────────────

    @property
    def is_webhook(self) -> bool:
        """True gdy zrodlo odbiera dokumenty przez webhook (connection_mode=push)."""
        return self.connection_mode == "push"

    @property
    def needs_sync(self) -> bool:
        """
        True gdy worker synchronizacji powinien teraz przetworzyc to zrodlo.

        Warunki:
          - connection_mode == 'pull' (push = webhook, nie cykliczny)
          - is_active == True
          - last_sync_at jest NULL (nigdy nie synchronizowane)
            LUB minal sync_interval_minutes od last_sync_at

        Uzywane przez source_sync_task przy filtrowaniu zrodel do przetworzenia.
        """
        if self.connection_mode != "pull":
            return False
        if not self.is_active:
            return False
        if self.last_sync_at is None:
            return True

        now_utc = datetime.now(timezone.utc)
        # last_sync_at moze byc naive (MSSQL DATETIME2 bez TZ) — normalizujemy
        last = self.last_sync_at
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)

        elapsed_minutes = (now_utc - last).total_seconds() / 60
        return elapsed_minutes >= self.sync_interval_minutes

    @property
    def sync_status_ok(self) -> bool:
        """True jesli ostatnia synchronizacja zakonczyla sie sukcesem."""
        return self.last_sync_status == "ok"

    def mark_sync_started(self) -> None:
        """Oznacza poczatek synchronizacji — resetuje status na None."""
        self.last_sync_status = None
        self.last_sync_message = None

    def mark_sync_success(self, message: str | None = None) -> None:
        """Oznacza zakonczenie synchronizacji sukcesem."""
        self.last_sync_at = datetime.now(timezone.utc)
        self.last_sync_status = "ok"
        self.last_sync_message = (message or "")[:500]

    def mark_sync_error(self, message: str) -> None:
        """Oznacza blad synchronizacji. Zachowuje last_sync_at z poprzedniej."""
        self.last_sync_at = datetime.now(timezone.utc)
        self.last_sync_status = "error"
        self.last_sync_message = str(message)[:500]

    def mark_sync_partial(self, message: str) -> None:
        """Oznacza czesciowy sukces synchronizacji (np. niektore dokumenty pominiete)."""
        self.last_sync_at = datetime.now(timezone.utc)
        self.last_sync_status = "partial"
        self.last_sync_message = str(message)[:500]

    # ── Walidacja ─────────────────────────────────────────────────────────────

    def validate(self) -> list[str]:
        """
        Waliduje obiekt przed zapisem do bazy.
        Zwraca liste bledow (pusty list = OK).

        Wywolywana przez serwisy przed db.add() / db.flush().
        """
        errors: list[str] = []

        if self.source_type not in SOURCE_TYPES:
            errors.append(
                f"source_type='{self.source_type}' jest nieprawidlowy. "
                f"Dozwolone: {sorted(SOURCE_TYPES)}"
            )

        if self.connection_mode not in CONNECTION_MODES:
            errors.append(
                f"connection_mode='{self.connection_mode}' jest nieprawidlowy. "
                f"Dozwolone: {sorted(CONNECTION_MODES)}"
            )

        if self.connection_mode == "push" and not self.webhook_token:
            errors.append(
                "webhook_token jest wymagany gdy connection_mode='push'"
            )

        if self.connection_mode == "pull" and self.webhook_token:
            errors.append(
                "webhook_token powinien byc NULL gdy connection_mode='pull'"
            )

        if self.sync_interval_minutes < 1:
            errors.append("sync_interval_minutes musi byc >= 1")

        if self.last_sync_status and self.last_sync_status not in SYNC_STATUSES:
            errors.append(
                f"last_sync_status='{self.last_sync_status}' nieprawidlowy. "
                f"Dozwolone: {sorted(SYNC_STATUSES)}"
            )

        return errors

    # ── Reprezentacja ─────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        return (
            f"<DocumentSource id={self.id_source} name={self.source_name!r} "
            f"type={self.source_type!r} mode={self.connection_mode!r} "
            f"active={self.is_active} test={self.is_test_mode}>"
        )