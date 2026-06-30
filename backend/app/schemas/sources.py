# backend/app/schemas/sources.py
"""
Schematy Pydantic dla F6 — panel administracyjny zrodel dokumentow.

Pokrywa:
  SourceCreate / SourceUpdate / SourceOut       — CRUD /sources
  SourceTestConnectionResult                    — POST /sources/{id}/test-connection
  SourceSyncTriggerResult                       — POST /sources/{id}/sync
  SourceSyncStatusOut                           — GET /sources/{id}/sync-status
  SourceHealthOut                               — GET /sources/health
  HookCreate / HookUpdate / HookOut             — CRUD /sources/{id}/hooks
  ActionCreate / ActionUpdate / ActionOut       — CRUD /sources/{id}/actions
  WebhookTokenCreatedOut                        — token webhooka (pokazany 1x)

UWAGA: from __future__ import annotations — OK w plikach schemas (Pydantic v2
rozwiazuje stringi przy starcie aplikacji, nie przy imporcie modulu ORM).
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# =============================================================================
# Stale walidacyjne — zsynchronizowane z CHECK constraintami w DB (migracja 0039)
# =============================================================================

SOURCE_TYPES     = ("database", "api", "ftp", "email", "manual", "ksef20")
CONNECTION_MODES = ("pull", "push")
SYNC_STATUSES    = ("ok", "error", "partial")
TRIGGER_ACTIONS  = ("accepted", "rejected")
OPERATION_TYPES  = ("sql_procedure", "api_call")
ACTION_OPERATION_TYPES = ("sql_procedure", "api_call", "file_move", "file_delete")
SEVERITIES       = ("critical", "informational")

SourceType      = Literal["database", "api", "ftp", "email", "manual", "ksef20"]
ConnectionMode  = Literal["pull", "push"]
TriggerAction   = Literal["accepted", "rejected"]
OperationType   = Literal["sql_procedure", "api_call"]
ActionOperationType = Literal["sql_procedure", "api_call", "file_move", "file_delete"]
Severity        = Literal["critical", "informational"]


# =============================================================================
# SOURCES — CRUD
# =============================================================================

class SourceCreate(BaseModel):
    """
    Body dla POST /sources.

    connection_config przyjmowany jako plaintext dict — backend szyfruje
    przed zapisem (Fernet, app.core.encryption.encrypt_value).
    Nigdy nie loguj tego pola w pelnej postaci.

    Nowe zrodlo zawsze startuje z is_test_mode=True (decyzja bezpieczenstwa) —
    operator musi recznie przelaczyc na produkcyjny po weryfikacji.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    source_name: str = Field(
        ..., min_length=2, max_length=100,
        description="Unikalna nazwa techniczna zrodla (np. 'fakir', 'ksef20_prod').",
    )
    source_type: SourceType = Field(
        ..., description="Typ zrodla: database | api | ftp | email | manual | ksef20.",
    )
    connection_mode: ConnectionMode = Field(
        ..., description="pull = system odpytuje cyklicznie, push = zrodlo wysyla webhook.",
    )
    connection_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Konfiguracja polaczenia (hasla, tokeny, certyfikaty). Szyfrowana przed zapisem.",
    )
    sync_interval_minutes: Optional[int] = Field(
        default=15, ge=1, le=1440,
        description="Interwal synchronizacji w minutach (tylko connection_mode=pull).",
    )
    is_active: bool = Field(default=True, description="Czy zrodlo jest aktywne.")

    @field_validator("source_name")
    @classmethod
    def _validate_source_name(cls, v: str) -> str:
        import re
        if not re.match(r"^[a-z][a-z0-9_]{1,99}$", v):
            raise ValueError(
                "source_name musi zaczynac sie od litery, zawierac tylko "
                "male litery, cyfry, podkreslenia (np. 'fakir', 'ksef20_test')."
            )
        return v

    @model_validator(mode="after")
    def _validate_pull_requires_interval(self) -> "SourceCreate":
        if self.connection_mode == "pull" and not self.sync_interval_minutes:
            raise ValueError("connection_mode='pull' wymaga sync_interval_minutes.")
        return self


class SourceUpdate(BaseModel):
    """
    Body dla PUT /sources/{id}. Wszystkie pola opcjonalne (partial update).
    connection_config: jesli podane, calkowicie zastapuje istniejaca konfiguracje
    (po odszyfrowaniu starej i scaleniu po stronie serwisu jesli potrzeba).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    source_name: Optional[str] = Field(default=None, min_length=2, max_length=100)
    source_type: Optional[SourceType] = None
    connection_mode: Optional[ConnectionMode] = None
    connection_config: Optional[dict[str, Any]] = None
    sync_interval_minutes: Optional[int] = Field(default=None, ge=1, le=1440)
    is_active: Optional[bool] = None


class SourceOut(BaseModel):
    """
    Odpowiedz GET /sources, GET /sources/{id}, POST /sources, PUT /sources/{id}.

    connection_config NIE jest zwracany (zaszyfrowany, nie ma powodu go odsylac).
    Zamiast tego connection_config_keys pokazuje tylko nazwy kluczy konfiguracji
    (np. ["host", "port", "username"]) bez wartosci — przydatne dla UI formularza.
    """

    model_config = ConfigDict(from_attributes=True)

    id_source:              int
    source_name:            str
    source_type:            str
    connection_mode:        str
    connection_config_keys: list[str] = Field(default_factory=list)
    sync_interval_minutes:  Optional[int]
    last_sync_at:           Optional[datetime]
    last_sync_status:       Optional[str]
    last_sync_message:      Optional[str]
    is_test_mode:           bool
    has_webhook_token:      bool = Field(
        description="True jesli webhook_token jest ustawiony (nie pokazujemy samego tokenu).",
    )
    is_active:              bool
    created_at:             Optional[datetime] = None
    updated_at:             Optional[datetime] = None


class SourceListOut(BaseModel):
    """Odpowiedz GET /sources (lista z paginacja)."""

    model_config = ConfigDict(extra="forbid")

    items:    list[SourceOut]
    total:    int
    page:     int
    per_page: int


# =============================================================================
# TEST CONNECTION
# =============================================================================

class SourceTestConnectionResult(BaseModel):
    """Odpowiedz POST /sources/{id}/test-connection."""

    model_config = ConfigDict(extra="forbid")

    success:        bool
    message:        str
    latency_ms:     Optional[int] = None
    sample_count:   Optional[int] = Field(
        default=None, description="Liczba przykladowych dokumentow znalezionych (jesli dotyczy).",
    )
    tested_at:      datetime


# =============================================================================
# SYNC TRIGGER + STATUS
# =============================================================================

class SourceSyncTriggerResult(BaseModel):
    """Odpowiedz POST /sources/{id}/sync — kolejkowanie synchronizacji."""

    model_config = ConfigDict(extra="forbid")

    queued:      bool
    job_id:      Optional[str] = None
    message:     str


class SourceSyncStatusOut(BaseModel):
    """Odpowiedz GET /sources/{id}/sync-status."""

    model_config = ConfigDict(extra="forbid")

    id_source:          int
    last_sync_at:       Optional[datetime]
    last_sync_status:   Optional[str]
    last_sync_message:  Optional[str]
    next_sync_at:       Optional[datetime] = Field(
        default=None, description="Wyliczone: last_sync_at + sync_interval_minutes (tylko pull).",
    )
    is_currently_syncing: bool = Field(
        default=False, description="True jesli distributed lock sync_lock:{id_source} jest aktywny.",
    )


class SourceHealthEntry(BaseModel):
    """Pojedynczy wpis w GET /sources/health."""

    model_config = ConfigDict(extra="forbid")

    id_source:         int
    source_name:       str
    is_active:         bool
    is_test_mode:      bool
    last_sync_status:  Optional[str]
    last_sync_at:       Optional[datetime]
    minutes_since_sync: Optional[int] = None
    health:             Literal["ok", "warning", "critical", "unknown"]


class SourceHealthOut(BaseModel):
    """Odpowiedz GET /sources/health — dashboard admina."""

    model_config = ConfigDict(extra="forbid")

    sources:        list[SourceHealthEntry]
    overall_health: Literal["ok", "warning", "critical"]
    checked_at:     datetime


# =============================================================================
# TEST MODE TOGGLE
# =============================================================================

class SourceTestModePatch(BaseModel):
    """Body dla PATCH /sources/{id}/test-mode."""

    model_config = ConfigDict(extra="forbid")

    is_test_mode: bool = Field(
        ..., description="False = produkcyjny (hooki krytyczne dzialaja naprawde).",
    )


# =============================================================================
# WEBHOOK TOKEN
# =============================================================================

class WebhookTokenCreatedOut(BaseModel):
    """
    Odpowiedz po (re)generacji tokenu webhooka.

    KRYTYCZNE: token jest pokazywany WYLACZNIE w tej jednej odpowiedzi.
    W bazie przechowywany jest tylko SHA-256 hash. Jesli operator zgubi
    token — jedyna opcja to wygenerowanie nowego (stary natychmiast
    przestaje dzialac).
    """

    model_config = ConfigDict(extra="forbid")

    id_source:    int
    token:        str = Field(description="Token w postaci plaintext — zapisz go teraz, nie bedzie pokazany ponownie.")
    webhook_url:  str = Field(description="Pelny URL do skonfigurowania w systemie zewnetrznym.")
    generated_at: datetime
    warning:      str = Field(
        default=(
            "Ten token nie zostanie ponownie wyswietlony. Skopiuj go teraz "
            "i wklej do konfiguracji systemu zewnetrznego. Wygenerowanie "
            "nowego tokenu uniewazni ten natychmiast."
        ),
    )


# =============================================================================
# SOURCE HOOKS — CRUD
# =============================================================================

class HookOperationConfig(BaseModel):
    """
    Walidowana struktura operation_config dla hookow.
    Dokladna zawartosc zalezy od operation_type — patrz przykłady.

    sql_procedure:
        {"procedure_name": "dbo.skw_Func", "params": {"x": "{extra.y}"}, "timeout_seconds": 30}
    api_call:
        {"url": "https://...", "method": "POST", "headers": {...}, "body": {...}}
    """

    model_config = ConfigDict(extra="allow")

    timeout_seconds: int = Field(default=30, ge=5, le=120)


class HookCreate(BaseModel):
    """Body dla POST /sources/{id_source}/hooks."""

    model_config = ConfigDict(extra="forbid")

    trigger_action:    TriggerAction
    operation_type:    OperationType
    operation_config:  dict[str, Any] = Field(default_factory=dict)
    severity:          Severity = Field(default="informational")
    is_active:         bool = Field(default=True)

    @field_validator("operation_config")
    @classmethod
    def _validate_operation_config(cls, v: dict, info) -> dict:
        op_type = info.data.get("operation_type")
        if op_type == "sql_procedure":
            if "procedure_name" not in v:
                raise ValueError("operation_config musi zawierac 'procedure_name' dla sql_procedure.")
            import re as _re
            if not _re.match(r"^[\w.]+$", v["procedure_name"]):
                raise ValueError("procedure_name zawiera niedozwolone znaki.")
        elif op_type == "api_call":
            if "url" not in v:
                raise ValueError("operation_config musi zawierac 'url' dla api_call.")
        timeout = v.get("timeout_seconds", 30)
        if not isinstance(timeout, int) or not (5 <= timeout <= 120):
            raise ValueError("timeout_seconds musi byc int w zakresie 5-120.")
        return v


class HookUpdate(BaseModel):
    """Body dla PUT /sources/{id_source}/hooks/{id_hook}. Partial update."""

    model_config = ConfigDict(extra="forbid")

    operation_type:    Optional[OperationType] = None
    operation_config:  Optional[dict[str, Any]] = None
    severity:          Optional[Severity] = None
    is_active:         Optional[bool] = None


class HookOut(BaseModel):
    """Odpowiedz dla GET/POST/PUT hookow."""

    model_config = ConfigDict(from_attributes=True)

    id_hook:           int
    id_source:         int
    trigger_action:    str
    operation_type:    str
    operation_config:  dict[str, Any]
    severity:          str
    is_active:         bool
    created_at:        Optional[datetime] = None

    @field_validator("operation_config", mode="before")
    @classmethod
    def _parse_config_json(cls, v: Any) -> dict:
        """operation_config w DB jest NVARCHAR(MAX) z JSON-em — parsuj przy odczycie."""
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                return {}
        return v or {}


# =============================================================================
# SOURCE ACTIONS — CRUD
# =============================================================================

class ActionCreate(BaseModel):
    """
    Body dla POST /sources/{id_source}/actions.

    action_label jest WYMAGANE (NOT NULL w bazie) — to etykieta wyswietlana
    uzytkownikowi w UI (przycisk kontekstowy), odrebna od action_name
    (nazwa techniczna, snake_case).
    """

    model_config = ConfigDict(extra="forbid")

    action_name:           str = Field(..., min_length=2, max_length=100)
    action_label:          str = Field(..., min_length=2, max_length=200)
    operation_type:        ActionOperationType
    operation_config:      dict[str, Any] = Field(default_factory=dict)
    required_permission:   Optional[str] = Field(
        default=None,
        description="Nazwa uprawnienia wymaganego do wykonania (np. 'sources.execute_action'). None = brak dodatkowej weryfikacji.",
    )
    is_predefined:          bool = Field(
        default=False,
        description="True = predeklarowany 'klocek' aktywowany przez admina. False = w pelni niestandardowa akcja.",
    )
    is_active:              bool = Field(default=True)
    sort_order:             int = Field(default=0, ge=0, le=9999, description="Kolejnosc wyswietlania w UI (ASC).")

    @field_validator("action_name")
    @classmethod
    def _validate_action_name(cls, v: str) -> str:
        import re
        if not re.match(r"^[a-z][a-z0-9_]{1,99}$", v):
            raise ValueError(
                "action_name musi zaczynac sie od litery, zawierac tylko "
                "male litery, cyfry, podkreslenia (np. 'wyslij_powiadomienie')."
            )
        return v

    @field_validator("operation_config")
    @classmethod
    def _validate_operation_config(cls, v: dict, info) -> dict:
        op_type = info.data.get("operation_type")
        if op_type == "sql_procedure":
            if "procedure_name" not in v:
                raise ValueError("operation_config musi zawierac 'procedure_name' dla sql_procedure.")
            import re as _re
            if not _re.match(r"^[\w.]+$", v["procedure_name"]):
                raise ValueError("procedure_name zawiera niedozwolone znaki.")
        elif op_type == "api_call":
            if "url" not in v:
                raise ValueError("operation_config musi zawierac 'url' dla api_call.")
        elif op_type in ("file_move", "file_delete"):
            if "path_template" not in v:
                raise ValueError(
                    f"operation_config musi zawierac 'path_template' dla {op_type}."
                )
        return v


class ActionUpdate(BaseModel):
    """Body dla PUT /sources/{id_source}/actions/{id_action}. Partial update."""

    model_config = ConfigDict(extra="forbid")

    action_name:           Optional[str] = Field(default=None, min_length=2, max_length=100)
    action_label:          Optional[str] = Field(default=None, min_length=2, max_length=200)
    operation_type:        Optional[ActionOperationType] = None
    operation_config:      Optional[dict[str, Any]] = None
    required_permission:   Optional[str] = None
    is_predefined:          Optional[bool] = None
    is_active:              Optional[bool] = None
    sort_order:             Optional[int] = Field(default=None, ge=0, le=9999)


class ActionOut(BaseModel):
    """Odpowiedz dla GET/POST/PUT akcji."""

    model_config = ConfigDict(from_attributes=True)

    id_action:             int
    id_source:              int
    action_name:            str
    action_label:           str
    operation_type:         str
    operation_config:       dict[str, Any]
    required_permission:    Optional[str]
    is_predefined:           bool
    is_active:               bool
    sort_order:              int
    created_at:              Optional[datetime] = None

    @field_validator("operation_config", mode="before")
    @classmethod
    def _parse_config_json(cls, v: Any) -> dict:
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                return {}
        return v or {}