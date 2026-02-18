"""
backend/app/services/audit_service.py
=======================================
Serwis audytu — centralny punkt logowania wszystkich akcji w systemie.

Zasady działania:
    1. KAŻDA mutacja w systemie → wywołanie audit_service.log()
    2. Zapis do DWÓCH miejsc jednocześnie (redundancja):
         a) dbo_ext.AuditLog  — baza danych (trwały rekord, queryowalny)
         b) logs/audit_YYYY-MM-DD.jsonl — plik (append-only, nieusuwalne)
    3. Fire-and-forget: asyncio.create_task() — NIGDY nie blokuje response
    4. Błędy zapisu NIE przerywają requestu — własny try/except z fallback
    5. Dane wrażliwe (hasła, tokeny) są zawsze redagowane przed zapisem
    6. RequestID przepływa przez contextvars (ustawiany przez AuditMiddleware)
    7. Absurdalna ilość metadanych diagnostycznych w każdym rekordzie

Kolumny tabeli dbo_ext.AuditLog:
    ID_LOG          BIGINT IDENTITY — PK
    ID_USER         INT NULL        — FK Users (NULL dla systemowych)
    Username        NVARCHAR(50)    — kopia na wypadek usunięcia usera
    Action          NVARCHAR(100)   — np. "user_login", "role_permissions_updated"
    ActionCategory  NVARCHAR(50)    — Auth/Users/Roles/Debtors/Monits/Comments/System
    EntityType      NVARCHAR(50)    — User/Role/Debtor/Monit/Comment/Config
    EntityID        INT NULL        — ID encji
    OldValue        NVARCHAR(MAX)   — poprzedni stan (JSON)
    NewValue        NVARCHAR(MAX)   — nowy stan (JSON)
    Details         NVARCHAR(MAX)   — dodatkowe dane diagnostyczne (JSON)
    IPAddress       NVARCHAR(45)    — IP requestu
    UserAgent       NVARCHAR(500)   — User-Agent
    RequestURL      NVARCHAR(500)   — endpoint API
    RequestMethod   NVARCHAR(10)    — GET/POST/PUT/DELETE
    RequestID       NVARCHAR(36)    — UUID requestu (z AuditMiddleware)
    Timestamp       DATETIME        — DEFAULT GETDATE()
    Success         BIT             — 1=OK, 0=błąd
    ErrorMessage    NVARCHAR(500)   — komunikat błędu jeśli Success=0

Wersja: 1.0.0
Data:   2026-02-18
Autor:  System Windykacja
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import traceback
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional

import orjson
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# ---------------------------------------------------------------------------
# Logger modułu — osobny logger dla serwisu audytu
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ContextVars — przepływają przez cały request (ustawiane przez AuditMiddleware)
# ---------------------------------------------------------------------------
request_id_var: ContextVar[Optional[str]] = ContextVar("request_id", default=None)
user_id_var:    ContextVar[Optional[int]] = ContextVar("user_id",    default=None)
username_var:   ContextVar[Optional[str]] = ContextVar("username",   default=None)
ip_address_var: ContextVar[Optional[str]] = ContextVar("ip_address", default=None)
user_agent_var: ContextVar[Optional[str]] = ContextVar("user_agent", default=None)
request_url_var:    ContextVar[Optional[str]] = ContextVar("request_url",    default=None)
request_method_var: ContextVar[Optional[str]] = ContextVar("request_method", default=None)


def set_request_context(
    *,
    request_id: str,
    user_id: Optional[int] = None,
    username: Optional[str] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
    request_url: Optional[str] = None,
    request_method: Optional[str] = None,
) -> None:
    """
    Ustawia kontekst requestu w ContextVars.
    Wywoływane przez AuditMiddleware na początku każdego requestu.
    """
    request_id_var.set(request_id)
    user_id_var.set(user_id)
    username_var.set(username)
    ip_address_var.set(ip_address)
    user_agent_var.set(user_agent)
    request_url_var.set(request_url)
    request_method_var.set(request_method)


def get_request_context() -> dict[str, Any]:
    """Zwraca aktualny kontekst requestu jako dict."""
    return {
        "request_id": request_id_var.get(),
        "user_id":    user_id_var.get(),
        "username":   username_var.get(),
        "ip_address": ip_address_var.get(),
        "user_agent": user_agent_var.get(),
        "request_url": request_url_var.get(),
        "request_method": request_method_var.get(),
    }


# ---------------------------------------------------------------------------
# Typy — Action Categories + Entity Types
# ---------------------------------------------------------------------------

ActionCategory = Literal[
    "Auth",
    "Users",
    "Roles",
    "Permissions",
    "Debtors",
    "Monits",
    "Comments",
    "Templates",
    "System",
    "Snapshots",
    "Reports",
]

EntityType = Literal[
    "User",
    "Role",
    "Permission",
    "Debtor",
    "Monit",
    "Comment",
    "Template",
    "SystemConfig",
    "Snapshot",
    "SYSTEM",
]

# Mapa akcji → kategoria (auto-kategoryzacja gdy category nie podana)
_ACTION_CATEGORY_MAP: dict[str, ActionCategory] = {
    "user_login":                 "Auth",
    "user_logout":                "Auth",
    "user_token_refresh":         "Auth",
    "user_password_changed":      "Auth",
    "user_otp_verified":          "Auth",
    "user_password_reset":        "Auth",
    "user_impersonation_start":   "Auth",
    "user_impersonation_end":     "Auth",
    "master_access":              "Auth",
    "user_created":               "Users",
    "user_updated":               "Users",
    "user_deleted":               "Users",
    "user_delete_initiated":      "Users",
    "user_locked":                "Users",
    "user_unlocked":              "Users",
    "role_created":               "Roles",
    "role_updated":               "Roles",
    "role_deleted":               "Roles",
    "role_permissions_updated":   "Roles",
    "monit_sent":                 "Monits",
    "monit_bulk_sent":            "Monits",
    "comment_created":            "Comments",
    "comment_updated":            "Comments",
    "comment_deleted":            "Comments",
    "config_updated":             "System",
    "cors_updated":               "System",
    "schema_tamper_detected":     "System",
    "snapshot_created":           "Snapshots",
    "snapshot_restored":          "Snapshots",
}


def _infer_category(action: str) -> ActionCategory:
    """Automatycznie przypisuje kategorię na podstawie nazwy akcji."""
    return _ACTION_CATEGORY_MAP.get(action, "System")


# ---------------------------------------------------------------------------
# Redakcja danych wrażliwych
# ---------------------------------------------------------------------------

# Klucze JSON, których wartości są zawsze redagowane
_SENSITIVE_KEYS: frozenset[str] = frozenset({
    "password", "password_hash", "passwordhash", "hashed_password",
    "new_password", "old_password", "current_password",
    "token", "access_token", "refresh_token", "secret",
    "pin", "pin_hash", "master_key", "master_pin",
    "otp_code", "code", "secret_key",
    "authorization", "bearer",
})

_SENSITIVE_PATTERN = re.compile(
    r"(?i)(password|token|secret|pin|hash|key|bearer|auth)",
)


def _redact_dict(data: dict[str, Any], depth: int = 0) -> dict[str, Any]:
    """
    Rekursywnie redaguje wrażliwe wartości w słowniku.
    Nie zmienia struktury — tylko wartości przy wrażliwych kluczach.
    Limit głębokości = 5 (ochrona przed rekursją).
    """
    if depth > 5:
        return {"_truncated": "max_depth_reached"}

    result: dict[str, Any] = {}
    for key, value in data.items():
        key_lower = key.lower()
        if key_lower in _SENSITIVE_KEYS or _SENSITIVE_PATTERN.search(key_lower):
            result[key] = "[REDACTED]"
        elif isinstance(value, dict):
            result[key] = _redact_dict(value, depth + 1)
        elif isinstance(value, list):
            result[key] = [
                _redact_dict(item, depth + 1) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            result[key] = value
    return result


def _serialize_value(value: Any) -> Optional[str]:
    """
    Serializuje wartość do JSON string dla kolumn OldValue/NewValue/Details.
    Redaguje wrażliwe dane. Truncuje do 100k znaków (NVARCHAR(MAX) limit safety).
    """
    if value is None:
        return None
    if isinstance(value, str):
        # Już string — sprawdź czy to JSON do redakcji
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                redacted = _redact_dict(parsed)
                serialized = orjson.dumps(redacted).decode("utf-8")
            else:
                serialized = value
        except (json.JSONDecodeError, ValueError):
            serialized = value
    elif isinstance(value, dict):
        redacted = _redact_dict(value)
        serialized = orjson.dumps(redacted, option=orjson.OPT_NON_STR_KEYS).decode("utf-8")
    elif isinstance(value, (list, tuple)):
        serialized = orjson.dumps(list(value), option=orjson.OPT_NON_STR_KEYS).decode("utf-8")
    else:
        serialized = orjson.dumps(value, option=orjson.OPT_NON_STR_KEYS).decode("utf-8")

    # Truncate safety — NVARCHAR(MAX) ale na wszelki wypadek
    if len(serialized) > 100_000:
        truncated = serialized[:99_900]
        return truncated + '...[TRUNCATED]"}'

    return serialized


# ---------------------------------------------------------------------------
# Dataclass wpisu audytu
# ---------------------------------------------------------------------------

@dataclass
class AuditEntry:
    """
    Pojedynczy wpis audytu — wszystkie dane przed zapisem.
    Budowany przez log() przed fire-and-forget taskiem.
    """
    # Akcja — WYMAGANA
    action: str
    category: Optional[ActionCategory] = None

    # Kontekst encji
    entity_type: Optional[EntityType] = None
    entity_id: Optional[int] = None

    # Dane stanu
    old_value: Optional[Any] = None      # dict/str/None → JSON
    new_value: Optional[Any] = None      # dict/str/None → JSON
    details: Optional[Any] = None        # dodatkowe info → JSON

    # Kontekst użytkownika (override ContextVars)
    user_id: Optional[int] = None
    username: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    request_url: Optional[str] = None
    request_method: Optional[str] = None
    request_id: Optional[str] = None

    # Wynik
    success: bool = True
    error_message: Optional[str] = None

    # Timestamp (auto)
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def resolve_context(self) -> None:
        """
        Uzupełnia pola z ContextVars jeśli nie podane explicite.
        Wywołać PRZED przekazaniem do task (ContextVars są per-task).
        """
        if self.request_id is None:
            self.request_id = request_id_var.get()
        if self.user_id is None:
            self.user_id = user_id_var.get()
        if self.username is None:
            self.username = username_var.get()
        if self.ip_address is None:
            self.ip_address = ip_address_var.get()
        if self.user_agent is None:
            self.user_agent = user_agent_var.get()
        if self.request_url is None:
            self.request_url = request_url_var.get()
        if self.request_method is None:
            self.request_method = request_method_var.get()
        if self.category is None:
            self.category = _infer_category(self.action)

    def to_log_dict(self) -> dict[str, Any]:
        """Serializacja do JSON Lines — maksymalne dane diagnostyczne."""
        return {
            "event": "audit_log",
            "timestamp": self.timestamp.isoformat(),
            "action": self.action,
            "category": self.category,
            "entity": {
                "type": self.entity_type,
                "id": self.entity_id,
            },
            "user": {
                "id": self.user_id,
                "username": self.username,
                "ip_address": self.ip_address,
                "user_agent": (
                    self.user_agent[:200] if self.user_agent else None
                ),
            },
            "request": {
                "id": self.request_id,
                "url": self.request_url,
                "method": self.request_method,
            },
            "result": {
                "success": self.success,
                "error_message": self.error_message,
            },
            "data": {
                "old_value_present": self.old_value is not None,
                "new_value_present": self.new_value is not None,
                "details_present": self.details is not None,
            },
        }


# ---------------------------------------------------------------------------
# Ścieżki plików — audit JSONL
# ---------------------------------------------------------------------------

def _get_audit_log_path() -> Path:
    """Dziennika ścieżka pliku audit JSONL (rotacja dzienna)."""
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return log_dir / f"audit_{today}.jsonl"


def _write_audit_jsonl(entry: AuditEntry) -> None:
    """
    Zapisuje wpis do pliku audit JSONL (append-only, NIEUSUWALNE).
    Wywoływana synchronicznie w wątku executor lub bezpośrednio.
    Błędy tej funkcji są logowane do stderr — nie przerywają niczego.
    """
    path = _get_audit_log_path()
    try:
        record = entry.to_log_dict()
        line = orjson.dumps(record, option=orjson.OPT_NON_STR_KEYS).decode("utf-8")
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError as exc:
        # Fallback na stderr — nie możemy stracić informacji
        print(
            f"[AUDIT_SERVICE] Błąd zapisu do pliku {path}: {exc}",
            file=sys.stderr,
        )


# ---------------------------------------------------------------------------
# Zapis do bazy danych — INSERT do dbo_ext.AuditLog
# ---------------------------------------------------------------------------

_INSERT_AUDIT_SQL = text("""
    INSERT INTO dbo_ext.AuditLog (
        ID_USER,
        Username,
        Action,
        ActionCategory,
        EntityType,
        EntityID,
        OldValue,
        NewValue,
        Details,
        IPAddress,
        UserAgent,
        RequestURL,
        RequestMethod,
        RequestID,
        Timestamp,
        Success,
        ErrorMessage
    ) VALUES (
        :user_id,
        :username,
        :action,
        :category,
        :entity_type,
        :entity_id,
        :old_value,
        :new_value,
        :details,
        :ip_address,
        :user_agent,
        :request_url,
        :request_method,
        :request_id,
        :timestamp,
        :success,
        :error_message
    )
""")


async def _write_to_db(db: AsyncSession, entry: AuditEntry) -> None:
    """
    Zapisuje wpis audytu do dbo_ext.AuditLog.
    Używa osobnej sesji z własnym commit/rollback — nie ingeruje w session callera.

    WAŻNE: Ta funkcja jest wywoływana przez asyncio.create_task() —
    błędy są przechwytywane i logowane, NIE propagowane.
    """
    try:
        await db.execute(
            _INSERT_AUDIT_SQL,
            {
                "user_id":       entry.user_id,
                "username":      (entry.username or "")[:50] if entry.username else None,
                "action":        entry.action[:100],
                "category":      (entry.category or "System")[:50],
                "entity_type":   (entry.entity_type or "")[:50] if entry.entity_type else None,
                "entity_id":     entry.entity_id,
                "old_value":     _serialize_value(entry.old_value),
                "new_value":     _serialize_value(entry.new_value),
                "details":       _serialize_value(entry.details),
                "ip_address":    (entry.ip_address or "")[:45] if entry.ip_address else None,
                "user_agent":    (entry.user_agent or "")[:500] if entry.user_agent else None,
                "request_url":   (entry.request_url or "")[:500] if entry.request_url else None,
                "request_method": (entry.request_method or "")[:10] if entry.request_method else None,
                "request_id":    (entry.request_id or "")[:36] if entry.request_id else None,
                "timestamp":     entry.timestamp,
                "success":       1 if entry.success else 0,
                "error_message": (entry.error_message or "")[:500] if entry.error_message else None,
            },
        )
        await db.commit()

        logger.debug(
            "AuditLog zapisany do DB: action=%s, user=%s, entity=%s/%s, success=%s",
            entry.action,
            entry.username or entry.user_id,
            entry.entity_type,
            entry.entity_id,
            entry.success,
            extra={
                "audit_action": entry.action,
                "audit_user_id": entry.user_id,
                "audit_entity_type": entry.entity_type,
                "audit_entity_id": entry.entity_id,
                "audit_request_id": entry.request_id,
            },
        )

    except Exception as exc:
        # Rollback i logowanie błędu — ale NIE propagujemy
        try:
            await db.rollback()
        except Exception:
            pass

        logger.error(
            "BŁĄD zapisu AuditLog do DB (action=%s, user=%s): %s",
            entry.action,
            entry.user_id,
            exc,
            extra={
                "audit_action": entry.action,
                "audit_user_id": entry.user_id,
                "audit_request_id": entry.request_id,
                "traceback": traceback.format_exc(),
            },
        )
        # FALLBACK: przynajmniej mamy plik JSONL — nie zgubimy danych


# ---------------------------------------------------------------------------
# Główna funkcja zapisu — FIRE AND FORGET
# ---------------------------------------------------------------------------

async def _persist_entry(db: AsyncSession, entry: AuditEntry) -> None:
    """
    Wewnętrzna coroutine wykonywana przez asyncio.create_task().
    Zapisuje do ODRAZU do obu miejsc (plik + DB) — redundancja.
    Błędy każdego z zapisów są izolowane.
    """
    # 1. Plik JSONL — zawsze jako pierwsze (szybsze, nie blokuje DB pool)
    try:
        _write_audit_jsonl(entry)
    except Exception as exc:
        logger.error(
            "Błąd zapisu audit JSONL (action=%s): %s",
            entry.action, exc,
            extra={"traceback": traceback.format_exc()},
        )

    # 2. Baza danych
    await _write_to_db(db, entry)


def log(
    db: AsyncSession,
    *,
    action: str,
    category: Optional[ActionCategory] = None,
    entity_type: Optional[EntityType] = None,
    entity_id: Optional[int] = None,
    old_value: Optional[Any] = None,
    new_value: Optional[Any] = None,
    details: Optional[Any] = None,
    user_id: Optional[int] = None,
    username: Optional[str] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
    request_url: Optional[str] = None,
    request_method: Optional[str] = None,
    request_id: Optional[str] = None,
    success: bool = True,
    error_message: Optional[str] = None,
) -> asyncio.Task:
    """
    Loguje akcję audytu — FIRE AND FORGET.

    Zwraca Task który można await'ować jeśli potrzebna gwarancja zapisu,
    lub zignorować (fire-and-forget) w normalnym użyciu.

    ZASADA: Wywołaj log() BEZ await we wszystkich serwisach.
    Tylko w krytycznych miejscach (np. login/logout) użyj await log().

    Args:
        db:             AsyncSession (z FastAPI Depends)
        action:         Nazwa akcji — snake_case, np. "user_login"
        category:       Kategoria (auto-inferred jeśli None)
        entity_type:    Typ encji (User/Role/Debtor/...)
        entity_id:      ID encji
        old_value:      Poprzedni stan (dict/str → JSON)
        new_value:      Nowy stan (dict/str → JSON)
        details:        Dodatkowe dane diagnostyczne (dict/str → JSON)
        user_id:        ID usera (override ContextVar)
        username:       Username (override ContextVar)
        ip_address:     IP (override ContextVar)
        user_agent:     User-Agent (override ContextVar)
        request_url:    URL endpointu (override ContextVar)
        request_method: HTTP method (override ContextVar)
        request_id:     UUID requestu (override ContextVar)
        success:        Czy akcja się powiodła
        error_message:  Komunikat błędu jeśli success=False

    Returns:
        asyncio.Task — można await'ować lub zignorować

    Example (fire-and-forget):
        log(db, action="user_updated", entity_type="User", entity_id=42,
            old_value=old_data, new_value=new_data)

    Example (await — gwarancja zapisu przed response):
        await log(db, action="user_login", success=True, user_id=user.id)
    """
    entry = AuditEntry(
        action=action,
        category=category,
        entity_type=entity_type,
        entity_id=entity_id,
        old_value=old_value,
        new_value=new_value,
        details=details,
        user_id=user_id,
        username=username,
        ip_address=ip_address,
        user_agent=user_agent,
        request_url=request_url,
        request_method=request_method,
        request_id=request_id,
        success=success,
        error_message=error_message,
    )

    # KRYTYCZNE: resolve ContextVars PRZED przekazaniem do task
    # (ContextVars nie są automatycznie kopiowane do nowych tasków w asyncio)
    entry.resolve_context()

    # Zaloguj do modułowego loggera (zawsze synchronicznie — szybkie)
    if success:
        logger.info(
            "AUDIT: %s | user=%s | %s/%s | ok",
            action,
            entry.username or entry.user_id or "system",
            entity_type or "-",
            entity_id or "-",
            extra={
                "audit_action": action,
                "audit_category": entry.category,
                "audit_user_id": entry.user_id,
                "audit_entity_type": entity_type,
                "audit_entity_id": entity_id,
                "audit_request_id": entry.request_id,
                "audit_success": True,
            },
        )
    else:
        logger.warning(
            "AUDIT: %s | user=%s | %s/%s | BŁĄD: %s",
            action,
            entry.username or entry.user_id or "system",
            entity_type or "-",
            entity_id or "-",
            error_message or "unknown",
            extra={
                "audit_action": action,
                "audit_category": entry.category,
                "audit_user_id": entry.user_id,
                "audit_entity_type": entity_type,
                "audit_entity_id": entity_id,
                "audit_request_id": entry.request_id,
                "audit_success": False,
                "audit_error": error_message,
            },
        )

    # Fire-and-forget task
    task = asyncio.create_task(
        _persist_entry(db, entry),
        name=f"audit_{action}_{entry.request_id or 'no_req'}",
    )

    # Dodaj callback obsługi błędów task (bez await)
    task.add_done_callback(_handle_task_exception)

    return task


def _handle_task_exception(task: asyncio.Task) -> None:
    """
    Callback dla fire-and-forget tasków.
    Przechwytuje nieobsłużone wyjątki i loguje je — bez propagacji.
    """
    if task.cancelled():
        logger.warning(
            "Task audytu anulowany: %s",
            task.get_name(),
        )
        return

    exc = task.exception()
    if exc is not None:
        logger.error(
            "Nieobsłużony błąd w task audytu %s: %s",
            task.get_name(),
            exc,
            extra={
                "task_name": task.get_name(),
                "traceback": "".join(
                    traceback.format_exception(type(exc), exc, exc.__traceback__)
                ),
            },
        )


# ---------------------------------------------------------------------------
# Warianty pomocnicze — wygodne opakowania dla typowych przypadków
# ---------------------------------------------------------------------------

def log_auth(
    db: AsyncSession,
    *,
    action: str,
    user_id: Optional[int] = None,
    username: Optional[str] = None,
    success: bool = True,
    error_message: Optional[str] = None,
    details: Optional[dict[str, Any]] = None,
    ip_address: Optional[str] = None,
) -> asyncio.Task:
    """
    Skrót dla logowania akcji autentykacji.
    Używaj dla: login, logout, refresh, change-password, OTP, impersonacja.
    """
    return log(
        db,
        action=action,
        category="Auth",
        entity_type="User",
        entity_id=user_id,
        user_id=user_id,
        username=username,
        success=success,
        error_message=error_message,
        details=details,
        ip_address=ip_address,
    )


def log_crud(
    db: AsyncSession,
    *,
    action: str,
    entity_type: EntityType,
    entity_id: Optional[int] = None,
    old_value: Optional[dict[str, Any]] = None,
    new_value: Optional[dict[str, Any]] = None,
    user_id: Optional[int] = None,
    username: Optional[str] = None,
    details: Optional[dict[str, Any]] = None,
    success: bool = True,
    error_message: Optional[str] = None,
) -> asyncio.Task:
    """
    Skrót dla logowania operacji CRUD na encjach.
    Używaj dla: create/update/delete na User/Role/Comment/Template/Config.
    Auto-infer category z nazwy akcji.
    """
    return log(
        db,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        old_value=old_value,
        new_value=new_value,
        details=details,
        user_id=user_id,
        username=username,
        success=success,
        error_message=error_message,
    )


def log_system(
    db: AsyncSession,
    *,
    action: str,
    details: Optional[dict[str, Any]] = None,
    success: bool = True,
    error_message: Optional[str] = None,
) -> asyncio.Task:
    """
    Skrót dla logowania akcji systemowych (bez usera).
    Używaj dla: schema_tamper, startup, snapshot, cron tasks.
    """
    return log(
        db,
        action=action,
        category="System",
        entity_type="SYSTEM",
        details=details,
        success=success,
        error_message=error_message,
    )


def log_failed_login(
    db: AsyncSession,
    *,
    username_attempt: str,
    ip_address: Optional[str] = None,
    reason: str = "invalid_credentials",
) -> asyncio.Task:
    """
    Dedykowana funkcja dla nieudanych prób logowania.
    NIE loguje hasła — tylko username i powód.
    """
    return log(
        db,
        action="user_login",
        category="Auth",
        entity_type="User",
        success=False,
        error_message=reason,
        details={
            "username_attempt": username_attempt[:50],
            "reason": reason,
        },
        ip_address=ip_address,
    )


# ---------------------------------------------------------------------------
# Funkcje pomocnicze do przygotowania danych przed logiem
# ---------------------------------------------------------------------------

def model_to_audit_dict(
    obj: Any,
    exclude_fields: Optional[set[str]] = None,
) -> dict[str, Any]:
    """
    Konwertuje model SQLAlchemy do słownika bezpiecznego dla AuditLog.
    Automatycznie redaguje pola wrażliwe.

    Args:
        obj:            Instancja modelu SQLAlchemy
        exclude_fields: Zestaw nazw pól do pominięcia (np. {"password_hash"})

    Returns:
        Dict gotowy do przekazania jako old_value/new_value w log()
    """
    exclude_fields = exclude_fields or set()
    # Zawsze wyklucz pola wrażliwe
    always_exclude = {"password_hash", "passwordhash", "pin_hash", "_sa_instance_state"}
    exclude_all = exclude_fields | always_exclude

    result: dict[str, Any] = {}

    # Obsługa SQLAlchemy ORM (via __dict__ lub __mapper__)
    if hasattr(obj, "__dict__"):
        for key, value in obj.__dict__.items():
            if key.startswith("_") or key in exclude_all:
                continue
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            elif hasattr(value, "__class__") and hasattr(value.__class__, "__tablename__"):
                # Skip related objects (lazy-loaded relationships)
                continue
            else:
                result[key] = value
    else:
        result = {"_repr": str(obj)}

    return _redact_dict(result)


def diff_dicts(
    before: dict[str, Any],
    after: dict[str, Any],
) -> dict[str, Any]:
    """
    Zwraca diff między dwoma słownikami — tylko zmienione pola.
    Używaj do budowania 'details' w log_crud() dla operacji UPDATE.

    Returns:
        {"changed_fields": ["field1", "field2"], "before": {...}, "after": {...}}
    """
    changed_keys = [
        k for k in set(before) | set(after)
        if before.get(k) != after.get(k)
    ]
    return {
        "changed_fields": sorted(changed_keys),
        "changed_count": len(changed_keys),
        "before_snapshot": {k: before.get(k) for k in changed_keys},
        "after_snapshot": {k: after.get(k) for k in changed_keys},
    }


# ---------------------------------------------------------------------------
# Funkcja do query audit logów (dla GET /audit endpoint)
# ---------------------------------------------------------------------------

async def get_logs(
    db: AsyncSession,
    *,
    user_id: Optional[int] = None,
    action: Optional[str] = None,
    category: Optional[str] = None,
    entity_type: Optional[str] = None,
    entity_id: Optional[int] = None,
    success: Optional[bool] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    request_id: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    """
    Pobiera logi audytu z bazy z filtrami.
    Używany przez endpoint GET /audit/logs.

    Returns:
        (lista_logów, total_count) — do paginacji
    """
    conditions: list[str] = []
    params: dict[str, Any] = {}

    if user_id is not None:
        conditions.append("ID_USER = :user_id")
        params["user_id"] = user_id

    if action is not None:
        conditions.append("Action = :action")
        params["action"] = action[:100]

    if category is not None:
        conditions.append("ActionCategory = :category")
        params["category"] = category[:50]

    if entity_type is not None:
        conditions.append("EntityType = :entity_type")
        params["entity_type"] = entity_type[:50]

    if entity_id is not None:
        conditions.append("EntityID = :entity_id")
        params["entity_id"] = entity_id

    if success is not None:
        conditions.append("Success = :success")
        params["success"] = 1 if success else 0

    if date_from is not None:
        conditions.append("Timestamp >= :date_from")
        params["date_from"] = date_from

    if date_to is not None:
        conditions.append("Timestamp <= :date_to")
        params["date_to"] = date_to

    if request_id is not None:
        conditions.append("RequestID = :request_id")
        params["request_id"] = request_id[:36]

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    # Data query
    data_sql = text(f"""
        SELECT
            ID_LOG, ID_USER, Username, Action, ActionCategory,
            EntityType, EntityID,
            OldValue, NewValue, Details,
            IPAddress, UserAgent, RequestURL, RequestMethod, RequestID,
            Timestamp, Success, ErrorMessage
        FROM dbo_ext.AuditLog
        {where_clause}
        ORDER BY Timestamp DESC, ID_LOG DESC
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
    """)
    params_data = {**params, "offset": offset, "limit": limit}

    # Count query
    count_sql = text(f"""
        SELECT COUNT(*) AS total
        FROM dbo_ext.AuditLog
        {where_clause}
    """)

    try:
        data_result = await db.execute(data_sql, params_data)
        rows_raw = data_result.fetchall()
        count_result = await db.execute(count_sql, params)
        total = count_result.scalar() or 0

        rows = [
            {
                "id_log": r.ID_LOG,
                "user_id": r.ID_USER,
                "username": r.Username,
                "action": r.Action,
                "category": r.ActionCategory,
                "entity_type": r.EntityType,
                "entity_id": r.EntityID,
                "old_value": _safe_parse_json(r.OldValue),
                "new_value": _safe_parse_json(r.NewValue),
                "details": _safe_parse_json(r.Details),
                "ip_address": r.IPAddress,
                "user_agent": r.UserAgent,
                "request_url": r.RequestURL,
                "request_method": r.RequestMethod,
                "request_id": r.RequestID,
                "timestamp": r.Timestamp.isoformat() if r.Timestamp else None,
                "success": bool(r.Success),
                "error_message": r.ErrorMessage,
            }
            for r in rows_raw
        ]

        logger.debug(
            "get_logs: pobrano %d/%d wpisów (offset=%d, limit=%d)",
            len(rows), total, offset, limit,
        )
        return rows, total

    except Exception as exc:
        logger.error(
            "Błąd pobierania logów audytu: %s",
            exc,
            extra={"traceback": traceback.format_exc()},
        )
        raise


def _safe_parse_json(value: Optional[str]) -> Any:
    """Bezpieczna deserializacja JSON — zwraca string jeśli nie jest valid JSON."""
    if value is None:
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


# ---------------------------------------------------------------------------
# Eksport publicznego API
# ---------------------------------------------------------------------------

__all__ = [
    # Context management
    "set_request_context",
    "get_request_context",
    # Główne funkcje logowania
    "log",
    "log_auth",
    "log_crud",
    "log_system",
    "log_failed_login",
    # Query
    "get_logs",
    # Pomocnicze
    "model_to_audit_dict",
    "diff_dicts",
    # ContextVars (eksportowane dla middleware)
    "request_id_var",
    "user_id_var",
    "username_var",
    "ip_address_var",
    "user_agent_var",
    "request_url_var",
    "request_method_var",
    # Typy
    "AuditEntry",
    "ActionCategory",
    "EntityType",
]