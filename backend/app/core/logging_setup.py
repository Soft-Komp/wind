"""
Konfiguracja systemu logowania.

Architektura:
  - Trzy kanały logowania: app, events, worker
  - Format: JSON Lines (jedna linia = jeden log) — łatwy do parsowania przez ELK/Grafana
  - Rotacja: dzienna (TimedRotatingFileHandler) — jeden plik na dzień
  - Pliki nieusuwalne przez aplikację (backupCount=0 — brak automatycznego usuwania)
  - Redakcja sekretów: klasy filtrów czyszczą wrażliwe dane przed zapisem
  - Kontekst requestu: contextvars przechowują request_id, user_id, ip — widoczne w każdym logu
  - Poziomy: DEBUG w dev, INFO w prod

Pliki logów:
  logs/app_YYYY-MM-DD.log     — główny log aplikacji (wszystko: INFO+)
  logs/events_YYYY-MM-DD.jsonl — zdarzenia SSE (task_completed, permissions_updated...)
  logs/worker_YYYY-MM-DD.log  — logi workera ARQ

Użycie:
    from app.core.logging import setup_logging, get_logger

    # W main.py (lifespan):
    setup_logging()

    # W modułach:
    logger = get_logger(__name__)
    logger.info("Użytkownik zalogowany", extra={"user_id": 42, "ip": "192.168.1.1"})

    # Ustawienie kontekstu requestu (w middleware):
    from app.core.logging import set_request_context
    set_request_context(request_id="abc-123", user_id=42, ip="192.168.1.1")
"""

from __future__ import annotations

import json
import logging
import logging.config
import os
import re
import sys
import traceback
import uuid
from contextvars import ContextVar
from datetime import date, datetime, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# ContextVars — przechowują dane requestu przez cały cykl życia żądania
# Ustawiane w middleware, dostępne w każdym module bez przekazywania parametrów
# ---------------------------------------------------------------------------

_ctx_request_id: ContextVar[str] = ContextVar("request_id", default="")
_ctx_user_id:    ContextVar[Optional[int]] = ContextVar("user_id", default=None)
_ctx_username:   ContextVar[str] = ContextVar("username", default="")
_ctx_ip_address: ContextVar[str] = ContextVar("ip_address", default="")
_ctx_user_agent: ContextVar[str] = ContextVar("user_agent", default="")


def set_request_context(
    *,
    request_id: Optional[str] = None,
    user_id: Optional[int] = None,
    username: str = "",
    ip_address: str = "",
    user_agent: str = "",
) -> str:
    """
    Ustawia kontekst bieżącego requestu w zmiennych kontekstowych.

    Wywoływana w middleware na początku każdego requestu.
    Zwraca request_id (generuje nowy jeśli nie podano).

    Returns:
        str: request_id (nowy lub przekazany).
    """
    rid = request_id or str(uuid.uuid4())
    _ctx_request_id.set(rid)
    _ctx_user_id.set(user_id)
    _ctx_username.set(username or "")
    _ctx_ip_address.set(ip_address or "")
    _ctx_user_agent.set(user_agent or "")
    return rid


def get_request_context() -> Dict[str, Any]:
    """
    Pobiera aktualny kontekst requestu.

    Używany przez formatter przy budowaniu każdego rekordu logu.

    Returns:
        dict: Aktualny kontekst: request_id, user_id, ip, user_agent.
    """
    return {
        "request_id": _ctx_request_id.get(),
        "user_id":    _ctx_user_id.get(),
        "username":   _ctx_username.get(),
        "ip_address": _ctx_ip_address.get(),
        "user_agent": _ctx_user_agent.get(),
    }


# ---------------------------------------------------------------------------
# Wzorce redakcji sekretów — regex dopasowujące wrażliwe dane w logowanych stringach
# ---------------------------------------------------------------------------

# Lista par (pattern, replacement) — stosowana w kolejności
_REDACTION_PATTERNS = [
    # Hasła w JSON/dict: "password": "..." lub "pwd": "..."
    (re.compile(
        r'("(?:password|passwd|pwd|secret|token|key|authorization|auth|'
        r'master_key|pin|otp|refresh_token|access_token|confirm_token)"'
        r'\s*:\s*")([^"]{0,500})(")',
        re.IGNORECASE,
    ), r'\1**REDACTED**\3'),

    # Bearer token w nagłówku Authorization
    (re.compile(
        r'(Bearer\s+)[A-Za-z0-9\-._~+/]+=*',
        re.IGNORECASE,
    ), r'\1**REDACTED**'),

    # Hasła w ODBC connection string: PWD=...; lub PWD=...end
    (re.compile(
        r'((?:PWD|Password)=)([^;]{0,200})(;|$)',
        re.IGNORECASE,
    ), r'\1**REDACTED**\3'),

    # DSN URL: //user:password@host
    (re.compile(
        r'(://[^:@]{1,100}:)([^@]{1,200})(@)',
    ), r'\1**REDACTED**\3'),

    # Numery kart płatniczych (16 cyfr z separatorami)
    (re.compile(r'\b(\d{4})[\s\-]?(\d{4})[\s\-]?(\d{4})[\s\-]?(\d{4})\b'),
     r'\1-****-****-\4'),

    # PESEL (11 cyfr — ochrona danych osobowych)
    (re.compile(r'\b\d{11}\b'), r'***PESEL***'),
]


def _redact_sensitive(value: str) -> str:
    """
    Redaguje wrażliwe dane ze stringa logu.

    Stosuje wszystkie wzorce redakcji w kolejności.
    Wywoływana przez formatter na każdym rekordzie logu.

    Args:
        value: Surowy string logu.

    Returns:
        str: String z zredagowanymi sekretami.
    """
    for pattern, replacement in _REDACTION_PATTERNS:
        value = pattern.sub(replacement, value)
    return value


# ---------------------------------------------------------------------------
# JSON Lines Formatter — każdy log = jeden wiersz JSON
# ---------------------------------------------------------------------------

class JsonLinesFormatter(logging.Formatter):
    """
    Formatter serializujący każdy rekord logu do jednej linii JSON.

    Format:
    {
        "timestamp": "2026-02-17T14:30:00.123456+00:00",
        "level":     "INFO",
        "logger":    "app.api.auth",
        "message":   "Użytkownik zalogowany pomyślnie",
        "request_id": "abc-123-def",
        "user_id":   42,
        "username":  "jan.kowalski",
        "ip_address": "192.168.1.100",
        "user_agent": "Mozilla/5.0...",
        "module":    "auth",
        "function":  "login",
        "line":      87,
        "process":   12345,
        "thread":    140234,
        "extra":     { ... }   ← pola z extra= w wywołaniu logger.*()
        "exception": "..."     ← traceback jeśli był wyjątek
    }

    Wszystkie stringi przechodzą przez _redact_sensitive() przed zapisem.
    """

    # Pola które NIE trafiają do klucza "extra" (standardowe pola LogRecord)
    _STANDARD_FIELDS = frozenset({
        "name", "msg", "args", "levelname", "levelno", "pathname",
        "filename", "module", "exc_info", "exc_text", "stack_info",
        "lineno", "funcName", "created", "msecs", "relativeCreated",
        "thread", "threadName", "processName", "process", "message",
        "taskName",
    })

    def format(self, record: logging.LogRecord) -> str:
        """Serializuje LogRecord do JSON Lines."""

        # Podstawowe pola każdego logu
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level":    record.levelname,
            "logger":   record.name,
            "message":  _redact_sensitive(record.getMessage()),
            "module":   record.module,
            "function": record.funcName,
            "line":     record.lineno,
            "process":  record.process,
            "thread":   record.thread,
        }

        # Kontekst requestu z ContextVar
        ctx = get_request_context()
        if ctx["request_id"]:
            log_entry["request_id"] = ctx["request_id"]
        if ctx["user_id"] is not None:
            log_entry["user_id"] = ctx["user_id"]
        if ctx["username"]:
            log_entry["username"] = ctx["username"]
        if ctx["ip_address"]:
            log_entry["ip_address"] = ctx["ip_address"]
        if ctx["user_agent"]:
            log_entry["user_agent"] = _redact_sensitive(ctx["user_agent"])

        # Pola extra przekazane przez wywołujący kod (np. extra={"entity_id": 5})
        extra: Dict[str, Any] = {}
        for key, value in record.__dict__.items():
            if key not in self._STANDARD_FIELDS and not key.startswith("_"):
                # Redakcja wartości stringowych
                if isinstance(value, str):
                    value = _redact_sensitive(value)
                extra[key] = value
        if extra:
            log_entry["extra"] = extra

        # Wyjątek — pełny traceback jako string
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        elif record.exc_text:
            log_entry["exception"] = record.exc_text

        # Stack info (np. z logger.warning(..., stack_info=True))
        if record.stack_info:
            log_entry["stack_info"] = record.stack_info

        # Serializacja do JSON — obsługa niestandardowych typów
        try:
            return json.dumps(log_entry, ensure_ascii=False, default=_json_default)
        except Exception as e:
            # Fallback — nie możemy stracić logu z powodu błędu serializacji
            safe_entry = {
                "timestamp": log_entry.get("timestamp", ""),
                "level":     "ERROR",
                "logger":    "logging.formatter",
                "message":   f"BŁĄD SERIALIZACJI LOGU: {e} | oryg: {str(record.getMessage())[:200]}",
            }
            return json.dumps(safe_entry, ensure_ascii=False)


def _json_default(obj: Any) -> Any:
    """Obsługuje typy niepasujące do domyślnej serializacji JSON."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.hex()
    if hasattr(obj, "__dict__"):
        return str(obj)
    return str(obj)


# ---------------------------------------------------------------------------
# Handler z rotacją dzienną — pliki nieusuwalne (backupCount=0)
# ---------------------------------------------------------------------------

class NonDeletingTimedRotatingFileHandler(TimedRotatingFileHandler):
    """
    TimedRotatingFileHandler który NIE usuwa starych plików po rotacji.

    Zgodnie z ustaleniami projektu: pliki logów są nieusuwalne przez aplikację.
    DBA może je archiwizować/usuwać ręcznie.

    Zmiana nazwy po rotacji: log.log → log_2026-02-17.log
    """

    def __init__(self, log_dir: Path, log_name: str, **kwargs):
        """
        Args:
            log_dir: Katalog na pliki logów.
            log_name: Bazowa nazwa pliku (bez rozszerzenia i daty).
        """
        log_dir.mkdir(parents=True, exist_ok=True)
        # Plik aktywny — bez daty (data dodawana po rotacji)
        filepath = log_dir / f"{log_name}.log"

        super().__init__(
            filename=str(filepath),
            when="midnight",         # Rotacja o północy
            interval=1,              # Co 1 dzień
            backupCount=0,           # NIE usuwaj starych plików
            encoding="utf-8",
            delay=False,             # Otwórz plik od razu
            utc=True,                # Czas UTC w nazwie pliku
            **kwargs,
        )

    def doRollover(self) -> None:
        """Nadpisujemy rotację — zmiana nazwy ze znacznikiem daty w ISO format."""
        if self.stream:
            self.stream.close()
            self.stream = None  # type: ignore[assignment]

        # Nowa nazwa: app_2026-02-17.log
        today = date.today().strftime("%Y-%m-%d")
        base = self.baseFilename
        # Wyciągamy katalog i bazową nazwę
        dir_path = os.path.dirname(base)
        base_name = os.path.basename(base).replace(".log", "").replace(".jsonl", "")
        ext = ".jsonl" if ".jsonl" in self.baseFilename else ".log"
        archived_path = os.path.join(dir_path, f"{base_name}_{today}{ext}")

        # Rename aktualnego pliku
        if os.path.exists(base):
            if not os.path.exists(archived_path):
                os.rename(base, archived_path)

        # Otwórz nowy plik
        self.stream = self._open()
        self.rolloverAt = self.computeRollover(
            int(datetime.now(tz=timezone.utc).timestamp())
        )


# ---------------------------------------------------------------------------
# Filtr redakcji — dodatkowa warstwa bezpieczeństwa na poziomie handlera
# ---------------------------------------------------------------------------

class SensitiveDataFilter(logging.Filter):
    """
    Filtr logów redagujący wrażliwe dane z pola msg i args.

    Stosowany na wszystkich handlerach jako dodatkowa warstwa ochrony.
    JsonLinesFormatter również redaguje, ale filtr działa wcześniej —
    zapobiega trafieniu sekretów do msg przed formatowaniem.
    """

    # Klucze dict uważane za wrażliwe — wartości przy tych kluczach są maskowane
    _SENSITIVE_KEYS = frozenset({
        "password", "passwd", "pwd", "secret", "token", "key",
        "authorization", "auth", "master_key", "pin", "otp",
        "refresh_token", "access_token", "confirm_token", "reset_token",
        "new_password", "old_password", "api_key", "private_key",
    })

    def filter(self, record: logging.LogRecord) -> bool:
        """Modyfikuje rekord in-place i zawsze zwraca True (przepuszcza log)."""
        # Redakcja pola msg
        if isinstance(record.msg, str):
            record.msg = _redact_sensitive(record.msg)

        # Redakcja args — obsługa tuple/list i dict
        if record.args:
            if isinstance(record.args, dict):
                record.args = self._redact_dict(record.args)
            elif isinstance(record.args, (list, tuple)):
                record.args = tuple(
                    self._redact_arg(a) for a in record.args
                )
        return True

    def _redact_dict(self, d: dict) -> dict:
        """Redaguje wartości przy wrażliwych kluczach słownika."""
        result = {}
        for k, v in d.items():
            if isinstance(k, str) and k.lower() in self._SENSITIVE_KEYS:
                result[k] = "**REDACTED**"
            elif isinstance(v, str):
                result[k] = _redact_sensitive(v)
            elif isinstance(v, dict):
                result[k] = self._redact_dict(v)
            else:
                result[k] = v
        return result

    def _redact_arg(self, arg) -> object:
        """Redaguje pojedynczy argument — string lub dict."""
        if isinstance(arg, str):
            return _redact_sensitive(arg)
        if isinstance(arg, dict):
            return self._redact_dict(arg)
        return arg


# ---------------------------------------------------------------------------
# Główna funkcja konfiguracji logowania
# ---------------------------------------------------------------------------

def setup_logging(log_dir: Optional[Path] = None, debug: bool = False) -> None:
    """
    Inicjalizuje system logowania dla całej aplikacji.

    Tworzy i konfiguruje:
      - Handler konsoli (stdout) — zawsze aktywny
      - Handler pliku aplikacji (logs/app_YYYY-MM-DD.log)
      - Handler pliku eventów SSE (logs/events_YYYY-MM-DD.jsonl)
      - Handler pliku workera (logs/worker_YYYY-MM-DD.log)

    Wywoływana RAZ w lifespan aplikacji FastAPI przed wszystkim innym.

    Args:
        log_dir: Ścieżka do katalogu logów. Domyślnie z settings.
        debug:   Poziom DEBUG zamiast INFO. Domyślnie z settings.
    """
    # Importujemy tu — unikamy circular import (logging.py importowany przed config.py)
    try:
        from app.core.config import settings as _settings
        _log_dir = log_dir or _settings.log_dir
        _debug = debug if debug else _settings.debug
        _app_env = _settings.app_env.value
    except Exception:
        # Fallback gdy settings nie są dostępne (np. testy)
        _log_dir = log_dir or Path("/app/logs")
        _debug = debug
        _app_env = "development"

    _log_dir.mkdir(parents=True, exist_ok=True)
    log_level = logging.DEBUG if _debug else logging.INFO

    # ---- Formatter ----
    json_formatter = JsonLinesFormatter()

    # ---- Filtr redakcji ----
    redaction_filter = SensitiveDataFilter()

    # ---- Handler: konsola ----
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(json_formatter)
    console_handler.setLevel(log_level)
    console_handler.addFilter(redaction_filter)

    # ---- Handler: plik aplikacji ----
    app_file_handler = NonDeletingTimedRotatingFileHandler(
        log_dir=_log_dir,
        log_name="app",
    )
    app_file_handler.setFormatter(json_formatter)
    app_file_handler.setLevel(log_level)
    app_file_handler.addFilter(redaction_filter)

    # ---- Handler: plik eventów SSE (tylko logger "events") ----
    events_file_handler = NonDeletingTimedRotatingFileHandler(
        log_dir=_log_dir,
        log_name="events",
    )
    # Wymuszamy rozszerzenie .jsonl dla pliku eventów
    events_file_handler.baseFilename = str(_log_dir / "events.jsonl")
    events_file_handler.setFormatter(json_formatter)
    events_file_handler.setLevel(logging.DEBUG)  # Wszystkie eventy
    events_file_handler.addFilter(redaction_filter)

    # ---- Handler: plik workera ARQ ----
    worker_file_handler = NonDeletingTimedRotatingFileHandler(
        log_dir=_log_dir,
        log_name="worker",
    )
    worker_file_handler.setFormatter(json_formatter)
    worker_file_handler.setLevel(log_level)
    worker_file_handler.addFilter(redaction_filter)

    # ── Handler diagnostyczny: faktury (invoices_diag.log) ───────────────────
    invoices_diag_handler = NonDeletingTimedRotatingFileHandler(
        log_dir=_log_dir,
        log_name="invoices_diag",
    )
    invoices_diag_handler.setFormatter(json_formatter)
    invoices_diag_handler.setLevel(logging.DEBUG)
    invoices_diag_handler.addFilter(redaction_filter)

    invoices_diag_logger = logging.getLogger("windykacja.invoices_diag")
    invoices_diag_logger.setLevel(logging.DEBUG)
    invoices_diag_logger.addHandler(invoices_diag_handler)
    invoices_diag_logger.propagate = False

    # ---- Konfiguracja root loggera ----
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Usuń istniejące handlery (uvicorn/gunicorn mogą je dodać przed nami)
    root_logger.handlers.clear()
    root_logger.addHandler(console_handler)
    root_logger.addHandler(app_file_handler)

    # ---- Logger dla eventów SSE ----
    events_logger = logging.getLogger("windykacja.events")
    events_logger.setLevel(logging.DEBUG)
    events_logger.addHandler(events_file_handler)
    events_logger.propagate = True  # Propaguje do root (też do app.log)

    # ---- Logger dla workera ARQ ----
    worker_logger = logging.getLogger("windykacja.worker")
    worker_logger.setLevel(log_level)
    worker_logger.addHandler(worker_file_handler)
    worker_logger.propagate = True

    # ── Handler diagnostyczny: faktury (invoices_diag.log) ───────────────────
    invoices_diag_handler = NonDeletingTimedRotatingFileHandler(
        log_dir=_log_dir,
        log_name="invoices_diag",
    )
    invoices_diag_handler.setFormatter(json_formatter)
    invoices_diag_handler.setLevel(logging.DEBUG)
    invoices_diag_handler.addFilter(redaction_filter)

    invoices_diag_logger = logging.getLogger("windykacja.invoices_diag")
    invoices_diag_logger.setLevel(logging.DEBUG)
    invoices_diag_logger.addHandler(invoices_diag_handler)
    invoices_diag_logger.propagate = False

    # ---- Wyciszenie nadmiernie gadatliwych bibliotek ----
    _configure_third_party_loggers(debug=_debug)

    # Pierwszy log po konfiguracji
    startup_logger = logging.getLogger("windykacja.startup")
    startup_logger.info(
        "System logowania zainicjalizowany",
        extra={
            "log_dir":   str(_log_dir),
            "log_level": logging.getLevelName(log_level),
            "app_env":   _app_env,
            "handlers": [
                "console",
                f"file:{_log_dir}/app.log",
                f"file:{_log_dir}/events.jsonl",
                f"file:{_log_dir}/worker.log",
            ],
        },
    )


def _configure_third_party_loggers(debug: bool) -> None:
    """
    Konfiguruje poziomy logowania dla bibliotek zewnętrznych.

    SQLAlchemy i aioodbc na poziomie WARNING w prod — na DEBUG generują
    absurdalne ilości logów z treścią zapytań SQL (zawierają dane osobowe!).
    W trybie debug włączamy SQLAlchemy na INFO (echo SQL bez parametrów).
    """
    # SQLAlchemy — echo SQL tylko w debug i tylko na INFO (nie DEBUG — tam są bind params)
    sql_level = logging.INFO if debug else logging.WARNING
    logging.getLogger("sqlalchemy.engine").setLevel(sql_level)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.dialects").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.orm").setLevel(logging.WARNING)

    # aioodbc — tylko błędy
    logging.getLogger("aioodbc").setLevel(logging.ERROR)

    # uvicorn — access logi na INFO, inne na WARNING
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.INFO)

    # FastAPI / Starlette
    logging.getLogger("fastapi").setLevel(logging.INFO)
    logging.getLogger("starlette").setLevel(logging.WARNING)

    # Redis/aioredis
    logging.getLogger("redis").setLevel(logging.WARNING)
    logging.getLogger("aioredis").setLevel(logging.WARNING)

    # ARQ worker
    logging.getLogger("arq").setLevel(logging.INFO)

    # httpx (używany wewnętrznie przez FastAPI)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # python-jose
    logging.getLogger("jose").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Specjalizowane funkcje logowania eventów SSE
# ---------------------------------------------------------------------------

_events_logger = logging.getLogger("windykacja.events")


def log_sse_event(
    event_type: str,
    data: Dict[str, Any],
    *,
    user_id: Optional[int] = None,
    channel: Optional[str] = None,
) -> None:
    """
    Loguje zdarzenie SSE do pliku events_YYYY-MM-DD.jsonl.

    Wywoływana z event_service.py przy każdym publikowanym evencie.
    Pliki eventów są nieusuwalne — kompletna historia zdarzeń systemu.

    Args:
        event_type: Typ eventu (task_completed, permissions_updated, ...).
        data:       Dane eventu (payload).
        user_id:    ID użytkownika do którego event był skierowany (None = broadcast).
        channel:    Kanał SSE (None = domyślny).
    """
    _events_logger.info(
        "SSE event: %s",
        event_type,
        extra={
            "event_type": event_type,
            "event_data": data,
            "target_user_id": user_id,
            "channel": channel,
        },
    )


# ---------------------------------------------------------------------------
# Factory — get_logger() jako skrót do logging.getLogger z prefiksem
# ---------------------------------------------------------------------------

def get_logger(name: str) -> logging.Logger:
    """
    Zwraca logger z prefiksem 'windykacja.' dla modułów aplikacji.

    Zapewnia spójną hierarchię loggerów:
        windykacja.api.auth
        windykacja.services.auth_service
        windykacja.db.session
        itp.

    Args:
        name: Nazwa modułu, np. __name__ z modułu wywołującego.

    Returns:
        logging.Logger: Skonfigurowany logger.

    Przykład:
        logger = get_logger(__name__)
        # → logger "windykacja.app.api.auth" jeśli __name__ = "app.api.auth"
    """
    # Jeśli przekazano pełną nazwę z 'app.' — normalizujemy
    if name.startswith("app."):
        name = "windykacja." + name[4:]
    elif not name.startswith("windykacja."):
        name = f"windykacja.{name}"
    return logging.getLogger(name)