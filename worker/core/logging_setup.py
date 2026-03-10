# =============================================================================
# worker/core/logging_setup.py — Strukturalne logowanie JSONL
# =============================================================================
# Każdy log = jedna linia JSON z pełnym kontekstem.
# Rotacja dzienna: worker_YYYY-MM-DD.log (nieusuwalne)
# Klucz zasady: jeśli coś się wydarzy, MUSI BYĆ możliwość odtworzenia co/kiedy.
# =============================================================================

from __future__ import annotations

import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from logging.handlers import BaseRotatingHandler
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

_WARSAW = ZoneInfo("Europe/Warsaw")


class _JSONLFormatter(logging.Formatter):
    """
    Formatter → każdy rekord loga to jeden JSON na jednej linii.
    Zawiera maksymalny kontekst: czas, poziom, moduł, linia, message,
    extra pola, stack trace (jeśli wyjątek).
    """

    def format(self, record: logging.LogRecord) -> str:
        now_utc = datetime.fromtimestamp(record.created, tz=timezone.utc)
        now_pl = now_utc.astimezone(_WARSAW)

        entry: dict[str, Any] = {
            # Czas — oba formaty dla wygody
            "ts_utc":    now_utc.isoformat(),
            "ts_warsaw": now_pl.isoformat(),
            "ts_epoch":  record.created,
            # Identyfikacja
            "level":     record.levelname,
            "logger":    record.name,
            "module":    record.module,
            "func":      record.funcName,
            "line":      record.lineno,
            "pid":       os.getpid(),
            # Treść
            "message":   record.getMessage(),
        }

        # Extra pola przekazane via logger.xxx(..., extra={...})
        _STANDARD_ATTRS = {
            "args", "asctime", "created", "exc_info", "exc_text",
            "filename", "funcName", "id", "levelname", "levelno",
            "lineno", "message", "module", "msecs", "msg", "name",
            "pathname", "process", "processName", "relativeCreated",
            "stack_info", "taskName", "thread", "threadName",
        }
        extra = {
            k: v for k, v in record.__dict__.items()
            if k not in _STANDARD_ATTRS and not k.startswith("_")
        }
        if extra:
            entry["extra"] = extra

        # Stack trace
        if record.exc_info:
            entry["exception"] = {
                "type":     record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message":  str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": traceback.format_exception(*record.exc_info),
            }

        if record.stack_info:
            entry["stack_info"] = record.stack_info

        try:
            return json.dumps(entry, ensure_ascii=False, default=str)
        except Exception:
            # Nigdy nie wolno crashować z powodu logowania
            return json.dumps({
                "ts_utc": now_utc.isoformat(),
                "level": "ERROR",
                "message": f"[LOG SERIALIZE ERROR] {record.getMessage()[:200]}",
            })


class _DailyRotatingJSONLHandler(BaseRotatingHandler):
    """
    Handler rotujący pliki dziennie: worker_YYYY-MM-DD.log
    NIEUSUWALNE — append-only.
    """

    def __init__(self, log_dir: str, prefix: str = "worker") -> None:
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._prefix = prefix
        self._current_date = self._today()
        filepath = self._filepath(self._current_date)
        # 'a' = append — nigdy nie nadpisujemy
        super().__init__(str(filepath), mode="a", encoding="utf-8", delay=False)

    def _today(self) -> str:
        return datetime.now(_WARSAW).strftime("%Y-%m-%d")

    def _filepath(self, date_str: str) -> Path:
        return self._log_dir / f"{self._prefix}_{date_str}.log"

    def shouldRollover(self, record: logging.LogRecord) -> bool:
        return self._today() != self._current_date

    def doRollover(self) -> None:
        if self.stream:
            self.stream.close()
            self.stream = None  # type: ignore[assignment]
        self._current_date = self._today()
        self.baseFilename = str(self._filepath(self._current_date))
        self.stream = self._open()


def setup_logging(log_dir: str = "/app/logs", level: str = "DEBUG") -> None:
    """
    Inicjalizuje logowanie dla workera.

    Dwa handlery:
      1. Plik JSONL — rotacja dzienna, poziom DEBUG (pełny zapis)
      2. stdout    — czytelny format dla docker logs, poziom INFO

    Wywołać JEDEN raz przy starcie workera/API.
    """
    log_level = getattr(logging, level.upper(), logging.DEBUG)

    # ── Root logger ───────────────────────────────────────────────────────────
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Usuń domyślne handlery
    root.handlers.clear()

    # ── Handler 1: Plik JSONL ─────────────────────────────────────────────────
    file_handler = _DailyRotatingJSONLHandler(log_dir=log_dir, prefix="worker")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(_JSONLFormatter())
    root.addHandler(file_handler)

    # ── Handler 2: stdout (docker logs) ───────────────────────────────────────
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(console_fmt)
    root.addHandler(console_handler)

    # ── Wycisz hałaśliwe biblioteki ───────────────────────────────────────────
    logging.getLogger("arq").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("aiosmtplib").setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("weasyprint").setLevel(logging.WARNING)

    logger = logging.getLogger("worker.startup")
    logger.info(
        "Logowanie zainicjowane",
        extra={
            "log_dir": log_dir,
            "file_level": "DEBUG",
            "console_level": level,
            "timezone": "Europe/Warsaw",
        },
    )


def get_logger(name: str) -> logging.Logger:
    """Zwraca logger z prefixem 'worker.'"""
    return logging.getLogger(f"worker.{name}")


# ── Audit log do osobnego pliku (events_YYYY-MM-DD.jsonl) ────────────────────

class _WorkerEventLogger:
    """
    Dedykowany logger eventów workera → events_YYYY-MM-DD.jsonl
    Ten sam plik co API events — współdzielony volume /app/logs
    """

    def __init__(self, log_dir: str) -> None:
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)

    def _event_file(self) -> Path:
        date_str = datetime.now(_WARSAW).strftime("%Y-%m-%d")
        return self._log_dir / f"events_{date_str}.jsonl"

    def log(self, event_type: str, data: dict[str, Any], user_id: Optional[int] = None) -> None:
        entry = {
            "ts_utc":     datetime.now(timezone.utc).isoformat(),
            "ts_warsaw":  datetime.now(_WARSAW).isoformat(),
            "source":     "worker",
            "event_type": event_type,
            "user_id":    user_id,
            "data":       data,
        }
        try:
            with open(self._event_file(), "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
        except Exception as e:
            logging.getLogger("worker.event_logger").error(
                "Błąd zapisu eventu do pliku",
                extra={"event_type": event_type, "error": str(e)},
            )


_event_logger_instance: Optional[_WorkerEventLogger] = None


def get_event_logger(log_dir: str = "/app/logs") -> _WorkerEventLogger:
    global _event_logger_instance
    if _event_logger_instance is None:
        _event_logger_instance = _WorkerEventLogger(log_dir)
    return _event_logger_instance