# =============================================================================
# alertmanager/checkers/db_checker.py
# System Windykacja — Alert Manager — Checker bazy MSSQL
#
# Sprawdza:
#   1. Dostępność bazy MSSQL (SELECT 1)
#   2. Latencję — alert gdy > db_latency_warn_ms (z RuntimeConfig)
#
# Połączenie: pyodbc w asyncio.run_in_executor (synchroniczny sterownik)
# =============================================================================

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, Any

import pyodbc

from checkers.base import BaseChecker
from models.alert import AlertLevel, AlertType, CheckResult, CheckStatus, RuntimeConfig

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


def _sync_check_db(connection_string: str) -> dict[str, Any]:
    """
    Synchroniczne sprawdzenie DB — uruchamiane w thread executor.

    Returns dict z polami:
        ok, latency_ms, error, server_version, db_name
    """
    start = time.monotonic()
    result: dict[str, Any] = {
        "ok": False,
        "latency_ms": None,
        "error": None,
        "server_version": None,
        "db_name": None,
        "row_count": None,
    }

    try:
        conn = pyodbc.connect(connection_string, autocommit=True)
        try:
            cursor = conn.cursor()

            # Sprawdzenie 1: podstawowe SELECT 1
            cursor.execute("SELECT 1 AS ping")
            row = cursor.fetchone()
            assert row and row[0] == 1, "SELECT 1 zwróciło nieoczekiwany wynik"

            # Sprawdzenie 2: wersja serwera + nazwa DB
            cursor.execute(
                "SELECT @@VERSION AS ver, DB_NAME() AS db_name, "
                "COUNT(*) AS table_count "
                "FROM sys.tables WHERE type='U'"
            )
            meta = cursor.fetchone()
            result["server_version"] = (meta.ver or "")[:100] if meta else None
            result["db_name"] = meta.db_name if meta else None
            result["row_count"] = meta.table_count if meta else None

        finally:
            conn.close()

        result["latency_ms"] = round((time.monotonic() - start) * 1000, 2)
        result["ok"] = True

    except pyodbc.OperationalError as exc:
        result["error"] = f"OperationalError: {exc}"
        result["latency_ms"] = round((time.monotonic() - start) * 1000, 2)
    except pyodbc.Error as exc:
        result["error"] = f"pyodbc.Error [{exc.args[0]}]: {exc.args[1] if len(exc.args) > 1 else exc}"
        result["latency_ms"] = round((time.monotonic() - start) * 1000, 2)
    except Exception as exc:
        result["error"] = f"Nieoczekiwany błąd: {type(exc).__name__}: {exc}"
        result["latency_ms"] = round((time.monotonic() - start) * 1000, 2)

    return result


class DbChecker(BaseChecker):
    """
    Checker dostępności bazy MSSQL.

    Alerty:
        - DB_DOWN (CRITICAL)    — baza niedostępna
        - DB_HIGH_LATENCY (WARNING) — latencja > progu
    """

    alert_type: str = AlertType.DB_DOWN
    checker_name: str = "DbChecker"
    default_level: AlertLevel = AlertLevel.CRITICAL
    timeout_seconds: float = 15.0

    def __init__(self, settings: Any, runtime_config: RuntimeConfig) -> None:
        super().__init__(settings)
        self._runtime_config = runtime_config

    async def _perform_check(self, redis_client: "aioredis.Redis") -> CheckResult:
        checked_at = datetime.now(timezone.utc)
        start = time.monotonic()

        loop = asyncio.get_event_loop()
        sync_fn = partial(_sync_check_db, self._settings.db_connection_string)
        db_result = await loop.run_in_executor(None, sync_fn)

        duration_ms = (time.monotonic() - start) * 1000

        details = {
            "host": self._settings.db_host,
            "port": self._settings.db_port,
            "db_name": db_result.get("db_name"),
            "server_version_short": (db_result.get("server_version") or "")[:80],
            "latency_ms": db_result.get("latency_ms"),
            "table_count": db_result.get("row_count"),
            "error": db_result.get("error"),
        }

        # ── Przypadek 1: Baza niedostępna ─────────────────────────────────
        if not db_result["ok"]:
            return self._make_problem_result(
                checked_at=checked_at,
                duration_ms=duration_ms,
                title="CRITICAL: Baza danych MSSQL niedostępna",
                message=(
                    f"Nie można połączyć się z bazą danych {self._settings.db_host}. "
                    f"Błąd: {db_result['error']}. "
                    "System windykacji nie może działać bez bazy danych."
                ),
                details=details,
                level=AlertLevel.CRITICAL,
                status=CheckStatus.CRITICAL,
            )

        # ── Przypadek 2: Wysoka latencja ───────────────────────────────────
        latency = db_result.get("latency_ms", 0) or 0
        warn_threshold = self._runtime_config.db_latency_warn_ms

        if latency > warn_threshold:
            return self._make_problem_result(
                checked_at=checked_at,
                duration_ms=duration_ms,
                title=f"WARNING: Wysoka latencja bazy MSSQL ({latency:.0f}ms)",
                message=(
                    f"Baza danych odpowiada wolno: {latency:.1f}ms "
                    f"(próg: {warn_threshold:.0f}ms). "
                    "Może to wskazywać na przeciążenie serwera lub problemy sieciowe."
                ),
                details=details,
                level=AlertLevel.WARNING,
                status=CheckStatus.WARNING,
            )

        # ── Przypadek 3: OK ────────────────────────────────────────────────
        return self._make_ok_result(
            checked_at=checked_at,
            duration_ms=duration_ms,
            message=f"Baza MSSQL dostępna — latencja {latency:.1f}ms",
            details=details,
        )