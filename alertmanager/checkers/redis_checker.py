# =============================================================================
# alertmanager/checkers/redis_checker.py
# System Windykacja — Alert Manager — Checker Redis
# =============================================================================

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from checkers.base import BaseChecker
from models.alert import AlertLevel, AlertType, CheckResult, CheckStatus

if TYPE_CHECKING:
    import redis.asyncio as aioredis


class RedisChecker(BaseChecker):
    """Checker dostępności Redis — PING + pomiar latencji."""

    alert_type: str = AlertType.REDIS_DOWN
    checker_name: str = "RedisChecker"
    default_level: AlertLevel = AlertLevel.WARNING
    timeout_seconds: float = 10.0

    async def _perform_check(self, redis_client: "aioredis.Redis") -> CheckResult:
        checked_at = datetime.now(timezone.utc)
        start = time.monotonic()

        try:
            pong = await redis_client.ping()
            latency_ms = (time.monotonic() - start) * 1000

            if not pong:
                return self._make_problem_result(
                    checked_at=checked_at,
                    duration_ms=latency_ms,
                    title="WARNING: Redis PING zwrócił False",
                    message=(
                        "Redis odpowiedział na PING, ale zwrócił nieoczekiwaną wartość. "
                        "Może to wskazywać na problem z autoryzacją lub stanem serwera."
                    ),
                    details={"pong": pong, "latency_ms": round(latency_ms, 2)},
                    level=AlertLevel.WARNING,
                    status=CheckStatus.WARNING,
                )

            # Dodatkowy test: sprawdź info o pamięci
            info = await redis_client.info("memory")
            used_memory_mb = int(info.get("used_memory", 0)) / 1024 / 1024
            max_memory = int(info.get("maxmemory", 0))

            details: dict[str, Any] = {
                "latency_ms": round(latency_ms, 2),
                "used_memory_mb": round(used_memory_mb, 2),
                "maxmemory_bytes": max_memory,
                "redis_version": info.get("redis_version"),
                "connected_clients": info.get("connected_clients"),
            }

            return self._make_ok_result(
                checked_at=checked_at,
                duration_ms=latency_ms,
                message=f"Redis dostępny — latencja {latency_ms:.1f}ms, pamięć {used_memory_mb:.1f}MB",
                details=details,
            )

        except Exception as exc:
            duration_ms = (time.monotonic() - start) * 1000
            return self._make_problem_result(
                checked_at=checked_at,
                duration_ms=duration_ms,
                title="WARNING: Redis niedostępny",
                message=(
                    f"Nie można połączyć się z Redis: {type(exc).__name__}: {exc}. "
                    "Powiadomienia SSE real-time są niedostępne. "
                    "Kolejka zadań ARQ może być zablokowana."
                ),
                details={"error": str(exc), "error_type": type(exc).__name__},
                level=AlertLevel.WARNING,
                status=CheckStatus.WARNING,
            )


# =============================================================================
# alertmanager/checkers/fakir_checker.py
# Checker połączenia z Fakirem (WAPRO write connection)
# =============================================================================

import asyncio
from functools import partial

import pyodbc

from models.alert import AlertType


def _sync_check_fakir(connection_string: str) -> dict[str, Any]:
    """Synchroniczne sprawdzenie połączenia Fakir — w executor."""
    start = time.monotonic()
    result: dict[str, Any] = {
        "ok": False, "latency_ms": None, "error": None,
    }
    try:
        conn = pyodbc.connect(connection_string, autocommit=True)
        try:
            cursor = conn.cursor()
            # Sprawdź tylko dostęp — nie czytaj danych
            cursor.execute(
                "SELECT TOP 0 KOD_STATUSU FROM dbo.BUF_DOKUMENT WHERE 1=0"
            )
            cursor.fetchone()
        finally:
            conn.close()
        result["latency_ms"] = round((time.monotonic() - start) * 1000, 2)
        result["ok"] = True
    except pyodbc.Error as exc:
        result["error"] = f"pyodbc [{getattr(exc, 'args', [exc])[0]}]: {exc}"
        result["latency_ms"] = round((time.monotonic() - start) * 1000, 2)
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["latency_ms"] = round((time.monotonic() - start) * 1000, 2)
    return result


class FakirChecker(BaseChecker):
    """
    Checker połączenia z bazą Fakir (WAPRO ERP).
    Używa osobnego connection string dla Fakira.
    """

    alert_type: str = AlertType.FAKIR_DOWN
    checker_name: str = "FakirChecker"
    default_level: AlertLevel = AlertLevel.WARNING
    timeout_seconds: float = 15.0

    async def _perform_check(self, redis_client: "aioredis.Redis") -> CheckResult:
        checked_at = datetime.now(timezone.utc)
        start = time.monotonic()

        # Buduj connection string Fakira z ustawień
        fakir_cs = getattr(self._settings, "fakir_connection_string", None)
        if not fakir_cs:
            # Jeśli brak konfiguracji Fakira — pomiń checker
            return self._make_ok_result(
                checked_at=checked_at,
                duration_ms=0,
                message="FakirChecker pominięty — brak konfiguracji FAKIR_* w .env",
                details={"skipped": True, "reason": "no_fakir_config"},
            )

        loop = asyncio.get_event_loop()
        fakir_result = await loop.run_in_executor(
            None, partial(_sync_check_fakir, fakir_cs)
        )
        duration_ms = (time.monotonic() - start) * 1000
        details = {
            "latency_ms": fakir_result.get("latency_ms"),
            "error": fakir_result.get("error"),
        }

        if not fakir_result["ok"]:
            return self._make_problem_result(
                checked_at=checked_at,
                duration_ms=duration_ms,
                title="WARNING: Połączenie z Fakirem (WAPRO) niedostępne",
                message=(
                    f"Nie można połączyć się z bazą Fakir: {fakir_result['error']}. "
                    "Akceptacja faktur KSeF będzie niemożliwa."
                ),
                details=details,
                level=AlertLevel.WARNING,
                status=CheckStatus.WARNING,
            )

        return self._make_ok_result(
            checked_at=checked_at,
            duration_ms=duration_ms,
            message=f"Fakir dostępny — latencja {fakir_result['latency_ms']:.1f}ms",
            details=details,
        )


# =============================================================================
# alertmanager/checkers/worker_checker.py
# Checker heartbeatu workera ARQ
# =============================================================================

from models.alert import AlertType, RuntimeConfig


class WorkerChecker(BaseChecker):
    """
    Checker żywotności workera ARQ.

    ARQ zapisuje heartbeat do Redis pod kluczem:
        arq:health:{worker_name}   (TTL = ~60s, odświeżany przez workera)

    Jeśli klucz wygasł → worker jest martwy.
    """

    alert_type: str = AlertType.WORKER_DEAD
    checker_name: str = "WorkerChecker"
    default_level: AlertLevel = AlertLevel.WARNING
    timeout_seconds: float = 10.0

    # Nazwy workerów do sprawdzenia (klucze Redis)
    WORKER_NAMES = ["windykacja_worker"]

    def __init__(self, settings: Any, runtime_config: RuntimeConfig) -> None:
        super().__init__(settings)
        self._runtime_config = runtime_config

    async def _perform_check(self, redis_client: "aioredis.Redis") -> CheckResult:
        checked_at = datetime.now(timezone.utc)
        start = time.monotonic()

        dead_workers: list[str] = []
        alive_workers: list[dict] = []

        for worker_name in self.WORKER_NAMES:
            # Klucz heartbeatu ARQ — sprawdź kilka wariantów nazewnictwa
            keys_to_check = [
                f"arq:health:{worker_name}",
                f"arq:health",               # generyczny fallback
            ]

            found = False
            for key in keys_to_check:
                try:
                    ttl = await redis_client.ttl(key)
                    exists = await redis_client.exists(key)
                    if exists:
                        value = await redis_client.get(key)
                        alive_workers.append({
                            "name": worker_name,
                            "key": key,
                            "ttl_seconds": ttl,
                            "heartbeat_value": str(value)[:200] if value else None,
                        })
                        found = True
                        break
                except Exception as exc:
                    self._logger.warning(
                        "Błąd odczytu klucza heartbeatu %s: %s", key, exc
                    )

            if not found:
                dead_workers.append(worker_name)

        duration_ms = (time.monotonic() - start) * 1000
        details = {
            "checked_workers": self.WORKER_NAMES,
            "alive_workers": alive_workers,
            "dead_workers": dead_workers,
            "heartbeat_timeout_seconds": self._runtime_config.worker_heartbeat_timeout_seconds,
        }

        if dead_workers:
            return self._make_problem_result(
                checked_at=checked_at,
                duration_ms=duration_ms,
                title=f"WARNING: Worker ARQ martwy: {', '.join(dead_workers)}",
                message=(
                    f"Worker(y) ARQ nie odświeżają heartbeatu w Redis: {dead_workers}. "
                    "Zadania asynchroniczne (wysyłka email, SMS, PDF) mogą być zablokowane. "
                    "Sprawdź logi kontenera: docker logs windykacja_worker"
                ),
                details=details,
                level=AlertLevel.WARNING,
                status=CheckStatus.WARNING,
            )

        return self._make_ok_result(
            checked_at=checked_at,
            duration_ms=duration_ms,
            message=f"Worker ARQ aktywny ({len(alive_workers)} workerów)",
            details=details,
        )


# =============================================================================
# alertmanager/checkers/dlq_checker.py
# Checker Dead Letter Queue ARQ
# =============================================================================

from models.alert import AlertType, RuntimeConfig


class DlqChecker(BaseChecker):
    """
    Checker przepełnienia Dead Letter Queue (DLQ) w ARQ.

    ARQ przechowuje nieudane zadania w Redis pod kluczem:
        arq:queue:windykacja_worker:failed  (lub arq:results z status=failed)

    Duża liczba zadań w DLQ = worker gubi pracę.
    """

    alert_type: str = AlertType.DLQ_OVERFLOW
    checker_name: str = "DlqChecker"
    default_level: AlertLevel = AlertLevel.WARNING
    timeout_seconds: float = 10.0

    # Klucze DLQ w Redis (ARQ)
    DLQ_KEYS = [
        "arq:queue:windykacja_worker:failed",
        "arq:queue:failed",
    ]

    def __init__(self, settings: Any, runtime_config: RuntimeConfig) -> None:
        super().__init__(settings)
        self._runtime_config = runtime_config

    async def _perform_check(self, redis_client: "aioredis.Redis") -> CheckResult:
        checked_at = datetime.now(timezone.utc)
        start = time.monotonic()

        total_dlq = 0
        key_counts: dict[str, int] = {}

        for key in self.DLQ_KEYS:
            try:
                count = await redis_client.llen(key)
                key_counts[key] = count
                total_dlq += count
            except Exception as exc:
                self._logger.debug("Klucz DLQ %s niedostępny: %s", key, exc)
                key_counts[key] = -1  # -1 = niedostępny

        duration_ms = (time.monotonic() - start) * 1000
        threshold = self._runtime_config.dlq_overflow_threshold

        details = {
            "total_dlq_count": total_dlq,
            "threshold": threshold,
            "key_counts": key_counts,
        }

        if total_dlq >= threshold:
            return self._make_problem_result(
                checked_at=checked_at,
                duration_ms=duration_ms,
                title=f"WARNING: DLQ przepełniona ({total_dlq} zadań)",
                message=(
                    f"Dead Letter Queue workera ARQ zawiera {total_dlq} nieudanych zadań "
                    f"(próg: {threshold}). "
                    "Oznacza to że system gubi zadania (emaile, SMS, PDF). "
                    "Sprawdź logi: docker logs windykacja_worker"
                ),
                details=details,
                level=AlertLevel.WARNING,
                status=CheckStatus.WARNING,
            )

        return self._make_ok_result(
            checked_at=checked_at,
            duration_ms=duration_ms,
            message=f"DLQ OK — {total_dlq} zadań (próg: {threshold})",
            details=details,
        )


# =============================================================================
# alertmanager/checkers/brute_force_checker.py
# Checker ataków brute-force (z Redis)
# =============================================================================

from models.alert import AlertType, RuntimeConfig


class BruteForceChecker(BaseChecker):
    """
    Checker ataków brute-force na endpoint logowania.

    Sprawdza klucze Redis ustawiane przez auth system:
        auth:failures:{identifier}   — licznik błędów dla IP/username

    Jeśli dowolny licznik > threshold → alert SECURITY.

    UWAGA: Ten checker skanuje WZORZEC kluczy — może być wolniejszy przy
    dużej liczbie wpisów. Timeout jest odpowiednio wyższy.
    """

    alert_type: str = AlertType.BRUTE_FORCE
    checker_name: str = "BruteForceChecker"
    default_level: AlertLevel = AlertLevel.SECURITY
    timeout_seconds: float = 15.0

    # Wzorzec kluczy Redis ustawianych przez auth system
    REDIS_KEY_PATTERN = "auth:failures:*"

    def __init__(self, settings: Any, runtime_config: RuntimeConfig) -> None:
        super().__init__(settings)
        self._runtime_config = runtime_config

    async def _perform_check(self, redis_client: "aioredis.Redis") -> CheckResult:
        checked_at = datetime.now(timezone.utc)
        start = time.monotonic()

        threshold = self._runtime_config.brute_force_threshold
        offenders: list[dict] = []

        try:
            # SCAN zamiast KEYS — nie blokuje Redis
            async for key in redis_client.scan_iter(
                match=self.REDIS_KEY_PATTERN, count=100
            ):
                try:
                    value = await redis_client.get(key)
                    ttl = await redis_client.ttl(key)
                    count = int(value or 0)
                    if count >= threshold:
                        identifier = str(key).replace("auth:failures:", "")
                        offenders.append({
                            "identifier": identifier,
                            "failures": count,
                            "ttl_seconds": ttl,
                            "key": str(key),
                        })
                except (ValueError, TypeError):
                    pass  # Klucz z nieoczekiwaną wartością

        except Exception as exc:
            duration_ms = (time.monotonic() - start) * 1000
            return self._make_ok_result(
                checked_at=checked_at,
                duration_ms=duration_ms,
                message=f"BruteForceChecker: skanowanie niemożliwe ({exc}) — zakładam OK",
                details={"scan_error": str(exc)},
            )

        duration_ms = (time.monotonic() - start) * 1000
        details = {
            "threshold": threshold,
            "offenders_count": len(offenders),
            "offenders": offenders[:20],  # max 20 w logu
            "pattern": self.REDIS_KEY_PATTERN,
        }

        if offenders:
            return self._make_problem_result(
                checked_at=checked_at,
                duration_ms=duration_ms,
                title=f"SECURITY: Wykryto próby brute-force ({len(offenders)} adresów)",
                message=(
                    f"Wykryto {len(offenders)} adres(y) IP/kont z liczbą błędnych logowań "
                    f">= {threshold}. Szczegóły: "
                    + ", ".join(
                        f"{o['identifier']} ({o['failures']} prób)" for o in offenders[:5]
                    )
                    + ("..." if len(offenders) > 5 else "")
                ),
                details=details,
                level=AlertLevel.SECURITY,
                status=CheckStatus.WARNING,
            )

        return self._make_ok_result(
            checked_at=checked_at,
            duration_ms=duration_ms,
            message=f"Brak prób brute-force (próg: {threshold} błędów)",
            details=details,
        )