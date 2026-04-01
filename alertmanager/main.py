# =============================================================================
# alertmanager/main.py
# System Windykacja — Alert Manager — Główna pętla asyncio
#
# ARCHITEKTURA:
#   - Czyste asyncio, BRAK FastAPI
#   - Dwie równoległe ścieżki:
#       A) Pętla periodyczna (co 30s) → wszystkie checkery → throttle → email
#       B) Pub/Sub listener (ciągły) → watchdog tamper → natychmiastowy email
#   - Przeładowanie RuntimeConfig co 5 minut (z SystemConfig w DB)
#   - Rotacyjne logi JSON do /app/logs/alertmanager.json
#   - Graceful shutdown na SIGTERM / SIGINT (Docker stop)
#   - PID file dla Docker HEALTHCHECK
#
# URUCHOMIENIE:
#   python -u main.py     (flag -u = unbuffered, wymagany w Docker)
# =============================================================================

from __future__ import annotations

import asyncio
import json
import logging
import logging.handlers
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import orjson
import redis.asyncio as aioredis

from checkers.brute_force_checker import BruteForceChecker
from checkers.db_checker import DbChecker
from checkers.dlq_checker import DlqChecker
from checkers.redis_checker import (
    FakirChecker,
    RedisChecker,
    WorkerChecker,
)
from checkers.watchdog_pubsub import WatchdogPubSubListener
from config import AlertManagerSettings
from models.alert import (
    AlertEmail,
    AlertLevel,
    AlertState,
    CheckResult,
    CheckStatus,
    RuntimeConfig,
)
from services.alert_log_service import log_alert
from services.config_service import load_runtime_config
from services.smtp_alert_service import (
    build_alert_email_html,
    send_alert_email,
)
from services.throttle_service import ThrottleService


# =============================================================================
# KONFIGURACJA LOGOWANIA — JSON + konsola + plik rotacyjny
# =============================================================================


class JsonFormatter(logging.Formatter):
    """
    Formatter produkujący logi jako JSON — jeden JSON per linia.
    Idealne do parsowania przez Grafana Loki, ELK, splunk.
    """

    SERVICE_NAME = "windykacja_alertmanager"

    def format(self, record: logging.LogRecord) -> str:
        log_obj: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
            "service": self.SERVICE_NAME,
            "pid": os.getpid(),
        }

        # Dodaj extra fields (ze structlogiem kompatybilny)
        extra_keys = (
            set(record.__dict__)
            - logging.LogRecord.__dict__.keys()
            - {"message", "asctime", "args"}
        )
        for key in extra_keys:
            val = record.__dict__[key]
            if isinstance(val, (str, int, float, bool, type(None))):
                log_obj[key] = val
            else:
                try:
                    log_obj[key] = orjson.loads(orjson.dumps(val))
                except Exception:
                    log_obj[key] = str(val)

        # Wyjątek (stack trace) — tylko przy ERROR+
        if record.exc_info:
            import traceback
            log_obj["exception"] = "".join(
                traceback.format_exception(*record.exc_info)
            )

        try:
            return orjson.dumps(log_obj).decode()
        except Exception:
            # Ostatnia deska ratunku — nie chcemy stracić logu przez błąd serializacji
            return json.dumps({"ts": log_obj["ts"], "msg": str(log_obj), "level": "ERROR"})


def setup_logging(settings: AlertManagerSettings) -> None:
    """
    Konfiguruje logging:
        - stderr: czytelne logi (dev) lub JSON (prod)
        - plik: JSON rotacyjny (zawsze)
    """
    log_dir = Path(settings.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Handler 1: stderr — czytelne logi
    console_handler = logging.StreamHandler(sys.stderr)
    if settings.environment == "development":
        console_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-8s %(name)s %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
    else:
        console_handler.setFormatter(JsonFormatter())
    console_handler.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)

    # Handler 2: plik JSON rotacyjny — WSZYSTKIE poziomy
    json_file_handler = logging.handlers.RotatingFileHandler(
        filename=str(log_dir / "alertmanager.json"),
        maxBytes=settings.log_max_bytes,
        backupCount=settings.log_backup_count,
        encoding="utf-8",
    )
    json_file_handler.setFormatter(JsonFormatter())
    json_file_handler.setLevel(logging.DEBUG)
    root_logger.addHandler(json_file_handler)

    # Wycisz hałaśliwe biblioteki
    logging.getLogger("aiosmtplib").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


logger = logging.getLogger("alertmanager.main")


# =============================================================================
# PID FILE
# =============================================================================


def write_pid_file(log_dir: str) -> None:
    """Zapisz PID do pliku — używany przez Docker HEALTHCHECK."""
    pid_path = Path(log_dir) / "alertmanager.pid"
    pid_path.write_text(str(os.getpid()))
    logger.info("PID file zapisany: %s (PID=%d)", pid_path, os.getpid())


def remove_pid_file(log_dir: str) -> None:
    """Usuń PID file przy shutdown."""
    pid_path = Path(log_dir) / "alertmanager.pid"
    try:
        pid_path.unlink(missing_ok=True)
    except Exception:
        pass


# =============================================================================
# GŁÓWNA KLASA ALERT MANAGERA
# =============================================================================


class AlertManager:
    """
    Główna klasa koordynująca działanie Alert Managera.

    Odpowiada za:
        - Inicjalizację połączeń (Redis)
        - Budowanie i uruchamianie checkerów
        - Pętlę periodyczną
        - Pub/Sub listener
        - Przeładowanie konfiguracji
        - Graceful shutdown
    """

    def __init__(self, settings: AlertManagerSettings) -> None:
        self._settings = settings
        self._runtime_config: RuntimeConfig = RuntimeConfig()   # defaults na start
        self._redis: aioredis.Redis | None = None
        self._throttle: ThrottleService | None = None
        self._running = False
        self._start_time = time.monotonic()
        self._cycle_count = 0
        self._pubsub_task: asyncio.Task | None = None
        self._config_reload_task: asyncio.Task | None = None

    # -----------------------------------------------------------------------
    # INICJALIZACJA
    # -----------------------------------------------------------------------

    async def start(self) -> None:
        """Inicjalizacja i start wszystkich komponentów."""
        logger.info(
            "AlertManager START",
            extra={
                "service": self._settings.service_name,
                "environment": self._settings.environment,
                "pid": os.getpid(),
                "check_interval_seconds": self._settings.check_interval_seconds,
                "redis_url": self._settings.redis_url,
                "db_host": self._settings.db_host,
            }
        )

        # Połączenie Redis
        self._redis = aioredis.from_url(
            **self._settings.redis_connection_kwargs,
        )
        # Test połączenia
        try:
            await self._redis.ping()
            logger.info("Redis: połączenie OK")
        except Exception as exc:
            logger.error("Redis: brak połączenia przy starcie: %s", exc)
            # Nie przerywamy — Redis może wrócić po chwili

        self._throttle = ThrottleService(self._redis)

        # Ładowanie RuntimeConfig z DB
        self._runtime_config = await load_runtime_config(
            connection_string=self._settings.db_connection_string,
            fallback_recipients=self._settings.alert_recipients_fallback,
        )

        write_pid_file(self._settings.log_dir)
        self._running = True

        # Uruchom pub/sub listener jako osobny task
        pubsub_listener = WatchdogPubSubListener(
            settings=self._settings,
            redis_client=self._redis,
            on_alert=self._handle_alert,
            runtime_config=self._runtime_config,
        )
        self._pubsub_task = asyncio.create_task(
            pubsub_listener.run(),
            name="watchdog_pubsub_listener",
        )

        # Uruchom przeładowanie konfiguracji
        self._config_reload_task = asyncio.create_task(
            self._config_reload_loop(),
            name="config_reload_loop",
        )

        logger.info(
            "AlertManager gotowy — startuję główną pętlę (interval=%ds)",
            self._settings.check_interval_seconds,
        )

    async def stop(self) -> None:
        """Graceful shutdown."""
        logger.info("AlertManager STOP — graceful shutdown...")
        self._running = False

        if self._pubsub_task and not self._pubsub_task.done():
            self._pubsub_task.cancel()
            try:
                await self._pubsub_task
            except asyncio.CancelledError:
                pass

        if self._config_reload_task and not self._config_reload_task.done():
            self._config_reload_task.cancel()
            try:
                await self._config_reload_task
            except asyncio.CancelledError:
                pass

        if self._redis:
            await self._redis.aclose()
            logger.info("Redis: połączenie zamknięte")

        remove_pid_file(self._settings.log_dir)
        uptime = time.monotonic() - self._start_time
        logger.info(
            "AlertManager zatrzymany — uptime=%.0fs, cykli=%d",
            uptime, self._cycle_count,
        )

    # -----------------------------------------------------------------------
    # GŁÓWNA PĘTLA
    # -----------------------------------------------------------------------

    async def run(self) -> None:
        """Główna pętla periodyczna — uruchamia wszystkie checkery co X sekund."""
        while self._running:
            cycle_start = time.monotonic()
            self._cycle_count += 1
            cycle_id = f"cycle-{self._cycle_count}"

            logger.info(
                "=== Cykl #%d START ===",
                self._cycle_count,
                extra={
                    "cycle_id": cycle_id,
                    "cycle_number": self._cycle_count,
                    "alerts_enabled": self._runtime_config.alerts_enabled,
                    "recipients_count": len(self._runtime_config.recipients),
                    "config_stale": self._runtime_config.is_stale,
                }
            )

            if not self._runtime_config.alerts_enabled:
                logger.info(
                    "Cykl #%d: alerty wyłączone (alerts.enabled=false)",
                    self._cycle_count,
                )
            else:
                # Zbuduj i uruchom checkery równolegle
                checkers = self._build_checkers()
                results = await asyncio.gather(
                    *[checker.check(self._redis) for checker in checkers],
                    return_exceptions=True,
                )

                # Przetwórz wyniki
                for result in results:
                    if isinstance(result, Exception):
                        logger.error(
                            "Checker zwrócił wyjątek (gather): %s",
                            result, exc_info=result,
                        )
                        continue
                    if isinstance(result, CheckResult):
                        await self._process_result(result)

                # Log stanu throttlingu co 5 cykli
                if self._cycle_count % 5 == 0:
                    states = await self._throttle.get_all_states()
                    logger.info(
                        "Stan throttlingu: %d aktywnych alertów",
                        len([s for s in states if s.get("is_firing")]),
                        extra={"throttle_states": states},
                    )

            cycle_duration = time.monotonic() - cycle_start
            logger.info(
                "=== Cykl #%d KONIEC [%.1fms] ===",
                self._cycle_count,
                cycle_duration * 1000,
                extra={
                    "cycle_id": cycle_id,
                    "cycle_duration_ms": round(cycle_duration * 1000, 2),
                }
            )

            # Poczekaj do następnego cyklu (uwzględnij czas wykonania)
            sleep_time = max(
                0,
                self._settings.check_interval_seconds - cycle_duration,
            )
            await asyncio.sleep(sleep_time)

    # -----------------------------------------------------------------------
    # BUDOWANIE CHECKERÓW
    # -----------------------------------------------------------------------

    def _build_checkers(self) -> list:
        """Buduje listę aktywnych checkerów dla bieżącego cyklu."""
        return [
            DbChecker(self._settings, self._runtime_config),
            RedisChecker(self._settings),
            FakirChecker(self._settings),
            WorkerChecker(self._settings, self._runtime_config),
            BruteForceChecker(self._settings, self._runtime_config),
            DlqChecker(self._settings, self._runtime_config),
        ]

    # -----------------------------------------------------------------------
    # PRZETWARZANIE WYNIKÓW
    # -----------------------------------------------------------------------

    async def _process_result(self, result: CheckResult) -> None:
        """
        Przetwarza wynik jednego checkera:
            1. Sprawdź czy to RECOVERY (był firing, teraz OK)
            2. Sprawdź THROTTLE (czy wysłać nowy alert)
            3. Wyślij email (jeśli potrzeba)
            4. Zapisz do AlertLog
        """
        # ── Recovery check ─────────────────────────────────────────────────
        previous_state = await self._throttle.check_recovery(result)
        if previous_state:
            await self._handle_recovery(result, previous_state)
            return

        # ── Jeśli OK i nie ma recovery — nic nie rób ─────────────────────
        if result.is_ok:
            return

        # ── Problem — sprawdź throttle ─────────────────────────────────────
        await self._handle_alert(result)

    async def _handle_alert(self, result: CheckResult) -> None:
        """Obsłuż alert (problem) — throttle → email → log."""
        should_send = await self._throttle.should_send_alert(
            result,
            cooldown_minutes=self._runtime_config.cooldown_minutes,
        )

        email_sent = None

        if should_send and self._runtime_config.recipients:
            email_sent = await self._send_alert_email(
                result=result,
                recipients=self._runtime_config.recipients,
                is_recovery=False,
                previous_state=None,
            )
            await self._throttle.register_alert_sent(
                result,
                cooldown_minutes=self._runtime_config.cooldown_minutes,
            )

        # Zawsze zapisz do AlertLog (nawet gdy throttle zablokował email)
        await log_alert(
            connection_string=self._settings.db_connection_string,
            result=result,
            alert_email=email_sent,
            is_recovery=False,
        )

    async def _handle_recovery(
        self,
        result: CheckResult,
        previous_state: AlertState,
    ) -> None:
        """Obsłuż recovery — wyślij email odzyskania + wyczyść state."""
        logger.info(
            "Recovery dla '%s' — wysyłam email odzyskania",
            result.alert_type,
        )

        email_sent = None
        if self._runtime_config.recipients:
            email_sent = await self._send_alert_email(
                result=result,
                recipients=self._runtime_config.recipients,
                is_recovery=True,
                previous_state=previous_state,
            )

        await self._throttle.register_recovery(result)

        await log_alert(
            connection_string=self._settings.db_connection_string,
            result=result,
            alert_email=email_sent,
            is_recovery=True,
        )

    # -----------------------------------------------------------------------
    # WYSYŁKA EMAIL
    # -----------------------------------------------------------------------

    async def _send_alert_email(
        self,
        result: CheckResult,
        recipients: list[str],
        is_recovery: bool,
        previous_state: AlertState | None,
    ) -> AlertEmail:
        """Buduje i wysyła email alertu."""
        subject, html_body, text_body = build_alert_email_html(
            result=result,
            recipients=recipients,
            is_recovery=is_recovery,
            previous_state=previous_state,
            service_name=self._settings.service_name,
            environment=self._settings.environment,
        )

        alert_email = AlertEmail(
            recipients=recipients,
            subject=subject,
            html_body=html_body,
            text_body=text_body,
            result=result,
            is_recovery=is_recovery,
        )

        return await send_alert_email(alert_email, self._settings)

    # -----------------------------------------------------------------------
    # PRZEŁADOWANIE KONFIGURACJI
    # -----------------------------------------------------------------------

    async def _config_reload_loop(self) -> None:
        """Co config_reload_interval_seconds przeładuj RuntimeConfig z DB."""
        while self._running:
            await asyncio.sleep(self._settings.config_reload_interval_seconds)
            logger.info("Przeładowuję RuntimeConfig z SystemConfig...")
            try:
                new_config = await load_runtime_config(
                    connection_string=self._settings.db_connection_string,
                    fallback_recipients=self._settings.alert_recipients_fallback,
                )
                self._runtime_config = new_config
                logger.info(
                    "RuntimeConfig przeładowany — recipients=%d, cooldown=%dmin",
                    len(new_config.recipients),
                    new_config.cooldown_minutes,
                )
            except Exception as exc:
                logger.error(
                    "Błąd przeładowania RuntimeConfig: %s — używam poprzedniej wersji",
                    exc,
                )


# =============================================================================
# ENTRY POINT
# =============================================================================


async def main() -> None:
    """Główna funkcja asyncio."""
    settings = AlertManagerSettings()
    setup_logging(settings)

    logger.info(
        "=" * 60 + "\n"
        "  System Windykacja — Alert Manager\n"
        "  Środowisko: %s\n"
        "  PID: %d\n"
        + "=" * 60,
        settings.environment,
        os.getpid(),
    )

    manager = AlertManager(settings)

    # Graceful shutdown handlers
    loop = asyncio.get_event_loop()
    shutdown_event = asyncio.Event()

    def _signal_handler(sig: int) -> None:
        sig_name = signal.Signals(sig).name
        logger.warning("Otrzymano sygnał %s — inicjuję graceful shutdown", sig_name)
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler, sig)

    try:
        await manager.start()

        # Uruchom główną pętlę i shutdown event równolegle
        main_task = asyncio.create_task(manager.run(), name="main_loop")
        shutdown_task = asyncio.create_task(shutdown_event.wait(), name="shutdown_waiter")

        done, pending = await asyncio.wait(
            [main_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    except Exception as exc:
        logger.critical(
            "Krytyczny błąd AlertManagera: %s",
            exc, exc_info=True,
        )
        raise
    finally:
        await manager.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        sys.exit(1)