# =============================================================================
# worker/main.py — ARQ WorkerSettings + on_startup / on_shutdown
# =============================================================================
# Punkt wejścia dla ARQ: `python -m arq worker.main.WorkerSettings`
# Rejestruje wszystkie taski i cron jobs.
# =============================================================================

from __future__ import annotations

import logging
import os
from typing import Any

from arq.connections import RedisSettings
from arq.cron import cron

from worker.settings import get_settings
from worker.core.logging_setup import setup_logging, get_event_logger, get_logger
from worker.tasks.email_task import send_bulk_emails
from worker.tasks.sms_task import send_bulk_sms
from worker.tasks.otp_pdf_task import generate_pdf_task, send_otp
from worker.tasks.snapshot_task import daily_snapshot

# ── Inicjalizacja logowania (ZANIM cokolwiek się załaduje) ────────────────────
_settings = get_settings()
setup_logging(log_dir=_settings.LOG_DIR, level="DEBUG")
logger = get_logger("main")


# =============================================================================
# on_startup — wywoływana przez ARQ przy starcie workera
# =============================================================================

async def on_startup(ctx: dict[str, Any]) -> None:
    """
    Inicjalizuje zasoby wspólne dla wszystkich tasków.
    ctx['db']    — async session factory
    ctx['redis'] — redis connection (arq zarządza sam)
    """
    from worker.core.db import init_db
    from worker.core.redis_client import init_redis

    logger.info("=== ARQ Worker startuje ===")
    logger.info(
        "Konfiguracja workera",
        extra={
            "redis_host":   _settings.REDIS_HOST,
            "redis_port":   _settings.REDIS_PORT,
            "db_host":      _settings.DB_HOST,
            "db_name":      _settings.DB_NAME,
            "db_user":      _settings.effective_db_user,
            "max_jobs":     _settings.ARQ_MAX_JOBS,
            "job_timeout":  _settings.ARQ_JOB_TIMEOUT,
            "max_retries":  _settings.TASK_MAX_RETRIES,
            "retry_delays": _settings.retry_delays,
            "log_dir":      _settings.LOG_DIR,
            "snapshot_dir": _settings.SNAPSHOT_DIR,
        },
    )

    # Inicjalizacja DB
    await init_db()
    logger.info("DB zainicjalizowana")

    # Inicjalizacja Redis (osobna pula dla worker tasks, ARQ ma swoją)
    redis = await init_redis()
    ctx["worker_redis"] = redis
    logger.info("Redis worker zainicjalizowany")

    # Health-check: zapisz timestamp startu do Redis (ARQ też to robi, ale dla pewności)
    from datetime import datetime, timezone
    import json
    try:
        await redis.set(
            "windykacja:worker:started_at",
            datetime.now(timezone.utc).isoformat(),
            ex=86400,  # TTL: 24h
        )
    except Exception as exc:
        logger.warning("Nie można zapisać worker:started_at do Redis", extra={"error": str(exc)})

    get_event_logger(_settings.LOG_DIR).log(
        "worker_started",
        {
            "pid":        os.getpid(),
            "max_jobs":   _settings.ARQ_MAX_JOBS,
            "tasks":      ["send_bulk_emails", "send_bulk_sms", "generate_pdf_task", "send_otp", "daily_snapshot"],
        },
    )
    logger.info("=== ARQ Worker gotowy — czekam na zadania ===")


# =============================================================================
# on_shutdown — wywoływana przez ARQ przy zatrzymaniu
# =============================================================================

async def on_shutdown(ctx: dict[str, Any]) -> None:
    """Sprząta zasoby przy shutdown."""
    from worker.core.db import close_db
    from worker.core.redis_client import close_redis
    import json

    logger.info("ARQ Worker zatrzymuje się...")

    try:
        await close_db()
    except Exception as exc:
        logger.error("Błąd zamykania DB", extra={"error": str(exc)})

    try:
        worker_redis = ctx.get("worker_redis")
        if worker_redis:
            await close_redis()
    except Exception as exc:
        logger.error("Błąd zamykania Redis", extra={"error": str(exc)})

    get_event_logger(_settings.LOG_DIR).log("worker_stopped", {"pid": os.getpid()})
    logger.info("ARQ Worker zatrzymany")


# =============================================================================
# WorkerSettings — rejestracja tasków i cronów
# =============================================================================

class WorkerSettings:
    """
    Główna konfiguracja ARQ workera.
    Używana przez `python -m arq worker.main.WorkerSettings`
    """

    # ── Redis connection ──────────────────────────────────────────────────────
    redis_settings = RedisSettings(
        host=_settings.REDIS_HOST,
        port=_settings.REDIS_PORT,
        password=_settings.REDIS_PASSWORD or None,
        database=_settings.REDIS_DB,
        conn_timeout=10,
        conn_retries=5,
        conn_retry_delay=2,
    )

    # ── Zarejestrowane funkcje (taski) ────────────────────────────────────────
    functions = [
        send_bulk_emails,
        send_bulk_sms,
        generate_pdf_task,
        send_otp,
        daily_snapshot,
    ]

    # ── Cron jobs ─────────────────────────────────────────────────────────────
    # ARQ 0.26.1 nie obsługuje tzinfo w cron() — używamy UTC
    # 01:00 UTC = 02:00 Europe/Warsaw (czas zimowy, CET = UTC+1)
    # 00:00 UTC = 02:00 Europe/Warsaw (czas letni, CEST = UTC+2)
    # Kompromis: 01:00 UTC — snapshot odpali się o 02:00 lub 03:00 PL zależnie od sezonu
    cron_jobs = [
        cron(
            daily_snapshot,
            hour=1,
            minute=0,
            timeout=3600,       # Max 1h na snapshot
            unique=True,        # Nie duplikuj jeśli poprzedni jeszcze działa
            run_at_startup=False,
        ),
    ]

    # ── Lifecycle hooks ───────────────────────────────────────────────────────
    on_startup = on_startup
    on_shutdown = on_shutdown

    # ── Parametry workera ─────────────────────────────────────────────────────
    max_jobs: int = _settings.ARQ_MAX_JOBS
    job_timeout: int = _settings.ARQ_JOB_TIMEOUT
    max_tries: int = _settings.TASK_MAX_RETRIES
    health_check_interval: int = _settings.ARQ_HEALTH_CHECK_INTERVAL
    health_check_key: str = "arq:health-check"
    queue_name: str = "arq:queue:default"

    # Jak długo trzymać wyniki zakończonych tasków w Redis
    keep_result: int = 3600        # 1h
    keep_result_forever: bool = False

    # Abort timeout (SIGTERM → SIGKILL delay)
    abort_on_health_check_failure: bool = False