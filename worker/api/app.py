# =============================================================================
# worker/api/app.py — Worker REST API (FastAPI, port 8001)
# =============================================================================
# Zabezpieczony X-Worker-Key header (shared secret).
# Endpointy: /health, /queue, /tasks, /tasks/{id}, /cancel/{id}, /dlq
# =============================================================================

from __future__ import annotations

import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional

import psutil
from fastapi import Depends, FastAPI, HTTPException, Header, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from worker.settings import get_settings
from worker.core.logging_setup import setup_logging, get_logger
from worker.core.redis_client import (
    ARQ_QUEUE_KEY, KEY_DLQ, KEY_TASK_RESULTS,
    init_redis, close_redis, get_redis,
)
from worker.core.db import init_db, close_db
from worker.services.dlq_service import list_dlq, dlq_count, remove_from_dlq

_settings = get_settings()
logger = get_logger("api")

_API_START_TIME = time.time()


# =============================================================================
# Lifespan
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicjalizacja zasobów przy starcie API."""
    logger.info("Worker API startuje (port 8001)")
    await init_redis()
    await init_db()
    logger.info("Worker API gotowy")
    yield
    logger.info("Worker API zatrzymuje się")
    await close_redis()
    await close_db()


# =============================================================================
# FastAPI App
# =============================================================================

app = FastAPI(
    title="Windykacja Worker API",
    description="Monitoring i zarządzanie ARQ workerem",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Worker API powinien być dostępny tylko wewnętrznie
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)


# =============================================================================
# Request logging middleware
# =============================================================================

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.monotonic()
    response = await call_next(request)
    duration_ms = (time.monotonic() - start) * 1000

    logger.info(
        "Worker API request",
        extra={
            "method":      request.method,
            "path":        request.url.path,
            "status_code": response.status_code,
            "duration_ms": round(duration_ms, 2),
            "client_ip":   request.client.host if request.client else "unknown",
            "has_auth":    "x-worker-key" in request.headers,
        },
    )
    return response


# =============================================================================
# Autoryzacja — X-Worker-Key header
# =============================================================================

def verify_worker_key(x_worker_key: Optional[str] = Header(default=None)) -> None:
    """
    Dependency: waliduje X-Worker-Key header.
    Używana we wszystkich chronionych endpointach.
    """
    if not x_worker_key:
        logger.warning("Żądanie bez X-Worker-Key")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="X-Worker-Key header wymagany",
            headers={"WWW-Authenticate": "X-Worker-Key"},
        )
    if x_worker_key != _settings.WORKER_SECRET_KEY:
        logger.warning(
            "Nieprawidłowy X-Worker-Key",
            extra={"provided_prefix": x_worker_key[:8] + "..."},
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Nieprawidłowy X-Worker-Key",
        )


# Skrócony alias dla Depends
RequireKey = Depends(verify_worker_key)


# =============================================================================
# Modele odpowiedzi
# =============================================================================

class HealthResponse(BaseModel):
    status: str
    uptime_seconds: float
    worker_online: bool
    redis_ok: bool
    db_ok: bool
    queue_length: int
    dlq_count: int
    pid: int
    memory_mb: float
    cpu_percent: float
    ts: str


class QueueResponse(BaseModel):
    queue_name: str
    queued_jobs: int
    worker_online: bool
    worker_last_heartbeat: Optional[str]
    jobs: list[dict]


class TaskResultResponse(BaseModel):
    job_id: str
    function: str
    status: str
    enqueued_at: Optional[str]
    started_at: Optional[str]
    finished_at: Optional[str]
    result: Optional[Any]
    error: Optional[str]


class DlqResponse(BaseModel):
    total: int
    items: list[dict]


# =============================================================================
# Endpointy
# =============================================================================

@app.get("/health", response_model=HealthResponse, tags=["monitoring"])
async def health():
    """
    Health check — publiczny (bez X-Worker-Key).
    Używany przez Docker healthcheck i load balancer.
    """
    redis_ok = False
    worker_online = False
    queue_length = 0
    dlq_cnt = 0

    try:
        redis = get_redis()
        await redis.ping()
        redis_ok = True

        # ARQ health-check key (worker pisze co N sekund)
        hc = await redis.get("arq:health-check")
        worker_online = hc is not None

        # Długość kolejki ARQ (ZSET)
        queue_length = await redis.zcard(ARQ_QUEUE_KEY)
        dlq_cnt = await dlq_count()

    except Exception as exc:
        logger.warning("Health check: Redis error", extra={"error": str(exc)})

    db_ok = False
    try:
        from worker.core.db import get_engine
        from sqlalchemy import text
        engine = get_engine()
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        db_ok = True
    except Exception as exc:
        logger.warning("Health check: DB error", extra={"error": str(exc)})

    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / 1024 / 1024
    cpu_pct = process.cpu_percent(interval=0.1)
    uptime = time.time() - _API_START_TIME

    overall_status = "ok" if (redis_ok and db_ok and worker_online) else "degraded"
    if not redis_ok or not db_ok:
        overall_status = "error"

    return HealthResponse(
        status=overall_status,
        uptime_seconds=round(uptime, 1),
        worker_online=worker_online,
        redis_ok=redis_ok,
        db_ok=db_ok,
        queue_length=queue_length,
        dlq_count=dlq_cnt,
        pid=os.getpid(),
        memory_mb=round(memory_mb, 1),
        cpu_percent=round(cpu_pct, 1),
        ts=datetime.now(timezone.utc).isoformat(),
    )


@app.get("/worker/queue", response_model=QueueResponse, tags=["queue"], dependencies=[RequireKey])
async def get_queue():
    """
    Stan kolejki ARQ — liczba oczekujących zadań, lista jobów.
    """
    redis = get_redis()

    worker_hb = await redis.get("arq:health-check")
    worker_online = worker_hb is not None
    worker_last_hb = None
    if worker_hb:
        try:
            hb_data = json.loads(worker_hb)
            worker_last_hb = hb_data.get("time") or str(worker_hb)
        except Exception:
            worker_last_hb = str(worker_hb)

    # Pobierz joby z kolejki (ZSET: score = scheduled timestamp)
    raw_jobs = await redis.zrange(ARQ_QUEUE_KEY, 0, 99, withscores=True)
    jobs = []
    for raw_val, score in raw_jobs:
        try:
            job_data = json.loads(raw_val) if isinstance(raw_val, str) else {}
            jobs.append({
                "job_id":       job_data.get("job_id", "?"),
                "function":     job_data.get("function", "?"),
                "enqueued_at":  job_data.get("enqueue_time", "?"),
                "score":        score,
            })
        except Exception:
            jobs.append({"raw": str(raw_val)[:100], "score": score})

    queue_length = await redis.zcard(ARQ_QUEUE_KEY)

    return QueueResponse(
        queue_name=ARQ_QUEUE_KEY,
        queued_jobs=queue_length,
        worker_online=worker_online,
        worker_last_heartbeat=worker_last_hb,
        jobs=jobs,
    )


@app.get("/worker/tasks", tags=["tasks"], dependencies=[RequireKey])
async def list_tasks(limit: int = 50, offset: int = 0):
    """
    Lista zakończonych zadań — wyniki przechowywane w Redis przez keep_result sekund.
    """
    redis = get_redis()

    # ARQ trzyma wyniki jako: arq:result:{job_id}
    keys = await redis.keys("arq:result:*")
    total = len(keys)

    tasks = []
    for key in keys[offset: offset + limit]:
        try:
            raw = await redis.get(key)
            if raw:
                data = json.loads(raw)
                tasks.append({
                    "job_id":      key.replace("arq:result:", ""),
                    "function":    data.get("function", "?"),
                    "status":      data.get("status", "?"),
                    "enqueue_time": data.get("enqueue_time"),
                    "start_time":  data.get("start_time"),
                    "finish_time": data.get("finish_time"),
                    "job_try":     data.get("job_try"),
                    "success":     data.get("success"),
                })
        except Exception:
            pass

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": tasks,
        "ts": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/worker/tasks/{job_id}", tags=["tasks"], dependencies=[RequireKey])
async def get_task(job_id: str):
    """Pobiera szczegóły konkretnego zadania po job_id."""
    redis = get_redis()
    raw = await redis.get(f"arq:result:{job_id}")
    if not raw:
        raise HTTPException(status_code=404, detail=f"Task {job_id!r} nie znaleziony")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"job_id": job_id, "raw": str(raw)[:500]}


@app.post("/worker/cancel/{job_id}", tags=["tasks"], dependencies=[RequireKey])
async def cancel_task(job_id: str):
    """
    Anuluje zaplanowane zadanie (usunięcie z kolejki ARQ).
    Uwaga: nie można anulować zadania które już się wykonuje.
    """
    redis = get_redis()

    # Szukaj w kolejce ARQ (ZSET)
    raw_jobs = await redis.zrange(ARQ_QUEUE_KEY, 0, -1)
    removed = 0
    for raw_val in raw_jobs:
        try:
            job_data = json.loads(raw_val) if isinstance(raw_val, str) else {}
            if job_data.get("job_id") == job_id:
                removed = await redis.zrem(ARQ_QUEUE_KEY, raw_val)
                break
        except Exception:
            continue

    if removed:
        logger.info("Task anulowany z kolejki", extra={"job_id": job_id})
        return {"cancelled": True, "job_id": job_id, "ts": datetime.now(timezone.utc).isoformat()}
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Task {job_id!r} nie znaleziony w kolejce (może już się wykonał lub nie istnieje)",
        )


@app.get("/worker/dlq", response_model=DlqResponse, tags=["dlq"], dependencies=[RequireKey])
async def get_dlq(limit: int = 50, offset: int = 0):
    """Dead Letter Queue — zadania które wyczerpały wszystkie próby."""
    items = await list_dlq(limit=limit, offset=offset)
    total = await dlq_count()
    return DlqResponse(total=total, items=items)


@app.delete("/worker/dlq/{job_id}", tags=["dlq"], dependencies=[RequireKey])
async def delete_dlq_item(job_id: str):
    """Usuwa wpis z DLQ (po manualnym rozwiązaniu problemu)."""
    removed = await remove_from_dlq(job_id)
    if not removed:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} nie znaleziony w DLQ")
    logger.info("DLQ item usunięty", extra={"job_id": job_id})
    return {"deleted": True, "job_id": job_id}


@app.post("/worker/dlq/{job_id}/retry", tags=["dlq"], dependencies=[RequireKey])
async def retry_dlq_item(job_id: str):
    """
    Re-enqueue zadania z DLQ — ponowna próba po manualnej interwencji.
    """
    from arq import create_pool
    from arq.connections import RedisSettings

    redis = get_redis()

    # Znajdź wpis w DLQ
    all_items = await list_dlq(limit=10000)
    target = next((item for item in all_items if item.get("job_id") == job_id), None)

    if not target:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} nie znaleziony w DLQ")

    # Re-enqueue przez ARQ
    arq_pool = await create_pool(RedisSettings(
        host=_settings.REDIS_HOST,
        port=_settings.REDIS_PORT,
        password=_settings.REDIS_PASSWORD or None,
        database=_settings.REDIS_DB,
    ))

    new_job = await arq_pool.enqueue_job(
        target["task_name"],
        **target.get("task_kwargs", {}),
    )
    await arq_pool.aclose()

    # Usuń z DLQ
    await remove_from_dlq(job_id)

    logger.info(
        "DLQ task re-enqueued",
        extra={"old_job_id": job_id, "new_job_id": str(new_job.job_id) if new_job else "?"},
    )
    return {
        "re_enqueued": True,
        "old_job_id": job_id,
        "new_job_id": str(new_job.job_id) if new_job else None,
        "ts": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/worker/stats", tags=["monitoring"], dependencies=[RequireKey])
async def get_stats():
    """Rozszerzone statystyki workera — kolejka, DLQ, Redis, system."""
    redis = get_redis()

    queue_len = await redis.zcard(ARQ_QUEUE_KEY)
    dlq_cnt = await dlq_count()

    # Wyniki z ostatniej godziny
    result_keys = await redis.keys("arq:result:*")
    success_count = 0
    failed_count = 0
    for key in result_keys:
        try:
            raw = await redis.get(key)
            if raw:
                data = json.loads(raw)
                if data.get("success") is True:
                    success_count += 1
                elif data.get("success") is False:
                    failed_count += 1
        except Exception:
            pass

    # System info
    process = psutil.Process(os.getpid())
    mem = process.memory_info()

    return {
        "queue": {
            "queued_jobs": queue_len,
            "dlq_items": dlq_cnt,
        },
        "recent_results": {
            "total": len(result_keys),
            "success": success_count,
            "failed": failed_count,
        },
        "system": {
            "pid": os.getpid(),
            "memory_rss_mb": round(mem.rss / 1024 / 1024, 1),
            "memory_vms_mb": round(mem.vms / 1024 / 1024, 1),
            "cpu_percent": process.cpu_percent(interval=0.1),
            "uptime_seconds": round(time.time() - _API_START_TIME, 1),
            "open_files": len(process.open_files()),
        },
        "ts": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# Global exception handler
# =============================================================================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(
        "Nieobsłużony wyjątek w Worker API",
        extra={
            "path":   request.url.path,
            "method": request.method,
            "error":  str(exc),
            "type":   type(exc).__name__,
        },
        exc_info=True,
    )
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Błąd wewnętrzny serwera",
            "type":   type(exc).__name__,
            "ts":     datetime.now(timezone.utc).isoformat(),
        },
    )