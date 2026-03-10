# =============================================================================
# worker/services/dlq_service.py — Dead Letter Queue
# =============================================================================
# Gdy task wyczerpie wszystkie próby → trafia do DLQ w Redis (ZSET).
# DLQ można przeglądać przez Worker API i ręcznie re-enqueue.
# Logi DLQ zapisywane też do pliku: logs/dlq_YYYY-MM-DD.jsonl
# =============================================================================

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

from worker.core.redis_client import KEY_DLQ, get_redis
from worker.settings import get_settings

logger = logging.getLogger("worker.dlq")
_WARSAW = ZoneInfo("Europe/Warsaw")


def _dlq_log_file() -> Path:
    settings = get_settings()
    date_str = datetime.now(_WARSAW).strftime("%Y-%m-%d")
    path = Path(settings.LOG_DIR) / f"dlq_{date_str}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


async def add_to_dlq(
    task_name: str,
    task_kwargs: dict[str, Any],
    job_id: str,
    error_message: str,
    retry_count: int,
    user_id: Optional[int] = None,
) -> None:
    """
    Dodaje failed task do Dead Letter Queue (Redis ZSET).

    Score = timestamp (do sortowania chronologicznego).
    Wartość = JSON z pełnym kontekstem.

    Args:
        task_name:     Nazwa funkcji ARQ
        task_kwargs:   Argumenty oryginalnego taska
        job_id:        ARQ job ID
        error_message: Ostatni błąd
        retry_count:   Ile razy próbowano
        user_id:       ID usera który zlecił (jeśli dostępny)
    """
    now = datetime.now(timezone.utc)
    score = now.timestamp()

    entry: dict[str, Any] = {
        "job_id":       job_id,
        "task_name":    task_name,
        "task_kwargs":  task_kwargs,
        "error":        error_message,
        "retry_count":  retry_count,
        "user_id":      user_id,
        "failed_at":    now.isoformat(),
        "failed_at_pl": now.astimezone(_WARSAW).isoformat(),
        "status":       "dead",
    }

    # ── Zapis do Redis ZSET ────────────────────────────────────────────────────
    try:
        redis = get_redis()
        await redis.zadd(KEY_DLQ, {json.dumps(entry, default=str): score})
        logger.error(
            "Task trafił do DLQ — wyczerpane próby",
            extra={
                "job_id":      job_id,
                "task_name":   task_name,
                "retry_count": retry_count,
                "error":       error_message,
                "user_id":     user_id,
            },
        )
    except Exception as exc:
        logger.critical(
            "Błąd zapisu do DLQ Redis — zapis tylko do pliku",
            extra={"job_id": job_id, "error": str(exc)},
        )

    # ── Zawsze zapis do pliku (redundancja) ──────────────────────────────────
    try:
        with open(_dlq_log_file(), "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
    except Exception as exc:
        logger.critical(
            "Błąd zapisu DLQ do pliku — dane mogą być utracone",
            extra={"job_id": job_id, "error": str(exc)},
        )


async def list_dlq(limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
    """
    Pobiera wpisy z DLQ (posortowane od najnowszych).

    Returns:
        Lista dict z danymi failed tasków.
    """
    redis = get_redis()
    # ZREVRANGE = od najnowszych (najwyższy score = najnowszy timestamp)
    raw_items = await redis.zrevrange(KEY_DLQ, offset, offset + limit - 1, withscores=True)

    result = []
    for raw_value, score in raw_items:
        try:
            entry = json.loads(raw_value)
            entry["_score"] = score
            result.append(entry)
        except json.JSONDecodeError:
            logger.warning("Nieparsywalny wpis DLQ", extra={"raw": str(raw_value)[:100]})
    return result


async def dlq_count() -> int:
    """Zwraca liczbę wpisów w DLQ."""
    redis = get_redis()
    return await redis.zcard(KEY_DLQ)


async def remove_from_dlq(job_id: str) -> bool:
    """
    Usuwa wpis z DLQ (po re-enqueue lub ręcznym rozwiązaniu).

    Returns:
        True jeśli usunięto, False jeśli nie znaleziono.
    """
    redis = get_redis()
    # Musimy przejrzeć ZSET żeby znaleźć matching job_id
    all_items = await redis.zrange(KEY_DLQ, 0, -1)
    for raw_value in all_items:
        try:
            entry = json.loads(raw_value)
            if entry.get("job_id") == job_id:
                removed = await redis.zrem(KEY_DLQ, raw_value)
                if removed:
                    logger.info(
                        "Wpis usunięty z DLQ",
                        extra={"job_id": job_id},
                    )
                    return True
        except json.JSONDecodeError:
            continue
    return False