# worker/core/job_tracker.py
"""
Uniwersalny tracker zadan ARQ — F7.

Dekorator @track_job owija KAZDA funkcje w WorkerSettings.functions,
zapisujac jej cykl zycia (queued -> running -> success/failed) do
skw_ArqJobRegistry. Zero zmian wymaganych w samych taskach.

Uzycie w worker/main.py:

    from worker.core.job_tracker import track_job

    @track_job("send_bulk_emails")
    async def send_bulk_emails(ctx, *args, **kwargs):
        ...

LUB prosciej — owinij liste functions przy rejestracji w WorkerSettings:

    from worker.core.job_tracker import track_job
    from worker.tasks.email_task import send_bulk_emails as _send_bulk_emails
    send_bulk_emails = track_job("send_bulk_emails")(_send_bulk_emails)

    class WorkerSettings:
        functions = [send_bulk_emails, ...]

Filozofia bledow: kazdy blad zapisu do skw_ArqJobRegistry jest logowany
i POCHŁANIANY — tracker nigdy nie moze zepsuc faktycznego wykonania taska.
Lepiej stracic jeden wpis w rejestrze niz zablokowac wysylke maili.

UWAGA: from __future__ import annotations OK w tym pliku (nie ORM, nie router).
"""

from __future__ import annotations

import functools
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, TypeVar

logger = logging.getLogger("worker.core.job_tracker")

_SCHEMA = "dbo"

F = TypeVar("F", bound=Callable[..., Awaitable[Any]])

# Maksymalna dlugosc JSON wyniku zapisywanego do result_summary —
# unikamy NVARCHAR(MAX) zapchanego ogromnymi payloadami
_MAX_RESULT_SUMMARY_CHARS = 2000


def track_job(task_name: str) -> Callable[[F], F]:
    """
    Dekorator fabryki — owija funkcje taska ARQ trackingiem w skw_ArqJobRegistry.

    Args:
        task_name: Nazwa zapisywana w kolumnie task_name (np. 'send_bulk_emails').
                   Niezalezna od nazwy funkcji Python — pozwala na czytelne
                   nazwy w rejestrze nawet jesli funkcja ma inna nazwe wewnetrznie.

    Dziala dla funkcji ARQ o sygnaturze async def fn(ctx, *args, **kwargs).
    ctx['job_id'] (gdy dostepne) jest zapisywany jako job_id w rejestrze.
    """
    def decorator(fn: F) -> F:
        @functools.wraps(fn)
        async def wrapper(ctx: dict[str, Any], *args: Any, **kwargs: Any) -> Any:
            arq_job_id = str(ctx.get("job_id", "")) or None
            triggered_by = _infer_triggered_by(kwargs)

            id_job = await _record_enqueued(task_name, arq_job_id, triggered_by)
            t_start = time.monotonic()
            await _record_started(id_job)

            try:
                result = await fn(ctx, *args, **kwargs)
                duration_ms = round((time.monotonic() - t_start) * 1000)
                await _record_success(id_job, result, duration_ms)
                return result

            except Exception as exc:
                duration_ms = round((time.monotonic() - t_start) * 1000)
                await _record_failure(id_job, exc, duration_ms)
                raise  # NIGDY nie pochłaniaj wyjątku z faktycznego taska — ARQ musi wiedzieć o błędzie

        return wrapper  # type: ignore[return-value]
    return decorator


def _infer_triggered_by(kwargs: dict[str, Any]) -> str:
    """
    Najlepsza próba odgadnięcia kto/co wywołało zadanie.

    Konwencja w projekcie: wiele tasków przyjmuje triggered_by_user_id.
    Brak tego kwarg = prawdopodobnie cron.
    """
    user_id = kwargs.get("triggered_by_user_id") or kwargs.get("actor_id")
    if user_id is not None:
        return f"user:{user_id}"
    return "cron"


async def _get_session():
    """Lazy import — unika circular imports przy starcie modułu."""
    from worker.core.db import get_session
    return get_session()


async def _record_enqueued(task_name: str, arq_job_id: str | None, triggered_by: str) -> int | None:
    """
    Tworzy rekord ze statusem 'queued'. Zwraca id_job (PK) do dalszych aktualizacji.
    Zwraca None jesli zapis się nie powiódł — kolejne wywołania _record_* są no-op.
    """
    try:
        from sqlalchemy import text
        async with await _get_session() as db:
            result = await db.execute(
                text(f"""
                    INSERT INTO [{_SCHEMA}].[skw_ArqJobRegistry] (
                        [job_id], [task_name], [status], [enqueued_at], [triggered_by]
                    )
                    OUTPUT INSERTED.[id_job]
                    VALUES (:job_id, :task_name, N'queued', SYSUTCDATETIME(), :triggered_by)
                """),
                {"job_id": arq_job_id, "task_name": task_name, "triggered_by": triggered_by},
            )
            row = result.fetchone()
            await db.commit()
            return int(row[0]) if row else None
    except Exception as exc:
        logger.error("job_tracker._record_enqueued: blad zapisu dla task=%s: %s", task_name, exc)
        return None


async def _record_started(id_job: int | None) -> None:
    if id_job is None:
        return
    try:
        from sqlalchemy import text
        async with await _get_session() as db:
            await db.execute(
                text(f"""
                    UPDATE [{_SCHEMA}].[skw_ArqJobRegistry]
                    SET [status] = N'running', [started_at] = SYSUTCDATETIME()
                    WHERE [id_job] = :id
                """),
                {"id": id_job},
            )
            await db.commit()
    except Exception as exc:
        logger.error("job_tracker._record_started: blad dla id_job=%s: %s", id_job, exc)


async def _record_success(id_job: int | None, result: Any, duration_ms: int) -> None:
    if id_job is None:
        return
    try:
        summary = _summarize_result(result)
        from sqlalchemy import text
        async with await _get_session() as db:
            await db.execute(
                text(f"""
                    UPDATE [{_SCHEMA}].[skw_ArqJobRegistry]
                    SET [status] = N'success', [finished_at] = SYSUTCDATETIME(),
                        [duration_ms] = :dur, [result_summary] = :summary
                    WHERE [id_job] = :id
                """),
                {"id": id_job, "dur": duration_ms, "summary": summary},
            )
            await db.commit()
    except Exception as exc:
        logger.error("job_tracker._record_success: blad dla id_job=%s: %s", id_job, exc)


async def _record_failure(id_job: int | None, exc: Exception, duration_ms: int) -> None:
    if id_job is None:
        return
    try:
        error_msg = f"{type(exc).__name__}: {exc}"[:500]
        from sqlalchemy import text
        async with await _get_session() as db:
            await db.execute(
                text(f"""
                    UPDATE [{_SCHEMA}].[skw_ArqJobRegistry]
                    SET [status] = N'failed', [finished_at] = SYSUTCDATETIME(),
                        [duration_ms] = :dur, [error_message] = :err
                    WHERE [id_job] = :id
                """),
                {"id": id_job, "dur": duration_ms, "err": error_msg},
            )
            await db.commit()
    except Exception as log_exc:
        logger.error(
            "job_tracker._record_failure: blad zapisu dla id_job=%s (oryginalny blad taska: %s): %s",
            id_job, exc, log_exc,
        )


def _summarize_result(result: Any) -> str | None:
    """
    Serializuje wynik taska do JSON, obciety do _MAX_RESULT_SUMMARY_CHARS.
    Nie-serializowalne wyniki -> string reprezentacja.
    """
    if result is None:
        return None
    try:
        text_repr = json.dumps(result, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        text_repr = str(result)
    return text_repr[:_MAX_RESULT_SUMMARY_CHARS]