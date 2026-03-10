# =============================================================================
# worker/tasks/sms_task.py — ARQ Task: Masowa wysyłka SMS
# =============================================================================

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from arq import Retry
from sqlalchemy import select, update

from worker.core.db import AuditLog, MonitHistory, get_session
from worker.core.redis_client import publish_task_completed
from worker.services.dlq_service import add_to_dlq
from worker.services.sms_service import SmsMessage, send_sms
from worker.settings import get_settings
from worker.core.logging_setup import get_event_logger

logger = logging.getLogger("worker.tasks.sms")
_RETRY_DELAYS = [10, 60, 300]


async def send_bulk_sms(
    ctx: dict[str, Any],
    *,
    monit_ids: list[int],
    triggered_by_user_id: int,
    job_id: Optional[str] = None,
) -> dict[str, Any]:
    """ARQ Task: Masowa wysyłka SMS przez SMSAPI.pl."""
    settings = get_settings()
    task_start = time.monotonic()
    effective_job_id = job_id or str(ctx.get("job_id", uuid.uuid4()))
    retry_count = ctx.get("job_try", 1) - 1

    logger.info(
        "Rozpoczynam send_bulk_sms",
        extra={
            "job_id":       effective_job_id,
            "monit_count":  len(monit_ids),
            "triggered_by": triggered_by_user_id,
            "retry_count":  retry_count,
        },
    )

    success_ids: list[int] = []
    failed_ids: list[int] = []
    errors: list[dict] = []

    async with get_session() as db:
        result = await db.execute(
            select(MonitHistory).where(
                MonitHistory.id_monit.in_(monit_ids),
                MonitHistory.monit_type == "sms",
            )
        )
        monits = result.scalars().all()

    for monit in monits:
        if not monit.recipient:
            logger.warning(
                "Monit SMS bez numeru telefonu",
                extra={"monit_id": monit.id_monit},
            )
            failed_ids.append(monit.id_monit)
            errors.append({"monit_id": monit.id_monit, "error": "Brak numeru telefonu"})
            continue

        sms_msg = SmsMessage(
            phone_number=monit.recipient,
            message=monit.message_body or _default_sms_body(monit, settings),
            monit_id=monit.id_monit,
            user_id=triggered_by_user_id,
        )

        result_sms = await send_sms(sms_msg)
        new_status = "sent" if result_sms.success else "failed"

        async with get_session() as db:
            await db.execute(
                update(MonitHistory)
                .where(MonitHistory.id_monit == monit.id_monit)
                .values(
                    status=new_status,
                    sent_at=datetime.now(timezone.utc) if result_sms.success else None,
                    external_id=result_sms.smsapi_message_id,
                    error_message=result_sms.error[:500] if result_sms.error else None,
                    retry_count=retry_count,
                )
            )
            db.add(AuditLog(
                timestamp=datetime.now(timezone.utc),
                user_id=triggered_by_user_id,
                action="sms.sent" if result_sms.success else "sms.failed",
                entity_type="MonitHistory",
                entity_id=str(monit.id_monit),
                new_value=json.dumps({
                    "status": new_status,
                    "smsapi_id": result_sms.smsapi_message_id,
                    "points": result_sms.points_used,
                }, default=str),
                success=result_sms.success,
                error_message=result_sms.error[:500] if result_sms.error else None,
                extra_data=json.dumps({"job_id": effective_job_id}),
            ))

        (success_ids if result_sms.success else failed_ids).append(monit.id_monit)
        if not result_sms.success:
            errors.append({"monit_id": monit.id_monit, "error": result_sms.error})

    total_duration = (time.monotonic() - task_start) * 1000
    summary = {
        "job_id": effective_job_id,
        "success": len(success_ids),
        "failed": len(failed_ids),
        "total": len(monits),
        "duration_ms": round(total_duration, 1),
        "errors": errors,
    }

    get_event_logger(settings.LOG_DIR).log(
        "task_completed",
        {"task": "send_bulk_sms", **summary},
        user_id=triggered_by_user_id,
    )

    if failed_ids and retry_count < settings.TASK_MAX_RETRIES - 1:
        delay = _RETRY_DELAYS[retry_count] if retry_count < len(_RETRY_DELAYS) else 300
        raise Retry(defer=delay)

    if failed_ids and retry_count >= settings.TASK_MAX_RETRIES - 1:
        await add_to_dlq(
            task_name="send_bulk_sms",
            task_kwargs={"monit_ids": failed_ids, "triggered_by_user_id": triggered_by_user_id},
            job_id=effective_job_id,
            error_message=f"{len(failed_ids)} SMS failed",
            retry_count=retry_count,
            user_id=triggered_by_user_id,
        )

    await publish_task_completed(
        task_name="send_bulk_sms",
        success_count=len(success_ids),
        failed_count=len(failed_ids),
        message=f"SMS: {len(success_ids)} wysłanych, {len(failed_ids)} błędów",
        user_id=triggered_by_user_id,
        extra={"job_id": effective_job_id},
    )
    return summary


def _default_sms_body(monit: MonitHistory, settings) -> str:
    debt = f"{float(monit.total_debt or 0):.2f}" if monit.total_debt else "?"
    return (
        f"Wezwanie do zaplaty: {debt} PLN. "
        f"Prosimy o niezwloczna wplate. "
        f"{settings.COMPANY_NAME}"
    )[:160]