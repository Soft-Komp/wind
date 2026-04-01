# =============================================================================
# alertmanager/services/alert_log_service.py
# System Windykacja — Alert Manager — Zapis historii alertów do bazy
# =============================================================================

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from functools import partial
from typing import Any

import orjson
import pyodbc

from models.alert import AlertEmail, CheckResult

logger = logging.getLogger("alertmanager.services.alert_log")

_MAX_TITLE = 500
_MAX_MESSAGE = 4000
_MAX_DETAILS_JSON = 65535
_MAX_EMAIL_RECIPIENTS = 1000
_MAX_EMAIL_ERROR = 500


def _sync_insert_alert_log(
    connection_string: str,
    alert_type: str,
    level: str,
    title: str,
    message: str,
    details_json: str | None,
    email_sent: bool,
    email_recipients: str | None,
    email_error: str | None,
    is_recovery: bool,
    incident_id: str,
    checked_at: datetime,
) -> int | None:
    sql = """
    INSERT INTO [dbo_ext].[skw_AlertLog] (
        [AlertType], [Level], [Title], [Message], [Details],
        [EmailSent], [EmailRecipients], [EmailError],
        [IsRecovery], [IncidentId], [CheckedAt], [CreatedAt]
    )
    OUTPUT INSERTED.ID
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    title_safe = title[:_MAX_TITLE] if title else ""
    message_safe = message[:_MAX_MESSAGE] if message else ""
    details_safe = details_json[:_MAX_DETAILS_JSON] if details_json else None
    recipients_safe = email_recipients[:_MAX_EMAIL_RECIPIENTS] if email_recipients else None
    error_safe = email_error[:_MAX_EMAIL_ERROR] if email_error else None

    try:
        conn = pyodbc.connect(connection_string, autocommit=True)
        try:
            cursor = conn.cursor()
            cursor.execute(sql, (
                alert_type, level, title_safe, message_safe, details_safe,
                1 if email_sent else 0, recipients_safe, error_safe,
                1 if is_recovery else 0, incident_id,
                checked_at.replace(tzinfo=None),
                datetime.now().replace(tzinfo=None),
            ))
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            conn.close()
    except pyodbc.Error as exc:
        logger.error("Błąd INSERT do skw_AlertLog: %s | incident_id=%s", exc, incident_id)
        return None
    except Exception as exc:
        logger.error("Nieoczekiwany błąd AlertLog: %s | incident_id=%s", exc, incident_id, exc_info=True)
        return None


async def log_alert(
    connection_string: str,
    result: CheckResult,
    alert_email: AlertEmail | None,
    is_recovery: bool = False,
) -> int | None:
    try:
        details_json = orjson.dumps(result.details).decode() if result.details else None
    except Exception as exc:
        details_json = orjson.dumps({"serialization_error": str(exc)}).decode()

    email_sent = alert_email.sent if alert_email else False
    email_recipients = (
        ", ".join(alert_email.recipients)
        if alert_email and alert_email.recipients else None
    )
    email_error = alert_email.send_error if alert_email else None

    log_ctx: dict[str, Any] = {
        "incident_id": result.incident_id,
        "alert_type": result.alert_type,
        "level": result.level.value,
        "is_recovery": is_recovery,
        "email_sent": email_sent,
    }

    loop = asyncio.get_event_loop()
    record_id = await loop.run_in_executor(
        None,
        partial(
            _sync_insert_alert_log,
            connection_string,
            result.alert_type,
            result.level.value,
            result.title,
            result.message,
            details_json,
            email_sent,
            email_recipients,
            email_error,
            is_recovery,
            result.incident_id,
            result.checked_at,
        ),
    )

    if record_id is not None:
        logger.info(
            "Alert zapisany w skw_AlertLog (ID=%d) | incident_id=%s",
            record_id, result.incident_id,
            extra={**log_ctx, "alert_log_id": record_id},
        )
    else:
        logger.error(
            "NIE UDAŁO SIĘ zapisać alertu w skw_AlertLog! incident_id=%s",
            result.incident_id, extra=log_ctx,
        )

    return record_id