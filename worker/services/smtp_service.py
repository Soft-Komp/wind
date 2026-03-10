# =============================================================================
# worker/services/smtp_service.py — Wysyłka email z failover chain
# =============================================================================
# Strategia: Primary SMTP → jeśli error/timeout → następny w kolejności.
# Wszystkie próby logowane do pliku smtp_YYYY-MM-DD.jsonl
# =============================================================================

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import aiosmtplib

from worker.settings import SMTPConfig, get_settings

logger = logging.getLogger("worker.smtp")
_WARSAW = ZoneInfo("Europe/Warsaw")


def _smtp_log_file() -> Path:
    settings = get_settings()
    date_str = datetime.now(_WARSAW).strftime("%Y-%m-%d")
    path = Path(settings.LOG_DIR) / f"smtp_{date_str}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _append_smtp_log(entry: dict) -> None:
    """Zapis do dziennika SMTP — append-only, nigdy nie usuwa."""
    try:
        with open(_smtp_log_file(), "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
    except Exception as exc:
        logger.error("Błąd zapisu SMTP log", extra={"error": str(exc)})


@dataclass
class EmailMessage:
    """Wiadomość email do wysyłki."""
    to_email: str
    to_name: str
    subject: str
    html_body: str
    text_body: Optional[str] = None
    reply_to: Optional[str] = None
    attachments: list[dict] = field(default_factory=list)
    # attachments format: [{"filename": "...", "data": bytes, "mime_type": "application/pdf"}]
    monit_id: Optional[int] = None
    user_id: Optional[int] = None


@dataclass
class SendResult:
    """Wynik wysyłki emaila."""
    success: bool
    smtp_host_used: Optional[str]
    smtp_attempt: int          # Który provider (1=primary, 2=fallback1, ...)
    duration_ms: float
    error: Optional[str] = None
    message_id: Optional[str] = None


async def _try_send(
    config: SMTPConfig,
    message: EmailMessage,
    attempt_num: int,
) -> SendResult:
    """
    Próba wysyłki przez jeden konkretny serwer SMTP.

    Returns:
        SendResult z wynikiem (success=True lub False z błędem).
    """
    start = time.monotonic()

    # Buduj wiadomość MIME
    msg = MIMEMultipart("alternative")
    msg["From"] = f"{config.from_name} <{config.from_email}>"
    msg["To"] = f"{message.to_name} <{message.to_email}>" if message.to_name else message.to_email
    msg["Subject"] = message.subject
    if message.reply_to:
        msg["Reply-To"] = message.reply_to

    # Treść tekstowa (fallback)
    if message.text_body:
        msg.attach(MIMEText(message.text_body, "plain", "utf-8"))

    # Treść HTML
    msg.attach(MIMEText(message.html_body, "html", "utf-8"))

    # Załączniki (np. PDF monitu)
    if message.attachments:
        outer = MIMEMultipart("mixed")
        outer.attach(msg)
        for att in message.attachments:
            part = MIMEApplication(att["data"], Name=att["filename"])
            part["Content-Disposition"] = f'attachment; filename="{att["filename"]}"'
            outer.attach(part)
        final_msg = outer
    else:
        final_msg = msg

    # Logowanie próby
    log_entry_base = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "attempt": attempt_num,
        "smtp_host": config.host,
        "smtp_port": config.port,
        "smtp_user": config.user,
        "to_email": message.to_email,
        "subject": message.subject,
        "monit_id": message.monit_id,
        "user_id": message.user_id,
        "has_attachments": bool(message.attachments),
    }

    try:
        smtp = aiosmtplib.SMTP(
            hostname=config.host,
            port=config.port,
            use_tls=config.use_ssl,   # SSL (port 465)
            start_tls=config.use_tls, # STARTTLS (port 587)
            timeout=config.timeout,
        )

        async with smtp:
            await smtp.login(config.user, config.password)
            response = await smtp.send_message(final_msg)

        duration_ms = (time.monotonic() - start) * 1000
        message_id = str(response) if response else None

        log_entry = {
            **log_entry_base,
            "status": "sent",
            "duration_ms": round(duration_ms, 2),
            "message_id": message_id,
        }
        _append_smtp_log(log_entry)

        logger.info(
            "Email wysłany",
            extra={
                "to": message.to_email,
                "smtp_host": config.host,
                "attempt": attempt_num,
                "duration_ms": round(duration_ms, 2),
                "monit_id": message.monit_id,
            },
        )
        return SendResult(
            success=True,
            smtp_host_used=config.host,
            smtp_attempt=attempt_num,
            duration_ms=duration_ms,
            message_id=message_id,
        )

    except Exception as exc:
        duration_ms = (time.monotonic() - start) * 1000
        error_msg = f"{type(exc).__name__}: {exc}"

        log_entry = {
            **log_entry_base,
            "status": "error",
            "duration_ms": round(duration_ms, 2),
            "error": error_msg,
        }
        _append_smtp_log(log_entry)

        logger.warning(
            "Błąd wysyłki SMTP",
            extra={
                "smtp_host": config.host,
                "attempt": attempt_num,
                "error": error_msg,
                "monit_id": message.monit_id,
            },
        )
        return SendResult(
            success=False,
            smtp_host_used=config.host,
            smtp_attempt=attempt_num,
            duration_ms=duration_ms,
            error=error_msg,
        )


async def send_email(message: EmailMessage) -> SendResult:
    """
    Wysyła email przez łańcuch SMTP (primary → fallback1 → fallback2 → ...).

    Strategia: PRIMARY + FALLBACK CHAIN
    - Próbuje każdy serwer po kolei
    - Zatrzymuje się przy pierwszym sukcesie
    - Jeśli wszystkie fail → ostatni wynik (z błędem)

    Returns:
        SendResult ostatniej (udanej lub failed) próby.
    """
    settings = get_settings()
    configs = settings.smtp_configs

    if not configs:
        logger.error(
            "Brak konfiguracji SMTP — nie można wysłać emaila",
            extra={"to": message.to_email, "monit_id": message.monit_id},
        )
        return SendResult(
            success=False,
            smtp_host_used=None,
            smtp_attempt=0,
            duration_ms=0,
            error="Brak konfiguracji SMTP w ustawieniach",
        )

    last_result: Optional[SendResult] = None

    for i, config in enumerate(configs, start=1):
        logger.debug(
            "Próba wysyłki SMTP",
            extra={
                "attempt": i,
                "total_providers": len(configs),
                "smtp_host": config.host,
                "to": message.to_email,
            },
        )
        result = await _try_send(config, message, attempt_num=i)
        last_result = result

        if result.success:
            return result

        # Jeśli to nie ostatni provider — kontynuuj
        if i < len(configs):
            logger.warning(
                "SMTP provider failed, przełączam na następny",
                extra={
                    "failed_host": config.host,
                    "next_attempt": i + 1,
                    "to": message.to_email,
                },
            )

    # Wszystkie providery zawiodły
    logger.error(
        "Wszystkie SMTP providery zawiodły",
        extra={
            "tried_providers": len(configs),
            "to": message.to_email,
            "monit_id": message.monit_id,
            "last_error": last_result.error if last_result else "unknown",
        },
    )
    return last_result  # type: ignore[return-value]