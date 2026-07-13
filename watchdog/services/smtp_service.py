"""
watchdog/services/smtp_service.py
Łańcuch failover SMTP — TA SAMA logika co worker/services/smtp_service.py,
świadomie zduplikowana (Watchdog to osobny kontener bez dostępu do pakietu
`worker`), z użyciem aiosmtplib zgodnie z Pana decyzją.
"""
from __future__ import annotations

import logging
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import aiosmtplib

from watchdog.config import get_settings

logger = logging.getLogger("watchdog.smtp")


async def wyslij_alert(temat: str, tresc_html: str) -> bool:
    """
    Wysyła alert e-mail przez łańcuch SMTP z SMTP_CONFIGS_JSON (ten sam
    format co worker: primary → fallback1 → fallback2 → ...).
    Zwraca True przy pierwszym sukcesie, False jeśli wszystkie próby zawiodły.
    """
    settings = get_settings()
    configs = settings.smtp_configs

    if not configs:
        logger.error(
            "watchdog: SMTP_CONFIGS puste — alert NIE zostanie wysłany",
            extra={"event": "watchdog.smtp_brak_konfiguracji", "temat": temat},
        )
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = temat
    msg["To"] = settings.WATCHDOG_ALERT_EMAIL
    msg.attach(MIMEText(tresc_html, "html", "utf-8"))

    for attempt, cfg in enumerate(configs, start=1):
        start = time.monotonic()
        try:
            msg["From"] = f"{cfg.get('from_name', 'Watchdog')} <{cfg['from_email']}>"
            await aiosmtplib.send(
                msg,
                hostname=cfg["host"],
                port=cfg.get("port", 587),
                username=cfg["user"],
                password=cfg["password"],
                start_tls=cfg.get("use_tls", True),
                use_tls=cfg.get("use_ssl", False),
                timeout=cfg.get("timeout", 30),
                validate_certs=False,
            )
            duration_ms = (time.monotonic() - start) * 1000
            logger.info(
                "watchdog: alert e-mail wysłany",
                extra={
                    "event": "watchdog.alert_wyslany",
                    "host": cfg["host"], "attempt": attempt,
                    "duration_ms": round(duration_ms, 1), "temat": temat,
                },
            )
            return True
        except Exception as exc:
            duration_ms = (time.monotonic() - start) * 1000
            logger.warning(
                "watchdog: błąd wysyłki alertu przez %s — %s", cfg.get("host"), exc,
                extra={
                    "event": "watchdog.alert_blad",
                    "host": cfg.get("host"), "attempt": attempt,
                    "duration_ms": round(duration_ms, 1), "error": str(exc),
                },
            )

    logger.critical(
        "watchdog: WSZYSTKIE próby wysyłki alertu zawiodły",
        extra={"event": "watchdog.alert_wszystkie_proby_zawiodly", "temat": temat, "prob": len(configs)},
    )
    return False