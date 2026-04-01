# =============================================================================
# alertmanager/services/smtp_alert_service.py
# System Windykacja — Alert Manager — Serwis wysyłki email alertów
#
# ARCHITEKTURA DUAL-SMTP:
#   1. Próba: własny SMTP alertów (ALERT_SMTP_*)
#   2. Fallback: główny SMTP systemu (SMTP_*)
#   3. Jeśli oba zawiodą → zapisz błąd w logu + alert_log (nie rzucaj wyjątku)
#
# Template HTML: inline, brak zależności od Jinja2.
# Polish characters: UTF-8, nie ma problemów w SMTP.
# =============================================================================

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Optional

import aiosmtplib

from models.alert import AlertEmail, AlertLevel, AlertState, CheckResult

logger = logging.getLogger("alertmanager.services.smtp_alert")


@dataclass
class SmtpConfig:
    """Konfiguracja jednego serwera SMTP."""
    host: str
    port: int
    user: str
    password: str
    from_email: str
    use_tls: bool
    use_ssl: bool
    timeout: int
    label: str  # "primary" / "fallback" — do logów


def _build_smtp_configs(settings: Any) -> list[SmtpConfig]:
    """
    Buduje listę konfiguracji SMTP w kolejności prób.
    Zwraca [] jeśli żaden SMTP nie jest skonfigurowany.
    """
    configs = []

    # Próba 1: dedykowany SMTP alertów
    if settings.alert_smtp_host and settings.alert_smtp_user:
        password = (
            settings.alert_smtp_password.get_secret_value()
            if settings.alert_smtp_password else ""
        )
        configs.append(SmtpConfig(
            host=settings.alert_smtp_host,
            port=settings.alert_smtp_port,
            user=settings.alert_smtp_user,
            password=password,
            from_email=settings.alert_smtp_from or settings.alert_smtp_user,
            use_tls=settings.alert_smtp_use_tls,
            use_ssl=settings.alert_smtp_use_ssl,
            timeout=settings.alert_smtp_timeout,
            label="primary_alert_smtp",
        ))

    # Próba 2: główny SMTP systemu (fallback)
    if settings.smtp_host and settings.smtp_user:
        fallback_password = (
            settings.smtp_password.get_secret_value()
            if settings.smtp_password else ""
        )
        configs.append(SmtpConfig(
            host=settings.smtp_host,
            port=settings.smtp_port,
            user=settings.smtp_user,
            password=fallback_password,
            from_email=settings.smtp_from or settings.smtp_user,
            use_tls=settings.smtp_use_tls,
            use_ssl=settings.smtp_use_ssl,
            timeout=settings.smtp_timeout,
            label="fallback_main_smtp",
        ))

    return configs


# =============================================================================
# GENEROWANIE HTML EMAILA
# =============================================================================

# Kolory per poziom alertu
_LEVEL_COLORS = {
    AlertLevel.CRITICAL: "#dc2626",   # czerwony
    AlertLevel.SECURITY: "#7c3aed",   # fioletowy
    AlertLevel.WARNING: "#d97706",    # pomarańczowy
    AlertLevel.INFO: "#2563eb",       # niebieski
}

_LEVEL_EMOJIS = {
    AlertLevel.CRITICAL: "🚨",
    AlertLevel.SECURITY: "🔒",
    AlertLevel.WARNING: "⚠️",
    AlertLevel.INFO: "ℹ️",
}


def _format_details_html(details: dict[str, Any]) -> str:
    """Formatuje słownik details jako tabela HTML."""
    if not details:
        return "<p><em>Brak dodatkowych szczegółów.</em></p>"

    rows = []
    for key, val in details.items():
        if val is None:
            continue
        if isinstance(val, (dict, list)):
            import orjson
            val_str = f"<code>{orjson.dumps(val, option=orjson.OPT_INDENT_2).decode()[:2000]}</code>"
        else:
            val_str = f"<code>{str(val)[:500]}</code>"

        rows.append(
            f"<tr>"
            f"<td style='padding:4px 8px;font-weight:bold;white-space:nowrap;"
            f"border:1px solid #e5e7eb;background:#f9fafb'>{key}</td>"
            f"<td style='padding:4px 8px;border:1px solid #e5e7eb;word-break:break-all'>{val_str}</td>"
            f"</tr>"
        )

    if not rows:
        return "<p><em>Brak dodatkowych szczegółów.</em></p>"

    return (
        "<table style='border-collapse:collapse;width:100%;font-size:12px;"
        "font-family:monospace;margin-top:8px'>"
        + "".join(rows)
        + "</table>"
    )


def build_alert_email_html(
    result: CheckResult,
    recipients: list[str],
    is_recovery: bool = False,
    previous_state: AlertState | None = None,
    service_name: str = "System Windykacja",
    environment: str = "production",
) -> tuple[str, str, str]:
    """
    Generuje treść emaila alertu.

    Returns:
        (subject, html_body, text_body)
    """
    level = result.level
    color = _LEVEL_COLORS.get(level, "#374151")
    emoji = _LEVEL_EMOJIS.get(level, "📢")
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    env_badge = f"[{environment.upper()}]" if environment != "production" else ""

    if is_recovery:
        subject = (
            f"✅ RECOVERY [{service_name}] {env_badge} "
            f"Odzyskano: {result.alert_type}"
        )
        header_text = "✅ SYSTEM ODZYSKAŁ SPRAWNOŚĆ"
        header_color = "#16a34a"
        status_badge = "RESOLVED"
    else:
        subject = (
            f"{emoji} {level.value} [{service_name}] {env_badge} "
            f"{result.title}"
        )
        header_text = f"{emoji} ALERT SYSTEMOWY — {level.value}"
        header_color = color
        status_badge = level.value

    # ── Historia awarii (dla recovery) ─────────────────────────────────
    history_html = ""
    if is_recovery and previous_state:
        duration = datetime.now(timezone.utc) - previous_state.first_fired_at
        hours = int(duration.total_seconds() // 3600)
        minutes = int((duration.total_seconds() % 3600) // 60)
        history_html = f"""
        <div style='background:#f0fdf4;border:1px solid #86efac;border-radius:6px;
                    padding:12px;margin:16px 0'>
            <strong>📊 Podsumowanie awarii:</strong><br>
            Czas trwania: <strong>{hours}h {minutes}min</strong><br>
            Pierwsze wystąpienie: <strong>{previous_state.first_fired_at.strftime('%Y-%m-%d %H:%M:%S UTC')}</strong><br>
            Liczba alertów wysłanych: <strong>{previous_state.fire_count}</strong>
        </div>
        """

    html = f"""<!DOCTYPE html>
<html lang="pl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{subject}</title>
</head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,Helvetica,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#f3f4f6;padding:24px 0">
  <tr><td align="center">
    <table width="600" cellpadding="0" cellspacing="0"
           style="background:#ffffff;border-radius:8px;overflow:hidden;
                  box-shadow:0 1px 3px rgba(0,0,0,.1)">

      <!-- NAGŁÓWEK -->
      <tr>
        <td style="background:{header_color};padding:24px 32px;color:#ffffff">
          <div style="font-size:11px;opacity:.8;margin-bottom:4px">
            {service_name} · Alert Manager · {environment.upper()}
          </div>
          <div style="font-size:22px;font-weight:bold">{header_text}</div>
          <div style="font-size:12px;opacity:.8;margin-top:8px">{now_str}</div>
        </td>
      </tr>

      <!-- TREŚĆ -->
      <tr>
        <td style="padding:32px">

          <!-- Status badge -->
          <div style="display:inline-block;background:{header_color};color:#fff;
                      padding:4px 12px;border-radius:20px;font-size:12px;
                      font-weight:bold;margin-bottom:20px">
            {status_badge}
          </div>

          <!-- Tytuł alertu -->
          <h2 style="margin:0 0 12px;font-size:18px;color:#111827">
            {result.title}
          </h2>

          <!-- Opis -->
          <p style="margin:0 0 20px;color:#374151;line-height:1.6;font-size:14px">
            {result.message}
          </p>

          {history_html}

          <!-- Metadane -->
          <div style="background:#f9fafb;border:1px solid #e5e7eb;
                      border-radius:6px;padding:16px;margin-bottom:20px">
            <table width="100%" style="font-size:13px;color:#374151">
              <tr>
                <td width="40%"><strong>Typ alertu:</strong></td>
                <td><code>{result.alert_type}</code></td>
              </tr>
              <tr>
                <td><strong>Incident ID:</strong></td>
                <td><code style="font-size:11px">{result.incident_id}</code></td>
              </tr>
              <tr>
                <td><strong>Checker:</strong></td>
                <td><code>{result.checker_name}</code></td>
              </tr>
              <tr>
                <td><strong>Wykryto:</strong></td>
                <td>{result.checked_at.strftime('%Y-%m-%d %H:%M:%S UTC')}</td>
              </tr>
              <tr>
                <td><strong>Czas sprawdzenia:</strong></td>
                <td>{result.duration_ms:.1f}ms</td>
              </tr>
            </table>
          </div>

          <!-- Szczegóły techniczne -->
          <details style="cursor:pointer">
            <summary style="font-weight:bold;color:#374151;font-size:13px;
                           padding:8px 0">
              🔧 Szczegóły techniczne (rozwiń)
            </summary>
            {_format_details_html(result.details)}
          </details>

        </td>
      </tr>

      <!-- STOPKA -->
      <tr>
        <td style="background:#f9fafb;padding:16px 32px;border-top:1px solid #e5e7eb">
          <p style="margin:0;font-size:11px;color:#9ca3af;text-align:center">
            Ten email został wygenerowany automatycznie przez Alert Manager.<br>
            System Windykacja GPGK Jasło · alert:cooldown:{result.alert_type}
          </p>
        </td>
      </tr>

    </table>
  </td></tr>
</table>
</body>
</html>"""

    text = (
        f"{'=' * 60}\n"
        f"{'RECOVERY: ' if is_recovery else ''}{level.value} ALERT — {service_name}\n"
        f"{'=' * 60}\n\n"
        f"Tytuł: {result.title}\n"
        f"Wiadomość: {result.message}\n\n"
        f"Typ alertu: {result.alert_type}\n"
        f"Incident ID: {result.incident_id}\n"
        f"Checker: {result.checker_name}\n"
        f"Wykryto: {result.checked_at.isoformat()}\n\n"
        f"{'=' * 60}\n"
    )

    return subject, html, text


# =============================================================================
# GŁÓWNA FUNKCJA WYSYŁKI
# =============================================================================


async def send_alert_email(
    alert_email: AlertEmail,
    settings: Any,
) -> AlertEmail:
    """
    Wysyła email alertu przez dual-SMTP (primary → fallback).

    Modyfikuje alert_email in-place (ustawia sent, smtp_host_used, itp.)
    Nigdy nie rzuca wyjątku — błędy są logowane.

    Returns:
        Zmodyfikowany AlertEmail z wynikiem wysyłki.
    """
    smtp_configs = _build_smtp_configs(settings)

    if not smtp_configs:
        logger.error(
            "Brak konfiguracji SMTP — email NIE może być wysłany! "
            "Ustaw ALERT_SMTP_HOST lub SMTP_HOST w .env",
            extra={
                "incident_id": alert_email.result.incident_id,
                "alert_type": alert_email.result.alert_type,
            }
        )
        alert_email.send_error = "NO_SMTP_CONFIG"
        return alert_email

    if not alert_email.recipients:
        logger.error(
            "Brak odbiorców — email NIE może być wysłany! "
            "Ustaw alerts.recipients w SystemConfig lub ALERT_RECIPIENTS_FALLBACK w .env",
            extra={"incident_id": alert_email.result.incident_id}
        )
        alert_email.send_error = "NO_RECIPIENTS"
        return alert_email

    for attempt_num, smtp_cfg in enumerate(smtp_configs, start=1):
        start = time.monotonic()
        log_ctx = {
            "attempt": attempt_num,
            "smtp_host": smtp_cfg.host,
            "smtp_port": smtp_cfg.port,
            "smtp_label": smtp_cfg.label,
            "recipients": alert_email.recipients,
            "incident_id": alert_email.result.incident_id,
            "alert_type": alert_email.result.alert_type,
            "is_recovery": alert_email.is_recovery,
        }

        try:
            logger.info(
                "Wysyłam email alert [próba %d/%d] via %s → %s",
                attempt_num, len(smtp_configs),
                smtp_cfg.label,
                alert_email.recipients,
                extra=log_ctx,
            )

            # Zbuduj wiadomość MIME
            msg = MIMEMultipart("alternative")
            msg["From"] = f"Alert Manager <{smtp_cfg.from_email}>"
            msg["To"] = ", ".join(alert_email.recipients)
            msg["Subject"] = alert_email.subject
            msg["X-Mailer"] = "Windykacja-AlertManager/1.0"
            msg["X-Alert-Type"] = alert_email.result.alert_type
            msg["X-Incident-ID"] = alert_email.result.incident_id

            if alert_email.text_body:
                msg.attach(MIMEText(alert_email.text_body, "plain", "utf-8"))
            msg.attach(MIMEText(alert_email.html_body, "html", "utf-8"))

            smtp = aiosmtplib.SMTP(
                hostname=smtp_cfg.host,
                port=smtp_cfg.port,
                use_tls=smtp_cfg.use_ssl,
                start_tls=smtp_cfg.use_tls,
                timeout=smtp_cfg.timeout,
                validate_certs=False,
            )

            async with smtp:
                await smtp.login(smtp_cfg.user, smtp_cfg.password)
                await smtp.send_message(
                    msg,
                    recipients=alert_email.recipients,
                )

            duration_ms = (time.monotonic() - start) * 1000
            alert_email.sent = True
            alert_email.smtp_host_used = smtp_cfg.host
            alert_email.smtp_attempt = attempt_num
            alert_email.sent_at = datetime.now(timezone.utc)

            logger.info(
                "Email alert WYSŁANY [%.1fms] via %s → %d odbiorców",
                duration_ms,
                smtp_cfg.label,
                len(alert_email.recipients),
                extra={**log_ctx, "duration_ms": round(duration_ms, 2), "sent": True},
            )
            return alert_email

        except aiosmtplib.SMTPException as exc:
            duration_ms = (time.monotonic() - start) * 1000
            logger.warning(
                "SMTP ERROR [próba %d] via %s: %s [%.1fms]",
                attempt_num, smtp_cfg.label, exc, duration_ms,
                extra={**log_ctx, "error": str(exc), "duration_ms": round(duration_ms, 2)},
            )
            alert_email.send_error = f"SMTP [{smtp_cfg.label}]: {exc}"

        except Exception as exc:
            duration_ms = (time.monotonic() - start) * 1000
            logger.error(
                "Nieoczekiwany błąd SMTP [próba %d] via %s: %s",
                attempt_num, smtp_cfg.label, exc,
                exc_info=True,
                extra={**log_ctx, "error": str(exc)},
            )
            alert_email.send_error = f"UNEXPECTED [{smtp_cfg.label}]: {exc}"

    # Wszystkie próby nieudane
    logger.critical(
        "Email alert NIE WYSŁANY po %d próbach! incident_id=%s alert_type=%s",
        len(smtp_configs),
        alert_email.result.incident_id,
        alert_email.result.alert_type,
        extra={
            "incident_id": alert_email.result.incident_id,
            "alert_type": alert_email.result.alert_type,
            "attempts": len(smtp_configs),
            "last_error": alert_email.send_error,
        }
    )
    return alert_email