# =============================================================================
# worker/tasks/pdf_task.py — ARQ Task: Generowanie PDF
# =============================================================================

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import select, update

from worker.core.db import AuditLog, MonitHistory, get_session
from worker.core.redis_client import publish_task_completed
from worker.services.pdf_service import generate_pdf, save_pdf_to_disk
from worker.settings import get_settings
from worker.core.logging_setup import get_event_logger

logger = logging.getLogger("worker.tasks.pdf")


async def generate_pdf_task(
    ctx: dict[str, Any],
    *,
    monit_id: int,
    debtor_name: str,
    debtor_nip: Optional[str] = None,
    debtor_address: Optional[str] = None,
    invoices: Optional[list[dict]] = None,
    total_debt: float = 0.0,
    payment_deadline: Optional[str] = None,
    payment_account: Optional[str] = None,
    triggered_by_user_id: int,
    job_id: Optional[str] = None,
) -> dict[str, Any]:
    """
    ARQ Task: Generuje PDF monitu i zapisuje ścieżkę w MonitHistory.

    Wywoływany przez:
        - monit_service.py przy typie 'print'
        - Ręczne żądanie PDF (GET /monits/{id}/pdf)
    """
    settings = get_settings()
    task_start = time.monotonic()
    effective_job_id = job_id or str(ctx.get("job_id", uuid.uuid4()))

    # ── BLOKADA L2: Tryb demonstracyjny ──────────────────────────────────────
    if settings.DEMO_MODE:
        logger.warning(
            "generate_pdf_task: ZABLOKOWANO przez DEMO_MODE=true",
            extra={
                "job_id":       effective_job_id,
                "monit_id":     monit_id,
                "triggered_by": triggered_by_user_id,
                "demo_mode":    True,
            },
        )
        return {
            "status":    "blocked_demo_mode",
            "job_id":    effective_job_id,
            "message":   "Wysyłka zablokowana — DEMO_MODE=true",
            "success":   False,
            "monit_id":  monit_id,
        }
    # ── koniec blokady DEMO_MODE ──────────────────────────────────────────────

    logger.info(
        "Generuję PDF monitu",
        extra={
            "monit_id": monit_id,
            "debtor_name": debtor_name,
            "job_id": effective_job_id,
        },
    )

    try:
# Odczytaj wszystkie dane z MonitHistory — szablon, faktury, kwota
        template_body    = None
        invoice_list_str = "—"
        effective_total  = total_debt

        async with get_session() as db:
            mh_result = await db.execute(
                select(MonitHistory).where(MonitHistory.id_monit == monit_id)
            )
            monit_row = mh_result.scalar_one_or_none()

            if monit_row:
                # Kwota z MonitHistory (najbardziej aktualna)
                if monit_row.kwota_calkowita is not None:
                    effective_total = float(monit_row.kwota_calkowita)
                elif monit_row.total_debt is not None:
                    effective_total = float(monit_row.total_debt)

                # Numery faktur z MonitHistory
                if monit_row.invoice_numbers:
                    invoice_list_str = monit_row.invoice_numbers

                # Szablon z bazy
                if monit_row.template_id:
                    from worker.core.db import Template
                    tmpl_result = await db.execute(
                        select(Template).where(
                            Template.id_template == monit_row.template_id,
                            Template.is_active == True,
                        )
                    )
                    tmpl = tmpl_result.scalar_one_or_none()
                    if tmpl and tmpl.body:
                        template_body = tmpl.body
                        logger.info(
                            "generate_pdf_task: szablon z bazy",
                            extra={
                                "monit_id":    monit_id,
                                "template_id": monit_row.template_id,
                                "invoice_list": invoice_list_str,
                                "total_debt":  effective_total,
                            },
                        )
                    else:
                        logger.warning(
                            "generate_pdf_task: brak szablonu — fallback domyslny",
                            extra={"monit_id": monit_id, "template_id": monit_row.template_id},
                        )

        # Generuj PDF — jesli jest szablon, renderuj Jinja2 i buduj przez ReportLab
        # tak samo jak generate_pdf_preview (identyczny wyglad)
        if template_body:
            pdf_bytes = await _generate_pdf_from_template(
                monit_id=monit_id,
                template_body=template_body,
                debtor_name=debtor_name,
                invoice_list=invoice_list_str,
                total_debt=effective_total,
                payment_deadline=payment_deadline or _calc_deadline(),
                payment_account=payment_account,
            )
        else:
            pdf_bytes = await generate_pdf(
                monit_id=monit_id,
                debtor_name=debtor_name,
                debtor_nip=debtor_nip,
                debtor_address=debtor_address,
                invoices=invoices or [],
                total_debt=effective_total,
                payment_deadline=payment_deadline or _calc_deadline(),
                payment_account=payment_account,
            )

        pdf_path = save_pdf_to_disk(pdf_bytes, monit_id, "print")
        duration_ms = (time.monotonic() - task_start) * 1000

        # Aktualizuj MonitHistory
        async with get_session() as db:
            await db.execute(
                update(MonitHistory)
                .where(MonitHistory.id_monit == monit_id)
                .values(
                    status="sent",
                    pdf_path=pdf_path,
                    sent_at=datetime.now(timezone.utc),
                )
            )
            db.add(AuditLog(
                timestamp=datetime.now(timezone.utc),
                user_id=triggered_by_user_id,
                action="pdf.generated",
                entity_type="MonitHistory",
                entity_id=str(monit_id),
                new_value=json.dumps({"pdf_path": pdf_path, "size_kb": round(len(pdf_bytes)/1024, 1)}),
                success=True,
                details=json.dumps({"job_id": effective_job_id}),
            ))

        get_event_logger(settings.LOG_DIR).log(
            "task_completed",
            {"task": "generate_pdf", "monit_id": monit_id, "pdf_path": pdf_path},
            user_id=triggered_by_user_id,
        )
        await publish_task_completed(
            task_name="generate_pdf",
            success_count=1,
            failed_count=0,
            message=f"PDF wygenerowany: monit #{monit_id}",
            user_id=triggered_by_user_id,
            extra={"monit_id": monit_id, "pdf_path": pdf_path},
        )

        logger.info(
            "PDF task zakończony",
            extra={"monit_id": monit_id, "pdf_path": pdf_path, "duration_ms": round(duration_ms, 1)},
        )
        return {"success": True, "pdf_path": pdf_path, "size_bytes": len(pdf_bytes), "job_id": effective_job_id}

    except Exception as exc:
        duration_ms = (time.monotonic() - task_start) * 1000
        error_msg = f"{type(exc).__name__}: {exc}"
        logger.error(
            "Błąd generowania PDF",
            extra={"monit_id": monit_id, "error": error_msg, "job_id": effective_job_id},
            exc_info=True,
        )
        async with get_session() as db:
            await db.execute(
                update(MonitHistory)
                .where(MonitHistory.id_monit == monit_id)
                .values(status="failed", error_message=error_msg[:500])
            )
        await publish_task_completed(
            task_name="generate_pdf",
            success_count=0,
            failed_count=1,
            message=f"Błąd PDF monit #{monit_id}: {error_msg[:100]}",
            user_id=triggered_by_user_id,
        )
        raise


async def _generate_pdf_from_template(
    monit_id: int,
    template_body: str,
    debtor_name: str,
    invoice_list: str,
    total_debt: float,
    payment_deadline: str,
    payment_account: Optional[str] = None,
) -> bytes:
    """
    Generuje PDF z szablonu Jinja2 — identyczny wyglad jak generate_pdf_preview.
    Uzywane gdy monit ma przypisany szablon z bazy (skw_Templates).
    """
    from io import BytesIO
    from jinja2 import Environment, BaseLoader, Undefined
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import cm
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    import re

    settings = get_settings()

    # Rejestracja fontow DejaVu
    _DEJAVU_DIR = "/usr/share/fonts/truetype/dejavu"
    try:
        pdfmetrics.registerFont(TTFont("DejaVu", f"{_DEJAVU_DIR}/DejaVuSans.ttf"))
        pdfmetrics.registerFont(TTFont("DejaVu-Bold", f"{_DEJAVU_DIR}/DejaVuSans-Bold.ttf"))
        pdfmetrics.registerFontFamily("DejaVu", normal="DejaVu", bold="DejaVu-Bold")
        _font = "DejaVu"
    except Exception:
        _font = "Helvetica"

    # Renderuj Jinja2
    class _Silent(Undefined):
        def __str__(self): return ""
        def __iter__(self): return iter([])
        def __bool__(self): return False

    _env = Environment(loader=BaseLoader(), undefined=_Silent)
    rendered = _env.from_string(template_body).render(
        debtor_name=debtor_name,
        total_debt=f"{total_debt:.2f}",
        invoice_list=invoice_list,
        due_date=payment_deadline,
        company_name=settings.COMPANY_NAME,
    )

    # Zamien <br/> i <br> na newline przed podziałem na linie
    rendered = re.sub(r"<br\s*/?>", "\n", rendered)
    # Zamien <p> i </p> na newline
    rendered = re.sub(r"</p>", "\n", rendered, flags=re.IGNORECASE)
    rendered = re.sub(r"<p[^>]*>", "\n", rendered, flags=re.IGNORECASE)
    # Usun pozostale tagi HTML (zachowaj <b> <i>)
    rendered = re.sub(r"<(?!/?b>|/?i>)[^>]+>", "", rendered)
    # Znormalizuj wielokrotne spacje w linii (ale nie newline)
    rendered = re.sub(r"[^\S\n]+", " ", rendered)

    # Buduj PDF przez ReportLab — te same style co podglad
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer, pagesize=A4,
        rightMargin=2*cm, leftMargin=2*cm,
        topMargin=2*cm, bottomMargin=2*cm,
    )
    base = getSampleStyleSheet()
    style_normal = ParagraphStyle(
        "N", parent=base["Normal"],
        fontName=_font, fontSize=10, leading=14,
    )
    style_title = ParagraphStyle(
        "T", parent=base["Title"],
        fontName=_font, fontSize=14, leading=18,
    )

    story = []
    story.append(Paragraph(f"<b>{settings.COMPANY_NAME}</b>", style_title))
    if getattr(settings, "COMPANY_NIP", None):
        story.append(Paragraph(f"NIP: {settings.COMPANY_NIP}", style_normal))
    if getattr(settings, "COMPANY_ADDRESS", None):
        story.append(Paragraph(settings.COMPANY_ADDRESS, style_normal))
    story.append(Spacer(1, 0.5*cm))

    # Każda linia jako osobny paragraf — identycznie jak podgląd
    for line in rendered.splitlines():
        line = line.strip()
        if line:
            story.append(Paragraph(line, style_normal))
        else:
            story.append(Spacer(1, 0.3*cm))

    story.append(Spacer(1, 0.5*cm))
    if payment_deadline:
        story.append(Paragraph(
            f"Termin zapłaty: <b>{payment_deadline}</b>", style_normal
        ))
    if payment_account:
        story.append(Paragraph(f"Numer konta: {payment_account}", style_normal))
    story.append(Spacer(1, 1*cm))
    story.append(Paragraph(
        f"Z poważaniem,<br/><b>{settings.COMPANY_NAME}</b><br/>Dział Windykacji",
        style_normal,
    ))

    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()

    logger.info(
        "_generate_pdf_from_template: wygenerowano",
        extra={
            "monit_id":    monit_id,
            "pdf_size_kb": round(len(pdf_bytes) / 1024, 1),
            "debtor_name": debtor_name,
            "invoice_list": invoice_list,
            "total_debt":  total_debt,
        },
    )
    return pdf_bytes

def _calc_deadline(days: int = 7) -> str:
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    dt = datetime.now(ZoneInfo("Europe/Warsaw")) + timedelta(days=days)
    return dt.strftime("%d.%m.%Y")


# =============================================================================
# worker/tasks/otp_task.py — ARQ Task: Wysyłka OTP (email/SMS)
# =============================================================================

import logging as _logging
import time as _time
import uuid as _uuid
from datetime import datetime as _datetime, timezone as _timezone
from typing import Any as _Any, Optional as _Optional

from worker.core.logging_setup import get_event_logger as _get_event_logger
from worker.services.smtp_service import EmailMessage as _EmailMessage, send_email as _send_email
from worker.services.sms_service import SmsMessage as _SmsMessage, send_sms as _send_sms
from worker.settings import get_settings as _get_settings

_otp_logger = _logging.getLogger("worker.tasks.otp")


async def send_otp(
    ctx: _Any,
    *,
    otp_id: int,
    user_id: int,
    username: str,
    email: _Optional[str],
    phone: _Optional[str],
    full_name: str,
    otp_code: str,
    purpose: str,   # "password_reset" | "2fa"
    expires_at: str,
    channel: str = "email",  # "email" | "sms"
    ip_address: _Optional[str] = None,
    job_id: _Optional[str] = None,
) -> dict[str, _Any]:
    """
    ARQ Task: Wysyłka kodu OTP.

    WAŻNE: otp_code przekazywany w kwargs — ARQ serializuje przez msgpack w Redis.
    To jest akceptowalne (Redis powinien być zabezpieczony hasłem, sieć wewnętrzna).
    Stub OTP w otp_service.py zapisuje do JSONL jako dodatkowy dziennik — oba mechanizmy działają.
    """
    settings = _get_settings()
    start = _time.monotonic()
    effective_job_id = job_id or str(ctx.get("job_id", _uuid.uuid4()))

    _otp_logger.info(
        "Wysyłam OTP",
        extra={
            "otp_id":     otp_id,
            "user_id":    user_id,
            "channel":    channel,
            "purpose":    purpose,
            "expires_at": expires_at,
            "ip_address": ip_address,
            "job_id":     effective_job_id,
            # CELOWO nie logujemy otp_code do pliku (tylko w JSONL queue z flaga stub=True)
        },
    )

    success = False
    error_msg = None

    if channel == "email" and email:
        subject, html_body = _build_otp_email(
            full_name=full_name,
            otp_code=otp_code,
            purpose=purpose,
            expires_at=expires_at,
            settings=settings,
        )
        result = await _send_email(_EmailMessage(
            to_email=email,
            to_name=full_name,
            subject=subject,
            html_body=html_body,
            user_id=user_id,
        ))
        success = result.success
        error_msg = result.error

    elif channel == "sms" and phone:
        sms_body = _build_otp_sms(otp_code=otp_code, purpose=purpose, settings=settings)
        result_sms = await _send_sms(_SmsMessage(
            phone_number=phone,
            message=sms_body,
            user_id=user_id,
        ))
        success = result_sms.success
        error_msg = result_sms.error
    else:
        error_msg = f"Brak kanału lub danych (channel={channel}, email={email}, phone={phone})"
        _otp_logger.error("Nie można wysłać OTP — brak danych kontaktowych", extra={"otp_id": otp_id})

    duration_ms = (_time.monotonic() - start) * 1000
    _get_event_logger(settings.LOG_DIR).log(
        "otp_sent" if success else "otp_failed",
        {
            "otp_id":    otp_id,
            "user_id":   user_id,
            "channel":   channel,
            "purpose":   purpose,
            "success":   success,
            "error":     error_msg,
            "job_id":    effective_job_id,
            "duration_ms": round(duration_ms, 1),
        },
        user_id=user_id,
    )

    _otp_logger.info(
        "OTP task zakończony",
        extra={
            "otp_id": otp_id, "success": success,
            "duration_ms": round(duration_ms, 1), "job_id": effective_job_id,
        },
    )

    if not success and error_msg:
        # OTP failure = nie robimy retry (kod wygaśnie) — logujemy i kończymy
        _otp_logger.error(
            "OTP wysyłka nieudana — nie robimy retry (kod ma TTL)",
            extra={"otp_id": otp_id, "error": error_msg},
        )

    return {"success": success, "otp_id": otp_id, "channel": channel, "job_id": effective_job_id}


def _build_otp_email(full_name, otp_code, purpose, expires_at, settings) -> tuple[str, str]:
    purpose_labels = {
        "password_reset": ("Reset hasła", "resetowania hasła"),
        "2fa": ("Kod weryfikacyjny 2FA", "weryfikacji tożsamości"),
    }
    title, verb = purpose_labels.get(purpose, ("Kod weryfikacyjny", "weryfikacji"))

    subject = f"{title} — {settings.COMPANY_NAME}"
    html = f"""
    <html><body style="font-family: Arial, sans-serif; background: #f5f5f5; padding: 20px;">
    <div style="max-width: 480px; margin: 0 auto; background: #fff; border-radius: 8px;
                padding: 32px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
        <h2 style="color: #1a365d; margin-top: 0;">{title}</h2>
        <p>Drogi/a <strong>{full_name}</strong>,</p>
        <p>Twój kod do {verb}:</p>
        <div style="font-size: 32px; font-weight: bold; letter-spacing: 8px;
                    color: #1a365d; text-align: center; padding: 16px;
                    background: #ebf8ff; border-radius: 4px; margin: 16px 0;">
            {otp_code}
        </div>
        <p style="color: #666; font-size: 12px;">
            Kod ważny do: <strong>{expires_at}</strong><br>
            Jeśli to nie Ty, zignoruj tę wiadomość i zabezpiecz swoje konto.
        </p>
        <hr style="border: none; border-top: 1px solid #e2e8f0; margin: 24px 0;">
        <p style="color: #999; font-size: 11px; text-align: center;">
            {settings.COMPANY_NAME} | Wiadomość automatyczna — nie odpowiadaj
        </p>
    </div>
    </body></html>
    """
    return subject, html


def _build_otp_sms(otp_code, purpose, settings) -> str:
    labels = {"password_reset": "reset hasla", "2fa": "weryfikacja"}
    label = labels.get(purpose, "kod")
    return f"Twoj kod {label}: {otp_code}. Wazny 10 min. {settings.COMPANY_NAME}"[:160]