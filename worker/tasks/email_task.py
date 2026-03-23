# =============================================================================
# worker/tasks/email_task.py — ARQ Task: Masowa wysyłka email
# =============================================================================
# Pobiera MonitHistory records → renderuje treść → wysyła przez smtp_service
# → aktualizuje status w DB → publikuje SSE → zapisuje AuditLog
# Retry: 3 próby, exponential backoff (10s → 60s → 300s)
# DLQ: po wyczerpaniu prób
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

from worker.core import db
from worker.core.db import AuditLog, MonitHistory, MonitHistoryInvoice, get_session
from worker.core.redis_client import publish_task_completed
from worker.services.dlq_service import add_to_dlq
from worker.services.smtp_service import EmailMessage, send_email
from worker.services.test_recipient_service import get_test_recipient_config
from worker.services.bcc_service import get_bcc_config
from worker.services.pdf_service import generate_pdf, save_pdf_to_disk
from worker.settings import get_settings
from worker.core.logging_setup import get_event_logger

logger = logging.getLogger("worker.tasks.email")

# Opóźnienia retry: [10s, 60s, 300s]
_RETRY_DELAYS = [10, 60, 300]


async def send_bulk_emails(
    ctx: dict[str, Any],
    *,
    monit_ids: list[int],
    triggered_by_user_id: int,
    job_id: Optional[str] = None,
    include_pdf: bool = True,
    invoice_ids: Optional[list[int]] = None,  # NOWE — ID rozrachunków do zapisu
) -> dict[str, Any]:
    """
    ARQ Task: Masowa wysyłka emaili.

    ctx zawiera (z on_startup):
        ctx['job_id'] — ARQ job ID
        ctx['redis']  — redis connection

    Args:
        monit_ids:             Lista ID rekordów MonitHistory do wysłania
        triggered_by_user_id:  ID usera zlecającego
        job_id:                Opcjonalny custom job ID (do traceability)
        include_pdf:           Czy dołączyć PDF jako załącznik

    Returns:
        Słownik z podsumowaniem: {success, failed, total, duration_ms}
    """
    settings = get_settings()
    task_start = time.monotonic()
    effective_job_id = job_id or str(ctx.get("job_id", uuid.uuid4()))
    retry_count = ctx.get("job_try", 1) - 1

    # ── BLOKADA L2: Tryb demonstracyjny ──────────────────────────────────────
    if settings.DEMO_MODE:
        logger.warning(
            "send_bulk_emails: ZABLOKOWANO przez DEMO_MODE=true",
            extra={
                "job_id":       effective_job_id,
                "monit_ids":    monit_ids,
                "triggered_by": triggered_by_user_id,
                "demo_mode":    True,
            },
        )
        return {
            "status":     "blocked_demo_mode",
            "job_id":     effective_job_id,
            "message":    "Wysyłka zablokowana — DEMO_MODE=true",
            "success":    0,
            "failed":     0,
            "monit_ids":  monit_ids,
        }
    # ── koniec blokady DEMO_MODE ──────────────────────────────────────────────

    # ── Tryb testowy wysyłki — odczyt z DB (Redis cache) lub .env ────────────
    redis_client = ctx.get("worker_redis")
    test_cfg = await get_test_recipient_config(redis_client)
    bcc_cfg = await get_bcc_config(redis_client)

    if bcc_cfg.is_active:
        logger.info(
            "BCC aktywne — kopie emaili będą wysyłane na dodatkowe adresy",
            extra={
                "job_id":      effective_job_id,
                "bcc_count":   len(bcc_cfg.emails),
                "source":      bcc_cfg.source,
            },
        )

    if test_cfg.enabled:
        if not test_cfg.test_email:
            logger.error(
                "send_bulk_emails: test_mode.enabled=true ale test_email jest pusty — BLOKUJĘ",
                extra={"job_id": effective_job_id, "source": test_cfg.source},
            )
            return {
                "status":    "blocked_test_mode_no_email",
                "job_id":    effective_job_id,
                "message":   "test_mode.enabled=true ale test_mode.email jest pusty",
                "success":   0,
                "failed":    0,
                "monit_ids": monit_ids,
            }
        logger.warning(
            "send_bulk_emails: TRYB TESTOWY — wszystkie maile → test_email",
            extra={
                "job_id":      effective_job_id,
                "test_email":  test_cfg.test_email,
                "source":      test_cfg.source,
                "monit_count": len(monit_ids),
            },
        )
    # ── koniec konfiguracji trybu testowego ───────────────────────────────────


    logger.info(
        "Rozpoczynam send_bulk_emails",
        extra={
            "job_id":             effective_job_id,
            "monit_ids":          monit_ids,
            "monit_count":        len(monit_ids),
            "triggered_by":       triggered_by_user_id,
            "retry_count":        retry_count,
            "include_pdf":        include_pdf,
        },
    )

    get_event_logger(settings.LOG_DIR).log(
        "task_started",
        {"task": "send_bulk_emails", "job_id": effective_job_id, "count": len(monit_ids)},
        user_id=triggered_by_user_id,
    )

    success_ids: list[int] = []
    failed_ids: list[int] = []
    errors: list[dict] = []

    # ── Pobierz rekordy MonitHistory ─────────────────────────────────────────
    async with get_session() as db:
        result = await db.execute(
            select(MonitHistory).where(
                MonitHistory.id_monit.in_(monit_ids),
                MonitHistory.monit_type == "email",
            )
        )
        monits = result.scalars().all()

    if not monits:
        logger.warning(
            "Brak rekordów email do wysłania",
            extra={"job_id": effective_job_id, "monit_ids": monit_ids},
        )
        return {"success": 0, "failed": 0, "total": 0, "job_id": effective_job_id}

    logger.info(
        "Pobrano rekordy MonitHistory",
        extra={"job_id": effective_job_id, "found": len(monits), "requested": len(monit_ids)},
    )

    # ── Wysyłaj każdy email ──────────────────────────────────────────────────
    for monit in monits:
        monit_start = time.monotonic()

        if not monit.recipient:
            logger.warning(
                "Monit bez adresu email — pomijam",
                extra={"monit_id": monit.id_monit, "job_id": effective_job_id},
            )
            failed_ids.append(monit.id_monit)
            errors.append({"monit_id": monit.id_monit, "error": "Brak adresu email (recipient is NULL)"})
            continue

        # ── Opcjonalnie generuj PDF ────────────────────────────────────────
        pdf_attachment = None
        pdf_path_saved = None

        if include_pdf:
            try:
                pdf_bytes = await generate_pdf(
                    monit_id=monit.id_monit,
                    debtor_name=monit.recipient,  # W tym kontekście recipient = email, ale mamy też inne dane
                    debtor_nip=None,
                    debtor_address=None,
                    invoices=_parse_invoice_numbers(monit.invoice_numbers),
                    total_debt=float(monit.total_debt or 0),
                    payment_deadline=_calc_payment_deadline(),
                )
                pdf_path_saved = save_pdf_to_disk(pdf_bytes, monit.id_monit, "email")
                pdf_attachment = {
                    "filename": f"wezwanie_do_zaplaty_{monit.id_monit}.pdf",
                    "data": pdf_bytes,
                    "mime_type": "application/pdf",
                }
            except Exception as exc:
                logger.warning(
                    "Błąd generowania PDF — wysyłam email bez załącznika",
                    extra={"monit_id": monit.id_monit, "error": str(exc)},
                )

        # ── Buduj i wyślij email ───────────────────────────────────────────
        html_body = monit.message_body or _default_email_body(
            monit=monit, settings=settings
        )

        # Renderuj subject przez Jinja2 — monit.subject może zawierać {{ company_name }} itp.
        raw_subject = monit.subject or "Wezwanie do zapłaty"
        rendered_email_subject = _render_template(raw_subject, monit, settings)

        # ── Override adresu gdy tryb testowy aktywny ──────────────────────
        effective_email = test_cfg.test_email if test_cfg.enabled else monit.recipient
        if test_cfg.enabled:
            logger.info(
                "Test mode: przekierowuję email",
                extra={
                    "monit_id":       monit.id_monit,
                    "original_email": monit.recipient,
                    "test_email":     test_cfg.test_email,
                },
            )
        # ─────────────────────────────────────────────────────────────────

        email_msg = EmailMessage(
            to_email=effective_email,
            to_name="",
            subject=rendered_email_subject,
            html_body=html_body,
            text_body=_html_to_plain(html_body),
            attachments=[pdf_attachment] if pdf_attachment else [],
            monit_id=monit.id_monit,
            user_id=triggered_by_user_id,
            bcc=list(bcc_cfg.emails) if bcc_cfg.is_active else [],
        )

        result = await send_email(email_msg)
        monit_duration = (time.monotonic() - monit_start) * 1000

        # ── Aktualizuj status w DB ─────────────────────────────────────────
        new_status = "sent" if result.success else "failed"
        error_msg = result.error[:500] if result.error else None

        async with get_session() as db:
            await db.execute(
                update(MonitHistory)
                .where(MonitHistory.id_monit == monit.id_monit)
                .values(
                    status=new_status,
                    sent_at=datetime.now(timezone.utc) if result.success else None,
                    external_id=result.message_id,
                    error_message=error_msg,
                    pdf_path=pdf_path_saved,
                    retry_count=retry_count,
                )
            )
            await db.commit()

            # AuditLog
            db.add(AuditLog(
                timestamp=datetime.now(timezone.utc),
                user_id=triggered_by_user_id,
                action="email.sent" if result.success else "email.failed",
                entity_type="MonitHistory",
                entity_id=str(monit.id_monit),
                new_value=json.dumps({
                    "status": new_status,
                    "smtp_host": result.smtp_host_used,
                    "smtp_attempt": result.smtp_attempt,
                    "duration_ms": round(monit_duration, 1),
                    "error": error_msg,
                }, default=str),
                success=result.success,
                error_message=error_msg,
                details=json.dumps({"job_id": effective_job_id}),
            ))
            await db.commit()
        if result.success:
            success_ids.append(monit.id_monit)
            logger.info(
                "Email wysłany pomyślnie",
                extra={
                    "monit_id": monit.id_monit,
                    "to": monit.recipient,
                    "smtp_host": result.smtp_host_used,
                    "duration_ms": round(monit_duration, 1),
                    "job_id": effective_job_id,
                },
            )

            # ── Zapis powiązań monit↔rozrachunek ─────────────────────────────
            # Wykonywany tylko po pomyślnej wysyłce.
            # Błąd zapisu NIE blokuje głównego wyniku — graceful degradation.
            if invoice_ids:
                try:
                    async with get_session() as inv_db:
                        now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
                        for inv_id in invoice_ids:
                            inv_db.add(MonitHistoryInvoice(
                                id_monit=monit.id_monit,
                                id_rozrachunku=inv_id,
                                created_at=now_naive,
                            ))
                        await inv_db.commit()

                    logger.info(
                        "MonitHistory_Invoices zapisane",
                        extra={
                            "monit_id":    monit.id_monit,
                            "invoice_ids": invoice_ids,
                            "count":       len(invoice_ids),
                            "job_id":      effective_job_id,
                        },
                    )
                except Exception as inv_exc:
                    # Błąd zapisu NIE wpływa na status wysyłki
                    logger.error(
                        "BLAD zapisu MonitHistory_Invoices — email wysłany ale brak rekordu",
                        extra={
                            "monit_id":    monit.id_monit,
                            "invoice_ids": invoice_ids,
                            "error":       str(inv_exc),
                            "job_id":      effective_job_id,
                        },
                    )
        else:
            failed_ids.append(monit.id_monit)
            errors.append({
                "monit_id": monit.id_monit,
                "email": monit.recipient,
                "error": error_msg,
            })
            logger.error(
                "Email failed",
                extra={
                    "monit_id": monit.id_monit,
                    "to": monit.recipient,
                    "error": error_msg,
                    "job_id": effective_job_id,
                },
            )

    # ── Podsumowanie ─────────────────────────────────────────────────────────
    total_duration = (time.monotonic() - task_start) * 1000
    summary = {
        "job_id":        effective_job_id,
        "success":       len(success_ids),
        "failed":        len(failed_ids),
        "total":         len(monits),
        "duration_ms":   round(total_duration, 1),
        "success_ids":   success_ids,
        "failed_ids":    failed_ids,
        "errors":        errors,
        "retry_attempt": retry_count,
    }

    get_event_logger(settings.LOG_DIR).log(
        "task_completed",
        {"task": "send_bulk_emails", **summary},
        user_id=triggered_by_user_id,
    )

    # ── Jeśli są failures i to nie ostatnia próba → Retry ────────────────────
    if failed_ids and retry_count < settings.TASK_MAX_RETRIES - 1:
        delay = _RETRY_DELAYS[retry_count] if retry_count < len(_RETRY_DELAYS) else 300
        logger.warning(
            "Część emaili failed — retry",
            extra={
                "failed_count": len(failed_ids),
                "retry_in_s": delay,
                "next_attempt": retry_count + 2,
                "job_id": effective_job_id,
            },
        )
        # Przy retry wysyłamy tylko te które się nie powiodły
        raise Retry(defer=delay)

    # ── Jeśli po wszystkich próbach nadal failures → DLQ ─────────────────────
    if failed_ids and retry_count >= settings.TASK_MAX_RETRIES - 1:
        await add_to_dlq(
            task_name="send_bulk_emails",
            task_kwargs={"monit_ids": failed_ids, "triggered_by_user_id": triggered_by_user_id},
            job_id=effective_job_id,
            error_message=f"{len(failed_ids)} emaili failed po {retry_count + 1} próbach",
            retry_count=retry_count,
            user_id=triggered_by_user_id,
        )

    # ── SSE event ─────────────────────────────────────────────────────────────
    await publish_task_completed(
        task_name="send_bulk_emails",
        success_count=len(success_ids),
        failed_count=len(failed_ids),
        message=f"Email: {len(success_ids)} wysłanych, {len(failed_ids)} błędów",
        user_id=triggered_by_user_id,
        extra={"job_id": effective_job_id, "duration_ms": round(total_duration, 1)},
    )

    logger.info(
        "send_bulk_emails zakończony",
        extra={
            "job_id":       effective_job_id,
            "success":      len(success_ids),
            "failed":       len(failed_ids),
            "duration_ms":  round(total_duration, 1),
        },
    )
    return summary


# =============================================================================
# Helpers
# =============================================================================

def _parse_invoice_numbers(invoice_numbers_str: Optional[str]) -> list[dict]:
    """Parsuje string faktur → lista dict dla szablonu PDF."""
    if not invoice_numbers_str:
        return []
    numbers = [n.strip() for n in invoice_numbers_str.split(",") if n.strip()]
    return [
        {
            "number": n,
            "issue_date": "—",
            "due_date": "—",
            "amount": 0.0,
            "remaining": 0.0,
            "days_overdue": 0,
        }
        for n in numbers
    ]


def _calc_payment_deadline(days: int = 7) -> str:
    """Oblicza termin płatności (dziś + N dni)."""
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    dt = datetime.now(ZoneInfo("Europe/Warsaw")) + timedelta(days=days)
    return dt.strftime("%d.%m.%Y")


def _default_email_body(monit: MonitHistory, settings) -> str:
    """Domyślna treść HTML emaila gdy brak szablonu."""
    return f"""
    <html><body style="font-family: Arial, sans-serif; color: #333;">
    <h2>Wezwanie do zapłaty</h2>
    <p>Szanowni Państwo,</p>
    <p>Informujemy o zaległościach w płatnościach na łączną kwotę:
       <strong>{monit.total_debt or 0:.2f} PLN</strong>.</p>
    <p>Faktury: {monit.invoice_numbers or '—'}</p>
    <p>Prosimy o niezwłoczne uregulowanie należności.</p>
    <p>W razie pytań prosimy o kontakt.</p>
    <br>
    <p>Z poważaniem,<br><strong>{settings.COMPANY_NAME}</strong><br>Dział Windykacji</p>
    </body></html>
    """

def _render_template(template_str: str, monit: MonitHistory, settings) -> str:
    """Renderuje szablon Jinja2 zastępując zmienne danymi monitu."""
    from jinja2 import Environment, BaseLoader, Undefined

    class SilentUndefined(Undefined):
        """Zwraca pusty string dla niezdefiniowanych zmiennych zamiast błędu."""
        def __str__(self) -> str:
            return ""
        def __iter__(self):
            return iter([])
        def __bool__(self) -> bool:
            return False

    try:
        env = Environment(loader=BaseLoader(), undefined=SilentUndefined)
        tmpl = env.from_string(template_str)
        rendered = tmpl.render(
            debtor_name=monit.recipient or "",
            total_debt=f"{float(monit.total_debt or 0):.2f}",
            invoice_list=monit.invoice_numbers or "—",
            due_date=_calc_payment_deadline(),
            company_name=settings.COMPANY_NAME,
        )
        return rendered
    except Exception as exc:
        logger.warning(
            "Błąd renderowania szablonu Jinja2 — używam surowej treści",
            extra={"error": str(exc), "monit_id": monit.id_monit},
        )
        return template_str

def _html_to_plain(html: str) -> str:
    """Prosta konwersja HTML → plain text (usunięcie tagów)."""
    import re
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text