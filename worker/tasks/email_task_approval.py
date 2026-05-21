# worker/tasks/email_task_approval.py
"""
Dwu-fazowy system emaili dla modulu Approval.

FAZA 1 — queue_approval_email():
    ARQ task wywoływany z routerow / serwisow po kazdej akcji.
    Nie wysyla emaila od razu — dodaje zdarzenie do Redis hash:
        HSET approval_email_queue:{id_user}  {event_key}  {json_payload}
    Ustawia TTL na hashu = APPROVAL_EMAIL_DEBOUNCE_MINUTES * 60.
    Nastepnie enqueue'uje flush_approval_emails z opoznieniem
    (arq.jobs.JobDef, run_at=now + debounce).

FAZA 2 — flush_approval_emails():
    ARQ task — wywolywany po uplywie debounce.
    Odczytuje caly hash, buduje jeden zbiorczy email HTML,
    wysyla przez aiosmtplib, usuwa hash.
    Jesli hash pusty (zdarzenia juz wyslano) — wychodzi bez akcji.
    Jesli kilka flush zostalo zaplanowanych (race condition) —
        GETSET / sprawdzenie pustosci po HGETALL chroni przed podwojnym wyslaniem.

Zalety:
    - User dostaje JEDEN zbiorczy email zamiast N osobnych
    - Debounce konfigurowalny per instancja (APPROVAL_EMAIL_DEBOUNCE_MINUTES)
    - Blokada DEMO_MODE i feature flag
    - Brak wycieku emaili przy bledie SMTP (hash nie jest kasowany)

Redis keys:
    approval_email_queue:{id_user}  — hash: {event_key -> json_payload}
                                       TTL = debounce_minutes * 60

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import aiosmtplib

logger = logging.getLogger(__name__)

_SCHEMA           = "dbo"
_DEFAULT_DEBOUNCE = 15  # minut

# Szablon HTML emaila zbiorczego
_HTML_HEADER = """\
<!DOCTYPE html>
<html lang="pl"><head><meta charset="utf-8">
<style>
  body {{ font-family: Arial, sans-serif; font-size: 14px; color: #333; }}
  h2   {{ color: #1a56a4; }}
  .event {{ border-left: 4px solid #1a56a4; padding: 8px 12px;
             margin: 10px 0; background: #f5f8ff; }}
  .label {{ font-weight: bold; color: #555; }}
  .footer {{ font-size: 12px; color: #999; margin-top: 20px; }}
</style>
</head><body>
<h2>Powiadomienia z obiegu dokumentow</h2>
<p>Ponizej znajdziesz zbiorcze powiadomienia z ostatnich {minutes} minut:</p>
"""

_HTML_EVENT = """\
<div class="event">
  <div class="label">{title}</div>
  <div>{message}</div>
</div>
"""

_HTML_FOOTER = """\
<div class="footer">
  Wiadomosc wygenerowana automatycznie przez System Windykacja.<br>
  Prosimy nie odpowiadac na ta wiadomosc.
</div>
</body></html>
"""


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _debounce_minutes() -> int:
    try:
        return int(os.environ.get("APPROVAL_EMAIL_DEBOUNCE_MINUTES", str(_DEFAULT_DEBOUNCE)))
    except (ValueError, TypeError):
        return _DEFAULT_DEBOUNCE


# =============================================================================
# FAZA 1: Kolejkowanie zdarzenia
# =============================================================================

async def queue_approval_email(
    ctx: dict[str, Any],
    *,
    event_type: str,
    id_instance: int,
    id_user: int,
    document_title: str | None,
    step_order: int = 0,
    hours_to_deadline: int | None = None,
) -> dict:
    """
    ARQ task fazy 1 — dodaje zdarzenie do Redis hash i planuuje flush.

    Nie wysyla emaila bezposrednio — agreguje zdarzenia w oknie debounce.
    Jesli hash juz istnieje (trwa okno debounce), tylko dodaje zdarzenie.
    Jesli hash nowy — rowniez planuje flush po uplywie debounce.

    Args:
        event_type:        Typ zdarzenia (approval_pending, approval_accepted itp.)
        id_instance:       ID instancji obiegu.
        id_user:           ID odbiorcy.
        document_title:    Tytul dokumentu.
        step_order:        Numer etapu.
        hours_to_deadline: Dla deadline_warning.

    Returns:
        {"queued": True, "hash_key": "...", "event_key": "..."}
    """
    redis = ctx.get("worker_redis")
    if not redis:
        logger.warning("queue_approval_email | brak worker_redis")
        return {"queued": False, "reason": "no_redis"}

    debounce_min = _debounce_minutes()
    hash_key     = f"approval_email_queue:{id_user}"
    event_key    = f"{event_type}:{id_instance}:{int(datetime.now(timezone.utc).timestamp())}"

    payload = json.dumps({
        "event_type":        event_type,
        "id_instance":       id_instance,
        "document_title":    document_title or f"Dokument #{id_instance}",
        "step_order":        step_order,
        "hours_to_deadline": hours_to_deadline,
        "queued_at":         datetime.now(timezone.utc).isoformat(),
    }, ensure_ascii=False)

    try:
        # Dodaj zdarzenie do hasha — jesli hash nowy ustaw TTL
        is_new = not await redis.exists(hash_key)
        await redis.hset(hash_key, event_key, payload)
        await redis.expire(hash_key, debounce_min * 60)

        if is_new:
            # Zaplanuj flush po uplywie debounce — enqueue do ARQ
            try:
                from arq.connections import ArqRedis
                arq_redis = ArqRedis(redis.connection_pool)
                run_at = datetime.now(timezone.utc) + timedelta(minutes=debounce_min)
                await arq_redis.enqueue_job(
                    "flush_approval_emails",
                    id_user=id_user,
                    hash_key=hash_key,
                    _defer_until=run_at,
                )
            except Exception as exc:
                logger.warning(
                    "queue_approval_email | Nie udalo sie enqueue flush: %s", exc
                )

        logger.debug(
            "queue_approval_email | hash=%s event=%s new_hash=%s",
            hash_key, event_key, is_new,
        )
        return {"queued": True, "hash_key": hash_key, "event_key": event_key}

    except Exception as exc:
        logger.error("queue_approval_email | Redis error: %s", exc)
        return {"queued": False, "reason": str(exc)}


# =============================================================================
# FAZA 2: Flush — wyslanie zbiorczego emaila
# =============================================================================

async def flush_approval_emails(
    ctx: dict[str, Any],
    *,
    id_user: int,
    hash_key: str,
) -> dict:
    """
    ARQ task fazy 2 — odczytuje hash i wysyla zbiorczy email HTML.

    Wywoływany po uplywie debounce przez queue_approval_email().
    Jesli hash pusty lub nie istnieje — nic nie wysyla (idempotentne).
    Chroni przed podwojnym wyslaniem przez atomowe GETDEL hasha.

    Args:
        id_user:  ID uzytkownika — odbiorca emaila.
        hash_key: Klucz Redis hasha ze zdarzeniami.
    """
    from worker.core.db import get_db_session
    from worker.settings import get_settings

    redis    = ctx.get("worker_redis")
    settings = get_settings()

    # ── Blokada feature flag ──────────────────────────────────────────────────
    if redis:
        try:
            flag = await redis.get("syscfg:APPROVAL_EMAIL_NOTIFICATIONS_ENABLED")
            val  = flag.decode() if isinstance(flag, bytes) else flag
            if val and val.lower() != "true":
                logger.debug("flush_approval_emails | email notifications wylaczone")
                return {"status": "skipped", "reason": "feature_disabled"}
        except Exception:
            pass

    # ── Blokada DEMO_MODE ────────────────────────────────────────────────────
    if getattr(settings, "DEMO_MODE", False):
        logger.warning("flush_approval_emails | DEMO_MODE — nie wysylam | user=%d", id_user)
        return {"status": "skipped", "reason": "demo_mode"}

    # ── Atomowe pobranie i usuniecie hasha ────────────────────────────────────
    # HGETALL + DEL w potoku — minimalizacja race condition
    events_raw: dict = {}
    if redis:
        try:
            pipe = redis.pipeline()
            pipe.hgetall(hash_key)
            pipe.delete(hash_key)
            results = await pipe.execute()
            events_raw = results[0] or {}
        except Exception as exc:
            logger.error("flush_approval_emails | Redis pipeline error: %s", exc)
            return {"status": "error", "reason": str(exc)}

    if not events_raw:
        logger.debug("flush_approval_emails | Hash pusty — nic do wyslania user=%d", id_user)
        return {"status": "skipped", "reason": "empty_queue"}

    # ── Parsuj zdarzenia ─────────────────────────────────────────────────────
    events = []
    for _key, raw in events_raw.items():
        raw_str = raw.decode() if isinstance(raw, bytes) else raw
        try:
            events.append(json.loads(raw_str))
        except Exception:
            events.append({"event_type": "unknown", "document_title": raw_str,
                            "step_order": 0})

    # Posortuj chronologicznie po queued_at
    events.sort(key=lambda e: e.get("queued_at", ""))

    # ── Pobierz email uzytkownika z DB ────────────────────────────────────────
    user_email: str | None = None
    user_name:  str | None = None
    try:
        async with get_db_session() as db:
            from sqlalchemy import text
            row = (await db.execute(
                text(f"SELECT [Email],[FullName] FROM [{_SCHEMA}].[skw_Users] "
                     f"WHERE [ID_USER]=:u AND [IsActive]=1"),
                {"u": id_user},
            )).fetchone()
            if row and row[0] and row[0].strip():
                user_email = row[0].strip()
                user_name  = row[1]
    except Exception as exc:
        logger.error("flush_approval_emails | DB error: %s", exc)
        return {"status": "error", "reason": str(exc)}

    if not user_email:
        logger.warning("flush_approval_emails | brak emaila dla user=%d — pomijam", id_user)
        return {"status": "skipped", "reason": "no_email"}

    # ── Buduj HTML ────────────────────────────────────────────────────────────
    debounce_min = _debounce_minutes()
    html_parts   = [_HTML_HEADER.format(minutes=debounce_min)]

    _TITLE_MAP = {
        "approval_pending":          "Dokument oczekuje na akceptacje",
        "approval_accepted":         "Dokument zaakceptowany",
        "approval_rejected":         "Dokument odrzucony",
        "approval_deadline_warning": "Zbliза sie termin akceptacji",
        "approval_deadline_expired": "Termin akceptacji przekroczony",
        "approval_escalated":        "Eskalacja — brak akceptacji",
    }

    for ev in events:
        ev_type  = ev.get("event_type", "unknown")
        title    = _TITLE_MAP.get(ev_type, ev_type.replace("_", " ").title())
        doc      = ev.get("document_title", "")
        step     = ev.get("step_order", 0)
        hours    = ev.get("hours_to_deadline")

        if hours is not None:
            msg = f"Dokument: <b>{doc}</b> | Etap: {step} | Pozostalo: {hours}h"
        else:
            msg = f"Dokument: <b>{doc}</b>" + (f" | Etap: {step}" if step else "")

        html_parts.append(_HTML_EVENT.format(title=title, message=msg))

    html_parts.append(_HTML_FOOTER)
    html_body = "".join(html_parts)

    # Tresc tekstowa (fallback)
    text_body = f"Masz {len(events)} nowych powiadomien w systemie obiegu dokumentow.\n\n"
    for ev in events:
        text_body += f"- {ev.get('document_title', '')} ({ev.get('event_type', '')})\n"
    text_body += "\nZaloguj sie do systemu aby zobaczyc szczegoly."

    # ── Konfiguracja SMTP ─────────────────────────────────────────────────────
    smtp_from = getattr(settings, "SMTP_FROM", None) or getattr(settings, "smtp_from", None)
    smtp_host = getattr(settings, "SMTP_HOST", None) or getattr(settings, "smtp_host", None)
    smtp_port = int(getattr(settings, "SMTP_PORT", None) or getattr(settings, "smtp_port", 587))
    smtp_user = getattr(settings, "SMTP_USER", None) or getattr(settings, "smtp_user", None)
    sp_obj    = getattr(settings, "SMTP_PASSWORD", None) or getattr(settings, "smtp_password", None)
    smtp_pass = sp_obj.get_secret_value() if sp_obj and hasattr(sp_obj, "get_secret_value") else (str(sp_obj) if sp_obj else None)

    if not smtp_host or not smtp_from:
        logger.warning("flush_approval_emails | SMTP nie skonfigurowany")
        return {"status": "skipped", "reason": "smtp_not_configured"}

    # ── Buduj i wyslij wiadomosc ──────────────────────────────────────────────
    subject  = f"Powiadomienia z obiegu dokumentow ({len(events)})"
    if user_name:
        subject = f"[Windykacja] {subject}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = smtp_from
    msg["To"]      = user_email
    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html",  "utf-8"))

    try:
        use_tls = getattr(settings, "SMTP_USE_TLS", True)
        await aiosmtplib.send(
            msg,
            hostname=smtp_host,
            port=smtp_port,
            username=smtp_user,
            password=smtp_pass,
            use_tls=bool(use_tls),
            timeout=15,
        )
        logger.info(
            "flush_approval_emails | WYSLANO | user=%d to=%s events=%d",
            id_user, user_email, len(events),
        )
        return {"status": "sent", "id_user": id_user, "events_count": len(events)}

    except Exception as exc:
        logger.error(
            "flush_approval_emails | BLAD SMTP | user=%d error=%s", id_user, exc,
        )
        # Przywroc hash na wypadek bledu (idempotentne ponowienie)
        if redis and events_raw:
            try:
                pipe = redis.pipeline()
                for k, v in events_raw.items():
                    pipe.hset(hash_key, k, v)
                pipe.expire(hash_key, debounce_min * 60)
                await pipe.execute()
                logger.info("flush_approval_emails | Hash przywrocony po bledzie SMTP")
            except Exception as restore_exc:
                logger.error("flush_approval_emails | Nie udalo sie przywrocic hasha: %s", restore_exc)

        return {"status": "error", "reason": str(exc), "id_user": id_user}