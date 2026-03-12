# =============================================================================
# worker/services/sms_service.py — Wysyłka SMS przez SMSAPI.pl
# =============================================================================
# Dokumentacja API: https://www.smsapi.pl/docs
# Token Bearer w nagłówku Authorization.
# Logi do: logs/sms_YYYY-MM-DD.jsonl
# =============================================================================

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import httpx

from worker.settings import get_settings

logger = logging.getLogger("worker.sms")
_WARSAW = ZoneInfo("Europe/Warsaw")

# Maksymalna długość SMS (160 znaków ASCII / 70 znaków Unicode)
SMS_MAX_LENGTH = 160


def _sms_log_file() -> Path:
    settings = get_settings()
    date_str = datetime.now(_WARSAW).strftime("%Y-%m-%d")
    path = Path(settings.LOG_DIR) / f"sms_{date_str}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _append_sms_log(entry: dict) -> None:
    try:
        with open(_sms_log_file(), "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
    except Exception as exc:
        logger.error("Błąd zapisu SMS log", extra={"error": str(exc)})


@dataclass
class SmsMessage:
    """Wiadomość SMS do wysyłki."""
    phone_number: str       # Format: 48XXXXXXXXX (z kierunkowym) lub XXXXXXXXX
    message: str            # Treść SMS — max 160 znaków
    monit_id: Optional[int] = None
    user_id: Optional[int] = None
    group: Optional[str] = None  # Opcjonalny tag grupy SMSAPI


@dataclass
class SmsSendResult:
    """Wynik wysyłki SMS."""
    success: bool
    phone_number: str
    smsapi_message_id: Optional[str]
    points_used: Optional[float]
    duration_ms: float
    error: Optional[str] = None
    raw_response: Optional[dict] = None


def _normalize_phone(phone: str) -> str:
    """
    Normalizuje numer telefonu do formatu akceptowanego przez SMSAPI.
    Usuwa spacje, myślniki, +, dodaje 48 jeśli brak prefiksu.
    """
    # Usuń białe znaki, myślniki, nawiasy
    phone = phone.strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")

    # Usuń wiodący +
    if phone.startswith("+"):
        phone = phone[1:]

    # Jeśli 9 cyfr (bez kierunkowego) — dodaj 48
    if len(phone) == 9 and phone.isdigit():
        phone = f"48{phone}"

    # Jeśli zaczyna się od 0 (stary format) — zamień na 48
    if phone.startswith("0") and len(phone) == 10:
        phone = f"48{phone[1:]}"

    return phone


# Mapa transliteracji polskich znaków → ASCII
# SMS w GSM-7 (160 znaków) — polskie litery zużywają 2 bajty (UCS-2, limit = 70 znaków)
# Transliteracja pozwala zmieścić 160 znaków zamiast 70 i unika problemów z kodowaniem
_PL_TRANSLITERATION: dict[int, str] = str.maketrans(
    "ąćęłńóśźżĄĆĘŁŃÓŚŹŻ",
    "acelnoszzACELNOSZZ",
)


def _transliterate_pl(text: str) -> str:
    """
    Zamienia polskie znaki diakrytyczne na ich ASCII odpowiedniki.

    Powód: SMS w standardzie GSM-7 = 160 znaków.
    Polskie litery wymagają UCS-2 = limit spada do 70 znaków i rosną koszty.
    Transliteracja: ą→a, ć→c, ę→e, ł→l, ń→n, ó→o, ś→s, ź/ż→z (i wielkie litery).

    Wywoływane ZAWSZE przed wysyłką SMS — nawet jeśli frontend już to zrobił
    (defense in depth).
    """
    transliterated = text.translate(_PL_TRANSLITERATION)
    if transliterated != text:
        logger.debug(
            "Transliteracja polskich znaków w SMS",
            extra={
                "original_len": len(text),
                "transliterated_len": len(transliterated),
                "changed": True,
            },
        )
    return transliterated


def _truncate_message(text: str, max_len: int = SMS_MAX_LENGTH) -> str:
    """Skróć wiadomość jeśli za długa + dodaj info o skróceniu."""
    if len(text) <= max_len:
        return text
    truncated = text[: max_len - 3] + "..."
    logger.warning(
        "Wiadomość SMS skrócona",
        extra={"original_len": len(text), "truncated_len": len(truncated)},
    )
    return truncated


async def send_sms(message: SmsMessage) -> SmsSendResult:
    """
    Wysyła SMS przez SMSAPI.pl REST API.

    Zwraca SmsSendResult z wynikiem.
    W trybie testowym (SMSAPI_TEST_MODE=True) — loguje ale nie wysyła.
    """
    settings = get_settings()
    start = time.monotonic()

    normalized_phone = _normalize_phone(message.phone_number)
    transliterated_message = _transliterate_pl(message.message)
    truncated_message = _truncate_message(transliterated_message)

    log_base = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "phone": normalized_phone,
        "original_phone": message.phone_number,
        "message_len": len(truncated_message),
        "monit_id": message.monit_id,
        "user_id": message.user_id,
        "test_mode": settings.SMSAPI_TEST_MODE,
        "sender": settings.SMSAPI_SENDER,
    }

    # ── Tryb testowy ──────────────────────────────────────────────────────────
    if settings.SMSAPI_TEST_MODE:
        duration_ms = (time.monotonic() - start) * 1000
        log_entry = {**log_base, "status": "test_mode_skipped", "duration_ms": round(duration_ms, 2)}
        _append_sms_log(log_entry)
        logger.warning(
            "[TEST MODE] SMS nie wysłany",
            extra={"phone": normalized_phone, "monit_id": message.monit_id},
        )
        return SmsSendResult(
            success=True,  # W test mode = "sukces" (nie blokujemy flow)
            phone_number=normalized_phone,
            smsapi_message_id="test_mode",
            points_used=0.0,
            duration_ms=duration_ms,
        )

    if not settings.SMSAPI_TOKEN:
        error = "SMSAPI_TOKEN nie ustawiony"
        logger.error(error)
        return SmsSendResult(
            success=False,
            phone_number=normalized_phone,
            smsapi_message_id=None,
            points_used=None,
            duration_ms=0,
            error=error,
        )

    # ── Właściwa wysyłka ──────────────────────────────────────────────────────
    payload = {
        "to": normalized_phone,
        "message": truncated_message,
        "from": settings.SMSAPI_SENDER,
        "format": "json",
    }
    if message.group:
        payload["group"] = message.group

    headers = {
        "Authorization": f"Bearer {settings.SMSAPI_TOKEN}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                settings.SMSAPI_URL,
                data=payload,
                headers=headers,
            )

        duration_ms = (time.monotonic() - start) * 1000
        resp_data = {}

        try:
            resp_data = response.json()
        except Exception:
            resp_data = {"raw": response.text[:500]}

        # SMSAPI zwraca status w JSON: {"count": 1, "list": [{"id": "...", "points": 0.07, "number": "..."}]}
        if response.status_code == 200 and "list" in resp_data:
            sms_list = resp_data.get("list", [])
            first = sms_list[0] if sms_list else {}
            smsapi_id = first.get("id")
            points = float(first.get("points", 0))

            log_entry = {
                **log_base,
                "status": "sent",
                "smsapi_id": smsapi_id,
                "points_used": points,
                "duration_ms": round(duration_ms, 2),
                "http_status": response.status_code,
            }
            _append_sms_log(log_entry)

            logger.info(
                "SMS wysłany",
                extra={
                    "phone": normalized_phone,
                    "smsapi_id": smsapi_id,
                    "points": points,
                    "duration_ms": round(duration_ms, 2),
                    "monit_id": message.monit_id,
                },
            )
            return SmsSendResult(
                success=True,
                phone_number=normalized_phone,
                smsapi_message_id=smsapi_id,
                points_used=points,
                duration_ms=duration_ms,
                raw_response=resp_data,
            )
        else:
            # Błąd API
            error_msg = resp_data.get("message") or resp_data.get("invalid_numbers") or str(resp_data)
            log_entry = {
                **log_base,
                "status": "api_error",
                "http_status": response.status_code,
                "error": str(error_msg)[:200],
                "raw_response": str(resp_data)[:500],
                "duration_ms": round(duration_ms, 2),
            }
            _append_sms_log(log_entry)

            logger.error(
                "Błąd SMSAPI",
                extra={
                    "phone": normalized_phone,
                    "http_status": response.status_code,
                    "error": str(error_msg)[:200],
                    "monit_id": message.monit_id,
                },
            )
            return SmsSendResult(
                success=False,
                phone_number=normalized_phone,
                smsapi_message_id=None,
                points_used=None,
                duration_ms=duration_ms,
                error=str(error_msg)[:200],
                raw_response=resp_data,
            )

    except httpx.TimeoutException as exc:
        duration_ms = (time.monotonic() - start) * 1000
        error_msg = f"Timeout połączenia SMSAPI: {exc}"
        log_entry = {**log_base, "status": "timeout", "error": error_msg, "duration_ms": round(duration_ms, 2)}
        _append_sms_log(log_entry)
        logger.error("Timeout SMSAPI", extra={"phone": normalized_phone, "monit_id": message.monit_id})
        return SmsSendResult(
            success=False, phone_number=normalized_phone,
            smsapi_message_id=None, points_used=None,
            duration_ms=duration_ms, error=error_msg,
        )

    except Exception as exc:
        duration_ms = (time.monotonic() - start) * 1000
        error_msg = f"{type(exc).__name__}: {exc}"
        log_entry = {**log_base, "status": "exception", "error": error_msg, "duration_ms": round(duration_ms, 2)}
        _append_sms_log(log_entry)
        logger.error(
            "Wyjątek podczas wysyłki SMS",
            extra={"phone": normalized_phone, "error": error_msg, "monit_id": message.monit_id},
            exc_info=True,
        )
        return SmsSendResult(
            success=False, phone_number=normalized_phone,
            smsapi_message_id=None, points_used=None,
            duration_ms=duration_ms, error=error_msg,
        )