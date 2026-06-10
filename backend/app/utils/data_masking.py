# backend/app/utils/data_masking.py
# =============================================================================
# System Windykacja — Data Masking Utility
#
# Cel: deterministyczne maskowanie danych wrażliwych (nazwa kontrahenta,
#      numer faktury) w odpowiedziach API na potrzeby środowisk testowych,
#      prezentacji, szkoleń i demo.
#
# Mechanizm:
#   - Flaga DATA_MASKING_ENABLED w .env (bool, domyślnie False)
#   - Deterministyczność: HMAC-SHA256(SECRET_MASKING_SALT + wartość) →
#     pierwsze 8 znaków hex → stabilny token per wartość
#   - Kontrahent: "KONTRAHENT-[A7F3B2C1]"  (zawsze ta sama firma → ten sam token)
#   - Numer:      "TST/2026/[4D8E]/0001"    (zachowana struktura formatu)
#   - Audit trail: każde maskowanie logowane do pliku JSONL z pełnym kontekstem
#
# Aktywacja:
#   .env:  DATA_MASKING_ENABLED=true
#          DATA_MASKING_SALT=<losowy_string_min_32_znaki>
#
# UWAGA BEZPIECZEŃSTWA:
#   Salt NIE jest sekretem kryptograficznym — to sól do pseudonimizacji.
#   Nie chroni przed reverse-engineering przy znanych wartościach wejściowych.
#   Cel to wyłącznie ukrycie przed przypadkowym okiem, nie przed atakującym.
#
# =============================================================================

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import threading
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Any

# =============================================================================
# Konfiguracja loggera dedykowanego — osobny plik JSONL
# =============================================================================

_MASKING_LOG_DIR  = os.environ.get("LOG_DIR", "/app/logs")
_MASKING_LOG_FILE = os.path.join(_MASKING_LOG_DIR, "data_masking.jsonl")

# Upewnij się, że katalog istnieje — jeśli nie, próbuj /tmp jako fallback
try:
    os.makedirs(_MASKING_LOG_DIR, exist_ok=True)
    _log_path = _MASKING_LOG_FILE
except OSError:
    _log_path = "/tmp/data_masking.jsonl"

_masking_file_handler = RotatingFileHandler(
    _log_path,
    maxBytes=50 * 1024 * 1024,   # 50 MB per plik
    backupCount=10,               # 10 plików archiwalnych = 500 MB max
    encoding="utf-8",
)
_masking_file_handler.setLevel(logging.DEBUG)

_masking_logger = logging.getLogger("windykacja.data_masking")
_masking_logger.setLevel(logging.DEBUG)
_masking_logger.addHandler(_masking_file_handler)
_masking_logger.propagate = False  # nie duplikuj do root loggera

# Główny logger aplikacji (do stdout/kontenerów)
_logger = logging.getLogger(__name__)

# =============================================================================
# Licznik operacji maskowania — thread-safe, reset per process lifecycle
# =============================================================================

_mask_counter_lock = threading.Lock()
_mask_counter: dict[str, int] = {
    "nazwa_kontrahenta": 0,
    "numer_faktury":     0,
    "total":             0,
    "errors":            0,
}


def _increment_counter(field: str) -> None:
    with _mask_counter_lock:
        _mask_counter[field] = _mask_counter.get(field, 0) + 1
        _mask_counter["total"] += 1


def get_masking_stats() -> dict[str, int]:
    """Zwraca kopię liczników — do health-check / diagnostyki."""
    with _mask_counter_lock:
        return dict(_mask_counter)


# =============================================================================
# Główna logika maskowania
# =============================================================================

def _compute_token(value: str, salt: str) -> str:
    """
    Deterministyczny 8-znakowy token hex z HMAC-SHA256.

    Właściwości:
        - Ten sam input → zawsze ten sam output (deterministyczność)
        - Różne inputy → różne outputy (pseudolosowość)
        - Nie da się odwrócić bez znajomości salt (jednokierunkowość)
        - Zmiana salt → zmiana wszystkich tokenów (re-keying)

    Args:
        value: oryginalna wartość do zamaskowania
        salt:  DATA_MASKING_SALT z .env

    Returns:
        8-znakowy uppercase hex, np. "A7F3B2C1"
    """
    digest = hmac.new(
        key=salt.encode("utf-8"),
        msg=value.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()
    return digest[:8].upper()


def mask_nazwa_kontrahenta(
    value: str | None,
    *,
    salt: str,
    record_id: int | str | None = None,
    context: dict[str, Any] | None = None,
) -> str | None:
    """
    Maskuje nazwę kontrahenta.

    Wzorzec wyjściowy: "KONTRAHENT-[A7F3B2C1]"

    Args:
        value:     oryginalna nazwa kontrahenta (None → None passthrough)
        salt:      DATA_MASKING_SALT z settings
        record_id: ID rekordu do audit trail (faktura.id lub numer_ksef)
        context:   dodatkowe dane do logu (endpoint, user_id itp.)

    Returns:
        Zamaskowana nazwa lub None jeśli value is None.

    Raises:
        Nigdy — błędy logowane, zwracany placeholder awaryjny.
    """
    if value is None:
        return None

    try:
        token = _compute_token(value, salt)
        masked = f"KONTRAHENT-[{token}]"

        _increment_counter("nazwa_kontrahenta")
        _write_audit_log(
            field="nazwa_kontrahenta",
            token=token,
            original_len=len(value),
            record_id=record_id,
            context=context or {},
        )

        return masked

    except Exception as exc:
        # Failsafe — nigdy nie zwracamy oryginalnej wartości przy błędzie
        with _mask_counter_lock:
            _mask_counter["errors"] += 1

        _logger.error(
            "data_masking | mask_nazwa_kontrahenta FAILED | "
            "record_id=%s exc=%s — returning safe placeholder",
            record_id,
            exc,
            exc_info=True,
        )
        return "KONTRAHENT-[ERROR]"


def mask_numer_faktury(
    value: str | None,
    *,
    salt: str,
    record_id: int | str | None = None,
    context: dict[str, Any] | None = None,
) -> str | None:
    """
    Maskuje numer faktury (WAPRO/Fakir — NIE numer KSeF).

    Wzorzec wyjściowy: "TST/2026/[4D8E]/0001"
    Zachowana struktura wizualna (segmenty rozdzielone /).

    Logika tokenu: HMAC z pełnej oryginalnej wartości →
    deterministycznie ta sama faktura → ten sam token.

    Args:
        value:     oryginalny numer faktury (None → None passthrough)
        salt:      DATA_MASKING_SALT z settings
        record_id: ID rekordu do audit trail
        context:   dodatkowe dane do logu

    Returns:
        Zamaskowany numer lub None jeśli value is None.
    """
    if value is None:
        return None

    try:
        token = _compute_token(value, salt)
        # Skrócony token 4 znaki dla numeru — segment bardziej zwięzły
        short_token = token[:4]
        year = datetime.now(timezone.utc).year
        masked = f"TST/{year}/[{short_token}]/0001"

        _increment_counter("numer_faktury")
        _write_audit_log(
            field="numer_faktury",
            token=short_token,
            original_len=len(value),
            record_id=record_id,
            context=context or {},
        )

        return masked

    except Exception as exc:
        with _mask_counter_lock:
            _mask_counter["errors"] += 1

        _logger.error(
            "data_masking | mask_numer_faktury FAILED | "
            "record_id=%s exc=%s — returning safe placeholder",
            record_id,
            exc,
            exc_info=True,
        )
        return "TST/[ERROR]/0001"


def mask_document_fields(
    doc: dict[str, Any],
    *,
    salt: str,
    record_id: int | str | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Maskuje wszystkie wrażliwe pola w słowniku dokumentu IN-PLACE.

    Pola objęte maskowaniem:
        - "nazwa_kontrahenta"
        - "numer"

    Nie dotyka pozostałych pól (kwoty, daty, statusy itd.).
    Modyfikuje przekazany słownik i zwraca go (dla wygody chainingu).

    Args:
        doc:       słownik reprezentujący dokument/fakturę
        salt:      DATA_MASKING_SALT z settings
        record_id: ID rekordu (do logu)
        context:   kontekst żądania (endpoint, user_id itp.)

    Returns:
        Ten sam słownik z zamaskowanymi polami.
    """
    ctx = context or {}

    if "nazwa_kontrahenta" in doc:
        doc["nazwa_kontrahenta"] = mask_nazwa_kontrahenta(
            doc["nazwa_kontrahenta"],
            salt=salt,
            record_id=record_id,
            context=ctx,
        )

    if "numer" in doc:
        doc["numer"] = mask_numer_faktury(
            doc["numer"],
            salt=salt,
            record_id=record_id,
            context=ctx,
        )

    return doc


# =============================================================================
# Audit log writer
# =============================================================================

def _write_audit_log(
    *,
    field: str,
    token: str,
    original_len: int,
    record_id: int | str | None,
    context: dict[str, Any],
) -> None:
    """
    Zapisuje pojedynczy wpis audit logu do pliku JSONL.

    Format JSONL — jeden JSON per linia, łatwy do przetwarzania przez jq/grep.

    Zawartość wpisu:
        ts          — timestamp UTC ISO-8601 z mikrosekundami
        field       — które pole zostało zamaskowane
        token       — wygenerowany token (nie oryginał!)
        orig_len    — długość oryginalnej wartości (do weryfikacji bez ujawniania)
        record_id   — ID rekordu (faktura.id lub numer_ksef)
        endpoint    — endpoint który wywołał maskowanie (z context)
        user_id     — zalogowany użytkownik (z context)
        request_id  — correlation ID żądania (z context)
        counter     — bieżąca wartość licznika total (dla detekcji anomalii)
    """
    try:
        with _mask_counter_lock:
            current_total = _mask_counter["total"]

        entry = {
            "ts":          datetime.now(timezone.utc).isoformat(timespec="microseconds"),
            "event":       "data_masked",
            "field":       field,
            "token":       token,
            "orig_len":    original_len,
            "record_id":   str(record_id) if record_id is not None else None,
            "endpoint":    context.get("endpoint"),
            "user_id":     context.get("user_id"),
            "request_id":  context.get("request_id"),
            "counter":     current_total,
        }

        _masking_logger.info(json.dumps(entry, ensure_ascii=False))

    except Exception as exc:
        # Log writer nigdy nie może crashować wywołującego
        _logger.warning("data_masking | _write_audit_log FAILED: %s", exc)