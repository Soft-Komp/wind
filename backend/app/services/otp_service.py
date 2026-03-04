"""
Serwis OTP (One-Time Password) — System Windykacja
====================================================
Krok 6 / Faza 3 — services/otp_service.py

Odpowiedzialność:
    - Generowanie 6-cyfrowych kodów OTP (password_reset / 2fa)
    - Bezpieczne przechowywanie (SHA-256 hash w tabeli OtpCodes)
    - Weryfikacja z constant-time compare
    - Blokada po 5 nieudanych próbach (Redis, 30 min)
    - send_stub: zapis do kolejki JSONL + log diagnostyczny
      (właściwy send realizowany przez ARQ worker w Fazie 6)

Decyzje projektowe:
    - SHA-256 (nie argon2) — OTP jest jednorazowy, krótkotrwały (≤15 min),
      constant-time compare eliminuje timing attacks
    - Kod przechowywany jako hex SHA-256 (64 znaki) w NVARCHAR(128)
    - Max 3 aktywne kody per user+purpose — stare są unieważniane przy przekroczeniu
    - TTL z SystemConfig("otp.expiry_minutes", default=15)
    - Wszystkie operacje asynchroniczne (SQLAlchemy async + aioodbc)
    - Pliki logów: append-only, JSON Lines, nieusuwalne przez aplikację

Zależności:
    - services/audit_service.py
    - services/config_service.py

Ścieżka docelowa: backend/app/services/otp_service.py
Autor: System Windykacja — Faza 3 Krok 6
Wersja: 1.0.0
Data: 2026-02-18
"""

from __future__ import annotations

import hashlib
import logging
import secrets
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import orjson
from redis.asyncio import Redis
from sqlalchemy import and_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.otp_code import OtpCode
from app.db.models.user import User
from app.services import audit_service
from app.services import config_service

# ---------------------------------------------------------------------------
# Logger własny dla tego modułu
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------

# Maksymalna liczba aktywnych kodów OTP per user+purpose
_MAX_ACTIVE_CODES: int = 3

# Liczba nieudanych prób przed blokadą emaila
_MAX_FAILED_ATTEMPTS: int = 5

# Czas blokady w sekundach po przekroczeniu limitu (30 min)
_LOCKOUT_TTL_SECONDS: int = 1800

# Domyślny TTL kodu OTP w minutach (fallback gdy baza niedostępna)
_DEFAULT_OTP_EXPIRY_MINUTES: int = 15

# Dozwolone cele kodu OTP (whitelist — walidacja na wejściu)
_VALID_PURPOSES: frozenset[str] = frozenset({"password_reset", "2fa"})

# Klucze Redis
_REDIS_KEY_FAIL_COUNT = "otp:fail:{email}"   # licznik nieudanych prób
_REDIS_KEY_LOCKOUT    = "otp:lock:{email}"   # flaga blokady

# Plik kolejki oczekujących wysyłek OTP (do odczytu przez ARQ worker w Fazie 6)
_OTP_SEND_QUEUE_FILE_PATTERN = "logs/otp_send_queue_{date}.jsonl"

# Plik logów diagnostycznych OTP (wszystkie operacje)
_OTP_AUDIT_FILE_PATTERN = "logs/otp_YYYY-MM-DD.jsonl"


# ===========================================================================
# Klasy wyjątków
# ===========================================================================

class OtpError(Exception):
    """Bazowy wyjątek serwisu OTP."""


class OtpPurposeInvalidError(OtpError):
    """Niedozwolony cel OTP (spoza whitelist)."""


class OtpRateLimitError(OtpError):
    """Przekroczono limit prób weryfikacji — konto zablokowane."""

    def __init__(self, email: str, ttl_seconds: int) -> None:
        self.email = email
        self.ttl_seconds = ttl_seconds
        super().__init__(
            f"Konto {email!r} zablokowane na {ttl_seconds}s po {_MAX_FAILED_ATTEMPTS} "
            f"nieudanych próbach weryfikacji OTP."
        )


class OtpVerificationError(OtpError):
    """Nieprawidłowy kod OTP (zły kod, wygasły lub już użyty)."""


class OtpUserNotFoundError(OtpError):
    """Użytkownik nie istnieje lub jest nieaktywny."""


# ===========================================================================
# Funkcje pomocnicze — prywatne
# ===========================================================================

def _hash_code(plain_code: str) -> str:
    """
    Oblicza SHA-256 hash kodu OTP.

    Args:
        plain_code: Czysty 6-cyfrowy kod OTP jako string.

    Returns:
        Hex-string SHA-256 (64 znaki) gotowy do zapisu w OtpCodes.Code (NVARCHAR(128)).
    """
    return hashlib.sha256(plain_code.encode("utf-8")).hexdigest()


def _constant_time_compare(val1: str, val2: str) -> bool:
    """
    Porównuje dwa stringi w stałym czasie (odporne na timing attacks).

    Używa hashlib.compare_digest, które zawsze zajmuje taki sam czas
    niezależnie od miejsca różnicy między stringami.

    Args:
        val1: Pierwszy string (np. hash z bazy).
        val2: Drugi string (np. świeżo obliczony hash).

    Returns:
        True jeśli identyczne, False w przeciwnym razie.
    """
    return hashlib.compare_digest(val1.encode("utf-8"), val2.encode("utf-8"))


def _sanitize_purpose(purpose: str) -> str:
    """
    Normalizuje i waliduje cel OTP.

    Normalizacja NFC + strip + lowercase + whitelist check.

    Args:
        purpose: Cel kodu OTP.

    Returns:
        Znormalizowany cel.

    Raises:
        OtpPurposeInvalidError: Jeśli cel nie jest w whiteliście.
    """
    normalized = unicodedata.normalize("NFC", purpose.strip().lower())
    if normalized not in _VALID_PURPOSES:
        raise OtpPurposeInvalidError(
            f"Niedozwolony cel OTP: {purpose!r}. "
            f"Dozwolone: {sorted(_VALID_PURPOSES)}"
        )
    return normalized


def _get_log_dir() -> Path:
    """
    Zwraca ścieżkę do katalogu logów, tworzy go jeśli nie istnieje.

    Returns:
        Path do katalogu logs/.
    """
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def _get_otp_audit_file() -> Path:
    """Zwraca dzienną ścieżkę pliku audytu OTP (JSON Lines)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return _get_log_dir() / f"otp_{today}.jsonl"


def _get_send_queue_file() -> Path:
    """Zwraca dzienną ścieżkę pliku kolejki wysyłek OTP (JSON Lines)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return _get_log_dir() / f"otp_send_queue_{today}.jsonl"


def _append_to_file(filepath: Path, record: dict) -> None:
    """
    Dopisuje rekord JSON do pliku JSON Lines (append-only, thread-safe przez OS).

    W przypadku błędu zapisu — loguje warning, NIE przerywa działania aplikacji.
    Pliki są nieusuwalne przez aplikację — tylko operacje append.

    Args:
        filepath: Ścieżka docelowego pliku.
        record:   Słownik do serializacji i zapisania.
    """
    try:
        line = orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE)
        with filepath.open("ab") as f:  # binary append — atomowy na poziomie OS
            f.write(line)
    except OSError as exc:
        logger.warning(
            "Nie można zapisać do pliku logu OTP",
            extra={
                "filepath": str(filepath),
                "error": str(exc),
                "record_action": record.get("action", "unknown"),
            }
        )


async def _get_user_by_id(db: AsyncSession, user_id: int) -> Optional[User]:
    """
    Pobiera aktywnego użytkownika po ID.

    Args:
        db:      Sesja SQLAlchemy.
        user_id: ID użytkownika.

    Returns:
        Obiekt User lub None jeśli nie istnieje/nieaktywny.
    """
    result = await db.execute(
        select(User).where(
            and_(
                User.id_user == user_id,
                User.is_active == True,  # noqa: E712
            )
        )
    )
    return result.scalar_one_or_none()


async def _invalidate_old_codes(
    db: AsyncSession,
    user_id: int,
    purpose: str,
) -> int:
    """
    Unieważnia nadmiarowe aktywne kody OTP dla danego użytkownika i celu.

    Pozostawia maksymalnie (_MAX_ACTIVE_CODES - 1) aktywnych kodów,
    aby zrobić miejsce na nowy. Unieważnia najstarsze kody (ORDER BY CreatedAt ASC).

    Args:
        db:      Sesja SQLAlchemy.
        user_id: ID użytkownika.
        purpose: Cel kodu OTP.

    Returns:
        Liczba unieważnionych kodów.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # Pobierz aktywne (nieużyte, nie wygasłe) kody — posortowane od najstarszego
    result = await db.execute(
        select(OtpCode)
        .where(
            and_(
                OtpCode.id_user == user_id,
                OtpCode.purpose == purpose,
                OtpCode.is_used == False,   # noqa: E712
                OtpCode.expires_at > now,
            )
        )
        .order_by(OtpCode.created_at.asc())
    )
    active_codes = result.scalars().all()

    # Jeśli jest ich za dużo — unieważnij najstarsze
    codes_to_invalidate = active_codes[: max(0, len(active_codes) - (_MAX_ACTIVE_CODES - 1))]
    invalidated_count = 0

    for code_obj in codes_to_invalidate:
        await db.execute(
            update(OtpCode)
            .where(OtpCode.id_otp == code_obj.id_otp)
            .values(is_used=True)
        )
        invalidated_count += 1

    if invalidated_count > 0:
        logger.info(
            "Unieważniono nadmiarowe kody OTP",
            extra={
                "user_id": user_id,
                "purpose": purpose,
                "invalidated_count": invalidated_count,
                "active_before": len(active_codes),
            }
        )

    return invalidated_count


def _build_audit_record(action: str, **kwargs) -> dict:
    """
    Buduje rekord audytowy do zapisu w pliku JSONL.

    Args:
        action: Nazwa akcji (np. "otp_generated", "otp_verified").
        **kwargs: Dodatkowe pola rekordu.

    Returns:
        Słownik z kompletnym rekordem audytowym.
    """
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": "otp_service",
        "action": action,
        **kwargs,
    }


# ===========================================================================
# Publiczne API serwisu
# ===========================================================================

async def generate(
    db: AsyncSession,
    redis: Redis,
    user_id: int,
    purpose: str,
    ip_address: Optional[str] = None,
) -> str:
    """
    Generuje nowy 6-cyfrowy kod OTP dla użytkownika.

    Przepływ:
        1. Walidacja i sanityzacja `purpose`
        2. Weryfikacja istnienia i aktywności użytkownika
        3. Unieważnienie nadmiarowych aktywnych kodów (max 3)
        4. Generowanie kryptograficznie bezpiecznego kodu (secrets.randbelow)
        5. Obliczenie SHA-256 hash kodu → zapis do OtpCodes
        6. Wywołanie send_stub (log + kolejka JSONL dla ARQ)
        7. Zapis do pliku audytu OTP

    Args:
        db:         Sesja SQLAlchemy (async).
        redis:      Klient Redis (async).
        user_id:    ID użytkownika dla którego generujemy kod.
        purpose:    Cel kodu — "password_reset" lub "2fa".
        ip_address: Adres IP inicjatora (opcjonalny, logowany).

    Returns:
        Czysty 6-cyfrowy kod OTP jako string (np. "847291").
        Zwracamy PLAIN KOD — żeby można go było wysłać użytkownikowi.
        W bazie przechowywany jest wyłącznie hash SHA-256.

    Raises:
        OtpPurposeInvalidError:  Gdy purpose nie jest w whiteliście.
        OtpUserNotFoundError:    Gdy użytkownik nie istnieje lub jest nieaktywny.
        OtpError:                Przy innych błędach serwisu.
    """
    # --- 1. Walidacja i sanityzacja purpose ---
    purpose = _sanitize_purpose(purpose)

    # --- 2. Weryfikacja użytkownika ---
    user = await _get_user_by_id(db, user_id)
    if user is None:
        logger.warning(
            "Próba generowania OTP dla nieistniejącego/nieaktywnego użytkownika",
            extra={"user_id": user_id, "purpose": purpose, "ip_address": ip_address}
        )
        raise OtpUserNotFoundError(
            f"Użytkownik ID={user_id} nie istnieje lub jest nieaktywny."
        )

    # --- 3. TTL z konfiguracji (z graceful fallback na default) ---
    expiry_minutes = await config_service.get_int(
        db, redis,
        key="otp.expiry_minutes",
        default=_DEFAULT_OTP_EXPIRY_MINUTES,
    )
    expires_at = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=expiry_minutes)

    # --- 4. Unieważnienie nadmiarowych starych kodów ---
    invalidated = await _invalidate_old_codes(db, user_id, purpose)

    # --- 5. Generowanie kryptograficznie bezpiecznego kodu ---
    # secrets.randbelow(900000) → zakres [0, 899999]
    # + 100000 → zakres [100000, 999999] — gwarantuje 6 cyfr
    plain_code: str = str(secrets.randbelow(900_000) + 100_000)
    code_hash: str = _hash_code(plain_code)

    # --- 6. Zapis do bazy danych ---
    new_otp = OtpCode(
        id_user=user_id,
        code=code_hash,
        purpose=purpose,
        expires_at=expires_at,
        is_used=False,
        ip_address=ip_address,
    )
    db.add(new_otp)
    await db.flush()  # Wymuszamy zapis do bazy przed wysyłką (mamy ID_OTP)

    otp_id: int = new_otp.id_otp

    logger.info(
        "Wygenerowano kod OTP",
        extra={
            "user_id": user_id,
            "username": user.username,
            "purpose": purpose,
            "otp_id": otp_id,
            "expires_at": expires_at.isoformat(),
            "expiry_minutes": expiry_minutes,
            "invalidated_old": invalidated,
            "ip_address": ip_address,
        }
    )

    # --- 7. send_stub — log + kolejka dla ARQ workera ---
    await send_stub(
        db=db,
        user=user,
        otp_code=plain_code,
        purpose=purpose,
        otp_id=otp_id,
        expires_at=expires_at,
        ip_address=ip_address,
    )

    await db.commit()

    # --- 8. Zapis do pliku audytu OTP ---
    _append_to_file(
        _get_otp_audit_file(),
        _build_audit_record(
            action="otp_generated",
            user_id=user_id,
            username=user.username,
            email=user.email,
            purpose=purpose,
            otp_id=otp_id,
            expires_at=expires_at.isoformat(),
            expiry_minutes=expiry_minutes,
            invalidated_old_codes=invalidated,
            ip_address=ip_address,
        )
    )

    # --- 9. AuditLog (fire-and-forget) ---
    audit_service.log(
        db=db,
        action="user_otp_generated",
        entity_type="OtpCode",
        entity_id=otp_id,
        details={
            "purpose": purpose,
            "expires_at": expires_at.isoformat(),
            "expiry_minutes": expiry_minutes,
            "invalidated_old_codes": invalidated,
        },
        success=True,
    )

    return plain_code


async def verify(
    db: AsyncSession,
    redis: Redis,
    user_id: int,
    plain_code: str,
    purpose: str,
    ip_address: Optional[str] = None,
) -> bool:
    """
    Weryfikuje kod OTP dla użytkownika.

    Przepływ:
        1. Sprawdzenie blokady Redis (otp:lock:{email})
        2. Sanityzacja purpose i plain_code
        3. Pobranie aktywnego, nie-wygasłego kodu z bazy
        4. Constant-time compare (SHA-256 hashes)
        5. Sukces: mark as used (is_used=True), reset licznika błędów
        6. Porażka: inkrementuj licznik błędów Redis,
           po _MAX_FAILED_ATTEMPTS → blokada na _LOCKOUT_TTL_SECONDS

    Args:
        db:         Sesja SQLAlchemy (async).
        redis:      Klient Redis (async).
        user_id:    ID użytkownika.
        plain_code: Czysty kod OTP podany przez użytkownika.
        purpose:    Cel weryfikacji ("password_reset" lub "2fa").
        ip_address: IP inicjatora (logowane).

    Returns:
        True jeśli kod jest prawidłowy i nie wygasł.

    Raises:
        OtpPurposeInvalidError: Gdy purpose nie jest w whiteliście.
        OtpRateLimitError:      Gdy konto jest zablokowane po 5 błędnych próbach.
        OtpVerificationError:   Gdy kod jest nieprawidłowy, wygasł lub użyty.
        OtpUserNotFoundError:   Gdy użytkownik nie istnieje.
    """
    # --- 1. Sanityzacja ---
    purpose = _sanitize_purpose(purpose)
    # Sanityzacja kodu — tylko cyfry, max 6 znaków
    plain_code = plain_code.strip()
    if not plain_code.isdigit() or len(plain_code) != 6:
        logger.warning(
            "OTP: Nieprawidłowy format kodu (nie jest 6-cyfrowy)",
            extra={"user_id": user_id, "purpose": purpose, "ip_address": ip_address}
        )
        raise OtpVerificationError("Kod OTP musi zawierać dokładnie 6 cyfr.")

    # --- 2. Pobierz użytkownika (potrzebujemy email do kluczy Redis) ---
    user = await _get_user_by_id(db, user_id)
    if user is None:
        raise OtpUserNotFoundError(
            f"Użytkownik ID={user_id} nie istnieje lub jest nieaktywny."
        )

    email = user.email
    redis_fail_key   = _REDIS_KEY_FAIL_COUNT.format(email=email)
    redis_lock_key   = _REDIS_KEY_LOCKOUT.format(email=email)

    # --- 3. Sprawdź blokadę Redis ---
    is_locked = await redis.exists(redis_lock_key)
    if is_locked:
        ttl = await redis.ttl(redis_lock_key)
        logger.warning(
            "OTP: Próba weryfikacji zablokowanego konta",
            extra={
                "user_id": user_id,
                "email": email,
                "purpose": purpose,
                "lockout_ttl_remaining": ttl,
                "ip_address": ip_address,
            }
        )
        raise OtpRateLimitError(email=email, ttl_seconds=max(ttl, 0))

    # --- 4. Pobierz aktywny, nie-wygasły kod z bazy ---
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    result = await db.execute(
        select(OtpCode)
        .where(
            and_(
                OtpCode.id_user == user_id,
                OtpCode.purpose == purpose,
                OtpCode.is_used == False,   # noqa: E712
                OtpCode.expires_at > now,
            )
        )
        .order_by(OtpCode.created_at.desc())  # Najnowszy kod
        .limit(1)
    )
    code_obj = result.scalar_one_or_none()

    if code_obj is None:
        logger.info(
            "OTP: Brak aktywnego kodu dla użytkownika",
            extra={"user_id": user_id, "purpose": purpose, "ip_address": ip_address}
        )
        await _handle_failed_attempt(
            redis=redis,
            email=email,
            redis_fail_key=redis_fail_key,
            redis_lock_key=redis_lock_key,
            user_id=user_id,
            purpose=purpose,
            ip_address=ip_address,
            reason="no_active_code",
        )
        raise OtpVerificationError(
            "Brak aktywnego kodu OTP. Wygeneruj nowy kod."
        )

    # --- 5. Constant-time compare ---
    submitted_hash = _hash_code(plain_code)
    code_valid = _constant_time_compare(code_obj.code, submitted_hash)

    if not code_valid:
        logger.info(
            "OTP: Nieprawidłowy kod",
            extra={
                "user_id": user_id,
                "purpose": purpose,
                "otp_id": code_obj.id_otp,
                "ip_address": ip_address,
            }
        )
        await _handle_failed_attempt(
            redis=redis,
            email=email,
            redis_fail_key=redis_fail_key,
            redis_lock_key=redis_lock_key,
            user_id=user_id,
            purpose=purpose,
            ip_address=ip_address,
            reason="invalid_code",
        )
        raise OtpVerificationError(
            "Nieprawidłowy kod OTP."
        )

    # --- 6. Sukces — oznacz jako użyty ---
    await db.execute(
        update(OtpCode)
        .where(OtpCode.id_otp == code_obj.id_otp)
        .values(is_used=True)
    )

    # Reset licznika błędów
    await redis.delete(redis_fail_key)

    logger.info(
        "OTP: Weryfikacja zakończona sukcesem",
        extra={
            "user_id": user_id,
            "username": user.username,
            "purpose": purpose,
            "otp_id": code_obj.id_otp,
            "ip_address": ip_address,
        }
    )

    # Zapis do pliku audytu
    _append_to_file(
        _get_otp_audit_file(),
        _build_audit_record(
            action="otp_verified",
            user_id=user_id,
            username=user.username,
            email=email,
            purpose=purpose,
            otp_id=code_obj.id_otp,
            ip_address=ip_address,
        )
    )

    # AuditLog (fire-and-forget)
    audit_service.log(
        db=db,
        action="user_otp_verified",
        action_category="Auth",
        entity_type="OtpCode",
        entity_id=code_obj.id_otp,
        details={"purpose": purpose},
        success=True,
    )

    return True


async def _handle_failed_attempt(
    redis: Redis,
    email: str,
    redis_fail_key: str,
    redis_lock_key: str,
    user_id: int,
    purpose: str,
    ip_address: Optional[str],
    reason: str,
) -> None:
    """
    Obsługuje nieudaną próbę weryfikacji OTP.

    Inkrementuje licznik nieudanych prób w Redis.
    Po przekroczeniu _MAX_FAILED_ATTEMPTS → ustawia blokadę na _LOCKOUT_TTL_SECONDS.

    Args:
        redis:           Klient Redis.
        email:           Email użytkownika (klucz blokady).
        redis_fail_key:  Klucz Redis licznika błędów.
        redis_lock_key:  Klucz Redis flagi blokady.
        user_id:         ID użytkownika (do logowania).
        purpose:         Cel OTP (do logowania).
        ip_address:      IP inicjatora (do logowania).
        reason:          Przyczyna niepowodzenia (do logowania).
    """
    try:
        fail_count = await redis.incr(redis_fail_key)
        # TTL licznika — po 30 min reset (niezależnie od blokady)
        await redis.expire(redis_fail_key, _LOCKOUT_TTL_SECONDS)

        logger.warning(
            "OTP: Nieudana próba weryfikacji",
            extra={
                "user_id": user_id,
                "email": email,
                "purpose": purpose,
                "fail_count": fail_count,
                "max_attempts": _MAX_FAILED_ATTEMPTS,
                "reason": reason,
                "ip_address": ip_address,
            }
        )

        _append_to_file(
            _get_otp_audit_file(),
            _build_audit_record(
                action="otp_verification_failed",
                user_id=user_id,
                email=email,
                purpose=purpose,
                fail_count=fail_count,
                max_attempts=_MAX_FAILED_ATTEMPTS,
                reason=reason,
                ip_address=ip_address,
            )
        )

        if fail_count >= _MAX_FAILED_ATTEMPTS:
            # Ustaw blokadę
            await redis.set(redis_lock_key, "1", ex=_LOCKOUT_TTL_SECONDS)
            # Reset licznika (blokada teraz obowiązuje)
            await redis.delete(redis_fail_key)

            logger.error(
                "OTP: Konto zablokowane po przekroczeniu limitu prób",
                extra={
                    "user_id": user_id,
                    "email": email,
                    "purpose": purpose,
                    "lockout_seconds": _LOCKOUT_TTL_SECONDS,
                    "ip_address": ip_address,
                }
            )

            _append_to_file(
                _get_otp_audit_file(),
                _build_audit_record(
                    action="otp_account_locked",
                    user_id=user_id,
                    email=email,
                    purpose=purpose,
                    lockout_seconds=_LOCKOUT_TTL_SECONDS,
                    ip_address=ip_address,
                )
            )

    except Exception as exc:
        # Redis niedostępny — logujemy, ale NIE przerywamy flow
        logger.error(
            "OTP: Błąd Redis przy obsłudze nieudanej próby",
            extra={
                "error": str(exc),
                "user_id": user_id,
                "email": email,
                "purpose": purpose,
            }
        )


async def send_stub(
    db: AsyncSession,
    user: User,
    otp_code: str,
    purpose: str,
    otp_id: int,
    expires_at: datetime,
    ip_address: Optional[str] = None,
) -> None:
    """
    STUB wysyłki kodu OTP — zastępstwo do czasu implementacji ARQ workera (Faza 6).

    Działanie:
        1. Loguje szczegóły planowanej wysyłki do pliku diagnostycznego
        2. Zapisuje rekord do kolejki JSONL (logs/otp_send_queue_YYYY-MM-DD.jsonl)
           — plik ten jest odczytywany przez ARQ workera w Fazie 6
        3. Log WARNING widoczny w docker logs (informacja że to stub)

    ⚠️  WAŻNE: Ten stub NIE wysyła żadnego emaila/SMS.
    Właściwa wysyłka implementowana jest w:
        worker/tasks/email_task.py (Faza 6)

    Kolejka JSONL zawiera wszystkie dane potrzebne do wysyłki:
        - user_id, email, full_name
        - otp_code (PLAIN — wyłącznie w kolejce, NIE w bazie)
        - purpose, otp_id, expires_at

    Args:
        db:         Sesja SQLAlchemy (do potwierdzenia stanu w logach).
        user:       Obiekt User (email, username, full_name).
        otp_code:   Czysty 6-cyfrowy kod OTP do "wysłania".
        purpose:    Cel wysyłki ("password_reset" lub "2fa").
        otp_id:     ID rekordu w tabeli OtpCodes.
        expires_at: Czas wygaśnięcia kodu.
        ip_address: IP inicjatora (opcjonalny).
    """
    now = datetime.now(timezone.utc)

    # Rekord kolejki wysyłki — kompletne dane dla ARQ workera
    send_queue_record = {
        "ts": now.isoformat(),
        "action": "otp_send_pending",
        "otp_id": otp_id,
        "user_id": user.id_user,
        "username": user.username,
        "email": user.email,
        "full_name": user.full_name,
        "purpose": purpose,
        # UWAGA: plain OTP code zapisany tylko w kolejce (nie w DB)
        # ARQ worker odczyta ten plik i wyśle email/SMS
        "otp_code": otp_code,
        "expires_at": expires_at.isoformat(),
        "channel": "email",  # Domyślny kanał — worker może obsługiwać email/SMS
        "ip_address": ip_address,
        "stub": True,  # Flaga: to jest stub — właściwy send w Fazie 6
    }

    # Zapis do kolejki JSONL (ARQ worker odbierze w Fazie 6)
    _append_to_file(_get_send_queue_file(), send_queue_record)

    # Log diagnostyczny
    logger.warning(
        "[STUB] Kod OTP wygenerowany — faktyczna wysyłka przez ARQ worker (Faza 6). "
        "Kod zapisany do kolejki JSONL.",
        extra={
            "otp_id": otp_id,
            "user_id": user.id_user,
            "email": user.email,
            "purpose": purpose,
            "expires_at": expires_at.isoformat(),
            "queue_file": str(_get_send_queue_file()),
            "stub": True,
        }
    )


async def invalidate_all_for_user(
    db: AsyncSession,
    user_id: int,
    purpose: Optional[str] = None,
) -> int:
    """
    Unieważnia WSZYSTKIE aktywne kody OTP dla użytkownika.

    Używane przy:
        - Zmianie hasła (invalidacja wszystkich reset kodów)
        - Dezaktywacji konta
        - Wymuszonym wylogowaniu

    Args:
        db:      Sesja SQLAlchemy.
        user_id: ID użytkownika.
        purpose: Opcjonalne filtrowanie po celu. None = wszystkie cele.

    Returns:
        Liczba unieważnionych kodów.
    """
    now = datetime.now(timezone.utc)

    conditions = [
        OtpCode.id_user == user_id,
        OtpCode.is_used == False,   # noqa: E712
        OtpCode.expires_at > now,
    ]
    if purpose is not None:
        purpose = _sanitize_purpose(purpose)
        conditions.append(OtpCode.purpose == purpose)

    result = await db.execute(
        update(OtpCode)
        .where(and_(*conditions))
        .values(is_used=True)
        .returning(OtpCode.id_otp)
    )
    invalidated_ids = [row[0] for row in result.fetchall()]
    count = len(invalidated_ids)

    if count > 0:
        logger.info(
            "Unieważniono wszystkie aktywne kody OTP użytkownika",
            extra={
                "user_id": user_id,
                "purpose": purpose,
                "invalidated_count": count,
                "invalidated_ids": invalidated_ids,
            }
        )
        _append_to_file(
            _get_otp_audit_file(),
            _build_audit_record(
                action="otp_bulk_invalidated",
                user_id=user_id,
                purpose=purpose,
                invalidated_count=count,
                invalidated_ids=invalidated_ids,
            )
        )

    return count


async def get_active_count(
    db: AsyncSession,
    user_id: int,
    purpose: Optional[str] = None,
) -> int:
    """
    Zwraca liczbę aktywnych (nieużytych, nie wygasłych) kodów OTP dla użytkownika.

    Przydatne do diagnostyki i monitoringu.

    Args:
        db:      Sesja SQLAlchemy.
        user_id: ID użytkownika.
        purpose: Opcjonalne filtrowanie po celu.

    Returns:
        Liczba aktywnych kodów.
    """
    now = datetime.now(timezone.utc)

    conditions = [
        OtpCode.id_user == user_id,
        OtpCode.is_used == False,   # noqa: E712
        OtpCode.expires_at > now,
    ]
    if purpose is not None:
        purpose = _sanitize_purpose(purpose)
        conditions.append(OtpCode.purpose == purpose)

    from sqlalchemy import func
    result = await db.execute(
        select(func.count(OtpCode.id_otp)).where(and_(*conditions))
    )
    return result.scalar_one() or 0


async def cleanup_expired(db: AsyncSession) -> int:
    """
    Usuwa (soft-invalidate) wygasłe kody OTP z bazy.

    Wywoływana przez cron ARQ (opcjonalnie) lub przy starcie serwisu
    jako housekeeping. Nie dotyczy używanych — te już mają is_used=True.

    Args:
        db: Sesja SQLAlchemy.

    Returns:
        Liczba "wyczyszczonych" rekordów.
    """
    now = datetime.now(timezone.utc)

    result = await db.execute(
        update(OtpCode)
        .where(
            and_(
                OtpCode.is_used == False,   # noqa: E712
                OtpCode.expires_at <= now,
            )
        )
        .values(is_used=True)
        .returning(OtpCode.id_otp)
    )
    cleaned_ids = [row[0] for row in result.fetchall()]
    count = len(cleaned_ids)

    if count > 0:
        logger.info(
            "OTP cleanup: Unieważniono wygasłe kody",
            extra={
                "cleaned_count": count,
                "timestamp": now.isoformat(),
            }
        )
        _append_to_file(
            _get_otp_audit_file(),
            _build_audit_record(
                action="otp_expired_cleanup",
                cleaned_count=count,
                timestamp=now.isoformat(),
            )
        )

    return count


async def check_lockout_status(
    redis: Redis,
    email: str,
) -> dict:
    """
    Sprawdza status blokady konta dla danego emaila.

    Przydatne do monitoringu i diagnostyki — endpoint systemu.

    Args:
        redis: Klient Redis.
        email: Adres email do sprawdzenia.

    Returns:
        Słownik z informacjami o blokadzie:
        {
            "is_locked": bool,
            "lockout_ttl_seconds": int | None,
            "fail_count": int,
            "max_attempts": int,
        }
    """
    redis_fail_key = _REDIS_KEY_FAIL_COUNT.format(email=email)
    redis_lock_key = _REDIS_KEY_LOCKOUT.format(email=email)

    try:
        is_locked_raw = await redis.exists(redis_lock_key)
        fail_count_raw = await redis.get(redis_fail_key)
        lockout_ttl = await redis.ttl(redis_lock_key) if is_locked_raw else None

        return {
            "is_locked": bool(is_locked_raw),
            "lockout_ttl_seconds": max(lockout_ttl, 0) if lockout_ttl is not None else None,
            "fail_count": int(fail_count_raw) if fail_count_raw else 0,
            "max_attempts": _MAX_FAILED_ATTEMPTS,
        }
    except Exception as exc:
        logger.error(
            "OTP: Błąd Redis przy sprawdzaniu statusu blokady",
            extra={"email": email, "error": str(exc)}
        )
        return {
            "is_locked": False,
            "lockout_ttl_seconds": None,
            "fail_count": 0,
            "max_attempts": _MAX_FAILED_ATTEMPTS,
            "redis_error": str(exc),
        }
    
async def request_otp(
    db: AsyncSession,
    redis: Redis,
    email: str,
    purpose: str,
    ip: Optional[str] = None,
) -> None:
    """
    Wrapper dla endpointu — szuka usera po emailu i generuje OTP.
    Nie rzuca wyjątku gdy email nie istnieje (anty-enumeracja).
    """
    from sqlalchemy import select
    from app.db.models.user import User

    email_clean = email.lower().strip()[:100]

    result = await db.execute(
        select(User).where(
            User.email == email_clean,
            User.is_active == True,
        )
    )
    user = result.scalar_one_or_none()

    if user is None:
        logger.debug(
            "request_otp: email nie istnieje lub nieaktywny — bez akcji (anty-enumeracja)",
            extra={"email_hash": hashlib.sha256(email_clean.encode()).hexdigest()},
        )
        return  # Celowo nic nie robimy

    await generate(
        db=db,
        redis=redis,
        user_id=user.id_user,
        purpose=purpose,
        ip_address=ip,
    )