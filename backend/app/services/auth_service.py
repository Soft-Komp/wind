"""
backend/app/services/auth_service.py
======================================
Serwis autentykacji — centralny moduł bezpieczeństwa systemu.

Odpowiedzialności:
    - Logowanie / wylogowanie / odświeżanie tokenów JWT
    - Weryfikacja tożsamości (argon2id + lockout mechanism)
    - Zarządzanie RefreshTokens w bazie danych
    - Blacklista access tokenów w Redis
    - Zmiana / reset hasła z OTP
    - Impersonacja użytkownika (auth.impersonate)
    - Rate limiting: slowapi (deklaratywny) + Redis counter (redundancja)
    - Master Key access (zapis TYLKO do MasterAccessLog)
    - Dependency Injection: get_current_user(), require_permission()

Security Model:
    ┌─────────────────────────────────────────────────────────────┐
    │  Linia obrony #1: Rate limiting (slowapi + Redis counter)   │
    │  Linia obrony #2: Argon2id password verification           │
    │  Linia obrony #3: Account lockout (FailedLoginAttempts)    │
    │  Linia obrony #4: JWT blacklist (Redis)                    │
    │  Linia obrony #5: RefreshToken DB validation               │
    │  Linia obrony #6: Constant-time comparisons                │
    └─────────────────────────────────────────────────────────────┘

Rate Limiting (podwójna redundancja):
    - slowapi: deklaratywny, integracja z FastAPI, widoczny w Swagger
    - Redis counter: własny, precyzyjny per-IP, niezależny od slowapi
    - Blokada IP przy master_key: 3 próby / 15min → Redis ban 1h

Wersja: 1.0.0
Data:   2026-02-18
Autor:  System Windykacja
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import math
import os
import secrets
import traceback
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import bcrypt
from argon2 import PasswordHasher
from argon2.exceptions import (
    InvalidHashError,
    VerificationError,
    VerifyMismatchError,
)
from jose import ExpiredSignatureError, JWTError, jwt
from redis.asyncio import Redis
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.config import get_settings
from app.db.models.audit_log import AuditLog
from app.db.models.master_access_log import MasterAccessLog
from app.db.models.otp_code import OtpCode
from app.db.models.refresh_token import RefreshToken
from app.db.models.user import User
from app.db.models.role import Role
from app.db.models.role_permission import RolePermission
from app.db.models.permission import Permission
from app.services import audit_service
from app.services import config_service

# ---------------------------------------------------------------------------
# Logger modułu
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Argon2id hasher — konfiguracja produkcyjna
# ---------------------------------------------------------------------------
_ph = PasswordHasher(
    time_cost=3,       # iteracje (OWASP minimum dla argon2id)
    memory_cost=65536, # 64MB RAM
    parallelism=4,     # 4 wątki
    hash_len=32,
    salt_len=16,
    encoding="utf-8",
)

# ---------------------------------------------------------------------------
# Stałe bezpieczeństwa
# ---------------------------------------------------------------------------

# Maksymalna liczba nieudanych prób logowania przed blokadą
_MAX_FAILED_ATTEMPTS: int = 5
# Czas blokady konta w minutach
_LOCKOUT_DURATION_MINUTES: int = 30

# Redis klucze
_REDIS_PREFIX_BLACKLIST:      str = "auth:blacklist:"    # access token blacklist
_REDIS_PREFIX_RL_LOGIN:       str = "rl:login:"          # rate limit login per IP
_REDIS_PREFIX_RL_REFRESH:     str = "rl:refresh:"        # rate limit refresh per IP
_REDIS_PREFIX_RL_MASTER:      str = "rl:master:"         # rate limit master key per IP
_REDIS_PREFIX_RL_BAN:         str = "rl:ban:"            # IP ban (master key abuse)
_REDIS_PREFIX_OTP_FAIL:       str = "otp:fail:"          # OTP failed attempts per email
_REDIS_PREFIX_RESET_TOKEN:    str = "reset_token:"       # password reset token

# Rate limit windows (sekundy)
_RL_LOGIN_WINDOW:   int = 60       # 1 minuta
_RL_LOGIN_LIMIT:    int = 10       # 10 prób / minutę / IP
_RL_REFRESH_WINDOW: int = 60       # 1 minuta
_RL_REFRESH_LIMIT:  int = 30       # 30 prób / minutę / IP
_RL_MASTER_WINDOW:  int = 900      # 15 minut
_RL_MASTER_LIMIT:   int = 3        # 3 próby / 15min / IP
_RL_BAN_DURATION:   int = 3600     # 1 godzina bana po przekroczeniu master

# OTP
_OTP_FAIL_LIMIT:    int = 5        # max nieudanych prób OTP
_OTP_FAIL_WINDOW:   int = 1800     # 30 minut

# Reset token TTL
_RESET_TOKEN_TTL:   int = 600      # 10 minut

# Token hasher (RefreshToken → SHA256 przed zapisem do DB)
_TOKEN_HASH_ALGORITHM: str = "sha256"

# Maksymalna liczba aktywnych sesji per użytkownik
_MAX_ACTIVE_SESSIONS: int = 10

# ---------------------------------------------------------------------------
# Typy wyjątków
# ---------------------------------------------------------------------------

class AuthError(Exception):
    """Błąd autentykacji — zwracany jako HTTP 401."""
    def __init__(self, message: str, code: str = "auth_error") -> None:
        super().__init__(message)
        self.message = message
        self.code = code


class AccountLockedError(AuthError):
    """Konto zablokowane — HTTP 423."""
    def __init__(self, locked_until: datetime) -> None:
        super().__init__(
            f"Konto zablokowane do {locked_until.isoformat()}",
            code="account_locked",
        )
        self.locked_until = locked_until


class PermissionDeniedError(Exception):
    """Brak uprawnienia — HTTP 403."""
    def __init__(self, permission: str) -> None:
        super().__init__(f"Brak uprawnienia: {permission}")
        self.permission = permission


class RateLimitExceededError(Exception):
    """Przekroczono limit żądań — HTTP 429."""
    def __init__(self, retry_after: int = 60) -> None:
        super().__init__(f"Zbyt wiele żądań. Spróbuj za {retry_after}s.")
        self.retry_after = retry_after


class TokenExpiredError(AuthError):
    """Token wygasł — HTTP 401."""
    def __init__(self) -> None:
        super().__init__("Token wygasł", code="token_expired")


class TokenBlacklistedError(AuthError):
    """Token unieważniony — HTTP 401."""
    def __init__(self) -> None:
        super().__init__("Token unieważniony", code="token_blacklisted")


# ---------------------------------------------------------------------------
# Dataclasses wyników
# ---------------------------------------------------------------------------

from dataclasses import dataclass, field


@dataclass
class TokenPair:
    """Para tokenów JWT zwracana przy login/refresh."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int = 0       # sekundy do wygaśnięcia access_token
    user_id: int = 0
    username: str = ""
    role: str = ""
    permissions: list[str] = field(default_factory=list)


@dataclass
class CurrentUser:
    """Zdenormalizowany obiekt zalogowanego użytkownika — używany przez Depends."""
    id: int
    username: str
    email: str
    full_name: Optional[str]
    role_id: int
    role_name: str
    permissions: frozenset[str]
    is_active: bool
    is_impersonation: bool = False
    impersonated_by: Optional[int] = None

    def has_permission(self, permission: str) -> bool:
        return permission in self.permissions

    def require_permission(self, permission: str) -> None:
        if not self.has_permission(permission):
            raise PermissionDeniedError(permission)


# ---------------------------------------------------------------------------
# Helpers: hasło, token, porównania
# ---------------------------------------------------------------------------

def hash_password(password: str) -> str:
    """
    Hashuje hasło używając argon2id.
    Zwraca kompletny hash z parametrami (PHC string format).
    """
    return _ph.hash(password)


def verify_password(plain_password: str, hashed: str) -> bool:
    """
    Weryfikuje hasło argon2id.
    Zwraca True jeśli poprawne, False jeśli niepoprawne.
    Nigdy nie rzuca wyjątku do callera.
    """
    try:
        return _ph.verify(hashed, plain_password)
    except VerifyMismatchError:
        return False
    except (VerificationError, InvalidHashError) as exc:
        logger.error(
            "Błąd weryfikacji hasła argon2: %s",
            exc,
            extra={"traceback": traceback.format_exc()},
        )
        return False


def password_needs_rehash(hashed: str) -> bool:
    """
    Sprawdza czy hash wymaga aktualizacji (zmiana parametrów argon2).
    Używany po udanym logowaniu.
    """
    try:
        return _ph.check_needs_rehash(hashed)
    except Exception:
        return False


def _hash_token(token: str) -> str:
    """SHA256 hash tokena do zapisu w DB (RefreshToken.Token)."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _constant_time_compare(val1: str, val2: str) -> bool:
    """
    Porównanie ciągów odporne na timing attacks.
    Używaj zawsze przy porównywaniu kluczy/tokenów.
    """
    return hmac.compare_digest(
        val1.encode("utf-8"),
        val2.encode("utf-8"),
    )


def _sanitize_credential(value, max_len: int = 255) -> str:
    # Obsłuż SecretStr z Pydantic
    if hasattr(value, "get_secret_value"):
        value = value.get_secret_value()
    normalized = unicodedata.normalize("NFC", value)
    stripped = normalized.strip()
    return stripped[:max_len]


# ---------------------------------------------------------------------------
# JWT: tworzenie i dekodowanie tokenów
# ---------------------------------------------------------------------------

def _get_settings():
    """Lazy import ustawień (unikamy circular imports)."""
    return get_settings()


def _create_access_token(
    user_id: int,
    username: str,
    role: str,
    permissions: list[str],
    *,
    is_impersonation: bool = False,
    impersonated_by: Optional[int] = None,
    extra_claims: Optional[dict[str, Any]] = None,
) -> tuple[str, datetime]:
    """
    Tworzy access token JWT (krótkotrwały).

    Returns:
        (token_string, expires_at_utc)
    """
    settings = _get_settings()
    now = datetime.now(timezone.utc)
    expire_hours = int(settings.ACCESS_TOKEN_EXPIRE_HOURS)
    expires_at = now + timedelta(hours=expire_hours)

    payload: dict[str, Any] = {
        "sub":         str(user_id),
        "username":    username,
        "role":        role,
        "permissions": permissions,
        "iat":         int(now.timestamp()),
        "exp":         int(expires_at.timestamp()),
        "type":        "access",
        "jti":         secrets.token_hex(16),   # JWT ID — unikalny per token
    }

    if is_impersonation:
        payload["is_impersonation"] = True
        payload["impersonated_by"] = impersonated_by

    if extra_claims:
        payload.update(extra_claims)

    token = jwt.encode(
        payload,
        settings.SECRET_KEY.get_secret_value(),
        algorithm=settings.ALGORITHM,
    )

    logger.debug(
        "Access token utworzony: user_id=%d, username=%s, expires=%s, impersonation=%s",
        user_id, username, expires_at.isoformat(), is_impersonation,
        extra={
            "user_id": user_id,
            "token_type": "access",
            "expires_at": expires_at.isoformat(),
            "is_impersonation": is_impersonation,
        },
    )
    return token, expires_at


def _create_refresh_token() -> tuple[str, str]:
    """
    Generuje refresh token.
    Returns:
        (raw_token, hashed_token)
        raw_token   — wysyłany do klienta
        hashed_token — zapisywany w DB
    """
    raw = secrets.token_urlsafe(64)
    hashed = _hash_token(raw)
    return raw, hashed


def _decode_access_token(token: str) -> dict[str, Any]:
    """
    Dekoduje i weryfikuje access token JWT.

    Raises:
        TokenExpiredError:  Token wygasł
        AuthError:          Token niepoprawny (signature, format itp.)
    """
    settings = _get_settings()
    try:
        payload = jwt.decode(
            token,
            settings.SECRET_KEY.get_secret_value(),
            algorithms=[settings.ALGORITHM],
        )
        if payload.get("type") != "access":
            raise AuthError("Nieprawidłowy typ tokena", code="invalid_token_type")
        return payload
    except ExpiredSignatureError as exc:
        raise TokenExpiredError() from exc
    except JWTError as exc:
        raise AuthError(f"Nieprawidłowy token: {exc}", code="invalid_token") from exc


# ---------------------------------------------------------------------------
# Rate Limiting — Redis counter (warstwa #1 redundancji)
# ---------------------------------------------------------------------------

async def _check_rate_limit_redis(
    redis: Optional[Redis],
    ip: str,
    prefix: str,
    limit: int,
    window: int,
) -> tuple[int, int]:
    """
    Sprawdza i inkrementuje licznik rate limit w Redis.
    Używa INCR + EXPIRE (atomic per polecenie, bezpieczne dla wysokiej concurrency).

    Returns:
        (current_count, ttl_remaining)

    Raises:
        RateLimitExceededError: Gdy limit przekroczony
    """
    if redis is None:
        # Redis niedostępny — nie blokujemy (graceful degradation)
        logger.warning(
            "Redis niedostępny — rate limiting Redis pominięty dla IP=%s, prefix=%s",
            ip, prefix,
        )
        return 0, window

    key = f"{prefix}{ip}"
    try:
        current = await redis.incr(key)
        if current == 1:
            # Pierwszy wpis — ustaw TTL
            await redis.expire(key, window)

        ttl = await redis.ttl(key)
        retry_after = max(ttl, 1)

        if current > limit:
            logger.warning(
                "Rate limit PRZEKROCZONY (Redis): prefix=%s, IP=%s, count=%d/%d, ttl=%ds",
                prefix, ip, current, limit, ttl,
                extra={
                    "rate_limit_prefix": prefix,
                    "ip": ip,
                    "count": current,
                    "limit": limit,
                    "ttl": ttl,
                },
            )
            raise RateLimitExceededError(retry_after=retry_after)

        logger.debug(
            "Rate limit OK (Redis): prefix=%s, IP=%s, count=%d/%d",
            prefix, ip, current, limit,
            extra={
                "rate_limit_prefix": prefix,
                "ip": ip,
                "count": current,
                "limit": limit,
            },
        )
        return current, ttl

    except RateLimitExceededError:
        raise
    except Exception as exc:
        # Redis error — fail open (graceful)
        logger.error(
            "Błąd Redis rate limit: %s — kontynuuję bez blokady",
            exc,
            extra={"traceback": traceback.format_exc()},
        )
        return 0, window


async def _check_ip_ban(redis: Optional[Redis], ip: str) -> None:
    """
    Sprawdza czy IP jest na liście banów (master key abuse).
    Raises RateLimitExceededError jeśli zbanowany.
    """
    if redis is None:
        return
    try:
        ban_key = f"{_REDIS_PREFIX_RL_BAN}{ip}"
        ttl = await redis.ttl(ban_key)
        if ttl > 0:
            logger.warning(
                "Zbanowane IP próbuje dostępu: %s, ban_ttl=%ds",
                ip, ttl,
                extra={"ip": ip, "ban_ttl": ttl},
            )
            raise RateLimitExceededError(retry_after=ttl)
    except RateLimitExceededError:
        raise
    except Exception as exc:
        logger.error("Błąd sprawdzania IP ban: %s", exc)


async def _ban_ip(redis: Optional[Redis], ip: str, duration: int = _RL_BAN_DURATION) -> None:
    """Ustawia ban IP (master key abuse)."""
    if redis is None:
        return
    try:
        ban_key = f"{_REDIS_PREFIX_RL_BAN}{ip}"
        await redis.setex(ban_key, duration, "1")
        logger.warning(
            "IP zbanowane na %ds: %s",
            duration, ip,
            extra={"ip": ip, "ban_duration": duration},
        )
    except Exception as exc:
        logger.error("Błąd ustawiania IP ban: %s", exc)


# ---------------------------------------------------------------------------
# Blacklista tokenów Redis
# ---------------------------------------------------------------------------

async def _blacklist_token(
    redis: Optional[Redis],
    token: str,
    ttl: int,
) -> None:
    """
    Dodaje access token do Redis blacklisty.
    TTL = pozostały czas życia tokena (nie ma sensu trzymać dłużej).
    """
    if redis is None:
        logger.warning(
            "Redis niedostępny — access token NIE dodany do blacklisty!",
            extra={"critical": True},
        )
        return
    if ttl <= 0:
        # Token już wygasł — nie dodajemy do blacklisty
        return
    try:
        jti = _extract_jti_unsafe(token)
        key = f"{_REDIS_PREFIX_BLACKLIST}{jti or _hash_token(token)}"
        await redis.setex(key, ttl, "1")
        logger.debug(
            "Access token dodany do blacklisty Redis: jti=%s, ttl=%ds",
            jti, ttl,
        )
    except Exception as exc:
        logger.error(
            "Błąd dodawania tokena do blacklisty Redis: %s",
            exc,
            extra={"traceback": traceback.format_exc()},
        )


async def _is_token_blacklisted(redis: Optional[Redis], token: str) -> bool:
    """
    Sprawdza czy access token jest na blackliście.
    Returns False jeśli Redis niedostępny (fail open — lepsze UX, akceptowalne ryzyko).
    """
    if redis is None:
        return False
    try:
        jti = _extract_jti_unsafe(token)
        key = f"{_REDIS_PREFIX_BLACKLIST}{jti or _hash_token(token)}"
        result = await redis.exists(key)
        return bool(result)
    except Exception as exc:
        logger.error("Błąd sprawdzania blacklisty: %s", exc)
        return False


def _extract_jti_unsafe(token: str) -> Optional[str]:
    """
    Wyciąga JTI z tokena JWT BEZ weryfikacji podpisu.
    Używane tylko do blacklisty — weryfikacja podpisu jest gdzie indziej.
    """
    try:
        # Decode bez weryfikacji (tylko payload)
        payload = jwt.get_unverified_claims(token)
        return payload.get("jti")
    except Exception:
        return None


def _token_remaining_ttl(token: str) -> int:
    """
    Oblicza pozostały TTL tokena w sekundach.
    Zwraca 0 jeśli token wygasł lub nie można odczytać.
    """
    try:
        payload = jwt.get_unverified_claims(token)
        exp = payload.get("exp", 0)
        now = int(datetime.now(timezone.utc).timestamp())
        return max(0, exp - now)
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Baza danych: User queries
# ---------------------------------------------------------------------------

async def _get_user_by_username(
    db: AsyncSession, username: str
) -> Optional[User]:
    """Pobiera użytkownika po username z eager-load roli i uprawnień."""
    result = await db.execute(
        select(User)
        .options(
            selectinload(User.role)
            .selectinload(Role.role_permissions)
            .selectinload(RolePermission.permission),
        )
        .where(User.username == username, User.is_active == True)
    )
    return result.scalar_one_or_none()


async def _get_user_by_id(
    db: AsyncSession, user_id: int
) -> Optional[User]:
    """Pobiera użytkownika po ID z eager-load roli i uprawnień."""
    result = await db.execute(
        select(User)
        .options(
            selectinload(User.role)
            .selectinload(Role.role_permissions)
            .selectinload(RolePermission.permission),
        )
        .where(User.id == user_id, User.is_active == True)
    )
    return result.scalar_one_or_none()


async def _get_user_by_email(
    db: AsyncSession, email: str
) -> Optional[User]:
    """Pobiera użytkownika po email (case-insensitive)."""
    result = await db.execute(
        select(User)
        .where(
            User.email == email.lower().strip(),
            User.is_active == True,
        )
    )
    return result.scalar_one_or_none()


def _user_permissions(user: User) -> list[str]:
    """Wyciąga listę uprawnień z załadowanego użytkownika."""
    try:
        if user.role and hasattr(user.role, "role_permissions"):
            perms: list[str] = []
            for rp in user.role.role_permissions:
                perm = getattr(rp, "permission", None)
                if perm and getattr(perm, "is_active", True):
                    perms.append(perm.permission_name)
            return perms
    except Exception:
        # w razie czego: nie blokujemy logowania przez błąd w mapowaniu
        pass
    return []


async def _increment_failed_login(db: AsyncSession, user_id: int) -> int:
    """
    Inkrementuje FailedLoginAttempts i jeśli >= MAX ustawia LockedUntil.
    Zwraca nową liczbę nieudanych prób.
    """
    result = await db.execute(
        select(User.failed_login_attempts).where(User.id == user_id)
    )
    current_attempts = result.scalar_one_or_none() or 0
    new_attempts = current_attempts + 1

    update_vals: dict[str, Any] = {
        "failed_login_attempts": new_attempts,
        "updated_at": datetime.now(timezone.utc),
    }
    if new_attempts >= _MAX_FAILED_ATTEMPTS:
        locked_until = datetime.now(timezone.utc) + timedelta(
            minutes=_LOCKOUT_DURATION_MINUTES
        )
        update_vals["locked_until"] = locked_until
        logger.warning(
            "Konto zablokowane: user_id=%d, attempts=%d, locked_until=%s",
            user_id, new_attempts, locked_until.isoformat(),
            extra={
                "user_id": user_id,
                "failed_attempts": new_attempts,
                "locked_until": locked_until.isoformat(),
            },
        )

    await db.execute(
        update(User).where(User.id == user_id).values(**update_vals)
    )
    await db.commit()
    return new_attempts


async def _reset_failed_login(db: AsyncSession, user_id: int) -> None:
    """Zeruje FailedLoginAttempts i LockedUntil po udanym logowaniu."""
    await db.execute(
        update(User)
        .where(User.id == user_id)
        .values(
            failed_login_attempts=0,
            locked_until=None,
            last_login_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
    )
    await db.commit()


async def _save_refresh_token(
    db: AsyncSession,
    user_id: int,
    hashed_token: str,
    expires_at: datetime,
    ip: Optional[str],
    user_agent: Optional[str],
) -> None:
    """
    Zapisuje nowy RefreshToken do DB.
    Przed zapisem: czyści stare tokeny jeśli przekroczono MAX_ACTIVE_SESSIONS.
    """
    # Sprawdź liczbę aktywnych sesji
    result = await db.execute(
        select(RefreshToken)
        .where(
            RefreshToken.user_id == user_id,
            RefreshToken.is_revoked == False,
            RefreshToken.expires_at > datetime.now(timezone.utc),
        )
        .order_by(RefreshToken.created_at.asc())
    )
    active_tokens = result.scalars().all()

    # Wyczyść najstarsze jeśli za dużo
    if len(active_tokens) >= _MAX_ACTIVE_SESSIONS:
        tokens_to_revoke = active_tokens[: len(active_tokens) - _MAX_ACTIVE_SESSIONS + 1]
        for old_token in tokens_to_revoke:
            old_token.is_revoked = True
            old_token.revoked_at = datetime.now(timezone.utc)
        logger.info(
            "Unieważniono %d starych sesji dla user_id=%d (limit=%d)",
            len(tokens_to_revoke), user_id, _MAX_ACTIVE_SESSIONS,
            extra={"user_id": user_id, "revoked_count": len(tokens_to_revoke)},
        )
        await db.flush()

    # Zapisz nowy token
    new_token = RefreshToken(
        user_id=user_id,
        token=hashed_token,
        expires_at=expires_at,
        created_at=datetime.now(timezone.utc),
        is_revoked=False,
        ip_address=(ip or "")[:45] if ip else None,
        user_agent=(user_agent or "")[:500] if user_agent else None,
    )
    db.add(new_token)
    await db.commit()
    logger.debug(
        "RefreshToken zapisany: user_id=%d, expires=%s",
        user_id, expires_at.isoformat(),
        extra={"user_id": user_id, "expires_at": expires_at.isoformat()},
    )


# ---------------------------------------------------------------------------
# METODA: login()
# ---------------------------------------------------------------------------

async def login(
    db: AsyncSession,
    redis: Optional[Redis],
    *,
    username: str,
    password: str,
    ip: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> TokenPair:
    """
    Loguje użytkownika — kompletny przepływ autentykacji.

    Kolejność kroków:
        1. Sanityzacja wejścia
        2. Rate limit Redis (#1 linia obrony)
        3. Pobierz użytkownika z DB (nie ujawniaj czy istnieje — anti-enumeration)
        4. Sprawdź is_active i locked_until
        5. Verify argon2id (constant-time)
        6. Inkrementuj/zeruj FailedLoginAttempts
        7. Utwórz TokenPair
        8. Zapisz RefreshToken do DB
        9. Aktualizuj LastLoginAt, zeruj FailedLoginAttempts
        10. Opcjonalne rehash hasła (argon2 parametry)
        11. AuditLog (fire-and-forget)

    Args:
        username:   Login użytkownika (case-insensitive)
        password:   Hasło (raw, max 1000 znaków dla bezpieczeństwa)
        ip:         IP klienta (do rate limit i audit)
        user_agent: User-Agent (do audit i RefreshToken)

    Returns:
        TokenPair — para tokenów JWT

    Raises:
        RateLimitExceededError: Zbyt wiele prób z tego IP
        AuthError:              Nieprawidłowe dane logowania
        AccountLockedError:     Konto tymczasowo zablokowane
    """
    # 1. Sanityzacja
    username_clean = _sanitize_credential(username.lower().strip(), max_len=50)
    password_clean = _sanitize_credential(
        password.get_secret_value() if hasattr(password, "get_secret_value") else password,
        max_len=1000,
    )
    ip_clean = (ip or "unknown")[:45]

    logger.info(
        "Próba logowania: username=%s, IP=%s",
        username_clean, ip_clean,
        extra={"username": username_clean, "ip": ip_clean},
    )

    # 2. Rate limit Redis
    await _check_rate_limit_redis(
        redis, ip_clean,
        _REDIS_PREFIX_RL_LOGIN,
        _RL_LOGIN_LIMIT,
        _RL_LOGIN_WINDOW,
    )

    # 3. Pobierz użytkownika
    user = await _get_user_by_username(db, username_clean)

    # Zawsze wykonuj porównanie hasła (nawet jeśli user nie istnieje)
    # — zapobiega timing-based user enumeration
    if user is None:
        # Dummy compare aby czas odpowiedzi był stały
        _ph.hash("dummy_password_to_prevent_timing_attack")
        logger.warning(
            "Logowanie NIEUDANE: użytkownik '%s' nie istnieje (IP=%s)",
            username_clean, ip_clean,
            extra={"username": username_clean, "ip": ip_clean, "reason": "user_not_found"},
        )
        await audit_service.log_failed_login(
            db,
            username_attempt=username_clean,
            ip_address=ip_clean,
            reason="user_not_found",
        )
        raise AuthError("Nieprawidłowy login lub hasło", code="invalid_credentials")

    # 4. Sprawdź blokadę konta
    if user.locked_until and user.locked_until > datetime.now(timezone.utc):
        logger.warning(
            "Logowanie NIEUDANE: konto zablokowane user_id=%d (IP=%s, locked_until=%s)",
            user.id, ip_clean, user.locked_until.isoformat(),
            extra={"user_id": user.id, "locked_until": user.locked_until.isoformat()},
        )
        await audit_service.log_auth(
            db,
            action="user_login",
            user_id=user.id,
            username=user.username,
            success=False,
            error_message="account_locked",
            ip_address=ip_clean,
        )
        raise AccountLockedError(locked_until=user.locked_until)

    # 5. Weryfikacja hasła argon2id
    password_ok = verify_password(password_clean, user.password_hash)

    if not password_ok:
        # Inkrementuj licznik nieudanych prób
        new_attempts = await _increment_failed_login(db, user.id)

        logger.warning(
            "Logowanie NIEUDANE: złe hasło user_id=%d, username=%s, "
            "attempt=%d/%d (IP=%s)",
            user.id, user.username, new_attempts, _MAX_FAILED_ATTEMPTS, ip_clean,
            extra={
                "user_id": user.id,
                "username": user.username,
                "failed_attempts": new_attempts,
                "max_attempts": _MAX_FAILED_ATTEMPTS,
                "ip": ip_clean,
            },
        )
        await audit_service.log_failed_login(
            db,
            username_attempt=username_clean,
            ip_address=ip_clean,
            reason="wrong_password",
        )
        raise AuthError("Nieprawidłowy login lub hasło", code="invalid_credentials")

    # 6. Logowanie UDANE — zeruj failed attempts
    permissions = _user_permissions(user)
    role_name = user.role.role_name if user.role else "Unknown"

    # 7. Tworzenie tokenów
    settings = _get_settings()
    access_token, access_expires_at = _create_access_token(
        user_id=user.id,
        username=user.username,
        role=role_name,
        permissions=permissions,
    )
    raw_refresh, hashed_refresh = _create_refresh_token()
    refresh_expire_days = int(settings.REFRESH_TOKEN_EXPIRE_DAYS)
    refresh_expires_at = datetime.now(timezone.utc) + timedelta(days=refresh_expire_days)

    # 8. Zapisz RefreshToken
    await _save_refresh_token(
        db, user.id, hashed_refresh, refresh_expires_at, ip_clean, user_agent
    )

    # 9. Aktualizuj LastLoginAt, zeruj failed attempts
    await _reset_failed_login(db, user.id)

    # 10. Opcjonalne rehash (argon2 parametry zmieniły się)
    if password_needs_rehash(user.password_hash):
        new_hash = hash_password(password_clean)
        await db.execute(
            update(User)
            .where(User.id == user.id)
            .values(password_hash=new_hash, updated_at=datetime.now(timezone.utc))
        )
        await db.commit()
        logger.info(
            "Hasło użytkownika %d zrehashowane (aktualizacja parametrów argon2)",
            user.id,
            extra={"user_id": user.id},
        )

    # 11. AuditLog (fire-and-forget)
    await audit_service.log_auth(
        db,
        action="user_login",
        user_id=user.id,
        username=user.username,
        success=True,
        details={
            "ip": ip_clean,
            "user_agent": (user_agent or "")[:200],
            "role": role_name,
            "permissions_count": len(permissions),
        },
        ip_address=ip_clean,
    )

    logger.info(
        "Logowanie UDANE: user_id=%d, username=%s, role=%s (IP=%s)",
        user.id, user.username, role_name, ip_clean,
        extra={
            "user_id": user.id,
            "username": user.username,
            "role": role_name,
            "ip": ip_clean,
            "permissions_count": len(permissions),
        },
    )

    access_expires_in = int(
        (access_expires_at - datetime.now(timezone.utc)).total_seconds()
    )

    return TokenPair(
        access_token=access_token,
        refresh_token=raw_refresh,
        token_type="bearer",
        expires_in=max(0, access_expires_in),
        user_id=user.id,
        username=user.username,
        role=role_name,
        permissions=permissions,
    )


# ---------------------------------------------------------------------------
# METODA: logout()
# ---------------------------------------------------------------------------

async def logout(
    db: AsyncSession,
    redis: Optional[Redis],
    *,
    access_token: str,
    refresh_token_raw: Optional[str] = None,
    user_id: int,
    username: Optional[str] = None,
    ip: Optional[str] = None,
) -> None:
    """
    Wylogowuje użytkownika.

    Kroki:
        1. Dodaj access_token do Redis blacklisty (TTL = remaining lifetime)
        2. Unieważnij RefreshToken w DB (is_revoked = True)
        3. AuditLog(action="user_logout")
    """
    ip_clean = (ip or "unknown")[:45]

    logger.info(
        "Wylogowanie: user_id=%d, username=%s (IP=%s)",
        user_id, username or "unknown", ip_clean,
        extra={"user_id": user_id, "username": username, "ip": ip_clean},
    )

    # 1. Blacklista access token
    ttl = _token_remaining_ttl(access_token)
    await _blacklist_token(redis, access_token, ttl)

    # 2. Unieważnij RefreshToken
    if refresh_token_raw:
        hashed = _hash_token(refresh_token_raw)
        await db.execute(
            update(RefreshToken)
            .where(
                RefreshToken.user_id == user_id,
                RefreshToken.token == hashed,
                RefreshToken.is_revoked == False,
            )
            .values(
                is_revoked=True,
                revoked_at=datetime.now(timezone.utc),
            )
        )
        await db.commit()
        logger.debug(
            "RefreshToken unieważniony: user_id=%d",
            user_id,
            extra={"user_id": user_id},
        )

    # 3. AuditLog (fire-and-forget)
    await audit_service.log_auth(
        db,
        action="user_logout",
        user_id=user_id,
        username=username,
        success=True,
        details={"ip": ip_clean, "access_token_ttl": ttl},
        ip_address=ip_clean,
    )

    logger.info(
        "Wylogowanie UKOŃCZONE: user_id=%d (IP=%s)",
        user_id, ip_clean,
        extra={"user_id": user_id, "ip": ip_clean},
    )


# ---------------------------------------------------------------------------
# METODA: refresh()
# ---------------------------------------------------------------------------

async def refresh(
    db: AsyncSession,
    redis: Optional[Redis],
    *,
    refresh_token_raw: str,
    ip: Optional[str] = None,
) -> TokenPair:
    """
    Odświeża access token używając refresh token.

    Kroki:
        1. Rate limit Redis
        2. Hash refresh token → szukaj w DB
        3. Sprawdź is_revoked i expiry
        4. Pobierz użytkownika
        5. Generuj nowy access_token (refresh_token NIE jest rotowany — decyzja projektowa)
        6. AuditLog

    Raises:
        AuthError: Nieprawidłowy lub wygasły refresh token
    """
    ip_clean = (ip or "unknown")[:45]

    # 1. Rate limit
    await _check_rate_limit_redis(
        redis, ip_clean,
        _REDIS_PREFIX_RL_REFRESH,
        _RL_REFRESH_LIMIT,
        _RL_REFRESH_WINDOW,
    )

    # 2. Szukaj w DB
    hashed = _hash_token(refresh_token_raw)
    result = await db.execute(
        select(RefreshToken).where(
            RefreshToken.token == hashed,
            RefreshToken.is_revoked == False,
        )
    )
    db_token: Optional[RefreshToken] = result.scalar_one_or_none()

    if db_token is None:
        logger.warning(
            "Refresh NIEUDANY: token nie znaleziony/unieważniony (IP=%s)",
            ip_clean,
            extra={"ip": ip_clean, "reason": "token_not_found"},
        )
        raise AuthError("Nieprawidłowy refresh token", code="invalid_refresh_token")

    # 3. Sprawdź wygaśnięcie
    if db_token.expires_at < datetime.now(timezone.utc):
        # Unieważnij wygasły token
        db_token.is_revoked = True
        db_token.revoked_at = datetime.now(timezone.utc)
        await db.commit()
        logger.warning(
            "Refresh NIEUDANY: token wygasł (user_id=%d, IP=%s)",
            db_token.user_id, ip_clean,
            extra={"user_id": db_token.user_id, "ip": ip_clean},
        )
        raise AuthError("Refresh token wygasł", code="refresh_token_expired")

    # 4. Pobierz użytkownika
    user = await _get_user_by_id(db, db_token.user_id)
    if user is None:
        raise AuthError("Użytkownik nie istnieje lub jest nieaktywny", code="user_inactive")

    # Sprawdź lockout
    if user.locked_until and user.locked_until > datetime.now(timezone.utc):
        raise AccountLockedError(locked_until=user.locked_until)

    # 5. Generuj nowy access token
    permissions = _user_permissions(user)
    role_name = user.role.role_name if user.role else "Unknown"

    new_access_token, access_expires_at = _create_access_token(
        user_id=user.id,
        username=user.username,
        role=role_name,
        permissions=permissions,
    )

    # 6. AuditLog
    audit_service.log(
        db,
        action="user_token_refresh",
        category="Auth",
        entity_type="User",
        entity_id=user.id,
        user_id=user.id,
        username=user.username,
        success=True,
        details={"ip": ip_clean},
        ip_address=ip_clean,
    )

    logger.info(
        "Refresh UDANY: user_id=%d, username=%s (IP=%s)",
        user.id, user.username, ip_clean,
        extra={"user_id": user.id, "username": user.username, "ip": ip_clean},
    )

    expires_in = int((access_expires_at - datetime.now(timezone.utc)).total_seconds())
    return TokenPair(
        access_token=new_access_token,
        refresh_token=refresh_token_raw,  # nie rotujemy
        token_type="bearer",
        expires_in=max(0, expires_in),
        user_id=user.id,
        username=user.username,
        role=role_name,
        permissions=permissions,
    )


# ---------------------------------------------------------------------------
# METODA: get_current_user() — FastAPI Dependency
# ---------------------------------------------------------------------------

async def get_current_user(
    db: AsyncSession,
    redis: Optional[Redis],
    token: str,
) -> CurrentUser:
    """
    Weryfikuje access token i zwraca CurrentUser.

    Kroki:
        1. Decode JWT (weryfikacja podpisu + expiry)
        2. Sprawdź blacklistę Redis
        3. Pobierz User z DB (fresh dane uprawnień)
        4. Zwróć CurrentUser z frozenset uprawnień

    Używany jako FastAPI Dependency:
        user: CurrentUser = Depends(get_current_user_dep)

    Raises:
        TokenExpiredError:    Token wygasł
        TokenBlacklistedError: Token unieważniony
        AuthError:            Token nieprawidłowy lub user nieaktywny
    """
    # 1. Decode JWT
    try:
        payload = _decode_access_token(token)
    except TokenExpiredError:
        raise
    except AuthError:
        raise

    user_id_str = payload.get("sub")
    if not user_id_str:
        raise AuthError("Brak sub w tokenie", code="invalid_token")

    try:
        user_id = int(user_id_str)
    except (ValueError, TypeError) as exc:
        raise AuthError("Nieprawidłowy sub w tokenie", code="invalid_token") from exc

    # 2. Blacklista
    if await _is_token_blacklisted(redis, token):
        raise TokenBlacklistedError()

    # 3. User z DB (świeże dane — nie ufamy permissions z tokena dla RBAC)
    user = await _get_user_by_id(db, user_id)
    if user is None:
        raise AuthError("Użytkownik nieaktywny lub usunięty", code="user_inactive")

    if user.locked_until and user.locked_until > datetime.now(timezone.utc):
        raise AccountLockedError(locked_until=user.locked_until)

    # 4. Buduj CurrentUser
    permissions_list = _user_permissions(user)
    role_name = user.role.role_name if user.role else "Unknown"

    is_impersonation = bool(payload.get("is_impersonation", False))
    impersonated_by = payload.get("impersonated_by")

    return CurrentUser(
        id=user.id,
        username=user.username,
        email=user.email,
        full_name=user.full_name,
        role_id=user.role_id,
        role_name=role_name,
        permissions=frozenset(permissions_list),
        is_active=user.is_active,
        is_impersonation=is_impersonation,
        impersonated_by=impersonated_by,
    )


# ---------------------------------------------------------------------------
# METODA: change_password()
# ---------------------------------------------------------------------------

async def change_password(
    db: AsyncSession,
    redis: Optional[Redis],
    *,
    user_id: int,
    old_password: str,
    new_password: str,
    ip: Optional[str] = None,
) -> None:
    """
    Zmiana hasła użytkownika.

    Kroki:
        1. Pobierz user z DB
        2. Verify old_password (argon2)
        3. Hash new_password
        4. UPDATE PasswordHash
        5. Unieważnij WSZYSTKIE refresh tokeny usera
        6. AuditLog

    Raises:
        AuthError: Złe stare hasło lub user nie istnieje
    """
    ip_clean = (ip or "unknown")[:45]

    # Sanityzacja
    old_password_clean = _sanitize_credential(old_password, max_len=1000)
    new_password_clean = _sanitize_credential(new_password, max_len=1000)

    # Pobierz usera (bez is_active filtra — user może sam zmienić hasło)
    result = await db.execute(
        select(User).where(User.id == user_id)
    )
    user: Optional[User] = result.scalar_one_or_none()

    if user is None:
        raise AuthError("Użytkownik nie istnieje", code="user_not_found")

    # Verify old
    if not verify_password(old_password_clean, user.password_hash):
        logger.warning(
            "Zmiana hasła NIEUDANA: złe stare hasło user_id=%d (IP=%s)",
            user_id, ip_clean,
            extra={"user_id": user_id, "ip": ip_clean},
        )
        audit_service.log_auth(
            db,
            action="user_password_changed",
            user_id=user_id,
            username=user.username,
            success=False,
            error_message="wrong_old_password",
            ip_address=ip_clean,
        )
        raise AuthError("Stare hasło jest nieprawidłowe", code="wrong_old_password")

    # Hash new
    new_hash = hash_password(new_password_clean)

    # UPDATE
    await db.execute(
        update(User)
        .where(User.id == user_id)
        .values(
            password_hash=new_hash,
            updated_at=datetime.now(timezone.utc),
        )
    )

    # Unieważnij wszystkie refresh tokeny
    await db.execute(
        update(RefreshToken)
        .where(
            RefreshToken.user_id == user_id,
            RefreshToken.is_revoked == False,
        )
        .values(
            is_revoked=True,
            revoked_at=datetime.now(timezone.utc),
        )
    )
    await db.commit()

    logger.info(
        "Hasło zmienione: user_id=%d (IP=%s) — wszystkie sesje unieważnione",
        user_id, ip_clean,
        extra={"user_id": user_id, "ip": ip_clean},
    )

    await audit_service.log_auth(
        db,
        action="user_password_changed",
        user_id=user_id,
        username=user.username,
        success=True,
        details={"ip": ip_clean, "all_sessions_revoked": True},
        ip_address=ip_clean,
    )


# ---------------------------------------------------------------------------
# METODA: forgot_password() — anti-enumeration
# ---------------------------------------------------------------------------

async def forgot_password(
    db: AsyncSession,
    redis: Optional[Redis],
    *,
    email: str,
    ip: Optional[str] = None,
) -> None:
    """
    Inicjuje reset hasła.

    WAŻNE: ZAWSZE zwraca None niezależnie czy email istnieje.
    Zapobiega user enumeration przez timing lub różne odpowiedzi.

    W tej implementacji: generuje OTP code i zapisuje do Redis.
    Faktyczne wysłanie emaila: stub (Faza 6 — Worker ARQ).
    """
    ip_clean = (ip or "unknown")[:45]
    email_clean = email.lower().strip()[:100]

    logger.info(
        "forgot_password: email=%s (IP=%s)",
        email_clean, ip_clean,
        extra={"ip": ip_clean},  # NIE logujemy emaila w details — PII
    )

    user = await _get_user_by_email(db, email_clean)

    if user is None:
        # Anti-enumeration: symuluj normalne działanie
        logger.debug("forgot_password: email nie istnieje — bez akcji (anti-enumeration)")
        return  # Zawsze None

    # Generuj OTP (6-cyfrowy)
    import secrets as _secrets
    otp_code = str(_secrets.randbelow(900000) + 100000)  # 100000–999999

    # Hash OTP przed zapisem (SHA256 wystarczy dla krótkotrwałego kodu)
    otp_hash = hashlib.sha256(otp_code.encode()).hexdigest()

    # Pobierz TTL z konfiguracji
    try:
        expiry_minutes = await config_service.get_otp_expiry_minutes(db, redis)
    except Exception:
        expiry_minutes = 15  # fallback

    expires_at = datetime.now(timezone.utc) + timedelta(minutes=expiry_minutes)

    # Unieważnij stare kody OTP dla tego usera
    await db.execute(
        update(OtpCode)
        .where(
            OtpCode.user_id == user.id,
            OtpCode.purpose == "password_reset",
            OtpCode.is_used == False,
        )
        .values(is_used=True)
    )

    # Zapisz nowy OTP
    new_otp = OtpCode(
        user_id=user.id,
        code=otp_hash,
        purpose="password_reset",
        expires_at=expires_at,
        is_used=False,
        created_at=datetime.now(timezone.utc),
        ip_address=ip_clean,
    )
    db.add(new_otp)
    await db.commit()

    logger.info(
        "OTP code wygenerowany: user_id=%d, expires_at=%s",
        user.id, expires_at.isoformat(),
        extra={"user_id": user.id, "expires_at": expires_at.isoformat()},
    )

    # STUB: W Fazie 6 → arq_queue.enqueue("send_otp_email", user.email, otp_code)
    logger.info(
        "[STUB] OTP email do wysłania: user_id=%d — Worker ARQ nie zaimplementowany (Faza 6)",
        user.id,
        extra={"user_id": user.id, "stub": True},
    )

    audit_service.log_auth(
        db,
        action="user_password_reset",
        user_id=user.id,
        username=user.username,
        success=True,
        details={"ip": ip_clean, "otp_generated": True, "expires_minutes": expiry_minutes},
        ip_address=ip_clean,
    )


# ---------------------------------------------------------------------------
# METODA: verify_otp() + reset_password()
# ---------------------------------------------------------------------------

async def verify_otp_and_get_reset_token(
    db: AsyncSession,
    redis: Optional[Redis],
    *,
    email: str,
    otp_code: str,
    ip: Optional[str] = None,
) -> str:
    """
    Weryfikuje OTP code i zwraca jednorazowy reset_token.

    Reset_token zapisany w Redis (TTL=600s), używany w reset_password().

    Raises:
        AuthError: Nieprawidłowy lub wygasły OTP
    """
    ip_clean = (ip or "unknown")[:45]
    email_clean = email.lower().strip()[:100]
    otp_clean = otp_code.strip()[:10]

    # Sprawdź czy IP/email nie przekroczył limitu prób OTP
    fail_key = f"{_REDIS_PREFIX_OTP_FAIL}{email_clean}"
    if redis:
        try:
            fail_count_raw = await redis.get(fail_key)
            fail_count = int(fail_count_raw) if fail_count_raw else 0
            if fail_count >= _OTP_FAIL_LIMIT:
                logger.warning(
                    "OTP verify: zbyt wiele prób dla email=%s (IP=%s)",
                    email_clean, ip_clean,
                )
                raise RateLimitExceededError(retry_after=_OTP_FAIL_WINDOW)
        except RateLimitExceededError:
            raise
        except Exception as exc:
            logger.error("Błąd OTP fail check: %s", exc)

    user = await _get_user_by_email(db, email_clean)
    if user is None:
        # Anti-enumeration
        raise AuthError("Nieprawidłowy kod OTP", code="invalid_otp")

    # Hash podanego kodu
    otp_hash = hashlib.sha256(otp_clean.encode()).hexdigest()

    # Szukaj aktywnego, nie-wygasłego kodu
    result = await db.execute(
        select(OtpCode).where(
            OtpCode.user_id == user.id,
            OtpCode.code == otp_hash,
            OtpCode.purpose == "password_reset",
            OtpCode.is_used == False,
            OtpCode.expires_at > datetime.now(timezone.utc),
        )
    )
    otp_record: Optional[OtpCode] = result.scalar_one_or_none()

    if otp_record is None:
        # Inkrementuj licznik niepowodzeń
        if redis:
            try:
                count = await redis.incr(fail_key)
                if count == 1:
                    await redis.expire(fail_key, _OTP_FAIL_WINDOW)
            except Exception:
                pass

        logger.warning(
            "OTP verify NIEUDANY: user_id=%d, email=%s (IP=%s)",
            user.id, email_clean, ip_clean,
            extra={"user_id": user.id, "ip": ip_clean},
        )
        raise AuthError("Nieprawidłowy lub wygasły kod OTP", code="invalid_otp")

    # Oznacz jako używany
    otp_record.is_used = True
    await db.commit()

    # Wyczyść licznik prób
    if redis:
        try:
            await redis.delete(fail_key)
        except Exception:
            pass

    # Generuj reset_token
    reset_token = secrets.token_urlsafe(32)
    reset_key = f"{_REDIS_PREFIX_RESET_TOKEN}{reset_token}"

    if redis:
        try:
            import json
            await redis.setex(
                reset_key,
                _RESET_TOKEN_TTL,
                json.dumps({"user_id": user.id, "email": email_clean}),
            )
        except Exception as exc:
            logger.error("Błąd zapisu reset_token do Redis: %s", exc)
            raise AuthError("Błąd systemu — spróbuj ponownie", code="system_error") from exc

    logger.info(
        "OTP verify UDANY: user_id=%d, reset_token wygenerowany (IP=%s)",
        user.id, ip_clean,
        extra={"user_id": user.id, "ip": ip_clean},
    )
    return reset_token


async def reset_password(
    db: AsyncSession,
    redis: Optional[Redis],
    *,
    reset_token: str,
    new_password: str,
    ip: Optional[str] = None,
) -> None:
    """
    Resetuje hasło przy użyciu jednorazowego reset_token (z verify_otp).

    Raises:
        AuthError: Nieprawidłowy/wygasły token lub błąd systemu
    """
    ip_clean = (ip or "unknown")[:45]

    if not redis:
        raise AuthError("Serwis reset hasła niedostępny (Redis)", code="service_unavailable")

    reset_key = f"{_REDIS_PREFIX_RESET_TOKEN}{reset_token}"
    try:
        import json
        raw = await redis.get(reset_key)
        if not raw:
            raise AuthError("Nieprawidłowy lub wygasły token resetowania", code="invalid_reset_token")
        data = json.loads(raw)
        user_id = int(data["user_id"])
    except AuthError:
        raise
    except Exception as exc:
        raise AuthError("Błąd odczytu tokena reset", code="invalid_reset_token") from exc

    # Invaliduj token (jednorazowy)
    await redis.delete(reset_key)

    # Hash nowego hasła
    new_password_clean = _sanitize_credential(new_password, max_len=1000)
    new_hash = hash_password(new_password_clean)

    # UPDATE
    result = await db.execute(
        select(User).where(User.id == user_id)
    )
    user: Optional[User] = result.scalar_one_or_none()
    if user is None:
        raise AuthError("Użytkownik nie istnieje", code="user_not_found")

    await db.execute(
        update(User)
        .where(User.id == user_id)
        .values(
            password_hash=new_hash,
            updated_at=datetime.now(timezone.utc),
            failed_login_attempts=0,
            locked_until=None,
        )
    )

    # Unieważnij wszystkie sesje
    await db.execute(
        update(RefreshToken)
        .where(RefreshToken.user_id == user_id, RefreshToken.is_revoked == False)
        .values(is_revoked=True, revoked_at=datetime.now(timezone.utc))
    )
    await db.commit()

    logger.info(
        "Hasło zresetowane: user_id=%d (IP=%s) — wszystkie sesje unieważnione",
        user_id, ip_clean,
        extra={"user_id": user_id, "ip": ip_clean},
    )

    await audit_service.log_auth(
        db,
        action="user_password_reset",
        user_id=user_id,
        username=user.username,
        success=True,
        details={"ip": ip_clean, "all_sessions_revoked": True, "method": "otp_reset"},
        ip_address=ip_clean,
    )


# ---------------------------------------------------------------------------
# METODA: revoke_all_sessions()
# ---------------------------------------------------------------------------

async def revoke_all_sessions(
    db: AsyncSession,
    *,
    user_id: int,
    revoked_by_id: Optional[int] = None,
    reason: str = "admin_action",
) -> int:
    """
    Unieważnia wszystkie aktywne sesje użytkownika.
    Używane przez admin przy podejrzeniu kompromitacji konta.

    Returns:
        Liczba unieważnionych tokenów
    """
    result = await db.execute(
        update(RefreshToken)
        .where(
            RefreshToken.user_id == user_id,
            RefreshToken.is_revoked == False,
        )
        .values(
            is_revoked=True,
            revoked_at=datetime.now(timezone.utc),
        )
        .returning(RefreshToken.id_token)
    )
    revoked_count = len(result.fetchall())
    await db.commit()

    logger.warning(
        "Wszystkie sesje unieważnione: user_id=%d, count=%d, by=%s, reason=%s",
        user_id, revoked_count,
        revoked_by_id or "system",
        reason,
        extra={
            "user_id": user_id,
            "revoked_count": revoked_count,
            "revoked_by": revoked_by_id,
            "reason": reason,
        },
    )
    return revoked_count


# ---------------------------------------------------------------------------
# METODA: master_access()
# ---------------------------------------------------------------------------

async def master_access(
    db: AsyncSession,
    redis: Optional[Redis],
    *,
    master_key_input: str,
    pin_input: str,
    target_user_id: int,
    ip: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> TokenPair:
    """
    Dostęp serwisowy przez Master Key.

    WAŻNE:
        - Zapis TYLKO do MasterAccessLog (nie do AuditLog!)
        - Historia logowań docelowego użytkownika czysta
        - Rate limit: 3 próby / 15min → ban IP 1h

    Kroki:
        1. Sprawdź czy master_key.enabled
        2. Sprawdź ban IP
        3. Rate limit (Redis counter)
        4. Constant-time compare MASTER_KEY
        5. Bcrypt verify PIN
        6. Pobierz target user
        7. Generuj impersonation token
        8. Zapisz do MasterAccessLog

    Raises:
        AuthError:            Błędny klucz/PIN
        RateLimitExceededError: Zbyt wiele prób
    """
    ip_clean = (ip or "unknown")[:45]

    logger.warning(
        "MASTER ACCESS próba: target_user_id=%d, IP=%s",
        target_user_id, ip_clean,
        extra={"target_user_id": target_user_id, "ip": ip_clean},
    )

    # 1. Sprawdź czy włączony
    try:
        is_enabled = await config_service.is_master_key_enabled(db, redis)
    except Exception:
        is_enabled = True  # fail safe = enabled

    if not is_enabled:
        logger.warning(
            "MASTER ACCESS zablokowany przez konfigurację (IP=%s)",
            ip_clean,
            extra={"ip": ip_clean},
        )
        raise AuthError("Master Key access jest wyłączony", code="master_key_disabled")

    # 2. Sprawdź ban IP
    await _check_ip_ban(redis, ip_clean)

    # 3. Rate limit
    try:
        await _check_rate_limit_redis(
            redis, ip_clean,
            _REDIS_PREFIX_RL_MASTER,
            _RL_MASTER_LIMIT,
            _RL_MASTER_WINDOW,
        )
    except RateLimitExceededError:
        # Ban IP po przekroczeniu limitu
        await _ban_ip(redis, ip_clean, _RL_BAN_DURATION)
        logger.critical(
            "MASTER ACCESS: IP zbanowane po przekroczeniu limitu (IP=%s)",
            ip_clean,
            extra={"ip": ip_clean, "event": "master_key_ip_banned"},
        )
        raise

    # 4. Constant-time compare MASTER_KEY
    settings = _get_settings()
    env_master_key = settings.MASTER_KEY.get_secret_value()

    if not _constant_time_compare(master_key_input, env_master_key):
        logger.critical(
            "MASTER ACCESS: błędny Master Key (IP=%s)",
            ip_clean,
            extra={"ip": ip_clean, "event": "master_key_invalid"},
        )
        raise AuthError("Nieprawidłowy Master Key lub PIN", code="invalid_master_credentials")

    # 5. Bcrypt verify PIN
    try:
        pin_hash = await config_service.get_master_pin_hash(db, redis)
    except Exception as exc:
        raise AuthError("Błąd odczytu PIN hash", code="system_error") from exc

    if not pin_hash:
        raise AuthError(
            "PIN nie jest skonfigurowany. Uruchom setup.py --set-master-pin",
            code="pin_not_configured",
        )

    try:
        pin_ok = bcrypt.checkpw(
            pin_input.encode("utf-8"),
            pin_hash.encode("utf-8"),
        )
    except Exception as exc:
        logger.error("Błąd bcrypt verify PIN: %s", exc)
        raise AuthError("Błąd weryfikacji PIN", code="system_error") from exc

    if not pin_ok:
        logger.critical(
            "MASTER ACCESS: błędny PIN (IP=%s)",
            ip_clean,
            extra={"ip": ip_clean, "event": "master_pin_invalid"},
        )
        raise AuthError("Nieprawidłowy Master Key lub PIN", code="invalid_master_credentials")

    # 6. Pobierz target user
    target_user = await _get_user_by_id(db, target_user_id)
    if target_user is None:
        raise AuthError(f"Użytkownik ID={target_user_id} nie istnieje", code="user_not_found")

    permissions = _user_permissions(target_user)
    role_name = target_user.role.role_name if target_user.role else "Unknown"

    # 7. Impersonation token
    settings_obj = _get_settings()
    max_hours = 4  # master key sesja max 4h
    try:
        max_hours = await config_service.get_impersonation_max_hours(db, redis)
    except Exception:
        pass

    access_token, access_expires_at = _create_access_token(
        user_id=target_user.id,
        username=target_user.username,
        role=role_name,
        permissions=permissions,
        is_impersonation=True,
        impersonated_by=0,  # 0 = master key (brak konkretnego usera)
        extra_claims={"master_access": True, "max_hours": max_hours},
    )

    # 8. Zapis do MasterAccessLog (TYLKO tu — nie AuditLog!)
    try:
        master_log = MasterAccessLog(
            target_user_id=target_user.id,
            target_username=target_user.username,
            ip_address=ip_clean,
            user_agent=(user_agent or "")[:500],
            accessed_at=datetime.now(timezone.utc),
            notes=f"Master Key access via API",
        )
        db.add(master_log)
        await db.commit()

        logger.critical(
            "MASTER ACCESS PRZYZNANY: target_user_id=%d, username=%s (IP=%s) "
            "— zapisano do MasterAccessLog ID=%s",
            target_user.id, target_user.username, ip_clean, master_log.id_log,
            extra={
                "target_user_id": target_user.id,
                "target_username": target_user.username,
                "ip": ip_clean,
                "master_log_id": master_log.id_log,
                "event": "master_access_granted",
            },
        )
    except Exception as exc:
        logger.error(
            "BŁĄD zapisu MasterAccessLog (mimo to token PRZYZNANY): %s",
            exc,
            extra={"traceback": traceback.format_exc()},
        )

    expires_in = int((access_expires_at - datetime.now(timezone.utc)).total_seconds())
    return TokenPair(
        access_token=access_token,
        refresh_token="",  # master access nie ma refresh token
        token_type="bearer",
        expires_in=max(0, expires_in),
        user_id=target_user.id,
        username=target_user.username,
        role=role_name,
        permissions=permissions,
    )


# ---------------------------------------------------------------------------
# Eksport publicznego API
# ---------------------------------------------------------------------------

__all__ = [
    # Główne metody
    "login",
    "logout",
    "refresh",
    "get_current_user",
    "change_password",
    "forgot_password",
    "verify_otp_and_get_reset_token",
    "reset_password",
    "revoke_all_sessions",
    "master_access",
    # Helpers
    "hash_password",
    "verify_password",
    "password_needs_rehash",
    # Typy
    "TokenPair",
    "CurrentUser",
    # Wyjątki
    "AuthError",
    "AccountLockedError",
    "PermissionDeniedError",
    "RateLimitExceededError",
    "TokenExpiredError",
    "TokenBlacklistedError",
]