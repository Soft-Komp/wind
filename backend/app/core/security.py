"""
Moduł bezpieczeństwa — JWT, hashing, blacklist tokenów, weryfikacja tożsamości.

Odpowiada za:
  1. Hashing haseł (argon2-cffi) i weryfikację
  2. Historia haseł — porównanie z poprzednimi hashami
  3. Generowanie i weryfikację JWT (access token + refresh token)
  4. Blacklist tokenów w Redis (logout, revoke, jednorazowe tokeny)
  5. HMAC weryfikację master key + PIN
  6. FastAPI Dependencies: get_current_user, require_permission
  7. OTP — generowanie i hashowanie kodów jednorazowych

Zasady bezpieczeństwa:
  - argon2id zamiast bcrypt — odporniejszy na GPU/ASIC ataki
  - Constant-time compare wszędzie gdzie porównujemy sekrety
  - JWT payload nie zawiera wrażliwych danych (tylko ID + uprawnienia)
  - Access token krótki (max 24h), refresh token długi (30 dni)
  - Refresh token w bazie jako SHA-256 hash — nigdy plain
  - Po wylogowaniu access token na blackliście Redis (TTL = pozostały czas życia)
  - Impersonation token — osobna flaga w payload
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from jose import JWTError, jwt
from fastapi import Depends, Header, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger(__name__)

# Typy tokenów JWT — używane w dependencies.py i api/*.py
TOKEN_TYPE_ACCESS  = "access"
TOKEN_TYPE_REFRESH = "refresh"

# Lazy import — unikamy circular import (security importuje z config)
# Importy wewnętrzne realizowane w funkcjach które ich potrzebują

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------

# Zakresy tokenów JWT — weryfikowane przy każdej operacji
TOKEN_SCOPE_ACCESS          = "access"
TOKEN_SCOPE_REFRESH         = "refresh"
TOKEN_SCOPE_CONFIRM_DELETE  = "confirm_delete"
TOKEN_SCOPE_RESET_PASSWORD  = "reset_password"
TOKEN_SCOPE_IMPERSONATION   = "impersonation"

# Prefix klucza Redis dla blacklisty tokenów
_REDIS_BLACKLIST_PREFIX  = "token:blacklist:"
# Prefix klucza Redis dla rate limitingu
_REDIS_RATELIMIT_PREFIX  = "ratelimit:"
# Prefix klucza Redis dla OTP (tymczasowe dane)
_REDIS_OTP_PREFIX        = "otp:"

# Maksymalna liczba poprzednich haseł pamiętanych w historii
PASSWORD_HISTORY_DEPTH = 5

# Schemat Bearer — wymagany w nagłówku Authorization
_bearer_scheme = HTTPBearer(auto_error=False)


# ---------------------------------------------------------------------------
# 1. HASHING HASEŁ — argon2id
# ---------------------------------------------------------------------------

def _get_argon2_hasher():
    """
    Zwraca skonfigurowany hasher argon2.
    Lazy import — argon2-cffi nie jest zawsze potrzebne przy imporcie modułu.
    """
    try:
        from argon2 import PasswordHasher
        from argon2.profiles import RFC_9106_LOW_MEMORY
        # RFC_9106_LOW_MEMORY: dobre kompromis bezpieczeństwo/wydajność dla serwera
        # time_cost=3 iterations, memory_cost=65536 KB (64MB), parallelism=4
        return PasswordHasher.from_parameters(RFC_9106_LOW_MEMORY)
    except ImportError:
        logger.critical(
            "BRAK BIBLIOTEKI argon2-cffi! Zainstaluj: pip install argon2-cffi"
        )
        raise


def hash_password(plain_password: str) -> str:
    """
    Hashuje hasło algorytmem argon2id.

    Nigdy nie loguje plain_password — parametr celowo bez logowania.

    Args:
        plain_password: Plaintext hasło.

    Returns:
        str: Hash argon2id w formacie PHC (zawiera sól i parametry).

    Raises:
        RuntimeError: Gdy biblioteka argon2-cffi jest niedostępna.
    """
    hasher = _get_argon2_hasher()
    hashed = hasher.hash(plain_password)
    logger.debug("Wygenerowano hash hasła (argon2id).")
    return hashed


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Weryfikuje hasło względem hasha argon2id.

    Używa constant-time compare wewnętrznie (gwarantowane przez argon2-cffi).
    Nigdy nie loguje plain_password.

    Args:
        plain_password:   Hasło do weryfikacji.
        hashed_password:  Hash z bazy danych.

    Returns:
        bool: True jeśli hasło poprawne, False w przeciwnym razie.
    """
    from argon2.exceptions import VerifyMismatchError, VerificationError, InvalidHashError

    hasher = _get_argon2_hasher()
    try:
        return hasher.verify(hashed_password, plain_password)
    except VerifyMismatchError:
        # Nieprawidłowe hasło — normalna sytuacja
        return False
    except (VerificationError, InvalidHashError) as exc:
        # Uszkodzony hash lub niezgodny format
        logger.error(
            "Błąd weryfikacji hasła (uszkodzony hash?): %s",
            type(exc).__name__,
        )
        return False


def needs_rehash(hashed_password: str) -> bool:
    """
    Sprawdza czy hash wymaga ponownego generowania (np. po zmianie parametrów).

    Wywoływana przy logowaniu — jeśli True, hash jest aktualizowany w tle.

    Returns:
        bool: True jeśli hash powinien być przeregenerowany.
    """
    hasher = _get_argon2_hasher()
    return hasher.check_needs_rehash(hashed_password)


def check_password_history(
    plain_password: str,
    password_hashes: List[str],
) -> bool:
    """
    Sprawdza czy nowe hasło jest inne niż poprzednie PASSWORD_HISTORY_DEPTH haseł.

    Weryfikuje przez verify_password — constant-time compare dla każdego hasha.
    Loguje tylko fakt sprawdzenia, nie wartości.

    Args:
        plain_password:   Nowe hasło do sprawdzenia.
        password_hashes:  Lista poprzednich hashów (najnowszy pierwszy).
                          Pobierana z tabeli PasswordHistory (do zaimplementowania)
                          lub z pola dedykowanego w bazie.

    Returns:
        bool: True jeśli hasło NIE jest w historii (można użyć).
              False jeśli hasło było już używane (odrzuć).
    """
    for i, old_hash in enumerate(password_hashes[:PASSWORD_HISTORY_DEPTH]):
        if verify_password(plain_password, old_hash):
            logger.warning(
                "Próba ustawienia hasła które było już używane "
                "(historia: pozycja %d/%d).",
                i + 1,
                PASSWORD_HISTORY_DEPTH,
            )
            return False  # Hasło było używane — odrzuć
    return True  # Hasło nowe — zaakceptuj


# ---------------------------------------------------------------------------
# 2. HASHOWANIE TOKENÓW — SHA-256 dla refresh tokenów w bazie
# ---------------------------------------------------------------------------

def hash_token(plain_token: str) -> str:
    """
    Hashuje token (refresh token, OTP) algorytmem SHA-256.

    Refresh tokeny przechowywane w bazie jako SHA-256 hash.
    Przy weryfikacji: hash przesłanego tokenu porównywany z hashem w bazie.

    Args:
        plain_token: Plaintext token.

    Returns:
        str: SHA-256 hex digest.
    """
    return hashlib.sha256(plain_token.encode("utf-8")).hexdigest()


def generate_secure_token(length: int = 64) -> str:
    """
    Generuje kryptograficznie bezpieczny token URL-safe.

    Używany do refresh tokenów, reset tokenów itp.

    Args:
        length: Długość tokenu w znakach (domyślnie 64).

    Returns:
        str: URL-safe base64 token.
    """
    return secrets.token_urlsafe(length)


def generate_otp_code(length: int = 6) -> str:
    """
    Generuje cyfrowy kod OTP.

    Args:
        length: Liczba cyfr (domyślnie 6, max 8).

    Returns:
        str: Kod OTP złożony z cyfr, uzupełniony zerami wiodącymi.
    """
    max_val = 10 ** length
    code = secrets.randbelow(max_val)
    return str(code).zfill(length)


# ---------------------------------------------------------------------------
# 3. JWT — generowanie i weryfikacja
# ---------------------------------------------------------------------------

def _get_jwt_config() -> tuple[str, str]:
    """Pobiera SECRET_KEY i ALGORITHM z settings. Lazy import."""
    from app.core.config import settings
    return (
        settings.secret_key.get_secret_value(),
        settings.algorithm,
    )


def create_access_token(
    *,
    user_id: int,
    username: str,
    role_id: int,
    permissions: List[str],
    is_impersonation: bool = False,
    impersonated_by: Optional[int] = None,
    expire_hours: Optional[int] = None,
) -> tuple[str, datetime]:
    """
    Generuje JWT access token.

    Payload zawiera minimalne dane — nigdy hasło, email ani inne PII.
    Uprawnienia dołączone do tokenu eliminują konieczność odpytywania DB
    przy każdym żądaniu (Redis cache i tak jest fallbackiem).

    Args:
        user_id:          ID użytkownika (subject).
        username:         Login użytkownika (do logów, nie weryfikacji).
        role_id:          ID roli.
        permissions:      Lista uprawnień: ['auth.login', 'users.view_list', ...].
        is_impersonation: Czy token dotyczy sesji impersonacji.
        impersonated_by:  ID admina (tylko przy is_impersonation=True).
        expire_hours:     Nadpisuje domyślny czas życia z settings.

    Returns:
        tuple[str, datetime]: (zakodowany_token, czas_wygaśnięcia_UTC)
    """
    from app.core.config import settings

    secret_key, algorithm = _get_jwt_config()
    hours = expire_hours or settings.access_token_expire_hours
    expires_at = datetime.now(tz=timezone.utc) + timedelta(hours=hours)

    payload: Dict[str, Any] = {
        "sub":   str(user_id),           # Subject — ID usera jako string (standard JWT)
        "scope": TOKEN_SCOPE_ACCESS,
        "uid":   user_id,
        "uname": username,
        "rid":   role_id,
        "perms": permissions,            # Lista uprawnień — do szybkiej weryfikacji
        "iat":   datetime.now(tz=timezone.utc),
        "exp":   expires_at,
        "jti":   secrets.token_hex(16),  # JWT ID — unikalny identyfikator tokenu
    }

    if is_impersonation:
        payload["is_imp"] = True
        payload["imp_by"] = impersonated_by

    token = jwt.encode(payload, secret_key, algorithm=algorithm)

    logger.info(
        "Wygenerowano access token | user_id=%d | username=%s | "
        "is_impersonation=%s | expires=%s | jti=%s",
        user_id,
        username,
        is_impersonation,
        expires_at.isoformat(),
        payload["jti"],
    )

    return token, expires_at


def create_refresh_token(
    *,
    user_id: int,
    expire_days: Optional[int] = None,
) -> tuple[str, str, datetime]:
    """
    Generuje refresh token.

    Refresh token to kryptograficznie bezpieczny losowy string (nie JWT).
    W bazie przechowywany jako SHA-256 hash.
    JWT nie jest używany dla refresh tokenów — zapobiega to możliwości
    weryfikacji tokenu bez bazy danych.

    Args:
        user_id:     ID użytkownika.
        expire_days: Czas życia w dniach (domyślnie z settings).

    Returns:
        tuple[str, str, datetime]: (plain_token, hashed_token, expires_at)
        plain_token    — do wysłania do klienta (tylko raz!)
        hashed_token   — do zapisu w bazie danych
        expires_at     — czas wygaśnięcia UTC
    """
    from app.core.config import settings

    days = expire_days or settings.refresh_token_expire_days
    expires_at = datetime.now(tz=timezone.utc) + timedelta(days=days)

    plain_token = generate_secure_token(64)
    hashed_token = hash_token(plain_token)

    logger.info(
        "Wygenerowano refresh token | user_id=%d | expires=%s",
        user_id,
        expires_at.isoformat(),
    )

    return plain_token, hashed_token, expires_at


def decode_access_token(token: str) -> Dict[str, Any]:
    """
    Dekoduje i weryfikuje JWT access token.

    Weryfikuje: podpis, czas wygaśnięcia, scope.
    NIE weryfikuje blacklisty — to robi get_current_user().

    Args:
        token: Zakodowany JWT.

    Returns:
        dict: Zdekodowany payload.

    Raises:
        HTTPException 401: Gdy token jest nieprawidłowy lub wygasł.
    """
    secret_key, algorithm = _get_jwt_config()

    try:
        payload = jwt.decode(
            token,
            secret_key,
            algorithms=[algorithm],
            options={"require": ["sub", "exp", "jti", "scope"]},
        )
    except JWTError as exc:
        logger.warning(
            "Nieprawidłowy JWT: %s | token_fragment=%s...",
            str(exc),
            token[:20] if len(token) > 20 else "krótki",
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token nieprawidłowy lub wygasł.",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc

    # Weryfikacja scope
    if payload.get("scope") != TOKEN_SCOPE_ACCESS:
        logger.warning(
            "Próba użycia tokenu z błędnym scope: %s",
            payload.get("scope"),
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Nieprawidłowy typ tokenu.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return payload


def create_short_lived_token(
    *,
    scope: str,
    entity_type: str,
    entity_id: int,
    requested_by: int,
    ttl_seconds: int = 60,
) -> str:
    """
    Generuje krótkotrwały JWT do jednorazowych operacji.

    Używany do:
      - Tokenów potwierdzających DELETE (scope: confirm_delete)
      - Tokenów resetowania hasła (scope: reset_password)

    Args:
        scope:        Zakres tokenu ('confirm_delete', 'reset_password').
        entity_type:  Typ encji ('User', 'Role', itp.).
        entity_id:    ID encji.
        requested_by: ID użytkownika inicjującego operację.
        ttl_seconds:  Czas życia w sekundach.

    Returns:
        str: Zakodowany JWT.
    """
    secret_key, algorithm = _get_jwt_config()
    expires_at = datetime.now(tz=timezone.utc) + timedelta(seconds=ttl_seconds)

    payload = {
        "scope":        scope,
        "entity_type":  entity_type,
        "entity_id":    entity_id,
        "requested_by": requested_by,
        "exp":          expires_at,
        "jti":          secrets.token_hex(16),
    }

    token = jwt.encode(payload, secret_key, algorithm=algorithm)

    logger.info(
        "Wygenerowano token jednorazowy | scope=%s | entity=%s:%d | "
        "requested_by=%d | ttl=%ds | jti=%s",
        scope,
        entity_type,
        entity_id,
        requested_by,
        ttl_seconds,
        payload["jti"],
    )

    return token


def decode_short_lived_token(
    token: str,
    *,
    expected_scope: str,
    expected_entity_type: str,
    expected_entity_id: int,
    requesting_user_id: int,
) -> Dict[str, Any]:
    """
    Dekoduje i weryfikuje jednorazowy JWT.

    Weryfikuje: podpis, exp, scope, entity_type, entity_id, requested_by.

    Args:
        token:                 Zakodowany JWT.
        expected_scope:        Oczekiwany scope.
        expected_entity_type:  Oczekiwany typ encji.
        expected_entity_id:    Oczekiwane ID encji.
        requesting_user_id:    ID użytkownika wykonującego potwierdzenie.

    Returns:
        dict: Payload tokenu.

    Raises:
        HTTPException 400: Gdy token nieprawidłowy, wygasł lub niezgodne dane.
    """
    secret_key, algorithm = _get_jwt_config()

    try:
        payload = jwt.decode(
            token,
            secret_key,
            algorithms=[algorithm],
            options={"require": ["scope", "entity_type", "entity_id",
                                 "requested_by", "exp", "jti"]},
        )
    except JWTError as exc:
        logger.warning("Nieprawidłowy token jednorazowy: %s", str(exc))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Token potwierdzający jest nieprawidłowy lub wygasł.",
        ) from exc

    # Weryfikacja wszystkich pól
    errors = []
    if payload.get("scope") != expected_scope:
        errors.append(f"scope: oczekiwano '{expected_scope}', otrzymano '{payload.get('scope')}'")
    if payload.get("entity_type") != expected_entity_type:
        errors.append(f"entity_type: niezgodność")
    if payload.get("entity_id") != expected_entity_id:
        errors.append(f"entity_id: niezgodność")
    if payload.get("requested_by") != requesting_user_id:
        errors.append("requested_by: token należy do innego użytkownika")

    if errors:
        logger.warning(
            "Weryfikacja tokenu jednorazowego nieudana: %s | jti=%s",
            "; ".join(errors),
            payload.get("jti", "brak"),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Token potwierdzający jest nieprawidłowy.",
        )

    return payload


# ---------------------------------------------------------------------------
# 4. BLACKLISTA TOKENÓW — Redis
# ---------------------------------------------------------------------------

async def blacklist_token(jti: str, ttl_seconds: int) -> None:
    """
    Dodaje JTI tokenu do blacklisty Redis.

    Wywoływana przy: logout, zmianie hasła, dezaktywacji konta.
    TTL = pozostały czas życia tokenu (nie ma sensu trzymać po wygaśnięciu).

    Args:
        jti:         JWT ID (pole 'jti' z payloadu).
        ttl_seconds: Czas po którym Redis automatycznie usuwa wpis.
    """
    from app.core.redis import get_redis

    redis = await get_redis()
    key = f"{_REDIS_BLACKLIST_PREFIX}{jti}"

    await redis.set(key, "1", ex=ttl_seconds)

    logger.info(
        "Token dodany do blacklisty | jti=%s | ttl=%ds",
        jti,
        ttl_seconds,
    )


async def is_token_blacklisted(jti: str) -> bool:
    """
    Sprawdza czy token jest na blackliście Redis.

    Wywoływana przy każdym żądaniu w get_current_user().

    Args:
        jti: JWT ID z payloadu access tokenu.

    Returns:
        bool: True jeśli token jest unieważniony.
    """
    from app.core.redis import get_redis

    redis = await get_redis()
    key = f"{_REDIS_BLACKLIST_PREFIX}{jti}"
    result = await redis.exists(key)
    return bool(result)


# ---------------------------------------------------------------------------
# 5. MASTER KEY — HMAC weryfikacja
# ---------------------------------------------------------------------------

def verify_master_key(submitted_key: str) -> bool:
    """
    Weryfikuje master key constant-time compare.

    NIGDY nie loguje submitted_key ani rzeczywistego master_key.

    Args:
        submitted_key: Klucz przesłany przez klienta.

    Returns:
        bool: True jeśli klucz poprawny.
    """
    from app.core.config import settings

    real_key = settings.master_key.get_secret_value()
    # hmac.compare_digest — gwarantuje constant-time compare
    result = hmac.compare_digest(
        submitted_key.encode("utf-8"),
        real_key.encode("utf-8"),
    )

    if not result:
        logger.warning(
            "Nieudana weryfikacja master key | "
            "Potencjalna próba nieautoryzowanego dostępu serwisowego."
        )

    return result


def verify_master_pin(submitted_pin: str, stored_pin_hash: str) -> bool:
    """
    Weryfikuje PIN master access względem hasha z SystemConfig.

    PIN hash to argon2id hash (taki sam jak hasła użytkowników).
    Constant-time compare gwarantowany przez argon2-cffi.

    Args:
        submitted_pin:   PIN przesłany przez klienta.
        stored_pin_hash: argon2id hash z SystemConfig.master_key.pin_hash.

    Returns:
        bool: True jeśli PIN poprawny.
    """
    result = verify_password(submitted_pin, stored_pin_hash)

    if not result:
        logger.warning(
            "Nieudana weryfikacja PIN master access. "
            "Potencjalna próba nieautoryzowanego dostępu serwisowego."
        )

    return result


# ---------------------------------------------------------------------------
# 6. RATE LIMITING — Redis (dla master-access i logowania)
# ---------------------------------------------------------------------------

async def check_rate_limit(
    key: str,
    *,
    max_attempts: int,
    window_seconds: int,
    block_seconds: int,
) -> tuple[bool, int]:
    """
    Sprawdza i inkrementuje licznik prób dla danego klucza.

    Używany dla:
      - POST /auth/login — blokada konta po X nieudanych próbach
      - POST /auth/master-access — 3 próby/15 min → blokada IP na 1h

    Args:
        key:             Klucz Redis (np. "ratelimit:login:192.168.1.1").
        max_attempts:    Maksymalna liczba prób.
        window_seconds:  Okno czasowe w sekundach.
        block_seconds:   Czas blokady po przekroczeniu limitu.

    Returns:
        tuple[bool, int]:
            - allowed: True jeśli próba dozwolona, False jeśli zablokowana
            - remaining: Pozostała liczba prób (0 jeśli zablokowany)
    """
    from app.core.redis import get_redis

    redis = await get_redis()
    block_key = f"{_REDIS_RATELIMIT_PREFIX}block:{key}"
    counter_key = f"{_REDIS_RATELIMIT_PREFIX}count:{key}"

    # Sprawdź czy IP/klucz jest zablokowany
    if await redis.exists(block_key):
        ttl = await redis.ttl(block_key)
        logger.warning(
            "Żądanie z zablokowanego klucza | key=%s | blokada_pozostała=%ds",
            key,
            ttl,
        )
        return False, 0

    # Inkrementuj licznik
    current = await redis.incr(counter_key)

    # Ustaw TTL przy pierwszym inkremencie
    if current == 1:
        await redis.expire(counter_key, window_seconds)

    remaining = max(0, max_attempts - current)

    # Przekroczono limit — zablokuj
    if current >= max_attempts:
        await redis.set(block_key, "1", ex=block_seconds)
        await redis.delete(counter_key)
        logger.warning(
            "Limit prób przekroczony — blokada | key=%s | "
            "próby=%d | blokada=%ds",
            key,
            current,
            block_seconds,
        )
        return False, 0

    logger.debug(
        "Rate limit check | key=%s | próba=%d/%d | pozostało=%d",
        key,
        current,
        max_attempts,
        remaining,
    )

    return True, remaining


# ---------------------------------------------------------------------------
# 7. FastAPI Dependencies — weryfikacja tożsamości i uprawnień
# ---------------------------------------------------------------------------

class CurrentUser:
    """
    Model zalogowanego użytkownika dostępny w endpointach.

    Wypełniany przez get_current_user() z danych JWT payloadu.
    Nie wymaga odpytywania bazy danych przy każdym żądaniu.
    """
    __slots__ = (
        "user_id", "username", "role_id", "permissions",
        "is_impersonation", "impersonated_by", "jti",
    )

    def __init__(self, payload: Dict[str, Any]) -> None:
        self.user_id:         int            = int(payload["uid"])
        self.username:        str            = payload.get("uname", "")
        self.role_id:         int            = payload.get("rid", 0)
        self.permissions:     List[str]      = payload.get("perms", [])
        self.is_impersonation: bool          = payload.get("is_imp", False)
        self.impersonated_by: Optional[int]  = payload.get("imp_by")
        self.jti:             str            = payload.get("jti", "")

    def has_permission(self, permission: str) -> bool:
        """Sprawdza czy użytkownik ma dane uprawnienie."""
        return permission in self.permissions

    def has_any_permission(self, *permissions: str) -> bool:
        """Sprawdza czy użytkownik ma co najmniej jedno z uprawnień."""
        return any(p in self.permissions for p in permissions)

    def has_all_permissions(self, *permissions: str) -> bool:
        """Sprawdza czy użytkownik ma wszystkie wymienione uprawnienia."""
        return all(p in self.permissions for p in permissions)

    def __repr__(self) -> str:
        return (
            f"CurrentUser(user_id={self.user_id}, username='{self.username}', "
            f"role_id={self.role_id}, is_impersonation={self.is_impersonation})"
        )


async def get_current_user(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer_scheme),
) -> CurrentUser:
    """
    FastAPI Dependency — weryfikuje JWT i zwraca zalogowanego użytkownika.

    Kroki weryfikacji:
      1. Wyciągniecie tokenu z nagłówka Authorization: Bearer <token>
      2. Dekodowanie i weryfikacja JWT (podpis, exp, scope)
      3. Sprawdzenie blacklisty Redis (czy token nie był unieważniony)
      4. Zwrócenie CurrentUser z danymi z payload

    Użycie:
        @router.get("/me")
        async def get_me(user: CurrentUser = Depends(get_current_user)):
            return user.user_id

    Raises:
        HTTPException 401: Gdy brak tokenu, token nieprawidłowy lub unieważniony.
    """
    if not credentials:
        logger.warning(
            "Brak tokenu Authorization | path=%s | ip=%s",
            request.url.path,
            request.client.host if request.client else "unknown",
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Brak tokenu autoryzacyjnego.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials
    payload = decode_access_token(token)

    jti = payload.get("jti", "")

    # Sprawdzenie blacklisty
    if jti and await is_token_blacklisted(jti):
        logger.warning(
            "Użycie unieważnionego tokenu | jti=%s | user_id=%s | "
            "path=%s | ip=%s",
            jti,
            payload.get("uid"),
            request.url.path,
            request.client.host if request.client else "unknown",
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token został unieważniony. Zaloguj się ponownie.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = CurrentUser(payload)

    logger.debug(
        "Autoryzacja OK | user_id=%d | username=%s | path=%s | "
        "is_impersonation=%s",
        user.user_id,
        user.username,
        request.url.path,
        user.is_impersonation,
    )

    return user


def require_permission(permission: str):
    """
    Factory Dependency — weryfikuje że użytkownik ma konkretne uprawnienie.

    Użycie:
        @router.get("/users")
        async def list_users(
            user: CurrentUser = Depends(require_permission("users.view_list")),
        ):

    Args:
        permission: Wymagane uprawnienie (format: 'kategoria.akcja').

    Returns:
        Dependency zwracający CurrentUser (jeśli uprawnienie OK).

    Raises:
        HTTPException 403: Gdy brak uprawnienia.
    """
    async def _dependency(
        current_user: CurrentUser = Depends(get_current_user),
    ) -> CurrentUser:
        if not current_user.has_permission(permission):
            logger.warning(
                "Odmowa dostępu | user_id=%d | username=%s | "
                "wymagane=%s | posiadane=%s",
                current_user.user_id,
                current_user.username,
                permission,
                current_user.permissions,
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Brak wymaganego uprawnienia: {permission}",
            )
        return current_user

    # Nadajemy czytelną nazwę dla Swagger UI
    _dependency.__name__ = f"require_{permission.replace('.', '_')}"
    return _dependency


def require_any_permission(*permissions: str):
    """
    Dependency — wymaga co najmniej jednego z podanych uprawnień.

    Użycie:
        Depends(require_any_permission("monits.send_email_single", "monits.send_email_bulk"))
    """
    async def _dependency(
        current_user: CurrentUser = Depends(get_current_user),
    ) -> CurrentUser:
        if not current_user.has_any_permission(*permissions):
            logger.warning(
                "Odmowa dostępu (brak ANY) | user_id=%d | wymagane_any=%s",
                current_user.user_id,
                permissions,
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Brak wymaganego uprawnienia.",
            )
        return current_user

    return _dependency


async def get_current_user_optional(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer_scheme),
) -> Optional[CurrentUser]:
    """
    Opcjonalna wersja get_current_user — nie rzuca błędu gdy brak tokenu.

    Używana w endpointach które działają zarówno dla anonimowych
    jak i zalogowanych użytkowników.
    """
    if not credentials:
        return None
    try:
        payload = decode_access_token(credentials.credentials)
        jti = payload.get("jti", "")
        if jti and await is_token_blacklisted(jti):
            return None
        return CurrentUser(payload)
    except HTTPException:
        return None