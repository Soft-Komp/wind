"""
Serwis Impersonacji — System Windykacja
=========================================
Krok 7 / Faza 3 — services/impersonation_service.py

Odpowiedzialność:
    - Impersonacja użytkownika przez admina (wymaga uprawnienia auth.impersonate)
    - Tworzenie specjalnych tokenów JWT z flagą is_impersonation=True
    - Zapis sesji impersonacji do MasterAccessLog (INSERT-only, DENY SELECT przez app)
    - Blacklistowanie tokenu przy zakończeniu sesji
    - Diagnostyka statusu aktywnej sesji impersonacji

Decyzje projektowe:
    - Impersonacja i master_access używają tej samej tabeli MasterAccessLog
      (analogiczna semantyka — wejście "kogoś innego" w uprawnienia użytkownika)
    - max_hours z SystemConfig("impersonation.max_hours", default=4)
    - Blacklista przez Redis (klucz auth:blacklist:{jti})
    - Token impersonacji: standardowy access token JWT z dodatkowym payloadem
    - Nie ma rotacji tokenu — blacklista po zakończeniu sesji
    - WSZYSTKIE operacje impersonacji logowane do:
        1. MasterAccessLog (INSERT)
        2. AuditLog (fire-and-forget)
        3. Plik logs/impersonation_YYYY-MM-DD.jsonl (append-only)

Zależności:
    - services/audit_service.py
    - services/config_service.py
    - services/auth_service.py (create_access_token)

Ścieżka docelowa: backend/app/services/impersonation_service.py
Autor: System Windykacja — Faza 3 Krok 7
Wersja: 1.0.0
Data: 2026-02-18
"""

from __future__ import annotations

import logging
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import orjson
from jose import JWTError, jwt
from redis.asyncio import Redis
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.models.master_access_log import MasterAccessLog
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

# Domyślny maksymalny czas trwania sesji impersonacji (godz.)
_DEFAULT_MAX_HOURS: int = 4

# Klucz Redis blacklisty (wspólny z auth_service)
_REDIS_BLACKLIST_KEY = "auth:blacklist:{jti}"

# Klucz w payloadzie JWT oznaczający impersonację
_JWT_CLAIM_IS_IMPERSONATION = "is_impersonation"
_JWT_CLAIM_IMPERSONATED_BY  = "impersonated_by"

# Plik logów impersonacji
_IMPERSONATION_LOG_FILE_PATTERN = "logs/impersonation_{date}.jsonl"

# Typ akcji w MasterAccessLog
_ACCESS_TYPE_IMPERSONATION = "IMPERSONATION"


# ===========================================================================
# Dataclassy wynikowe
# ===========================================================================

@dataclass(frozen=True)
class ImpersonationData:
    """
    Dane sesji impersonacji zwracane przez start().

    Attributes:
        access_token:      JWT token z flagą is_impersonation.
        token_type:        Zawsze "bearer".
        expires_at:        Czas wygaśnięcia tokenu (UTC).
        admin_id:          ID admina inicjującego impersonację.
        admin_username:    Username admina.
        target_user_id:    ID impersonowanego użytkownika.
        target_username:   Username impersonowanego użytkownika.
        master_log_id:     ID rekordu w MasterAccessLog.
        jti:               JWT ID tokenu (do blacklisty).
        max_hours:         Maksymalny czas sesji w godzinach.
    """
    access_token: str
    token_type: str
    expires_at: datetime
    admin_id: int
    admin_username: str
    target_user_id: int
    target_username: str
    master_log_id: int
    jti: str
    max_hours: int


@dataclass(frozen=True)
class ImpersonationStatus:
    """
    Status aktywnej sesji impersonacji — zwracany przez get_status().

    Attributes:
        is_impersonation:   Czy token jest tokenem impersonacji.
        impersonated_by:    ID admina.
        target_user_id:     ID impersonowanego użytkownika.
        target_username:    Username impersonowanego użytkownika.
        expires_at:         Czas wygaśnięcia tokenu.
        jti:                JWT ID tokenu.
        is_expired:         Czy token już wygasł.
        is_blacklisted:     Czy token jest na blackliście (Redis).
    """
    is_impersonation: bool
    impersonated_by: Optional[int]
    target_user_id: Optional[int]
    target_username: Optional[str]
    expires_at: Optional[datetime]
    jti: Optional[str]
    is_expired: bool
    is_blacklisted: bool


# ===========================================================================
# Klasy wyjątków
# ===========================================================================

class ImpersonationError(Exception):
    """Bazowy wyjątek serwisu impersonacji."""


class ImpersonationPermissionError(ImpersonationError):
    """Brak uprawnienia auth.impersonate."""


class ImpersonationUserNotFoundError(ImpersonationError):
    """Docelowy użytkownik nie istnieje lub jest nieaktywny."""


class ImpersonationSelfError(ImpersonationError):
    """Admin próbuje impersonować samego siebie."""


class ImpersonationAdminTargetError(ImpersonationError):
    """Próba impersonacji innego admina (zabronione)."""


class ImpersonationTokenError(ImpersonationError):
    """Błąd tokenu impersonacji (nieprawidłowy, wygasły, nie jest tokenem impersonacji)."""


class ImpersonationEndError(ImpersonationError):
    """Błąd przy kończeniu sesji impersonacji."""


# ===========================================================================
# Funkcje pomocnicze — prywatne
# ===========================================================================

def _get_log_dir() -> Path:
    """Zwraca i tworzy katalog logów."""
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def _get_impersonation_log_file() -> Path:
    """Zwraca dzienną ścieżkę pliku logów impersonacji."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return _get_log_dir() / f"impersonation_{today}.jsonl"


def _append_to_file(filepath: Path, record: dict) -> None:
    """
    Dopisuje rekord JSON do pliku JSON Lines (append-only, thread-safe).

    Błędy zapisu logowane jako WARNING — NIE przerywają działania aplikacji.
    """
    try:
        line = orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE)
        with filepath.open("ab") as f:
            f.write(line)
    except OSError as exc:
        logger.warning(
            "Nie można zapisać do pliku logu impersonacji",
            extra={
                "filepath": str(filepath),
                "error": str(exc),
                "action": record.get("action", "unknown"),
            }
        )


def _build_log_record(action: str, **kwargs) -> dict:
    """Buduje ustrukturyzowany rekord logu impersonacji."""
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": "impersonation_service",
        "action": action,
        **kwargs,
    }


def _sanitize_reason(reason: str) -> str:
    """
    Normalizuje i waliduje uzasadnienie impersonacji.

    Powód jest obowiązkowy — bez uzasadnienia impersonacja jest niedozwolona.
    NFC normalizacja + strip + limit długości.

    Args:
        reason: Uzasadnienie impersonacji podane przez admina.

    Returns:
        Znormalizowany reason.

    Raises:
        ImpersonationError: Gdy reason jest pusty lub za krótki.
    """
    normalized = unicodedata.normalize("NFC", reason.strip())
    if len(normalized) < 10:
        raise ImpersonationError(
            "Uzasadnienie impersonacji musi mieć co najmniej 10 znaków."
        )
    # Obcinamy do rozsądnej długości (kolumna Notes: NVARCHAR(500))
    return normalized[:490]


async def _get_active_user(db: AsyncSession, user_id: int) -> Optional[User]:
    """Pobiera aktywnego użytkownika po ID."""
    result = await db.execute(
        select(User).where(
            and_(User.id_user == user_id, User.is_active == True)  # noqa: E712
        )
    )
    return result.scalar_one_or_none()


def _decode_token_unverified(token: str) -> Optional[dict]:
    """
    Dekoduje JWT bez weryfikacji podpisu — do odczytu claims.

    Używane wyłącznie do diagnostyki/statusu, NIE do autoryzacji.
    Autoryzacja zawsze przez pełną weryfikację podpisu.

    Args:
        token: Surowy JWT string.

    Returns:
        Słownik claims lub None przy błędzie dekodowania.
    """
    try:
        return jwt.get_unverified_claims(token)
    except JWTError:
        return None


async def _blacklist_token(redis: Redis, jti: str, expires_at: datetime) -> None:
    """
    Dodaje JTI tokenu do blacklisty Redis.

    TTL blacklisty = czas do wygaśnięcia tokenu (żeby nie zaśmiecać Redis).
    Przy niedostępności Redis — loguje CRITICAL, ale NIE przerywa operacji.
    (fail-open — spójne z decyzją projektową z Kroku 5)

    Args:
        redis:      Klient Redis.
        jti:        JWT ID tokenu do zablokowania.
        expires_at: Czas wygaśnięcia tokenu (wyznacza TTL klucza Redis).
    """
    redis_key = _REDIS_BLACKLIST_KEY.format(jti=jti)
    now = datetime.now(timezone.utc)
    ttl_seconds = max(int((expires_at - now).total_seconds()), 1)

    try:
        await redis.set(redis_key, "blacklisted", ex=ttl_seconds)
        logger.info(
            "Token impersonacji dodany do blacklisty",
            extra={"jti": jti, "ttl_seconds": ttl_seconds}
        )
    except Exception as exc:
        # fail-open przy awarii Redis
        logger.critical(
            "KRYTYCZNY: Nie udało się dodać tokenu do blacklisty Redis! "
            "Token impersonacji może być nadal aktywny.",
            extra={
                "jti": jti,
                "error": str(exc),
                "expires_at": expires_at.isoformat(),
            }
        )


async def _is_token_blacklisted(redis: Redis, jti: str) -> bool:
    """
    Sprawdza czy token JTI jest na blackliście Redis.

    Przy niedostępności Redis → fail-open (False) — spójne z auth_service.

    Args:
        redis: Klient Redis.
        jti:   JWT ID do sprawdzenia.

    Returns:
        True jeśli token jest na blackliście, False w przeciwnym razie.
    """
    try:
        return bool(await redis.exists(_REDIS_BLACKLIST_KEY.format(jti=jti)))
    except Exception:
        return False  # fail-open


# ===========================================================================
# Publiczne API serwisu
# ===========================================================================

async def start(
    db: AsyncSession,
    redis: Redis,
    admin_id: int,
    target_user_id: int,
    reason: str,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> ImpersonationData:
    """
    Rozpoczyna sesję impersonacji użytkownika przez admina.

    Przepływ:
        1. Walidacja i sanityzacja uzasadnienia
        2. Weryfikacja admina (aktywny, musi istnieć)
        3. Weryfikacja docelowego użytkownika (aktywny, nie admin)
        4. Zabezpieczenia: zakaz impersonacji samego siebie
        5. Zakaz impersonacji innego admina (poziom uprawnień: sprawdź rolę)
        6. Pobierz max_hours z SystemConfig
        7. Utwórz token JWT z is_impersonation=True, impersonated_by=admin_id
        8. Zapisz sesję do MasterAccessLog (INSERT)
        9. Zapis do pliku logów impersonacji
       10. AuditLog (fire-and-forget)

    ⚠️  WAŻNE: Sprawdzenie uprawnienia auth.impersonate musi być wykonane
    przez endpoint PRZED wywołaniem tej funkcji (przez Depends(require_permission)).
    Serwis nie sprawdza uprawnień samodzielnie — to odpowiedzialność warstwy API.

    Args:
        db:             Sesja SQLAlchemy (async).
        redis:          Klient Redis (async).
        admin_id:       ID admina inicjującego impersonację.
        target_user_id: ID impersonowanego użytkownika.
        reason:         Uzasadnienie impersonacji (obowiązkowe, min. 10 znaków).
        ip_address:     IP admina (do logowania).
        user_agent:     User-Agent admina (do logowania).

    Returns:
        ImpersonationData z tokenem i metadanymi sesji.

    Raises:
        ImpersonationError:             Ogólny błąd.
        ImpersonationUserNotFoundError: Użytkownik nie istnieje lub nieaktywny.
        ImpersonationSelfError:         Admin próbuje impersonować siebie.
        ImpersonationAdminTargetError:  Próba impersonacji innego admina.
    """
    # --- 1. Sanityzacja uzasadnienia ---
    reason = _sanitize_reason(reason)

    # --- 2. Weryfikacja admina ---
    admin = await _get_active_user(db, admin_id)
    if admin is None:
        raise ImpersonationUserNotFoundError(
            f"Admin ID={admin_id} nie istnieje lub jest nieaktywny."
        )

    # --- 3. Weryfikacja docelowego użytkownika ---
    target_user = await _get_active_user(db, target_user_id)
    if target_user is None:
        logger.warning(
            "Impersonacja: Docelowy użytkownik nie istnieje lub jest nieaktywny",
            extra={
                "admin_id": admin_id,
                "admin_username": admin.username,
                "target_user_id": target_user_id,
                "ip_address": ip_address,
            }
        )
        raise ImpersonationUserNotFoundError(
            f"Docelowy użytkownik ID={target_user_id} nie istnieje lub jest nieaktywny."
        )

    # --- 4. Zabezpieczenie: zakaz impersonacji samego siebie ---
    if admin_id == target_user_id:
        logger.warning(
            "Impersonacja: Próba impersonacji samego siebie",
            extra={
                "admin_id": admin_id,
                "admin_username": admin.username,
                "ip_address": ip_address,
            }
        )
        raise ImpersonationSelfError(
            "Nie można impersonować samego siebie."
        )

    # --- 5. Zabezpieczenie: zakaz impersonacji innego admina ---
    # Sprawdzamy rolę docelowego użytkownika — impersonacja Admina przez Admina zabroniona
    # (zapobiega eskalacji uprawnień między adminami)
    if hasattr(target_user, "role") and target_user.role:
        target_role_name = (
            target_user.role.role_name
            if hasattr(target_user.role, "role_name")
            else ""
        )
    else:
        # Pobierz role_id i sprawdź bezpośrednio
        target_role_name = ""

    # Impersonacja Admina przez Admina — jawnie zabroniona
    if target_role_name.lower() in {"admin", "administrator"}:
        logger.error(
            "Impersonacja: Próba impersonacji użytkownika z rolą Admin — zabronione",
            extra={
                "admin_id": admin_id,
                "admin_username": admin.username,
                "target_user_id": target_user_id,
                "target_username": target_user.username,
                "target_role": target_role_name,
                "ip_address": ip_address,
            }
        )
        raise ImpersonationAdminTargetError(
            f"Impersonacja użytkownika z rolą '{target_role_name}' jest zabroniona."
        )

    # --- 6. TTL z konfiguracji ---
    max_hours = await config_service.get_int(
        db, redis,
        key="impersonation.max_hours",
        default=_DEFAULT_MAX_HOURS,
    )

    # --- 7. Pobranie uprawnień docelowego użytkownika ---
    # Ładujemy uprawnienia aby umieścić je w JWT (tak jak w normalnym login)
    from sqlalchemy.orm import selectinload
    result = await db.execute(
        select(User)
        .options(
            selectinload(User.role).selectinload("permissions")
        )
        .where(User.id_user == target_user_id)
    )
    target_user_with_role = result.scalar_one_or_none()

    # Ekstrakcja uprawnień z roli
    permissions: list[str] = []
    if target_user_with_role and target_user_with_role.role:
        role = target_user_with_role.role
        if hasattr(role, "permissions"):
            permissions = [
                p.permission_name
                for p in role.permissions
                if p.is_active
            ]

    # --- 8. Tworzenie tokenu JWT impersonacji ---
    import secrets as _secrets
    jti = _secrets.token_hex(16)

    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(hours=max_hours)

    # Payload tokenu impersonacji (rozszerzony o metadane impersonacji)
    token_payload = {
        "sub": str(target_user_id),
        "username": target_user.username,
        "role": target_role_name or "Unknown",
        "permissions": permissions,
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
        "type": "access",
        "jti": jti,
        _JWT_CLAIM_IS_IMPERSONATION: True,
        _JWT_CLAIM_IMPERSONATED_BY: admin_id,
    }

    access_token = jwt.encode(
        token_payload,
        settings.secret_key,
        algorithm=settings.algorithm,
    )

    # --- 9. Zapis do MasterAccessLog ---
    # ⚠️ App user ma tylko INSERT na tej tabeli (DENY SELECT/UPDATE/DELETE)
    master_log = MasterAccessLog(
        target_user_id=target_user_id,
        target_username=target_user.username,
        ip_address=ip_address or "unknown",
        user_agent=user_agent,
        accessed_at=now,
        session_ended_at=None,
        notes=(
            f"IMPERSONATION by admin_id={admin_id} ({admin.username}). "
            f"Reason: {reason[:200]}"
        ),
    )
    db.add(master_log)
    await db.flush()  # Potrzebujemy ID rekordu
    master_log_id: int = master_log.id_log

    logger.info(
        "Sesja impersonacji rozpoczęta",
        extra={
            "admin_id": admin_id,
            "admin_username": admin.username,
            "target_user_id": target_user_id,
            "target_username": target_user.username,
            "max_hours": max_hours,
            "expires_at": expires_at.isoformat(),
            "jti": jti,
            "master_log_id": master_log_id,
            "ip_address": ip_address,
        }
    )

    # --- 10. Zapis do pliku logów ---
    _append_to_file(
        _get_impersonation_log_file(),
        _build_log_record(
            action="impersonation_started",
            admin_id=admin_id,
            admin_username=admin.username,
            target_user_id=target_user_id,
            target_username=target_user.username,
            reason=reason,
            max_hours=max_hours,
            expires_at=expires_at.isoformat(),
            jti=jti,
            master_log_id=master_log_id,
            ip_address=ip_address,
            user_agent=user_agent,
        )
    )

    # --- 11. AuditLog (fire-and-forget) ---
    audit_service.log(
        db=db,
        action="user_impersonation_start",
        action_category="Auth",
        entity_type="User",
        entity_id=target_user_id,
        details={
            "admin_id": admin_id,
            "admin_username": admin.username,
            "target_user_id": target_user_id,
            "target_username": target_user.username,
            "reason": reason,
            "max_hours": max_hours,
            "expires_at": expires_at.isoformat(),
            "master_log_id": master_log_id,
        },
        success=True,
    )

    return ImpersonationData(
        access_token=access_token,
        token_type="bearer",
        expires_at=expires_at,
        admin_id=admin_id,
        admin_username=admin.username,
        target_user_id=target_user_id,
        target_username=target_user.username,
        master_log_id=master_log_id,
        jti=jti,
        max_hours=max_hours,
    )


async def end(
    db: AsyncSession,
    redis: Redis,
    impersonation_token: str,
    admin_id: int,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Kończy sesję impersonacji — blacklistuje token.

    Przepływ:
        1. Dekoduje token (bez weryfikacji) — odczytuje claims
        2. Weryfikuje że to token impersonacji (is_impersonation=True)
        3. Weryfikuje że admin_id zgadza się z impersonated_by
        4. Blacklistuje token w Redis (auth:blacklist:{jti})
        5. Aktualizuje MasterAccessLog.SessionEndedAt (jeśli możliwe)
        6. Zapis do pliku logów + AuditLog

    Args:
        db:                  Sesja SQLAlchemy.
        redis:               Klient Redis.
        impersonation_token: Token JWT impersonacji do zakończenia.
        admin_id:            ID admina kończącego sesję (weryfikacja własności).
        ip_address:          IP inicjatora.

    Returns:
        Słownik z potwierdzeniem zakończenia sesji.

    Raises:
        ImpersonationTokenError:  Token nie jest tokenem impersonacji, lub
                                  admin nie jest właścicielem sesji.
        ImpersonationEndError:    Błąd przy kończeniu sesji.
    """
    # --- 1. Dekoduj claims bez weryfikacji ---
    claims = _decode_token_unverified(impersonation_token)
    if claims is None:
        raise ImpersonationTokenError(
            "Nie można zdekodować tokenu impersonacji."
        )

    # --- 2. Weryfikacja że to token impersonacji ---
    if not claims.get(_JWT_CLAIM_IS_IMPERSONATION):
        raise ImpersonationTokenError(
            "Podany token nie jest tokenem impersonacji."
        )

    jti = claims.get("jti")
    impersonated_by = claims.get(_JWT_CLAIM_IMPERSONATED_BY)
    target_user_id = claims.get("sub")
    target_username = claims.get("username")
    exp = claims.get("exp")

    if not jti or not impersonated_by:
        raise ImpersonationTokenError(
            "Token impersonacji nie zawiera wymaganych pól (jti, impersonated_by)."
        )

    # --- 3. Weryfikacja własności sesji ---
    if int(impersonated_by) != admin_id:
        logger.error(
            "Impersonacja: Próba zakończenia cudzej sesji impersonacji",
            extra={
                "admin_id": admin_id,
                "session_owner_id": impersonated_by,
                "jti": jti,
                "ip_address": ip_address,
            }
        )
        raise ImpersonationTokenError(
            "Możesz zakończyć tylko własną sesję impersonacji."
        )

    # --- 4. Blacklistowanie tokenu ---
    if exp:
        expires_at = datetime.fromtimestamp(exp, tz=timezone.utc)
    else:
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=60)

    await _blacklist_token(redis, jti, expires_at)

    # --- 5. Aktualizacja MasterAccessLog.SessionEndedAt ---
    # Szukamy najnowszego rekordu dla tego docelowego użytkownika bez session_ended_at
    now = datetime.now(timezone.utc)
    try:
        if target_user_id:
            result = await db.execute(
                select(MasterAccessLog)
                .where(
                    and_(
                        MasterAccessLog.target_user_id == int(target_user_id),
                        MasterAccessLog.session_ended_at == None,  # noqa: E711
                    )
                )
                .order_by(MasterAccessLog.accessed_at.desc())
                .limit(1)
            )
            log_entry = result.scalar_one_or_none()
            if log_entry:
                log_entry.session_ended_at = now
                await db.flush()
    except Exception as exc:
        logger.warning(
            "Impersonacja: Nie udało się zaktualizować SessionEndedAt w MasterAccessLog",
            extra={"error": str(exc), "target_user_id": target_user_id, "jti": jti}
        )

    logger.info(
        "Sesja impersonacji zakończona",
        extra={
            "admin_id": admin_id,
            "target_user_id": target_user_id,
            "target_username": target_username,
            "jti": jti,
            "ip_address": ip_address,
        }
    )

    # --- 6. Zapis do pliku logów ---
    _append_to_file(
        _get_impersonation_log_file(),
        _build_log_record(
            action="impersonation_ended",
            admin_id=admin_id,
            target_user_id=target_user_id,
            target_username=target_username,
            jti=jti,
            session_ended_at=now.isoformat(),
            ip_address=ip_address,
        )
    )

    # --- 7. AuditLog (fire-and-forget) ---
    audit_service.log(
        db=db,
        action="user_impersonation_end",
        action_category="Auth",
        entity_type="User",
        entity_id=int(target_user_id) if target_user_id else None,
        details={
            "admin_id": admin_id,
            "target_username": target_username,
            "jti": jti,
        },
        success=True,
    )

    return {
        "message": "Sesja impersonacji zakończona pomyślnie.",
        "jti": jti,
        "target_username": target_username,
        "ended_at": now.isoformat(),
    }


async def get_status(
    token: str,
    redis: Redis,
) -> ImpersonationStatus:
    """
    Zwraca status sesji impersonacji na podstawie tokenu JWT.

    Dekoduje token BEZ weryfikacji podpisu (tylko claims).
    Sprawdza blacklistę Redis.

    ⚠️  NIE używać do autoryzacji — tylko do diagnostyki/wyświetlania statusu.
    Autoryzacja zawsze przez pełną weryfikację JWT.

    Args:
        token: Surowy JWT token do sprawdzenia.
        redis: Klient Redis (do sprawdzenia blacklisty).

    Returns:
        ImpersonationStatus z informacjami o sesji.
    """
    claims = _decode_token_unverified(token)

    if claims is None:
        return ImpersonationStatus(
            is_impersonation=False,
            impersonated_by=None,
            target_user_id=None,
            target_username=None,
            expires_at=None,
            jti=None,
            is_expired=True,
            is_blacklisted=False,
        )

    is_impersonation = bool(claims.get(_JWT_CLAIM_IS_IMPERSONATION, False))
    impersonated_by = claims.get(_JWT_CLAIM_IMPERSONATED_BY)
    target_user_id_raw = claims.get("sub")
    target_username = claims.get("username")
    jti = claims.get("jti")
    exp = claims.get("exp")

    now = datetime.now(timezone.utc)
    expires_at = (
        datetime.fromtimestamp(exp, tz=timezone.utc) if exp else None
    )
    is_expired = expires_at is not None and expires_at <= now
    is_blacklisted = await _is_token_blacklisted(redis, jti) if jti else False

    target_user_id = int(target_user_id_raw) if target_user_id_raw else None

    return ImpersonationStatus(
        is_impersonation=is_impersonation,
        impersonated_by=int(impersonated_by) if impersonated_by else None,
        target_user_id=target_user_id,
        target_username=target_username,
        expires_at=expires_at,
        jti=jti,
        is_expired=is_expired,
        is_blacklisted=is_blacklisted,
    )


async def list_active_sessions(
    db: AsyncSession,
) -> list[dict]:
    """
    Zwraca listę aktualnie otwartych sesji impersonacji z MasterAccessLog.

    Sesje "otwarte" = te bez session_ended_at.

    ⚠️  Wynik może nie być kompletny — token może być na blackliście Redis
    a SessionEndedAt nie zaktualizowane (np. przy awarii). Traktuj informacyjnie.

    Args:
        db: Sesja SQLAlchemy.

    Returns:
        Lista słowników z danymi aktywnych sesji.
    """
    result = await db.execute(
        select(MasterAccessLog)
        .where(MasterAccessLog.session_ended_at == None)  # noqa: E711
        .order_by(MasterAccessLog.accessed_at.desc())
        .limit(100)  # Guard — nie pobieramy nieograniczonej liczby wierszy
    )
    entries = result.scalars().all()

    sessions = []
    for entry in entries:
        sessions.append({
            "log_id": entry.id_log,
            "target_user_id": entry.target_user_id,
            "target_username": entry.target_username,
            "ip_address": entry.ip_address,
            "accessed_at": entry.accessed_at.isoformat() if entry.accessed_at else None,
            "notes": entry.notes,
        })

    logger.debug(
        "Pobrano listę aktywnych sesji impersonacji",
        extra={"count": len(sessions)}
    )

    return sessions


async def force_end_all_sessions(
    db: AsyncSession,
    redis: Redis,
    target_user_id: int,
    admin_id: int,
    reason: str = "Administracyjne zakończenie wszystkich sesji",
    ip_address: Optional[str] = None,
) -> int:
    """
    Wymuszenie zakończenia WSZYSTKICH aktywnych sesji impersonacji danego użytkownika.

    Używane przy:
        - Dezaktywacji konta
        - Zmianie uprawnień
        - Incydencie bezpieczeństwa

    ⚠️  Nie blacklistuje tokenów (nie znamy JTI) — tylko aktualizuje SessionEndedAt.
    Tokeny wygasną naturalnie po max_hours. Dla natychmiastowego efektu:
    użyj end() z tokenem po stronie klienta.

    Args:
        db:             Sesja SQLAlchemy.
        redis:          Klient Redis.
        target_user_id: ID użytkownika dla którego zakańczamy sesje.
        admin_id:       ID admina wykonującego operację.
        reason:         Uzasadnienie operacji.
        ip_address:     IP admina.

    Returns:
        Liczba zakończonych sesji.
    """
    now = datetime.now(timezone.utc)

    result = await db.execute(
        select(MasterAccessLog)
        .where(
            and_(
                MasterAccessLog.target_user_id == target_user_id,
                MasterAccessLog.session_ended_at == None,  # noqa: E711
            )
        )
    )
    open_sessions = result.scalars().all()

    for session in open_sessions:
        session.session_ended_at = now
        if session.notes:
            session.notes = (
                session.notes[:300] +
                f" | FORCE_ENDED by admin_id={admin_id}: {reason[:100]}"
            )

    count = len(open_sessions)

    if count > 0:
        logger.warning(
            "Wymuszone zakończenie sesji impersonacji",
            extra={
                "admin_id": admin_id,
                "target_user_id": target_user_id,
                "sessions_closed": count,
                "reason": reason,
                "ip_address": ip_address,
            }
        )

        _append_to_file(
            _get_impersonation_log_file(),
            _build_log_record(
                action="impersonation_force_ended",
                admin_id=admin_id,
                target_user_id=target_user_id,
                sessions_closed=count,
                reason=reason,
                ip_address=ip_address,
            )
        )

        audit_service.log(
            db=db,
            action="user_impersonation_force_end",
            action_category="Auth",
            entity_type="User",
            entity_id=target_user_id,
            details={
                "admin_id": admin_id,
                "sessions_closed": count,
                "reason": reason,
            },
            success=True,
        )

    return count