"""
Serwis Użytkowników — System Windykacja
=========================================
Krok 8 / Faza 3 — services/user_service.py

Odpowiedzialność:
    - Pełny CRUD użytkowników (get_list, get_by_id, create, update, delete)
    - Dwuetapowe usuwanie z tokenem potwierdzającym (Redis, TTL z SystemConfig)
    - Blokowanie/odblokowywanie konta (lock/unlock)
    - Walidacja i sanityzacja danych wejściowych
    - Archiwizacja usuniętych użytkowników (JSON.gz)
    - Cache Redis dla pojedynczych użytkowników i list
    - Pełny AuditLog dla każdej mutacji (old_value → new_value JSON)
    - Plik logów users_YYYY-MM-DD.jsonl (append-only)

Decyzje projektowe:
    - Soft-delete (is_active=False), nigdy fizyczny DELETE z bazy
    - Archiwizacja: plik archives/YYYY-MM-DD/archive_users_{id}_{ts}.json.gz
    - Token potwierdzający DELETE: JWT (sub=user_id, type=delete_confirm, TTL z config)
    - argon2-cffi dla hasła przy tworzeniu/zmianie hasła przez admina
    - Zmiana hasła przez samego użytkownika: auth_service.change_password()
    - Cache Redis: users:{id} (TTL 300s), users:list:{hash} (TTL 60s)
    - Żadne hasła/tokeny NIE pojawiają się w logach

Zależności:
    - services/audit_service.py
    - services/config_service.py

Ścieżka docelowa: backend/app/services/user_service.py
Autor: System Windykacja — Faza 3 Krok 8
Wersja: 1.0.0
Data: 2026-02-18
"""

from __future__ import annotations

import gzip
import hashlib
import logging
import secrets
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import orjson
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from jose import jwt
from redis.asyncio import Redis
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.config import settings
from app.db.models.role import Role
from app.db.models.user import User
from app.services import audit_service
from app.services import config_service
from jose import JWTError, jwt

# ---------------------------------------------------------------------------
# Logger własny dla tego modułu
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Hasher argon2 — singleton (inicjalizowany raz, thread-safe)
# ---------------------------------------------------------------------------
_ph = PasswordHasher(
    time_cost=2,
    memory_cost=65536,  # 64 MB
    parallelism=2,
    hash_len=32,
    salt_len=16,
)

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------

# Domyślny TTL tokenu potwierdzającego DELETE w sekundach
_DEFAULT_DELETE_TOKEN_TTL: int = 60

# Domyślna paginacja
_DEFAULT_PAGE_SIZE: int = 50
_MAX_PAGE_SIZE: int = 200

# Minimalna długość hasła
_MIN_PASSWORD_LENGTH: int = 8

# TTL cache Redis (sekundy)
_CACHE_USER_TTL: int = 300    # 5 min dla pojedynczego użytkownika
_CACHE_LIST_TTL: int = 60     # 1 min dla list (szybciej się dezaktualizują)

# Klucze Redis
_REDIS_KEY_USER    = "user:{user_id}"
_REDIS_KEY_DELETE  = "delete_confirm:user:{token}"

# Dozwolone pola sortowania (whitelist — ochrona przed SQL injection)
_ALLOWED_SORT_FIELDS: frozenset[str] = frozenset({
    "id_user", "username", "email", "full_name",
    "created_at", "updated_at", "last_login_at",
})

# Pliki logów
_USERS_LOG_FILE_PATTERN = "logs/users_{date}.jsonl"

# Katalog archiwum
_ARCHIVE_BASE_DIR = "archives"


# ===========================================================================
# Dataclassy wejściowe / wyjściowe
# ===========================================================================

@dataclass(frozen=True)
class UserListParams:
    """
    Parametry filtrowania listy użytkowników.

    Wszystkie pola opcjonalne — pominięte = bez filtrowania.

    Attributes:
        search:         Wyszukiwanie pełnotekstowe (username, email, full_name).
        role_id:        Filtr po roli.
        is_active:      Filtr po statusie aktywności.
        is_locked:      Filtr po statusie blokady.
        page:           Numer strony (1-based).
        page_size:      Rozmiar strony (max _MAX_PAGE_SIZE).
        sort_by:        Pole sortowania (whitelist).
        sort_desc:      Sortowanie malejące.
    """
    search: Optional[str] = None
    role_id: Optional[int] = None
    is_active: Optional[bool] = None
    is_locked: Optional[bool] = None
    page: int = 1
    page_size: int = _DEFAULT_PAGE_SIZE
    sort_by: str = "created_at"
    sort_desc: bool = True

    def __post_init__(self) -> None:
        """Walidacja parametrów po inicjalizacji."""
        # Walidacja paginacji
        if self.page < 1:
            object.__setattr__(self, "page", 1)
        if self.page_size < 1:
            object.__setattr__(self, "page_size", 1)
        if self.page_size > _MAX_PAGE_SIZE:
            object.__setattr__(self, "page_size", _MAX_PAGE_SIZE)

        # Walidacja sort_by (whitelist)
        if self.sort_by not in _ALLOWED_SORT_FIELDS:
            object.__setattr__(self, "sort_by", "created_at")

        # Sanityzacja search (NFC + strip)
        if self.search is not None:
            sanitized = unicodedata.normalize("NFC", self.search.strip())
            object.__setattr__(self, "search", sanitized if sanitized else None)


@dataclass(frozen=True)
class UserCreateData:
    """
    Dane do tworzenia nowego użytkownika.

    Attributes:
        username:  Nazwa użytkownika (unique, 3-50 znaków, tylko alfanumeryczne + ._-).
        email:     Adres email (unique, walidowany).
        password:  Hasło w plaintexcie (zostanie zahashowane argon2).
        full_name: Pełne imię i nazwisko (opcjonalne).
        role_id:   ID roli użytkownika (wymagane).
    """
    username: str
    email: str
    password: str
    role_id: int
    full_name: Optional[str] = None

    def __post_init__(self) -> None:
        """Walidacja i sanityzacja danych tworzenia użytkownika."""
        # Username
        username = unicodedata.normalize("NFC", self.username.strip())
        if len(username) < 3 or len(username) > 50:
            raise UserValidationError(
                "Nazwa użytkownika musi mieć od 3 do 50 znaków."
            )
        import re
        if not re.match(r"^[a-zA-Z0-9._-]+$", username):
            raise UserValidationError(
                "Nazwa użytkownika może zawierać tylko litery, cyfry i znaki: . _ -"
            )
        object.__setattr__(self, "username", username.lower())

        # Email
        email = unicodedata.normalize("NFC", self.email.strip().lower())
        if not _validate_email_format(email):
            raise UserValidationError(f"Nieprawidłowy format adresu email: {email!r}")
        object.__setattr__(self, "email", email)

        # Hasło
        if len(self.password) < _MIN_PASSWORD_LENGTH:
            raise UserValidationError(
                f"Hasło musi mieć co najmniej {_MIN_PASSWORD_LENGTH} znaków."
            )

        # Full name
        if self.full_name is not None:
            full_name = unicodedata.normalize("NFC", self.full_name.strip())
            object.__setattr__(self, "full_name", full_name[:100] if full_name else None)

        # role_id
        if self.role_id < 1:
            raise UserValidationError("ID roli musi być liczbą dodatnią.")


@dataclass
class UserUpdateData:
    """
    Dane do aktualizacji użytkownika.

    Wszystkie pola opcjonalne — None = brak zmiany tego pola.

    Attributes:
        email:     Nowy adres email.
        full_name: Nowe pełne imię.
        role_id:   Nowe ID roli.
        password:  Nowe hasło (tylko admin może zmienić cudze hasło).
    """
    email: Optional[str] = None
    full_name: Optional[str] = None
    role_id: Optional[int] = None
    password: Optional[str] = None

    def __post_init__(self) -> None:
        """Sanityzacja i walidacja pól aktualizacji."""
        if self.email is not None:
            email = unicodedata.normalize("NFC", self.email.strip().lower())
            if not _validate_email_format(email):
                raise UserValidationError(
                    f"Nieprawidłowy format adresu email: {email!r}"
                )
            self.email = email

        if self.full_name is not None:
            full_name = unicodedata.normalize("NFC", self.full_name.strip())
            self.full_name = full_name[:100] if full_name else None

        if self.role_id is not None and self.role_id < 1:
            raise UserValidationError("ID roli musi być liczbą dodatnią.")

        if self.password is not None and len(self.password) < _MIN_PASSWORD_LENGTH:
            raise UserValidationError(
                f"Hasło musi mieć co najmniej {_MIN_PASSWORD_LENGTH} znaków."
            )

    def has_changes(self) -> bool:
        """Sprawdza czy jakiekolwiek pole ma wartość (coś do zmiany)."""
        return any([
            self.email is not None,
            self.full_name is not None,
            self.role_id is not None,
            self.password is not None,
        ])


@dataclass(frozen=True)
class PaginatedUsers:
    """
    Wynik paginowanego zapytania użytkowników.

    Attributes:
        items:       Lista użytkowników (słowniki).
        total:       Całkowita liczba rekordów (przed paginacją).
        page:        Aktualny numer strony.
        page_size:   Rozmiar strony.
        total_pages: Całkowita liczba stron.
    """
    items: list[dict]
    total: int
    page: int
    page_size: int
    total_pages: int


@dataclass(frozen=True)
class DeleteConfirmData:
    """
    Dane tokenu potwierdzającego DELETE użytkownika.

    Attributes:
        token:       JWT token potwierdzający (do wysłania do klienta).
        expires_in:  Czas ważności w sekundach.
        action:      Opis akcji ("delete_user").
        user_id:     ID użytkownika do usunięcia.
        username:    Nazwa użytkownika.
    """
    token: str
    expires_in: int
    action: str
    user_id: int
    username: str


# ===========================================================================
# Klasy wyjątków
# ===========================================================================

class UserError(Exception):
    """Bazowy wyjątek serwisu użytkowników."""


class UserValidationError(UserError):
    """Błąd walidacji danych wejściowych."""


class UserNotFoundError(UserError):
    """Użytkownik nie istnieje."""


class UserAlreadyExistsError(UserError):
    """Użytkownik o podanym username lub email już istnieje."""

    def __init__(self, field: str, value: str) -> None:
        self.field = field
        self.value = value
        super().__init__(f"Użytkownik z {field}={value!r} już istnieje.")


class UserRoleNotFoundError(UserError):
    """Podana rola nie istnieje lub jest nieaktywna."""


class UserDeleteTokenError(UserError):
    """Błąd tokenu potwierdzającego DELETE (nieprawidłowy, wygasły lub już użyty)."""


class UserSelfDeleteError(UserError):
    """Próba usunięcia własnego konta (zabronione)."""


class UserLockError(UserError):
    """Błąd podczas blokowania/odblokowywania konta."""


# ===========================================================================
# Funkcje pomocnicze — prywatne
# ===========================================================================

def _validate_email_format(email: str) -> bool:
    """
    Prosta walidacja formatu email — nie wysyła żadnych zapytań.

    Sprawdza podstawową strukturę: coś @ coś . coś
    Pełna walidacja przez EmailStr Pydantic w warstwie schemas.

    Args:
        email: Adres email do walidacji.

    Returns:
        True jeśli format poprawny, False w przeciwnym razie.
    """
    import re
    pattern = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email)) and len(email) <= 100


def _get_log_dir() -> Path:
    """Zwraca i tworzy katalog logów."""
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def _get_archive_dir() -> Path:
    """Zwraca i tworzy dzienny katalog archiwum."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    archive_dir = Path(_ARCHIVE_BASE_DIR) / today
    archive_dir.mkdir(parents=True, exist_ok=True)
    return archive_dir


def _get_users_log_file() -> Path:
    """Zwraca dzienną ścieżkę pliku logów użytkowników."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return _get_log_dir() / f"users_{today}.jsonl"


def _append_to_file(filepath: Path, record: dict) -> None:
    """Dopisuje rekord JSON do pliku JSON Lines (append-only)."""
    try:
        line = orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE)
        with filepath.open("ab") as f:
            f.write(line)
    except OSError as exc:
        logger.warning(
            "Nie można zapisać do pliku logu użytkowników",
            extra={"filepath": str(filepath), "error": str(exc)}
        )


def _build_log_record(action: str, **kwargs) -> dict:
    """Buduje ustrukturyzowany rekord logu."""
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": "user_service",
        "action": action,
        **kwargs,
    }


def _user_to_dict(user: User, include_sensitive: bool = False) -> dict:
    """
    Konwertuje obiekt User na słownik bezpieczny do logowania/zwracania.

    Args:
        user:              Obiekt SQLAlchemy User.
        include_sensitive: Jeśli False (domyślnie) — wyklucza PasswordHash.

    Returns:
        Słownik z danymi użytkownika.
    """
    data = {
        "id_user": user.id_user,
        "username": user.username,
        "email": user.email,
        "full_name": user.full_name,
        "role_id": user.role_id,
        "is_active": user.is_active,
        "failed_login_attempts": user.failed_login_attempts,
        "locked_until": user.locked_until.isoformat() if user.locked_until else None,
        "last_login_at": user.last_login_at.isoformat() if user.last_login_at else None,
        "created_at": user.created_at.isoformat() if user.created_at else None,
        "updated_at": user.updated_at.isoformat() if user.updated_at else None,
    }
    if include_sensitive:
        # NIGDY nie logujemy PasswordHash — nawet w trybie sensitive
        # (tylko do celów auditowych — np. stary hash przy zmianie hasła)
        data["password_hash_redacted"] = "[REDACTED]"
    return data


def _user_to_safe_dict(user: User) -> dict:
    """
    Konwertuje User na słownik bezpieczny do odpowiedzi API.

    Wyklucza PasswordHash, FailedLoginAttempts, LockedUntil (dla zwykłych userów).
    Pełne dane zwracane przez endpointy admin users.view_details.

    Args:
        user: Obiekt SQLAlchemy User.

    Returns:
        Słownik z bezpiecznymi danymi użytkownika.
    """
    return {
        "id_user": user.id_user,
        "username": user.username,
        "email": user.email,
        "full_name": user.full_name,
        "role_id": user.role_id,
        "role_name": user.role.role_name if user.role else None,
        "is_active": user.is_active,
        "is_locked": _is_user_locked(user),
        "failed_login_attempts": user.failed_login_attempts,
        "locked_until": user.locked_until.isoformat() if user.locked_until else None,
        "last_login_at": user.last_login_at.isoformat() if user.last_login_at else None,
        "created_at": user.created_at.isoformat() if user.created_at else None,
        "updated_at": user.updated_at.isoformat() if user.updated_at else None,
    }


def _is_user_locked(user: User) -> bool:
    """
    Sprawdza czy konto użytkownika jest aktualnie zablokowane.

    Blokada jest aktywna jeśli locked_until > now().

    Args:
        user: Obiekt User.

    Returns:
        True jeśli konto zablokowane.
    """
    if user.locked_until is None:
        return False
    # Porównujemy z timezone.utc — locked_until w bazie jako UTC
    now = datetime.now(timezone.utc)
    locked_until = user.locked_until
    if locked_until.tzinfo is None:
        locked_until = locked_until.replace(tzinfo=timezone.utc)
    return locked_until > now


async def _get_redis_cache(redis: Redis, key: str) -> Optional[dict]:
    """Pobiera dane z Redis cache (JSON). Zwraca None przy braku/błędzie."""
    try:
        raw = await redis.get(key)
        if raw:
            return orjson.loads(raw)
    except Exception as exc:
        logger.debug("Cache miss (Redis error)", extra={"key": key, "error": str(exc)})
    return None


async def _set_redis_cache(redis: Redis, key: str, data: dict, ttl: int) -> None:
    """Zapisuje dane do Redis cache (JSON). Błędy logowane jako DEBUG."""
    try:
        await redis.set(key, orjson.dumps(data), ex=ttl)
    except Exception as exc:
        logger.debug("Błąd zapisu do cache", extra={"key": key, "error": str(exc)})


async def _invalidate_user_cache(redis: Redis, user_id: int) -> None:
    """Inwaliduje cache dla danego użytkownika."""
    try:
        await redis.delete(_REDIS_KEY_USER.format(user_id=user_id))
    except Exception:
        pass  # Cache miss — nie krytyczne


def _archive_user(user: User) -> Optional[Path]:
    """
    Archiwizuje dane użytkownika do pliku JSON.gz przed soft-delete.

    Format pliku: archives/YYYY-MM-DD/archive_users_{id}_{ts}.json.gz

    Args:
        user: Obiekt User do archiwizacji.

    Returns:
        Ścieżka do pliku archiwum lub None przy błędzie.
    """
    now = datetime.now(timezone.utc)
    ts_str = now.strftime("%Y%m%d_%H%M%S")
    filename = f"archive_users_{user.id_user}_{ts_str}.json.gz"
    filepath = _get_archive_dir() / filename

    archive_data = {
        "archived_at": now.isoformat(),
        "archive_type": "soft_delete",
        "table": "dbo_ext.Users",
        "record": _user_to_dict(user, include_sensitive=False),
    }

    try:
        with gzip.open(filepath, "wb") as f:
            f.write(orjson.dumps(archive_data, option=orjson.OPT_INDENT_2))
        logger.info(
            "Zarchiwizowano dane użytkownika",
            extra={"user_id": user.id_user, "archive_path": str(filepath)}
        )
        return filepath
    except OSError as exc:
        logger.error(
            "Nie udało się zarchiwizować danych użytkownika",
            extra={"user_id": user.id_user, "error": str(exc)}
        )
        return None


async def _get_role_by_id(db: AsyncSession, role_id: int) -> Optional[Role]:
    """Pobiera aktywną rolę po ID."""
    result = await db.execute(
        select(Role).where(
            and_(Role.id_role == role_id, Role.is_active == True)  # noqa: E712
        )
    )
    return result.scalar_one_or_none()


async def _check_unique_username(db: AsyncSession, username: str, exclude_id: Optional[int] = None) -> None:
    """
    Sprawdza unikalność username w bazie.

    Args:
        db:         Sesja SQLAlchemy.
        username:   Nazwa do sprawdzenia.
        exclude_id: ID użytkownika do wykluczenia (przy UPDATE — własne konto).

    Raises:
        UserAlreadyExistsError: Gdy username jest zajęty.
    """
    # IsActive = 1 — soft-deleted userzy NIE blokują ponownego użycia username
    conditions = [User.username == username, User.is_active == True]
    if exclude_id:
        from sqlalchemy import not_
        conditions.append(not_(User.id_user == exclude_id))

    result = await db.execute(
        select(func.count(User.id_user)).where(and_(*conditions))
    )
    count = result.scalar_one()
    if count > 0:
        raise UserAlreadyExistsError("username", username)

async def _check_unique_email(db: AsyncSession, email: str, exclude_id: Optional[int] = None) -> None:
    """
    Sprawdza unikalność emaila w bazie.

    Args:
        db:         Sesja SQLAlchemy.
        email:      Email do sprawdzenia.
        exclude_id: ID użytkownika do wykluczenia (przy UPDATE).

    Raises:
        UserAlreadyExistsError: Gdy email jest zajęty.
    """
    # IsActive = 1 — soft-deleted userzy NIE blokują ponownego użycia emaila
    conditions = [User.email == email, User.is_active == True]
    if exclude_id:
        from sqlalchemy import not_
        conditions.append(not_(User.id_user == exclude_id))

    result = await db.execute(
        select(func.count(User.id_user)).where(and_(*conditions))
    )
    count = result.scalar_one()
    if count > 0:
        raise UserAlreadyExistsError("email", email)


# ===========================================================================
# Publiczne API serwisu — READ
# ===========================================================================

async def get_list(
    db: AsyncSession,
    redis: Redis,
    params: UserListParams,
) -> PaginatedUsers:
    """
    Pobiera paginowaną listę użytkowników z opcjonalnym filtrowaniem.

    Przepływ:
        1. Sprawdzenie cache Redis (key = hash parametrów)
        2. Budowanie zapytania SQLAlchemy z filtrami
        3. Równoległe zapytania: COUNT + dane
        4. Zapis wyniku do cache Redis

    Args:
        db:     Sesja SQLAlchemy.
        redis:  Klient Redis.
        params: Parametry filtrowania i paginacji.

    Returns:
        PaginatedUsers z listą użytkowników i metadanymi paginacji.
    """
    # Cache key = hash parametrów
    params_json = orjson.dumps({
        "search": params.search,
        "role_id": params.role_id,
        "is_active": params.is_active,
        "is_locked": params.is_locked,
        "page": params.page,
        "page_size": params.page_size,
        "sort_by": params.sort_by,
        "sort_desc": params.sort_desc,
    })
    cache_key = f"users:list:{hashlib.md5(params_json).hexdigest()}"

    cached = await _get_redis_cache(redis, cache_key)
    if cached:
        logger.debug("Lista użytkowników pobrana z cache", extra={"cache_key": cache_key})
        return PaginatedUsers(**cached)

    # Budowanie warunków
    conditions = []
    if params.is_active is not None:
        conditions.append(User.is_active == params.is_active)

    if params.is_locked is not None:
        now = datetime.now(timezone.utc)
        if params.is_locked:
            conditions.append(User.locked_until > now)
        else:
            conditions.append(
                or_(User.locked_until == None, User.locked_until <= now)  # noqa: E711
            )

    if params.role_id is not None:
        conditions.append(User.role_id == params.role_id)

    if params.search:
        search_term = f"%{params.search}%"
        conditions.append(
            or_(
                User.username.ilike(search_term),
                User.email.ilike(search_term),
                User.full_name.ilike(search_term),
            )
        )

    # Zapytanie COUNT
    count_query = select(func.count(User.id_user))
    if conditions:
        count_query = count_query.where(and_(*conditions))

    count_result = await db.execute(count_query)
    total = count_result.scalar_one() or 0

    if total == 0:
        return PaginatedUsers(
            items=[], total=0, page=params.page,
            page_size=params.page_size, total_pages=0,
        )

    # Zapytanie danych
    data_query = (
        select(User)
        .options(selectinload(User.role))
        .offset((params.page - 1) * params.page_size)
        .limit(params.page_size)
    )
    if conditions:
        data_query = data_query.where(and_(*conditions))

    # Sortowanie
    sort_column = getattr(User, params.sort_by, User.created_at)
    if params.sort_desc:
        data_query = data_query.order_by(sort_column.desc())
    else:
        data_query = data_query.order_by(sort_column.asc())

    data_result = await db.execute(data_query)
    users = data_result.scalars().all()

    total_pages = (total + params.page_size - 1) // params.page_size
    items = [_user_to_safe_dict(u) for u in users]

    result = PaginatedUsers(
        items=items,
        total=total,
        page=params.page,
        page_size=params.page_size,
        total_pages=total_pages,
    )

    # Zapis do cache
    await _set_redis_cache(redis, cache_key, {
        "items": items,
        "total": total,
        "page": params.page,
        "page_size": params.page_size,
        "total_pages": total_pages,
    }, _CACHE_LIST_TTL)

    logger.debug(
        "Lista użytkowników pobrana z bazy",
        extra={"total": total, "page": params.page, "returned": len(items)}
    )

    return result


async def get_by_id(
    db: AsyncSession,
    redis: Redis,
    user_id: int,
) -> dict:
    """
    Pobiera szczegóły użytkownika po ID.

    Sprawdza cache Redis → jeśli miss → baza danych.
    Cache TTL: _CACHE_USER_TTL (300s).

    Args:
        db:      Sesja SQLAlchemy.
        redis:   Klient Redis.
        user_id: ID użytkownika.

    Returns:
        Słownik z danymi użytkownika.

    Raises:
        UserNotFoundError: Gdy użytkownik nie istnieje.
    """
    cache_key = _REDIS_KEY_USER.format(user_id=user_id)
    cached = await _get_redis_cache(redis, cache_key)
    if cached:
        logger.debug("Użytkownik pobrany z cache", extra={"user_id": user_id})
        return cached

    result = await db.execute(
        select(User)
        .options(selectinload(User.role))
        .where(User.id_user == user_id)
    )
    user = result.scalar_one_or_none()

    if user is None:
        raise UserNotFoundError(f"Użytkownik ID={user_id} nie istnieje.")

    data = _user_to_safe_dict(user)
    await _set_redis_cache(redis, cache_key, data, _CACHE_USER_TTL)

    return data


# ===========================================================================
# Publiczne API serwisu — WRITE (CREATE / UPDATE / DELETE)
# ===========================================================================

async def create(
    db: AsyncSession,
    redis: Redis,
    data: UserCreateData,
    created_by_user_id: Optional[int] = None,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Tworzy nowego użytkownika.

    Przepływ:
        1. Walidacja unikalności username i email
        2. Weryfikacja istnienia roli
        3. Hashowanie hasła (argon2)
        4. Zapis do bazy danych
        5. Inwalidacja cache list (users:list:*)
        6. Zapis do pliku logów
        7. AuditLog (fire-and-forget)

    Args:
        db:                 Sesja SQLAlchemy.
        redis:              Klient Redis.
        data:               Zwalidowane dane nowego użytkownika.
        created_by_user_id: ID admina tworzącego użytkownika (do AuditLog).
        ip_address:         IP inicjatora.

    Returns:
        Słownik z danymi nowo utworzonego użytkownika.

    Raises:
        UserAlreadyExistsError: Gdy username lub email jest zajęty.
        UserRoleNotFoundError:  Gdy podana rola nie istnieje.
        UserValidationError:    Gdy dane nie spełniają wymagań.
    """
    # Walidacja unikalności
    await _check_unique_username(db, data.username)
    await _check_unique_email(db, data.email)

    # Weryfikacja roli
    role = await _get_role_by_id(db, data.role_id)
    if role is None:
        raise UserRoleNotFoundError(
            f"Rola ID={data.role_id} nie istnieje lub jest nieaktywna."
        )

    # Hashowanie hasła
    raw_password = data.password.get_secret_value() if hasattr(data.password, 'get_secret_value') else data.password
    password_hash = _ph.hash(raw_password)

    # Tworzenie użytkownika
    new_user = User(
        username=data.username,
        email=data.email,
        password_hash=password_hash,
        full_name=data.full_name,
        role_id=data.role_id,
        is_active=True,
        failed_login_attempts=0,
    )
    db.add(new_user)
    await db.flush()  # Potrzebujemy ID
    await db.commit()      

    # Pobierz nowy stan roli z uprawnieniami
    await db.refresh(new_user)
    user_id = new_user.id_user

    # Inwalidacja cache list (klucze prefixowane users:list:*)
    try:
        async for key in redis.scan_iter("users:list:*"):
            await redis.delete(key)
    except Exception:
        pass

    user_data = _user_to_safe_dict(new_user)
    # Ładujemy rolę do odpowiedzi
    user_data["role_name"] = role.role_name

    logger.info(
        "Utworzono nowego użytkownika",
        extra={
            "new_user_id": user_id,
            "username": data.username,
            "email": data.email,
            "role_id": data.role_id,
            "created_by": created_by_user_id,
            "ip_address": ip_address,
        }
    )

    _append_to_file(
        _get_users_log_file(),
        _build_log_record(
            action="user_created",
            new_user_id=user_id,
            username=data.username,
            email=data.email,
            role_id=data.role_id,
            role_name=role.role_name,
            created_by=created_by_user_id,
            ip_address=ip_address,
        )
    )

    # AuditLog (fire-and-forget)
    audit_service.log_crud(
        db=db,
        action="user_created",
        entity_type="User",
        entity_id=user_id,
        new_value={
            "id_user": user_id,
            "username": data.username,
            "email": data.email,
            "role_id": data.role_id,
            "full_name": data.full_name,
            # password_hash — NIGDY w AuditLog
        },
        success=True,
    )

    return user_data


async def update(
    db: AsyncSession,
    redis: Redis,
    user_id: int,
    data: UserUpdateData,
    updated_by_user_id: Optional[int] = None,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Aktualizuje dane użytkownika.

    Przepływ:
        1. Pobranie aktualnych danych (old_value do AuditLog)
        2. Walidacja unikalności email (jeśli zmieniony)
        3. Walidacja roli (jeśli zmieniona)
        4. Aktualizacja pól (tylko tych podanych — partial update)
        5. Zapis do bazy
        6. Inwalidacja cache (user:{id} + users:list:*)
        7. Zapis do pliku logów (z diff old→new)
        8. AuditLog z old_value i new_value

    Args:
        db:                 Sesja SQLAlchemy.
        redis:              Klient Redis.
        user_id:            ID użytkownika do aktualizacji.
        data:               Zwalidowane dane aktualizacji.
        updated_by_user_id: ID admina wykonującego operację (do AuditLog).
        ip_address:         IP inicjatora.

    Returns:
        Słownik z zaktualizowanymi danymi użytkownika.

    Raises:
        UserNotFoundError:      Gdy użytkownik nie istnieje.
        UserAlreadyExistsError: Gdy nowy email jest już zajęty.
        UserRoleNotFoundError:  Gdy podana rola nie istnieje.
        UserValidationError:    Gdy brak zmian do zapisania.
    """
    if not data.has_changes():
        raise UserValidationError("Brak danych do aktualizacji.")

    # Pobierz aktualnego użytkownika
    result = await db.execute(
        select(User)
        .options(selectinload(User.role))
        .where(User.id_user == user_id)
    )
    user = result.scalar_one_or_none()

    if user is None:
        raise UserNotFoundError(f"Użytkownik ID={user_id} nie istnieje.")

    # Zapis stanu PRZED zmianą (do AuditLog old_value)
    old_value = _user_to_dict(user)

    # Walidacja i aplikowanie zmian
    changed_fields: dict[str, Any] = {}

    if data.email is not None and data.email != user.email:
        await _check_unique_email(db, data.email, exclude_id=user_id)
        user.email = data.email
        changed_fields["email"] = data.email

    if data.full_name is not None and data.full_name != user.full_name:
        user.full_name = data.full_name
        changed_fields["full_name"] = data.full_name

    if data.role_id is not None and data.role_id != user.role_id:
        role = await _get_role_by_id(db, data.role_id)
        if role is None:
            raise UserRoleNotFoundError(
                f"Rola ID={data.role_id} nie istnieje lub jest nieaktywna."
            )
        user.role_id = data.role_id
        changed_fields["role_id"] = data.role_id

    if data.password is not None:
        user.password_hash = _ph.hash(data.password)
        changed_fields["password"] = "[CHANGED — hash nie logowany]"

    if not changed_fields:
        raise UserValidationError("Brak rzeczywistych zmian do zapisania.")

    user.updated_at = datetime.now(timezone.utc)
    await db.flush()

    # Inwalidacja cache
    await _invalidate_user_cache(redis, user_id)
    try:
        async for key in redis.scan_iter("users:list:*"):
            await redis.delete(key)
    except Exception:
        pass

    new_value = _user_to_dict(user)

    logger.info(
        "Zaktualizowano użytkownika",
        extra={
            "user_id": user_id,
            "username": user.username,
            "changed_fields": list(changed_fields.keys()),
            "updated_by": updated_by_user_id,
            "ip_address": ip_address,
        }
    )

    _append_to_file(
        _get_users_log_file(),
        _build_log_record(
            action="user_updated",
            user_id=user_id,
            username=user.username,
            changed_fields=list(changed_fields.keys()),
            updated_by=updated_by_user_id,
            ip_address=ip_address,
        )
    )

    # AuditLog z old_value i new_value (fire-and-forget)
    audit_service.log_crud(
        db=db,
        action="user_updated",
        entity_type="User",
        entity_id=user_id,
        old_value=old_value,
        new_value=new_value,
        details={"changed_fields": list(changed_fields.keys())},
        success=True,
    )

    # Odczytaj zaktualizowanego użytkownika z rolą
    result = await db.execute(
        select(User).options(selectinload(User.role)).where(User.id_user == user_id)
    )
    updated_user = result.scalar_one()
    return _user_to_safe_dict(updated_user)


# ===========================================================================
# Dwuetapowe usuwanie
# ===========================================================================

async def initiate_delete(
    db: AsyncSession,
    redis: Redis,
    user_id: int,
    initiating_user_id: int,
    ip_address: Optional[str] = None,
) -> DeleteConfirmData:
    """
    Inicjuje dwuetapowe usunięcie użytkownika — Krok 1.

    Zwraca token potwierdzający (JWT) z krótkim TTL (z SystemConfig).
    Klient musi wysłać ten token do DELETE /users/{id}/confirm.

    ⚠️  Zakaz usuwania własnego konta.

    Przepływ:
        1. Weryfikacja że użytkownik istnieje
        2. Zakaz samo-usunięcia
        3. TTL z SystemConfig("delete_token.ttl_seconds", default=60)
        4. Generowanie JWT tokenu potwierdzającego
        5. Zapis tokenu do Redis (klucz: delete_confirm:user:{token})
        6. AuditLog (fire-and-forget) + log do pliku

    Args:
        db:                   Sesja SQLAlchemy.
        redis:                Klient Redis.
        user_id:              ID użytkownika do usunięcia.
        initiating_user_id:   ID admina inicjującego delete.
        ip_address:           IP inicjatora.

    Returns:
        DeleteConfirmData z tokenem i metadanymi.

    Raises:
        UserNotFoundError:   Gdy użytkownik nie istnieje.
        UserSelfDeleteError: Gdy admin próbuje usunąć własne konto.
    """
    # Weryfikacja użytkownika
    result = await db.execute(
        select(User).where(User.id_user == user_id)
    )
    user = result.scalar_one_or_none()

    if user is None:
        raise UserNotFoundError(f"Użytkownik ID={user_id} nie istnieje.")

    # Zakaz samo-usunięcia
    if user_id == initiating_user_id:
        raise UserSelfDeleteError(
            "Nie można usunąć własnego konta. Skontaktuj się z innym administratorem."
        )

    # TTL z konfiguracji
    ttl_seconds = await config_service.get_int(
        db, redis,
        key="delete_token.ttl_seconds",
        default=_DEFAULT_DELETE_TOKEN_TTL,
    )

    # Generowanie tokenu
    now = datetime.now(timezone.utc)
    jti = secrets.token_hex(16)
    expires_at = now + timedelta(seconds=ttl_seconds)

    token_payload = {
        "sub": str(user_id),
        "type": "delete_confirm",
        "action": "delete_user",
        "initiated_by": initiating_user_id,
        "jti": jti,
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
    }

    secret = settings.secret_key.get_secret_value() if hasattr(settings.secret_key, 'get_secret_value') else settings.secret_key
    delete_token = jwt.encode(
        token_payload,
        secret,
        algorithm=settings.algorithm,
    )

    # Zapis do Redis (klucz ważny przez ttl_seconds)
    redis_key = _REDIS_KEY_DELETE.format(token=jti)
    await redis.set(redis_key, str(user_id), ex=ttl_seconds)

    logger.info(
        "Zainicjowano usunięcie użytkownika — krok 1",
        extra={
            "user_id": user_id,
            "username": user.username,
            "initiated_by": initiating_user_id,
            "ttl_seconds": ttl_seconds,
            "expires_at": expires_at.isoformat(),
            "ip_address": ip_address,
        }
    )

    _append_to_file(
        _get_users_log_file(),
        _build_log_record(
            action="user_delete_initiated",
            user_id=user_id,
            username=user.username,
            initiated_by=initiating_user_id,
            ttl_seconds=ttl_seconds,
            expires_at=expires_at.isoformat(),
            ip_address=ip_address,
        )
    )

    audit_service.log_crud(
        db=db,
        action="user_delete_initiated",
        entity_type="User",
        entity_id=user_id,
        details={
            "username": user.username,
            "initiated_by": initiating_user_id,
            "ttl_seconds": ttl_seconds,
        },
        success=True,
    )

    return DeleteConfirmData(
        token=delete_token,
        expires_in=ttl_seconds,
        action="delete_user",
        user_id=user_id,
        username=user.username,
    )


async def confirm_delete(
    db: AsyncSession,
    redis: Redis,
    user_id: int,
    confirm_token: str,
    initiating_user_id: int,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Potwierdza i wykonuje soft-delete użytkownika — Krok 2.

    Weryfikuje token JWT, sprawdza Redis, archiwizuje dane, wykonuje soft-delete.

    ⚠️  Soft-delete = is_active=False. Dane NIE są kasowane z bazy.
    Archiwum: archives/YYYY-MM-DD/archive_users_{id}_{ts}.json.gz

    Przepływ:
        1. Dekodowanie i weryfikacja tokenu JWT
        2. Sprawdzenie tokenu w Redis (jednorazowość)
        3. Weryfikacja że token.sub == user_id (ochrona przed podmienionym ID)
        4. Weryfikacja że initiated_by == initiating_user_id
        5. Archiwizacja danych użytkownika (JSON.gz)
        6. Soft-delete (is_active=False)
        7. Unieważnienie tokenów Redis użytkownika (cache, refresh tokens)
        8. Inwalidacja cache Redis
        9. Zapis do pliku logów + AuditLog

    Args:
        db:                   Sesja SQLAlchemy.
        redis:                Klient Redis.
        user_id:              ID użytkownika do usunięcia.
        confirm_token:        Token JWT z initiate_delete().
        initiating_user_id:   ID admina potwierdzającego (musi być ten sam co inicjował).
        ip_address:           IP inicjatora.

    Returns:
        Słownik z potwierdzeniem wykonanego usunięcia.

    Raises:
        UserDeleteTokenError: Gdy token jest nieprawidłowy, wygasły lub już użyty.
        UserNotFoundError:    Gdy użytkownik nie istnieje.
    """
    # --- 1. Dekodowanie i weryfikacja tokenu JWT ---
    try:
        secret = settings.secret_key.get_secret_value() if hasattr(settings.secret_key, 'get_secret_value') else settings.secret_key
        payload = jwt.decode(
            confirm_token,
            secret,
            algorithms=[settings.algorithm],
        )
    except JWTError as exc:
        logger.warning(
            "Delete confirm: Nieprawidłowy token JWT",
            extra={"user_id": user_id, "error": str(exc), "ip_address": ip_address}
        )
        raise UserDeleteTokenError("Token potwierdzający jest nieprawidłowy lub wygasł.")

    # --- 2. Weryfikacja pól tokenu ---
    token_type = payload.get("type")
    token_sub = payload.get("sub")
    token_action = payload.get("action")
    token_jti = payload.get("jti")
    token_initiated_by = payload.get("initiated_by")

    if token_type != "delete_confirm" or token_action != "delete_user":
        raise UserDeleteTokenError("Token nie jest tokenem potwierdzającym usunięcia.")

    if token_sub is None or int(token_sub) != user_id:
        raise UserDeleteTokenError(
            "Token dotyczy innego użytkownika niż podany user_id."
        )

    if token_initiated_by is None or int(token_initiated_by) != initiating_user_id:
        raise UserDeleteTokenError(
            "Token był wygenerowany przez innego administratora."
        )

    # --- 3. Sprawdzenie Redis (jednorazowość) ---
    redis_key = _REDIS_KEY_DELETE.format(token=token_jti)
    stored_user_id = await redis.get(redis_key)

    if stored_user_id is None:
        raise UserDeleteTokenError(
            "Token potwierdzający wygasł lub został już użyty."
        )

    # Usuń token z Redis (jednorazowe użycie)
    await redis.delete(redis_key)

    # --- 4. Pobranie użytkownika ---
    result = await db.execute(
        select(User).options(selectinload(User.role)).where(User.id_user == user_id)
    )
    user = result.scalar_one_or_none()

    if user is None:
        raise UserNotFoundError(f"Użytkownik ID={user_id} nie istnieje.")

    # --- 5. Archiwizacja ---
    archive_path = _archive_user(user)

    # --- 6. Soft-delete ---
    old_value = _user_to_dict(user)
    user.is_active = False
    user.updated_at = datetime.now(timezone.utc)
    await db.flush()
    await db.commit()

    # --- 7. Inwalidacja cache ---
    await _invalidate_user_cache(redis, user_id)
    try:
        async for key in redis.scan_iter("users:list:*"):
            await redis.delete(key)
    except Exception:
        pass

    logger.warning(
        "Użytkownik usunięty (soft-delete)",
        extra={
            "user_id": user_id,
            "username": user.username,
            "email": user.email,
            "deleted_by": initiating_user_id,
            "archive_path": str(archive_path) if archive_path else None,
            "ip_address": ip_address,
        }
    )

    _append_to_file(
        _get_users_log_file(),
        _build_log_record(
            action="user_deleted",
            user_id=user_id,
            username=user.username,
            email=user.email,
            deleted_by=initiating_user_id,
            archive_path=str(archive_path) if archive_path else None,
            ip_address=ip_address,
        )
    )

    audit_service.log_crud(
        db=db,
        action="user_deleted",
        entity_type="User",
        entity_id=user_id,
        old_value=old_value,
        new_value={"is_active": False},
        details={
            "username": user.username,
            "deleted_by": initiating_user_id,
            "archive_path": str(archive_path) if archive_path else None,
        },
        success=True,
    )

    return {
        "message": f"Użytkownik '{user.username}' został trwale dezaktywowany.",
        "user_id": user_id,
        "username": user.username,
        "deleted_at": datetime.now(timezone.utc).isoformat(),
        "archive_path": str(archive_path) if archive_path else None,
    }


# ===========================================================================
# Blokowanie / odblokowywanie konta
# ===========================================================================

async def lock(
    db: AsyncSession,
    redis: Redis,
    user_id: int,
    duration_minutes: int,
    locked_by_user_id: int,
    reason: Optional[str] = None,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Blokuje konto użytkownika na podany czas.

    Ustawia locked_until = now() + duration_minutes.
    Nieskończona blokada: duration_minutes = 0 → locked_until = 9999-12-31.

    Args:
        db:                 Sesja SQLAlchemy.
        redis:              Klient Redis.
        user_id:            ID użytkownika do zablokowania.
        duration_minutes:   Czas blokady w minutach. 0 = bezterminowa.
        locked_by_user_id:  ID admina wykonującego blokadę.
        reason:             Opcjonalne uzasadnienie blokady (logowane).
        ip_address:         IP inicjatora.

    Returns:
        Słownik z potwierdzeniem blokady.

    Raises:
        UserNotFoundError: Gdy użytkownik nie istnieje.
        UserLockError:     Gdy próba zablokowania własnego konta.
    """
    if user_id == locked_by_user_id:
        raise UserLockError("Nie można zablokować własnego konta.")

    if duration_minutes < 0:
        raise UserValidationError("Czas blokady nie może być ujemny.")

    result = await db.execute(select(User).where(User.id_user == user_id))
    user = result.scalar_one_or_none()

    if user is None:
        raise UserNotFoundError(f"Użytkownik ID={user_id} nie istnieje.")

    # Bezterminowa blokada = data w dalekiej przyszłości
    if duration_minutes == 0:
        locked_until = datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        lock_description = "bezterminowa"
    else:
        locked_until = datetime.now(timezone.utc) + timedelta(minutes=duration_minutes)
        lock_description = f"{duration_minutes} min"

    old_locked_until = user.locked_until
    user.locked_until = locked_until
    user.updated_at = datetime.now(timezone.utc)
    await db.flush()
    await db.commit()

    # Inwalidacja cache
    await _invalidate_user_cache(redis, user_id)

    logger.warning(
        "Konto użytkownika zablokowane",
        extra={
            "user_id": user_id,
            "username": user.username,
            "locked_until": locked_until.isoformat(),
            "duration_minutes": duration_minutes,
            "lock_description": lock_description,
            "locked_by": locked_by_user_id,
            "reason": reason,
            "ip_address": ip_address,
        }
    )

    _append_to_file(
        _get_users_log_file(),
        _build_log_record(
            action="user_locked",
            user_id=user_id,
            username=user.username,
            locked_until=locked_until.isoformat(),
            duration_minutes=duration_minutes,
            locked_by=locked_by_user_id,
            reason=reason,
            ip_address=ip_address,
        )
    )

    audit_service.log_crud(
        db=db,
        action="user_locked",
        entity_type="User",
        entity_id=user_id,
        old_value={"locked_until": old_locked_until.isoformat() if old_locked_until else None},
        new_value={"locked_until": locked_until.isoformat()},
        details={"duration_minutes": duration_minutes, "reason": reason, "locked_by": locked_by_user_id},
        success=True,
    )

    return {
        "message": f"Konto '{user.username}' zablokowane ({lock_description}).",
        "user_id": user_id,
        "username": user.username,
        "locked_until": locked_until.isoformat(),
        "duration_minutes": duration_minutes,
    }


async def unlock(
    db: AsyncSession,
    redis: Redis,
    user_id: int,
    unlocked_by_user_id: int,
    reason: Optional[str] = None,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Odblokowuje konto użytkownika.

    Zeruje locked_until = None i failed_login_attempts = 0.

    Args:
        db:                   Sesja SQLAlchemy.
        redis:                Klient Redis.
        user_id:              ID użytkownika do odblokowania.
        unlocked_by_user_id:  ID admina wykonującego odblokowanie.
        reason:               Opcjonalne uzasadnienie.
        ip_address:           IP inicjatora.

    Returns:
        Słownik z potwierdzeniem odblokowania.

    Raises:
        UserNotFoundError: Gdy użytkownik nie istnieje.
    """
    result = await db.execute(select(User).where(User.id_user == user_id))
    user = result.scalar_one_or_none()

    if user is None:
        raise UserNotFoundError(f"Użytkownik ID={user_id} nie istnieje.")

    old_locked_until = user.locked_until
    was_locked = _is_user_locked(user)

    user.locked_until = None
    user.failed_login_attempts = 0
    user.updated_at = datetime.now(timezone.utc)
    await db.flush()

    # Inwalidacja cache
    await _invalidate_user_cache(redis, user_id)

    logger.info(
        "Konto użytkownika odblokowane",
        extra={
            "user_id": user_id,
            "username": user.username,
            "was_locked": was_locked,
            "old_locked_until": old_locked_until.isoformat() if old_locked_until else None,
            "unlocked_by": unlocked_by_user_id,
            "reason": reason,
            "ip_address": ip_address,
        }
    )

    _append_to_file(
        _get_users_log_file(),
        _build_log_record(
            action="user_unlocked",
            user_id=user_id,
            username=user.username,
            was_locked=was_locked,
            unlocked_by=unlocked_by_user_id,
            reason=reason,
            ip_address=ip_address,
        )
    )

    audit_service.log_crud(
        db=db,
        action="user_unlocked",
        entity_type="User",
        entity_id=user_id,
        old_value={"locked_until": old_locked_until.isoformat() if old_locked_until else None},
        new_value={"locked_until": None, "failed_login_attempts": 0},
        details={"was_locked": was_locked, "reason": reason, "unlocked_by": unlocked_by_user_id},
        success=True,
    )

    return {
        "message": f"Konto '{user.username}' zostało odblokowane.",
        "user_id": user_id,
        "username": user.username,
        "unlocked_at": datetime.now(timezone.utc).isoformat(),
        "was_locked": was_locked,
    }


# ===========================================================================
# Funkcje diagnostyczne / administracyjne
# ===========================================================================

async def get_locked_users(db: AsyncSession) -> list[dict]:
    """
    Zwraca listę aktualnie zablokowanych użytkowników.

    Przydatne do dashboardu administracyjnego.

    Args:
        db: Sesja SQLAlchemy.

    Returns:
        Lista słowników z danymi zablokowanych użytkowników.
    """
    now = datetime.now(timezone.utc)
    result = await db.execute(
        select(User)
        .options(selectinload(User.role))
        .where(
            and_(
                User.is_active == True,  # noqa: E712
                User.locked_until > now,
            )
        )
        .order_by(User.locked_until.asc())
    )
    locked_users = result.scalars().all()

    return [
        {
            "user_id": u.id_user,
            "username": u.username,
            "email": u.email,
            "locked_until": u.locked_until.isoformat(),
            "failed_login_attempts": u.failed_login_attempts,
            "role_name": u.role.role_name if u.role else None,
        }
        for u in locked_users
    ]


async def reset_failed_attempts(
    db: AsyncSession,
    redis: Redis,
    user_id: int,
) -> None:
    """
    Resetuje licznik nieudanych prób logowania dla użytkownika.

    Wywoływane po pomyślnym logowaniu przez auth_service.

    Args:
        db:      Sesja SQLAlchemy.
        redis:   Klient Redis.
        user_id: ID użytkownika.
    """
    result = await db.execute(select(User).where(User.id_user == user_id))
    user = result.scalar_one_or_none()

    if user and user.failed_login_attempts > 0:
        user.failed_login_attempts = 0
        user.updated_at = datetime.now(timezone.utc)
        await db.flush()
        await _invalidate_user_cache(redis, user_id)

        logger.debug(
            "Zresetowano licznik nieudanych prób logowania",
            extra={"user_id": user_id}
        )


async def increment_failed_attempts(
    db: AsyncSession,
    redis: Redis,
    user_id: int,
    auto_lock_threshold: int = 5,
    lock_duration_minutes: int = 15,
) -> int:
    """
    Inkrementuje licznik nieudanych prób logowania.

    Po przekroczeniu auto_lock_threshold — automatyczna blokada konta.

    Args:
        db:                   Sesja SQLAlchemy.
        redis:                Klient Redis.
        user_id:              ID użytkownika.
        auto_lock_threshold:  Po ilu próbach automatyczna blokada (domyślnie 5).
        lock_duration_minutes: Czas blokady w minutach (domyślnie 15).

    Returns:
        Aktualny licznik nieudanych prób po inkrementacji.
    """
    result = await db.execute(select(User).where(User.id_user == user_id))
    user = result.scalar_one_or_none()

    if user is None:
        return 0

    user.failed_login_attempts = (user.failed_login_attempts or 0) + 1
    current_count = user.failed_login_attempts

    if current_count >= auto_lock_threshold:
        locked_until = datetime.now(timezone.utc) + timedelta(minutes=lock_duration_minutes)
        user.locked_until = locked_until
        logger.warning(
            "Automatyczna blokada konta po przekroczeniu limitu prób logowania",
            extra={
                "user_id": user_id,
                "username": user.username,
                "failed_attempts": current_count,
                "locked_until": locked_until.isoformat(),
                "lock_duration_minutes": lock_duration_minutes,
            }
        )
        _append_to_file(
            _get_users_log_file(),
            _build_log_record(
                action="user_auto_locked",
                user_id=user_id,
                username=user.username,
                failed_attempts=current_count,
                locked_until=locked_until.isoformat(),
                lock_duration_minutes=lock_duration_minutes,
            )
        )

    user.updated_at = datetime.now(timezone.utc)
    await db.flush()
    await _invalidate_user_cache(redis, user_id)

    return current_count

async def admin_reset_password(
    db: AsyncSession,
    redis: Redis,
    user_id: int,
    admin_id: int,
    ip: Optional[str] = None,
) -> dict:
    """
    Admin inicjuje reset hasła dla wybranego użytkownika.

    Przepływ:
        1. Weryfikacja że użytkownik istnieje i jest aktywny
        2. Unieważnienie wszystkich aktywnych sesji
        3. Wygenerowanie OTP i zapis do kolejki (otp_service)
        4. AuditLog + log do pliku

    Args:
        db:       Sesja SQLAlchemy.
        redis:    Klient Redis.
        user_id:  ID użytkownika którego hasło resetujemy.
        admin_id: ID admina wykonującego operację.
        ip:       IP inicjatora.

    Returns:
        Dict z liczbą unieważnionych sesji.

    Raises:
        UserNotFoundError: Użytkownik nie istnieje.
    """
    from app.services import otp_service

    ip_clean = (ip or "unknown")[:45]

    # 1. Pobierz usera
    result = await db.execute(
        select(User).where(User.id_user == user_id)
    )
    user: Optional[User] = result.scalar_one_or_none()

    if user is None:
        raise UserNotFoundError(f"Użytkownik ID={user_id} nie istnieje.")

    # 2. Unieważnij wszystkie sesje
    from app.db.models.refresh_token import RefreshToken
    from sqlalchemy import update as sa_update
    revoke_result = await db.execute(
        sa_update(RefreshToken)
        .where(
            RefreshToken.user_id == user_id,
            RefreshToken.is_revoked == False,
        )
        .values(
            is_revoked=True,
            revoked_at=datetime.now(timezone.utc),
        )
    )
    sessions_revoked = revoke_result.rowcount
    await db.commit()

    # 3. Generuj OTP
    await otp_service.request_otp(
        db=db,
        redis=redis,
        email=user.email,
        purpose="password_reset",
        ip=ip_clean,
    )

    # 4. Log do pliku
    _append_to_file(
        _get_users_log_file(),
        _build_log_record(
            action="admin_reset_password",
            user_id=user_id,
            username=user.username,
            admin_id=admin_id,
            sessions_revoked=sessions_revoked,
            ip_address=ip_clean,
        )
    )

    # 5. AuditLog
    audit_service.log_crud(
        db=db,
        action="admin_reset_password",
        entity_type="User",
        entity_id=user_id,
        details={
            "admin_id": admin_id,
            "sessions_revoked": sessions_revoked,
            "ip": ip_clean,
        },
        success=True,
    )

    logger.warning(
        "Admin reset hasła: user_id=%d, admin_id=%d, sesje_unieważnione=%d (IP=%s)",
        user_id, admin_id, sessions_revoked, ip_clean,
        extra={
            "user_id": user_id,
            "admin_id": admin_id,
            "sessions_revoked": sessions_revoked,
            "ip": ip_clean,
        }
    )

    return {"sessions_revoked": sessions_revoked}