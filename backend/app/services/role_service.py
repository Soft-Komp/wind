"""
Serwis Ról — System Windykacja
================================
Krok 9 / Faza 3 — services/role_service.py

Odpowiedzialność:
    - Pełny CRUD ról (get_list, get_by_id, create, update, delete)
    - Przypisywanie uprawnień do roli (assign_permissions) — atomowa transakcja
    - Macierz uprawnień wszystkich ról (get_permissions_matrix)
    - Dwuetapowe usuwanie ról (z blokadem jeśli rola ma użytkowników)
    - Cache Redis dla list/detail/matrix
    - Inwalidacja cache uprawnień per-user po zmianie roli (perm:{user_id}:*)
    - Publikacja SSE event permissions_updated po każdej zmianie uprawnień
    - Pełny AuditLog z old_value → new_value dla każdej mutacji
    - Archiwizacja usuniętych ról (JSON.gz)
    - Plik logów roles_YYYY-MM-DD.jsonl (append-only)

Decyzje projektowe:
    - assign_permissions: DELETE wszystkich + INSERT nowych (atomowo w jednej sesji)
      Prostsze i bezpieczniejsze niż obliczanie diff i wykonywanie partial updates.
    - Rola z użytkownikami: blokada DELETE (żadne konto nie może zostać bez roli)
    - Cache matrix TTL: 300s — invalidowany przy każdej zmianie uprawnień
    - Cache list/detail TTL: 300s — invalidowany przy każdej mutacji roli
    - SSE event: publications przez event_service (Porcja B) — tutaj jako stub
      który bezpośrednio woła Redis publish (event_service nie jest jeszcze gotowy)

Zależności:
    - services/audit_service.py
    - services/config_service.py (delete token TTL)

Ścieżka docelowa: backend/app/services/role_service.py
Autor: System Windykacja — Faza 3 Krok 9
Wersja: 1.0.0
Data: 2026-02-19
"""

from __future__ import annotations

import gzip
import logging
import secrets
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import orjson
from jose import JWTError, jwt
from redis.asyncio import Redis
from sqlalchemy import and_, delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.config import settings
from app.db.models.permission import Permission
from app.db.models.role import Role
from app.db.models.role_permission import RolePermission
from app.db.models.user import User
from app.services import audit_service
from app.services import config_service

# ---------------------------------------------------------------------------
# Logger własny modułu
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------

# TTL cache Redis w sekundach
_CACHE_ROLES_LIST_TTL: int = 300      # 5 min — lista wszystkich ról
_CACHE_ROLE_DETAIL_TTL: int = 300     # 5 min — szczegóły jednej roli
_CACHE_MATRIX_TTL: int = 300          # 5 min — macierz uprawnień

# Domyślny TTL tokenu DELETE w sekundach (fallback gdy config niedostępny)
_DEFAULT_DELETE_TOKEN_TTL: int = 60

# Klucze Redis
_REDIS_KEY_ROLES_LIST  = "roles:list"
_REDIS_KEY_ROLE_DETAIL = "role:{role_id}"
_REDIS_KEY_MATRIX      = "roles:matrix"
_REDIS_KEY_DELETE      = "delete_confirm:role:{jti}"

# SSE channel — publikacja event permissions_updated
_SSE_CHANNEL_ADMINS = "channel:admins"

# Pliki logów i archiwów
_ROLES_LOG_FILE_PATTERN = "logs/roles_{date}.jsonl"
_ARCHIVE_BASE_DIR = "archives"

# Predefiniowane nazwy ról których NIE można usunąć (systemowe)
_PROTECTED_ROLE_NAMES: frozenset[str] = frozenset({"admin", "administrator"})


# ===========================================================================
# Dataclassy wejściowe / wyjściowe
# ===========================================================================

@dataclass(frozen=True)
class RoleCreateData:
    """
    Dane do tworzenia nowej roli.

    Attributes:
        role_name:   Nazwa roli (unique, 2–50 znaków).
        description: Opcjonalny opis roli (max 200 znaków).
    """
    role_name: str
    description: Optional[str] = None

    def __post_init__(self) -> None:
        role_name = unicodedata.normalize("NFC", self.role_name.strip())
        if len(role_name) < 2 or len(role_name) > 50:
            raise RoleValidationError(
                "Nazwa roli musi mieć od 2 do 50 znaków."
            )
        object.__setattr__(self, "role_name", role_name)

        if self.description is not None:
            desc = unicodedata.normalize("NFC", self.description.strip())
            object.__setattr__(self, "description", desc[:200] if desc else None)


@dataclass
class RoleUpdateData:
    """
    Dane do aktualizacji roli. Wszystkie pola opcjonalne.

    Attributes:
        role_name:   Nowa nazwa roli.
        description: Nowy opis roli.
    """
    role_name: Optional[str] = None
    description: Optional[str] = None

    def __post_init__(self) -> None:
        if self.role_name is not None:
            role_name = unicodedata.normalize("NFC", self.role_name.strip())
            if len(role_name) < 2 or len(role_name) > 50:
                raise RoleValidationError(
                    "Nazwa roli musi mieć od 2 do 50 znaków."
                )
            self.role_name = role_name

        if self.description is not None:
            desc = unicodedata.normalize("NFC", self.description.strip())
            self.description = desc[:200] if desc else None

    def has_changes(self) -> bool:
        return self.role_name is not None or self.description is not None


@dataclass(frozen=True)
class DeleteConfirmData:
    """
    Dane tokenu potwierdzającego DELETE roli.

    Attributes:
        token:      JWT token potwierdzający.
        expires_in: TTL w sekundach.
        action:     Opis akcji ("delete_role").
        role_id:    ID roli do usunięcia.
        role_name:  Nazwa roli.
        warning:    Opcjonalne ostrzeżenie (np. liczba powiązanych użytkowników).
    """
    token: str
    expires_in: int
    action: str
    role_id: int
    role_name: str
    warning: Optional[str] = None


# ===========================================================================
# Klasy wyjątków
# ===========================================================================

class RoleError(Exception):
    """Bazowy wyjątek serwisu ról."""


class RoleValidationError(RoleError):
    """Błąd walidacji danych wejściowych."""


class RoleNotFoundError(RoleError):
    """Rola nie istnieje lub jest nieaktywna."""


class RoleAlreadyExistsError(RoleError):
    """Rola o podanej nazwie już istnieje."""


class RoleProtectedError(RoleError):
    """Próba modyfikacji lub usunięcia chronionej roli systemowej."""


class RoleHasUsersError(RoleError):
    """
    Próba usunięcia roli która ma przypisanych użytkowników.

    Attributes:
        user_count: Liczba aktywnych użytkowników z tą rolą.
    """
    def __init__(self, role_name: str, user_count: int) -> None:
        self.role_name = role_name
        self.user_count = user_count
        super().__init__(
            f"Nie można usunąć roli '{role_name}' — ma {user_count} "
            f"przypisanych użytkowników. Najpierw zmień im rolę."
        )


class RoleDeleteTokenError(RoleError):
    """Token potwierdzający DELETE jest nieprawidłowy, wygasły lub już użyty."""


class RolePermissionAssignError(RoleError):
    """Błąd przy przypisywaniu uprawnień do roli."""


# ===========================================================================
# Funkcje pomocnicze — prywatne
# ===========================================================================

def _get_log_dir() -> Path:
    """Zwraca i tworzy katalog logów."""
    p = Path("logs")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _get_archive_dir() -> Path:
    """Zwraca i tworzy dzienny katalog archiwum."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    p = Path(_ARCHIVE_BASE_DIR) / today
    p.mkdir(parents=True, exist_ok=True)
    return p


def _get_roles_log_file() -> Path:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return _get_log_dir() / f"roles_{today}.jsonl"


def _append_to_file(filepath: Path, record: dict) -> None:
    """Dopisuje rekord JSON do pliku JSON Lines (append-only)."""
    try:
        line = orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE)
        with filepath.open("ab") as f:
            f.write(line)
    except OSError as exc:
        logger.warning(
            "Nie można zapisać do pliku logu ról",
            extra={"filepath": str(filepath), "error": str(exc)}
        )


def _build_log_record(action: str, **kwargs) -> dict:
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": "role_service",
        "action": action,
        **kwargs,
    }


def _role_to_dict(role: Role, include_permissions: bool = False) -> dict:
    """
    Konwertuje obiekt Role na słownik bezpieczny do zwrócenia/logowania.

    Args:
        role:               Obiekt SQLAlchemy Role.
        include_permissions: Jeśli True — dołącza listę uprawnień.

    Returns:
        Słownik z danymi roli.
    """
    data: dict = {
        "id_role": role.id_role,
        "role_name": role.role_name,
        "description": role.description,
        "is_active": role.is_active,
        "created_at": role.created_at.isoformat() if role.created_at else None,
        "updated_at": role.updated_at.isoformat() if role.updated_at else None,
    }
    if include_permissions and hasattr(role, "permissions"):
        data["permissions"] = [
            {
                "id_permission": p.id_permission,
                "permission_name": p.permission_name,
                "category": p.category,
                "description": p.description,
            }
            for p in role.permissions
            if p.is_active
        ]
        data["permission_count"] = len(data["permissions"])
    return data


def _archive_role(role: Role) -> Optional[Path]:
    """
    Archiwizuje dane roli do pliku JSON.gz przed soft-delete.

    Format: archives/YYYY-MM-DD/archive_roles_{id}_{ts}.json.gz
    """
    now = datetime.now(timezone.utc)
    ts_str = now.strftime("%Y%m%d_%H%M%S")
    filename = f"archive_roles_{role.id_role}_{ts_str}.json.gz"
    filepath = _get_archive_dir() / filename

    archive_data = {
        "archived_at": now.isoformat(),
        "archive_type": "soft_delete",
        "table": "dbo_ext.Roles",
        "record": _role_to_dict(role, include_permissions=True),
    }

    try:
        with gzip.open(filepath, "wb") as f:
            f.write(orjson.dumps(archive_data, option=orjson.OPT_INDENT_2))
        logger.info(
            "Zarchiwizowano dane roli",
            extra={"role_id": role.id_role, "archive_path": str(filepath)}
        )
        return filepath
    except OSError as exc:
        logger.error(
            "Nie udało się zarchiwizować danych roli",
            extra={"role_id": role.id_role, "error": str(exc)}
        )
        return None


async def _get_redis_cache(redis: Redis, key: str) -> Optional[dict | list]:
    """Pobiera dane z Redis cache. Zwraca None przy braku lub błędzie."""
    try:
        raw = await redis.get(key)
        if raw:
            return orjson.loads(raw)
    except Exception as exc:
        logger.debug("Cache miss", extra={"key": key, "error": str(exc)})
    return None


async def _set_redis_cache(redis: Redis, key: str, data, ttl: int) -> None:
    """Zapisuje dane do Redis cache. Błędy logowane jako debug."""
    try:
        await redis.set(key, orjson.dumps(data), ex=ttl)
    except Exception as exc:
        logger.debug("Błąd zapisu do cache Redis", extra={"key": key, "error": str(exc)})


async def _invalidate_role_caches(redis: Redis, role_id: Optional[int] = None) -> None:
    """
    Inwaliduje wszystkie klucze cache związane z rolami.

    Zawsze kasuje: roles:list, roles:matrix.
    Jeśli podano role_id: kasuje też role:{role_id}.

    Args:
        redis:   Klient Redis.
        role_id: ID roli (opcjonalne — kasuje szczegóły tej roli).
    """
    keys_to_delete = [_REDIS_KEY_ROLES_LIST, _REDIS_KEY_MATRIX]
    if role_id is not None:
        keys_to_delete.append(_REDIS_KEY_ROLE_DETAIL.format(role_id=role_id))
    try:
        if keys_to_delete:
            await redis.delete(*keys_to_delete)
    except Exception as exc:
        logger.warning(
            "Błąd inwalidacji cache ról",
            extra={"keys": keys_to_delete, "error": str(exc)}
        )


async def _invalidate_user_permission_caches(
    db: AsyncSession,
    redis: Redis,
    role_id: int,
) -> int:
    """
    Inwaliduje cache uprawnień wszystkich użytkowników z daną rolą.

    Wywoływana po assign_permissions() — żeby użytkownicy natychmiast
    zaczęli używać nowych uprawnień (bez czekania na TTL 300s).

    Klucze Redis: perm:{user_id}:{permission_name}
    Inwalidacja przez scan_iter("perm:{user_id}:*") dla każdego usera.

    Args:
        db:      Sesja SQLAlchemy.
        redis:   Klient Redis.
        role_id: ID roli — szukamy użytkowników z tą rolą.

    Returns:
        Liczba użytkowników których cache unieważniono.
    """
    # Pobierz ID wszystkich aktywnych użytkowników z tą rolą
    result = await db.execute(
        select(User.id_user).where(
            and_(User.role_id == role_id, User.is_active == True)  # noqa: E712
        )
    )
    user_ids = [row[0] for row in result.fetchall()]

    if not user_ids:
        return 0

    invalidated_count = 0
    try:
        for user_id in user_ids:
            pattern = f"perm:{user_id}:*"
            async for key in redis.scan_iter(pattern):
                await redis.delete(key)
            invalidated_count += 1

        logger.info(
            "Zinwalidowano cache uprawnień użytkowników po zmianie roli",
            extra={
                "role_id": role_id,
                "affected_users": len(user_ids),
                "user_ids": user_ids,
            }
        )
    except Exception as exc:
        logger.warning(
            "Błąd inwalidacji cache uprawnień użytkowników",
            extra={"role_id": role_id, "error": str(exc)}
        )

    return invalidated_count


async def _publish_permissions_updated_event(redis: Redis, role_id: int) -> None:
    """
    Publikuje SSE event permissions_updated do kanału admins.

    Stub przed zaimplementowaniem event_service (Porcja B).
    Wiadomość jest zgodna z formatem oczekiwanym przez SSE endpoint.

    Args:
        redis:   Klient Redis.
        role_id: ID roli której uprawnienia się zmieniły.
    """
    event_payload = orjson.dumps({
        "type": "permissions_updated",
        "data": {"role_id": role_id},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
    try:
        await redis.publish(_SSE_CHANNEL_ADMINS, event_payload)
        logger.info(
            "SSE event permissions_updated opublikowany",
            extra={"role_id": role_id, "channel": _SSE_CHANNEL_ADMINS}
        )
    except Exception as exc:
        logger.warning(
            "Nie udało się opublikować SSE event permissions_updated",
            extra={"role_id": role_id, "error": str(exc)}
        )


async def _get_role_with_permissions(db: AsyncSession, role_id: int) -> Optional[Role]:
    """
    Pobiera rolę z eager-loaded uprawnieniami.

    Args:
        db:      Sesja SQLAlchemy.
        role_id: ID roli.

    Returns:
        Obiekt Role z załadowanymi permissions lub None.
    """
    result = await db.execute(
        select(Role)
        .options(
            selectinload(Role.role_permissions).selectinload(RolePermission.permission)
        )
        .where(Role.id_role == role_id)
    )
    return result.scalar_one_or_none()


async def _count_users_with_role(db: AsyncSession, role_id: int) -> int:
    """
    Liczy aktywnych użytkowników przypisanych do roli.

    Args:
        db:      Sesja SQLAlchemy.
        role_id: ID roli.

    Returns:
        Liczba aktywnych użytkowników z tą rolą.
    """
    result = await db.execute(
        select(func.count(User.id_user)).where(
            and_(User.role_id == role_id, User.is_active == True)  # noqa: E712
        )
    )
    return result.scalar_one() or 0


def _is_role_protected(role_name: str) -> bool:
    """Sprawdza czy rola jest chronioną rolą systemową (np. Admin)."""
    return role_name.lower().strip() in _PROTECTED_ROLE_NAMES


# ===========================================================================
# Publiczne API serwisu — READ
# ===========================================================================

async def get_list(db: AsyncSession, redis: Redis) -> list[dict]:
    """
    Pobiera listę wszystkich aktywnych ról.

    Zwraca role bez szczegółowych uprawnień — tylko metadane.
    Dla szczegółów z uprawnieniami użyj get_by_id().

    Cache: roles:list (TTL 300s) — inwalidowany przy każdej mutacji.

    Args:
        db:    Sesja SQLAlchemy.
        redis: Klient Redis.

    Returns:
        Lista słowników z danymi ról, posortowana po role_name.
    """
    cached = await _get_redis_cache(redis, _REDIS_KEY_ROLES_LIST)
    if cached is not None:
        logger.debug("Lista ról pobrana z cache Redis")
        return cached  # type: ignore[return-value]

    result = await db.execute(
        select(Role)
        .options(
            selectinload(Role.role_permissions).selectinload(RolePermission.permission)
        )
        .where(Role.is_active == True)  # noqa: E712
        .order_by(Role.role_name.asc())
    )
    roles = result.scalars().all()

    data = []
    for role in roles:
        role_dict = _role_to_dict(role)
        role_dict["permission_count"] = sum(
            1 for p in role.permissions if p.is_active
        )
        data.append(role_dict)

    await _set_redis_cache(redis, _REDIS_KEY_ROLES_LIST, data, _CACHE_ROLES_LIST_TTL)

    logger.debug("Lista ról pobrana z bazy danych", extra={"count": len(data)})
    return data


async def get_by_id(
    db: AsyncSession,
    redis: Redis,
    role_id: int,
) -> dict:
    """
    Pobiera szczegóły roli wraz z pełną listą uprawnień.

    Cache: role:{role_id} (TTL 300s).

    Args:
        db:      Sesja SQLAlchemy.
        redis:   Klient Redis.
        role_id: ID roli.

    Returns:
        Słownik z danymi roli i listą uprawnień.

    Raises:
        RoleNotFoundError: Gdy rola nie istnieje lub jest nieaktywna.
    """
    cache_key = _REDIS_KEY_ROLE_DETAIL.format(role_id=role_id)
    cached = await _get_redis_cache(redis, cache_key)
    if cached is not None:
        logger.debug("Szczegóły roli pobrane z cache", extra={"role_id": role_id})
        return cached  # type: ignore[return-value]

    role = await _get_role_with_permissions(db, role_id)
    if role is None or not role.is_active:
        raise RoleNotFoundError(f"Rola ID={role_id} nie istnieje lub jest nieaktywna.")

    # Dołącz liczbę użytkowników
    user_count = await _count_users_with_role(db, role_id)

    data = _role_to_dict(role, include_permissions=True)
    data["user_count"] = user_count

    await _set_redis_cache(redis, cache_key, data, _CACHE_ROLE_DETAIL_TTL)

    logger.debug("Szczegóły roli pobrane z bazy", extra={"role_id": role_id})
    return data


async def get_permissions_matrix(
    db: AsyncSession,
    redis: Redis,
) -> dict[str, list[str]]:
    """
    Zwraca macierz uprawnień: {nazwa_roli: [lista uprawnień]}.

    Używana przez frontend do budowania UI matrix uprawnień.
    Cache: roles:matrix (TTL 300s) — inwalidowany przy assign_permissions().

    Args:
        db:    Sesja SQLAlchemy.
        redis: Klient Redis.

    Returns:
        Słownik {role_name: [permission_name, ...]}.
    """
    cached = await _get_redis_cache(redis, _REDIS_KEY_MATRIX)
    if cached is not None:
        logger.debug("Macierz uprawnień pobrana z cache Redis")
        return cached  # type: ignore[return-value]

    result = await db.execute(
    select(Role)
        .options(
            selectinload(Role.role_permissions).selectinload(RolePermission.permission)
        )
        .where(Role.is_active == True)  # noqa: E712
        .order_by(Role.role_name.asc())
    )
    roles = result.scalars().all()

    matrix: dict[str, list[str]] = {}
    for role in roles:
        matrix[role.role_name] = sorted([
            p.permission_name
            for p in role.permissions    # ← association_proxy → obiekty Permission
            if p.is_active
        ])

    await _set_redis_cache(redis, _REDIS_KEY_MATRIX, matrix, _CACHE_MATRIX_TTL)

    logger.debug(
        "Macierz uprawnień pobrana z bazy",
        extra={"roles_count": len(matrix)}
    )
    return matrix


# ===========================================================================
# Publiczne API serwisu — WRITE
# ===========================================================================

async def create(
    db: AsyncSession,
    redis: Redis,
    data: RoleCreateData,
    created_by_id: Optional[int] = None,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Tworzy nową rolę.

    Przepływ:
        1. Walidacja unikalności role_name (case-insensitive)
        2. Zapis do bazy
        3. Inwalidacja cache (roles:list, roles:matrix)
        4. AuditLog (fire-and-forget)
        5. Zapis do pliku logów

    Args:
        db:            Sesja SQLAlchemy.
        redis:         Klient Redis.
        data:          Zwalidowane dane nowej roli.
        created_by_id: ID admina tworzącego rolę.
        ip_address:    IP inicjatora.

    Returns:
        Słownik z danymi nowo utworzonej roli.

    Raises:
        RoleAlreadyExistsError: Gdy rola o podanej nazwie już istnieje.
    """
    # Sprawdzenie unikalności (case-insensitive przez MSSQL COLLATION)
    existing = await db.execute(
        select(func.count(Role.id_role)).where(
            Role.role_name == data.role_name
        )
    )
    if (existing.scalar_one() or 0) > 0:
        raise RoleAlreadyExistsError(
            f"Rola o nazwie '{data.role_name}' już istnieje."
        )

    new_role = Role(
        role_name=data.role_name,
        description=data.description,
        is_active=True,
    )
    db.add(new_role)
    await db.flush()

    role_id = new_role.id_role

    await _invalidate_role_caches(redis)

    result_dict = _role_to_dict(new_role, include_permissions=True)

    logger.info(
        "Utworzono nową rolę",
        extra={
            "role_id": role_id,
            "role_name": data.role_name,
            "created_by": created_by_id,
            "ip_address": ip_address,
        }
    )

    _append_to_file(
        _get_roles_log_file(),
        _build_log_record(
            action="role_created",
            role_id=role_id,
            role_name=data.role_name,
            description=data.description,
            created_by=created_by_id,
            ip_address=ip_address,
        )
    )

    audit_service.log_crud(
        db=db,
        action="role_created",
        entity_type="Role",
        entity_id=role_id,
        new_value={"role_name": data.role_name, "description": data.description},
        success=True,
    )

    return result_dict


async def update(
    db: AsyncSession,
    redis: Redis,
    role_id: int,
    data: RoleUpdateData,
    updated_by_id: Optional[int] = None,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Aktualizuje dane roli (nazwa i/lub opis).

    ⚠️  Nie można zmieniać nazwy chronionych ról systemowych (Admin).

    Przepływ:
        1. Pobranie roli z bazy (old_value)
        2. Walidacja że rola nie jest chroniona (jeśli zmiana nazwy)
        3. Walidacja unikalności nowej nazwy
        4. Aktualizacja pól
        5. Inwalidacja cache
        6. AuditLog + plik logów

    Args:
        db:            Sesja SQLAlchemy.
        redis:         Klient Redis.
        role_id:       ID roli do aktualizacji.
        data:          Dane aktualizacji.
        updated_by_id: ID admina wykonującego operację.
        ip_address:    IP inicjatora.

    Returns:
        Słownik z zaktualizowanymi danymi roli.

    Raises:
        RoleNotFoundError:      Gdy rola nie istnieje.
        RoleAlreadyExistsError: Gdy nowa nazwa jest już zajęta.
        RoleProtectedError:     Gdy próba zmiany nazwy chronionej roli.
        RoleValidationError:    Gdy brak zmian.
    """
    if not data.has_changes():
        raise RoleValidationError("Brak danych do aktualizacji.")

    role = await _get_role_with_permissions(db, role_id)
    if role is None or not role.is_active:
        raise RoleNotFoundError(f"Rola ID={role_id} nie istnieje lub jest nieaktywna.")

    old_value = _role_to_dict(role)

    # Ochrona systemowej roli Admin — zakaz zmiany nazwy
    if data.role_name is not None and _is_role_protected(role.role_name):
        raise RoleProtectedError(
            f"Nie można zmieniać nazwy chronionej roli systemowej '{role.role_name}'."
        )

    changed_fields: list[str] = []

    if data.role_name is not None and data.role_name != role.role_name:
        # Sprawdź unikalność
        existing = await db.execute(
            select(func.count(Role.id_role)).where(
                and_(Role.role_name == data.role_name, Role.id_role != role_id)
            )
        )
        if (existing.scalar_one() or 0) > 0:
            raise RoleAlreadyExistsError(
                f"Rola o nazwie '{data.role_name}' już istnieje."
            )
        role.role_name = data.role_name
        changed_fields.append("role_name")

    if data.description is not None and data.description != role.description:
        role.description = data.description
        changed_fields.append("description")

    if not changed_fields:
        raise RoleValidationError("Brak rzeczywistych zmian do zapisania.")

    role.updated_at = datetime.now(timezone.utc)
    await db.flush()

    await _invalidate_role_caches(redis, role_id)

    new_value = _role_to_dict(role)

    logger.info(
        "Zaktualizowano rolę",
        extra={
            "role_id": role_id,
            "role_name": role.role_name,
            "changed_fields": changed_fields,
            "updated_by": updated_by_id,
            "ip_address": ip_address,
        }
    )

    _append_to_file(
        _get_roles_log_file(),
        _build_log_record(
            action="role_updated",
            role_id=role_id,
            role_name=role.role_name,
            changed_fields=changed_fields,
            updated_by=updated_by_id,
            ip_address=ip_address,
        )
    )

    audit_service.log_crud(
        db=db,
        action="role_updated",
        entity_type="Role",
        entity_id=role_id,
        old_value=old_value,
        new_value=new_value,
        details={"changed_fields": changed_fields},
        success=True,
    )

    return _role_to_dict(role, include_permissions=True)


# ===========================================================================
# Dwuetapowe usuwanie
# ===========================================================================

async def initiate_delete(
    db: AsyncSession,
    redis: Redis,
    role_id: int,
    initiated_by_id: int,
    ip_address: Optional[str] = None,
) -> DeleteConfirmData:
    """
    Inicjuje dwuetapowe usunięcie roli — Krok 1.

    Blokuje DELETE jeśli:
        - Rola jest chronioną rolą systemową (Admin)
        - Rola ma przypisanych aktywnych użytkowników

    Przepływ:
        1. Weryfikacja że rola istnieje
        2. Sprawdzenie czy rola jest chroniona
        3. Sprawdzenie czy rola ma użytkowników (BLOKADA jeśli tak)
        4. Generowanie JWT tokenu potwierdzającego
        5. Zapis JTI do Redis (jednorazowość)
        6. AuditLog + plik logów

    Args:
        db:              Sesja SQLAlchemy.
        redis:           Klient Redis.
        role_id:         ID roli do usunięcia.
        initiated_by_id: ID admina inicjującego delete.
        ip_address:      IP inicjatora.

    Returns:
        DeleteConfirmData z tokenem i metadanymi.

    Raises:
        RoleNotFoundError:   Gdy rola nie istnieje.
        RoleProtectedError:  Gdy próba usunięcia chronionej roli.
        RoleHasUsersError:   Gdy rola ma przypisanych użytkowników.
    """
    role = await _get_role_with_permissions(db, role_id)
    if role is None or not role.is_active:
        raise RoleNotFoundError(f"Rola ID={role_id} nie istnieje lub jest nieaktywna.")

    # Ochrona ról systemowych
    if _is_role_protected(role.role_name):
        raise RoleProtectedError(
            f"Nie można usunąć chronionej roli systemowej '{role.role_name}'."
        )

    # Blokada gdy rola ma użytkowników
    user_count = await _count_users_with_role(db, role_id)
    if user_count > 0:
        raise RoleHasUsersError(role.role_name, user_count)

    # TTL tokenu z konfiguracji
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
        "sub": str(role_id),
        "type": "delete_confirm",
        "action": "delete_role",
        "entity_type": "Role",
        "initiated_by": initiated_by_id,
        "jti": jti,
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
    }

    delete_token = jwt.encode(
        token_payload,
        settings.secret_key.get_secret_value() if hasattr(settings.secret_key, "get_secret_value") else str(settings.secret_key),
        algorithm=settings.algorithm,
    )

    # Zapis JTI do Redis (jednorazowość)
    await redis.set(
        _REDIS_KEY_DELETE.format(jti=jti),
        str(role_id),
        ex=ttl_seconds,
    )

    warning = None
    perm_count = sum(1 for p in role.permissions if p.is_active)
    if perm_count > 0:
        warning = (
            f"Rola '{role.role_name}' ma przypisanych {perm_count} uprawnień — "
            f"zostaną one usunięte razem z rolą."
        )

    logger.info(
        "Zainicjowano usunięcie roli — krok 1",
        extra={
            "role_id": role_id,
            "role_name": role.role_name,
            "initiated_by": initiated_by_id,
            "ttl_seconds": ttl_seconds,
            "ip_address": ip_address,
        }
    )

    _append_to_file(
        _get_roles_log_file(),
        _build_log_record(
            action="role_delete_initiated",
            role_id=role_id,
            role_name=role.role_name,
            initiated_by=initiated_by_id,
            ttl_seconds=ttl_seconds,
            ip_address=ip_address,
        )
    )

    audit_service.log_crud(
        db=db,
        action="role_delete_initiated",
        entity_type="Role",
        entity_id=role_id,
        details={
            "role_name": role.role_name,
            "permission_count": perm_count,
            "initiated_by": initiated_by_id,
            "ttl_seconds": ttl_seconds,
        },
        success=True,
    )

    return DeleteConfirmData(
        token=delete_token,
        expires_in=ttl_seconds,
        action="delete_role",
        role_id=role_id,
        role_name=role.role_name,
        warning=warning,
    )


async def confirm_delete(
    db: AsyncSession,
    redis: Redis,
    role_id: int,
    confirm_token: str,
    initiated_by_id: int,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Potwierdza i wykonuje soft-delete roli — Krok 2.

    Kasuje też wszystkie RolePermissions przypisane do roli.
    Archiwizuje dane roli do JSON.gz przed usunięciem.

    Przepływ:
        1. Dekodowanie i weryfikacja tokenu JWT
        2. Sprawdzenie JTI w Redis (jednorazowość)
        3. Ponowne sprawdzenie czy nie pojawiły się nowi użytkownicy
        4. Archiwizacja roli (z jej uprawnieniami)
        5. Usunięcie RolePermissions (fizyczne DELETE — te rekordy nie mają soft-delete)
        6. Soft-delete roli (is_active=False)
        7. Inwalidacja wszystkich cache
        8. AuditLog + plik logów

    Args:
        db:              Sesja SQLAlchemy.
        redis:           Klient Redis.
        role_id:         ID roli do usunięcia.
        confirm_token:   Token JWT z initiate_delete().
        initiated_by_id: ID admina potwierdzającego.
        ip_address:      IP inicjatora.

    Returns:
        Słownik z potwierdzeniem wykonanego usunięcia.

    Raises:
        RoleDeleteTokenError: Nieprawidłowy/wygasły/użyty token.
        RoleNotFoundError:    Rola nie istnieje.
        RoleHasUsersError:    Rola zyskała użytkowników od czasu initiate_delete.
    """
    # Dekoduj i weryfikuj token JWT
    try:
        payload = jwt.decode(
            confirm_token,
            settings.secret_key.get_secret_value() if hasattr(settings.secret_key, "get_secret_value") else str(settings.secret_key),
            algorithms=[settings.algorithm],
        )
    except JWTError as exc:
        raise RoleDeleteTokenError(
            f"Token potwierdzający jest nieprawidłowy lub wygasł: {exc}"
        )

    token_type = payload.get("type")
    token_sub  = payload.get("sub")
    token_action = payload.get("action")
    token_jti    = payload.get("jti")
    token_by     = payload.get("initiated_by")

    if token_type != "delete_confirm" or token_action != "delete_role":
        raise RoleDeleteTokenError("Token nie jest tokenem potwierdzającym usunięcia roli.")

    if token_sub is None or int(token_sub) != role_id:
        raise RoleDeleteTokenError("Token dotyczy innej roli niż podany role_id.")

    if token_by is None or int(token_by) != initiated_by_id:
        raise RoleDeleteTokenError("Token był wygenerowany przez innego administratora.")

    # Sprawdź jednorazowość w Redis
    redis_key = _REDIS_KEY_DELETE.format(jti=token_jti)
    stored = await redis.get(redis_key)
    if stored is None:
        raise RoleDeleteTokenError(
            "Token potwierdzający wygasł lub został już użyty."
        )
    await redis.delete(redis_key)

    # Pobierz rolę
    role = await _get_role_with_permissions(db, role_id)
    if role is None or not role.is_active:
        raise RoleNotFoundError(f"Rola ID={role_id} nie istnieje.")

    # Ponowne sprawdzenie użytkowników (race condition guard)
    user_count = await _count_users_with_role(db, role_id)
    if user_count > 0:
        raise RoleHasUsersError(role.role_name, user_count)

    # Archiwizacja
    archive_path = _archive_role(role)

    old_value = _role_to_dict(role, include_permissions=True)

    # Fizyczne usunięcie RolePermissions (tabela bez soft-delete)
    await db.execute(
        delete(RolePermission).where(RolePermission.id_role == role_id)
    )

    # Soft-delete roli
    role.is_active = False
    role.updated_at = datetime.now(timezone.utc)
    await db.flush()

    # Inwalidacja cache
    await _invalidate_role_caches(redis, role_id)

    logger.warning(
        "Rola usunięta (soft-delete) — RolePermissions fizycznie usunięte",
        extra={
            "role_id": role_id,
            "role_name": role.role_name,
            "deleted_by": initiated_by_id,
            "archive_path": str(archive_path) if archive_path else None,
            "ip_address": ip_address,
        }
    )

    _append_to_file(
        _get_roles_log_file(),
        _build_log_record(
            action="role_deleted",
            role_id=role_id,
            role_name=role.role_name,
            deleted_by=initiated_by_id,
            archive_path=str(archive_path) if archive_path else None,
            ip_address=ip_address,
        )
    )

    audit_service.log_crud(
        db=db,
        action="role_deleted",
        entity_type="Role",
        entity_id=role_id,
        old_value=old_value,
        new_value={"is_active": False},
        details={
            "role_name": role.role_name,
            "deleted_by": initiated_by_id,
            "archive_path": str(archive_path) if archive_path else None,
        },
        success=True,
    )

    return {
        "message": f"Rola '{role.role_name}' została trwale dezaktywowana.",
        "role_id": role_id,
        "role_name": role.role_name,
        "deleted_at": datetime.now(timezone.utc).isoformat(),
        "archive_path": str(archive_path) if archive_path else None,
    }


# ===========================================================================
# Zarządzanie uprawnieniami roli
# ===========================================================================

async def assign_permissions(
    db: AsyncSession,
    redis: Redis,
    role_id: int,
    permission_ids: list[int],
    updated_by_id: Optional[int] = None,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Atomowo zastępuje zestaw uprawnień roli.

    Strategia: DELETE wszystkich istniejących + INSERT nowych.
    Wykonywane w jednej sesji DB — atomowo (rollback przy błędzie).

    ⚠️  Jeśli permission_ids = [] → rola zostaje bez żadnych uprawnień.

    Po operacji:
        - Inwalidacja cache: role:{id}, roles:list, roles:matrix
        - Inwalidacja cache uprawnień wszystkich userów z tą rolą (perm:{uid}:*)
        - SSE event: permissions_updated {role_id} → channel:admins

    Args:
        db:             Sesja SQLAlchemy.
        redis:          Klient Redis.
        role_id:        ID roli.
        permission_ids: Nowa kompletna lista ID uprawnień (zastępuje starą).
        updated_by_id:  ID admina wykonującego operację.
        ip_address:     IP inicjatora.

    Returns:
        Słownik z zaktualizowanymi danymi roli (z pełną listą uprawnień).

    Raises:
        RoleNotFoundError:           Gdy rola nie istnieje.
        RolePermissionAssignError:   Gdy podane permission_ids zawierają nieistniejące ID.
    """
    role = await _get_role_with_permissions(db, role_id)
    if role is None or not role.is_active:
        raise RoleNotFoundError(f"Rola ID={role_id} nie istnieje lub jest nieaktywna.")

    # Zapisz stare uprawnienia do AuditLog
    old_permissions = sorted([
        p.permission_name for p in role.permissions if p.is_active
    ])

    # Walidacja: czy wszystkie podane permission_ids istnieją i są aktywne
    if permission_ids:
        valid_result = await db.execute(
            select(Permission.id_permission).where(
                and_(
                    Permission.id_permission.in_(permission_ids),
                    Permission.is_active == True,  # noqa: E712
                )
            )
        )
        valid_ids = {row[0] for row in valid_result.fetchall()}
        invalid_ids = set(permission_ids) - valid_ids
        if invalid_ids:
            raise RolePermissionAssignError(
                f"Następujące ID uprawnień nie istnieją lub są nieaktywne: {sorted(invalid_ids)}"
            )

    # --- ATOMOWA OPERACJA: DELETE + INSERT ---

    # 1. Usuń wszystkie istniejące RolePermissions dla tej roli
    await db.execute(
        delete(RolePermission).where(RolePermission.id_role == role_id)
    )

    # 2. Wstaw nowe (deduplikacja przez set)
    new_ids_dedup = sorted(set(permission_ids))
    now = datetime.now(timezone.utc)
    for perm_id in new_ids_dedup:
        new_rp = RolePermission(
            id_role=role_id,
            id_permission=perm_id,
            created_at=now,
        )
        db.add(new_rp)

    await db.flush()
    await db.commit()        # ← BRAKUJĄCE — zatwierdź transakcję

    # Pobierz nowy stan roli z uprawnieniami
    await db.refresh(role)
    role = await _get_role_with_permissions(db, role_id)

    new_permissions = sorted([
        p.permission_name for p in role.permissions if p.is_active  # type: ignore[union-attr]
    ])

    added = sorted(set(new_permissions) - set(old_permissions))
    removed = sorted(set(old_permissions) - set(new_permissions))

    logger.info(
        "Zaktualizowano uprawnienia roli",
        extra={
            "role_id": role_id,
            "role_name": role.role_name,
            "old_count": len(old_permissions),
            "new_count": len(new_permissions),
            "added": added,
            "removed": removed,
            "updated_by": updated_by_id,
            "ip_address": ip_address,
        }
    )

    # Inwalidacja cache ról
    await _invalidate_role_caches(redis, role_id)

    # Inwalidacja cache uprawnień użytkowników z tą rolą
    affected_users = await _invalidate_user_permission_caches(db, redis, role_id)

    # SSE event: permissions_updated
    await _publish_permissions_updated_event(redis, role_id)

    _append_to_file(
        _get_roles_log_file(),
        _build_log_record(
            action="role_permissions_updated",
            role_id=role_id,
            role_name=role.role_name,
            old_permissions=old_permissions,
            new_permissions=new_permissions,
            added=added,
            removed=removed,
            affected_users_cache_invalidated=affected_users,
            updated_by=updated_by_id,
            ip_address=ip_address,
        )
    )

    audit_service.log_crud(
        db=db,
        action="role_permissions_updated",
        entity_type="Role",
        entity_id=role_id,
        old_value={"permissions": old_permissions},
        new_value={"permissions": new_permissions},
        details={
            "added": added,
            "removed": removed,
            "affected_users": affected_users,
            "updated_by": updated_by_id,
        },
        success=True,
    )

    result = _role_to_dict(role, include_permissions=True)  # type: ignore[arg-type]
    result["user_count"] = await _count_users_with_role(db, role_id)
    result["permissions_change"] = {
        "added": added,
        "removed": removed,
        "affected_users_cache_invalidated": affected_users,
    }
    return result


async def get_users_with_role(
    db: AsyncSession,
    role_id: int,
    page: int = 1,
    page_size: int = 50,
) -> dict:
    """
    Zwraca listę użytkowników przypisanych do danej roli.

    Endpoint: GET /api/v1/roles/{id}/users — uprawnienie roles.view_users.

    Args:
        db:        Sesja SQLAlchemy.
        role_id:   ID roli.
        page:      Numer strony (1-based).
        page_size: Rozmiar strony (max 200).

    Returns:
        Słownik z paginowaną listą użytkowników i metadanymi.

    Raises:
        RoleNotFoundError: Gdy rola nie istnieje.
    """
    page_size = min(max(page_size, 1), 200)
    page      = max(page, 1)

    role_exists = await db.execute(
        select(func.count(Role.id_role)).where(
            and_(Role.id_role == role_id, Role.is_active == True)  # noqa: E712
        )
    )
    if (role_exists.scalar_one() or 0) == 0:
        raise RoleNotFoundError(f"Rola ID={role_id} nie istnieje lub jest nieaktywna.")

    # COUNT
    count_result = await db.execute(
        select(func.count(User.id_user)).where(
            and_(User.role_id == role_id, User.is_active == True)  # noqa: E712
        )
    )
    total = count_result.scalar_one() or 0

    # DATA
    data_result = await db.execute(
        select(User)
        .where(and_(User.role_id == role_id, User.is_active == True))  # noqa: E712
        .order_by(User.username.asc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    users = data_result.scalars().all()

    total_pages = (total + page_size - 1) // page_size if total > 0 else 0

    return {
        "role_id": role_id,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "items": [
            {
                "id_user": u.id_user,
                "username": u.username,
                "email": u.email,
                "full_name": u.full_name,
                "last_login_at": u.last_login_at.isoformat() if u.last_login_at else None,
            }
            for u in users
        ],
    }