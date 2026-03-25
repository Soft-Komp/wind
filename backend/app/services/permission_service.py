"""
Serwis Uprawnień — System Windykacja
======================================

Odpowiedzialność:
    - Listowanie uprawnień (pogrupowane po Category)
    - Pobieranie szczegółów pojedynczego uprawnienia
    - Sprawdzanie uprawnień użytkownika z cache Redis
    - Batch check wielu uprawnień jednocześnie (redis.mget)
    - Inwalidacja cache uprawnień per-user i per-role
    - Cache Redis: permissions:list (TTL 600s), perm:{uid}:{name} (TTL 300s)
    - Plik logów permission_YYYY-MM-DD.jsonl (tylko operacje zapisu/inwalidacji)

Decyzje projektowe:
    - Cache uprawnień per-user per-permission zamiast per-user-all:
      Granularność pozwala na inwalidację selektywną (zmiana jednego uprawnienia
      nie wymaga przeładowania całego zestawu).
    - check() i check_many() są operacjami READ — nie logują do AuditLog
      (zbyt dużo szumu; RBAC jest sprawdzany przy każdym requeście).
    - mget() dla batch check — jedno round-trip do Redis zamiast N.
    - Przy błędzie Redis w check() → fallback do DB (fail-closed dla bezpieczeństwa).
      Odwrotnie niż blacklista JWT (tam fail-open) — tutaj lepiej odmówić dostępu
      niż przyznać go przez przypadek przy awarii Redis.

Zależności:
    - services/audit_service.py (tylko przy mutacjach)

"""

from __future__ import annotations

import logging
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import orjson
from redis.asyncio import Redis
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.permission import Permission
from app.db.models.role import Role
from app.db.models.role_permission import RolePermission
from app.db.models.user import User
from app.services import audit_service

# ---------------------------------------------------------------------------
# Logger własny modułu
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------

# TTL cache Redis w sekundach
_CACHE_PERMISSIONS_LIST_TTL: int = 600   # 10 min — lista uprawnień rzadko się zmienia
_CACHE_PERM_CHECK_TTL: int = 300         # 5 min — cache sprawdzania uprawnienia per-user

# Klucze Redis
_REDIS_KEY_PERMISSIONS_LIST = "permissions:list"
_REDIS_KEY_PERM_CHECK       = "perm:{user_id}:{permission_name}"  # wartość: "1" lub "0"

# Wartości cache — string (Redis przechowuje tylko stringi)
_CACHE_PERM_ALLOWED: str = "1"
_CACHE_PERM_DENIED: str  = "0"

# Plik logów (tylko inwalidacje i operacje zapisu — nie check)
_PERM_LOG_FILE_PATTERN = "logs/permissions_{date}.jsonl"


# ===========================================================================
# Klasy wyjątków
# ===========================================================================

class PermissionError(Exception):
    """Bazowy wyjątek serwisu uprawnień."""


class PermissionNotFoundError(PermissionError):
    """Uprawnienie nie istnieje lub jest nieaktywne."""


class PermissionValidationError(PermissionError):
    """Błąd walidacji danych wejściowych."""


# ===========================================================================
# Funkcje pomocnicze — prywatne
# ===========================================================================

def _get_log_dir() -> Path:
    """Zwraca i tworzy katalog logów."""
    p = Path("logs")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _get_perm_log_file() -> Path:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return _get_log_dir() / f"permissions_{today}.jsonl"


def _append_to_file(filepath: Path, record: dict) -> None:
    """Dopisuje rekord JSON do pliku JSON Lines (append-only)."""
    try:
        line = orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE)
        with filepath.open("ab") as f:
            f.write(line)
    except OSError as exc:
        logger.warning(
            "Nie można zapisać do pliku logu uprawnień",
            extra={"filepath": str(filepath), "error": str(exc)}
        )


def _build_log_record(action: str, **kwargs) -> dict:
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": "permission_service",
        "action": action,
        **kwargs,
    }


def _sanitize_permission_name(name: str) -> str:
    """
    Normalizuje i waliduje nazwę uprawnienia.

    Format oczekiwany: `kategoria.akcja` (lowercase, tylko alfanumeryczne i kropka).

    Args:
        name: Nazwa uprawnienia do walidacji.

    Returns:
        Znormalizowana nazwa.

    Raises:
        PermissionValidationError: Gdy nazwa jest pusta lub ma nieprawidłowy format.
    """
    normalized = unicodedata.normalize("NFC", name.strip().lower())
    if not normalized:
        raise PermissionValidationError("Nazwa uprawnienia nie może być pusta.")
    if "." not in normalized:
        raise PermissionValidationError(
            f"Nazwa uprawnienia musi mieć format 'kategoria.akcja'. "
            f"Otrzymano: {name!r}"
        )
    if len(normalized) > 100:
        raise PermissionValidationError(
            f"Nazwa uprawnienia przekracza 100 znaków: {len(normalized)}"
        )
    return normalized


def _permission_to_dict(permission: Permission) -> dict:
    """Konwertuje obiekt Permission na słownik."""
    return {
        "id_permission": permission.id_permission,
        "permission_name": permission.permission_name,
        "description": permission.description,
        "category": permission.category,
        "is_active": permission.is_active,
        "created_at": permission.created_at.isoformat() if permission.created_at else None,
        "updated_at": permission.updated_at.isoformat() if permission.updated_at else None,
    }


async def _get_redis_cache(redis: Redis, key: str) -> Optional[bytes]:
    """Pobiera surowe bajty z Redis. Zwraca None przy braku lub błędzie."""
    try:
        return await redis.get(key)
    except Exception:
        return None


async def _set_redis_cache(redis: Redis, key: str, data: bytes, ttl: int) -> None:
    """Zapisuje bajty do Redis. Błędy logowane jako debug."""
    try:
        await redis.set(key, data, ex=ttl)
    except Exception as exc:
        logger.debug("Błąd zapisu do cache Redis", extra={"key": key, "error": str(exc)})


async def _fetch_user_permissions_from_db(
    db: AsyncSession,
    user_id: int,
) -> set[str]:
    """
    Pobiera WSZYSTKIE aktywne uprawnienia użytkownika bezpośrednio z bazy.

    Używane jako fallback gdy Redis jest niedostępny, lub do wypełnienia cache.

    Query: Users.role_id → RolePermissions.id_role → Permissions.permission_name

    Args:
        db:      Sesja SQLAlchemy.
        user_id: ID użytkownika.

    Returns:
        Zbiór nazw aktywnych uprawnień użytkownika.
    """
    result = await db.execute(
        select(Permission.permission_name)
        .join(RolePermission, RolePermission.id_permission == Permission.id_permission)
        .join(User, User.role_id == RolePermission.id_role)
        .where(
            and_(
                User.id_user == user_id,
                User.is_active == True,        # noqa: E712
                Permission.is_active == True,  # noqa: E712
            )
        )
    )
    return {row[0] for row in result.fetchall()}


async def _warm_user_permission_cache(
    db: AsyncSession,
    redis: Redis,
    user_id: int,
    permissions: set[str],
) -> None:
    """
    Wypełnia cache Redis uprawnieniami użytkownika po pobraniu z DB.

    Dla każdego uprawnienia z `permissions` → ustawia perm:{uid}:{perm} = "1".
    NIE ustawia "0" dla brakujących (zbyt duże ryzyko zaśmiecenia Redis).
    Brak klucza w cache = sprawdź DB.

    Args:
        db:          Sesja SQLAlchemy (nieużywane — dla spójności API).
        redis:       Klient Redis.
        user_id:     ID użytkownika.
        permissions: Zbiór uprawnień do zapisania w cache.
    """
    if not permissions:
        return

    try:
        # Używamy pipeline dla efektywności — N SET w jednym round-trip
        pipeline = redis.pipeline()
        for perm_name in permissions:
            key = _REDIS_KEY_PERM_CHECK.format(
                user_id=user_id,
                permission_name=perm_name,
            )
            pipeline.set(key, _CACHE_PERM_ALLOWED, ex=_CACHE_PERM_CHECK_TTL)
        await pipeline.execute()

        logger.debug(
            "Cache uprawnień użytkownika wypełniony",
            extra={"user_id": user_id, "permissions_cached": len(permissions)}
        )
    except Exception as exc:
        logger.warning(
            "Nie udało się wypełnić cache uprawnień użytkownika",
            extra={"user_id": user_id, "error": str(exc)}
        )


# ===========================================================================
# Publiczne API serwisu — READ
# ===========================================================================

async def get_list(
    db: AsyncSession,
    redis: Redis,
) -> dict[str, list[dict]]:
    """
    Pobiera listę wszystkich aktywnych uprawnień, pogrupowaną po Category.

    Format zwrotu: {kategoria: [lista uprawnień]}
    Przykład:
        {
            "auth": [{"id_permission": 1, "permission_name": "auth.login", ...}],
            "users": [...],
            ...
        }

    Cache: permissions:list (TTL 600s) — inwalidowany przy każdej zmianie uprawnień.

    Args:
        db:    Sesja SQLAlchemy.
        redis: Klient Redis.

    Returns:
        Słownik kategorii → lista uprawnień.
    """
    raw_cached = await _get_redis_cache(redis, _REDIS_KEY_PERMISSIONS_LIST)
    if raw_cached:
        logger.debug("Lista uprawnień pobrana z cache Redis")
        return orjson.loads(raw_cached)

    result = await db.execute(
        select(Permission)
        .where(Permission.is_active == True)  # noqa: E712
        .order_by(Permission.category.asc(), Permission.permission_name.asc())
    )
    permissions = result.scalars().all()

    # Grupowanie po kategorii
    grouped: dict[str, list[dict]] = {}
    for perm in permissions:
        cat = perm.category or "other"
        grouped.setdefault(cat, []).append(_permission_to_dict(perm))

    await _set_redis_cache(
        redis,
        _REDIS_KEY_PERMISSIONS_LIST,
        orjson.dumps(grouped),
        _CACHE_PERMISSIONS_LIST_TTL,
    )

    total = sum(len(v) for v in grouped.values())
    logger.debug(
        "Lista uprawnień pobrana z bazy",
        extra={"total": total, "categories": list(grouped.keys())}
    )
    return grouped


async def get_by_id(
    db: AsyncSession,
    permission_id: int,
) -> dict:
    """
    Pobiera szczegóły pojedynczego uprawnienia po ID.

    Bez cache — uprawnienia rzadko pobierane pojedynczo,
    a szczegóły są już w liście z get_list().

    Args:
        db:            Sesja SQLAlchemy.
        permission_id: ID uprawnienia.

    Returns:
        Słownik z danymi uprawnienia.

    Raises:
        PermissionNotFoundError: Gdy uprawnienie nie istnieje lub jest nieaktywne.
    """
    result = await db.execute(
        select(Permission).where(Permission.id_permission == permission_id)
    )
    perm = result.scalar_one_or_none()

    if perm is None or not perm.is_active:
        raise PermissionNotFoundError(
            f"Uprawnienie ID={permission_id} nie istnieje lub jest nieaktywne."
        )

    return _permission_to_dict(perm)


async def get_by_name(
    db: AsyncSession,
    permission_name: str,
) -> Optional[dict]:
    """
    Pobiera uprawnienie po nazwie (permission_name).

    Args:
        db:              Sesja SQLAlchemy.
        permission_name: Nazwa uprawnienia (np. "auth.login").

    Returns:
        Słownik z danymi uprawnienia lub None jeśli nie istnieje.
    """
    name = _sanitize_permission_name(permission_name)
    result = await db.execute(
        select(Permission).where(
            and_(Permission.permission_name == name, Permission.is_active == True)  # noqa: E712
        )
    )
    perm = result.scalar_one_or_none()
    return _permission_to_dict(perm) if perm else None


async def get_flat_list(
    db: AsyncSession,
    redis: Redis,
) -> list[str]:
    """
    Zwraca płaską listę nazw wszystkich aktywnych uprawnień.

    Przydatna przy walidacji permission_name w request body.
    Używa tego samego cache co get_list() — parsuje go.

    Args:
        db:    Sesja SQLAlchemy.
        redis: Klient Redis.

    Returns:
        Posortowana lista stringów (permission_name).
    """
    grouped = await get_list(db, redis)
    flat: list[str] = []
    for perms in grouped.values():
        flat.extend(p["permission_name"] for p in perms)
    return sorted(flat)


# ===========================================================================
# Sprawdzanie uprawnień (RBAC)
# ===========================================================================

async def check(
    db: AsyncSession,
    redis: Redis,
    user_id: int,
    permission_name: str,
) -> bool:
    """
    Sprawdza czy użytkownik ma dane uprawnienie.

    Strategia cache (fail-closed przy błędzie Redis):
        1. Sprawdź Redis: perm:{user_id}:{permission_name}
           - Hit "1" → True (allowed)
           - Hit "0" → False (denied) — UWAGA: nie ustawiamy "0" aktywnie,
             ale obsługujemy na wypadek zewnętrznych operacji
           - Miss → sprawdź DB
        2. Jeśli miss → pobierz WSZYSTKIE uprawnienia usera z DB
        3. Warm cache dla wszystkich uprawnień (pipeline SET)
        4. Zwróć wynik

    Fail-closed: błąd Redis → sprawdź DB. Bezpieczeństwo > dostępność.

    Args:
        db:              Sesja SQLAlchemy.
        redis:           Klient Redis.
        user_id:         ID użytkownika.
        permission_name: Nazwa uprawnienia do sprawdzenia.

    Returns:
        True jeśli użytkownik ma uprawnienie, False w przeciwnym razie.
    """
    try:
        name = _sanitize_permission_name(permission_name)
    except PermissionValidationError:
        logger.warning(
            "Nieprawidłowy format nazwy uprawnienia w check()",
            extra={"user_id": user_id, "permission_name": permission_name}
        )
        return False

    cache_key = _REDIS_KEY_PERM_CHECK.format(
        user_id=user_id,
        permission_name=name,
    )

    # --- Sprawdź cache Redis ---
    raw = await _get_redis_cache(redis, cache_key)
    if raw is not None:
        cached_val = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        result = cached_val == _CACHE_PERM_ALLOWED
        logger.debug(
            "Uprawnienie sprawdzone z cache Redis",
            extra={"user_id": user_id, "permission": name, "result": result}
        )
        return result

    # --- Cache miss → sprawdź DB ---
    logger.debug(
        "Cache miss dla uprawnienia — sprawdzam bazę danych",
        extra={"user_id": user_id, "permission": name}
    )

    user_permissions = await _fetch_user_permissions_from_db(db, user_id)
    has_permission = name in user_permissions

    # Warm cache dla wszystkich uprawnień usera
    await _warm_user_permission_cache(db, redis, user_id, user_permissions)

    # Jeśli uprawnienie NIE istnieje w zestawie — ustaw "0" żeby uniknąć
    # kolejnych zapytań do DB dla tego samego uprawnienia
    if not has_permission:
        try:
            await redis.set(cache_key, _CACHE_PERM_DENIED, ex=_CACHE_PERM_CHECK_TTL)
        except Exception:
            pass

    return has_permission


async def check_many(
    db: AsyncSession,
    redis: Redis,
    user_id: int,
    permission_names: list[str],
) -> dict[str, bool]:
    """
    Batch check wielu uprawnień dla jednego użytkownika.

    Wydajniejszy niż N × check() — używa redis.mget() dla jednego round-trip.
    Dla brakujących w cache → fallback do DB i warm cache.

    Args:
        db:               Sesja SQLAlchemy.
        redis:            Klient Redis.
        user_id:          ID użytkownika.
        permission_names: Lista nazw uprawnień do sprawdzenia.

    Returns:
        Słownik {permission_name: bool}.
    """
    if not permission_names:
        return {}

    # Sanityzacja wszystkich nazw
    sanitized: list[str] = []
    for name in permission_names:
        try:
            sanitized.append(_sanitize_permission_name(name))
        except PermissionValidationError:
            logger.warning(
                "Pominięto nieprawidłową nazwę uprawnienia w check_many()",
                extra={"user_id": user_id, "permission_name": name}
            )

    if not sanitized:
        return {}

    # Buduj klucze Redis dla wszystkich uprawnień
    cache_keys = [
        _REDIS_KEY_PERM_CHECK.format(user_id=user_id, permission_name=n)
        for n in sanitized
    ]

    results: dict[str, bool] = {}
    missing_names: list[str] = []

    # --- Próba Redis mget (jeden round-trip) ---
    try:
        cached_values = await redis.mget(*cache_keys)
        for perm_name, cached_val in zip(sanitized, cached_values):
            if cached_val is not None:
                val = cached_val.decode("utf-8") if isinstance(cached_val, bytes) else cached_val
                results[perm_name] = (val == _CACHE_PERM_ALLOWED)
            else:
                missing_names.append(perm_name)
    except Exception as exc:
        logger.warning(
            "Błąd Redis w check_many() — fallback do DB dla wszystkich",
            extra={"user_id": user_id, "error": str(exc)}
        )
        missing_names = sanitized

    # --- Fallback do DB dla brakujących w cache ---
    if missing_names:
        user_permissions = await _fetch_user_permissions_from_db(db, user_id)
        await _warm_user_permission_cache(db, redis, user_id, user_permissions)

        for perm_name in missing_names:
            has = perm_name in user_permissions
            results[perm_name] = has
            # Ustaw "0" dla brakujących (żeby uniknąć ponownych zapytań do DB)
            if not has:
                try:
                    cache_key = _REDIS_KEY_PERM_CHECK.format(
                        user_id=user_id, permission_name=perm_name
                    )
                    await redis.set(cache_key, _CACHE_PERM_DENIED, ex=_CACHE_PERM_CHECK_TTL)
                except Exception:
                    pass

    logger.debug(
        "Batch check uprawnień zakończony",
        extra={
            "user_id": user_id,
            "checked": len(sanitized),
            "from_cache": len(sanitized) - len(missing_names),
            "from_db": len(missing_names),
            "granted": sum(1 for v in results.values() if v),
        }
    )

    return results


async def require(
    db: AsyncSession,
    redis: Redis,
    user_id: int,
    permission_name: str,
) -> None:
    """
    Sprawdza uprawnienie i rzuca wyjątek jeśli brak.

    Convenience wrapper nad check() do użycia w endpointach jako Depends().
    Zamiast `if not await check(...)` → `await require(...)`.

    Args:
        db:              Sesja SQLAlchemy.
        redis:           Klient Redis.
        user_id:         ID użytkownika.
        permission_name: Wymagane uprawnienie.

    Raises:
        PermissionDeniedError: Gdy użytkownik nie ma wymaganego uprawnienia.
    """
    has_permission = await check(db, redis, user_id, permission_name)
    if not has_permission:
        logger.warning(
            "Odmowa dostępu — brak wymaganego uprawnienia",
            extra={"user_id": user_id, "required_permission": permission_name}
        )
        raise PermissionDeniedError(
            f"Brak wymaganego uprawnienia: '{permission_name}'"
        )


class PermissionDeniedError(PermissionError):
    """
    Użytkownik nie ma wymaganego uprawnienia.

    Rzucany przez require() — mapowany na HTTP 403 w warstwie API.
    """


# ===========================================================================
# Inwalidacja cache
# ===========================================================================

async def invalidate_for_user(
    redis: Redis,
    user_id: int,
) -> int:
    """
    Inwaliduje CAŁY cache uprawnień dla danego użytkownika.

    Usuwa wszystkie klucze perm:{user_id}:* z Redis.
    Używana przy:
        - Zmianie roli użytkownika (user_service.update)
        - Dezaktywacji konta
        - Wymuszonym wylogowaniu

    Args:
        redis:   Klient Redis.
        user_id: ID użytkownika.

    Returns:
        Liczba usuniętych kluczy Redis.
    """
    pattern = f"perm:{user_id}:*"
    deleted_count = 0

    try:
        async for key in redis.scan_iter(pattern):
            await redis.delete(key)
            deleted_count += 1

        if deleted_count > 0:
            logger.info(
                "Zinwalidowano cache uprawnień użytkownika",
                extra={"user_id": user_id, "deleted_keys": deleted_count}
            )
            _append_to_file(
                _get_perm_log_file(),
                _build_log_record(
                    action="permission_cache_invalidated_for_user",
                    user_id=user_id,
                    deleted_keys=deleted_count,
                )
            )
    except Exception as exc:
        logger.warning(
            "Błąd inwalidacji cache uprawnień użytkownika",
            extra={"user_id": user_id, "error": str(exc)}
        )

    return deleted_count


async def invalidate_for_role(
    db: AsyncSession,
    redis: Redis,
    role_id: int,
) -> dict:
    """
    Inwaliduje cache uprawnień wszystkich użytkowników z daną rolą.

    Deleguje do invalidate_for_user() dla każdego znalezionego usera.

    Wywoływana po:
        - role_service.assign_permissions()
        - Zmianie roli użytkownika

    Args:
        db:      Sesja SQLAlchemy.
        redis:   Klient Redis.
        role_id: ID roli.

    Returns:
        Słownik z liczbą dotkniętych użytkowników i usuniętych kluczy.
    """
    result = await db.execute(
        select(User.id_user).where(
            and_(User.role_id == role_id, User.is_active == True)  # noqa: E712
        )
    )
    user_ids = [row[0] for row in result.fetchall()]

    total_deleted = 0
    for user_id in user_ids:
        deleted = await invalidate_for_user(redis, user_id)
        total_deleted += deleted

    logger.info(
        "Zinwalidowano cache uprawnień dla roli",
        extra={
            "role_id": role_id,
            "affected_users": len(user_ids),
            "total_deleted_keys": total_deleted,
        }
    )

    _append_to_file(
        _get_perm_log_file(),
        _build_log_record(
            action="permission_cache_invalidated_for_role",
            role_id=role_id,
            affected_users=len(user_ids),
            total_deleted_keys=total_deleted,
        )
    )

    return {
        "affected_users": len(user_ids),
        "total_deleted_keys": total_deleted,
        "user_ids": user_ids,
    }


async def invalidate_permissions_list_cache(redis: Redis) -> None:
    """
    Inwaliduje cache listy uprawnień (permissions:list).

    Wywoływana gdy zmienia się lista uprawnień (rzadko).

    Args:
        redis: Klient Redis.
    """
    try:
        await redis.delete(_REDIS_KEY_PERMISSIONS_LIST)
        logger.info("Cache listy uprawnień zinwalidowany")
    except Exception as exc:
        logger.warning(
            "Błąd inwalidacji cache listy uprawnień",
            extra={"error": str(exc)}
        )


# ===========================================================================
# Diagnostyka i raportowanie
# ===========================================================================

async def get_cache_stats(
    redis: Redis,
    user_id: int,
) -> dict:
    """
    Zwraca statystyki cache uprawnień dla danego użytkownika.

    Przydatne do diagnostyki i monitoringu.

    Args:
        redis:   Klient Redis.
        user_id: ID użytkownika.

    Returns:
        Słownik ze statystykami cache.
    """
    pattern = f"perm:{user_id}:*"
    cached_permissions: list[str] = []
    allowed: list[str] = []
    denied: list[str] = []

    try:
        async for key in redis.scan_iter(pattern):
            key_str = key.decode("utf-8") if isinstance(key, bytes) else key
            # Wyciągnij nazwę uprawnienia z klucza (perm:{user_id}:{perm_name})
            parts = key_str.split(":", 2)
            if len(parts) == 3:
                perm_name = parts[2]
                cached_permissions.append(perm_name)
                val = await redis.get(key)
                val_str = val.decode("utf-8") if isinstance(val, bytes) else (val or "")
                if val_str == _CACHE_PERM_ALLOWED:
                    allowed.append(perm_name)
                else:
                    denied.append(perm_name)
    except Exception as exc:
        return {"error": str(exc), "user_id": user_id}

    return {
        "user_id": user_id,
        "total_cached": len(cached_permissions),
        "allowed_count": len(allowed),
        "denied_count": len(denied),
        "allowed": sorted(allowed),
        "denied": sorted(denied),
        "cache_ttl_seconds": _CACHE_PERM_CHECK_TTL,
    }


async def get_role_permission_summary(
    db: AsyncSession,
    role_id: int,
) -> dict:
    """
    Zwraca podsumowanie uprawnień dla danej roli — bezpośrednio z bazy.

    Bez cache — do użycia w endpointach diagnostycznych.

    Args:
        db:      Sesja SQLAlchemy.
        role_id: ID roli.

    Returns:
        Słownik z podsumowaniem uprawnień roli.
    """
    result = await db.execute(
        select(Permission)
        .join(RolePermission, RolePermission.id_permission == Permission.id_permission)
        .where(
            and_(
                RolePermission.id_role == role_id,
                Permission.is_active == True,  # noqa: E712
            )
        )
        .order_by(Permission.category.asc(), Permission.permission_name.asc())
    )
    permissions = result.scalars().all()

    grouped: dict[str, list[str]] = {}
    for perm in permissions:
        cat = perm.category or "other"
        grouped.setdefault(cat, []).append(perm.permission_name)

    return {
        "role_id": role_id,
        "total_permissions": len(permissions),
        "by_category": grouped,
    }


async def count_users_with_permission(
    db: AsyncSession,
    permission_name: str,
) -> int:
    """
    Liczy użytkowników którzy mają dane uprawnienie (przez swoje role).

    Przydatne do weryfikacji przed usunięciem uprawnienia.

    Args:
        db:              Sesja SQLAlchemy.
        permission_name: Nazwa uprawnienia.

    Returns:
        Liczba użytkowników z tym uprawnieniem.
    """
    name = _sanitize_permission_name(permission_name)
    result = await db.execute(
        select(func.count(User.id_user))
        .join(RolePermission, RolePermission.id_role == User.role_id)
        .join(Permission, Permission.id_permission == RolePermission.id_permission)
        .where(
            and_(
                Permission.permission_name == name,
                Permission.is_active == True,   # noqa: E712
                User.is_active == True,          # noqa: E712
            )
        )
    )
    return result.scalar_one() or 0

# ===========================================================================
# Dwuetapowe usunięcie uprawnienia
# ===========================================================================

class PermissionDeleteTokenError(Exception):
    """Token potwierdzający wygasł lub został już użyty."""

class PermissionHasRolesError(Exception):
    """Uprawnienie jest przypisane do aktywnych ról."""


_REDIS_KEY_PERM_DELETE = "perm_delete_jti:{jti}"
_DEFAULT_PERM_DELETE_TTL = 60


async def initiate_delete(
    db: AsyncSession,
    redis: Redis,
    permission_id: int,
    initiated_by_id: int,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Krok 1 dwuetapowego usunięcia uprawnienia.

    Sprawdza czy uprawnienie istnieje i nie jest przypisane do ról.
    Generuje jednorazowy token JWT i zapisuje JTI w Redis.
    """
    import secrets as _secrets
    from datetime import timedelta
    from jose import jwt as _jwt
    from app.core.config import settings as _settings

    # Pobierz uprawnienie
    result = await db.execute(
        select(Permission).where(
            and_(Permission.id_permission == permission_id, Permission.is_active == True)  # noqa: E712
        )
    )
    perm = result.scalar_one_or_none()
    if perm is None:
        raise PermissionNotFoundError(f"Uprawnienie ID={permission_id} nie istnieje.")

    # Sprawdź przypisania do ról
    role_count_result = await db.execute(
        select(func.count(RolePermission.id_role)).where(
            RolePermission.id_permission == permission_id
        )
    )
    role_count = role_count_result.scalar_one() or 0

    warning = None
    if role_count > 0:
        warning = (
            f"Uprawnienie '{perm.permission_name}' jest przypisane do {role_count} ról — "
            f"zostanie z nich usunięte po potwierdzeniu."
        )

    # Generuj token
    ttl = _DEFAULT_PERM_DELETE_TTL
    jti = _secrets.token_hex(16)
    now = datetime.now(timezone.utc)
    token_payload = {
        "sub":           str(permission_id),
        "scope":         "delete_permission",
        "permission_id": permission_id,
        "initiated_by":  initiated_by_id,
        "jti":           jti,
        "iat":           int(now.timestamp()),
        "exp":           int((now + timedelta(seconds=ttl)).timestamp()),
    }
    secret = (
        _settings.secret_key.get_secret_value()
        if hasattr(_settings.secret_key, "get_secret_value")
        else str(_settings.secret_key)
    )
    token = _jwt.encode(token_payload, secret, algorithm=_settings.algorithm)

    # Zapisz JTI w Redis
    await redis.set(_REDIS_KEY_PERM_DELETE.format(jti=jti), str(permission_id), ex=ttl)

    logger.warning(
        "Inicjacja usunięcia uprawnienia — krok 1",
        extra={
            "permission_id":   permission_id,
            "permission_name": perm.permission_name,
            "role_count":      role_count,
            "initiated_by":    initiated_by_id,
            "ip_address":      ip_address,
        }
    )

    _append_to_file(
        _get_perm_log_file(),
        _build_log_record(
            action="permission_delete_initiated",
            permission_id=permission_id,
            permission_name=perm.permission_name,
            role_count=role_count,
            initiated_by=initiated_by_id,
            ip_address=ip_address,
        )
    )

    return {
        "token":           token,
        "expires_in":      ttl,
        "permission_id":   permission_id,
        "permission_name": perm.permission_name,
        "warning":         warning,
    }


async def confirm_delete(
    db: AsyncSession,
    redis: Redis,
    permission_id: int,
    confirm_token: str,
    confirmed_by_id: int,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Krok 2 dwuetapowego usunięcia uprawnienia.

    Weryfikuje token, unieważnia JTI, wykonuje soft-delete,
    inwaliduje cache i zapisuje AuditLog.
    """
    from jose import jwt as _jwt, JWTError as _JWTError
    from app.core.config import settings as _settings
    from sqlalchemy import delete as sa_delete

    # Zweryfikuj token
    secret = (
        _settings.secret_key.get_secret_value()
        if hasattr(_settings.secret_key, "get_secret_value")
        else str(_settings.secret_key)
    )
    try:
        payload = _jwt.decode(confirm_token, secret, algorithms=[_settings.algorithm])
    except _JWTError:
        raise PermissionDeleteTokenError(
            "Token potwierdzający wygasł lub jest nieprawidłowy."
        )

    if (
        payload.get("scope") != "delete_permission"
        or payload.get("permission_id") != permission_id
    ):
        raise PermissionDeleteTokenError("Token nie dotyczy tej operacji.")

    # Sprawdź jednorazowość JTI
    jti = payload.get("jti", "")
    jti_key = _REDIS_KEY_PERM_DELETE.format(jti=jti)
    if not await redis.exists(jti_key):
        raise PermissionDeleteTokenError(
            "Token potwierdzający wygasł lub został już użyty."
        )
    await redis.delete(jti_key)

    # Pobierz uprawnienie
    result = await db.execute(
        select(Permission).where(
            and_(Permission.id_permission == permission_id, Permission.is_active == True)  # noqa: E712
        )
    )
    perm = result.scalar_one_or_none()
    if perm is None:
        raise PermissionNotFoundError(f"Uprawnienie ID={permission_id} nie istnieje.")

    old_value = {
        "id_permission":   perm.id_permission,
        "permission_name": perm.permission_name,
        "description":     perm.description,
        "category":        perm.category,
        "is_active":       True,
    }

    # Usuń przypisania do ról (fizycznie — RolePermission nie ma soft-delete)
    await db.execute(
        sa_delete(RolePermission).where(RolePermission.id_permission == permission_id)
    )

    # Soft-delete
    perm.is_active = False
    await db.flush()
    await db.commit()

    # Inwalidacja cache
    await invalidate_permissions_list_cache(redis)

    logger.warning(
        "Uprawnienie usunięte (soft-delete) — RolePermissions fizycznie usunięte",
        extra={
            "permission_id":   permission_id,
            "permission_name": perm.permission_name,
            "confirmed_by":    confirmed_by_id,
            "ip_address":      ip_address,
        }
    )

    _append_to_file(
        _get_perm_log_file(),
        _build_log_record(
            action="permission_deleted",
            permission_id=permission_id,
            permission_name=perm.permission_name,
            confirmed_by=confirmed_by_id,
            ip_address=ip_address,
        )
    )

    from app.services import audit_service
    audit_service.log_crud(
        db=db,
        action="permission_deleted",
        entity_type="Permission",
        entity_id=permission_id,
        old_value=old_value,
        new_value={"is_active": False},
        details={
            "permission_name": perm.permission_name,
            "confirmed_by":    confirmed_by_id,
        },
        success=True,
    )

    return {
        "permission_id":   permission_id,
        "permission_name": perm.permission_name,
        "message":         f"Uprawnienie '{perm.permission_name}' zostało trwale dezaktywowane.",
    }    