"""
Serwis konfiguracji dynamicznej systemu.

Architektura:
    ┌─────────────┐    hit     ┌───────────────────┐
    │   caller    │ ─────────► │   Redis cache     │
    │  (service)  │            │  TTL=3600s        │
    └─────────────┘            └───────────────────┘
           │ miss / stale               │ miss
           ▼                            ▼
    ┌──────────────────────────────────────────────┐
    │        dbo_ext.skw_SystemConfig (MSSQL)          │
    │  ConfigKey (UNIQUE)  │  ConfigValue (TEXT)   │
    └──────────────────────────────────────────────┘

Zasady:
    1. Redis jako cache L1 (TTL 3600s domyślnie, 300s dla CORS)
    2. Fallback na bazę gdy Redis niedostępny — NIGDY nie crashujemy
    3. Każda zmiana konfiguracji:
         a) UPDATE w bazie
         b) Invalidacja cache Redis (nie update — wymuszamy świeży odczyt)
         c) AuditLog(action="config_updated")
         d) Zapis do logs/config_changes_YYYY.jsonl (append-only, roczny)
    4. Klucze wrażliwe (pin_hash) — maskowane w logach
    5. Walidacja typów i wartości przed zapisem
    6. Thread-safe, async-native

Klucze konfiguracji (z 05_system_config.sql):
    cors.allowed_origins       — string CSV z origins
    otp.expiry_minutes         — int
    delete_token.ttl_seconds   — int
    impersonation.max_hours    — int
    master_key.enabled         — bool
    master_key.pin_hash        — string (bcrypt hash, WRAŻLIWY)
    schema_integrity.reaction  — enum: WARN/ALERT/BLOCK
    snapshot.retention_days    — int

"""
from __future__ import annotations

import json
import logging
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union

import orjson
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services import audit_service

# ---------------------------------------------------------------------------
# Logger modułu
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------

# Prefiks kluczy Redis dla konfiguracji
_CACHE_PREFIX: str = "config:"

# TTL cache (sekundy)
_DEFAULT_TTL: int = 3600       # 1 godzina dla większości kluczy
_CORS_TTL: int = 300           # 5 minut dla CORS (bezpieczeństwo)
_ALL_CONFIGS_TTL: int = 300    # 5 minut dla get_all()

# Klucz cache dla get_all()
_ALL_CONFIGS_CACHE_KEY: str = "config:__all__"

# Klucze wrażliwe — maskowane w logach i AuditLog
_SENSITIVE_CONFIG_KEYS: frozenset[str] = frozenset({
    "master_key.pin_hash",
    "master_key.secret",
})

# Dozwolone wartości dla kluczy enum
_ENUM_CONSTRAINTS: dict[str, frozenset[str]] = {
    "schema_integrity.reaction": frozenset({"WARN", "ALERT", "BLOCK"}),
}

# Ograniczenia zakresów dla kluczy int
_INT_RANGE_CONSTRAINTS: dict[str, tuple[int, int]] = {
    "otp.expiry_minutes":        (1, 1440),       # 1min – 24h
    "delete_token.ttl_seconds":  (10, 600),       # 10s – 10min
    "impersonation.max_hours":   (1, 72),         # 1h – 3 dni
    "snapshot.retention_days":   (1, 365),        # 1 dzień – rok
}

# Klucze boolean
_BOOL_CONFIG_KEYS: frozenset[str] = frozenset({
    "master_key.enabled",
})

# Klucze integer
_INT_CONFIG_KEYS: frozenset[str] = frozenset({
    "otp.expiry_minutes",
    "delete_token.ttl_seconds",
    "impersonation.max_hours",
    "snapshot.retention_days",
})

# Ścieżka do pliku zmian konfiguracji (roczna rotacja)
def _get_config_changes_log_path() -> Path:
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    year = datetime.now(timezone.utc).strftime("%Y")
    return log_dir / f"config_changes_{year}.jsonl"


def _write_config_change_log(
    key: str,
    old_value: Optional[str],
    new_value: str,
    user_id: Optional[int],
    username: Optional[str],
) -> None:
    """
    Append-only log zmian konfiguracji do pliku rocznego.
    Wartości wrażliwe są maskowane.
    """
    path = _get_config_changes_log_path()
    is_sensitive = key in _SENSITIVE_CONFIG_KEYS
    try:
        record = {
            "event": "config_changed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "key": key,
            "old_value": "[REDACTED]" if is_sensitive else old_value,
            "new_value": "[REDACTED]" if is_sensitive else new_value,
            "changed_by": {
                "user_id": user_id,
                "username": username,
            },
        }
        line = orjson.dumps(record).decode("utf-8")
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError as exc:
        print(
            f"[CONFIG_SERVICE] Błąd zapisu config_changes log do {path}: {exc}",
            file=sys.stderr,
        )


# ---------------------------------------------------------------------------
# Helpers: cache key, TTL wybór
# ---------------------------------------------------------------------------

def _cache_key(config_key: str) -> str:
    """Zwraca klucz Redis dla danego klucza konfiguracji."""
    return f"{_CACHE_PREFIX}{config_key}"


def _ttl_for_key(config_key: str) -> int:
    """Zwraca odpowiedni TTL cache dla danego klucza."""
    if config_key.startswith("cors."):
        return _CORS_TTL
    return _DEFAULT_TTL


def _mask_value(key: str, value: Optional[str]) -> str:
    """Maskuje wartość wrażliwych kluczy do logów."""
    if key in _SENSITIVE_CONFIG_KEYS:
        return "[REDACTED]"
    if value is None:
        return "None"
    return value[:50] + ("..." if len(value) > 50 else "")


# ---------------------------------------------------------------------------
# Redis helpers — z graceful fallback
# ---------------------------------------------------------------------------

async def _redis_get(redis: Optional[Redis], key: str) -> Optional[str]:
    """
    Pobiera wartość z Redis. Zwraca None jeśli Redis niedostępny.
    NIGDY nie rzuca wyjątku do callera.
    """
    if redis is None:
        return None
    try:
        value = await redis.get(key)
        return value.decode("utf-8") if isinstance(value, bytes) else value
    except Exception as exc:
        logger.warning(
            "Redis GET niedostępny (key=%s): %s",
            key, exc,
            extra={"redis_key": key, "error": str(exc)},
        )
        return None


async def _redis_set(
    redis: Optional[Redis],
    key: str,
    value: str,
    ttl: int,
) -> bool:
    """
    Ustawia wartość w Redis z TTL. Zwraca False jeśli Redis niedostępny.
    NIGDY nie rzuca wyjątku do callera.
    """
    if redis is None:
        return False
    try:
        await redis.setex(key, ttl, value)
        logger.debug(
            "Redis SET: key=%s, ttl=%ds",
            key, ttl,
            extra={"redis_key": key, "ttl": ttl},
        )
        return True
    except Exception as exc:
        logger.warning(
            "Redis SET niedostępny (key=%s): %s",
            key, exc,
            extra={"redis_key": key, "error": str(exc)},
        )
        return False


async def _redis_delete(redis: Optional[Redis], *keys: str) -> int:
    """
    Usuwa klucze z Redis. Zwraca 0 jeśli Redis niedostępny.
    NIGDY nie rzuca wyjątku do callera.
    """
    if redis is None or not keys:
        return 0
    try:
        deleted = await redis.delete(*keys)
        logger.debug(
            "Redis DEL: keys=%s, deleted=%d",
            keys, deleted,
            extra={"redis_keys": list(keys), "deleted_count": deleted},
        )
        return deleted
    except Exception as exc:
        logger.warning(
            "Redis DEL niedostępny (keys=%s): %s",
            keys, exc,
            extra={"redis_keys": list(keys), "error": str(exc)},
        )
        return 0


# ---------------------------------------------------------------------------
# Pobieranie z bazy
# ---------------------------------------------------------------------------

async def _fetch_from_db(db: AsyncSession, key: str) -> Optional[str]:
    """
    Pobiera wartość konfiguracji z dbo_ext.skw_SystemConfig.
    Zwraca None jeśli klucz nie istnieje lub IsActive=0.
    """
    try:
        result = await db.execute(
            text("""
                SELECT ConfigValue
                FROM dbo_ext.skw_SystemConfig
                WHERE ConfigKey  = :key
                AND IsActive   = 1
            """),
            {"key": key},
        )
        row = result.fetchone()
        value = row[0] if row else None

        logger.debug(
            "DB fetch config: key=%s, found=%s",
            key, value is not None,
            extra={
                "config_key": key,
                "found": value is not None,
                "value_preview": _mask_value(key, value),
            },
        )
        return value

    except Exception as exc:
        logger.error(
            "Błąd pobierania config z DB (key=%s): %s",
            key, exc,
            extra={
                "config_key": key,
                "traceback": traceback.format_exc(),
            },
        )
        raise


async def _fetch_all_from_db(db: AsyncSession) -> dict[str, str]:
    """
    Pobiera wszystkie aktywne klucze konfiguracji z bazy.
    """
    try:
        result = await db.execute(
            text("""
                SELECT ConfigKey, ConfigValue
                FROM dbo_ext.skw_SystemConfig
                WHERE IsActive = 1
                ORDER BY ConfigKey
            """)
        )
        rows = result.fetchall()
        configs = {row[0]: (row[1] or "") for row in rows}

        logger.debug(
            "DB fetch all configs: %d kluczy",
            len(configs),
            extra={
                "config_keys": list(configs.keys()),
                "count": len(configs),
            },
        )
        return configs

    except Exception as exc:
        logger.error(
            "Błąd pobierania wszystkich konfiguracji z DB: %s",
            exc,
            extra={"traceback": traceback.format_exc()},
        )
        raise


# ---------------------------------------------------------------------------
# Publiczne API — odczyt
# ---------------------------------------------------------------------------

async def get(
    db: AsyncSession,
    redis: Optional[Redis],
    key: str,
    default: Optional[str] = None,
) -> Optional[str]:
    """
    Pobiera wartość konfiguracji.

    Kolejność:
        1. Redis cache (TTL zależny od klucza)
        2. dbo_ext.skw_SystemConfig (fallback i cache fill)
        3. `default` jeśli klucz nieistnieje

    Args:
        db:      AsyncSession SQLAlchemy
        redis:   Redis client (None = tylko baza)
        key:     Klucz konfiguracji (np. "otp.expiry_minutes")
        default: Wartość domyślna gdy klucz nieistnieje

    Returns:
        Wartość jako string lub default
    """
    cache_key = _cache_key(key)

    # L1: Redis cache
    cached = await _redis_get(redis, cache_key)
    if cached is not None:
        logger.debug(
            "Config cache HIT: key=%s",
            key,
            extra={"config_key": key, "source": "redis"},
        )
        return cached

    # L2: Baza danych
    logger.debug(
        "Config cache MISS: key=%s — odpytuję DB",
        key,
        extra={"config_key": key, "source": "db"},
    )

    try:
        value = await _fetch_from_db(db, key)
    except Exception:
        # Baza też niedostępna — zwróć default
        logger.warning(
            "Config key=%s niedostępny z DB i Redis — używam default=%s",
            key, default,
            extra={"config_key": key},
        )
        return default

    if value is None:
        logger.debug(
            "Config key=%s nie istnieje w DB — zwracam default=%s",
            key, default,
            extra={"config_key": key},
        )
        return default

    # Wypełnij cache
    ttl = _ttl_for_key(key)
    await _redis_set(redis, cache_key, value, ttl)

    return value


async def get_int(
    db: AsyncSession,
    redis: Optional[Redis],
    key: str,
    default: int = 0,
) -> int:
    """
    Pobiera wartość konfiguracji jako int.
    Waliduje zakres jeśli key ma _INT_RANGE_CONSTRAINTS.

    Raises:
        ValueError: Gdy wartość w bazie nie jest poprawnym int.
    """
    raw = await get(db, redis, key, default=str(default))
    if raw is None:
        return default

    try:
        value = int(raw)
    except (ValueError, TypeError) as exc:
        logger.error(
            "Config key=%s — wartość '%s' nie jest int: %s",
            key, raw, exc,
            extra={"config_key": key, "raw_value": raw},
        )
        return default

    # Walidacja zakresu
    if key in _INT_RANGE_CONSTRAINTS:
        min_val, max_val = _INT_RANGE_CONSTRAINTS[key]
        if not (min_val <= value <= max_val):
            logger.warning(
                "Config key=%s wartość %d poza zakresem [%d, %d] — używam domyślnego %d",
                key, value, min_val, max_val, default,
                extra={
                    "config_key": key,
                    "value": value,
                    "min": min_val,
                    "max": max_val,
                },
            )
            return default

    return value


async def get_bool(
    db: AsyncSession,
    redis: Optional[Redis],
    key: str,
    default: bool = False,
) -> bool:
    """
    Pobiera wartość konfiguracji jako bool.
    Interpretuje: "true"/"1"/"yes"/"on" → True, reszta → False.
    """
    raw = await get(db, redis, key, default=str(default).lower())
    if raw is None:
        return default

    return raw.strip().lower() in ("true", "1", "yes", "on", "enabled")


async def get_list(
    db: AsyncSession,
    redis: Optional[Redis],
    key: str,
    separator: str = ",",
    default: Optional[list[str]] = None,
) -> list[str]:
    """
    Pobiera wartość konfiguracji jako listę stringów (split po separatorze).
    Każdy element trimowany ze whitespace. Puste elementy odrzucane.
    """
    raw = await get(db, redis, key)
    if raw is None:
        return default or []

    return [item.strip() for item in raw.split(separator) if item.strip()]


async def get_all(
    db: AsyncSession,
    redis: Optional[Redis],
) -> dict[str, str]:
    """
    Pobiera wszystkie aktywne klucze konfiguracji jako dict.
    Cache: Redis TTL=300s klucz "config:__all__".
    Wartości wrażliwe NIE są maskowane (surowe dane, security layer wyżej).
    """
    # L1: Redis cache
    cached_raw = await _redis_get(redis, _ALL_CONFIGS_CACHE_KEY)
    if cached_raw is not None:
        try:
            configs = json.loads(cached_raw)
            logger.debug(
                "Config get_all — cache HIT (%d kluczy)",
                len(configs),
                extra={"source": "redis", "count": len(configs)},
            )
            return configs
        except (json.JSONDecodeError, TypeError) as exc:
            logger.warning(
                "Błąd deserializacji get_all z Redis: %s — odpytuję DB",
                exc,
            )

    # L2: Baza danych
    configs = await _fetch_all_from_db(db)

    # Wypełnij cache
    try:
        serialized = json.dumps(configs, ensure_ascii=False)
        await _redis_set(redis, _ALL_CONFIGS_CACHE_KEY, serialized, _ALL_CONFIGS_TTL)
    except Exception as exc:
        logger.warning("Błąd serializacji get_all do Redis: %s", exc)

    return configs


async def get_cors_origins(
    db: AsyncSession,
    redis: Optional[Redis],
    fallback_origins: Optional[list[str]] = None,
) -> list[str]:
    """
    Pobiera dozwolone origins CORS z konfiguracji.
    Cache: Redis TTL=300s (bezpieczeństwo — krótki TTL).

    Args:
        db:              AsyncSession
        redis:           Redis client
        fallback_origins: Lista origins z .env jako ostateczny fallback

    Returns:
        Lista origins (np. ["http://localhost:3000", "http://0.53:3000"])
    """
    origins = await get_list(db, redis, "cors.allowed_origins")

    if origins:
        logger.debug(
            "CORS origins załadowane: %d origins",
            len(origins),
            extra={"origins": origins},
        )
        return origins

    # Fallback z .env
    if fallback_origins:
        logger.warning(
            "cors.allowed_origins puste w DB — używam fallback z .env: %s",
            fallback_origins,
            extra={"fallback_origins": fallback_origins},
        )
        return fallback_origins

    logger.error(
        "cors.allowed_origins: BRAK w DB i BRAK fallback — CORS może blokować wszystko!",
        extra={"critical": True},
    )
    return []


# ---------------------------------------------------------------------------
# Publiczne API — zapis
# ---------------------------------------------------------------------------

async def set_value(
    db: AsyncSession,
    redis: Optional[Redis],
    *,
    key: str,
    value: str,
    updated_by_id: Optional[int] = None,
    updated_by_username: Optional[str] = None,
) -> dict[str, Any]:
    """
    Ustawia wartość konfiguracji.

    Kroki:
        1. Walidacja klucza i wartości
        2. Pobranie starej wartości (do AuditLog)
        3. UPDATE w bazie (MERGE — upsert)
        4. Invalidacja Redis (cache key + __all__)
        5. AuditLog(action="config_updated")
        6. Zapis do pliku config_changes.jsonl

    Args:
        db:                   AsyncSession
        redis:                Redis client
        key:                  Klucz konfiguracji
        value:                Nowa wartość
        updated_by_id:        ID usera który zmienia
        updated_by_username:  Username (do AuditLog)

    Returns:
        {"key": ..., "old_value": ..., "new_value": ..., "updated_at": ...}

    Raises:
        ValueError: Gdy wartość nie przejdzie walidacji
        RuntimeError: Gdy zapis do DB się nie powiódł
    """
    # --- Walidacja ---
    _validate_config_value(key, value)

    # --- Pobierz starą wartość ---
    old_value = await get(db, redis, key)

    is_sensitive = key in _SENSITIVE_CONFIG_KEYS
    now = datetime.now(timezone.utc)

    logger.info(
        "Config SET: key=%s, old=%s → new=%s, by=%s",
        key,
        "[REDACTED]" if is_sensitive else _mask_value(key, old_value),
        "[REDACTED]" if is_sensitive else _mask_value(key, value),
        updated_by_username or updated_by_id or "system",
        extra={
            "config_key": key,
            "changed_by_id": updated_by_id,
            "changed_by_username": updated_by_username,
            "is_sensitive": is_sensitive,
        },
    )

    # --- UPSERT do bazy ---
    try:
        await db.execute(
            text("""
                MERGE dbo_ext.skw_SystemConfig AS target
                USING (SELECT :key AS ConfigKey) AS source
                ON (target.ConfigKey = source.ConfigKey)
                WHEN MATCHED THEN
                    UPDATE SET
                        ConfigValue = :value,
                        UpdatedAt   = :now
                WHEN NOT MATCHED THEN
                    INSERT (ConfigKey, ConfigValue, IsActive, CreatedAt)
                    VALUES (:key, :value, 1, :now);
            """),
            {"key": key, "value": value, "now": now},
        )
        await db.commit()

        logger.info(
            "Config ZAPISANY w DB: key=%s, updated_at=%s",
            key, now.isoformat(),
            extra={"config_key": key, "updated_at": now.isoformat()},
        )

    except Exception as exc:
        await db.rollback()
        logger.error(
            "BŁĄD zapisu config do DB (key=%s): %s",
            key, exc,
            extra={
                "config_key": key,
                "traceback": traceback.format_exc(),
            },
        )
        raise RuntimeError(
            f"Nie udało się zapisać konfiguracji '{key}' do bazy danych: {exc}"
        ) from exc

    # --- Invalidacja Redis ---
    await invalidate(redis, key)

    # --- AuditLog (fire-and-forget) ---
    audit_service.log(
        db,
        action="config_updated",
        category="System",
        entity_type="SystemConfig",
        user_id=updated_by_id,
        username=updated_by_username,
        old_value={
            "key": key,
            "value": "[REDACTED]" if is_sensitive else old_value,
        },
        new_value={
            "key": key,
            "value": "[REDACTED]" if is_sensitive else value,
        },
        details={
            "is_sensitive": is_sensitive,
            "key": key,
        },
        success=True,
    )

    # --- Plik zmian (append-only) ---
    _write_config_change_log(
        key=key,
        old_value=old_value,
        new_value=value,
        user_id=updated_by_id,
        username=updated_by_username,
    )

    return {
        "key": key,
        "old_value": "[REDACTED]" if is_sensitive else old_value,
        "new_value": "[REDACTED]" if is_sensitive else value,
        "updated_at": now.isoformat(),
        "updated_by": updated_by_username or updated_by_id,
    }


async def update_cors(
    db: AsyncSession,
    redis: Optional[Redis],
    *,
    origins: list[str],
    updated_by_id: Optional[int] = None,
    updated_by_username: Optional[str] = None,
) -> dict[str, Any]:
    """
    Aktualizuje listę dozwolonych origins CORS.

    Walidacja:
        - Każdy origin musi zaczynać się od http:// lub https://
        - Brak trailing slash
        - Brak wildcardów (*)
        - Max 20 origins

    Args:
        origins: Lista origins do zapisania

    Returns:
        Wynik set_value() z informacją o zmianie
    """
    # Walidacja każdego originu
    validated_origins = _validate_cors_origins(origins)

    # Zapisz jako CSV
    value = ",".join(validated_origins)

    logger.info(
        "update_cors: %d origins → '%s'",
        len(validated_origins), value,
        extra={
            "origins_count": len(validated_origins),
            "origins": validated_origins,
            "changed_by": updated_by_username or updated_by_id,
        },
    )

    result = await set_value(
        db, redis,
        key="cors.allowed_origins",
        value=value,
        updated_by_id=updated_by_id,
        updated_by_username=updated_by_username,
    )

    # Dodatkowy audit log dla CORS (bardziej widoczny w logach)
    logger.warning(
        "CORS ZMIENIONY przez %s: %d origins: %s",
        updated_by_username or updated_by_id or "system",
        len(validated_origins),
        validated_origins,
        extra={
            "event": "cors_updated",
            "origins": validated_origins,
            "changed_by_id": updated_by_id,
            "changed_by_username": updated_by_username,
        },
    )

    return result


# ---------------------------------------------------------------------------
# Invalidacja cache
# ---------------------------------------------------------------------------

async def invalidate(
    redis: Optional[Redis],
    key: str,
) -> None:
    """
    Invaliduje cache Redis dla danego klucza konfiguracji.
    Usuwa zarówno konkretny klucz jak i cache __all__.
    Graceful — nie rzuca wyjątku jeśli Redis niedostępny.
    """
    cache_key = _cache_key(key)
    deleted = await _redis_delete(redis, cache_key, _ALL_CONFIGS_CACHE_KEY)
    logger.debug(
        "Config cache invalidated: key=%s, deleted_entries=%d",
        key, deleted,
        extra={"config_key": key, "cache_key": cache_key, "deleted": deleted},
    )


async def invalidate_all(redis: Optional[Redis]) -> int:
    """
    Invaliduje cały cache konfiguracji (wszystkie klucze z prefixem config:).
    Używane po masowych zmianach lub przy restarcie.

    Returns:
        Liczba usuniętych kluczy
    """
    if redis is None:
        return 0
    try:
        # SCAN + DEL — bezpieczne dla środowisk produkcyjnych (nie KEYS *)
        deleted_count = 0
        async for key in redis.scan_iter(f"{_CACHE_PREFIX}*"):
            await redis.delete(key)
            deleted_count += 1

        logger.info(
            "Config cache FULL INVALIDATION: usunięto %d kluczy",
            deleted_count,
            extra={"deleted_count": deleted_count},
        )
        return deleted_count

    except Exception as exc:
        logger.warning(
            "Błąd invalidacji całego config cache: %s",
            exc,
            extra={"traceback": traceback.format_exc()},
        )
        return 0


# ---------------------------------------------------------------------------
# Walidacja wartości
# ---------------------------------------------------------------------------

def _validate_config_value(key: str, value: str) -> None:
    """
    Waliduje wartość przed zapisem do bazy.

    Checks:
        1. Klucz nie może być pusty
        2. Wartość nie może być None
        3. Klucze enum mają ograniczony zestaw wartości
        4. Klucze int muszą być poprawnymi intami w zakresie
        5. Klucze bool muszą być "true"/"false"/"1"/"0"
        6. Wartość nie może przekraczać 8000 znaków (bezpieczeństwo)

    Raises:
        ValueError: Przy naruszeniu walidacji
    """
    if not key or not key.strip():
        raise ValueError("Klucz konfiguracji nie może być pusty")

    if value is None:
        raise ValueError(f"Wartość dla klucza '{key}' nie może być None")

    if len(value) > 8000:
        raise ValueError(
            f"Wartość klucza '{key}' przekracza maksymalną długość 8000 znaków "
            f"(podano {len(value)})"
        )

    # Walidacja enum
    if key in _ENUM_CONSTRAINTS:
        allowed = _ENUM_CONSTRAINTS[key]
        if value not in allowed:
            raise ValueError(
                f"Niedozwolona wartość '{value}' dla klucza '{key}'. "
                f"Dozwolone: {sorted(allowed)}"
            )

    # Walidacja int
    if key in _INT_CONFIG_KEYS:
        try:
            int_val = int(value)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"Klucz '{key}' wymaga wartości całkowitej, otrzymano: '{value}'"
            ) from exc

        if key in _INT_RANGE_CONSTRAINTS:
            min_val, max_val = _INT_RANGE_CONSTRAINTS[key]
            if not (min_val <= int_val <= max_val):
                raise ValueError(
                    f"Wartość {int_val} dla klucza '{key}' jest poza zakresem "
                    f"[{min_val}, {max_val}]"
                )

    # Walidacja bool
    if key in _BOOL_CONFIG_KEYS:
        if value.strip().lower() not in ("true", "false", "1", "0", "yes", "no"):
            raise ValueError(
                f"Klucz '{key}' wymaga wartości boolean "
                f"(true/false/1/0/yes/no), otrzymano: '{value}'"
            )


def _validate_cors_origins(origins: list[str]) -> list[str]:
    """
    Waliduje i normalizuje listę CORS origins.

    Rules:
        - Max 20 origins
        - Każdy musi zaczynać się od http:// lub https://
        - Brak trailing slash (normalizacja)
        - Brak wildcardów (*)
        - Brak duplikatów

    Returns:
        Znormalizowana lista origins (deduplikowana)

    Raises:
        ValueError: Przy naruszeniu reguł
    """
    if not origins:
        raise ValueError("Lista CORS origins nie może być pusta")

    if len(origins) > 20:
        raise ValueError(
            f"Zbyt wiele CORS origins: {len(origins)}. Maksimum: 20."
        )

    validated: list[str] = []
    seen: set[str] = set()

    for origin in origins:
        origin = origin.strip()

        if not origin:
            continue

        # Wildcard zabroniony
        if "*" in origin:
            raise ValueError(
                f"Wildcard (*) niedozwolony w CORS origins: '{origin}'. "
                "Użyj konkretnych adresów."
            )

        # Wymagany protokół
        if not (origin.startswith("http://") or origin.startswith("https://")):
            raise ValueError(
                f"CORS origin musi zaczynać się od http:// lub https://: '{origin}'"
            )

        # Usuń trailing slash
        origin_normalized = origin.rstrip("/")

        # Deduplikacja (case-sensitive)
        if origin_normalized in seen:
            logger.debug("Duplikat CORS origin pominięty: %s", origin_normalized)
            continue

        seen.add(origin_normalized)
        validated.append(origin_normalized)

    if not validated:
        raise ValueError("Po walidacji nie pozostał żaden poprawny CORS origin")

    return validated


# ---------------------------------------------------------------------------
# Wygodne gettery dla często używanych kluczy
# ---------------------------------------------------------------------------

async def get_otp_expiry_minutes(
    db: AsyncSession, redis: Optional[Redis]
) -> int:
    """Pobiera czas życia kodu OTP w minutach (default: 15)."""
    return await get_int(db, redis, "otp.expiry_minutes", default=15)


async def get_delete_token_ttl(
    db: AsyncSession, redis: Optional[Redis]
) -> int:
    """Pobiera TTL tokenu potwierdzenia DELETE w sekundach (default: 60)."""
    return await get_int(db, redis, "delete_token.ttl_seconds", default=60)


async def get_impersonation_max_hours(
    db: AsyncSession, redis: Optional[Redis]
) -> int:
    """Pobiera maksymalny czas sesji impersonacji w godzinach (default: 4)."""
    return await get_int(db, redis, "impersonation.max_hours", default=4)


async def is_master_key_enabled(
    db: AsyncSession, redis: Optional[Redis]
) -> bool:
    """Sprawdza czy Master Key access jest włączony (default: True)."""
    return await get_bool(db, redis, "master_key.enabled", default=True)


async def get_master_pin_hash(
    db: AsyncSession, redis: Optional[Redis]
) -> Optional[str]:
    """
    Pobiera hash PIN Master Key.
    UWAGA: Zwraca surowy bcrypt hash — nie logować, nie serializować!
    """
    return await get(db, redis, "master_key.pin_hash")


async def get_schema_integrity_reaction(
    db: AsyncSession, redis: Optional[Redis]
) -> str:
    """Pobiera poziom reakcji na niezgodność checksumów (default: BLOCK)."""
    value = await get(db, redis, "schema_integrity.reaction", default="BLOCK")
    if value not in ("WARN", "ALERT", "BLOCK"):
        logger.warning(
            "Nieprawidłowa wartość schema_integrity.reaction='%s' — używam BLOCK",
            value,
        )
        return "BLOCK"
    return value


async def get_snapshot_retention_days(
    db: AsyncSession, redis: Optional[Redis]
) -> int:
    """Pobiera liczbę dni przechowywania snapshotów (default: 30)."""
    return await get_int(db, redis, "snapshot.retention_days", default=30)


# ---------------------------------------------------------------------------
# Bulk update (dla PUT /system/config endpoint)
# ---------------------------------------------------------------------------

async def update_multiple(
    db: AsyncSession,
    redis: Optional[Redis],
    *,
    updates: dict[str, str],
    updated_by_id: Optional[int] = None,
    updated_by_username: Optional[str] = None,
) -> dict[str, Any]:
    """
    Aktualizuje wiele kluczy konfiguracji naraz.
    Waliduje WSZYSTKIE przed jakimkolwiek zapisem (atomiczna walidacja).

    Args:
        updates: Słownik {key: new_value}

    Returns:
        {
            "updated": [{"key": ..., "old_value": ..., "new_value": ...}],
            "failed":  [{"key": ..., "error": ...}],
            "total":   n,
            "success_count": n,
        }
    """
    if not updates:
        return {"updated": [], "failed": [], "total": 0, "success_count": 0}

    # Max 20 kluczy naraz — ochrona
    if len(updates) > 20:
        raise ValueError(
            f"Zbyt wiele kluczy do aktualizacji: {len(updates)}. Maksimum: 20."
        )

    validation_errors: list[dict[str, str]] = []
    for key, value in updates.items():
        try:
            _validate_config_value(key, value)
        except ValueError as exc:
            validation_errors.append({"key": key, "error": str(exc)})

    if validation_errors:
        raise ValueError(
            f"Błędy walidacji dla {len(validation_errors)} kluczy: "
            + "; ".join(f"{e['key']}: {e['error']}" for e in validation_errors)
        )

    updated_results: list[dict[str, Any]] = []
    failed_results: list[dict[str, Any]] = []

    for key, value in updates.items():
        try:
            result = await set_value(
                db, redis,
                key=key,
                value=value,
                updated_by_id=updated_by_id,
                updated_by_username=updated_by_username,
            )
            updated_results.append(result)
        except Exception as exc:
            logger.error(
                "Błąd zapisu config key=%s w update_multiple: %s",
                key, exc,
            )
            failed_results.append({"key": key, "error": str(exc)})

    logger.info(
        "update_multiple: %d/%d kluczy zaktualizowanych",
        len(updated_results), len(updates),
        extra={
            "total": len(updates),
            "success_count": len(updated_results),
            "failed_count": len(failed_results),
            "updated_keys": [r["key"] for r in updated_results],
            "failed_keys": [r["key"] for r in failed_results],
            "changed_by": updated_by_username or updated_by_id,
        },
    )

    return {
        "updated": updated_results,
        "failed": failed_results,
        "total": len(updates),
        "success_count": len(updated_results),
    }


# ---------------------------------------------------------------------------
# Alias dla kompatybilności z modułem faktur (Sprint 2)
# ---------------------------------------------------------------------------
async def get_config_value(
    *,
    redis,
    key: str,
    default: str = "",
    db=None,
) -> str:
    """Alias get() — uproszczony interfejs dla modułu faktur."""
    result = await get(db=db, redis=redis, key=key, default=default)
    return result or default

# ---------------------------------------------------------------------------
# Eksport publicznego API
# ---------------------------------------------------------------------------

__all__ = [
    # Odczyt
    "get",
    "get_int",
    "get_bool",
    "get_list",
    "get_all",
    "get_cors_origins",
    # Zapis
    "set_value",
    "update_cors",
    "update_multiple",
    # Invalidacja
    "invalidate",
    "invalidate_all",
    # Wygodne gettery
    "get_otp_expiry_minutes",
    "get_delete_token_ttl",
    "get_impersonation_max_hours",
    "is_master_key_enabled",
    "get_master_pin_hash",
    "get_schema_integrity_reaction",
    "get_snapshot_retention_days",
    # Walidacja (eksport dla testów)
    "_validate_config_value",
    "_validate_cors_origins",
]