# =============================================================================
# backend/app/core/cache_bypass.py
# =============================================================================
# Globalny bypass cache Redis.
#
# Gdy cache.bypass_enabled = true w skw_SystemConfig,
# wszystkie serwisy pomijaja Redis cache i czytaja dane bezposrednio z DB.
# Kolejki ARQ, SSE Pub/Sub i DLQ pozostaja NIENARUSZONE.
#
# Klucze Redis ktorych ten modul NIE dotyka:
#   arq:queue:*, arq:result:*, arq:health-check,
#   windykacja:dlq, windykacja:task_results,
#   channel:admins, channel:user:*
# =============================================================================

from __future__ import annotations

import asyncio
import logging
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import orjson
from sqlalchemy import text as sa_text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stale
# ---------------------------------------------------------------------------

_CONFIG_KEY: str = "cache.bypass_enabled"

# In-memory TTL (sekundy) — zmiana w DB zacznie dzialac po max N sekundach
_IN_MEMORY_TTL: float = 5.0


def _get_bypass_log_path() -> Path:
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    year = datetime.now(timezone.utc).strftime("%Y")
    return log_dir / f"cache_bypass_{year}.jsonl"


# ---------------------------------------------------------------------------
# CacheBypassManager — singleton per-process
# ---------------------------------------------------------------------------

class CacheBypassManager:
    """
    Singleton zarzadzajacy stanem globalnego bypassu cache.

    Odczytuje cache.bypass_enabled z bazy danych (NIGDY z Redis).
    Uzywa in-memory TTL (5s) zeby nie zalewac DB zapytaniami.
    """

    _instance: Optional[CacheBypassManager] = None

    def __new__(cls) -> CacheBypassManager:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        self._cached_value: bool = False
        self._cached_at: float = 0.0
        self._lock: asyncio.Lock = asyncio.Lock()

        # Liczniki diagnostyczne
        self._total_checks: int = 0
        self._cache_hits: int = 0
        self._db_reads: int = 0
        self._db_errors: int = 0
        self._bypass_activations: int = 0

        logger.info(
            orjson.dumps({
                "event": "cache_bypass_manager_init",
                "in_memory_ttl_seconds": _IN_MEMORY_TTL,
                "config_key": _CONFIG_KEY,
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )

    # ------------------------------------------------------------------
    # Publiczne API
    # ------------------------------------------------------------------

    async def is_bypass_active(self, db: AsyncSession) -> bool:
        """
        Sprawdza czy cache bypass jest aktywny.

        Returns:
            True  -> pomij Redis, czytaj z DB
            False -> normalna sciezka cache (domyslne)

        Nigdy nie rzuca wyjatku.
        """
        self._total_checks += 1
        now = time.monotonic()

        # In-memory cache check
        async with self._lock:
            if (now - self._cached_at) < _IN_MEMORY_TTL:
                self._cache_hits += 1
                if self._cached_value:
                    self._bypass_activations += 1
                return self._cached_value

        # Cache miss — czytaj z DB
        fresh_value = await self._read_from_db(db)

        async with self._lock:
            self._cached_value = fresh_value
            self._cached_at = time.monotonic()

        if fresh_value:
            self._bypass_activations += 1

        return fresh_value

    def invalidate_in_memory_cache(self) -> None:
        """
        Wymusza odczyt z DB przy nastepnym sprawdzeniu.
        Wywolaj po zmianie cache.bypass_enabled przez PUT /system/config.
        """
        self._cached_at = 0.0
        logger.info(
            orjson.dumps({
                "event": "cache_bypass_in_memory_invalidated",
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )

    def get_diagnostics(self) -> dict:
        """Zwraca dane diagnostyczne managera."""
        age = time.monotonic() - self._cached_at if self._cached_at > 0 else None
        return {
            "current_state": self._cached_value,
            "in_memory_age_seconds": round(age, 2) if age is not None else None,
            "in_memory_ttl_seconds": _IN_MEMORY_TTL,
            "total_checks": self._total_checks,
            "cache_hits": self._cache_hits,
            "db_reads": self._db_reads,
            "db_errors": self._db_errors,
            "bypass_activations": self._bypass_activations,
            "hit_ratio_pct": round(
                self._cache_hits / self._total_checks * 100, 1
            ) if self._total_checks > 0 else 0.0,
            "ts": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    # Prywatne
    # ------------------------------------------------------------------

    async def _read_from_db(self, db: AsyncSession) -> bool:
        """Czyta wartosc cache.bypass_enabled bezposrednio z bazy. Nigdy nie rzuca."""
        self._db_reads += 1
        try:
            result = await db.execute(
                sa_text("""
                    SELECT [ConfigValue]
                    FROM [dbo_ext].[skw_SystemConfig]
                    WHERE [ConfigKey] = :key
                      AND [IsActive] = 1
                """),
                {"key": _CONFIG_KEY},
            )
            row = result.fetchone()

            if row is None:
                logger.debug(
                    orjson.dumps({
                        "event": "cache_bypass_key_not_found",
                        "config_key": _CONFIG_KEY,
                        "result": False,
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }).decode()
                )
                return False

            raw_value: str = str(row[0]).strip().lower()
            is_active: bool = raw_value in ("true", "1", "yes", "tak")

            logger.debug(
                orjson.dumps({
                    "event": "cache_bypass_db_read",
                    "config_key": _CONFIG_KEY,
                    "raw_value": raw_value,
                    "parsed": is_active,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )

            if is_active != self._cached_value:
                self._log_state_change(
                    old_state=self._cached_value,
                    new_state=is_active,
                    raw_value=raw_value,
                )

            return is_active

        except Exception as exc:
            self._db_errors += 1
            logger.error(
                orjson.dumps({
                    "event": "cache_bypass_db_read_error",
                    "config_key": _CONFIG_KEY,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                    "traceback": traceback.format_exc(),
                    "fallback": self._cached_value,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )
            # Przy bledzie DB zwroc poprzedni stan — nie False
            # (zapobiega naglemu "wlaczeniu" cache gdy DB chwilowo niedostepna)
            return self._cached_value

    def _log_state_change(
        self,
        old_state: bool,
        new_state: bool,
        raw_value: str,
    ) -> None:
        """Zapisuje zmiane stanu bypassu do pliku JSONL. Append-only."""
        path = _get_bypass_log_path()
        try:
            record = {
                "event": "cache_bypass_state_changed",
                "old_state": old_state,
                "new_state": new_state,
                "raw_db_value": raw_value,
                "db_reads_total": self._db_reads,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
            line = orjson.dumps(record).decode("utf-8")
            with open(path, "a", encoding="utf-8") as f:
                f.write(line + "\n")

            level = logging.WARNING if new_state else logging.INFO
            logger.log(
                level,
                "CACHE BYPASS: stan zmienil sie z %s na %s",
                old_state,
                new_state,
                extra={
                    "old_state": old_state,
                    "new_state": new_state,
                    "raw_value": raw_value,
                },
            )
        except OSError as exc:
            logger.warning(
                "Nie udalo sie zapisac zmiany stanu bypassu do pliku: %s", exc
            )


# ---------------------------------------------------------------------------
# Singleton — globalny dostep
# ---------------------------------------------------------------------------

cache_bypass_manager = CacheBypassManager()


# ---------------------------------------------------------------------------
# Shortcut — uzycie w serwisach
# ---------------------------------------------------------------------------

async def is_cache_bypassed(db: AsyncSession) -> bool:
    """
    Sprawdza czy cache bypass jest aktywny.

    Uzycie:
        from app.core.cache_bypass import is_cache_bypassed

        if await is_cache_bypassed(db):
            # idz do bazy
            ...
        else:
            # normalna sciezka z cache
            ...
    """
    return await cache_bypass_manager.is_bypass_active(db)