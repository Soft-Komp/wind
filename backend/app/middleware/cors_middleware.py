"""
DynamicCORSMiddleware — System Windykacja
==========================================
Dynamiczne middleware CORS ładujące allowed origins z:
    1. Redis cache (TTL: 5 minut)          ← najszybsze
    2. Baza danych — SystemConfig          ← źródło prawdy
    3. Zmienna środowiskowa CORS_ORIGINS_FALLBACK ← fallback awaryjny

Mechanizm odświeżania:
    - Przy każdym żądaniu sprawdza wiek cache
    - PUT /system/cors → `CORSCacheInvalidator.invalidate()` → kolejne żądanie
      przeładowuje origins z bazy
    - Tło: asyncio task odświeża cache co 4 minuty (przed TTL 5 min)

Szczegóły implementacji:
    • Poprawna obsługa preflight (OPTIONS) z cache
    • Walidacja wartości Origin nagłówka (regex + whitelist)
    • Blokowanie origins z wildcard w produkcji
    • Szczegółowe logowanie każdej decyzji CORS (allow/reject/preflight)
    • Samoleczenie: błędy Redis/DB nie blokują aplikacji — fallback do env
    • Thread-safe: asyncio.Lock chroni aktualizacje cache
    • Obsługa wielu origins jako CSV (np. "http://a.com,http://b.com")

Wersja: 1.0.0 | Data: 2026-02-20 | Python: 3.12+
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Final

import orjson
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
logger = logging.getLogger("windykacja.middleware.cors")

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------

# Klucz w Redis cache
_REDIS_CACHE_KEY: Final[str] = "cors:allowed_origins"
_REDIS_CACHE_TTL: Final[int] = 300  # 5 minut (sekundy)

# Klucz w SystemConfig (baza danych)
_DB_CONFIG_KEY: Final[str] = "cors.allowed_origins"

# Minimalny czas między odświeżeniami z DB (sekund) — ochrona przed flood
_MIN_REFRESH_INTERVAL_SECONDS: Final[float] = 10.0

# Tło odświeżanie — 4 minuty (przed wygaśnięciem TTL)
_BACKGROUND_REFRESH_INTERVAL: Final[float] = 240.0

# Dozwolone metody i nagłówki (standardowe dla SPA + API)
_ALLOWED_METHODS: Final[str] = "GET, POST, PUT, PATCH, DELETE, OPTIONS, HEAD"
_ALLOWED_HEADERS: Final[str] = (
    "Content-Type, Authorization, Accept, X-Requested-With, "
    "X-Request-ID, Cache-Control, Pragma"
)
_EXPOSE_HEADERS: Final[str] = "X-Request-ID, X-Response-Time, X-Total-Count"
_MAX_AGE: Final[str] = "600"  # 10 minut preflight cache

# Regex walidacja origin — tylko http(s)://host[:port]
_ORIGIN_PATTERN: Final[re.Pattern] = re.compile(
    r"^https?://"                     # Protokół
    r"(?:"
    r"(?:[a-zA-Z0-9\-_.]+)"          # Hostname lub IP
    r"(?::\d{1,5})?"                  # Opcjonalny port
    r")$"
)

# Domyślne origins gdy wszystko zawiedzie
_ULTIMATE_FALLBACK_ORIGINS: Final[frozenset[str]] = frozenset({
    "http://localhost:3000",
    "http://localhost:5173",
    "http://127.0.0.1:3000",
})

# Ścieżki których nie sprawdzamy przez CORS
_CORS_SKIP_PATHS: Final[frozenset[str]] = frozenset({
    "/health",
    "/ping",
    "/favicon.ico",
})

# ---------------------------------------------------------------------------
# CORSCacheInvalidator — singleton do inwalidacji cache z endpointu
# ---------------------------------------------------------------------------
class CORSCacheInvalidator:
    """
    Singleton umożliwiający inwalidację cache CORS z endpointu /system/cors.

    Użycie w api/system.py::

        from app.middleware.cors_middleware import CORSCacheInvalidator

        @router.put("/system/cors")
        async def update_cors(redis: RedisClient, ...):
            # ... zaktualizuj DB ...
            await CORSCacheInvalidator.invalidate(redis)
    """

    _instance: CORSCacheInvalidator | None = None
    _middleware: DynamicCORSMiddleware | None = None

    @classmethod
    def register(cls, middleware: DynamicCORSMiddleware) -> None:
        """Rejestruje instancję middleware (wywoływane w __init__)."""
        cls._middleware = middleware
        logger.debug("CORSCacheInvalidator: zarejestrowano middleware")

    @classmethod
    async def invalidate(cls, redis_client: Any = None) -> bool:
        """
        Inwaliduje cache CORS.

        Args:
            redis_client: Opcjonalny klient Redis (jeśli None — tylko lokalna flaga)

        Returns:
            True jeśli inwalidacja się powiodła
        """
        success = False

        # 1. Inwaliduj w Redis
        if redis_client is not None:
            try:
                await redis_client.delete(_REDIS_CACHE_KEY)
                logger.info(
                    orjson.dumps({
                        "event": "cors_cache_invalidated_redis",
                        "key": _REDIS_CACHE_KEY,
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }).decode()
                )
                success = True
            except Exception as exc:
                logger.error(
                    orjson.dumps({
                        "event": "cors_cache_invalidate_redis_error",
                        "error": str(exc)[:200],
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }).decode()
                )

        # 2. Inwaliduj lokalną kopię w middleware
        if cls._middleware is not None:
            async with cls._middleware._cache_lock:
                cls._middleware._cached_origins = None
                cls._middleware._cache_loaded_at = 0.0
            logger.info(
                orjson.dumps({
                    "event": "cors_cache_invalidated_local",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )
            success = True

        return success

    @classmethod
    async def get_current_origins(cls) -> frozenset[str] | None:
        """Zwraca aktualne origins z cache (do endpointu GET /system/cors)."""
        if cls._middleware is not None:
            return cls._middleware._cached_origins
        return None


# ---------------------------------------------------------------------------
# Funkcje pomocnicze
# ---------------------------------------------------------------------------

def _parse_origins_csv(raw: str) -> frozenset[str]:
    """
    Parsuje CSV origins: "http://a.com, http://b.com" → frozenset.
    Waliduje każde origin — nieprawidłowe są logowane i odrzucane.
    """
    origins: set[str] = set()
    parts = [part.strip() for part in raw.split(",") if part.strip()]

    for part in parts:
        # Sanityzacja NFC
        sanitized = unicodedata.normalize("NFC", part).rstrip("/")

        # Walidacja formatu
        if not _ORIGIN_PATTERN.match(sanitized):
            logger.warning(
                orjson.dumps({
                    "event": "cors_invalid_origin_skipped",
                    "origin": sanitized[:100],
                    "reason": "nie pasuje do wzorca http(s)://host[:port]",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )
            continue

        # Blokuj wildcard
        if "*" in sanitized:
            logger.warning(
                orjson.dumps({
                    "event": "cors_wildcard_blocked",
                    "origin": sanitized[:100],
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )
            continue

        origins.add(sanitized)

    return frozenset(origins)


def _get_env_fallback_origins() -> frozenset[str]:
    """Wczytuje CORS origins z zmiennej środowiskowej."""
    env_value = os.environ.get("CORS_ORIGINS_FALLBACK", "")
    if env_value:
        origins = _parse_origins_csv(env_value)
        if origins:
            logger.info(
                orjson.dumps({
                    "event": "cors_origins_loaded_from_env",
                    "origins": sorted(origins),
                    "count": len(origins),
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )
            return origins

    logger.warning(
        orjson.dumps({
            "event": "cors_using_ultimate_fallback",
            "origins": sorted(_ULTIMATE_FALLBACK_ORIGINS),
            "message": "CORS_ORIGINS_FALLBACK nie ustawiony — używam hardcoded fallback",
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )
    return _ULTIMATE_FALLBACK_ORIGINS


def _is_valid_request_origin(origin: str) -> bool:
    """Sprawdza czy Origin z requestu ma poprawny format (obrona przed injection)."""
    if not origin:
        return False
    # Limit długości
    if len(origin) > 256:
        return False
    # Walidacja formatu
    return bool(_ORIGIN_PATTERN.match(origin.rstrip("/")))


# ---------------------------------------------------------------------------
# Główna klasa middleware
# ---------------------------------------------------------------------------
class DynamicCORSMiddleware(BaseHTTPMiddleware):
    """
    Dynamiczne middleware CORS dla systemu Windykacja.

    Ładuje allowed origins z Redis/DB/env z mechanizmem fallback.
    Każda decyzja CORS jest logowana w JSONL.

    WAŻNE: Musi być zarejestrowane PIERWSZE w main.py (obsługuje preflight
    przed jakimkolwiek auth middleware).

    Przykład rejestracji (add_middleware jest LIFO w Starlette)::

        app.add_middleware(AuditMiddleware)   # drugi w kolejności
        app.add_middleware(DynamicCORSMiddleware, ...)  # pierwszy

    Albo przez CORSMiddleware lifespan — wtedy odwrócona kolejność add.
    """

    def __init__(
        self,
        app: ASGIApp,
        allow_credentials: bool = True,
        max_age: str = _MAX_AGE,
    ) -> None:
        super().__init__(app)
        self._allow_credentials = allow_credentials
        self._max_age = max_age

        # Cache lokalny (w pamięci procesu)
        self._cached_origins: frozenset[str] | None = None
        self._cache_loaded_at: float = 0.0
        self._cache_lock = asyncio.Lock()

        # Rejestracja w invalidatorze
        CORSCacheInvalidator.register(self)

        # Background refresh task (startuje przy pierwszym żądaniu)
        self._bg_refresh_task: asyncio.Task | None = None
        self._shutdown_event = asyncio.Event()

        logger.info(
            orjson.dumps({
                "event": "cors_middleware_init",
                "allow_credentials": allow_credentials,
                "max_age": max_age,
                "redis_cache_key": _REDIS_CACHE_KEY,
                "redis_cache_ttl": _REDIS_CACHE_TTL,
                "db_config_key": _DB_CONFIG_KEY,
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )

    # ------------------------------------------------------------------
    # Główna metoda dispatch
    # ------------------------------------------------------------------
    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        """
        Obsługa CORS dla każdego żądania.

        Preflight (OPTIONS):
            → Sprawdź origin → Zwróć 200 z CORS headers (bez call_next)

        Zwykłe żądanie:
            → Sprawdź origin → Wywołaj call_next → Dodaj CORS headers
        """
        # Pomiń CORS dla specjalnych ścieżek
        if request.url.path in _CORS_SKIP_PATHS:
            return await call_next(request)

        # Uruchom background refresh jeśli nie działa
        self._ensure_bg_refresh()

        origin = request.headers.get("origin", "")
        method = request.method.upper()

        # Żądanie bez Origin — nie jest CORS (np. curl bez nagłówka, SSR)
        if not origin:
            response = await call_next(request)
            return response

        # Walidacja formatu Origin (zabezpieczenie przed injection)
        if not _is_valid_request_origin(origin):
            logger.warning(
                orjson.dumps({
                    "event": "cors_invalid_origin_header",
                    "origin": origin[:100],
                    "method": method,
                    "path": request.url.path,
                    "client_ip": request.headers.get("x-forwarded-for", "")
                        or (request.client.host if request.client else ""),
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )
            # Zwróć response bez CORS headers — przeglądarka zablokuje
            if method == "OPTIONS":
                return Response(status_code=403, content="Niedozwolony origin")
            return await call_next(request)

        # Wczytaj aktualne origins
        allowed_origins = await self._get_allowed_origins(request)
        origin_normalized = origin.rstrip("/")
        is_allowed = origin_normalized in allowed_origins

        # ================================================================
        # Preflight (OPTIONS)
        # ================================================================
        if method == "OPTIONS":
            return await self._handle_preflight(
                request=request,
                origin=origin_normalized,
                is_allowed=is_allowed,
                allowed_origins=allowed_origins,
            )

        # ================================================================
        # Zwykłe żądanie CORS
        # ================================================================
        response = await call_next(request)
        self._add_cors_headers(
            response=response,
            origin=origin_normalized,
            is_allowed=is_allowed,
        )

        # Logowanie decyzji CORS
        self._log_cors_decision(
            origin=origin_normalized,
            method=method,
            path=request.url.path,
            is_allowed=is_allowed,
            is_preflight=False,
            request=request,
        )

        return response

    # ------------------------------------------------------------------
    # Obsługa preflight
    # ------------------------------------------------------------------
    async def _handle_preflight(
        self,
        request: Request,
        origin: str,
        is_allowed: bool,
        allowed_origins: frozenset[str],
    ) -> Response:
        """
        Obsługa żądania OPTIONS (preflight CORS).

        Jeśli origin jest dozwolony → 200 z CORS headers.
        Jeśli nie → 403 z logiem.
        """
        requested_method = request.headers.get(
            "access-control-request-method", ""
        ).upper()
        requested_headers = request.headers.get(
            "access-control-request-headers", ""
        )

        self._log_cors_decision(
            origin=origin,
            method="OPTIONS",
            path=request.url.path,
            is_allowed=is_allowed,
            is_preflight=True,
            request=request,
            extra={
                "requested_method": requested_method,
                "requested_headers": requested_headers,
            },
        )

        if not is_allowed:
            return Response(
                status_code=403,
                headers={
                    "Content-Type": "application/json",
                    "X-CORS-Status": "rejected",
                },
                content=orjson.dumps({
                    "success": False,
                    "code": "cors.origin_not_allowed",
                    "message": "Origin nie jest na liście dozwolonych",
                    "errors": [{"field": "origin", "message": f"'{origin}' nie jest dozwolony"}],
                }),
            )

        # Weryfikuj żądaną metodę
        allowed_methods_list = [m.strip() for m in _ALLOWED_METHODS.split(",")]
        if requested_method and requested_method not in allowed_methods_list:
            return Response(
                status_code=405,
                headers={
                    "Content-Type": "application/json",
                    "X-CORS-Status": "method_not_allowed",
                },
                content=orjson.dumps({
                    "success": False,
                    "code": "cors.method_not_allowed",
                    "message": f"Metoda {requested_method} nie jest dozwolona",
                    "errors": [{"field": "method", "message": f"'{requested_method}' nie jest dozwolony"}],
                }),
            )

        # Poprawny preflight
        headers: dict[str, str] = {
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Methods": _ALLOWED_METHODS,
            "Access-Control-Allow-Headers": _ALLOWED_HEADERS,
            "Access-Control-Max-Age": self._max_age,
            "Vary": "Origin",
            "X-CORS-Status": "preflight_allowed",
        }
        if self._allow_credentials:
            headers["Access-Control-Allow-Credentials"] = "true"

        return Response(
            status_code=200,
            headers=headers,
        )

    # ------------------------------------------------------------------
    # Dodawanie nagłówków CORS do odpowiedzi
    # ------------------------------------------------------------------
    def _add_cors_headers(
        self,
        response: Response,
        origin: str,
        is_allowed: bool,
    ) -> None:
        """Dodaje nagłówki CORS do response na zwykłe żądania."""
        if is_allowed:
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Access-Control-Expose-Headers"] = _EXPOSE_HEADERS
            response.headers["Vary"] = "Origin"
            response.headers["X-CORS-Status"] = "allowed"
            if self._allow_credentials:
                response.headers["Access-Control-Allow-Credentials"] = "true"
        else:
            # Nie dodaj Access-Control-Allow-Origin — przeglądarka zablokuje
            response.headers["X-CORS-Status"] = "rejected"
            response.headers["Vary"] = "Origin"

    # ------------------------------------------------------------------
    # Ładowanie origins — trójpoziomowy fallback
    # ------------------------------------------------------------------
    async def _get_allowed_origins(self, request: Request) -> frozenset[str]:
        """
        Pobiera aktualne allowed origins z cache / Redis / DB / env.

        Kolejność:
        1. Lokalny cache (w pamięci procesu) — jeśli nie wygasł
        2. Redis cache — jeśli dostępny
        3. Baza danych (SystemConfig) — przez config_service
        4. Zmienna środowiskowa CORS_ORIGINS_FALLBACK
        5. Hardcoded fallback (_ULTIMATE_FALLBACK_ORIGINS)
        """
        import time

        now = time.monotonic()

        # ---- Krok 1: Lokalny cache ----
        async with self._cache_lock:
            if (
                self._cached_origins is not None
                and (now - self._cache_loaded_at) < _REDIS_CACHE_TTL
            ):
                return self._cached_origins

        # Cache wygasł — odśwież
        return await self._refresh_origins(request)

    async def _refresh_origins(self, request: Request) -> frozenset[str]:
        """
        Odświeża origins z Redis/DB. Używa asyncio.Lock dla bezpieczeństwa.
        """
        import time

        now = time.monotonic()

        async with self._cache_lock:
            # Double-check po nabyciu locka (inny task mógł już odświeżyć)
            if (
                self._cached_origins is not None
                and (now - self._cache_loaded_at) < _REDIS_CACHE_TTL
            ):
                return self._cached_origins

            origins = await self._load_origins_from_redis(request)
            if origins is None:
                origins = await self._load_origins_from_db(request)
            if origins is None:
                origins = _get_env_fallback_origins()

            self._cached_origins = origins
            self._cache_loaded_at = time.monotonic()

            logger.info(
                orjson.dumps({
                    "event": "cors_origins_cache_refreshed",
                    "origins": sorted(origins),
                    "count": len(origins),
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )

            return origins

    async def _load_origins_from_redis(
        self, request: Request
    ) -> frozenset[str] | None:
        """Próbuje wczytać origins z Redis cache."""
        try:
            # Próba wyciągnięcia Redis z app.state (ustawione w lifespan main.py)
            redis_client = getattr(request.app.state, "redis", None)
            if redis_client is None:
                return None

            cached = await redis_client.get(_REDIS_CACHE_KEY)
            if cached:
                origins = _parse_origins_csv(cached)
                if origins:
                    logger.debug(
                        orjson.dumps({
                            "event": "cors_origins_loaded_from_redis",
                            "count": len(origins),
                            "ts": datetime.now(timezone.utc).isoformat(),
                        }).decode()
                    )
                    return origins

            return None

        except Exception as exc:
            logger.warning(
                orjson.dumps({
                    "event": "cors_redis_load_error",
                    "error": str(exc)[:200],
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )
            return None

    async def _load_origins_from_db(
        self, request: Request
    ) -> frozenset[str] | None:
        """Próbuje wczytać origins z bazy danych przez config_service."""
        try:
            # Próba wyciągnięcia session factory z app.state
            db_session_factory = getattr(request.app.state, "db_session_factory", None)
            if db_session_factory is None:
                return None

            # Import tutaj żeby uniknąć circular imports
            try:
                from app.services.config_service import ConfigService
            except ImportError:
                logger.warning(
                    orjson.dumps({
                        "event": "cors_config_service_import_error",
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }).decode()
                )
                return None

            redis_client = getattr(request.app.state, "redis", None)

            async with db_session_factory() as db:
                raw_value = await ConfigService.get(
                    db=db,
                    redis=redis_client,
                    key=_DB_CONFIG_KEY,
                )

            if not raw_value:
                return None

            origins = _parse_origins_csv(raw_value)
            if not origins:
                return None

            # Zapisz do Redis (repopulacja cache)
            await self._save_to_redis(request, raw_value)

            logger.info(
                orjson.dumps({
                    "event": "cors_origins_loaded_from_db",
                    "config_key": _DB_CONFIG_KEY,
                    "count": len(origins),
                    "origins": sorted(origins),
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )
            return origins

        except Exception as exc:
            logger.error(
                orjson.dumps({
                    "event": "cors_db_load_error",
                    "error": str(exc)[:200],
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )
            return None

    async def _save_to_redis(self, request: Request, value: str) -> None:
        """Zapisuje wartość origins do Redis cache z TTL."""
        try:
            redis_client = getattr(request.app.state, "redis", None)
            if redis_client:
                await redis_client.setex(_REDIS_CACHE_KEY, _REDIS_CACHE_TTL, value)
        except Exception as exc:
            logger.warning(
                orjson.dumps({
                    "event": "cors_redis_save_error",
                    "error": str(exc)[:200],
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )

    # ------------------------------------------------------------------
    # Background refresh task
    # ------------------------------------------------------------------
    def _ensure_bg_refresh(self) -> None:
        """Uruchamia background task jeśli nie działa."""
        if self._bg_refresh_task is None or self._bg_refresh_task.done():
            try:
                loop = asyncio.get_running_loop()
                self._bg_refresh_task = loop.create_task(
                    self._bg_refresh_loop(),
                    name="cors_bg_refresh",
                )
            except RuntimeError:
                pass  # Brak event loop — OK dla testów

    async def _bg_refresh_loop(self) -> None:
        """
        Background loop odświeżający cache CORS co 4 minuty.
        Zapobiega jednoczesnym żądaniom trafienia na wygasły cache.
        """
        logger.debug("Uruchomiono background refresh loop CORS cache")
        try:
            while not self._shutdown_event.is_set():
                await asyncio.sleep(_BACKGROUND_REFRESH_INTERVAL)

                try:
                    # Wymuś odświeżenie — zeruj cache
                    async with self._cache_lock:
                        self._cache_loaded_at = 0.0

                    logger.debug(
                        orjson.dumps({
                            "event": "cors_bg_cache_expired",
                            "message": "Cache CORS wygasł — odświeżenie przy następnym żądaniu",
                            "ts": datetime.now(timezone.utc).isoformat(),
                        }).decode()
                    )
                except Exception as exc:
                    logger.warning(
                        orjson.dumps({
                            "event": "cors_bg_refresh_error",
                            "error": str(exc)[:200],
                            "ts": datetime.now(timezone.utc).isoformat(),
                        }).decode()
                    )
        except asyncio.CancelledError:
            logger.debug("Background refresh loop CORS został anulowany")

    async def shutdown(self) -> None:
        """Zatrzymuje background task (wywoływane z lifespan shutdown)."""
        self._shutdown_event.set()
        if self._bg_refresh_task and not self._bg_refresh_task.done():
            self._bg_refresh_task.cancel()
            try:
                await self._bg_refresh_task
            except asyncio.CancelledError:
                pass
        logger.info(
            orjson.dumps({
                "event": "cors_middleware_shutdown",
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )

    # ------------------------------------------------------------------
    # Logowanie decyzji CORS
    # ------------------------------------------------------------------
    def _log_cors_decision(
        self,
        origin: str,
        method: str,
        path: str,
        is_allowed: bool,
        is_preflight: bool,
        request: Request,
        extra: dict | None = None,
    ) -> None:
        """Loguje każdą decyzję CORS — allow/reject z pełnym kontekstem."""
        client_ip = (
            request.headers.get("x-forwarded-for", "").split(",")[0].strip()
            or (request.client.host if request.client else "unknown")
        )

        record: dict[str, Any] = {
            "event": "cors_decision",
            "origin": origin,
            "method": method,
            "path": path,
            "is_preflight": is_preflight,
            "decision": "allowed" if is_allowed else "rejected",
            "client_ip": client_ip,
            "request_id": getattr(request.state, "request_id", "-"),
            "ts": datetime.now(timezone.utc).isoformat(),
        }

        if extra:
            record["extra"] = extra

        if is_allowed:
            logger.debug(orjson.dumps(record).decode())
        else:
            # Odrzucone — loguj na WARNING dla widoczności
            logger.warning(orjson.dumps(record).decode())