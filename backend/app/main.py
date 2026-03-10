"""
main.py
═══════════════════════════════════════════════════════════════════════════════
Punkt wejścia aplikacji FastAPI — System Windykacja Backend.

Odpowiedzialności tego pliku:
  • Lifespan (startup / shutdown) — sprawdzenie DB, Redis, schema integrity,
    wczytanie konfiguracji CORS z SystemConfig przy starcie
  • Middleware — DynamicCORSMiddleware, AuditMiddleware (request_id, timing, IP)
  • Exception handlers — ujednolicona struktura błędów (BaseResponse) dla:
      RequestValidationError, HTTPException, Exception catch-all
  • Rejestracja routerów przez api/router.py
  • Swagger UI (PL opis, wersja, opis kontaktowy)
  • Nagłówek X-Request-ID w każdej odpowiedzi

Wzorce:
  • Wszystkie błędy → BaseResponse.error() z kodem i tablicą errors[]
  • CORS dynamiczny → cache Redis → fallback .env → fallback hardcoded localhost
  • Logi → JSON Lines (orjson) do pliku logs/app_YYYY-MM-DD.jsonl + stderr
  • datetime → datetime.now(timezone.utc) — NIGDY utcnow()

Ścieżka docelowa: backend/app/main.py
Autor: System Windykacja
Wersja: 1.0.0
Data: 2026-02-20
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import orjson
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response
from starlette.types import ASGIApp

from app.core.config import get_settings
from app.core.logging_setup import setup_logging
from app.db.session import close_db_engine, get_async_session, get_redis_client
from app.db.wapro import initialize_pool as wapro_initialize_pool
from app.db.wapro import shutdown_pool as wapro_shutdown_pool
from app.core.arq_pool import init_arq_pool, close_arq_pool

# ─────────────────────────────────────────────────────────────────────────────
# Logger modułu (skonfigurowany przez setup_logging w lifespan)
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Stałe aplikacji
# ─────────────────────────────────────────────────────────────────────────────

_APP_TITLE = "System Windykacja — API"
_APP_DESCRIPTION = """
## System Windykacji Należności

Backend systemu do masowej wysyłki monitów (email, SMS, druk) do dłużników
WAPRO z pełnym audit trail, zarządzaniem użytkownikami RBAC i historią komunikacji.

### Uwierzytelnianie
Wszystkie endpointy wymagają nagłówka `Authorization: Bearer <access_token>`.
Token uzyskiwany przez `POST /api/v1/auth/login`.

### Format odpowiedzi
Każda odpowiedź ma ujednoliconą strukturę:
```json
{
  "success": true,
  "code": "resource.action",
  "data": { ... },
  "errors": null,
  "meta": {
    "request_id": "uuid",
    "timestamp": "ISO8601"
  }
}
```

### Błędy
Błędy zawsze jako tablica w polu `errors`:
```json
{
  "success": false,
  "code": "validation.error",
  "data": null,
  "errors": [{"field": "email", "message": "Nieprawidłowy format"}]
}
```
"""
_APP_VERSION = "1.0.0"
_APP_CONTACT = {
    "name": "System Windykacja",
    "email": "admin@windykacja.pl",
}

# Klucz Redis dla cache CORS (spójny z SystemConfig i cors_middleware)
_CORS_CACHE_KEY = "cfg:cors.allowed_origins"
_CORS_CACHE_TTL = 300  # sekund

# Ścieżki wykluczone z logowania żądań (zbyt częste, śmiecą logi)
_AUDIT_EXCLUDE_PATHS = frozenset(
    [
        "/health",
        "/api/v1/docs",
        "/api/v1/redoc",
        "/api/v1/openapi.json",
        "/favicon.ico",
    ]
)

# Ścieżki wykluczone z CORS middleware (wewnętrzne endpointy)
_CORS_EXCLUDE_PATHS = frozenset(["/health"])


# ─────────────────────────────────────────────────────────────────────────────
# POMOCNICZE: Budowanie odpowiedzi błędu (BaseResponse-compatible)
# ─────────────────────────────────────────────────────────────────────────────

def _error_response(
    *,
    code: str,
    message: str,
    errors: list[dict[str, str]] | None = None,
    request_id: str | None = None,
    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
) -> JSONResponse:
    """
    Buduje JSONResponse zgodny z BaseResponse.error() z schemas/common.py.
    Używany wyłącznie przez exception handlery — nie przez endpointy.

    Dlaczego nie importujemy BaseResponse bezpośrednio?
    Exception handlery mogą być wywoływane zanim Pydantic przetworzy request,
    więc musimy budować odpowiedź ręcznie (bez Pydantic serialization).
    """
    body = {
        "success": False,
        "code": code,
        "data": None,
        "errors": errors or [{"field": "_", "message": message}],
        "meta": {
            "request_id": request_id or "unknown",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    }
    return JSONResponse(
        status_code=status_code,
        content=body,
        headers={"X-Request-ID": request_id or "unknown"},
    )


# ─────────────────────────────────────────────────────────────────────────────
# POMOCNICZE: Wczytaj allowed_origins CORS z SystemConfig / Redis / fallback
# ─────────────────────────────────────────────────────────────────────────────

async def _load_cors_origins() -> list[str]:
    """
    Wczytuje listę dozwolonych originów CORS w kolejności priorytetów:
      1. Redis cache (TTL 300s) — najszybsze
      2. Baza danych (SystemConfig key='cors.allowed_origins')
      3. .env CORS_ORIGINS_FALLBACK — gdy baza niedostępna
      4. Hardcoded localhost — ostatnia deska ratunku

    Format w DB/Redis: "http://0.53:3000,http://localhost:3000" (CSV)

    Nigdy nie rzuca wyjątku — CORS musi działać zawsze.
    """
    settings = get_settings()
    fallback: list[str] = _parse_cors_csv(
        getattr(settings, "CORS_ORIGINS_FALLBACK", "http://localhost:3000")
    )

    # 1. Próba Redis
    try:
        redis = await get_redis_client()
        cached = await redis.get(_CORS_CACHE_KEY)
        if cached:
            value = cached.decode() if isinstance(cached, bytes) else cached
            origins = _parse_cors_csv(value)
            if origins:
                logger.debug(
                    orjson.dumps(
                        {
                            "event": "cors_loaded_from_cache",
                            "count": len(origins),
                            "ts": datetime.now(timezone.utc).isoformat(),
                        }
                    ).decode()
                )
                return origins
    except Exception as exc:
        logger.warning(
            orjson.dumps(
                {
                    "event": "cors_redis_unavailable",
                    "error": str(exc),
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )

    # 2. Próba DB (przez sesję SQLAlchemy)
    try:
        from sqlalchemy import select
        from app.db.models.system_config import SystemConfig

        async for db in get_async_session():
            stmt = select(SystemConfig.config_value).where(
                SystemConfig.config_key == "cors.allowed_origins",
                SystemConfig.is_active == 1,
            )
            result = await db.execute(stmt)
            value = result.scalar_one_or_none()

            if value:
                origins = _parse_cors_csv(value)
                if origins:
                    # Zapisz do Redis (fire-and-forget)
                    try:
                        redis = await get_redis_client()
                        await redis.setex(_CORS_CACHE_KEY, _CORS_CACHE_TTL, value)
                    except Exception:
                        pass

                    logger.info(
                        orjson.dumps(
                            {
                                "event": "cors_loaded_from_db",
                                "count": len(origins),
                                "ts": datetime.now(timezone.utc).isoformat(),
                            }
                        ).decode()
                    )
                    return origins
    except Exception as exc:
        logger.warning(
            orjson.dumps(
                {
                    "event": "cors_db_unavailable",
                    "error": str(exc),
                    "fallback_used": True,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )

    # 3. Fallback z .env / hardcoded
    logger.warning(
        orjson.dumps(
            {
                "event": "cors_using_fallback",
                "origins": fallback,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ).decode()
    )
    return fallback


def _parse_cors_csv(value: str) -> list[str]:
    """Parsuje CSV string originów na listę, pomija puste."""
    return [o.strip() for o in value.split(",") if o.strip()]


# ─────────────────────────────────────────────────────────────────────────────
# MIDDLEWARE: Dynamiczny CORS
# ─────────────────────────────────────────────────────────────────────────────

class DynamicCORSMiddleware(BaseHTTPMiddleware):
    """
    Middleware CORS z dynamicznym odświeżaniem listy allowed_origins.

    Różnica od standardowego CORSMiddleware FastAPI:
      • Odczytuje allowed_origins z Redis/DB przy każdym requestu
        (ale z cache — efektywny koszt to 1x Redis GET per request)
      • Gdy PUT /system/cors invaliduje cache Redis → nowe originy
        działają w ciągu max 1 requestu bez restartu aplikacji

    Konfiguracja CORS (bezpieczna):
      - allow_credentials: True (dla JWT w Authorization header)
      - allow_methods: GET, POST, PUT, DELETE, OPTIONS, PATCH
      - allow_headers: Authorization, Content-Type, X-Request-ID,
                       X-Master-Key, X-Master-Pin, Accept, Origin
      - expose_headers: X-Request-ID (widoczny dla frontend JavaScript)
      - max_age: 600 sekund (preflight cache)
    """

    _ALLOWED_METHODS = ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"]
    _ALLOWED_HEADERS = [
        "Authorization",
        "Content-Type",
        "X-Request-ID",
        "X-Master-Key",
        "X-Master-Pin",
        "Accept",
        "Origin",
    ]
    _EXPOSE_HEADERS = ["X-Request-ID"]
    _MAX_AGE = 600

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        origin = request.headers.get("origin", "")

        # Brak origin → nie jest request przeglądarkowy → CORS pomijamy
        if not origin:
            return await call_next(request)

        # Wczytaj aktualne originy (z cache Redis — tanie)
        allowed_origins = await _load_cors_origins()

        # Sprawdź czy origin jest dozwolony
        origin_allowed = origin in allowed_origins or "*" in allowed_origins

        # Preflight request (OPTIONS)
        if request.method == "OPTIONS":
            if origin_allowed:
                return Response(
                    status_code=204,
                    headers=self._build_cors_headers(origin, preflight=True),
                )
            # Nieznany origin → odmowa bez szczegółów
            return Response(status_code=403)

        # Normalny request
        response = await call_next(request)

        if origin_allowed:
            for key, val in self._build_cors_headers(origin, preflight=False).items():
                response.headers[key] = val

        return response

    def _build_cors_headers(self, origin: str, *, preflight: bool) -> dict[str, str]:
        headers: dict[str, str] = {
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Credentials": "true",
            "Vary": "Origin",
        }
        if preflight:
            headers["Access-Control-Allow-Methods"] = ", ".join(self._ALLOWED_METHODS)
            headers["Access-Control-Allow-Headers"] = ", ".join(self._ALLOWED_HEADERS)
            headers["Access-Control-Max-Age"] = str(self._MAX_AGE)
        else:
            headers["Access-Control-Expose-Headers"] = ", ".join(self._EXPOSE_HEADERS)
        return headers


# ─────────────────────────────────────────────────────────────────────────────
# MIDDLEWARE: Audit (request_id, timing, logowanie każdego żądania)
# ─────────────────────────────────────────────────────────────────────────────

class AuditMiddleware(BaseHTTPMiddleware):
    """
    Middleware logujący każdy request HTTP w formacie JSON Lines.

    Dla każdego requestu (poza wykluczonymi ścieżkami):
      1. Generuje request_id (UUID4) → zapisuje do request.state
      2. Wyciąga IP klienta (X-Forwarded-For lub client.host)
      3. Mierzy czas odpowiedzi (monotonic clock — nie zależy od NTP)
      4. Loguje: metoda, URL, status, czas_ms, user_id (z state), ip
      5. Dodaje nagłówek X-Request-ID do każdej odpowiedzi
      6. Przy błędach 4xx/5xx: szczegółowe logowanie z kodem błędu

    Format logu:
    {
      "event": "http_request",
      "method": "GET",
      "path": "/api/v1/users",
      "status": 200,
      "duration_ms": 45.3,
      "request_id": "uuid4",
      "user_id": 1,
      "ip": "192.168.1.1",
      "user_agent": "...",
      "ts": "ISO8601"
    }

    Uwaga: user_id jest dostępny tylko JEŚLI endpoint ustawił request.state.user_id.
    Middleware nie dekoduje JWT samodzielnie (to rola dependencies.py).
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        # Generuj request_id — zapisz w state przed wywołaniem endpointa
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id

        # Inicjalizuj state (dependencies.py może to wypełnić)
        request.state.real_user_id = None
        request.state.is_impersonating = False

        path = request.url.path
        method = request.method

        # Pomiń logowanie dla hałaśliwych ścieżek (health, docs)
        skip_logging = path in _AUDIT_EXCLUDE_PATHS

        start_time = time.monotonic()

        try:
            response = await call_next(request)
        except Exception as exc:
            # Nieoczekiwany wyjątek — loguj przed re-raise
            duration_ms = round((time.monotonic() - start_time) * 1000, 2)
            _log_request(
                method=method,
                path=path,
                status_code=500,
                duration_ms=duration_ms,
                request_id=request_id,
                request=request,
                error=str(exc),
            )
            raise

        duration_ms = round((time.monotonic() - start_time) * 1000, 2)

        # Dodaj X-Request-ID do odpowiedzi (zawsze)
        response.headers["X-Request-ID"] = request_id

        if not skip_logging:
            status_code = response.status_code
            _log_request(
                method=method,
                path=path,
                status_code=status_code,
                duration_ms=duration_ms,
                request_id=request_id,
                request=request,
                # Przy błędach klienta/serwera loguj więcej
                is_error=(status_code >= 400),
            )

        return response


def _log_request(
    *,
    method: str,
    path: str,
    status_code: int,
    duration_ms: float,
    request_id: str,
    request: Request,
    error: str | None = None,
    is_error: bool = False,
) -> None:
    """Loguje request do JSON Lines. Wywoływane przez AuditMiddleware."""
    now_utc = datetime.now(timezone.utc)

    # IP — X-Forwarded-For lub direct
    forwarded_for = request.headers.get("X-Forwarded-For", "")
    ip = (
        forwarded_for.split(",")[0].strip()
        if forwarded_for
        else (request.client.host if request.client else "unknown")
    )

    user_id = getattr(request.state, "real_user_id", None)
    is_impersonating = getattr(request.state, "is_impersonating", False)
    impersonated_id = getattr(request.state, "impersonated_user_id", None)

    entry: dict[str, Any] = {
        "event": "http_request",
        "method": method,
        "path": path,
        "status": status_code,
        "duration_ms": duration_ms,
        "request_id": request_id,
        "user_id": user_id,
        "ip": ip,
        "ts": now_utc.isoformat(),
    }

    # Dodaj user-agent tylko przy błędach (oszczędność miejsca w logach)
    if is_error or error:
        user_agent = request.headers.get("User-Agent", "")[:200]
        entry["user_agent"] = user_agent

    if error:
        entry["error"] = error
        entry["severity"] = "ERROR"

    if is_impersonating:
        entry["is_impersonating"] = True
        entry["impersonated_user_id"] = impersonated_id

    log_level = logging.ERROR if (error or status_code >= 500) else (
        logging.WARNING if status_code >= 400 else logging.INFO
    )

    logger.log(log_level, orjson.dumps(entry).decode())


# ─────────────────────────────────────────────────────────────────────────────
# LIFESPAN: Startup i Shutdown
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Kontekst życia aplikacji — FastAPI lifespan pattern (nie @on_event).

    STARTUP:
      1. Konfiguracja logowania (plik + stderr, JSON Lines)
      2. Test połączenia z bazą danych (SELECT 1)
      3. Test połączenia Redis (PING)
      4. Weryfikacja integralności schematu DB (schema_integrity.py)
         → Jeśli reaction=BLOCK i checksums niezgodne → odmowa startu
      5. Wczytanie allowed_origins CORS (SystemConfig → Redis cache)
      6. Inicjalizacja konfiguracji HttpOnly cookie (CookieConfig)  ← NOWE v2.0
      7. Logowanie finalnego potwierdzenia startu

    SHUTDOWN:
      1. Zamknięcie engine SQLAlchemy (flush connections pool)
      2. Logowanie shutdown

    Przy błędach krytycznych (DB, Redis) → aplikacja NIE startuje.
    Błędy niekrytyczne (schema integrity WARN) → ostrzeżenie w logach.
    """
    settings = get_settings()

    # ── KROK 1: Logging ──────────────────────────────────────────────────────
    setup_logging()
    start_ts = datetime.now(timezone.utc)

    logger.info(
        orjson.dumps(
            {
                "event": "app_startup_begin",
                "version": _APP_VERSION,
                "environment": getattr(settings, "ENVIRONMENT", "production"),
                "ts": start_ts.isoformat(),
            }
        ).decode()
    )

    # ── KROK 2: Baza danych ──────────────────────────────────────────────────
    try:
        from sqlalchemy import text

        async for db in get_async_session():
            result = await db.execute(text("SELECT 1 AS ping"))
            ping = result.scalar_one()
            assert ping == 1
            logger.info(
                orjson.dumps(
                    {
                        "event": "startup_db_ok",
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }
                ).decode()
            )
            break
    except Exception as exc:
        logger.critical(
            orjson.dumps(
                {
                    "event": "startup_db_failed",
                    "error": str(exc),
                    "severity": "CRITICAL",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )
        raise RuntimeError(f"Baza danych niedostępna przy starcie: {exc}") from exc

    # ── KROK 3: Redis ─────────────────────────────────────────────────────────
    try:
        redis = await get_redis_client()
        pong = await redis.ping()
        assert pong is True or pong == b"PONG"
        logger.info(
            orjson.dumps(
                {
                    "event": "startup_redis_ok",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )
    except Exception as exc:
        logger.critical(
            orjson.dumps(
                {
                    "event": "startup_redis_failed",
                    "error": str(exc),
                    "severity": "CRITICAL",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )
        raise RuntimeError(f"Redis niedostępny przy starcie: {exc}") from exc

    # ── KROK 4: Schema integrity ──────────────────────────────────────────────
    try:
        from app.core.schema_integrity import SchemaIntegrityChecker

        async for db in get_async_session():
            checker = SchemaIntegrityChecker(db=db)
            integrity_ok, issues = await checker.verify_all()

            if not integrity_ok:
                from sqlalchemy import select
                from app.db.models.system_config import SystemConfig

                stmt = select(SystemConfig.config_value).where(
                    SystemConfig.config_key == "schema_integrity.reaction",
                    SystemConfig.is_active == 1,
                )
                result = await db.execute(stmt)
                reaction = (result.scalar_one_or_none() or "BLOCK").upper()

                log_entry = {
                    "event": "startup_schema_integrity_failed",
                    "reaction": reaction,
                    "issues_count": len(issues),
                    "issues": issues[:10],
                    "ts": datetime.now(timezone.utc).isoformat(),
                }

                if reaction == "BLOCK":
                    logger.critical(
                        orjson.dumps({**log_entry, "severity": "CRITICAL"}).decode()
                    )
                    raise RuntimeError(
                        f"Weryfikacja schematu DB nie powiodła się ({len(issues)} niezgodności). "
                        f"Reakcja: BLOCK — aplikacja nie startuje. "
                        f"Zmień schema_integrity.reaction na WARN aby pominąć."
                    )
                else:
                    logger.warning(orjson.dumps(log_entry).decode())
            else:
                logger.info(
                    orjson.dumps(
                        {
                            "event": "startup_schema_integrity_ok",
                            "ts": datetime.now(timezone.utc).isoformat(),
                        }
                    ).decode()
                )
            break
    except RuntimeError:
        raise
    except Exception as exc:
        logger.warning(
            orjson.dumps(
                {
                    "event": "startup_schema_integrity_error",
                    "error": str(exc),
                    "note": "Weryfikacja pominięta — kontynuuję start",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )

    # ── KROK 5: CORS — wczytanie origins do app.state ─────────────────────────
    try:
        async for db in get_async_session():
            from app.services import config_service

            origins = await config_service.get_cors_origins(
                db=db,
                redis=redis,
                fallback_origins=settings.cors_origins_fallback_list,
            )
            app.state.redis = redis
            logger.info(
                orjson.dumps(
                    {
                        "event": "startup_cors_ok",
                        "origins_count": len(origins),
                        "origins": origins,
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }
                ).decode()
            )
            break
    except Exception as exc:
        logger.warning(
            orjson.dumps(
                {
                    "event": "startup_cors_warning",
                    "error": str(exc),
                    "note": "CORS załadowane z fallback .env",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )
        app.state.redis = redis

    # ── KROK 6: Cookie config (HttpOnly refresh token) ────────────────────────
    try:
        import app.core.cookie_manager as _cm
        from app.core.cookie_manager import (
            CookieConfig,
            validate_cookie_config_for_environment,
        )

        _cm.COOKIE_CFG = CookieConfig.from_settings(settings)

        cookie_warnings = validate_cookie_config_for_environment(
            _cm.COOKIE_CFG,
            environment=getattr(settings, "environment", "production"),
        )

        if cookie_warnings:
            for warning in cookie_warnings:
                logger.warning(
                    orjson.dumps(
                        {
                            "event": "startup_cookie_config_warning",
                            "warning": warning,
                            "ts": datetime.now(timezone.utc).isoformat(),
                        }
                    ).decode()
                )
        else:
            logger.info(
                orjson.dumps(
                    {
                        "event": "startup_cookie_config_ok",
                        "cookie_name": _cm.COOKIE_CFG.name,
                        "samesite": _cm.COOKIE_CFG.samesite,
                        "secure": _cm.COOKIE_CFG.secure,
                        "path": _cm.COOKIE_CFG.path,
                        "domain": _cm.COOKIE_CFG.domain,
                        "max_age_seconds": _cm.COOKIE_CFG.max_age_seconds,
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }
                ).decode()
            )

    except Exception as exc:
        # Cookie config jest krytyczna — bez niej endpointy auth nie działają
        logger.critical(
            orjson.dumps(
                {
                    "event": "startup_cookie_config_failed",
                    "error": str(exc),
                    "severity": "CRITICAL",
                    "hint": (
                        "Sprawdź pola COOKIE_* w .env.docker: "
                        "COOKIE_NAME, COOKIE_SECURE, COOKIE_SAMESITE, "
                        "COOKIE_PATH, COOKIE_DOMAIN"
                    ),
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )
        raise RuntimeError(f"Inicjalizacja cookie config nieudana: {exc}") from exc

    # ── KROK 7: Startup zakończony ────────────────────────────────────────────
    elapsed_ms = (datetime.now(timezone.utc) - start_ts).total_seconds() * 1000

    logger.info(
        orjson.dumps(
            {
                "event": "app_startup_complete",
                "version": _APP_VERSION,
                "elapsed_ms": round(elapsed_ms, 1),
                "environment": getattr(settings, "ENVIRONMENT", "production"),
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ).decode()
    )

    # ── Aplikacja działa ──────────────────────────────────────────────────────
    # ── KROK 7: WAPRO connection pool (pyodbc, read-only) ────────────────────
    try:
        wapro_initialize_pool(
            connection_string=settings.get_odbc_dsn(),
            pool_size=5,
            timeout=30,
            executor_workers=10,
        )
        logger.info(
            orjson.dumps({
                "event": "startup_wapro_pool_ok",
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
    except Exception as exc:
        logger.critical(
            orjson.dumps({
                "event": "startup_wapro_pool_error",
                "error": str(exc),
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        raise  # WAPRO niedostępne = aplikacja nie startuje

    # ── KROK 8: ARQ Pool (enqueue tasków do workera) ──────────────────────────
    try:
        await init_arq_pool()
        logger.info(
            orjson.dumps({
                "event": "startup_arq_pool_ok",
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
    except Exception as exc:
        # Celowo NIE robimy raise — API działa bez workera
        # OTP fallback = tylko JSONL, monity nie będą wysyłane
        logger.error(
            orjson.dumps({
                "event": "startup_arq_pool_error",
                "error": str(exc),
                "note": "Worker niedostępny — OTP/monity tylko JSONL fallback",
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )

    yield

    # ══════════════════════════════════════════════════════════════════════════
    # SHUTDOWN
    # ══════════════════════════════════════════════════════════════════════════

    logger.info(
        orjson.dumps(
            {
                "event": "app_shutdown_begin",
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ).decode()
    )

    try:
        # ── Zamknij WAPRO pool ────────────────────────────────────────────────────
        try:
            await close_arq_pool()
            logger.info(
                orjson.dumps({
                    "event": "app_shutdown_arq_closed",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )
        except Exception as exc:
            logger.error(
                orjson.dumps({
                    "event": "app_shutdown_arq_error",
                    "error": str(exc),
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )

        try:
            wapro_shutdown_pool()
            logger.info(
                orjson.dumps({
                    "event": "app_shutdown_wapro_closed",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )
        except Exception as exc:
            logger.error(
                orjson.dumps({
                    "event": "app_shutdown_wapro_error",
                    "error": str(exc),
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )

        # ← istniejący close_db_engine() już tu jest
        await close_db_engine()
        logger.info(
            orjson.dumps(
                {
                    "event": "app_shutdown_db_closed",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )
    except Exception as exc:
        logger.error(
            orjson.dumps(
                {
                    "event": "app_shutdown_db_error",
                    "error": str(exc),
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )

    logger.info(
        orjson.dumps(
            {
                "event": "app_stopped",
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ).decode()
    )

# ─────────────────────────────────────────────────────────────────────────────
# APLIKACJA FastAPI
# ─────────────────────────────────────────────────────────────────────────────

def create_app() -> FastAPI:
    """
    Factory funkcja tworząca instancję FastAPI.
    Wywoływana raz przy starcie procesu (lub wielokrotnie w testach).

    Kolejność middleware jest WAŻNA w Starlette/FastAPI:
      Middleware są przetwarzane w odwrotnej kolejności dodania (LIFO).
      Zewnętrzny (dodany ostatni) przetwarza request pierwszy.
      Dlatego: DynamicCORSMiddleware dodajemy OSTATNI → obsłuży CORS jako pierwszy.
    """
    settings = get_settings()

    app = FastAPI(
        title=_APP_TITLE,
        description=_APP_DESCRIPTION,
        version=_APP_VERSION,
        contact=_APP_CONTACT,
        docs_url="/api/v1/docs",
        redoc_url="/api/v1/redoc",
        openapi_url="/api/v1/openapi.json",
        lifespan=lifespan,
        # Wyłącz domyślny error handler — używamy własnych poniżej
        swagger_ui_parameters={
            "defaultModelsExpandDepth": 1,
            "syntaxHighlight.theme": "monokai",
            "persistAuthorization": True,  # JWT zapamiętany między refreshami
        },
    )

    # ── Exception Handlers ────────────────────────────────────────────────────
    _register_exception_handlers(app)

    # ── Middleware (LIFO — ostatni dodany = pierwsza warstwa) ─────────────────
    # 1. AuditMiddleware — wewnętrzna, loguje wszystkie requesty
    app.add_middleware(AuditMiddleware)

    # 2. DynamicCORSMiddleware — zewnętrzna, obsługuje CORS przed resztą
    app.add_middleware(DynamicCORSMiddleware)

    # ── Routery ───────────────────────────────────────────────────────────────
    _register_routers(app)

    # ── Endpointy bazowe ──────────────────────────────────────────────────────
    _register_base_endpoints(app)

    return app


# ─────────────────────────────────────────────────────────────────────────────
# EXCEPTION HANDLERS
# ─────────────────────────────────────────────────────────────────────────────

def _register_exception_handlers(app: FastAPI) -> None:
    """Rejestruje ujednolicone handlery wyjątków dla całej aplikacji."""

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        """
        Handler błędów walidacji Pydantic / FastAPI.
        Konwertuje listę błędów Pydantic na nasz format BaseResponse.

        Pydantic v2 errors mają strukturę:
          [{"type": "string_too_short", "loc": ["body", "email"], "msg": "...", ...}]

        My zwracamy:
          [{"field": "email", "message": "..."}]
        """
        request_id = getattr(request.state, "request_id", None) or str(uuid.uuid4())

        errors: list[dict[str, str]] = []
        for error in exc.errors():
            # loc to tuple: ("body", "field_name") lub ("query", "param")
            loc = error.get("loc", [])
            field = ".".join(str(part) for part in loc if part not in ("body", "query", "path"))
            if not field:
                field = "_"
            errors.append(
                {
                    "field": field,
                    "message": error.get("msg", "Błąd walidacji"),
                }
            )

        logger.warning(
            orjson.dumps(
                {
                    "event": "validation_error",
                    "path": str(request.url.path),
                    "method": request.method,
                    "errors_count": len(errors),
                    "request_id": request_id,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )

        return _error_response(
            code="validation.error",
            message="Błąd walidacji danych wejściowych",
            errors=errors,
            request_id=request_id,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        )

    @app.exception_handler(HTTPException)
    async def http_exception_handler(
        request: Request, exc: HTTPException
    ) -> JSONResponse:
        """
        Handler HTTPException — używany przez wszystkie endpointy i dependencies.

        Oczekujemy że exc.detail to dict z polami:
          {"code": str, "message": str, "errors": list}
        Jeśli detail to prosty string — opakowujemy go.
        """
        request_id = getattr(request.state, "request_id", None) or str(uuid.uuid4())

        detail = exc.detail
        if isinstance(detail, dict):
            code = detail.get("code", "http.error")
            message = detail.get("message", str(exc.detail))
            errors = detail.get("errors") or [{"field": "_", "message": message}]
        else:
            code = f"http.{exc.status_code}"
            message = str(detail) if detail else "Błąd HTTP"
            errors = [{"field": "_", "message": message}]

        # Loguj tylko 5xx jako error — 4xx jako warning
        log_level = logging.ERROR if exc.status_code >= 500 else logging.WARNING
        logger.log(
            log_level,
            orjson.dumps(
                {
                    "event": "http_exception",
                    "status_code": exc.status_code,
                    "code": code,
                    "path": str(request.url.path),
                    "method": request.method,
                    "request_id": request_id,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode(),
        )

        # Przekaż nagłówki z wyjątku (np. WWW-Authenticate: Bearer)
        response = _error_response(
            code=code,
            message=message,
            errors=errors,
            request_id=request_id,
            status_code=exc.status_code,
        )
        if exc.headers:
            for key, val in exc.headers.items():
                response.headers[key] = val
        return response

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        """
        Catch-all dla nieobsłużonych wyjątków.
        Zawsze zwraca HTTP 500. Nigdy nie ujawnia szczegółów wyjątku klientowi.
        Pełny traceback logowany po stronie serwera.
        """
        request_id = getattr(request.state, "request_id", None) or str(uuid.uuid4())

        logger.exception(
            orjson.dumps(
                {
                    "event": "unhandled_exception",
                    "exception_type": type(exc).__name__,
                    "path": str(request.url.path),
                    "method": request.method,
                    "request_id": request_id,
                    "severity": "CRITICAL",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )

        return _error_response(
            code="server.internal_error",
            message="Wewnętrzny błąd serwera. Skontaktuj się z administratorem.",
            errors=[
                {
                    "field": "_",
                    "message": f"Identyfikator błędu: {request_id}",
                }
            ],
            request_id=request_id,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


# ─────────────────────────────────────────────────────────────────────────────
# ROUTERY
# ─────────────────────────────────────────────────────────────────────────────

def _register_routers(app: FastAPI) -> None:
    """
    Rejestruje wszystkie sub-routery API przez centralny api/router.py.
    Import jest wewnątrz funkcji — zapobiega circular imports przy starcie.

    Prefix globalny: /api/v1/ (ustawiony w api/router.py, nie tutaj)
    Tags dla Swagger generowane przez każdy sub-router.
    """
    try:
        from app.api.router import api_router

        app.include_router(api_router)
        logger.debug(
            orjson.dumps(
                {
                    "event": "routers_registered",
                    "router": "api_router",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )
    except ImportError as exc:
        # api/router.py jeszcze nie istnieje (np. podczas TDD) → ostrzeżenie, nie crash
        logger.warning(
            orjson.dumps(
                {
                    "event": "routers_import_failed",
                    "error": str(exc),
                    "note": "api/router.py nie istnieje — endpointy niedostępne",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINTY BAZOWE (poza prefixem /api/v1/)
# ─────────────────────────────────────────────────────────────────────────────

def _register_base_endpoints(app: FastAPI) -> None:
    """Rejestruje endpointy infrastrukturalne (liveness probe, redirect)."""

    @app.get(
        "/",
        include_in_schema=False,
        summary="Redirect do dokumentacji",
    )
    async def root_redirect() -> RedirectResponse:
        """Przekierowanie z / do Swagger UI."""
        return RedirectResponse(url="/api/v1/docs", status_code=302)

    @app.get(
        "/health",
        tags=["Infrastruktura"],
        summary="Liveness probe",
        description=(
            "Punkt sprawdzania dostępności dla Docker HEALTHCHECK i load balancerów. "
            "Nie wymaga uwierzytelnienia. Zwraca HTTP 200 gdy aplikacja działa. "
            "Szczegółowy health check (DB, Redis, WAPRO) dostępny przez "
            "GET /api/v1/system/health [wymaga uprawnienia system.view_health]."
        ),
        response_description="Aplikacja działa",
    )
    async def health_check() -> JSONResponse:
        """
        Lekki liveness probe — sprawdza tylko czy proces żyje.
        Nie odpytuje bazy ani Redis — te sprawdzane przy starcie i przez /system/health.
        """
        return JSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "version": _APP_VERSION,
                "ts": datetime.now(timezone.utc).isoformat(),
            },
        )


# ─────────────────────────────────────────────────────────────────────────────
# INSTANCJA APLIKACJI (module-level — używana przez Uvicorn / Gunicorn)
# ─────────────────────────────────────────────────────────────────────────────

app = create_app()


# ─────────────────────────────────────────────────────────────────────────────
# URUCHOMIENIE BEZPOŚREDNIE (python -m app.main lub python main.py)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    settings = get_settings()

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=getattr(settings, "DEBUG", False),
        log_config=None,          # Wyłącz domyślny uvicorn logging — używamy własnego
        access_log=False,         # AuditMiddleware obsługuje logi requestów
        server_header=False,      # Nie ujawniaj wersji Uvicorn w nagłówkach
        date_header=True,
        workers=1,                # Więcej workerów = osobny Gunicorn (nie uvicorn.run)
        loop="uvloop",            # Szybszy event loop (uvicorn[standard] go instaluje)
    )

