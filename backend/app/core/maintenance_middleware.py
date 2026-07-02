# backend/app/core/maintenance_middleware.py
"""
Middleware serwisowy — dodaje sekcje maintenance do kazdej odpowiedzi JSON.

Dzialanie:
  - Gdy maintenance_mode.enabled = 'true' w SystemConfig:
    kazda odpowiedz JSON dostaje dodatkowy klucz "maintenance": {}
  - Gdy 'false' (domyslnie): middleware jest przezroczysty, zero narzutu

Stan flagi cachowany w Redis (klucz: cfg:maintenance_mode.enabled, TTL 30s).
Zmiana w SystemConfig propaguje sie do wszystkich instancji API w max 30s
bez restartu.

Rejestracja w backend/app/main.py — patrz instrukcja ponizej.

UWAGA: from __future__ import annotations OK (nie ORM, nie router).
"""

from __future__ import annotations

import json
import logging
from typing import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)

_CACHE_KEY = "cfg:maintenance_mode.enabled"
_CACHE_TTL = 30  # sekund — czas propagacji zmiany flagi

# Sciezki ktore NIGDY nie dostaja sekcji maintenance
# (health check musi dzialac nawet gdy tryb serwisowy wlaczony)
_EXCLUDED_PATHS = frozenset([
    "/health",
    "/api/v1/openapi.json",
    "/api/v1/docs",
    "/api/v1/redoc",
])


class MaintenanceMiddleware(BaseHTTPMiddleware):
    """
    Middleware dodajacy sekcje maintenance do odpowiedzi JSON.

    Projektowany jako zero-narzut gdy tryb serwisowy wylaczony:
      - Odczyt z Redis (< 1ms)
      - Brak modyfikacji response gdy flaga = false
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        response: Response = await call_next(request)

        # Szybka sciezka — wykluczone endpointy
        if request.url.path in _EXCLUDED_PATHS:
            return response

        # Szybka sciezka — nie JSON
        content_type = response.headers.get("content-type", "")
        if "application/json" not in content_type:
            return response

        # Sprawdz flage z Redis (z cache)
        maintenance_enabled = await _is_maintenance_enabled(request)
        if not maintenance_enabled:
            return response

        # Pobierz dane maintenance
        maintenance_data = await _get_maintenance_data(request)

        # Dodaj wymagane uprawnienie dla tego endpointu
        required_permission = getattr(request.state, "required_permission", None)
        if required_permission:
            maintenance_data["required_permission"] = required_permission

        # Wstrzyknij sekcje maintenance do body JSON
        return await _inject_maintenance(response, maintenance_data)


async def _is_maintenance_enabled(request: Request) -> bool:
    """
    Sprawdza flage maintenance w kolejnosci priorytetow:
      1. Zmienna srodowiskowa MAINTENANCE_MODE=on  — natychmiastowa, bez restartu DB/Redis
      2. Redis cache (TTL 30s)
      3. Baza danych (SystemConfig: maintenance_mode.enabled)
      4. Fallback: False (serwis dziala normalnie)

    ENV nadpisuje wszystko — mozna wlaczyc maintenance bez dostepu do bazy i frontu.
    """
    import os

    # Priorytet 1 — zmienna srodowiskowa (natychmiastowa, bez restartu)
    env_val = os.environ.get("MAINTENANCE_MODE", "").strip().lower()
    if env_val in ("on", "true", "1"):
        return True
    if env_val in ("off", "false", "0"):
        return False
    # Brak zmiennej — sprawdz baze/Redis
    # Probuj z Redis (najszybsze)
    redis = getattr(request.app.state, "redis", None)
    if redis:
        try:
            cached = await redis.get(_CACHE_KEY)
            if cached is not None:
                return cached.decode() == "true" if isinstance(cached, bytes) else str(cached) == "true"
        except Exception as exc:
            logger.debug("maintenance_middleware: Redis niedostepny: %s", exc)

    # Fallback na baze
    try:
        from app.services.config_service import get_config
        value = await get_config("maintenance_mode.enabled", default="false")
        enabled = str(value).lower() == "true"

        # Zapisz do Redis na kolejne requesty
        if redis:
            try:
                await redis.set(_CACHE_KEY, "true" if enabled else "false", ex=_CACHE_TTL)
            except Exception:
                pass

        return enabled
    except Exception as exc:
        logger.debug("maintenance_middleware: blad odczytu config: %s", exc)
        return False


async def _get_maintenance_data(request: Request) -> dict:
    """
    Zwraca dane konserwacyjne: stan ARQ, synchronizacja zrodel, OCR.
    Cachowane w Redis (TTL 15s) — baza odpytywana max raz na 15 sekund.
    """
    try:
        from app.services.maintenance_data_service import get_maintenance_data
        from app.core.dependencies import get_db

        redis = getattr(request.app.state, "redis", None)

        async for db in get_db():
            return await get_maintenance_data(db, redis)
    except Exception as exc:
        logger.warning("_get_maintenance_data: blad zbierania danych: %s", exc)

    return {}


async def _inject_maintenance(response: Response, maintenance_data: dict) -> Response:
    """
    Deserializuje body JSON, dodaje klucz 'maintenance', serializuje z powrotem.
    Tworzy nowy Response z tym samym statusem i naglowkami.
    """
    try:
        # Odczytaj body
        body_bytes = b""
        async for chunk in response.body_iterator:
            body_bytes += chunk if isinstance(chunk, bytes) else chunk.encode()

        # Parsuj JSON
        body_dict = json.loads(body_bytes)

        # Dodaj sekcje maintenance
        if isinstance(body_dict, dict):
            body_dict["maintenance"] = maintenance_data

        # Serializuj z powrotem
        new_body = json.dumps(body_dict, ensure_ascii=False, default=str).encode("utf-8")

        # Buduj nowy Response z zaktualizowanym body i naglowkami
        from starlette.responses import Response as StarletteResponse
        new_response = StarletteResponse(
            content=new_body,
            status_code=response.status_code,
            media_type="application/json",
        )
        # Skopiuj naglowki (poza content-length — zmieni sie po modyfikacji)
        for key, value in response.headers.items():
            if key.lower() != "content-length":
                new_response.headers[key] = value

        return new_response

    except (json.JSONDecodeError, AttributeError) as exc:
        # Nie udalo sie sparsowac — zwroc oryginalna odpowiedz bez zmian
        logger.warning("maintenance_middleware: nie mozna sparsowac JSON: %s", exc)
        return response
    except Exception as exc:
        logger.error("maintenance_middleware: nieoczekiwany blad: %s", exc)
        return response