# backend/app/core/endpoint_toggle_middleware.py
"""
Middleware wlacznikow endpointow.

Przy kazdym requeście sprawdza czy endpoint jest wlaczony w skw_EndpointRegistry.
Gdy wylaczony — zwraca 503 Service Unavailable z komunikatem.

Hot path: Redis cache (TTL 30s) -> baza. Przy bledzie Redis/DB -> fail-open
(nie blokuje requestu — bezpieczenstwo > dostepnosc jest tu odwrocone,
bo wylacznik jest narzedziem operacyjnym, nie zabezpieczeniem).

Wykluczone z kontroli:
  - /health, /docs, /redoc, /openapi.json
  - /api/v1/admin/endpoints (sam panel zarzadzania)
  - endpointy bez metody HTTP (WebSocket itp.)

UWAGA: from __future__ import annotations OK (nie ORM, nie router).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)

_EXCLUDED_PATHS = frozenset([
    "/health",
    "/api/v1/docs",
    "/api/v1/redoc",
    "/api/v1/openapi.json",
    "/api/v1/admin/endpoints",        # sam panel zarzadzania wlacznikami
    "/api/v1/admin/endpoints/toggle", # endpointy toggle nie moga byc wylaczone
])

_EXCLUDED_PREFIXES = (
    "/api/v1/admin/endpoints",
)


class EndpointToggleMiddleware(BaseHTTPMiddleware):
    """
    Middleware sprawdzajacy wlacznik endpointu przed przetworzeniem requestu.
    Rejestruje endpoint lazy jesli nie ma go jeszcze w rejestrze.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        path   = request.url.path
        method = request.method

        # Szybka sciezka — wykluczone
        if path in _EXCLUDED_PATHS:
            return await call_next(request)
        if any(path.startswith(p) for p in _EXCLUDED_PREFIXES):
            return await call_next(request)

        # Pobierz endpoint_key z trasy FastAPI (zawiera parametry jako {param})
        endpoint_key = _get_endpoint_key(request, method)
        if not endpoint_key:
            return await call_next(request)

        # Zapisz w state dla middleware maintenance
        request.state.endpoint_key = endpoint_key

        # Sprawdz wlacznik
        try:
            from app.services.endpoint_registry_service import is_enabled
            from app.core.dependencies import get_db

            redis = getattr(request.app.state, "redis", None)

            enabled = True
            disabled_reason = None

            async for db in get_db():
                enabled = await is_enabled(db, redis, endpoint_key)

                if not enabled:
                    # Pobierz powod wylaczenia do komunikatu
                    from sqlalchemy import text
                    result = await db.execute(
                        text(
                            "SELECT [disabled_reason], [disabled_at] "
                            "FROM [dbo].[skw_EndpointRegistry] "
                            "WHERE [endpoint_key] = :key"
                        ),
                        {"key": endpoint_key},
                    )
                    row = result.fetchone()
                    if row:
                        disabled_reason = row[0]

            if not enabled:
                return _disabled_response(endpoint_key, disabled_reason)

        except Exception as exc:
            # Fail-open — blad sprawdzania nie blokuje requestu
            logger.error(
                "EndpointToggleMiddleware: blad sprawdzania %s: %s",
                endpoint_key, exc,
            )

        return await call_next(request)


def _get_endpoint_key(request: Request, method: str) -> str | None:
    """
    Pobiera endpoint_key z trasy FastAPI.
    Zwraca format "METHOD:/sciezka/{param}" np. "GET:/documents/{id_instance}/status-summary".
    """
    try:
        from fastapi.routing import APIRoute
        route = request.scope.get("route")
        if route and isinstance(route, APIRoute):
            return f"{method.upper()}:{route.path}"
    except Exception:
        pass
    # Fallback na rzeczywista sciezke (mniej dokladne — parametry sa wartosciami)
    return f"{method.upper()}:{request.url.path}"


def _disabled_response(endpoint_key: str, reason: str | None) -> Response:
    """Zwraca 503 z ustandaryzowanym komunikatem."""
    body = {
        "code":   503,
        "errors": [
            {
                "field":   "endpoint",
                "message": reason or "Ten endpoint jest tymczasowo wylaczony.",
            }
        ],
        "data": None,
        "app_code": "endpoint.disabled",
        "meta": {
            "endpoint_key": endpoint_key,
            "timestamp":    datetime.now(timezone.utc).isoformat(),
        },
    }
    return Response(
        content=json.dumps(body, ensure_ascii=False),
        status_code=503,
        media_type="application/json",
    )