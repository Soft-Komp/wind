from __future__ import annotations
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

_EXEMPT_PATHS = {"/health", "/healthz", "/api/v1/health"}

class ApplicationEnabledMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path in _EXEMPT_PATHS:
            return await call_next(request)

        redis_client = request.app.state.redis
        enabled = await redis_client.get("app:enabled")
        if enabled == "0":
            return JSONResponse(
                status_code=503,
                content={"code": "application.disabled",
                         "message": "Aplikacja została wyłączona - skontaktuj się z administratorem"},
            )
        return await call_next(request)