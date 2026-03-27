"""
app/core/idempotency.py
=======================
Ochrona przed podwójnym kliknięciem — idempotentność requestów POST.

MECHANIZM (Sprint 2, Sekcja 6.3):
    Klucz deterministyczny bez udziału frontendu:
    key = SHA256(user_id:method:path:SHA256(body)[:16]:timestamp//window)

    Trzy stany w Redis:
    1. SETNX idem:{hash} 'processing' EX 30  → True  = pierwsze żądanie → wykonaj
    2. GET   idem:{hash}:result              → JSON  = zwróć wynik z cache
    3. GET   idem:{hash}:error               → JSON  = zwróć błąd z cache

    Odpowiedź z cache: dodaje nagłówek X-Idempotency-Replayed: true

ENDPOINTY CHRONIONE:
    • POST /faktury-akceptacja          TTL 30s
    • POST /moje-faktury/{id}/decyzja   TTL 30s
    • POST /faktury-akceptacja/{id}/reset/confirm   TTL 60s
    • PATCH /faktury-akceptacja/{id}/status/confirm TTL 60s

FALLBACK:
    Przy niedostępności Redis: loguj WARNING, przepuść request.
    Idempotency to ochrona UX, nie blokada bezpieczeństwa.

UŻYCIE jako FastAPI Depends():
    @router.post("/faktury-akceptacja")
    async def create_faktura(
        request: Request,
        body: FakturaCreateRequest,
        current_user: CurrentUser = Depends(require_permission("faktury.create")),
        idempotency: IdempotencyResult = Depends(
            IdempotencyGuard(window_seconds=10, result_ttl=30)
        ),
    ):
        if idempotency.is_replay:
            return idempotency.cached_response
        # ... logika biznesowa ...
        result = await do_something()
        await idempotency.store_result(result, status_code=201)
        return result
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import orjson
from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

from app.core.dependencies import get_redis

logger = logging.getLogger("app.core.idempotency")

# ─────────────────────────────────────────────────────────────────────────────
# Stałe
# ─────────────────────────────────────────────────────────────────────────────

_KEY_PREFIX        = "idem"
_PROCESSING_VALUE  = b"processing"
_PROCESSING_TTL    = 30   # sekund — timeout na "processing" (ochrona przed zawieszeniem)
_DEFAULT_WINDOW    = 10   # sekund — okno idempotentności
_DEFAULT_RESULT_TTL = 30  # sekund — jak długo przechowujemy wynik


# ─────────────────────────────────────────────────────────────────────────────
# Dataclass wyniku
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class IdempotencyResult:
    """
    Wynik sprawdzenia idempotentności.
    Przekazywany do endpoint function przez Depends().
    """
    is_replay:        bool           = False
    cached_response:  Optional[Any]  = None  # JSONResponse jeśli is_replay=True
    idempotency_key:  str            = ""
    operation_id:     str            = field(default_factory=lambda: str(uuid.uuid4()))
    _redis:           Optional[Redis] = field(default=None, repr=False)
    _result_ttl:      int            = 30

    async def store_result(
        self,
        response_data: Any,
        status_code:   int = 200,
    ) -> None:
        """
        Zapisuje wynik do Redis po pomyślnym wykonaniu endpointu.
        Wywołaj to na końcu logiki biznesowej.
        """
        if not self._redis or not self.idempotency_key:
            return

        result_key = f"{self.idempotency_key}:result"
        payload = orjson.dumps({
            "status_code": status_code,
            "data":        response_data if isinstance(response_data, dict)
                           else (response_data.model_dump() if hasattr(response_data, "model_dump")
                                 else str(response_data)),
            "stored_at":   datetime.now(timezone.utc).isoformat(),
        })

        try:
            await self._redis.setex(result_key, self._result_ttl, payload)
            # Usuń klucz "processing" — zastąpiony przez "result"
            await self._redis.delete(self.idempotency_key)
            logger.debug(
                orjson.dumps({
                    "event":           "idempotency_result_stored",
                    "key":             result_key,
                    "ttl":             self._result_ttl,
                    "operation_id":    self.operation_id,
                    "ts":              datetime.now(timezone.utc).isoformat(),
                }).decode()
            )
        except Exception as exc:
            logger.warning(
                orjson.dumps({
                    "event":        "idempotency_store_failed",
                    "error":        str(exc),
                    "operation_id": self.operation_id,
                    "ts":           datetime.now(timezone.utc).isoformat(),
                }).decode()
            )

    async def store_error(
        self,
        status_code: int,
        detail:      Any,
    ) -> None:
        """
        Zapisuje błąd do Redis — kolejne zduplikowane requesty dostaną ten sam błąd.
        """
        if not self._redis or not self.idempotency_key:
            return

        error_key = f"{self.idempotency_key}:error"
        payload = orjson.dumps({
            "status_code": status_code,
            "detail":      detail,
            "stored_at":   datetime.now(timezone.utc).isoformat(),
        })

        try:
            await self._redis.setex(error_key, self._result_ttl, payload)
            await self._redis.delete(self.idempotency_key)
        except Exception as exc:
            logger.warning(f"Idempotency store_error failed: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Generowanie klucza
# ─────────────────────────────────────────────────────────────────────────────

def _compute_idempotency_key(
    user_id:        int,
    method:         str,
    path:           str,
    body_bytes:     bytes,
    window_seconds: int,
) -> str:
    """
    Deterministyczny klucz idempotentności.

    Format: idem:{SHA256(user_id:method:path:body_hash16:bucket)}
    bucket  = int(timestamp // window_seconds)  — zmienia się co window_seconds

    Dwa identyczne requesty w oknie window_seconds → ten sam klucz.
    """
    body_hash = hashlib.sha256(body_bytes).hexdigest()[:16]
    bucket    = int(time.time()) // window_seconds
    raw       = f"{user_id}:{method.upper()}:{path}:{body_hash}:{bucket}"
    digest    = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"{_KEY_PREFIX}:{digest}"


# ─────────────────────────────────────────────────────────────────────────────
# Dependency Factory
# ─────────────────────────────────────────────────────────────────────────────

class IdempotencyGuard:
    """
    FastAPI Dependency Factory dla ochrony idempotentności.

    Użycie:
        _guard = IdempotencyGuard(window_seconds=10, result_ttl=30)

        @router.post("/endpoint")
        async def my_endpoint(
            request: Request,
            idem: IdempotencyResult = Depends(_guard),
        ):
            if idem.is_replay:
                return idem.cached_response
            # ... logika ...
            await idem.store_result(response_data)
            return response_data
    """

    def __init__(
        self,
        window_seconds: int = _DEFAULT_WINDOW,
        result_ttl:     int = _DEFAULT_RESULT_TTL,
    ) -> None:
        self.window_seconds = window_seconds
        self.result_ttl     = result_ttl

    async def __call__(
        self,
        request: Request,
        redis:   Redis = Depends(get_redis),
    ) -> IdempotencyResult:
        """
        Sprawdza idempotentność requestu.
        Wymaga że user_id jest w request.state (ustawiony przez get_current_user).
        """
        # Wyciągnij user_id ze stanu requestu (ustawiany przez auth dependency)
        user = getattr(request.state, "current_user", None)
        user_id = getattr(user, "user_id", None) if user else None

        if user_id is None:
            # Brak usera w stanie — endpoint nie chroniony auth lub błąd konfiguracji
            logger.warning(
                orjson.dumps({
                    "event": "idempotency_no_user",
                    "path":  request.url.path,
                    "ts":    datetime.now(timezone.utc).isoformat(),
                }).decode()
            )
            return IdempotencyResult(operation_id=str(uuid.uuid4()))

        # Odczytaj body (FastAPI już je odczytał — musimy użyć cache)
        try:
            body_bytes = await request.body()
        except Exception:
            body_bytes = b""

        key = _compute_idempotency_key(
            user_id=user_id,
            method=request.method,
            path=request.url.path,
            body_bytes=body_bytes,
            window_seconds=self.window_seconds,
        )

        logger.debug(
            orjson.dumps({
                "event":   "idempotency_check",
                "key":     key,
                "user_id": user_id,
                "path":    request.url.path,
                "ts":      datetime.now(timezone.utc).isoformat(),
            }).decode()
        )

        try:
            # Sprawdź czy wynik już istnieje
            result_key = f"{key}:result"
            error_key  = f"{key}:error"

            cached_result = await redis.get(result_key)
            if cached_result:
                payload = orjson.loads(cached_result)
                logger.info(
                    orjson.dumps({
                        "event":    "idempotency_replay_result",
                        "key":      key,
                        "user_id":  user_id,
                        "ts":       datetime.now(timezone.utc).isoformat(),
                    }).decode()
                )
                response = JSONResponse(
                    content=payload.get("data"),
                    status_code=payload.get("status_code", 200),
                    headers={"X-Idempotency-Replayed": "true"},
                )
                return IdempotencyResult(
                    is_replay=True,
                    cached_response=response,
                    idempotency_key=key,
                )

            cached_error = await redis.get(error_key)
            if cached_error:
                payload = orjson.loads(cached_error)
                logger.info(
                    orjson.dumps({
                        "event":   "idempotency_replay_error",
                        "key":     key,
                        "user_id": user_id,
                        "ts":      datetime.now(timezone.utc).isoformat(),
                    }).decode()
                )
                raise HTTPException(
                    status_code=payload.get("status_code", 400),
                    detail=payload.get("detail"),
                    headers={"X-Idempotency-Replayed": "true"},
                )

            # Sprawdź czy inny request już przetwarza to żądanie
            is_first = await redis.setnx(key, _PROCESSING_VALUE)
            if is_first:
                await redis.expire(key, _PROCESSING_TTL)
                return IdempotencyResult(
                    idempotency_key=key,
                    _redis=redis,
                    _result_ttl=self.result_ttl,
                )
            else:
                # Inny request już przetwarza — 409
                logger.warning(
                    orjson.dumps({
                        "event":   "idempotency_concurrent",
                        "key":     key,
                        "user_id": user_id,
                        "ts":      datetime.now(timezone.utc).isoformat(),
                    }).decode()
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Identyczne żądanie jest właśnie przetwarzane. Odczekaj chwilę.",
                )

        except HTTPException:
            raise
        except Exception as exc:
            # Redis niedostępny — fallback: przepuść request bez idempotency
            logger.warning(
                orjson.dumps({
                    "event":   "idempotency_redis_unavailable",
                    "error":   str(exc),
                    "path":    request.url.path,
                    "user_id": user_id,
                    "ts":      datetime.now(timezone.utc).isoformat(),
                }).decode()
            )
            return IdempotencyResult(operation_id=str(uuid.uuid4()))


# ─────────────────────────────────────────────────────────────────────────────
# Pre-built guards dla modułu faktur
# ─────────────────────────────────────────────────────────────────────────────

# POST /faktury-akceptacja — okno 10s, wynik przechowywany 30s
faktury_create_guard = IdempotencyGuard(window_seconds=10, result_ttl=30)

# POST /moje-faktury/{id}/decyzja — okno 10s, wynik 30s
decyzja_guard = IdempotencyGuard(window_seconds=10, result_ttl=30)

# POST /faktury-akceptacja/{id}/reset/confirm — okno=TTL tokenu (60s)
reset_confirm_guard = IdempotencyGuard(window_seconds=60, result_ttl=60)

# PATCH /faktury-akceptacja/{id}/status/confirm — okno=TTL tokenu (60s)
force_status_confirm_guard = IdempotencyGuard(window_seconds=60, result_ttl=60)