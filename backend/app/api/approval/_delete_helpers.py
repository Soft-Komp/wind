# backend/app/api/approval/_delete_helpers.py
"""
Helper dwuetapowego usuwania dla modulu Approval.

Wzorzec zgodny z projektem (koszt_service, template_service, roles_permissions):
  - JWT podpisany SECRET_KEY, algo HS256
  - Jednorazowosc przez JTI w Redis (TTL = DELETE_TOKEN_TTL)
  - Krok 1: DELETE /{id}/initiate   → 202, body: {delete_token, expires_in, ...}
  - Krok 2: DELETE /{id}/confirm    → 200, body wejsciowe: {delete_token: "eyJ..."}

Uzycie w routerze:
    from app.api.approval._delete_helpers import generate_delete_token, verify_delete_token

    # Krok 1
    token, ttl = await generate_delete_token(redis, entity_id=id_group, scope="delete_group")

    # Krok 2
    await verify_delete_token(redis, token=delete_token, entity_id=id_group, scope="delete_group")

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException, status
from jose import JWTError, jwt as jose_jwt
from redis.asyncio import Redis

from app.core.config import get_settings

DELETE_TOKEN_TTL = 60  # sekund — zgodnie z projektem


def _get_secret() -> str:
    return get_settings().secret_key.get_secret_value()


async def generate_delete_token(
    redis: Redis,
    entity_id: int,
    scope: str,
    initiated_by: int,
    extra: dict | None = None,
) -> tuple[str, int]:
    """
    Generuje jednorazowy JWT token potwierdzajacy usuniecie.

    Args:
        redis:        Klient Redis.
        entity_id:    ID usuwanej encji.
        scope:        Identyfikator operacji (np. 'delete_group', 'delete_comment').
        initiated_by: ID usera inicjujacego.
        extra:        Opcjonalne dodatkowe pola do payload.

    Returns:
        (token: str, expires_in: int) — token JWT i TTL w sekundach.
    """
    jti = uuid.uuid4().hex
    now = datetime.now(timezone.utc)
    payload = {
        "sub":          str(entity_id),
        "scope":        scope,
        "initiated_by": initiated_by,
        "jti":          jti,
        "iat":          int(now.timestamp()),
        "exp":          int((now + timedelta(seconds=DELETE_TOKEN_TTL)).timestamp()),
        **(extra or {}),
    }
    token = jose_jwt.encode(payload, _get_secret(), algorithm="HS256")
    await redis.set(f"del_jti:{jti}", str(entity_id), ex=DELETE_TOKEN_TTL)
    return token, DELETE_TOKEN_TTL


async def verify_delete_token(
    redis: Redis,
    token: str,
    entity_id: int,
    scope: str,
) -> dict:
    """
    Weryfikuje token, sprawdza jednorazowosc JTI, unierwaznia.

    Args:
        redis:     Klient Redis.
        token:     Token JWT z kroku 1.
        entity_id: ID encji ktora ma byc usunieta.
        scope:     Oczekiwany scope operacji.

    Returns:
        Zdekodowany payload JWT.

    Raises:
        HTTPException(400): Token nieprawidlowy, wygasl, uzyty lub nie dotyczy tej encji.
    """
    try:
        payload = jose_jwt.decode(token, _get_secret(), algorithms=["HS256"])
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "delete.invalid_token",
                    "message": "Token nieprawidlowy lub wygasl."},
        )

    if str(payload.get("sub")) != str(entity_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "delete.token_mismatch",
                    "message": "Token nie dotyczy tej encji."},
        )

    if payload.get("scope") != scope:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "delete.wrong_scope",
                    "message": "Nieprawidlowy typ tokenu."},
        )

    jti = payload.get("jti", "")
    redis_key = f"del_jti:{jti}"
    stored = await redis.get(redis_key)
    if not stored:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "delete.token_used",
                    "message": "Token wygasl lub zostal juz uzyty."},
        )

    # Uniewaznienie — jednorazowosc
    await redis.delete(redis_key)
    return payload