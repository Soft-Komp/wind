"""
api/auth.py
═══════════════════════════════════════════════════════════════════════════════
Router uwierzytelniania — System Windykacja.

12 endpointów:
  POST /auth/login                  — logowanie (username + hasło → JWT)
  POST /auth/logout                 — wylogowanie (blacklista tokena)
  POST /auth/refresh                — odświeżenie access tokena
  POST /auth/otp/request            — krok 1 OTP: generuj kod, wyślij email
  POST /auth/otp/verify             — krok 2 OTP: weryfikuj kod → reset token
  POST /auth/password-reset/confirm — krok 3: ustaw nowe hasło (wymaga reset_token)
  GET  /auth/me                     — dane zalogowanego użytkownika
  POST /auth/me/change-password     — zmiana hasła przez zalogowanego
  POST /auth/impersonate/{user_id}  — start impersonacji [perm: auth.impersonate]
  POST /auth/impersonate/stop       — zakończenie impersonacji
  POST /auth/master-key/login       — logowanie przez Master Key + PIN
  GET  /auth/sessions               — lista aktywnych sesji [perm: auth.view_sessions]

Format odpowiedzi: BaseResponse[T] z schemas/common.py
Rate limiting: obsługiwany przez auth_service (Redis counters)
AuditLog: każda operacja logowana przez audit_service

Zmiany v2.0 (HttpOnly cookie):
  - login:   ustawia refresh_token jako HttpOnly cookie, NIE zwraca go w body
  - refresh: czyta token z cookie (fallback: body dla Postman/API), odnawia cookie
  - logout:  czyta token z cookie (fallback: body), usuwa cookie

Powiązane serwisy:
  services/auth_service.py          — login, logout, refresh, change_password
  services/otp_service.py           — request_otp, verify_otp
  services/impersonation_service.py — start, stop
  core/cookie_manager.py            — set/clear/read HttpOnly cookie

Autor: System Windykacja
Wersja: 2.0.0
Data: 2026-02-27
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Annotated, Optional

import orjson
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.core.dependencies import (
    DB,
    ClientIP,
    CurrentUser,
    RedisClient,
    RequestID,
    get_current_user,
    require_permission,
)
from app.schemas.common import BaseResponse, MessageData, PaginatedData

# ── Nowe importy v2.0 — HttpOnly cookie ──────────────────────────────────────
import app.core.cookie_manager as _cookie_module
from app.core.cookie_manager import (
    clear_refresh_cookie,
    extract_refresh_token_hybrid,
    set_refresh_cookie,
)
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Logger
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Router
# ─────────────────────────────────────────────────────────────────────────────

router = APIRouter()

# Schemat Bearer do wyciągania tokena z nagłówka (auto_error=False — robimy sami)
_bearer = HTTPBearer(auto_error=False)


# ─────────────────────────────────────────────────────────────────────────────
# HELPER v2.0 — bezpieczny dostęp do singletona CookieConfig
# ─────────────────────────────────────────────────────────────────────────────

def _get_cookie_cfg():
    """
    Zwraca globalny CookieConfig zainicjalizowany w lifespan().

    Raises:
        RuntimeError: Jeśli lifespan() nie zainicjalizował COOKIE_CFG.
                      W praktyce niemożliwe — endpointy są wywoływane po starcie.
    """
    cfg = _cookie_module.COOKIE_CFG
    if cfg is None:
        logger.critical(
            orjson.dumps({
                "event": "cookie_cfg_not_initialized",
                "message": (
                    "COOKIE_CFG jest None — lifespan() nie zainicjalizował "
                    "cookie config! Sprawdź main.py → funkcja lifespan()."
                ),
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        raise RuntimeError(
            "CookieConfig nie zainicjalizowany. Sprawdź lifespan() w main.py."
        )
    return cfg


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 1: POST /auth/login
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/login",
    summary="Logowanie użytkownika",
    description=(
        "Logowanie za pomocą nazwy użytkownika i hasła. "
        "Zwraca access token w body. "
        "Refresh token ustawiany jako HttpOnly cookie (niewidoczny dla JS). "
        "**Rate limit:** 10 prób / minutę / IP. "
        "Po 5 nieudanych próbach konto blokowane na 30 minut."
    ),
    response_description="Access token JWT + HttpOnly cookie z refresh tokenem",
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Logowanie pomyślne — access token w body, refresh token w cookie"},
        401: {"description": "Nieprawidłowe dane logowania"},
        423: {"description": "Konto tymczasowo zablokowane"},
        429: {"description": "Zbyt wiele prób logowania"},
    },
)
async def login(
    request: Request,
    response: Response,          # FastAPI inject — do ustawienia Set-Cookie header
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    """
    Logowanie użytkownika — v2.0 (HttpOnly cookie).

    Body (application/json):
        username (str): Nazwa użytkownika (login)
        password (str): Hasło (min 8 znaków)

    Zmiany vs v1.0:
        - Parametr `response: Response` — FastAPI inject, niewidoczny w Swagger
        - Po zalogowaniu: set_refresh_cookie() ustawia HttpOnly cookie
        - Body odpowiedzi NIE zawiera refresh_token (cookie-only mode)
        - Loguje: cookie_set=True, cookie_name

    Returns:
        BaseResponse z: access_token, token_type, expires_in
    """
    from app.schemas.auth import LoginRequest
    from app.services import auth_service

    # ── Parsowanie body ───────────────────────────────────────────────────────
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Nieprawidłowy format JSON",
                "errors": [{"field": "_", "message": "Wymagane dane logowania w formacie JSON"}],
            },
        )

    # ── Walidacja Pydantic ────────────────────────────────────────────────────
    try:
        login_data = LoginRequest(**body)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Błąd walidacji danych logowania",
                "errors": _pydantic_errors(exc),
            },
        )

    user_agent = request.headers.get("User-Agent", "")

    # ── Wywołanie serwisu ─────────────────────────────────────────────────────
    try:
        token_pair = await auth_service.login(
            db=db,
            redis=redis,
            username=login_data.username,
            password=login_data.password,
            ip=client_ip,
            user_agent=user_agent,
        )
    except Exception as exc:
        _raise_from_auth_error(exc)

    # ── Ustaw HttpOnly cookie z refresh tokenem ───────────────────────────────
    cookie_cfg = _get_cookie_cfg()

    set_refresh_cookie(
        response=response,
        token=token_pair.refresh_token,
        config=cookie_cfg,
        request_id=request_id,
        user_id=token_pair.user_id,
        ip=client_ip,
    )

    # ── Log sukcesu ───────────────────────────────────────────────────────────
    logger.info(
        orjson.dumps(
            {
                "event": "api_login_success",
                "user_id": token_pair.user_id,
                "username": token_pair.username,
                "request_id": request_id,
                "ip": client_ip,
                "cookie_set": True,
                "cookie_name": cookie_cfg.name,
                "cookie_samesite": cookie_cfg.samesite,
                "cookie_secure": cookie_cfg.secure,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ).decode()
    )

    # ── Odpowiedź — access token w body, refresh token w cookie ──────────────
    # CELOWO brak pola refresh_token — przeglądarka zarządza cookie automatycznie
    # is_impersonation: zwykłe logowanie nigdy nie jest impersonacją
    # (impersonacja zwraca osobny token przez /auth/impersonate/{user_id})
    return BaseResponse.ok(
        data={
            "access_token": token_pair.access_token,
            "token_type": token_pair.token_type,
            "expires_in": token_pair.expires_in,
        },
        code=200,
        app_code="auth.login_success",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 2: POST /auth/logout
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/logout",
    summary="Wylogowanie użytkownika",
    description=(
        "Unieważnia access token (blacklista JTI w Redis) oraz refresh token w DB. "
        "Usuwa HttpOnly cookie z przeglądarki (Set-Cookie: Max-Age=0). "
        "Refresh token czytany z cookie (primary) lub body JSON (fallback dla API). "
        "Logout możliwy bez refresh tokena — cookie usuwane zawsze. "
        "Po wywołaniu token nie może być użyty ponownie nawet jeśli nie wygasł."
    ),
    response_description="Potwierdzenie wylogowania",
    status_code=status.HTTP_200_OK,
)
async def logout(
    request: Request,
    response: Response,          # FastAPI inject — do usunięcia cookie
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
    credentials: Annotated[Optional[HTTPAuthorizationCredentials], Depends(_bearer)] = None,
):
    """
    Wylogowanie użytkownika — v2.0 (HttpOnly cookie).

    Zmiany vs v1.0:
        - Parametr `response: Response` — FastAPI inject, niewidoczny w Swagger
        - Refresh token czytany z cookie (primary) lub body JSON (fallback)
        - Po wylogowaniu: clear_refresh_cookie() usuwa cookie (Max-Age=0)
        - Cookie jest zawsze usuwane — nawet jeśli brak tokena (bezpieczeństwo)
        - Loguje: cookie_cleared=True, źródło tokena

    Access token: invalidowany przez Redis blacklist (z Authorization header).
    Refresh token: opcjonalny — logout działa też bez niego (cookie mogło wygasnąć).
    """
    from app.services import auth_service

    cookie_cfg = _get_cookie_cfg()

    access_token = credentials.credentials if credentials else ""
    user_id = current_user.id_user
    username = current_user.username

    # ── Odczyt refresh tokena — cookie PRIMARY, body FALLBACK ─────────────────
    body: Optional[dict] = None
    try:
        body = await request.json()
    except Exception:
        body = {}   # Body opcjonalne przy logout

    refresh_token: Optional[str] = extract_refresh_token_hybrid(
        request=request,
        body=body,
        config=cookie_cfg,
        request_id=request_id,
        ip=client_ip,
    )

    if not refresh_token:
        # Logout bez refresh tokena jest dozwolony
        # Access token zinvalidowany przez Redis, cookie wyczyszczone poniżej
        logger.info(
            orjson.dumps({
                "event": "api_logout_no_refresh_token",
                "message": "Logout bez refresh tokena — OK (token/cookie mógł wygasnąć).",
                "user_id": user_id,
                "username": username,
                "request_id": request_id,
                "ip": client_ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )

    # ── Wywołanie serwisu ─────────────────────────────────────────────────────
    try:
        await auth_service.logout(
            db=db,
            redis=redis,
            access_token=access_token,
            refresh_token_raw=refresh_token,   # może być None — serwis obsługuje
            user_id=user_id,
            username=username,
            ip=client_ip,
        )
    except Exception as exc:
        _raise_from_auth_error(exc)

    # ── Usuń cookie — ZAWSZE, niezależnie od obecności tokena ─────────────────
    # Zapobiega "zombie cookie": stare cookie po wylogowaniu przez body/API
    clear_refresh_cookie(
        response=response,
        config=cookie_cfg,
        request_id=request_id,
        user_id=user_id,
        ip=client_ip,
    )

    # ── Log sukcesu ───────────────────────────────────────────────────────────
    logger.info(
        orjson.dumps(
            {
                "event": "api_logout_success",
                "user_id": user_id,
                "username": username,
                "request_id": request_id,
                "ip": client_ip,
                "cookie_cleared": True,
                "had_refresh_token": refresh_token is not None,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ).decode()
    )

    return BaseResponse.ok(
        data={"message": "Wylogowano pomyślnie"},
        code=200,
        app_code="auth.logout_success",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 3: POST /auth/refresh
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/refresh",
    summary="Odświeżenie access tokena",
    description=(
        "Generuje nowy access token na podstawie ważnego refresh tokena. "
        "Refresh token czytany z HttpOnly cookie (primary) lub body JSON (fallback). "
        "Cookie jest odnawiane po każdym odświeżeniu (sliding window). "
        "Refresh token NIE jest rotowany (ta sama wartość po odświeżeniu). "
        "**Rate limit:** 30 żądań / minutę / IP."
    ),
    response_description="Nowy access token (refresh token w odnowionym cookie)",
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Nowy access token, cookie odnowione"},
        401: {"description": "Nieprawidłowy lub wygasły refresh token"},
        422: {"description": "Brak refresh tokena (cookie i body)"},
        429: {"description": "Zbyt wiele żądań odświeżenia"},
    },
)
async def refresh_token(
    request: Request,
    response: Response,          # FastAPI inject — do odnowienia cookie (Max-Age reset)
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    """
    Odświeżenie access tokena — v2.0 (HttpOnly cookie).

    Zmiany vs v1.0:
        - Parametr `response: Response` — FastAPI inject, niewidoczny w Swagger
        - Refresh token czytany z cookie (primary) lub body JSON (fallback dla Postman)
        - Body jest OPCJONALNE — klienci cookie mogą wysłać puste żądanie
        - Po odświeżeniu: cookie odnawiane (sliding window — resetuje Max-Age)
        - Body odpowiedzi NIE zawiera refresh_token
        - Loguje: źródło tokena (cookie/body), cookie_renewed

    Sliding window:
        set_refresh_cookie() po refresh resetuje Max-Age cookie.
        Aktywni użytkownicy nigdy nie tracą sesji z powodu wygaśnięcia cookie.
    """
    from app.services import auth_service

    cookie_cfg = _get_cookie_cfg()

    # ── Odczyt refresh tokena — cookie PRIMARY, body FALLBACK ─────────────────
    # Body jest opcjonalne — klienci cookie nie muszą nic wysyłać w body
    body: Optional[dict] = None
    try:
        body = await request.json()
    except Exception:
        body = {}   # Brak body lub nieprawidłowy JSON = brak fallback tokena

    refresh_token_raw: Optional[str] = extract_refresh_token_hybrid(
        request=request,
        body=body,
        config=cookie_cfg,
        request_id=request_id,
        ip=client_ip,
    )

    # ── Walidacja — token musi pochodzić z jakiegoś źródła ───────────────────
    if not refresh_token_raw:
        logger.warning(
            orjson.dumps({
                "event": "api_refresh_no_token",
                "request_id": request_id,
                "ip": client_ip,
                "available_cookies": list(request.cookies.keys()),
                "body_keys": list(body.keys()) if body else [],
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": (
                    "Brak refresh tokena. "
                    "Wymagane: HttpOnly cookie 'refresh_token' (wysyłane automatycznie) "
                    "lub pole 'refresh_token' w body JSON (dla klientów API)."
                ),
                "errors": [
                    {
                        "field": "refresh_token",
                        "message": "Pole wymagane (cookie lub body)",
                        "sources_checked": ["httponly_cookie", "body_json"],
                    }
                ],
            },
        )

    # ── Wywołanie serwisu ─────────────────────────────────────────────────────
    try:
        token_pair = await auth_service.refresh(
            db=db,
            redis=redis,
            refresh_token_raw=refresh_token_raw,
            ip=client_ip,
        )
    except Exception as exc:
        _raise_from_auth_error(exc)

    # ── Odnów cookie (sliding window — reset Max-Age) ─────────────────────────
    # Token nie jest rotowany (decyzja projektowa), ale cookie jest odświeżane
    # żeby aktywni użytkownicy nie tracili sesji przy długim użytkowaniu
    set_refresh_cookie(
        response=response,
        token=token_pair.refresh_token,
        config=cookie_cfg,
        request_id=request_id,
        ip=client_ip,
    )

    # ── Log sukcesu ───────────────────────────────────────────────────────────
    logger.info(
        orjson.dumps(
            {
                "event": "api_token_refreshed",
                "request_id": request_id,
                "ip": client_ip,
                "cookie_renewed": True,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ).decode()
    )

    # ── Odpowiedź — tylko access token, refresh token w odnowionym cookie ─────
    return BaseResponse.ok(
        data={
            "access_token": token_pair.access_token,
            "token_type": token_pair.token_type,
            "expires_in": token_pair.expires_in,
            # refresh_token CELOWO POMINIĘTY — jest w HttpOnly cookie
        },
        code=200,
        app_code="auth.token_refreshed",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 4: POST /auth/otp/request
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/otp/request",
    summary="Krok 1/3 — Żądanie kodu OTP (reset hasła)",
    description=(
        "Inicjuje 3-krokowy proces resetu hasła. "
        "Generuje 6-cyfrowy kod OTP i wysyła na email podanego użytkownika. "
        "Odpowiedź jest ZAWSZE taka sama (anty-enumeracja) — "
        "nie ujawniamy czy email istnieje w systemie. "
        "Kod ważny przez czas z SystemConfig (otp.expiry_minutes, domyślnie 15 min)."
    ),
    response_description="Potwierdzenie wysłania kodu (zawsze 200, anty-enumeracja)",
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Zawsze 200 — kod wysłany lub nie (anty-enumeracja)"},
        429: {"description": "Zbyt wiele żądań OTP"},
    },
)
async def otp_request(
    request: Request,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import auth_service

    try:
        body = await request.json()
        email_raw = (body.get("email") or "").strip()[:254]
    except Exception:
        email_raw = ""

    if not email_raw:
        # Anty-enumeracja: nie ujawniamy błędu, zawsze zwracamy 200
        return BaseResponse.ok(
            data={"message": "Jeśli podany email istnieje w systemie, kod OTP został wysłany."},
            app_code="auth.otp_sent",
        )

    try:
        from app.services import otp_service
        await otp_service.request_otp(
            db=db,
            redis=redis,
            email=email_raw,
            purpose="password_reset",
            ip=client_ip,
        )
    except Exception as exc:
        # RateLimitExceededError → 429, reszta → cicha (anty-enumeracja)
        exc_type = type(exc).__name__
        if exc_type == "RateLimitExceededError":
            _raise_from_auth_error(exc)
        # Inne błędy (np. user nie istnieje) — logujemy, ale zwracamy 200
        # TYMCZASOWO — loguj pełny traceback żeby znaleźć błąd
        logger.exception(
            "OTP request FAILED (połknięty wyjątek): %s | type=%s",
            exc, exc_type,
            extra={"exc_type": exc_type, "exc_msg": str(exc)},
        )

    logger.info(
        orjson.dumps({
            "event": "api_otp_request",
            "email_hash": _hash_email(email_raw),
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={"message": "Jeśli podany email istnieje w systemie, kod OTP został wysłany."},
        app_code="auth.otp_sent",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 5: POST /auth/otp/verify
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/otp/verify",
    summary="Krok 2/3 — Weryfikacja kodu OTP",
    description=(
        "Weryfikuje 6-cyfrowy kod OTP wysłany w kroku 1. "
        "Po poprawnej weryfikacji zwraca jednorazowy `reset_token` (JWT, ważny 15 min). "
        "reset_token wymagany w kroku 3 (POST /auth/password-reset/confirm). "
        "**Rate limit:** 5 prób / 30 minut / IP."
    ),
    response_description="Token resetu hasła (jednorazowy, 15 min)",
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Kod OTP poprawny — reset_token w odpowiedzi"},
        400: {"description": "Nieprawidłowy lub wygasły kod OTP"},
        429: {"description": "Zbyt wiele błędnych prób OTP"},
    },
)
async def otp_verify(
    request: Request,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import auth_service

    try:
        body = await request.json()
        email_raw = (body.get("email") or "").strip()[:254]
        otp_code = (body.get("otp_code") or "").strip()[:8]
    except Exception:
        email_raw = otp_code = ""

    errors = []
    if not email_raw:
        errors.append({"field": "email", "message": "Pole wymagane"})
    if not otp_code:
        errors.append({"field": "otp_code", "message": "Pole wymagane"})
    if errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "validation.error", "message": "Błąd walidacji", "errors": errors},
        )

    # PO (poprawne):
    try:
        from app.services import otp_service
        reset_token = await auth_service.verify_otp_and_get_reset_token(
            db=db,
            redis=redis,
            email=email_raw,
            otp_code=otp_code,
            ip=client_ip,
        )
    except Exception as exc:
        _raise_from_auth_error(exc)

    logger.info(
        orjson.dumps({
            "event": "api_otp_verified",
            "email_hash": _hash_email(email_raw),
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={"reset_token": reset_token, "message": "Kod OTP poprawny. Ustaw nowe hasło."},
        app_code="auth.otp_verified",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 6: POST /auth/password-reset/confirm
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/password-reset/confirm",
    summary="Krok 3/3 — Ustawienie nowego hasła",
    description=(
        "Ustawia nowe hasło na podstawie reset_token z kroku 2 (OTP verify). "
        "Token jest jednorazowy — po użyciu wygasa. "
        "Po pomyślnym resecie: wszystkie aktywne sesje są unieważniane. "
        "Polityka hasła: min 8 znaków, min 1 wielka litera, min 1 cyfra."
    ),
    response_description="Potwierdzenie zmiany hasła",
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Hasło zmienione — wszystkie sesje unieważnione"},
        400: {"description": "Nieprawidłowy lub wygasły reset_token"},
        422: {"description": "Hasło nie spełnia wymagań polityki"},
    },
)
async def password_reset_confirm(
    request: Request,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import auth_service

    try:
        body = await request.json()
        reset_token = (body.get("reset_token") or "").strip()
        new_password = body.get("new_password") or ""
    except Exception:
        reset_token = new_password = ""

    errors = []
    if not reset_token:
        errors.append({"field": "reset_token", "message": "Pole wymagane"})
    if not new_password:
        errors.append({"field": "new_password", "message": "Pole wymagane"})
    if errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "validation.error", "message": "Błąd walidacji", "errors": errors},
        )

    try:
        await auth_service.reset_password(
            db=db,
            redis=redis,
            reset_token=reset_token,
            new_password=new_password,
            ip=client_ip,
        )
    except Exception as exc:
        _raise_from_auth_error(exc)

    logger.info(
        orjson.dumps(
            {
                "event": "api_password_reset_success",
                "request_id": request_id,
                "ip": client_ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ).decode()
    )

    return BaseResponse.ok(
        data={"message": "Hasło zostało zmienione. Wszystkie aktywne sesje zostały unieważnione."},
        app_code="auth.password_reset_success",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 7: GET /auth/me
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/me",
    summary="Dane zalogowanego użytkownika",
    description=(
        "Zwraca profil aktualnie zalogowanego użytkownika wraz z jego rolą i uprawnieniami. "
        "Przy aktywnej impersonacji — dane impersonowanego użytkownika "
        "(pole `is_impersonation: true` w odpowiedzi)."
    ),
    response_description="Profil użytkownika z uprawnieniami",
    status_code=status.HTTP_200_OK,
)
async def get_me(
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request_id: RequestID,
):
    from sqlalchemy import select
    from app.db.models.role import Role
    from app.core.dependencies import _get_role_permissions

    # Pobierz nazwę roli
    stmt = select(Role.role_name).where(Role.id_role == current_user.role_id)
    result = await db.execute(stmt)
    role_name = result.scalar_one_or_none() or "unknown"

    # Pobierz uprawnienia z cache
    permissions = await _get_role_permissions(current_user.role_id, db, redis)

    is_impersonating = getattr(request.state, "is_impersonating", False)
    real_user_id = getattr(request.state, "real_user_id", current_user.id_user)

    data = {
        "id": current_user.id_user,
        "username": current_user.username,
        "email": current_user.email,
        "full_name": current_user.full_name,
        "role_id": current_user.role_id,
        "role_name": role_name,
        "is_active": bool(current_user.is_active),
        "last_login_at": (
            current_user.last_login_at.isoformat()
            if current_user.last_login_at
            else None
        ),
        "permissions": sorted(permissions),
        "is_impersonation": is_impersonating,
        "impersonated_by_id": real_user_id if is_impersonating else None,
    }

    return BaseResponse.ok(data=data, app_code="auth.me")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 8: POST /auth/me/change-password
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/me/change-password",
    summary="Zmiana własnego hasła",
    description=(
        "Zmiana hasła przez zalogowanego użytkownika. "
        "Wymaga podania aktualnego hasła (weryfikacja argon2). "
        "Po zmianie: wszystkie inne sesje (refresh tokeny) są unieważniane. "
        "Aktualna sesja pozostaje aktywna."
    ),
    response_description="Potwierdzenie zmiany hasła",
    status_code=status.HTTP_200_OK,
)
async def change_password(
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import auth_service

    try:
        body = await request.json()
        old_password = body.get("old_password") or ""
        new_password = body.get("new_password") or ""
    except Exception:
        old_password = new_password = ""

    errors = []
    if not old_password:
        errors.append({"field": "old_password", "message": "Pole wymagane"})
    if not new_password:
        errors.append({"field": "new_password", "message": "Pole wymagane"})
    if errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "validation.error", "message": "Błąd walidacji", "errors": errors},
        )

    try:
        await auth_service.change_password(
            db=db,
            redis=redis,
            user_id=current_user.id_user,
            old_password=old_password,
            new_password=new_password,
            ip=client_ip,
        )
    except Exception as exc:
        _raise_from_auth_error(exc)

    logger.info(
        orjson.dumps({
            "event": "api_change_password_success",
            "user_id": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={"message": "Hasło zostało zmienione. Inne aktywne sesje zostały unieważnione."},
        app_code="auth.password_changed",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 9: POST /auth/impersonate/{user_id}
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/impersonate/{user_id:int}",
    summary="Start impersonacji użytkownika",
    description=(
        "Administrator przejmuje tożsamość wskazanego użytkownika. "
        "Zwraca nowy access token z danymi impersonowanego użytkownika. "
        "Pole `is_impersonation: true` w tokenie. "
        "**Wymaga uprawnienia:** `auth.impersonate`"
    ),
    response_description="Access token impersonacji",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("auth.impersonate")],
)
async def start_impersonation(
    user_id: int,
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import impersonation_service

    try:
        body = await request.json()
        reason = (body.get("reason") or "").strip()[:500]
    except Exception:
        reason = ""

    if not reason:
        reason = "Impersonacja zainicjowana przez administratora"

    try:
        result = await impersonation_service.start(
            db=db,
            redis=redis,
            admin_id=current_user.id_user,
            target_user_id=user_id,
            reason=reason,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_auth_error(exc)

    logger.warning(
        orjson.dumps(
            {
                "event": "api_impersonation_started",
                "admin_id": current_user.id_user,
                "target_user_id": user_id,
                "request_id": request_id,
                "ip": client_ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ).decode()
    )

    return BaseResponse.ok(
        data={
            "access_token": result.access_token,
            "token_type": result.token_type,
            "expires_at": result.expires_at.isoformat(),
            "is_impersonation": True,
            "impersonated_user_id": user_id,
            "message": "Impersonacja aktywna. Użyj access_token do dalszych żądań.",
        },
        app_code="auth.impersonation_started",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 10: POST /auth/impersonate/stop
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/impersonate/stop",
    summary="Zakończenie impersonacji",
    description=(
        "Kończy aktywną sesję impersonacji i wraca do własnej tożsamości. "
        "Unieważnia token impersonacji. "
        "Jeśli nie ma aktywnej impersonacji — endpoint zwraca błąd."
    ),
    response_description="Potwierdzenie zakończenia impersonacji",
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Impersonacja zakończona"},
        400: {"description": "Brak aktywnej sesji impersonacji"},
    },
)
async def stop_impersonation(
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
    credentials: Annotated[Optional[HTTPAuthorizationCredentials], Depends(_bearer)] = None,
):
    from app.services import impersonation_service

    is_impersonating = getattr(request.state, "is_impersonating", False)
    if not is_impersonating:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code": "auth.no_active_impersonation",
                "message": "Brak aktywnej sesji impersonacji",
                "errors": [],
            },
        )

    real_user_id = getattr(request.state, "real_user_id", None)
    access_token = credentials.credentials if credentials else ""

    try:
        result = await impersonation_service.end(
            db=db,
            redis=redis,
            impersonation_token=access_token,
            admin_id=real_user_id,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_auth_error(exc)

    logger.warning(
        orjson.dumps(
            {
                "event": "api_impersonation_stopped",
                "admin_id": real_user_id,
                "impersonated_user_id": current_user.id_user,
                "request_id": request_id,
                "ip": client_ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ).decode()
    )

    return BaseResponse.ok(
        data={
            "message": "Impersonacja zakończona. Powrót do własnej tożsamości.",
        },
        app_code="auth.impersonation_stopped",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 11: POST /auth/master-key/login
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/master-key/login",
    summary="Logowanie przez Master Key + PIN",
    description=(
        "Serwisowe logowanie dla administratorów systemowych. "
        "Wymaga podania Master Key (z .env) i PIN-u (z SystemConfig). "
        "Zwraca pełne uprawnienia systemowe. "
        "Każda akcja jest logowana."
    ),
    response_description="Access token z uprawnieniami systemowymi",
    status_code=status.HTTP_200_OK,
)
async def master_key_login(
    request: Request,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import auth_service

    try:
        body = await request.json()
        master_key_raw = (body.get("master_key") or "").strip()
        pin_raw = (body.get("pin") or "").strip()
        target_user_id_raw = body.get("target_user_id")
    except Exception:
        master_key_raw = pin_raw = ""
        target_user_id_raw = None
    errors = []
    if not master_key_raw:
        errors.append({"field": "master_key", "message": "Pole wymagane"})
    if not pin_raw:
        errors.append({"field": "pin", "message": "Pole wymagane"})
    if not target_user_id_raw:
        errors.append({"field": "target_user_id", "message": "Pole wymagane"})
    if errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "validation.error", "message": "Błąd walidacji", "errors": errors},
        )
    try:
        target_user_id = int(target_user_id_raw)
    except (ValueError, TypeError):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "validation.error", "message": "Błąd walidacji",
                    "errors": [{"field": "target_user_id", "message": "Musi być liczbą całkowitą"}]},
        )
    try:
        result = await auth_service.master_access(
            db=db,
            redis=redis,
            master_key_input=master_key_raw,
            pin_input=pin_raw,
            target_user_id=target_user_id,
            ip=client_ip,
        )
    except Exception as exc:
        _raise_from_auth_error(exc)
    logger.warning(
        orjson.dumps({
            "event": "api_master_key_login",
            "target_user_id": target_user_id,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )
    return BaseResponse.ok(
        data={
            "access_token": result.access_token,
            "token_type": result.token_type,
            "expires_in": result.expires_in,
            "message": "Zalogowano przez Master Key. Każda akcja jest logowana.",
        },
        app_code="auth.master_key_success",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 12: GET /auth/sessions
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/sessions",
    summary="Lista aktywnych sesji użytkownika",
    description=(
        "Zwraca listę aktywnych refresh tokenów (sesji) zalogowanego użytkownika. "
        "Przydatne do wykrycia nieautoryzowanego dostępu i unieważnienia sesji. "
        "Wymaga uprawnienia `auth.view_sessions`."
    ),
    response_description="Lista aktywnych sesji",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("auth.view_own_sessions")],
)
async def list_sessions(
    current_user: CurrentUser,
    db: DB,
    request_id: RequestID,
):
    from datetime import timezone as tz
    from sqlalchemy import select
    from app.db.models.refresh_token import RefreshToken

    stmt = (
        select(
            RefreshToken.id_token,
            RefreshToken.created_at,
            RefreshToken.expires_at,
            RefreshToken.ip_address,
            RefreshToken.user_agent,
        )
        .where(
            RefreshToken.id_user == current_user.id_user,
            RefreshToken.is_revoked == 0,
            RefreshToken.expires_at > datetime.now(timezone.utc),
        )
        .order_by(RefreshToken.created_at.desc())
        .limit(20)
    )
    result = await db.execute(stmt)
    rows = result.all()

    sessions = [
        {
            "session_id": row.id_token,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "expires_at": row.expires_at.isoformat() if row.expires_at else None,
            "ip_address": row.ip_address,
            "user_agent": (row.user_agent or "")[:100],
        }
        for row in rows
    ]

    return BaseResponse.ok(
        data={"sessions": sessions, "total": len(sessions)},
        code=200,
        app_code="auth.sessions_listed",
    )


# ─────────────────────────────────────────────────────────────────────────────
# POMOCNICZE: Konwersja wyjątków serwisów → HTTPException
# ─────────────────────────────────────────────────────────────────────────────

def _raise_from_auth_error(exc: Exception) -> None:
    """
    Konwertuje wyjątki z auth_service / otp_service / impersonation_service
    na ujednolicone HTTPException z kodem błędu w detail.

    Obsługiwane typy wyjątków (z services/auth_service.py):
      AuthError               → 401
      AccountLockedError      → 423 (Locked)
      PermissionDeniedError   → 403
      RateLimitExceededError  → 429
      TokenExpiredError       → 401
      TokenBlacklistedError   → 401

    Nieznane wyjątki → re-raise (catch-all w main.py zwróci 500).
    """
    exc_type = type(exc).__name__
    exc_msg = str(exc)

    _MAP: dict[str, tuple[int, str]] = {
        "AuthError":             (401, "auth.invalid_credentials"),
        "AccountLockedError":    (423, "auth.account_locked"),
        "PermissionDeniedError": (403, "auth.permission_denied"),
        "RateLimitExceededError":(429, "auth.rate_limit_exceeded"),
        "TokenExpiredError":     (401, "auth.token_expired"),
        "TokenBlacklistedError": (401, "auth.token_revoked"),
        "InvalidOTPError":       (400, "auth.otp_invalid"),
        "OTPExpiredError":       (400, "auth.otp_expired"),
        "OTPRateLimitError":     (429, "auth.otp_rate_limit"),
        "InvalidResetTokenError":(400, "auth.reset_token_invalid"),
        "ImpersonationError":    (400, "auth.impersonation_error"),
        "UserNotFoundError":     (404, "users.not_found"),
        "MasterKeyDisabledError":(401, "auth.master_key_disabled"),
        "MasterKeyInvalidError": (401, "auth.master_key_invalid"),
        "IPBannedError":         (429, "auth.ip_banned"),
    }

    if exc_type in _MAP:
        http_status, code = _MAP[exc_type]

        headers = {}
        retry_after = getattr(exc, "retry_after", None)
        if retry_after:
            headers["Retry-After"] = str(int(retry_after))

        locked_until = getattr(exc, "locked_until", None)
        extra_errors = []
        if locked_until:
            extra_errors.append({
                "field": "account",
                "message": f"Konto zablokowane do: {locked_until.isoformat() if hasattr(locked_until, 'isoformat') else locked_until}",
            })

        raise HTTPException(
            status_code=http_status,
            detail={
                "code": code,
                "message": _pl_message(exc_type, exc_msg),
                "errors": extra_errors or [{"field": "_", "message": exc_msg}],
            },
            headers=headers or None,
        )

    raise


def _pl_message(exc_type: str, original: str) -> str:
    """Zwraca przyjazny komunikat po polsku dla danego typu wyjątku."""
    _MESSAGES = {
        "AuthError":             "Nieprawidłowa nazwa użytkownika lub hasło",
        "AccountLockedError":    "Konto tymczasowo zablokowane po zbyt wielu nieudanych próbach",
        "PermissionDeniedError": "Brak wymaganego uprawnienia",
        "RateLimitExceededError":"Zbyt wiele żądań — odczekaj chwilę i spróbuj ponownie",
        "TokenExpiredError":     "Token wygasł — zaloguj się ponownie",
        "TokenBlacklistedError": "Token został unieważniony",
        "InvalidOTPError":       "Nieprawidłowy kod OTP",
        "OTPExpiredError":       "Kod OTP wygasł — wygeneruj nowy",
        "OTPRateLimitError":     "Za dużo nieudanych prób OTP — odczekaj 30 minut",
        "InvalidResetTokenError":"Token resetu hasła jest nieprawidłowy lub wygasł",
        "ImpersonationError":    "Błąd impersonacji",
        "UserNotFoundError":     "Użytkownik nie istnieje",
        "MasterKeyDisabledError":"Master Key jest wyłączony w konfiguracji",
        "MasterKeyInvalidError": "Nieprawidłowy Master Key lub PIN",
        "IPBannedError":         "Adres IP zbanowany po przekroczeniu limitu prób",
    }
    return _MESSAGES.get(exc_type, original)


def _pydantic_errors(exc: Exception) -> list[dict[str, str]]:
    """Konwertuje błędy Pydantic na nasz format errors[]."""
    errors = []
    if hasattr(exc, "errors"):
        for err in exc.errors():
            loc = err.get("loc", [])
            field = ".".join(str(p) for p in loc if p not in ("body",))
            errors.append({"field": field or "_", "message": err.get("msg", "Błąd walidacji")})
    else:
        errors.append({"field": "_", "message": str(exc)})
    return errors


def _hash_email(email: str) -> str:
    """Zwraca SHA-256 prefix emaila dla logów (anty-PII)."""
    import hashlib
    return hashlib.sha256(email.encode()).hexdigest()[:12]