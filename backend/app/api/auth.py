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

Powiązane serwisy:
  services/auth_service.py          — login, logout, refresh, change_password
  services/otp_service.py           — request_otp, verify_otp
  services/impersonation_service.py — start, stop

Autor: System Windykacja
Wersja: 1.0.0
Data: 2026-02-20
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Annotated, Optional

import orjson
from fastapi import APIRouter, Depends, HTTPException, Request, status
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
# ENDPOINT 1: POST /auth/login
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/login",
    summary="Logowanie użytkownika",
    description=(
        "Logowanie za pomocą nazwy użytkownika i hasła. "
        "Zwraca parę tokenów JWT: access token (max 24h) i refresh token. "
        "**Rate limit:** 10 prób / minutę / IP. "
        "Po 5 nieudanych próbach konto blokowane na 30 minut."
    ),
    response_description="Para tokenów JWT",
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Logowanie pomyślne — tokeny w odpowiedzi"},
        401: {"description": "Nieprawidłowe dane logowania"},
        423: {"description": "Konto tymczasowo zablokowane"},
        429: {"description": "Zbyt wiele prób logowania"},
    },
)
async def login(
    request: Request,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    """
    Logowanie użytkownika.

    Body (application/json):
        username (str): Nazwa użytkownika (login)
        password (str): Hasło (min 8 znaków)

    Returns:
        BaseResponse z TokenPair: access_token, refresh_token, expires_in, role, permissions
    """
    from app.schemas.auth import LoginRequest
    from app.services import auth_service

    # Ręczny parsing body — pozwala obsłużyć błędy walidacji z własnym formatem
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

    # Walidacja Pydantic
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

    logger.info(
        orjson.dumps(
            {
                "event": "api_login_success",
                "user_id": token_pair.user_id,
                "username": token_pair.username,
                "request_id": request_id,
                "ip": client_ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ).decode()
    )

    return BaseResponse.ok(
        data=token_pair.__dict__,
        code=200,
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 2: POST /auth/logout
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/logout",
    summary="Wylogowanie użytkownika",
    description=(
        "Unieważnia access token (blacklista JTI w Redis) oraz refresh token. "
        "Po wywołaniu token nie może być użyty ponownie nawet jeśli nie wygasł."
    ),
    response_description="Potwierdzenie wylogowania",
    status_code=status.HTTP_200_OK,
)
async def logout(
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
    credentials: Annotated[Optional[HTTPAuthorizationCredentials], Depends(_bearer)] = None,
):
    from app.services import auth_service

    access_token = credentials.credentials if credentials else ""

    # Pobierz refresh token z body (opcjonalny — logout możliwy bez niego)
    refresh_token: Optional[str] = None
    try:
        body = await request.json()
        refresh_token = body.get("refresh_token")
    except Exception:
        pass  # Body opcjonalne przy logout

    user_id = current_user.id_user
    username = current_user.username

    try:
        await auth_service.logout(
            db=db,
            redis=redis,
            access_token=access_token,
            refresh_token_raw=refresh_token,
            user_id=current_user.id_user,
            username=current_user.username,
            ip=client_ip,
        )
    except Exception as exc:
        _raise_from_auth_error(exc)

    logger.info(
        orjson.dumps(
            {
                "event": "api_logout_success",
                "user_id": user_id,
                "request_id": request_id,
                "ip": client_ip,
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
        "Refresh token NIE jest rotowany (ta sama wartość po odświeżeniu). "
        "**Rate limit:** 30 żądań / minutę / IP."
    ),
    response_description="Nowy access token",
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Nowy access token"},
        401: {"description": "Nieprawidłowy lub wygasły refresh token"},
        429: {"description": "Zbyt wiele żądań odświeżenia"},
    },
)
async def refresh_token(
    request: Request,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import auth_service

    try:
        body = await request.json()
        refresh_token_raw = body.get("refresh_token", "")
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Wymagane pole refresh_token w body",
                "errors": [{"field": "refresh_token", "message": "Pole wymagane"}],
            },
        )

    if not refresh_token_raw:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Wymagane pole refresh_token w body",
                "errors": [{"field": "refresh_token", "message": "Pole wymagane"}],
            },
        )

    try:
        token_pair = await auth_service.refresh(
            db=db,
            redis=redis,
            refresh_token_raw=refresh_token_raw,
            ip=client_ip,
        )
    except Exception as exc:
        _raise_from_auth_error(exc)

    return BaseResponse.ok(
        data={
            "access_token": token_pair.access_token,
            "token_type": token_pair.token_type,
            "expires_in": token_pair.expires_in,
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
    response_description="Potwierdzenie wysłania OTP (zawsze sukces — anty-enumeracja)",
    status_code=status.HTTP_200_OK,
)
async def otp_request(
    request: Request,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    import traceback as _traceback
    from app.services import otp_service

    try:
        body = await request.json()
        email = (body.get("email") or "").strip()
    except Exception:
        email = ""

    if not email:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Wymagane pole email",
                "errors": [{"field": "email", "message": "Pole wymagane"}],
            },
        )

    # Fire-and-forget — nie czekamy na wynik (anty-enumeracja + szybkość)
    try:
        await otp_service.request_otp(
            db=db,
            redis=redis,
            email=email,
            purpose="password_reset",
            ip=client_ip,
        )
    except Exception as exc:
        # CELOWO ignorujemy błędy — odpowiedź zawsze identyczna
        # TYMCZASOWO: logujemy pełny traceback dla debugowania
        logger.error(
            "otp_request INTERNAL ERROR | type=%s | error=%s | traceback=%s",
            type(exc).__name__,
            str(exc),
            _traceback.format_exc(),
        )

    logger.info(
        orjson.dumps(
            {
                "event": "api_otp_requested",
                "email_hash": _hash_email(email),
                "request_id": request_id,
                "ip": client_ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ).decode()
    )

    # Zawsze ta sama odpowiedź (anty-enumeracja)
    return BaseResponse.ok(
        data={
            "message": "Jeśli konto istnieje, kod OTP został wysłany na podany adres email."
        },
        app_code="auth.otp_sent",
    )

# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 5: POST /auth/otp/verify
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/otp/verify",
    summary="Krok 2/3 — Weryfikacja kodu OTP",
    description=(
        "Weryfikuje 6-cyfrowy kod OTP. "
        "Jeśli poprawny — zwraca jednorazowy **reset_token** (ważny 10 min) "
        "do użycia w kroku 3 (POST /auth/password-reset/confirm). "
        "Po 5 nieudanych próbach endpoint blokuje kolejne próby na 30 min."
    ),
    response_description="Jednorazowy token resetu hasła",
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "OTP poprawny — reset_token w odpowiedzi"},
        400: {"description": "Nieprawidłowy lub wygasły kod OTP"},
        429: {"description": "Za dużo nieudanych prób weryfikacji"},
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
        email = (body.get("email") or "").strip()
        otp_code = (body.get("code") or "").strip()
    except Exception:
        email = otp_code = ""

    errors = []
    if not email:
        errors.append({"field": "email", "message": "Pole wymagane"})
    if not otp_code:
        errors.append({"field": "code", "message": "Pole wymagane"})
    if errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "validation.error", "message": "Błąd walidacji", "errors": errors},
        )

    try:
        reset_token = await auth_service.verify_otp_and_get_reset_token(
            db=db,
            redis=redis,
            email=email,
            otp_code=otp_code,
            ip=client_ip,
        )
    except Exception as exc:
        _raise_from_auth_error(exc)

    return BaseResponse.ok(
        data={
            "reset_token": reset_token,
            "expires_in": 600,
            "message": "Kod OTP poprawny. Użyj reset_token do ustawienia nowego hasła.",
        },
        app_code="auth.otp_verified",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 6: POST /auth/password-reset/confirm
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/password-reset/confirm",
    summary="Krok 3/3 — Ustawienie nowego hasła",
    description=(
        "Ostatni krok resetu hasła. Wymaga `reset_token` z kroku 2 (OTP verify). "
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
    responses={
        200: {"description": "Hasło zmienione — pozostałe sesje unieważnione"},
        400: {"description": "Nieprawidłowe obecne hasło"},
        422: {"description": "Nowe hasło nie spełnia wymagań polityki"},
    },
)
async def change_my_password(
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import auth_service

    user_id = current_user.id_user
    username = current_user.username

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
            user_id=user_id,
            old_password=old_password,
            new_password=new_password,
            ip=client_ip,
        )
    except Exception as exc:
        _raise_from_auth_error(exc)

    logger.info(
        orjson.dumps(
            {
                "event": "api_password_changed",
                "user_id": user_id,
                "request_id": request_id,
                "ip": client_ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ).decode()
    )

    return BaseResponse.ok(
        data={"message": "Hasło zostało zmienione. Pozostałe sesje zostały unieważnione."},
        code=200, 
        app_code="auth.password_changed",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 9: POST /auth/impersonate/{user_id}
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/impersonate/{user_id}",
    summary="Start impersonacji użytkownika",
    description=(
        "Admin przejmuje tożsamość wybranego użytkownika. "
        "Generuje specjalny JWT z polem `imp` = impersonowany user_id. "
        "Historia impersonowanego użytkownika pozostaje czysta — "
        "akcje logowane pod ID admina. "
        "Czas trwania ograniczony przez SystemConfig (impersonation.max_hours, domyślnie 4h). "
        "**Uwaga:** Nie można impersonować administratora ani siebie."
    ),
    response_description="Token impersonacji",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("auth.impersonate")],
    responses={
        200: {"description": "Token impersonacji"},
        403: {"description": "Brak uprawnienia auth.impersonate"},
        404: {"description": "Użytkownik docelowy nie istnieje"},
        409: {"description": "Nie można impersonować admina lub siebie"},
    },
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

    # Blokada impersonacji siebie samego
    if user_id == current_user.id_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "auth.impersonation_self",
                "message": "Nie można impersonować siebie",
                "errors": [{"field": "user_id", "message": "Docelowy użytkownik to Ty sam"}],
            },
        )

    user_agent = request.headers.get("User-Agent", "")

    try:
        token_pair = await impersonation_service.start(
            db=db,
            redis=redis,
            admin_id=current_user.id_user,
            target_user_id=user_id,
            ip=client_ip,
            user_agent=user_agent,
        )
    except Exception as exc:
        _raise_from_auth_error(exc)

    logger.warning(
        orjson.dumps(
            {
                "event": "api_impersonation_started",
                "admin_id": current_user.id_user,
                "admin_username": current_user.username,
                "target_user_id": user_id,
                "request_id": request_id,
                "ip": client_ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ).decode()
    )

    return BaseResponse.ok(
        data={
            "access_token": token_pair.access_token,
            "token_type": token_pair.token_type,
            "expires_in": token_pair.expires_in,
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
        result = await impersonation_service.stop(
            db=db,
            redis=redis,
            admin_id=real_user_id,
            impersonated_user_id=current_user.id_user,
            access_token=access_token,
            ip=client_ip,
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
            "admin_id": real_user_id,
        },
        app_code="auth.impersonation_stopped",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 11: POST /auth/master-key/login
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/master-key/login",
    summary="Logowanie przez Master Key (awaryjny dostęp)",
    description=(
        "Awaryjny dostęp administracyjny bez normalnego konta użytkownika. "
        "Wymaga: nagłówka `X-Master-Key` i `X-Master-Pin`. "
        "Master Key: stałe czasowo compare_digest. "
        "PIN: bcrypt verify (hash w SystemConfig, NIE w .env). "
        "**Rate limit:** 3 próby / 15 minut / IP. "
        "Przekroczenie → ban IP na 1 godzinę. "
        "Każde użycie (sukces i porażka) logowane jako CRITICAL. "
        "Wymaga `master_key.enabled = true` w SystemConfig."
    ),
    response_description="Token dostępu Master Key",
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Dostęp przyznany — token impersonacji admina"},
        401: {"description": "Nieprawidłowy Master Key lub PIN"},
        403: {"description": "Master Key wyłączony lub IP zbanowany"},
        429: {"description": "Przekroczono limit prób — IP zbanowany"},
    },
)
async def master_key_login(
    request: Request,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import auth_service

    master_key = request.headers.get("X-Master-Key", "")
    master_pin = request.headers.get("X-Master-Pin", "")

    # Pobierz target_user_id z body (opcjonalne — domyślnie admin ID 1)
    target_user_id: int = 1
    try:
        body = await request.json()
        if body.get("target_user_id"):
            target_user_id = int(body["target_user_id"])
    except Exception:
        pass

    user_agent = request.headers.get("User-Agent", "")

    try:
        token_pair = await auth_service.master_access(
            db=db,
            redis=redis,
            master_key=master_key,
            pin=master_pin,
            target_user_id=target_user_id,
            ip=client_ip,
            user_agent=user_agent,
        )
    except Exception as exc:
        _raise_from_auth_error(exc)

    logger.critical(
        orjson.dumps(
            {
                "event": "api_master_key_login_success",
                "target_user_id": target_user_id,
                "request_id": request_id,
                "ip": client_ip,
                "severity": "CRITICAL",
                "alert": "Master Key użyty pomyślnie — wymagana weryfikacja",
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ).decode()
    )

    return BaseResponse.ok(
        data={
            "access_token": token_pair.access_token,
            "token_type": token_pair.token_type,
            "expires_in": token_pair.expires_in,
            "message": "Dostęp Master Key przyznany. Każda akcja jest logowana.",
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
        .limit(20)  # max 20 sesji (MAX_ACTIVE_SESSIONS z auth_service)
    )
    result = await db.execute(stmt)
    rows = result.all()

    sessions = [
        {
            "session_id": row.id_token,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "expires_at": row.expires_at.isoformat() if row.expires_at else None,
            "ip_address": row.ip_address,
            "user_agent": (row.user_agent or "")[:100],  # truncate dla frontend
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

        # Pobierz retry_after dla RateLimit jeśli dostępny
        headers = {}
        retry_after = getattr(exc, "retry_after", None)
        if retry_after:
            headers["Retry-After"] = str(int(retry_after))

        # Pobierz locked_until dla AccountLocked jeśli dostępny
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

    # Nieznany wyjątek — re-raise, catch-all handler w main.py zwróci 500
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
    """Zwraca pierwsze 3 znaki + *** dla logów (anty-PII)."""
    import hashlib
    return hashlib.sha256(email.encode()).hexdigest()[:12]
