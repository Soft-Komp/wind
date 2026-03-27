"""
Router zarządzania użytkownikami — System Windykacja.

9 endpointów:
  GET    /users                   — lista użytkowników (paginacja, filtry)
  POST   /users                   — tworzenie użytkownika
  GET    /users/{id}              — szczegóły użytkownika
  PUT    /users/{id}              — edycja danych
  POST   /users/{id}/lock         — blokada konta
  POST   /users/{id}/unlock       — odblokowanie konta
  POST   /users/{id}/reset-password — reset hasła przez admina (generuje OTP)
  DELETE /users/{id}/initiate     — krok 1 dwuetapowego DELETE → token Redis
  DELETE /users/{id}/confirm      — krok 2 → archiwizacja + soft-delete

Wzorce:
  • Każda mutacja → AuditLog (fire-and-forget przez user_service)
  • DELETE dwuetapowy: initiate (JWT token TTL z SystemConfig) → confirm
  • Invalidacja cache Redis po każdej mutacji (przez user_service)
  • Blokada usunięcia własnego konta
  • Blokada edycji cudzej roli bez uprawnienia `users.change_role`

Format odpowiedzi: BaseResponse[T] z schemas/common.py
Serwis: services/user_service.py (CRUD + lock/unlock + dwuetapowy DELETE)

"""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import Optional
import orjson
from fastapi import APIRouter, HTTPException, Query, Request, status
from app.core.dependencies import (
    DB,
    ClientIP,
    CurrentUser,
    Pagination,
    RedisClient,
    RequestID,
    require_permission,
)
from app.schemas.common import BaseResponse
from app.services.user_service import UserListParams

# ─────────────────────────────────────────────────────────────────────────────
# Logger
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Router
# ─────────────────────────────────────────────────────────────────────────────

router = APIRouter()


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 1: GET /users
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "",
    summary="Lista użytkowników",
    description=(
        "Zwraca paginowaną listę użytkowników systemu. "
        "Sortowanie: najnowsi pierwsi (CreatedAt DESC). "
        "Filtry: is_active (true/false), role_id, search (Username/Email/FullName). "
        "**Wymaga uprawnienia:** `users.view_list`"
    ),
    response_description="Paginowana lista użytkowników",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("users.view_list")],
)
async def list_users(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    pagination: Pagination,
    request_id: RequestID,
    is_active: Optional[bool] = Query(None, description="Filtr: tylko aktywni/nieaktywni"),
    role_id: Optional[int] = Query(None, description="Filtr: po ID roli"),
    search: Optional[str] = Query(None, max_length=100, description="Wyszukiwanie po nazwie, emailu, pełnej nazwie"),
):
    from app.services import user_service

    params = UserListParams(
        page=pagination.page,
        page_size=pagination.per_page,
        search=search,
        role_id=role_id,
        is_active=is_active,
        sort_by="created_at",
        sort_desc=True,          # ← domyślnie DESC, bo dataclass tego oczekuje
    )

    result = await user_service.get_list(
        db=db,
        redis=redis,
        params=params,
    )

    return BaseResponse.ok(
        data={
            "items": result.items,
            "total": result.total,
            "page": result.page,
            "per_page": result.page_size,
            "pages": result.total_pages,
        },
        app_code="users.list",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 2: POST /users
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "",
    summary="Tworzenie użytkownika",
    description=(
        "Tworzy nowego użytkownika systemu. "
        "Wymagane pola: username, email, password, role_id. "
        "Hasło hashowane argon2id — nigdy nie zapisywane w plain text. "
        "Po utworzeniu: AuditLog + inwalidacja cache listy użytkowników. "
        "**Wymaga uprawnienia:** `users.create`"
    ),
    response_description="Utworzony użytkownik",
    status_code=status.HTTP_201_CREATED,
    dependencies=[require_permission("users.create")],
    responses={
        201: {"description": "Użytkownik utworzony"},
        409: {"description": "Nazwa użytkownika lub email już istnieje"},
        422: {"description": "Błąd walidacji danych"},
    },
)
async def create_user(
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.schemas.users import UserCreateRequest
    from app.services import user_service

    body = await _parse_body(request)

    try:
        data = UserCreateRequest(**body)
    except Exception as exc:
        raise _validation_error(exc)

    try:
        user = await user_service.create(
            db=db,
            redis=redis,
            data=data,
            created_by_user_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_user_error(exc)

    logger.info(
        orjson.dumps({
            "event": "api_user_created",
            "new_user_id": user["id_user"],
            "created_by": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.utcnow().isoformat(),
        }).decode()
    )

    return BaseResponse.ok(data=user, app_code="users.created")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 3: GET /users/{id}
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/{user_id}",
    summary="Szczegóły użytkownika",
    description=(
        "Zwraca pełne dane użytkownika: profil, rolę, uprawnienia, "
        "statystyki (liczba sesji, ostatnie logowanie, liczba komentarzy). "
        "**Wymaga uprawnienia:** `users.view`"
    ),
    response_description="Szczegóły użytkownika",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("users.view_details")],
    responses={
        404: {"description": "Użytkownik nie istnieje"},
    },
)
async def get_user(
    user_id: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request_id: RequestID,
):
    from app.services import user_service

    try:
        user = await user_service.get_by_id(db=db, redis=redis, user_id=user_id)
    except Exception as exc:
        _raise_from_user_error(exc)

    return BaseResponse.ok(data=user, app_code="users.detail")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 4: PUT /users/{id}
# ─────────────────────────────────────────────────────────────────────────────

@router.put(
    "/{user_id}",
    summary="Edycja użytkownika",
    description=(
        "Aktualizuje dane użytkownika. Obsługuje partial update (PATCH semantics). "
        "Zmiana roli wymaga dodatkowo uprawnienia `users.change_role`. "
        "Nie można zmienić własnej roli (zapobiega samowolnemu eskalowaniu uprawnień). "
        "Zmiany zapisywane w AuditLog z polami old_value/new_value (JSON). "
        "**Wymaga uprawnienia:** `users.edit`"
    ),
    response_description="Zaktualizowany użytkownik",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("users.edit")],
    responses={
        404: {"description": "Użytkownik nie istnieje"},
        409: {"description": "Email lub nazwa użytkownika zajęta"},
        422: {"description": "Błąd walidacji"},
    },
)
async def update_user(
    user_id: int,
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.schemas.users import UserUpdateRequest
    from app.services import user_service
    from app.core.dependencies import _get_role_permissions
    from app.services.user_service import UserUpdateData

    body = await _parse_body(request)

    try:
        data = UserUpdateRequest(**body)
    except Exception as exc:
        raise _validation_error(exc)

    # Zmiana roli wymaga dodatkowego uprawnienia
    if data.role_id is not None:
        # Sprawdź czy ma uprawnienie users.change_role
        perms = await _get_role_permissions(current_user.role_id, db, redis)
        if "users.change_role" not in perms:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "code": "auth.permission_denied",
                    "message": "Zmiana roli użytkownika wymaga uprawnienia users.change_role",
                    "errors": [{"field": "role_id", "message": "Brak uprawnienia users.change_role"}],
                },
            )
        # Blokada zmiany własnej roli
        if user_id == current_user.id_user:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "users.cannot_change_own_role",
                    "message": "Nie możesz zmienić własnej roli",
                    "errors": [{"field": "role_id", "message": "Operacja niedozwolona na własnym koncie"}],
                },
            )

    try:
        update_data = UserUpdateData(
            email=data.email,
            full_name=data.full_name,
            role_id=data.role_id,
            # is_active nie ma w UserUpdateData — obsłużyć osobno jeśli potrzeba
        )

        user = await user_service.update(
            db=db,
            redis=redis,
            user_id=user_id,
            data=update_data,
            updated_by_user_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_user_error(exc)

    logger.info(
        orjson.dumps({
            "event": "api_user_updated",
            "user_id": user_id,
            "updated_by": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(data=user, app_code="users.updated")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 5: POST /users/{id}/lock
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/{user_id}/lock",
    summary="Blokada konta użytkownika",
    description=(
        "Blokuje konto użytkownika na określony czas (lub bezterminowo). "
        "Zablokowany użytkownik nie może się zalogować. "
        "Wszystkie aktywne sesje (refresh tokeny) są unieważniane. "
        "Nie można zablokować własnego konta. "
        "**Wymaga uprawnienia:** `users.lock`"
    ),
    response_description="Potwierdzenie blokady",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("users.lock")],
    responses={
        400: {"description": "Konto już zablokowane"},
        403: {"description": "Próba zablokowania własnego konta"},
        404: {"description": "Użytkownik nie istnieje"},
    },
)
async def lock_user(
    user_id: int,
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import user_service

    # Blokada własnego konta
    if user_id == current_user.id_user:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "code": "users.cannot_lock_self",
                "message": "Nie możesz zablokować własnego konta",
                "errors": [{"field": "user_id", "message": "Operacja niedozwolona na własnym koncie"}],
            },
        )

    # Opcjonalnie: czas blokady w minutach (0 lub brak = bezterminowo)
    lock_minutes: int = 0
    reason: str = ""
    try:
        body = await request.json()
        lock_minutes = int(body.get("lock_minutes", 0))
        reason = str(body.get("reason", ""))[:200]
    except Exception:
        pass

    try:
        result = await user_service.lock(
            db=db,
            redis=redis,
            user_id=user_id,
            locked_by_user_id=current_user.id_user,
            duration_minutes=lock_minutes,
            reason=reason,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_user_error(exc)

    logger.warning(
        orjson.dumps({
            "event": "api_user_locked",
            "user_id": user_id,
            "locked_by": current_user.id_user,
            "lock_minutes": lock_minutes,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(data=result, app_code="users.locked")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 6: POST /users/{id}/unlock
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/{user_id}/unlock",
    summary="Odblokowanie konta użytkownika",
    description=(
        "Usuwa blokadę konta użytkownika (zeruje LockedUntil i FailedLoginAttempts). "
        "Konto staje się aktywne natychmiast. "
        "**Wymaga uprawnienia:** `users.unlock`"
    ),
    response_description="Potwierdzenie odblokowania",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("users.unlock")],
    responses={
        400: {"description": "Konto nie jest zablokowane"},
        404: {"description": "Użytkownik nie istnieje"},
    },
)
async def unlock_user(
    user_id: int,
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import user_service

    # Parsowanie opcjonalnego body
    reason: str = ""
    try:
        body = await request.json()
        reason = str(body.get("reason", ""))[:200]
    except Exception:
        pass

    try:
        result = await user_service.unlock(
            db=db,
            redis=redis,
            user_id=user_id,
            unlocked_by_user_id=current_user.id_user,
            reason=reason,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_user_error(exc)

    logger.info(
        orjson.dumps({
            "event": "api_user_unlocked",
            "user_id": user_id,
            "unlocked_by": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(data=result, app_code="users.unlocked")

# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 7: POST /users/{id}/reset-password
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/{user_id}/reset-password",
    summary="Reset hasła przez administratora",
    description=(
        "Administrator inicjuje reset hasła dla wybranego użytkownika. "
        "Generuje 6-cyfrowy kod OTP i wysyła na email użytkownika. "
        "Użytkownik musi samodzielnie ustawić nowe hasło (3-krokowy flow OTP). "
        "Wszystkie aktywne sesje użytkownika są unieważniane. "
        "**Wymaga uprawnienia:** `users.reset_password`"
    ),
    response_description="Potwierdzenie inicjacji resetu hasła",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("auth.reset_password_any")],
    responses={
        404: {"description": "Użytkownik nie istnieje"},
    },
)
async def admin_reset_password(
    user_id: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import user_service

    try:
        result = await user_service.admin_reset_password(
            db=db,
            redis=redis,
            user_id=user_id,
            admin_id=current_user.id_user,
            ip=client_ip,
        )
    except Exception as exc:
        _raise_from_user_error(exc)

    logger.warning(
        orjson.dumps({
            "event": "api_admin_reset_password",
            "user_id": user_id,
            "admin_id": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={
            "message": "Kod OTP wysłany na email użytkownika. Użytkownik musi samodzielnie ustawić nowe hasło.",
            "sessions_revoked": result.get("sessions_revoked", 0),
        },
        app_code="users.password_reset_initiated",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 8: DELETE /users/{id}/initiate
# ─────────────────────────────────────────────────────────────────────────────

@router.delete(
    "/{user_id}/initiate",
    summary="Krok 1/2 — Inicjacja usunięcia użytkownika",
    description=(
        "Pierwszy krok dwuetapowego usuwania użytkownika. "
        "Weryfikuje możliwość usunięcia (czy user istnieje, czy nie usuwa siebie). "
        "Zwraca jednorazowy `delete_token` ważny przez TTL z SystemConfig "
        "(`delete_token.ttl_seconds`, domyślnie 60 sekund). "
        "Token wymagany w kroku 2 (DELETE /users/{id}/confirm). "
        "**Uwaga:** Usunięcie to soft-delete (IsActive=0) + archiwum JSON.gz. "
        "**Wymaga uprawnienia:** `users.delete`"
    ),
    response_description="Token potwierdzający usunięcie (jednorazowy, TTL z config)",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("users.delete")],
    responses={
        200: {"description": "Token DELETE wygenerowany"},
        400: {"description": "Próba usunięcia własnego konta"},
        404: {"description": "Użytkownik nie istnieje"},
        409: {"description": "Nie można usunąć jedynego administratora"},
    },
)
async def initiate_delete_user(
    user_id: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import user_service

    # Blokada usunięcia własnego konta
    if user_id == current_user.id_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code": "users.cannot_delete_self",
                "message": "Nie możesz usunąć własnego konta",
                "errors": [{"field": "user_id", "message": "Użyj opcji dezaktywacji konta"}],
            },
        )

    try:
        result = await user_service.initiate_delete(
            db=db,
            redis=redis,
            user_id=user_id,
            initiating_user_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_user_error(exc)

    logger.warning(
        orjson.dumps({
            "event": "api_user_delete_initiated",
            "user_id": user_id,
            "initiated_by": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={
            "delete_token": result.token,
            "expires_in_seconds": result.expires_in,
            "user_id": result.user_id,
            "username": result.username,
            "message": (
                f"Token wygaśnie za {result.expires_in} sekund. "
                f"Użyj go w DELETE /users/{user_id}/confirm."
            ),
        },
        app_code="users.delete_initiated",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 9: DELETE /users/{id}/confirm
# ─────────────────────────────────────────────────────────────────────────────

@router.delete(
    "/{user_id}/confirm",
    summary="Krok 2/2 — Potwierdzenie usunięcia użytkownika",
    description=(
        "Drugi krok dwuetapowego usuwania. Wymaga `delete_token` z kroku 1. "
        "Token jest jednorazowy — po użyciu wygasa. "
        "Akcje przy usunięciu: "
        "① Archiwizacja danych użytkownika → `/app/archives/YYYY-MM-DD/` (JSON.gz), "
        "② Soft-delete (IsActive=0), "
        "③ Unieważnienie wszystkich sesji (refresh tokeny), "
        "④ AuditLog z pełnym snapshote'm danych użytkownika (old_value), "
        "⑤ Inwalidacja cache Redis. "
        "**Wymaga uprawnienia:** `users.delete`"
    ),
    response_description="Potwierdzenie usunięcia użytkownika",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("users.delete")],
    responses={
        200: {"description": "Użytkownik usunięty (soft-delete + archiwum)"},
        400: {"description": "Nieprawidłowy lub wygasły delete_token"},
        404: {"description": "Użytkownik nie istnieje"},
    },
)
async def confirm_delete_user(
    user_id: int,
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import user_service

    # Pobierz delete_token z body
    delete_token: str = ""
    try:
        body = await request.json()
        delete_token = (body.get("delete_token") or "").strip()
    except Exception:
        pass

    if not delete_token:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Wymagane pole delete_token w body",
                "errors": [{"field": "delete_token", "message": "Pole wymagane"}],
            },
        )

    try:
        result = await user_service.confirm_delete(
            db=db,
            redis=redis,
            user_id=user_id,
            confirm_token=delete_token,
            initiating_user_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_user_error(exc)

    logger.warning(
        orjson.dumps({
            "event": "api_user_deleted",
            "user_id": user_id,
            "deleted_by": current_user.id_user,
            "archive_path": result.get("archive_path", ""),
            "sessions_revoked": result.get("sessions_revoked", 0),
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={
            "message": "Użytkownik został usunięty z systemu.",
            "user_id": user_id,
            "archive_path": result.get("archive_path"),
            "sessions_revoked": result.get("sessions_revoked", 0),
        },
        app_code="users.deleted",
    )


# ─────────────────────────────────────────────────────────────────────────────
# POMOCNICZE
# ─────────────────────────────────────────────────────────────────────────────

async def _parse_body(request: Request) -> dict:
    """Parsuje JSON body — rzuca HTTP 422 przy błędzie formatu."""
    try:
        return await request.json()
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Nieprawidłowy format JSON",
                "errors": [{"field": "_", "message": "Wymagany body w formacie JSON"}],
            },
        )


def _validation_error(exc: Exception) -> HTTPException:
    """Konwertuje wyjątek Pydantic na HTTP 422."""
    errors = []
    if hasattr(exc, "errors"):
        for err in exc.errors():
            loc = err.get("loc", [])
            field = ".".join(str(p) for p in loc if p not in ("body",))
            errors.append({"field": field or "_", "message": err.get("msg", "Błąd walidacji")})
    else:
        errors = [{"field": "_", "message": str(exc)}]
    return HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        detail={"code": "validation.error", "message": "Błąd walidacji danych", "errors": errors},
    )


def _raise_from_user_error(exc: Exception) -> None:
    """
    Konwertuje wyjątki z user_service na ujednolicone HTTPException.
    """
    from app.services.user_service import UserValidationError

    # Najpierw sprawdź UserValidationError (nie ma go w _MAP)
    if isinstance(exc, UserValidationError):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "users.validation_error",
                "message": str(exc),
                "errors": [{"field": "_", "message": str(exc)}],
            },
        )

    _MAP: dict[str, tuple[int, str, str]] = {
        "UserNotFoundError":          (404, "users.not_found",            "Użytkownik nie istnieje"),
        "UserAlreadyExistsError":     (409, "users.already_exists",       "Nazwa użytkownika lub email już zajęte"),
        "UserAlreadyLockedError":     (400, "users.already_locked",       "Konto jest już zablokowane"),
        "UserNotLockedError":         (400, "users.not_locked",           "Konto nie jest zablokowane"),
        "DeleteTokenInvalidError":    (400, "users.delete_token_invalid", "Nieprawidłowy token potwierdzający"),
        "DeleteTokenExpiredError":    (400, "users.delete_token_expired", "Token potwierdzający wygasł — zainicjuj usunięcie ponownie"),
        "LastAdminError":             (409, "users.last_admin",           "Nie można usunąć jedynego administratora systemu"),
        "UserServiceError":           (400, "users.service_error",        "Błąd operacji na użytkowniku"),
        "PermissionDeniedError":      (403, "auth.permission_denied",     "Brak wymaganego uprawnienia"),
        "UserOperationBlockedError":  (409, "users.operation_blocked",    "Operacja zablokowana — użytkownik ma aktywne zasoby"),
    }

    exc_type = type(exc).__name__
    if exc_type in _MAP:
        http_status, code, default_msg = _MAP[exc_type]
        msg = str(exc) if str(exc) else default_msg
        raise HTTPException(
            status_code=http_status,
            detail={
                "code": code,
                "message": default_msg,
                "errors": [{"field": "_", "message": msg}],
            },
        )

    # Nieznany wyjątek — propaguj dalej, main.py złapie jako 500
    raise exc

def _pages(total: int, per_page: int) -> int:
    """Oblicza liczbę stron."""
    if per_page <= 0:
        return 0
    return (total + per_page - 1) // per_page
