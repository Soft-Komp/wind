"""
Fundament warstwy API — zależności FastAPI (Depends) używane przez wszystkie
endpointy systemu Windykacja.

Dostarcza:
  • get_db()            → AsyncSession   (SQLAlchemy, z connection poolem)
  • get_redis()         → Redis          (asyncio Redis client)
  • get_current_user()  → User           (JWT auth + blacklist + blokada konta)
  • get_optional_user() → User | None    (opcjonalne auth, dla mixed endpoints)
  • require_permission()→ Depends factory (RBAC z cache Redis)
  • require_master_key()→ sprawdza nagłówek X-Master-Key + PIN
  • get_pagination()    → PaginationParams
  • get_client_ip()     → str            (X-Forwarded-For lub client.host)
  • get_request_id()    → str            (UUID requestu z contextvars)

Wzorce:
  • Logi w formacie JSON Lines (orjson) do pliku + stderr
  • Cache uprawnień w Redis: klucz `perm:{user_id}:{perm}` TTL 300s
  • Przy błędach auth → zawsze HTTP 401 (nigdy 403 dla nieznanych userów)
  • Impersonacja: payload JWT zawiera `sub` (realny user) + `imp` (impersonowany)
  • Master Key: nagłówek X-Master-Key + X-Master-Pin (bcrypt verify)

"""
from __future__ import annotations

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated, AsyncGenerator, Optional

import orjson
from fastapi import Depends, Header, HTTPException, Query, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.security import (
    TOKEN_TYPE_ACCESS,
    decode_access_token,
    is_token_blacklisted,
)
from app.db.models.permission import Permission
from app.db.models.role import Role
from app.db.models.role_permission import RolePermission
from app.db.models.system_config import SystemConfig
from app.db.models.user import User
from app.db.session import get_async_session, get_redis_client

# ─────────────────────────────────────────────────────────────────────────────
# Logger (JSON Lines — spójny z logging_setup.py projektu)
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Stałe
# ─────────────────────────────────────────────────────────────────────────────

# Czas życia cache uprawnień w Redis (sekundy)
_PERM_CACHE_TTL: int = 300

# Czas życia cache konfiguracji master_key w Redis (sekundy)
_MASTER_KEY_CACHE_TTL: int = 60

# Nagłówek Master Key
_MASTER_KEY_HEADER: str = "X-Master-Key"
_MASTER_PIN_HEADER: str = "X-Master-Pin"

# Schemat Bearer dla FastAPI docs
_http_bearer = HTTPBearer(auto_error=False)

# Limit paginacji — zabezpieczenie przed gigantycznymi zapytaniami
_PAGINATION_MAX_PER_PAGE: int = 200
_PAGINATION_DEFAULT_PER_PAGE: int = 50


# ─────────────────────────────────────────────────────────────────────────────
# Typy pomocnicze
# ─────────────────────────────────────────────────────────────────────────────

class PaginationParams:
    """
    Parametry paginacji wyekstrahowane z query string.
    Wszystkie endpointy listujące używają tego obiektu zamiast
    bezpośrednich Query() w sygnaturze — mniej duplikacji.
    """

    __slots__ = ("page", "per_page", "offset")

    def __init__(self, page: int, per_page: int) -> None:
        self.page = page
        self.per_page = per_page
        self.offset = (page - 1) * per_page

    def __repr__(self) -> str:
        return (
            f"PaginationParams(page={self.page}, "
            f"per_page={self.per_page}, offset={self.offset})"
        )


class AuthContext:
    """
    Rozszerzony kontekst uwierzytelnienia — wyekstrahowany z JWT.
    Zawiera zarówno prawdziwego użytkownika jak i (opcjonalnie) impersonowanego.

    Pola:
        user         → aktualnie działający user (po impersonacji: impersonowany)
        real_user    → oryginalny zalogowany user (admin, który wykonał impersonację)
        is_impersonating → True jeśli aktywna sesja impersonacji
        jti          → JWT Token ID (do blacklisty)
        token_issued_at → kiedy token został wystawiony
    """

    __slots__ = ("user", "real_user", "is_impersonating", "jti", "token_issued_at")

    def __init__(
        self,
        user: User,
        real_user: User,
        is_impersonating: bool,
        jti: str,
        token_issued_at: datetime,
    ) -> None:
        self.user = user
        self.real_user = real_user
        self.is_impersonating = is_impersonating
        self.jti = jti
        self.token_issued_at = token_issued_at


# ─────────────────────────────────────────────────────────────────────────────
# DEPENDENCY: Baza danych
# ─────────────────────────────────────────────────────────────────────────────

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI Dependency — dostarcza sesję SQLAlchemy AsyncSession.

    Każdy request dostaje własną sesję z puli połączeń.
    Sesja jest automatycznie zamykana po zakończeniu requestu.
    Przy wyjątku: rollback + zamknięcie.

    Użycie:
        @router.get("/endpoint")
        async def handler(db: Annotated[AsyncSession, Depends(get_db)]):
            ...
    """
    async for session in get_async_session():
        yield session


# ─────────────────────────────────────────────────────────────────────────────
# DEPENDENCY: Redis
# ─────────────────────────────────────────────────────────────────────────────

async def get_redis() -> AsyncGenerator[Redis, None]:
    """
    FastAPI Dependency — dostarcza połączenie Redis (asyncio).

    Klient jest współdzielony (singleton) — nie tworzy nowego połączenia
    per-request. Redis używany do:
      - cache uprawnień (perm:{user_id}:{perm})
      - blacklista tokenów JWT (jti:{jti})
      - delete-token store (del_token:{entity}:{id})
      - SSE Pub/Sub
      - kolejka ARQ

    Użycie:
        @router.get("/endpoint")
        async def handler(redis: Annotated[Redis, Depends(get_redis)]):
            ...
    """
    client = await get_redis_client()
    try:
        yield client
    except Exception:
        logger.exception(
            orjson.dumps(
                {
                    "event": "redis_dependency_error",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )
        raise


# ─────────────────────────────────────────────────────────────────────────────
# DEPENDENCY: IP klienta
# ─────────────────────────────────────────────────────────────────────────────

async def get_client_ip(request: Request) -> str:
    """
    Wyciąga IP klienta z nagłówka X-Forwarded-For (proxy/load balancer)
    lub z request.client.host (bezpośrednie połączenie).

    Logika:
      1. X-Forwarded-For: pierwsza wartość (client IP), reszta to proxy
      2. Fallback: request.client.host
      3. Ostateczny fallback: "unknown"

    Nigdy nie rzuca wyjątku — IP jest informacyjne, nie bloker.
    """
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        # "client, proxy1, proxy2" → bierzemy pierwszego
        ip = forwarded_for.split(",")[0].strip()
        if ip:
            return ip

    if request.client and request.client.host:
        return request.client.host

    return "unknown"


# ─────────────────────────────────────────────────────────────────────────────
# DEPENDENCY: Request ID
# ─────────────────────────────────────────────────────────────────────────────

async def get_request_id(request: Request) -> str:
    """
    Zwraca request_id z contextvars (ustawiony przez AuditMiddleware).
    Jeśli middleware nie ustawił (np. w testach) — generuje nowy UUID.

    Request ID jest propagowany do:
      - logów strukturalnych
      - nagłówka X-Request-ID w odpowiedzi
      - AuditLog.Details (JSON)
    """
    # AuditMiddleware ustawia request.state.request_id
    request_id = getattr(request.state, "request_id", None)
    if not request_id:
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
    return request_id


# ─────────────────────────────────────────────────────────────────────────────
# DEPENDENCY: Paginacja
# ─────────────────────────────────────────────────────────────────────────────

async def get_pagination(
    page: Annotated[
        int,
        Query(ge=1, description="Numer strony (od 1)"),
    ] = 1,
    per_page: Annotated[
        int,
        Query(
            ge=1,
            le=_PAGINATION_MAX_PER_PAGE,
            description=f"Rekordów na stronę (max {_PAGINATION_MAX_PER_PAGE})",
        ),
    ] = _PAGINATION_DEFAULT_PER_PAGE,
) -> PaginationParams:
    """
    FastAPI Dependency — parametry paginacji z query string.

    Blokuje per_page > 200 (limit hardcodowany + walidacja Query).
    Liczy offset automatycznie: offset = (page - 1) * per_page.

    Użycie:
        @router.get("/items")
        async def list_items(
            pagination: Annotated[PaginationParams, Depends(get_pagination)],
            db: Annotated[AsyncSession, Depends(get_db)],
        ):
            items = await service.list(db, offset=pagination.offset, limit=pagination.per_page)
    """
    return PaginationParams(page=page, per_page=per_page)


# ─────────────────────────────────────────────────────────────────────────────
# WEWNĘTRZNA: Dekoduj i waliduj JWT
# ─────────────────────────────────────────────────────────────────────────────

async def _extract_token_payload(
    credentials: Optional[HTTPAuthorizationCredentials],
    redis: Redis,
    request_id: str,
    client_ip: str,
) -> dict:
    """
    Wewnętrzna funkcja — wyciąga i waliduje payload z tokena JWT.

    Kroki:
      1. Sprawdź czy nagłówek Authorization istnieje
      2. Zdekoduj JWT (verify signature + expiry)
      3. Sprawdź typ tokena (musi być 'access')
      4. Sprawdź blacklistę (JTI w Redis)

    Rzuca:
      HTTPException 401 przy każdym błędzie (nigdy nie ujawnia szczegółów)
    """
    if not credentials or not credentials.credentials:
        logger.warning(
            orjson.dumps(
                {
                    "event": "auth_missing_token",
                    "request_id": request_id,
                    "ip": client_ip,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "auth.token_missing",
                "message": "Brak tokena uwierzytelniającego",
                "errors": [{"field": "Authorization", "message": "Nagłówek wymagany"}],
            },
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials

    # Dekoduj JWT — decode_access_token sprawdza podpis i expiry
    payload = decode_access_token(token)
    if payload is None:
        logger.warning(
            orjson.dumps(
                {
                    "event": "auth_invalid_token",
                    "request_id": request_id,
                    "ip": client_ip,
                    "token_prefix": token[:20] + "...",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "auth.token_invalid",
                "message": "Token nieważny lub wygasły",
                "errors": [],
            },
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Sprawdź blacklistę JTI
    jti = payload.get("jti")
    if not jti:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "auth.token_malformed",
                "message": "Token nie zawiera wymaganego identyfikatora",
                "errors": [],
            },
            headers={"WWW-Authenticate": "Bearer"},
        )

    blacklisted = await is_token_blacklisted(redis, jti)
    if blacklisted:
        logger.warning(
            orjson.dumps(
                {
                    "event": "auth_blacklisted_token",
                    "jti": jti,
                    "request_id": request_id,
                    "ip": client_ip,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "auth.token_revoked",
                "message": "Token został unieważniony (wylogowanie)",
                "errors": [],
            },
            headers={"WWW-Authenticate": "Bearer"},
        )

    return payload


# ─────────────────────────────────────────────────────────────────────────────
# WEWNĘTRZNA: Pobierz usera z DB i zwaliduj stan konta
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_and_validate_user(
    user_id: int,
    db: AsyncSession,
    request_id: str,
    client_ip: str,
    context_label: str = "auth",
) -> User:
    """
    Pobiera usera z DB po ID i sprawdza:
      1. Czy user istnieje
      2. Czy konto jest aktywne (IsActive = 1)
      3. Czy konto nie jest zablokowane (LockedUntil)

    Parametr context_label używany w logach (np. 'auth', 'impersonation_target').

    Rzuca HTTPException 401 dla nieistniejącego/nieaktywnego usera.
    """
    now_utc = datetime.now(timezone.utc)

    stmt = (
        select(User)
        .where(User.id_user == user_id)
        .limit(1)
    )
    result = await db.execute(stmt)
    user: Optional[User] = result.scalar_one_or_none()

    if user is None:
        logger.warning(
            orjson.dumps(
                {
                    "event": f"{context_label}_user_not_found",
                    "user_id": user_id,
                    "request_id": request_id,
                    "ip": client_ip,
                    "ts": now_utc.isoformat(),
                }
            ).decode()
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "auth.user_not_found",
                "message": "Użytkownik nie istnieje",
                "errors": [],
            },
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_active:
        logger.warning(
            orjson.dumps(
                {
                    "event": f"{context_label}_user_inactive",
                    "user_id": user_id,
                    "username": user.username,
                    "request_id": request_id,
                    "ip": client_ip,
                    "ts": now_utc.isoformat(),
                }
            ).decode()
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "auth.account_inactive",
                "message": "Konto użytkownika jest nieaktywne",
                "errors": [],
            },
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Sprawdź blokadę czasową
    if user.locked_until is not None:
        # LockedUntil może być timezone-naive (MSSQL DATETIME) — normalizujemy
        locked_until = user.locked_until
        if locked_until.tzinfo is None:
            locked_until = locked_until.replace(tzinfo=timezone.utc)

        if locked_until > now_utc:
            remaining_seconds = int((locked_until - now_utc).total_seconds())
            logger.warning(
                orjson.dumps(
                    {
                        "event": f"{context_label}_user_locked",
                        "user_id": user_id,
                        "username": user.username,
                        "locked_until": locked_until.isoformat(),
                        "remaining_seconds": remaining_seconds,
                        "request_id": request_id,
                        "ip": client_ip,
                        "ts": now_utc.isoformat(),
                    }
                ).decode()
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={
                    "code": "auth.account_locked",
                    "message": "Konto tymczasowo zablokowane",
                    "errors": [
                        {
                            "field": "account",
                            "message": (
                                f"Konto zablokowane na {remaining_seconds} sekund"
                            ),
                        }
                    ],
                },
                headers={"WWW-Authenticate": "Bearer"},
            )

    return user


# ─────────────────────────────────────────────────────────────────────────────
# DEPENDENCY: Aktualny użytkownik (wymagany)
# ─────────────────────────────────────────────────────────────────────────────

async def get_current_user(
    request: Request,
    credentials: Annotated[
        Optional[HTTPAuthorizationCredentials],
        Depends(_http_bearer),
    ],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[Redis, Depends(get_redis)],
    request_id: Annotated[str, Depends(get_request_id)],
    client_ip: Annotated[str, Depends(get_client_ip)],
) -> User:
    """
    FastAPI Dependency — zwraca aktualnie zalogowanego użytkownika.

    Obsługuje impersonację:
      - Jeśli token zawiera `imp` (impersonated user ID):
        → waliduje OBIE strony (real_user + impersonated)
        → zwraca impersonowanego usera
        → ustawia request.state.real_user_id dla audit middleware

    Jeśli token jest nieprawidłowy / user nieaktywny / konto zablokowane
    → zawsze HTTP 401 z kodem błędu.

    Logowanie:
      - Każde nieudane uwierzytelnienie → log WARNING (JSON)
      - Pomyślne logowanie → log DEBUG (JSON) — nie spamuje w produkcji

    Użycie:
        CurrentUser = Annotated[User, Depends(get_current_user)]

        @router.get("/me")
        async def get_me(current_user: CurrentUser):
            return current_user
    """
    payload = await _extract_token_payload(credentials, redis, request_id, client_ip)

    # Wyciągnij user_id z pola 'sub' (standard JWT)
    sub = payload.get("sub")
    if not sub:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "auth.token_malformed",
                "message": "Token nie zawiera identyfikatora użytkownika",
                "errors": [],
            },
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        real_user_id = int(sub)
    except (ValueError, TypeError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "auth.token_malformed",
                "message": "Nieprawidłowy format identyfikatora użytkownika",
                "errors": [],
            },
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Pobierz i zwaliduj prawdziwego usera
    real_user = await _fetch_and_validate_user(
        real_user_id, db, request_id, client_ip, context_label="auth"
    )

    # Obsługa impersonacji — payload zawiera `imp` gdy aktywna
    impersonated_id = payload.get("imp")
    is_impersonating = impersonated_id is not None

    if is_impersonating:
        try:
            imp_id = int(impersonated_id)
        except (ValueError, TypeError):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={
                    "code": "auth.token_malformed",
                    "message": "Nieprawidłowy identyfikator impersonowanego użytkownika",
                    "errors": [],
                },
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Pobierz i zwaliduj impersonowanego usera
        impersonated_user = await _fetch_and_validate_user(
            imp_id, db, request_id, client_ip, context_label="impersonation_target"
        )

        # Ustaw kontekst dla middleware i serwisów
        request.state.real_user_id = real_user_id
        request.state.is_impersonating = True
        request.state.impersonated_user_id = imp_id

        logger.debug(
            orjson.dumps(
                {
                    "event": "auth_impersonation_active",
                    "real_user_id": real_user_id,
                    "real_username": real_user.username,
                    "impersonated_user_id": imp_id,
                    "impersonated_username": impersonated_user.username,
                    "request_id": request_id,
                    "ip": client_ip,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )

        # Zwróć impersonowanego — wszystkie operacje działają pod jego tożsamością
        return impersonated_user

    # Standardowy przypadek — brak impersonacji
    request.state.real_user_id = real_user_id
    request.state.is_impersonating = False

    logger.debug(
        orjson.dumps(
            {
                "event": "auth_success",
                "user_id": real_user_id,
                "username": real_user.username,
                "jti": payload.get("jti"),
                "request_id": request_id,
                "ip": client_ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ).decode()
    )

    return real_user


# ─────────────────────────────────────────────────────────────────────────────
# DEPENDENCY: Opcjonalny użytkownik (public + auth endpoints)
# ─────────────────────────────────────────────────────────────────────────────

async def get_optional_user(
    request: Request,
    credentials: Annotated[
        Optional[HTTPAuthorizationCredentials],
        Depends(_http_bearer),
    ],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[Redis, Depends(get_redis)],
    request_id: Annotated[str, Depends(get_request_id)],
    client_ip: Annotated[str, Depends(get_client_ip)],
) -> Optional[User]:
    """
    FastAPI Dependency — opcjonalne uwierzytelnienie.

    Jeśli token jest dostarczony i prawidłowy → zwraca User.
    Jeśli brak tokena lub token nieprawidłowy → zwraca None (NIE rzuca 401).

    Używany przez endpointy dostępne zarówno anonimowo jak i dla zalogowanych
    (np. /health który pokazuje więcej info dla adminów).
    """
    if not credentials or not credentials.credentials:
        return None

    try:
        return await get_current_user(
            request=request,
            credentials=credentials,
            db=db,
            redis=redis,
            request_id=request_id,
            client_ip=client_ip,
        )
    except HTTPException:
        # Token jest ale nieprawidłowy — traktujemy jak brak tokena
        return None


# ─────────────────────────────────────────────────────────────────────────────
# WEWNĘTRZNA: Pobierz uprawnienia roli z DB lub cache Redis
# ─────────────────────────────────────────────────────────────────────────────

async def _get_role_permissions(
    role_id: int,
    db: AsyncSession,
    redis: Redis,
) -> set[str]:
    """
    Pobiera zestaw nazw uprawnień przypisanych do roli.

    Cache Redis:
      Klucz: `role_perms:{role_id}`
      TTL: _PERM_CACHE_TTL (300s)
      Format: JSON array stringów ["users.list", "roles.view", ...]

    Przy cache miss → zapytanie do DB → zapis do Redis.
    Przy błędzie Redis → fallback do DB (never fail on cache error).
    """
    cache_key = f"role_perms:{role_id}"

    # Próba odczytu z cache
    try:
        cached = await redis.get(cache_key)
        if cached:
            perms: list[str] = orjson.loads(cached)
            return set(perms)
    except Exception as exc:
        logger.warning(
            orjson.dumps(
                {
                    "event": "redis_cache_miss_fallback",
                    "cache_key": cache_key,
                    "error": str(exc),
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )

    # Cache miss → DB query
    stmt = (
        select(Permission.permission_name)
        .join(RolePermission, RolePermission.id_permission == Permission.id_permission)
        .where(
            RolePermission.id_role == role_id,
            Permission.is_active == True,  # noqa: E712
        )
    )
    result = await db.execute(stmt)
    perm_names: list[str] = list(result.scalars().all())

    # Zapis do cache (błąd cache nie blokuje odpowiedzi)
    try:
        await redis.setex(
            cache_key,
            _PERM_CACHE_TTL,
            orjson.dumps(perm_names),
        )
    except Exception as exc:
        logger.warning(
            orjson.dumps(
                {
                    "event": "redis_cache_write_failed",
                    "cache_key": cache_key,
                    "error": str(exc),
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )

    return set(perm_names)


# ─────────────────────────────────────────────────────────────────────────────
# DEPENDENCY FACTORY: Wymagane uprawnienie RBAC
# ─────────────────────────────────────────────────────────────────────────────

def require_permission(permission: str):
    """
    FastAPI Dependency Factory — sprawdza czy zalogowany użytkownik
    posiada konkretne uprawnienie RBAC.

    Uprawnienia mają format `kategoria.akcja`, np.:
      - `users.list`
      - `roles.manage_permissions`
      - `debtors.send_bulk`
      - `system.config_edit`

    Cache dwupoziomowy:
      L1: Redis klucz `perm:{user_id}:{permission}` TTL 300s (szybka weryfikacja)
      L2: Redis klucz `role_perms:{role_id}` TTL 300s (cały zestaw roli)
      L3: Baza danych (fallback przy braku obu cache)

    Rzuca:
      HTTP 401 → gdy user nie jest uwierzytelniony
      HTTP 403 → gdy user nie posiada uprawnienia

    Użycie:
        @router.get("/users")
        async def list_users(
            _: Annotated[None, Depends(require_permission("users.list"))],
            current_user: Annotated[User, Depends(get_current_user)],
        ):
            ...

        # Lub jako dependency z userem:
        HasPermission = Depends(require_permission("users.list"))
    """

    async def _check(
        request: Request,
        current_user: Annotated[User, Depends(get_current_user)],
        db: Annotated[AsyncSession, Depends(get_db)],
        redis: Annotated[Redis, Depends(get_redis)],
        request_id: Annotated[str, Depends(get_request_id)],
        client_ip: Annotated[str, Depends(get_client_ip)],
    ) -> User:
        user_id = current_user.id_user
        role_id = current_user.role_id

        # L1 cache: per-user per-permission
        l1_key = f"perm:{user_id}:{permission}"
        try:
            l1_cached = await redis.get(l1_key)
            if l1_cached is not None:
                has_perm = l1_cached == b"1"
                if not has_perm:
                    _log_permission_denied(
                        user_id, current_user.username,
                        permission, "l1_cache_denied",
                        request_id, client_ip,
                    )
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=_permission_denied_detail(permission),
                    )
                return current_user
        except HTTPException:
            raise
        except Exception as exc:
            logger.warning(
                orjson.dumps(
                    {
                        "event": "redis_l1_cache_error",
                        "l1_key": l1_key,
                        "error": str(exc),
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }
                ).decode()
            )

        # L2 cache + DB: pobierz cały zestaw uprawnień roli
        role_permissions = await _get_role_permissions(role_id, db, redis)
        has_perm = permission in role_permissions

        # Zapisz wynik do L1 cache (błąd nie blokuje)
        try:
            await redis.setex(l1_key, _PERM_CACHE_TTL, b"1" if has_perm else b"0")
        except Exception:
            pass  # Cache jest opcjonalny

        if not has_perm:
            _log_permission_denied(
                user_id, current_user.username,
                permission, "permission_missing",
                request_id, client_ip,
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=_permission_denied_detail(permission),
            )

        logger.debug(
            orjson.dumps(
                {
                    "event": "permission_granted",
                    "user_id": user_id,
                    "username": current_user.username,
                    "permission": permission,
                    "role_id": role_id,
                    "request_id": request_id,
                    "ip": client_ip,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ).decode()
        )

        return current_user

    # Nazwij dependency dla czytelności w Swagger / traceback
    _check.__name__ = f"require_permission_{permission.replace('.', '_')}"
    return Depends(_check)


def _permission_denied_detail(permission: str) -> dict:
    """Buduje ujednolicony detail dla HTTP 403."""
    return {
        "code": "auth.permission_denied",
        "message": f"Brak uprawnienia: {permission}",
        "errors": [
            {
                "field": "permission",
                "message": f"Wymagane uprawnienie '{permission}' nie jest przypisane do Twojej roli",
            }
        ],
    }


def _log_permission_denied(
    user_id: int,
    username: str,
    permission: str,
    reason: str,
    request_id: str,
    client_ip: str,
) -> None:
    """Loguje odmowę uprawnienia w formacie JSON Lines."""
    logger.warning(
        orjson.dumps(
            {
                "event": "permission_denied",
                "reason": reason,
                "user_id": user_id,
                "username": username,
                "permission": permission,
                "request_id": request_id,
                "ip": client_ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ).decode()
    )


# ─────────────────────────────────────────────────────────────────────────────
# WEWNĘTRZNA: Pobierz wartość z SystemConfig (z cache)
# ─────────────────────────────────────────────────────────────────────────────

async def _get_config_value(
    key: str,
    db: AsyncSession,
    redis: Redis,
    default: Optional[str] = None,
) -> Optional[str]:
    """
    Pobiera wartość klucza z SystemConfig.
    Cache Redis: `cfg:{key}` TTL 60s.
    """
    cache_key = f"cfg:{key}"

    try:
        cached = await redis.get(cache_key)
        if cached is not None:
            return cached.decode() if isinstance(cached, bytes) else cached
    except Exception:
        pass

    stmt = select(SystemConfig.ConfigValue).where(
        SystemConfig.ConfigKey == key,
        SystemConfig.IsActive == 1,
    )
    result = await db.execute(stmt)
    value = result.scalar_one_or_none()

    if value is not None:
        try:
            await redis.setex(cache_key, _MASTER_KEY_CACHE_TTL, value)
        except Exception:
            pass

    return value if value is not None else default


# ─────────────────────────────────────────────────────────────────────────────
# DEPENDENCY: Master Key (specjalny dostęp administracyjny)
# ─────────────────────────────────────────────────────────────────────────────

async def require_master_key(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[Redis, Depends(get_redis)],
    request_id: Annotated[str, Depends(get_request_id)],
    client_ip: Annotated[str, Depends(get_client_ip)],
    x_master_key: Annotated[Optional[str], Header(alias="X-Master-Key")] = None,
    x_master_pin: Annotated[Optional[str], Header(alias="X-Master-Pin")] = None,
) -> None:
    """
    FastAPI Dependency — weryfikuje Master Key + PIN.

    Mechanizm:
      1. Sprawdź czy master_key.enabled = 'true' w SystemConfig
      2. Sprawdź czy nagłówek X-Master-Key jest dostarczony
      3. Porównaj z MASTER_KEY z settings (stałe czasowo)
      4. Sprawdź nagłówek X-Master-Pin
      5. Zweryfikuj bcrypt(pin, hash) gdzie hash z SystemConfig('master_key.pin_hash')

    Zabezpieczenia:
      - Porównanie stałoczasowe (secrets.compare_digest) dla klucza
      - bcrypt verify dla PINu (hash w DB, nie w .env)
      - Rate limiting — obsługiwany przez Redis w impersonation_service
      - Każda próba (udana i nieudana) → log CRITICAL

    Rzuca:
      HTTP 401 → zawsze (nigdy 403 — nie ujawniamy czy klucz istnieje)
    """
    import secrets
    import bcrypt

    now_utc = datetime.now(timezone.utc)
    settings = get_settings()

    # Sprawdź czy master key jest włączony
    mk_enabled = await _get_config_value("master_key.enabled", db, redis, default="false")
    if mk_enabled.lower() != "true":
        logger.warning(
            orjson.dumps(
                {
                    "event": "master_key_disabled_attempt",
                    "request_id": request_id,
                    "ip": client_ip,
                    "ts": now_utc.isoformat(),
                }
            ).decode()
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "auth.master_key_disabled",
                "message": "Dostęp przez Master Key jest wyłączony",
                "errors": [],
            },
        )

    # Waliduj nagłówek
    if not x_master_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "auth.master_key_missing",
                "message": "Wymagany nagłówek X-Master-Key",
                "errors": [{"field": "X-Master-Key", "message": "Brak nagłówka"}],
            },
        )

    # Porównanie stałoczasowe klucza (zapobiega timing attacks)
    expected_key = settings.MASTER_KEY.get_secret_value() if hasattr(settings, "MASTER_KEY") else ""
    key_valid = secrets.compare_digest(
        x_master_key.encode("utf-8"),
        expected_key.encode("utf-8"),
    )

    if not key_valid:
        logger.critical(
            orjson.dumps(
                {
                    "event": "master_key_invalid",
                    "request_id": request_id,
                    "ip": client_ip,
                    "ts": now_utc.isoformat(),
                    "severity": "CRITICAL",
                }
            ).decode()
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "auth.master_key_invalid",
                "message": "Nieprawidłowe dane uwierzytelniające",
                "errors": [],
            },
        )

    # Weryfikuj PIN (bcrypt)
    if not x_master_pin:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "auth.master_pin_missing",
                "message": "Wymagany nagłówek X-Master-Pin",
                "errors": [{"field": "X-Master-Pin", "message": "Brak nagłówka"}],
            },
        )

    pin_hash = await _get_config_value("master_key.pin_hash", db, redis)
    if not pin_hash:
        logger.critical(
            orjson.dumps(
                {
                    "event": "master_key_pin_hash_missing",
                    "message": "Brak hash PINu w SystemConfig — master_key.pin_hash",
                    "request_id": request_id,
                    "ip": client_ip,
                    "ts": now_utc.isoformat(),
                    "severity": "CRITICAL",
                }
            ).decode()
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "auth.master_key_misconfigured",
                "message": "Błąd konfiguracji serwera",
                "errors": [],
            },
        )

    # bcrypt verify — CPU-intensive, uruchamiane w executor żeby nie blokować event loop
    try:
        loop = asyncio.get_event_loop()
        pin_valid = await loop.run_in_executor(
            None,
            bcrypt.checkpw,
            x_master_pin.encode("utf-8"),
            pin_hash.encode("utf-8"),
        )
    except Exception as exc:
        logger.error(
            orjson.dumps(
                {
                    "event": "master_key_pin_verify_error",
                    "error": str(exc),
                    "request_id": request_id,
                    "ip": client_ip,
                    "ts": now_utc.isoformat(),
                }
            ).decode()
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "auth.master_key_invalid",
                "message": "Nieprawidłowe dane uwierzytelniające",
                "errors": [],
            },
        )

    if not pin_valid:
        logger.critical(
            orjson.dumps(
                {
                    "event": "master_key_pin_invalid",
                    "request_id": request_id,
                    "ip": client_ip,
                    "ts": now_utc.isoformat(),
                    "severity": "CRITICAL",
                }
            ).decode()
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "auth.master_key_invalid",
                "message": "Nieprawidłowe dane uwierzytelniające",
                "errors": [],
            },
        )

    logger.critical(
        orjson.dumps(
            {
                "event": "master_key_access_granted",
                "request_id": request_id,
                "ip": client_ip,
                "ts": now_utc.isoformat(),
                "severity": "CRITICAL",
                "alert": "Master Key użyty — sprawdź czy autoryzowane",
            }
        ).decode()
    )

    # Zapisz w stanie requestu dla AuditMiddleware
    request.state.master_key_used = True


# ─────────────────────────────────────────────────────────────────────────────
# TYPY SKRÓTOWE — używane w sygnaturach endpointów
# ─────────────────────────────────────────────────────────────────────────────

# Wstrzyknięcia przez Depends — krótsze sygnatury funkcji
DB = Annotated[AsyncSession, Depends(get_db)]
WaproDB = DB  
RedisClient = Annotated[Redis, Depends(get_redis)]
CurrentUser = Annotated[User, Depends(get_current_user)]
OptionalUser = Annotated[Optional[User], Depends(get_optional_user)]
Pagination = Annotated[PaginationParams, Depends(get_pagination)]
ClientIP = Annotated[str, Depends(get_client_ip)]
RequestID = Annotated[str, Depends(get_request_id)]

# Przykład użycia require_permission (nie Annotated — to factory):
# PermUserslist = require_permission("users.list")
# PermRolesCreate = require_permission("roles.create")
#
# @router.get("/users")
# async def list_users(
#     _: CurrentUser = PermUserslist,  # sprawdza i blokuje bez uprawnienia
#     db: DB,
#     pagination: Pagination,
# ):
#     ...

# ─────────────────────────────────────────────────────────────────────────────
# EKSPORT (czytelne importy w API routerach)
# ─────────────────────────────────────────────────────────────────────────────

__all__ = [
    # Dependencies
    "get_db",
    "get_redis",
    "get_current_user",
    "get_optional_user",
    "get_pagination",
    "get_client_ip",
    "get_request_id",
    "require_permission",
    "require_master_key",
    # Klasy
    "PaginationParams",
    "AuthContext",
    # Skrótowe typy (Annotated)
    "DB",
    "WaproDB",
    "RedisClient",
    "CurrentUser",
    "OptionalUser",
    "Pagination",
    "ClientIP",
    "RequestID",
]