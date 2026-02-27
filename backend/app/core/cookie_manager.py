"""
app/core/cookie_manager.py
══════════════════════════════════════════════════════════════════════════════
Centralny menedżer HttpOnly cookies dla refresh tokenów.

Odpowiedzialność:
    - Ustawienie refresh_token cookie po udanym logowaniu (set_refresh_cookie)
    - Usunięcie cookie przy wylogowaniu (clear_refresh_cookie)
    - Odczyt cookie z requestu (get_refresh_from_request)
    - Walidacja konfiguracji cookie przy starcie aplikacji

Architektura bezpieczeństwa:
    ┌─────────────────────────────────────────────────────────────┐
    │  HttpOnly  → JS nie może odczytać (ochrona XSS)            │
    │  Secure    → tylko HTTPS — ZAWSZE True na produkcji        │
    │  SameSite  → Strict (żądanie frontendowe — max ochrona)    │
    │  Path      → /api/v1/auth (minimalizacja ekspozycji)        │
    │  Domain    → konfigurowalny (np. api.app.pl)                │
    │  Max-Age   → sync z refresh_token_expire_days z settings    │
    └─────────────────────────────────────────────────────────────┘

Tryb cookie-only (od v2.0 — decyzja frontendu):
    Cookie jest JEDYNYM źródłem refresh tokena.
    Body JSON odpowiedzi NIE zawiera już refresh_token.
    Przeglądarka wysyła cookie automatycznie — frontend go nie czyta.

    Wyjątek: serwer NADAL akceptuje token z body przy READ (refresh/logout)
    — wyłącznie dla klientów API (Postman, testy integracyjne).
    Ten fallback nie narusza bezpieczeństwa — jest server-side only.

SameSite=Strict — uzasadnienie dla tej architektury:
    Frontend (app.pl) + backend (api.app.pl) = same-site (ta sama domena nadrzędna).
    Strict nie blokuje cookie dla żądań SPA → API w obrębie tej samej domeny.
    Blokuje cookie przy nawigacji z zewnętrznych stron — ale SPA nie potrzebuje
    refresh tokena przy pierwszym załadowaniu strony (access token w pamięci).
    Maksymalna ochrona CSRF bez utraty funkcjonalności dla SPA.

    ⚠️  Secure=True NIE działa na http://localhost.
        W .env.docker ustaw COOKIE_SECURE=false dla lokalnego dewelopmentu.
        W .env produkcyjnym: COOKIE_SECURE=true (obowiązkowe).

Autor: System Windykacja Backend
Wersja: 2.0.0 (cookie-only, SameSite=Strict)
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Final, Optional

import orjson
from fastapi import Request, Response

# ---------------------------------------------------------------------------
# Logger — dedykowany dla cookie operacji
# ---------------------------------------------------------------------------
logger = logging.getLogger("windykacja.core.cookie_manager")

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------

#: Domyślna nazwa cookie — zmieniana przez settings
_DEFAULT_COOKIE_NAME: Final[str] = "refresh_token"

#: Dozwolone wartości SameSite
_VALID_SAMESITE_VALUES: Final[frozenset[str]] = frozenset({"lax", "strict", "none"})

#: Regex do weryfikacji nazwy cookie (RFC 6265 §4.1.1)
_COOKIE_NAME_PATTERN: Final[re.Pattern] = re.compile(r"^[a-zA-Z0-9_\-]+$")

#: Max długość wartości cookie (żeby nie wpuścić garbage danych)
_MAX_COOKIE_VALUE_LEN: Final[int] = 2048

#: Prefix logu dla łatwego grep-owania
_LOG_PREFIX: Final[str] = "COOKIE_MGR"


# ===========================================================================
# Klasa CookieConfig — konfiguracja zbudowana z settings
# ===========================================================================

class CookieConfig:
    """
    Niezmienialny obiekt konfiguracji cookie.

    Tworzony raz przy starcie (lub przy pierwszym użyciu), potem
    przekazywany do funkcji set/clear. Waliduje parametry przy
    inicjalizacji — fail fast, nie fail late.

    Przykład inicjalizacji:
        from app.core.config import settings
        COOKIE_CFG = CookieConfig.from_settings(settings)
    """

    __slots__ = (
        "name",
        "secure",
        "samesite",
        "path",
        "domain",
        "max_age_seconds",
        "httponly",
        "_log_safe_repr",
    )

    def __init__(
        self,
        *,
        name: str = _DEFAULT_COOKIE_NAME,
        secure: bool = True,
        samesite: str = "strict",
        path: str = "/api/v1/auth",
        domain: Optional[str] = None,
        max_age_seconds: int = 30 * 86400,  # 30 dni default
        httponly: bool = True,  # ZAWSZE True — to jest sens tej klasy
    ) -> None:
        # --- Walidacja ---
        if not _COOKIE_NAME_PATTERN.match(name):
            raise ValueError(
                f"[{_LOG_PREFIX}] Nieprawidłowa nazwa cookie: '{name}'. "
                "Dozwolone: litery, cyfry, _, -"
            )

        samesite_normalized = samesite.lower().strip()
        if samesite_normalized not in _VALID_SAMESITE_VALUES:
            raise ValueError(
                f"[{_LOG_PREFIX}] Nieprawidłowa wartość SameSite: '{samesite}'. "
                f"Dozwolone: {_VALID_SAMESITE_VALUES}"
            )

        # SameSite=None WYMAGA Secure=True (RFC 6265bis)
        if samesite_normalized == "none" and not secure:
            raise ValueError(
                f"[{_LOG_PREFIX}] SameSite=None wymaga Secure=True. "
                "Bez HTTPS przeglądarki odrzucają takie cookie."
            )

        if max_age_seconds <= 0:
            raise ValueError(
                f"[{_LOG_PREFIX}] max_age_seconds musi być > 0, otrzymano: {max_age_seconds}"
            )

        if domain is not None and len(domain) > 253:
            raise ValueError(
                f"[{_LOG_PREFIX}] Domena zbyt długa: {len(domain)} znaków (max 253)"
            )

        # Ostrzeżenie przy Secure=False (wymagane na dev localhost, niedopuszczalne na prod)
        if not secure:
            logger.warning(
                orjson.dumps({
                    "event": f"{_LOG_PREFIX}_insecure_config",
                    "message": (
                        "COOKIE_SECURE=False — cookie wysyłane przez HTTP. "
                        "DOZWOLONE TYLKO NA LOKALNYM DEWELOPMENCIE (localhost). "
                        "Na produkcji zawsze COOKIE_SECURE=true!"
                    ),
                    "name": name,
                    "samesite": samesite,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )

        self.name: str = name
        self.secure: bool = secure
        self.samesite: str = samesite_normalized
        self.path: str = path
        self.domain: Optional[str] = domain
        self.max_age_seconds: int = max_age_seconds
        self.httponly: bool = True  # Zawsze True — nie ma innej opcji

        # Bezpieczna reprezentacja do logów (bez wartości tokena)
        self._log_safe_repr: str = (
            f"name={name!r} | secure={secure} | samesite={samesite_normalized!r} | "
            f"path={path!r} | domain={domain!r} | max_age={max_age_seconds}s | "
            f"httponly=True"
        )

        logger.info(
            orjson.dumps({
                "event": f"{_LOG_PREFIX}_config_initialized",
                "config": {
                    "name": name,
                    "secure": secure,
                    "samesite": samesite_normalized,
                    "path": path,
                    "domain": domain,
                    "max_age_seconds": max_age_seconds,
                    "httponly": True,
                },
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )

    @classmethod
    def from_settings(cls, settings: object) -> "CookieConfig":
        """
        Fabryka tworząca CookieConfig z obiektu Settings (Pydantic).

        Oczekiwane pola w settings:
            - cookie_name: str
            - cookie_secure: bool
            - cookie_samesite: str
            - cookie_path: str
            - cookie_domain: Optional[str]
            - refresh_token_expire_days: int

        Raises:
            AttributeError: Jeśli wymagane pola nie istnieją w settings.
            ValueError: Jeśli wartości są nieprawidłowe.
        """
        try:
            return cls(
                name=getattr(settings, "cookie_name", _DEFAULT_COOKIE_NAME),
                secure=getattr(settings, "cookie_secure", True),
                samesite=getattr(settings, "cookie_samesite", "strict"),
                path=getattr(settings, "cookie_path", "/api/v1/auth"),
                domain=getattr(settings, "cookie_domain", None) or None,
                max_age_seconds=int(
                    getattr(settings, "refresh_token_expire_days", 30)
                ) * 86400,
            )
        except (AttributeError, TypeError) as exc:
            logger.critical(
                orjson.dumps({
                    "event": f"{_LOG_PREFIX}_config_init_failed",
                    "error": str(exc),
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).decode()
            )
            raise

    def __repr__(self) -> str:
        return f"CookieConfig({self._log_safe_repr})"


# ===========================================================================
# Publiczne funkcje API — używane w endpointach
# ===========================================================================

def set_refresh_cookie(
    response: Response,
    token: str,
    config: CookieConfig,
    *,
    request_id: Optional[str] = None,
    user_id: Optional[int] = None,
    ip: Optional[str] = None,
) -> None:
    """
    Ustawia HttpOnly cookie z refresh tokenem w odpowiedzi HTTP.

    WAŻNE: Wywołaj PRZED zwróceniem response z endpointu.
    FastAPI inject-uje Response object — użyj `response: Response`
    jako parametr endpoint-u.

    Args:
        response:   Obiekt Response FastAPI (inject przez parametr endpoint-u).
        token:      Plaintext refresh token (będzie w Set-Cookie header).
        config:     Konfiguracja cookie (CookieConfig.from_settings(settings)).
        request_id: ID żądania do logów (opcjonalny).
        user_id:    ID użytkownika do logów (opcjonalny).
        ip:         IP klienta do logów (opcjonalny).

    Raises:
        ValueError: Jeśli token jest pusty lub za długi.

    Przykład użycia w endpoint-zie:
        @router.post("/login")
        async def login(response: Response, ...):
            token_pair = await auth_service.login(...)
            set_refresh_cookie(response, token_pair.refresh_token, COOKIE_CFG,
                               request_id=request_id, user_id=user.id_user)
            return BaseResponse.ok(data={...})
    """
    # --- Walidacja tokena ---
    if not token or not token.strip():
        _log_error("set_refresh_cookie: pusty token", request_id, user_id, ip)
        raise ValueError(f"[{_LOG_PREFIX}] Token do ustawienia w cookie nie może być pusty")

    if len(token) > _MAX_COOKIE_VALUE_LEN:
        _log_error(
            f"set_refresh_cookie: token za długi ({len(token)} > {_MAX_COOKIE_VALUE_LEN})",
            request_id, user_id, ip,
        )
        raise ValueError(
            f"[{_LOG_PREFIX}] Token zbyt długi: {len(token)} znaków "
            f"(max {_MAX_COOKIE_VALUE_LEN})"
        )

    # --- Ustaw cookie ---
    response.set_cookie(
        key=config.name,
        value=token,
        httponly=config.httponly,       # ZAWSZE True
        secure=config.secure,           # True na prod, False na dev
        samesite=config.samesite,       # "strict" (domyślnie)
        max_age=config.max_age_seconds, # sekund do wygaśnięcia
        path=config.path,               # np. "/api/v1/auth"
        domain=config.domain,           # np. "api.app.pl" lub None
    )

    # --- Log sukcesu (bez wartości tokena!) ---
    logger.info(
        orjson.dumps({
            "event": f"{_LOG_PREFIX}_set_cookie_success",
            "cookie_name": config.name,
            "path": config.path,
            "domain": config.domain,
            "secure": config.secure,
            "samesite": config.samesite,
            "max_age_seconds": config.max_age_seconds,
            "token_len": len(token),
            "token_prefix": token[:8] + "...",   # Pierwsze 8 znaków do śledzenia
            "request_id": request_id,
            "user_id": user_id,
            "ip": ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )


def clear_refresh_cookie(
    response: Response,
    config: CookieConfig,
    *,
    request_id: Optional[str] = None,
    user_id: Optional[int] = None,
    ip: Optional[str] = None,
) -> None:
    """
    Usuwa HttpOnly cookie z refresh tokenem (logout / wymuszony logout).

    Ustawia cookie z pustą wartością i Max-Age=0, co każe przeglądarce
    natychmiast je usunąć. Path i Domain MUSZĄ zgadzać się z tym, co
    było przy set_cookie — inaczej przeglądarka nie usunie właściwego cookie.

    Args:
        response:   Obiekt Response FastAPI.
        config:     Ta sama konfiguracja co przy set_refresh_cookie.
        request_id: ID żądania do logów.
        user_id:    ID użytkownika do logów.
        ip:         IP klienta do logów.
    """
    response.delete_cookie(
        key=config.name,
        path=config.path,
        domain=config.domain,
        secure=config.secure,
        samesite=config.samesite,
        httponly=config.httponly,
    )

    logger.info(
        orjson.dumps({
            "event": f"{_LOG_PREFIX}_clear_cookie_success",
            "cookie_name": config.name,
            "path": config.path,
            "domain": config.domain,
            "request_id": request_id,
            "user_id": user_id,
            "ip": ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )


def get_refresh_from_request(
    request: Request,
    config: CookieConfig,
    *,
    request_id: Optional[str] = None,
    ip: Optional[str] = None,
) -> Optional[str]:
    """
    Odczytuje refresh token z cookie requestu.

    HttpOnly = przeglądarka wysyła cookie automatycznie,
    ale JS nie może go odczytać. Backend odczytuje z nagłówka Cookie.

    Args:
        request:    Obiekt Request FastAPI.
        config:     Konfiguracja cookie (potrzebna do nazwy cookie).
        request_id: ID żądania do logów.
        ip:         IP klienta do logów.

    Returns:
        Plaintext token lub None jeśli cookie nie ma w żądaniu.
    """
    token = request.cookies.get(config.name)

    if token is None:
        logger.debug(
            orjson.dumps({
                "event": f"{_LOG_PREFIX}_cookie_not_found",
                "cookie_name": config.name,
                "available_cookies": list(request.cookies.keys()),  # Tylko nazwy, nie wartości!
                "request_id": request_id,
                "ip": ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        return None

    # Prosta sanityzacja
    token = token.strip()

    if not token:
        logger.warning(
            orjson.dumps({
                "event": f"{_LOG_PREFIX}_cookie_empty_value",
                "cookie_name": config.name,
                "request_id": request_id,
                "ip": ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        return None

    if len(token) > _MAX_COOKIE_VALUE_LEN:
        logger.warning(
            orjson.dumps({
                "event": f"{_LOG_PREFIX}_cookie_value_too_long",
                "cookie_name": config.name,
                "token_len": len(token),
                "max_len": _MAX_COOKIE_VALUE_LEN,
                "request_id": request_id,
                "ip": ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        return None  # Odrzuć — potencjalny atak

    logger.debug(
        orjson.dumps({
            "event": f"{_LOG_PREFIX}_cookie_found",
            "cookie_name": config.name,
            "token_prefix": token[:8] + "...",
            "token_len": len(token),
            "request_id": request_id,
            "ip": ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )
    return token


def extract_refresh_token_hybrid(
    request: Request,
    body: Optional[dict],
    config: CookieConfig,
    *,
    request_id: Optional[str] = None,
    ip: Optional[str] = None,
) -> Optional[str]:
    """
    Tryb hybrydowy: cookie PRIMARY, body FALLBACK.

    Kolejność:
    1. Cookie (HttpOnly — bezpieczne, niewidoczne dla JS)
    2. Body JSON field "refresh_token" (fallback dla Postman/API/mobile)

    Loguje z jakiego źródła pochodzi token.

    Args:
        request:    Request FastAPI.
        body:       Zdekodowane body JSON (może być None).
        config:     Konfiguracja cookie.
        request_id: ID żądania.
        ip:         IP klienta.

    Returns:
        Plaintext token lub None jeśli nie znaleziono w żadnym źródle.
    """
    # --- Źródło 1: Cookie ---
    token = get_refresh_from_request(request, config, request_id=request_id, ip=ip)
    if token:
        logger.info(
            orjson.dumps({
                "event": f"{_LOG_PREFIX}_token_source_cookie",
                "source": "httponly_cookie",
                "token_prefix": token[:8] + "...",
                "request_id": request_id,
                "ip": ip,
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        return token

    # --- Źródło 2: Body JSON (fallback) ---
    if body and isinstance(body, dict):
        body_token = body.get("refresh_token")
        if body_token and isinstance(body_token, str):
            body_token = body_token.strip()
            if body_token and len(body_token) <= _MAX_COOKIE_VALUE_LEN:
                logger.info(
                    orjson.dumps({
                        "event": f"{_LOG_PREFIX}_token_source_body_fallback",
                        "source": "body_json",
                        "warning": "Używanie body jako źródła refresh tokena — "
                                   "upewnij się że to dozwolony klient (Postman/mobile).",
                        "token_prefix": body_token[:8] + "...",
                        "request_id": request_id,
                        "ip": ip,
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }).decode()
                )
                return body_token

    # --- Brak tokena ---
    logger.warning(
        orjson.dumps({
            "event": f"{_LOG_PREFIX}_token_not_found_any_source",
            "checked_sources": ["httponly_cookie", "body_json"],
            "available_cookies": list(request.cookies.keys()),
            "body_has_refresh_token": bool(body and body.get("refresh_token")),
            "request_id": request_id,
            "ip": ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )
    return None


# ===========================================================================
# Funkcja pomocnicza do logowania błędów
# ===========================================================================

def _log_error(
    message: str,
    request_id: Optional[str],
    user_id: Optional[int],
    ip: Optional[str],
) -> None:
    """Pomocnicza funkcja do logowania błędów cookie manager-a."""
    logger.error(
        orjson.dumps({
            "event": f"{_LOG_PREFIX}_error",
            "message": message,
            "request_id": request_id,
            "user_id": user_id,
            "ip": ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )


# ===========================================================================
# Walidacja konfiguracji przy starcie aplikacji (Startup check)
# ===========================================================================

def validate_cookie_config_for_environment(
    config: CookieConfig,
    environment: str,
) -> list[str]:
    """
    Waliduje konfigurację cookie pod kątem środowiska.

    Zwraca listę ostrzeżeń/błędów do logowania przy starcie.
    Wywołaj w lifespan() aplikacji po zbudowaniu CookieConfig.

    Args:
        config:      Konfiguracja cookie.
        environment: Nazwa środowiska ("production", "staging", "development").

    Returns:
        Lista stringów z ostrzeżeniami (pusta = OK).
    """
    warnings: list[str] = []
    env = environment.lower().strip()

    if env in ("production", "prod"):
        if not config.secure:
            warnings.append(
                "KRYTYCZNE: COOKIE_SECURE=False na PRODUKCJI! "
                "Cookie wysyłane przez HTTP — poważna podatność bezpieczeństwa. "
                "Ustaw COOKIE_SECURE=true i upewnij się że HTTPS jest aktywne!"
            )
        if config.samesite != "strict":
            warnings.append(
                f"INFO: SameSite={config.samesite!r} na produkcji. "
                "Frontend zaleca SameSite=strict dla maksymalnej ochrony CSRF. "
                "Zmień COOKIE_SAMESITE=strict jeśli architektura na to pozwala."
            )
        if not config.domain:
            warnings.append(
                "INFO: COOKIE_DOMAIN nie ustawiony — cookie ograniczone do hosta API. "
                "OK dla architektury app.pl + api.app.pl (same-site SPA)."
            )
    elif env in ("development", "dev", "local"):
        if config.secure:
            warnings.append(
                "UWAGA: COOKIE_SECURE=True na środowisku dev. "
                "Cookie NIE będzie działać na http://localhost (brak HTTPS). "
                "Ustaw COOKIE_SECURE=false w .env.docker dla lokalnego dewelopmentu."
            )

    for warning in warnings:
        logger.warning(
            orjson.dumps({
                "event": f"{_LOG_PREFIX}_env_validation_warning",
                "environment": environment,
                "warning": warning,
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )

    if not warnings:
        logger.info(
            orjson.dumps({
                "event": f"{_LOG_PREFIX}_env_validation_ok",
                "environment": environment,
                "config_summary": {
                    "name": config.name,
                    "secure": config.secure,
                    "samesite": config.samesite,
                    "path": config.path,
                    "domain": config.domain,
                },
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )

    return warnings


# ===========================================================================
# Globalny singleton konfiguracji cookie
# ===========================================================================
# Inicjalizowany w lifespan() aplikacji (main.py):
#
#   import app.core.cookie_manager as _cm
#   _cm.COOKIE_CFG = CookieConfig.from_settings(settings)
#
# Używany w auth.py przez _get_cookie_cfg():
#
#   from app.core.cookie_manager import set_refresh_cookie
#   import app.core.cookie_manager as _cookie_module
#   cfg = _cookie_module.COOKIE_CFG
#
# WAŻNE: None przed wywołaniem lifespan() — endpointy są wywoływane
#        dopiero po starcie aplikacji, więc w praktyce zawsze zainicjalizowany.
# ===========================================================================

COOKIE_CFG: Optional[CookieConfig] = None

COOKIE_MANAGER_GLOBAL = """
# ===========================================================================
# Globalny singleton konfiguracji cookie (inicjalizowany w lifespan)
# ===========================================================================
# Użycie w endpointach:
#   from app.core.cookie_manager import COOKIE_CFG
#   set_refresh_cookie(response, token, COOKIE_CFG, ...)
#
# WAŻNE: COOKIE_CFG jest None przed inicjalizacją w lifespan().
#        Endpointy auth są wywołane PO lifespan → bezpieczne.
# ===========================================================================
COOKIE_CFG: Optional[CookieConfig] = None
"""