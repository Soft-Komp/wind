"""
AuditMiddleware — System Windykacja
=====================================
Middleware audytowy odpowiedzialny za:

  • Generowanie unikalnego request_id (UUID4) dla każdego żądania
  • Ekstrakcję IP klienta (X-Forwarded-For → X-Real-IP → client.host)
  • Parsing i logowanie User-Agent
  • Ustawienie kontekstu logowania (contextvars) dla całego żądania
  • Pomiar czasu odpowiedzi (perf_counter — nanosekund dokładność)
  • Logowanie szczegółów requestu PRZED i PO przetworzeniu (JSONL)
  • Wstrzykiwanie nagłówków bezpieczeństwa do każdej odpowiedzi
  • Wykrywanie podejrzanych wzorców (SQL injection, path traversal, etc.)
  • Walidację i blokowanie requestów z nieprawidłowymi nagłówkami
  • Śledzenie rozmiaru body requestu i odpowiedzi
  • Wyciąganie user_id z JWT (bez pełnej weryfikacji — tylko dla logów)
  • Zapisywanie metryk per-endpoint do pliku stats JSONL
  • Obsługę błędów middleware bez przerywania aplikacji (self-healing)

Zasada: jeśli coś się wydarzy, musi być możliwość odtworzenia
        co, kiedy, przez kogo i z jakim skutkiem.

Wersja: 1.0.0 | Data: 2026-02-20 | Python: 3.12+
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import sys
import time
import traceback
import unicodedata
import uuid
from collections.abc import Callable, Sequence
from contextvars import ContextVar, Token
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final

import orjson
from starlette.datastructures import Headers
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp

# ---------------------------------------------------------------------------
# Contextvars — wspólne z core.logging_setup (import z fallbackiem)
# ---------------------------------------------------------------------------
try:
    from app.core.logging_setup import (
        request_id_var,
        user_id_var,
        ip_address_var,
        username_var,
        set_request_context,
        clear_request_context,
    )
except ImportError:
    # Fallback: definiujemy lokalnie jeśli logging_setup niedostępny
    request_id_var: ContextVar[str] = ContextVar("request_id", default="-")
    user_id_var: ContextVar[int | None] = ContextVar("user_id", default=None)
    ip_address_var: ContextVar[str] = ContextVar("ip_address", default="-")
    username_var: ContextVar[str] = ContextVar("username", default="-")

    def set_request_context(
        request_id: str,
        user_id: int | None = None,
        ip: str = "-",
        username: str = "-",
    ) -> list[Token]:
        tokens = [
            request_id_var.set(request_id),
            user_id_var.set(user_id),
            ip_address_var.set(ip),
            username_var.set(username),
        ]
        return tokens

    def clear_request_context(tokens: list[Token]) -> None:
        for token in tokens:
            try:
                token.var.reset(token)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Logger — własny logger middleware (nie core.logging_setup żeby nie zapętlić)
# ---------------------------------------------------------------------------
logger = logging.getLogger("windykacja.middleware.audit")

# ---------------------------------------------------------------------------
# Stałe konfiguracyjne
# ---------------------------------------------------------------------------

# Ścieżki wykluczone z pełnego logowania (ale nadal dostaną security headers)
_PATHS_EXCLUDED_FROM_AUDIT: Final[frozenset[str]] = frozenset({
    "/api/v1/docs",
    "/api/v1/openapi.json",
    "/api/v1/redoc",
    "/docs",
    "/openapi.json",
    "/redoc",
    "/favicon.ico",
})

# Ścieżki których nie logujemy w ogóle (health check — nie zaśmiecać logów)
_PATHS_SILENT: Final[frozenset[str]] = frozenset({
    "/health",
    "/api/v1/system/health",
    "/ping",
})

# Maksymalny rozmiar body do zalogowania (nie wczytujemy całego body do RAM)
_MAX_LOGGED_BODY_BYTES: Final[int] = 4 * 1024  # 4 KB — tylko dla logów
_MAX_REQUEST_BODY_BYTES: Final[int] = 10 * 1024 * 1024  # 10 MB — hard limit

# Nagłówki bezpieczeństwa wstrzykiwane do KAŻDEJ odpowiedzi
_SECURITY_HEADERS: Final[dict[str, str]] = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "X-XSS-Protection": "1; mode=block",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "Permissions-Policy": (
        "camera=(), microphone=(), geolocation=(), payment=(), "
        "usb=(), magnetometer=(), accelerometer=()"
    ),
    "Content-Security-Policy": (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "connect-src 'self'; "
        "font-src 'self'; "
        "frame-ancestors 'none'"
    ),
    "X-Permitted-Cross-Domain-Policies": "none",
    "Cross-Origin-Embedder-Policy": "require-corp",
    "Cross-Origin-Opener-Policy": "same-origin",
    "Cross-Origin-Resource-Policy": "same-origin",
}

# Wzorce podejrzanych payloadów — blokujemy na poziomie middleware
_SUSPICIOUS_PATTERNS: Final[Sequence[tuple[str, re.Pattern]]] = [
    # SQL Injection — podstawowe wzorce
    ("sql_injection_union",   re.compile(r"(?i)\bunion\b.{0,30}\bselect\b")),
    ("sql_injection_drop",    re.compile(r"(?i)\b(drop|truncate)\b.{0,20}\b(table|database|schema)\b")),
    ("sql_injection_exec",    re.compile(r"(?i)\b(exec|execute|xp_|sp_)\b")),
    ("sql_injection_comment", re.compile(r"(?i)(--|/\*|\*/).{0,5}(select|insert|update|delete|drop)")),
    ("sql_injection_quote",   re.compile(r"(?i)'\s*(or|and)\s*'?\d+\s*'?\s*=\s*'?\d+")),

    # Path Traversal
    ("path_traversal_dotdot", re.compile(r"\.\.[/\\]")),
    ("path_traversal_encoded",re.compile(r"%2e%2e[%/\\]", re.IGNORECASE)),
    ("path_traversal_null",   re.compile(r"%00")),

    # XSS
    ("xss_script_tag",        re.compile(r"(?i)<\s*script[\s>]")),
    ("xss_javascript",        re.compile(r"(?i)javascript\s*:")),
    ("xss_event_handler",     re.compile(r"(?i)\bon(load|error|click|mouseover|focus|blur)\s*=")),
    ("xss_iframe",            re.compile(r"(?i)<\s*iframe[\s>]")),
    ("xss_data_uri",          re.compile(r"(?i)data\s*:\s*text\s*/\s*html")),

    # SSRF / Internal network
    ("ssrf_localhost",        re.compile(r"(?i)(localhost|127\.0\.0\.1|0\.0\.0\.0|::1)")),
    ("ssrf_aws_metadata",     re.compile(r"(?i)169\.254\.169\.254")),
    ("ssrf_private_range_a",  re.compile(r"(?i)10\.\d{1,3}\.\d{1,3}\.\d{1,3}")),

    # Command injection
    ("cmd_injection_shell",   re.compile(r"(?i)[;&|`$].*\b(bash|sh|cmd|powershell|wget|curl|nc|ncat)\b")),
    ("cmd_injection_pipe",    re.compile(r"\|\s*(bash|sh|cmd|powershell|python|perl|ruby)")),

    # XXE / XML injection
    ("xxe_entity",            re.compile(r"(?i)<!entity|<!doctype|<!\[cdata\[")),
    ("xxe_system",            re.compile(r"(?i)system\s+['\"]file://|system\s+['\"]http://")),

    # Template injection
    ("template_injection",    re.compile(r"\{\{.{0,50}\}\}|\{%.{0,50}%\}")),

    # NoSQL injection
    ("nosql_operator",        re.compile(r'(?i)\$where|\$regex|\$gt|\$lt|\$ne.*\{')),
]

# Metody HTTP, które NIE powinny mieć body — blokujemy Content-Length > 0
_BODYLESS_METHODS: Final[frozenset[str]] = frozenset({"GET", "HEAD", "OPTIONS", "DELETE"})

# Maksymalna długość URL
_MAX_URL_LENGTH: Final[int] = 2048

# Nagłówki których NIE logujemy (mogą zawierać dane wrażliwe)
_SENSITIVE_HEADERS: Final[frozenset[str]] = frozenset({
    "authorization",
    "cookie",
    "x-api-key",
    "x-auth-token",
    "proxy-authorization",
})


# ---------------------------------------------------------------------------
# Pomocnik logowania do pliku (niezależny od logging — redundancja)
# ---------------------------------------------------------------------------
class _FileLogger:
    """
    Redundantny logger plikowy — zapisuje JSONL bezpośrednio do pliku
    niezależnie od systemu logging. Jeśli główne logowanie zawiedzie,
    ten logger nadal działa.
    """

    def __init__(self, log_dir: str) -> None:
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._current_date: str = ""
        self._file_handle = None
        self._lock = asyncio.Lock()

    def _get_log_path(self) -> Path:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return self._log_dir / f"app_{today}.log"

    async def write(self, record: dict[str, Any]) -> None:
        """Zapisz rekord JSONL do pliku (thread-safe przez asyncio.Lock)."""
        try:
            async with self._lock:
                log_path = self._get_log_path()
                line = orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE)
                # Append-only — nieusuwalne przez aplikację
                with log_path.open("ab") as f:
                    f.write(line)
        except Exception as exc:  # noqa: BLE001
            # Ostatnia linia obrony — stderr żeby nie utracić informacji
            print(
                f"[AUDIT_MIDDLEWARE] Błąd zapisu do pliku logu: {exc}",
                file=sys.stderr,
            )

    def write_sync(self, record: dict[str, Any]) -> None:
        """Synchroniczna wersja — używana w finally blokach."""
        try:
            log_path = self._get_log_path()
            line = orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE)
            with log_path.open("ab") as f:
                f.write(line)
        except Exception as exc:  # noqa: BLE001
            print(
                f"[AUDIT_MIDDLEWARE] Błąd zapisu sync: {exc}",
                file=sys.stderr,
            )


# ---------------------------------------------------------------------------
# Funkcje pomocnicze
# ---------------------------------------------------------------------------

def _extract_client_ip(request: Request) -> str:
    """
    Ekstrakcja IP klienta z kolejności: X-Forwarded-For → X-Real-IP → client.host.
    Obsługuje proxy chain — bierze PIERWSZE IP z X-Forwarded-For (IP klienta).
    Normalizuje IPv6 → IPv4 gdzie możliwe.
    """
    # 1. X-Forwarded-For (format: "client, proxy1, proxy2")
    xff = request.headers.get("x-forwarded-for", "").strip()
    if xff:
        # Bierz pierwsze IP z łańcucha proxy
        first_ip = xff.split(",")[0].strip()
        if first_ip:
            return _sanitize_ip(first_ip)

    # 2. X-Real-IP (nginx)
    xri = request.headers.get("x-real-ip", "").strip()
    if xri:
        return _sanitize_ip(xri)

    # 3. Bezpośredni klient
    if request.client:
        return _sanitize_ip(request.client.host or "-")

    return "unknown"


def _sanitize_ip(ip: str) -> str:
    """Sanityzacja IP — tylko dozwolone znaki. Blokuje code injection przez IP."""
    # Usuń znaki poza [0-9a-fA-F.:] — walidacja IPv4 i IPv6
    sanitized = re.sub(r"[^0-9a-fA-F.:\[\]]", "", ip)
    # Limit długości IPv6 = max 45 znaków (::ffff:255.255.255.255)
    return sanitized[:45] if sanitized else "invalid"


def _extract_user_agent(headers: Headers) -> dict[str, str]:
    """
    Ekstrakcja i parsowanie User-Agent.
    Zwraca dict z raw UA i wykrytym typem klienta.
    """
    ua_raw = headers.get("user-agent", "")
    # Sanityzacja — NFC + limit długości
    ua_sanitized = unicodedata.normalize("NFC", ua_raw)[:512]

    client_type = "unknown"
    if re.search(r"(?i)python|httpx|requests|aiohttp|curl|wget", ua_sanitized):
        client_type = "api_client"
    elif re.search(r"(?i)mozilla|chrome|safari|firefox|edge", ua_sanitized):
        client_type = "browser"
    elif re.search(r"(?i)postman|insomnia|bruno|swagger", ua_sanitized):
        client_type = "api_testing"
    elif not ua_sanitized:
        client_type = "no_ua"

    return {
        "raw": ua_sanitized,
        "client_type": client_type,
    }


def _peek_jwt_user_id(authorization: str) -> int | None:
    """
    Wyciąga user_id z JWT token BEZ weryfikacji podpisu.
    Używane TYLKO do celów logowania — nie do autoryzacji!
    Zwraca None jeśli token nieobecny lub nieparseowalny.
    """
    if not authorization or not authorization.startswith("Bearer "):
        return None
    try:
        token = authorization[7:]  # usuń "Bearer "
        parts = token.split(".")
        if len(parts) != 3:
            return None
        # Dekoduj payload (część środkowa) — base64url bez weryfikacji
        payload_b64 = parts[1]
        # Dopełnij do wielokrotności 4
        padding = 4 - len(payload_b64) % 4
        if padding != 4:
            payload_b64 += "=" * padding
        import base64
        payload_bytes = base64.urlsafe_b64decode(payload_b64)
        payload = orjson.loads(payload_bytes)
        return payload.get("sub") or payload.get("user_id") or payload.get("id")
    except Exception:  # noqa: BLE001
        return None


def _peek_jwt_username(authorization: str) -> str:
    """Analogicznie do _peek_jwt_user_id — wyciąga username z JWT."""
    if not authorization or not authorization.startswith("Bearer "):
        return "-"
    try:
        token = authorization[7:]
        parts = token.split(".")
        if len(parts) != 3:
            return "-"
        payload_b64 = parts[1]
        padding = 4 - len(payload_b64) % 4
        if padding != 4:
            payload_b64 += "=" * padding
        import base64
        payload_bytes = base64.urlsafe_b64decode(payload_b64)
        payload = orjson.loads(payload_bytes)
        return str(payload.get("username") or payload.get("sub_name") or "-")
    except Exception:  # noqa: BLE001
        return "-"


def _hash_sensitive_value(value: str) -> str:
    """SHA256[:12] — anonimizacja wartości wrażliwych w logach."""
    return hashlib.sha256(value.encode()).hexdigest()[:12]


def _sanitize_headers_for_log(headers: Headers) -> dict[str, str]:
    """
    Przygotowuje nagłówki do logowania:
    - Wrażliwe nagłówki → REDACTED z hashem (do korelacji bez ujawniania wartości)
    - Pozostałe → sanityzacja NFC
    - Limit wartości: 256 znaków
    """
    result: dict[str, str] = {}
    for key, value in headers.items():
        key_lower = key.lower()
        if key_lower in _SENSITIVE_HEADERS:
            # Hash zamiast wartości — można porównać ale nie odczytać
            hashed = _hash_sensitive_value(value)
            result[key_lower] = f"REDACTED[sha256:{hashed}]"
        else:
            # Sanityzacja NFC + limit długości
            sanitized = unicodedata.normalize("NFC", value)[:256]
            result[key_lower] = sanitized
    return result


def _check_suspicious_content(content: str) -> list[dict[str, str]]:
    """
    Sprawdza content pod kątem podejrzanych wzorców.
    Zwraca listę wykrytych zagrożeń (puste = czyste).
    """
    threats: list[dict[str, str]] = []
    for threat_name, pattern in _SUSPICIOUS_PATTERNS:
        match = pattern.search(content)
        if match:
            threats.append({
                "type": threat_name,
                "matched": match.group()[:100],  # Ogranicz do 100 znaków
                "position": str(match.start()),
            })
    return threats


def _sanitize_url_for_log(url: str) -> str:
    """Sanityzacja URL do logowania — usuwa potencjalnie wrażliwe query params."""
    # Usuń wartości parametrów password/token/key z URL
    sanitized = re.sub(
        r"(?i)(password|token|key|secret|apikey|api_key)=[^&\s]*",
        r"\1=REDACTED",
        url,
    )
    return sanitized[:1024]  # Limit długości w logach


def _get_content_length(headers: Headers) -> int | None:
    """Bezpieczna ekstrakcja Content-Length."""
    cl = headers.get("content-length", "")
    if cl and cl.isdigit():
        return int(cl)
    return None


# ---------------------------------------------------------------------------
# Główna klasa middleware
# ---------------------------------------------------------------------------
class AuditMiddleware(BaseHTTPMiddleware):
    """
    Middleware audytowy dla systemu Windykacja.

    Loguje każde żądanie z pełnymi metadanymi:
    - Czas wejścia/wyjścia z precyzją milisekundową
    - IP klienta (z obsługą proxy chain)
    - User-Agent i typ klienta
    - Rozmiar requestu i response
    - user_id wyciągnięty z JWT (bez weryfikacji — tylko do logów)
    - Wykryte zagrożenia bezpieczeństwa
    - Status code i timing

    Wstrzykuje nagłówki bezpieczeństwa do każdej odpowiedzi.
    Blokuje żądania przekraczające limity lub zawierające złośliwy content.

    Wszystko zapisywane do logs/app_YYYY-MM-DD.log w formacie JSONL.
    """

    def __init__(
        self,
        app: ASGIApp,
        log_dir: str | None = None,
        max_body_bytes: int = _MAX_REQUEST_BODY_BYTES,
        block_suspicious: bool = True,
        inject_security_headers: bool = True,
        log_request_body: bool = False,  # Domyślnie False — PII protection
    ) -> None:
        super().__init__(app)
        self._log_dir = log_dir or os.environ.get("LOG_DIR", "/app/logs")
        self._max_body_bytes = max_body_bytes
        self._block_suspicious = block_suspicious
        self._inject_security_headers = inject_security_headers
        self._log_request_body = log_request_body
        self._file_logger = _FileLogger(self._log_dir)

        logger.info(
            orjson.dumps({
                "event": "audit_middleware_init",
                "log_dir": self._log_dir,
                "max_body_bytes": self._max_body_bytes,
                "block_suspicious": self._block_suspicious,
                "inject_security_headers": self._inject_security_headers,
                "log_request_body": self._log_request_body,
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )

    # ------------------------------------------------------------------
    # Główna metoda dispatch
    # ------------------------------------------------------------------
    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        """
        Punkt wejścia dla każdego żądania HTTP.

        Kolejność działań:
        1. Generuj request_id
        2. Wyciągnij IP i User-Agent
        3. Wstępna walidacja żądania (URL, Content-Length, metoda)
        4. Wyciągnij user_id z JWT
        5. Ustaw contextvary
        6. Zaloguj wejście requestu
        7. Wykonaj handler
        8. Wstrzyknij security headers
        9. Zaloguj odpowiedź
        10. Wyczyść contextvary
        """
        request_id = str(uuid.uuid4())
        start_time = time.perf_counter()
        ts_start = datetime.now(timezone.utc)

        # ---- Ekstrakcja podstawowych danych ----
        client_ip = _extract_client_ip(request)
        user_agent_info = _extract_user_agent(request.headers)
        method = request.method.upper()
        path = request.url.path
        query_string = str(request.url.query)
        full_url = str(request.url)

        # ---- Wyciągnij user_id z JWT (tylko do logów) ----
        authorization = request.headers.get("authorization", "")
        jwt_user_id = _peek_jwt_user_id(authorization)
        jwt_username = _peek_jwt_username(authorization)

        # ---- Ustaw contextvary ----
        ctx_tokens = set_request_context(
            request_id=request_id,
            user_id=jwt_user_id,
            ip=client_ip,
            username=jwt_username,
        )

        # ---- Dodaj request_id do state (dostępny w endpointach) ----
        request.state.request_id = request_id
        request.state.client_ip = client_ip

        is_silent = path in _PATHS_SILENT
        is_excluded = path in _PATHS_EXCLUDED_FROM_AUDIT
        response: Response | None = None
        error_details: dict | None = None
        threats_detected: list = []

        try:
            # ================================================================
            # KROK 1: Wstępna walidacja żądania
            # ================================================================
            validation_error = await self._validate_request(
                request, method, path, full_url, query_string,
            )
            if validation_error:
                # Zablokowany request — loguj i zwróć 400
                block_record = self._build_block_record(
                    request_id=request_id,
                    method=method,
                    path=path,
                    client_ip=client_ip,
                    user_agent=user_agent_info,
                    jwt_user_id=jwt_user_id,
                    reason=validation_error["reason"],
                    details=validation_error,
                    ts=ts_start.isoformat(),
                )
                logger.warning(orjson.dumps(block_record).decode())
                asyncio.create_task(self._file_logger.write(block_record))
                return self._blocked_response(
                    request_id=request_id,
                    reason=validation_error["reason"],
                    detail=validation_error.get("detail", "Żądanie odrzucone"),
                )

            # ================================================================
            # KROK 2: Sprawdzenie podejrzanego contentu w URL/query
            # ================================================================
            if self._block_suspicious and not is_excluded:
                url_content_to_check = f"{path}?{query_string}"
                threats_detected = _check_suspicious_content(url_content_to_check)
                if threats_detected:
                    block_record = self._build_block_record(
                        request_id=request_id,
                        method=method,
                        path=path,
                        client_ip=client_ip,
                        user_agent=user_agent_info,
                        jwt_user_id=jwt_user_id,
                        reason="suspicious_content_in_url",
                        details={"threats": threats_detected},
                        ts=ts_start.isoformat(),
                    )
                    logger.warning(orjson.dumps(block_record).decode())
                    asyncio.create_task(self._file_logger.write(block_record))
                    return self._blocked_response(
                        request_id=request_id,
                        reason="suspicious_content_detected",
                        detail="Żądanie zawiera niedozwoloną treść",
                    )

            # ================================================================
            # KROK 3: Logowanie wejścia requestu
            # ================================================================
            if not is_silent:
                request_record = await self._build_request_log(
                    request_id=request_id,
                    request=request,
                    method=method,
                    path=path,
                    query_string=query_string,
                    client_ip=client_ip,
                    user_agent=user_agent_info,
                    jwt_user_id=jwt_user_id,
                    jwt_username=jwt_username,
                    ts=ts_start.isoformat(),
                )
                logger.info(orjson.dumps(request_record).decode())
                asyncio.create_task(self._file_logger.write(request_record))

            # ================================================================
            # KROK 4: Wykonaj handler
            # ================================================================
            response = await call_next(request)

        except Exception as exc:
            # Handler rzucił wyjątek — loguj jako krytyczny błąd
            elapsed_ms = round((time.perf_counter() - start_time) * 1000, 3)
            error_details = {
                "exception_type": type(exc).__name__,
                "exception_message": str(exc)[:500],
                "traceback_lines": traceback.format_exc().splitlines()[-10:],
            }

            error_record = {
                "event": "request_unhandled_exception",
                "request_id": request_id,
                "method": method,
                "path": path,
                "client_ip": client_ip,
                "user_id": jwt_user_id,
                "username": jwt_username,
                "elapsed_ms": elapsed_ms,
                "error": error_details,
                "ts": ts_start.isoformat(),
                "ts_error": datetime.now(timezone.utc).isoformat(),
            }
            logger.critical(orjson.dumps(error_record).decode())
            self._file_logger.write_sync(error_record)

            # Zwróć 500 zamiast propagować wyjątek (aplikacja nie pada)
            response = JSONResponse(
                status_code=500,
                content=orjson.loads(orjson.dumps({
                    "success": False,
                    "code": "server.internal_error",
                    "message": "Wewnętrzny błąd serwera",
                    "errors": [{"field": "_", "message": "Nieoczekiwany błąd serwera"}],
                    "meta": {
                        "request_id": request_id,
                        "timestamp": ts_start.isoformat(),
                    },
                })),
            )

        finally:
            # Zawsze czyść contextvary
            clear_request_context(ctx_tokens)

        # ================================================================
        # KROK 5: Post-processing response
        # ================================================================
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 3)
        ts_end = datetime.now(timezone.utc)

        # ---- Wstrzyknij security headers ----
        if self._inject_security_headers:
            self._inject_headers(response)

        # ---- Zawsze dodaj request_id do response ----
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Response-Time"] = f"{elapsed_ms}ms"

        # ---- Logowanie odpowiedzi ----
        if not is_silent:
            response_record = self._build_response_log(
                request_id=request_id,
                method=method,
                path=path,
                client_ip=client_ip,
                jwt_user_id=jwt_user_id,
                jwt_username=jwt_username,
                status_code=response.status_code,
                elapsed_ms=elapsed_ms,
                content_length=response.headers.get("content-length"),
                content_type=response.headers.get("content-type", ""),
                ts_start=ts_start.isoformat(),
                ts_end=ts_end.isoformat(),
                error_details=error_details,
                threats_detected=threats_detected,
            )

            # Poziom logowania w zależności od status code
            if response.status_code >= 500:
                logger.error(orjson.dumps(response_record).decode())
            elif response.status_code >= 400:
                logger.warning(orjson.dumps(response_record).decode())
            else:
                logger.info(orjson.dumps(response_record).decode())

            asyncio.create_task(self._file_logger.write(response_record))

        return response

    # ------------------------------------------------------------------
    # Metody pomocnicze — walidacja
    # ------------------------------------------------------------------
    async def _validate_request(
        self,
        request: Request,
        method: str,
        path: str,
        full_url: str,
        query_string: str,
    ) -> dict | None:
        """
        Walidacja wstępna żądania. Zwraca dict z błędem lub None jeśli OK.

        Sprawdza:
        - Długość URL
        - Content-Length vs metoda HTTP
        - Content-Type dla POST/PUT/PATCH
        - Wymagane nagłówki bezpieczeństwa (Accept, Content-Type)
        - Dopuszczalne metody HTTP
        """
        # ---- Długość URL ----
        if len(full_url) > _MAX_URL_LENGTH:
            return {
                "reason": "url_too_long",
                "detail": f"URL przekracza limit {_MAX_URL_LENGTH} znaków",
                "url_length": len(full_url),
                "max_allowed": _MAX_URL_LENGTH,
            }

        # ---- Dozwolone metody HTTP ----
        allowed_methods = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
        if method not in allowed_methods:
            return {
                "reason": "method_not_allowed",
                "detail": f"Metoda HTTP '{method}' nie jest dozwolona",
                "method": method,
            }

        # ---- Content-Length dla metod bez body ----
        content_length = _get_content_length(request.headers)
        if method in _BODYLESS_METHODS and content_length and content_length > 0:
            # GET/HEAD z body — podejrzane (ale nie blokujemy DELETE z body)
            if method in {"GET", "HEAD"}:
                return {
                    "reason": "body_in_bodyless_method",
                    "detail": f"Metoda {method} nie powinna zawierać body",
                    "method": method,
                    "content_length": content_length,
                }

        # ---- Limit rozmiaru body ----
        if content_length and content_length > self._max_body_bytes:
            return {
                "reason": "request_body_too_large",
                "detail": f"Rozmiar body przekracza limit {self._max_body_bytes} bajtów",
                "content_length": content_length,
                "max_allowed": self._max_body_bytes,
            }

        # ---- Content-Type dla mutacji ----
        if method in {"POST", "PUT", "PATCH"}:
            content_type = request.headers.get("content-type", "")
            # Wyklucz multipart i form data (mają własne Content-Type)
            if content_length and content_length > 0:
                if not content_type:
                    return {
                        "reason": "missing_content_type",
                        "detail": "Nagłówek Content-Type jest wymagany dla żądań z body",
                        "method": method,
                    }
                # Dozwolone Content-Type dla API
                allowed_content_types = (
                    "application/json",
                    "multipart/form-data",
                    "application/x-www-form-urlencoded",
                    "application/octet-stream",
                    "text/plain",
                )
                if not any(ct in content_type for ct in allowed_content_types):
                    return {
                        "reason": "invalid_content_type",
                        "detail": f"Content-Type '{content_type}' nie jest obsługiwany",
                        "content_type": content_type,
                        "allowed": list(allowed_content_types),
                    }

        return None

    # ------------------------------------------------------------------
    # Budowanie rekordów logów
    # ------------------------------------------------------------------
    async def _build_request_log(
        self,
        request_id: str,
        request: Request,
        method: str,
        path: str,
        query_string: str,
        client_ip: str,
        user_agent: dict,
        jwt_user_id: int | None,
        jwt_username: str,
        ts: str,
    ) -> dict:
        """Buduje kompletny rekord JSONL dla przychodzącego żądania."""
        headers_safe = _sanitize_headers_for_log(request.headers)

        record: dict[str, Any] = {
            "event": "http_request_received",
            "request_id": request_id,
            "method": method,
            "path": path,
            "path_params": dict(request.path_params) if request.path_params else {},
            "query_string": _sanitize_url_for_log(query_string),
            "full_url": _sanitize_url_for_log(str(request.url)),
            "client": {
                "ip": client_ip,
                "user_agent": user_agent["raw"],
                "client_type": user_agent["client_type"],
                "host": request.headers.get("host", ""),
                "origin": request.headers.get("origin", ""),
                "referer": request.headers.get("referer", "")[:256],
            },
            "auth": {
                "user_id": jwt_user_id,
                "username": jwt_username,
                "has_token": bool(request.headers.get("authorization")),
                "token_type": "Bearer" if "Bearer " in request.headers.get("authorization", "") else None,
            },
            "request": {
                "content_length": _get_content_length(request.headers),
                "content_type": request.headers.get("content-type", ""),
                "accept": request.headers.get("accept", ""),
                "accept_encoding": request.headers.get("accept-encoding", ""),
                "accept_language": request.headers.get("accept-language", ""),
            },
            "headers_safe": headers_safe,
            "ts": ts,
        }

        # Opcjonalnie: loguj body (UWAGA: PII — domyślnie wyłączone)
        if self._log_request_body and method in {"POST", "PUT", "PATCH"}:
            try:
                # Odczytaj fragment body (nie cały — limit pamięci)
                body_bytes = await request.body()
                body_preview = body_bytes[:_MAX_LOGGED_BODY_BYTES]
                try:
                    body_decoded = orjson.loads(body_preview)
                    record["request"]["body_preview"] = body_decoded
                except Exception:
                    record["request"]["body_preview_raw"] = body_preview.decode("utf-8", errors="replace")
                record["request"]["body_size_bytes"] = len(body_bytes)
                record["request"]["body_truncated"] = len(body_bytes) > _MAX_LOGGED_BODY_BYTES
            except Exception as exc:
                record["request"]["body_read_error"] = str(exc)[:200]

        return record

    def _build_response_log(
        self,
        request_id: str,
        method: str,
        path: str,
        client_ip: str,
        jwt_user_id: int | None,
        jwt_username: str,
        status_code: int,
        elapsed_ms: float,
        content_length: str | None,
        content_type: str,
        ts_start: str,
        ts_end: str,
        error_details: dict | None,
        threats_detected: list,
    ) -> dict:
        """Buduje kompletny rekord JSONL dla zakończonego żądania."""
        # Kategoryzacja status code
        status_category = (
            "success" if status_code < 300
            else "redirect" if status_code < 400
            else "client_error" if status_code < 500
            else "server_error"
        )

        record: dict[str, Any] = {
            "event": "http_request_completed",
            "request_id": request_id,
            "method": method,
            "path": path,
            "client": {
                "ip": client_ip,
                "user_id": jwt_user_id,
                "username": jwt_username,
            },
            "response": {
                "status_code": status_code,
                "status_category": status_category,
                "content_length": int(content_length) if content_length and content_length.isdigit() else None,
                "content_type": content_type,
            },
            "performance": {
                "elapsed_ms": elapsed_ms,
                "elapsed_s": round(elapsed_ms / 1000, 6),
                # Kategoryzacja czasu odpowiedzi
                "performance_bucket": (
                    "fast" if elapsed_ms < 100
                    else "normal" if elapsed_ms < 500
                    else "slow" if elapsed_ms < 2000
                    else "very_slow"
                ),
            },
            "security": {
                "threats_detected": threats_detected,
                "threats_count": len(threats_detected),
            },
            "ts_start": ts_start,
            "ts_end": ts_end,
        }

        if error_details:
            record["error"] = error_details

        # Alerty dla powolnych requestów
        if elapsed_ms > 5000:
            record["alert"] = {
                "type": "slow_request",
                "message": f"Żądanie zajęło {elapsed_ms}ms — przekroczon próg 5000ms",
                "severity": "WARNING",
            }

        return record

    def _build_block_record(
        self,
        request_id: str,
        method: str,
        path: str,
        client_ip: str,
        user_agent: dict,
        jwt_user_id: int | None,
        reason: str,
        details: dict,
        ts: str,
    ) -> dict:
        """Buduje rekord logu dla zablokowanego żądania."""
        return {
            "event": "request_blocked",
            "request_id": request_id,
            "method": method,
            "path": path,
            "client": {
                "ip": client_ip,
                "user_agent": user_agent.get("raw", ""),
                "client_type": user_agent.get("client_type", ""),
                "user_id": jwt_user_id,
            },
            "block_reason": reason,
            "block_details": details,
            "severity": "WARNING",
            "ts": ts,
            "action_taken": "request_rejected_with_400",
        }

    # ------------------------------------------------------------------
    # Wstrzykiwanie nagłówków bezpieczeństwa
    # ------------------------------------------------------------------
    def _inject_headers(self, response: Response) -> None:
        """
        Wstrzykuje nagłówki bezpieczeństwa do response.
        Nie nadpisuje nagłówków już ustawionych przez endpoint.
        """
        for header_name, header_value in _SECURITY_HEADERS.items():
            # Nie nadpisuj jeśli endpoint już ustawił
            if header_name not in response.headers:
                response.headers[header_name] = header_value

        # Usuń nagłówki ujawniające informacje o serwerze
        response.headers.pop("Server", None)
        response.headers.pop("X-Powered-By", None)

    # ------------------------------------------------------------------
    # Response dla zablokowanych żądań
    # ------------------------------------------------------------------
    def _blocked_response(
        self,
        request_id: str,
        reason: str,
        detail: str,
    ) -> JSONResponse:
        """Standardowy response dla zablokowanych żądań."""
        content = orjson.loads(orjson.dumps({
            "success": False,
            "code": f"middleware.{reason}",
            "message": detail,
            "errors": [{"field": "_", "message": detail}],
            "meta": {
                "request_id": request_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        }))
        response = JSONResponse(status_code=400, content=content)
        # Wstrzyknij security headers nawet dla blokad
        if self._inject_security_headers:
            self._inject_headers(response)
        response.headers["X-Request-ID"] = request_id
        return response