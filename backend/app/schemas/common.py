# =============================================================================
# backend/app/schemas/common.py
# =============================================================================
# Wspólne typy pomocnicze Pydantic v2 — używane w wielu miejscach systemu.
#
# ZAWARTOŚĆ:
#   DeleteTokenResponse   — odpowiedź 202 przy inicjacji dwuetapowego DELETE
#   HealthCheckComponent  — status pojedynczego komponentu (DB, Redis)
#   HealthResponse        — odpowiedź GET /system/health
#   MessageData           — dane dla prostych odpowiedzi tekstowych
#   MessageResponse       — BaseResponse[MessageData]
#   PaginationMeta        — metadane paginacji (page, per_page, total, pages)
#   SortOrder             — enum ASC/DESC do sortowania
#   ComponentStatus       — enum OK/DEGRADED/DOWN dla health checku
#   ErrorDetail           — pojedynczy błąd walidacji (field + message)
#
# ZASADY (wg USTALENIA_PROJEKTU §19 + Konfiguracja):
#   - Błędy zawsze jako tablica (nigdy string)
#   - Kod response zawsze jeden (code: int)
#   - extra='forbid' na wszystkich schemach INPUT
#   - Sortowanie domyślne: od najnowszych (DESC)
#   - Komunikaty błędów: po polsku
#
# Ten moduł NIE importuje z innych schemas/* — zerowe ryzyko circular import.
# Inne schemas importują z tego modułu.
#
# Wersja: 1.0.0 | Data: 2026-02-17 | Faza: 1
# =============================================================================

from __future__ import annotations

import logging
from datetime import datetime
from enum import Enum
from typing import Any, Generic, TypeVar

import orjson
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

logger = logging.getLogger(__name__)

# TypeVar dla generycznych schematów odpowiedzi
DataT = TypeVar("DataT")


# =============================================================================
# ENUMS — wartości słownikowe używane w wielu miejscach
# =============================================================================

class SortOrder(str, Enum):
    """Kierunek sortowania — używany w parametrach filtrowania list."""

    ASC = "asc"
    DESC = "desc"

    def to_sql(self) -> str:
        """Zwraca SQL keyword dla ORDER BY."""
        return "ASC" if self == SortOrder.ASC else "DESC"


class ComponentStatus(str, Enum):
    """
    Status komponentu infrastrukturalnego w health checku.

    OK       — komponent działa poprawnie
    DEGRADED — komponent działa z problemami (np. wolne zapytania, wysokie opóźnienie)
    DOWN     — komponent niedostępny
    """

    OK = "ok"
    DEGRADED = "degraded"
    DOWN = "down"


# =============================================================================
# BLOKI BAZOWE — używane jako building blocks
# =============================================================================

class ErrorDetail(BaseModel):
    """
    Pojedynczy błąd walidacji lub biznesowy.

    Używany w BaseResponse.errors — ZAWSZE tablica, nigdy string.
    Format zgodny z RFC 7807 (Problem Details for HTTP APIs).

    Przykład:
        {"field": "email", "message": "Nieprawidłowy format adresu email"}
        {"field": "__root__", "message": "Użytkownik o tej nazwie już istnieje"}
    """

    model_config = ConfigDict(
        frozen=True,  # ErrorDetail jest immutable — tworzony raz, nie modyfikowany
        populate_by_name=True,
    )

    field: str = Field(
        description=(
            "Nazwa pola którego dotyczy błąd. "
            "'__root__' dla błędów globalnych (nie powiązanych z polem)."
        ),
        examples=["email", "username", "__root__"],
    )
    message: str = Field(
        description="Opis błędu po polsku — czytelny dla użytkownika końcowego.",
        examples=["Pole jest wymagane", "Wartość musi być większa niż 0"],
        min_length=1,
        max_length=500,
    )

    def __str__(self) -> str:
        return f"{self.field}: {self.message}"


class BaseResponse(BaseModel, Generic[DataT]):
    """
    Ujednolicony format odpowiedzi API — WSZYSTKIE endpointy używają tego formatu.

    Format wg USTALENIA_PROJEKTU §12 + Konfiguracja:
        {"code": 200, "errors": [], "data": {...}}
        {"code": 422, "errors": [{"field": "email", "message": "..."}], "data": null}

    Zasady:
        - code: zawsze jeden kod HTTP (nie tablica kodów)
        - errors: zawsze tablica (nawet pusta) — nigdy string, nigdy null
        - data: null gdy błąd, obiekt/lista gdy sukces

    Generyczny — DataT określa typ pola data:
        BaseResponse[UserResponse]          → data: UserResponse | None
        BaseResponse[list[DebtorListItem]]  → data: list[DebtorListItem] | None
        BaseResponse[PaginatedData[X]]      → data: PaginatedData[X] | None
    """

    model_config = ConfigDict(
        populate_by_name=True,
        # Używamy orjson przez json_encoders dla szybkości
        # orjson jest ~3x szybszy niż stdlib json
    )

    code: int = Field(
        description="Kod HTTP odpowiedzi — jeden kod, zawsze.",
        examples=[200, 201, 400, 401, 403, 404, 422, 500],
        ge=100,
        le=599,
    )
    app_code: str | None = Field(
        default=None,
        description="Aplikacyjny kod odpowiedzi (np. users.created, validation.error)",
        examples=["users.created", "validation.error"],
    )
    errors: list[ErrorDetail] = Field(
        default_factory=list,
        description=(
            "Lista błędów — ZAWSZE tablica (wg Konfiguracja: 'Błędy zwracaj zawsze jako tablicę'). "
            "Pusta lista [] przy sukcesie. Nigdy null."
        ),
    )
    data: DataT | None = Field(
        default=None,
        description="Dane odpowiedzi. None przy błędzie.",
    )

    @classmethod
    def ok(cls, data: DataT, code: int = 200, app_code: str | None = None) -> "BaseResponse[DataT]":
        """
        Factory method dla sukcesów.
        Użycie: return BaseResponse.ok(user_data)
        """
        return cls(code=code, errors=[], data=data, app_code=app_code)

    @classmethod
    def error(
        cls,
        message: str,
        field: str = "__root__",
        code: int = 400,
        app_code: str | None = None,
    ) -> "BaseResponse[None]":
        """
        Factory method dla pojedynczego błędu.
        Użycie: return BaseResponse.error("Użytkownik nie istnieje", code=404)
        """
        return cls(
            code=code,
            errors=[ErrorDetail(field=field, message=message)],
            data=None,
            app_code=app_code,
        )

    @classmethod
    def errors_list(
        cls,
        errors: list[ErrorDetail],
        code: int = 422,
    ) -> "BaseResponse[None]":
        """
        Factory method dla wielu błędów walidacji.
        Użycie: return BaseResponse.errors_list([ErrorDetail(...), ...])
        """
        return cls(code=code, errors=errors, data=None)

    def to_json_bytes(self) -> bytes:
        """
        Serializuje odpowiedź do JSON bytes używając orjson (szybszy niż stdlib).
        Używany przez custom JSONResponse w main.py.
        """
        return orjson.dumps(self.model_dump(mode="json"))


class PaginatedData(BaseModel, Generic[DataT]):
    """
    Opakowanie dla paginowanych list — zawsze używany z BaseResponse.

    Przykład użycia:
        BaseResponse[PaginatedData[DebtorListItem]]

    Limity paginacji (wg PLAN_PRAC.md §1.1):
        - max 200 rekordów na stronę
        - domyślnie 50 rekordów na stronę
    """

    model_config = ConfigDict(populate_by_name=True)

    items: list[DataT] = Field(
        description="Lista elementów na bieżącej stronie.",
    )
    total: int = Field(
        description="Łączna liczba elementów (wszystkie strony).",
        ge=0,
    )
    page: int = Field(
        description="Bieżąca strona (1-based).",
        ge=1,
    )
    per_page: int = Field(
        description="Liczba elementów na stronie.",
        ge=1,
        le=200,
    )
    pages: int = Field(
        description="Łączna liczba stron.",
        ge=0,
    )
    has_next: bool = Field(
        description="Czy istnieje następna strona.",
    )
    has_prev: bool = Field(
        description="Czy istnieje poprzednia strona.",
    )

    @classmethod
    def create(
        cls,
        items: list[DataT],
        total: int,
        page: int,
        per_page: int,
    ) -> "PaginatedData[DataT]":
        """
        Factory method — oblicza pages, has_next, has_prev automatycznie.

        Użycie:
            return PaginatedData.create(
                items=debtors,
                total=total_count,
                page=filter.page,
                per_page=filter.per_page,
            )
        """
        import math

        pages = math.ceil(total / per_page) if per_page > 0 else 0
        return cls(
            items=items,
            total=total,
            page=page,
            per_page=per_page,
            pages=pages,
            has_next=page < pages,
            has_prev=page > 1,
        )


# =============================================================================
# DELETE TOKEN — dwuetapowy DELETE (USTALENIA §9)
# =============================================================================

class DeleteTokenResponse(BaseModel):
    """
    Odpowiedź 202 Accepted przy inicjacji operacji DELETE (krok 1 z 2).

    Mechanizm dwuetapowy (wg USTALENIA_PROJEKTU §9):
        KROK 1: DELETE /api/v1/users/{id}
                → 202 Accepted + DeleteTokenResponse
        KROK 2: DELETE /api/v1/users/{id}/confirm
                + {"confirm_token": "eyJ..."}
                → 200 OK + zapis do archiwum

    Token JWT (TTL z SystemConfig 'delete_token.ttl_seconds', domyślnie 60s):
        {"scope": "confirm_delete", "entity_type": "User",
         "entity_id": 42, "requested_by": 1, "exp": ...}

    Token jednorazowy — po użyciu trafia do Redis blacklist.
    """

    model_config = ConfigDict(populate_by_name=True)

    confirm_token: str = Field(
        description=(
            "Podpisany JWT token potwierdzający operację DELETE. "
            "Użyj w kroku 2 jako {'confirm_token': '...'}. "
            "Token jednorazowy — po użyciu unieważniony."
        ),
    )
    expires_in: int = Field(
        description=(
            "Czas ważności tokenu w sekundach (z SystemConfig 'delete_token.ttl_seconds'). "
            "Domyślnie 60 sekund."
        ),
        ge=1,
        examples=[60],
    )
    action: str = Field(
        description=(
            "Opis operacji do potwierdzenia — czytelny dla użytkownika. "
            "Przykład: 'Usunięcie użytkownika Jan Kowalski (ID: 42)'"
        ),
        examples=["Usunięcie użytkownika Jan Kowalski (ID: 42)"],
    )
    summary: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Dodatkowe informacje o konsekwencjach operacji. "
            "Przykład: {'entity': 'User', 'id': 42, "
            "'warning': 'Użytkownik ma 15 powiązanych rekordów w MonitHistory'}"
        ),
    )

    @model_validator(mode="after")
    def validate_confirm_token_format(self) -> "DeleteTokenResponse":
        """Podstawowa weryfikacja że token ma format JWT (3 segmenty base64 oddzielone '.')."""
        parts = self.confirm_token.split(".")
        if len(parts) != 3:
            raise ValueError(
                "confirm_token musi być poprawnym tokenem JWT (format: header.payload.signature)"
            )
        return self


# =============================================================================
# HEALTH CHECK — GET /api/v1/system/health
# =============================================================================

class HealthCheckComponent(BaseModel):
    """Status pojedynczego komponentu infrastrukturalnego."""

    model_config = ConfigDict(populate_by_name=True)

    status: ComponentStatus = Field(
        description="Status komponentu: ok / degraded / down",
    )
    latency_ms: float | None = Field(
        default=None,
        description="Czas odpowiedzi w milisekundach (None jeśli komponent down).",
        ge=0,
        examples=[1.5, 12.3],
    )
    detail: str | None = Field(
        default=None,
        description=(
            "Dodatkowe informacje. "
            "Przy status=ok: None lub opis (np. 'Pool: 5/10 connections'). "
            "Przy status=degraded/down: opis problemu po polsku."
        ),
        max_length=500,
    )


class HealthResponse(BaseModel):
    """
    Odpowiedź GET /api/v1/system/health (perm: system.view_health).

    Zwraca stan wszystkich komponentów systemu:
        - db:    połączenie z MSSQL (aioodbc)
        - redis: połączenie z Redis
        - wapro: dostępność widoków WAPRO przez pyodbc

    Używany przez:
        - Docker HEALTHCHECK
        - Monitoring (Grafana, Zabbix itp.)
        - Operatora przy diagnozowaniu problemów
    """

    model_config = ConfigDict(populate_by_name=True)

    status: ComponentStatus = Field(
        description=(
            "Ogólny status systemu — najgorszy ze statusów komponentów. "
            "ok = wszystkie OK. degraded = co najmniej jeden DEGRADED. down = co najmniej jeden DOWN."
        ),
    )
    version: str = Field(
        description="Wersja aplikacji (z __version__ lub settings).",
        examples=["1.0.0"],
    )
    uptime_seconds: float = Field(
        description="Czas działania aplikacji w sekundach od ostatniego uruchomienia.",
        ge=0,
        examples=[3600.5],
    )
    checked_at: datetime = Field(
        description="Czas wykonania health checku (UTC).",
    )
    components: dict[str, HealthCheckComponent] = Field(
        description=(
            "Status poszczególnych komponentów. "
            "Klucze: 'db', 'redis', 'wapro'. "
            "Możliwe rozszerzenie o dodatkowe klucze bez zmiany schematu."
        ),
        examples=[{
            "db": {"status": "ok", "latency_ms": 1.5},
            "redis": {"status": "ok", "latency_ms": 0.3},
            "wapro": {"status": "ok", "latency_ms": 5.2},
        }],
    )
    schema_integrity: str | None = Field(
        default=None,
        description=(
            "Status weryfikacji checksumów schematu DB. "
            "'OK' — wszystkie obiekty zgodne. "
            "Opis rozbieżności — jeśli wykryto zmiany. "
            "None — weryfikacja nie była uruchamiana."
        ),
    )

    @property
    def is_healthy(self) -> bool:
        """Czy system jest w pełni sprawny (wszystkie komponenty OK)."""
        return self.status == ComponentStatus.OK


# =============================================================================
# MESSAGE — proste odpowiedzi tekstowe
# =============================================================================

class MessageData(BaseModel):
    """
    Dane dla prostych odpowiedzi tekstowych.
    Używany przez MessageResponse = BaseResponse[MessageData].

    Przykłady użycia:
        POST /auth/logout → {"code": 200, "errors": [], "data": {"message": "Wylogowano pomyślnie"}}
        POST /users/{id}/lock → {"code": 200, "errors": [], "data": {"message": "Konto zablokowane"}}
    """

    model_config = ConfigDict(populate_by_name=True)

    message: str = Field(
        description="Komunikat tekstowy po polsku.",
        min_length=1,
        max_length=500,
        examples=["Operacja wykonana pomyślnie", "Wylogowano pomyślnie"],
    )
    details: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Opcjonalne dodatkowe dane strukturalne. "
            "Używane gdy sama wiadomość to za mało. "
            "Przykład: {'archived_at': '2026-02-17T14:30:00', 'backup_path': '...'}"
        ),
    )


# Alias dla wygody — używany w endpointach zamiast BaseResponse[MessageData]
MessageResponse = BaseResponse[MessageData]


# =============================================================================
# PARAMETRY PAGINACJI — używane jako query parameters w endpointach list
# =============================================================================

class PaginationParams(BaseModel):
    """
    Wspólne parametry paginacji dla endpointów list.
    Używany jako Depends() w FastAPI endpointach.

    Domyślna kolejność: DESC (od najnowszych — wg TABELE_REFERENCJA zasady ogólne).
    Limit: max 200 rekordów/stronę (wg PLAN_PRAC.md §1.1).

    Użycie w endpoincie:
        @router.get("/debtors")
        async def list_debtors(pagination: PaginationParams = Depends()):
            ...
    """

    model_config = ConfigDict(
        extra="forbid",  # Blokuj nieznane query params
        populate_by_name=True,
    )

    page: int = Field(
        default=1,
        ge=1,
        description="Numer strony (1-based). Minimum 1.",
        examples=[1, 2, 5],
    )
    per_page: int = Field(
        default=50,
        ge=1,
        le=200,
        description=(
            "Liczba rekordów na stronę. "
            "Minimum 1, maksimum 200 (wg PLAN_PRAC.md §1.1). "
            "Domyślnie 50."
        ),
        examples=[20, 50, 100],
    )
    sort_order: SortOrder = Field(
        default=SortOrder.DESC,
        description=(
            "Kierunek sortowania. "
            "desc = od najnowszych (domyślnie, wg TABELE_REFERENCJA). "
            "asc = od najstarszych."
        ),
    )

    @property
    def offset(self) -> int:
        """Oblicza SQL OFFSET dla bieżącej strony."""
        return (self.page - 1) * self.per_page

    @field_validator("page", "per_page", mode="before")
    @classmethod
    def coerce_to_int(cls, v: Any) -> int:
        """
        Konwersja query param string → int.
        FastAPI przekazuje query params jako stringi — Pydantic v2 wymaga jawnej konwersji.
        """
        try:
            return int(v)
        except (TypeError, ValueError):
            raise ValueError("Wartość musi być liczbą całkowitą")


# =============================================================================
# CONFIRM TOKEN REQUEST — krok 2 dwuetapowego DELETE
# =============================================================================

class ConfirmTokenRequest(BaseModel):
    """
    Ciało żądania kroku 2 operacji DELETE.

    Użycie:
        DELETE /api/v1/users/{id}/confirm
        Body: {"confirm_token": "eyJ..."}
    """

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
    )

    confirm_token: str = Field(
        description=(
            "Token JWT otrzymany w kroku 1 (DeleteTokenResponse.confirm_token). "
            "TTL: z SystemConfig 'delete_token.ttl_seconds' (domyślnie 60s). "
            "Jednorazowy — po użyciu unieważniony w Redis blacklist."
        ),
        min_length=10,
    )

    @field_validator("confirm_token")
    @classmethod
    def validate_jwt_format(cls, v: str) -> str:
        """Podstawowa weryfikacja formatu JWT przed dalszą walidacją w security.py."""
        parts = v.split(".")
        if len(parts) != 3:
            raise ValueError(
                "Nieprawidłowy format tokenu — oczekiwano tokenu JWT"
            )
        # Sprawdź czy żaden segment nie jest pusty
        if any(len(p) == 0 for p in parts):
            raise ValueError("Token JWT zawiera pusty segment")
        return v