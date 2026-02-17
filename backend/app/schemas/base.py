"""
Bazowe schematy odpowiedzi API.

Każdy endpoint w systemie zwraca dane przez jeden z tych modeli.
Gwarantuje to spójność API i ułatwia obsługę błędów po stronie frontendu.

Format sukcesu:   { "code": 200, "errors": [], "data": { ... } }
Format błędu:     { "code": 422, "errors": [{"field": "email", "message": "..."}], "data": null }
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Generic, List, Optional, TypeVar

from pydantic import BaseModel, ConfigDict, Field

# Logger dla tego modułu
logger = logging.getLogger(__name__)

# Generyczny typ danych payload
DataT = TypeVar("DataT")


# ---------------------------------------------------------------------------
# Model błędu walidacji — jeden błąd na pole
# ---------------------------------------------------------------------------

class FieldError(BaseModel):
    """Pojedynczy błąd walidacji pola."""

    model_config = ConfigDict(extra="forbid")

    field: str = Field(
        ...,
        description="Nazwa pola, którego dotyczy błąd. 'non_field' dla błędów ogólnych.",
        min_length=1,
        max_length=100,
        examples=["email", "password", "non_field"],
    )
    message: str = Field(
        ...,
        description="Komunikat błędu czytelny dla użytkownika — NIE ujawnia szczegółów technicznych.",
        min_length=1,
        max_length=500,
    )
    # Opcjonalny kod błędu — do obsługi po stronie frontendu
    code: Optional[str] = Field(
        default=None,
        description="Opcjonalny kod maszynowy błędu (np. 'FIELD_REQUIRED', 'INVALID_FORMAT').",
        max_length=50,
        examples=["FIELD_REQUIRED", "INVALID_FORMAT", "VALUE_TOO_SHORT"],
    )


# ---------------------------------------------------------------------------
# Bazowy model odpowiedzi — opakowuje każdą odpowiedź API
# ---------------------------------------------------------------------------

class BaseResponse(BaseModel, Generic[DataT]):
    """
    Bazowy model odpowiedzi API.

    Każdy endpoint MUSI zwracać dane przez ten model.
    Gwarantuje spójność i ułatwia debugowanie.

    Przykład użycia w endpointach:
        return BaseResponse(code=200, data={"token": "eyJ..."})
        return BaseResponse(code=422, errors=[FieldError(field="email", message="Nieprawidłowy email")])
    """

    model_config = ConfigDict(
        # Pozwalamy na extra w odpowiedziach (to nie jest input schema)
        extra="ignore",
        # Serializacja datetime do ISO 8601
        json_encoders={datetime: lambda v: v.isoformat()},
        populate_by_name=True,
    )

    # HTTP status code — powielamy w body dla wygody frontendu
    code: int = Field(
        ...,
        description="HTTP status code odpowiedzi.",
        ge=100,
        le=599,
        examples=[200, 201, 400, 401, 403, 404, 409, 422, 429, 500],
    )

    # Lista błędów walidacji — pusta przy sukcesie
    errors: List[FieldError] = Field(
        default_factory=list,
        description="Lista błędów walidacji. Pusta lista przy sukcesie.",
    )

    # Właściwe dane odpowiedzi — None przy błędzie
    data: Optional[DataT] = Field(
        default=None,
        description="Payload odpowiedzi. null przy błędzie.",
    )

    # Timestamp odpowiedzi — do debugowania i korelacji logów
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="UTC timestamp wygenerowania odpowiedzi.",
    )

    # Unikalny identyfikator żądania — do korelacji w logach
    # W docelowej implementacji nadpisywany przez middleware z X-Request-ID
    request_id: Optional[str] = Field(
        default=None,
        description="Identyfikator żądania do korelacji z logami serwera.",
        max_length=64,
    )

    # ---------------------------------------------------------------------------
    # Factory methods — czytelne tworzenie odpowiedzi
    # ---------------------------------------------------------------------------

    @classmethod
    def ok(
        cls,
        data: DataT,
        *,
        request_id: Optional[str] = None,
    ) -> "BaseResponse[DataT]":
        """Odpowiedź sukcesu 200 OK."""
        return cls(code=200, data=data, request_id=request_id)

    @classmethod
    def created(
        cls,
        data: DataT,
        *,
        request_id: Optional[str] = None,
    ) -> "BaseResponse[DataT]":
        """Odpowiedź sukcesu 201 Created."""
        return cls(code=201, data=data, request_id=request_id)

    @classmethod
    def accepted(
        cls,
        data: DataT,
        *,
        request_id: Optional[str] = None,
    ) -> "BaseResponse[DataT]":
        """Odpowiedź sukcesu 202 Accepted — np. inicjacja DELETE."""
        return cls(code=202, data=data, request_id=request_id)

    @classmethod
    def error(
        cls,
        code: int,
        errors: List[FieldError],
        *,
        request_id: Optional[str] = None,
    ) -> "BaseResponse[None]":
        """Odpowiedź błędu z listą field errors."""
        return cls(code=code, errors=errors, data=None, request_id=request_id)

    @classmethod
    def simple_error(
        cls,
        code: int,
        message: str,
        *,
        field: str = "non_field",
        error_code: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> "BaseResponse[None]":
        """Szybkie tworzenie odpowiedzi z jednym błędem ogólnym."""
        return cls(
            code=code,
            errors=[FieldError(field=field, message=message, code=error_code)],
            data=None,
            request_id=request_id,
        )

    # ---------------------------------------------------------------------------
    # Właściwości pomocnicze
    # ---------------------------------------------------------------------------

    @property
    def is_success(self) -> bool:
        """Czy odpowiedź reprezentuje sukces (2xx)."""
        return 200 <= self.code < 300

    @property
    def has_errors(self) -> bool:
        """Czy odpowiedź zawiera błędy."""
        return len(self.errors) > 0


# ---------------------------------------------------------------------------
# Paginowana odpowiedź — dla endpointów listujących zasoby
# ---------------------------------------------------------------------------

class PaginationMeta(BaseModel):
    """Metadane paginacji dołączane do każdej odpowiedzi listującej."""

    model_config = ConfigDict(extra="forbid")

    # Bieżąca strona (1-indexed)
    page: int = Field(
        ...,
        description="Aktualna strona (zaczyna się od 1).",
        ge=1,
    )

    # Liczba elementów na stronie
    limit: int = Field(
        ...,
        description="Maksymalna liczba elementów na stronie.",
        ge=1,
        le=500,
    )

    # Łączna liczba elementów w całym zbiorze
    total: int = Field(
        ...,
        description="Łączna liczba elementów spełniających kryteria filtru.",
        ge=0,
    )

    # Łączna liczba stron
    pages: int = Field(
        ...,
        description="Łączna liczba stron.",
        ge=0,
    )

    # Czy istnieje następna strona
    has_next: bool = Field(
        ...,
        description="Czy istnieje następna strona.",
    )

    # Czy istnieje poprzednia strona
    has_prev: bool = Field(
        ...,
        description="Czy istnieje poprzednia strona.",
    )

    @classmethod
    def build(cls, *, page: int, limit: int, total: int) -> "PaginationMeta":
        """
        Oblicza metadane paginacji na podstawie parametrów.

        Przykład:
            meta = PaginationMeta.build(page=2, limit=20, total=95)
            # pages=5, has_next=True, has_prev=True
        """
        if limit <= 0:
            raise ValueError("limit musi być większy od 0")

        pages = max(1, -(-total // limit))  # Ceiling division
        return cls(
            page=page,
            limit=limit,
            total=total,
            pages=pages,
            has_next=page < pages,
            has_prev=page > 1,
        )


class PaginatedData(BaseModel, Generic[DataT]):
    """Dane paginowane — lista elementów + metadane."""

    model_config = ConfigDict(extra="ignore")

    items: List[DataT] = Field(
        default_factory=list,
        description="Lista elementów na bieżącej stronie.",
    )
    pagination: PaginationMeta = Field(
        ...,
        description="Metadane paginacji.",
    )


# Alias dla wygody — paginowana odpowiedź API
PaginatedResponse = BaseResponse[PaginatedData[DataT]]


# ---------------------------------------------------------------------------
# Schema parametrów paginacji — do reużycia w endpointach (query params)
# ---------------------------------------------------------------------------

class PaginationParams(BaseModel):
    """
    Standardowe parametry paginacji przekazywane jako query params.

    Użycie w endpointach FastAPI:
        @router.get("/users")
        async def list_users(params: PaginationParams = Depends()):
            ...
    """

    model_config = ConfigDict(extra="forbid")

    page: int = Field(
        default=1,
        ge=1,
        le=10_000,
        description="Numer strony (zaczyna się od 1).",
    )
    limit: int = Field(
        default=20,
        ge=1,
        le=500,
        description="Liczba elementów na stronie. Maksymalnie 500.",
    )

    @property
    def offset(self) -> int:
        """SQL OFFSET dla bieżącej strony."""
        return (self.page - 1) * self.limit


# ---------------------------------------------------------------------------
# Pomocnicza schema dla odpowiedzi potwierdzenia DELETE (dwuetapowe)
# ---------------------------------------------------------------------------

class DeleteConfirmData(BaseModel):
    """Payload odpowiedzi 202 przy inicjacji DELETE."""

    model_config = ConfigDict(extra="forbid")

    confirm_token: str = Field(
        ...,
        description="JWT token potwierdzający operację DELETE. Jednorazowy, TTL: 60s.",
    )
    expires_in: int = Field(
        ...,
        description="Czas ważności tokenu w sekundach.",
        ge=1,
    )
    summary: dict = Field(
        ...,
        description="Podsumowanie operacji — co zostanie usunięte, ostrzeżenia.",
    )


class DeleteResultData(BaseModel):
    """Payload odpowiedzi 200 po potwierdzeniu DELETE."""

    model_config = ConfigDict(extra="forbid")

    archived_at: datetime = Field(
        ...,
        description="Timestamp soft-delete (archiwizacji).",
    )
    entity_type: str = Field(
        ...,
        description="Typ usuniętej encji.",
    )
    entity_id: int = Field(
        ...,
        description="ID usuniętej encji.",
    )