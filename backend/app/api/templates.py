"""
api/templates.py
═══════════════════════════════════════════════════════════════════════════════
Router szablonów monitów — System Windykacja

Prefix: /api/v1/templates  (zarejestrowany w api/router.py)

5 endpointów:
  GET    /templates           — lista szablonów (paginacja + filtry)
  GET    /templates/{id}      — szczegóły szablonu (pełna treść Body)
  POST   /templates           — tworzenie nowego szablonu
  PUT    /templates/{id}      — aktualizacja szablonu (częściowa)
  DELETE /templates/{id}      — dezaktywacja szablonu (soft-delete)

Uprawnienia RBAC:
  templates.view_list     — GET /templates
  templates.view_details  — GET /templates/{id}
  templates.create        — POST /templates
  templates.edit          — PUT /templates/{id}
  templates.delete        — DELETE /templates/{id}

Walidacja wejść:
  - extra='forbid' na wszystkich schematach Pydantic
  - Sanityzacja NFC + strip + truncate w serwisie
  - Subject wymagany dla type=email, ignorowany dla sms/print
  - TemplateName globalnie unikalne (HTTP 409 przy duplikacie)
  - Dezaktywacja zablokowana gdy szablon używany przez pending/queued monity

Wzorce:
  - Każda mutacja → AuditLog (fire-and-forget w serwisie)
  - Inwalidacja cache Redis po każdej mutacji
  - Logi JSONL w serwisie (templates_YYYY-MM-DD.jsonl)
  - BaseResponse.ok() dla odpowiedzi sukcesu
  - HTTP kody: 200 list/update/delete, 201 create, 404 not found,
               409 conflict (duplicate), 422 validation, 423 in-use

Autor: System Windykacja
Wersja: 1.0.0
Data:   2026-03-11
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import orjson
from fastapi import APIRouter, HTTPException, Path, Query, Request, status
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.core.dependencies import (
    DB,
    ClientIP,
    CurrentUser,
    RedisClient,
    RequestID,
    require_permission,
)
from app.schemas.common import BaseResponse

# ─────────────────────────────────────────────────────────────────────────────
# Logger
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Router
# ─────────────────────────────────────────────────────────────────────────────

router = APIRouter()

# ─────────────────────────────────────────────────────────────────────────────
# Schematy wejściowe — Pydantic v2
# ─────────────────────────────────────────────────────────────────────────────

_ALLOWED_TYPES = {"email", "sms", "print"}


class TemplateCreateRequest(BaseModel):
    """Dane do tworzenia nowego szablonu."""
    model_config = ConfigDict(extra="forbid")

    template_name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Unikalna nazwa szablonu (max 100 znaków).",
    )
    template_type: str = Field(
        ...,
        description="Typ szablonu: email | sms | print.",
    )
    subject: Optional[str] = Field(
        default=None,
        max_length=200,
        description="Temat wiadomości — wymagany dla type=email.",
    )
    body: str = Field(
        ...,
        min_length=1,
        description="Treść szablonu Jinja2. Zmienne: {{ debtor_name }}, "
                    "{{ total_debt }}, {{ invoice_list }}, {{ due_date }}, "
                    "{{ company_name }}.",
    )

    @field_validator("template_type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        v = v.strip().lower()
        if v not in _ALLOWED_TYPES:
            raise ValueError(
                f"Nieprawidłowy typ '{v}'. Dozwolone: email, sms, print."
            )
        return v

    @field_validator("template_name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Nazwa szablonu nie może być pusta.")
        return v


class TemplateUpdateRequest(BaseModel):
    """Dane do aktualizacji szablonu — wszystkie pola opcjonalne."""
    model_config = ConfigDict(extra="forbid")

    template_name: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=100,
        description="Nowa nazwa szablonu.",
    )
    template_type: Optional[str] = Field(
        default=None,
        description="Nowy typ: email | sms | print.",
    )
    subject: Optional[str] = Field(
        default=None,
        max_length=200,
        description="Temat email. Wymagany gdy type=email.",
    )
    body: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Nowa treść Jinja2.",
    )
    is_active: Optional[bool] = Field(
        default=None,
        description="Aktywność szablonu. False = soft-delete.",
    )

    @field_validator("template_type")
    @classmethod
    def validate_type(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = v.strip().lower()
        if v not in _ALLOWED_TYPES:
            raise ValueError(
                f"Nieprawidłowy typ '{v}'. Dozwolone: email, sms, print."
            )
        return v

class TemplateDeleteConfirmRequest(BaseModel):
    """Token potwierdzający usunięcie szablonu."""
    model_config = ConfigDict(extra="forbid")

    confirm_token: str = Field(
        ...,
        min_length=10,
        description="Token JWT otrzymany z DELETE /templates/{id}/initiate.",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Mapowanie wyjątków serwisu → HTTP
# ─────────────────────────────────────────────────────────────────────────────

def _raise_from_template_error(exc: Exception) -> None:
    """
    Mapuje wyjątki template_service na HTTPException z odpowiednim kodem.
    Zawsze rzuca — nigdy nie zwraca None.
    """
    from app.services.template_service import (
        TemplateDuplicateError,
        TemplateDeleteTokenError,
        TemplateInUseError,
        TemplateNotFoundError,
        TemplateValidationError,
    )

    if isinstance(exc, TemplateNotFoundError):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "code": "templates.not_found",
                "message": str(exc),
                "errors": [{"field": "_", "message": str(exc)}],
            },
        )
    if isinstance(exc, TemplateValidationError):
        # Serwis zwraca JSON z listą błędów lub zwykły string
        try:
            errors = orjson.loads(str(exc))
        except Exception:
            errors = [{"field": "_", "message": str(exc)}]
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code":    "templates.validation_error",
                "message": "Błąd walidacji danych szablonu.",
                "errors":  errors,
            },
        )
    if isinstance(exc, TemplateDuplicateError):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code":    "templates.duplicate_name",
                "message": str(exc),
                "errors":  [{"field": "template_name", "message": str(exc)}],
            },
        )
    if isinstance(exc, TemplateInUseError):
        raise HTTPException(
            status_code=status.HTTP_423_LOCKED,
            detail={
                "code":    "templates.in_use",
                "message": str(exc),
                "errors":  [{"field": "_", "message": str(exc)}],
            },
        )
    if isinstance(exc, TemplateDeleteTokenError):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code":    "templates.invalid_token",
                "message": str(exc),
                "errors":  [{"field": "confirm_token", "message": str(exc)}],
            },
        )
    # Nieznany błąd — loguj i zwróć 500
    logger.exception(
        orjson.dumps({
            "event":     "template_service_unexpected_error",
            "error":     str(exc),
            "error_type": type(exc).__name__,
            "ts":        datetime.now(timezone.utc).isoformat(),
        }).decode()
    )
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail={
            "code":    "templates.internal_error",
            "message": "Wewnętrzny błąd serwera. Skontaktuj się z administratorem.",
            "errors":  [{"field": "_", "message": str(exc)}],
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 1: GET /templates  — lista szablonów
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "",
    summary="Lista szablonów monitów",
    description=(
        "Zwraca paginowaną listę szablonów. "
        "Można filtrować po typie (email/sms/print) i statusie aktywności. "
        "Lista nie zawiera treści Body — użyj GET /templates/{id} po pełne dane. "
        "Wyniki cachowane w Redis (TTL 60s). "
        "**Wymaga uprawnienia:** `templates.view_list`"
    ),
    response_description="Lista szablonów z paginacją",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("templates.view_list")],
)
async def list_templates(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request_id: RequestID,
    page: int = Query(default=1, ge=1, description="Numer strony."),
    limit: int = Query(default=20, ge=1, le=100, description="Elementów na stronę (max 100)."),
    template_type: Optional[str] = Query(
        default=None,
        description="Filtr po typie: email | sms | print.",
        pattern="^(email|sms|print)$",
    ),
    is_active: Optional[bool] = Query(
        default=None,
        description="Filtr po statusie aktywności. Brak = wszystkie.",
    ),
    search: Optional[str] = Query(
        default=None,
        max_length=100,
        description="Wyszukiwanie po nazwie szablonu (LIKE).",
    ),
    sort: Optional[str] = Query(
        default="-created_at",
        description="Sortowanie: name | -name | created_at | -created_at | type.",
        pattern="^(-?name|-?created_at|type)$",
    ),
):
    from app.services import template_service

    logger.debug(
        orjson.dumps({
            "event":         "api_templates_list",
            "page":          page,
            "limit":         limit,
            "template_type": template_type,
            "is_active":     is_active,
            "search":        bool(search),
            "sort":          sort,
            "requested_by":  current_user.id_user,
            "request_id":    request_id,
            "ts":            datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    try:
        result = await template_service.get_list(
            db=db,
            redis=redis,
            page=page,
            limit=limit,
            template_type=template_type,
            is_active=is_active,
            search=search,
            sort=sort,
        )
    except Exception as exc:
        _raise_from_template_error(exc)

    return BaseResponse.ok(
        data=result,
        app_code="templates.list",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 2: GET /templates/{template_id}  — szczegóły szablonu
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/{template_id:int}",
    summary="Szczegóły szablonu",
    description=(
        "Zwraca pełne dane szablonu włącznie z treścią Body (Jinja2). "
        "Wynik cachowany w Redis (TTL 300s). "
        "**Wymaga uprawnienia:** `templates.view_details`"
    ),
    response_description="Pełne dane szablonu",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("templates.view_details")],
)
async def get_template(
    template_id: int = Path(..., ge=1, description="ID szablonu."),
    current_user: CurrentUser = None,
    db: DB = None,
    redis: RedisClient = None,
    request_id: RequestID = None,
):
    from app.services import template_service

    logger.debug(
        orjson.dumps({
            "event":        "api_template_detail",
            "template_id":  template_id,
            "requested_by": current_user.id_user,
            "request_id":   request_id,
            "ts":           datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    try:
        data = await template_service.get_by_id(db=db, redis=redis, template_id=template_id)
    except Exception as exc:
        _raise_from_template_error(exc)

    return BaseResponse.ok(
        data=data,
        app_code="templates.detail",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 3: POST /templates  — tworzenie szablonu
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "",
    summary="Tworzenie szablonu monitów",
    description=(
        "Tworzy nowy szablon wiadomości. "
        "Dla type=email pole subject jest wymagane. "
        "Dla type=sms i print — subject ignorowany (DB constraint). "
        "Treść Body obsługuje zmienne Jinja2: "
        "{{ debtor_name }}, {{ total_debt }}, {{ invoice_list }}, "
        "{{ due_date }}, {{ company_name }}. "
        "**Wymaga uprawnienia:** `templates.create`"
    ),
    response_description="Nowo utworzony szablon",
    status_code=status.HTTP_201_CREATED,
    dependencies=[require_permission("templates.create")],
)
async def create_template(
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    # Parsowanie body
    try:
        body_raw = await request.json()
        payload = TemplateCreateRequest(**body_raw)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code":    "templates.validation_error",
                "message": "Nieprawidłowy format danych wejściowych.",
                "errors":  [{"field": "_", "message": str(exc)}],
            },
        )

    logger.info(
        orjson.dumps({
            "event":         "api_template_create",
            "template_name": payload.template_name,
            "template_type": payload.template_type,
            "has_subject":   bool(payload.subject),
            "body_length":   len(payload.body),
            "created_by":    current_user.id_user,
            "ip_address":    client_ip,
            "request_id":    request_id,
            "ts":            datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    from app.services import template_service

    try:
        result = await template_service.create(
            db=db,
            redis=redis,
            raw_name=payload.template_name,
            raw_type=payload.template_type,
            raw_body=payload.body,
            raw_subject=payload.subject,
            created_by_user_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_template_error(exc)

    return BaseResponse.ok(
        data=result,
        app_code="templates.created",
        code=201,
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 4: PUT /templates/{template_id}  — aktualizacja szablonu
# ─────────────────────────────────────────────────────────────────────────────

@router.put(
    "/{template_id:int}",
    summary="Aktualizacja szablonu",
    description=(
        "Aktualizuje istniejący szablon. Wszystkie pola opcjonalne — "
        "podaj tylko te, które chcesz zmienić. "
        "Przy zmianie type=email → sms/print: subject automatycznie ustawiony na NULL. "
        "Dezaktywacja (is_active=false) zablokowana gdy szablon ma monity "
        "w statusie pending/queued. "
        "Po aktualizacji: inwalidacja cache Redis. "
        "**Wymaga uprawnienia:** `templates.edit`"
    ),
    response_description="Zaktualizowany szablon",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("templates.edit")],
)
async def update_template(
    request: Request,
    template_id: int = Path(..., ge=1, description="ID szablonu."),
    current_user: CurrentUser = None,
    db: DB = None,
    redis: RedisClient = None,
    client_ip: ClientIP = None,
    request_id: RequestID = None,
):
    try:
        body_raw = await request.json()
        payload = TemplateUpdateRequest(**body_raw)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code":    "templates.validation_error",
                "message": "Nieprawidłowy format danych wejściowych.",
                "errors":  [{"field": "_", "message": str(exc)}],
            },
        )

    logger.info(
        orjson.dumps({
            "event":        "api_template_update",
            "template_id":  template_id,
            "fields_sent":  [k for k, v in payload.model_dump().items() if v is not None],
            "updated_by":   current_user.id_user,
            "ip_address":   client_ip,
            "request_id":   request_id,
            "ts":           datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    from app.services import template_service

    try:
        result = await template_service.update(
            db=db,
            redis=redis,
            template_id=template_id,
            raw_name=payload.template_name,
            raw_type=payload.template_type,
            raw_body=payload.body,
            raw_subject=payload.subject,
            is_active=payload.is_active,
            updated_by_user_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_template_error(exc)

    return BaseResponse.ok(
        data=result,
        app_code="templates.updated",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 5: DELETE /templates/{template_id}  — dezaktywacja (soft-delete)
# ─────────────────────────────────────────────────────────────────────────────

@router.delete(
    "/{template_id:int}/initiate",
    summary="Inicjacja usunięcia szablonu — krok 1",
    description=(
        "Krok 1 z 2 — inicjuje dwuetapowe usunięcie szablonu. "
        "Sprawdza czy szablon nie jest używany przez aktywne monity. "
        "Zwraca jednorazowy token JWT ważny przez czas z `delete_token.ttl_seconds` (SystemConfig). "
        "Token należy przekazać do DELETE /templates/{id}/confirm. "
        "**Wymaga uprawnienia:** `templates.delete`"
    ),
    response_description="Token potwierdzający usunięcie",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[require_permission("templates.delete")],
)
async def initiate_delete_template(
    template_id: int = Path(..., ge=1, description="ID szablonu."),
    current_user: CurrentUser = None,
    db: DB = None,
    redis: RedisClient = None,
    client_ip: ClientIP = None,
    request_id: RequestID = None,
):
    logger.info(
        orjson.dumps({
            "event":        "api_template_delete_initiate",
            "template_id":  template_id,
            "initiated_by": current_user.id_user,
            "ip_address":   client_ip,
            "request_id":   request_id,
            "ts":           datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    from app.services import template_service

    try:
        result = await template_service.initiate_delete(
            db=db,
            redis=redis,
            template_id=template_id,
            initiated_by_user_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_template_error(exc)

    return BaseResponse.ok(
        data={
            "confirm_token":  result.token,
            "expires_in":     result.expires_in,
            "template_id":    result.template_id,
            "template_name":  result.template_name,
            "template_type":  result.template_type,
            "warning": (
                "Dezaktywacja szablonu jest nieodwracalna przez API. "
                "Szablon zostanie zachowany w bazie dla historii monitów."
            ),
        },
        app_code="templates.delete_initiated",
        code=202,
    )


@router.delete(
    "/{template_id:int}/confirm",
    summary="Potwierdzenie usunięcia szablonu — krok 2",
    description=(
        "Krok 2 z 2 — potwierdza i wykonuje soft-delete szablonu. "
        "Wymaga tokenu JWT z DELETE /templates/{id}/initiate. "
        "Token jednorazowy — po użyciu traci ważność natychmiast. "
        "**Wymaga uprawnienia:** `templates.delete`"
    ),
    response_description="Potwierdzenie dezaktywacji",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("templates.delete")],
)
async def confirm_delete_template(
    request: Request,
    template_id: int = Path(..., ge=1, description="ID szablonu."),
    current_user: CurrentUser = None,
    db: DB = None,
    redis: RedisClient = None,
    client_ip: ClientIP = None,
    request_id: RequestID = None,
):
    try:
        body_raw = await request.json()
        payload = TemplateDeleteConfirmRequest(**body_raw)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code":    "templates.validation_error",
                "message": "Nieprawidłowy format danych.",
                "errors":  [{"field": "confirm_token", "message": str(exc)}],
            },
        )

    logger.info(
        orjson.dumps({
            "event":        "api_template_delete_confirm",
            "template_id":  template_id,
            "confirmed_by": current_user.id_user,
            "ip_address":   client_ip,
            "request_id":   request_id,
            "ts":           datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    from app.services import template_service

    try:
        result = await template_service.confirm_delete(
            db=db,
            redis=redis,
            template_id=template_id,
            confirm_token=payload.confirm_token,
            requesting_user_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_template_error(exc)

    return BaseResponse.ok(
        data={
            "message":     f"Szablon ID={template_id} został trwale dezaktywowany.",
            "template_id": template_id,
            "is_active":   result.get("is_active", False),
        },
        app_code="templates.deleted",
    )