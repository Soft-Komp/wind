"""
Router systemowy — System Windykacja.

8 endpointów:
  GET  /system/health                   — health check DB + Redis + WAPRO
  GET  /system/config                   — lista kluczy konfiguracji
  PUT  /system/config/{key}             — zmiana wartości klucza
  GET  /system/cors                     — aktualna lista originów CORS
  PUT  /system/cors                     — aktualizacja CORS → inwalidacja Redis
  GET  /system/schema-integrity         — wynik ostatniej weryfikacji checksumów
  POST /system/schema-integrity/check   — wymuś ponowną weryfikację
  GET  /system/audit-log                — przeglądarka audit logu (paginacja, filtry)

Kolejność ścieżek (KRYTYCZNA):
  /schema-integrity/check (POST) i /schema-integrity (GET) to osobne ścieżki —
  FastAPI rozróżnia po metodzie HTTP, kolejność nie jest problemem.
  ALE: /cors i /config PRZED /{key} — literal przed parametrem.

Serwisy:
  config_service   — GET/PUT /system/config + /system/cors
  schema_integrity — GET/POST /system/schema-integrity
  audit_service    — GET /system/audit-log
  db/wapro.py      — health check WAPRO

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

logger = logging.getLogger(__name__)
router = APIRouter()


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 1: GET /system/health
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/health",
    summary="Health check systemu",
    description=(
        "Szczegółowy health check — sprawdza dostępność wszystkich komponentów:\n"
        "- **DB (MSSQL):** `SELECT 1` — latencja w ms\n"
        "- **Redis:** `PING` — latencja w ms\n"
        "- **WAPRO:** `SELECT 1` przez pyodbc — latencja + pool stats\n"
        "\nStatus `healthy` = wszystkie OK. "
        "`degraded` = Redis niedostępny (app działa, bez cache). "
        "`unhealthy` = DB lub WAPRO niedostępne.\n"
        "\n**Uwaga:** Endpoint `/health` na root (bez auth) to liveness probe dla Dockera. "
        "Ten endpoint (`/system/health`) zwraca pełne dane diagnostyczne. "
        "**Wymaga uprawnienia:** `system.view_health`"
    ),
    response_description="Status wszystkich komponentów systemu",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("system.view_health")],
)
async def system_health(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request_id: RequestID,
):
    import time
    from sqlalchemy import text

    results: dict = {
        "status": "healthy",
        "components": {},
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }

    # Sprawdź DB
    t0 = time.monotonic()
    try:
        await db.execute(text("SELECT 1 AS ping"))
        results["components"]["db"] = {
            "status": "ok",
            "latency_ms": round((time.monotonic() - t0) * 1000, 2),
        }
    except Exception as exc:
        results["components"]["db"] = {"status": "error", "error": str(exc)[:200]}
        results["status"] = "unhealthy"

    # Sprawdź Redis
    t0 = time.monotonic()
    try:
        await redis.ping()
        results["components"]["redis"] = {
            "status": "ok",
            "latency_ms": round((time.monotonic() - t0) * 1000, 2),
        }
    except Exception as exc:
        results["components"]["redis"] = {"status": "error", "error": str(exc)[:200]}
        if results["status"] == "healthy":
            results["status"] = "degraded"

    # Sprawdź WAPRO
    try:
        from app.db.wapro import ping as wapro_ping
        wapro_result = await wapro_ping()
        results["components"]["wapro"] = {
            "status": "ok" if wapro_result.get("ok") else "error",
            "latency_ms": wapro_result.get("latency_ms"),
            "pool": wapro_result.get("pool_stats"),
        }
        if not wapro_result.get("ok"):
            results["status"] = "unhealthy"
    except Exception as exc:
        results["components"]["wapro"] = {"status": "error", "error": str(exc)[:200]}
        results["status"] = "unhealthy"

    # HTTP status na podstawie health
    http_status = (
        status.HTTP_200_OK
        if results["status"] in ("healthy", "degraded")
        else status.HTTP_503_SERVICE_UNAVAILABLE
    )

    return BaseResponse.ok(data=results, app_code="system.health")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 2: GET /system/config
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/config",
    summary="Lista kluczy konfiguracji systemu",
    description=(
        "Zwraca wszystkie klucze konfiguracji z `dbo_ext.SystemConfig`. "
        "Wartości wrażliwe (`master_key.pin_hash`) są redagowane (`***`). "
        "Wyniki z cache Redis (`config:__all__` TTL 300s). "
        "**Wymaga uprawnienia:** `system.config_view`"
    ),
    response_description="Lista kluczy konfiguracji",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("system.config_view")],
)
async def get_config(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request_id: RequestID,
):
    from app.services import config_service

    configs = await config_service.get_all(db=db, redis=redis)

    return BaseResponse.ok(data={"items": configs, "total": len(configs)}, app_code="system.config")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 3: PUT /system/config/{key}
# ─────────────────────────────────────────────────────────────────────────────

@router.put(
    "/config/{config_key}",
    summary="Zmiana wartości klucza konfiguracji",
    description=(
        "Aktualizuje wartość klucza konfiguracji. "
        "Walidacja 3-warstwowa (config_service): enum constraints, zakresy int, bool. "
        "Inwalidacja cache Redis po zapisie. "
        "AuditLog: `config_updated` z old_value → new_value. "
        "\n\nPrzykłady kluczy:\n"
        "- `otp.expiry_minutes` (int 1-1440)\n"
        "- `schema_integrity.reaction` (WARN/ALERT/BLOCK)\n"
        "- `master_key.enabled` (true/false)\n"
        "- `snapshot.retention_days` (int 1-365)\n"
        "**Wymaga uprawnienia:** `system.config_edit`"
    ),
    response_description="Zaktualizowany klucz konfiguracji",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("system.config_edit")],
    responses={
        404: {"description": "Klucz konfiguracji nie istnieje"},
        422: {"description": "Wartość nie spełnia wymagań walidacji"},
    },
)
async def update_config(
    config_key: str,
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import config_service

    try:
        body = await request.json()
        value = body.get("value")
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Wymagane pole 'value' w body JSON",
                "errors": [{"field": "value", "message": "Pole wymagane"}],
            },
        )

    if value is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Wymagane pole 'value'",
                "errors": [{"field": "value", "message": "Pole wymagane"}],
            },
        )

    try:
        result = await config_service.set_value(
            db=db,
            redis=redis,
            key=config_key,
            value=str(value),
            updated_by_id=current_user.id_user,
            updated_by_username=current_user.username,
        )
    except Exception as exc:
        _raise_from_config_error(exc)

    logger.warning(
        orjson.dumps({
            "event": "api_config_updated",
            "key": config_key,
            "updated_by": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(data=result, app_code="system.config_updated")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 4: GET /system/cors
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/cors",
    summary="Aktualna lista dozwolonych originów CORS",
    description=(
        "Zwraca aktualną listę originów CORS z SystemConfig. "
        "Cache Redis: `cfg:cors.allowed_origins` TTL 300s. "
        "Źródła fallback: .env → `[\"http://localhost:3000\"]`. "
        "**Wymaga uprawnienia:** `system.cors_manage`"
    ),
    response_description="Lista dozwolonych originów CORS",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("system.cors_manage")],
)
async def get_cors(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request_id: RequestID,
):
    from app.services import config_service

    origins = await config_service.get_cors_origins(db=db, redis=redis)

    return BaseResponse.ok(
        data={"origins": origins, "total": len(origins)},
        app_code="system.cors",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 5: PUT /system/cors
# ─────────────────────────────────────────────────────────────────────────────

@router.put(
    "/cors",
    summary="Aktualizacja dozwolonych originów CORS",
    description=(
        "Aktualizuje listę originów CORS. "
        "Walidacja: zakaz `*`, wymagany protokół (http:// lub https://), "
        "deduplication, trailing slash strip, max 20 originów. "
        "Po zapisie: inwalidacja cache Redis → nowe originy aktywne natychmiast. "
        "AuditLog: `cors_updated`. "
        "**Wymaga uprawnienia:** `system.cors_manage`"
    ),
    response_description="Zaktualizowana lista originów CORS",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("system.cors_manage")],
    responses={
        422: {"description": "Nieprawidłowy origin (wildcard, brak protokołu, >20 originów)"},
    },
)
async def update_cors(
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.services import config_service

    try:
        body = await request.json()
        origins = body.get("origins")
    except Exception:
        origins = None

    if not origins or not isinstance(origins, list):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Wymagane pole 'origins' jako niepusta tablica stringów",
                "errors": [{"field": "origins", "message": "Pole wymagane, format: [\"https://app.example.com\"]"}],
            },
        )

    try:
        result = await config_service.update_cors(
            db=db,
            redis=redis,
            origins=origins,
            updated_by_id=current_user.id_user,
            updated_by_username=current_user.username,
        )
    except Exception as exc:
        _raise_from_config_error(exc)

    logger.warning(
        orjson.dumps({
            "event": "api_cors_updated",
            "origins_count": len(origins),
            "updated_by": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data={
            "origins": result.get("origins", origins),
            "total": len(result.get("origins", origins)),
            "cache_invalidated": True,
            "message": "Lista CORS zaktualizowana. Zmiany aktywne natychmiast.",
        },
        app_code="system.cors_updated",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 6: GET /system/schema-integrity
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/schema-integrity",
    summary="Wynik ostatniej weryfikacji checksumów",
    description=(
        "Zwraca wynik ostatniej weryfikacji integralności schematu bazy danych "
        "(uruchamianej przy każdym starcie aplikacji). "
        "Zawiera: status (OK/ALERT/BLOCK), listę rozbieżności (jeśli były), "
        "alembic revision, datę weryfikacji, czas trwania. "
        "Wyniki z pliku `logs/schema_integrity_YYYY-MM-DD.jsonl` (ostatni wpis). "
        "**Wymaga uprawnienia:** `system.schema_integrity_view`"
    ),
    response_description="Wynik ostatniej weryfikacji checksumów",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("system.schema_integrity_view")],
)
async def get_schema_integrity(
    current_user: CurrentUser,
    db: DB,
    request_id: RequestID,
):
    from app.core.schema_integrity import SchemaIntegrityChecker

    try:
        result = await SchemaIntegrityChecker.get_last_result(db=db)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "code": "system.schema_integrity_read_error",
                "message": "Błąd odczytu wyników weryfikacji",
                "errors": [{"field": "_", "message": str(exc)[:200]}],
            },
        )

    return BaseResponse.ok(data=result, app_code="system.schema_integrity")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 7: POST /system/schema-integrity/check
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/schema-integrity/check",
    summary="Wymuś ponowną weryfikację checksumów",
    description=(
        "Uruchamia weryfikację integralności schematu bazy on-demand "
        "(normalnie odpala się tylko przy starcie aplikacji). "
        "**Uwaga:** Może trwać kilka sekund (SELECT checksumów z sys.sql_modules). "
        "Przy wykryciu rozbieżności (typ BLOCK): zwraca wyniki, ale NIE zatrzymuje aplikacji "
        "(w przeciwieństwie do startup — runtime check jest informacyjny). "
        "Wynik zapisywany do `logs/schema_integrity_YYYY-MM-DD.jsonl`. "
        "**Wymaga uprawnienia:** `system.schema_integrity_view`"
    ),
    response_description="Wynik weryfikacji checksumów",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("system.schema_integrity_view")],
)
async def force_schema_integrity_check(
    current_user: CurrentUser,
    db: DB,
    client_ip: ClientIP,
    request_id: RequestID,
):
    from app.core.schema_integrity import SchemaIntegrityChecker

    logger.warning(
        orjson.dumps({
            "event": "api_schema_integrity_forced",
            "requested_by": current_user.id_user,
            "request_id": request_id,
            "ip": client_ip,
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    try:
        result = await SchemaIntegrityChecker.verify(db=db, runtime_check=True)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "code": "system.schema_integrity_check_error",
                "message": "Błąd weryfikacji integralności schematu",
                "errors": [{"field": "_", "message": str(exc)[:200]}],
            },
        )

    return BaseResponse.ok(data=result, app_code="system.schema_integrity_checked")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 8: GET /system/audit-log
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/audit-log",
    summary="Przeglądarka audit logu",
    description=(
        "Zwraca paginowany audit log z `dbo_ext.AuditLog`. "
        "\n\n**Filtry:**\n"
        "- `user_id` — akcje konkretnego użytkownika\n"
        "- `action` — filtr po nazwie akcji (np. `user_login`, `config_updated`)\n"
        "- `action_category` — AUTH / CRUD / SYSTEM\n"
        "- `entity_type` — typ encji (User, Role, Config...)\n"
        "- `success` — true/false (udane/nieudane akcje)\n"
        "- `date_from` / `date_to` — zakres dat (YYYY-MM-DD)\n"
        "- `ip_address` — filtr po IP\n"
        "\nSortowanie: Timestamp DESC (najnowsze pierwsze). "
        "**Wymaga uprawnienia:** `system.audit_log_view`"
    ),
    response_description="Paginowany audit log",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("audit.view_all")],
)
async def get_audit_log(
    current_user: CurrentUser,
    db: DB,
    pagination: Pagination,
    request_id: RequestID,
    user_id: Optional[int] = Query(None, description="Filtr: ID użytkownika"),
    action: Optional[str] = Query(None, max_length=100, description="Filtr akcji (np. user_login)"),
    action_category: Optional[str] = Query(None, description="Filtr kategorii: AUTH/CRUD/SYSTEM"),
    entity_type: Optional[str] = Query(None, max_length=100, description="Filtr typu encji"),
    success: Optional[bool] = Query(None, description="true = udane, false = nieudane"),
    date_from: Optional[str] = Query(None, description="Data od (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="Data do (YYYY-MM-DD)"),
    ip_address: Optional[str] = Query(None, max_length=45, description="Filtr IP"),
):
    from app.services import audit_service

    # Konwersja date_from / date_to string → datetime
    date_from_dt: Optional[datetime] = None
    date_to_dt: Optional[datetime] = None

    if date_from:
        try:
            from datetime import datetime as _dt
            date_from_dt = _dt.fromisoformat(date_from).replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code": "validation.error",
                    "message": "Nieprawidłowy format date_from — wymagany: YYYY-MM-DD",
                    "errors": [{"field": "date_from", "message": "Format: YYYY-MM-DD"}],
                },
            )

    if date_to:
        try:
            from datetime import datetime as _dt
            date_to_dt = _dt.fromisoformat(date_to).replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "code": "validation.error",
                    "message": "Nieprawidłowy format date_to — wymagany: YYYY-MM-DD",
                    "errors": [{"field": "date_to", "message": "Format: YYYY-MM-DD"}],
                },
            )

    rows, total = await audit_service.get_logs(
        db=db,
        user_id=user_id,
        action=action,
        category=action_category,
        entity_type=entity_type,
        success=success,
        date_from=date_from_dt,
        date_to=date_to_dt,
        limit=pagination.per_page,
        offset=pagination.offset,
    )

    return BaseResponse.ok(
        data={
            "items": rows,              # ← element [0] tuple
            "total": total,             # ← element [1] tuple
            "page": pagination.page,
            "per_page": pagination.per_page,
            "pages": _pages(total, pagination.per_page),
        },
        app_code="system.audit_log",
    )

@router.get(
    "/demo-mode",
    summary="Status trybu demonstracyjnego",
    description="Zwraca czy system działa w trybie demo (wysyłka zablokowana).",
    tags=["System"],
    response_model=None,
)
async def get_demo_mode_status(
    request: Request,
    current_user: CurrentUser,
) -> JSONResponse:
    """
    Informuje frontend czy DEMO_MODE jest aktywny.
    Frontend powinien wyświetlić banner/komunikat gdy demo_mode=true.
    """
    from app.core.config import get_settings as _gs
    settings = _gs()
    request_id = getattr(request.state, "request_id", None)

    return JSONResponse(
        content={
            "success": True,
            "code": "ok",
            "data": {
                "demo_mode": settings.DEMO_MODE,
                "message": (
                    "Tryb demonstracyjny aktywny — wysyłka email/SMS/PDF jest zablokowana."
                    if settings.DEMO_MODE
                    else "System w trybie produkcyjnym — wysyłka aktywna."
                ),
            },
            "meta": {
                "request_id": request_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        }
    )

# ─────────────────────────────────────────────────────────────────────────────
# POMOCNICZE
# ─────────────────────────────────────────────────────────────────────────────

def _pages(total: int, per_page: int) -> int:
    return (total + per_page - 1) // per_page if per_page > 0 else 0


def _raise_from_config_error(exc: Exception) -> None:
    exc_type = type(exc).__name__
    _MAP: dict[str, tuple[int, str, str]] = {
        "ConfigKeyNotFoundError":    (404, "system.config_not_found",   "Klucz konfiguracji nie istnieje"),
        "ConfigValidationError":     (422, "system.config_invalid_value","Wartość nie spełnia wymagań walidacji"),
        "ConfigReadOnlyError":       (403, "system.config_readonly",    "Klucz konfiguracji jest tylko do odczytu"),
        "CORSValidationError":       (422, "system.cors_invalid",       "Nieprawidłowy origin CORS"),
        "ConfigServiceError":        (400, "system.config_error",       "Błąd operacji konfiguracji"),
    }
    if exc_type in _MAP:
        http_status, code, msg = _MAP[exc_type]
        raise HTTPException(
            status_code=http_status,
            detail={"code": code, "message": msg, "errors": [{"field": "_", "message": str(exc) or msg}]},
        )
    raise