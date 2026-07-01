# backend/app/api/admin/sources.py
"""
Panel administracyjny zrodel dokumentow — F6.

UWAGA: To NOWY plik w NOWEJ lokalizacji (app/api/admin/), oddzielony od
istniejacego app/api/approval/sources.py ktory obsluguje tylko
GET /approval/sources (prosty dropdown dla frontendu obiegu).
Te dwa pliki NIE koliduja — różne prefixy routera.

9 endpointow:
  GET    /admin/sources                       — lista z paginacja i filtrami      [sources.view]
  GET    /admin/sources/{id_source}           — szczegoly                        [sources.view]
  POST   /admin/sources                       — utworz                           [sources.manage]
  PUT    /admin/sources/{id_source}           — aktualizuj (partial)             [sources.manage]
  DELETE /admin/sources/{id_source}           — usun (blokuje gdy ma instancje)   [sources.manage]
  POST   /admin/sources/{id_source}/test-connection                              [sources.test_connection]
  POST   /admin/sources/{id_source}/sync                                          [sources.sync]
  GET    /admin/sources/{id_source}/sync-status                                   [sources.view_log]
  GET    /admin/sources/health                                                    [sources.view_health]
  PATCH  /admin/sources/{id_source}/test-mode                                      [sources.toggle_test_mode]
  POST   /admin/sources/{id_source}/webhook-token  — (re)generacja, token 1x       [webhooks.manage]
  DELETE /admin/sources/{id_source}/webhook-token  — revoke                       [webhooks.manage]

Wszystkie nazwy uprawnien zgodne z migracja 0039 (sources.*) i 0040 (webhooks.*),
za wyjatkiem sources.view_health ktore migracja 0039 NIE zasiala — dodane
w migracji 0041 (jedyne brakujace uprawnienie z pelnej listy Etapu 2).

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, Request, status

from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.schemas.common import BaseResponse
from app.schemas.sources import (
    SourceCreate,
    SourceUpdate,
    SourceTestModePatch,
)
from app.services import source_admin_service as svc
from app.services.source_admin_service import (
    SourceNotFoundError,
    SourceNameConflictError,
    SourceValidationError,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sources")


def _raise_from_service_error(exc: Exception) -> None:
    """Mapuje wyjatki serwisu na HTTPException. Wspolny helper dla wszystkich endpointow."""
    if isinstance(exc, SourceNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, SourceNameConflictError):
        raise HTTPException(status_code=409, detail=str(exc))
    if isinstance(exc, SourceValidationError):
        raise HTTPException(status_code=422, detail=str(exc))
    raise


# =============================================================================
# GET /admin/sources — lista
# =============================================================================

@router.get(
    "",
    summary="Lista zrodel dokumentow (panel admina)",
    description=(
        "Pelna lista zrodel z paginacja i filtrami. Zwraca wiecej szczegolow "
        "niz GET /approval/sources (przeznaczony dla dropdownow) — wlacznie "
        "z connection_config_keys, statusem synchronizacji, has_webhook_token. "
        "connection_config NIGDY nie jest zwracany w plaintext. "
        "**Wymaga:** `sources.view`."
    ),
    dependencies=[require_permission("sources.view")],
)
async def list_sources_admin(
    current_user: CurrentUser,
    db: DB,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    source_type: str | None = Query(None),
    is_active: bool | None = Query(None),
):
    result = await svc.list_sources(
        db, page=page, per_page=per_page, source_type=source_type, is_active=is_active,
    )
    return BaseResponse.ok(data=result, app_code="sources.list")


# =============================================================================
# GET /admin/sources/health — PRZED /{id_source} (literal musi byc pierwszy)
# =============================================================================

@router.get(
    "/health",
    summary="Przeglad zdrowia wszystkich zrodel",
    description=(
        "Dashboard admina — klasyfikuje kazde zrodlo jako ok/warning/critical/unknown "
        "na podstawie czasu od ostatniej synchronizacji i statusu. "
        "Progi konfigurowalne przez SystemConfig: "
        "`source_health.warning_minutes` (domyslnie 60), "
        "`source_health.critical_minutes` (domyslnie 240). "
        "**Wymaga:** `sources.view_health`."
    ),
    dependencies=[require_permission("sources.view_health")],
)
async def get_sources_health(current_user: CurrentUser, db: DB):
    result = await svc.get_health(db)
    return BaseResponse.ok(data=result, app_code="sources.health")


# =============================================================================
# GET /admin/sources/{id_source} — szczegoly
# =============================================================================

@router.get(
    "/{id_source}",
    summary="Szczegoly zrodla",
    responses={404: {"description": "Zrodlo nie istnieje"}},
    dependencies=[require_permission("sources.view")],
)
async def get_source_admin(id_source: int, current_user: CurrentUser, db: DB):
    try:
        source = await svc.get_source(db, id_source)
    except SourceNotFoundError as exc:
        _raise_from_service_error(exc)
    return BaseResponse.ok(data=svc.to_source_out_dict(source), app_code="sources.get")


# =============================================================================
# POST /admin/sources — utworz
# =============================================================================

@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    summary="Utworz nowe zrodlo dokumentow",
    description=(
        "Nowe zrodlo zawsze startuje z is_test_mode=True (decyzja bezpieczenstwa) — "
        "operator musi recznie przelaczyc na produkcyjny przez "
        "PATCH /admin/sources/{id}/test-mode po weryfikacji. "
        "Dla connection_mode='push' webhook_token NIE jest generowany automatycznie — "
        "wywolaj osobno POST /admin/sources/{id}/webhook-token. "
        "**Wymaga:** `sources.manage`."
    ),
    responses={409: {"description": "source_name juz istnieje"}},
    dependencies=[require_permission("sources.manage")],
)
async def create_source_admin(
    body: SourceCreate,
    current_user: CurrentUser,
    db: DB,
):
    try:
        source = await svc.create_source(
            db,
            source_name=body.source_name,
            source_type=body.source_type,
            connection_mode=body.connection_mode,
            connection_config=body.connection_config,
            sync_interval_minutes=body.sync_interval_minutes,
            is_active=body.is_active,
            actor_id=current_user.id_user,
        )
    except (SourceNameConflictError, SourceValidationError) as exc:
        _raise_from_service_error(exc)
    return BaseResponse.ok(data=svc.to_source_out_dict(source), app_code="sources.created")


# =============================================================================
# PUT /admin/sources/{id_source} — aktualizuj
# =============================================================================

@router.put(
    "/{id_source}",
    summary="Aktualizuj zrodlo (partial update)",
    responses={404: {"description": "Zrodlo nie istnieje"}, 409: {"description": "source_name konflikt"}},
    dependencies=[require_permission("sources.manage")],
)
async def update_source_admin(
    id_source: int,
    body: SourceUpdate,
    current_user: CurrentUser,
    db: DB,
):
    try:
        source = await svc.update_source(
            db, id_source,
            actor_id=current_user.id_user,
            source_name=body.source_name,
            source_type=body.source_type,
            connection_mode=body.connection_mode,
            connection_config=body.connection_config,
            sync_interval_minutes=body.sync_interval_minutes,
            is_active=body.is_active,
        )
    except (SourceNotFoundError, SourceNameConflictError, SourceValidationError) as exc:
        _raise_from_service_error(exc)
    return BaseResponse.ok(data=svc.to_source_out_dict(source), app_code="sources.updated")


# =============================================================================
# DELETE /admin/sources/{id_source} — usun
# =============================================================================

@router.delete(
    "/{id_source}",
    summary="Usun zrodlo",
    description=(
        "Hard delete. Blokowane (409) jesli zrodlo ma powiazane instancje obiegu — "
        "w takim przypadku dezaktywuj zrodlo (PUT z is_active=false) zamiast usuwac. "
        "**Wymaga:** `sources.manage`."
    ),
    responses={404: {"description": "Zrodlo nie istnieje"}, 409: {"description": "Zrodlo ma powiazane instancje"}},
    dependencies=[require_permission("sources.manage")],
)
async def delete_source_admin(id_source: int, current_user: CurrentUser, db: DB):
    try:
        await svc.delete_source(db, id_source, actor_id=current_user.id_user)
    except SourceNotFoundError as exc:
        _raise_from_service_error(exc)
    return BaseResponse.ok(
        data={"id_source": id_source, "deleted": True},
        app_code="sources.deleted",
    )


# =============================================================================
# POST /admin/sources/{id_source}/test-connection
# =============================================================================

@router.post(
    "/{id_source}/test-connection",
    summary="Testuj polaczenie ze zrodlem",
    description=(
        "Wykonuje proba polaczenia bez zapisywania zadnych danych ani uruchamiania "
        "synchronizacji. Dla source_type='database' wykonuje SELECT TOP 5 z "
        "widoku/procedury wskazanej w connection_config. "
        "**Wymaga:** `sources.test_connection`."
    ),
    responses={404: {"description": "Zrodlo nie istnieje"}},
    dependencies=[require_permission("sources.test_connection")],
)
async def test_connection_admin(id_source: int, current_user: CurrentUser, db: DB):
    try:
        result = await svc.test_connection(db, id_source)
    except SourceNotFoundError as exc:
        _raise_from_service_error(exc)
    return BaseResponse.ok(data=result, app_code="sources.test_connection")


# =============================================================================
# POST /admin/sources/{id_source}/sync
# =============================================================================

@router.post(
    "/{id_source}/sync",
    summary="Wywolaj synchronizacje recznie (poza cyklem cron)",
    description=(
        "Kolejkuje natychmiastowa synchronizacje dla connection_mode='pull'. "
        "Dla connection_mode='push' zwraca queued=False (zrodlo czeka na webhook). "
        "Sprawdza distributed lock — jesli sync juz trwa, zwraca queued=False. "
        "**Wymaga:** `sources.sync`."
    ),
    responses={404: {"description": "Zrodlo nie istnieje"}},
    dependencies=[require_permission("sources.sync")],
)
async def trigger_sync_admin(
    id_source: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    try:
        result = await svc.trigger_sync(db, redis, id_source, actor_id=current_user.id_user)
    except SourceNotFoundError as exc:
        _raise_from_service_error(exc)
    return BaseResponse.ok(data=result, app_code="sources.sync_triggered")


# =============================================================================
# GET /admin/sources/{id_source}/sync-status
# =============================================================================

@router.get(
    "/{id_source}/sync-status",
    summary="Status synchronizacji zrodla",
    description="Do polling przez UI panelu admina — pokazuje aktualny status i nastepny zaplanowany sync.",
    responses={404: {"description": "Zrodlo nie istnieje"}},
    dependencies=[require_permission("sources.view_log")],
)
async def get_sync_status_admin(
    id_source: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    try:
        result = await svc.get_sync_status(db, redis, id_source)
    except SourceNotFoundError as exc:
        _raise_from_service_error(exc)
    return BaseResponse.ok(data=result, app_code="sources.sync_status")


# =============================================================================
# PATCH /admin/sources/{id_source}/test-mode
# =============================================================================

@router.patch(
    "/{id_source}/test-mode",
    summary="Przelacz tryb testowy/produkcyjny",
    description=(
        "Przejscie test->produkcyjny (is_test_mode: true->false) jest momentem "
        "od ktorego hooki krytyczne zaczynaja realnie wplywac na systemy zewnetrzne "
        "(np. Fakir). Loguje sie z priorytetem WARNING w audycie. "
        "**Wymaga:** `sources.toggle_test_mode`."
    ),
    responses={404: {"description": "Zrodlo nie istnieje"}},
    dependencies=[require_permission("sources.toggle_test_mode")],
)
async def set_test_mode_admin(
    id_source: int,
    body: SourceTestModePatch,
    current_user: CurrentUser,
    db: DB,
):
    try:
        source = await svc.set_test_mode(
            db, id_source, is_test_mode=body.is_test_mode, actor_id=current_user.id_user,
        )
    except SourceNotFoundError as exc:
        _raise_from_service_error(exc)
    return BaseResponse.ok(data=svc.to_source_out_dict(source), app_code="sources.test_mode_changed")


# =============================================================================
# POST /admin/sources/{id_source}/webhook-token — (re)generacja
# =============================================================================

@router.post(
    "/{id_source}/webhook-token",
    summary="(Re)generuj token webhooka",
    description=(
        "Generuje nowy token webhooka. Jesli token juz istnial — jest natychmiast "
        "uniewazniony (ta operacja jest jednoczesnie regeneracja). "
        "\n\n**KRYTYCZNE:** token jest pokazywany WYLACZNIE w tej jednej odpowiedzi. "
        "W bazie nie jest przechowywany hash — porownanie odbywa sie przez "
        "constant-time compare (ochrona przed timing attack). Jesli operator zgubi "
        "token, jedyna opcja to wygenerowanie nowego. "
        "\n\nWymaga connection_mode='push' (400 w przeciwnym razie). "
        "**Wymaga:** `webhooks.manage`."
    ),
    responses={
        400: {"description": "Zrodlo nie ma connection_mode='push'"},
        404: {"description": "Zrodlo nie istnieje"},
    },
    dependencies=[require_permission("webhooks.manage")],
)
async def generate_webhook_token_admin(
    id_source: int,
    current_user: CurrentUser,
    db: DB,
    request: Request,
):
    base_url = str(request.base_url).rstrip("/") + "/api/v1"
    try:
        result = await svc.generate_webhook_token(
            db, id_source, actor_id=current_user.id_user, base_url=base_url,
        )
    except SourceNotFoundError as exc:
        _raise_from_service_error(exc)
    return BaseResponse.ok(data=result, app_code="sources.webhook_token_generated")


# =============================================================================
# DELETE /admin/sources/{id_source}/webhook-token — revoke
# =============================================================================

@router.delete(
    "/{id_source}/webhook-token",
    summary="Uniewaznij token webhooka",
    description=(
        "Uniewaznia token bez generowania nowego — zrodlo przestaje przyjmowac "
        "wywolania webhooka do momentu wygenerowania nowego tokenu. "
        "Idempotentne — wywolanie gdy token nie istnieje nie powoduje bledu. "
        "**Wymaga:** `webhooks.manage`."
    ),
    responses={404: {"description": "Zrodlo nie istnieje"}},
    dependencies=[require_permission("webhooks.manage")],
)
async def revoke_webhook_token_admin(
    id_source: int,
    current_user: CurrentUser,
    db: DB,
):
    try:
        await svc.revoke_webhook_token(db, id_source, actor_id=current_user.id_user)
    except SourceNotFoundError as exc:
        _raise_from_service_error(exc)
    return BaseResponse.ok(
        data={"id_source": id_source, "revoked": True},
        app_code="sources.webhook_token_revoked",
    )