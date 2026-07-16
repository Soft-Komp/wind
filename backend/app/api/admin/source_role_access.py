# backend/app/api/admin/source_role_access.py
"""
Kontrola dostepu do zrodel per rola + field-preview — TODO-05, TODO-06, brakujacy endpoint.

Dolaczany do app/api/admin/__init__.py.

5 endpointow:
  GET    /admin/sources/{id_source}/roles         — lista rol z dostepem   [sources.manage_access]
  POST   /admin/sources/{id_source}/roles/{id_role} — nadaj dostep         [sources.manage_access]
  DELETE /admin/sources/{id_source}/roles/{id_role} — odbierz dostep       [sources.manage_access]
  GET    /admin/sources/my-accessible              — moje dostepne zrodla  [sources.view]
  GET    /admin/sources/{id_source}/field-preview  — probka pol ze zrodla  [sources.view]

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging

from fastapi import APIRouter, HTTPException

from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.schemas.common import BaseResponse
from app.services import source_role_access_service as svc

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sources")


# =============================================================================
# Zarzadzanie rolami per zrodlo
# =============================================================================

@router.get(
    "/{id_source}/roles",
    summary="Lista rol z dostepem do zrodla",
    description=(
        "Zwraca role ktore maja dostep do dokumentow z tego zrodla. "
        "Uzytkownicy z approval.supervise lub documents.view_all maja dostep "
        "niezaleznie od tej listy (bypass). "
        "**Wymaga:** `sources.manage_access`."
    ),
    responses={404: {"description": "Zrodlo nie istnieje"}},
    dependencies=[require_permission("sources.manage_access")],
)
async def list_source_roles(id_source: int, current_user: CurrentUser, db: DB):
    roles = await svc.list_roles_for_source(db, id_source)
    return BaseResponse.ok(
        data={"items": roles, "total": len(roles)},
        app_code="source_roles.list",
    )


@router.post(
    "/{id_source}/roles/{id_role}",
    status_code=201,
    summary="Nadaj dostep do zrodla roli",
    description=(
        "Idempotentne — brak bledu jesli rola juz ma dostep. "
        "Invaliduje cache Redis natychmiast (propagacja w max 300s do wygasniecia starych tokenow JWT). "
        "**Wymaga:** `sources.manage_access`."
    ),
    dependencies=[require_permission("sources.manage_access")],
)
async def add_role_to_source(
    id_source: int,
    id_role: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    try:
        result = await svc.add_role_to_source(
            db, redis, id_source, id_role,
            actor_id=current_user.id_user,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return BaseResponse.ok(data=result, app_code="source_roles.added")


@router.delete(
    "/{id_source}/roles/{id_role}",
    summary="Odbierz dostep do zrodla roli",
    description=(
        "Idempotentne — brak bledu jesli rola nie miala dostepu. "
        "**KRYTYCZNE:** Po odebraniu dostepu uzytkownicy z ta rola "
        "natychmiast traca widocznosc dokumentow ze zrodla (cache invalidowany). "
        "**Wymaga:** `sources.manage_access`."
    ),
    dependencies=[require_permission("sources.manage_access")],
)
async def remove_role_from_source(
    id_source: int,
    id_role: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    await svc.remove_role_from_source(
        db, redis, id_source, id_role,
        actor_id=current_user.id_user,
    )
    return BaseResponse.ok(
        data={"id_source": id_source, "id_role": id_role, "removed": True},
        app_code="source_roles.removed",
    )


# =============================================================================
# GET /sources/my-accessible — zrodla dostepne dla zalogowanego usera
# =============================================================================

@router.get(
    "/my-accessible",
    summary="Lista zrodel dokumentow dostepnych dla zalogowanego uzytkownika",
    description=(
        "Poziom 1 kontroli dostepu: zwraca tylko zrodla do ktorych "
        "rola uzytkownika ma przypisany dostep w skw_source_role_access. "
        "Supervisor (approval.supervise / documents.view_all) widzi wszystkie aktywne zrodla. "
        "Uzywane przez frontend do budowania dropdownow filtrow. "
        "**Wymaga:** `sources.view`."
    ),
    dependencies=[require_permission("sources.view")],
)
async def list_my_accessible_sources(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    # Pobierz uprawnienia usera z JWT (dostepne w request.state lub z serwisu)
    try:
        from app.services import permission_service
        user_perms = await permission_service.get_user_permissions_set(
            db, redis, id_user=current_user.id_user, role_id=current_user.role_id,
        )
    except Exception:
        user_perms = set()

    sources = await svc.list_accessible_sources_for_user(
        db, redis,
        id_user=current_user.id_user,
        id_role=current_user.role_id,
        user_permissions=user_perms,
    )
    return BaseResponse.ok(
        data={"items": sources, "total": len(sources)},
        app_code="sources.my_accessible",
    )


# =============================================================================
# GET /sources/{id}/field-preview — probka pol ze zrodla (cache Redis)
# =============================================================================

_SOURCE_TYPES_WITHOUT_FIELD_MAPPING = frozenset({"ftp", "email", "ksef20", "manual"})


@router.get(
    "/{id_source}/field-preview",
    summary="Probka pol dostepnych w zrodle (z cache Redis)",
    description=(
        "Zwraca liste pol dostepnych w zrodle z przykladowymi wartosciami — "
        "uzywane przy konfiguracji mapowania pol w UI. Dotyczy WYLACZNIE "
        "source_type='database'/'api' — te typy realnie obsluguja field_mappings "
        "w adapterze. Dla source_type in (ftp, email, ksef20, manual) endpoint "
        "zwraca 409 (nie 424) — te typy nie obsluguja mapowania pol architektonicznie, "
        "wiec zaden test-connection tego nie zmieni. "
        "Rozni sie od test-connection tym ze: (1) uzywane jest cache Redis (TTL 1h), "
        "(2) zwraca sformatowana liste par {field_name, sample_value} gotowa do "
        "renderowania dropdownow mapowania, (3) nie pobiera nowych danych jesli cache aktualny. "
        "Dla database/api wymaga wczesniejszego wykonania POST /sources/{id}/test-connection "
        "ktore zapelnia cache. "
        "**Wymaga:** `sources.view`."
    ),
    responses={
        404: {"description": "Zrodlo nie istnieje"},
        409: {"description": "Ten source_type nie obsluguje mapowania pol (ftp/email/ksef20/manual)"},
        424: {"description": "Cache pusty — wykonaj najpierw test-connection (tylko database/api)"},
    },
    dependencies=[require_permission("sources.view")],
)
async def get_field_preview(
    id_source: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    # Pobierz TEZ source_type — poprzednia wersja pobierala tylko id/nazwe,
    # przez co endpoint nie mial jak odroznic "cache jeszcze pusty" od
    # "ten typ zrodla nigdy nie bedzie mial cache". Sprawdzamy PRZED cache,
    # zeby nie marnowac odpytania Redis dla typow ktore i tak dostana 409.
    from sqlalchemy import text
    result = await db.execute(
        text(
            f"SELECT [id_source], [source_name], [source_type] "
            f"FROM [dbo].[skw_document_sources] WHERE [id_source] = :s"
        ),
        {"s": id_source},
    )
    row = result.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Zrodlo ID={id_source} nie istnieje.")

    source_type = row[2]
    if source_type in _SOURCE_TYPES_WITHOUT_FIELD_MAPPING:
        logger.info(
            "field_preview: source_type=%r nie obsluguje mapowania pol | id_source=%s",
            source_type, id_source,
        )
        raise HTTPException(
            status_code=409,
            detail={
                "code":    "field_preview.not_applicable",
                "message": (
                    f"Zrodlo typu '{source_type}' nie obsluguje mapowania pol. "
                    f"Dane sa przetwarzane automatycznie (OCR dla ftp/email, "
                    f"predefiniowany adapter dla ksef20, brak synchronizacji dla manual) — "
                    f"nie wymagaja konfiguracji mapowania przez administratora."
                ),
            },
        )

    cache_key = f"field_preview:{id_source}"

    # Probuj z cache (TTL 1h — test-connection jest kosztowne)
    cached_preview = None
    if redis:
        try:
            cached = await redis.get(cache_key)
            if cached is not None:
                import json
                cached_preview = json.loads(
                    cached.decode() if isinstance(cached, bytes) else cached
                )
        except Exception as exc:
            logger.debug("field_preview: blad odczytu cache: %s", exc)

    if cached_preview is not None:
        return BaseResponse.ok(
            data={"fields": cached_preview, "from_cache": True},
            app_code="sources.field_preview",
        )

    raise HTTPException(
        status_code=424,
        detail={
            "code":    "field_preview.cache_empty",
            "message": (
                f"Brak danych podgladu dla zrodla ID={id_source}. "
                f"Wykonaj najpierw POST /admin/sources/{id_source}/test-connection "
                f"aby zapelnic cache (TTL 1h)."
            ),
        },
    )