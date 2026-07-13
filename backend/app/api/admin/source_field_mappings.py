# backend/app/api/admin/source_field_mappings.py
"""
Mapowanie pol zrodel dokumentow — GET/PUT /admin/sources/{id_source}/field-mappings.

Domyka punkt 3.3 specyfikacji Etap 2 (Etap2_Scalony_Backend_Frontend.docx),
ktory zostal wczesniej zrealizowany tylko czesciowo — GET field-preview
(odczyt probki z cache) powstal w source_role_access.py, ale zapis
mapowania do skw_document_source_field_mappings nigdy nie mial gdzie
sie wydarzyc (SourceUpdate/PUT /sources/{id} nigdy nie mial pola na to).

NOWY, dedykowany plik (nie dopisany do source_role_access.py) — ten plik
miesza juz dwa tematy (dostep per rola + field-preview), trzeci by
zaszkodzil czytelnosci.

2 endpointy:
  GET /admin/sources/{id_source}/field-mappings — lista mapowan     [sources.view]
  PUT /admin/sources/{id_source}/field-mappings — pelna zamiana     [sources.manage]

Dolaczany do app/api/admin/__init__.py.

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging

from fastapi import APIRouter, HTTPException

from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.schemas.common import BaseResponse
from app.schemas.source_field_mapping import FieldMappingOut, FieldMappingsReplaceRequest
from app.services import field_mapping_service as svc
from app.services.field_mapping_service import FieldMappingValidationError
from app.services.source_admin_service import SourceNotFoundError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sources")


def _raise_from_service_error(exc: Exception) -> None:
    """Mapuje wyjatki serwisu na HTTPException. Wspolny helper — konwencja modulu sources.py."""
    if isinstance(exc, SourceNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, FieldMappingValidationError):
        raise HTTPException(status_code=422, detail=str(exc))
    raise


# =============================================================================
# GET /admin/sources/{id_source}/field-mappings — lista
# =============================================================================

@router.get(
    "/{id_source}/field-mappings",
    summary="Lista mapowan pol zrodla",
    description=(
        "Zwraca aktualna, pelna liste mapowan pole-zrodlowe -> pole "
        "UnifiedDocument dla danego zrodla. Pusta lista oznacza, ze "
        "zrodlo jeszcze nie ma skonfigurowanego mapowania (wszystkie "
        "pola trafiaja do extra_data przy synchronizacji). "
        "**Wymaga:** `sources.view`."
    ),
    responses={404: {"description": "Zrodlo nie istnieje"}},
    dependencies=[require_permission("sources.view")],
)
async def list_field_mappings_admin(
    id_source: int,
    current_user: CurrentUser,
    db: DB,
):
    try:
        mappings = await svc.list_field_mappings(db, id_source)
    except SourceNotFoundError as exc:
        _raise_from_service_error(exc)
        return  # nieosiagalne — _raise_from_service_error zawsze rzuca

    data = [FieldMappingOut.model_validate(m).model_dump() for m in mappings]
    return BaseResponse.ok(
        data={"items": data, "total": len(data)},
        app_code="source_field_mappings.list",
    )


# =============================================================================
# PUT /admin/sources/{id_source}/field-mappings — pelna zamiana
# =============================================================================

@router.put(
    "/{id_source}/field-mappings",
    summary="Zastap pelna liste mapowan pol zrodla",
    description=(
        "PELNA ZAMIANA (replace-all) w jednej transakcji — wszystkie "
        "istniejace mapowania tego zrodla sa usuwane i zastepowane podana "
        "lista. Zasada wszystko-albo-nic: jesli choc jedna pozycja nie "
        "przejdzie walidacji, ZADNA zmiana nie jest zapisywana.\n\n"
        "Wymaga wczesniejszego POST /sources/{id}/test-connection — "
        "source_field kazdej pozycji jest walidowane wzgledem probki pol "
        "w cache Redis (dopasowanie case-insensitive). Brak aktualnego "
        "cache = 422 z instrukcja co zrobic.\n\n"
        "transform_expression jest ograniczone do zamknietej whitelisty "
        "bezpiecznych wzorcow SQL (DATEADD dla dat Clarion, ROUND, CAST, "
        "CONVERT, TRIM) — nigdy nie jest interpolowane surowo do zapytania. "
        "Kazda odrzucona proba (zle source_field lub transform_expression "
        "poza whitelista) jest logowana do "
        "logs/security_rejections_YYYY-MM-DD.jsonl niezaleznie od "
        "standardowego audytu.\n\n"
        "**Wymaga:** `sources.manage`."
    ),
    responses={
        404: {"description": "Zrodlo nie istnieje"},
        422: {"description": "Walidacja nie powiodla sie — nic nie zostalo zapisane"},
    },
    dependencies=[require_permission("sources.manage")],
)
async def replace_field_mappings_admin(
    id_source: int,
    body: FieldMappingsReplaceRequest,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    try:
        mappings = await svc.replace_field_mappings(
            db, redis, id_source, body.mappings,
            actor_id=current_user.id_user,
            actor_username=getattr(current_user, "username", None),
        )
    except (SourceNotFoundError, FieldMappingValidationError) as exc:
        _raise_from_service_error(exc)
        return  # nieosiagalne

    data = [FieldMappingOut.model_validate(m).model_dump() for m in mappings]
    return BaseResponse.ok(
        data={"items": data, "total": len(data)},
        app_code="source_field_mappings.replaced",
    )