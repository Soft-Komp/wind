# backend/app/services/field_mapping_service.py
"""
Logika biznesowa mapowania pol zrodel dokumentow -> UnifiedDocument.

Uzupelnia luke opisana w specyfikacji (sekcja 3.3 Etap2_Scalony_Backend_Frontend.docx),
ktora zostala zrealizowana tylko czesciowo: GET /sources/{id}/field-preview
(odczyt probki z cache) powstal, ale zapis mapowania do
skw_document_source_field_mappings nigdy nie zostal zaimplementowany.
Ten plik domyka punkt 3.3 w calosci.

Ustalenia (sesja 2026-07-07):
    - PUT jest PELNA ZAMIANA (replace-all), transakcyjnie: usun wszystkie
      stare mapowania zrodla, wstaw nowe, w jednej transakcji DB.
    - source_field jest walidowane wzgledem cache field-preview (Redis,
      klucz field_preview:{id_source}, wypelniany przez
      POST /sources/{id}/test-connection). Wymagane istnienie cache —
      brak = 422 z jasna instrukcja co zrobic.
    - Dopasowanie source_field wzgledem cache jest TOLERANCYJNE
      (case-insensitive) — WAPRO UPPER_SNAKE_CASE vs mozliwe camelCase z API.
    - transform_expression zweryfikowane DWUKROTNIE: raz w Pydantic
      (app/schemas/source_field_mapping.py), raz tutaj tuz przed zapisem
      (defense in depth — chroni tez przed przyszlym wywolaniem tego
      serwisu z pominieciem warstwy API/Pydantic).
    - Uprawnienie: sources.manage (PUT) / sources.view (GET) — juz istnieja,
      zero nowych migracji uprawnien.
    - Kazda ODRZUCONA proba (source_field nieznane w zrodle, transform_expression
      poza whitelista) trafia do logs/security_rejections_YYYY-MM-DD.jsonl —
      NIEZALEZNIE od audit_service. Inny cel: slad prob nieprawidlowych
      (rowniez potencjalnie zlosliwych), nie tylko udanych zmian.
    - Udana zamiana loguje sie przez audit_service.log_crud — kanoniczny,
      juz istniejacy w projekcie mechanizm redundantny (DB + plik JSONL,
      fire-and-forget, pelne ContextVars), z PELNYM zrzutem before/after
      (nie diff — decyzja Michala: "pelna", odtwarzalnosc > zwiezlosc).
"""
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import orjson
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.approval.document_source_field_mapping import DocumentSourceFieldMapping
from app.schemas.source_field_mapping import FieldMappingItem, is_transform_expression_safe
from app.services import audit_service
from app.services.audit_service import ip_address_var
from app.services.source_admin_service import SourceNotFoundError, get_source

logger = logging.getLogger(__name__)

_CACHE_KEY_PREFIX = "field_preview:"


class FieldMappingValidationError(Exception):
    """Rzucany gdy mapowanie nie przechodzi walidacji biznesowej (mapowane na HTTP 422)."""


# =============================================================================
# Odczyt
# =============================================================================

async def list_field_mappings(db: AsyncSession, id_source: int) -> list[DocumentSourceFieldMapping]:
    """
    Zwraca aktualne mapowania zrodla, posortowane po common_field.
    Rzuca SourceNotFoundError gdy zrodlo nie istnieje (zgodnie z konwencja
    uzyta w reszcie modulu source_admin_service).
    """
    await get_source(db, id_source)

    result = await db.execute(
        select(DocumentSourceFieldMapping)
        .where(DocumentSourceFieldMapping.id_source == id_source)
        .order_by(DocumentSourceFieldMapping.common_field)
    )
    return list(result.scalars().all())


# =============================================================================
# Zapis — PELNA ZAMIANA
# =============================================================================

async def replace_field_mappings(
    db: AsyncSession,
    redis: Any,
    id_source: int,
    mappings: list[FieldMappingItem],
    *,
    actor_id: int,
    actor_username: Optional[str] = None,
) -> list[DocumentSourceFieldMapping]:
    """
    Zastepuje CALA kolekcje mapowan zrodla podana lista, w jednej transakcji.

    Zasada "wszystko albo nic": jesli choc jedna pozycja nie przejdzie
    walidacji, ZADNA zmiana nie jest zapisywana, a proba (pelny payload)
    trafia do dedykowanego logu bezpieczenstwa.
    """
    source = await get_source(db, id_source)

    available_fields = await _get_available_source_fields(redis, id_source)
    if available_fields is None:
        raise FieldMappingValidationError(
            "Brak probki pol dla tego zrodla w cache (field-preview). Wykonaj "
            "najpierw POST /sources/{id}/test-connection, aby wygenerowac "
            "probke dostepnych pol (cache Redis TTL 3600s), a nastepnie "
            "sprobuj ponownie zapisac mapowanie."
        )

    available_lower = {f.lower() for f in available_fields if f}
    rejection_reasons: list[str] = []

    for item in mappings:
        if item.source_field.lower() not in available_lower:
            rejection_reasons.append(
                f"source_field={item.source_field!r} nie wystepuje w probce pol "
                f"zrodla (common_field={item.common_field!r}). Sprawdz pisownie "
                f"lub odswiez probke przez test-connection."
            )
        if item.transform_expression and not is_transform_expression_safe(item.transform_expression):
            # Nie powinno tu dotrzec (Pydantic juz to zlapal) — druga,
            # niezalezna kontrola tuz przed zapisem do bazy.
            rejection_reasons.append(
                f"transform_expression={item.transform_expression!r} poza "
                f"whitelista (common_field={item.common_field!r})."
            )

    if rejection_reasons:
        _log_security_rejection(
            id_source=id_source,
            actor_id=actor_id,
            actor_username=actor_username,
            payload=[m.model_dump() for m in mappings],
            reasons=rejection_reasons,
        )
        raise FieldMappingValidationError(
            "Walidacja mapowania nie powiodla sie — NIC nie zostalo zapisane: "
            + "; ".join(rejection_reasons)
        )

    # ── Stan przed zmiana (do pelnego zrzutu audytowego) ────────────────────
    existing = await list_field_mappings(db, id_source)
    old_state = [_mapping_to_audit_dict(m) for m in existing]

    # ── Pelna zamiana, transakcyjnie ────────────────────────────────────────
    for old in existing:
        await db.delete(old)
    await db.flush()

    new_objects: list[DocumentSourceFieldMapping] = []
    for item in mappings:
        obj = DocumentSourceFieldMapping(
            id_source=id_source,
            common_field=item.common_field,
            source_field=item.source_field,
            field_type=item.field_type,
            transform_expression=item.transform_expression,
        )
        db.add(obj)
        new_objects.append(obj)

    await db.flush()
    new_state = [_mapping_to_audit_dict(m) for m in new_objects]

    await db.commit()

    for obj in new_objects:
        await db.refresh(obj)

    logger.info(
        "Mapowania pol zrodla zastapione | id_source=%s liczba_starych=%d "
        "liczba_nowych=%d actor=%s",
        id_source, len(old_state), len(new_state), actor_id,
    )

    # Redundantny, podwojny zapis (DB + plik JSONL) — kanoniczny mechanizm
    # audit_service, fire-and-forget, pelny before/after.
    audit_service.log_crud(
        db,
        action="source.field_mappings_replaced",
        entity_type="DocumentSource",
        entity_id=id_source,
        old_value={"mappings": old_state, "source_name": source.source_name},
        new_value={"mappings": new_state, "source_name": source.source_name},
        user_id=actor_id,
        username=actor_username,
        details={
            "count_before": len(old_state),
            "count_after": len(new_state),
        },
    )

    return new_objects


# =============================================================================
# Cache field-preview (Redis)
# =============================================================================

async def _get_available_source_fields(redis: Any, id_source: int) -> Optional[list[str]]:
    """
    Odczytuje liste nazw pol z cache field-preview.

    Zwraca None jesli cache nie istnieje / wygasl / jest uszkodzony —
    swiadomie NIE zwracamy pustej listy w tych przypadkach, bo pusta lista
    odrzucalaby WSZYSTKIE mapowania z mylacym komunikatem "pole nie istnieje",
    podczas gdy prawdziwy problem to brak/przedawnienie cache.
    """
    if redis is None:
        return None
    try:
        raw = await redis.get(f"{_CACHE_KEY_PREFIX}{id_source}")
    except Exception as exc:
        logger.warning(
            "field_mapping_service: blad odczytu cache field-preview dla "
            "id_source=%s: %s", id_source, exc,
        )
        return None
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.error(
            "field_mapping_service: cache field-preview dla id_source=%s "
            "zawiera niepoprawny JSON — ignoruje.", id_source,
        )
        return None
    if not isinstance(parsed, list):
        return None
    return [
        entry.get("field_name", "")
        for entry in parsed
        if isinstance(entry, dict) and entry.get("field_name")
    ]


def _mapping_to_audit_dict(m: DocumentSourceFieldMapping) -> dict[str, Any]:
    return {
        "id_mapping": m.id_mapping,
        "common_field": m.common_field,
        "source_field": m.source_field,
        "field_type": m.field_type,
        "transform_expression": m.transform_expression,
    }


# =============================================================================
# Log dedykowany — proby ODRZUCONE (bezpieczenstwo), NIEZALEZNY od audit_service
# =============================================================================

def _get_security_rejections_log_path() -> Path:
    """Sciezka pliku JSONL prob odrzuconych — rotacja dzienna, ten sam wzorzec co audit_service."""
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return log_dir / f"security_rejections_{today}.jsonl"


def _log_security_rejection(
    *,
    id_source: int,
    actor_id: int,
    actor_username: Optional[str],
    payload: list[dict[str, Any]],
    reasons: list[str],
) -> None:
    """
    Zapisuje KAZDA odrzucona probe zapisu mapowania do dedykowanego pliku
    JSONL, niezaleznie od audit_service.log(). Cel jest inny niz standardowy
    audyt "co sie zmienilo": to slad prob NIEPRAWIDLOWYCH (rowniez
    potencjalnie zlosliwych prob wstrzykniecia SQL przez transform_expression).

    Blad zapisu tego logu NIGDY nie przerywa zwrotu bledu 422 do klienta —
    izolowany try/except, tak jak wszystkie sciezki logowania w projekcie.
    """
    entry = {
        "event": "security_rejection",
        "area": "source_field_mappings",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "id_source": id_source,
        "actor": {
            "id_user": actor_id,
            "username": actor_username,
            "ip_address": ip_address_var.get(),
        },
        "reasons": reasons,
        "rejected_payload": payload,
    }
    try:
        path = _get_security_rejections_log_path()
        line = orjson.dumps(entry, option=orjson.OPT_NON_STR_KEYS).decode("utf-8")
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError as exc:
        print(
            f"[field_mapping_service] Blad zapisu security_rejections: {exc}",
            file=sys.stderr,
        )
    logger.warning(
        "Odrzucona proba zapisu mapowania pol | id_source=%s actor=%s powody=%s",
        id_source, actor_id, reasons,
    )