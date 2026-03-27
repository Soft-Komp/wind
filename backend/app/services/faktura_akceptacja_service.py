"""
app/services/faktura_akceptacja_service.py
==========================================
Logika biznesowa referenta — moduł Akceptacji Faktur KSeF.

Funkcje publiczne:
    get_faktury_list()          — lista faktur (merge WAPRO + nasza tabela)
    create_faktura_akceptacja() — wpuszczenie do obiegu + SSE push
    patch_faktura()             — edycja priorytetu/opisu/uwag
    initiate_reset_przypisania()— krok 1 resetu (JWT confirm_token)
    confirm_reset_przypisania() — krok 2 resetu (weryfikacja + wykonanie)
    initiate_force_status()     — krok 1 force_status (JWT confirm_token)
    confirm_force_status()      — krok 2 force_status
    get_historia()              — historia z skw_faktura_log (cache 300s)
    get_faktura_pdf()           — PDF z ReportLab (cache 300s)

CACHE Redis:
    faktura:list:referent:{hash}  TTL 60s  → inwalidacja przy POST/PATCH/reset
    faktura:detail:{id}           TTL 120s → inwalidacja przy PATCH/reset/decyzja
    faktura:historia:{id}         TTL 300s → inwalidacja przy każdej akcji
    faktura:pdf:{id}:{hash}       TTL 300s → inwalidacja przy zmianie danych

WAPRO READ:
    Widok: dbo.skw_faktury_akceptacja_naglowek  (nagłówki)
    Widok: dbo.skw_faktury_akceptacja_pozycje   (pozycje — tylko przy szczegółach)
    Dostęp: wapro.py (WaproConnectionPool, read-only)

DWUETAPOWE OPERACJE:
    Identyczny mechanizm jak DELETE /users/{id}:
    - Krok 1: create_one_time_token(scope, entity_type, entity_id, requested_by, ttl)
    - Krok 2: verify_one_time_token(...) → blacklist JTI w Redis
    Scope:
      reset:       "confirm_reset_faktura"
      force_akcept: "confirm_force_akceptacja"
      anulowanie:  "confirm_anuluj_faktura"
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import orjson
from fastapi import HTTPException, status
from redis.asyncio import Redis
from sqlalchemy import and_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.security import create_one_time_token, verify_one_time_token
from app.db.models.faktura_akceptacja import (
    FakturaAkceptacja,
    FakturaLog,
    FakturaPrzypisanie,
)
from app.db.wapro import execute_query
from app.schemas.faktura_akceptacja import (
    AkcjaLog,
    ConfirmTokenResponse,
    FakturaCreateRequest,
    FakturaCreateResponse,
    FakturaDetailResponse,
    FakturaHistoriaResponse,
    FakturaLogDetails,
    FakturaLogItemResponse,
    FakturaForceStatusRequest,
    FakturaPatchRequest,
    FakturaResetRequest,
    FakturaResetResponse,
    Priorytet,
    StatusWewnetrzny,
    WaproFakturaNaglowek,
    WaproFakturaPozycja,
)
from app.services.config_service import get_config_value
from app.services.event_service import publish_faktura_event

logger = logging.getLogger("app.services.faktura_akceptacja")

# ─────────────────────────────────────────────────────────────────────────────
# Cache key helpers
# ─────────────────────────────────────────────────────────────────────────────

def _cache_key_historia(faktura_id: int) -> str:
    return f"faktura:historia:{faktura_id}"

def _cache_key_detail(faktura_id: int) -> str:
    return f"faktura:detail:{faktura_id}"

def _cache_key_pdf(faktura_id: int, data_hash: str) -> str:
    return f"faktura:pdf:{faktura_id}:{data_hash}"

def _cache_key_list(page: int, limit: int, filters: dict) -> str:
    filters_hash = hashlib.md5(
        orjson.dumps(filters, option=orjson.OPT_SORT_KEYS)
    ).hexdigest()[:12]
    return f"faktura:list:referent:{page}:{limit}:{filters_hash}"

async def _invalidate_faktura_cache(redis: Redis, faktura_id: int) -> None:
    """Inwalidacja wszystkich kluczy cache dla danej faktury."""
    try:
        keys_to_delete = [
            _cache_key_historia(faktura_id),
            _cache_key_detail(faktura_id),
        ]
        # Inwalidacja list (pattern — usuń wszystkie strony)
        list_keys = await redis.keys("faktura:list:referent:*")
        keys_to_delete.extend(list_keys)
        # Inwalidacja PDF (pattern)
        pdf_keys = await redis.keys(f"faktura:pdf:{faktura_id}:*")
        keys_to_delete.extend(pdf_keys)

        if keys_to_delete:
            await redis.delete(*keys_to_delete)
    except Exception as exc:
        logger.warning(f"Cache invalidation error faktura_id={faktura_id}: {exc}")

# ─────────────────────────────────────────────────────────────────────────────
# WAPRO helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _get_wapro_naglowek(numer_ksef: str) -> Optional[WaproFakturaNaglowek]:
    """Pobiera nagłówek faktury z widoku WAPRO po KSEF_ID."""
    try:
        rows = await execute_query(
            query_type="faktura_naglowek",
            params={"ksef_id": numer_ksef},
        )
        if not rows:
            return None
        r = rows[0]
        return WaproFakturaNaglowek(
            id_buf_dokument=r.get("ID_BUF_DOKUMENT"),
            ksef_id=r.get("KSEF_ID", numer_ksef),
            numer=r.get("NUMER"),
            kod_statusu=r.get("KOD_STATUSU"),
            status_opis=r.get("StatusOpis"),
            data_wystawienia=r.get("DataWystawienia"),
            data_otrzymania=r.get("DataOtrzymania"),
            termin_platnosci=r.get("TerminPlatnosci"),
            wartosc_netto=r.get("WARTOSC_NETTO"),
            wartosc_brutto=r.get("WARTOSC_BRUTTO"),
            kwota_vat=r.get("KWOTA_VAT"),
            forma_platnosci=r.get("FORMA_PLATNOSCI"),
            uwagi=r.get("UWAGI"),
            nazwa_kontrahenta=r.get("NazwaKontrahenta"),
            email_kontrahenta=r.get("EmailKontrahenta"),
            telefon_kontrahenta=r.get("TelefonKontrahenta"),
        )
    except Exception as exc:
        logger.warning(f"WAPRO nagłówek query error ksef_id={numer_ksef}: {exc}")
        return None

async def _get_wapro_nowe_ksef_ids(db: AsyncSession) -> set[str]:
    """
    Pobiera KSEF_IDs faktur które są w WAPRO ale NIE w naszej tabeli.
    Używane do budowania listy "NOWE" dla referenta.
    """
    try:
        rows = await execute_query(
            query_type="faktury_nowe_ksef_ids",
            params={},
        )
        existing_stmt = select(FakturaAkceptacja.numer_ksef).where(
            FakturaAkceptacja.is_active == True  # noqa: E712
        )
        return {r.get("KSEF_ID") for r in rows if r.get("KSEF_ID")}
    except Exception as exc:
        logger.warning(f"WAPRO nowe_ksef_ids error: {exc}")
        return set()

# ─────────────────────────────────────────────────────────────────────────────
# Log helper — DRY
# ─────────────────────────────────────────────────────────────────────────────

async def _log_event(
    db: AsyncSession,
    *,
    faktura_id: int,
    user_id: int,
    akcja: AkcjaLog,
    before: Optional[dict] = None,
    after: Optional[dict] = None,
    meta: Optional[dict] = None,
    actor_name: str = "",
    actor_ip: str = "",
    request_id: str = "",
    endpoint: str = "",
) -> None:
    details = FakturaLogDetails.build(
        user_id=user_id,
        username=actor_name,
        ip=actor_ip,
        before=before,
        after=after,
        meta=meta,
        request_id=request_id,
        endpoint=endpoint,
    )
    db.add(FakturaLog(
        faktura_id=faktura_id,
        user_id=user_id,
        akcja=akcja.value,
        szczegoly=details.to_json_str(),
        created_at=datetime.now().replace(tzinfo=None),
    ))

# ─────────────────────────────────────────────────────────────────────────────
# ET-01: Obsługa faktury "orphaned" — znikła z WAPRO w trakcie obiegu
# ─────────────────────────────────────────────────────────────────────────────

async def _handle_orphan_if_needed(
    *,
    db: AsyncSession,
    redis: Redis,
    faktura: FakturaAkceptacja,
) -> bool:
    """
    Wykrywa i obsługuje ET-01: faktura aktywna w naszej tabeli, ale zniknęła z WAPRO.

    Warunki wykrycia:
      - status_wewnetrzny NOT IN ('orphaned', 'anulowana') — nie reaguj podwójnie
      - is_active = True
      - _get_wapro_naglowek() zwraca None

    Akcje:
      1. UPDATE status_wewnetrzny → 'orphaned'
      2. Wpis do skw_faktura_log (akcja: status_zmieniony)
      3. LOG ERROR do pliku
      4. SSE system_notification → referent (level=CRITICAL)

    Returns:
        True  — faktura była aktywna i właśnie oznaczona jako orphaned
        False — faktura już była orphaned/anulowana lub WAPRO odpowiedział
    """
    # Nie reaguj podwójnie
    if faktura.status_wewnetrzny in ("orphaned", "anulowana"):
        return False

    # WAPRO — sprawdź czy faktura nadal tam jest
    wapro = await _get_wapro_naglowek(faktura.numer_ksef)
    if wapro is not None:
        return False  # Wszystko OK

    # ── Faktura zniknęła z WAPRO ───────────────────────────────────────────
    now = datetime.now().replace(tzinfo=None)
    stary_status = faktura.status_wewnetrzny

    faktura.status_wewnetrzny = "orphaned"
    faktura.updated_at = now

    await _log_event(
        db=db,
        faktura_id=faktura.id,
        user_id=faktura.utworzony_przez,
        akcja=AkcjaLog.STATUS_ZMIENIONY,
        before={"status_wewnetrzny": stary_status},
        after={"status_wewnetrzny": "orphaned"},
        meta={
            "numer_ksef": faktura.numer_ksef,
            "powod": "Faktura zniknęła z widoku WAPRO (BUF_DOKUMENT). "
                     "Możliwe: usunięcie lub przeniesienie w Fakirze.",
        },
        endpoint="system/et-01-detection",
    )

    await db.commit()

    logger.error(
        orjson.dumps({
            "event":           "faktura_orphaned_detected",
            "faktura_id":      faktura.id,
            "numer_ksef":      faktura.numer_ksef,
            "stary_status":    stary_status,
            "referent_id":     faktura.utworzony_przez,
            "ts":              datetime.now(timezone.utc).isoformat(),
            "action_required": "Sprawdź BUF_DOKUMENT w Fakirze — faktura zniknęła z widoku WAPRO",
        }).decode()
    )

    # SSE system_notification → referent
    try:
        await publish_faktura_event(
            redis=redis,
            user_id=faktura.utworzony_przez,
            event_type="system_notification",
            data={
                "level":      "CRITICAL",
                "message":    f"Faktura {faktura.numer_ksef} zniknęła z systemu WAPRO/Fakir "
                              f"i wymaga interwencji. Sprawdź BUF_DOKUMENT.",
                "faktura_id": faktura.id,
                "numer_ksef": faktura.numer_ksef,
                "component":  "faktura_akceptacja",
            },
        )
    except Exception as exc:
        logger.warning(f"ET-01: SSE notification failed faktura_id={faktura.id}: {exc}")

    return True

# ─────────────────────────────────────────────────────────────────────────────
# 1. GET lista faktur
# ─────────────────────────────────────────────────────────────────────────────

async def get_faktury_list(
    *,
    db: AsyncSession,
    redis: Redis,
    page: int = 1,
    limit: int = 50,
    priorytet: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> dict[str, Any]:
    """
    Lista faktur dla referenta: nasze (W_TOKU/NOWE) z filtrami.
    Merge w pamięci Python — paginacja po złączeniu.
    Cache Redis 60s.
    """
    filters = {
        "priorytet": priorytet, "status": status,
        "search": search, "date_from": date_from, "date_to": date_to,
    }
    cache_key = _cache_key_list(page, limit, filters)

    # Cache hit
    try:
        cached = await redis.get(cache_key)
        if cached:
            return orjson.loads(cached)
    except Exception:
        pass

    # Buduj query
    stmt = select(FakturaAkceptacja).where(
        FakturaAkceptacja.is_active == True  # noqa: E712
    )

    if priorytet:
        stmt = stmt.where(FakturaAkceptacja.priorytet == priorytet)
    if status:
        stmt = stmt.where(FakturaAkceptacja.status_wewnetrzny == status)

    stmt = stmt.order_by(FakturaAkceptacja.created_at.desc())
    result = await db.execute(stmt)
    wszystkie = result.scalars().all()

    # Filtr search (po numer_ksef i opis_skrocony)
    if search:
        search_lower = search.lower()
        wszystkie = [
            f for f in wszystkie
            if search_lower in (f.numer_ksef or "").lower()
            or search_lower in (f.opis_dokumentu or "").lower()
        ]

    total = len(wszystkie)
    offset = (page - 1) * limit
    strona = wszystkie[offset : offset + limit]

    items = []
    for f in strona:
        # ET-01: sprawdź czy faktura nadal jest w WAPRO
        orphaned = await _handle_orphan_if_needed(db=db, redis=redis, faktura=f)

        wapro = await _get_wapro_naglowek(f.numer_ksef) if not orphaned else None
        items.append({
            "id":                f.id,
            "numer_ksef":        f.numer_ksef,
            "status_wewnetrzny": f.status_wewnetrzny,  # już 'orphaned' jeśli wykryto
            "priorytet":         f.priorytet,
            "opis_skrocony":     (f.opis_dokumentu or "")[:120] or None,
            "is_active":         f.is_active,
            "created_at":        f.created_at.isoformat() if f.created_at else None,
            "updated_at":        f.updated_at.isoformat() if f.updated_at else None,
            "numer":             wapro.numer if wapro else None,
            "wartosc_brutto":    float(wapro.wartosc_brutto) if wapro and wapro.wartosc_brutto else None,
            "nazwa_kontrahenta": wapro.nazwa_kontrahenta if wapro else None,
            "termin_platnosci":  wapro.termin_platnosci.isoformat() if wapro and wapro.termin_platnosci else None,
        })

    response = {"items": items, "total": total}

    # Cache write
    try:
        await redis.setex(cache_key, 60, orjson.dumps(response))
    except Exception:
        pass

    return response

# ─────────────────────────────────────────────────────────────────────────────
# 2. POST — wpuszczenie faktury do obiegu
# ─────────────────────────────────────────────────────────────────────────────

async def create_faktura_akceptacja(
    *,
    db: AsyncSession,
    redis: Redis,
    body: FakturaCreateRequest,
    actor_id: int,
    actor_name: str,
    actor_ip: str,
    request_id: str,
) -> FakturaCreateResponse:
    """
    Tworzy skw_faktura_akceptacja + skw_faktura_przypisanie.
    SSE push (nowa_faktura) do każdego przypisanego pracownika.
    Idempotentność zapewniona przez IdempotencyGuard w routerze.
    """
    # Sprawdź duplikat numer_ksef
    existing = await db.execute(
        select(FakturaAkceptacja).where(
            FakturaAkceptacja.numer_ksef == body.numer_ksef,
            FakturaAkceptacja.is_active == True,  # noqa: E712
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Faktura z KSEF_ID={body.numer_ksef!r} jest już w obiegu.",
        )

    # Sprawdź limit przypisanych
    max_przypisanych = int(await get_config_value(
        redis=redis,
        key="faktury.max_przypisanych_pracownikow",
        default="10",
    ))
    if len(body.user_ids) > max_przypisanych:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Przekroczono limit przypisanych pracowników ({max_przypisanych}).",
        )

    now = datetime.now().replace(tzinfo=None)

    # Utwórz fakturę
    faktura = FakturaAkceptacja(
        numer_ksef=body.numer_ksef,
        status_wewnetrzny="nowe",
        priorytet=body.priorytet if isinstance(body.priorytet, str) else body.priorytet.value,
        opis_dokumentu=body.opis_dokumentu,
        uwagi=body.uwagi,
        utworzony_przez=actor_id,
        is_active=True,
        created_at=now,
    )
    db.add(faktura)
    await db.flush()  # Pobierz ID bez commit

    # Utwórz przypisania
    for user_id in body.user_ids:
        db.add(FakturaPrzypisanie(
            faktura_id=faktura.id,
            user_id=user_id,
            status="oczekuje",
            is_active=True,
            created_at=now,
        ))

    # Log
    await _log_event(
        db=db,
        faktura_id=faktura.id,
        user_id=actor_id,
        akcja=AkcjaLog.PRZYPISANO,
        after={"status_wewnetrzny": "nowe", "priorytet": faktura.priorytet},
        meta={"numer_ksef": body.numer_ksef, "przypisani": body.user_ids},
        actor_name=actor_name,
        actor_ip=actor_ip,
        request_id=request_id,
        endpoint="/faktury-akceptacja",
    )

    await db.commit()

    # Inwalidacja cache list
    try:
        list_keys = await redis.keys("faktura:list:referent:*")
        if list_keys:
            await redis.delete(*list_keys)
    except Exception:
        pass

    # SSE push — nowa_faktura do każdego przypisanego
    await _sse_push_nowa_faktura(
        redis=redis,
        faktura=faktura,
        user_ids=body.user_ids,
    )

    logger.info(orjson.dumps({
        "event":      "faktura_created",
        "faktura_id": faktura.id,
        "numer_ksef": body.numer_ksef,
        "actor_id":   actor_id,
        "user_ids":   body.user_ids,
        "request_id": request_id,
        "ts":         datetime.now(timezone.utc).isoformat(),
    }).decode())

    return FakturaCreateResponse(
        id=faktura.id,
        numer_ksef=faktura.numer_ksef,
        status=StatusWewnetrzny.NOWE,
        priorytet=Priorytet(faktura.priorytet),
        przypisano_do=body.user_ids,
        created_at=faktura.created_at,
    )

# ─────────────────────────────────────────────────────────────────────────────
# 3. PATCH — edycja priorytetu/opisu/uwag
# ─────────────────────────────────────────────────────────────────────────────

async def patch_faktura(
    *,
    db: AsyncSession,
    redis: Redis,
    faktura_id: int,
    body: FakturaPatchRequest,
    actor_id: int,
    actor_name: str,
    actor_ip: str,
    request_id: str,
) -> dict[str, Any]:
    """Partial update faktury. Inwalidacja cache po zapisie."""
    result = await db.execute(
        select(FakturaAkceptacja).where(
            FakturaAkceptacja.id == faktura_id,
            FakturaAkceptacja.is_active == True,  # noqa: E712
        )
    )
    faktura = result.scalar_one_or_none()
    if not faktura:
        raise HTTPException(status_code=404, detail=f"Faktura ID={faktura_id} nie istnieje.")

    if faktura.status_wewnetrzny == "anulowana":
        raise HTTPException(status_code=409, detail="Nie można edytować anulowanej faktury.")

    before = {
        "priorytet": faktura.priorytet,
        "opis_dokumentu": faktura.opis_dokumentu,
        "uwagi": faktura.uwagi,
    }

    changes: dict[str, Any] = {}
    if body.priorytet is not None:
        val = body.priorytet if isinstance(body.priorytet, str) else body.priorytet.value
        faktura.priorytet = val
        changes["priorytet"] = val
    if body.opis_dokumentu is not None:
        faktura.opis_dokumentu = body.opis_dokumentu
        changes["opis_dokumentu"] = body.opis_dokumentu
    if body.uwagi is not None:
        faktura.uwagi = body.uwagi
        changes["uwagi"] = body.uwagi

    faktura.updated_at = datetime.now().replace(tzinfo=None)

    akcja = AkcjaLog.PRIORYTET_ZMIENIONY if "priorytet" in changes else AkcjaLog.STATUS_ZMIENIONY
    await _log_event(
        db=db,
        faktura_id=faktura_id,
        user_id=actor_id,
        akcja=akcja,
        before=before,
        after=changes,
        meta={"numer_ksef": faktura.numer_ksef},
        actor_name=actor_name,
        actor_ip=actor_ip,
        request_id=request_id,
        endpoint=f"/faktury-akceptacja/{faktura_id}",
    )

    await db.commit()
    await _invalidate_faktura_cache(redis, faktura_id)

    return {"id": faktura_id, "changes": changes, "message": "Faktura zaktualizowana."}

# ─────────────────────────────────────────────────────────────────────────────
# 4. POST /reset — krok 1
# ─────────────────────────────────────────────────────────────────────────────

async def initiate_reset_przypisania(
    *,
    db: AsyncSession,
    redis: Redis,
    faktura_id: int,
    body: FakturaResetRequest,
    actor_id: int,
    actor_name: str,
    actor_ip: str,
    request_id: str,
) -> ConfirmTokenResponse:
    """Krok 1 resetu: walidacja stanu + wygenerowanie JWT confirm_token."""
    result = await db.execute(
        select(FakturaAkceptacja).where(
            FakturaAkceptacja.id == faktura_id,
            FakturaAkceptacja.is_active == True,  # noqa: E712
        )
    )
    faktura = result.scalar_one_or_none()
    if not faktura:
        raise HTTPException(status_code=404, detail=f"Faktura ID={faktura_id} nie istnieje.")

    if faktura.status_wewnetrzny in ("anulowana",):
        raise HTTPException(
            status_code=409,
            detail="Nie można resetować anulowanej faktury.",
        )

    # Sprawdź czy można cofnąć już zatwierdzoną (fakir_rollback_enabled)
    if faktura.status_wewnetrzny == "zaakceptowana":
        rollback_enabled = await get_config_value(
            redis=redis, key="faktury.fakir_rollback_enabled", default="false"
        )
        if str(rollback_enabled).lower() != "true":
            raise HTTPException(
                status_code=409,
                detail="Faktura jest już zaakceptowana w Fakirze. Reset niedostępny.",
            )

    ttl = int(await get_config_value(
        redis=redis, key="faktury.confirm_token_ttl_seconds", default="60"
    ))

    token = await create_one_time_token(
        redis=redis,
        scope="confirm_reset_faktura",
        entity_type="FakturaAkceptacja",
        entity_id=faktura_id,
        requested_by=actor_id,
        ttl_seconds=ttl,
        extra_payload={
            "nowe_user_ids": body.nowe_user_ids,
            "powod": body.powod,
        },
    )

    logger.info(orjson.dumps({
        "event":       "reset_initiated",
        "faktura_id":  faktura_id,
        "actor_id":    actor_id,
        "request_id":  request_id,
        "ttl":         ttl,
    }).decode())

    return ConfirmTokenResponse(
        confirm_token=token,
        expires_in=ttl,
        action="reset_przypisania",
        message=f"Potwierdź reset przypisań w ciągu {ttl} sekund.",
    )

# ─────────────────────────────────────────────────────────────────────────────
# 5. POST /reset/confirm — krok 2
# ─────────────────────────────────────────────────────────────────────────────

async def confirm_reset_przypisania(
    *,
    db: AsyncSession,
    redis: Redis,
    faktura_id: int,
    confirm_token: str,
    actor_id: int,
    actor_name: str,
    actor_ip: str,
    request_id: str,
) -> FakturaResetResponse:
    """Krok 2 resetu: weryfikacja tokenu → dezaktywacja starych → nowe przypisania."""
    payload = await verify_one_time_token(
        redis=redis,
        token=confirm_token,
        expected_scope="confirm_reset_faktura",
        expected_entity_type="FakturaAkceptacja",
        expected_entity_id=faktura_id,
        requesting_user_id=actor_id,
    )

    nowe_user_ids: list[int] = payload.get("nowe_user_ids", [])
    powod: Optional[str] = payload.get("powod")

    result = await db.execute(
        select(FakturaAkceptacja).where(FakturaAkceptacja.id == faktura_id)
    )
    faktura = result.scalar_one_or_none()
    if not faktura:
        raise HTTPException(status_code=404, detail=f"Faktura ID={faktura_id} nie istnieje.")

    # Pobierz aktywne przypisania do dezaktywacji
    stmt = select(FakturaPrzypisanie).where(
        FakturaPrzypisanie.faktura_id == faktura_id,
        FakturaPrzypisanie.is_active == True,  # noqa: E712
    )
    res = await db.execute(stmt)
    stare = res.scalars().all()
    dezaktywowane_ids = [p.user_id for p in stare]

    # Dezaktywuj stare
    now = datetime.now().replace(tzinfo=None)
    for p in stare:
        p.is_active = False
        p.updated_at = now

    # Nowe przypisania
    for user_id in nowe_user_ids:
        db.add(FakturaPrzypisanie(
            faktura_id=faktura_id,
            user_id=user_id,
            status="oczekuje",
            is_active=True,
            created_at=now,
        ))

    # Reset statusu faktury do w_toku
    faktura.status_wewnetrzny = "w_toku"
    faktura.updated_at = now

    await _log_event(
        db=db,
        faktura_id=faktura_id,
        user_id=actor_id,
        akcja=AkcjaLog.ZRESETOWANO,
        before={"przypisani": dezaktywowane_ids},
        after={"przypisani": nowe_user_ids},
        meta={
            "numer_ksef": faktura.numer_ksef,
            "powod": powod,
            "dezaktywowane": dezaktywowane_ids,
            "nowe": nowe_user_ids,
        },
        actor_name=actor_name,
        actor_ip=actor_ip,
        request_id=request_id,
        endpoint=f"/faktury-akceptacja/{faktura_id}/reset/confirm",
    )

    await db.commit()
    await _invalidate_faktura_cache(redis, faktura_id)

    # SSE push faktura_zresetowana → dezaktywowani pracownicy
    await _sse_push_zresetowana(
        redis=redis,
        faktura=faktura,
        dezaktywowane_ids=dezaktywowane_ids,
    )

    logger.info(orjson.dumps({
        "event":       "reset_confirmed",
        "faktura_id":  faktura_id,
        "actor_id":    actor_id,
        "dezakt":      dezaktywowane_ids,
        "nowe":        nowe_user_ids,
        "request_id":  request_id,
    }).decode())

    return FakturaResetResponse(
        faktura_id=faktura_id,
        dezaktywowane=dezaktywowane_ids,
        nowe_przypisania=nowe_user_ids,
    )

# ─────────────────────────────────────────────────────────────────────────────
# 6. PATCH /status — krok 1
# ─────────────────────────────────────────────────────────────────────────────

async def initiate_force_status(
    *,
    db: AsyncSession,
    redis: Redis,
    faktura_id: int,
    body: FakturaForceStatusRequest,
    actor_id: int,
    actor_name: str,
    actor_ip: str,
    request_id: str,
) -> ConfirmTokenResponse:
    """Krok 1 force_status: walidacja + JWT confirm_token."""
    result = await db.execute(
        select(FakturaAkceptacja).where(
            FakturaAkceptacja.id == faktura_id,
            FakturaAkceptacja.is_active == True,  # noqa: E712
        )
    )
    faktura = result.scalar_one_or_none()
    if not faktura:
        raise HTTPException(status_code=404, detail=f"Faktura ID={faktura_id} nie istnieje.")

    nowy_status = body.nowy_status if isinstance(body.nowy_status, str) else body.nowy_status.value

    if faktura.status_wewnetrzny == nowy_status:
        raise HTTPException(
            status_code=409,
            detail=f"Faktura już ma status {nowy_status!r}.",
        )

    scope = (
        "confirm_force_akceptacja"
        if nowy_status == "zaakceptowana"
        else "confirm_anuluj_faktura"
    )

    ttl = int(await get_config_value(
        redis=redis, key="faktury.confirm_token_ttl_seconds", default="60"
    ))

    token = await create_one_time_token(
        redis=redis,
        scope=scope,
        entity_type="FakturaAkceptacja",
        entity_id=faktura_id,
        requested_by=actor_id,
        ttl_seconds=ttl,
        extra_payload={"nowy_status": nowy_status, "powod": body.powod},
    )

    return ConfirmTokenResponse(
        confirm_token=token,
        expires_in=ttl,
        action="force_status",
        message=f"Potwierdź zmianę statusu na '{nowy_status}' w ciągu {ttl} sekund.",
    )

# ─────────────────────────────────────────────────────────────────────────────
# 7. POST /status/confirm — krok 2
# ─────────────────────────────────────────────────────────────────────────────

async def confirm_force_status(
    *,
    db: AsyncSession,
    redis: Redis,
    faktura_id: int,
    confirm_token: str,
    actor_id: int,
    actor_name: str,
    actor_ip: str,
    request_id: str,
) -> dict[str, Any]:
    """Krok 2 force_status: weryfikacja tokenu → zmiana statusu."""
    # Weryfikuj (scope zależy od wartości — sprawdź oba)
    payload = None
    for scope in ("confirm_force_akceptacja", "confirm_anuluj_faktura"):
        try:
            payload = await verify_one_time_token(
                redis=redis,
                token=confirm_token,
                expected_scope=scope,
                expected_entity_type="FakturaAkceptacja",
                expected_entity_id=faktura_id,
                requesting_user_id=actor_id,
            )
            break
        except HTTPException:
            continue

    if payload is None:
        raise HTTPException(
            status_code=400,
            detail="Token potwierdzający jest nieprawidłowy lub wygasł.",
        )

    nowy_status: str = payload.get("nowy_status", "zaakceptowana")
    powod: Optional[str] = payload.get("powod")

    result = await db.execute(
        select(FakturaAkceptacja).where(FakturaAkceptacja.id == faktura_id)
    )
    faktura = result.scalar_one_or_none()
    if not faktura:
        raise HTTPException(status_code=404, detail=f"Faktura ID={faktura_id} nie istnieje.")

    stary_status = faktura.status_wewnetrzny
    now = datetime.now().replace(tzinfo=None)

    faktura.status_wewnetrzny = nowy_status
    faktura.updated_at = now

    # Anulowanie → soft-delete + archiwum
    if nowy_status == "anulowana":
        faktura.is_active = False
        await _archive_faktura(faktura)

    akcja = (
        AkcjaLog.FORCE_AKCEPTACJA
        if nowy_status == "zaakceptowana"
        else AkcjaLog.ANULOWANO
    )

    await _log_event(
        db=db,
        faktura_id=faktura_id,
        user_id=actor_id,
        akcja=akcja,
        before={"status_wewnetrzny": stary_status},
        after={"status_wewnetrzny": nowy_status},
        meta={"numer_ksef": faktura.numer_ksef, "powod": powod},
        actor_name=actor_name,
        actor_ip=actor_ip,
        request_id=request_id,
        endpoint=f"/faktury-akceptacja/{faktura_id}/status/confirm",
    )

    await db.commit()
    await _invalidate_faktura_cache(redis, faktura_id)

    logger.info(orjson.dumps({
        "event":      "force_status_confirmed",
        "faktura_id": faktura_id,
        "stary":      stary_status,
        "nowy":       nowy_status,
        "actor_id":   actor_id,
        "request_id": request_id,
    }).decode())

    return {
        "faktura_id":       faktura_id,
        "status_wewnetrzny": nowy_status,
        "message": f"Status faktury zmieniony na '{nowy_status}'.",
    }

# ─────────────────────────────────────────────────────────────────────────────
# 8. GET /historia
# ─────────────────────────────────────────────────────────────────────────────

async def get_historia(
    *,
    db: AsyncSession,
    redis: Redis,
    faktura_id: int,
) -> FakturaHistoriaResponse:
    """Historia zdarzeń z skw_faktura_log. Cache Redis 300s."""
    cache_key = _cache_key_historia(faktura_id)

    try:
        cached = await redis.get(cache_key)
        if cached:
            data = orjson.loads(cached)
            return FakturaHistoriaResponse(**data)
    except Exception:
        pass

    result = await db.execute(
        select(FakturaLog)
        .where(FakturaLog.faktura_id == faktura_id)
        .order_by(FakturaLog.created_at.desc())
    )
    logs = result.scalars().all()

    items = []
    for log in logs:
        szczegoly = {}
        if log.szczegoly:
            try:
                szczegoly = json.loads(log.szczegoly)
            except Exception:
                pass

        actor = szczegoly.get("actor", {})
        before = szczegoly.get("before", {})
        after  = szczegoly.get("after", {})

        items.append(FakturaLogItemResponse(
            id=log.id,
            faktura_id=log.faktura_id,
            user_id=log.user_id,
            akcja=AkcjaLog(log.akcja),
            created_at=log.created_at,
            actor_username=actor.get("username"),
            before_status=before.get("status_wewnetrzny") if before else None,
            after_status=after.get("status_wewnetrzny") if after else None,
        ))

    response = FakturaHistoriaResponse(
        faktura_id=faktura_id,
        total=len(items),
        items=items,
    )

    try:
        await redis.setex(cache_key, 300, orjson.dumps(response.model_dump()))
    except Exception:
        pass

    return response

# ─────────────────────────────────────────────────────────────────────────────
# 9. GET /pdf
# ─────────────────────────────────────────────────────────────────────────────

async def get_faktura_pdf(
    *,
    db: AsyncSession,
    redis: Redis,
    faktura_id: int,
    actor_id: int,
) -> bytes:
    """
    Generuje PDF wizualizacji faktury (karta akceptacji).
    Cache Redis TTL z SystemConfig: faktury.pdf_cache_ttl_seconds.
    ReportLab in-memory — brak zapisu na dysk.
    """
    result = await db.execute(
        select(FakturaAkceptacja).where(FakturaAkceptacja.id == faktura_id)
    )
    faktura = result.scalar_one_or_none()
    if not faktura:
        raise HTTPException(status_code=404, detail=f"Faktura ID={faktura_id} nie istnieje.")

    # Cache key bazuje na updated_at (świeżość danych)
    data_hash = hashlib.md5(
        f"{faktura_id}:{faktura.updated_at}:{faktura.status_wewnetrzny}".encode()
    ).hexdigest()[:12]
    cache_key = _cache_key_pdf(faktura_id, data_hash)

    try:
        cached_pdf = await redis.get(cache_key)
        if cached_pdf:
            logger.debug(f"PDF cache hit: faktura_id={faktura_id}")
            return cached_pdf
    except Exception:
        pass

    # Pobierz dane WAPRO
    # ET-01: sprawdź czy faktura nadal jest w WAPRO
    await _handle_orphan_if_needed(db=db, redis=redis, faktura=faktura)

    wapro = await _get_wapro_naglowek(faktura.numer_ksef)

    # Pobierz przypisania
    res_p = await db.execute(
        select(FakturaPrzypisanie).where(
            FakturaPrzypisanie.faktura_id == faktura_id
        ).order_by(FakturaPrzypisanie.created_at)
    )
    przypisania = res_p.scalars().all()

    # Generuj PDF przez faktura_pdf_service
    from app.services.faktura_pdf_service import generate_pdf
    pdf_bytes = await generate_pdf(
        faktura=faktura,
        wapro=wapro,
        przypisania=przypisania,
    )

    # Cache PDF
    try:
        ttl = int(await get_config_value(
            redis=redis, key="faktury.pdf_cache_ttl_seconds", default="300"
        ))
        await redis.setex(cache_key, ttl, pdf_bytes)
    except Exception:
        pass

    return pdf_bytes

# ─────────────────────────────────────────────────────────────────────────────
# SSE helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _sse_push_nowa_faktura(
    redis: Redis,
    faktura: FakturaAkceptacja,
    user_ids: list[int],
) -> None:
    """SSE push: nowa_faktura → przypisani pracownicy."""
    settings = get_settings()
    if getattr(settings, "DEMO_MODE", False):
        return
    try:
        sse_enabled = await get_config_value(
            redis=redis, key="faktury.powiadomienia_sse_enabled", default="true"
        )
        if str(sse_enabled).lower() != "true":
            return
    except Exception:
        return

    # Pobierz dane WAPRO do payloadu (raz, przed pętlą)
    wapro_dane = None
    try:
        wapro_dane = await _get_wapro_naglowek(faktura.numer_ksef)
    except Exception as exc:
        logger.warning(f"SSE nowa_faktura: brak danych WAPRO dla {faktura.numer_ksef}: {exc}")

    event = orjson.dumps({
        "type": "nowa_faktura",
        "data": {
            "faktura_id":        faktura.id,
            "numer_ksef":        faktura.numer_ksef,
            "numer":             wapro_dane.numer if wapro_dane else None,
            "priorytet":         faktura.priorytet,
            "nazwa_kontrahenta": wapro_dane.nazwa_kontrahenta if wapro_dane else None,
            "opis_skrocony":     (faktura.opis_dokumentu or "")[:120],
        },
    })

    for user_id in user_ids:
        await publish_faktura_event(
            redis=redis,
            user_id=user_id,
            event_type="nowa_faktura",
            data={
                "faktura_id":        faktura.id,
                "numer_ksef":        faktura.numer_ksef,
                "numer":             wapro_dane.numer if wapro_dane else None,
                "priorytet":         faktura.priorytet,
                "nazwa_kontrahenta": wapro_dane.nazwa_kontrahenta if wapro_dane else None,
                "opis_skrocony":     (faktura.opis_dokumentu or "")[:120],
            },
        )


async def _sse_push_zresetowana(
    redis: Redis,
    faktura: FakturaAkceptacja,
    dezaktywowane_ids: list[int],
) -> None:
    """SSE push: faktura_zresetowana → dezaktywowani pracownicy."""
    settings = get_settings()
    if getattr(settings, "DEMO_MODE", False):
        return
    try:
        sse_enabled = await get_config_value(
            redis=redis, key="faktury.powiadomienia_sse_enabled", default="true"
        )
        if str(sse_enabled).lower() != "true":
            return
    except Exception:
        return

    # Pobierz numer dokumentu z WAPRO
    wapro_dane = None
    try:
        wapro_dane = await _get_wapro_naglowek(faktura.numer_ksef)
    except Exception as exc:
        logger.warning(f"SSE zresetowana: brak danych WAPRO dla {faktura.numer_ksef}: {exc}")

    event = orjson.dumps({
        "type": "faktura_zresetowana",
        "data": {
            "faktura_id": faktura.id,
            "numer_ksef": faktura.numer_ksef,
            "numer":      wapro_dane.numer if wapro_dane else None,
        },
    })

    for user_id in dezaktywowane_ids:
        await publish_faktura_event(
            redis=redis,
            user_id=user_id,
            event_type="faktura_zresetowana",
            data={
                "faktura_id": faktura.id,
                "numer_ksef": faktura.numer_ksef,
                "numer":      wapro_dane.numer if wapro_dane else None,
            },
        )

# ─────────────────────────────────────────────────────────────────────────────
# Archiwum JSON.gz przy anulowaniu
# ─────────────────────────────────────────────────────────────────────────────

async def _archive_faktura(faktura: FakturaAkceptacja) -> None:
    """
    Tworzy archiwum JSON.gz anulowanej faktury.
    Zapis do archives/ — immutable, nigdy nie usuwane.
    """
    import os
    from pathlib import Path

    archive_data = {
        "id":                faktura.id,
        "numer_ksef":        faktura.numer_ksef,
        "status_wewnetrzny": faktura.status_wewnetrzny,
        "priorytet":         faktura.priorytet,
        "opis_dokumentu":    faktura.opis_dokumentu,
        "uwagi":             faktura.uwagi,
        "utworzony_przez":   faktura.utworzony_przez,
        "created_at":        faktura.created_at.isoformat() if faktura.created_at else None,
        "archived_at":       datetime.now(timezone.utc).isoformat(),
    }

    try:
        archive_dir = Path("archives") / datetime.now().strftime("%Y-%m-%d")
        archive_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = archive_dir / f"faktura_{faktura.id}_{ts}.json.gz"

        with gzip.open(path, "wb") as f:
            f.write(orjson.dumps(archive_data))

        logger.info(f"Faktura ID={faktura.id} zarchiwizowana: {path}")
    except Exception as exc:
        logger.error(f"Archiwizacja faktury ID={faktura.id} nieudana: {exc}")