"""
app/services/moje_faktury_service.py
=====================================
Logika biznesowa pracownika — moduł Akceptacji Faktur KSeF.

Funkcje publiczne:
    get_moje_faktury_list()    — lista przypisanych faktur (cache 60s per user)
    get_moja_faktura_detail()  — szczegóły faktury + weryfikacja przypisania
    zapisz_decyzje()           — decyzja pracownika + trigger saga Fakira
    check_przypisanie()        — czy faktura przypisana do usera (helper dla PDF)

LOGIKA BIZNESOWA (Sprint 2, Sekcja 6):
    Po każdej decyzji:
    1. Zapisz status w skw_faktura_przypisanie
    2. SSE push: faktura_zdecydowana → referent (zawsze)
    3. Sprawdź czy wszyscy odmówili → SSE: faktura_wymaga_interwencji (warunkowo)
    4. Sprawdź czy wszyscy zaakceptowali → trigger saga Fakira (warunkowo)

    Powód interwencji (wszyscy_odmowili / wszyscy_nie_moje / mieszane):
    - wszyscy_odmowili:  wszyscy mają status=odrzucone
    - wszyscy_nie_moje:  wszyscy mają status=nie_moje
    - mieszane:          mix odrzucone/nie_moje, żaden nie zaakceptował

    Saga Fakira wywoływana z fakir_service.py — nie bezpośrednio.

CACHE Redis:
    faktura:moje:{user_id}:{page} TTL 60s → inwalidacja po decyzji/przypisaniu
    faktura:detail:{id}           TTL 120s → inwalidacja po decyzji

AUDIT:
    Komentarz pracownika → skw_faktura_log.szczegoly (pełna treść)
    Komentarz pracownika → skw_AuditLog.Details = {komentarz_sha256: "..."} (tylko hash)
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import orjson
from fastapi import HTTPException, status
from redis.asyncio import Redis
from sqlalchemy import and_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.models.faktura_akceptacja import (
    FakturaAkceptacja,
    FakturaLog,
    FakturaPrzypisanie,
)
from app.schemas.faktura_akceptacja import (
    AkcjaLog,
    DecyzjaRequest,
    DecyzjaResponse,
    FakturaDetailResponse,
    FakturaLogDetails,
    PowodInterwencji,
    PrzypisanieResponse,
    StatusPrzypisania,
    StatusWewnetrzny,
    WaproFakturaNaglowek,
    WaproFakturaPozycja,
)
from app.services.config_service import get_config_value
from app.services.event_service import publish_faktura_event
from app.services.faktura_akceptacja_service import (
    _get_wapro_naglowek,
    _log_event,
)

logger = logging.getLogger("app.services.moje_faktury")

# ─────────────────────────────────────────────────────────────────────────────
# Cache key helpers
# ─────────────────────────────────────────────────────────────────────────────

def _cache_key_moje(user_id: int, page: int, archiwum: bool) -> str:
    arch = "arch" if archiwum else "akt"
    return f"faktura:moje:{user_id}:{page}:{arch}"

def _cache_key_detail(faktura_id: int) -> str:
    return f"faktura:detail:{faktura_id}"

async def _invalidate_user_cache(redis: Redis, user_id: int) -> None:
    try:
        keys = await redis.keys(f"faktura:moje:{user_id}:*")
        if keys:
            await redis.delete(*keys)
    except Exception as exc:
        logger.warning(f"Cache invalidation user_id={user_id}: {exc}")

# ─────────────────────────────────────────────────────────────────────────────
# 1. GET /moje-faktury — lista przypisanych
# ─────────────────────────────────────────────────────────────────────────────

async def get_moje_faktury_list(
    *,
    db: AsyncSession,
    redis: Redis,
    user_id: int,
    page: int = 1,
    limit: int = 50,
    archiwum: bool = False,
) -> dict[str, Any]:
    """
    Lista faktur przypisanych do zalogowanego usera.
    archiwum=False: tylko is_active=1 + status=oczekuje
    archiwum=True:  wszystkie przypisania (w tym zdecydowane i nieaktywne)
    Cache Redis 60s per user per page.
    """
    cache_key = _cache_key_moje(user_id, page, archiwum)

    try:
        cached = await redis.get(cache_key)
        if cached:
            return orjson.loads(cached)
    except Exception:
        pass

    # Buduj query przypisań
    stmt = select(FakturaPrzypisanie).where(
        FakturaPrzypisanie.user_id == user_id,
    )
    if not archiwum:
        stmt = stmt.where(
            FakturaPrzypisanie.is_active == True,  # noqa: E712
            FakturaPrzypisanie.status == "oczekuje",
        )
    stmt = stmt.order_by(FakturaPrzypisanie.created_at.desc())

    result = await db.execute(stmt)
    przypisania = result.scalars().all()

    total = len(przypisania)
    offset = (page - 1) * limit
    strona = przypisania[offset : offset + limit]

    items = []
    for p in strona:
        # Pobierz fakturę
        f_res = await db.execute(
            select(FakturaAkceptacja).where(FakturaAkceptacja.id == p.faktura_id)
        )
        faktura = f_res.scalar_one_or_none()
        if not faktura:
            continue

        wapro = await _get_wapro_naglowek(faktura.numer_ksef)

        items.append({
            "id":                faktura.id,
            "numer_ksef":        faktura.numer_ksef,
            "status_wewnetrzny": faktura.status_wewnetrzny,
            "priorytet":         faktura.priorytet,
            "moj_status":        p.status,
            "is_active":         p.is_active,
            "created_at":        p.created_at.isoformat() if p.created_at else None,
            "decided_at":        p.decided_at.isoformat() if p.decided_at else None,
            "numer":             wapro.numer if wapro else None,
            "wartosc_brutto":    float(wapro.wartosc_brutto) if wapro and wapro.wartosc_brutto else None,
            "nazwa_kontrahenta": wapro.nazwa_kontrahenta if wapro else None,
            "termin_platnosci":  wapro.termin_platnosci.isoformat() if wapro and wapro.termin_platnosci else None,
        })

    response = {"items": items, "total": total}

    try:
        await redis.setex(cache_key, 60, orjson.dumps(response))
    except Exception:
        pass

    return response

# ─────────────────────────────────────────────────────────────────────────────
# 2. GET /moje-faktury/{id} — szczegóły
# ─────────────────────────────────────────────────────────────────────────────

async def get_moja_faktura_detail(
    *,
    db: AsyncSession,
    redis: Redis,
    faktura_id: int,
    user_id: int,
) -> dict[str, Any]:
    """
    Szczegóły faktury. Weryfikacja: faktura MUSI być przypisana do usera.
    Cache Redis 120s.
    """
    # Sprawdź przypisanie
    is_assigned = await check_przypisanie(db=db, faktura_id=faktura_id, user_id=user_id)
    if not is_assigned:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Faktura nie jest przypisana do Ciebie.",
        )

    cache_key = _cache_key_detail(faktura_id)
    try:
        cached = await redis.get(cache_key)
        if cached:
            data = orjson.loads(cached)
            # Dodaj mój status z przypisania (nie cachujemy per-user)
            moje_przypisanie = await _get_moje_przypisanie(db, faktura_id, user_id)
            data["moj_status"] = moje_przypisanie.status if moje_przypisanie else None
            return data
    except Exception:
        pass

    result = await db.execute(
        select(FakturaAkceptacja).where(FakturaAkceptacja.id == faktura_id)
    )
    faktura = result.scalar_one_or_none()
    if not faktura:
        raise HTTPException(status_code=404, detail=f"Faktura ID={faktura_id} nie istnieje.")

    # Dane WAPRO
    wapro = await _get_wapro_naglowek(faktura.numer_ksef)

    # Pozycje WAPRO
    pozycje = await _get_wapro_pozycje(faktura.numer_ksef)

    # Przypisania
    res_p = await db.execute(
        select(FakturaPrzypisanie).where(FakturaPrzypisanie.faktura_id == faktura_id)
    )
    wszystkie_przypisania = res_p.scalars().all()

    # Mój status przypisania
    moje = next((p for p in wszystkie_przypisania if p.user_id == user_id), None)

    detail = {
        "id":                faktura.id,
        "numer_ksef":        faktura.numer_ksef,
        "status_wewnetrzny": faktura.status_wewnetrzny,
        "priorytet":         faktura.priorytet,
        "opis_dokumentu":    faktura.opis_dokumentu,
        "uwagi":             faktura.uwagi,
        "is_active":         faktura.is_active,
        "moj_status":        moje.status if moje else None,
        "created_at":        faktura.created_at.isoformat() if faktura.created_at else None,
        "updated_at":        faktura.updated_at.isoformat() if faktura.updated_at else None,
        # WAPRO
        "numer":             wapro.numer if wapro else None,
        "wartosc_netto":     float(wapro.wartosc_netto) if wapro and wapro.wartosc_netto else None,
        "wartosc_brutto":    float(wapro.wartosc_brutto) if wapro and wapro.wartosc_brutto else None,
        "kwota_vat":         float(wapro.kwota_vat) if wapro and wapro.kwota_vat else None,
        "forma_platnosci":   wapro.forma_platnosci if wapro else None,
        "termin_platnosci":  wapro.termin_platnosci.isoformat() if wapro and wapro.termin_platnosci else None,
        "nazwa_kontrahenta": wapro.nazwa_kontrahenta if wapro else None,
        "pozycje":           pozycje,
    }

    # Cache (bez moj_status — jest per-user)
    cache_data = {k: v for k, v in detail.items() if k != "moj_status"}
    try:
        await redis.setex(cache_key, 120, orjson.dumps(cache_data))
    except Exception:
        pass

    return detail

# ─────────────────────────────────────────────────────────────────────────────
# 3. POST /moje-faktury/{id}/decyzja — decyzja pracownika
# ─────────────────────────────────────────────────────────────────────────────

async def zapisz_decyzje(
    *,
    db: AsyncSession,
    redis: Redis,
    faktura_id: int,
    body: DecyzjaRequest,
    actor_id: int,
    actor_name: str,
    actor_ip: str,
    request_id: str,
) -> DecyzjaResponse:
    """
    Zapisuje decyzję pracownika (zaakceptowane / odrzucone / nie_moje).

    Kolejność:
    1. Walidacja przypisania i stanu faktury
    2. UPDATE skw_faktura_przypisanie
    3. AuditLog (hash komentarza — nie treść)
    4. skw_faktura_log (pełna treść komentarza)
    5. SSE: faktura_zdecydowana → referent (zawsze)
    6. SSE: faktura_wymaga_interwencji → referent (warunkowo)
    7. Trigger saga Fakira (warunkowo — jeśli wszyscy zaakceptowali)
    """
    # 1. Walidacja przypisania
    p_result = await db.execute(
        select(FakturaPrzypisanie).where(
            FakturaPrzypisanie.faktura_id == faktura_id,
            FakturaPrzypisanie.user_id == actor_id,
            FakturaPrzypisanie.is_active == True,  # noqa: E712
        )
    )
    przypisanie = p_result.scalar_one_or_none()
    if not przypisanie:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Nie masz aktywnego przypisania do tej faktury.",
        )

    if przypisanie.status != "oczekuje":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Już podjąłeś decyzję dla tej faktury: '{przypisanie.status}'.",
        )

    # Pobierz fakturę
    f_result = await db.execute(
        select(FakturaAkceptacja).where(FakturaAkceptacja.id == faktura_id)
    )
    faktura = f_result.scalar_one_or_none()
    if not faktura:
        raise HTTPException(status_code=404, detail=f"Faktura ID={faktura_id} nie istnieje.")

    if faktura.status_wewnetrzny in ("zaakceptowana", "anulowana"):
        raise HTTPException(
            status_code=409,
            detail=f"Faktura ma już status '{faktura.status_wewnetrzny}' — nie możesz zmieniać decyzji.",
        )

    # 2. UPDATE przypisania
    nowy_status = body.status if isinstance(body.status, str) else body.status.value
    now = datetime.now().replace(tzinfo=None)

    przypisanie.status    = nowy_status
    przypisanie.komentarz = body.komentarz
    przypisanie.decided_at = now
    przypisanie.updated_at = now

    # Ustaw fakturę w_toku jeśli była nowe
    if faktura.status_wewnetrzny == "nowe":
        faktura.status_wewnetrzny = "w_toku"
        faktura.updated_at = now

    # 3+4. Log (komentarz tylko do faktura_log, hash do AuditLog via meta)
    await _log_event(
        db=db,
        faktura_id=faktura_id,
        user_id=actor_id,
        akcja=_akcja_dla_statusu(nowy_status),
        before={"status": "oczekuje"},
        after={"status": nowy_status},
        meta={
            "numer_ksef":     faktura.numer_ksef,
            "komentarz":      body.komentarz,   # pełna treść — tylko w faktura_log
            "komentarz_sha256": body.komentarz_hash(),
        },
        actor_name=actor_name,
        actor_ip=actor_ip,
        request_id=request_id,
        endpoint=f"/moje-faktury/{faktura_id}/decyzja",
    )

    await db.commit()

    # Inwalidacja cache
    await _invalidate_user_cache(redis, actor_id)
    try:
        await redis.delete(_cache_key_detail(faktura_id))
    except Exception:
        pass

    logger.info(orjson.dumps({
        "event":       "decyzja_zapisana",
        "faktura_id":  faktura_id,
        "user_id":     actor_id,
        "status":      nowy_status,
        "request_id":  request_id,
        "ts":          datetime.now(timezone.utc).isoformat(),
    }).decode())

    # 5. SSE: faktura_zdecydowana → referent
    # Pobierz numer dokumentu z WAPRO dla payloadu
    _wapro_numer: Optional[str] = None
    try:
        _wapro_tmp = await _get_wapro_naglowek(faktura.numer_ksef)
        _wapro_numer = _wapro_tmp.numer if _wapro_tmp else None
    except Exception:
        pass

    await _sse_faktura_zdecydowana(
        redis=redis,
        faktura=faktura,
        pracownik_id=actor_id,
        pracownik_name=actor_name,
        decyzja=nowy_status,
        wapro_numer=_wapro_numer,
    )

    # Pobierz wszystkie aktywne przypisania po decyzji
    all_res = await db.execute(
        select(FakturaPrzypisanie).where(
            FakturaPrzypisanie.faktura_id == faktura_id,
            FakturaPrzypisanie.is_active == True,  # noqa: E712
        )
    )
    aktywne = all_res.scalars().all()

    # 6. Sprawdź czy wszyscy odmówili → SSE interwencja
    powod = _oblicz_powod_interwencji(aktywne)
    if powod is not None:
        await _sse_faktura_wymaga_interwencji(
            redis=redis,
            faktura=faktura,
            powod=powod,
        )

    # 7. Trigger saga Fakira (jeśli wszyscy zaakceptowali)
    fakir_updated = False
    wapro = None
    if nowy_status == "zaakceptowane":
        try:
            from app.services.fakir_service import trigger_fakir_update_if_complete
            wapro = await _get_wapro_naglowek(faktura.numer_ksef)
            fakir_result = await trigger_fakir_update_if_complete(
                db=db,
                redis=redis,
                faktura_id=faktura_id,
                numer_ksef=faktura.numer_ksef,
                actor_id=actor_id,
                actor_name=actor_name,
                actor_ip=actor_ip,
                request_id=request_id,
                wapro_numer=wapro.numer if wapro else None,
                wapro_nazwa_kontrahenta=wapro.nazwa_kontrahenta if wapro else None,
            )
            if fakir_result and fakir_result.success and not fakir_result.blocked_by_config:
                fakir_updated = True
        except Exception as exc:
            logger.error(f"Trigger Fakira error faktura_id={faktura_id}: {exc}")

    # Odśwież status faktury po sadze
    await db.refresh(faktura)

    return DecyzjaResponse(
        faktura_id=faktura_id,
        twoja_decyzja=StatusPrzypisania(nowy_status),
        faktura_status=StatusWewnetrzny(faktura.status_wewnetrzny),
        fakir_updated=fakir_updated,
        message=_message_dla_decyzji(nowy_status, fakir_updated),
    )

# ─────────────────────────────────────────────────────────────────────────────
# 4. check_przypisanie — helper dla PDF endpoint
# ─────────────────────────────────────────────────────────────────────────────

async def check_przypisanie(
    *,
    db: AsyncSession,
    faktura_id: int,
    user_id: int,
) -> bool:
    """Sprawdza czy user ma aktywne przypisanie do faktury."""
    result = await db.execute(
        select(FakturaPrzypisanie).where(
            FakturaPrzypisanie.faktura_id == faktura_id,
            FakturaPrzypisanie.user_id == user_id,
            FakturaPrzypisanie.is_active == True,  # noqa: E712
        )
    )
    return result.scalar_one_or_none() is not None

# ─────────────────────────────────────────────────────────────────────────────
# Logika: powód interwencji
# ─────────────────────────────────────────────────────────────────────────────

def _oblicz_powod_interwencji(
    aktywne: list[FakturaPrzypisanie],
) -> Optional[PowodInterwencji]:
    """
    Sprawdza czy wszyscy aktywni odmówili (odrzucone lub nie_moje).
    Zwraca PowodInterwencji lub None jeśli ktoś jeszcze oczekuje lub zaakceptował.
    """
    if not aktywne:
        return None

    statusy = {p.status for p in aktywne}

    # Jeśli ktokolwiek zaakceptował lub oczekuje — brak interwencji
    if "zaakceptowane" in statusy or "oczekuje" in statusy:
        return None

    # Wszyscy mają odrzucone lub nie_moje
    if statusy == {"odrzucone"}:
        return PowodInterwencji.WSZYSCY_ODMOWILI
    if statusy == {"nie_moje"}:
        return PowodInterwencji.WSZYSCY_NIE_MOJE
    return PowodInterwencji.MIESZANE

def _akcja_dla_statusu(status: str) -> AkcjaLog:
    mapping = {
        "zaakceptowane": AkcjaLog.ZAAKCEPTOWANO,
        "odrzucone":     AkcjaLog.ODRZUCONO,
        "nie_moje":      AkcjaLog.NIE_MOJE,
    }
    return mapping.get(status, AkcjaLog.STATUS_ZMIENIONY)

def _message_dla_decyzji(status: str, fakir_updated: bool) -> str:
    if status == "zaakceptowane":
        if fakir_updated:
            return "Zaakceptowano. Faktura zatwierdzona w Fakirze (KOD_STATUSU='K')."
        return "Zaakceptowano. Oczekiwanie na pozostałych akceptantów."
    if status == "odrzucone":
        return "Faktura odrzucona. Referent zostanie powiadomiony."
    return "Faktura oznaczona jako 'nie moja'. Referent zostanie powiadomiony."

# ─────────────────────────────────────────────────────────────────────────────
# WAPRO pozycje
# ─────────────────────────────────────────────────────────────────────────────

async def _get_wapro_pozycje(numer_ksef: str) -> list[dict[str, Any]]:
    """Pobiera pozycje faktury z widoku WAPRO."""
    try:
        from app.db.wapro import execute_query
        rows = await execute_query(
            query_type="faktura_pozycje",
            params={"ksef_id": numer_ksef},
        )
        result = []
        for r in rows:
            result.append({
                "id_buf_dokument": r.get("ID_BUF_DOKUMENT"),
                "numer_pozycji":   r.get("NumerPozycji"),
                "nazwa_towaru":    r.get("NazwaTowaru"),
                "ilosc":           float(r["Ilosc"]) if r.get("Ilosc") is not None else None,
                "jednostka":       r.get("Jednostka"),
                "cena_netto":      float(r["CenaNetto"]) if r.get("CenaNetto") is not None else None,
                "wartosc_netto":   float(r["WartoscNetto"]) if r.get("WartoscNetto") is not None else None,
                "wartosc_brutto":  float(r["WartoscBrutto"]) if r.get("WartoscBrutto") is not None else None,
                "stawka_vat":      r.get("StawkaVAT"),
            })
        return result
    except Exception as exc:
        logger.warning(f"WAPRO pozycje error ksef_id={numer_ksef}: {exc}")
        return []

async def _get_moje_przypisanie(
    db: AsyncSession,
    faktura_id: int,
    user_id: int,
) -> Optional[FakturaPrzypisanie]:
    result = await db.execute(
        select(FakturaPrzypisanie).where(
            FakturaPrzypisanie.faktura_id == faktura_id,
            FakturaPrzypisanie.user_id == user_id,
        ).order_by(FakturaPrzypisanie.created_at.desc())
    )
    return result.scalars().first()

# ─────────────────────────────────────────────────────────────────────────────
# SSE helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _sse_faktura_zdecydowana(
    redis: Redis,
    faktura: FakturaAkceptacja,
    pracownik_id: int,
    pracownik_name: str,
    decyzja: str,
    wapro_numer: Optional[str] = None,
) -> None:
    """SSE push: faktura_zdecydowana → referent (twórca obiegu)."""
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

    event = orjson.dumps({
        "type": "faktura_zdecydowana",
        "data": {
            "faktura_id": faktura.id,
            "numer_ksef": faktura.numer_ksef,
            "numer":      wapro_numer,
            "user_id":    pracownik_id,
            "user_name":  pracownik_name,
            "decyzja":    decyzja,
        },
    })

    await publish_faktura_event(
        redis=redis,
        user_id=faktura.utworzony_przez,
        event_type="faktura_zdecydowana",
        data={
            "faktura_id": faktura.id,
            "numer_ksef": faktura.numer_ksef,
            "numer":      wapro_numer,
            "user_id":    pracownik_id,
            "user_name":  pracownik_name,
            "decyzja":    decyzja,
        },
    )


async def _sse_faktura_wymaga_interwencji(
    redis: Redis,
    faktura: FakturaAkceptacja,
    powod: PowodInterwencji,
) -> None:
    """SSE push: faktura_wymaga_interwencji → referent."""
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

    event = orjson.dumps({
        "type": "faktura_wymaga_interwencji",
        "data": {
            "faktura_id": faktura.id,
            "numer_ksef": faktura.numer_ksef,
            "powod":      powod.value,
        },
    })

    await publish_faktura_event(
        redis=redis,
        user_id=faktura.utworzony_przez,
        event_type="faktura_wymaga_interwencji",
        data={
            "faktura_id": faktura.id,
            "numer_ksef": faktura.numer_ksef,
            "powod":      powod.value,
        },
    )