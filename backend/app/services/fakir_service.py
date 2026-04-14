"""
app/services/fakir_service.py
=============================
Saga UPDATE BUF_DOKUMENT — krok finalny akceptacji faktury w WAPRO/Fakirze.

KRYTYCZNA KOLEJNOŚĆ (Sprint 2, Sekcja 6.1 — SAGA PATTERN):
    1. Decyzja pracownika już zapisana (skw_faktura_przypisanie.status)
    2. Redis LOCK: faktura:lock:{faktura_id} TTL 30s
    3. Odśwież stan — jeśli już zaakceptowana: return (inny request wygrał)
    4. UPDATE dbo.BUF_DOKUMENT SET KOD_STATUSU='K' (przez fakir_write.py)
    5. Weryfikacja: SELECT KOD_STATUSU po UPDATE
    6. UPDATE skw_faktura_akceptacja SET status_wewnetrzny='zaakceptowana'
    7. COMMIT
    8. SSE broadcast (event: faktura_zakonczona)

    ⚠ NIGDY nie zmieniaj status_wewnetrzny PRZED potwierdzeniem UPDATE Fakira.

WŁĄCZNIKI (sprawdzane w kolejności):
    1. DEMO_MODE (settings) — blokuje zawsze
    2. faktury.fakir_update_enabled (SystemConfig) — blokuje jeśli false
    Oba blokują bez wyjątku — zwracają FakirUpdateResult z flagą blocked_*.

RETRY:
    Delegowane do fakir_write.update_kod_statusu().
    Liczba prób z SystemConfig: faktury.fakir_retry_attempts (domyślnie 3).

RACE CONDITION (6.2):
    Redis SETNX faktura:lock:{id} EX 30 → atomowe zajęcie locka.
    Inny request: odśwież stan faktury. Jeśli już zaakceptowana: return sukces.
    Jeśli nie: HTTP 409.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import orjson
from redis.asyncio import Redis
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.fakir_write import FakirUpdateResult, is_fakir_available, update_kod_statusu
from app.db.models.faktura_akceptacja import FakturaAkceptacja, FakturaLog, FakturaPrzypisanie
from app.schemas.faktura_akceptacja import (
    AkcjaLog,
    FakturaLogDetails,
    StatusWewnetrzny,
)
from app.services.config_service import get_config_value
from app.services.event_service import publish_faktura_event

logger = logging.getLogger("app.services.fakir_service")

# Redis lock TTL
_LOCK_TTL = 30   # sekund
_LOCK_PREFIX = "faktura:lock"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _lock_key(faktura_id: int) -> str:
    return f"{_LOCK_PREFIX}:{faktura_id}"


async def _is_fakir_update_enabled(redis: Redis) -> bool:
    """Sprawdza SystemConfig: faktury.fakir_update_enabled (cache Redis 300s)."""
    try:
        value = await get_config_value(
            redis=redis,
            key="faktury.fakir_update_enabled",
            default="false",
        )
        return str(value).lower() == "true"
    except Exception as exc:
        logger.warning(
            orjson.dumps({
                "event":  "fakir_config_read_error",
                "error":  str(exc),
                "ts":     datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        return False  # fail-safe: jeśli nie możemy odczytać → nie wykonuj


async def _get_retry_count(redis: Redis) -> int:
    """Odczytuje faktury.fakir_retry_attempts z SystemConfig."""
    try:
        value = await get_config_value(
            redis=redis,
            key="faktury.fakir_retry_attempts",
            default="3",
        )
        return max(1, min(10, int(value)))
    except Exception:
        return 3


async def _wszyscy_zaakceptowali(
    db: AsyncSession,
    faktura_id: int,
) -> bool:
    """
    Sprawdza czy wszyscy aktywni przypisani pracownicy zaakceptowali.
    SELECT WITH (UPDLOCK, HOLDLOCK) — ochrona przed race condition.
    """
    stmt = select(FakturaPrzypisanie).where(
        FakturaPrzypisanie.faktura_id == faktura_id,
        FakturaPrzypisanie.is_active == True,  # noqa: E712
    )
    result = await db.execute(stmt)
    przypisania = result.scalars().all()

    if not przypisania:
        return False

    return all(p.status == "zaakceptowane" for p in przypisania)


# ─────────────────────────────────────────────────────────────────────────────
# Główna saga
# ─────────────────────────────────────────────────────────────────────────────

async def trigger_fakir_update_if_complete(
    *,
    db:          AsyncSession,
    redis:       Redis,
    faktura_id:  int,
    numer_ksef:  str,
    actor_id:    int,
    actor_name:  str,
    actor_ip:    str,
    request_id:  str,
    wapro_numer: Optional[str] = None,
    wapro_nazwa_kontrahenta: Optional[str] = None,
) -> Optional[FakirUpdateResult]:
    """
    Sprawdza czy wszyscy zaakceptowali i wywołuje sagę UPDATE Fakira.

    Wywoływana po każdej decyzji pracownika z moje_faktury_service.py.
    Zwraca None jeśli nie wszyscy zaakceptowali (normalny przypadek).
    Zwraca FakirUpdateResult jeśli saga została uruchomiona.

    Args:
        db:           AsyncSession SQLAlchemy
        redis:        Redis client
        faktura_id:   ID z skw_faktura_akceptacja
        numer_ksef:   Unikalny KSEF_ID (klucz do BUF_DOKUMENT)
        actor_id/name/ip: Kto wywołał (do logów)
        request_id:   UUID requestu HTTP
        wapro_*:      Dane z widoku WAPRO (do SSE payload)
    """
    settings = get_settings()

    # ── Krok 0: DEMO_MODE — zawsze blokuje
    if getattr(settings, "DEMO_MODE", False):
        logger.warning(
            orjson.dumps({
                "event":      "fakir_blocked_demo_mode",
                "faktura_id": faktura_id,
                "numer_ksef": numer_ksef,
                "ts":         datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        result = FakirUpdateResult(numer_ksef=numer_ksef)
        result.blocked_by_demo = True
        result.error = "DEMO_MODE aktywny — UPDATE Fakira zablokowany"
        return result

    # ── Krok 1: Sprawdź włącznik config
    if not await _is_fakir_update_enabled(redis):
        logger.info(
            orjson.dumps({
                "event":      "fakir_blocked_config_disabled",
                "faktura_id": faktura_id,
                "numer_ksef": numer_ksef,
                "ts":         datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        result = FakirUpdateResult(numer_ksef=numer_ksef)
        result.blocked_by_config = True
        result.error = "faktury.fakir_update_enabled=false — UPDATE Fakira zablokowany"
        return result

    # ── Krok 2: Sprawdź czy wszyscy zaakceptowali
    if not await _wszyscy_zaakceptowali(db, faktura_id):
        logger.debug(
            orjson.dumps({
                "event":      "fakir_not_all_accepted",
                "faktura_id": faktura_id,
                "ts":         datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        return None  # normalny przypadek — czekamy na pozostałych

    # ── Krok 3: Redis LOCK — ochrona race condition
    lock_key = _lock_key(faktura_id)
    acquired = await redis.setnx(lock_key, b"processing")

    if not acquired:
        # Inny request już przetwarja — odśwież stan faktury
        stmt = select(FakturaAkceptacja).where(FakturaAkceptacja.id == faktura_id)
        result_db = await db.execute(stmt)
        faktura = result_db.scalar_one_or_none()

        if faktura and faktura.status_wewnetrzny == StatusWewnetrzny.ZAAKCEPTOWANA:
            logger.info(
                orjson.dumps({
                    "event":      "fakir_already_accepted_by_concurrent",
                    "faktura_id": faktura_id,
                    "ts":         datetime.now(timezone.utc).isoformat(),
                }).decode()
            )
            result = FakirUpdateResult(numer_ksef=numer_ksef)
            result.success = True
            result.error   = "Faktura już zaakceptowana przez współbieżny request"
            return result

        logger.warning(
            orjson.dumps({
                "event":      "fakir_lock_not_acquired",
                "faktura_id": faktura_id,
                "ts":         datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        return None

    try:
        await redis.expire(lock_key, _LOCK_TTL)

        # ── Krok 4: Re-check stanu w locku
        stmt = select(FakturaAkceptacja).where(FakturaAkceptacja.id == faktura_id)
        result_db = await db.execute(stmt)
        faktura = result_db.scalar_one_or_none()

        if not faktura:
            logger.error(f"FakirSaga: faktura_id={faktura_id} nie istnieje")
            return None

        if faktura.status_wewnetrzny == StatusWewnetrzny.ZAAKCEPTOWANA:
            logger.info(f"FakirSaga: faktura {faktura_id} już zaakceptowana — pomijam")
            result = FakirUpdateResult(numer_ksef=numer_ksef)
            result.success = True
            return result

        # ── Krok 5: UPDATE BUF_DOKUMENT
        retry_count = await _get_retry_count(redis)

        logger.info(
            orjson.dumps({
                "event":        "fakir_saga_start",
                "faktura_id":   faktura_id,
                "numer_ksef":   numer_ksef,
                "retry_count":  retry_count,
                "actor_id":     actor_id,
                "request_id":   request_id,
                "ts":           datetime.now(timezone.utc).isoformat(),
            }).decode()
        )

        fakir_result = await update_kod_statusu(
            numer_ksef=numer_ksef,
            retry_count=retry_count,
        )

        if not fakir_result.success:
            # ── ROLLBACK: Fakir nie zaktualizowany → loguj, NIE zmieniaj statusu
            await _log_faktura_event(
                db=db,
                faktura_id=faktura_id,
                user_id=actor_id,
                akcja=AkcjaLog.FAKIR_UPDATE_FAILED,
                before={"status_wewnetrzny": faktura.status_wewnetrzny},
                after=None,
                meta={
                    "faktura_id":   faktura_id,
                    "numer_ksef":   numer_ksef,
                    "fakir_error":  fakir_result.error,
                    "retry_count":  fakir_result.retry_count,
                    "duration_ms":  fakir_result.duration_ms,
                    "operation_id": fakir_result.operation_id,
                },
                actor_name=actor_name,
                actor_ip=actor_ip,
                request_id=request_id,
                endpoint=f"/saga/faktura/{faktura_id}",
            )
            logger.error(
                orjson.dumps({
                    **fakir_result.to_log_dict(),
                    "event":      "fakir_saga_failed",
                    "faktura_id": faktura_id,
                    "request_id": request_id,
                }).decode()
            )
            return fakir_result

        # ── Krok 6: UPDATE skw_faktura_akceptacja (po sukcesie Fakira)
        await db.execute(
            update(FakturaAkceptacja)
            .where(FakturaAkceptacja.id == faktura_id)
            .values(
                status_wewnetrzny="zaakceptowana",
                updated_at=datetime.now().replace(tzinfo=None),
            )
        )

        # ── Krok 7: Log zdarzenia
        await _log_faktura_event(
            db=db,
            faktura_id=faktura_id,
            user_id=actor_id,
            akcja=AkcjaLog.FAKIR_UPDATE,
            before={"status_wewnetrzny": "w_toku", "kod_statusu_fakir": fakir_result.kod_statusu_before},
            after={"status_wewnetrzny": "zaakceptowana", "kod_statusu_fakir": "K"},
            meta={
                "faktura_id":   faktura_id,
                "numer_ksef":   numer_ksef,
                "operation_id": fakir_result.operation_id,
                "duration_ms":  fakir_result.duration_ms,
            },
            actor_name=actor_name,
            actor_ip=actor_ip,
            request_id=request_id,
            endpoint=f"/saga/faktura/{faktura_id}",
        )

        await db.commit()

        logger.info(
            orjson.dumps({
                "event":        "fakir_saga_success",
                "faktura_id":   faktura_id,
                "numer_ksef":   numer_ksef,
                "duration_ms":  fakir_result.duration_ms,
                "request_id":   request_id,
                "ts":           datetime.now(timezone.utc).isoformat(),
            }).decode()
        )

        # ── Krok 8: SSE broadcast faktura_zakonczona (po COMMIT — nigdy wcześniej)
        await _broadcast_faktura_zakonczona(
            db=db,
            redis=redis,
            faktura_id=faktura_id,
            numer_ksef=numer_ksef,
            wapro_numer=wapro_numer,
            wapro_nazwa_kontrahenta=wapro_nazwa_kontrahenta,
            utworzony_przez=faktura.utworzony_przez,
        )

        return fakir_result

    except Exception as exc:
        await db.rollback()
        logger.error(
            orjson.dumps({
                "event":      "fakir_saga_exception",
                "faktura_id": faktura_id,
                "error":      str(exc),
                "ts":         datetime.now(timezone.utc).isoformat(),
            }).decode()
        )
        result = FakirUpdateResult(numer_ksef=numer_ksef)
        result.error = str(exc)
        return result

    finally:
        # Zawsze zwolnij lock
        try:
            await redis.delete(lock_key)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# SSE broadcast
# ─────────────────────────────────────────────────────────────────────────────

async def _broadcast_faktura_zakonczona(
    *,
    db:                     AsyncSession,
    redis:                  Redis,
    faktura_id:             int,
    numer_ksef:             str,
    wapro_numer:            Optional[str],
    wapro_nazwa_kontrahenta: Optional[str],
    utworzony_przez:        int,
) -> None:
    """
    SSE broadcast: faktura_zakonczona → referent + wszyscy aktywni przypisani.
    Wywoływane PO COMMIT transakcji sagi — zgodnie z regułą R-05.
    Błąd SSE nie cofa COMMIT.
    """
    settings = get_settings()

    # Sprawdź DEMO_MODE i włącznik SSE
    if getattr(settings, "DEMO_MODE", False):
        return

    try:
        sse_enabled = await get_config_value(
            redis=redis,
            key="faktury.powiadomienia_sse_enabled",
            default="true",
        )
        if str(sse_enabled).lower() != "true":
            return
    except Exception:
        return

    # Zbierz odbiorców
    stmt = select(FakturaPrzypisanie).where(
        FakturaPrzypisanie.faktura_id == faktura_id,
        FakturaPrzypisanie.is_active == True,  # noqa: E712
    )
    result = await db.execute(stmt)
    aktywni = result.scalars().all()
    aktywni_ids = [p.user_id for p in aktywni]

    odbiorcy = list(set([utworzony_przez] + aktywni_ids))

    payload = {
        "faktura_id":        faktura_id,
        "numer_ksef":        numer_ksef,
        "numer":             wapro_numer,
        "nazwa_kontrahenta": wapro_nazwa_kontrahenta,
        "status_fakir":      "K",
    }

    event = orjson.dumps({"type": "faktura_zakonczona", "data": payload})

    for user_id in odbiorcy:
        await publish_faktura_event(
            redis=redis,
            user_id=user_id,
            event_type="faktura_zakonczona",
            data=payload,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Helper: zapis do skw_faktura_log
# ─────────────────────────────────────────────────────────────────────────────

async def _log_faktura_event(
    *,
    db:         AsyncSession,
    faktura_id: int,
    user_id:    int,
    akcja:      AkcjaLog,
    before:     Optional[dict],
    after:      Optional[dict],
    meta:       Optional[dict],
    actor_name: str,
    actor_full_name: str = "",   # ← NOWE
    actor_ip:   str,
    request_id: str,
    endpoint:   str,
) -> None:
    """Zapisuje wpis do skw_faktura_log. Używa FakturaLogDetails — nigdy raw dict."""
    details = FakturaLogDetails.build(
        user_id=user_id,
        username=actor_name,
        full_name=actor_full_name,   # ← NOWE
        ip=actor_ip,
        before=before,
        after=after,
        meta=meta,
        request_id=request_id,
        endpoint=endpoint,
    )
    log_entry = FakturaLog(
        faktura_id=faktura_id,
        user_id=user_id,
        akcja=akcja.value,
        szczegoly=details.to_json_str(),
        created_at=datetime.now().replace(tzinfo=None),
    )
    db.add(log_entry)