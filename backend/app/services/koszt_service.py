# backend/app/services/koszt_service.py
"""
Serwis kosztów dodatkowych monitów — System Windykacja.

Odpowiedzialność:
    - CRUD dla dbo_ext.skw_KosztyDodatkowe
    - Cache Redis list aktywnych kosztów per typ monitu
    - Dwuetapowe usuwanie z tokenem JWT (soft-delete)
    - Logi JSONL per dzień

Używany przez:
    - api/koszty.py          (CRUD endpointy)
    - api/debtors.py         (monit-cost-preview)
    - services/monit_service (send_bulk — obliczanie KwotaCałkowitej)
"""
from __future__ import annotations

import logging
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from secrets import token_hex
from typing import Any, Optional

import orjson
from jose import jwt as jose_jwt
from redis.asyncio import Redis
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.models.koszt_dodatkowy import KosztDodatkowy
from app.services import audit_service

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Stałe
# ─────────────────────────────────────────────────────────────────────────────

_VALID_TYP_MONITU: frozenset[str] = frozenset({"email", "sms", "print"})
_MAX_NAZWA: int      = 200
_MAX_OPIS: int       = 500
_CACHE_TTL: int      = 120   # sekundy
_DELETE_TOKEN_TTL: int = 60
_REDIS_KEY_LIST    = "koszty:list:{typ}"
_REDIS_KEY_DETAIL  = "koszty:detail:{id}"
_REDIS_KEY_DELETE  = "koszty_delete:{jti}"
_LOG_PATTERN       = "logs/koszty_{date}.jsonl"


# ─────────────────────────────────────────────────────────────────────────────
# Wyjątki
# ─────────────────────────────────────────────────────────────────────────────

class KosztError(Exception):
    """Bazowy wyjątek serwisu kosztów."""

class KosztNotFoundError(KosztError):
    """Koszt o podanym ID nie istnieje lub jest nieaktywny."""

class KosztValidationError(KosztError):
    """Błąd walidacji danych wejściowych."""

class KosztDeleteTokenError(KosztError):
    """Token DELETE jest nieprawidłowy, wygasł lub już użyty."""


# ─────────────────────────────────────────────────────────────────────────────
# Dataclasses wejściowe
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class KosztCreateData:
    """Zwalidowane dane do tworzenia kosztu."""
    nazwa:      str
    kwota:      Decimal
    typ_monitu: str
    opis:       Optional[str] = None

    def __post_init__(self) -> None:
        nazwa = unicodedata.normalize("NFC", (self.nazwa or "").strip())
        if not nazwa:
            raise KosztValidationError("Nazwa kosztu nie może być pusta.")
        if len(nazwa) > _MAX_NAZWA:
            raise KosztValidationError(f"Nazwa przekracza {_MAX_NAZWA} znaków.")
        object.__setattr__(self, "nazwa", nazwa)

        if self.kwota is None or self.kwota <= Decimal("0"):
            raise KosztValidationError("Kwota musi być większa od 0.")

        if self.typ_monitu not in _VALID_TYP_MONITU:
            raise KosztValidationError(
                f"Nieprawidłowy typ monitu: {self.typ_monitu!r}. "
                f"Dozwolone: {sorted(_VALID_TYP_MONITU)}"
            )

        if self.opis is not None:
            opis = unicodedata.normalize("NFC", self.opis.strip())
            if len(opis) > _MAX_OPIS:
                raise KosztValidationError(f"Opis przekracza {_MAX_OPIS} znaków.")
            object.__setattr__(self, "opis", opis or None)


@dataclass(frozen=True)
class KosztUpdateData:
    """Zwalidowane dane do aktualizacji kosztu (None = nie zmieniaj)."""
    nazwa:      Optional[str]     = None
    kwota:      Optional[Decimal] = None
    typ_monitu: Optional[str]     = None
    opis:       Optional[str]     = None
    is_active:  Optional[bool]    = None

    def __post_init__(self) -> None:
        if self.nazwa is not None:
            nazwa = unicodedata.normalize("NFC", self.nazwa.strip())
            if not nazwa:
                raise KosztValidationError("Nazwa nie może być pusta.")
            if len(nazwa) > _MAX_NAZWA:
                raise KosztValidationError(f"Nazwa przekracza {_MAX_NAZWA} znaków.")
            object.__setattr__(self, "nazwa", nazwa)

        if self.kwota is not None and self.kwota <= Decimal("0"):
            raise KosztValidationError("Kwota musi być większa od 0.")

        if self.typ_monitu is not None and self.typ_monitu not in _VALID_TYP_MONITU:
            raise KosztValidationError(
                f"Nieprawidłowy typ monitu: {self.typ_monitu!r}."
            )


# ─────────────────────────────────────────────────────────────────────────────
# Helpers prywatne
# ─────────────────────────────────────────────────────────────────────────────

def _get_log_file() -> Path:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = Path(_LOG_PATTERN.format(date=today))
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _append_log(record: dict) -> None:
    try:
        with _get_log_file().open("ab") as f:
            f.write(orjson.dumps(record) + b"\n")
    except Exception as exc:
        logger.warning("Błąd zapisu logu kosztów: %s", exc)


def _to_dict(k: KosztDodatkowy) -> dict:
    return {
        "id_kosztu":   k.id_kosztu,
        "nazwa":       k.nazwa,
        "kwota":       float(k.kwota),
        "typ_monitu":  k.typ_monitu,
        "opis":        k.opis,
        "is_active":   k.is_active,
        "created_at":  k.created_at.isoformat() if k.created_at else None,
        "updated_at":  k.updated_at.isoformat() if k.updated_at else None,
    }


async def _cache_get(redis: Redis, key: str) -> Any | None:
    try:
        raw = await redis.get(key)
        return orjson.loads(raw) if raw else None
    except Exception:
        return None


async def _cache_set(redis: Redis, key: str, value: Any, ttl: int) -> None:
    try:
        await redis.setex(key, ttl, orjson.dumps(value))
    except Exception:
        pass


async def _cache_invalidate_typ(redis: Redis, typ_monitu: str) -> None:
    """Inwaliduje cache listy dla danego typu + listy 'all'."""
    for key in [
        _REDIS_KEY_LIST.format(typ=typ_monitu),
        _REDIS_KEY_LIST.format(typ="all"),
    ]:
        try:
            await redis.delete(key)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Publiczne API serwisu
# ─────────────────────────────────────────────────────────────────────────────

async def get_list(
    db: AsyncSession,
    redis: Redis,
    *,
    typ_monitu: Optional[str] = None,
    tylko_aktywne: bool = True,
) -> list[dict]:
    """
    Lista kosztów dodatkowych, opcjonalnie filtrowana po typie monitu.
    Cache Redis TTL 120s.
    """
    cache_key = _REDIS_KEY_LIST.format(typ=typ_monitu or "all")
    cached = await _cache_get(redis, cache_key)
    if cached is not None:
        return cached

    stmt = select(KosztDodatkowy).order_by(
        KosztDodatkowy.typ_monitu, KosztDodatkowy.nazwa
    )
    if tylko_aktywne:
        stmt = stmt.where(KosztDodatkowy.is_active == True)  # noqa: E712
    if typ_monitu:
        stmt = stmt.where(KosztDodatkowy.typ_monitu == typ_monitu)

    result = await db.execute(stmt)
    items = [_to_dict(k) for k in result.scalars().all()]

    await _cache_set(redis, cache_key, items, _CACHE_TTL)
    logger.debug("get_list: %d kosztów (typ=%s)", len(items), typ_monitu)
    return items


async def get_by_id(
    db: AsyncSession,
    redis: Redis,
    id_kosztu: int,
) -> dict:
    """Szczegóły kosztu. Cache Redis TTL 120s."""
    cache_key = _REDIS_KEY_DETAIL.format(id=id_kosztu)
    cached = await _cache_get(redis, cache_key)
    if cached is not None:
        return cached

    result = await db.execute(
        select(KosztDodatkowy).where(KosztDodatkowy.id_kosztu == id_kosztu)
    )
    koszt = result.scalar_one_or_none()
    if koszt is None:
        raise KosztNotFoundError(f"Koszt ID={id_kosztu} nie istnieje.")

    data = _to_dict(koszt)
    await _cache_set(redis, cache_key, data, _CACHE_TTL)
    return data


async def get_active_for_channel(
    db: AsyncSession,
    redis: Redis,
    channel: str,
) -> list[dict]:
    """
    Zwraca aktywne koszty dla danego kanału.
    Używana przez monit-cost-preview i send_bulk.
    """
    return await get_list(db, redis, typ_monitu=channel, tylko_aktywne=True)


async def create(
    db: AsyncSession,
    redis: Redis,
    *,
    raw_nazwa:      Any,
    raw_kwota:      Any,
    raw_typ_monitu: Any,
    raw_opis:       Any = None,
    created_by_user_id: int,
    ip_address:     Optional[str] = None,
) -> dict:
    """Tworzy nowy koszt dodatkowy."""
    try:
        kwota = Decimal(str(raw_kwota)) if raw_kwota is not None else Decimal("0")
    except Exception:
        raise KosztValidationError("Nieprawidłowy format kwoty.")

    data = KosztCreateData(
        nazwa=str(raw_nazwa or ""),
        kwota=kwota,
        typ_monitu=str(raw_typ_monitu or ""),
        opis=str(raw_opis) if raw_opis is not None else None,
    )

    koszt = KosztDodatkowy(
        nazwa=data.nazwa,
        kwota=data.kwota,
        typ_monitu=data.typ_monitu,
        opis=data.opis,
        is_active=True,
        created_at=datetime.utcnow(),
    )
    db.add(koszt)
    await db.flush()
    await db.commit()
    await db.refresh(koszt)
    result = _to_dict(koszt)

    await _cache_invalidate_typ(redis, data.typ_monitu)
    try:
        await redis.delete(_REDIS_KEY_LIST.format(typ="all"))
    except Exception:
        pass

    _append_log({
        "ts":      datetime.now(timezone.utc).isoformat(),
        "action":  "koszt_created",
        "id":      koszt.id_kosztu,
        "nazwa":   data.nazwa,
        "kwota":   float(data.kwota),
        "typ":     data.typ_monitu,
        "user_id": created_by_user_id,
        "ip":      ip_address,
    })

    audit_service.log(
        db=db,
        action="koszt_created",
        entity_type="KosztDodatkowy",
        entity_id=koszt.id_kosztu,
        details=result,
        success=True,
    )

    logger.info(
        "Koszt dodatkowy utworzony: ID=%d nazwa=%r kwota=%s typ=%s",
        koszt.id_kosztu, data.nazwa, data.kwota, data.typ_monitu,
    )
    return result


async def update(
    db: AsyncSession,
    redis: Redis,
    id_kosztu: int,
    *,
    raw_nazwa:      Any = None,
    raw_kwota:      Any = None,
    raw_typ_monitu: Any = None,
    raw_opis:       Any = None,
    raw_is_active:  Any = None,
    updated_by_user_id: int,
    ip_address:     Optional[str] = None,
) -> dict:
    """Aktualizuje koszt dodatkowy."""
    result = await db.execute(
        select(KosztDodatkowy).where(KosztDodatkowy.id_kosztu == id_kosztu)
    )
    koszt = result.scalar_one_or_none()
    if koszt is None:
        raise KosztNotFoundError(f"Koszt ID={id_kosztu} nie istnieje.")

    try:
        kwota = Decimal(str(raw_kwota)) if raw_kwota is not None else None
    except Exception:
        raise KosztValidationError("Nieprawidłowy format kwoty.")

    data = KosztUpdateData(
        nazwa=str(raw_nazwa) if raw_nazwa is not None else None,
        kwota=kwota,
        typ_monitu=str(raw_typ_monitu) if raw_typ_monitu is not None else None,
        opis=str(raw_opis) if raw_opis is not None else None,
        is_active=bool(raw_is_active) if raw_is_active is not None else None,
    )

    stary_typ = koszt.typ_monitu

    if data.nazwa      is not None: koszt.nazwa      = data.nazwa
    if data.kwota      is not None: koszt.kwota      = data.kwota
    if data.typ_monitu is not None: koszt.typ_monitu = data.typ_monitu
    if data.opis       is not None: koszt.opis       = data.opis
    if data.is_active  is not None: koszt.is_active  = data.is_active
    koszt.updated_at = datetime.utcnow()

    await db.flush()
    await db.commit()
    updated = _to_dict(koszt)

    # Inwaliduj cache dla starego i nowego typu
    await _cache_invalidate_typ(redis, stary_typ)
    if data.typ_monitu and data.typ_monitu != stary_typ:
        await _cache_invalidate_typ(redis, data.typ_monitu)
    try:
        await redis.delete(_REDIS_KEY_DETAIL.format(id=id_kosztu))
    except Exception:
        pass

    _append_log({
        "ts":      datetime.now(timezone.utc).isoformat(),
        "action":  "koszt_updated",
        "id":      id_kosztu,
        "changes": {k: v for k, v in data.__dict__.items() if v is not None},
        "user_id": updated_by_user_id,
        "ip":      ip_address,
    })

    audit_service.log(
        db=db,
        action="koszt_updated",
        entity_type="KosztDodatkowy",
        entity_id=id_kosztu,
        details=updated,
        success=True,
    )

    logger.info("Koszt ID=%d zaktualizowany przez user=%d", id_kosztu, updated_by_user_id)
    return updated


async def initiate_delete(
    db: AsyncSession,
    redis: Redis,
    id_kosztu: int,
    *,
    initiated_by_user_id: int,
    ip_address: Optional[str] = None,
) -> dict:
    """Krok 1/2 — generuje jednorazowy token DELETE."""
    result = await db.execute(
        select(KosztDodatkowy).where(KosztDodatkowy.id_kosztu == id_kosztu)
    )
    koszt = result.scalar_one_or_none()
    if koszt is None:
        raise KosztNotFoundError(f"Koszt ID={id_kosztu} nie istnieje.")

    settings = get_settings()
    jti = token_hex(16)
    now = datetime.now(timezone.utc)
    payload = {
        "sub":       str(id_kosztu),
        "scope":     "delete_koszt",
        "id_kosztu": id_kosztu,
        "initiated_by": initiated_by_user_id,
        "jti":       jti,
        "iat":       int(now.timestamp()),
        "exp":       int((now + timedelta(seconds=_DELETE_TOKEN_TTL)).timestamp()),
    }
    secret = (
        settings.secret_key.get_secret_value()
        if hasattr(settings.secret_key, "get_secret_value")
        else str(settings.secret_key)
    )
    token = jose_jwt.encode(payload, secret, algorithm="HS256")
    await redis.set(_REDIS_KEY_DELETE.format(jti=jti), str(id_kosztu), ex=_DELETE_TOKEN_TTL)

    logger.warning(
        "Inicjacja usunięcia kosztu ID=%d przez user=%d", id_kosztu, initiated_by_user_id
    )
    return {
        "delete_token": token,
        "expires_in":   _DELETE_TOKEN_TTL,
        "id_kosztu":    id_kosztu,
        "nazwa":        koszt.nazwa,
        "typ_monitu":   koszt.typ_monitu,
        "warning":      "Dezaktywacja kosztu jest nieodwracalna przez API.",
    }


async def confirm_delete(
    db: AsyncSession,
    redis: Redis,
    id_kosztu: int,
    confirm_token: str,
    *,
    confirmed_by_user_id: int,
    ip_address: Optional[str] = None,
) -> dict:
    """Krok 2/2 — weryfikuje token i wykonuje soft-delete."""
    settings = get_settings()
    secret = (
        settings.secret_key.get_secret_value()
        if hasattr(settings.secret_key, "get_secret_value")
        else str(settings.secret_key)
    )

    try:
        payload = jose_jwt.decode(confirm_token, secret, algorithms=["HS256"])
    except Exception:
        raise KosztDeleteTokenError("Token nieprawidłowy lub wygasł.")

    if payload.get("scope") != "delete_koszt" or payload.get("id_kosztu") != id_kosztu:
        raise KosztDeleteTokenError("Token nie dotyczy tego kosztu.")

    jti = payload.get("jti", "")
    redis_key = _REDIS_KEY_DELETE.format(jti=jti)
    stored = await redis.get(redis_key)
    if not stored:
        raise KosztDeleteTokenError("Token wygasł lub już został użyty.")
    await redis.delete(redis_key)

    result = await db.execute(
        select(KosztDodatkowy).where(KosztDodatkowy.id_kosztu == id_kosztu)
    )
    koszt = result.scalar_one_or_none()
    if koszt is None:
        raise KosztNotFoundError(f"Koszt ID={id_kosztu} nie istnieje.")

    typ = koszt.typ_monitu
    koszt.is_active  = False
    koszt.updated_at = datetime.utcnow()
    await db.flush()
    await db.commit()

    await _cache_invalidate_typ(redis, typ)
    try:
        await redis.delete(_REDIS_KEY_DETAIL.format(id=id_kosztu))
    except Exception:
        pass

    _append_log({
        "ts":      datetime.now(timezone.utc).isoformat(),
        "action":  "koszt_deleted",
        "id":      id_kosztu,
        "user_id": confirmed_by_user_id,
        "ip":      ip_address,
    })

    audit_service.log(
        db=db,
        action="koszt_deleted",
        entity_type="KosztDodatkowy",
        entity_id=id_kosztu,
        details={"soft_delete": True},
        success=True,
    )

    logger.warning("Koszt ID=%d dezaktywowany przez user=%d", id_kosztu, confirmed_by_user_id)
    return {"id_kosztu": id_kosztu, "is_active": False, "message": "Koszt dezaktywowany."}