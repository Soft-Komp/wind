"""
Serwis Dłużników — System Windykacja
======================================
Krok 11 / Faza 3 — services/debtor_service.py

Odpowiedzialność:
    - Warstwa biznesowa nad db/wapro.py (WAPRO read-only via pyodbc)
    - Cache Redis dla list i szczegółów dłużników
    - Łączenie danych WAPRO z dbo_ext.MonitHistory (historia monitów)
    - AuditLog dla operacji odczytu (debtors.view_list, debtors.view_details)
    - Walidacja i sanityzacja parametrów filtrowania przed przekazaniem do wapro.py

Architektura (dwie warstwy):
    ┌─────────────────────────────────────────┐
    │  debtor_service (warstwa biznesowa)     │
    │  • Cache Redis                          │
    │  • AuditLog                             │
    │  • Łączenie WAPRO + MonitHistory        │
    │  • Walidacja business rules             │
    └───────────────┬─────────────────────────┘
                    │
    ┌───────────────▼─────────────────────────┐
    │  db/wapro.py (warstwa dostępu do danych)│
    │  • Zapytania SQL (parametryzowane)      │
    │  • Connection pool pyodbc               │
    │  • Retry + exponential backoff          │
    │  • SQL injection guards                 │
    └─────────────────────────────────────────┘

Decyzje projektowe:
    - debtor_service NIE duplikuje logiki z wapro.py — tylko cache + audit + merge
    - Cache TTL 60s dla list (dane WAPRO aktualizowane co ~15 min)
    - Cache TTL 120s dla szczegółów dłużnika
    - AuditLog dla view_list/view_details: fire-and-forget (nie blokuje odpowiedzi)
    - MonitHistory: ostatnie N monitów (domyślnie 10) per dłużnik
    - Brak mutacji — wszystkie operacje READ-ONLY (WAPRO nigdy nie jest modyfikowane)

Zależności:
    - db/wapro.py (WaproConnectionPool)
    - services/audit_service.py
    - db/models/monit_history.py (SQLAlchemy dbo_ext)

Ścieżka docelowa: backend/app/services/debtor_service.py
Autor: System Windykacja — Faza 3 Krok 11
Wersja: 1.0.0
Data: 2026-02-19
"""

from __future__ import annotations

import hashlib
import logging
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import orjson
from redis.asyncio import Redis
from sqlalchemy import and_, desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.monit_history import MonitHistory
from app.db.wapro import (
    DebtorFilterParams,
    InvoiceFilterParams,
    WaproConnectionPool,
    get_debtor_by_id,
    get_debtors,
    get_invoices_for_debtor,
    validate_kontrahent_ids,
)
from app.services import audit_service

# ---------------------------------------------------------------------------
# Logger własny modułu
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------

# TTL cache Redis w sekundach
_CACHE_DEBTOR_LIST_TTL: int = 60     # 1 min — lista dłużników (dane WAPRO mogą się zmieniać)
_CACHE_DEBTOR_DETAIL_TTL: int = 120  # 2 min — szczegóły dłużnika
_CACHE_INVOICES_TTL: int = 120       # 2 min — faktury dłużnika

# Limity
_DEFAULT_MONIT_HISTORY_LIMIT: int = 10   # Domyślna liczba ostatnich monitów w szczegółach
_MAX_MONIT_HISTORY_LIMIT: int = 100      # Maksymalny limit monitów
_MAX_VALIDATE_IDS_BATCH: int = 500       # Maksymalna liczba ID w jednym batch validate

# Klucze Redis
_REDIS_KEY_DEBTOR_LIST   = "debtors:list:{params_hash}"
_REDIS_KEY_DEBTOR_DETAIL = "debtor:{debtor_id}"
_REDIS_KEY_INVOICES      = "debtor:{debtor_id}:invoices:{page}"

# Dozwolone kanały komunikacji (dla walidacji w get_monit_history)
_VALID_MONIT_TYPES: frozenset[str] = frozenset({
    "email", "sms", "print",
})

# Dozwolone statusy monitów
_VALID_MONIT_STATUSES: frozenset[str] = frozenset({
    "pending", "sent", "delivered", "bounced",
    "failed", "opened", "clicked",
})


# ===========================================================================
# Dataclassy parametrów filtrowania (wejście warstwy biznesowej)
# ===========================================================================

@dataclass(frozen=True)
class DebtorListParams:
    """
    Parametry filtrowania listy dłużników — warstwa biznesowa.

    Pola są mapowane na DebtorFilterParams z wapro.py po walidacji.
    Sanityzacja NFC + walidacja wartości biznesowych tutaj (nie w wapro.py).

    Attributes:
        search:          Szukaj po nazwie/NIP/adresie.
        min_debt:        Minimalna kwota zadłużenia.
        max_debt:        Maksymalna kwota zadłużenia.
        overdue_min_days: Minimalna liczba dni przeterminowania.
        overdue_max_days: Maksymalna liczba dni przeterminowania.
        has_active_monit: Filtr: czy ma aktywny monit.
        page:            Numer strony (1-based).
        page_size:       Rozmiar strony (max 200).
        sort_by:         Kolumna sortowania.
        sort_desc:       Sortowanie malejące.
    """
    search: Optional[str] = None
    min_debt: Optional[float] = None
    max_debt: Optional[float] = None
    overdue_min_days: Optional[int] = None
    overdue_max_days: Optional[int] = None
    has_active_monit: Optional[bool] = None
    page: int = 1
    page_size: int = 50
    sort_by: str = "total_debt"
    sort_desc: bool = True

    def __post_init__(self) -> None:
        # Paginacja
        if self.page < 1:
            object.__setattr__(self, "page", 1)
        if self.page_size < 1:
            object.__setattr__(self, "page_size", 1)
        if self.page_size > 200:
            object.__setattr__(self, "page_size", 200)

        # Sanityzacja search (NFC)
        if self.search is not None:
            sanitized = unicodedata.normalize("NFC", self.search.strip())
            object.__setattr__(self, "search", sanitized if sanitized else None)

        # Walidacja kwot
        if self.min_debt is not None and self.min_debt < 0:
            object.__setattr__(self, "min_debt", 0.0)
        if self.max_debt is not None and self.max_debt < 0:
            object.__setattr__(self, "max_debt", None)
        if (
            self.min_debt is not None
            and self.max_debt is not None
            and self.min_debt > self.max_debt
        ):
            raise DebtorValidationError(
                f"min_debt ({self.min_debt}) nie może być większy niż max_debt ({self.max_debt})."
            )

        # Walidacja dni
        if self.overdue_min_days is not None and self.overdue_min_days < 0:
            object.__setattr__(self, "overdue_min_days", 0)
        if self.overdue_max_days is not None and self.overdue_max_days < 0:
            object.__setattr__(self, "overdue_max_days", None)

    def to_wapro_params(self) -> DebtorFilterParams:
        # Mapowanie przyjaznych nazw frontendu → kolumny SQL widoku
        _SORT_MAP: dict[str, str] = {
            "total_debt":    "SumaDlugu",
            "name":          "NazwaKontrahenta",
            "overdue_days":  "DniPrzeterminowania",
            "invoice_count": "LiczbaFaktur",
            "monit_count":   "LiczbaMonitow",
            "last_monit":    "OstatniMonitData",
            # SQL pass-through
            "SumaDlugu":          "SumaDlugu",
            "NazwaKontrahenta":   "NazwaKontrahenta",
            "DniPrzeterminowania":"DniPrzeterminowania",
            "LiczbaFaktur":       "LiczbaFaktur",
            "LiczbaMonitow":      "LiczbaMonitow",
            "OstatniMonitData":   "OstatniMonitData",
            "NajstarszaFaktura":  "NajstarszaFaktura",
        }
        order_by_sql = _SORT_MAP.get(self.sort_by, "SumaDlugu")  # fallback bezpieczny

        return DebtorFilterParams(
            search_query=self.search,
            min_debt_amount=self.min_debt,
            max_debt_amount=self.max_debt,
            overdue_days_min=self.overdue_min_days,
            overdue_days_max=self.overdue_max_days,
            limit=self.page_size,
            offset=(self.page - 1) * self.page_size,
            order_by=order_by_sql,
            order_dir="DESC" if self.sort_desc else "ASC",
        )

    def cache_hash(self) -> str:
        """
        Oblicza hash parametrów do użycia jako klucz cache Redis.

        Returns:
            MD5 hex string (8 znaków — wystarczający dla klucza cache).
        """
        params_bytes = orjson.dumps({
            "s": self.search,
            "min": self.min_debt,
            "max": self.max_debt,
            "od_min": self.overdue_min_days,
            "od_max": self.overdue_max_days,
            "ham": self.has_active_monit,
            "p": self.page,
            "ps": self.page_size,
            "sb": self.sort_by,
            "sd": self.sort_desc,
        })
        return hashlib.md5(params_bytes).hexdigest()[:16]


@dataclass(frozen=True)
class MonitHistoryParams:
    """
    Parametry filtrowania historii monitów dłużnika.

    Attributes:
        debtor_id:  ID kontrahenta WAPRO.
        monit_type: Filtr po typie monitu (email/sms/print).
        status:     Filtr po statusie.
        limit:      Maksymalna liczba wyników.
        page:       Numer strony.
    """
    debtor_id: int
    monit_type: Optional[str] = None
    status: Optional[str] = None
    limit: int = _DEFAULT_MONIT_HISTORY_LIMIT
    page: int = 1

    def __post_init__(self) -> None:
        if self.debtor_id <= 0:
            raise DebtorValidationError("debtor_id musi być dodatnią liczbą całkowitą.")
        if self.limit < 1:
            object.__setattr__(self, "limit", 1)
        if self.limit > _MAX_MONIT_HISTORY_LIMIT:
            object.__setattr__(self, "limit", _MAX_MONIT_HISTORY_LIMIT)
        if self.page < 1:
            object.__setattr__(self, "page", 1)

        if self.monit_type is not None:
            mt = self.monit_type.strip().lower()
            if mt not in _VALID_MONIT_TYPES:
                raise DebtorValidationError(
                    f"Nieprawidłowy typ monitu: {self.monit_type!r}. "
                    f"Dozwolone: {sorted(_VALID_MONIT_TYPES)}"
                )
            object.__setattr__(self, "monit_type", mt)

        if self.status is not None:
            st = self.status.strip().lower()
            if st not in _VALID_MONIT_STATUSES:
                raise DebtorValidationError(
                    f"Nieprawidłowy status monitu: {self.status!r}. "
                    f"Dozwolone: {sorted(_VALID_MONIT_STATUSES)}"
                )
            object.__setattr__(self, "status", st)


# ===========================================================================
# Klasy wyjątków
# ===========================================================================

class DebtorError(Exception):
    """Bazowy wyjątek serwisu dłużników."""


class DebtorValidationError(DebtorError):
    """Błąd walidacji parametrów filtrowania."""


class DebtorNotFoundError(DebtorError):
    """Dłużnik o podanym ID nie istnieje w WAPRO."""


class DebtorWaproError(DebtorError):
    """Błąd połączenia z bazą WAPRO lub wykonania zapytania."""


class DebtorBatchValidationError(DebtorError):
    """
    Błąd przy batch-walidacji ID dłużników.

    Attributes:
        invalid_ids: Lista ID które nie istnieją w WAPRO.
    """
    def __init__(self, invalid_ids: list[int]) -> None:
        self.invalid_ids = invalid_ids
        super().__init__(
            f"Następujące ID dłużników nie istnieją w WAPRO: {invalid_ids}"
        )


# ===========================================================================
# Funkcje pomocnicze — prywatne
# ===========================================================================

async def _get_redis_cache(redis: Redis, key: str) -> Optional[Any]:
    """Pobiera dane z Redis cache (JSON). Zwraca None przy braku lub błędzie."""
    try:
        raw = await redis.get(key)
        if raw:
            return orjson.loads(raw)
    except Exception as exc:
        logger.debug("Cache miss", extra={"key": key, "error": str(exc)})
    return None


async def _set_redis_cache(redis: Redis, key: str, data: Any, ttl: int) -> None:
    """Zapisuje dane do Redis cache (JSON). Błędy logowane jako debug."""
    try:
        await redis.set(key, orjson.dumps(data), ex=ttl)
    except Exception as exc:
        logger.debug(
            "Błąd zapisu do cache Redis",
            extra={"key": key, "error": str(exc)}
        )


def _monit_history_to_dict(monit: MonitHistory) -> dict:
    """Konwertuje obiekt MonitHistory na słownik bezpieczny do zwrotu przez API."""
    return {
        "id_monit": monit.id_monit,
        "id_kontrahenta": monit.id_kontrahenta,
        "id_user": monit.id_user,
        "monit_type": monit.monit_type,
        "status": monit.status,
        "recipient": monit.recipient,
        "subject": monit.subject,
        "total_debt": float(monit.total_debt) if monit.total_debt is not None else None,
        "invoice_numbers": monit.invoice_numbers,
        "external_id": monit.external_id,
        "scheduled_at": monit.scheduled_at.isoformat() if monit.scheduled_at else None,
        "sent_at": monit.sent_at.isoformat() if monit.sent_at else None,
        "delivered_at": monit.delivered_at.isoformat() if monit.delivered_at else None,
        "opened_at": monit.opened_at.isoformat() if monit.opened_at else None,
        "clicked_at": monit.clicked_at.isoformat() if monit.clicked_at else None,
        "error_message": monit.error_message,
        "retry_count": monit.retry_count,
        "cost": float(monit.cost) if monit.cost is not None else None,
        "created_at": monit.created_at.isoformat() if monit.created_at else None,
    }


def _validate_debtor_ids_input(ids: list[int]) -> list[int]:
    """
    Waliduje i deduplikuje listę ID dłużników.

    Args:
        ids: Lista ID do walidacji.

    Returns:
        Zdeduplikowana, posortowana lista ID (pozytywnych liczb całkowitych).

    Raises:
        DebtorValidationError: Gdy lista jest pusta, za długa lub zawiera nieprawidłowe ID.
    """
    if not ids:
        raise DebtorValidationError("Lista ID dłużników nie może być pusta.")
    if len(ids) > _MAX_VALIDATE_IDS_BATCH:
        raise DebtorValidationError(
            f"Maksymalna liczba ID w jednym batch to {_MAX_VALIDATE_IDS_BATCH}. "
            f"Otrzymano: {len(ids)}"
        )
    invalid = [i for i in ids if not isinstance(i, int) or i <= 0]
    if invalid:
        raise DebtorValidationError(
            f"Następujące ID są nieprawidłowe (muszą być dodatnimi int): {invalid[:10]}"
        )
    return sorted(set(ids))


# ===========================================================================
# Publiczne API serwisu
# ===========================================================================

async def get_list(
    wapro: WaproConnectionPool,
    redis: Redis,
    db: AsyncSession,
    params: DebtorListParams,
    requesting_user_id: Optional[int] = None,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Pobiera paginowaną listę dłużników z WAPRO (z cache Redis).

    Przepływ:
        1. Sprawdzenie cache Redis (klucz = hash parametrów)
        2. Jeśli miss → wapro.get_debtors(params) — z connection pool i retry
        3. Zapis do cache Redis (TTL 60s)
        4. AuditLog(action="debtors_list_viewed") — fire-and-forget
        5. Log diagnostyczny

    Cache: debtors:list:{params_hash} (TTL 60s)
    Uwaga: Cache NIE jest inwalidowany aktywnie — dane WAPRO są read-only,
    TTL 60s wystarczy do akceptowalnej świeżości danych.

    Args:
        wapro:              Pula połączeń WAPRO (pyodbc).
        redis:              Klient Redis.
        db:                 Sesja SQLAlchemy (do AuditLog).
        params:             Parametry filtrowania i paginacji.
        requesting_user_id: ID użytkownika wykonującego request (do AuditLog).
        ip_address:         IP inicjatora (do AuditLog).

    Returns:
        Słownik z listą dłużników i metadanymi paginacji:
        {
            "items": [...],
            "total": int,
            "page": int,
            "page_size": int,
            "total_pages": int,
            "from_cache": bool,
        }

    Raises:
        DebtorWaproError: Gdy połączenie z WAPRO się nie powiodło.
    """
    cache_key = _REDIS_KEY_DEBTOR_LIST.format(params_hash=params.cache_hash())
    cached = await _get_redis_cache(redis, cache_key)

    if cached is not None:
        logger.debug(
            "Lista dłużników pobrana z cache Redis",
            extra={"cache_key": cache_key, "user_id": requesting_user_id}
        )
        cached["from_cache"] = True
        # AuditLog nawet przy cache hit — śledzimy kto przeglądał listę
        audit_service.log(
            db=db,
            action="debtors_list_viewed",
            entity_type="Debtor",
            details={
                "from_cache": True,
                "total": cached.get("total"),
                "page": params.page,
                "search": params.search,
            },
            success=True,
        )
        return cached

    # Pobierz z WAPRO
    try:
        wapro_params = params.to_wapro_params()
        wapro_result = await get_debtors(wapro_params)
    except Exception as exc:
        logger.error(
            "Błąd pobierania listy dłużników z WAPRO",
            extra={
                "error": str(exc),
                "user_id": requesting_user_id,
                "params": {
                    "search": params.search,
                    "page": params.page,
                    "page_size": params.page_size,
                }
            }
        )
        raise DebtorWaproError(f"Nie udało się pobrać listy dłużników: {exc}") from exc

    total = wapro_result.total_count or 0
    items = wapro_result.rows
    total_pages = (total + params.page_size - 1) // params.page_size if total > 0 else 0

    result = {
        "items": items,
        "total": total,
        "page": params.page,
        "page_size": params.page_size,
        "total_pages": total_pages,
        "from_cache": False,
    }

    # Zapis do cache
    cache_data = dict(result)
    await _set_redis_cache(redis, cache_key, cache_data, _CACHE_DEBTOR_LIST_TTL)

    logger.info(
        "Lista dłużników pobrana z WAPRO",
        extra={
            "total": total,
            "returned": len(items),
            "page": params.page,
            "search": params.search,
            "user_id": requesting_user_id,
            "ip_address": ip_address,
        }
    )

    # AuditLog (fire-and-forget)
    audit_service.log(
        db=db,
        action="debtors_list_viewed",
        entity_type="Debtor",
        details={
            "from_cache": False,
            "total": total,
            "returned": len(items),
            "page": params.page,
            "search": params.search,
            "min_debt": params.min_debt,
            "max_debt": params.max_debt,
        },
        success=True,
    )

    return result


async def get_by_id(
    wapro: WaproConnectionPool,
    db: AsyncSession,
    redis: Redis,
    debtor_id: int,
    monit_history_limit: int = _DEFAULT_MONIT_HISTORY_LIMIT,
    requesting_user_id: Optional[int] = None,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Pobiera szczegóły dłużnika wraz z historią monitów.

    Łączy dane z dwóch źródeł:
        1. WAPRO (dbo.VIEW_kontrahenci + faktury) → dane finansowe
        2. dbo_ext.MonitHistory (SQLAlchemy) → historia wysłanych monitów

    Cache: debtor:{debtor_id} (TTL 120s)

    Args:
        wapro:               Pula połączeń WAPRO.
        db:                  Sesja SQLAlchemy (MonitHistory + AuditLog).
        redis:               Klient Redis.
        debtor_id:           ID kontrahenta WAPRO (ID_KONTRAHENTA).
        monit_history_limit: Liczba ostatnich monitów do dołączenia (domyślnie 10).
        requesting_user_id:  ID użytkownika (do AuditLog).
        ip_address:          IP inicjatora.

    Returns:
        Słownik z danymi dłużnika i historią monitów:
        {
            "debtor": {...},         ← dane z WAPRO
            "monit_history": [...],  ← z dbo_ext
            "monit_stats": {...},    ← agregaty (sent/failed/etc.)
            "from_cache": bool,
        }

    Raises:
        DebtorNotFoundError: Gdy dłużnik nie istnieje w WAPRO.
        DebtorWaproError:    Gdy błąd połączenia z WAPRO.
    """
    if debtor_id <= 0:
        raise DebtorValidationError("debtor_id musi być dodatnią liczbą całkowitą.")

    monit_history_limit = min(max(monit_history_limit, 1), _MAX_MONIT_HISTORY_LIMIT)

    # Sprawdź cache Redis
    cache_key = _REDIS_KEY_DEBTOR_DETAIL.format(debtor_id=debtor_id)
    cached = await _get_redis_cache(redis, cache_key)

    if cached is not None:
        logger.debug(
            "Szczegóły dłużnika pobrane z cache",
            extra={"debtor_id": debtor_id, "user_id": requesting_user_id}
        )
        cached["from_cache"] = True
        audit_service.log(
            db=db,
            action="debtors_view_details",
            entity_type="Debtor",
            entity_id=debtor_id,
            details={"from_cache": True},
            success=True,
        )
        return cached

    # Pobierz dane z WAPRO
    try:
        wapro_result = await get_debtor_by_id(debtor_id)
    except Exception as exc:
        logger.error(
            "Błąd pobierania szczegółów dłużnika z WAPRO",
            extra={
                "debtor_id": debtor_id,
                "error": str(exc),
                "user_id": requesting_user_id,
            }
        )
        raise DebtorWaproError(
            f"Nie udało się pobrać dłużnika ID={debtor_id}: {exc}"
        ) from exc

    if not wapro_result.rows:
        raise DebtorNotFoundError(
            f"Dłużnik ID={debtor_id} nie istnieje w bazie WAPRO."
        )

    debtor_data = wapro_result.rows[0]  # ← dict z pierwszego wiersza

    # Pobierz historię monitów z dbo_ext
    monit_history = await _fetch_monit_history(
        db=db,
        debtor_id=debtor_id,
        limit=monit_history_limit,
    )

    # Oblicz statystyki monitów
    monit_stats = await _compute_monit_stats(db, debtor_id)

    result = {
        "debtor": debtor_data,
        "monit_history": monit_history,
        "monit_stats": monit_stats,
        "monit_history_limit": monit_history_limit,
        "from_cache": False,
    }

    # Zapis do cache
    await _set_redis_cache(redis, cache_key, result, _CACHE_DEBTOR_DETAIL_TTL)

    logger.info(
        "Szczegóły dłużnika pobrane z WAPRO i dbo_ext",
        extra={
            "debtor_id": debtor_id,
            "monit_count": len(monit_history),
            "user_id": requesting_user_id,
            "ip_address": ip_address,
        }
    )

    # AuditLog (fire-and-forget)
    audit_service.log(
        db=db,
        action="debtors_view_details",
        entity_type="Debtor",
        entity_id=debtor_id,
        details={
            "from_cache": False,
            "monit_count": len(monit_history),
            "total_debt": debtor_data.get("suma_dlugu"),
        },
        success=True,
    )

    return result


async def get_invoices(
    wapro: WaproConnectionPool,
    redis: Redis,
    db: AsyncSession,
    debtor_id: int,
    page: int = 1,
    page_size: int = 50,
    paid: Optional[bool] = None,
    requesting_user_id: Optional[int] = None,
) -> dict:
    """
    Pobiera faktury dłużnika z WAPRO (z cache Redis).

    Faktury pobierane z dbo.VIEW_rozrachunki_faktur.
    Opcjonalny filtr po statusie płatności (paid=True/False/None=wszystkie).

    Cache: debtor:{debtor_id}:invoices:{page} (TTL 120s)

    Args:
        wapro:               Pula połączeń WAPRO.
        redis:               Klient Redis.
        db:                  Sesja SQLAlchemy (do AuditLog).
        debtor_id:           ID kontrahenta WAPRO.
        page:                Numer strony (1-based).
        page_size:           Rozmiar strony (max 200).
        paid:                Filtr: True=zapłacone, False=niezapłacone, None=wszystkie.
        requesting_user_id:  ID użytkownika (do AuditLog).

    Returns:
        Słownik z listą faktur i metadanymi paginacji.

    Raises:
        DebtorValidationError: Gdy parametry są nieprawidłowe.
        DebtorWaproError:      Gdy błąd połączenia z WAPRO.
    """
    if debtor_id <= 0:
        raise DebtorValidationError("debtor_id musi być dodatnią liczbą całkowitą.")

    page      = max(page, 1)
    page_size = min(max(page_size, 1), 200)

    cache_key = _REDIS_KEY_INVOICES.format(debtor_id=debtor_id, page=page)
    cached = await _get_redis_cache(redis, cache_key)
    if cached is not None:
        logger.debug("Faktury dłużnika pobrane z cache", extra={"debtor_id": debtor_id})
        return cached

    try:
        invoice_params = InvoiceFilterParams(
            kontrahent_id=debtor_id,
            include_paid=paid if paid is not None else False,
            limit=page_size,
            offset=(page - 1) * page_size,
        )
        wapro_result = await get_invoices_for_debtor(invoice_params)
    except Exception as exc:
        logger.error(
            "Błąd pobierania faktur dłużnika z WAPRO",
            extra={"debtor_id": debtor_id, "error": str(exc)}
        )
        raise DebtorWaproError(f"Nie udało się pobrać faktur: {exc}") from exc

    total = wapro_result.total_count
    items = wapro_result.rows
    total_pages = (total + page_size - 1) // page_size if total > 0 else 0

    result = {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "debtor_id": debtor_id,
        "filter_paid": paid,
    }

    await _set_redis_cache(redis, cache_key, result, _CACHE_INVOICES_TTL)

    logger.info(
        "Faktury dłużnika pobrane z WAPRO",
        extra={
            "debtor_id": debtor_id,
            "total": total,
            "returned": len(items),
            "user_id": requesting_user_id,
        }
    )

    audit_service.log(
        db=db,
        action="debtors_view_invoices",
        entity_type="Debtor",
        entity_id=debtor_id,
        details={"total_invoices": total, "page": page, "filter_paid": paid},
        success=True,
    )

    return result


async def validate_ids(
    wapro: WaproConnectionPool,
    ids: list[int],
) -> list[int]:
    """
    Weryfikuje które ID z listy istnieją w WAPRO i zwraca tylko istniejące.

    Używane przez monit_service przed masową wysyłką — żeby nie tworzyć monitów
    dla nieistniejących dłużników.

    Limit: max _MAX_VALIDATE_IDS_BATCH ID na jedno wywołanie.

    Args:
        wapro: Pula połączeń WAPRO.
        ids:   Lista ID do walidacji.

    Returns:
        Lista ID które istnieją w WAPRO (subset wejściowego ids).

    Raises:
        DebtorValidationError: Gdy lista jest pusta lub za długa.
        DebtorWaproError:      Gdy błąd połączenia z WAPRO.
    """
    sanitized_ids = _validate_debtor_ids_input(ids)

    try:
        valid_ids = await validate_kontrahent_ids(wapro, sanitized_ids)
    except Exception as exc:
        logger.error(
            "Błąd batch-walidacji ID dłużników w WAPRO",
            extra={"ids_count": len(sanitized_ids), "error": str(exc)}
        )
        raise DebtorWaproError(f"Błąd walidacji ID dłużników: {exc}") from exc

    invalid_ids = sorted(set(sanitized_ids) - set(valid_ids))
    if invalid_ids:
        logger.warning(
            "Wykryto nieistniejące ID dłużników",
            extra={
                "requested": len(sanitized_ids),
                "valid": len(valid_ids),
                "invalid_count": len(invalid_ids),
                "invalid_sample": invalid_ids[:20],
            }
        )

    logger.info(
        "Batch-walidacja ID dłużników zakończona",
        extra={
            "requested": len(sanitized_ids),
            "valid": len(valid_ids),
            "invalid": len(invalid_ids),
        }
    )

    return sorted(valid_ids)


async def validate_ids_strict(
    wapro: WaproConnectionPool,
    ids: list[int],
) -> list[int]:
    """
    Weryfikuje ID dłużników — rzuca wyjątek jeśli którekolwiek nie istnieje.

    Wariant ścisły validate_ids() — używany gdy wszystkie ID MUSZĄ być prawidłowe.

    Args:
        wapro: Pula połączeń WAPRO.
        ids:   Lista ID do walidacji.

    Returns:
        Lista ID (identyczna z wejściową, jeśli wszystkie są prawidłowe).

    Raises:
        DebtorBatchValidationError: Gdy którekolwiek ID nie istnieje w WAPRO.
    """
    valid_ids = await validate_ids(wapro, ids)
    invalid_ids = sorted(set(ids) - set(valid_ids))
    if invalid_ids:
        raise DebtorBatchValidationError(invalid_ids)
    return valid_ids


# ===========================================================================
# Historia monitów (dbo_ext — SQLAlchemy)
# ===========================================================================

async def get_monit_history(
    db: AsyncSession,
    params: MonitHistoryParams,
    requesting_user_id: Optional[int] = None,
) -> dict:
    """
    Pobiera historię monitów dla dłużnika z tabeli dbo_ext.MonitHistory.

    To jest INNA operacja niż wapro.get_debtors() — to są dane z naszej bazy
    (dbo_ext), nie z WAPRO.

    Obsługuje filtrowanie po typie monitu i statusie.

    Args:
        db:                  Sesja SQLAlchemy.
        params:              Parametry filtrowania historii monitów.
        requesting_user_id:  ID użytkownika (do logowania).

    Returns:
        Słownik z listą monitów i metadanymi paginacji.
    """
    conditions = [
        MonitHistory.id_kontrahenta == params.debtor_id
    ]

    if params.monit_type:
        conditions.append(MonitHistory.monit_type == params.monit_type)
    if params.status:
        conditions.append(MonitHistory.status == params.status)

    from sqlalchemy import func as sql_func

    # COUNT
    count_result = await db.execute(
        select(sql_func.count(MonitHistory.id_monit)).where(and_(*conditions))
    )
    total = count_result.scalar_one() or 0

    if total == 0:
        return {
            "items": [],
            "total": 0,
            "page": params.page,
            "page_size": params.limit,
            "total_pages": 0,
            "debtor_id": params.debtor_id,
        }

    # DATA
    offset = (params.page - 1) * params.limit
    data_result = await db.execute(
        select(MonitHistory)
        .where(and_(*conditions))
        .order_by(desc(MonitHistory.created_at))
        .offset(offset)
        .limit(params.limit)
    )
    monits = data_result.scalars().all()

    total_pages = (total + params.limit - 1) // params.limit

    logger.debug(
        "Historia monitów pobrana z dbo_ext",
        extra={
            "debtor_id": params.debtor_id,
            "total": total,
            "returned": len(monits),
            "user_id": requesting_user_id,
        }
    )

    return {
        "items": [_monit_history_to_dict(m) for m in monits],
        "total": total,
        "page": params.page,
        "page_size": params.limit,
        "total_pages": total_pages,
        "debtor_id": params.debtor_id,
    }


async def _fetch_monit_history(
    db: AsyncSession,
    debtor_id: int,
    limit: int = _DEFAULT_MONIT_HISTORY_LIMIT,
) -> list[dict]:
    """
    Wewnętrzna helper — pobiera ostatnie N monitów dłużnika.

    Używana przez get_by_id() do dołączenia historii do szczegółów.

    Args:
        db:       Sesja SQLAlchemy.
        debtor_id: ID kontrahenta.
        limit:    Liczba monitów do pobrania.

    Returns:
        Lista słowników z danymi monitów.
    """
    result = await db.execute(
        select(MonitHistory)
        .where(
            and_(
                MonitHistory.id_kontrahenta == debtor_id
            )
        )
        .order_by(desc(MonitHistory.created_at))
        .limit(limit)
    )
    monits = result.scalars().all()
    return [_monit_history_to_dict(m) for m in monits]


async def _compute_monit_stats(
    db: AsyncSession,
    debtor_id: int,
) -> dict:
    """
    Oblicza statystyki wysłanych monitów dla dłużnika.

    Agreguje liczby per status i per typ monitu.

    Args:
        db:       Sesja SQLAlchemy.
        debtor_id: ID kontrahenta.

    Returns:
        Słownik ze statystykami:
        {
            "total": int,
            "by_status": {"sent": N, "failed": N, ...},
            "by_type": {"email": N, "sms": N, "print": N},
            "last_sent_at": "ISO datetime" | None,
        }
    """
    from sqlalchemy import case, func as sql_func

    result = await db.execute(
        select(
            sql_func.count(MonitHistory.id_monit).label("total"),
            sql_func.max(MonitHistory.sent_at).label("last_sent_at"),
            sql_func.sum(
                case((MonitHistory.status == "sent", 1), else_=0)
            ).label("sent_count"),
            sql_func.sum(
                case((MonitHistory.status == "delivered", 1), else_=0)
            ).label("delivered_count"),
            sql_func.sum(
                case((MonitHistory.status == "failed", 1), else_=0)
            ).label("failed_count"),
            sql_func.sum(
                case((MonitHistory.status == "bounced", 1), else_=0)
            ).label("bounced_count"),
            sql_func.sum(
                case((MonitHistory.status == "pending", 1), else_=0)
            ).label("pending_count"),
            sql_func.sum(
                case((MonitHistory.status == "opened", 1), else_=0)
            ).label("opened_count"),
            sql_func.sum(
                case((MonitHistory.monit_type == "email", 1), else_=0)
            ).label("email_count"),
            sql_func.sum(
                case((MonitHistory.monit_type == "sms", 1), else_=0)
            ).label("sms_count"),
            sql_func.sum(
                case((MonitHistory.monit_type == "print", 1), else_=0)
            ).label("print_count"),
        )
        .where(
            and_(
                MonitHistory.id_kontrahenta == debtor_id
            )
        )
    )
    row = result.one()

    last_sent_at = None
    if row.last_sent_at:
        ts = row.last_sent_at
        if hasattr(ts, "isoformat"):
            last_sent_at = ts.isoformat()

    return {
        "total": row.total or 0,
        "last_sent_at": last_sent_at,
        "by_status": {
            "sent":      row.sent_count or 0,
            "delivered": row.delivered_count or 0,
            "failed":    row.failed_count or 0,
            "bounced":   row.bounced_count or 0,
            "pending":   row.pending_count or 0,
            "opened":    row.opened_count or 0,
        },
        "by_type": {
            "email": row.email_count or 0,
            "sms":   row.sms_count or 0,
            "print": row.print_count or 0,
        },
    }


async def invalidate_debtor_cache(
    redis: Redis,
    debtor_id: int,
) -> None:
    """
    Inwaliduje cache dłużnika (detail + faktury).

    Wywoływana po aktualizacji MonitHistory — żeby detail pokazywał nową historię.

    Args:
        redis:    Klient Redis.
        debtor_id: ID kontrahenta.
    """
    keys_to_delete = [_REDIS_KEY_DEBTOR_DETAIL.format(debtor_id=debtor_id)]

    # Usuwamy też faktury (kilka stron)
    try:
        async for key in redis.scan_iter(
            f"debtor:{debtor_id}:invoices:*"
        ):
            keys_to_delete.append(key)
    except Exception:
        pass

    try:
        if keys_to_delete:
            await redis.delete(*keys_to_delete)
        logger.debug(
            "Cache dłużnika zinwalidowany",
            extra={"debtor_id": debtor_id, "deleted_keys": len(keys_to_delete)}
        )
    except Exception as exc:
        logger.warning(
            "Błąd inwalidacji cache dłużnika",
            extra={"debtor_id": debtor_id, "error": str(exc)}
        )


async def ping_wapro(wapro: WaproConnectionPool) -> dict:
    """
    Sprawdza dostępność połączenia z WAPRO.

    Deleguje do wapro.ping() — zwraca latencję i stan puli.
    Używane przez health check endpoint.

    Args:
        wapro: Pula połączeń WAPRO.

    Returns:
        Słownik ze statusem WAPRO (latency_ms, pool_stats, status).
    """
    from app.db.wapro import ping as wapro_ping
    try:
        result = await wapro_ping(wapro)
        return result
    except Exception as exc:
        logger.error("Health check WAPRO: FAILED", extra={"error": str(exc)})
        return {
            "status": "error",
            "error": str(exc),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }