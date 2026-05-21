# backend/app/services/filter_engine.py
"""
Silnik filtrow automatycznego przydzialu sciezek akceptacyjnych.

Odpowiedzialnosc:
    resolve_path() — glowna funkcja; zwraca id_path lub None.

Algorytm (wg specyfikacji 3.7):
    1. Pobierz aktywne filtry posortowane po priority DESC (wyzszy = wazniejszy).
    2. Dla kazdego filtra:
       a. Sprawdz typ: standard lub universal
       b. standard: oceń warunki (logika AND) przez evaluate_standard_filter()
       c. universal: wywolaj funkcje SQL przez evaluate_universal_filter()
          — try/except: blad = loguj, traktuj jako brak dopasowania, NIE przerywaj
    3. Ostatni pasujacy filtr wygrywa (last match wins — najszczegolowszy).
    4. Brak dopasowan → None → dokument idzie do pending_dispatch.

Bezpieczenstwo:
    - universal: whitelist nazwy funkcji SQL (regex ^[a-zA-Z0-9_]+$)
    - standard: cast wartosci przez _cast_value() — brak raw SQL od usera

Integracja z dispatch():
    id_path = await filter_engine.resolve_path(db, id_source, unified_doc)
    Jesli None → dispatch bez sciezki (status=pending_dispatch, id_path=None)
    Jesli int  → dispatch z ta sciezka

UWAGA: from __future__ import annotations — NIGDY w tym pliku.
"""

import logging
import re
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_SCHEMA = "dbo"

# Whitelist nazwy funkcji SQL dla filtrow universal
_FUNC_NAME_RE = re.compile(r"^[a-zA-Z0-9_]+$")

# Maksymalna dlugosc nazwy funkcji SQL (ochrona przed SQL injection przez dlugos)
_FUNC_NAME_MAX_LEN = 128


# =============================================================================
# Glowna funkcja: resolve_path
# =============================================================================

async def resolve_path(
    db: AsyncSession,
    id_source: int,
    unified_doc: dict,
    *,
    auto_filters_enabled: bool = True,
) -> int | None:
    """
    Rozstrzyga ktora sciezka akceptacyjna przydzielic dokumentowi.

    Args:
        db:                   Sesja SQLAlchemy async.
        id_source:            ID zrodla dokumentu (fakir=1, ksef=2 wg seeda).
        unified_doc:          Slownik z polami UnifiedDocument (patrz schemas/unified_document.py).
                              Wymagane pola: id_document, id_source, source_name.
                              Opcjonalne: amount_gross, contractor_name, document_type itp.
        auto_filters_enabled: Jesli False — od razu zwraca None (feature flag).

    Returns:
        id_path jesli znaleziono dopasowanie, None jesli brak lub filtry wylaczone.
    """
    if not auto_filters_enabled:
        logger.debug("filter_engine | APPROVAL_AUTO_FILTERS_ENABLED=false — pomijam")
        return None

    # Pobierz aktywne filtry dla tego zrodla lub globalne (id_source IS NULL)
    # Posortowane: wyzszy priority = bardziej szczegolowy = OSTATNI (last match wins)
    # Dlatego sortujemy ASC — ostatni dopasowany (najwyzszy priority) wygra
    filters_rows = await db.execute(
        text(
            f"SELECT f.[id_filter], f.[filter_type], f.[id_path], "
            f"       f.[universal_function], f.[priority] "
            f"FROM [{_SCHEMA}].[skw_approval_filters] f "
            f"WHERE f.[is_active] = 1 "
            f"  AND (f.[id_source] = :src OR f.[id_source] IS NULL) "
            f"ORDER BY f.[priority] ASC"
        ),
        {"src": id_source},
    )
    filters = filters_rows.fetchall()

    if not filters:
        logger.debug(
            "filter_engine | Brak aktywnych filtrow dla id_source=%d", id_source
        )
        return None

    last_match: int | None = None

    for id_filter, filter_type, id_path, universal_func, priority in filters:
        try:
            matched = False

            if filter_type == "standard":
                matched = await _evaluate_standard_filter(db, id_filter, unified_doc)
            elif filter_type == "universal":
                matched = await _evaluate_universal_filter(
                    db, id_filter, universal_func, unified_doc
                )
            else:
                logger.warning(
                    "filter_engine | Nieznany filter_type='%s' dla id_filter=%d — pomijam",
                    filter_type, id_filter,
                )
                continue

            if matched:
                last_match = id_path
                logger.debug(
                    "filter_engine | Dopasowanie: id_filter=%d type=%s priority=%d id_path=%d",
                    id_filter, filter_type, priority, id_path,
                )

        except Exception as exc:
            # KRYTYCZNE: blad pojedynczego filtra NIE przerywa potoku
            logger.error(
                "filter_engine | Blad ewaluacji id_filter=%d: %s — traktuje jako brak dopasowania",
                id_filter, exc,
            )
            continue

    if last_match is not None:
        logger.info(
            "filter_engine | id_source=%d → id_path=%d (last match wins)",
            id_source, last_match,
        )
    else:
        logger.info(
            "filter_engine | id_source=%d → brak dopasowania → pending_dispatch", id_source,
        )

    return last_match


# =============================================================================
# Ewaluacja filtru standard
# =============================================================================

async def _evaluate_standard_filter(
    db: AsyncSession,
    id_filter: int,
    doc: dict,
) -> bool:
    """
    Sprawdza warunki filtru standard.
    Logika AND: WSZYSTKIE warunki musza byc spelnione.
    Wartosc dokumentu porownywana z wartosc_filtra po caście do wlasciwego typu.
    """
    conditions_rows = await db.execute(
        text(
            f"SELECT [field_name], [operator], [field_value] "
            f"FROM [{_SCHEMA}].[skw_approval_filter_conditions] "
            f"WHERE [id_filter] = :f "
            f"ORDER BY [id_condition] ASC"
        ),
        {"f": id_filter},
    )
    conditions = conditions_rows.fetchall()

    if not conditions:
        # Filtr bez warunkow = zawsze pasuje (catch-all)
        logger.debug(
            "filter_engine | id_filter=%d: brak warunkow — catch-all (True)", id_filter
        )
        return True

    for field_name, operator, filter_value in conditions:
        # Pobierz wartosc z dokumentu (zagniezdzone pola przez '.')
        doc_value = _get_nested(doc, field_name)

        if doc_value is None:
            # Pole nie istnieje w dokumencie — warunek nie jest spelniony
            logger.debug(
                "filter_engine | id_filter=%d: pole '%s' nie istnieje w doc — warunek False",
                id_filter, field_name,
            )
            return False

        result = _compare(doc_value, operator, filter_value)
        logger.debug(
            "filter_engine | id_filter=%d: %s %s %r → %s",
            id_filter, field_name, operator, filter_value, result,
        )

        if not result:
            return False

    return True


# =============================================================================
# Ewaluacja filtru universal
# =============================================================================

async def _evaluate_universal_filter(
    db: AsyncSession,
    id_filter: int,
    function_name: str | None,
    doc: dict,
) -> bool:
    """
    Wywoluje funkcje SQL w try/except.
    Funkcja SQL musi zwracac INT (0/NULL = brak dopasowania, !=0 = dopasowanie).
    Nazwa funkcji jest whitelist-owana przed wywolaniem.

    BEZPIECZENSTWO: Whitelist regex ^[a-zA-Z0-9_]+$ + maksymalna dlugosc.
    Blad SQL = loguj + return False (nie przerywa potoku).
    """
    if not function_name:
        logger.warning(
            "filter_engine | id_filter=%d universal: brak universal_function — pomijam",
            id_filter,
        )
        return False

    # Whitelist nazwy funkcji
    if len(function_name) > _FUNC_NAME_MAX_LEN:
        logger.error(
            "filter_engine | id_filter=%d: universal_function za dluga (%d zn) — odrzucam",
            id_filter, len(function_name),
        )
        return False

    if not _FUNC_NAME_RE.match(function_name):
        logger.error(
            "filter_engine | id_filter=%d: niepoprawna nazwa funkcji '%s' — odrzucam",
            id_filter, function_name,
        )
        return False

    # Przygotuj parametry dla funkcji SQL
    # Funkcja musi akceptowac @id_document NVARCHAR(100), @id_source INT
    id_document = doc.get("id_document", "")
    id_source   = doc.get("id_source", 0)

    try:
        result_row = await db.execute(
            text(
                f"SELECT [{_SCHEMA}].[{function_name}](:id_doc, :id_src)"
            ),
            {"id_doc": str(id_document), "id_src": int(id_source)},
        )
        result = result_row.scalar()
        matched = result is not None and result != 0

        logger.debug(
            "filter_engine | id_filter=%d universal: %s(%r, %d) = %r → %s",
            id_filter, function_name, id_document, id_source, result, matched,
        )
        return matched

    except Exception as exc:
        # KRYTYCZNE: blad SQL traktujemy jako brak dopasowania — nie przerywamy potoku
        logger.error(
            "filter_engine | id_filter=%d universal: blad wywolania %s: %s",
            id_filter, function_name, exc,
        )
        return False


# =============================================================================
# Pomocnicze — operacje na polach i wartosciach
# =============================================================================

def _get_nested(doc: dict, field_path: str) -> Any:
    """
    Pobiera wartosc z dokumentu. Obsluguje zagniezdzone pola przez '.'.
    Przyklad: 'extra.amount_gross' → doc['extra']['amount_gross']
    """
    parts = field_path.split(".")
    value = doc
    for part in parts:
        if not isinstance(value, dict):
            return None
        value = value.get(part)
        if value is None:
            return None
    return value


def _cast_value(raw: str, target: Any) -> Any:
    """
    Rzutuje stringa filter_value na typ zbliżony do wartosci dokumentu.
    Jesli rzutowanie sie nie uda — zwraca oryginaly string (porownanie tekstowe).
    """
    if isinstance(target, (int, float)):
        try:
            return type(target)(raw)
        except (ValueError, TypeError):
            pass
    if isinstance(target, Decimal):
        try:
            return Decimal(raw)
        except InvalidOperation:
            pass
    return raw


def _compare(doc_value: Any, operator: str, filter_value: str) -> bool:
    """
    Porownuje wartosc dokumentu z wartoscia filtra uzywajac podanego operatora.
    Whitelist: eq, neq, contains, gt, lt, gte, lte.

    Rzutuje filter_value do typu doc_value jesli to mozliwe.
    """
    casted = _cast_value(filter_value, doc_value)

    # Porownanie tekstowe dla 'contains' zawsze na stringu
    if operator == "contains":
        return str(filter_value).lower() in str(doc_value).lower()

    try:
        if operator == "eq":
            return doc_value == casted
        if operator == "neq":
            return doc_value != casted
        if operator == "gt":
            return doc_value > casted
        if operator == "lt":
            return doc_value < casted
        if operator == "gte":
            return doc_value >= casted
        if operator == "lte":
            return doc_value <= casted
    except TypeError:
        # Typy nieporownywalne — traktuj jako False
        logger.debug(
            "filter_engine | _compare: nieporownywalne typy %s vs %s (op=%s)",
            type(doc_value).__name__, type(casted).__name__, operator,
        )
        return False

    logger.warning("filter_engine | Nieznany operator '%s' — zwracam False", operator)
    return False