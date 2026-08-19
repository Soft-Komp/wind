# backend/app/services/filter_engine.py
"""
Silnik filtrow automatycznego przydzialu sciezek akceptacyjnych.

Odpowiedzialnosc:
    resolve_path() — glowna funkcja; zwraca id_path lub None.

Algorytm (wg specyfikacji 3.7 + rozszerzenie AND/OR z 2026-07-31):
    1. Pobierz aktywne filtry posortowane po priority DESC (wyzszy = wazniejszy).
    2. Dla kazdego filtra:
       a. Sprawdz typ: standard lub universal
       b. standard: oceń warunki przez evaluate_standard_filter() - logika
          okreslona kolumna logic_operator ('AND' = wszystkie warunki musza
          byc spelnione, 'OR' = wystarczy jeden). Dotyczy WYLACZNIE filtrow
          typu standard - filtry universal nie maja warunkow i nie odczytuja
          tej kolumny w ogole.
       c. universal: wywolaj funkcje SQL przez evaluate_universal_filter()
          — try/except: blad = loguj, traktuj jako brak dopasowania, NIE przerywaj
    3. Ostatni pasujacy filtr wygrywa (last match wins — najszczegolowszy).
    4. Brak dopasowan → None → dokument idzie do pending_dispatch.

ZMIANA 2026-07-31 (migracja 0073, filter_type='standard' only):
    Dodano kolumne skw_approval_filters.logic_operator (NVARCHAR(3),
    CHECK IN ('AND','OR'), DEFAULT 'AND' - potwierdzone pisemnie przez
    wlasciciela produktu, 2026-07-31). _evaluate_standard_filter() ewaluuje
    TERAZ WSZYSTKIE warunki (bez early-return/short-circuit jak poprzednio)
    - celowa zmiana zachowania wewnetrznego: pelna macierz warunkow trafia
    do logu za kazdym razem, niezaleznie od tego ktory warunek zawiodl jako
    pierwszy. Wynik koncowy (dopasowanie/brak) jest identyczny jak przy
    short-circuit dla trybu AND - zmienia sie wylacznie kompletnosc logu
    diagnostycznego, nie logika biznesowa.
    Pusty filtr (0 warunkow):
        - AND: catch-all, zawsze pasuje (zachowanie bez zmian)
        - OR:  nigdy nie pasuje (pusta alternatywa logiczna = falsz)

Bezpieczenstwo:
    - universal: whitelist nazwy funkcji SQL (regex ^[a-zA-Z0-9_]+$)
    - standard: cast wartosci przez _cast_value() — brak raw SQL od usera

Integracja z dispatch():
    id_path = await filter_engine.resolve_path(db, id_source, unified_doc)
    Jesli None → dispatch bez sciezki (status=pending_dispatch, id_path=None)
    Jesli int  → dispatch z ta sciezka

UWAGA: sygnatura zewnetrzna resolve_path() jest BEZ ZMIAN wzgledem wersji
sprzed 2026-07-31 - zmiany dotycza wylacznie wnetrza funkcji prywatnej
_evaluate_standard_filter(). Jesli w kodzie wywolujacym (auto_dispatch_task.py
lub inne) wystepuje niezgodnosc kolejnosci argumentow wzgledem tej sygnatury
- to problem NIEZALEZNY od tej zmiany, wymagajacy odrebnej weryfikacji na
zywym pliku przed wdrozeniem.

UWAGA: from __future__ import annotations — NIGDY w tym pliku.
"""

import json
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

# Dopuszczalne wartosci logic_operator - musi byc zgodne z CHECK constraint
# CHK_saf_logic_operator w migracji 0073. Kazda inna wartosc w kolumnie
# (co teoretycznie nie powinno sie zdarzyc przez CHECK, ale bronimy sie
# rowniez w Pythonie - redundancja celowa) traktowana jest jak 'AND'.
_VALID_LOGIC_OPERATORS = ("AND", "OR")
_DEFAULT_LOGIC_OPERATOR = "AND"


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
    #
    # NAPRAWA 2026-08-10 (zgloszenie frontu, notatka auto-dispatch 2026-08-07,
    # pkt 3.4 / sugestia 4.4): dotychczasowy ORDER BY priority ASC nie mial
    # drugiego kryterium sortowania. Przy kilku filtrach o identycznym priority
    # (front zglosil, ze to w praktyce czesty przypadek) o tym "ktory jest
    # ostatni" decydowala niedeterministyczna kolejnosc zwracana przez MSSQL —
    # mogla sie zmienic miedzy wywolaniami bez zadnej zmiany danych. Dodano
    # id_filter ASC jako stabilny tie-break, doslownie wg sugestii frontu.
    #
    # BRAK FORMALNEJ SPECYFIKACJI dla tej zmiany — zaden dokument Etap2 nie
    # okresla zachowania przy remisie priority. Patrz PODSUMOWANIE_zmian.md.
    filters_rows = await db.execute(
        text(
            f"SELECT f.[id_filter], f.[filter_type], f.[id_path], "
            f"       f.[universal_function], f.[priority], f.[logic_operator] "
            f"FROM [{_SCHEMA}].[skw_approval_filters] f "
            f"WHERE f.[is_active] = 1 "
            f"  AND (f.[id_source] = :src OR f.[id_source] IS NULL) "
            f"ORDER BY f.[priority] ASC, f.[id_filter] ASC"
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

    for id_filter, filter_type, id_path, universal_func, priority, logic_operator in filters:
        try:
            matched = False

            if filter_type == "standard":
                matched = await _evaluate_standard_filter(
                    db, id_filter, unified_doc, logic_operator
                )
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

def _normalize_logic_operator(raw: str | None, id_filter: int) -> str:
    """
    Waliduje wartosc logic_operator odczytana z bazy. Redundancja celowa
    wzgledem CHECK constraint (CHK_saf_logic_operator z migracji 0073) —
    jesli mimo constraintu w kolumnie pojawi sie cos innego niz AND/OR
    (np. NULL na starych wierszach sprzed migracji, przed jej zastosowaniem),
    NIE przerywamy przetwarzania - logujemy blad danych i uzywamy bezpiecznego
    domyslnego zachowania (AND), ktore jest zgodne z zachowaniem sprzed
    wprowadzenia tej kolumny.
    """
    if raw in _VALID_LOGIC_OPERATORS:
        return raw

    logger.error(
        "filter_engine | id_filter=%d: nieprawidlowa/brakujaca wartosc "
        "logic_operator=%r (oczekiwano AND/OR) — uzywam domyslnej '%s'. "
        "SPRAWDZ integralnosc danych w skw_approval_filters dla tego filtra.",
        id_filter, raw, _DEFAULT_LOGIC_OPERATOR,
    )
    return _DEFAULT_LOGIC_OPERATOR


async def _evaluate_standard_filter(
    db: AsyncSession,
    id_filter: int,
    doc: dict,
    logic_operator: str | None = _DEFAULT_LOGIC_OPERATOR,
) -> bool:
    """
    Sprawdza warunki filtru standard.

    logic_operator == 'AND': WSZYSTKIE warunki musza byc spelnione.
    logic_operator == 'OR':  WYSTARCZY jeden spelniony warunek.

    Wartosc dokumentu porownywana z wartoscia filtra po caście do wlasciwego typu.

    UWAGA (zmiana 2026-07-31): funkcja ewaluuje TERAZ WSZYSTKIE warunki
    zawsze - bez wczesnego return po pierwszym niespelnionym warunku
    (poprzednia wersja robila short-circuit dla AND). Powod: pelna macierz
    wynikow trafia do jednego strukturalnego logu JSON, co pozwala
    jednoznacznie odtworzyc "dlaczego ten dokument NIE trafil na sciezke"
    bez koniecznosci wlaczania DEBUG i analizy wielu osobnych linii logu.
    Wynik koncowy (bool) jest identyczny jak przy short-circuit.
    """
    logic_operator = _normalize_logic_operator(logic_operator, id_filter)

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
        # Pusty filtr - zachowanie zalezne od trybu (ustalone 2026-07-31):
        #   AND -> catch-all, zawsze pasuje (bez zmian wzgledem wersji
        #          sprzed logic_operator)
        #   OR  -> nigdy nie pasuje (pusta alternatywa logiczna = falsz)
        matched_empty = logic_operator == "AND"
        logger.debug(
            "filter_engine | id_filter=%d: brak warunkow, logic_operator=%s — %s",
            id_filter, logic_operator,
            "catch-all (True)" if matched_empty else "pusty OR (False)",
        )
        logger.info(json.dumps({
            "event": "filter_engine.evaluate_standard",
            "id_filter": id_filter,
            "logic_operator": logic_operator,
            "conditions_count": 0,
            "conditions": [],
            "matched": matched_empty,
        }, default=str, ensure_ascii=False))
        return matched_empty

    results: list[dict[str, Any]] = []

    for field_name, operator, filter_value in conditions:
        doc_value = _get_nested(doc, field_name)

        if doc_value is None:
            cond_result = False
            reason = "pole_nieobecne_w_dokumencie"
        else:
            cond_result = _compare(doc_value, operator, filter_value)
            reason = None

        results.append({
            "field": field_name,
            "operator": operator,
            "expected": filter_value,
            "actual": doc_value,
            "result": cond_result,
            **({"reason": reason} if reason else {}),
        })

        logger.debug(
            "filter_engine | id_filter=%d: %s %s %r (doc=%r) → %s",
            id_filter, field_name, operator, filter_value, doc_value, cond_result,
        )

    if logic_operator == "OR":
        matched = any(r["result"] for r in results)
    else:
        matched = all(r["result"] for r in results)

    # Pelny log strukturalny JSON - absudalnie szczegolowy celowo, zgodnie
    # z wymogiem pelnej odtwarzalnosci decyzji dispatchu.
    logger.info(json.dumps({
        "event": "filter_engine.evaluate_standard",
        "id_filter": id_filter,
        "logic_operator": logic_operator,
        "conditions_count": len(results),
        "conditions": results,
        "matched": matched,
    }, default=str, ensure_ascii=False))

    return matched


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

    UWAGA: filtry universal NIE MAJA warunkow i NIE odczytuja logic_operator -
    ta kolumna jest dla tego typu filtra calkowicie ignorowana.

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