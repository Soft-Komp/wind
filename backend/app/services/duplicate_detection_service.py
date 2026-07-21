# backend/app/services/duplicate_detection_service.py
"""
Serwis wykrywania duplikatow dokumentow — DuplicateDetectionService.

NAPRAWA 2026-07-17 (KRYTYCZNA): ten plik NIGDY WCZESNIEJ NIE ISTNIAL w
repozytorium, mimo ze:
  1. Jest szczegolowo opisany w dokumentacji projektu (Etap2_Instrukcja_
     Techniczna, sekcja 4.11 — trzy niezalezne wersje pliku, wszystkie
     ze soba zgodne co do specyfikacji algorytmu).
  2. Byl importowany przez webhook_service.py OD POCZATKU istnienia tego
     pliku (from app.services.duplicate_detection_service import
     DuplicateDetectionService) — a wiec KAZDE pierwsze (nie-idempotentne)
     wywolanie webhooka konczylo sie ModuleNotFoundError -> HTTP 500,
     niezaleznie od jakichkolwiek zmian Tier 1/Tier 2 wprowadzonych w tej
     samej sesji. Blad byl wczesniej maskowany przez niezalezny problem
     routingu (podwojny prefix /api/v1/webhooks/webhooks/...), ktory
     blokowal ruch na etapie 404, zanim doszlo do tego glebszego problemu.

WAZNE — rozbieznosc specyfikacja vs. kod wywolujacy:
  Dokumentacja opisuje metode check_duplicate(unified_doc, db), wywolywana
  PRZED zapisem instancji, zwracajaca wynik Literal["certain","probable","none"].
  Rzeczywisty kod wywolujacy (webhook_service.py, juz istniejacy, NIE
  modyfikowany w tej naprawie) wola:

      is_duplicate = await DuplicateDetectionService.check_and_mark(
          db, id_instance=id_instance, id_source=source.id_source,
          id_document=unified_doc.id_document,
      )

  — PO wstawieniu instancji (id_instance juz istnieje w bazie, w tej samej,
  niescommitowanej jeszcze transakcji), oczekujac bool. Ta implementacja
  realizuje SYGNATURE WYMAGANA PRZEZ ISTNIEJACY KOD, nie te opisana w
  dokumentacji — inaczej naprawa nie zadzialalaby. Wewnetrzny algorytm
  (3 z 4 kryteriow, certain/probable) jest jednak zgodny z opisem
  dokumentacji, tylko opakowany w inny interfejs.

Algorytm (Etap2_Instrukcja_Techniczna, sekcja 4.11):
  Sprawdza dokumenty juz zapisane (tego samego zrodla) pod katem zgodnosci
  co najmniej 3 z 4 warunkow:
    1. numer dokumentu (dokladne dopasowanie)
    2. kwota brutto (tolerancja 0.01)
    3. kontrahent (po NIP dokladnie LUB podobienstwo nazwy)
    4. data dokumentu (dokladne dopasowanie)
  Zgodnosc numeru I NIP jednoczesnie = duplikat PEWNY (certain) —
  niezaleznie od pozostalych dwoch kryteriow.

ZALOZENIA WLASNE (dokumentacja ich NIE precyzuje — jawnie oznaczone,
do weryfikacji/korekty jesli nie odpowiadaja oczekiwaniom biznesowym):
  - Kandydaci do porownania: TYLKO instancje tego samego id_source, status
    NOT IN ('approved','cancelled') — te same statusy wykluczone co
    idempotencja webhooka (Tier 1a), dla spojnosci pojeciowej "aktywny
    dokument w systemie".
  - Okno czasowe: ostatnie 90 dni (created_at) — bez tego pelne skanowanie
    calej historii zrodla przy KAZDYM webhooku bylby kosztowne. Liczba
    dni NIE jest konfigurowalna przez SystemConfig w tej wersji (mozna
    dodac, jesli 90 dni okaze sie niewlasciwe w praktyce).
  - Prog "probable": >=3 z 4 kryteriow (zgodnie z opisem "co najmniej 3 z 4").
  - Podobienstwo nazwy kontrahenta: difflib.SequenceMatcher (biblioteka
    standardowa, zero nowych zaleznosci), prog 0.85, porownanie
    case-insensitive po .strip().
  - Zrodlo danych do porownania: extra_data (JSON) instancji — bo
    doc_number/nip/contractor/doc_date NIE SA osobnymi kolumnami tabeli
    skw_document_approval_instances (potwierdzone w kodzie: tylko
    document_title i document_amount sa kolumnami, reszta w extra_data).
  - UWAGA na nazwe klucza: UnifiedDocument.to_extra_data_json() zapisuje
    kontrahenta pod kluczem "contractor", NIE "contractor_name" —
    latwa do pomylenia niespojnosc nazewnictwa miedzy atrybutem klasy
    a kluczem JSON. Ten plik uzywa poprawnego klucza "contractor".
  - Fail-safe: KAZDY blad wewnetrzny (parsowanie JSON, zapytanie SQL,
    cokolwiek nieoczekiwanego) jest lapany i logowany jako ERROR, funkcja
    zwraca False zamiast propagowac wyjatek — webhook_service.py NIE MA
    wlasnego try/except wokol tego wywolania, wiec nieobsluzony wyjatek
    tutaj zamienilby sie w HTTP 500 dla KAZDEGO nowego dokumentu, dokladnie
    tak jak przy braku tego pliku. Awaria wykrywania duplikatow nie moze
    blokowac przyjecia dokumentu.

STATUS: NIGDY nie testowane na zywym wywolaniu w momencie napisania.
Wymaga weryfikacji na prawdziwym webhooku z rzeczywistymi, zdublowanymi
payloadami przed pelnym zaufaniem.

UWAGA: from __future__ import annotations — NIGDY w tym pliku (SQLAlchemy ORM
uzywane posrednio przez AsyncSession w innych czesciach modulu).
"""
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_SCHEMA = "dbo"

# Te same statusy wykluczone co idempotencja webhooka (webhook_service.py::
# _ACTIVE_STATUSES_EXCLUDED) — swiadoma spojnosc pojeciowa "aktywny dokument".
# Jesli jedno sie zmieni, rozwazyc zmiane drugiego rownolegle.
_ACTIVE_STATUSES_EXCLUDED = ("approved", "cancelled")

# Zalozenie wlasne — patrz docstring modulu.
_DEFAULT_LOOKBACK_DAYS = 90

_AMOUNT_TOLERANCE = Decimal("0.01")
_NAME_SIMILARITY_THRESHOLD = 0.85

# Prog liczby zgodnych kryteriow dla wyniku "probable" (dokumentacja:
# "co najmniej 3 z 4"). "certain" (numer+NIP) omija ten prog niezaleznie.
_MIN_CRITERIA_FOR_PROBABLE = 3


class DuplicateDetectionService:
    """Wykrywanie potencjalnych duplikatow dokumentow tego samego zrodla."""

    @staticmethod
    async def check_and_mark(
        db: AsyncSession,
        *,
        id_instance: int,
        id_source: int,
        id_document: str,
    ) -> bool:
        """
        Sprawdza czy nowo wstawiona instancja (id_instance) jest
        prawdopodobnym duplikatem innej, aktywnej instancji tego samego
        zrodla. Jesli tak — AKTUALIZUJE (UPDATE, nie INSERT) status tej
        instancji na 'duplicate_pending' i dopisuje adnotacje do extra_data.

        Wywolywane PO wstawieniu instancji, PRZED commit (ta sama
        transakcja co INSERT — jesli ta funkcja zawiedzie/rzuci, cala
        transakcja webhooka i tak sie wycofa dzieki fail-safe ponizej,
        ktory NIGDY nie rzuca dalej).

        Returns:
            True jesli oznaczono jako duplicate_pending, False w przeciwnym razie.
        """
        try:
            return await DuplicateDetectionService._check_and_mark_impl(
                db, id_instance=id_instance, id_source=id_source, id_document=id_document,
            )
        except Exception as exc:
            logger.error(
                "DuplicateDetectionService.check_and_mark: nieoczekiwany blad "
                "(fail-safe — traktuje jako NIE duplikat, dokument mimo to zostaje "
                "przyjety normalnie) | id_instance=%s id_source=%s id_document=%s: %s",
                id_instance, id_source, id_document, exc, exc_info=True,
            )
            return False

    @staticmethod
    async def _check_and_mark_impl(
        db: AsyncSession,
        *,
        id_instance: int,
        id_source: int,
        id_document: str,
    ) -> bool:
        # ── Krok 1: dane nowej instancji (juz wstawionej w tej transakcji) ───
        new_row_result = await db.execute(
            text(f"""
                SELECT [document_amount], [extra_data]
                FROM [{_SCHEMA}].[skw_document_approval_instances]
                WHERE [id_instance] = :i
            """),
            {"i": id_instance},
        )
        new_row = new_row_result.fetchone()
        if new_row is None:
            logger.warning(
                "DuplicateDetectionService: id_instance=%s nie znalezione zaraz po "
                "insert — nieoczekiwany stan, pomijam sprawdzenie duplikatow",
                id_instance,
            )
            return False

        new_amount, new_extra_raw = new_row
        new_extra = _parse_extra(new_extra_raw)
        new_doc_number = _norm_str(new_extra.get("doc_number"))
        new_nip        = _norm_str(new_extra.get("nip"))
        new_contractor = _norm_str(new_extra.get("contractor"))
        new_doc_date   = _norm_str(new_extra.get("doc_date"))

        # Brak jakichkolwiek danych do porownania — nie ma sensu szukac
        # (unika falszywych trafien opartych wylacznie na pustych polach).
        if not any([new_doc_number, new_amount is not None, new_nip, new_contractor, new_doc_date]):
            return False

        # ── Krok 2: kandydaci — to samo zrodlo, inna instancja, aktywne,
        #    w oknie czasowym ────────────────────────────────────────────────
        excluded_placeholders = ", ".join(f":excl{i}" for i in range(len(_ACTIVE_STATUSES_EXCLUDED)))
        params: dict[str, Any] = {
            "src":           id_source,
            "inst":          id_instance,
            "lookback_days": _DEFAULT_LOOKBACK_DAYS,
        }
        for i, val in enumerate(_ACTIVE_STATUSES_EXCLUDED):
            params[f"excl{i}"] = val

        candidates_result = await db.execute(
            text(f"""
                SELECT [id_instance], [document_amount], [extra_data]
                FROM [{_SCHEMA}].[skw_document_approval_instances]
                WHERE [id_source] = :src
                  AND [id_instance] <> :inst
                  AND [status] NOT IN ({excluded_placeholders})
                  AND [created_at] >= DATEADD(DAY, -:lookback_days, SYSUTCDATETIME())
            """),
            params,
        )
        candidates = candidates_result.fetchall()

        best_id: int | None = None
        best_score = 0
        best_certain = False

        for cand_id, cand_amount, cand_extra_raw in candidates:
            cand_extra = _parse_extra(cand_extra_raw)
            cand_doc_number = _norm_str(cand_extra.get("doc_number"))
            cand_nip        = _norm_str(cand_extra.get("nip"))
            cand_contractor = _norm_str(cand_extra.get("contractor"))
            cand_doc_date   = _norm_str(cand_extra.get("doc_date"))

            score = 0

            number_match = bool(new_doc_number) and new_doc_number == cand_doc_number
            if number_match:
                score += 1

            if _amounts_match(new_amount, cand_amount):
                score += 1

            nip_match = bool(new_nip) and new_nip == cand_nip
            name_match = (
                not nip_match
                and bool(new_contractor) and bool(cand_contractor)
                and _names_similar(new_contractor, cand_contractor)
            )
            if nip_match or name_match:
                score += 1

            if bool(new_doc_date) and new_doc_date == cand_doc_date:
                score += 1

            certain = number_match and nip_match

            # Preferuj: certain > wyzszy score. Przy remisie — pierwszy znaleziony.
            if certain and not best_certain:
                best_id, best_score, best_certain = cand_id, score, True
            elif not best_certain and score > best_score:
                best_id, best_score, best_certain = cand_id, score, False

        is_duplicate = best_certain or best_score >= _MIN_CRITERIA_FOR_PROBABLE
        if not is_duplicate or best_id is None:
            return False

        # ── Krok 3: oznacz NOWA instancje jako duplicate_pending ─────────────
        new_extra["duplicate_of_id_instance"] = best_id
        new_extra["duplicate_confidence"] = "certain" if best_certain else "probable"
        new_extra["duplicate_score"] = best_score
        new_extra["duplicate_detected_at"] = datetime.utcnow().isoformat()

        await db.execute(
            text(f"""
                UPDATE [{_SCHEMA}].[skw_document_approval_instances]
                SET [status] = N'duplicate_pending',
                    [extra_data] = :extra,
                    [updated_at] = :now
                WHERE [id_instance] = :i
            """),
            {
                "extra": json.dumps(new_extra, ensure_ascii=False, default=str),
                "now":   datetime.utcnow(),
                "i":     id_instance,
            },
        )

        logger.warning(
            "DuplicateDetectionService: WYKRYTO duplikat | nowa_instancja=%s "
            "podobna_do=%s confidence=%s score=%s/4 id_source=%s id_document=%s",
            id_instance, best_id,
            "certain" if best_certain else "probable", best_score,
            id_source, id_document,
        )
        return True


# =============================================================================
# Funkcje pomocnicze
# =============================================================================

def _parse_extra(raw: str | None) -> dict[str, Any]:
    """Bezpieczne parsowanie extra_data — pusty dict przy bledzie, nigdy wyjatek."""
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _norm_str(val: Any) -> str | None:
    """Normalizuje wartosc do porownania stringowego — None dla pustych/None."""
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _amounts_match(a: Any, b: Any) -> bool:
    """Porownanie kwot z tolerancja _AMOUNT_TOLERANCE. False jesli ktoras None."""
    if a is None or b is None:
        return False
    try:
        return abs(Decimal(str(a)) - Decimal(str(b))) <= _AMOUNT_TOLERANCE
    except (InvalidOperation, ValueError, TypeError):
        return False


def _names_similar(a: str, b: str) -> bool:
    """Podobienstwo nazw kontrahentow — difflib, case-insensitive, prog 0.85."""
    try:
        ratio = SequenceMatcher(None, a.lower(), b.lower()).ratio()
        return ratio >= _NAME_SIMILARITY_THRESHOLD
    except Exception:
        return False