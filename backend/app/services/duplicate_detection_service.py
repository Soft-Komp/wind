# backend/app/services/duplicate_detection_service.py
"""
Serwis wykrywania duplikatow dokumentow — DuplicateDetectionService.

WERSJA 2 (2026-07-28) — PRZEBUDOWA, porzuca model D-09.

PORZUCONE (decyzja wlasciciela projektu, jawna, 2026-07-28):
  - adnotacja duplikatu w extra_data (duplicate_of_id_instance/
    duplicate_confidence/duplicate_score) — ZASTAPIONA kolumnami
    matched_instance_id/match_type/match_reason (migracja 0068,
    analogia do superseded_by_instance_id z migracji 0064)
  - ograniczenie do tego samego id_source — USUNIETE, sprawdzanie
    obejmuje WSZYSTKIE zrodla
  - okno czasowe 90 dni (created_at) — USUNIETE, brak ograniczenia
  - wykluczenie statusow approved/cancelled z kandydatow — USUNIETE,
    duplikat moze wskazywac rowniez na dokument archiwalny/terminalny

Kaskada metod (w tej kolejnosci — pierwsze trafienie wygrywa):
  1. ksef_id            — dokladne dopasowanie extra_data.ksef_id
                           (przez wyliczana, indeksowana kolumne
                           ksef_id_lookup). Duplikat PEWNY.
  2. file_sha256        — dokladne dopasowanie hasha pliku (manual/
                           ftp/email). Duplikat PEWNY.
  3. invoice_fingerprint — NIP + numer + data + kwota (tolerancja
                           0.01) + waluta, WSZYSTKIE rownoczesnie.
                           Duplikat PEWNY.
  4. contractor_fallback — jak wyzej, ale BEZ NIP (fallback po nazwie
                           kontrahenta, podobienstwo >=0.85). Duplikat
                           TYLKO "probable" — ZAWSZE trafia do recznej
                           weryfikacji referenta, nigdy nie jest
                           traktowany jako automatycznie potwierdzony
                           (wymog: "zgodnosc nazwy kontrahenta nie
                           powinna automatycznie potwierdzac duplikatu").

INTERFEJS ZACHOWANY BEZ ZMIAN wzgledem wersji 1 — webhook_service.py
(NIE modyfikowany w tej sesji, plik nie byl dostarczony) woluje:

    is_duplicate = await DuplicateDetectionService.check_and_mark(
        db, id_instance=id_instance, id_source=source.id_source,
        id_document=unified_doc.id_document,
    )

id_source/id_document sa teraz uzywane WYLACZNIE do logowania — kaskada
metod powyzej ich nie potrzebuje do zapytan (brak ograniczenia po zrodle).

Fail-safe: bez zmian wzgledem wersji 1 — kazdy blad wewnetrzny jest
lapany i logowany jako ERROR, funkcja zwraca False. Awaria wykrywania
duplikatow NIE MOZE blokowac przyjecia dokumentu.

UWAGA: from __future__ import annotations — NIGDY w tym pliku.
"""
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_SCHEMA = "dbo"

_AMOUNT_TOLERANCE = Decimal("0.01")
_NAME_SIMILARITY_THRESHOLD = 0.85


class DuplicateDetectionService:
    """Wykrywanie duplikatow dokumentow — kaskada 4 metod, bez ograniczenia zrodla/czasu/statusu."""

    @staticmethod
    async def check_and_mark(
        db: AsyncSession,
        *,
        id_instance: int,
        id_source: int,
        id_document: str,
    ) -> bool:
        try:
            return await DuplicateDetectionService._check_and_mark_impl(
                db, id_instance=id_instance, id_source=id_source, id_document=id_document,
            )
        except Exception as exc:
            logger.error(
                "DuplicateDetectionService.check_and_mark: nieoczekiwany blad "
                "(fail-safe — dokument mimo to zostaje przyjety normalnie) | "
                "id_instance=%s id_source=%s id_document=%s: %s",
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
        row_result = await db.execute(
            text(f"""
                SELECT [document_amount], [extra_data], [file_sha256], [ksef_id_lookup],
                       [created_at]
                FROM [{_SCHEMA}].[skw_document_approval_instances]
                WHERE [id_instance] = :i
            """),
            {"i": id_instance},
        )
        row = row_result.fetchone()
        if row is None:
            logger.warning(
                "DuplicateDetectionService: id_instance=%s nie znalezione — pomijam",
                id_instance,
            )
            return False

        # NAPRAWA (2026-07-28, incydent: pary wzajemne 523<->1406 i podobne
        # po ponownym sprawdzeniu po OCR/korekcie). Kandydat MUSI byc scisle
        # starszy (albo rowny w czasie z nizszym id_instance jako tie-break)
        # — bez tego ponowne sprawdzenie starego dokumentu moze znalezc
        # jego wlasny, pozniejszy duplikat i wskazac NA NIEGO jako "oryginal",
        # tworzac pare wzajemna. Warunek ten sam we wszystkich 4 metodach
        # ponizej — patrz _older_than_clause().
        new_amount, new_extra_raw, new_file_sha256, new_ksef_id, new_created_at = row
        new_extra = _parse_extra(new_extra_raw)
        new_doc_number = _norm_str(new_extra.get("doc_number"))
        new_nip        = _norm_str(new_extra.get("nip"))
        new_contractor = _norm_str(new_extra.get("contractor"))
        new_doc_date   = _norm_str(new_extra.get("doc_date"))
        new_currency   = _norm_str(new_extra.get("currency")) or "PLN"

        # ── METODA 1: KSeF ID — bez ograniczenia zrodla/czasu/statusu ────────
        # NAPRAWA (2026-07-28, incydent produkcyjny: instancje 146/1509):
        # ksef_id_lookup dziala TYLKO gdy extra_data.ksef_id zostalo faktycznie
        # zapisane. Historyczne wiersze (np. z jednorazowej migracji Krok 0 ze
        # starej tabeli skw_faktura_akceptacja) moga miec extra_data calkowicie
        # puste, mimo ze ich id_document JEST numerem KSeF — dla zrodla 'fakir'
        # to zalozenie projektowe (FakirDocumentAdapter: id_document = KSEF_ID,
        # ten sam wzorzec co juz uzywany w widokach SQL, patrz migracje 0062/
        # 0066/0067: LEFT JOIN ... ON ds.source_name='fakir' AND fah.KSEF_ID =
        # dai.id_document). Dopisujemy wiec DODATKOWY warunek: kandydat ze
        # zrodla 'fakir' dopasowuje TAKZE po surowym id_document, niezaleznie
        # od tego, czy jego extra_data w ogole cokolwiek zawiera.
        #
        # SWIADOMIE NIE robimy tego dla wszystkich zrodel — dla generycznych
        # zrodel typu database/api z prostym, sekwencyjnym id_column (np.
        # auto-increment) por0wnywanie surowego id_document miedzy zrodlami
        # byloby ryzykowne (przypadkowa kolizja technicznych identyfikatorow
        # z dwoch niepowiazanych systemow). Ograniczenie do source_name='fakir'
        # jest bezpieczne, bo tam id_document ma znaczenie biznesowe z zalozenia.
        if new_ksef_id:
            match = (await db.execute(
                text(f"""
                    SELECT TOP 1 [i2].[id_instance]
                    FROM [{_SCHEMA}].[skw_document_approval_instances] i2
                    JOIN [{_SCHEMA}].[skw_document_sources] ds2
                         ON ds2.[id_source] = i2.[id_source]
                    WHERE [i2].[id_instance] <> :i
                      AND {_older_than_clause("i2")}
                      AND (
                          [i2].[ksef_id_lookup] = :k
                          OR (ds2.[source_name] = N'fakir' AND [i2].[id_document] = :k)
                      )
                    ORDER BY [i2].[created_at] ASC
                """),
                {"k": new_ksef_id, "i": id_instance, "new_created_at": new_created_at},
            )).fetchone()
            if match:
                await _mark_duplicate(
                    db, id_instance, match[0], "ksef_id",
                    f"Identyczny numer KSeF ({new_ksef_id}) jak instancja #{match[0]}.",
                )
                logger.warning(
                    "DuplicateDetectionService: DUPLIKAT (ksef_id) | nowa=%s stara=%s "
                    "id_source=%s id_document=%s",
                    id_instance, match[0], id_source, id_document,
                )
                return True

        # ── METODA 2: SHA-256 pliku — ponowne wgranie tego samego pliku ──────
        if new_file_sha256:
            match = (await db.execute(
                text(f"""
                    SELECT TOP 1 [id_instance] FROM [{_SCHEMA}].[skw_document_approval_instances]
                    WHERE [file_sha256] = :h AND [id_instance] <> :i
                    ORDER BY [created_at] ASC
                """),
                {"h": new_file_sha256, "i": id_instance},
            )).fetchone()
            if match:
                await _mark_duplicate(
                    db, id_instance, match[0], "file_sha256",
                    f"Identyczny hash SHA-256 pliku jak instancja #{match[0]} "
                    f"— ponowne wgranie tego samego dokumentu.",
                )
                logger.warning(
                    "DuplicateDetectionService: DUPLIKAT (file_sha256) | nowa=%s stara=%s "
                    "id_source=%s id_document=%s",
                    id_instance, match[0], id_source, id_document,
                )
                return True

        # ── METODA 3: fingerprint faktury (NIP+numer+data+kwota+waluta) ─────
        if new_nip and new_doc_number and new_doc_date and new_amount is not None:
            candidates = (await db.execute(
                text(f"""
                    SELECT [id_instance], [document_amount], [extra_data]
                    FROM [{_SCHEMA}].[skw_document_approval_instances]
                    WHERE [id_instance] <> :i
                      AND JSON_VALUE([extra_data], '$.nip') = :nip
                      AND JSON_VALUE([extra_data], '$.doc_number') = :num
                """),
                {"i": id_instance, "nip": new_nip, "num": new_doc_number},
            )).fetchall()
            for cand_id, cand_amount, cand_extra_raw in candidates:
                cand_extra = _parse_extra(cand_extra_raw)
                cand_date = _norm_str(cand_extra.get("doc_date"))
                cand_currency = _norm_str(cand_extra.get("currency")) or "PLN"
                if (
                    cand_date == new_doc_date
                    and cand_currency == new_currency
                    and _amounts_match(new_amount, cand_amount)
                ):
                    await _mark_duplicate(
                        db, id_instance, cand_id, "invoice_fingerprint",
                        f"Zgodnosc NIP ({new_nip}), numeru ({new_doc_number}), "
                        f"daty i kwoty brutto z instancja #{cand_id}.",
                    )
                    logger.warning(
                        "DuplicateDetectionService: DUPLIKAT (invoice_fingerprint) | "
                        "nowa=%s stara=%s id_source=%s id_document=%s",
                        id_instance, cand_id, id_source, id_document,
                    )
                    return True

        # ── METODA 4: fallback po nazwie kontrahenta (BRAK NIP) ──────────────
        # ZAWSZE trafia do recznej weryfikacji — nigdy nie jest automatycznie
        # potwierdzonym duplikatem (wymog biznesowy, jawnie zapisany).
        if not new_nip and new_contractor and new_doc_number and new_doc_date and new_amount is not None:
            candidates = (await db.execute(
                text(f"""
                    SELECT [id_instance], [document_amount], [extra_data]
                    FROM [{_SCHEMA}].[skw_document_approval_instances]
                    WHERE [id_instance] <> :i
                      AND JSON_VALUE([extra_data], '$.doc_number') = :num
                """),
                {"i": id_instance, "num": new_doc_number},
            )).fetchall()
            for cand_id, cand_amount, cand_extra_raw in candidates:
                cand_extra = _parse_extra(cand_extra_raw)
                cand_contractor = _norm_str(cand_extra.get("contractor"))
                cand_date = _norm_str(cand_extra.get("doc_date"))
                cand_currency = _norm_str(cand_extra.get("currency")) or "PLN"
                if (
                    cand_date == new_doc_date
                    and cand_currency == new_currency
                    and _amounts_match(new_amount, cand_amount)
                    and cand_contractor
                    and _names_similar(new_contractor, cand_contractor)
                ):
                    await _mark_duplicate(
                        db, id_instance, cand_id, "contractor_fallback",
                        f"Prawdopodobny duplikat po nazwie kontrahenta (brak NIP na "
                        f"jednym z dokumentow) — WYMAGA RECZNEJ WERYFIKACJI. "
                        f"Podobna do instancji #{cand_id}.",
                    )
                    logger.warning(
                        "DuplicateDetectionService: PRAWDOPODOBNY duplikat "
                        "(contractor_fallback, wymaga weryfikacji) | nowa=%s stara=%s "
                        "id_source=%s id_document=%s",
                        id_instance, cand_id, id_source, id_document,
                    )
                    return True

        return False


async def _mark_duplicate(
    db: AsyncSession,
    id_instance: int,
    matched_instance_id: int,
    match_type: str,
    match_reason: str,
) -> None:
    """Oznacza instancje jako duplicate_pending z nowymi kolumnami (migracja 0068)."""
    await db.execute(
        text(f"""
            UPDATE [{_SCHEMA}].[skw_document_approval_instances]
            SET [status]               = N'duplicate_pending',
                [matched_instance_id]  = :matched,
                [match_type]           = :mtype,
                [match_reason]         = :reason,
                [updated_at]           = :now
            WHERE [id_instance] = :i
        """),
        {
            "matched": matched_instance_id,
            "mtype":   match_type,
            "reason":  match_reason[:500],
            "now":     datetime.now(timezone.utc),
            "i":       id_instance,
        },
    )


# =============================================================================
# Funkcje pomocnicze (bez zmian logiki wzgledem wersji 1)
# =============================================================================

def _parse_extra(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _norm_str(val: Any) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _amounts_match(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return False
    try:
        return abs(Decimal(str(a)) - Decimal(str(b))) <= _AMOUNT_TOLERANCE
    except (InvalidOperation, ValueError, TypeError):
        return False


def _names_similar(a: str, b: str) -> bool:
    try:
        ratio = SequenceMatcher(None, a.lower(), b.lower()).ratio()
        return ratio >= _NAME_SIMILARITY_THRESHOLD
    except Exception:
        return False

def _older_than_clause(alias: str) -> str:
    """
    Zwraca fragment WHERE gwarantujacy, ze kandydat jest scisle starszy —
    zapobiega parom wzajemnym (A wskazuje B, B wskazuje A) niezaleznie od
    tego, w jakiej kolejnosci i ile razy sprawdzenie zostanie uruchomione
    na kazdym z dokumentow. Rownosc created_at rozstrzygana przez id_instance
    (importy wsadowe moga miec identyczny znacznik czasu co do milisekundy).
    """
    return (
        f"([{alias}].[created_at] < :new_created_at "
        f" OR ([{alias}].[created_at] = :new_created_at AND [{alias}].[id_instance] < :i))"
    )