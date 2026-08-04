# worker/services/duplicate_detection_service.py
"""
Port DuplicateDetectionService dla workera ARQ.

Identyczna logika jak backend/app/services/duplicate_detection_service.py
(wersja 2, 2026-07-28) — zero importu z app.* (izolacja kontenerow, worker
i backend nie dziela procesu ani sciezki importu).

Uzywane przez:
  - worker/tasks/source_sync_task.py — po INSERT nowej instancji
  - worker/tasks/ocr_task.py — ponownie, po ekstrakcji pol przez OCR
    (dane nieznane przy pierwszym zapisie moga sie teraz odkryc jako duplikat)

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

logger = logging.getLogger("worker.services.duplicate_detection")

_SCHEMA = "dbo"
_AMOUNT_TOLERANCE = Decimal("0.01")
_NAME_SIMILARITY_THRESHOLD = 0.85


class DuplicateDetectionService:
    """Identyczna logika co wersja backendowa — patrz tamten plik dla pelnego docstringa."""

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
                "DuplicateDetectionService.check_and_mark (worker): nieoczekiwany "
                "blad (fail-safe) | id_instance=%s id_source=%s id_document=%s: %s",
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
                SELECT [document_amount], [extra_data], [file_sha256], [ksef_id_lookup]
                FROM [{_SCHEMA}].[skw_document_approval_instances]
                WHERE [id_instance] = :i
            """),
            {"i": id_instance},
        )
        row = row_result.fetchone()
        if row is None:
            logger.warning(
                "DuplicateDetectionService (worker): id_instance=%s nie znalezione", id_instance,
            )
            return False

        new_amount, new_extra_raw, new_file_sha256, new_ksef_id = row
        new_extra = _parse_extra(new_extra_raw)
        new_doc_number = _norm_str(new_extra.get("doc_number"))
        new_nip        = _norm_str(new_extra.get("nip"))
        new_contractor = _norm_str(new_extra.get("contractor"))
        new_doc_date   = _norm_str(new_extra.get("doc_date"))
        new_currency   = _norm_str(new_extra.get("currency")) or "PLN"

        if new_ksef_id:
            match = (await db.execute(
                text(f"""
                    SELECT TOP 1 [id_instance] FROM [{_SCHEMA}].[skw_document_approval_instances]
                    WHERE [ksef_id_lookup] = :k AND [id_instance] <> :i
                    ORDER BY [created_at] ASC
                """),
                {"k": new_ksef_id, "i": id_instance},
            )).fetchone()
            if match:
                await _mark_duplicate(db, id_instance, match[0], "ksef_id",
                    f"Identyczny numer KSeF ({new_ksef_id}) jak instancja #{match[0]}.")
                logger.warning(
                    "DuplicateDetectionService (worker): DUPLIKAT (ksef_id) | "
                    "nowa=%s stara=%s id_source=%s id_document=%s",
                    id_instance, match[0], id_source, id_document,
                )
                return True

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
                await _mark_duplicate(db, id_instance, match[0], "file_sha256",
                    f"Identyczny hash SHA-256 pliku jak instancja #{match[0]}.")
                logger.warning(
                    "DuplicateDetectionService (worker): DUPLIKAT (file_sha256) | "
                    "nowa=%s stara=%s id_source=%s id_document=%s",
                    id_instance, match[0], id_source, id_document,
                )
                return True

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
                if cand_date == new_doc_date and cand_currency == new_currency and _amounts_match(new_amount, cand_amount):
                    await _mark_duplicate(db, id_instance, cand_id, "invoice_fingerprint",
                        f"Zgodnosc NIP ({new_nip}), numeru ({new_doc_number}), daty i kwoty z instancja #{cand_id}.")
                    logger.warning(
                        "DuplicateDetectionService (worker): DUPLIKAT (invoice_fingerprint) | "
                        "nowa=%s stara=%s id_source=%s id_document=%s",
                        id_instance, cand_id, id_source, id_document,
                    )
                    return True

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
                    cand_date == new_doc_date and cand_currency == new_currency
                    and _amounts_match(new_amount, cand_amount)
                    and cand_contractor and _names_similar(new_contractor, cand_contractor)
                ):
                    await _mark_duplicate(db, id_instance, cand_id, "contractor_fallback",
                        f"Prawdopodobny duplikat po nazwie kontrahenta (brak NIP) — "
                        f"WYMAGA RECZNEJ WERYFIKACJI. Podobna do instancji #{cand_id}.")
                    logger.warning(
                        "DuplicateDetectionService (worker): PRAWDOPODOBNY duplikat "
                        "(contractor_fallback) | nowa=%s stara=%s id_source=%s id_document=%s",
                        id_instance, cand_id, id_source, id_document,
                    )
                    return True

        return False


async def _mark_duplicate(
    db: AsyncSession, id_instance: int, matched_instance_id: int,
    match_type: str, match_reason: str,
) -> None:
    await db.execute(
        text(f"""
            UPDATE [{_SCHEMA}].[skw_document_approval_instances]
            SET [status]              = N'duplicate_pending',
                [matched_instance_id] = :matched,
                [match_type]          = :mtype,
                [match_reason]        = :reason,
                [updated_at]          = :now
            WHERE [id_instance] = :i
        """),
        {
            "matched": matched_instance_id, "mtype": match_type,
            "reason": match_reason[:500], "now": datetime.now(timezone.utc),
            "i": id_instance,
        },
    )


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
        return SequenceMatcher(None, a.lower(), b.lower()).ratio() >= _NAME_SIMILARITY_THRESHOLD
    except Exception:
        return False