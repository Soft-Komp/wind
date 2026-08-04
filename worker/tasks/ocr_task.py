# worker/tasks/ocr_task.py
"""
ARQ Task: asynchroniczny OCR dokumentow — F7 (sekcja 4.12).

Wywolywany przez source_sync_task dla zrodel source_type IN ('ftp', 'email')
po zapisaniu instancji do skw_document_approval_instances.

Przepływ:
  1. Pobierz sciezke pliku z extra_data instancji (klucz 'file_path')
  2. Sprawdz OCR_ENABLED — jesli false, zakoncz bez przetwarzania
  3. Wywolaj ocr_service.extract_fields(file_path)
  4. Jezeli confidence_score >= OCR_MIN_CONFIDENCE_SCORE:
       - zaktualizuj extra_data o pola ocr_*
       - jesli ocr_doc_number/ocr_amount_gross sa pewniejsze niz aktualne pola —
         zaktualizuj document_title i document_amount
  5. Zapisz wynik do skw_ArqJobRegistry (przez job_tracker, automatycznie)

UWAGA: from __future__ import annotations NIGDY tu — nie jest potrzebne
i pomijamy dla konsekwencji z reszta taskow workera.
"""

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import text

from worker.core.db import get_session
from worker.services.duplicate_detection_service import DuplicateDetectionService
from worker.services.ocr_service import extract_fields, OcrResult
from worker.settings import get_settings
from worker.core.logging_setup import get_event_logger

logger = logging.getLogger("worker.tasks.ocr")

_SCHEMA = "dbo"


async def ocr_task(
    ctx: dict[str, Any],
    *,
    id_instance: int,
    file_path: str,
    job_id: Optional[str] = None,
) -> dict[str, Any]:
    """
    ARQ Task: OCR jednego dokumentu.

    Args:
        id_instance:  ID instancji obiegu (skw_document_approval_instances).
        file_path:    Sciezka do pliku PDF/obrazu na dysku workera.
        job_id:       Opcjonalny ID (do traceability z source_sync_task).

    Returns:
        Slownik z wynikiem OCR.
    """
    settings = get_settings()
    effective_job_id = job_id or str(ctx.get("job_id", uuid.uuid4()))
    t_start = time.monotonic()

    logger.info(
        "ocr_task START | id_instance=%d file=%s job_id=%s",
        id_instance, file_path, effective_job_id,
    )

    # ── Sprawdz flage OCR_ENABLED ─────────────────────────────────────────────
    ocr_enabled = await _get_config_bool("OCR_ENABLED", False)
    if not ocr_enabled:
        logger.info("ocr_task: OCR_ENABLED=false — pomijam | id_instance=%d", id_instance)
        return {"status": "skipped", "reason": "OCR_ENABLED=false", "id_instance": id_instance}

    # ── Odczytaj konfiguracje ─────────────────────────────────────────────────
    ocr_lang       = await _get_config_str("OCR_LANGUAGE", "pol")
    min_confidence = await _get_config_float("OCR_MIN_CONFIDENCE_SCORE", 0.6)
    max_pages      = await _get_config_int("OCR_MAX_PAGES", 3)

    # NAPRAWA (2026-07-23): guard na plik pusty/nieistniejacy PRZED wywolaniem
    # extract_fields() — unika bezcelowej proby OCR na 0-bajtowym pliku
    # (zgloszenie: zrodlo FTP qa_b_ftp dostarcza puste pliki-placeholdery).
    from pathlib import Path as _Path
    file_size_on_disk = _Path(file_path).stat().st_size if _Path(file_path).exists() else 0
    if file_size_on_disk == 0:
        logger.warning(
            "ocr_task: plik pusty lub nieistniejacy (0 bajtow) — pomijam OCR | "
            "id_instance=%d file=%s",
            id_instance, file_path,
        )
        ocr_result = OcrResult(error=f"Plik pusty (0 bajtow) lub nieistniejacy: {file_path}")
    else:
        # NAPRAWA (2026-07-23): timeout wokol pipeline OCR — poprzednio brak
        # jakiegokolwiek limitu czasu wokol wywolan zewnetrznych binarek
        # (poppler/tesseract przez pdf2image/pytesseract) mogl teoretycznie
        # zawiesic task na czas nieokreslony bez sladu w logach/rejestrze.
        ocr_timeout = await _get_config_int("OCR_TASK_TIMEOUT_SECONDS", 120)
        try:
            ocr_result = await asyncio.wait_for(
                extract_fields(file_path, lang=ocr_lang, max_pages=max_pages),
                timeout=ocr_timeout,
            )
        except asyncio.TimeoutError:
            logger.error(
                "ocr_task: TIMEOUT (%ds) — extract_fields nie zakonczylo sie | "
                "id_instance=%d file=%s",
                ocr_timeout, id_instance, file_path,
            )
            ocr_result = OcrResult(
                error=f"OCR przekroczyl limit czasu ({ocr_timeout}s) — plik moze byc uszkodzony."
            )

    # NAPRAWA (2026-07-23): bez wzgledu na to czy blad, poprzedni kod konczyl
    # task natychmiast, NIGDY nie zapisujac ocr_error do extra_data instancji.
    # Front (GET /documents/{id}/status-summary) sprawdza wlasnie klucz
    # 'ocr_error' w extra_data — bez tego zapisu ocr_data zawsze wychodzilo
    # null, nawet przy calkowicie poprawnie obsluzonym bledzie OCR.
    if ocr_result.error:
        logger.error(
            "ocr_task BLAD OCR | id_instance=%d error=%s",
            id_instance, ocr_result.error,
        )
        await _update_instance_with_ocr(
            id_instance,
            ocr_dict=ocr_result.to_dict(),
            confidence=0.0,
            min_confidence=min_confidence,
            ocr_result=ocr_result,
        )
        return {
            "status":      "error",
            "id_instance": id_instance,
            "error":       ocr_result.error,
        }

    # ── Zapisz wynik do extra_data instancji ─────────────────────────────────
    confidence = ocr_result.confidence_score
    if confidence < min_confidence:
        logger.info(
            "ocr_task: confidence=%.2f < min=%.2f — zapisuje tylko raw_text | id_instance=%d",
            confidence, min_confidence, id_instance,
        )

    ocr_dict = ocr_result.to_dict()
    verified, uploaded_by = await _update_instance_with_ocr(
        id_instance,
        ocr_dict=ocr_dict,
        confidence=confidence,
        min_confidence=min_confidence,
        ocr_result=ocr_result,
    )

    # NOWE (2026-07-28): ponowne sprawdzenie duplikatow PO OCR. Przy pierwszym
    # zapisie instancji (manual/ftp/email, PRZED OCR) numer/NIP/data/kwota
    # zazwyczaj nie sa jeszcze znane — pierwsze sprawdzenie w source_sync_task
    # /webhook_service nie mialo szans niczego znalezc. Teraz, gdy OCR
    # wypelnil te pola w extra_data, sprawdzamy jeszcze raz na TEJ SAMEJ
    # instancji (id_instance sie nie zmienia). Wykonywane niezaleznie od
    # tego czy status przeszedl do pending_dispatch (patrz decyzja
    # 2026-07-24 — auto-pass jest dzis zawsze wylaczony, wiec 'verified'
    # jest praktycznie zawsze False, ale duplikat i tak trzeba sprawdzic).
    if not ocr_result.error:
        id_source_result = await _get_id_source_and_document(id_instance)
        if id_source_result is not None:
            id_source_for_dup, id_document_for_dup = id_source_result
            async with get_session() as dup_db:
                try:
                    await DuplicateDetectionService.check_and_mark(
                        dup_db,
                        id_instance=id_instance,
                        id_source=id_source_for_dup,
                        id_document=id_document_for_dup,
                    )
                    await dup_db.commit()
                except Exception as exc:
                    logger.error(
                        "ocr_task: blad sprawdzania duplikatow po OCR (fail-safe) | "
                        "id_instance=%d: %s", id_instance, exc, exc_info=True,
                    )

    # SSE — powiadomienie osoby ktora wgrala plik (channel:user:{id}, kanal
    # faktycznie subskrybowany przez GET /events/stream). Worker nie ma
    # dostepu do app.services.event_service (izolacja od backendu), wiec
    # koperta budowana jest recznie, w tym samym ksztalcie co _build_event_envelope.
    if verified and uploaded_by:
        redis = ctx.get("worker_redis")
        if redis:
            import uuid as _uuid
            envelope = {
                "event_id":  str(_uuid.uuid4()),
                "type":      "document_ocr_verified",
                "data": {
                    "instance_id": id_instance,
                    "confidence":  confidence,
                    "verified_by": "system",
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "user_id":   None,
            }
            envelope_json = json.dumps(envelope, ensure_ascii=False)
            try:
                await redis.publish(f"channel:user:{uploaded_by}", envelope_json)
                await redis.publish("channel:admins", envelope_json)
            except Exception as exc:
                logger.warning("ocr_task: SSE publish blad | id_instance=%d: %s", id_instance, exc)

    duration_ms = round((time.monotonic() - t_start) * 1000)
    get_event_logger(settings.LOG_DIR).log("ocr_completed", {
        "id_instance":   id_instance,
        "file_path":     file_path,
        "confidence":    confidence,
        "pages":         ocr_result.pages_processed,
        "fields_found":  sum(1 for v in [
            ocr_result.doc_number, ocr_result.nip,
            ocr_result.doc_date, ocr_result.amount_gross,
            ocr_result.contractor_name,
        ] if v is not None),
        "duration_ms":   duration_ms,
        "job_id":        effective_job_id,
    })

    logger.info(
        "ocr_task ZAKONCZONE | id_instance=%d confidence=%.2f duration_ms=%d",
        id_instance, confidence, duration_ms,
    )

    return {
        "status":      "success",
        "id_instance": id_instance,
        "confidence":  confidence,
        "pages":       ocr_result.pages_processed,
        "duration_ms": duration_ms,
    }


async def _update_instance_with_ocr(
    id_instance: int,
    *,
    ocr_dict: dict[str, Any],
    confidence: float,
    min_confidence: float,
    ocr_result: Any,
) -> tuple[bool, int | None]:
    """
    Aktualizuje skw_document_approval_instances o dane OCR.

    Strategie aktualizacji:
    - extra_data zawsze wzbogacana o klucze ocr_* (nawet przy niskiej confidence)
    - document_title nadpisywany gdy ocr_doc_number pewny (confidence >= min)
      i obecny tytuł jest pusty lub ogólny (np. "Dokument ID_...")
    - document_amount nadpisywany gdy ocr_amount_gross pewny i aktualna wartość NULL

    Zwraca (verified, uploaded_by):
      verified:    True jesli przejscie ocr_review_pending -> pending_dispatch
      uploaded_by: id_user z extra_data (kto wgral plik) — do powiadomienia SSE
    """
    async with get_session() as db:
        # Pobierz aktualne extra_data + status.
        # POPRAWKA KOMENTARZA (2026-07-24): poprzednia wersja tego komentarza
        # twierdzila ze "FTP/email nigdy nie maja statusu ocr_review_pending"
        # — NIEPRAWDA od migracji/fixu w source_sync_task.py z 2026-07-14
        # (_upsert_instance: source_type in ('ftp','email') -> insert_status
        # = 'ocr_review_pending', identycznie jak manual_upload). Ten
        # nieaktualny komentarz doprowadzil do bledego zalozenia przy
        # analizie incydentu z instancja 792 (zrodlo qa_b_ftp) — zalozono
        # ze potrzebna jest osobna zmiana w source_sync_task.py, podczas gdy
        # bramka ponizej OD DAWNA obejmuje rowniez FTP/email. Status
        # 'ocr_review_pending' dotyczy dzis WSZYSTKICH zrodel przechodzacych
        # przez OCR (manual_upload, ftp, email) — logika ponizej dziala
        # jednolicie dla wszystkich trzech, bez wyjatkow.
        result = await db.execute(
            text(f"""
                SELECT [extra_data], [document_title], [document_amount], [status]
                FROM [{_SCHEMA}].[skw_document_approval_instances]
                WHERE [id_instance] = :i
            """),
            {"i": id_instance},
        )
        row = result.fetchone()
        if row is None:
            logger.warning("ocr_task._update_instance: id_instance=%d nie istnieje", id_instance)
            return

        current_extra_raw, current_title, current_amount, current_status = row

        # Parsuj istniejace extra_data
        current_extra: dict = {}
        if current_extra_raw:
            try:
                current_extra = json.loads(current_extra_raw)
            except Exception:
                pass

        # Wzbogac extra_data o wyniki OCR
        current_extra.update(ocr_dict)
        new_extra_json = json.dumps(current_extra, ensure_ascii=False, default=str)

        # Decyzja o nadpisaniu document_title i document_amount
        new_title  = current_title
        new_amount = current_amount

        if confidence >= min_confidence:
            if ocr_result.doc_number and (
                not current_title or current_title.startswith("Dokument ")
            ):
                new_title = ocr_result.doc_number

            if ocr_result.amount_gross is not None and current_amount is None:
                new_amount = ocr_result.amount_gross

        # Przejscie statusu — TYLKO dla dokumentow recznie wgranych
        # (status='ocr_review_pending'). Dla FTP/email (zawsze pending_dispatch
        # od poczatku) status nigdy sie tu nie zmienia — zero regresji.
        new_status = current_status
        requires_review = False
        review_reasons: list[str] = []

        if current_status == "ocr_review_pending":
            has_doc_number = bool(ocr_result.doc_number)
            has_amount     = ocr_result.amount_gross is not None

            # ZMIANA (2026-07-24, na wniosek frontu — incydent instancja 792):
            # WYLACZONE automatyczne przejscie ocr_review_pending ->
            # pending_dispatch, niezaleznie od confidence. Powod: confidence
            # mierzy pewnosc ROZPOZNANIA ZNAKOW przez OCR, nie poprawnosc
            # semantyczna wyciagnietych pol — numer/NIP/kwota moga byc
            # odczytane z confidence=1.0 i jednoczesnie bledne (dokladnie ten
            # przypadek). KAZDY dokument z OCR zostaje teraz w
            # ocr_review_pending. Jedyna droga dalej: POST /documents/{id}/
            # ocr-review/resolve z decision='confirm' — zapisuje verified_*
            # i wymaga swiadomego dzialania czlowieka. Przywrocenie auto-pass
            # dopiero po wdrozeniu walidacji semantycznej (suma kontrolna NIP,
            # sanity-check kwoty/numeru) — patrz rozmowa robocza 2026-07-24.
            # `min_confidence`/`confidence` NIE sa usuwane — nadal licza sie
            # do review_reasons (diagnostyka), tylko juz NIGDY nie bramkuja
            # przejscia do dalszego obiegu.
            new_status = "ocr_review_pending"
            requires_review = True
            if confidence < min_confidence:
                review_reasons.append("low_confidence")
            if not has_doc_number:
                review_reasons.append("missing_doc_number")
            if not has_amount:
                review_reasons.append("missing_amount")
            if confidence >= min_confidence and has_doc_number and has_amount:
                # Dane wygladaja na kompletne i pewne — ale auto-pass jest
                # dzis wylaczony polityka, nie brakiem danych. Osobny powod
                # w logu, zeby operator odrozniał "podejrzane dane" od
                # "dane OK, ale i tak wymagana reczna weryfikacja".
                review_reasons.append("manual_review_required_by_policy")

            current_extra["ocr_requires_review"] = requires_review
            current_extra["ocr_review_reasons"]  = review_reasons
            new_extra_json = json.dumps(current_extra, ensure_ascii=False, default=str)

        await db.execute(
            text(f"""
                UPDATE [{_SCHEMA}].[skw_document_approval_instances]
                SET [extra_data]       = :extra,
                    [document_title]   = :title,
                    [document_amount]  = :amount,
                    [status]           = :status,
                    [updated_at]       = SYSUTCDATETIME()
                WHERE [id_instance] = :i
            """),
            {
                "extra":  new_extra_json,
                "title":  new_title,
                "amount": new_amount,
                "status": new_status,
                "i":      id_instance,
            },
        )
        await db.commit()

        logger.debug(
            "_update_instance_with_ocr OK | id=%d title=%r amount=%s status=%s requires_review=%s",
            id_instance, new_title, new_amount, new_status, requires_review,
        )

        verified = current_status == "ocr_review_pending" and new_status == "pending_dispatch"
        uploaded_by = current_extra.get("uploaded_by")
        return verified, uploaded_by

        # Zweryfikowany = przejscie ocr_review_pending -> pending_dispatch
        # (automatyczna weryfikacja, bez udzialu czlowieka).
        return current_status == "ocr_review_pending" and new_status == "pending_dispatch"


# =============================================================================
# Pomocnicze — odczyt SystemConfig
# =============================================================================

async def _get_config_bool(key: str, default: bool) -> bool:
    async with get_session() as db:
        try:
            r = await db.execute(
                text(f"SELECT [ConfigValue] FROM [{_SCHEMA}].[skw_SystemConfig] WHERE [ConfigKey]=:k AND [IsActive]=1"),
                {"k": key},
            )
            row = r.fetchone()
            return str(row[0]).lower() == "true" if row else default
        except Exception:
            return default


async def _get_config_str(key: str, default: str) -> str:
    async with get_session() as db:
        try:
            r = await db.execute(
                text(f"SELECT [ConfigValue] FROM [{_SCHEMA}].[skw_SystemConfig] WHERE [ConfigKey]=:k AND [IsActive]=1"),
                {"k": key},
            )
            row = r.fetchone()
            return str(row[0]) if row else default
        except Exception:
            return default


async def _get_config_float(key: str, default: float) -> float:
    async with get_session() as db:
        try:
            r = await db.execute(
                text(f"SELECT [ConfigValue] FROM [{_SCHEMA}].[skw_SystemConfig] WHERE [ConfigKey]=:k AND [IsActive]=1"),
                {"k": key},
            )
            row = r.fetchone()
            return float(row[0]) if row else default
        except Exception:
            return default


async def _get_config_int(key: str, default: int) -> int:
    async with get_session() as db:
        try:
            r = await db.execute(
                text(f"SELECT [ConfigValue] FROM [{_SCHEMA}].[skw_SystemConfig] WHERE [ConfigKey]=:k AND [IsActive]=1"),
                {"k": key},
            )
            row = r.fetchone()
            return int(row[0]) if row else default
        except Exception:
            return default

async def _get_id_source_and_document(id_instance: int) -> tuple[int, str] | None:
    """
    NOWE (2026-07-28). id_source/id_document sa potrzebne wylacznie do
    logowania w DuplicateDetectionService (kaskada metod nie ogranicza
    juz sprawdzenia po zrodle) — szybki, osobny lookup, zeby nie
    rozszerzac istniejacego zapytania w _update_instance_with_ocr.
    """
    async with get_session() as db:
        try:
            result = await db.execute(
                text(f"""
                    SELECT [id_source], [id_document]
                    FROM [{_SCHEMA}].[skw_document_approval_instances]
                    WHERE [id_instance] = :i
                """),
                {"i": id_instance},
            )
            row = result.fetchone()
            return (row[0], row[1]) if row else None
        except Exception as exc:
            logger.error(
                "_get_id_source_and_document blad | id_instance=%d: %s",
                id_instance, exc,
            )
            return None