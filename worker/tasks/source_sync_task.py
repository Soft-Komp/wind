# worker/tasks/source_sync_task.py
"""
ARQ Task: source_sync_task — cykliczna synchronizacja zrodel dokumentow.

Cykl bazowy: co SOURCE_SYNC_WORKER_INTERVAL_MINUTES (domyslnie 5 min).
Per-zrodlo: co sync_interval_minutes (z skw_document_sources).

Dla kazdego aktywnego zrodla pull ktore potrzebuje sync (needs_sync=True):
  1. Pobierz adapter (get_adapter_by_source_id)
  2. Wywolaj adapter.fetch_new_documents(since=last_sync_at)
  3. Zapisz kazdy UnifiedDocument przez _upsert_instance()
  4. Zaktualizuj last_sync_at / last_sync_status / last_sync_message

Idempotentnosc:
  MERGE nie INSERT — jesli dokument juz istnieje (id_source, id_document),
  worker aktualizuje pola (contractor_name, amount, document_title) zamiast
  tworzyc duplikat. Nowy wiersz tylko gdy brakuje.

Bezpieczenstwo:
  - Distributed lock Redis: source_sync_lock:{id_source} (TTL = 2 * timeout)
    Zapobiega rownoczesnemu uruchomieniu sync dla tego samego zrodla.
  - Test mode: dokumenty nie wchodza do obiegu (status=pending_dispatch blokowany).

Logowanie:
  - Strukturowany JSON do workera logger
  - JSONL event log per synchronizacja

Rejestracja w WorkerSettings.cron_jobs:
    cron(source_sync_task, minute={*/5})

UWAGA: from __future__ import annotations — OK w pliku workera (nie FastAPI router).
"""

from __future__ import annotations

import asyncio
from email.policy import default
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from worker.core.db import get_engine, get_session
from worker.core.logging_setup import get_event_logger
from worker.services.duplicate_detection_service import DuplicateDetectionService
from worker.settings import get_settings

logger = logging.getLogger("worker.tasks.source_sync")

# Klucz Redis dla distributed locka synchronizacji zrodla
_SYNC_LOCK_PREFIX = "source_sync_lock:"
_SYNC_LOCK_TTL    = 600  # 10 minut — maks. czas jednej synchronizacji

# Statusy instancji w zaleznosci od trybu
_STATUS_NORMAL    = "pending_dispatch"
_STATUS_TEST_MODE = "pending_dispatch"  # tak samo — test_mode blokowany na poziomie dispatch


async def source_sync_task(ctx: dict[str, Any]) -> dict[str, Any]:
    settings  = get_settings()
    redis     = ctx.get("worker_redis")
    # POPRAWKA: ArqRedis (ma enqueue_job) osobno od worker_redis (lock/cache,
    # NIE ma enqueue_job) — ARQ wstrzykuje ArqRedis sam do ctx['redis'],
    # patrz main.py::on_startup docstring.
    arq_redis = ctx.get("redis")
    task_start = time.monotonic()
    now_utc   = datetime.now(timezone.utc)

    logger.info(
        "source_sync_task START",
        extra={"ts_utc": now_utc.isoformat()},
    )

    event_log = get_event_logger(settings.LOG_DIR)
    event_log.log("source_sync_started", {"ts_utc": now_utc.isoformat()})

    # Pobierz liste zrodel do synchronizacji
    sources = await _get_sources_needing_sync()

    summary = {
        "ts_utc":          now_utc.isoformat(),
        "sources_checked": len(sources),
        "sources_synced":  0,
        "sources_skipped": 0,
        "sources_error":   0,
        "total_docs":      0,
        "total_new":       0,
        "total_updated":   0,
        "duration_ms":     0,
    }

    for source in sources:
        id_source   = source["id_source"]
        source_name = source["source_name"]
        source_type = source["source_type"]

        result = await _sync_one_source(ctx, source, redis, arq_redis, settings, event_log)

        if result["status"] == "ok":
            summary["sources_synced"] += 1
            summary["total_docs"]     += result.get("docs_fetched", 0)
            summary["total_new"]      += result.get("docs_new", 0)
            summary["total_updated"]  += result.get("docs_updated", 0)
        elif result["status"] == "skipped":
            summary["sources_skipped"] += 1
        else:
            summary["sources_error"] += 1

    summary["duration_ms"] = round((time.monotonic() - task_start) * 1000, 1)

    logger.info("source_sync_task ZAKONCZONE", extra=summary)
    event_log.log("source_sync_completed", summary)

    return summary


async def source_sync_task_single(ctx: dict[str, Any], *, id_source: int) -> dict[str, Any]:
    """
    ARQ Task: recznie wymuszona synchronizacja JEDNEGO zrodla — poza cyklem cron.

    Wywolywana przez POST /admin/sources/{id}/sync (source_admin_service.trigger_sync).
    W przeciwienstwie do source_sync_task (cron) — NIE filtruje po needs_sync,
    bo to swiadome, reczne dzialanie administratora (ma zadzialac natychmiast,
    niezaleznie od tego czy minal sync_interval_minutes).
    """
    settings  = get_settings()
    redis     = ctx.get("worker_redis")
    arq_redis = ctx.get("redis")
    now_utc   = datetime.now(timezone.utc)
    event_log = get_event_logger(settings.LOG_DIR)

    logger.info(
        "source_sync_task_single START | id_source=%s", id_source,
        extra={"ts_utc": now_utc.isoformat()},
    )

    engine = get_engine()
    async with engine.connect() as conn:
        result = await conn.execute(
            text("""
                SELECT id_source, source_name, source_type, connection_config,
                       sync_interval_minutes, last_sync_at, is_test_mode
                FROM dbo.skw_document_sources
                WHERE id_source = :id
            """),
            {"id": id_source},
        )
        cols = list(result.keys())
        row = result.fetchone()

    if not row:
        logger.warning("source_sync_task_single: zrodlo id=%s nie istnieje", id_source)
        return {"status": "error", "reason": "source_not_found"}

    source = dict(zip(cols, row))
    result = await _sync_one_source(ctx, source, redis, arq_redis, settings, event_log)

    event_log.log("source_sync_single_completed", {"id_source": id_source, **result})
    logger.info(
        "source_sync_task_single ZAKONCZONE | id_source=%s status=%s",
        id_source, result.get("status"),
    )
    return result

async def _get_sources_needing_sync() -> list[dict[str, Any]]:
    """
    Pobiera liste zrodel ktore potrzebuja synchronizacji.

    Filtruje: is_active=1, connection_mode='pull',
    (last_sync_at IS NULL OR minuty_od_last_sync >= sync_interval_minutes)
    """
    engine = get_engine()
    now_utc = datetime.now(timezone.utc)

    async with engine.connect() as conn:
        result = await conn.execute(text("""
            SELECT
                id_source,
                source_name,
                source_type,
                connection_config,
                sync_interval_minutes,
                last_sync_at,
                is_test_mode,
                sync_cursor
            FROM dbo.skw_document_sources
            WHERE is_active = 1
              AND connection_mode = 'pull'
              AND source_type NOT IN ('manual', 'ksef20')
        """))

        cols = list(result.keys())   # MUSI byc przed fetchall() — SQLAlchemy 2.x async
        rows = result.fetchall()

    sources_to_sync = []
    for row in rows:
        s = dict(zip(cols, row))
        last_sync = s.get("last_sync_at")
        interval  = s.get("sync_interval_minutes", 15)

        needs_sync = False
        if last_sync is None:
            needs_sync = True
        else:
            if last_sync.tzinfo is None:
                last_sync = last_sync.replace(tzinfo=timezone.utc)
            elapsed = (now_utc - last_sync).total_seconds() / 60
            needs_sync = elapsed >= interval

        if needs_sync:
            sources_to_sync.append(s)
            logger.debug(
                "Zrodlo zakwalifikowane do sync | id=%s name=%s type=%s",
                s["id_source"], s["source_name"], s["source_type"],
            )

    return sources_to_sync


async def _sync_one_source(
    ctx: dict[str, Any],
    source: dict[str, Any],
    redis: Any,
    arq_redis: Any,
    settings: Any,
    event_log: Any,
) -> dict[str, Any]:
    """
    Synchronizuje jedno zrodlo z distributed lockiem.

    Returns:
        dict z kluczami: status (ok|skipped|error), docs_fetched, docs_new, duration_ms
    """
    id_source    = source["id_source"]
    source_name  = source["source_name"]
    source_type  = source["source_type"]
    last_sync_at = source.get("last_sync_at")
    is_test_mode = bool(source.get("is_test_mode", False))

    lock_key = f"{_SYNC_LOCK_PREFIX}{id_source}"
    t_start  = time.monotonic()

    # Distributed lock — zapobiega rownoleglem sync tego samego zrodla
    if redis:
        acquired = await redis.set(lock_key, "1", ex=_SYNC_LOCK_TTL, nx=True)
        if not acquired:
            logger.info(
                "source_sync: zrodlo id=%s jest juz synchronizowane — pomijam", id_source
            )
            return {"status": "skipped", "reason": "lock_held"}

    try:
        logger.info(
            "source_sync START | id=%s name=%s type=%s test=%s since=%s",
            id_source, source_name, source_type, is_test_mode, last_sync_at,
        )

        # Pobierz adapter — lokalny port (worker/services/source_adapter.py),
        # zamiast importu z backend/app — patrz uzasadnienie w naglowku tego pliku.
        engine = get_engine()
        async with engine.connect() as db_conn:
            from worker.services.source_adapter import get_adapter_by_source_id

            adapter = await get_adapter_by_source_id(db_conn, id_source)

        if adapter is None:
            logger.warning(
                "source_sync: brak adaptera dla id=%s type=%s — pomijam",
                id_source, source_type,
            )
            await _mark_sync_status(id_source, "error", "Brak adaptera dla tego source_type")
            return {"status": "error", "reason": "no_adapter"}

        # Pobierz nowe dokumenty. Kursor zlozony (jesli zrodlo go uzywa) —
        # parsowany z JSON zapisanego w skw_document_sources.sync_cursor.
        # NAPRAWA (2026-07-28): 'since'=last_sync_at przestaje byc jedynym
        # mechanizmem pozycjonowania — dla zrodel z cursor_date_column/
        # cursor_id_column w connection_config, adapter go CALKOWICIE
        # ignoruje na rzecz kursora zlozonego (patrz DatabaseAdapter
        # w unified_document.py).
        raw_cursor = source.get("sync_cursor")
        parsed_cursor: dict[str, Any] | None = None
        if raw_cursor:
            try:
                parsed_cursor = json.loads(raw_cursor)
            except Exception as exc:
                logger.warning(
                    "source_sync: nie udalo sie sparsowac sync_cursor jako JSON "
                    "(traktuje jak brak kursora — pierwsza synchronizacja od nowa "
                    "w tym trybie) | id=%s raw=%r: %s",
                    id_source, raw_cursor, exc,
                )

        max_docs = int(await _get_config_value("SOURCE_SYNC_MAX_DOCUMENTS_PER_CYCLE", "500"))
        fetch_kwargs: dict[str, Any] = {"since": last_sync_at, "limit": max_docs}
        if getattr(adapter, "supports_compound_cursor", lambda: False)():
            fetch_kwargs["cursor"] = parsed_cursor
        docs = await adapter.fetch_new_documents(**fetch_kwargs)

        logger.info(
            "source_sync: pobrano %d dokumentow | id=%s cursor=%s", len(docs), id_source, parsed_cursor,
        )

        # Zapisz do bazy
        docs_new = 0
        docs_updated = 0
        errors = 0

        for doc in docs:
            try:
                result, new_id_instance = await _upsert_instance(doc, is_test_mode, source_type)
                if result == "inserted":
                    docs_new += 1
                    # NOWE (2026-07-28): sprawdzenie duplikatow WYLACZNIE dla
                    # nowo wstawionych instancji (nie przy kazdej aktualizacji
                    # MERGE tego samego dokumentu) — ten sam punkt w przepływie
                    # co juz istniejacy webhook_service.py, teraz rowniez dla
                    # workera synchronizacji cyklicznej, ktory go dotad NIE MIAL
                    # WOGOLE WPIETEGO (potwierdzone przegladem tego pliku).
                    async with get_session() as dup_db:
                        try:
                            await DuplicateDetectionService.check_and_mark(
                                dup_db,
                                id_instance=new_id_instance,
                                id_source=doc.id_source,
                                id_document=doc.id_document,
                            )
                            await dup_db.commit()
                        except Exception as exc:
                            logger.error(
                                "source_sync: blad sprawdzania duplikatow (fail-safe, "
                                "dokument mimo to zostaje przyjety) | id_instance=%s: %s",
                                new_id_instance, exc, exc_info=True,
                            )
                else:
                    docs_updated += 1
            except Exception as exc:
                errors += 1
                logger.error(
                    "source_sync: blad upsert dokumentu id=%s source=%s: %s",
                    doc.id_document, source_name, exc,
                )

        # NOWE (2026-07-28): zapis kursora zlozonego DOPIERO po przetworzeniu
        # calej partii, i TYLKO gdy errors == 0 (wymog: "kursor zapisywac
        # dopiero po prawidlowym przetworzeniu calej partii" — czesciowa
        # partia z bledami NIE przesuwa kursora, zeby nieprzetworzone
        # rekordy zostaly ponownie pobrane w nastepnym cyklu).
        if errors == 0 and getattr(adapter, "supports_compound_cursor", lambda: False)():
            next_cursor = adapter.extract_cursor(docs)
            if next_cursor:
                await _mark_sync_cursor(id_source, next_cursor)

        # OCR — tylko dla zrodel FTP i email
        # POPRAWKA: arq_redis (ctx['redis']), NIE redis (ctx['worker_redis']) —
        # enqueue_job istnieje wylacznie na ArqRedis. Isinstance-check w
        # _enqueue_ocr_for_new_docs teraz powinien PRZECHODZIC.
        if source_type in ("ftp", "email") and arq_redis:
            await _enqueue_ocr_for_new_docs(arq_redis, id_source=id_source)

        # Zaktualizuj status zrodla
        if errors == 0:
            msg = f"Pobrano {len(docs)}: {docs_new} nowych, {docs_updated} zaktualizowanych"
            await _mark_sync_status(id_source, "ok", msg)
        elif errors < len(docs):
            msg = f"Czesciowy sukces: {len(docs) - errors}/{len(docs)} dokumentow OK, {errors} bledow"
            await _mark_sync_status(id_source, "partial", msg)
        else:
            await _mark_sync_status(id_source, "error", f"Wszystkie {errors} dokumenty nie powiodly sie")

        duration_ms = round((time.monotonic() - t_start) * 1000, 1)

        event_log.log("source_synced", {
            "id_source":    id_source,
            "source_name":  source_name,
            "docs_fetched": len(docs),
            "docs_new":     docs_new,
            "docs_updated": docs_updated,
            "errors":       errors,
            "duration_ms":  duration_ms,
            "is_test_mode": is_test_mode,
        })

        return {
            "status":      "ok",
            "docs_fetched": len(docs),
            "docs_new":     docs_new,
            "docs_updated": docs_updated,
            "errors":       errors,
            "duration_ms":  duration_ms,
        }

    except asyncio.CancelledError:
        logger.warning("source_sync: task anulowany dla id=%s", id_source)
        await _mark_sync_status(id_source, "error", "Task anulowany")
        raise

    except Exception as exc:
        duration_ms = round((time.monotonic() - t_start) * 1000, 1)
        logger.error(
            "source_sync BLAD | id=%s name=%s: %s", id_source, source_name, exc,
            exc_info=True,
        )
        await _mark_sync_status(id_source, "error", str(exc)[:500])
        event_log.log("source_sync_error", {
            "id_source":   id_source,
            "source_name": source_name,
            "error":       str(exc),
            "duration_ms": duration_ms,
        })
        return {"status": "error", "reason": str(exc)[:200], "duration_ms": duration_ms}

    finally:
        if redis:
            try:
                await redis.delete(lock_key)
            except Exception:
                pass


async def _upsert_instance(doc: Any, is_test_mode: bool, source_type: str) -> str:
    """
    MERGE dokumentu do skw_document_approval_instances.
    Zwraca 'inserted' jesli nowy rekord, 'updated' jesli istniejacy zaktualizowany.

    Idempotentnosc: identyfikacja po (id_source, id_document).
    Jesli status jest terminalny (approved/cancelled) — nie nadpisujemy.

    is_test_mode=True: extra_data aktualizowane, status pozostaje 'pending_dispatch'
    (nie wchodzi do obiegu az test_mode=False na poziomie dispatch).
    """
    engine = get_engine()
    now_utc = datetime.now(timezone.utc)

    # KSeF: faktury sprzedazowe (invoice_type=1) sa poza zakresem obiegu —
    # zapisujemy je (widocznosc/audyt), ale od razu jako cancelled, z adnotacja.
    # Dla wszystkich innych zrodel invoice_type nie istnieje w raw_data -> brak wplywu.
    extra_dict = doc.to_extra_data_json()
    is_sales_invoice = extra_dict.get("invoice_type") == 1
    if is_sales_invoice:
        extra_dict["auto_cancelled_reason"] = "faktura sprzedazowa, poza zakresem obiegu"
    extra_data_json = json.dumps(extra_dict, ensure_ascii=False, default=str)

    # NAPRAWA 2026-07-21 (na wyrazna prosbe frontu): document_title ma
    # pokazywac PRZEDE WSZYSTKIM nazwe kontrahenta, nie numer dokumentu —
    # dotyczy WSZYSTKICH zrodel pull jednolicie (database, ksef20, ftp,
    # email, api), zgodnie z decyzja z tej sesji.
    #
    # POPRAWKA (2026-07-14, zachowana bez zmian): dla zrodel bez numeru
    # dokumentu (ftp/email, przed OCR), preferuj nazwe pliku zalacznika
    # nad surowym naglowkiem e-mail nadawcy — bardziej uzyteczne dla
    # uzytkownika niz "Dok. | Jan Kowalski <jan@firma.pl>". Ta regula
    # NIE jest usuwana — wstawiona jako krok posredni, bo contractor_name
    # dla ftp/email PRZED OCR moze byc surowym naglowkiem nadawcy, nie
    # prawdziwym kontrahentem biznesowym.
    #
    # Kolejnosc: kontrahent -> (ftp/email bez kontrahenta) nazwa pliku ->
    # numer dokumentu -> "Dokument #<id>" (ostateczny fallback).
    original_filename = extra_dict.get("original_filename")
    if doc.contractor_name:
        document_title = str(doc.contractor_name)
    elif original_filename:
        document_title = str(original_filename)
    elif doc.doc_number:
        document_title = str(doc.doc_number)
    else:
        document_title = f"Dokument #{doc.id_document}"

    async with engine.begin() as conn:
        # Sprawdz czy juz istnieje
        exists_result = await conn.execute(
            text("""
                SELECT id_instance, status
                FROM dbo.skw_document_approval_instances
                WHERE id_source = :src AND id_document = :doc
            """),
            {"src": doc.id_source, "doc": doc.id_document},
        )
        existing = exists_result.fetchone()

        if existing:
            id_instance, current_status = existing

            # Nie nadpisuj terminalnych statusow
            if current_status in ("approved", "cancelled", "rejected"):
                # NAPRAWA (2026-07-29): brakujaca druga wartosc w return
                # powodowala ValueError "too many values to unpack (expected 2)"
                # w wywolujacej petli (_sync_one_source, linia 314), bo string
                # "updated" byl rozpakowywany znak-po-znaku zamiast jako krotka.
                # Dotyczylo KAZDEGO dokumentu widzianego ponownie przez worker
                # po osiagnieciu statusu terminalnego — glowna przyczyna
                # wysokiego odsetka bledow "czesciowy sukces" w synchronizacji.
                return "updated", id_instance

            # Zaktualizuj dostepne kolumny.
            # Tabela skw_document_approval_instances (migracja 0028) ma:
            #   document_title   NVARCHAR(500)
            #   document_amount  DECIMAL(18,2)
            #   extra_data       NVARCHAR(MAX) — JSON z pelnym zestawem pol
            # Kolumny doc_number, contractor_name, document_date sa w extra_data,
            # nie jako osobne kolumny — dane szczegolowe idą do extra_data JSON.
            await conn.execute(
                text("""
                    UPDATE dbo.skw_document_approval_instances
                    SET
                        document_title  = :title,
                        document_amount = :amount,
                        extra_data      = :extra,
                        updated_at      = :now
                    WHERE id_instance = :id
                """),
                {
                    "title":  document_title[:500],
                    # NAPRAWA (2026-07-28): `if doc.amount_gross` traktowal
                    # Decimal("0.00") jako falsy -> kwota 0,00 zamieniana na
                    # NULL. Ujemne kwoty (np. -128.97) juz dzialaly poprawnie
                    # (nonzero Decimal jest truthy) — problem byl WYLACZNIE
                    # z zerem. `is not None` sprawdza obecnosc wartosci, nie
                    # jej prawdziwosc logiczna.
                    "amount": float(doc.amount_gross) if doc.amount_gross is not None else None,
                    "extra":  extra_data_json,
                    "now":    now_utc,
                    "id":     id_instance,
                },
            )
            return "updated", id_instance

        else:
            # Nowy dokument — tylko kolumny ktore istnieja w tabeli (migracja 0028 + 0039)
            # Faktura sprzedazowa KSeF (invoice_type=1) -> od razu cancelled,
            # z adnotacja w extra_data (juz dopisana wyzej). Nigdy nie wchodzi
            # do pending_dispatch/auto_dispatch dla tego typu dokumentu.
            #
            # POPRAWKA (2026-07-14): zrodla ftp/email startuja jako
            # 'ocr_review_pending' — TAK SAMO jak recznie wgrywane pliki —
            # zamiast 'pending_dispatch'. Bez tego ocr_task._update_instance_with_ocr()
            # nigdy nie liczyl requires_review/review_reasons dla tych zrodel
            # (cala ta logika jest gated za `if current_status == 'ocr_review_pending'`),
            # wiec dokumenty o niskiej pewnosci OCR (albo zupelnie bledne, np.
            # doc_number='ktura') przechodzily bez oznaczenia do przegladu, a
            # potem gubily sie w auto_dispatch jako 'unassigned' bez sladu ze
            # cokolwiek bylo nie tak. database/api NIE przechodza przez OCR
            # (brak file_path w raw_data), wiec dla nich zachowanie bez zmian.
            if is_sales_invoice:
                insert_status = "cancelled"
            elif source_type in ("ftp", "email"):
                insert_status = "ocr_review_pending"
            else:
                insert_status = _STATUS_NORMAL

            # NAPRAWA (2026-07-28): OUTPUT INSERTED.id_instance — potrzebne,
            # zeby zaraz po wstawieniu wywolac DuplicateDetectionService na
            # WLASCIWEJ, nowo utworzonej instancji (poprzednio ta funkcja
            # nigdy nie zwracala nowego ID, wiec sprawdzenie duplikatow
            # bylo NIEMOZLIWE do wpiecia w te sciezke w ogole).
            insert_result = await conn.execute(
                text("""
                    INSERT INTO dbo.skw_document_approval_instances (
                        id_source, id_document, status, document_title,
                        document_amount, extra_data, dispatch_attempts,
                        file_sha256, created_at, updated_at
                    )
                    OUTPUT INSERTED.id_instance
                    VALUES (
                        :src, :doc, :status, :title,
                        :amount, :extra, 0,
                        :file_sha256, :now, :now
                    )
                """),
                {
                    "src":    doc.id_source,
                    "doc":    doc.id_document,
                    "status": insert_status,
                    "title":  document_title[:500],
                    # NAPRAWA (2026-07-28): patrz identyczny komentarz w
                    # gałęzi UPDATE powyzej — Decimal("0.00") jest falsy.
                    "amount": float(doc.amount_gross) if doc.amount_gross is not None else None,
                    "extra":  extra_data_json,
                    # NOWE (2026-07-28): file_sha256 — pole opcjonalne na
                    # UnifiedDocument, wypelniane przez adaptery, ktore maja
                    # dostep do surowego pliku (manual/ftp/email — NIE
                    # database/api/ksef20). Dla tego adaptera (DatabaseAdapter)
                    # bedzie zawsze None, co jest poprawne.
                    "file_sha256": getattr(doc, "file_sha256", None),
                    "now":    now_utc,
                },
            )
            new_id_instance = insert_result.scalar_one()
            return "inserted", new_id_instance


async def _mark_sync_status(id_source: int, status: str, message: str) -> None:
    """Aktualizuje last_sync_at, last_sync_status, last_sync_message w skw_document_sources."""
    engine = get_engine()
    now_utc = datetime.now(timezone.utc)

    try:
        async with engine.begin() as conn:
            await conn.execute(
                text("""
                    UPDATE dbo.skw_document_sources
                    SET
                        last_sync_at      = :now,
                        last_sync_status  = :status,
                        last_sync_message = :msg,
                        updated_at        = :now
                    WHERE id_source = :id
                """),
                {
                    "now":    now_utc,
                    "status": status[:20],
                    "msg":    message[:500],
                    "id":     id_source,
                },
            )
    except Exception as exc:
        logger.error(
            "_mark_sync_status blad | id=%s status=%s: %s", id_source, status, exc
        )

async def _get_config_value(key: str, default: str) -> str:
    """Pobiera wartosc z skw_SystemConfig. Fallback = default."""
    engine = get_engine()
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT ConfigValue FROM dbo.skw_SystemConfig
                    WHERE ConfigKey = :key AND IsActive = 1
                """),
                {"key": key},
            )
            row = result.fetchone()
            return row[0] if row and row[0] else default
    except Exception:
        return default
    except Exception:
        return default


async def _enqueue_ocr_for_new_docs(
    redis: Any,
    *,
    id_source: int,
) -> None:
    """
    Kolejkuje ocr_task dla nowych instancji ze zrodla FTP/email z ostatnich 6 minut.

    POPRAWKA (0055): parametr 'redis' historycznie bywal plain Redis client
    (ctx['worker_redis']), ktory NIE ma metody enqueue_job — to metoda
    ARQ-owego ArqRedis (ctx['redis']). Ten sam blad co juz raz naprawiony
    w test_connection (patrz notatka projektowa). Zamiast ufac typowi
    argumentu przekazanego przez wywolujacego, funkcja SAMA weryfikuje
    czy dostala wlasciwy klient PRZED proba wywolania enqueue_job — jesli
    nie, loguje BLAD (nie WARNING, zeby nie ginelo w szumie) z jawnym
    opisem problemu, zamiast lapac AttributeError per-dokument w petli.
    """
    from arq.connections import ArqRedis

    if not isinstance(redis, ArqRedis):
        logger.error(
            "_enqueue_ocr_for_new_docs: przekazany klient Redis to %s, nie ArqRedis — "
            "enqueue_job nie zadziala. Sprawdz wywolanie w _sync_one_source: "
            "musi przekazywac ctx['redis'] (ARQ-owy), NIE ctx['worker_redis'] "
            "(plain client do lockow/cache). Zero taskow OCR zostanie zakolejkowanych "
            "w tym wywolaniu | id_source=%s",
            type(redis).__name__, id_source,
        )
        return

    engine = get_engine()
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text("""
                SELECT [id_instance], [extra_data]
                FROM [dbo].[skw_document_approval_instances]
                WHERE [id_source] = :s
                  AND [created_at] >= DATEADD(MINUTE, -6, SYSUTCDATETIME())
                  AND [extra_data] IS NOT NULL
                  AND [extra_data] LIKE '%file_path%'
            """), {"s": id_source})
            rows = result.fetchall()
    except Exception as exc:
        logger.warning("_enqueue_ocr: blad pobierania instancji: %s", exc)
        return

    if not rows:
        return

    queued = 0
    for id_instance, extra_data_raw in rows:
        try:
            extra = json.loads(extra_data_raw or "{}")
            file_path = extra.get("file_path")
            if not file_path:
                continue
            await redis.enqueue_job("ocr_task", id_instance=id_instance, file_path=file_path)
            queued += 1
        except Exception as exc:
            logger.error(
                "_enqueue_ocr: blad dla id_instance=%d: %s (typ: %s)",
                id_instance, exc, type(exc).__name__,
            )

    if queued:
        logger.info(
            "_enqueue_ocr_for_new_docs: zakolejkowano %d taskow OCR | id_source=%d",
            queued, id_source,
        )

async def _mark_sync_cursor(id_source: int, cursor: dict[str, Any]) -> None:
    """
    NOWE (2026-07-28). Zapisuje kursor zlozony do skw_document_sources.sync_cursor.

    ODDZIELONE od _mark_sync_status/last_sync_at — last_sync_at pozostaje
    WYLACZNIE znacznikiem "kiedy worker ostatnio probowal synchronizowac"
    (uzywanym przez needs_sync), NIE pozycja w danych zrodla.
    """
    engine = get_engine()
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text("""
                    UPDATE dbo.skw_document_sources
                    SET sync_cursor = :cursor,
                        updated_at  = :now
                    WHERE id_source = :id
                """),
                {
                    "cursor": json.dumps(cursor, ensure_ascii=False, default=str),
                    "now":    datetime.now(timezone.utc),
                    "id":     id_source,
                },
            )
        logger.info("_mark_sync_cursor OK | id_source=%s cursor=%s", id_source, cursor)
    except Exception as exc:
        logger.error("_mark_sync_cursor blad | id_source=%s cursor=%s: %s", id_source, cursor, exc)