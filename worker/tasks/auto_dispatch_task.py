# worker/tasks/auto_dispatch_task.py
"""
ARQ Task: auto_dispatch_task — automatyczne przypisanie dokumentow do sciezek obiegu.

Cykl: co 1 minute (niezalezny od source_sync_task).
Przetwarza dokumenty w statusach pending_dispatch FIFO po created_at.

Dla kazdego dokumentu:
  1. Pobierz dokumenty status=pending_dispatch ORDER BY created_at ASC
  2. Dla kazdego: uruchom filter_engine.resolve_path(doc_data)
  3. Jesli sciezka znaleziona: approval_service.dispatch() → status=in_progress
  4. Jesli brak sciezki: inkrementuj dispatch_attempts
     Po progu AUTO_DISPATCH_MAX_ATTEMPTS → status=unassigned + SSE alert

Idempotentnosc:
  Distributed lock Redis: auto_dispatch_lock:{id_instance} (TTL 2 min)
  Gwarantuje ze ten sam dokument nie jest dispatchowany rownoczesnie z dwoch
  instancji workera (przy skalowaniu).

Bezpieczenstwo:
  AUTO_DISPATCH_WORKER_ENABLED=false → task natychmiast zwraca.
  is_test_mode=true na zrodle → dispatch wykonywany ale SSE nie wyslane.

UWAGA: from __future__ import annotations — OK w pliku workera.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any
from datetime import timedelta
from pathlib import Path

from sqlalchemy import text

from worker.core.db import get_engine
from worker.core.logging_setup import get_event_logger
from worker.core.redis_client import publish_sse_event
from worker.settings import get_settings

logger = logging.getLogger("worker.tasks.auto_dispatch")

_SCHEMA             = "dbo"
_DISPATCH_LOCK_PREFIX = "auto_dispatch_lock:"
_DISPATCH_LOCK_TTL    = 120   # 2 minuty
_MAX_DOCS_PER_CYCLE   = 50    # max dokumentow w jednym cyklu (ochrona przed spike)


def _dispatch_decision_log(entry: dict[str, Any]) -> None:
    """
    NAPRAWA (2026-07-16, na prosbe uzytkownika): osobny, dedykowany
    strumien JSONL WYLACZNIE dla decyzji auto_dispatch — nie mieszany z
    innymi eventami workera (snapshot, source_sync itd.) w wspolnym pliku
    events_YYYY-MM-DD.jsonl, ktory dotad byl jedynym miejscem zapisu
    (event_log.log(...) — nadal zostawiony bez zmian, to DODATKOWY log,
    nie zastepstwo).

    Kazda decyzja (dispatched/unassigned/skipped/error) trafia tu jako
    jeden wiersz z pelnym kontekstem — pozwala odpowiedziec na pytanie
    "co i kiedy zdecydowalo o tej sciezce dla tego dokumentu" bez
    przeszukiwania calego, wspolnego strumienia eventow.

    UWAGA — ograniczenie: zawiera id_path (KTORA sciezka), ale NIE
    zawiera uzasadnienia KTORY filtr/warunek do niej doprowadzil —
    resolve_path() zwraca gole id_path bez powodu. Pelne uzasadnienie
    wymagaloby zmiany w worker/services/filter_engine.py (osobne zadanie).
    """
    try:
        settings = get_settings()
        log_dir = Path(settings.LOG_DIR)
        log_dir.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        log_file = log_dir / f"auto_dispatch_{date_str}.jsonl"
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
    except Exception as exc:
        logger.error("_dispatch_decision_log: blad zapisu: %s", exc)


async def auto_dispatch_task(ctx: dict[str, Any]) -> dict[str, Any]:
    """
    ARQ Cron Task: automatyczne przypisanie dokumentow do sciezek obiegu.

    Uruchamiany co 1 minute.
    """
    redis    = ctx.get("worker_redis")
    settings = get_settings()
    t_start  = time.monotonic()
    now_utc  = datetime.now(timezone.utc)

    # Sprawdz feature flag
    if not await _is_dispatch_enabled():
        logger.debug("auto_dispatch_task: AUTO_DISPATCH_WORKER_ENABLED=false — pomijam")
        return {"status": "disabled"}

    event_log = get_event_logger(settings.LOG_DIR)
    event_log.log("auto_dispatch_started", {"ts_utc": now_utc.isoformat()})

    max_attempts = await _get_config_int("AUTO_DISPATCH_MAX_ATTEMPTS", 5)

    # Pobierz dokumenty do dispatcha
    pending = await _get_pending_documents(max_attempts)

    summary = {
        "ts_utc":      now_utc.isoformat(),
        "checked":     len(pending),
        "dispatched":  0,
        "unassigned":  0,
        "skipped":     0,
        "errors":      0,
        "duration_ms": 0,
    }

    for doc in pending:
        id_instance = doc["id_instance"]
        lock_key    = f"{_DISPATCH_LOCK_PREFIX}{id_instance}"

        # Distributed lock — zapobiega rownoleglemu dispatch tej samej instancji
        if redis:
            acquired = await redis.set(lock_key, "1", ex=_DISPATCH_LOCK_TTL, nx=True)
            if not acquired:
                summary["skipped"] += 1
                continue

        try:
            result = await _dispatch_one(doc, max_attempts, redis, event_log)
            if result == "dispatched":
                summary["dispatched"] += 1
            elif result == "unassigned":
                summary["unassigned"] += 1
            else:
                summary["skipped"] += 1
        except Exception as exc:
            summary["errors"] += 1
            logger.error(
                "auto_dispatch_task: blad przy instance=%s: %s",
                id_instance, exc, exc_info=True,
            )
        finally:
            if redis:
                try:
                    await redis.delete(lock_key)
                except Exception:
                    pass

    summary["duration_ms"] = round((time.monotonic() - t_start) * 1000, 1)
    logger.info("auto_dispatch_task ZAKONCZONE", extra=summary)
    event_log.log("auto_dispatch_completed", summary)

    # NAPRAWA (self-review "publikuj wszystkie wazne rzeczy"): alert do
    # adminow gdy w cyklu wystapilo duzo bledow — powtarzajace sie bledy
    # moga oznaczac systemowy problem (filter_engine, baza), nie
    # pojedynczy zly dokument.
    if summary["errors"] > 0:
        try:
            await _publish_dispatch_errors_alert(summary["errors"], summary["checked"])
        except Exception as exc:
            logger.warning("auto_dispatch_task: blad alertu bledow: %s", exc)

    return summary


async def _dispatch_one(
    doc: dict[str, Any],
    max_attempts: int,
    redis: Any,
    event_log: Any,
) -> str:
    """
    Probuje wyznaczyc sciezke obiegu dla jednego dokumentu.
    Zwraca: 'dispatched' | 'unassigned' | 'skipped'.
    """
    id_instance     = doc["id_instance"]
    dispatch_attempts = doc.get("dispatch_attempts", 0)
    id_source       = doc["id_source"]
    id_document     = doc["id_document"]
    extra_data_raw  = doc.get("extra_data") or "{}"

    # Parsuj extra_data
    # NAPRAWA (2026-07-29): brak logowania bledu parsowania oznaczal, ze
    # uszkodzony JSON w extra_data byl NIEODROZNIALNY od pustego extra_data —
    # oba dawaly cichy, pusty slownik. Teraz kazdy blad parsowania jest
    # widoczny w logach z pelnym kontekstem (instance + surowa tresc,
    # ograniczona do 500 znakow zeby nie zalac logu).
    extra: dict = {}
    try:
        extra = json.loads(extra_data_raw)
    except Exception:
        pass

    # Buduj dane dokumentu dla filter_engine
    doc_data = {
        "id_instance":     id_instance,
        "id_source":       id_source,
        "id_document":     id_document,
        "document_amount": doc.get("document_amount"),
        "document_title":  doc.get("document_title", ""),
        "extra_data":      extra,
    }

    # Wywolaj silnik filtrow
    try:
        from app.services.filter_engine import resolve_path
    engine = get_engine()
    id_path = None
    try:
        from worker.services.filter_engine import resolve_path
        auto_filters_enabled = await _get_config_bool("APPROVAL_AUTO_FILTERS_ENABLED", True)
        async with engine.connect() as conn:
            path_result = await resolve_path(conn, doc_data, id_source)
    except ImportError:
        # filter_engine nie jest dostepny w worker — uzyj prostego lookup
        path_result = await _simple_path_lookup(id_source)
    except Exception as exc:
        logger.warning(
            "_dispatch_one: filter_engine blad | instance=%s: %s", id_instance, exc
        )
        path_result = None

    engine = get_engine()

    if path_result:
        # Sciezka znaleziona — dispatch
        id_path = path_result if isinstance(path_result, int) else path_result.get("id_path")
        async with engine.begin() as conn:
            await conn.execute(
                text(f"""
                    UPDATE [{_SCHEMA}].[skw_document_approval_instances]
                    SET [status]           = N'in_progress',
                        [id_path]          = :path,
                        [current_step]     = 1,
                        [dispatched_at]    = SYSUTCDATETIME(),
                        [dispatch_attempts] = [dispatch_attempts] + 1,
                        [updated_at]       = SYSUTCDATETIME()
                    WHERE [id_instance] = :i
                      AND [status] = N'pending_dispatch'
                """),
                {"path": id_path, "i": id_instance},
            )
        except Exception as exc:
            logger.error(
                "_dispatch_one: blad budowy snapshotu | instance=%s path=%s: %s",
                id_instance, id_path, exc, exc_info=True,
            )
            id_path = None  # traktuj jako niepowodzenie dispatcha

    if id_path:
        event_log.log("auto_dispatched", {
            "id_instance": id_instance,
            "id_path":     id_path,
            "id_source":   id_source,
        })
        logger.info("auto_dispatch: dispatched | instance=%s path=%s", id_instance, id_path)

        # NAPRAWA (self-review "publikuj wszystkie wazne rzeczy"): trzy
        # kanaly powiadomien o nowym dokumencie w kolejce (SSE + trwale
        # powiadomienie w bazie + email) — wczesniej ZERO z nich nie
        # bylo wywolywanych z auto-dispatch, mimo ze reczny dispatch
        # przez API uzywa wszystkich trzech (approval_service.py::dispatch).
        # member_ids pobierane RAZ, przekazywane do obu funkcji nizej —
        # unikamy dwukrotnego zapytania do skw_approval_group_members.
        if snapshot_info and redis:
            member_ids: list[int] = []
            try:
                member_ids = await _get_group_members(snapshot_info["id_group_step1"])
            except Exception as members_exc:
                logger.warning(
                    "auto_dispatch: blad pobierania czlonkow grupy | instance=%s: %s",
                    id_instance, members_exc,
                )

            resolved_title = doc.get("document_title") or f"Dokument {id_document}"

            try:
                await _publish_document_waiting(
                    id_instance=id_instance,
                    id_document=id_document,
                    document_title=resolved_title,
                    member_ids=member_ids,
                )
            except Exception as sse_exc:
                logger.warning(
                    "auto_dispatch: document_waiting SSE blad | instance=%s: %s",
                    id_instance, sse_exc,
                )

            try:
                await _notify_group_members_full(
                    redis, member_ids, id_instance, resolved_title,
                )
            except Exception as notify_exc:
                logger.warning(
                    "auto_dispatch: notify_group_members_full blad | instance=%s: %s",
                    id_instance, notify_exc,
                )

        _dispatch_decision_log({
            "ts_utc":            datetime.now(timezone.utc).isoformat(),
            "decision":          "dispatched",
            "id_instance":       id_instance,
            "id_document":       id_document,
            "id_source":         id_source,
            "id_path":           id_path,
            "id_group_step1":    snapshot_info["id_group_step1"] if snapshot_info else None,
            "steps_count":       snapshot_info["steps_count"] if snapshot_info else None,
            "dispatch_attempts": dispatch_attempts,
        })
        return "dispatched"

    else:
        # Brak sciezki — inkrementuj licznik
        new_attempts = dispatch_attempts + 1

        if new_attempts >= max_attempts:
            # Przekroczono prog — status unassigned
            async with engine.begin() as conn:
                await conn.execute(
                    text(f"""
                        UPDATE [{_SCHEMA}].[skw_document_approval_instances]
                        SET [status]            = N'unassigned',
                            [dispatch_attempts] = :attempts,
                            [updated_at]        = SYSUTCDATETIME()
                        WHERE [id_instance] = :i
                          AND [status] = N'pending_dispatch'
                    """),
                    {"attempts": new_attempts, "i": id_instance},
                )

            # SSE alert do adminow
            if redis:
                try:
                    await _send_unassigned_sse(redis, id_instance, id_source, id_document)
                except Exception as sse_exc:
                    logger.warning("auto_dispatch: SSE alert blad: %s", sse_exc)

                # NAPRAWA (self-review "dokoncz symetrie unassigned"): trwale
                # powiadomienie w skw_user_notifications + email dla adminow —
                # wczesniej tylko SSE (bez retencji, admin offline nigdy sie
                # nie dowiadywal).
                try:
                    await _notify_unassigned_full(
                        redis, id_instance, id_document,
                        doc.get("document_title") or f"Dokument {id_document}",
                    )
                except Exception as notify_exc:
                    logger.warning(
                        "auto_dispatch: notify_unassigned_full blad | instance=%s: %s",
                        id_instance, notify_exc,
                    )

            event_log.log("auto_dispatch_unassigned", {
                "id_instance": id_instance,
                "attempts":    new_attempts,
                "id_source":   id_source,
            })
            logger.warning(
                "auto_dispatch: UNASSIGNED | instance=%s attempts=%s/%s",
                id_instance, new_attempts, max_attempts,
            )
            _dispatch_decision_log({
                "ts_utc":            datetime.now(timezone.utc).isoformat(),
                "decision":          "unassigned",
                "id_instance":       id_instance,
                "id_document":       id_document,
                "id_source":         id_source,
                "id_path":           None,
                "dispatch_attempts": new_attempts,
                "max_attempts":      max_attempts,
            })
            return "unassigned"

        else:
            # Jeszcze nie przekroczono progu — inkrementuj i zostaw pending_dispatch
            async with engine.begin() as conn:
                await conn.execute(
                    text(f"""
                        UPDATE [{_SCHEMA}].[skw_document_approval_instances]
                        SET [dispatch_attempts] = :attempts,
                            [updated_at]        = SYSUTCDATETIME()
                        WHERE [id_instance] = :i
                          AND [status] = N'pending_dispatch'
                    """),
                    {"attempts": new_attempts, "i": id_instance},
                )
            logger.debug(
                "auto_dispatch: brak sciezki | instance=%s attempts=%s/%s",
                id_instance, new_attempts, max_attempts,
            )
            _dispatch_decision_log({
                "ts_utc":            datetime.now(timezone.utc).isoformat(),
                "decision":          "skipped",
                "id_instance":       id_instance,
                "id_document":       id_document,
                "id_source":         id_source,
                "id_path":           None,
                "dispatch_attempts": new_attempts,
                "max_attempts":      max_attempts,
            })
            return "skipped"


async def _get_group_members(id_group: int) -> list[int]:
    """
    Zwraca liste id_user czlonkow grupy — worker-side, bez cache Redis
    (w odroznieniu od _get_group_members_cached w approval_sse_service.py,
    bo ten task uruchamia sie raz na minute, nie ma sensu cache'owac na
    tak krotki cykl).

    SWIADOME UPROSZCZENIE: NIE uwzglednia aktywnych delegacji (w
    odroznieniu od approval_sse_service.py::_fetch_group_members, ktore
    dolicza delegatow). Dla powiadomienia "nowy dokument w kolejce"
    powiadomienie samego czlonka (bez delegata) jest wystarczajace —
    delegat i tak zobaczy dokument przy nastepnym odswiezeniu/logowaniu.
    Jesli to zalozenie okaze sie bledne w praktyce, rozszerzenie o
    delegacje wymagaloby identycznego JOIN co w approval_sse_service.py.
    """
    engine = get_engine()
    async with engine.connect() as conn:
        result = await conn.execute(
            text(f"""
                SELECT [id_user] FROM [{_SCHEMA}].[skw_approval_group_members]
                WHERE [id_group] = :g
            """),
            {"g": id_group},
        )
        return [r[0] for r in result.fetchall()]


async def _publish_document_waiting(
    id_instance: int,
    id_document: str,
    document_title: str,
    member_ids: list[int],
) -> None:
    """
    SSE document_waiting po udanym auto-dispatch — odpowiednik
    approval_sse_service.py::on_dispatch() (galaz "document_waiting"),
    ale wywolywany z workera (nie moze zaimportowac backendu — izolacja).

    NAPRAWA (self-review "publikuj wszystkie wazne rzeczy"): przed ta
    poprawka galaz "dispatched" w _dispatch_one() NIE publikowala
    ZADNEGO SSE — czlonkowie grupy pierwszego kroku nie dowiadywali sie
    o nowym dokumencie w czasie rzeczywistym, mimo ze reczny dispatch
    przez API to robi (dispatch_ack + document_waiting).

    UWAGA na format koperty: worker.core.redis_client.publish_sse_event()
    uzywa INNEGO ksztaltu koperty niz backend/app/services/event_service.py
    (event_type/source/ts zamiast type/event_id/timestamp) — replikujemy
    format JUZ UZYWANY przez worker (spojnosc wewnatrz-workerowa), nie
    format backendu. To znana niespojnosc miedzy dwoma warstwami,
    zostawiona swiadomie — ujednolicenie wymagaloby zmiany po obu stronach
    (w tym frontu).

    member_ids przekazywane z zewnatrz (nie pobierane tutaj) — unikamy
    podwojnego zapytania do bazy, bo _notify_group_members_full potrzebuje
    tej samej listy dla powiadomien trwalych + email.
    """
    if not member_ids:
        logger.warning(
            "_publish_document_waiting: brak czlonkow grupy — brak "
            "odbiorcow SSE | id_instance=%s",
            id_instance,
        )
        return

    data = {
        "id_instance":    id_instance,
        "id_document":    id_document,
        "document_title": document_title,
        "status":         "in_progress",
        "current_step":   1,
    }

    for member_id in member_ids:
        try:
            await publish_sse_event(
                event_type="document_waiting",
                data=data,
                target_user_id=member_id,
            )
        except Exception as exc:
            logger.warning(
                "_publish_document_waiting: blad publikacji dla user=%s: %s",
                member_id, exc,
            )


async def _publish_dispatch_errors_alert(error_count: int, checked_count: int) -> None:
    """
    SSE system_notification do adminow, gdy w jednym cyklu auto_dispatch
    liczba bledow przekroczy prog AUTO_DISPATCH_ERROR_ALERT_THRESHOLD
    (SystemConfig, domyslnie 3) — powtarzajace sie bledy dispatchu moga
    oznaczac systemowy problem (np. filter_engine, baza), nie pojedynczy
    zly dokument, wiec admin powinien wiedziec od razu, nie dopiero przy
    przegladaniu logow.
    """
    threshold = await _get_config_int("AUTO_DISPATCH_ERROR_ALERT_THRESHOLD", 3)
    if error_count < threshold:
        return
    try:
        await publish_sse_event(
            event_type="system_notification",
            data={
                "level":   "WARNING",
                "message": (
                    f"auto_dispatch_task: {error_count}/{checked_count} dokumentow "
                    f"zakonczylo sie bledem w jednym cyklu (prog={threshold}). "
                    f"Sprawdz logi auto_dispatch_YYYY-MM-DD.jsonl."
                ),
                "component": "auto_dispatch_task",
                "error_count": error_count,
                "checked_count": checked_count,
            },
        )
    except Exception as exc:
        logger.warning("_publish_dispatch_errors_alert: blad publikacji SSE: %s", exc)


async def _notify_group_members_full(
    redis: Any,
    member_ids: list[int],
    id_instance: int,
    document_title: str,
) -> None:
    """
    NAPRAWA (self-review, kontynuacja "publikuj wszystkie wazne rzeczy"):
    SSE (_publish_document_waiting) to tylko JEDEN z TRZECH kanalow
    powiadomien uzywanych przez reczny dispatch (approval_service.py::
    dispatch() → send_approval_notification + queue_approval_email +
    SSE). Auto-dispatch mial WYLACZNIE SSE, przed ta poprawka —
    uzytkownik offline w momencie auto-dispatchu NIGDY nie dowiadywal
    sie o nowym dokumencie (SSE nie ma retencji), bo brakowalo:
      1. Trwalego powiadomienia w skw_user_notifications (widoczne w
         GET /approval/notifications, licznik notif_unread na badge UI)
      2. Zbiorczego emaila (debounced, queue_approval_email)

    Oba to zarejestrowane taski ARQ — wywolywane przez enqueue_job(),
    NIE bezposrednie wywolanie funkcji (standardowy wzorzec ARQ,
    identyczny jak queue_approval_email → flush_approval_emails
    w email_task_approval.py).
    """
    if not member_ids or not redis:
        return

    from arq.connections import ArqRedis
    arq_redis = ArqRedis(redis.connection_pool)

    # 1) Trwale powiadomienie w bazie — JEDNO enqueue dla calej listy
    #    (send_approval_notification przyjmuje recipient_user_ids: list[int])
    try:
        await arq_redis.enqueue_job(
            "send_approval_notification",
            action="dispatched",
            id_instance=id_instance,
            document_title=document_title,
            recipient_user_ids=member_ids,
            step_order=1,
        )
    except Exception as exc:
        logger.warning(
            "_notify_group_members_full: enqueue send_approval_notification blad "
            "| instance=%s: %s",
            id_instance, exc,
        )

    # 2) Email debounced — queue_approval_email przyjmuje JEDNEGO id_user,
    #    wiec enqueue per member (event_type='approval_pending' — zgodnie
    #    z _TITLE_MAP w flush_approval_emails, NIE surowe action='dispatched')
    for member_id in member_ids:
        try:
            await arq_redis.enqueue_job(
                "queue_approval_email",
                event_type="approval_pending",
                id_instance=id_instance,
                id_user=member_id,
                document_title=document_title,
                step_order=1,
            )
        except Exception as exc:
            logger.warning(
                "_notify_group_members_full: enqueue queue_approval_email blad "
                "| instance=%s user=%s: %s",
                id_instance, member_id, exc,
            )


async def _get_pending_documents(max_attempts: int) -> list[dict[str, Any]]:
    """Pobiera dokumenty pending_dispatch do przetworzenia."""
    engine = get_engine()
    async with engine.connect() as conn:
        result = await conn.execute(
            text(f"""
                SELECT TOP {_MAX_DOCS_PER_CYCLE}
                    i.[id_instance],
                    i.[id_source],
                    i.[id_document],
                    i.[document_amount],
                    i.[document_title],
                    i.[extra_data],
                    i.[dispatch_attempts]
                FROM [{_SCHEMA}].[skw_document_approval_instances] i
                JOIN [{_SCHEMA}].[skw_document_sources] s
                  ON s.[id_source] = i.[id_source]
                WHERE i.[status] = N'pending_dispatch'
                  AND s.[is_active] = 1
                  AND i.[dispatch_attempts] < :max_att
                ORDER BY i.[created_at] ASC, i.[id_instance] ASC
            """),
            {"max_att": max_attempts},
        )
        cols = list(result.keys())
        return [dict(zip(cols, r)) for r in result.fetchall()]


async def _simple_path_lookup(id_source: int) -> int | None:
    """
    Uproszczony lookup sciezki gdy filter_engine niedostepny w workerze.
    Zwraca pierwsza aktywna sciezke przypisana do zrodla.
    """
    engine = get_engine()
    async with engine.connect() as conn:
        result = await conn.execute(
            text(f"""
                SELECT TOP 1 [id_path]
                FROM [{_SCHEMA}].[skw_approval_paths]
                WHERE [is_active] = 1
                ORDER BY [id_path] ASC
            """),
        )
        row = result.fetchone()
        return row[0] if row else None


async def _send_unassigned_sse(
    redis: Any,
    id_instance: int,
    id_source: int,
    id_document: str,
) -> None:
    """Publikuje SSE event do kanalu adminow o dokumencie bez sciezki."""
    import uuid
    payload = json.dumps({
        "event":       "document_unassigned",
        "event_id":    str(uuid.uuid4()),
        "id_instance": id_instance,
        "id_source":   id_source,
        "id_document": id_document,
        "message":     f"Dokument {id_document} nie moze byc przypisany do sciezki obiegu",
        "ts":          datetime.now(timezone.utc).isoformat(),
    }, ensure_ascii=False)
    await redis.publish("channel:admins", payload)


async def _is_dispatch_enabled() -> bool:
    engine = get_engine()
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT [ConfigValue] FROM [dbo].[skw_SystemConfig] WHERE [ConfigKey] = N'AUTO_DISPATCH_WORKER_ENABLED' AND [IsActive] = 1")
            )
            row = result.fetchone()
            return str(row[0]).lower() == "true" if row else True
    except Exception:
        return True


async def _get_config_int(key: str, default: int) -> int:
    engine = get_engine()
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT [ConfigValue] FROM [dbo].[skw_SystemConfig] WHERE [ConfigKey] = :k AND [IsActive] = 1"),
                {"k": key},
            )
            row = result.fetchone()
            return int(row[0]) if row else default
    except Exception:
        return default
    
async def _build_dispatch_snapshot(
    engine: Any, *, id_instance: int, id_path: int, id_document: str,
) -> dict[str, Any]:
    """
    Buduje pelny snapshot krokow dla nowo zdyspozytorowanej instancji.
    Port Krok 4 + Krok 6+7 + Krok 8 z approval_service.py::dispatch() —
    zeby auto-dispatch tworzyl dokladnie taki sam stan jak reczny dispatch przez API.

    NAPRAWA: teraz zwraca dict (wczesniej None) — id_group_step1 potrzebny
    w _dispatch_one() do wyslania SSE document_waiting po udanym dispatch
    (wczesniej ta galaz w ogole nie publikowala SSE — patrz
    _publish_document_waiting powyzej w tym pliku).
    """
    async with engine.begin() as conn:
        steps_result = await conn.execute(
            text(f"""
                SELECT s.[step_order], s.[id_group], s.[deadline_hours], g.[consensus_type]
                FROM [{_SCHEMA}].[skw_approval_path_steps] s
                JOIN [{_SCHEMA}].[skw_approval_groups] g ON g.[id_group] = s.[id_group]
                WHERE s.[id_path] = :p
                ORDER BY s.[step_order] ASC
            """),
            {"p": id_path},
        )
        steps = steps_result.fetchall()
        if not steps:
            raise ValueError(f"Sciezka id_path={id_path} nie ma zdefiniowanych krokow.")

        now = datetime.now(timezone.utc)
        first_deadline_hours = steps[0][2]
        global_deadline = now + timedelta(hours=first_deadline_hours) if first_deadline_hours else None

        await conn.execute(
            text(f"""
                UPDATE [{_SCHEMA}].[skw_document_approval_instances]
                SET [status]            = N'in_progress',
                    [id_path]           = :path,
                    [current_step]      = 1,
                    [dispatched_at]     = :now,
                    [deadline_at]       = :dl,
                    [dispatch_attempts] = [dispatch_attempts] + 1,
                    [updated_at]        = :now
                WHERE [id_instance] = :i
                  AND [status] = N'pending_dispatch'
            """),
            {"path": id_path, "now": now, "dl": global_deadline, "i": id_instance},
        )

        for step_order, id_group, deadline_hours, consensus_type in steps:
            if consensus_type == "AND":
                members_result = await conn.execute(
                    text(f"""
                        SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_group_members]
                        WHERE [id_group] = :g
                    """),
                    {"g": id_group},
                )
                votes_required = max(1, members_result.scalar() or 0)
            else:
                votes_required = 1

            snap_status = "in_progress" if step_order == 1 else "pending"
            step_deadline = now + timedelta(hours=deadline_hours) if (step_order == 1 and deadline_hours) else None

            await conn.execute(
                text(f"""
                    INSERT INTO [{_SCHEMA}].[skw_document_approval_snapshot_steps]
                        ([id_instance],[step_order],[id_group],[status],
                         [votes_required],[votes_cast],[deadline_at],[created_at],[updated_at])
                    VALUES (:i,:so,:g,:st,:vr,0,:dl,:now,:now)
                """),
                {"i": id_instance, "so": step_order, "g": id_group,
                 "st": snap_status, "vr": votes_required, "dl": step_deadline, "now": now},
            )

        await conn.execute(
            text(f"""
                INSERT INTO [{_SCHEMA}].[skw_approval_log]
                    ([id_instance],[id_user],[username_snapshot],[action],
                     [step_order_snapshot],[id_group_snapshot],[consensus_snapshot],
                     [is_voided],[details],[logged_at])
                VALUES
                    (:i,NULL,N'system:auto_dispatch',N'dispatched',
                     1,:g0,:c0,0,:details,:now)
            """),
            {
                "i": id_instance, "g0": steps[0][1], "c0": steps[0][3],
                "details": json.dumps({
                    "id_path": id_path, "steps_count": len(steps),
                    "trigger": "auto_dispatch_task",
                }, ensure_ascii=False),
                "now": now,
            },
        )

        return {"id_group_step1": steps[0][1], "steps_count": len(steps)}


async def _get_config_bool(key: str, default: bool) -> bool:
    engine = get_engine()
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT [ConfigValue] FROM [dbo].[skw_SystemConfig] WHERE [ConfigKey] = :k AND [IsActive] = 1"),
                {"k": key},
            )
            row = result.fetchone()
            return str(row[0]).lower() == "true" if row else default
    except Exception:
        return default


async def _get_admin_user_ids() -> list[int]:
    """
    Zwraca liste id_user administratorow (rola 'Admin', aktywni).

    NAPRAWA (sesja 2026-07-16, "dokoncz symetrie unassigned"): brakowalo
    tej funkcji do tej pory — _send_unassigned_sse() wysyla WYLACZNIE
    broadcast na channel:admins (SSE, bez retencji), nigdy nie budowal
    listy konkretnych id_user, bo nie byla nigdzie potrzebna do tego
    momentu (persystentne powiadomienia wymagaja konkretnych id_user,
    nie samego kanalu SSE).

    JOIN po RoleName='Admin' (nie hardkodowany RoleID) — odpornosc na
    ewentualna zmiane numeracji ID ról w przyszlosci.
    """
    engine = get_engine()
    async with engine.connect() as conn:
        result = await conn.execute(
            text(f"""
                SELECT u.[ID_USER]
                FROM [{_SCHEMA}].[skw_Users] u
                JOIN [{_SCHEMA}].[skw_Roles] r ON r.[ID_ROLE] = u.[RoleID]
                WHERE r.[RoleName] = N'Admin' AND u.[IsActive] = 1
            """),
        )
        return [r[0] for r in result.fetchall()]


async def _notify_unassigned_full(
    redis: Any,
    id_instance: int,
    id_document: str,
    document_title: str,
) -> None:
    """
    Trwale powiadomienie + email dla administratorow gdy auto_dispatch_task
    nie znajdzie sciezki obiegu (galaz "unassigned") — analogiczne do
    _notify_group_members_full() dla galezi "dispatched", ale odbiorcy to
    administratorzy (_get_admin_user_ids), nie czlonkowie grupy kroku 1.

    Wymaga migracji 0059 (notif_type='approval_unassigned') i szablonu
    "unassigned" w worker/tasks/notification_task.py::_NOTIFICATION_TEMPLATES.
    """
    if not redis:
        return

    admin_ids = await _get_admin_user_ids()
    if not admin_ids:
        logger.warning(
            "_notify_unassigned_full: brak aktywnych administratorow — "
            "brak odbiorcow powiadomienia | instance=%s",
            id_instance,
        )
        return

    from arq.connections import ArqRedis
    arq_redis = ArqRedis(redis.connection_pool)

    try:
        await arq_redis.enqueue_job(
            "send_approval_notification",
            action="unassigned",
            id_instance=id_instance,
            document_title=document_title,
            recipient_user_ids=admin_ids,
        )
    except Exception as exc:
        logger.warning(
            "_notify_unassigned_full: enqueue send_approval_notification blad "
            "| instance=%s: %s",
            id_instance, exc,
        )

    for admin_id in admin_ids:
        try:
            await arq_redis.enqueue_job(
                "queue_approval_email",
                event_type="approval_unassigned",
                id_instance=id_instance,
                id_user=admin_id,
                document_title=document_title,
            )
        except Exception as exc:
            logger.warning(
                "_notify_unassigned_full: enqueue queue_approval_email blad "
                "| instance=%s admin=%s: %s",
                id_instance, admin_id, exc,
            )

# =============================================================================
# Mapowanie doc_data -> UnifiedDocument.to_filter_dict()
# =============================================================================
#
# NAPRAWA (2026-07-29, zgloszenie: filtr id_filter=4 "contractor_name
# contains 'praktyka lekarska'" nie zadzialal dla instancji 2576/2575/2571,
# wszystkie ze zrodla id_source=4 "ksef_fakir"):
#
# _dispatch_one() budowal doc_data z WYLACZNIE szesciu kluczy (id_instance,
# id_source, id_document, document_amount, document_title, extra_data).
# UnifiedDocument ma metode to_filter_dict() (backend/app/schemas/
# unified_document.py) ktora eksponuje POPRAWNE nazwy pol dla filter_engine
# (contractor_name, nip, doc_number, doc_date, document_type, currency,
# source_name, amount_net) — ale ta metoda dziala na ZYWYM obiekcie
# UnifiedDocument z adaptera, a _dispatch_one() dostaje juz zapisany wiersz
# z bazy, wiec nie moze jej bezposrednio wywolac.
#
# Rownoczesnie UnifiedDocument.to_extra_data_json() (ta sama klasa, inna
# metoda — patrz ten sam plik) serializuje CZESC tych samych pol pod
# INNYMI nazwami (m.in. contractor_name -> "contractor" w extra_data),
# bo ta metoda ma inny cel: zasila "sekcje techniczna" w widoku dokumentu
# na froncie (Etap2_Scalony_Backend_Frontend.docx, sekcja 3.3), NIE silnik
# filtrow. Administrator konfigurujac filtr uzywa nazw z to_filter_dict()
# (bo to one sa udokumentowane jako "pola UnifiedDocument"), ale
# _dispatch_one() dostarczal silnikowi strukture zgodna z to_extra_data_json().
# Rezultat: KAZDY warunek filtra standard na polu innym niz document_title/
# document_amount/id_instance/id_source/id_document zwracal cicho False,
# dla KAZDEGO dokumentu z KAZDEGO zrodla, od poczatku istnienia tego
# mechanizmu — nie tylko dla zgloszonego przypadku.
#
# Ponizsza tabela mapuje: nazwa pola oczekiwana w field_name warunku filtra
# (zgodna z UnifiedDocument.to_filter_dict())
#   -> nazwa klucza pod jaka ta sama wartosc FAKTYCZNIE lezy w extra_data
#      (zgodna z UnifiedDocument.to_extra_data_json()).
_EXTRA_DATA_FIELD_MAP: dict[str, str] = {
    "source_name":     "source_name",
    "doc_number":      "doc_number",
    "doc_date":        "doc_date",
    "amount_gross":    "amount_gross",
    "amount_net":      "amount_net",
    "contractor_name": "contractor",   # <- ROZNICA NAZW, pierwotna przyczyna zgloszenia
    "nip":             "nip",
    "document_type":   "document_type",
    "currency":        "currency",
}

# Pola UnifiedDocument.to_filter_dict(), ktorych NIE DA SIE odzyskac z
# extra_data, bo UnifiedDocument.to_extra_data_json() (backend/app/schemas/
# unified_document.py) w ogole ich nie zapisuje — nie chodzi o inna nazwe,
# tylko o CALKOWITY BRAK tych danych w bazie dla kazdego dokumentu.
# Filtr skonfigurowany na ktoryms z tych pol BEDZIE nadal zawsze zwracal
# False po tej poprawce. To NIE jest blad tej funkcji — to osobny,
# nierozwiazany jeszcze problem w warstwie persystencji (worker/tasks/
# source_sync_task.py::_upsert_instance -> UnifiedDocument.to_extra_data_json).
# Trzymane jawnie w kodzie (zamiast po cichu pomijane), zeby:
#   a) klucz byl obecny w doc_data z wartoscia None (spojne zachowanie
#      _get_nested() zamiast nieobecnosci klucza),
#   b) bylo to udokumentowane w JEDNYM miejscu, latwym do znalezienia
#      przy nastepnym zgloszeniu "filtr na polu X nie dziala".
_UNRECOVERABLE_FILTER_FIELDS: frozenset[str] = frozenset(
    {"amount_vat", "payment_term", "payment_method", "external_id"}
)


def _build_filter_dict(
    *,
    id_instance: int,
    id_source: int,
    id_document: str,
    document_amount: Any,
    document_title: str,
    extra: dict[str, Any],
) -> dict[str, Any]:
    """
    Buduje slownik dla filter_engine.resolve_path(), odtwarzajac z extra_data
    (na tyle, na ile to obecnie mozliwe) kontrakt UnifiedDocument.to_filter_dict().

    Zwraca slownik zawierajacy:
      - id_instance, id_source, id_document, document_amount, document_title
        — bez zmian, jak dotychczas (dla kodu ktory juz na nich polega),
      - extra_data — oryginalny, NIEZMIENIONY slownik z bazy, pod tym samym
        kluczem co dotychczas — dla wstecznej zgodnosci z ewentualnymi
        filtrami juz skonfigurowanymi na notacji kropkowej
        (np. field_name="extra_data.ksef_id"),
      - wszystkie pola z _EXTRA_DATA_FIELD_MAP pod nazwami zgodnymi z
        UnifiedDocument.to_filter_dict() — TO jest wlasciwa naprawa,
      - pola z _UNRECOVERABLE_FILTER_FIELDS jawnie ustawione na None.

    Kazde wywolanie loguje na poziomie DEBUG pelna liste kluczy i pol,
    ktorych nie udalo sie odzyskac z extra_data dla TEGO KONKRETNEGO
    dokumentu (przydatne przy debugowaniu "czemu ten jeden dokument sie
    nie zdispatchowal", w odroznieniu od strukturalnego ograniczenia
    opisanego w _UNRECOVERABLE_FILTER_FIELDS powyzej).
    """
    result: dict[str, Any] = {
        "id_instance":     id_instance,
        "id_source":       id_source,
        "id_document":     id_document,
        "document_amount": document_amount,
        "document_title":  document_title,
        "extra_data":      extra,
    }

    missing_in_extra: list[str] = []
    for filter_field, extra_key in _EXTRA_DATA_FIELD_MAP.items():
        value = extra.get(extra_key)
        result[filter_field] = value
        if value is None:
            missing_in_extra.append(filter_field)

    for field in _UNRECOVERABLE_FILTER_FIELDS:
        result[field] = None

    # amount_gross ma tez wlasna kolumne (document_amount) w tabeli.
    # Jesli z jakiegos powodu extra_data.amount_gross jest puste (np.
    # rekordy zapisane PRZED ta poprawka), uzyj document_amount jako
    # fallbacku zamiast zostawiac None.
    if result.get("amount_gross") is None and document_amount is not None:
        result["amount_gross"] = document_amount

    logger.debug(
        "_build_filter_dict: zbudowano slownik dla filter_engine",
        extra={
            "event":                 "auto_dispatch.filter_dict_built",
            "id_instance":           id_instance,
            "id_source":             id_source,
            "doc_data_keys":         sorted(result.keys()),
            "missing_in_extra_data": missing_in_extra,
        },
    )

    return result