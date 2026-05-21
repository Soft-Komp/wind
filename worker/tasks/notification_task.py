# worker/tasks/notification_task.py
"""
ARQ task — tworzenie persystentnych powiadomien biznesowych.

Wywolywany z background_tasks w routerach API po kazdej akcji
(dispatch, accept, rollback, reject itp.) — po commit, nie blokuje odpowiedzi.

Jeden task tworzy WIELE powiadomien jednoczesnie (dla wszystkich odbiorców
danej akcji) i inkrementuje liczniki Redis.

Typy powiadomien (zgodne z CK constraint w skw_user_notifications):
  approval_pending          — dokument czeka na akcje usera
  approval_accepted         — obieg zakonczony akceptacja
  approval_rejected         — dokument odrzucony
  approval_deadline_warning — deadline za X godzin
  approval_deadline_expired — przekroczony deadline
  approval_escalated        — eskalacja

Zasady:
  - ctx["worker_redis"] — nie ctx["redis"]
  - raw SQL text()
  - Blad INSERTu do notifications nie rzuca wyjatku — loguje i kontynuuje
  - INCR notif_unread:* po kazdym udanym INSERT

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)

_SCHEMA   = "dbo"
_LOG_DIR  = Path(os.environ.get("LOG_DIR", "/app/logs"))
_NOTIF_TTL = 86400  # 24h Redis TTL

# Mapowanie akcji na typ powiadomienia i szablon tekstu
_NOTIFICATION_TEMPLATES: dict[str, dict] = {
    "dispatched": {
        "type":  "approval_pending",
        "title": "Nowy dokument do akceptacji",
        "msg":   "Dokument '{title}' trafil do Twojej kolejki akceptacyjnej (etap {step}).",
    },
    "accepted": {
        "type":  "approval_accepted",
        "title": "Etap zaakceptowany",
        "msg":   "Dokument '{title}' — etap {step} zostal zaakceptowany.",
    },
    "step_advanced": {
        "type":  "approval_pending",
        "title": "Twoja kolej na akceptacje",
        "msg":   "Dokument '{title}' oczekuje na akceptacje Twojej grupy (etap {step}).",
    },
    "approved": {
        "type":  "approval_accepted",
        "title": "Dokument zaakceptowany",
        "msg":   "Obieg dokumentu '{title}' zakonczony pomyslnie — dokument zaakceptowany.",
    },
    "rejected": {
        "type":  "approval_rejected",
        "title": "Dokument odrzucony",
        "msg":   "Dokument '{title}' zostal odrzucony na etapie {step}.",
    },
    "rollback": {
        "type":  "approval_pending",
        "title": "Obieg cofniety — wymagana ponowna akceptacja",
        "msg":   "Obieg dokumentu '{title}' cofniety do etapu {step}. Prosimy o ponowna akcje.",
    },
    "cancelled": {
        "type":  "approval_rejected",
        "title": "Obieg anulowany",
        "msg":   "Obieg dokumentu '{title}' zostal anulowany.",
    },
    "deadline_warning": {
        "type":  "approval_deadline_warning",
        "title": "Zbliза sie termin akceptacji",
        "msg":   "Dokument '{title}' — pozostalo {hours}h do terminu akceptacji (etap {step}).",
    },
    "forwarded": {
        "type":  "approval_pending",
        "title": "Dokument przekazany do Twojej grupy",
        "msg":   "Dokument '{title}' zostal przekazany do Twojej grupy akceptacyjnej.",
    },
}


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def send_approval_notification(
    ctx: dict[str, Any],
    *,
    action: str,
    id_instance: int,
    document_title: str | None,
    recipient_user_ids: list[int],
    step_order: int = 0,
    hours_to_deadline: int | None = None,
    extra_data: dict | None = None,
) -> dict:
    """
    ARQ task — tworzy powiadomienia w skw_user_notifications i aktualizuje Redis.

    Args:
        action:              Typ akcji (dispatched, accepted, approved, rejected itp.)
        id_instance:         ID instancji obiegu.
        document_title:      Tytul dokumentu.
        recipient_user_ids:  Lista ID uzytkownikow do powiadomienia.
        step_order:          Numer etapu (do tresci powiadomienia).
        hours_to_deadline:   Dla deadline_warning — ile godzin do terminu.
        extra_data:          Dodatkowe dane do logu.

    Returns:
        {"notified": N, "errors": M, "action": action}
    """
    from worker.core.db import get_db_session

    redis = ctx.get("worker_redis")
    if not recipient_user_ids:
        return {"notified": 0, "errors": 0, "action": action}

    template = _NOTIFICATION_TEMPLATES.get(action)
    if not template:
        logger.warning("send_approval_notification | Nieznana akcja: %s", action)
        return {"notified": 0, "errors": 0, "action": action}

    title_display = document_title or f"Dokument #{id_instance}"
    now = _utcnow_naive()

    # Buduj tresc
    msg = template["msg"].format(
        title=title_display,
        step=step_order,
        hours=hours_to_deadline or 0,
    )
    notif_title = template["title"]
    notif_type  = template["type"]

    notified = 0
    errors   = 0

    try:
        async with get_db_session() as db:
            for id_user in recipient_user_ids:
                try:
                    await db.execute(
                        text(
                            f"INSERT INTO [{_SCHEMA}].[skw_user_notifications] "
                            f"([id_user],[notification_type],[id_instance],"
                            f"[title],[message],[created_at]) "
                            f"VALUES (:uid,:typ,:inst,:tit,:msg,:now)"
                        ),
                        {
                            "uid":  id_user,
                            "typ":  notif_type,
                            "inst": id_instance,
                            "tit":  notif_title[:200],
                            "msg":  msg,
                            "now":  now,
                        },
                    )
                    notified += 1
                except Exception as exc:
                    logger.error(
                        "send_approval_notification | INSERT error user=%d: %s",
                        id_user, exc,
                    )
                    errors += 1
                    continue

            if notified > 0:
                await db.commit()

    except Exception as exc:
        logger.error("send_approval_notification | DB error: %s", exc, exc_info=True)
        return {"notified": 0, "errors": len(recipient_user_ids), "action": action}

    # INCR Redis po commit
    if redis and notified > 0:
        for id_user in recipient_user_ids:
            try:
                key = f"notif_unread:{id_user}"
                await redis.incr(key)
                await redis.expire(key, _NOTIF_TTL)
            except Exception as exc:
                logger.warning(
                    "send_approval_notification | Redis INCR error user=%d: %s",
                    id_user, exc,
                )

    result = {
        "action":    action,
        "notified":  notified,
        "errors":    errors,
        "id_instance": id_instance,
    }
    logger.info("send_approval_notification | %s", result)
    return result