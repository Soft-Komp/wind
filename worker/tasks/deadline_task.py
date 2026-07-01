# worker/tasks/deadline_task.py
"""
Cron task — sprawdzanie terminow obiegow dokumentow.

Uruchamiany co godzine (WorkerSettings.cron_jobs, minuta 5).
Trzy petले skanowania per wywolanie:

  Petla A — WARNING (24h przed terminem)
    Warunek: deadline_at BETWEEN NOW() AND NOW()+24h
             AND is_deadline_notified = 0
    Akcja:   INSERT approval_log (deadline_warning)
             INSERT skw_user_notifications (approval_deadline_warning)
             INCR notif_unread:* w Redis
             SSE publish

  Petla B — EXPIRED (przekroczony termin)
    Warunek: deadline_at < NOW()
             AND is_deadline_notified = 0
    Akcja:   INSERT approval_log (deadline_expired)
             INSERT skw_user_notifications (approval_deadline_expired)
             INCR notif_unread:* w Redis
             SSE publish
             UPDATE is_deadline_notified = 1

  Petla C — ESCALATION (po N dniach od przekroczenia, bez akceptacji)
    Warunek: deadline_at < NOW() - ESCALATION_DAYS
             AND status = 'in_progress'
             AND is_deadline_notified = 1
             AND brak wpisu 'deadline_escalated' w approval_log
    Akcja:   INSERT approval_log (deadline_escalated)
             INSERT skw_user_notifications (approval_escalated) dla supervise
             INCR notif_unread:* w Redis

Konfiguracja (env):
  APPROVAL_ESCALATION_REMINDER_DAYS — po ilu dniach od przekroczenia escalacja
                                      (domyslnie 3)

Zasady:
  - ctx["worker_redis"] — nie ctx["redis"] (ARQ internal)
  - raw SQL text() — bez ORM
  - Blad jednej instancji NIE przerywa petli
  - Logi: JSONL + strukturalny logger

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_SCHEMA    = "dbo"
_LOG_DIR   = Path(os.environ.get("LOG_DIR", "/app/logs"))
_NOTIF_TTL = 86400  # 24h Redis TTL


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _escalation_days() -> int:
    try:
        return int(os.environ.get("APPROVAL_ESCALATION_REMINDER_DAYS", "3"))
    except (ValueError, TypeError):
        return 3


def _jsonl_log(action: str, data: dict) -> None:
    try:
        log_file = _LOG_DIR / f"approval_deadline_{_utcnow().strftime('%Y-%m-%d')}.jsonl"
        entry = {
            "ts":     _utcnow().isoformat(timespec="milliseconds"),
            "action": action,
            **data,
        }
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
    except Exception as exc:
        logger.error("deadline_jsonl_log | error: %s", exc)


# =============================================================================
# GLOWNY TASK
# =============================================================================

async def deadline_check_task(ctx: dict[str, Any]) -> dict:
    """
    Cron task — pelne sprawdzanie terminow: warning, expired, escalation.

    Rejestracja w WorkerSettings:
        cron(deadline_check_task, hour={0..23}, minute=5, timeout=300, unique=True)
    """
    from worker.core.db import get_db_session

    redis      = ctx.get("worker_redis")
    task_start = _utcnow()

    logger.info("deadline_check_task | START | ts=%s", task_start.isoformat())

    counters = {
        "warnings_sent":    0,
        "expired_notified": 0,
        "escalated":        0,
        "errors":           0,
    }

    try:
        async with get_db_session() as db:
            now = _utcnow_naive()

            # ── Petla A: WARNING — 24h przed terminem ─────────────────────────
            await _run_warnings(db, redis, now, counters)

            # ── Petla B: EXPIRED — termin przekroczony ────────────────────────
            await _run_expired(db, redis, now, counters)

            # ── Petla C: ESCALATION — N dni po przekroczeniu ──────────────────
            await _run_escalation(db, redis, now, counters)

    except Exception as exc:
        logger.critical("deadline_check_task | KRYTYCZNY BLAD: %s", exc, exc_info=True)
        counters["errors"] += 1

    duration_ms = int((_utcnow() - task_start).total_seconds() * 1000)
    result = {
        "status":      "ok" if counters["errors"] == 0 else "partial_error",
        "duration_ms": duration_ms,
        "ts":          task_start.isoformat(),
        **counters,
    }
    logger.info("deadline_check_task | KONIEC | %s", result)
    _jsonl_log("deadline_check_completed", result)
    return result


# =============================================================================
# PETLA A: WARNING — 24h przed terminem
# =============================================================================

async def _run_warnings(db, redis, now: datetime, counters: dict) -> None:
    """
    Skanuje instancje gdzie deadline za < 24h i is_deadline_notified = 0.

    Deduplicacja przez Redis klucz approval_warning_sent:{id_instance} TTL 23h —
    zapobiega spamowi przy kazdym uruchomieniu crona przez 24h okno.
    """
    from sqlalchemy import text

    warning_threshold = now + timedelta(hours=24)

    rows = await db.execute(
        text(
            f"SELECT i.[id_instance], i.[id_document], i.[current_step], "
            f"  i.[dispatched_by], i.[document_title], i.[deadline_at], s.[id_group] "
            f"FROM [{_SCHEMA}].[skw_document_approval_instances] i "
            f"JOIN [{_SCHEMA}].[skw_document_approval_snapshot_steps] s "
            f"  ON s.[id_instance]=i.[id_instance] AND s.[step_order]=i.[current_step] "
            f"WHERE i.[status]=N'in_progress' "
            f"  AND i.[deadline_at] IS NOT NULL "
            f"  AND i.[deadline_at] >= :now "
            f"  AND i.[deadline_at] <= :threshold "
            f"  AND i.[is_deadline_notified] = 0 "
            f"ORDER BY i.[deadline_at] ASC"
        ),
        {"now": now, "threshold": warning_threshold},
    )
    instances = rows.fetchall()
    logger.info("deadline_warnings | Znaleziono %d instancji", len(instances))

    skipped = 0
    for row in instances:
        id_instance, id_document, current_step, dispatched_by, doc_title, deadline_at, id_group = row
        try:
            # Deduplicacja — sprawdz czy warning juz wyslany w tym 24h oknie
            dedup_key = f"approval_warning_sent:{id_instance}"
            if redis:
                try:
                    already_sent = await redis.exists(dedup_key)
                    if already_sent:
                        skipped += 1
                        logger.debug(
                            "deadline_warning | SKIP (dedup) | inst=%d", id_instance
                        )
                        continue
                except Exception as exc:
                    logger.warning(
                        "deadline_warning | Redis dedup check error inst=%d: %s",
                        id_instance, exc,
                    )

            hours_remaining = max(0, int((deadline_at - now).total_seconds() / 3600))
            await _notify_users(
                db, redis,
                id_instance=id_instance,
                id_document=id_document,
                current_step=current_step,
                dispatched_by=dispatched_by,
                doc_title=doc_title,
                id_group=id_group,
                action="deadline_warning",
                notif_type="approval_deadline_warning",
                title_tpl="Zbliза sie termin: {title}",
                msg_tpl=(
                    "Termin akceptacji dokumentu '{title}' "
                    "(etap {step}) uplywa za {hours}h. Prosimy o pilna akcje."
                ),
                hours=hours_remaining,
                now=now,
                update_notified_flag=False,  # flage ustawia Petla B po przekroczeniu
            )

            # Ustaw klucz dedup — TTL 23h (nie 24h zeby nie nakryl sie z kolejnym cyklem)
            if redis:
                try:
                    await redis.set(dedup_key, "1", ex=23 * 3600)
                except Exception as exc:
                    logger.warning(
                        "deadline_warning | Redis dedup set error inst=%d: %s",
                        id_instance, exc,
                    )

            counters["warnings_sent"] += 1

        except Exception as exc:
            logger.error("deadline_warning | inst=%d error: %s", id_instance, exc)
            counters["errors"] += 1

    if skipped:
        logger.info("deadline_warnings | Pominieto (dedup): %d", skipped)


# =============================================================================
# PETLA B: EXPIRED — termin przekroczony
# =============================================================================

async def _run_expired(db, redis, now: datetime, counters: dict) -> None:
    """Skanuje instancje z przekroczonym terminem, is_deadline_notified = 0."""
    from sqlalchemy import text

    rows = await db.execute(
        text(
            f"SELECT i.[id_instance], i.[id_document], i.[current_step], "
            f"  i.[dispatched_by], i.[document_title], i.[deadline_at], s.[id_group] "
            f"FROM [{_SCHEMA}].[skw_document_approval_instances] i "
            f"JOIN [{_SCHEMA}].[skw_document_approval_snapshot_steps] s "
            f"  ON s.[id_instance]=i.[id_instance] AND s.[step_order]=i.[current_step] "
            f"WHERE i.[status]=N'in_progress' "
            f"  AND i.[deadline_at] IS NOT NULL "
            f"  AND i.[deadline_at] < :now "
            f"  AND i.[is_deadline_notified] = 0 "
            f"ORDER BY i.[deadline_at] ASC"
        ),
        {"now": now},
    )
    instances = rows.fetchall()
    logger.info("deadline_expired | Znaleziono %d instancji", len(instances))

    for row in instances:
        id_instance, id_document, current_step, dispatched_by, doc_title, deadline_at, id_group = row
        try:
            hours_overdue = max(0, int((now - deadline_at).total_seconds() / 3600))
            await _notify_users(
                db, redis,
                id_instance=id_instance,
                id_document=id_document,
                current_step=current_step,
                dispatched_by=dispatched_by,
                doc_title=doc_title,
                id_group=id_group,
                action="deadline_expired",
                notif_type="approval_deadline_expired",
                title_tpl="Przekroczony termin: {title}",
                msg_tpl=(
                    "Termin akceptacji dokumentu '{title}' "
                    "(etap {step}) zostal przekroczony o {hours}h. Wymagana natychmiastowa akcja."
                ),
                hours=hours_overdue,
                now=now,
                update_notified_flag=True,  # ustawiamy flage
            )
            counters["expired_notified"] += 1
        except Exception as exc:
            logger.error("deadline_expired | inst=%d error: %s", id_instance, exc)
            counters["errors"] += 1


# =============================================================================
# PETLA C: ESCALATION — N dni po przekroczeniu
# =============================================================================

async def _run_escalation(db, redis, now: datetime, counters: dict) -> None:
    """
    Skanuje instancje gdzie termin przekroczony o N dni i brak wpisu escalated.
    Powiadamia uzytkownikow z uprawnieniem approval.supervise.
    """
    from sqlalchemy import text

    esc_days      = _escalation_days()
    esc_threshold = now - timedelta(days=esc_days)

    rows = await db.execute(
        text(
            f"SELECT i.[id_instance], i.[id_document], i.[current_step], "
            f"  i.[dispatched_by], i.[document_title], i.[deadline_at], s.[id_group] "
            f"FROM [{_SCHEMA}].[skw_document_approval_instances] i "
            f"JOIN [{_SCHEMA}].[skw_document_approval_snapshot_steps] s "
            f"  ON s.[id_instance]=i.[id_instance] AND s.[step_order]=i.[current_step] "
            f"WHERE i.[status]=N'in_progress' "
            f"  AND i.[deadline_at] IS NOT NULL "
            f"  AND i.[deadline_at] < :threshold "
            f"  AND i.[is_deadline_notified] = 1 "
            f"  AND NOT EXISTS ("
            f"      SELECT 1 FROM [{_SCHEMA}].[skw_approval_log] l "
            f"      WHERE l.[id_instance] = i.[id_instance] "
            f"        AND l.[action] = N'deadline_escalated'"
            f"  ) "
            f"ORDER BY i.[deadline_at] ASC"
        ),
        {"threshold": esc_threshold},
    )
    instances = rows.fetchall()
    logger.info("deadline_escalation | Znaleziono %d instancji (prog=%dd)", len(instances), esc_days)

    if not instances:
        return

    # Pobierz uzytkownikow z approval.supervise — jednorazowo przed petla
    supervisors = await _get_supervisors(db)
    if not supervisors:
        logger.warning("deadline_escalation | Brak uzytkownikow z approval.supervise")

    for row in instances:
        id_instance, id_document, current_step, dispatched_by, doc_title, deadline_at, id_group = row
        try:
            days_overdue = max(0, int((now - deadline_at).total_seconds() / 86400))
            doc_display  = doc_title or f"Dokument #{id_document}"

            # INSERT approval_log
            await db.execute(
                text(
                    f"INSERT INTO [{_SCHEMA}].[skw_approval_log] "
                    f"([id_instance],[id_user],[username_snapshot],[action],"
                    f"[step_order_snapshot],[id_group_snapshot],[details],[logged_at]) "
                    f"VALUES (:inst,NULL,N'system',N'deadline_escalated',:step,:grp,:det,:now)"
                ),
                {
                    "inst": id_instance,
                    "step": current_step,
                    "grp":  id_group,
                    "det":  json.dumps({
                        "deadline_at":  deadline_at.isoformat() if deadline_at else None,
                        "days_overdue": days_overdue,
                        "esc_days":     esc_days,
                        "document_title": doc_title,
                    }),
                    "now": now,
                },
            )

            # INSERT notyfikacje dla supervisors + dyspozytora
            recipients: set[int] = set(supervisors)
            if dispatched_by:
                recipients.add(dispatched_by)

            for id_user in recipients:
                await db.execute(
                    text(
                        f"INSERT INTO [{_SCHEMA}].[skw_user_notifications] "
                        f"([id_user],[notification_type],[id_instance],[title],[message],[created_at]) "
                        f"VALUES (:uid,N'approval_escalated',:inst,:tit,:msg,:now)"
                    ),
                    {
                        "uid":  id_user,
                        "inst": id_instance,
                        "tit":  f"Eskalacja — brak akceptacji od {days_overdue} dni: {doc_display[:80]}",
                        "msg":  (
                            f"Dokument '{doc_display}' nie zostal zaakceptowany przez {days_overdue} dni "
                            f"od przekroczenia terminu. Wymagana interwencja nadzorcy (etap {current_step})."
                        ),
                        "now": now,
                    },
                )

            await db.commit()

            # INCR Redis
            if redis:
                for id_user in recipients:
                    try:
                        key = f"notif_unread:{id_user}"
                        await redis.incr(key)
                        await redis.expire(key, _NOTIF_TTL)
                    except Exception as exc:
                        logger.warning("escalation | Redis INCR user=%d: %s", id_user, exc)

            _jsonl_log("deadline_escalated", {
                "id_instance": id_instance,
                "days_overdue": days_overdue,
                "supervisors": list(supervisors),
                "dispatched_by": dispatched_by,
            })
            counters["escalated"] += 1
            logger.info("escalation | OK | inst=%d days=%d supervisors=%d",
                        id_instance, days_overdue, len(supervisors))

        except Exception as exc:
            logger.error("escalation | inst=%d error: %s", id_instance, exc)
            counters["errors"] += 1


# =============================================================================
# HELPER: notify_users — wspolny dla warning i expired
# =============================================================================

async def _notify_users(
    db,
    redis,
    *,
    id_instance: int,
    id_document: str,
    current_step: int,
    dispatched_by: int | None,
    doc_title: str | None,
    id_group: int,
    action: str,
    notif_type: str,
    title_tpl: str,
    msg_tpl: str,
    hours: int,
    now: datetime,
    update_notified_flag: bool,
) -> None:
    from sqlalchemy import text

    doc_display = doc_title or f"Dokument #{id_document}"
    title       = title_tpl.format(title=doc_display[:100])
    msg         = msg_tpl.format(title=doc_display, step=current_step, hours=hours)

    # INSERT approval_log
    await db.execute(
        text(
            f"INSERT INTO [{_SCHEMA}].[skw_approval_log] "
            f"([id_instance],[id_user],[username_snapshot],[action],"
            f"[step_order_snapshot],[id_group_snapshot],[details],[logged_at]) "
            f"VALUES (:inst,NULL,N'system',:action,:step,:grp,:det,:now)"
        ),
        {
            "inst":   id_instance,
            "action": action,
            "step":   current_step,
            "grp":    id_group,
            "det":    json.dumps({
                "hours": hours,
                "document_title": doc_title,
            }),
            "now": now,
        },
    )

    # Pobierz czlonkow grupy
    members_rows = await db.execute(
        text(f"SELECT [id_user] FROM [{_SCHEMA}].[skw_approval_group_members] "
             f"WHERE [id_group]=:g"),
        {"g": id_group},
    )
    members: set[int] = {r[0] for r in members_rows.fetchall()}

    recipients: set[int] = set(members)
    if dispatched_by:
        recipients.add(dispatched_by)

    for id_user in recipients:
        await db.execute(
            text(
                f"INSERT INTO [{_SCHEMA}].[skw_user_notifications] "
                f"([id_user],[notification_type],[id_instance],[title],[message],[created_at]) "
                f"VALUES (:uid,:typ,:inst,:tit,:msg,:now)"
            ),
            {
                "uid":  id_user,
                "typ":  notif_type,
                "inst": id_instance,
                "tit":  title,
                "msg":  msg,
                "now":  now,
            },
        )

    if update_notified_flag:
        await db.execute(
            text(
                f"UPDATE [{_SCHEMA}].[skw_document_approval_instances] "
                f"SET [is_deadline_notified]=1,[updated_at]=:now "
                f"WHERE [id_instance]=:inst"
            ),
            {"now": now, "inst": id_instance},
        )

    await db.commit()

    # INCR Redis
    if redis:
        for id_user in recipients:
            try:
                key = f"notif_unread:{id_user}"
                await redis.incr(key)
                await redis.expire(key, _NOTIF_TTL)
            except Exception as exc:
                logger.warning("notify_users | Redis INCR user=%d: %s", id_user, exc)

    # SSE publish
    # SMS (opcjonalny kanal — DEADLINE_SMS_ENABLED z SystemConfig)
    sms_enabled = await _get_config_bool(db, "DEADLINE_SMS_ENABLED", False)
    if sms_enabled and action in ("deadline_warning", "deadline_expired"):
        await _send_sms_reminders(
            db, recipients,
            doc_title=doc_title,
            action=action,
            hours=hours,
        )
    if redis:
        try:
            payload = json.dumps({
                "type":         action,
                "instance_id":  id_instance,
                "step_order":   current_step,
                "id_group":     id_group,
                "hours":        hours,
                "document_title": doc_title,
            })
            await redis.publish(f"sse:approval:group:{id_group}", payload)
            if dispatched_by:
                await redis.publish(f"sse:approval:instance:{id_instance}", payload)
        except Exception as exc:
            logger.warning("notify_users | SSE error: %s", exc)

    _jsonl_log(action, {
        "id_instance": id_instance,
        "id_group":    id_group,
        "step":        current_step,
        "hours":       hours,
        "recipients":  list(recipients),
        "flag_updated": update_notified_flag,
    })
    logger.info("%s | OK | inst=%d notified=%d flag=%s",
                action, id_instance, len(recipients), update_notified_flag)


# =============================================================================
# HELPER: get_supervisors — uzytkownicy z approval.supervise
# =============================================================================

async def _get_supervisors(db) -> list[int]:
    """
    Zwraca liste id_user uzytkownikow z uprawnieniem approval.supervise.

    Sciezka: skw_Users.role_id
             -> skw_RolePermissions.role_id / permission_id
             -> skw_Permissions.id (name = 'approval.supervise')

    Nazwy kolumn zgodne z modelami ORM projektu:
      skw_Permissions:    id, name
      skw_RolePermissions: role_id, permission_id
      skw_Users:          ID_USER, role_id, IsActive
    """
    from sqlalchemy import text

    rows = await db.execute(
        text(
            f"SELECT DISTINCT u.[ID_USER] "
            f"FROM [{_SCHEMA}].[skw_Users] u "
            f"JOIN [{_SCHEMA}].[skw_RolePermissions] rp ON rp.[ID_ROLE] = u.[RoleID] "
            f"JOIN [{_SCHEMA}].[skw_Permissions] p ON p.[ID_PERMISSION] = rp.[ID_PERMISSION] "
            f"WHERE p.[PermissionName] = N'approval.supervise' "
            f"  AND p.[IsActive] = 1 "
            f"  AND u.[IsActive] = 1"
        )
    )
    return [r[0] for r in rows.fetchall()]

async def _send_sms_reminders(
    db,
    recipients: set[int],
    *,
    doc_title: str,
    action: str,
    hours: float | None,
) -> None:
    """Wysyła SMS do użytkowników z ustawionym phone_number."""
    from sqlalchemy import text as _text

    if not recipients:
        return

    placeholders = ",".join(f":u{i}" for i in range(len(recipients)))
    params = {f"u{i}": uid for i, uid in enumerate(recipients)}

    rows = await db.execute(
        _text(
            f"SELECT [ID_USER], [phone_number], [FullName] "
            f"FROM [{_SCHEMA}].[skw_Users] "
            f"WHERE [ID_USER] IN ({placeholders}) "
            f"  AND [phone_number] IS NOT NULL "
            f"  AND [phone_number] != N''"
        ),
        params,
    )
    targets = rows.fetchall()
    if not targets:
        return

    label = "Uwaga — termin za" if action == "deadline_warning" else "PRZEKROCZONY termin"
    hours_str = f" ({int(hours)}h)" if hours is not None else ""
    message = f"{label}{hours_str}: {doc_title[:80]}. Sprawdz system windykacji."

    from worker.services.sms_service import SmsMessage, send_sms

    for id_user, phone, full_name in targets:
        try:
            await send_sms(SmsMessage(phone_number=phone, message=message, user_id=id_user))
        except Exception as exc:
            logger.error("_send_sms_reminders | user=%d: %s", id_user, exc)


async def _get_config_bool(db, key: str, default: bool) -> bool:
    """Odczytuje wartość bool z skw_SystemConfig."""
    from sqlalchemy import text as _text
    try:
        result = await db.execute(
            _text(
                f"SELECT [ConfigValue] FROM [{_SCHEMA}].[skw_SystemConfig] "
                f"WHERE [ConfigKey] = :k AND [IsActive] = 1"
            ),
            {"k": key},
        )
        row = result.fetchone()
        return str(row[0]).lower() == "true" if row else default
    except Exception:
        return default