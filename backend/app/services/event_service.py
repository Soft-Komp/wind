"""
Serwis Eventów / SSE — System Windykacja
==========================================
Krok 14 / Faza 3 — services/event_service.py

Odpowiedzialność:
    - Publikacja eventów do Redis Pub/Sub (dla SSE endpoint)
    - Zapis każdego eventu do pliku JSONL (append-only, nieusuwalne)
    - Silnie typowane funkcje publish_* dla każdego typu eventu
    - Obsługa dwóch kanałów: channel:admins (broadcast) i channel:user:{id}

Typy eventów (stałe):
    TASK_COMPLETED        = "task_completed"
    PERMISSIONS_UPDATED   = "permissions_updated"
    NEW_INVOICES          = "new_invoices"
    DEBTOR_UPDATED        = "debtor_updated"
    SYSTEM_NOTIFICATION   = "system_notification"
    MONIT_STATUS_CHANGED  = "monit_status_changed"
    SCHEMA_TAMPER_DETECTED = "schema_tamper_detected"

Kanały Redis Pub/Sub:
    channel:admins     → wszyscy zalogowani admini (broadcast krytycznych eventów)
    channel:user:{id}  → konkretny użytkownik (powiadomienia personalne)

Decyzje projektowe:
    - publish() jest non-blocking — błąd Redis NIE rzuca wyjątku, tylko loguje
    - Plik JSONL jest piszący nawet jeśli Redis jest niedostępny (dual-write)
    - Każdy event ma: type, data, timestamp, user_id (kto wygenerował), event_id (UUID4)
    - event_id pozwala na deduplicację po stronie SSE klienta

Ścieżka docelowa: backend/app/services/event_service.py
Autor: System Windykacja — Faza 3 Krok 14
Wersja: 1.0.0
Data: 2026-02-19
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional

import orjson
from redis.asyncio import Redis

# ---------------------------------------------------------------------------
# Logger własny modułu
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stałe — typy eventów
# ---------------------------------------------------------------------------

EventType = Literal[
    "task_completed",
    "permissions_updated",
    "new_invoices",
    "debtor_updated",
    "system_notification",
    "monit_status_changed",
    "schema_tamper_detected",
    "user_locked",
    "user_unlocked",
    "snapshot_created",
    "snapshot_restored",
]

# Poziomy powiadomień systemowych
NotificationLevel = Literal["INFO", "WARNING", "ERROR", "CRITICAL"]

# Kanały Redis
_CHANNEL_ADMINS       = "channel:admins"
_CHANNEL_USER_PATTERN = "channel:user:{user_id}"

# Plik logów eventów
_EVENTS_LOG_FILE_PATTERN = "logs/events_{date}.jsonl"


# ===========================================================================
# Funkcje pomocnicze — prywatne
# ===========================================================================

def _get_events_log_file() -> Path:
    """Zwraca ścieżkę do dziennego pliku logów eventów."""
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return log_dir / f"events_{today}.jsonl"


def _build_event_envelope(
    event_type: str,
    data: dict,
    user_id: Optional[int] = None,
) -> dict:
    """
    Buduje standaryzowaną kopertę eventu.

    Każdy event ma: event_id (UUID4), type, data, timestamp, user_id.
    event_id pozwala SSE klientowi na deduplicację.

    Args:
        event_type: Typ eventu (z EventType Literal).
        data:       Payload eventu (dowolne dane).
        user_id:    ID użytkownika który wygenerował event (None = systemowy).

    Returns:
        Słownik z pełną kopertą eventu.
    """
    return {
        "event_id":  str(uuid.uuid4()),
        "type":      event_type,
        "data":      data,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id":   user_id,
    }


def _append_event_to_log(envelope: dict) -> None:
    """
    Dopisuje event do pliku JSONL (append-only).

    Błąd zapisu NIE blokuje publikacji do Redis.

    Args:
        envelope: Koperta eventu do zapisania.
    """
    try:
        line = orjson.dumps(envelope, option=orjson.OPT_APPEND_NEWLINE)
        with _get_events_log_file().open("ab") as f:
            f.write(line)
    except OSError as exc:
        logger.warning(
            "Nie udało się zapisać eventu do pliku JSONL",
            extra={
                "event_type": envelope.get("type"),
                "event_id": envelope.get("event_id"),
                "error": str(exc),
            }
        )


async def _publish_to_channel(
    redis: Redis,
    channel: str,
    envelope: dict,
) -> bool:
    """
    Publikuje event do kanału Redis Pub/Sub.

    Non-blocking — błąd Redis jest logowany ale NIE rzuca wyjątku.

    Args:
        redis:    Klient Redis.
        channel:  Nazwa kanału Redis.
        envelope: Koperta eventu do opublikowania.

    Returns:
        True jeśli publikacja się powiodła, False w przeciwnym razie.
    """
    try:
        subscribers = await redis.publish(channel, orjson.dumps(envelope))
        logger.debug(
            "Event opublikowany do Redis Pub/Sub",
            extra={
                "channel": channel,
                "event_type": envelope.get("type"),
                "event_id": envelope.get("event_id"),
                "subscribers_reached": subscribers,
            }
        )
        return True
    except Exception as exc:
        logger.warning(
            "Nie udało się opublikować eventu do Redis Pub/Sub",
            extra={
                "channel": channel,
                "event_type": envelope.get("type"),
                "event_id": envelope.get("event_id"),
                "error": str(exc),
            }
        )
        return False


# ===========================================================================
# Publiczne API — bazowa funkcja publish
# ===========================================================================

async def publish(
    redis: Redis,
    event_type: str,
    data: dict,
    user_id: Optional[int] = None,
    target_user_id: Optional[int] = None,
    broadcast_to_admins: bool = True,
) -> dict:
    """
    Publikuje event do Redis Pub/Sub i zapisuje do pliku JSONL.

    Dual-write: zawsze zapisuje do pliku NIEZALEŻNIE od stanu Redis.
    Jeśli target_user_id jest podany → publikuje do channel:user:{id}.
    Jeśli broadcast_to_admins=True → publikuje też do channel:admins.

    Args:
        redis:               Klient Redis.
        event_type:          Typ eventu.
        data:                Payload eventu.
        user_id:             ID użytkownika generującego event.
        target_user_id:      ID odbiorcy (per-user channel). None = tylko broadcast.
        broadcast_to_admins: Czy publikować do channel:admins.

    Returns:
        Słownik z metadanymi operacji:
        {
            "event_id": str,
            "published_to": list[str],
            "logged_to_file": bool,
        }
    """
    envelope = _build_event_envelope(event_type, data, user_id)

    # Zapis do pliku JSONL (zawsze, niezależnie od Redis)
    _append_event_to_log(envelope)

    published_to: list[str] = []

    # Publikacja do kanału per-user
    if target_user_id is not None:
        user_channel = _CHANNEL_USER_PATTERN.format(user_id=target_user_id)
        if await _publish_to_channel(redis, user_channel, envelope):
            published_to.append(user_channel)

    # Broadcast do adminów
    if broadcast_to_admins:
        if await _publish_to_channel(redis, _CHANNEL_ADMINS, envelope):
            published_to.append(_CHANNEL_ADMINS)

    logger.info(
        "Event opublikowany",
        extra={
            "event_type": event_type,
            "event_id": envelope["event_id"],
            "target_user_id": target_user_id,
            "published_to": published_to,
        }
    )

    return {
        "event_id":       envelope["event_id"],
        "published_to":   published_to,
        "logged_to_file": True,
    }


# ===========================================================================
# Silnie typowane funkcje publish_* dla każdego typu eventu
# ===========================================================================

async def publish_task_completed(
    redis: Redis,
    task_name: str,
    success_count: int,
    failed_count: int,
    message: str,
    target_user_id: Optional[int] = None,
    triggered_by_user_id: Optional[int] = None,
    extra_data: Optional[dict] = None,
) -> dict:
    """
    Publikuje event zakończenia zadania asynchronicznego (ARQ).

    Wywoływany przez ARQ worker po zakończeniu bulk operacji (np. masowa wysyłka).

    Args:
        redis:                Klient Redis.
        task_name:            Nazwa zadania (np. "send_bulk_email").
        success_count:        Liczba pomyślnie przetworzonych rekordów.
        failed_count:         Liczba błędów.
        message:              Komunikat do wyświetlenia użytkownikowi.
        target_user_id:       ID użytkownika który zlecił zadanie (per-user channel).
        triggered_by_user_id: ID użytkownika (do metadanych eventu).
        extra_data:           Dodatkowe dane (opcjonalne).

    Returns:
        Metadane operacji publish.
    """
    data: dict = {
        "task_name":     task_name,
        "success_count": success_count,
        "failed_count":  failed_count,
        "total":         success_count + failed_count,
        "message":       message,
        "has_errors":    failed_count > 0,
    }
    if extra_data:
        data["extra"] = extra_data

    return await publish(
        redis=redis,
        event_type="task_completed",
        data=data,
        user_id=triggered_by_user_id,
        target_user_id=target_user_id,
        broadcast_to_admins=True,
    )


async def publish_permissions_updated(
    redis: Redis,
    role_id: int,
    role_name: str,
    updated_by_user_id: Optional[int] = None,
    added_permissions: Optional[list[str]] = None,
    removed_permissions: Optional[list[str]] = None,
) -> dict:
    """
    Publikuje event aktualizacji uprawnień roli.

    Frontend może zareagować wymuszając re-fetch uprawnień użytkownika.

    Args:
        redis:                Klient Redis.
        role_id:              ID roli której uprawnienia się zmieniły.
        role_name:            Nazwa roli.
        updated_by_user_id:   ID admina który dokonał zmiany.
        added_permissions:    Lista dodanych uprawnień.
        removed_permissions:  Lista usuniętych uprawnień.

    Returns:
        Metadane operacji publish.
    """
    data: dict = {
        "role_id":   role_id,
        "role_name": role_name,
        "message":   f"Uprawnienia roli '{role_name}' zostały zaktualizowane.",
    }
    if added_permissions:
        data["added"] = added_permissions
    if removed_permissions:
        data["removed"] = removed_permissions

    return await publish(
        redis=redis,
        event_type="permissions_updated",
        data=data,
        user_id=updated_by_user_id,
        target_user_id=None,
        broadcast_to_admins=True,
    )


async def publish_system_notification(
    redis: Redis,
    message: str,
    level: NotificationLevel = "INFO",
    component: Optional[str] = None,
    details: Optional[dict] = None,
) -> dict:
    """
    Publikuje systemowe powiadomienie do wszystkich adminów.

    Używane przez schema_integrity, health checks, ARQ scheduler.

    Args:
        redis:     Klient Redis.
        message:   Treść powiadomienia.
        level:     Poziom ważności (INFO/WARNING/ERROR/CRITICAL).
        component: Komponent który generuje powiadomienie (np. "schema_integrity").
        details:   Dodatkowe szczegóły techniczne.

    Returns:
        Metadane operacji publish.
    """
    data: dict = {
        "message":   message,
        "level":     level,
        "component": component or "system",
    }
    if details:
        data["details"] = details

    return await publish(
        redis=redis,
        event_type="system_notification",
        data=data,
        user_id=None,
        target_user_id=None,
        broadcast_to_admins=True,
    )


async def publish_debtor_updated(
    redis: Redis,
    debtor_id: int,
    update_type: str,
    updated_by_user_id: Optional[int] = None,
    details: Optional[dict] = None,
) -> dict:
    """
    Publikuje event aktualizacji dłużnika.

    Wywoływany gdy zmienia się stan monitu lub historia monitów dłużnika.
    Frontend może odświeżyć widok szczegółów dłużnika.

    Args:
        redis:                Klient Redis.
        debtor_id:            ID kontrahenta WAPRO.
        update_type:          Typ aktualizacji (np. "monit_sent", "monit_delivered").
        updated_by_user_id:   ID użytkownika który dokonał zmiany.
        details:              Dodatkowe szczegóły.

    Returns:
        Metadane operacji publish.
    """
    data: dict = {
        "debtor_id":   debtor_id,
        "update_type": update_type,
    }
    if details:
        data["details"] = details

    return await publish(
        redis=redis,
        event_type="debtor_updated",
        data=data,
        user_id=updated_by_user_id,
        target_user_id=None,
        broadcast_to_admins=True,
    )


async def publish_monit_status_changed(
    redis: Redis,
    monit_id: int,
    debtor_id: int,
    old_status: str,
    new_status: str,
    monit_type: str,
    target_user_id: Optional[int] = None,
) -> dict:
    """
    Publikuje event zmiany statusu monitu.

    Wywoływany przez webhook endpoint (callback od bramki SMS/email).

    Args:
        redis:           Klient Redis.
        monit_id:        ID monitu w dbo_ext.MonitHistory.
        debtor_id:       ID kontrahenta.
        old_status:      Poprzedni status (np. "sent").
        new_status:      Nowy status (np. "delivered").
        monit_type:      Typ monitu (email/sms/print).
        target_user_id:  ID użytkownika który zlecił wysyłkę (per-user notification).

    Returns:
        Metadane operacji publish.
    """
    data: dict = {
        "monit_id":   monit_id,
        "debtor_id":  debtor_id,
        "old_status": old_status,
        "new_status": new_status,
        "monit_type": monit_type,
        "message":    f"Status monitu #{monit_id} zmieniony: {old_status} → {new_status}",
    }
    return await publish(
        redis=redis,
        event_type="monit_status_changed",
        data=data,
        user_id=None,
        target_user_id=target_user_id,
        broadcast_to_admins=(new_status in {"failed", "bounced"}),
    )


async def publish_user_locked(
    redis: Redis,
    locked_user_id: int,
    locked_username: str,
    reason: str,
    locked_by_user_id: Optional[int] = None,
    locked_until: Optional[str] = None,
) -> dict:
    """
    Publikuje event zablokowania konta użytkownika.

    Powiadamia admina (broadcast) i zablokowanego usera (jeśli ma otwartą sesję).

    Args:
        redis:              Klient Redis.
        locked_user_id:     ID zablokowanego użytkownika.
        locked_username:    Username (do komunikatu).
        reason:             Powód blokady.
        locked_by_user_id:  ID admina który zablokował.
        locked_until:       ISO datetime do kiedy konto jest zablokowane.

    Returns:
        Metadane operacji publish.
    """
    data: dict = {
        "user_id":      locked_user_id,
        "username":     locked_username,
        "reason":       reason,
        "locked_until": locked_until,
        "message":      f"Konto użytkownika '{locked_username}' zostało zablokowane.",
    }
    return await publish(
        redis=redis,
        event_type="user_locked",
        data=data,
        user_id=locked_by_user_id,
        target_user_id=locked_user_id,
        broadcast_to_admins=True,
    )


async def publish_user_unlocked(
    redis: Redis,
    unlocked_user_id: int,
    unlocked_username: str,
    unlocked_by_user_id: Optional[int] = None,
) -> dict:
    """
    Publikuje event odblokowania konta użytkownika.

    Args:
        redis:                Klient Redis.
        unlocked_user_id:     ID odblokowanego użytkownika.
        unlocked_username:    Username (do komunikatu).
        unlocked_by_user_id:  ID admina który odblokował.

    Returns:
        Metadane operacji publish.
    """
    data: dict = {
        "user_id":  unlocked_user_id,
        "username": unlocked_username,
        "message":  f"Konto użytkownika '{unlocked_username}' zostało odblokowane.",
    }
    return await publish(
        redis=redis,
        event_type="user_unlocked",
        data=data,
        user_id=unlocked_by_user_id,
        target_user_id=unlocked_user_id,
        broadcast_to_admins=True,
    )


async def publish_snapshot_created(
    redis: Redis,
    tables: list[str],
    total_records: int,
    snapshot_date: str,
    triggered_by_user_id: Optional[int] = None,
) -> dict:
    """
    Publikuje event zakończenia tworzenia snapshotu bazy.

    Args:
        redis:                 Klient Redis.
        tables:                Lista nazw tabel które były snapshotowane.
        total_records:         Łączna liczba zarchiwizowanych rekordów.
        snapshot_date:         Data snapshotu (YYYY-MM-DD).
        triggered_by_user_id:  ID użytkownika który zlecił snapshot.

    Returns:
        Metadane operacji publish.
    """
    data: dict = {
        "tables":        tables,
        "tables_count":  len(tables),
        "total_records": total_records,
        "snapshot_date": snapshot_date,
        "message":       f"Snapshot bazy danych zakończony — {len(tables)} tabel, {total_records} rekordów.",
    }
    return await publish(
        redis=redis,
        event_type="snapshot_created",
        data=data,
        user_id=triggered_by_user_id,
        target_user_id=triggered_by_user_id,
        broadcast_to_admins=True,
    )


async def publish_schema_tamper_detected(
    redis: Redis,
    object_name: str,
    object_type: str,
    expected_checksum: int,
    actual_checksum: int,
) -> dict:
    """
    Publikuje krytyczny alert o naruszeniu integralności schematu bazy.

    POZIOM: CRITICAL — wymaga natychmiastowej reakcji DBA.
    Wywoływany przez core/schema_integrity.py przed SystemExit(1).

    Args:
        redis:             Klient Redis.
        object_name:       Nazwa obiektu bazy (widok/procedura).
        object_type:       Typ obiektu (VIEW/PROCEDURE).
        expected_checksum: Oczekiwany checksum z SchemaChecksums.
        actual_checksum:   Faktyczny checksum z sys.sql_modules.

    Returns:
        Metadane operacji publish.
    """
    data: dict = {
        "object_name":      object_name,
        "object_type":      object_type,
        "expected_checksum": expected_checksum,
        "actual_checksum":  actual_checksum,
        "message": (
            f"⛔ KRYTYCZNY ALERT: Naruszenie integralności schematu! "
            f"Obiekt '{object_name}' ({object_type}) został zmodyfikowany "
            f"poza migracją Alembic. Aplikacja zostanie zatrzymana."
        ),
        "level": "CRITICAL",
    }
    return await publish(
        redis=redis,
        event_type="schema_tamper_detected",
        data=data,
        user_id=None,
        target_user_id=None,
        broadcast_to_admins=True,
    )


# ===========================================================================
# Diagnostyka
# ===========================================================================

async def get_channel_info(redis: Redis) -> dict:
    """
    Zwraca informacje o aktywnych kanałach Redis Pub/Sub.

    Args:
        redis: Klient Redis.

    Returns:
        Słownik z informacjami o kanałach.
    """
    try:
        channels_raw = await redis.pubsub_channels("channel:*")
        channels = [
            c.decode("utf-8") if isinstance(c, bytes) else c
            for c in channels_raw
        ]

        # Liczba subskrybentów per kanał
        numsub: dict[str, int] = {}
        if channels:
            numsub_raw = await redis.pubsub_numsub(*channels)
            # pubsub_numsub zwraca listę par [(channel, count), ...]
            for i in range(0, len(numsub_raw), 2):
                ch = numsub_raw[i]
                cnt = numsub_raw[i + 1]
                ch_str = ch.decode("utf-8") if isinstance(ch, bytes) else str(ch)
                numsub[ch_str] = int(cnt)

        return {
            "active_channels": channels,
            "channel_count":   len(channels),
            "subscribers":     numsub,
            "timestamp":       datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        logger.warning(
            "Nie udało się pobrać informacji o kanałach Redis",
            extra={"error": str(exc)}
        )
        return {
            "active_channels": [],
            "channel_count":   0,
            "subscribers":     {},
            "error":           str(exc),
            "timestamp":       datetime.now(timezone.utc).isoformat(),
        }


def get_events_log_stats() -> dict:
    """
    Zwraca statystyki pliku logów eventów (bieżący dzień).

    Returns:
        Słownik ze statystykami (liczba linii, rozmiar pliku).
    """
    log_file = _get_events_log_file()
    if not log_file.exists():
        return {
            "file": str(log_file),
            "exists": False,
            "lines": 0,
            "size_bytes": 0,
        }
    try:
        size = log_file.stat().st_size
        with log_file.open("rb") as f:
            lines = sum(1 for _ in f)
        return {
            "file":       str(log_file),
            "exists":     True,
            "lines":      lines,
            "size_bytes": size,
            "size_kb":    round(size / 1024, 2),
        }
    except OSError as exc:
        return {
            "file":  str(log_file),
            "error": str(exc),
        }