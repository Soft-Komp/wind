# =============================================================================
# alertmanager/checkers/watchdog_pubsub.py
# System Windykacja — Alert Manager — Watchdog Redis Pub/Sub Listener
#
# ARCHITEKTURA:
#   - integrity_watchdog.py (kontener API) wykrywa tamper → publikuje do Redis
#   - WatchdogPubSubListener (kontener alertmanager) nasłuchuje → wysyła email
#
# To NIE jest BaseChecker — działa jako osobna coroutine (subscribe loop),
# nie jako periodyczne sprawdzenie. Tamper musi wywołać NATYCHMIASTOWY alert.
#
# Kanał Redis: channel:system:watchdog_tamper
# Format wiadomości: JSON (patrz _parse_tamper_message)
# =============================================================================

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Coroutine

import orjson

from models.alert import (
    AlertLevel,
    AlertType,
    CheckResult,
    CheckStatus,
    RuntimeConfig,
)

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = logging.getLogger("alertmanager.checkers.watchdog_pubsub")


# Typ callbacku wywoływanego gdy przyjdzie wiadomość tamper
AlertCallback = Callable[[CheckResult], Coroutine[Any, Any, None]]


class WatchdogPubSubListener:
    """
    Nasłuchuje na Redis pub/sub i generuje CheckResult przy wykryciu tamperu.

    Uruchamiany jako osobna asyncio Task — działa równolegle z główną pętlą checkerów.

    Mechanizm:
        1. Subskrybuje kanał channel:system:watchdog_tamper
        2. Przy odebraniu wiadomości → parsuje JSON → tworzy CheckResult
        3. Wywołuje callback (on_alert) — ten sam mechanizm co checkers
        4. Jeśli połączenie Redis zerwane → reconnect z exponential backoff

    Integracja z integrity_watchdog.py:
        W integrity_watchdog.py dodaj po wykryciu tamperu:
        ```python
        await redis.publish(
            "channel:system:watchdog_tamper",
            orjson.dumps({
                "incident_id": incident_id,
                "mismatches": mismatches_dicts,
                "detected_at": datetime.now(timezone.utc).isoformat(),
                "cycle_number": cycle_number,
            }).decode()
        )
        ```
    """

    def __init__(
        self,
        settings: Any,
        redis_client: "aioredis.Redis",
        on_alert: AlertCallback,
        runtime_config: RuntimeConfig,
    ) -> None:
        self._settings = settings
        self._redis = redis_client
        self._on_alert = on_alert
        self._runtime_config = runtime_config
        self._running = False
        self._reconnect_delay = 1.0       # sekundy — backoff startowy
        self._max_reconnect_delay = 60.0   # max backoff
        self._messages_received = 0
        self._last_message_at: datetime | None = None

    async def run(self) -> None:
        """
        Główna pętla nasłuchiwania — uruchom jako asyncio.create_task().
        Działa do momentu wywołania stop().
        """
        self._running = True
        channel = self._settings.redis_channel_watchdog_tamper
        logger.info(
            "WatchdogPubSubListener START — subskrybuje kanał: %s", channel
        )

        while self._running:
            pubsub = None
            try:
                pubsub = self._redis.pubsub()
                await pubsub.subscribe(channel)
                logger.info(
                    "WatchdogPubSubListener: subskrypcja aktywna na: %s", channel
                )

                # Reset backoff po udanym połączeniu
                self._reconnect_delay = 1.0

                async for raw_message in pubsub.listen():
                    if not self._running:
                        break

                    # Filtruj wiadomości systemowe (subscribe confirmation)
                    if raw_message["type"] != "message":
                        continue

                    self._messages_received += 1
                    self._last_message_at = datetime.now(timezone.utc)

                    logger.warning(
                        "WatchdogPubSubListener: otrzymano wiadomość #%d na kanale %s",
                        self._messages_received,
                        channel,
                        extra={
                            "channel": channel,
                            "message_number": self._messages_received,
                            "ts": self._last_message_at.isoformat(),
                        }
                    )

                    result = self._parse_tamper_message(raw_message["data"])

                    # Wywołaj callback — alert manager wyśle email + zapisze log
                    try:
                        await self._on_alert(result)
                    except Exception as exc:
                        logger.critical(
                            "WatchdogPubSubListener: błąd w on_alert callback: %s",
                            exc,
                            exc_info=True,
                        )

            except asyncio.CancelledError:
                logger.info("WatchdogPubSubListener: CancelledError — zatrzymuję")
                break

            except Exception as exc:
                logger.error(
                    "WatchdogPubSubListener: błąd pub/sub — reconnect za %.1fs: %s",
                    self._reconnect_delay,
                    exc,
                    exc_info=True,
                )
                await asyncio.sleep(self._reconnect_delay)
                # Exponential backoff
                self._reconnect_delay = min(
                    self._reconnect_delay * 2,
                    self._max_reconnect_delay,
                )

            finally:
                if pubsub:
                    try:
                        await pubsub.unsubscribe()
                        await pubsub.aclose()
                    except Exception:
                        pass

        logger.info("WatchdogPubSubListener: zatrzymany")

    async def stop(self) -> None:
        """Zatrzymaj nasłuchiwanie."""
        self._running = False

    def get_stats(self) -> dict[str, Any]:
        """Statystyki do logowania/monitoringu."""
        return {
            "running": self._running,
            "messages_received": self._messages_received,
            "last_message_at": (
                self._last_message_at.isoformat()
                if self._last_message_at else None
            ),
            "channel": self._settings.redis_channel_watchdog_tamper,
        }

    def _parse_tamper_message(self, data: str | bytes) -> CheckResult:
        """
        Parsuje wiadomość JSON z integrity_watchdog.py.

        Format oczekiwany:
        {
            "incident_id": "uuid",
            "mismatches": [...],
            "detected_at": "2026-01-01T12:00:00+00:00",
            "cycle_number": 42
        }

        Jeśli parsowanie się nie uda — zwraca CheckResult z surową wiadomością.
        """
        now = datetime.now(timezone.utc)

        try:
            payload: dict[str, Any] = orjson.loads(data)
        except (orjson.JSONDecodeError, TypeError, ValueError):
            # Wiadomość nie jest poprawnym JSON — traktuj jako prosty string
            logger.warning(
                "WatchdogPubSubListener: niepoprawny JSON w wiadomości: %s",
                str(data)[:200],
            )
            payload = {"raw_message": str(data)[:1000]}

        incident_id = payload.get("incident_id", "unknown")
        mismatches = payload.get("mismatches", [])
        cycle_number = payload.get("cycle_number", "?")

        # Zbuduj czytelny opis niezgodności
        mismatch_descriptions = []
        for m in mismatches[:5]:  # max 5 w opisie
            obj_name = m.get("object_name", "?")
            obj_type = m.get("object_type", "?")
            mismatch_descriptions.append(f"{obj_type} '{obj_name}'")

        mismatch_str = (
            ", ".join(mismatch_descriptions)
            + ("..." if len(mismatches) > 5 else "")
            if mismatch_descriptions
            else "szczegóły w logach"
        )

        return CheckResult(
            incident_id=incident_id,
            alert_type=AlertType.SCHEMA_TAMPER,
            status=CheckStatus.CRITICAL,
            level=AlertLevel.CRITICAL,
            title="🚨 CRITICAL: SCHEMA TAMPER DETECTED — Wykryto nieautoryzowaną zmianę bazy!",
            message=(
                f"Watchdog schematu wykrył {len(mismatches)} niezgodność/i w cyklu #{cycle_number}. "
                f"Zmienione obiekty: {mismatch_str}. "
                "System może być skompromitowany. Natychmiast skontaktuj się z administratorem!"
            ),
            details={
                "incident_id": incident_id,
                "cycle_number": cycle_number,
                "mismatches_count": len(mismatches),
                "mismatches": mismatches[:10],  # max 10 w logu
                "detected_at": payload.get("detected_at", now.isoformat()),
                "source": "watchdog_pubsub",
            },
            checked_at=now,
            duration_ms=0.0,
            checker_name="WatchdogPubSubListener",
        )