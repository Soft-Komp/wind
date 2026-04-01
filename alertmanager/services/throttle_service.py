# =============================================================================
# alertmanager/services/throttle_service.py
# System Windykacja — Alert Manager — Throttling alertów
#
# Mechanizm: DWIE warstwy ochrony przed spamem:
#
# 1. COOLDOWN — klucz Redis z TTL:
#    alert:cooldown:{alert_type}   TTL = cooldown_minutes * 60
#    Jeśli klucz istnieje → email już wysłano → pomiń.
#
# 2. STATE TRACKING — śledzenie przejść FIRING → OK:
#    alert:state:{alert_type}      JSON bez TTL
#    Używany do wysyłki RECOVERY EMAIL gdy problem ustąpi.
#
# Przykładowy przepływ:
#   T+0:   DB down → cooldown KEY ustawiony (TTL=15min) → email wysłany
#   T+1:   DB down → cooldown KEY istnieje → BRAK emaila (throttle)
#   T+14:  DB down → cooldown KEY istnieje → BRAK emaila
#   T+16:  DB down → cooldown KEY wygasł → email wysłany ponownie
#   T+20:  DB OK   → state KEY był "firing" → RECOVERY EMAIL → state KEY usunięty
# =============================================================================

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import orjson

from models.alert import AlertState, CheckResult

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = logging.getLogger("alertmanager.services.throttle")

# Prefiksy kluczy Redis
_PREFIX_COOLDOWN = "alert:cooldown:"
_PREFIX_STATE = "alert:state:"


class ThrottleService:
    """
    Serwis throttlingu alertów oparty na Redis.

    Zapobiega spamowaniu emailem gdy system jest w stanie ciągłej awarii.
    Jednocześnie śledzi stan aby wysłać powiadomienie o odzyskaniu.
    """

    def __init__(self, redis_client: "aioredis.Redis") -> None:
        self._redis = redis_client

    # -----------------------------------------------------------------------
    # GŁÓWNE METODY
    # -----------------------------------------------------------------------

    def _alert_key(prefix: str, alert_type) -> str:
        """Normalizuje alert_type do stringa niezależnie od typu."""
        val = alert_type.value if hasattr(alert_type, 'value') else str(alert_type)
        return f"{prefix}{val}"

    async def should_send_alert(
        self,
        result: CheckResult,
        cooldown_minutes: int,
    ) -> bool:
        """
        Sprawdź czy email powinien być wysłany dla tego alertu.

        Returns:
            True  = wyślij email (pierwszy raz lub po upływie cooldown)
            False = pomiń (cooldown aktywny)
        """
        cooldown_key = f"{_PREFIX_COOLDOWN}{result.alert_type if isinstance(result.alert_type, str) else result.alert_type.value}"

        exists = await self._redis.exists(cooldown_key)

        if exists:
            ttl = await self._redis.ttl(cooldown_key)
            logger.info(
                "Throttle: BLOKADA dla '%s' — cooldown aktywny, pozostało %ds",
                result.alert_type,
                ttl,
                extra={
                    "alert_type": result.alert_type,
                    "cooldown_remaining_seconds": ttl,
                    "incident_id": result.incident_id,
                }
            )
            return False

        logger.info(
            "Throttle: PRZEPUŚĆ dla '%s' — brak aktywnego cooldown",
            result.alert_type,
            extra={"alert_type": result.alert_type, "incident_id": result.incident_id}
        )
        return True

    async def register_alert_sent(
        self,
        result: CheckResult,
        cooldown_minutes: int,
    ) -> None:
        """
        Zarejestruj że alert został wysłany — ustaw cooldown i zaktualizuj state.
        Wywołaj PO udanym wysłaniu emaila.
        """
        now = datetime.now(timezone.utc)
        cooldown_key = f"{_PREFIX_COOLDOWN}{result.alert_type if isinstance(result.alert_type, str) else result.alert_type.value}"
        state_key = f"{_PREFIX_STATE}{result.alert_type}"

        # 1. Ustaw cooldown z TTL
        cooldown_ttl_seconds = cooldown_minutes * 60
        await self._redis.setex(
            cooldown_key,
            cooldown_ttl_seconds,
            result.incident_id,
        )

        # 2. Załaduj lub stwórz state
        existing_state_json = await self._redis.get(state_key)
        if existing_state_json:
            try:
                existing_state = AlertState.from_json(existing_state_json)
                fire_count = existing_state.fire_count + 1
                first_fired_at = existing_state.first_fired_at
            except Exception:
                fire_count = 1
                first_fired_at = now
        else:
            fire_count = 1
            first_fired_at = now

        new_state = AlertState(
            alert_type=result.alert_type,
            is_firing=True,
            first_fired_at=first_fired_at,
            last_fired_at=now,
            fire_count=fire_count,
            last_incident_id=result.incident_id,
        )
        await self._redis.set(state_key, new_state.to_json())

        logger.info(
            "Throttle: cooldown ustawiony dla '%s' na %ds (email #%d)",
            result.alert_type,
            cooldown_ttl_seconds,
            fire_count,
            extra={
                "alert_type": result.alert_type,
                "cooldown_seconds": cooldown_ttl_seconds,
                "fire_count": fire_count,
                "first_fired_at": first_fired_at.isoformat(),
                "incident_id": result.incident_id,
            }
        )

    async def check_recovery(
        self,
        result: CheckResult,
    ) -> AlertState | None:
        """
        Sprawdź czy alert przeszedł ze stanu FIRING do OK (recovery).

        Returns:
            AlertState jeśli był firing i teraz jest OK (wyślij recovery email)
            None jeśli nie było poprzedniego alertu lub już jest OK
        """
        if not result.is_ok:
            return None

        state_key = f"{_PREFIX_STATE}{result.alert_type}"
        state_json = await self._redis.get(state_key)

        if not state_json:
            return None  # Nigdy nie było alertu — brak recovery

        try:
            state = AlertState.from_json(state_json)
        except Exception as exc:
            logger.warning(
                "Throttle: błąd parsowania state dla '%s': %s",
                result.alert_type, exc
            )
            await self._redis.delete(state_key)
            return None

        if not state.is_firing:
            return None  # Już był w stanie OK — brak recovery

        logger.info(
            "Throttle: RECOVERY wykryte dla '%s' "
            "(był firing od %s, %d alertów)",
            result.alert_type,
            state.first_fired_at.isoformat(),
            state.fire_count,
            extra={
                "alert_type": result.alert_type,
                "first_fired_at": state.first_fired_at.isoformat(),
                "fire_count": state.fire_count,
                "incident_id": result.incident_id,
            }
        )
        return state

    async def register_recovery(self, result: CheckResult) -> None:
        """
        Zarejestruj recovery — wyczyść state i cooldown.
        Wywołaj PO wysłaniu recovery emaila.
        """
        state_key = f"{_PREFIX_STATE}{result.alert_type}"
        cooldown_key = f"{_PREFIX_COOLDOWN}{result.alert_type if isinstance(result.alert_type, str) else result.alert_type.value}"

        # Zaktualizuj state na is_firing=False (zachowaj historię)
        state_json = await self._redis.get(state_key)
        if state_json:
            try:
                state = AlertState.from_json(state_json)
                state.is_firing = False
                await self._redis.set(state_key, state.to_json())
            except Exception:
                await self._redis.delete(state_key)

        # Usuń cooldown — recovery = reset
        await self._redis.delete(cooldown_key)

        logger.info(
            "Throttle: recovery zarejestrowane dla '%s' — cooldown i state wyczyszczone",
            result.alert_type,
        )

    # -----------------------------------------------------------------------
    # DIAGNOSTYKA
    # -----------------------------------------------------------------------

    async def get_all_states(self) -> list[dict]:
        """Zwraca wszystkie aktywne stany alertów — do logów cyklicznych."""
        states = []
        async for key in self._redis.scan_iter(
            match=f"{_PREFIX_STATE}*", count=50
        ):
            try:
                val = await self._redis.get(key)
                if val:
                    state = AlertState.from_json(val)
                    cooldown_key = f"{_PREFIX_COOLDOWN}{state.alert_type}"
                    cooldown_ttl = await self._redis.ttl(cooldown_key)
                    states.append({
                        **orjson.loads(val),
                        "cooldown_remaining_seconds": max(0, cooldown_ttl),
                    })
            except Exception as exc:
                logger.debug("Błąd odczytu state %s: %s", key, exc)
        return states