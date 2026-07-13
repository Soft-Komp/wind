"""watchdog/services/kill_switch.py — zarządzanie ApplicationEnabled."""
from __future__ import annotations

import logging
import pyodbc
import redis

logger = logging.getLogger("watchdog.kill_switch")


def ustaw_stan_aplikacji(conn: pyodbc.Connection, redis_client: redis.Redis, wlaczona: bool) -> None:
    cur = conn.cursor()
    cur.execute(
        "UPDATE dbo.skw_SystemConfig SET ConfigValue = ? WHERE ConfigKey = 'ApplicationEnabled'",
        "1" if wlaczona else "0",
    )
    conn.commit()

    # Redis — natychmiastowy efekt, bez czekania na wygaśnięcie cache
    redis_client.set("app:enabled", "1" if wlaczona else "0")

    logger.warning(
        "watchdog: zmieniono globalny stan aplikacji na %s",
        "WŁĄCZONA" if wlaczona else "ZABLOKOWANA",
        extra={"event": "watchdog.kill_switch", "wlaczona": wlaczona},
    )