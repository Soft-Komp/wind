"""watchdog/main.py — główna pętla Watchdoga."""
from __future__ import annotations

import asyncio
import logging
import time
import traceback

import pyodbc
import redis

from watchdog.config import get_settings
from watchdog.services.checks import (
    pobierz_kolekcje, sprawdz_kolekcje_env, sprawdz_kolekcje_db, zapisz_wynik,
)
from watchdog.services.kill_switch import ustaw_stan_aplikacji
from watchdog.services.smtp_service import wyslij_alert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("watchdog.main")


async def cykl_sprawdzenia(settings) -> None:
    t0 = time.monotonic()
    try:
        conn = pyodbc.connect(settings.db_connection_string, timeout=10)
        redis_client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            password=settings.REDIS_PASSWORD.get_secret_value() if settings.REDIS_PASSWORD else None,
            db=settings.REDIS_DB,
            decode_responses=True,
        )
    except Exception as exc:
        # ── FAIL-OPEN (decyzja) — chwilowa awaria Watchdoga NIE blokuje produkcji ──
        logger.error(
            "watchdog: brak połączenia z DB/Redis — FAIL-OPEN, aplikacja pozostaje WŁĄCZONA",
            extra={"event": "watchdog.blad_polaczenia", "error": str(exc)},
            exc_info=True,
        )
        wyslij_alert(
            "[Watchdog] Brak połączenia z bazą/Redis",
            f"<p>Watchdog nie mógł połączyć się z infrastrukturą: {exc}</p>"
            f"<p>Zgodnie z polityką fail-open, aplikacja POZOSTAJE włączona.</p>",
        )
        return

    try:
        kolekcje = pobierz_kolekcje(conn)
        for kolekcja in kolekcje:
            if kolekcja.typ == "env":
                wynik = sprawdz_kolekcje_env(kolekcja)
            else:
                wynik = sprawdz_kolekcje_db(conn, kolekcja)

            zapisz_wynik(conn, wynik, settings.WATCHDOG_INSTANCE_ID)

            if wynik.wynik == "zablokowano":
                ustaw_stan_aplikacji(conn, redis_client, wlaczona=False)
                wyslij_alert(
                    f"[Watchdog] Zablokowano aplikację — kolekcja '{kolekcja.nazwa}'",
                    f"<pre>{wynik.szczegoly}</pre>",
                )
            elif wynik.wynik == "naprawiono":
                wyslij_alert(
                    f"[Watchdog] Automatycznie naprawiono kolekcję '{kolekcja.nazwa}'",
                    f"<pre>{wynik.szczegoly}</pre>",
                )
    finally:
        conn.close()
        redis_client.close()
        logger.info(
            "watchdog: cykl zakończony",
            extra={"event": "watchdog.cykl_zakonczony", "czas_trwania_ms": round((time.monotonic() - t0) * 1000, 1)},
        )


async def main() -> None:
    settings = get_settings()
    logger.info(
        "Watchdog wystartował",
        extra={"instance_id": settings.WATCHDOG_INSTANCE_ID, "interval_s": settings.WATCHDOG_CHECK_INTERVAL_SECONDS},
    )
    while True:
        try:
            await cykl_sprawdzenia(settings)
        except Exception:
            logger.critical("watchdog: nieoczekiwany błąd pętli głównej — kontynuuję", exc_info=True)
        await asyncio.sleep(settings.WATCHDOG_CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())