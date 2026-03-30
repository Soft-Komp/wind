# =============================================================================
# backend/app/core/integrity_watchdog.py
# =============================================================================
# Okresowa weryfikacja integralności schematu bazy danych.
#
# Uzupełnienie dla schema_integrity.verify() — które działa TYLKO przy starcie.
# Watchdog uruchamia się jako asyncio background task w FastAPI lifespan
# i co WATCHDOG_INTERVAL_SECONDS wykonuje pełną weryfikację checksumów.
#
# Reakcja na niezgodność (runtime):
#   1. Zapis incydentu do pliku logs/incidents/
#   2. Log CRITICAL (JSON Lines do pliku + stderr)
#   3. SSE broadcast do channel:admins (Redis PubSub)
#   4. os._exit(1) → Docker --restart=always uruchamia kontener od nowa
#      → przy restarcie lifespan.startup wywołuje verify() z reaction=BLOCK
#      → jeśli widok nadal zmodyfikowany → kontener NIE startuje
#
# Zasada: watchdog NIE naprawia — wykrywa i wymusza restart.
# Naprawa = DBA przywraca widok + aktualizuje SchemaChecksums przez Alembic.
#
# Konfiguracja (SystemConfig):
#   integrity_watchdog.enabled          → '1' | '0'  (default: '1')
#   integrity_watchdog.interval_seconds → int        (default: 900 = 15 min)
#   integrity_watchdog.grace_period_s   → int        (default: 60)
#       Czas od startu aplikacji przed pierwszym sprawdzeniem.
#       Zapobiega false-positive przy zimnym starcie (DB warmup).
#
# Autor:  System Windykacja
# Wersja: 1.0.0
# Data:   2026-03-25
# =============================================================================
from __future__ import annotations

import asyncio
import json
import logging
import os
import platform
import socket
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

# ---------------------------------------------------------------------------
# Logger — dedykowany dla watchdoga, łatwy do grep'owania w logach
# ---------------------------------------------------------------------------
logger = logging.getLogger("app.integrity_watchdog")

# ---------------------------------------------------------------------------
# Stałe domyślne — nadpisywane przez SystemConfig przy każdej iteracji
# ---------------------------------------------------------------------------
_DEFAULT_INTERVAL_S: int = 900        # 15 minut
_DEFAULT_GRACE_S: int = 60            # 1 minuta grace period po starcie
_DEFAULT_ENABLED: bool = True

# ---------------------------------------------------------------------------
# Flaga globalna — ustawiana przez lifespan shutdown aby watchdog wiedział
# że ma zakończyć pętlę zamiast crashować przy zamykaniu kontenera
# ---------------------------------------------------------------------------
_shutdown_requested: bool = False


def request_shutdown() -> None:
    """Sygnalizuje watchdogowi że trwa graceful shutdown — NIE crashuj."""
    global _shutdown_requested
    _shutdown_requested = True
    logger.info("Watchdog: otrzymano sygnał shutdown — pętla zostanie zatrzymana")


# =============================================================================
# Pobieranie konfiguracji z SystemConfig
# =============================================================================

async def _get_watchdog_config(db: "AsyncSession") -> dict[str, Any]:
    """
    Pobiera konfigurację watchdoga z tabeli SystemConfig.

    Zwraca słownik z kluczami:
        enabled          bool
        interval_s       int
        grace_period_s   int

    Przy błędzie DB → zwraca bezpieczne wartości domyślne.
    Nigdy nie rzuca wyjątku.
    """
    config: dict[str, Any] = {
        "enabled": _DEFAULT_ENABLED,
        "interval_s": _DEFAULT_INTERVAL_S,
        "grace_period_s": _DEFAULT_GRACE_S,
    }

    try:
        from sqlalchemy import text
        result = await db.execute(
            text("""
                SELECT ConfigKey, ConfigValue
                FROM   dbo_ext.skw_SystemConfig
                WHERE  ConfigKey IN (
                    'integrity_watchdog.enabled',
                    'integrity_watchdog.interval_seconds',
                    'integrity_watchdog.grace_period_s'
                )
                  AND IsActive = 1
            """)
        )
        rows = result.fetchall()

        for row in rows:
            key: str = row.ConfigKey
            val: str = row.ConfigValue

            if key == "integrity_watchdog.enabled":
                config["enabled"] = val.strip() not in ("0", "false", "False", "no")

            elif key == "integrity_watchdog.interval_seconds":
                try:
                    parsed = int(val)
                    # Minimalne 60 sekund — ochrona przed zbyt częstym odpytywaniem
                    config["interval_s"] = max(60, parsed)
                except ValueError:
                    logger.warning(
                        "Watchdog: nieprawidłowa wartość interval_seconds='%s' "
                        "— używam domyślnego %d s",
                        val, _DEFAULT_INTERVAL_S,
                    )

            elif key == "integrity_watchdog.grace_period_s":
                try:
                    parsed = int(val)
                    config["grace_period_s"] = max(0, parsed)
                except ValueError:
                    logger.warning(
                        "Watchdog: nieprawidłowa wartość grace_period_s='%s' "
                        "— używam domyślnego %d s",
                        val, _DEFAULT_GRACE_S,
                    )

    except Exception as exc:
        logger.warning(
            "Watchdog: błąd pobierania konfiguracji z SystemConfig (%s) "
            "— używam wartości domyślnych: enabled=%s interval=%ds grace=%ds",
            exc,
            config["enabled"],
            config["interval_s"],
            config["grace_period_s"],
        )

    return config


# =============================================================================
# Logowanie incydentu do pliku
# =============================================================================

def _get_log_dir() -> Path:
    """Zwraca katalog logów — tworzy jeśli nie istnieje."""
    base = Path(os.environ.get("LOG_DIR", "logs"))
    incidents_dir = base / "incidents"
    incidents_dir.mkdir(parents=True, exist_ok=True)
    return base


def _write_watchdog_incident(
    incident_id: str,
    mismatches: list[dict[str, Any]],
    extra_context: dict[str, Any],
) -> Path:
    """
    Zapisuje pełny raport incydentu do pliku JSON.

    Format: logs/incidents/watchdog_YYYY-MM-DD_<incident_id>.json
    Plik nigdy nie jest nadpisywany — każdy incydent ma unikalny ID.

    Returns:
        Path do zapisanego pliku.
    """
    log_dir = _get_log_dir()
    now_utc = datetime.now(timezone.utc)
    date_str = now_utc.strftime("%Y-%m-%d")
    filename = f"watchdog_{date_str}_{incident_id}.json"
    incident_path = log_dir / "incidents" / filename

    payload: dict[str, Any] = {
        "incident_id": incident_id,
        "incident_type": "SCHEMA_TAMPER_DETECTED_RUNTIME",
        "detected_at": now_utc.isoformat(),
        "detected_by": "integrity_watchdog",
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "mismatch_count": len(mismatches),
        "mismatches": mismatches,
        "action_taken": "SYSTEM_EXIT_1",
        "action_reason": (
            "Zmiana definicji widoku/procedury wykryta w trakcie działania aplikacji. "
            "os._exit(1) → Docker restart → verify() przy ponownym starcie "
            "zablokuje start jeśli widok nadal niezgodny."
        ),
        **extra_context,
    }

    try:
        incident_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        logger.info(
            "Watchdog: raport incydentu zapisany → %s", incident_path,
            extra={"incident_id": incident_id, "path": str(incident_path)},
        )
    except OSError as exc:
        # Nie możemy zapisać pliku — logujemy błąd ale NIE przerywamy procesu
        # (crashujemy zaraz i tak przez SystemExit)
        logger.error(
            "Watchdog: nie można zapisać raportu incydentu (%s) — kontynuuję exit",
            exc,
            extra={"incident_id": incident_id},
        )

    return incident_path


# =============================================================================
# SSE broadcast
# =============================================================================

async def _broadcast_sse_tamper_alert(
    incident_id: str,
    mismatches: list[dict[str, Any]],
) -> None:
    """
    Wysyła alert przez Redis PubSub do channel:admins.

    Zalogowani admini zobaczą natychmiastowe powiadomienie w UI
    zanim kontener się zrestartuje.

    Błąd Redis NIE blokuje procesu wyjścia.
    """
    try:
        import aioredis  # type: ignore[import]
        from app.core.config import get_settings
        settings = get_settings()

        redis = await aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
        )

        event_payload = json.dumps(
            {
                "event": "schema_tamper_detected",
                "data": {
                    "message": (
                        "🚨 KRYTYCZNE: Wykryto nieautoryzowaną zmianę "
                        "definicji obiektu bazodanowego podczas działania "
                        "aplikacji. Kontener zostanie zrestartowany."
                    ),
                    "level": "critical",
                    "incident_id": incident_id,
                    "mismatch_count": len(mismatches),
                    "mismatches": mismatches,
                    "detected_by": "integrity_watchdog",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "action": "CONTAINER_RESTART_IMMINENT",
                },
            },
            ensure_ascii=False,
            default=str,
        )

        await redis.publish("channel:admins", event_payload)
        await redis.aclose()

        logger.info(
            "Watchdog: SSE alert wysłany do Redis channel:admins",
            extra={"incident_id": incident_id},
        )

    except Exception as exc:
        logger.error(
            "Watchdog: błąd wysyłania SSE alert (%s) — kontynuuję exit",
            exc,
            extra={
                "incident_id": incident_id,
                "traceback": traceback.format_exc(),
            },
        )


# =============================================================================
# Jedna iteracja watchdoga
# =============================================================================

async def _run_single_check(db: "AsyncSession", cycle_number: int) -> bool:
    """
    Wykonuje jedną pełną weryfikację checksumów.

    Args:
        db:            SQLAlchemy async session
        cycle_number:  Numer cyklu (do logów)

    Returns:
        True  — wszystkie checksums OK
        False — wykryto niezgodności (NIE zwróci False bo wywołuje os._exit)

    Raises:
        Nic — wszelkie wyjątki są łapane i logowane.
        os._exit(1) przy wykrytej niezgodności.
    """
    check_start = datetime.now(timezone.utc)
    incident_id = str(uuid.uuid4())

    logger.info(
        "Watchdog: START cyklu #%d [incident_id=%s]",
        cycle_number, incident_id,
        extra={
            "cycle_number": cycle_number,
            "incident_id": incident_id,
            "check_started_at": check_start.isoformat(),
        },
    )

    try:
        # Import lokalny — unikamy circular imports
        from app.core.schema_integrity import _run_full_verification

        # runtime_check=True → NIE wywołuje SystemExit wewnątrz verify()
        # Kontrolę przejmujemy sami — możemy najpierw zalogować + SSE
        result = await _run_full_verification(db, runtime_check=True)

        check_end = datetime.now(timezone.utc)
        duration_ms = int((check_end - check_start).total_seconds() * 1000)

        if result.error:
            # Błąd infrastruktury (np. DB niedostępna) — NIE traktuj jako tamper
            # Watchdog nie może crashować przy chwilowych problemach z połączeniem
            logger.warning(
                "Watchdog: błąd weryfikacji w cyklu #%d (%s) "
                "— pomijam (nie jest to dowód na tamper)",
                cycle_number, result.error,
                extra={
                    "cycle_number": cycle_number,
                    "incident_id": incident_id,
                    "error": result.error,
                    "duration_ms": duration_ms,
                },
            )
            return True  # Zakładamy OK przy błędzie infrastruktury

        mismatch_count = len(result.mismatches)

        logger.info(
            "Watchdog: cykl #%d zakończony — %d obiektów OK, %d niezgodności "
            "[%d ms]",
            cycle_number,
            result.verified_ok,
            mismatch_count,
            duration_ms,
            extra={
                "cycle_number": cycle_number,
                "incident_id": incident_id,
                "verified_ok": result.verified_ok,
                "mismatch_count": mismatch_count,
                "total_live_objects": result.total_live_objects,
                "duration_ms": duration_ms,
                "check_started_at": check_start.isoformat(),
                "check_ended_at": check_end.isoformat(),
            },
        )

        if mismatch_count == 0:
            return True

        # ─────────────────────────────────────────────────────────────────────
        # MISMATCH WYKRYTY — sekwencja awaryjnego wyjścia
        # ─────────────────────────────────────────────────────────────────────
        mismatches_dicts = [m.to_dict() for m in result.mismatches]

        logger.critical(
            "Watchdog: !!!! SCHEMA TAMPER DETECTED RUNTIME !!!! "
            "cykl=#%d | niezgodności=%d | obiekty=%s | incident_id=%s",
            cycle_number,
            mismatch_count,
            [m.get("object_name") for m in mismatches_dicts],
            incident_id,
            extra={
                "cycle_number": cycle_number,
                "incident_id": incident_id,
                "mismatch_count": mismatch_count,
                "mismatches": mismatches_dicts,
                "duration_ms": duration_ms,
                "action": "INITIATING_SYSTEM_EXIT",
            },
        )

        # Krok 1: Zapisz raport incydentu do pliku
        incident_path = _write_watchdog_incident(
            incident_id=incident_id,
            mismatches=mismatches_dicts,
            extra_context={
                "cycle_number": cycle_number,
                "verified_ok": result.verified_ok,
                "total_live_objects": result.total_live_objects,
                "duration_ms": duration_ms,
                "verification_id": result.verification_id,
            },
        )

        # Krok 2: SSE broadcast — adminisi w UI zobaczą alert zanim kontener padnie
        await _broadcast_sse_tamper_alert(
            incident_id=incident_id,
            mismatches=mismatches_dicts,
        )

        # Krok 3: Krótka pauza — dajemy SSE czas na dotarcie do klientów
        logger.critical(
            "Watchdog: pauza 3s przed os._exit(1) "
            "— SSE powinien dotrzeć do adminów | incident=%s",
            incident_id,
        )
        await asyncio.sleep(3)

        # Krok 4: os._exit(1) — natychmiastowe zakończenie procesu
        # UWAGA: SystemExit(1) NIE działa w asyncio task (jest łapany przez event loop)
        # os._exit(1) jest jedynym sposobem na pewne zatrzymanie procesu
        logger.critical(
            "Watchdog: os._exit(1) — Docker --restart=always wznowi kontener. "
            "Raport: %s",
            incident_path,
        )
        os._exit(1)

    except asyncio.CancelledError:
        # Lifespan shutdown — normalne zakończenie tasków
        logger.info(
            "Watchdog: cykl #%d anulowany (CancelledError — shutdown)",
            cycle_number,
        )
        raise

    except Exception as exc:
        logger.error(
            "Watchdog: nieoczekiwany błąd w cyklu #%d: %s",
            cycle_number, exc,
            extra={
                "cycle_number": cycle_number,
                "incident_id": incident_id,
                "traceback": traceback.format_exc(),
            },
        )
        # Błąd wewnętrzny watchdoga NIE jest powodem do restartu
        return True


# =============================================================================
# Główna pętla watchdoga
# =============================================================================

async def run_watchdog_loop(db_factory: Any) -> None:
    """
    Główna pętla watchdoga — uruchamiana jako asyncio.create_task() w lifespan.

    Args:
        db_factory: Callable zwracający async context manager z AsyncSession.
                    Zwykle: app.db.session.get_async_session

    Pętla:
        1. Czeka grace_period_s przy starcie (DB warmup)
        2. Co interval_s wykonuje _run_single_check()
        3. Czyta konfigurację Z KAŻDEJ ITERACJI → zmiany w SystemConfig
           działają bez restartu
        4. Przy shutdown (CancelledError) kończy czysto
    """
    cycle: int = 0
    started_at = datetime.now(timezone.utc)

    logger.info(
        "Watchdog: uruchomiony [pid=%d host=%s]",
        os.getpid(), socket.gethostname(),
        extra={
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "started_at": started_at.isoformat(),
        },
    )

    try:
        # ── Grace period — czekamy aż DB/Redis w pełni zainicjalizowane ──────
        async with db_factory() as db:
            initial_config = await _get_watchdog_config(db)

        grace_s: int = initial_config["grace_period_s"]
        enabled: bool = initial_config["enabled"]

        if not enabled:
            logger.warning(
                "Watchdog: WYŁĄCZONY przez SystemConfig "
                "(integrity_watchdog.enabled=0) — pętla zakończona. "
                "NIE zalecane w środowisku produkcyjnym!",
            )
            return

        logger.info(
            "Watchdog: grace period %d s (czekam przed pierwszym sprawdzeniem)",
            grace_s,
            extra={"grace_period_s": grace_s},
        )
        await asyncio.sleep(grace_s)

        # ── Główna pętla ──────────────────────────────────────────────────────
        while not _shutdown_requested:
            cycle += 1

            # Pobierz aktualną konfigurację — może być zmieniona w SystemConfig
            try:
                async with db_factory() as db:
                    config = await _get_watchdog_config(db)
            except Exception as exc:
                logger.warning(
                    "Watchdog: błąd pobierania konfiguracji w cyklu #%d (%s) "
                    "— używam poprzednich ustawień",
                    cycle, exc,
                )
                config = initial_config

            interval_s: int = config["interval_s"]
            enabled = config["enabled"]

            if not enabled:
                logger.warning(
                    "Watchdog: wyłączony w cyklu #%d przez SystemConfig "
                    "— śpię %d s i sprawdzam ponownie",
                    cycle, interval_s,
                )
                await asyncio.sleep(interval_s)
                continue

            # Wykonaj weryfikację w osobnej sesji DB
            try:
                async with db_factory() as db:
                    await _run_single_check(db, cycle)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "Watchdog: błąd sesji DB w cyklu #%d: %s",
                    cycle, exc,
                    extra={"traceback": traceback.format_exc()},
                )

            # Loguj kiedy będzie następne sprawdzenie
            next_check_at = datetime.now(timezone.utc).timestamp() + interval_s
            logger.info(
                "Watchdog: cykl #%d zakończony — następne sprawdzenie za %d s "
                "(ok. %s UTC)",
                cycle,
                interval_s,
                datetime.fromtimestamp(next_check_at, tz=timezone.utc).strftime(
                    "%H:%M:%S"
                ),
                extra={
                    "cycle": cycle,
                    "next_check_in_s": interval_s,
                    "next_check_at": datetime.fromtimestamp(
                        next_check_at, tz=timezone.utc
                    ).isoformat(),
                },
            )

            await asyncio.sleep(interval_s)

    except asyncio.CancelledError:
        logger.info(
            "Watchdog: pętla zatrzymana (CancelledError) — graceful shutdown OK "
            "| cykli wykonanych: %d",
            cycle,
            extra={"cycles_completed": cycle},
        )
        # NIE re-raise — pozwalamy lifespan zakończyć się czysto

    except Exception as exc:
        logger.critical(
            "Watchdog: krytyczny błąd pętli głównej w cyklu #%d: %s",
            cycle, exc,
            extra={
                "cycle": cycle,
                "traceback": traceback.format_exc(),
            },
        )
        raise