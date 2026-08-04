# worker/tasks/source_config_snapshot_task.py
"""
ARQ Cron: okresowy zrzut aktualnej konfiguracji wszystkich aktywnych zrodel
do strumienia logow source_config_YYYY-MM-DD.jsonl.

UZUPELNIENIE (nie zastepstwo) logu natychmiastowego w
backend/app/services/source_admin_service.py::_log_config_snapshot() —
ten task lapie zmiany OMIJAJACE panel/API (np. reczna edycja
connection_config bezposrednio w SSMS), ktorych log natychmiastowy z
definicji nie moze zobaczyc.

Cron: co 20 minut (konfigurowalny w worker/main.py przy rejestracji).

UWAGA: worker jest izolowany od pakietu `app` (backend) — nie moze
importowac source_admin_service.py bezposrednio. Deszyfrowanie
connection_config uzywa WLASNEGO, juz istniejacego wrappera workera —
decrypt_connection_config() w worker/services/source_adapter.py (import
wewnatrz-workerowy, legalny — to NIE jest import z app.*). Logika
REDAKCJI (maskowanie wrazliwych pol przed zapisem do logu) jest
zduplikowana lokalnie w tym pliku (_redact_config, _SENSITIVE_CONFIG_KEYS)
— SYNC z backend/app/services/source_admin_service.py, bo to osobna
funkcjonalnosc (kosmetyka logu), nie krytyczny mechanizm bezpieczenstwa
jak samo szyfrowanie.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

from worker.core.db import get_engine
from worker.settings import get_settings

logger = logging.getLogger("worker.tasks.source_config_snapshot")

_SCHEMA = "dbo"

# SYNC z backend/app/services/source_admin_service.py::_SENSITIVE_CONFIG_KEYS
# — jesli zmieniasz jedno, zmien i drugie (worker nie moze zaimportowac
# tamtego pliku, to jest celowo zduplikowana kopia).
_SENSITIVE_CONFIG_KEYS = frozenset({
    "password", "pwd", "secret", "token", "api_key", "apikey",
    "auth_config", "webhook_token", "private_key", "certificate",
})


def _redact_config(config: dict[str, Any]) -> dict[str, Any]:
    """Identyczna logika co w source_admin_service.py — patrz komentarz SYNC powyzej."""
    redacted: dict[str, Any] = {}
    for k, v in config.items():
        if k.lower() in _SENSITIVE_CONFIG_KEYS:
            redacted[k] = "***"
        elif isinstance(v, dict):
            redacted[k] = _redact_config(v)
        elif isinstance(v, str) and "PWD=" in v.upper():
            import re
            redacted[k] = re.sub(r"(PWD=)[^;]*", r"\1***", v, flags=re.IGNORECASE)
        else:
            redacted[k] = v
    return redacted


def _get_config_snapshot_log_path(settings) -> Path:
    """
    Ten sam plik/katalog co log natychmiastowy z backend (jeden wspolny
    strumien source_config_YYYY-MM-DD.jsonl w /app/logs) — worker i api
    montuja ten sam wolumin logow (potwierdzone wzorcem innych plikow
    *.jsonl widocznych z obu kontenerow w tej sesji).
    """
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_dir = Path(settings.LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / f"source_config_{date_str}.jsonl"


async def source_config_snapshot_task(ctx: dict[str, Any]) -> dict[str, Any]:
    """
    ARQ Cron Task: zrzuca aktualna (zredagowana) konfiguracje wszystkich
    AKTYWNYCH zrodel do source_config_YYYY-MM-DD.jsonl, reason="periodic".

    NAPRAWA (self-review 2026-07-16): pierwsza wersja tej funkcji importowala
    from app.core.encryption import decrypt_value — LAMIE fundamentalna
    zasade izolacji workera od pakietu `app` (backend), ta sama, ktora na
    poczatku tej sesji spowodowala awarie auto_dispatch_task (ImportError
    dla app.* w kontenerze workera). Worker ma WLASNY, izolowany wrapper
    deszyfrowania — decrypt_connection_config() w
    worker/services/source_adapter.py ("lustrzane odbicie
    app.core.encryption.decrypt_value") — uzywamy TEGO, nie oryginalu.

    Returns:
        Podsumowanie: liczba zrodel, liczba bledow, sciezka pliku.
    """
    settings = get_settings()
    task_start = datetime.now(timezone.utc)

    from worker.services.source_adapter import decrypt_connection_config, WorkerEncryptionError

    engine = get_engine()
    log_path = _get_config_snapshot_log_path(settings)

    ok_count = 0
    error_count = 0

    async with engine.begin() as conn:
        result = await conn.execute(text(f"""
            SELECT [id_source], [source_name], [source_type], [connection_mode],
                   [connection_config], [is_active], [sync_interval_minutes]
            FROM [{_SCHEMA}].[skw_document_sources]
            WHERE [is_active] = 1
        """))
        rows = result.fetchall()

    with open(log_path, "a", encoding="utf-8") as f:
        for row in rows:
            id_source, source_name, source_type, connection_mode, cfg_raw, is_active, sync_interval = row
            try:
                config = json.loads(decrypt_connection_config(cfg_raw)) if cfg_raw else {}
                entry = {
                    "ts_utc":                task_start.isoformat(),
                    "event":                 "source_config_snapshot",
                    "reason":                "periodic",
                    "id_source":             id_source,
                    "source_name":           source_name,
                    "source_type":           source_type,
                    "connection_mode":       connection_mode,
                    "is_active":             bool(is_active),
                    "sync_interval_minutes": sync_interval,
                    "connection_config":     _redact_config(config),
                    "changed_by_user_id":    None,  # periodic — brak konkretnego aktora
                }
                f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
                ok_count += 1
            except WorkerEncryptionError as exc:
                error_count += 1
                logger.error(
                    "source_config_snapshot_task: blad deszyfrowania id_source=%s: %s",
                    id_source, exc,
                )
            except Exception as exc:
                error_count += 1
                logger.error(
                    "source_config_snapshot_task: blad przetwarzania id_source=%s: %s",
                    id_source, exc,
                )

    duration_ms = (datetime.now(timezone.utc) - task_start).total_seconds() * 1000
    summary = {
        "ts_utc":       task_start.isoformat(),
        "total_sources": len(rows),
        "ok":           ok_count,
        "errors":       error_count,
        "log_file":     str(log_path),
        "duration_ms":  round(duration_ms, 1),
    }
    logger.info(
        "source_config_snapshot_task zakonczony | zrodla=%d ok=%d bledy=%d czas_ms=%.1f",
        len(rows), ok_count, error_count, duration_ms,
    )
    return summary