# =============================================================================
# worker/tasks/snapshot_task.py — ARQ Cron: Daily snapshot dbo_ext
# =============================================================================
# Cron: codziennie 02:00 Europe/Warsaw
# Format: /app/snapshots/YYYY-MM-DD/snapshot_{table}_{YYYYMMDD_HHMMSS}.json.gz
# Retencja: 30 dni (konfigurowalny)
# =============================================================================

from __future__ import annotations

import gzip
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from worker.core.db import get_engine
from worker.core.redis_client import publish_sse_event
from worker.settings import get_settings
from worker.core.logging_setup import get_event_logger

logger = logging.getLogger("worker.tasks.snapshot")
_WARSAW = ZoneInfo("Europe/Warsaw")

# Tabele dbo_ext (skw_*) do snapshotowania
_SKW_TABLES = [
    "skw_Users",
    "skw_Roles",
    "skw_Permissions",
    "skw_UserRoles",
    "skw_RolePermissions",
    "skw_OtpCodes",
    "skw_SystemConfig",
    "skw_CorsConfig",
    "skw_AuditLog",
    "skw_MonitHistory",
    "skw_Comments",
    "skw_Templates",
    "skw_SchemaVersions",
    # Moduł Akceptacji Faktur (Sprint 2)
    "skw_faktura_akceptacja",
    "skw_faktura_przypisanie",   # ← szczególnie ważne: historyczne is_active=0
    "skw_faktura_log",           # ← audit trail modułu
]


async def daily_snapshot(ctx: dict[str, Any]) -> dict[str, Any]:
    """
    ARQ Cron Task: Codzienny snapshot wszystkich tabel dbo_ext.

    Uruchamiany przez WorkerSettings.cron_jobs o 02:00 Europe/Warsaw.
    Każda tabela → osobny plik .json.gz
    Po zakończeniu → SSE event + wpis w logach

    Returns:
        Słownik z podsumowaniem snapshotu.
    """
    settings = get_settings()
    task_start = time.monotonic()
    now = datetime.now(_WARSAW)
    now_utc = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y%m%d_%H%M%S")

    logger.info(
        "Rozpoczynam daily_snapshot",
        extra={
            "date": date_str,
            "tables": _SKW_TABLES,
            "triggered_by": "cron",
        },
    )
    get_event_logger(settings.LOG_DIR).log(
        "snapshot_started",
        {"date": date_str, "tables": _SKW_TABLES, "trigger": "cron"},
    )

    snapshot_dir = Path(settings.SNAPSHOT_DIR) / date_str
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict] = []
    total_rows = 0
    failed_tables: list[str] = []

    engine = get_engine()

    for table_name in _SKW_TABLES:
        table_start = time.monotonic()
        try:
            rows = await _dump_table(engine, table_name)
            row_count = len(rows)

            # Plik snapshot
            filename = f"snapshot_{table_name}_{ts_str}.json.gz"
            filepath = snapshot_dir / filename

            # Kompresja gzip (≈10x mniejszy rozmiar)
            snapshot_data = {
                "metadata": {
                    "table":       table_name,
                    "date":        date_str,
                    "ts_utc":      now_utc.isoformat(),
                    "ts_warsaw":   now.isoformat(),
                    "row_count":   row_count,
                    "source":      "cron_daily_snapshot",
                    "version":     "2.0",
                    "schema":      "dbo_ext",
                },
                "rows": rows,
            }

            compressed = gzip.compress(
                json.dumps(snapshot_data, ensure_ascii=False, default=str).encode("utf-8"),
                compresslevel=6,
            )
            filepath.write_bytes(compressed)

            table_duration = (time.monotonic() - table_start) * 1000
            total_rows += row_count

            result_entry = {
                "table":       table_name,
                "rows":        row_count,
                "file":        str(filepath),
                "size_kb":     round(len(compressed) / 1024, 1),
                "duration_ms": round(table_duration, 1),
                "status":      "ok",
            }
            results.append(result_entry)

            logger.info(
                "Tabela snapshotowana",
                extra={
                    "table":       table_name,
                    "rows":        row_count,
                    "size_kb":     round(len(compressed) / 1024, 1),
                    "duration_ms": round(table_duration, 1),
                    "file":        str(filepath),
                },
            )

        except Exception as exc:
            table_duration = (time.monotonic() - table_start) * 1000
            error_msg = f"{type(exc).__name__}: {exc}"
            failed_tables.append(table_name)
            results.append({
                "table":       table_name,
                "rows":        0,
                "status":      "error",
                "error":       error_msg,
                "duration_ms": round(table_duration, 1),
            })
            logger.error(
                "Błąd snapshotu tabeli",
                extra={
                    "table":    table_name,
                    "error":    error_msg,
                    "duration_ms": round(table_duration, 1),
                },
                exc_info=True,
            )

    # ── Retencja — usuń stare snapshoty ─────────────────────────────────────
    deleted_dirs = await _cleanup_old_snapshots(settings)

    # ── Podsumowanie ─────────────────────────────────────────────────────────
    total_duration = (time.monotonic() - task_start) * 1000
    summary = {
        "date":          date_str,
        "ts_utc":        now_utc.isoformat(),
        "total_tables":  len(_SKW_TABLES),
        "ok_tables":     len(_SKW_TABLES) - len(failed_tables),
        "failed_tables": failed_tables,
        "total_rows":    total_rows,
        "duration_ms":   round(total_duration, 1),
        "snapshot_dir":  str(snapshot_dir),
        "deleted_dirs":  deleted_dirs,
        "details":       results,
    }

    # Zapis podsumowania jako osobny plik snapshot_summary
    summary_file = snapshot_dir / f"snapshot_summary_{ts_str}.json"
    summary_file.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    get_event_logger(settings.LOG_DIR).log(
        "snapshot_completed",
        summary,
    )

    # SSE event
    await publish_sse_event(
        event_type="task_completed",
        data={
            "task":          "daily_snapshot",
            "success":       len(_SKW_TABLES) - len(failed_tables),
            "failed":        len(failed_tables),
            "message":       f"Snapshot {date_str}: {total_rows} wierszy, {len(failed_tables)} błędów",
            "snapshot_dir":  str(snapshot_dir),
            "duration_ms":   round(total_duration, 1),
        },
    )

    logger.info(
        "daily_snapshot zakończony",
        extra={
            "date":         date_str,
            "ok_tables":    len(_SKW_TABLES) - len(failed_tables),
            "failed":       len(failed_tables),
            "total_rows":   total_rows,
            "duration_ms":  round(total_duration, 1),
        },
    )
    return summary


async def _dump_table(engine, table_name: str) -> list[dict]:
    """
    Pobiera wszystkie wiersze z tabeli jako lista słowników.
    Używa raw SQL przez engine (bez ORM) — bardziej odporne na zmiany schematu.
    """
    async with engine.begin() as conn:
        # Sprawdź czy tabela istnieje
        exists_result = await conn.execute(text("""
            SELECT COUNT(1)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'dbo'
              AND TABLE_NAME = :table_name
        """), {"table_name": table_name})
        if not exists_result.scalar():
            logger.warning("Tabela nie istnieje — pomijam", extra={"table": table_name})
            return []

        result = await conn.execute(text(f"SELECT * FROM dbo.[{table_name}] WITH (NOLOCK)"))
        columns = list(result.keys())
        rows = []
        for row in result.fetchall():
            row_dict = {}
            for col, val in zip(columns, row):
                # Serializacja: datetime → ISO string, bytes → base64, reszta jak jest
                if isinstance(val, datetime):
                    row_dict[col] = val.isoformat()
                elif isinstance(val, (bytes, bytearray)):
                    import base64
                    row_dict[col] = base64.b64encode(val).decode("ascii")
                else:
                    row_dict[col] = val
            rows.append(row_dict)
        return rows


async def _cleanup_old_snapshots(settings) -> list[str]:
    """
    Usuwa katalogi snapshots starsze niż SNAPSHOT_RETENTION_DAYS.
    Zwraca listę usuniętych katalogów.
    """
    retention = settings.SNAPSHOT_RETENTION_DAYS
    cutoff = datetime.now(_WARSAW).date() - timedelta(days=retention)
    snapshot_root = Path(settings.SNAPSHOT_DIR)
    deleted = []

    if not snapshot_root.exists():
        return deleted

    for date_dir in sorted(snapshot_root.iterdir()):
        if not date_dir.is_dir():
            continue
        try:
            dir_date_str = date_dir.name  # format YYYY-MM-DD
            dir_date = datetime.strptime(dir_date_str, "%Y-%m-%d").date()
            if dir_date < cutoff:
                import shutil
                shutil.rmtree(date_dir)
                deleted.append(str(date_dir))
                logger.info(
                    "Stary snapshot usunięty",
                    extra={"dir": str(date_dir), "retention_days": retention},
                )
        except (ValueError, Exception) as exc:
            logger.warning(
                "Błąd podczas cleanup snapshot",
                extra={"dir": str(date_dir), "error": str(exc)},
            )

    return deleted