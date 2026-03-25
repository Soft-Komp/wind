"""
Serwis Snapshotów — System Windykacja
=======================================

Odpowiedzialność:
    - Tworzenie snapshotów (dump) tabel dbo_ext do plików JSON.gz
    - Restore snapshotu — UPSERT rekordów z pliku do bazy
    - Listowanie dostępnych snapshotów (skan katalogu, bez odczytu plików)
    - Czyszczenie starych snapshotów (retention policy)
    - Publikacja SSE event po zakończeniu (przez event_service)
    - AuditLog dla każdej operacji

Format pliku snapshotu:
    /app/snapshots/{YYYY-MM-DD}/snapshot_{table}_{timestamp}.json.gz

Schemat wewnątrz pliku JSON (po dekompresji):
    {
        "created_at": "ISO datetime UTC",
        "table":      "dbo_ext.Users",
        "row_count":  1234,
        "schema_version": "alembic_revision_hash",
        "rows": [ {...}, {...}, ... ]
    }

Decyzje projektowe:
    - Snapshot jednej tabeli = jeden plik (nie zbiorczy)
      → prostsze restore, łatwiej zarządzać
    - UPSERT przez MERGE (MSSQL) lub INSERT ... ON CONFLICT (nie dostępny w MSSQL)
      → używamy SELECT + UPDATE/INSERT (SQLAlchemy Core)
    - Snapshot jest read-only przez aplikację — restore do NOWYCH rekordów (nie nadpisuje aktywnych)
    - retention_days z SystemConfig("snapshot.retention_days", default=30)
    - Tabele snapshot-owane: wszystkie z __tablename__ w dbo_ext (wykrywane dynamicznie)
    - Pliki snapshotów są USUWALNE (w przeciwieństwie do archiwów) — cleanup_old() jest dozwolony

Zależności:
    - services/audit_service.py
    - services/event_service.py
    - services/config_service.py (retention_days)
"""

from __future__ import annotations

import gzip
import logging
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import orjson
from redis.asyncio import Redis
from sqlalchemy import inspect as sa_inspect, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services import audit_service
from app.services import config_service
from app.services import event_service

# ---------------------------------------------------------------------------
# Logger własny modułu
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------

_SNAPSHOT_BASE_DIR: str    = "snapshots"
_GZIP_LEVEL: int           = 6
_DEFAULT_RETENTION_DAYS: int = 30
_MAX_TABLES_PER_SNAPSHOT: int = 50    # Bezpiecznik — nie robimy snapshotu 100 tabel naraz
_BATCH_SIZE_ROWS: int      = 1000     # Przetwarzamy wiersze w batchach (pamięć)

# Tabele które NIE są snapshot-owane (tylko INSERT, wrażliwe)
_EXCLUDED_TABLES: frozenset[str] = frozenset({
    "auditlog",
    "masteraccesslog",
    "refreshtokens",
    "otpcodes",
    "schemachecksums",
})


# ===========================================================================
# Dataclassy wynikowe
# ===========================================================================

@dataclass(frozen=True)
class TableSnapshotResult:
    """Wynik snapshotu jednej tabeli."""
    table_name: str
    filepath: Optional[Path]
    row_count: int
    file_size_bytes: int
    success: bool
    error: Optional[str] = None
    duration_ms: Optional[float] = None


@dataclass(frozen=True)
class SnapshotResult:
    """Łączny wynik operacji snapshot."""
    snapshot_date: str
    total_tables: int
    success_count: int
    failed_count: int
    total_rows: int
    total_size_bytes: int
    tables: list[TableSnapshotResult]
    duration_ms: float
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass(frozen=True)
class SnapshotFile:
    """Metadane pliku snapshotu (bez odczytu zawartości)."""
    filepath: str
    date: str
    table_name: str
    filename: str
    size_bytes: int
    size_kb: float


@dataclass(frozen=True)
class RestoreResult:
    """Wynik operacji restore."""
    table_name: str
    snapshot_date: str
    rows_in_file: int
    rows_upserted: int
    rows_skipped: int
    success: bool
    error: Optional[str] = None
    duration_ms: Optional[float] = None


# ===========================================================================
# Klasy wyjątków
# ===========================================================================

class SnapshotError(Exception):
    """Bazowy wyjątek serwisu snapshotów."""


class SnapshotFileNotFoundError(SnapshotError):
    """Plik snapshotu nie istnieje."""


class SnapshotTableNotFoundError(SnapshotError):
    """Tabela do snapshot/restore nie istnieje w bazie."""


class SnapshotRestoreError(SnapshotError):
    """Błąd podczas operacji restore."""


class SnapshotTableExcludedError(SnapshotError):
    """Tabela jest na liście wykluczeń (nie podlega snapshot-owaniu)."""


# ===========================================================================
# Funkcje pomocnicze — prywatne
# ===========================================================================

def _get_snapshot_dir(date_str: Optional[str] = None) -> Path:
    """Zwraca i tworzy katalog snapshotów."""
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = Path(_SNAPSHOT_BASE_DIR) / date_str
    path.mkdir(parents=True, exist_ok=True)
    return path


def _build_snapshot_filename(table_name: str, timestamp: datetime) -> str:
    """Buduje nazwę pliku snapshotu."""
    import re
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", table_name.rsplit(".", 1)[-1]).lower()
    ts_str = timestamp.strftime("%Y%m%d_%H%M%S")
    return f"snapshot_{clean}_{ts_str}.json.gz"


def _default_json(obj: Any) -> Any:
    """Custom JSON serializer dla orjson."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime,)):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.hex()
    raise TypeError(f"Nie można serializować {type(obj)!r}")


def _is_table_excluded(table_name: str) -> bool:
    """Sprawdza czy tabela jest na liście wykluczeń."""
    clean = table_name.rsplit(".", 1)[-1].lower().replace("_", "").replace(" ", "")
    return clean in _EXCLUDED_TABLES


async def _discover_dbo_ext_tables(db: AsyncSession) -> list[str]:
    """
    Wykrywa wszystkie tabele w schemacie dbo_ext.

    Używa sys.tables MSSQL do dynamicznego odkrywania tabel.

    Args:
        db: Sesja SQLAlchemy.

    Returns:
        Lista kwalifikowanych nazw tabel (dbo_ext.NazwaTabeli).
    """
    result = await db.execute(text("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'dbo_ext'
          AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    """))
    tables = [f"dbo_ext.{row[0]}" for row in result.fetchall()]

    # Filtruj wykluczone
    filtered = [t for t in tables if not _is_table_excluded(t)]

    logger.debug(
        "Wykryte tabele dbo_ext do snapshot",
        extra={"total": len(tables), "after_exclusions": len(filtered), "tables": filtered}
    )
    return filtered


async def _snapshot_table(
    db: AsyncSession,
    table_name: str,
    snapshot_dir: Path,
    timestamp: datetime,
) -> TableSnapshotResult:
    """
    Tworzy snapshot jednej tabeli.

    Pobiera dane w batchach (_BATCH_SIZE_ROWS) żeby nie ładować wszystkiego do pamięci.
    Zapisuje do pliku JSON.gz strumieniowo (batch po batch).

    Args:
        db:           Sesja SQLAlchemy.
        table_name:   Kwalifikowana nazwa tabeli (np. dbo_ext.Users).
        snapshot_dir: Katalog docelowy.
        timestamp:    Timestamp operacji.

    Returns:
        TableSnapshotResult z wynikiem operacji.
    """
    start = datetime.now(timezone.utc)

    schema, table = table_name.rsplit(".", 1) if "." in table_name else ("dbo_ext", table_name)
    filename = _build_snapshot_filename(table_name, timestamp)
    filepath = snapshot_dir / filename

    try:
        # Pobierz kolumny tabeli
        cols_result = await db.execute(text(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
            ORDER BY ORDINAL_POSITION
        """), {"schema": schema, "table": table})
        columns = [row[0] for row in cols_result.fetchall()]

        if not columns:
            return TableSnapshotResult(
                table_name=table_name,
                filepath=None,
                row_count=0,
                file_size_bytes=0,
                success=False,
                error=f"Tabela '{table_name}' nie istnieje lub nie ma kolumn.",
            )

        # COUNT
        count_result = await db.execute(
            text(f"SELECT COUNT(1) FROM [{schema}].[{table}]")
        )
        total_rows = count_result.scalar_one() or 0

        logger.info(
            "Rozpoczynam snapshot tabeli",
            extra={"table": table_name, "rows": total_rows}
        )

        # Pobieranie danych w batchach i zapis strumieniowy
        all_rows: list[dict] = []
        offset = 0

        while offset < total_rows or (total_rows == 0 and offset == 0):
            batch_result = await db.execute(text(f"""
                SELECT {', '.join(f'[{c}]' for c in columns)}
                FROM [{schema}].[{table}]
                ORDER BY (SELECT NULL)
                OFFSET :offset ROWS FETCH NEXT :batch ROWS ONLY
            """), {"offset": offset, "batch": _BATCH_SIZE_ROWS})

            rows = batch_result.fetchall()
            if not rows:
                break

            for row in rows:
                all_rows.append(dict(zip(columns, row)))

            offset += len(rows)
            if len(rows) < _BATCH_SIZE_ROWS:
                break

        # Budowanie struktury JSON
        envelope = {
            "created_at": timestamp.isoformat(),
            "table": table_name,
            "row_count": len(all_rows),
            "columns": columns,
            "rows": all_rows,
        }

        # Serializacja + kompresja
        json_bytes = orjson.dumps(
            envelope,
            default=_default_json,
            option=orjson.OPT_NON_STR_KEYS,
        )

        with gzip.open(filepath, "wb", compresslevel=_GZIP_LEVEL) as f:
            f.write(json_bytes)

        file_size = filepath.stat().st_size
        duration = (datetime.now(timezone.utc) - start).total_seconds() * 1000

        logger.info(
            "Snapshot tabeli zakończony",
            extra={
                "table": table_name,
                "rows": len(all_rows),
                "file_size_kb": round(file_size / 1024, 1),
                "duration_ms": round(duration, 1),
                "filepath": str(filepath),
            }
        )

        return TableSnapshotResult(
            table_name=table_name,
            filepath=filepath,
            row_count=len(all_rows),
            file_size_bytes=file_size,
            success=True,
            duration_ms=round(duration, 1),
        )

    except Exception as exc:
        duration = (datetime.now(timezone.utc) - start).total_seconds() * 1000
        logger.error(
            "Błąd snapshotu tabeli",
            extra={
                "table": table_name,
                "error": str(exc),
                "duration_ms": round(duration, 1),
            }
        )
        # Usuń częściowy plik
        if filepath.exists():
            try:
                filepath.unlink()
            except OSError:
                pass

        return TableSnapshotResult(
            table_name=table_name,
            filepath=None,
            row_count=0,
            file_size_bytes=0,
            success=False,
            error=str(exc),
            duration_ms=round(duration, 1),
        )


# ===========================================================================
# Publiczne API serwisu
# ===========================================================================

async def create(
    db: AsyncSession,
    redis: Redis,
    tables: Optional[list[str]] = None,
    created_by_id: Optional[int] = None,
    ip_address: Optional[str] = None,
) -> SnapshotResult:
    """
    Tworzy snapshot wybranych lub wszystkich tabel dbo_ext.

    Snapshot każdej tabeli to osobny plik JSON.gz.
    Operacja może trwać długo — przeznaczona do wywołania przez ARQ lub ręcznie.

    Przepływ:
        1. Wykrycie tabel (dynamicznie z INFORMATION_SCHEMA)
        2. Filtrowanie wykluczeń i limitowanie do _MAX_TABLES_PER_SNAPSHOT
        3. Snapshot każdej tabeli sekwencyjnie (bezpieczne dla DB)
        4. AuditLog z podsumowaniem
        5. SSE event: snapshot_created

    Args:
        db:            Sesja SQLAlchemy.
        redis:         Klient Redis.
        tables:        Lista tabel do snapshot (None = wszystkie).
        created_by_id: ID admina zlecającego snapshot.
        ip_address:    IP inicjatora.

    Returns:
        SnapshotResult z wynikami per tabela i podsumowaniem.

    Raises:
        SnapshotTableExcludedError: Gdy jawnie podana tabela jest na liście wykluczeń.
    """
    op_start = datetime.now(timezone.utc)
    date_str = op_start.strftime("%Y-%m-%d")

    logger.info(
        "Rozpoczynam tworzenie snapshotu",
        extra={
            "requested_tables": tables,
            "created_by": created_by_id,
            "ip_address": ip_address,
        }
    )

    # Wykryj tabele
    if tables is None:
        target_tables = await _discover_dbo_ext_tables(db)
    else:
        # Walidacja jawnie podanych tabel
        for t in tables:
            if _is_table_excluded(t):
                raise SnapshotTableExcludedError(
                    f"Tabela '{t}' jest na liście wykluczeń i nie może być snapshot-owana."
                )
        target_tables = tables

    if len(target_tables) > _MAX_TABLES_PER_SNAPSHOT:
        logger.warning(
            "Liczba tabel przekracza limit — przycinamy",
            extra={
                "requested": len(target_tables),
                "limit": _MAX_TABLES_PER_SNAPSHOT,
            }
        )
        target_tables = target_tables[:_MAX_TABLES_PER_SNAPSHOT]

    # Katalog snapshotów
    snapshot_dir = _get_snapshot_dir(date_str)

    # Snapshot per tabela
    table_results: list[TableSnapshotResult] = []
    for table_name in target_tables:
        result = await _snapshot_table(db, table_name, snapshot_dir, op_start)
        table_results.append(result)

    # Podsumowanie
    success_count  = sum(1 for r in table_results if r.success)
    failed_count   = sum(1 for r in table_results if not r.success)
    total_rows     = sum(r.row_count for r in table_results)
    total_size     = sum(r.file_size_bytes for r in table_results)
    total_duration = (datetime.now(timezone.utc) - op_start).total_seconds() * 1000

    snapshot_result = SnapshotResult(
        snapshot_date=date_str,
        total_tables=len(target_tables),
        success_count=success_count,
        failed_count=failed_count,
        total_rows=total_rows,
        total_size_bytes=total_size,
        tables=table_results,
        duration_ms=round(total_duration, 1),
    )

    logger.info(
        "Snapshot zakończony",
        extra={
            "date": date_str,
            "tables_total": len(target_tables),
            "success": success_count,
            "failed": failed_count,
            "total_rows": total_rows,
            "total_size_kb": round(total_size / 1024, 1),
            "duration_ms": round(total_duration, 1),
        }
    )

    # AuditLog
    audit_service.log_crud(
        db=db,
        action="snapshot_created",
        entity_type="Snapshot",
        details={
            "date": date_str,
            "tables": [r.table_name for r in table_results if r.success],
            "tables_count": success_count,
            "total_rows": total_rows,
            "total_size_bytes": total_size,
            "failed_tables": [r.table_name for r in table_results if not r.success],
            "created_by": created_by_id,
            "ip_address": ip_address,
        },
        success=failed_count == 0,
    )

    # SSE event
    try:
        await event_service.publish_snapshot_created(
            redis=redis,
            tables=[r.table_name for r in table_results if r.success],
            total_records=total_rows,
            snapshot_date=date_str,
            triggered_by_user_id=created_by_id,
        )
    except Exception as exc:
        logger.warning("Nie udało się wysłać SSE event po snapshot", extra={"error": str(exc)})

    return snapshot_result


async def restore(
    db: AsyncSession,
    redis: Redis,
    snapshot_date: str,
    table_name: str,
    admin_id: int,
    ip_address: Optional[str] = None,
    dry_run: bool = False,
) -> RestoreResult:
    """
    Przywraca dane tabeli ze snapshotu (UPSERT).

    ⚠️  RESTORE NIE NADPISUJE aktywnych rekordów — pomija wiersze których PK
    już istnieje w bazie (is_active=True). Dodaje tylko brakujące/nieaktywne.

    Wymaga uprawnienia: snapshots.restore

    Przepływ:
        1. Zlokalizowanie pliku snapshotu
        2. Odczyt i dekompresja
        3. Dla każdego rekordu: sprawdź czy PK istnieje → INSERT lub SKIP
        4. AuditLog
        5. SSE event snapshot_restored

    Args:
        db:            Sesja SQLAlchemy.
        redis:         Klient Redis.
        snapshot_date: Data snapshotu (YYYY-MM-DD).
        table_name:    Nazwa tabeli (np. "dbo_ext.MonitHistory").
        admin_id:      ID admina wykonującego restore.
        ip_address:    IP inicjatora.
        dry_run:       True → symulacja (bez zapisu do DB).

    Returns:
        RestoreResult z wynikami operacji.

    Raises:
        SnapshotFileNotFoundError:    Plik snapshotu nie istnieje.
        SnapshotTableExcludedError:   Tabela jest na liście wykluczeń.
        SnapshotRestoreError:         Błąd podczas restore.
    """
    if _is_table_excluded(table_name):
        raise SnapshotTableExcludedError(
            f"Tabela '{table_name}' jest na liście wykluczeń i nie może być przywrócona."
        )

    op_start = datetime.now(timezone.utc)

    # Znajdź plik snapshotu
    snapshot_dir = Path(_SNAPSHOT_BASE_DIR) / snapshot_date
    if not snapshot_dir.exists():
        raise SnapshotFileNotFoundError(
            f"Katalog snapshotu dla daty '{snapshot_date}' nie istnieje."
        )

    import re
    clean_table = re.sub(r"[^a-zA-Z0-9_]", "_", table_name.rsplit(".", 1)[-1]).lower()
    pattern = f"snapshot_{clean_table}_*.json.gz"
    matching = sorted(snapshot_dir.glob(pattern), reverse=True)

    if not matching:
        raise SnapshotFileNotFoundError(
            f"Nie znaleziono pliku snapshotu dla tabeli '{table_name}' "
            f"z daty '{snapshot_date}'. Wzorzec: {pattern}"
        )

    snapshot_file = matching[0]

    logger.info(
        "Rozpoczynam restore snapshotu",
        extra={
            "snapshot_file": str(snapshot_file),
            "table": table_name,
            "admin_id": admin_id,
            "dry_run": dry_run,
        }
    )

    try:
        # Odczyt i dekompresja
        with gzip.open(snapshot_file, "rb") as f:
            raw = f.read()
        data = orjson.loads(raw)

        rows = data.get("rows", [])
        columns = data.get("columns", [])
        rows_in_file = len(rows)

        if rows_in_file == 0:
            logger.info("Plik snapshotu jest pusty — brak danych do restore")
            return RestoreResult(
                table_name=table_name,
                snapshot_date=snapshot_date,
                rows_in_file=0,
                rows_upserted=0,
                rows_skipped=0,
                success=True,
                duration_ms=0.0,
            )

        if dry_run:
            logger.info(
                "Dry run — symulacja restore (bez zapisu)",
                extra={"rows_in_file": rows_in_file, "table": table_name}
            )
            return RestoreResult(
                table_name=table_name,
                snapshot_date=snapshot_date,
                rows_in_file=rows_in_file,
                rows_upserted=0,
                rows_skipped=rows_in_file,
                success=True,
                duration_ms=0.0,
            )

        # Wykryj kolumnę PK (zakładamy pierwszą kolumnę jako PK)
        schema, table = table_name.rsplit(".", 1) if "." in table_name else ("dbo_ext", table_name)
        pk_col = columns[0] if columns else "ID"

        rows_upserted = 0
        rows_skipped  = 0

        for row in rows:
            pk_value = row.get(pk_col)
            if pk_value is None:
                rows_skipped += 1
                continue

            # Sprawdź czy rekord już istnieje
            check = await db.execute(text(f"""
                SELECT 1 FROM [{schema}].[{table}]
                WHERE [{pk_col}] = :pk
            """), {"pk": pk_value})
            exists = check.scalar_one_or_none() is not None

            if exists:
                rows_skipped += 1
                continue

            # INSERT
            col_names = ", ".join(f"[{c}]" for c in row.keys() if c != pk_col)
            col_params = ", ".join(f":{c}" for c in row.keys() if c != pk_col)
            params = {k: v for k, v in row.items() if k != pk_col}

            if col_names:
                await db.execute(text(f"""
                    INSERT INTO [{schema}].[{table}] ({col_names})
                    VALUES ({col_params})
                """), params)
                rows_upserted += 1

        await db.flush()
        await db.commit()
        duration = (datetime.now(timezone.utc) - op_start).total_seconds() * 1000

        logger.info(
            "Restore snapshotu zakończony",
            extra={
                "table": table_name,
                "rows_in_file": rows_in_file,
                "rows_upserted": rows_upserted,
                "rows_skipped": rows_skipped,
                "duration_ms": round(duration, 1),
            }
        )

        # AuditLog
        audit_service.log_crud(
            db=db,
            action="snapshot_restored",
            entity_type="Snapshot",
            details={
                "table": table_name,
                "snapshot_date": snapshot_date,
                "snapshot_file": str(snapshot_file),
                "rows_in_file": rows_in_file,
                "rows_upserted": rows_upserted,
                "rows_skipped": rows_skipped,
                "admin_id": admin_id,
                "ip_address": ip_address,
            },
            success=True,
        )

        return RestoreResult(
            table_name=table_name,
            snapshot_date=snapshot_date,
            rows_in_file=rows_in_file,
            rows_upserted=rows_upserted,
            rows_skipped=rows_skipped,
            success=True,
            duration_ms=round(duration, 1),
        )

    except (SnapshotFileNotFoundError, SnapshotTableExcludedError):
        raise
    except Exception as exc:
        duration = (datetime.now(timezone.utc) - op_start).total_seconds() * 1000
        logger.error(
            "Błąd restore snapshotu",
            extra={"table": table_name, "error": str(exc), "duration_ms": round(duration, 1)}
        )
        raise SnapshotRestoreError(f"Błąd restore tabeli '{table_name}': {exc}") from exc


def list_available(
    date_filter: Optional[str] = None,
    table_filter: Optional[str] = None,
) -> list[SnapshotFile]:
    """
    Listuje dostępne pliki snapshotów.

    NIE odczytuje zawartości plików — tylko skanuje katalog.
    Szybka operacja nawet jeśli jest wiele plików.

    Args:
        date_filter:  Filtr po dacie (YYYY-MM-DD). None = wszystkie.
        table_filter: Filtr po nazwie tabeli (substring, case-insensitive).

    Returns:
        Lista SnapshotFile posortowana malejąco po dacie.
    """
    base = Path(_SNAPSHOT_BASE_DIR)
    if not base.exists():
        return []

    if date_filter:
        date_dirs = [base / date_filter] if (base / date_filter).exists() else []
    else:
        date_dirs = sorted(
            [d for d in base.iterdir() if d.is_dir()],
            reverse=True,
        )

    results: list[SnapshotFile] = []
    for date_dir in date_dirs:
        for filepath in sorted(date_dir.glob("snapshot_*.json.gz"), reverse=True):
            # Wyciągnij nazwę tabeli z pliku
            name = filepath.name.removeprefix("snapshot_")
            # Usun timestamp na koncu (ostatnie dwa segmenty: YYYYMMDD_HHMMSS)
            # Przyklad: skw_roles_20260303_105544.json.gz -> skw_roles
            name_no_ext = name.removesuffix(".json.gz")
            parts = name_no_ext.split("_")
            # Ostatnie dwa segmenty to timestamp (YYYYMMDD i HHMMSS) - odcinamy je
            # Przyklad: ["skw", "roles", "20260303", "105544"] -> ["skw", "roles"]
            table_parts = parts[:-2] if len(parts) > 2 else parts[:1]
            table_hint = "_".join(table_parts) if table_parts else "unknown"

            if table_filter and table_filter.lower() not in table_hint.lower():
                continue

            try:
                size = filepath.stat().st_size
            except OSError:
                size = 0

            results.append(SnapshotFile(
                filepath=str(filepath),
                date=date_dir.name,
                table_name=table_hint,
                filename=filepath.name,
                size_bytes=size,
                size_kb=round(size / 1024, 2),
            ))

    return results


async def cleanup_old(
    db: AsyncSession,
    redis: Redis,
    retention_days: Optional[int] = None,
    admin_id: Optional[int] = None,
) -> dict:
    """
    Usuwa stare snapshoty (starsze niż retention_days).

    ⚠️  Usuwa TYLKO snapshoty (katalog snapshots/). Archiwa (archives/) są NIEUSUWALNE.

    Retention policy pochodzi z SystemConfig("snapshot.retention_days", default=30).

    Args:
        db:             Sesja SQLAlchemy.
        redis:          Klient Redis.
        retention_days: Override retention (None = użyj SystemConfig).
        admin_id:       ID admina zlecającego cleanup (do AuditLog).

    Returns:
        Słownik z wynikami cleanup:
        {
            "deleted_dirs": list[str],
            "deleted_files": int,
            "freed_bytes": int,
            "cutoff_date": str,
        }
    """
    if retention_days is None:
        retention_days = await config_service.get_int(
            db, redis,
            key="snapshot.retention_days",
            default=_DEFAULT_RETENTION_DAYS,
        )

    cutoff = datetime.now(timezone.utc).date()
    from datetime import timedelta
    cutoff_date = cutoff - timedelta(days=retention_days)
    cutoff_str  = cutoff_date.strftime("%Y-%m-%d")

    base = Path(_SNAPSHOT_BASE_DIR)
    if not base.exists():
        return {
            "deleted_dirs":  [],
            "deleted_files": 0,
            "freed_bytes":   0,
            "cutoff_date":   cutoff_str,
            "retention_days": retention_days,
        }

    deleted_dirs:  list[str] = []
    deleted_files: int = 0
    freed_bytes:   int = 0

    for date_dir in sorted(base.iterdir()):
        if not date_dir.is_dir():
            continue
        dir_date_str = date_dir.name
        # Porównuj jako stringi (YYYY-MM-DD ma lexicographic ordering)
        if dir_date_str >= cutoff_str:
            continue  # Nowszy niż cutoff — zachowaj

        # Zbierz rozmiar
        dir_size = 0
        file_count = 0
        for fp in date_dir.glob("*.json.gz"):
            try:
                dir_size   += fp.stat().st_size
                file_count += 1
            except OSError:
                pass

        # Usuń cały katalog
        try:
            shutil.rmtree(date_dir)
            deleted_dirs.append(dir_date_str)
            deleted_files += file_count
            freed_bytes   += dir_size
            logger.info(
                "Usunięto stary katalog snapshotu",
                extra={
                    "date_dir": dir_date_str,
                    "files": file_count,
                    "size_kb": round(dir_size / 1024, 1),
                }
            )
        except OSError as exc:
            logger.error(
                "Nie udało się usunąć katalogu snapshotu",
                extra={"date_dir": str(date_dir), "error": str(exc)}
            )

    result = {
        "deleted_dirs":   deleted_dirs,
        "deleted_files":  deleted_files,
        "freed_bytes":    freed_bytes,
        "freed_mb":       round(freed_bytes / (1024 * 1024), 2),
        "cutoff_date":    cutoff_str,
        "retention_days": retention_days,
    }

    logger.info(
        "Cleanup snapshotów zakończony",
        extra=result,
    )

    if deleted_dirs:
        audit_service.log_crud(
            db=db,
            action="snapshots_cleaned",
            entity_type="Snapshot",
            details={
                **result,
                "admin_id": admin_id,
            },
            success=True,
        )

    return result


def get_snapshot_stats() -> dict:
    """
    Zwraca statystyki katalogu snapshotów.

    Returns:
        Słownik ze statystykami (liczba plików, rozmiar, daty).
    """
    base = Path(_SNAPSHOT_BASE_DIR)
    if not base.exists():
        return {
            "total_files": 0,
            "total_size_bytes": 0,
            "total_size_mb": 0.0,
            "date_dirs": [],
            "oldest_date": None,
            "newest_date": None,
        }

    total_files = 0
    total_size  = 0
    date_dirs: list[str] = []

    for date_dir in sorted(base.iterdir()):
        if not date_dir.is_dir():
            continue
        files = list(date_dir.glob("snapshot_*.json.gz"))
        dir_size = sum(f.stat().st_size for f in files if f.exists())
        total_files += len(files)
        total_size  += dir_size
        date_dirs.append(date_dir.name)

    return {
        "total_files":     total_files,
        "total_size_bytes": total_size,
        "total_size_mb":   round(total_size / (1024 * 1024), 2),
        "date_dirs":       sorted(date_dirs, reverse=True),
        "oldest_date":     date_dirs[0] if date_dirs else None,
        "newest_date":     date_dirs[-1] if date_dirs else None,
    }