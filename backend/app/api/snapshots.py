"""
Router snapshotów — System Windykacja.

Prefix: /api/v1/snapshots  (zarejestrowany w api/router.py)

3 endpointy:
  POST /snapshots                          — utwórz snapshot wybranych/wszystkich tabel
  GET  /snapshots                          — lista dostępnych plików snapshotów
  POST /snapshots/{date}/{table}/restore   — przywróć tabelę ze snapshotu

Serwis: services/snapshot_service.py
  - create(db, redis, tables, created_by_id, ip_address) → SnapshotResult
  - restore(db, redis, snapshot_date, table_name, admin_id, ip_address, dry_run) → RestoreResult
  - list_available(date_filter, table_filter) → list[SnapshotFile]

Wyjątki serwisu mapowane na HTTP:
  SnapshotFileNotFoundError    → 404
  SnapshotTableExcludedError   → 403
  SnapshotRestoreError         → 500

Wzorce:
  - AuditLog przez snapshot_service (fire-and-forget)
  - SSE event po zakończeniu przez event_service
  - Walidacja tabel: max 50 na raz, lista wykluczeń w serwisie
  - extra='forbid' na schematach wejściowych

"""
from __future__ import annotations

import dataclasses
import logging
import re
import unicodedata
from datetime import datetime, timezone
from typing import List, Optional

import orjson
from fastapi import APIRouter, HTTPException, Path, Request, status
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.core.dependencies import (
    DB,
    ClientIP,
    CurrentUser,
    RedisClient,
    RequestID,
    require_permission,
)
from app.schemas.common import BaseResponse

logger = logging.getLogger(__name__)
router = APIRouter()

# ---------------------------------------------------------------------------
# Stałe walidacyjne
# ---------------------------------------------------------------------------

_TABLE_PATTERN = re.compile(r"^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$")  # schema.tabela
_DATE_PATTERN  = re.compile(r"^\d{4}-\d{2}-\d{2}$")              # YYYY-MM-DD
_MAX_TABLES    = 50


# ---------------------------------------------------------------------------
# Schematy wejściowe — Pydantic v2
# ---------------------------------------------------------------------------

def _sanitize_str(v: Optional[str], max_len: int = 200) -> Optional[str]:
    """NFC normalizacja + strip + truncate."""
    if v is None:
        return None
    v = unicodedata.normalize("NFC", v.strip())
    return v[:max_len] if v else None


class CreateSnapshotRequest(BaseModel):
    """
    Żądanie utworzenia snapshotu.

    tables = None → snapshot WSZYSTKICH tabel dbo_ext (poza wykluczonymi).
    tables = [...]  → snapshot tylko wymienionych tabel.
    """
    model_config = ConfigDict(extra="forbid")

    tables: Optional[List[str]] = Field(
        default=None,
        description=(
            "Lista tabel do snapshot-owania (format: 'schema.Tabela'). "
            "Brak pola lub null = snapshot wszystkich tabel dbo_ext."
        ),
        max_length=_MAX_TABLES,
    )

    @field_validator("tables", mode="before")
    @classmethod
    def validate_tables(cls, v: Optional[list]) -> Optional[list]:
        if v is None:
            return None
        if not isinstance(v, list):
            raise ValueError("Pole 'tables' musi być tablicą stringów.")
        if len(v) > _MAX_TABLES:
            raise ValueError(f"Maksymalnie {_MAX_TABLES} tabel na raz.")
        cleaned = []
        for i, t in enumerate(v):
            if not isinstance(t, str):
                raise ValueError(f"Element [{i}] musi być stringiem.")
            t = _sanitize_str(t, 100)
            if not t:
                raise ValueError(f"Element [{i}] jest pusty.")
            if not _TABLE_PATTERN.match(t):
                raise ValueError(
                    f"Nieprawidłowy format tabeli '{t}' — wymagany: 'schema.NazwaTabeli'."
                )
            cleaned.append(t)
        return cleaned or None


class RestoreSnapshotRequest(BaseModel):
    """Żądanie restore snapshotu."""
    model_config = ConfigDict(extra="forbid")

    dry_run: bool = Field(
        default=False,
        description=(
            "true → symulacja restore (bez zapisu do bazy). "
            "false → właściwy restore (INSERT brakujących rekordów)."
        ),
    )


# ---------------------------------------------------------------------------
# Helpery
# ---------------------------------------------------------------------------

def _dataclass_to_dict(obj) -> dict:
    """Konwertuje dataclass (frozen) na słownik, rekursywnie."""
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        result = {}
        for f in dataclasses.fields(obj):
            val = getattr(obj, f.name)
            if dataclasses.is_dataclass(val) and not isinstance(val, type):
                result[f.name] = _dataclass_to_dict(val)
            elif isinstance(val, list):
                result[f.name] = [
                    _dataclass_to_dict(i) if (dataclasses.is_dataclass(i) and not isinstance(i, type)) else i
                    for i in val
                ]
            else:
                result[f.name] = str(val) if hasattr(val, "__fspath__") else val
        return result
    return obj


def _raise_from_snapshot_error(exc: Exception) -> None:
    """Mapuje wyjątki serwisu na odpowiednie HTTP responses."""
    exc_type = type(exc).__name__
    _MAP = {
        "SnapshotFileNotFoundError":  (404, "snapshots.not_found",    "Plik snapshotu nie istnieje"),
        "SnapshotTableExcludedError": (403, "snapshots.table_excluded","Tabela jest wykluczona ze snapshotowania"),
        "SnapshotTableNotFoundError": (404, "snapshots.table_missing", "Tabela nie istnieje w bazie danych"),
        "SnapshotRestoreError":       (500, "snapshots.restore_error", "Błąd podczas restore snapshotu"),
        "SnapshotError":              (500, "snapshots.error",         "Błąd operacji snapshot"),
    }
    if exc_type in _MAP:
        http_status, code, msg = _MAP[exc_type]
        raise HTTPException(
            status_code=http_status,
            detail={
                "code": code,
                "message": msg,
                "errors": [{"field": "_", "message": str(exc)[:300] or msg}],
            },
        )
    raise


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 1: POST /snapshots
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "",
    summary="Utwórz manualny snapshot",
    description=(
        "Tworzy snapshot wybranych lub wszystkich tabel `dbo_ext` do plików JSON.gz. "
        "Format pliku: `/app/snapshots/{YYYY-MM-DD}/snapshot_{table}_{timestamp}.json.gz`. "
        "\n\n**Wykluczenia:** `AuditLog`, `MasterAccessLog`, `RefreshTokens`, "
        "`OtpCodes`, `SchemaChecksums` — nie są snapshot-owane ze względów bezpieczeństwa. "
        "\n\n`tables = null` → snapshot **wszystkich** tabel (poza wykluczonymi). "
        "`tables = [...]` → snapshot tylko wymienionych, max 50. "
        "\n\nPo zakończeniu: AuditLog `snapshot_created` + SSE event. "
        "**Wymaga uprawnienia:** `snapshots.create_manual`"
    ),
    response_description="Wynik operacji snapshot z podsumowaniem per-tabela",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("snapshots.create_manual")],
    responses={
        403: {"description": "Brak uprawnienia snapshots.create_manual"},
        422: {"description": "Nieprawidłowe nazwy tabel lub przekroczono limit 50"},
        500: {"description": "Błąd podczas tworzenia snapshotu"},
    },
)
async def create_snapshot(
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
) -> BaseResponse:
    from app.services import snapshot_service

    # Parsowanie body — tolerancja na brak body (wtedy tables=None)
    try:
        raw_body = await request.body()
        if raw_body:
            body_data = orjson.loads(raw_body)
            body = CreateSnapshotRequest.model_validate(body_data)
        else:
            body = CreateSnapshotRequest()
    except orjson.JSONDecodeError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Nieprawidłowy JSON w body żądania",
                "errors": [{"field": "_", "message": "Body musi być poprawnym JSON lub puste"}],
            },
        )

    logger.warning(
        orjson.dumps({
            "event":      "api_snapshot_create_requested",
            "requested_by": current_user.id_user,
            "tables":     body.tables,
            "request_id": request_id,
            "ip":         client_ip,
            "ts":         datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    try:
        result = await snapshot_service.create(
            db=db,
            redis=redis,
            tables=body.tables,
            created_by_id=current_user.id_user,
            ip_address=client_ip,
        )
    except Exception as exc:
        _raise_from_snapshot_error(exc)

    result_dict = _dataclass_to_dict(result)

    logger.warning(
        orjson.dumps({
            "event":        "api_snapshot_created",
            "created_by":   current_user.id_user,
            "snapshot_date": result_dict.get("snapshot_date"),
            "success_count": result_dict.get("success_count"),
            "failed_count":  result_dict.get("failed_count"),
            "total_rows":    result_dict.get("total_rows"),
            "request_id":   request_id,
            "ip":           client_ip,
            "ts":           datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data=result_dict,
        app_code="snapshots.created",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 2: GET /snapshots
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "",
    summary="Lista dostępnych snapshotów",
    description=(
        "Zwraca listę dostępnych plików snapshotów (skan katalogu — bez odczytu zawartości). "
        "\n\n**Filtry:**\n"
        "- `date` — data snapshotu (YYYY-MM-DD)\n"
        "- `table` — substring nazwy tabeli (case-insensitive)\n"
        "\n**Wymaga uprawnienia:** `snapshots.create_manual`"
    ),
    response_description="Lista plików snapshotów z metadanymi",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("snapshots.create_manual")],
)
async def list_snapshots(
    current_user: CurrentUser,
    request_id: RequestID,
    date: Optional[str] = None,
    table: Optional[str] = None,
) -> BaseResponse:
    from app.services import snapshot_service

    # Walidacja daty jeśli podana
    if date and not _DATE_PATTERN.match(date):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Nieprawidłowy format daty — wymagany: YYYY-MM-DD",
                "errors": [{"field": "date", "message": "Format: YYYY-MM-DD"}],
            },
        )

    # Sanityzacja table filter
    table_filter = _sanitize_str(table, 100) if table else None

    files = snapshot_service.list_available(
        date_filter=date,
        table_filter=table_filter,
    )

    items = [_dataclass_to_dict(f) for f in files]

    return BaseResponse.ok(
        data={
            "items": items,
            "total": len(items),
            "filters": {
                "date":  date,
                "table": table_filter,
            },
        },
        app_code="snapshots.list",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 3: POST /snapshots/{date}/{table}/restore
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/{snapshot_date}/{table_name}/restore",
    summary="Przywróć tabelę ze snapshotu",
    description=(
        "Przywraca dane tabeli ze snapshotu — **UPSERT tylko brakujących rekordów**. "
        "\n\n⚠️ **RESTORE NIE NADPISUJE** istniejących rekordów. "
        "Pomija wiersze, których PK już istnieje w bazie. Dodaje tylko brakujące. "
        "\n\n`dry_run: true` → symulacja bez zapisu (bezpieczne sprawdzenie ile rekordów zostanie dodanych). "
        "\n\n**Tabele wykluczone z restore:** `AuditLog`, `MasterAccessLog`, `RefreshTokens`, "
        "`OtpCodes`, `SchemaChecksums`."
        "\n\n**Format URL:** `{date}` = YYYY-MM-DD, `{table_name}` = np. `dbo_ext.Users`"
        "\n\nPo zakończeniu: AuditLog `snapshot_restored`. "
        "**Wymaga uprawnienia:** `snapshots.restore`"
    ),
    response_description="Wynik operacji restore z liczbą wstawionych/pominiętych rekordów",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("snapshots.restore")],
    responses={
        403: {"description": "Brak uprawnienia snapshots.restore lub tabela wykluczona"},
        404: {"description": "Plik snapshotu nie istnieje"},
        422: {"description": "Nieprawidłowy format daty lub nazwy tabeli"},
        500: {"description": "Błąd podczas restore"},
    },
)
async def restore_snapshot(
    request: Request,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    client_ip: ClientIP,
    request_id: RequestID,
    snapshot_date: str = Path(
        ...,
        description="Data snapshotu (YYYY-MM-DD)",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        examples=["2026-02-27"],
    ),
    table_name: str = Path(
        ...,
        description="Nazwa tabeli (schema.Tabela, np. dbo_ext.Users)",
        min_length=3,
        max_length=100,
        examples=["dbo_ext.Users"],
    ),
) -> BaseResponse:
    from app.services import snapshot_service

    # Walidacja path params
    errors = []
    if not _DATE_PATTERN.match(snapshot_date):
        errors.append({"field": "snapshot_date", "message": "Format: YYYY-MM-DD"})

    clean_table = _sanitize_str(table_name, 100)
    if not clean_table or not _TABLE_PATTERN.match(clean_table):
        errors.append({
            "field": "table_name",
            "message": "Format: 'schema.NazwaTabeli' (np. dbo_ext.Users)",
        })

    if errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "validation.error", "message": "Błąd walidacji parametrów", "errors": errors},
        )

    # Parsowanie body — dry_run opcjonalny
    try:
        raw_body = await request.body()
        if raw_body:
            body_data = orjson.loads(raw_body)
            body = RestoreSnapshotRequest.model_validate(body_data)
        else:
            body = RestoreSnapshotRequest()
    except orjson.JSONDecodeError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "validation.error",
                "message": "Nieprawidłowy JSON w body",
                "errors": [{"field": "_", "message": "Body musi być poprawnym JSON lub puste"}],
            },
        )

    logger.warning(
        orjson.dumps({
            "event":         "api_snapshot_restore_requested",
            "requested_by":  current_user.id_user,
            "snapshot_date": snapshot_date,
            "table_name":    clean_table,
            "dry_run":       body.dry_run,
            "request_id":    request_id,
            "ip":            client_ip,
            "ts":            datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    try:
        result = await snapshot_service.restore(
            db=db,
            redis=redis,
            snapshot_date=snapshot_date,
            table_name=clean_table,
            admin_id=current_user.id_user,
            ip_address=client_ip,
            dry_run=body.dry_run,
        )
    except Exception as exc:
        _raise_from_snapshot_error(exc)

    result_dict = _dataclass_to_dict(result)

    logger.warning(
        orjson.dumps({
            "event":          "api_snapshot_restored",
            "restored_by":    current_user.id_user,
            "snapshot_date":  snapshot_date,
            "table_name":     clean_table,
            "dry_run":        body.dry_run,
            "rows_in_file":   result_dict.get("rows_in_file"),
            "rows_upserted":  result_dict.get("rows_upserted"),
            "rows_skipped":   result_dict.get("rows_skipped"),
            "request_id":     request_id,
            "ip":             client_ip,
            "ts":             datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return BaseResponse.ok(
        data=result_dict,
        app_code="snapshots.restored",
    )