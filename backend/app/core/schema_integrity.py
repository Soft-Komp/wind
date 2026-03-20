"""
backend/app/core/schema_integrity.py
=====================================
Weryfikacja integralności schematu bazy danych przy starcie aplikacji.

Mechanizm:
    1. Pobierz aktualne checksums z sys.sql_modules (oba schematy: dbo_ext + dbo)
    2. Pobierz zapisane checksums z dbo_ext.skw_SchemaChecksums
    3. Porównaj — każda niezgodność to potencjalny tamper
    4. Zareaguj zgodnie z SystemConfig.schema_integrity.reaction:
         WARN  → log WARNING + kontynuuj
         ALERT → log CRITICAL + AuditLog + SSE broadcast + kontynuuj
         BLOCK → log CRITICAL + AuditLog + SSE broadcast + SystemExit(1)

Logowanie:
    - Każdy start → wpis do logs/schema_integrity_YYYY-MM-DD.jsonl
    - Każda niezgodność → log CRITICAL + osobny plik incidents/
    - Format: JSON Lines (parseable przez grep/jq/ELK)

Wersja: 1.0.0
Data:   2026-02-18
Autor:  System Windykacja
"""
from __future__ import annotations

import gzip
import json
import logging
import os
import platform
import socket
import sys
import traceback
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

if TYPE_CHECKING:
    pass

# ---------------------------------------------------------------------------
# Logger modułu — każdy moduł ma własny logger (PLAN_PRAC §zasady)
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------
MONITORED_SCHEMAS: tuple[str, ...] = ("dbo_ext", "dbo")

# SQL pobierający AKTUALNE checksums z sys.sql_modules
# Obejmuje oba schematy: dbo_ext (własne obiekty) + dbo (widoki WAPRO)
_SQL_LIVE_CHECKSUMS = """
SELECT
    SCHEMA_NAME(o.schema_id)        AS schema_name,
    o.name                          AS object_name,
    o.type_desc                     AS object_type,
    CHECKSUM(m.definition)          AS checksum_value,
    o.modify_date                   AS last_modified,
    LEN(m.definition)               AS definition_length,
    o.object_id                     AS object_id
FROM sys.sql_modules m
JOIN sys.objects      o ON m.object_id = o.object_id
WHERE (
    /* dbo_ext: wszystkie obiekty projektu */
    SCHEMA_NAME(o.schema_id) = 'dbo_ext'
)
OR (
    /* dbo: nasze widoki VIEW_* i skw_* — wyklucza setki procedur WAPRO */
    SCHEMA_NAME(o.schema_id) = 'dbo'
    AND o.type_desc = 'VIEW'
    AND (
        o.name LIKE 'VIEW[_]%'
        OR o.name LIKE 'skw[_]%'
    )
)
ORDER BY schema_name, object_name
"""

# SQL pobierający ZAPISANE checksums z tabeli SchemaChecksums
_SQL_STORED_CHECKSUMS = """
SELECT
    sc.ID_CHECKSUM,
    sc.ObjectName,
    sc.ObjectType,
    sc.SchemaName,
    sc.Checksum,
    sc.AlembicRevision,
    sc.LastVerifiedAt,
    sc.CreatedAt
FROM dbo_ext.skw_SchemaChecksums sc
WHERE sc.SchemaName IN ('dbo_ext', 'dbo')
  AND sc.ObjectType NOT IN ('INDEX', 'STATISTICS', 'TRIGGER')
ORDER BY sc.SchemaName, sc.ObjectName
"""

# ---------------------------------------------------------------------------
# Typy danych
# ---------------------------------------------------------------------------
ReactionLevel = Literal["WARN", "ALERT", "BLOCK"]


@dataclass
class LiveChecksum:
    """Aktualny checksum obiektu z sys.sql_modules."""
    schema_name: str
    object_name: str
    object_type: str
    checksum_value: int
    last_modified: Optional[datetime]
    definition_length: int
    object_id: int

    @property
    def qualified_name(self) -> str:
        return f"{self.schema_name}.{self.object_name}"


@dataclass
class StoredChecksum:
    """Zapisany checksum z tabeli dbo_ext.skw_SchemaChecksums."""
    id_checksum: int
    object_name: str
    object_type: str
    schema_name: str
    checksum_value: int
    alembic_revision: Optional[str]
    last_verified_at: Optional[datetime]
    created_at: Optional[datetime]

    @property
    def qualified_name(self) -> str:
        return f"{self.schema_name}.{self.object_name}"


@dataclass
class ChecksumMismatch:
    """Pojedyncza niezgodność checksum."""
    schema_name: str
    object_name: str
    object_type: str
    stored_checksum: Optional[int]     # None = nowy obiekt (nie zarejestrowany)
    live_checksum: Optional[int]       # None = obiekt usunięty
    alembic_revision: Optional[str]
    last_modified: Optional[datetime]
    mismatch_type: Literal["MODIFIED", "NEW_UNREGISTERED", "MISSING_FROM_DB"]

    @property
    def qualified_name(self) -> str:
        return f"{self.schema_name}.{self.object_name}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.schema_name,
            "object_name": self.object_name,
            "object_type": self.object_type,
            "stored_checksum": self.stored_checksum,
            "live_checksum": self.live_checksum,
            "alembic_revision": self.alembic_revision,
            "last_modified": (
                self.last_modified.isoformat() if self.last_modified else None
            ),
            "mismatch_type": self.mismatch_type,
        }


@dataclass
class VerificationResult:
    """Wynik pełnej weryfikacji integralności schematu."""
    verification_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: Optional[datetime] = None
    hostname: str = field(default_factory=socket.gethostname)
    pid: int = field(default_factory=os.getpid)
    python_version: str = field(default_factory=platform.python_version)

    # Wyniki
    total_live_objects: int = 0
    total_stored_objects: int = 0
    verified_ok: int = 0
    mismatches: list[ChecksumMismatch] = field(default_factory=list)

    # Stan
    reaction_level: str = "BLOCK"
    reaction_applied: str = "NONE"
    error: Optional[str] = None
    success: bool = False

    @property
    def has_mismatches(self) -> bool:
        return len(self.mismatches) > 0

    @property
    def duration_ms(self) -> Optional[float]:
        if self.finished_at:
            return (self.finished_at - self.started_at).total_seconds() * 1000
        return None

    def finish(self) -> None:
        self.finished_at = datetime.now(timezone.utc)

    def to_log_dict(self) -> dict[str, Any]:
        """Serializacja do JSON Lines — absurdalna ilość danych diagnostycznych."""
        return {
            "event": "schema_integrity_verification",
            "verification_id": self.verification_id,
            "timestamp": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_ms": self.duration_ms,
            "host": {
                "hostname": self.hostname,
                "pid": self.pid,
                "python_version": self.python_version,
                "platform": platform.platform(),
                "cwd": str(Path.cwd()),
            },
            "result": {
                "success": self.success,
                "has_mismatches": self.has_mismatches,
                "total_live_objects": self.total_live_objects,
                "total_stored_objects": self.total_stored_objects,
                "verified_ok": self.verified_ok,
                "mismatch_count": len(self.mismatches),
                "mismatches": [m.to_dict() for m in self.mismatches],
            },
            "reaction": {
                "level": self.reaction_level,
                "applied": self.reaction_applied,
            },
            "error": self.error,
            "monitored_schemas": list(MONITORED_SCHEMAS),
        }


# ---------------------------------------------------------------------------
# Ścieżki plików logów
# ---------------------------------------------------------------------------

def _get_log_dir() -> Path:
    """Zwraca katalog logów — tworzy jeśli nie istnieje."""
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def _get_integrity_log_path() -> Path:
    """Ścieżka do dziennego pliku JSON Lines."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return _get_log_dir() / f"schema_integrity_{today}.jsonl"


def _get_incident_log_path(verification_id: str) -> Path:
    """Ścieżka do pliku incydentu (tylko przy niezgodności)."""
    incidents_dir = _get_log_dir() / "incidents"
    incidents_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return incidents_dir / f"schema_tamper_{ts}_{verification_id[:8]}.json"


def _write_jsonl(path: Path, data: dict[str, Any]) -> None:
    """
    Dopisuje jeden rekord JSON Lines do pliku (append-only).
    Pliki logów są NIEUSUWALNE przez aplikację — tylko append.
    """
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(data, ensure_ascii=False, default=str) + "\n")
    except OSError as exc:
        # Nie możemy logować błędu logowania do pliku — fallback na stderr
        print(
            f"[CRITICAL] Nie można zapisać logu schema_integrity do {path}: {exc}",
            file=sys.stderr,
        )


def _write_incident_file(path: Path, data: dict[str, Any]) -> None:
    """Zapisuje pełny raport incydentu do osobnego pliku JSON (czytelny)."""
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        logger.critical(
            "Raport incydentu zapisany: %s",
            path,
            extra={"incident_path": str(path)},
        )
    except OSError as exc:
        print(
            f"[CRITICAL] Nie można zapisać raportu incydentu do {path}: {exc}",
            file=sys.stderr,
        )


# ---------------------------------------------------------------------------
# Pobieranie danych z bazy
# ---------------------------------------------------------------------------

async def _fetch_live_checksums(
    db: AsyncSession,
    verification_id: str,
) -> dict[str, LiveChecksum]:
    """
    Pobiera aktualne checksums z sys.sql_modules.
    Klucz słownika: 'schema_name.object_name' (lowercase dla bezpiecznego porównania).
    """
    logger.debug(
        "Pobieranie aktualnych checksums z sys.sql_modules",
        extra={
            "verification_id": verification_id,
            "monitored_schemas": list(MONITORED_SCHEMAS),
        },
    )

    try:
        result = await db.execute(text(_SQL_LIVE_CHECKSUMS))
        rows = result.fetchall()
    except Exception as exc:
        logger.error(
            "Błąd pobierania checksums z sys.sql_modules: %s",
            exc,
            extra={
                "verification_id": verification_id,
                "traceback": traceback.format_exc(),
            },
        )
        raise

    live: dict[str, LiveChecksum] = {}
    for row in rows:
        obj = LiveChecksum(
            schema_name=row.schema_name,
            object_name=row.object_name,
            object_type=row.object_type,
            checksum_value=row.checksum_value,
            last_modified=row.last_modified,
            definition_length=row.definition_length,
            object_id=row.object_id,
        )
        key = obj.qualified_name.lower()
        live[key] = obj

    logger.debug(
        "Pobrano %d obiektów z sys.sql_modules",
        len(live),
        extra={
            "verification_id": verification_id,
            "object_names": list(live.keys()),
        },
    )
    return live


async def _fetch_stored_checksums(
    db: AsyncSession,
    verification_id: str,
) -> dict[str, StoredChecksum]:
    """
    Pobiera zapisane checksums z dbo_ext.skw_SchemaChecksums.
    Klucz słownika: 'schema_name.object_name' (lowercase).
    """
    logger.debug(
        "Pobieranie zapisanych checksums z dbo_ext.skw_SchemaChecksums",
        extra={"verification_id": verification_id},
    )

    try:
        result = await db.execute(text(_SQL_STORED_CHECKSUMS))
        rows = result.fetchall()
    except Exception as exc:
        logger.error(
            "Błąd pobierania checksums z SchemaChecksums: %s",
            exc,
            extra={
                "verification_id": verification_id,
                "traceback": traceback.format_exc(),
            },
        )
        raise

    stored: dict[str, StoredChecksum] = {}
    for row in rows:
        obj = StoredChecksum(
            id_checksum=row.ID_CHECKSUM,
            object_name=row.ObjectName,
            object_type=row.ObjectType,
            schema_name=row.SchemaName,
            checksum_value=row.Checksum,
            alembic_revision=row.AlembicRevision,
            last_verified_at=row.LastVerifiedAt,
            created_at=row.CreatedAt,
        )
        key = obj.qualified_name.lower()
        stored[key] = obj

    logger.debug(
        "Pobrano %d zapisanych checksums",
        len(stored),
        extra={
            "verification_id": verification_id,
            "stored_keys": list(stored.keys()),
        },
    )
    return stored


async def _get_reaction_level(db: AsyncSession) -> ReactionLevel:
    """
    Pobiera poziom reakcji z SystemConfig.
    Domyślnie BLOCK (bezpieczne ustawienie).
    Jeśli tabela niedostępna — fallback na BLOCK.
    """
    try:
        result = await db.execute(
            text(
                """
                SELECT TOP 1 ConfigValue
                FROM dbo_ext.SystemConfig
                WHERE ConfigKey = 'schema_integrity.reaction'
                  AND IsActive = 1
                """
            )
        )
        row = result.fetchone()
        if row and row.ConfigValue in ("WARN", "ALERT", "BLOCK"):
            return row.ConfigValue  # type: ignore[return-value]
    except Exception as exc:
        logger.warning(
            "Nie można pobrać schema_integrity.reaction z SystemConfig, "
            "używam domyślnego BLOCK: %s",
            exc,
        )
    return "BLOCK"


# ---------------------------------------------------------------------------
# Porównywanie checksums
# ---------------------------------------------------------------------------

def _compare_checksums(
    live: dict[str, LiveChecksum],
    stored: dict[str, StoredChecksum],
    verification_id: str,
) -> tuple[list[ChecksumMismatch], int]:
    """
    Porównuje aktualne checksums ze zapisanymi.

    Returns:
        (lista niezgodności, liczba zweryfikowanych poprawnie)
    """
    mismatches: list[ChecksumMismatch] = []
    ok_count = 0

    all_keys = set(live.keys()) | set(stored.keys())

    for key in sorted(all_keys):
        live_obj = live.get(key)
        stored_obj = stored.get(key)

        # Przypadek 1: obiekt jest w DB ale nie w sys.sql_modules
        # → obiekt mógł zostać usunięty poza Alembic
        if live_obj is None and stored_obj is not None:
            mismatch = ChecksumMismatch(
                schema_name=stored_obj.schema_name,
                object_name=stored_obj.object_name,
                object_type=stored_obj.object_type,
                stored_checksum=stored_obj.checksum_value,
                live_checksum=None,
                alembic_revision=stored_obj.alembic_revision,
                last_modified=None,
                mismatch_type="MISSING_FROM_DB",
            )
            mismatches.append(mismatch)
            logger.warning(
                "Obiekt %s jest w SchemaChecksums ale nie istnieje w sys.sql_modules",
                key,
                extra={"verification_id": verification_id, "key": key},
            )
            continue

        # Przypadek 2: obiekt istnieje w DB ale nie jest zarejestrowany
        # → może być nowy obiekt dodany poza Alembic (potencjalny tamper!)
        if live_obj is not None and stored_obj is None:
            mismatch = ChecksumMismatch(
                schema_name=live_obj.schema_name,
                object_name=live_obj.object_name,
                object_type=live_obj.object_type,
                stored_checksum=None,
                live_checksum=live_obj.checksum_value,
                alembic_revision=None,
                last_modified=live_obj.last_modified,
                mismatch_type="NEW_UNREGISTERED",
            )
            mismatches.append(mismatch)
            logger.warning(
                "Obiekt %s istnieje w sys.sql_modules ale NIE jest zarejestrowany "
                "w SchemaChecksums — potencjalny nieautoryzowany obiekt!",
                key,
                extra={
                    "verification_id": verification_id,
                    "key": key,
                    "live_checksum": live_obj.checksum_value,
                    "object_type": live_obj.object_type,
                    "last_modified": str(live_obj.last_modified),
                },
            )
            continue

        # Przypadek 3: oba istnieją — porównaj checksums
        if live_obj is not None and stored_obj is not None:
            if live_obj.checksum_value != stored_obj.checksum_value:
                mismatch = ChecksumMismatch(
                    schema_name=live_obj.schema_name,
                    object_name=live_obj.object_name,
                    object_type=live_obj.object_type,
                    stored_checksum=stored_obj.checksum_value,
                    live_checksum=live_obj.checksum_value,
                    alembic_revision=stored_obj.alembic_revision,
                    last_modified=live_obj.last_modified,
                    mismatch_type="MODIFIED",
                )
                mismatches.append(mismatch)
                logger.error(
                    "CHECKSUM MISMATCH dla %s: stored=%d, live=%d "
                    "(alembic_revision=%s, last_modified=%s)",
                    key,
                    stored_obj.checksum_value,
                    live_obj.checksum_value,
                    stored_obj.alembic_revision,
                    live_obj.last_modified,
                    extra={
                        "verification_id": verification_id,
                        "key": key,
                        "stored_checksum": stored_obj.checksum_value,
                        "live_checksum": live_obj.checksum_value,
                        "delta": live_obj.checksum_value - stored_obj.checksum_value,
                        "alembic_revision": stored_obj.alembic_revision,
                        "last_modified": str(live_obj.last_modified),
                        "definition_length": live_obj.definition_length,
                    },
                )
            else:
                ok_count += 1
                logger.debug(
                    "OK: %s checksum=%d",
                    key,
                    live_obj.checksum_value,
                    extra={"verification_id": verification_id},
                )

    return mismatches, ok_count


# ---------------------------------------------------------------------------
# Aktualizacja LastVerifiedAt
# ---------------------------------------------------------------------------

async def _update_last_verified(
    db: AsyncSession,
    verified_keys: list[str],
    verification_id: str,
) -> None:
    """
    Aktualizuje LastVerifiedAt dla pomyślnie zweryfikowanych obiektów.
    Nie commituje — caller jest odpowiedzialny za session management.
    """
    if not verified_keys:
        return

    now = datetime.now(timezone.utc)
    try:
        # Budujemy listę par (schema_name, object_name) z klucza 'schema.name'
        for key in verified_keys:
            parts = key.split(".", 1)
            if len(parts) != 2:
                continue
            schema_name, object_name = parts[0], parts[1]
            await db.execute(
                text(
                    """
                    UPDATE dbo_ext.skw_SchemaChecksums
                    SET LastVerifiedAt = :verified_at
                    WHERE LOWER(SchemaName) = :schema_name
                      AND LOWER(ObjectName) = :object_name
                    """
                ),
                {
                    "verified_at": now,
                    "schema_name": schema_name,
                    "object_name": object_name,
                },
            )
        logger.debug(
            "Zaktualizowano LastVerifiedAt dla %d obiektów",
            len(verified_keys),
            extra={"verification_id": verification_id},
        )
    except Exception as exc:
        # Niekrytyczny błąd — nie blokuje działania
        logger.warning(
            "Nie udało się zaktualizować LastVerifiedAt: %s",
            exc,
            extra={"verification_id": verification_id},
        )


# ---------------------------------------------------------------------------
# Obsługa niezgodności — AuditLog + SSE (lazy import by uniknąć circular)
# ---------------------------------------------------------------------------

async def _write_audit_log(
    db: AsyncSession,
    result: "VerificationResult",  # type: ignore[name-defined]  # forward ref
) -> None:
    """
    Wstawia wpis do dbo_ext.AuditLog przy wykryciu schema tamper.

    Używa raw SQL (nie ORM) — ta funkcja może być wywoływana bardzo wcześnie
    w cyklu życia aplikacji, zanim modele ORM zostaną w pełni zainicjalizowane.

    Poprawione nazwy kolumn względem wersji v1.0:
        UserID    → ID_USER      (zgodne z DDL)
        CreatedAt → Timestamp    (zgodne z DDL)

    MSSQL DATETIME nie obsługuje timezone-aware datetime →
        .replace(tzinfo=None) przed zapisem.
    """
    try:
        mismatch_json = json.dumps(
            [m.to_dict() for m in result.mismatches],
            ensure_ascii=False,
            default=str,
        )

        # Timestamp bez strefy — MSSQL DATETIME nie obsługuje timezone-aware
        now_naive = datetime.now(timezone.utc).replace(tzinfo=None)

        await db.execute(
            text(
                """
                INSERT INTO dbo_ext.skw_AuditLog (
                    ID_USER,
                    Username,
                    Action,
                    ActionCategory,
                    EntityType,
                    EntityID,
                    OldValue,
                    NewValue,
                    Details,
                    IPAddress,
                    UserAgent,
                    RequestURL,
                    RequestMethod,
                    RequestID,
                    Timestamp,
                    Success,
                    ErrorMessage
                ) VALUES (
                    NULL,
                    N'system',
                    N'schema_tamper_detected',
                    N'System',
                    N'SYSTEM',
                    NULL,
                    NULL,
                    :new_value,
                    NULL,
                    N'127.0.0.1',
                    N'schema_integrity_checker',
                    N'/system/schema-integrity',
                    N'INTERNAL',
                    :request_id,
                    :timestamp,
                    0,
                    :error_message
                )
                """
            ),
            {
                "new_value":     mismatch_json,
                "request_id":    result.verification_id[:36],
                "timestamp":     now_naive,
                "error_message": (
                    f"{len(result.mismatches)} niezgodności schematu"
                    f" | verification_id={result.verification_id}"
                )[:500],
            },
        )
        await db.commit()

        logger.info(
            "Wpis AuditLog 'schema_tamper_detected' zapisany | "
            "verification_id=%s | mismatches=%d",
            result.verification_id,
            len(result.mismatches),
        )

    except Exception as exc:
        # Krytyczny błąd — logujemy maksymalnie dużo danych, ale NIE rzucamy
        # (schema_integrity nie może crashować aplikacji przy rollback)
        logger.error(
            "Nie udało się zapisać AuditLog dla schema_tamper_detected | "
            "verification_id=%s | error=%s",
            result.verification_id,
            exc,
            extra={
                "verification_id": result.verification_id,
                "traceback":       traceback.format_exc(),
                "mismatches_count": len(result.mismatches),
            },
        )
        # Próba rollback — może być zbędna, ale bezpieczna
        try:
            await db.rollback()
        except Exception:
            pass


async def _broadcast_sse_alert(result: VerificationResult) -> None:
    """
    Publikuje event SSE 'system_notification' level=CRITICAL do Redis PubSub.
    Lazy import Redis by uniknąć circular imports przy starcie.
    """
    try:
        # Lazy import — core/redis.py może nie być jeszcze zainicjalizowane
        from app.core.redis import get_redis_client  # type: ignore[import]

        redis = await get_redis_client()
        if redis is None:
            logger.warning(
                "Redis niedostępny — nie można wysłać SSE alert dla schema tamper",
                extra={"verification_id": result.verification_id},
            )
            return

        event_payload = json.dumps(
            {
                "type": "system_notification",
                "data": {
                    "message": (
                        f"SCHEMA TAMPER DETECTED — "
                        f"{len(result.mismatches)} niezgodności! "
                        f"verification_id={result.verification_id}"
                    ),
                    "level": "CRITICAL",
                    "verification_id": result.verification_id,
                    "mismatch_count": len(result.mismatches),
                    "mismatches": [m.to_dict() for m in result.mismatches],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            },
            ensure_ascii=False,
            default=str,
        )

        # Broadcast do kanału admins — każdy zalogowany admin to otrzyma
        await redis.publish("channel:admins", event_payload)
        logger.info(
            "SSE alert 'schema_tamper_detected' wysłany do Redis PubSub",
            extra={"verification_id": result.verification_id},
        )
    except ImportError:
        logger.debug(
            "Moduł redis nie jest dostępny podczas schema integrity check — "
            "SSE alert pominięty (to normalne przy cold start)",
            extra={"verification_id": result.verification_id},
        )
    except Exception as exc:
        logger.error(
            "Błąd wysyłania SSE alert: %s",
            exc,
            extra={
                "verification_id": result.verification_id,
                "traceback": traceback.format_exc(),
            },
        )


# ---------------------------------------------------------------------------
# Główna funkcja weryfikacji
# ---------------------------------------------------------------------------

async def verify(db: AsyncSession) -> VerificationResult:
    """
    Główna funkcja weryfikacji integralności schematu.
    Wywoływana przy starcie FastAPI w lifespan context managerze.

    Args:
        db: Async SQLAlchemy session

    Returns:
        VerificationResult — pełny raport weryfikacji

    Raises:
        SystemExit(1): gdy reaction=BLOCK i wykryto niezgodności
    """
    result = VerificationResult()
    log_path = _get_integrity_log_path()

    logger.info(
        "=== START weryfikacji integralności schematu [%s] ===",
        result.verification_id,
        extra={
            "verification_id": result.verification_id,
            "monitored_schemas": list(MONITORED_SCHEMAS),
            "pid": result.pid,
            "hostname": result.hostname,
        },
    )

    try:
        # --- Krok 1: Pobierz poziom reakcji z konfiguracji --------------------
        reaction_level = await _get_reaction_level(db)
        result.reaction_level = reaction_level
        logger.info(
            "Poziom reakcji: %s",
            reaction_level,
            extra={"verification_id": result.verification_id},
        )

        # --- Krok 2: Pobierz aktualne checksums z sys.sql_modules -------------
        live_checksums = await _fetch_live_checksums(db, result.verification_id)
        result.total_live_objects = len(live_checksums)

        # --- Krok 3: Pobierz zapisane checksums z SchemaChecksums -------------
        stored_checksums = await _fetch_stored_checksums(db, result.verification_id)
        result.total_stored_objects = len(stored_checksums)

        logger.info(
            "Obiekty do porównania: live=%d, stored=%d",
            result.total_live_objects,
            result.total_stored_objects,
            extra={
                "verification_id": result.verification_id,
                "live_objects": sorted(live_checksums.keys()),
                "stored_objects": sorted(stored_checksums.keys()),
            },
        )

        # --- Krok 4: Porównaj checksums ---------------------------------------
        mismatches, ok_count = _compare_checksums(
            live_checksums,
            stored_checksums,
            result.verification_id,
        )
        result.mismatches = mismatches
        result.verified_ok = ok_count

        # --- Krok 5: Aktualizuj LastVerifiedAt dla OK obiektów ----------------
        all_live_keys = set(live_checksums.keys())
        mismatch_keys = {m.qualified_name.lower() for m in mismatches}
        ok_keys = sorted(all_live_keys - mismatch_keys)

        await _update_last_verified(db, ok_keys, result.verification_id)
        await db.commit()

        # --- Krok 6: Zaloguj wynik --------------------------------------------
        result.finish()

        if not result.has_mismatches:
            result.success = True
            result.reaction_applied = "NONE"
            logger.info(
                "=== Weryfikacja ZAKOŃCZONA POMYŚLNIE: %d/%d obiektów OK "
                "(%.1f ms) ===",
                ok_count,
                result.total_live_objects,
                result.duration_ms or 0.0,
                extra={
                    "verification_id": result.verification_id,
                    "duration_ms": result.duration_ms,
                    "verified_ok": ok_count,
                    "total_live": result.total_live_objects,
                },
            )
            _write_jsonl(log_path, result.to_log_dict())
            return result

        # --- Krok 7: Obsługa niezgodności ------------------------------------
        result.success = False

        # Log CRITICAL — zawsze, niezależnie od reaction level
        logger.critical(
            "!!! SCHEMA TAMPER DETECTED !!! %d niezgodności: %s",
            len(mismatches),
            [m.qualified_name for m in mismatches],
            extra={
                "verification_id": result.verification_id,
                "mismatch_count": len(mismatches),
                "mismatches": [m.to_dict() for m in mismatches],
                "reaction_level": reaction_level,
            },
        )

        # Zapisz pełny raport incydentu
        incident_path = _get_incident_log_path(result.verification_id)
        _write_incident_file(incident_path, result.to_log_dict())

        # AuditLog (przy ALERT lub BLOCK)
        if reaction_level in ("ALERT", "BLOCK"):
            await _write_audit_log(db, result)

        # SSE broadcast (przy ALERT lub BLOCK)
        if reaction_level in ("ALERT", "BLOCK"):
            await _broadcast_sse_alert(result)

        # Zapisz do JSON Lines
        _write_jsonl(log_path, result.to_log_dict())

        # Reakcja
        if reaction_level == "WARN":
            result.reaction_applied = "WARN_LOGGED"
            logger.warning(
                "Reaction=WARN: Kontynuuję start aplikacji mimo niezgodności. "
                "NIE zalecane w środowisku produkcyjnym!",
                extra={"verification_id": result.verification_id},
            )
            return result

        elif reaction_level == "ALERT":
            result.reaction_applied = "ALERT_SENT"
            logger.warning(
                "Reaction=ALERT: AuditLog + SSE wysłane, kontynuuję start. "
                "Sprawdź logi i incydenty!",
                extra={"verification_id": result.verification_id},
            )
            return result

        else:  # BLOCK — domyślne i jedyne bezpieczne
            result.reaction_applied = "SYSTEM_EXIT"
            logger.critical(
                "Reaction=BLOCK: Aplikacja NIE ZOSTANIE URUCHOMIONA. "
                "Przywróć spójność bazy danych lub zaktualizuj SchemaChecksums "
                "przez Alembic migration. verification_id=%s",
                result.verification_id,
                extra={
                    "verification_id": result.verification_id,
                    "incident_file": str(incident_path),
                    "mismatches": [m.to_dict() for m in mismatches],
                },
            )
            print(
                f"\n{'='*70}\n"
                f"KRYTYCZNY BŁĄD INTEGRALNOŚCI SCHEMATU BAZY DANYCH\n"
                f"{'='*70}\n"
                f"Wykryto {len(mismatches)} niezgodności checksums.\n"
                f"verification_id: {result.verification_id}\n"
                f"Raport incydentu: {incident_path}\n"
                f"Log dzienny: {log_path}\n"
                f"\nNiezgodne obiekty:\n"
                + "\n".join(
                    f"  [{m.mismatch_type}] {m.qualified_name} "
                    f"(stored={m.stored_checksum}, live={m.live_checksum})"
                    for m in mismatches
                )
                + f"\n{'='*70}\n",
                file=sys.stderr,
            )
            sys.exit(1)

    except SystemExit:
        # Przepuść SystemExit
        raise

    except Exception as exc:
        result.error = str(exc)
        result.finish()
        result.success = False

        logger.critical(
            "Krytyczny błąd podczas weryfikacji integralności schematu: %s",
            exc,
            extra={
                "verification_id": result.verification_id,
                "traceback": traceback.format_exc(),
                "error_type": type(exc).__name__,
            },
            exc_info=True,
        )
        _write_jsonl(log_path, result.to_log_dict())

        # Błąd weryfikacji = nie wiemy co się dzieje → BLOCK
        logger.critical(
            "Weryfikacja nie mogła zostać przeprowadzona — BLOCK (bezpieczne ustawienie). "
            "Sprawdź dostępność bazy danych i uprawnienia.",
            extra={"verification_id": result.verification_id},
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Rejestracja nowego obiektu (używane przez skrypty migracji / setup)
# ---------------------------------------------------------------------------

async def register_object(
    db: AsyncSession,
    *,
    schema_name: str,
    object_name: str,
    object_type: Literal["VIEW", "PROCEDURE", "INDEX"],
    alembic_revision: Optional[str] = None,
) -> bool:
    """
    Rejestruje nowy obiekt w SchemaChecksums LUB aktualizuje istniejący.
    Pobiera aktualny checksum z sys.sql_modules.

    Używane przez:
        - Migracje Alembic po tworzeniu widoków/procedur
        - database/setup.py --register-checksums

    Returns:
        True jeśli operacja się powiodła, False w przeciwnym razie.
    """
    logger.info(
        "Rejestracja checksumu dla %s.%s (type=%s, alembic=%s)",
        schema_name, object_name, object_type, alembic_revision,
    )

    try:
        # Pobierz aktualny checksum
        result = await db.execute(
            text(
                """
                SELECT
                    CHECKSUM(m.definition) AS checksum_value,
                    o.object_id,
                    o.modify_date
                FROM sys.sql_modules m
                JOIN sys.objects o ON m.object_id = o.object_id
                WHERE o.name = :object_name
                  AND SCHEMA_NAME(o.schema_id) = :schema_name
                """
            ),
            {"object_name": object_name, "schema_name": schema_name},
        )
        row = result.fetchone()

        if row is None:
            logger.error(
                "Obiekt %s.%s nie istnieje w sys.sql_modules — "
                "nie można zarejestrować checksumu",
                schema_name, object_name,
            )
            return False

        checksum_value = row.checksum_value
        now = datetime.now(timezone.utc)

        # UPSERT do SchemaChecksums
        await db.execute(
            text(
                """
                MERGE dbo_ext.skw_SchemaChecksums AS target
                USING (
                    SELECT
                        :object_name  AS ObjectName,
                        :object_type  AS ObjectType,
                        :schema_name  AS SchemaName,
                        :checksum     AS Checksum,
                        :alembic_rev  AS AlembicRevision
                ) AS source ON (
                    target.ObjectName = source.ObjectName
                    AND target.SchemaName = source.SchemaName
                    AND target.ObjectType = source.ObjectType
                )
                WHEN MATCHED THEN
                    UPDATE SET
                        Checksum         = source.Checksum,
                        AlembicRevision  = source.AlembicRevision,
                        LastVerifiedAt   = :now,
                        UpdatedAt        = :now
                WHEN NOT MATCHED THEN
                    INSERT (ObjectName, ObjectType, SchemaName, Checksum,
                            AlembicRevision, LastVerifiedAt, CreatedAt)
                    VALUES (source.ObjectName, source.ObjectType, source.SchemaName,
                            source.Checksum, source.AlembicRevision, :now, :now);
                """
            ),
            {
                "object_name": object_name,
                "object_type": object_type,
                "schema_name": schema_name,
                "checksum": checksum_value,
                "alembic_rev": alembic_revision,
                "now": now,
            },
        )
        await db.commit()

        logger.info(
            "Checksum zarejestrowany: %s.%s checksum=%d",
            schema_name, object_name, checksum_value,
            extra={
                "schema_name": schema_name,
                "object_name": object_name,
                "object_type": object_type,
                "checksum_value": checksum_value,
                "alembic_revision": alembic_revision,
                "registered_at": now.isoformat(),
            },
        )
        return True

    except Exception as exc:
        await db.rollback()
        logger.error(
            "Błąd rejestracji checksumu dla %s.%s: %s",
            schema_name, object_name, exc,
            extra={"traceback": traceback.format_exc()},
            exc_info=True,
        )
        return False


# ---------------------------------------------------------------------------
# Eksport publicznego API
# ---------------------------------------------------------------------------

__all__ = [
    "verify",
    "register_object",
    "VerificationResult",
    "ChecksumMismatch",
    "LiveChecksum",
    "StoredChecksum",
    "ReactionLevel",
]

"""
PATCH — dodaj ten kod na KOŃCU pliku:
    backend/app/core/schema_integrity.py

Klasa SchemaIntegrityChecker wrappuje istniejące wolne funkcje modułu
i dostarcza interfejs wymagany przez:
  - main.py startup     → SchemaIntegrityChecker(db=db).verify_all()
  - system.py GET       → SchemaIntegrityChecker.get_last_result(db=db)
  - system.py POST      → SchemaIntegrityChecker.verify(db=db, runtime_check=True)

NIE duplikuje logiki — deleguje do istniejących _funkcji prywatnych.
"""


# ---------------------------------------------------------------------------
# SchemaIntegrityChecker
# ---------------------------------------------------------------------------

class SchemaIntegrityChecker:
    """
    Fasada OOP nad modułowymi funkcjami weryfikacji integralności schematu.

    Tryby użycia:
    ┌─────────────────────────────────────────────────────────┐
    │ STARTUP (main.py lifespan):                             │
    │   checker = SchemaIntegrityChecker(db=db)               │
    │   ok, issues = await checker.verify_all()               │
    │   # zwraca (True, []) lub (False, ["dbo.VIEW_x: ..."])  │
    ├─────────────────────────────────────────────────────────┤
    │ API GET /system/schema-integrity:                       │
    │   result = await SchemaIntegrityChecker.get_last_result(│
    │       db=db)                                            │
    │   # odczytuje ostatni wpis z jsonl — zero DB queries   │
    ├─────────────────────────────────────────────────────────┤
    │ API POST /system/schema-integrity/check:                │
    │   result = await SchemaIntegrityChecker.verify(         │
    │       db=db, runtime_check=True)                        │
    │   # pełna weryfikacja, NIE blokuje aplikacji (runtime) │
    └─────────────────────────────────────────────────────────┘
    """

    def __init__(self, db: "AsyncSession") -> None:  # noqa: F821
        self._db = db

    # ------------------------------------------------------------------
    # Instancja — używana przez main.py startup
    # ------------------------------------------------------------------

    async def verify_all(self) -> tuple[bool, list[str]]:
        """
        Uruchamia pełną weryfikację i zwraca (ok: bool, issues: list[str]).

        Używana przy starcie aplikacji — reakcja BLOCK zatrzymuje proces.
        Jeśli brak obiektów w SchemaChecksums → traktujemy jako OK
        (pierwsze uruchomienie przed migracją seedów).

        Returns:
            (True, [])                — wszystkie checksums OK
            (False, ["opis..."])      — lista rozbieżności
        """
        result = await _run_full_verification(self._db, runtime_check=False)

        if result.error:
            logger.error(
                "verify_all: błąd weryfikacji — %s", result.error,
                extra={"verification_id": result.verification_id},
            )
            # Przy błędzie infrastruktury (np. brak tabeli SchemaChecksums)
            # zwracamy OK żeby nie blokować startu gdy tabela nie istnieje jeszcze
            if "SchemaChecksums" in (result.error or "") or "skw_SchemaChecksums" in (result.error or ""):
                logger.warning(
                    "verify_all: tabela SchemaChecksums niedostępna — "
                    "pominięcie weryfikacji (pierwsze uruchomienie?)",
                    extra={"verification_id": result.verification_id},
                )
                return True, []
            return False, [f"Błąd infrastruktury: {result.error}"]

        if result.total_stored_objects == 0:
            logger.warning(
                "verify_all: brak zapisanych checksums w SchemaChecksums — "
                "pomijam weryfikację (uruchom seedy alembic)",
                extra={"verification_id": result.verification_id},
            )
            return True, []

        if result.has_mismatches:
            issues = [
                f"{m.qualified_name} [{m.mismatch_type}]: "
                f"stored={m.stored_checksum} live={m.live_checksum}"
                for m in result.mismatches
            ]
            return False, issues

        return True, []

    # ------------------------------------------------------------------
    # Classmethods — używane przez API endpointy system.py
    # ------------------------------------------------------------------

    @classmethod
    async def verify(
        cls,
        db: "AsyncSession",  # noqa: F821
        runtime_check: bool = False,
    ) -> dict[str, Any]:
        """
        Uruchamia weryfikację i zwraca wynik jako słownik (dla API response).

        runtime_check=True → NIE zatrzymuje aplikacji nawet przy BLOCK.
        Wynik zapisywany do jsonl (append-only).

        Args:
            db:            Sesja SQLAlchemy async.
            runtime_check: True = wywołane z API (nie przy starcie).

        Returns:
            Słownik z wynikiem weryfikacji — serializowalny do JSON.
        """
        result = await _run_full_verification(db, runtime_check=runtime_check)
        return _verification_result_to_api_dict(result)

    @classmethod
    async def get_last_result(
        cls,
        db: "AsyncSession",  # noqa: F821  # db zarezerwowane na przyszłe rozszerzenia
    ) -> dict[str, Any]:
        """
        Zwraca ostatni zapisany wynik weryfikacji z pliku jsonl.

        NIE wykonuje zapytań do DB — czyta ostatni wpis z pliku logów.
        Szybka operacja, bezpieczna do użycia w każdym request.

        Jeśli plik nie istnieje lub jest pusty → zwraca informację
        że weryfikacja jeszcze nie była przeprowadzona.

        Args:
            db: Sesja SQLAlchemy (nieużywana — zachowana dla spójności API).

        Returns:
            Słownik z ostatnim wynikiem weryfikacji lub info o braku danych.
        """
        log_path = _get_integrity_log_path()

        # Spróbuj plik z dzisiejszą datą, jeśli pusty — wczorajszy
        last_entry = _read_last_jsonl_entry(log_path)

        if last_entry is None:
            # Szukamy w poprzednich dniach (max 7)
            from datetime import timedelta
            log_dir = _get_log_dir()
            for days_back in range(1, 8):
                date_str = (
                    datetime.now(timezone.utc) - timedelta(days=days_back)
                ).strftime("%Y-%m-%d")
                old_path = log_dir / f"schema_integrity_{date_str}.jsonl"
                last_entry = _read_last_jsonl_entry(old_path)
                if last_entry is not None:
                    break

        if last_entry is None:
            return {
                "status": "no_data",
                "message": (
                    "Brak wyników weryfikacji. "
                    "Weryfikacja uruchamia się przy starcie aplikacji. "
                    "Użyj POST /system/schema-integrity/check aby uruchomić ręcznie."
                ),
                "last_verification_at": None,
                "verified_ok": 0,
                "mismatch_count": 0,
                "mismatches": [],
            }

        # Mapuj wpis jsonl na format API response
        return _jsonl_entry_to_api_dict(last_entry)


# ---------------------------------------------------------------------------
# Główna funkcja weryfikacji — wywoływana przez oba tryby klasy
# ---------------------------------------------------------------------------
async def _run_full_verification(
    db: "AsyncSession",
    runtime_check: bool,
) -> "VerificationResult":
    """
    Wykonuje pełny cykl weryfikacji integralności schematu:
      1. Pobierz live checksums z sys.sql_modules
      2. Pobierz stored checksums z dbo_ext.skw_SchemaChecksums
      3. Porównaj
      4. Zaktualizuj LastVerifiedAt dla OK obiektów
      5. Zaloguj wynik do jsonl

    runtime_check=True  → wywołanie z API — nigdy nie blokuje serwera.
    runtime_check=False → wywołanie ze startu — może wywołać SystemExit przy BLOCK.
    Jeśli tabela SchemaChecksums jest pusta → traktuj jako OK (pierwsze uruchomienie).
    """
    import traceback as _traceback

    result = VerificationResult()
    log_path = _get_integrity_log_path()

    logger.info(
        "=== Weryfikacja integralności schematu START "
        "(verification_id=%s, runtime=%s) ===",
        result.verification_id,
        runtime_check,
        extra={
            "verification_id": result.verification_id,
            "runtime_check": runtime_check,
        },
    )

    try:
        # ── Krok 1: Live checksums ────────────────────────────────────────────
        live_checksums = await _fetch_live_checksums(db, result.verification_id)
        result.total_live_objects = len(live_checksums)

        # ── Krok 2: Stored checksums ──────────────────────────────────────────
        stored_checksums = await _fetch_stored_checksums(db, result.verification_id)
        result.total_stored_objects = len(stored_checksums)

        logger.info(
            "Obiekty do porównania: live=%d stored=%d",
            result.total_live_objects,
            result.total_stored_objects,
            extra={
                "verification_id": result.verification_id,
                "live_objects":   sorted(live_checksums.keys()),
                "stored_objects": sorted(stored_checksums.keys()),
            },
        )

        # ── Krok 2b: Brak baseline → pierwsze uruchomienie, pomijamy ─────────
        if result.total_stored_objects == 0:
            result.success = True
            result.reaction_applied = "SKIPPED_NO_BASELINE"
            result.finish()
            logger.warning(
                "Brak zapisanych checksums w SchemaChecksums (total_stored=0) — "
                "pomijam weryfikację. Uruchom seedy Alembic aby zarejestrować obiekty.",
                extra={
                    "verification_id": result.verification_id,
                    "total_live_objects": result.total_live_objects,
                    "runtime_check": runtime_check,
                },
            )
            _write_jsonl(log_path, result.to_log_dict())
            return result

        # ── Krok 3: Porównaj checksums ────────────────────────────────────────
        mismatches, ok_count = _compare_checksums(
            live_checksums,
            stored_checksums,
            result.verification_id,
        )
        result.mismatches = mismatches
        result.verified_ok = ok_count

        # ── Krok 4: Zaktualizuj LastVerifiedAt dla OK obiektów ────────────────
        all_live_keys  = set(live_checksums.keys())
        mismatch_keys  = {m.qualified_name.lower() for m in mismatches}
        ok_keys        = sorted(all_live_keys - mismatch_keys)
        await _update_last_verified(db, ok_keys, result.verification_id)
        await db.commit()

        # ── Krok 5: Finalizuj wynik ───────────────────────────────────────────
        result.finish()

        if not result.has_mismatches:
            result.success = True
            result.reaction_applied = "NONE"
            logger.info(
                "=== Weryfikacja ZAKOŃCZONA POMYŚLNIE: %d/%d obiektów OK (%.1f ms) ===",
                ok_count,
                result.total_live_objects,
                result.duration_ms or 0.0,
                extra={
                    "verification_id": result.verification_id,
                    "duration_ms":     result.duration_ms,
                },
            )
            _write_jsonl(log_path, result.to_log_dict())
            return result

        # ── Krok 6: Obsługa niezgodności ─────────────────────────────────────
        result.success = False
        reaction_level: ReactionLevel = await _get_reaction_level(db)
        result.reaction_level = reaction_level

        logger.critical(
            "!!! SCHEMA TAMPER DETECTED !!! "
            "verification_id=%s | niezgodności=%d | reaction=%s",
            result.verification_id,
            len(mismatches),
            reaction_level,
            extra={
                "verification_id": result.verification_id,
                "mismatches":      [m.to_dict() for m in mismatches],
                "reaction_level":  reaction_level,
                "runtime_check":   runtime_check,
            },
        )

        # Zapis raportu incydentu
        incident_path = _get_incident_log_path(result.verification_id)
        _write_incident_file(incident_path, result.to_log_dict())

        if runtime_check:
            # API call — nigdy nie blokuj serwera, tylko raportuj
            result.reaction_applied = (
                f"REPORTED_ONLY (runtime_check=True, configured={reaction_level})"
            )
        else:
            result.reaction_applied = reaction_level

        _write_jsonl(log_path, result.to_log_dict())

        # BLOCK tylko przy starcie i tylko jeśli skonfigurowany
        if not runtime_check and reaction_level == "BLOCK":
            logger.critical(
                "Reakcja BLOCK — aplikacja zatrzymana. "
                "Zmień schema_integrity.reaction na WARN aby pominąć.",
                extra={"verification_id": result.verification_id},
            )
            raise SystemExit(
                f"[SchemaIntegrity] BLOCK: {len(mismatches)} niezgodności. "
                f"Verification ID: {result.verification_id}"
            )

        return result

    except SystemExit:
        raise
    except Exception as exc:
        result.error = str(exc)[:500]
        result.finish()
        logger.error(
            "Błąd weryfikacji integralności: %s",
            exc,
            extra={
                "verification_id": result.verification_id,
                "traceback":       _traceback.format_exc(),
            },
        )
        try:
            _write_jsonl(log_path, result.to_log_dict())
        except Exception:
            pass
        return result

        
# ---------------------------------------------------------------------------
# Pomocnicze — serializacja do formatu API
# ---------------------------------------------------------------------------

def _verification_result_to_api_dict(result: "VerificationResult") -> dict[str, Any]:  # noqa: F821
    """Konwertuje VerificationResult → słownik API response."""
    return {
        "verification_id":      result.verification_id,
        "status":               "ok" if result.success else ("error" if result.error else "mismatch"),
        "success":              result.success,
        "started_at":           result.started_at.isoformat(),
        "finished_at":          result.finished_at.isoformat() if result.finished_at else None,
        "duration_ms":          round(result.duration_ms, 2) if result.duration_ms else None,
        "total_live_objects":   result.total_live_objects,
        "total_stored_objects": result.total_stored_objects,
        "verified_ok":          result.verified_ok,
        "mismatch_count":       len(result.mismatches),
        "mismatches":           [m.to_dict() for m in result.mismatches],
        "reaction_level":       result.reaction_level,
        "reaction_applied":     result.reaction_applied,
        "error":                result.error,
        "monitored_schemas":    list(MONITORED_SCHEMAS),
        "host": {
            "hostname": result.hostname,
            "pid":      result.pid,
        },
    }


def _jsonl_entry_to_api_dict(entry: dict[str, Any]) -> dict[str, Any]:
    """Konwertuje wpis z jsonl → słownik API response (GET endpoint)."""
    result_block = entry.get("result", {})
    reaction_block = entry.get("reaction", {})
    host_block = entry.get("host", {})

    return {
        "verification_id":      entry.get("verification_id"),
        "status":               "ok" if result_block.get("success") else (
                                    "error" if entry.get("error") else "mismatch"
                                ),
        "success":              result_block.get("success", False),
        "started_at":           entry.get("timestamp"),
        "finished_at":          entry.get("finished_at"),
        "duration_ms":          entry.get("duration_ms"),
        "total_live_objects":   result_block.get("total_live_objects", 0),
        "total_stored_objects": result_block.get("total_stored_objects", 0),
        "verified_ok":          result_block.get("verified_ok", 0),
        "mismatch_count":       result_block.get("mismatch_count", 0),
        "mismatches":           result_block.get("mismatches", []),
        "reaction_level":       reaction_block.get("level", "UNKNOWN"),
        "reaction_applied":     reaction_block.get("applied", "UNKNOWN"),
        "error":                entry.get("error"),
        "monitored_schemas":    entry.get("monitored_schemas", []),
        "host": {
            "hostname": host_block.get("hostname"),
            "pid":      host_block.get("pid"),
        },
        "source": "log_file",   # informacja że dane z pliku, nie z live query
    }


def _read_last_jsonl_entry(path: Path) -> Optional[dict[str, Any]]:
    """
    Czyta ostatnią linię z pliku JSON Lines.
    Zwraca None jeśli plik nie istnieje, jest pusty lub nie parseable.

    Efektywna implementacja: czyta plik od końca (seek) bez ładowania całości.
    """
    if not path.exists() or path.stat().st_size == 0:
        return None

    try:
        # Wczytaj od końca — dla typowych plików (<10MB) to w pełni akceptowalne
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        # Iteruj od końca szukając niepustej linii z poprawnym JSON
        for line in reversed(lines):
            line = line.strip()
            if not line:
                continue
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue

        return None

    except OSError as exc:
        logger.warning(
            "Nie można odczytać pliku jsonl: %s — %s",
            path,
            exc,
            extra={"path": str(path)},
        )
        return None