#!/usr/bin/env python3
# tools/migrate_krok0.py
"""
Krok 0 — jednorazowa migracja danych historycznych.

Przenosi dane z trzech starych tabel faktur do nowego, jednolitego
silnika obiegu dokumentow (skw_document_approval_instances / skw_approval_log).

URUCHOMIENIE:
  # Dry-run (tylko weryfikacja, brak zapisu) — zawsze najpierw:
  python tools/migrate_krok0.py --dry-run

  # Pelna migracja (po potwierdzeniu dry-run na STOMIL):
  python tools/migrate_krok0.py

  # Z limitem (do testow):
  python tools/migrate_krok0.py --dry-run --limit 100

WYMAGANIA:
  - Migracja Alembic 0039 musi byc juz zastosowana
  - database/emergency/krok0_rollback.sql musi byc gotowy
  - Uruchamiac TYLKO na STOMIL przed GPGKJASLO
  - Zmienna srodowiskowa DATABASE_URL musi wskazywac na docelowa baze

KOLEJNOSC DZIALAN (nie zmieniac):
  1. Walidacja warunkow wstepnych
  2. Suche uruchomienie (--dry-run) z weryfikacja parowa
  3. Pelna migracja instancji (skw_faktura_akceptacja -> skw_document_approval_instances)
  4. Migracja historii (skw_faktura_log -> skw_approval_log)
  5. Triggery DENY na starych tabelach
  6. Weryfikacja koncowa parowa

MAPOWANIE STATUSOW:
  nowe          -> pending_dispatch   (czeka na auto-dispatch w nowym systemie)
  w_toku        -> in_progress
  zaakceptowana -> approved
  anulowana     -> cancelled
  orphaned      -> source_orphaned    (NIE cancelled — decyzja D-E04)

MAPOWANIE AKCJI LOGU:
  przypisano          -> dispatched
  zaakceptowano       -> accepted
  odrzucono           -> rejected
  zresetowano         -> cancelled    (reset przez referenta = cancel i re-dispatch)
  status_zmieniony    -> system_note
  priorytet_zmieniony -> system_note
  fakir_update        -> hook_executed
  fakir_update_failed -> hook_failed
  nie_moje            -> forwarded
  force_akceptacja    -> accepted
  anulowano           -> cancelled
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyodbc

# ---------------------------------------------------------------------------
# Konfiguracja loggera — zapisuje do pliku i konsoli
# ---------------------------------------------------------------------------

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"krok0_migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"

logger = logging.getLogger("krok0")
logger.setLevel(logging.DEBUG)

# Handler konsolowy
_ch = logging.StreamHandler(sys.stdout)
_ch.setLevel(logging.INFO)
_ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
logger.addHandler(_ch)

# Handler plikowy — JSONL, jeden wpis JSON per linia
class _JsonlHandler(logging.Handler):
    def __init__(self, path: Path) -> None:
        super().__init__()
        self._f = path.open("a", encoding="utf-8")

    def emit(self, record: logging.LogRecord) -> None:
        entry = {
            "ts":      datetime.now(timezone.utc).isoformat(),
            "level":   record.levelname,
            "msg":     self.format(record),
            "extra":   getattr(record, "extra", {}),
        }
        self._f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        self._f.flush()

_jh = _JsonlHandler(LOG_FILE)
_jh.setLevel(logging.DEBUG)
logger.addHandler(_jh)


def _log(level: str, msg: str, **extra: Any) -> None:
    record = logger.makeRecord(
        logger.name, getattr(logging, level.upper()), "", 0, msg, (), None
    )
    record.extra = extra
    logger.handle(record)


# ---------------------------------------------------------------------------
# Stale
# ---------------------------------------------------------------------------

SCHEMA = "dbo"

# Mapowanie statusow stary -> nowy
STATUS_MAP: dict[str, str] = {
    "nowe":          "pending_dispatch",
    "w_toku":        "in_progress",
    "zaakceptowana": "approved",
    "anulowana":     "cancelled",
    "orphaned":      "source_orphaned",
}

# Mapowanie akcji logu stary -> nowy
AKCJA_MAP: dict[str, str] = {
    "przypisano":          "dispatched",
    "zaakceptowano":       "accepted",
    "odrzucono":           "rejected",
    "zresetowano":         "cancelled",
    "status_zmieniony":    "system_note",
    "priorytet_zmieniony": "system_note",
    "fakir_update":        "hook_executed",
    "fakir_update_failed": "hook_failed",
    "nie_moje":            "forwarded",
    "force_akceptacja":    "accepted",
    "anulowano":           "cancelled",
}

# Mapowanie priorytetu stary string -> INT NULL
PRIORYTET_MAP: dict[str, int | None] = {
    "normalny":     1,
    "pilny":        2,
    "bardzo_pilny": 3,
}


# ---------------------------------------------------------------------------
# Polaczenie z baza
# ---------------------------------------------------------------------------

def _get_connection_string() -> str:
    """Buduje connection string z ENV lub konfiguracji."""
    server   = os.environ.get("MSSQL_HOST",     "192.168.0.50")
    port     = os.environ.get("MSSQL_PORT",     "59425")   # STOMIL domyslnie
    database = os.environ.get("MSSQL_DATABASE", "STOMIL")
    user     = os.environ.get("MSSQL_USER",     "windykacja_app")
    password = os.environ.get("MSSQL_PASSWORD", "")

    if not password:
        raise RuntimeError(
            "MSSQL_PASSWORD nie ustawione. "
            "Ustaw zmienne srodowiskowe: MSSQL_HOST, MSSQL_PORT, "
            "MSSQL_DATABASE, MSSQL_USER, MSSQL_PASSWORD"
        )

    return (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server},{port};"
        f"DATABASE={database};"
        f"UID={user};PWD={password};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=yes;"
    )


def _connect() -> pyodbc.Connection:
    conn_str = _get_connection_string()
    # Maskujemy haslo w logach
    safe = conn_str.replace(os.environ.get("MSSQL_PASSWORD", "***"), "***")
    _log("INFO", "Laczenie z baza", connection_string=safe)
    conn = pyodbc.connect(conn_str, autocommit=False)
    conn.setdecoding(pyodbc.SQL_CHAR,  encoding="utf-8")
    conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-8")
    conn.setencoding(encoding="utf-8")
    _log("INFO", "Polaczenie OK")
    return conn


# ---------------------------------------------------------------------------
# KROK 1 — Walidacja warunkow wstepnych
# ---------------------------------------------------------------------------

def validate_preconditions(conn: pyodbc.Connection) -> int:
    """
    Sprawdza czy system jest gotowy na Krok 0.
    Rzuca RuntimeError jesli cos nie gra.
    """
    _log("INFO", "=== WALIDACJA WARUNKOW WSTEPNYCH ===")
    cur = conn.cursor()

    # 1a. Alembic musi byc na 0039
    cur.execute(f"SELECT version_num FROM [{SCHEMA}].[alembic_version]")
    row = cur.fetchone()
    if not row or row[0] != "0039":
        raise RuntimeError(
            f"Alembic version = {row[0] if row else 'BRAK'}. "
            f"Wymagana: 0039. Uruchom najpierw: alembic upgrade head"
        )
    _log("INFO", "Alembic version OK", version=row[0])

    # 1b. Stare tabele musza istniec
    for table in ("skw_faktura_akceptacja", "skw_faktura_przypisanie", "skw_faktura_log"):
        cur.execute(
            "SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id "
            f"WHERE s.name = '{SCHEMA}' AND t.name = '{table}'"
        )
        if not cur.fetchone():
            raise RuntimeError(f"Stara tabela {SCHEMA}.{table} nie istnieje!")
        _log("INFO", f"Stara tabela OK: {table}")

    # 1c. Nowe tabele musza istniec
    for table in ("skw_document_approval_instances", "skw_approval_log", "skw_document_sources"):
        cur.execute(
            "SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id "
            f"WHERE s.name = '{SCHEMA}' AND t.name = '{table}'"
        )
        if not cur.fetchone():
            raise RuntimeError(f"Nowa tabela {SCHEMA}.{table} nie istnieje! Uruchom migrace 0039.")
        _log("INFO", f"Nowa tabela OK: {table}")

    # 1d. Zrodlo 'fakir' musi istniec w skw_document_sources
    cur.execute(
        f"SELECT id_source FROM [{SCHEMA}].[skw_document_sources] WHERE source_name = 'fakir'"
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(
            "Brak wpisu 'fakir' w skw_document_sources. "
            "Sprawdz seed z migracji 0028 (krok27)."
        )
    fakir_id = row[0]
    _log("INFO", "Zrodlo fakir OK", id_source=fakir_id)

    # 1e. Triggery DENY NIE powinny juz istniec (oznaczaloby powrot po rollbacku)
    for trigger in ("TR_skw_faktura_akceptacja_DENY", "TR_skw_faktura_przypisanie_DENY", "TR_skw_faktura_log_DENY"):
        cur.execute(f"SELECT 1 FROM sys.triggers WHERE name = '{trigger}'")
        if cur.fetchone():
            _log("WARNING", f"Trigger {trigger} juz istnieje — moze to byc powtorne uruchomienie")

    # 1f. Nowe tabele musza byc puste (lub zapytaj uzytkownika jesli nie)
    cur.execute(f"SELECT COUNT(*) FROM [{SCHEMA}].[skw_document_approval_instances] WHERE id_source = ?", fakir_id)
    existing = cur.fetchone()[0]
    if existing > 0:
        _log("WARNING", "skw_document_approval_instances ma juz rekordy dla zrodla fakir",
             count=existing)
        answer = input(f"\nZnaleziono {existing} istniejacych rekordow dla zrodla fakir. "
                       f"Kontynuowac? (yes/no): ").strip().lower()
        if answer != "yes":
            raise RuntimeError("Migracja przerwana przez uzytkownika.")

    _log("INFO", "=== WALIDACJA OK — system gotowy na Krok 0 ===")
    return fakir_id


# ---------------------------------------------------------------------------
# KROK 2 — Pobieranie danych zrodlowych
# ---------------------------------------------------------------------------

def fetch_source_data(
    conn: pyodbc.Connection,
    limit: int | None,
) -> tuple[list[dict], list[dict], dict[int, list[dict]]]:
    """
    Pobiera wszystkie dane ze starych tabel.
    Zwraca: (faktury, przypisania, {faktura_id: [logi]})
    """
    _log("INFO", "=== POBIERANIE DANYCH ZRODLOWYCH ===")
    cur = conn.cursor()

    # Faktury
    limit_clause = f"TOP {limit}" if limit else ""
    cur.execute(f"""
        SELECT {limit_clause}
            fa.id,
            fa.numer_ksef,
            fa.status_wewnetrzny,
            fa.priorytet,
            fa.opis_dokumentu,
            fa.uwagi,
            fa.utworzony_przez,
            fa.IsActive,
            fa.CreatedAt,
            fa.UpdatedAt
        FROM [{SCHEMA}].[skw_faktura_akceptacja] fa
        ORDER BY fa.id ASC
    """)
    cols_fa = [d[0] for d in cur.description]
    faktury = [dict(zip(cols_fa, row)) for row in cur.fetchall()]
    _log("INFO", f"Pobrano {len(faktury)} faktur")

    if not faktury:
        return [], [], {}

    faktura_ids = [f["id"] for f in faktury]
    placeholders = ",".join("?" * len(faktura_ids))

    # Przypisania (archiwalne — nie potrzebujemy ich w nowym systemie,
    # ale logujemy liczbe dla weryfikacji parowej)
    cur.execute(
        f"SELECT COUNT(*) FROM [{SCHEMA}].[skw_faktura_przypisanie] "
        f"WHERE faktura_id IN ({placeholders})",
        faktura_ids,
    )
    przypisania_count = cur.fetchone()[0]
    _log("INFO", f"Przypisania archiwalne (tylko liczba): {przypisania_count}")

    # Historia logow per faktura
    cur.execute(
        f"""
        SELECT
            sfl.id,
            sfl.faktura_id,
            sfl.user_id,
            sfl.akcja,
            sfl.szczegoly,
            sfl.CreatedAt
        FROM [{SCHEMA}].[skw_faktura_log] sfl
        WHERE sfl.faktura_id IN ({placeholders})
        ORDER BY sfl.faktura_id ASC, sfl.CreatedAt ASC
        """,
        faktura_ids,
    )
    cols_log = [d[0] for d in cur.description]
    all_logs = [dict(zip(cols_log, row)) for row in cur.fetchall()]
    logs_by_faktura: dict[int, list[dict]] = {}
    for log in all_logs:
        logs_by_faktura.setdefault(log["faktura_id"], []).append(log)

    _log("INFO", f"Pobrano {len(all_logs)} wpisow logu")
    return faktury, przypisania_count, logs_by_faktura


# ---------------------------------------------------------------------------
# KROK 3 — Budowanie rekordow docelowych
# ---------------------------------------------------------------------------

def _build_extra_data(faktura: dict) -> str:
    """
    Buduje JSON extra_data dla skw_document_approval_instances.
    ksef_id = numer_ksef (identyfikator zewnetrzny dla hooka Fakira).
    """
    extra: dict[str, Any] = {
        "ksef_id":        faktura["numer_ksef"],
        "invoice_type":   0,           # faktury zakupowe sa w obiegu
        "migrated_from":  "skw_faktura_akceptacja",
        "migration_date": datetime.now(timezone.utc).isoformat(),
        "original_id":    faktura["id"],
        "priorytet_stary": faktura.get("priorytet"),
    }
    if faktura.get("opis_dokumentu"):
        extra["opis_dokumentu"] = faktura["opis_dokumentu"]
    if faktura.get("uwagi"):
        extra["uwagi"] = faktura["uwagi"]
    return json.dumps(extra, ensure_ascii=False)


def _build_document_title(numer_ksef: str) -> str:
    """
    Buduje document_title dla instancji.
    Numer faktury i kontrahent sa w WAPRO — beda dostepne przez DatabaseAdapter.
    Na potrzeby migracji uzywamy KSEF_ID jako placeholder.
    """
    return f"Faktura KSeF {numer_ksef}"


def build_instance_record(
    faktura: dict,
    fakir_id_source: int,
) -> dict:
    """Przeksztalca rekord skw_faktura_akceptacja na dict dla skw_document_approval_instances."""
    old_status = faktura["status_wewnetrzny"]
    new_status = STATUS_MAP.get(old_status, "pending_dispatch")

    priority = PRIORYTET_MAP.get(faktura.get("priorytet") or "normalny")

    extra_data = _build_extra_data(faktura)

    return {
        "id_source":       fakir_id_source,
        "id_document":     faktura["numer_ksef"],    # KSEF_ID jako id_document (decyzja F0.3)
        "id_category":     None,                     # kategoria przypisywana pozniej przez admina
        "id_path":         None,                     # sciezka zostanie przypisana przez auto-dispatch
        "status":          new_status,
        "current_step":    None,
        "document_title":  _build_document_title(faktura["numer_ksef"]),
        "doc_number":      None,                     # uzupelni DatabaseAdapter przy sync
        "contractor_name": None,                     # uzupelni DatabaseAdapter przy sync
        "document_date":   None,                     # uzupelni DatabaseAdapter przy sync
        "document_amount": None,                     # uzupelni DatabaseAdapter przy sync
        "extra_data":      extra_data,
        "priority":        priority,
        "dispatch_attempts": 0,
        "dispatched_at":   None,
        "completed_at":    faktura["UpdatedAt"] if new_status in ("approved", "cancelled", "source_orphaned") else None,
        "created_at":      faktura["CreatedAt"],
        "updated_at":      faktura["UpdatedAt"] or faktura["CreatedAt"],
        "is_urgent":       1 if (faktura.get("priorytet") == "bardzo_pilny") else 0,
        "_old_id":         faktura["id"],             # tymczasowy klucz do dopasowania logow
    }


def build_log_records(
    old_logs: list[dict],
    new_instance_id: int,
    id_source: int,
) -> list[dict]:
    """Przeksztalca logi z skw_faktura_log na rekordy dla skw_approval_log."""
    result = []
    for log in old_logs:
        old_akcja = log["akcja"]
        new_action = AKCJA_MAP.get(old_akcja, "system_note")

        # Parsuj stare szczegoly — zachowaj jako payload w nowym logu
        old_details: dict = {}
        if log.get("szczegoly"):
            try:
                old_details = json.loads(log["szczegoly"])
            except Exception:
                old_details = {"raw": str(log["szczegoly"])}

        new_details = json.dumps({
            "migrated_from":   "skw_faktura_log",
            "original_log_id": log["id"],
            "original_action": old_akcja,
            "original_data":   old_details,
        }, ensure_ascii=False)

        result.append({
            "id_instance": new_instance_id,
            "id_user":     log["user_id"],
            "action":      new_action,
            "step_order":  None,
            "details":     new_details,
            "is_voided":   0,
            "created_at":  log["CreatedAt"],
        })
    return result


# ---------------------------------------------------------------------------
# KROK 4 — Migracja instancji
# ---------------------------------------------------------------------------

def migrate_instances(
    conn: pyodbc.Connection,
    faktury: list[dict],
    logs_by_faktura: dict[int, list[dict]],
    fakir_id_source: int,
    dry_run: bool,
) -> dict[int, int]:
    """
    Wstawia rekordy do skw_document_approval_instances.
    Zwraca mapowanie: old_id -> new_id_instance.
    """
    _log("INFO", "=== MIGRACJA INSTANCJI ===", dry_run=dry_run, count=len(faktury))
    cur = conn.cursor()
    old_to_new: dict[int, int] = {}

    stats = {
        "inserted": 0, "skipped_duplicate": 0, "errors": 0,
        "statuses": {},
    }

    for i, faktura in enumerate(faktury, 1):
        old_id     = faktura["id"]
        numer_ksef = faktura["numer_ksef"]

        try:
            # Sprawdz czy juz istnieje (idempotentnosc — bezpieczny re-run)
            cur.execute(
                f"SELECT id_instance FROM [{SCHEMA}].[skw_document_approval_instances] "
                f"WHERE id_source = ? AND id_document = ?",
                (fakir_id_source, numer_ksef),
            )
            existing = cur.fetchone()
            if existing:
                old_to_new[old_id] = existing[0]
                stats["skipped_duplicate"] += 1
                _log("DEBUG", f"[{i}/{len(faktury)}] Pominiety (juz istnieje)",
                     old_id=old_id, new_id=existing[0], numer_ksef=numer_ksef)
                continue

            rec = build_instance_record(faktura, fakir_id_source)
            new_status = rec["status"]
            stats["statuses"][new_status] = stats["statuses"].get(new_status, 0) + 1

            if not dry_run:
                cur.execute(f"""
                    INSERT INTO [{SCHEMA}].[skw_document_approval_instances] (
                        [id_source], [id_document], [id_category], [id_path],
                        [status], [current_step], [document_title],
                        [doc_number], [contractor_name], [document_date],
                        [document_amount], [extra_data], [priority],
                        [dispatch_attempts], [dispatched_at], [completed_at],
                        [created_at], [updated_at], [is_urgent]
                    ) VALUES (
                        ?, ?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?
                    )
                """,
                    rec["id_source"], rec["id_document"], rec["id_category"], rec["id_path"],
                    rec["status"], rec["current_step"], rec["document_title"],
                    rec["doc_number"], rec["contractor_name"], rec["document_date"],
                    rec["document_amount"], rec["extra_data"], rec["priority"],
                    rec["dispatch_attempts"], rec["dispatched_at"], rec["completed_at"],
                    rec["created_at"], rec["updated_at"], rec["is_urgent"],
                )
                # Pobierz nowe ID
                cur.execute("SELECT SCOPE_IDENTITY()")
                new_id = int(cur.fetchone()[0])
                old_to_new[old_id] = new_id
            else:
                # Dry-run: symuluj ID (ujemne, zeby nie kolidowaly)
                old_to_new[old_id] = -(i)

            stats["inserted"] += 1

            if i % 100 == 0:
                _log("INFO", f"Postep: {i}/{len(faktury)}", stats=stats)

        except Exception as exc:
            stats["errors"] += 1
            _log("ERROR", f"Blad przy fakturze {old_id}/{numer_ksef}: {exc}",
                 old_id=old_id, numer_ksef=numer_ksef, error=str(exc))
            if not dry_run:
                raise  # Nie kontynuujemy przy bledzie w trybie pelnym

    _log("INFO", "Migracja instancji zakonczona", stats=stats, dry_run=dry_run)
    return old_to_new


# ---------------------------------------------------------------------------
# KROK 5 — Migracja historii logow
# ---------------------------------------------------------------------------

def migrate_logs(
    conn: pyodbc.Connection,
    old_to_new: dict[int, int],
    logs_by_faktura: dict[int, list[dict]],
    fakir_id_source: int,
    dry_run: bool,
) -> None:
    """
    Wstawia wpisy do skw_approval_log (append-only, trigger DENY na UPDATE/DELETE).
    UWAGA: INSERT przez raw SQL (nie ORM) — tabela ma trigger DENY na UPDATE/DELETE.
    """
    _log("INFO", "=== MIGRACJA HISTORII LOGOW ===", dry_run=dry_run)
    cur = conn.cursor()

    total_logs = 0
    errors     = 0

    for old_id, new_id in old_to_new.items():
        old_logs = logs_by_faktura.get(old_id, [])
        if not old_logs:
            continue

        log_records = build_log_records(old_logs, new_id, fakir_id_source)

        for log_rec in log_records:
            try:
                if not dry_run:
                    # RAW SQL INSERT — tabela ma trigger DENY na UPDATE/DELETE
                    # Nie uzywamy ORM zeby nie ryzykowac przypadkowego UPDATE
                    cur.execute(f"""
                        INSERT INTO [{SCHEMA}].[skw_approval_log] (
                            [id_instance], [id_user], [action],
                            [step_order], [details], [is_voided], [created_at]
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                        log_rec["id_instance"],
                        log_rec["id_user"],
                        log_rec["action"],
                        log_rec["step_order"],
                        log_rec["details"],
                        log_rec["is_voided"],
                        log_rec["created_at"],
                    )
                total_logs += 1
            except Exception as exc:
                errors += 1
                _log("ERROR", f"Blad przy logu dla instancji {new_id}: {exc}", error=str(exc))
                if not dry_run:
                    raise

    _log("INFO", "Migracja logow zakonczona",
         total_logs=total_logs, errors=errors, dry_run=dry_run)


# ---------------------------------------------------------------------------
# KROK 6 — Triggery DENY na starych tabelach
# ---------------------------------------------------------------------------

DENY_TRIGGER_FAKTURA_AKCEPTACJA = f"""
CREATE TRIGGER [dbo].[TR_skw_faktura_akceptacja_DENY]
ON [{SCHEMA}].[skw_faktura_akceptacja]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    RAISERROR(
        N'Tabela skw_faktura_akceptacja jest archiwalna (Krok 0 Etapu 2). '
        N'Uzyj skw_document_approval_instances.',
        10,  -- severity <= 10: nie rollbackuje transakcji (zgodnie z decyzja F0.4)
        1
    );
END
"""

DENY_TRIGGER_FAKTURA_PRZYPISANIE = f"""
CREATE TRIGGER [dbo].[TR_skw_faktura_przypisanie_DENY]
ON [{SCHEMA}].[skw_faktura_przypisanie]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    RAISERROR(
        N'Tabela skw_faktura_przypisanie jest archiwalna (Krok 0 Etapu 2). '
        N'Przypisania obsluguje silnik obiegu approval.',
        10,
        1
    );
END
"""

DENY_TRIGGER_FAKTURA_LOG = f"""
CREATE TRIGGER [dbo].[TR_skw_faktura_log_DENY]
ON [{SCHEMA}].[skw_faktura_log]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    RAISERROR(
        N'Tabela skw_faktura_log jest archiwalna (Krok 0 Etapu 2). '
        N'Historia obiegu jest w skw_approval_log.',
        10,
        1
    );
END
"""


def apply_deny_triggers(conn: pyodbc.Connection, dry_run: bool) -> None:
    """Tworzy triggery DENY na starych tabelach po potwierdzeniu migracji."""
    _log("INFO", "=== TRIGGERY DENY ===", dry_run=dry_run)

    if dry_run:
        _log("INFO", "Dry-run: triggery DENY NIE zostana utworzone")
        return

    cur = conn.cursor()

    for trigger_name, table_name, sql in [
        ("TR_skw_faktura_akceptacja_DENY", "skw_faktura_akceptacja",  DENY_TRIGGER_FAKTURA_AKCEPTACJA),
        ("TR_skw_faktura_przypisanie_DENY", "skw_faktura_przypisanie", DENY_TRIGGER_FAKTURA_PRZYPISANIE),
        ("TR_skw_faktura_log_DENY",         "skw_faktura_log",         DENY_TRIGGER_FAKTURA_LOG),
    ]:
        # Sprawdz czy trigger juz istnieje
        cur.execute(f"SELECT 1 FROM sys.triggers WHERE name = '{trigger_name}'")
        if cur.fetchone():
            _log("INFO", f"Trigger {trigger_name} juz istnieje — pomijam")
            continue

        try:
            cur.execute(sql)
            _log("INFO", f"Trigger {trigger_name} na {table_name} — OK")
        except Exception as exc:
            _log("ERROR", f"Blad przy tworzeniu triggera {trigger_name}: {exc}", error=str(exc))
            raise


# ---------------------------------------------------------------------------
# KROK 7 — Weryfikacja parowa
# ---------------------------------------------------------------------------

def verify_parity(
    conn: pyodbc.Connection,
    fakir_id_source: int,
    dry_run: bool,
) -> bool:
    """
    Weryfikacja parowa — porownuje liczby rekordow i statusy.
    Zwraca True jesli wszystko sie zgadza.
    """
    _log("INFO", "=== WERYFIKACJA PAROWA ===", dry_run=dry_run)
    cur = conn.cursor()
    ok = True

    # Liczba rekordow
    cur.execute(f"SELECT COUNT(*) FROM [{SCHEMA}].[skw_faktura_akceptacja]")
    old_count = cur.fetchone()[0]

    cur.execute(
        f"SELECT COUNT(*) FROM [{SCHEMA}].[skw_document_approval_instances] WHERE id_source = ?",
        fakir_id_source,
    )
    new_count = cur.fetchone()[0]

    _log("INFO", "Liczba rekordow", stare=old_count, nowe=new_count)

    if not dry_run and old_count != new_count:
        _log("ERROR", "NIEZGODNOSC liczby rekordow!", stare=old_count, nowe=new_count)
        ok = False
    elif dry_run:
        _log("INFO", f"Dry-run: oczekiwana liczba nowych rekordow: {old_count}")

    # Weryfikacja statusow — parowanie
    cur.execute(f"""
        SELECT status_wewnetrzny, COUNT(*) as cnt
        FROM [{SCHEMA}].[skw_faktura_akceptacja]
        GROUP BY status_wewnetrzny
        ORDER BY status_wewnetrzny
    """)
    old_statuses = {row[0]: row[1] for row in cur.fetchall()}

    if not dry_run:
        cur.execute(f"""
            SELECT status, COUNT(*) as cnt
            FROM [{SCHEMA}].[skw_document_approval_instances]
            WHERE id_source = ?
            GROUP BY status
            ORDER BY status
        """, fakir_id_source)
        new_statuses = {row[0]: row[1] for row in cur.fetchall()}

        # Sprawdz mapowanie
        for old_status, count in old_statuses.items():
            expected_new = STATUS_MAP.get(old_status)
            actual_new   = new_statuses.get(expected_new, 0)
            match = "OK" if actual_new == count else "NIEZGODNOSC"
            _log(
                "INFO" if match == "OK" else "ERROR",
                f"Status {old_status} -> {expected_new}: stare={count}, nowe={actual_new} [{match}]",
            )
            if match != "OK":
                ok = False
    else:
        _log("INFO", "Dry-run: statusy ze starych tabel", statuses=old_statuses)
        _log("INFO", "Dry-run: oczekiwane statusy po migracji",
             expected={STATUS_MAP.get(s, "??"): c for s, c in old_statuses.items()})

    # Weryfikacja historii logow
    cur.execute(f"SELECT COUNT(*) FROM [{SCHEMA}].[skw_faktura_log]")
    old_logs = cur.fetchone()[0]

    if not dry_run:
        cur.execute(
            f"""
            SELECT COUNT(*) FROM [{SCHEMA}].[skw_approval_log] al
            JOIN [{SCHEMA}].[skw_document_approval_instances] i
                ON i.id_instance = al.id_instance
            WHERE i.id_source = ?
              AND al.details LIKE '%migrated_from%'
            """,
            fakir_id_source,
        )
        new_logs = cur.fetchone()[0]
        match = "OK" if old_logs == new_logs else "NIEZGODNOSC"
        _log(
            "INFO" if match == "OK" else "ERROR",
            f"Historia logow: stare={old_logs}, nowe={new_logs} [{match}]",
        )
        if match != "OK":
            ok = False
    else:
        _log("INFO", f"Dry-run: do migracji {old_logs} wpisow logu")

    status_str = "OK" if ok else "NIEZGODNOSCI — sprawdz logi!"
    _log("INFO", f"=== WERYFIKACJA PAROWA: {status_str} ===")
    return ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Krok 0 — migracja danych historycznych faktur do nowego silnika obiegu.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Tylko analiza i weryfikacja — brak zapisu do bazy.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Ogranicz liczbe migrowanych faktur (do testow).",
    )
    parser.add_argument(
        "--skip-triggers",
        action="store_true",
        help="Pominij tworzenie triggerow DENY (uzywac tylko do celow testowych).",
    )
    args = parser.parse_args()

    dry_run = args.dry_run

    _log("INFO", "=" * 70)
    _log("INFO", "KROK 0 — MIGRACJA DANYCH HISTORYCZNYCH FAKTUR",
         dry_run=dry_run, limit=args.limit, log_file=str(LOG_FILE))
    _log("INFO", "=" * 70)

    if dry_run:
        _log("INFO", "TRYB: DRY-RUN — brak zapisu do bazy")
    else:
        _log("WARNING", "TRYB: PRODUKCYJNY — dane beda zapisane do bazy!")
        answer = input("\nUruchamiasz pelna migracje. Czy na pewno? (yes/no): ").strip().lower()
        if answer != "yes":
            _log("INFO", "Migracja przerwana przez uzytkownika.")
            sys.exit(0)

    t_start = time.monotonic()

    try:
        conn = _connect()

        # 1. Walidacja
        fakir_id = validate_preconditions(conn)

        # 2. Pobieranie danych
        faktury, przypisania_count, logs_by_faktura = fetch_source_data(conn, args.limit)

        if not faktury:
            _log("INFO", "Brak faktur do migracji — zakonczono.")
            return

        _log("INFO", f"Do migracji: {len(faktury)} faktur, {przypisania_count} przypisania (archiwum)")

        # 3. Migracja instancji
        old_to_new = migrate_instances(conn, faktury, logs_by_faktura, fakir_id, dry_run)

        # 4. Migracja historii logow
        migrate_logs(conn, old_to_new, logs_by_faktura, fakir_id, dry_run)

        # 5. Commit (przed triggerami — triggery sa DDL, nie potrzebuja commita)
        if not dry_run:
            conn.commit()
            _log("INFO", "COMMIT — dane zapisane")

        # 6. Triggery DENY
        if not args.skip_triggers:
            apply_deny_triggers(conn, dry_run)
            if not dry_run:
                conn.commit()
                _log("INFO", "COMMIT — triggery zastosowane")

        # 7. Weryfikacja parowa
        parity_ok = verify_parity(conn, fakir_id, dry_run)

        elapsed = time.monotonic() - t_start
        _log("INFO", "=" * 70)
        _log(
            "INFO" if parity_ok else "ERROR",
            f"ZAKONCZONE {'(DRY-RUN)' if dry_run else '(PRODUKCJA)'}",
            elapsed_s=round(elapsed, 2),
            parity_ok=parity_ok,
            log_file=str(LOG_FILE),
        )
        _log("INFO", "=" * 70)

        if not parity_ok:
            _log("ERROR", "NIEZGODNOSCI PAROWE — sprawdz logi i rozważ rollback!")
            _log("ERROR", f"Rollback: uruchom database/emergency/krok0_rollback.sql w SSMS")
            sys.exit(1)

        if dry_run:
            _log("INFO", "Dry-run zakonczony pomyslnie. Uruchom bez --dry-run aby wykonac migracje.")

        conn.close()

    except Exception as exc:
        _log("ERROR", f"BLAD KRYTYCZNY: {exc}", error=str(exc))
        _log("ERROR", "W razie bledow w trybie produkcyjnym: uruchom krok0_rollback.sql w SSMS")
        raise


if __name__ == "__main__":
    main()