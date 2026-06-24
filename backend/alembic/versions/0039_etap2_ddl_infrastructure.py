# backend/alembic/versions/0039_etap2_ddl_infrastructure.py
"""0039_etap2_ddl_infrastructure

Migracja F1 — DDL infrastruktura Etapu 2.

Przeksztalca system z rozwiazania dedykowanego fakturom KSeF/Fakir
w uniwersalna platforme obiegu dokumentow zdolna przyjac dowolne
zrodlo wylacznie przez konfiguracje.

KROKI:
  01 — ALTER TABLE skw_document_sources (10 nowych kolumn)
       Wzorzec: NULL → UPDATE default → NOT NULL dla kolumn wymaganych
  02 — ALTER TABLE skw_document_approval_instances
       Nowe kolumny: dispatch_attempts INT, priority INT NULL
  03 — ALTER TABLE skw_document_approval_instances
       Wymiana CHECK constraint status na 8 wartosci (idempotentna)
  04 — ALTER TABLE skw_approval_filters
       Nowa kolumna visibility_mode NVARCHAR(20) DEFAULT 'inherit'
  05 — CREATE TABLE skw_source_hooks
  06 — CREATE TABLE skw_source_actions
  07 — CREATE TABLE skw_source_action_log
  08 — CREATE TABLE skw_document_folders
  09 — CREATE TABLE skw_document_folder_items
  10 — CREATE TABLE skw_approval_filter_visibility
  11 — SEED nowych uprawnien (MERGE — idempotentny)
  12 — SEED nowych kluczy SystemConfig (MERGE — idempotentny)
  13 — MERGE skw_SchemaChecksums (rejestracja nowych obiektow)

ZASADY MSSQL obowiazujace w tym pliku:
  - Kazda instrukcja DDL w osobnym op.execute() (brak multi-statement)
  - Brak SET NOCOUNT ON (lamiemy rowcount detection Alembic)
  - Idempotentnosc przez IF NOT EXISTS + IF EXISTS
  - SYSUTCDATETIME() zamiast GETDATE() dla DATETIME2
  - Brak polskich znakow w komentarzach wewnatrz SQL (problem ODBC Driver 18)
  - Kolumny NOT NULL z danymi istniejacymi: NULL -> UPDATE -> NOT NULL

UWAGA PO WYKONANIU:
  - Sprawdz: SELECT * FROM dbo.alembic_version
  - Sprawdz liczby wierszy w nowych tabelach (powinny byc 0)
  - Sprawdz nowe uprawnienia: SELECT * FROM dbo.skw_Permissions WHERE Category IN ('sources','documents')
  - Sprawdz nowe klucze: SELECT ConfigKey, ConfigValue FROM dbo.skw_SystemConfig WHERE ConfigKey LIKE 'ETAP2%'

Revision ID: 0039
Revises:     0038
Create Date: 2026-06-24
"""

from __future__ import annotations

import logging
from typing import Final

from alembic import op
from sqlalchemy import text

# ---------------------------------------------------------------------------
# Metadane Alembic
# ---------------------------------------------------------------------------
revision:      str = "0039"
down_revision: str = "0038"
branch_labels       = None
depends_on          = None

logger = logging.getLogger(f"alembic.migration.{revision}")

SCHEMA: Final[str] = "dbo"

# ---------------------------------------------------------------------------
# Pomocnicze
# ---------------------------------------------------------------------------

def _log(krok: str, msg: str) -> None:
    """Loguje krok migracji do konsoli i loggera Alembic."""
    prefix = f"[0039] KROK {krok}"
    print(f"{prefix} | {msg}")
    logger.info("%s | %s", prefix, msg)


def _execute(sql: str) -> None:
    """Wykonuje SQL. Kazde wywolanie = osobna instrukcja DDL."""
    op.execute(text(sql))


# =============================================================================
# UPGRADE
# =============================================================================

def upgrade() -> None:
    logger.info("[0039] ============================================================")
    logger.info("[0039] START upgrade — F1 DDL infrastruktura Etapu 2")
    logger.info("[0039] ============================================================")

    _krok01_document_sources_alter()
    _krok02_approval_instances_new_columns()
    _krok03_approval_instances_check_status()
    _krok04_approval_filters_visibility_mode()
    _krok05_source_hooks()
    _krok06_source_actions()
    _krok07_source_action_log()
    _krok08_document_folders()
    _krok09_document_folder_items()
    _krok10_approval_filter_visibility()
    _krok11_seed_permissions()
    _krok12_seed_system_config()
    _krok13_schema_checksums()

    logger.info("[0039] ============================================================")
    logger.info("[0039] ZAKONCZONE upgrade — wszystkie kroki OK")
    logger.info("[0039] ============================================================")


# =============================================================================
# KROK 01 — ALTER TABLE skw_document_sources (10 nowych kolumn)
# =============================================================================

def _krok01_document_sources_alter() -> None:
    """
    Rozszerza skw_document_sources o 10 nowych kolumn.

    Kolumny NOT NULL z defaultem sa dodawane bezposrednio jako NOT NULL
    (tabela moze miec wiersze — istniejace wpisy Fakir, KSeF).
    Wzorzec dla NOT NULL bez defaultu: NULL -> UPDATE -> NOT NULL.

    Kolumny:
      source_type          NVARCHAR(20) NOT NULL (database|api|ftp|email|manual)
      connection_mode      NVARCHAR(10) NOT NULL (pull|push)
      connection_config    NVARCHAR(MAX) NULL     (JSON szyfrowany)
      sync_interval_minutes INT NOT NULL DEFAULT 15
      last_sync_at         DATETIME2 NULL
      last_sync_status     NVARCHAR(20) NULL      (ok|error|partial)
      last_sync_message    NVARCHAR(500) NULL
      is_test_mode         BIT NOT NULL DEFAULT 1
      webhook_token        NVARCHAR(100) NULL UNIQUE
      updated_at           DATETIME2 NOT NULL     (DEFAULT SYSUTCDATETIME)
    """
    _log("01", "ALTER TABLE skw_document_sources — dodawanie 10 nowych kolumn")

    # ── source_type: NULL najpierw, potem NOT NULL (mogą być istniejące wiersze)
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'source_type'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_sources]
            ADD [source_type] NVARCHAR(20) NULL
    """)
    _log("01a", "source_type dodana jako NULL")

    # Wypelnij istniejace wiersze wartoscia domyslna przed NOT NULL
    _execute(f"""
        UPDATE [{SCHEMA}].[skw_document_sources]
           SET [source_type] = N'database'
         WHERE [source_type] IS NULL
    """)
    _log("01b", "source_type — UPDATE istniejacych wierszy -> 'database'")

    _execute(f"""
        IF EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'source_type'
              AND is_nullable = 1
        )
        ALTER TABLE [{SCHEMA}].[skw_document_sources]
            ALTER COLUMN [source_type] NVARCHAR(20) NOT NULL
    """)
    _log("01c", "source_type — ALTER COLUMN NOT NULL OK")

    # CHECK constraint na source_type — osobna instrukcja
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'CK_skw_ds_source_type'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_sources]
            ADD CONSTRAINT [CK_skw_ds_source_type]
                CHECK ([source_type] IN (
                    N'database', N'api', N'ftp', N'email', N'manual'
                ))
    """)
    _log("01d", "CHECK CK_skw_ds_source_type OK")

    # ── connection_mode: NULL najpierw, potem NOT NULL
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'connection_mode'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_sources]
            ADD [connection_mode] NVARCHAR(10) NULL
    """)

    _execute(f"""
        UPDATE [{SCHEMA}].[skw_document_sources]
           SET [connection_mode] = N'pull'
         WHERE [connection_mode] IS NULL
    """)

    _execute(f"""
        IF EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'connection_mode'
              AND is_nullable = 1
        )
        ALTER TABLE [{SCHEMA}].[skw_document_sources]
            ALTER COLUMN [connection_mode] NVARCHAR(10) NOT NULL
    """)
    _log("01e", "connection_mode NOT NULL OK")

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'CK_skw_ds_connection_mode'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_sources]
            ADD CONSTRAINT [CK_skw_ds_connection_mode]
                CHECK ([connection_mode] IN (N'pull', N'push'))
    """)
    _log("01f", "CHECK CK_skw_ds_connection_mode OK")

    # ── connection_config: NULL (dane wrazliwe, szyfrowane po stronie aplikacji)
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'connection_config'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_sources]
            ADD [connection_config] NVARCHAR(MAX) NULL
    """)
    _log("01g", "connection_config NULL OK")

    # ── sync_interval_minutes: NOT NULL z DEFAULT 15
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'sync_interval_minutes'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_sources]
            ADD [sync_interval_minutes] INT NOT NULL
                CONSTRAINT [DF_skw_ds_sync_interval_minutes] DEFAULT 15
    """)
    _log("01h", "sync_interval_minutes NOT NULL DEFAULT 15 OK")

    # ── last_sync_at: NULL (pierwsza synchronizacja jeszcze nie nastapila)
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'last_sync_at'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_sources]
            ADD [last_sync_at] DATETIME2(7) NULL
    """)
    _log("01i", "last_sync_at NULL OK")

    # ── last_sync_status: NULL + CHECK
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'last_sync_status'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_sources]
            ADD [last_sync_status] NVARCHAR(20) NULL
    """)

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'CK_skw_ds_last_sync_status'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_sources]
            ADD CONSTRAINT [CK_skw_ds_last_sync_status]
                CHECK ([last_sync_status] IS NULL OR [last_sync_status] IN (
                    N'ok', N'error', N'partial'
                ))
    """)
    _log("01j", "last_sync_status + CHECK OK")

    # ── last_sync_message: NULL (opcjonalny komunikat ostatniej synchonizacji)
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'last_sync_message'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_sources]
            ADD [last_sync_message] NVARCHAR(500) NULL
    """)
    _log("01k", "last_sync_message NULL OK")

    # ── is_test_mode: NOT NULL DEFAULT 1 (nowe zrodla startuja w trybie testowym)
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'is_test_mode'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_sources]
            ADD [is_test_mode] BIT NOT NULL
                CONSTRAINT [DF_skw_ds_is_test_mode] DEFAULT 1
    """)
    _log("01l", "is_test_mode NOT NULL DEFAULT 1 OK")

    # ── webhook_token: NULL UNIQUE (tylko dla connection_mode=push)
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'webhook_token'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_sources]
            ADD [webhook_token] NVARCHAR(100) NULL
    """)

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'UQ_skw_ds_webhook_token'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_sources]
            ADD CONSTRAINT [UQ_skw_ds_webhook_token]
                UNIQUE ([webhook_token])
    """)
    _log("01m", "webhook_token NULL UNIQUE OK")

    # ── updated_at: NOT NULL DEFAULT SYSUTCDATETIME()
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'updated_at'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_sources]
            ADD [updated_at] DATETIME2(7) NOT NULL
                CONSTRAINT [DF_skw_ds_updated_at] DEFAULT SYSUTCDATETIME()
    """)
    _log("01n", "updated_at NOT NULL DEFAULT SYSUTCDATETIME OK")

    _log("01", "ZAKONCZONE — skw_document_sources rozszerzona o 10 kolumn")


# =============================================================================
# KROK 02 — ALTER TABLE skw_document_approval_instances (nowe kolumny)
# =============================================================================

def _krok02_approval_instances_new_columns() -> None:
    """
    Dodaje dwie nowe kolumny do skw_document_approval_instances:
      dispatch_attempts INT NOT NULL DEFAULT 0  — licznik prob auto-dispatch
      priority          INT NULL                 — priorytet (NULL = brak)
    """
    _log("02", "ALTER TABLE skw_document_approval_instances — dispatch_attempts, priority")

    # dispatch_attempts: liczy probe auto-assignmentu, nie moze byc ujemna
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_approval_instances]')
              AND name = N'dispatch_attempts'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_approval_instances]
            ADD [dispatch_attempts] INT NOT NULL
                CONSTRAINT [DF_skw_dai_dispatch_attempts] DEFAULT 0
    """)
    _log("02a", "dispatch_attempts NOT NULL DEFAULT 0 OK")

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_approval_instances]')
              AND name = N'CK_skw_dai_dispatch_attempts'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_approval_instances]
            ADD CONSTRAINT [CK_skw_dai_dispatch_attempts]
                CHECK ([dispatch_attempts] >= 0)
    """)
    _log("02b", "CHECK CK_skw_dai_dispatch_attempts OK")

    # priority: NULL = brak priorytetu (frontend traktuje jawnie — dokument
    # bez priorytetu nie jest filtrowany gdy filtr priorytetu aktywny)
    # Zmigrowane faktury beda mialy wartosc 1/2/3.
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_approval_instances]')
              AND name = N'priority'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_approval_instances]
            ADD [priority] INT NULL
    """)
    _log("02c", "priority INT NULL OK")

    _log("02", "ZAKONCZONE — dispatch_attempts i priority dodane")


# =============================================================================
# KROK 03 — CHECK constraint status (8 wartosci, idempotentna wymiana)
# =============================================================================

def _krok03_approval_instances_check_status() -> None:
    """
    Aktualizuje CHECK constraint na kolumnie status w skw_document_approval_instances.

    Stare statusy (5): pending_dispatch, in_progress, approved, cancelled, rejected
    Nowe statusy (8): + unassigned, duplicate_pending, source_orphaned

    Podejscie:
      1. Znajdz nazwe istniejacego constraintu przez sys.check_constraints
      2. DROP jesli istnieje
      3. CREATE nowy z pelna lista 8 statusow

    KRYTYCZNE: Wszystkie 8 wartosci w JEDNEJ instrukcji ALTER TABLE.
    Brak source_orphaned powodowal by constraint violation w workerze.
    """
    _log("03", "CHECK constraint status — wymiana na 8 wartosci")

    # Krok 03a: Znajdz nazwe istniejacego CHECK constraintu na kolumnie status.
    #
    # UWAGA: sys.check_constraints NIE ma parent_column_id dla constraintow
    # table-level. Szukamy przez definition zawierajace 'status' — to wystarczy
    # bo mamy jeden CHECK na kolumnie status w tej tabeli.
    # Multi-statement T-SQL w jednym op.execute() jest niedozwolony przez
    # ODBC Driver 18 — dlatego pobieramy nazwe do Pythona i wykonujemy DROP
    # w osobnym op.execute().
    conn = op.get_bind()
    result = conn.execute(text(f"""
        SELECT cc.name
          FROM sys.check_constraints cc
         WHERE cc.parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_approval_instances]')
           AND cc.definition LIKE N'%status%'
    """))
    row = result.fetchone()
    if row:
        old_constraint = row[0]
        _log("03a", f"Znaleziono stary constraint: {old_constraint} — DROP")
        _execute(
            f"ALTER TABLE [{SCHEMA}].[skw_document_approval_instances] "
            f"DROP CONSTRAINT [{old_constraint}]"
        )
    else:
        _log("03a", "Brak istniejacego CHECK constraint na status — pomijam DROP")

    # Dodaj nowy constraint z pelna lista 8 statusow
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_approval_instances]')
              AND name = N'CK_skw_dai_status_etap2'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_approval_instances]
            ADD CONSTRAINT [CK_skw_dai_status_etap2]
                CHECK ([status] IN (
                    N'pending_dispatch',
                    N'in_progress',
                    N'approved',
                    N'cancelled',
                    N'rejected',
                    N'unassigned',
                    N'duplicate_pending',
                    N'source_orphaned'
                ))
    """)
    _log("03b", "CREATE CK_skw_dai_status_etap2 (8 wartosci) OK")

    _log("03", "ZAKONCZONE — CHECK constraint status zaktualizowany")


# =============================================================================
# KROK 04 — ALTER TABLE skw_approval_filters (visibility_mode)
# =============================================================================

def _krok04_approval_filters_visibility_mode() -> None:
    """
    Dodaje kolumne visibility_mode do skw_approval_filters.

    visibility_mode NVARCHAR(20) NOT NULL DEFAULT 'inherit'
      inherit    = widocznosc dziedziczona (zachowanie dotychczasowe)
      restricted = ograniczona do przypisanych grup/userow
    """
    _log("04", "ALTER TABLE skw_approval_filters — visibility_mode")

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_approval_filters]')
              AND name = N'visibility_mode'
        )
        ALTER TABLE [{SCHEMA}].[skw_approval_filters]
            ADD [visibility_mode] NVARCHAR(20) NOT NULL
                CONSTRAINT [DF_skw_af_visibility_mode] DEFAULT N'inherit'
    """)
    _log("04a", "visibility_mode NOT NULL DEFAULT 'inherit' OK")

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_approval_filters]')
              AND name = N'CK_skw_af_visibility_mode'
        )
        ALTER TABLE [{SCHEMA}].[skw_approval_filters]
            ADD CONSTRAINT [CK_skw_af_visibility_mode]
                CHECK ([visibility_mode] IN (N'inherit', N'restricted'))
    """)
    _log("04b", "CHECK CK_skw_af_visibility_mode OK")

    _log("04", "ZAKONCZONE — visibility_mode dodane")


# =============================================================================
# KROK 05 — CREATE TABLE skw_source_hooks
# =============================================================================

def _krok05_source_hooks() -> None:
    """
    Tabela hookow automatycznych po akcjach obiegowych.

    DECYZJA (D-E02): trigger_action TYLKO accepted i rejected.
    Zewnetrzne systemy nie rozumieja semantyki akcji posrednich
    (rollback, forward) — CHECK constraint egzekwuje to na poziomie DB.

    Unikalnosc: jeden aktywny hook per (id_source, trigger_action).
    Realizacja: UNIQUE INDEX z filtrem WHERE is_active = 1.
    """
    _log("05", "CREATE TABLE skw_source_hooks")

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_source_hooks'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_source_hooks] (
                [id_hook]          INT           IDENTITY(1,1) NOT NULL,
                [id_source]        INT                         NOT NULL,
                [trigger_action]   NVARCHAR(30)                NOT NULL,
                [operation_type]   NVARCHAR(20)                NOT NULL,
                [operation_config] NVARCHAR(MAX)               NULL,
                [severity]         NVARCHAR(20)                NOT NULL,
                [is_active]        BIT                         NOT NULL
                                   CONSTRAINT [DF_skw_sh_is_active]
                                   DEFAULT 1,
                [created_at]       DATETIME2(7)                NOT NULL
                                   CONSTRAINT [DF_skw_sh_created_at]
                                   DEFAULT SYSUTCDATETIME(),
                [updated_at]       DATETIME2(7)                NOT NULL
                                   CONSTRAINT [DF_skw_sh_updated_at]
                                   DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_source_hooks]
                    PRIMARY KEY CLUSTERED ([id_hook] ASC),

                CONSTRAINT [CK_skw_sh_trigger_action]
                    CHECK ([trigger_action] IN (N'accepted', N'rejected')),

                CONSTRAINT [CK_skw_sh_operation_type]
                    CHECK ([operation_type] IN (N'sql_procedure', N'api_call')),

                CONSTRAINT [CK_skw_sh_severity]
                    CHECK ([severity] IN (N'critical', N'informational')),

                CONSTRAINT [FK_skw_sh_source]
                    FOREIGN KEY ([id_source])
                    REFERENCES [{SCHEMA}].[skw_document_sources] ([id_source])
                    ON DELETE CASCADE ON UPDATE NO ACTION
            );
            PRINT N'[0039-05] Tabela skw_source_hooks utworzona.'
        END
        ELSE
            PRINT N'[0039-05] Tabela skw_source_hooks juz istnieje — pomijam.'
    """)
    _log("05a", "CREATE TABLE OK")

    # Unikalny indeks filtrowany: max 1 aktywny hook per (source, action)
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_source_hooks]')
              AND name = N'UQ_skw_sh_source_action_active'
        )
        CREATE UNIQUE NONCLUSTERED INDEX [UQ_skw_sh_source_action_active]
            ON [{SCHEMA}].[skw_source_hooks] ([id_source], [trigger_action])
            WHERE [is_active] = 1
    """)
    _log("05b", "UNIQUE filtered index (is_active=1) OK")

    _log("05", "ZAKONCZONE — skw_source_hooks")


# =============================================================================
# KROK 06 — CREATE TABLE skw_source_actions
# =============================================================================

def _krok06_source_actions() -> None:
    """
    Tabela akcji zrodlowych — przyciski kontekstowe dla dokumentu z danego zrodla.

    Akcje nie przesuwaja dokumentu po obiegu — sa zewnetrzna operacja
    (wywolanie procedury SQL, API call, operacja plikowa).

    is_predefined BIT: rozroznia "klocki" aktywowane przez admina od
    w pelni niestandardowych akcji konfigurowanych recznie.
    """
    _log("06", "CREATE TABLE skw_source_actions")

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_source_actions'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_source_actions] (
                [id_action]            INT           IDENTITY(1,1) NOT NULL,
                [id_source]            INT                         NOT NULL,
                [action_name]          NVARCHAR(100)               NOT NULL,
                [action_label]         NVARCHAR(200)               NOT NULL,
                [operation_type]       NVARCHAR(20)                NOT NULL,
                [operation_config]     NVARCHAR(MAX)               NULL,
                [required_permission]  NVARCHAR(100)               NULL,
                [is_predefined]        BIT                         NOT NULL
                                       CONSTRAINT [DF_skw_sa_is_predefined]
                                       DEFAULT 0,
                [is_active]            BIT                         NOT NULL
                                       CONSTRAINT [DF_skw_sa_is_active]
                                       DEFAULT 1,
                [sort_order]           INT                         NOT NULL
                                       CONSTRAINT [DF_skw_sa_sort_order]
                                       DEFAULT 0,
                [created_at]           DATETIME2(7)                NOT NULL
                                       CONSTRAINT [DF_skw_sa_created_at]
                                       DEFAULT SYSUTCDATETIME(),
                [updated_at]           DATETIME2(7)                NOT NULL
                                       CONSTRAINT [DF_skw_sa_updated_at]
                                       DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_source_actions]
                    PRIMARY KEY CLUSTERED ([id_action] ASC),

                CONSTRAINT [CK_skw_sa_operation_type]
                    CHECK ([operation_type] IN (
                        N'sql_procedure', N'api_call',
                        N'file_move', N'file_delete'
                    )),

                CONSTRAINT [FK_skw_sa_source]
                    FOREIGN KEY ([id_source])
                    REFERENCES [{SCHEMA}].[skw_document_sources] ([id_source])
                    ON DELETE CASCADE ON UPDATE NO ACTION
            );
            PRINT N'[0039-06] Tabela skw_source_actions utworzona.'
        END
        ELSE
            PRINT N'[0039-06] Tabela skw_source_actions juz istnieje — pomijam.'
    """)
    _log("06a", "CREATE TABLE OK")

    # Indeks na id_source dla przyspieszenia CRUD akcji per-zrodlo
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_source_actions]')
              AND name = N'IX_skw_sa_source'
        )
        CREATE NONCLUSTERED INDEX [IX_skw_sa_source]
            ON [{SCHEMA}].[skw_source_actions] ([id_source], [is_active], [sort_order])
    """)
    _log("06b", "INDEX IX_skw_sa_source OK")

    _log("06", "ZAKONCZONE — skw_source_actions")


# =============================================================================
# KROK 07 — CREATE TABLE skw_source_action_log
# =============================================================================

def _krok07_source_action_log() -> None:
    """
    Log wszystkich wywolan hookow i akcji zrodlowych.

    KLUCZOWY do diagnostyki: kazde wywolanie — niezaleznie od wyniku —
    trafia do tej tabeli. Przy hooku krytycznym ktory zablokuje akceptacje,
    ten log jest jedynym miejscem gdzie widac co sie stalo.

    id_hook i id_action sa obie NULL lub jedna jest NOT NULL.
    CHECK constraint egzekwuje ze co najmniej jedno jest podane.
    id_user NULL = wywolanie systemowe (automatyczny hook).

    request_payload / response_payload: surowe dane diagnostyczne.
    Moga zawierac duze JSON-y — NVARCHAR(MAX) celowo.
    """
    _log("07", "CREATE TABLE skw_source_action_log")

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_source_action_log'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_source_action_log] (
                [id_log]              INT           IDENTITY(1,1) NOT NULL,
                [id_hook]             INT                         NULL,
                [id_action]           INT                         NULL,
                [id_instance]         INT                         NOT NULL,
                [id_user]             INT                         NULL,
                [executed_at]         DATETIME2(7)                NOT NULL
                                      CONSTRAINT [DF_skw_sal_executed_at]
                                      DEFAULT SYSUTCDATETIME(),
                [status]              NVARCHAR(20)                NOT NULL,
                [message]             NVARCHAR(500)               NULL,
                [execution_ms]        INT                         NULL,
                [request_payload]     NVARCHAR(MAX)               NULL,
                [response_payload]    NVARCHAR(MAX)               NULL,

                CONSTRAINT [PK_skw_source_action_log]
                    PRIMARY KEY CLUSTERED ([id_log] ASC),

                CONSTRAINT [CK_skw_sal_status]
                    CHECK ([status] IN (N'success', N'error', N'warning')),

                -- Co najmniej jedno z id_hook / id_action musi byc podane
                CONSTRAINT [CK_skw_sal_source_ref]
                    CHECK ([id_hook] IS NOT NULL OR [id_action] IS NOT NULL),

                -- execution_ms nie moze byc ujemne
                CONSTRAINT [CK_skw_sal_execution_ms]
                    CHECK ([execution_ms] IS NULL OR [execution_ms] >= 0),

                CONSTRAINT [FK_skw_sal_hook]
                    FOREIGN KEY ([id_hook])
                    REFERENCES [{SCHEMA}].[skw_source_hooks] ([id_hook])
                    ON DELETE NO ACTION ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_sal_action]
                    FOREIGN KEY ([id_action])
                    REFERENCES [{SCHEMA}].[skw_source_actions] ([id_action])
                    ON DELETE NO ACTION ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_sal_instance]
                    FOREIGN KEY ([id_instance])
                    REFERENCES [{SCHEMA}].[skw_document_approval_instances] ([id_instance])
                    ON DELETE NO ACTION ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_sal_user]
                    FOREIGN KEY ([id_user])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE SET NULL ON UPDATE NO ACTION
            );
            PRINT N'[0039-07] Tabela skw_source_action_log utworzona.'
        END
        ELSE
            PRINT N'[0039-07] Tabela skw_source_action_log juz istnieje — pomijam.'
    """)
    _log("07a", "CREATE TABLE OK")

    # Indeks pokrywajacy typowe zapytania diagnostyczne:
    # - "pokaz wszystkie logi dla tej instancji"
    # - "pokaz bledy ostatniej godziny"
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_source_action_log]')
              AND name = N'IX_skw_sal_instance_executed'
        )
        CREATE NONCLUSTERED INDEX [IX_skw_sal_instance_executed]
            ON [{SCHEMA}].[skw_source_action_log] ([id_instance], [executed_at] DESC)
            INCLUDE ([status], [message], [execution_ms])
    """)
    _log("07b", "INDEX IX_skw_sal_instance_executed OK")

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_source_action_log]')
              AND name = N'IX_skw_sal_status_executed'
        )
        CREATE NONCLUSTERED INDEX [IX_skw_sal_status_executed]
            ON [{SCHEMA}].[skw_source_action_log] ([status], [executed_at] DESC)
    """)
    _log("07c", "INDEX IX_skw_sal_status_executed OK")

    _log("07", "ZAKONCZONE — skw_source_action_log")


# =============================================================================
# KROK 08 — CREATE TABLE skw_document_folders
# =============================================================================

def _krok08_document_folders() -> None:
    """
    Teczki dokumentow — niezalezne od kategorii (skw_document_categories).

    Teczka moze byc prywatna (owner = user) lub zespolowa (owner = grupa).
    Jeden dokument moze byc w wielu teczkach jednoczesnie (wielowymiarowosc).
    Teczki to wylacznie mechanizm filtrowania — nie wplywaja na obieg.

    color: opcjonalny kolor w formacie hex (#RRGGBB) dla UI.
    folder_type: private|team — kontroluje widocznosc w UI.
    """
    _log("08", "CREATE TABLE skw_document_folders")

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_document_folders'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_document_folders] (
                [id_folder]    INT           IDENTITY(1,1) NOT NULL,
                [folder_name]  NVARCHAR(200)               NOT NULL,
                [description]  NVARCHAR(500)               NULL,
                [color]        NVARCHAR(7)                 NULL,
                [folder_type]  NVARCHAR(10)                NOT NULL
                               CONSTRAINT [DF_skw_df_folder_type]
                               DEFAULT N'private',
                [owner_user]   INT                         NULL,
                [owner_group]  INT                         NULL,
                [is_active]    BIT                         NOT NULL
                               CONSTRAINT [DF_skw_df_is_active]
                               DEFAULT 1,
                [created_at]   DATETIME2(7)                NOT NULL
                               CONSTRAINT [DF_skw_df_created_at]
                               DEFAULT SYSUTCDATETIME(),
                [updated_at]   DATETIME2(7)                NOT NULL
                               CONSTRAINT [DF_skw_df_updated_at]
                               DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_document_folders]
                    PRIMARY KEY CLUSTERED ([id_folder] ASC),

                CONSTRAINT [CK_skw_df_folder_type]
                    CHECK ([folder_type] IN (N'private', N'team')),

                -- Teczka musi miec wlasciciela — usera LUB grupe
                CONSTRAINT [CK_skw_df_owner]
                    CHECK (
                        ([owner_user] IS NOT NULL AND [owner_group] IS NULL)
                        OR
                        ([owner_user] IS NULL AND [owner_group] IS NOT NULL)
                    ),

                -- Format hex koloru (#RRGGBB lub NULL) — dokladnie 7 znakow
                CONSTRAINT [CK_skw_df_color]
                    CHECK (
                        [color] IS NULL
                        OR (
                            LEN([color]) = 7
                            AND [color] LIKE N'#[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]'
                        )
                    ),

                CONSTRAINT [FK_skw_df_owner_user]
                    FOREIGN KEY ([owner_user])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE NO ACTION ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_df_owner_group]
                    FOREIGN KEY ([owner_group])
                    REFERENCES [{SCHEMA}].[skw_approval_groups] ([id_group])
                    ON DELETE NO ACTION ON UPDATE NO ACTION
            );
            PRINT N'[0039-08] Tabela skw_document_folders utworzona.'
        END
        ELSE
            PRINT N'[0039-08] Tabela skw_document_folders juz istnieje — pomijam.'
    """)
    _log("08a", "CREATE TABLE OK")

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_folders]')
              AND name = N'IX_skw_df_owner_user'
        )
        CREATE NONCLUSTERED INDEX [IX_skw_df_owner_user]
            ON [{SCHEMA}].[skw_document_folders] ([owner_user], [is_active])
    """)

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_folders]')
              AND name = N'IX_skw_df_owner_group'
        )
        CREATE NONCLUSTERED INDEX [IX_skw_df_owner_group]
            ON [{SCHEMA}].[skw_document_folders] ([owner_group], [is_active])
    """)
    _log("08b", "INDEX-y owner_user, owner_group OK")

    _log("08", "ZAKONCZONE — skw_document_folders")


# =============================================================================
# KROK 09 — CREATE TABLE skw_document_folder_items
# =============================================================================

def _krok09_document_folder_items() -> None:
    """
    Relacja wiele-do-wielu dokument <-> teczka.

    added_by: kto dodal dokument do teczki (NULL jezeli import zbiorczy).
    PK kompozytowy (id_folder, id_instance) — jeden dokument w jednej teczce max raz.
    CASCADE DELETE z obu stron (usun teczke lub instancje = usun wpis).
    """
    _log("09", "CREATE TABLE skw_document_folder_items")

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_document_folder_items'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_document_folder_items] (
                [id_folder]   INT          NOT NULL,
                [id_instance] INT          NOT NULL,
                [added_by]    INT          NULL,
                [added_at]    DATETIME2(7) NOT NULL
                              CONSTRAINT [DF_skw_dfi_added_at]
                              DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_document_folder_items]
                    PRIMARY KEY CLUSTERED ([id_folder] ASC, [id_instance] ASC),

                CONSTRAINT [FK_skw_dfi_folder]
                    FOREIGN KEY ([id_folder])
                    REFERENCES [{SCHEMA}].[skw_document_folders] ([id_folder])
                    ON DELETE CASCADE ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_dfi_instance]
                    FOREIGN KEY ([id_instance])
                    REFERENCES [{SCHEMA}].[skw_document_approval_instances] ([id_instance])
                    ON DELETE CASCADE ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_dfi_user]
                    FOREIGN KEY ([added_by])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE SET NULL ON UPDATE NO ACTION
            );
            PRINT N'[0039-09] Tabela skw_document_folder_items utworzona.'
        END
        ELSE
            PRINT N'[0039-09] Tabela skw_document_folder_items juz istnieje — pomijam.'
    """)
    _log("09a", "CREATE TABLE OK")

    # Indeks odwrotny: znajdz wszystkie teczki dla danego dokumentu
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_folder_items]')
              AND name = N'IX_skw_dfi_instance'
        )
        CREATE NONCLUSTERED INDEX [IX_skw_dfi_instance]
            ON [{SCHEMA}].[skw_document_folder_items] ([id_instance])
            INCLUDE ([id_folder], [added_by], [added_at])
    """)
    _log("09b", "INDEX IX_skw_dfi_instance OK")

    _log("09", "ZAKONCZONE — skw_document_folder_items")


# =============================================================================
# KROK 10 — CREATE TABLE skw_approval_filter_visibility
# =============================================================================

def _krok10_approval_filter_visibility() -> None:
    """
    Widocznosc filtrow ograniczonych per grupa / per user.

    Uzywana gdy filtr ma visibility_mode = 'restricted'.
    Co najmniej jedno z id_group / id_user musi byc podane (CHECK constraint).
    Unikalnosc: jeden wpis per (id_filter, id_group) i per (id_filter, id_user).
    """
    _log("10", "CREATE TABLE skw_approval_filter_visibility")

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_approval_filter_visibility'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_approval_filter_visibility] (
                [id_visibility] INT          IDENTITY(1,1) NOT NULL,
                [id_filter]     INT                        NOT NULL,
                [id_group]      INT                        NULL,
                [id_user]       INT                        NULL,
                [created_at]    DATETIME2(7)               NOT NULL
                                CONSTRAINT [DF_skw_afv_created_at]
                                DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_approval_filter_visibility]
                    PRIMARY KEY CLUSTERED ([id_visibility] ASC),

                -- Co najmniej jedno z id_group / id_user musi byc podane
                CONSTRAINT [CK_skw_afv_target]
                    CHECK (
                        ([id_group] IS NOT NULL AND [id_user] IS NULL)
                        OR
                        ([id_group] IS NULL AND [id_user] IS NOT NULL)
                    ),

                CONSTRAINT [FK_skw_afv_filter]
                    FOREIGN KEY ([id_filter])
                    REFERENCES [{SCHEMA}].[skw_approval_filters] ([id_filter])
                    ON DELETE CASCADE ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_afv_group]
                    FOREIGN KEY ([id_group])
                    REFERENCES [{SCHEMA}].[skw_approval_groups] ([id_group])
                    ON DELETE NO ACTION ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_afv_user]
                    FOREIGN KEY ([id_user])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE NO ACTION ON UPDATE NO ACTION
            );
            PRINT N'[0039-10] Tabela skw_approval_filter_visibility utworzona.'
        END
        ELSE
            PRINT N'[0039-10] Tabela skw_approval_filter_visibility juz istnieje — pomijam.'
    """)
    _log("10a", "CREATE TABLE OK")

    # Unikalny indeks per (filter, group) — jeden wpis per kombinacja
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_approval_filter_visibility]')
              AND name = N'UQ_skw_afv_filter_group'
        )
        CREATE UNIQUE NONCLUSTERED INDEX [UQ_skw_afv_filter_group]
            ON [{SCHEMA}].[skw_approval_filter_visibility] ([id_filter], [id_group])
            WHERE [id_group] IS NOT NULL
    """)
    _log("10b", "UNIQUE INDEX per (filter, group) OK")

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_approval_filter_visibility]')
              AND name = N'UQ_skw_afv_filter_user'
        )
        CREATE UNIQUE NONCLUSTERED INDEX [UQ_skw_afv_filter_user]
            ON [{SCHEMA}].[skw_approval_filter_visibility] ([id_filter], [id_user])
            WHERE [id_user] IS NOT NULL
    """)
    _log("10c", "UNIQUE INDEX per (filter, user) OK")

    _log("10", "ZAKONCZONE — skw_approval_filter_visibility")


# =============================================================================
# KROK 11 — SEED nowych uprawnien
# =============================================================================

def _krok11_seed_permissions() -> None:
    """
    Wstawia nowe uprawnienia Etapu 2 do skw_Permissions.

    Uzywamy MERGE (nie INSERT) — operacja idempotentna, bezpieczna
    przy wielokrotnym uruchomieniu i przy ponownym stosowaniu na
    instancjach ktore moga miec czesciowe dane.

    WHEN NOT MATCHED -> INSERT ONLY.
    Bez WHEN MATCHED UPDATE — chronimy wartosci zmienione przez admina.

    Kategorie:
      sources.*   — zarzadzanie zrodlami dokumentow
      documents.* — zarzadzanie dokumentami (foldery, widocznosc)
      ksef.*      — zarzadzanie protokolem KSeF 2.0
    """
    _log("11", "SEED skw_Permissions — nowe uprawnienia Etapu 2")

    # Krok 11a: Rozszerzenie CHECK constraint CK_skw_Permissions_Category
    # o nowe kategorie Etapu 2. Wzorzec z migracji 0017 — DROP + ADD.
    # Pelna lista = wszystkie dotychczasowe + nowe.
    _log("11a", "Rozszerzenie CK_skw_Permissions_Category o nowe kategorie")
    _execute(f"""
        IF EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_Permissions]')
              AND name = N'CK_skw_Permissions_Category'
        )
        ALTER TABLE [{SCHEMA}].[skw_Permissions]
            DROP CONSTRAINT [CK_skw_Permissions_Category]
    """)
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_Permissions]')
              AND name = N'CK_skw_Permissions_Category'
        )
        ALTER TABLE [{SCHEMA}].[skw_Permissions]
            ADD CONSTRAINT [CK_skw_Permissions_Category] CHECK (
                [Category] IN (
                    N'auth',        N'users',       N'roles',       N'permissions',
                    N'debtors',     N'monits',       N'comments',    N'pdf',
                    N'reports',     N'snapshots',    N'audit',       N'system',
                    N'templates',   N'faktury',      N'approval',    N'dashboard',
                    N'sources',     N'documents',    N'ksef',        N'webhooks',
                    N'ksef2',       N'ocr',         N'deadlines',   N'sync',
                    N'dispatch',    N'duplicates',   N'hooks',       N'folders',
                    N'snapshot',    N'etap2'
                )
            )
    """)
    _log("11a", "CK_skw_Permissions_Category rozszerzona OK")

    permissions = [
        # ── sources ──────────────────────────────────────────────────────────
        ("sources.manage",
         "Zarzadzanie zrodlami dokumentow (panel admina): CRUD, konfiguracja",
         "sources"),
        ("sources.view",
         "Podglad listy i statusu zrodel dokumentow bez mozliwosci edycji",
         "sources"),
        ("sources.sync",
         "Reczne uruchamianie synchronizacji zrodla (poza harmonogramem)",
         "sources"),
        ("sources.view_log",
         "Podglad logow synchronizacji, hookow i akcji zrodlowych",
         "sources"),
        ("sources.manage_hooks",
         "CRUD hookow po akcjach obiegowych dla zrodla",
         "sources"),
        ("sources.manage_actions",
         "CRUD akcji zrodlowych (przyciski kontekstowe dokumentu)",
         "sources"),
        ("sources.execute_action",
         "Wykonywanie akcji zrodlowych na dokumentach (uprawnienie bazowe)",
         "sources"),
        ("sources.test_connection",
         "Testowanie polaczenia ze zrodlem (probna synchronizacja max 5 rekordow)",
         "sources"),
        ("sources.toggle_test_mode",
         "Przelaczanie trybu testowego zrodla (is_test_mode)",
         "sources"),
        ("sources.view_config",
         "Podglad connection_config zrodla (bez hasel — zanonimizowane)",
         "sources"),
        ("sources.manage_ksef",
         "Zarzadzanie instancjami KSeF 2.0 (tokeny, certyfikaty, firmy)",
         "sources"),
        # ── documents ────────────────────────────────────────────────────────
        ("documents.view",
         "Podglad listy dokumentow z nowej tabeli skw_document_approval_instances",
         "documents"),
        ("documents.view_all",
         "Widok wszystkich dokumentow — override widocznosci filtrow restricted",
         "documents"),
        ("documents.manage_folders",
         "Tworzenie, edycja i usuwanie teczek dokumentow",
         "documents"),
        ("documents.assign_folder",
         "Przypisywanie i usuwanie dokumentow z teczek",
         "documents"),
        ("documents.manage_duplicates",
         "Rozwiazywanie konfliktow duplikatow (potwierdzenie lub odrzucenie)",
         "documents"),
        ("documents.force_status",
         "Reczna zmiana statusu dokumentu (administracyjna operacja awaryjna)",
         "documents"),
        ("documents.view_source_orphaned",
         "Widok dokumentow ze statusem source_orphaned (znikly ze zrodla)",
         "documents"),
        # ── ksef ─────────────────────────────────────────────────────────────
        ("ksef.manage_sessions",
         "Zarzadzanie sesjami KSeF 2.0 (inicjowanie, zamykanie)",
         "ksef"),
        ("ksef.view_sessions",
         "Podglad aktywnych i historycznych sesji KSeF 2.0",
         "ksef"),
        ("ksef.manage_certificates",
         "Zarzadzanie certyfikatami XAdES dla KSeF 2.0",
         "ksef"),
        ("ksef.view_diagnostic",
         "Podglad diagnostycznych logow protokolu KSeF (XML, bledy 9105)",
         "ksef"),
        ("ksef.force_sync",
         "Wymuszona synchronizacja KSeF poza harmonogramem",
         "ksef"),
        # ── webhooks ─────────────────────────────────────────────────────────
        ("webhooks.receive",
         "Odbior dokumentow przez endpoint webhook (server-to-server)",
         "webhooks"),
        ("webhooks.manage",
         "Zarzadzanie tokenami i konfiguracja webhook dla zrodla",
         "webhooks"),
    ]

    # Pojedynczy MERGE dla wszystkich uprawnien — osobne op.execute() per batch
    for perm_name, description, category in permissions:
        _execute(f"""
            MERGE [{SCHEMA}].[skw_Permissions] AS target
            USING (
                SELECT
                    N'{perm_name}'    AS PermissionName,
                    N'{description}'  AS Description,
                    N'{category}'     AS Category
            ) AS source
            ON target.[PermissionName] = source.[PermissionName]
            WHEN NOT MATCHED THEN
                INSERT ([PermissionName], [Description], [Category], [IsActive])
                VALUES (source.[PermissionName], source.[Description], source.[Category], 1);
        """)

    _log("11", f"ZAKONCZONE — wstawiono {len(permissions)} nowych uprawnien (MERGE idempotentny)")


# =============================================================================
# KROK 12 — SEED nowych kluczy SystemConfig
# =============================================================================

def _krok12_seed_system_config() -> None:
    """
    Wstawia nowe klucze konfiguracyjne Etapu 2 do skw_SystemConfig.

    Wartosci domyslne sa BEZPIECZNE (tryb ograniczony, flagi wylaczone).
    Nowe zrodla startuja w is_test_mode=1 — musi byc jawnie wlaczone.
    Hasel i tokenow nie przechowujemy w SystemConfig — sa w connection_config.

    WHEN NOT MATCHED -> INSERT ONLY.
    Bez WHEN MATCHED UPDATE — nie nadpisujemy wartosci zmienionych przez admina.
    """
    _log("12", "SEED skw_SystemConfig — nowe klucze Etapu 2")

    config_entries = [
        # ── Feature flagi Etapu 2 ────────────────────────────────────────────
        ("ETAP2_ENABLED",
         "false",
         "Glowna flaga wlaczajaca Etap 2. False = system dziala jak przed Etapem 2.",
         "etap2"),
        ("ETAP2_FAKTURA_ENDPOINT_NEW_IMPL",
         "false",
         "Flaga przelaczajaca /faktury-akceptacja na nowa implementacje (skw_document_approval_instances). Wymaga ukonczonego Kroku 0.",
         "etap2"),
        ("ETAP2_DOCUMENTS_ENDPOINT_ENABLED",
         "false",
         "Wlaczenie nowego endpointu GET /documents.",
         "etap2"),

        # ── Synchronizacja zrodel ─────────────────────────────────────────────
        ("SOURCE_SYNC_WORKER_INTERVAL_MINUTES",
         "5",
         "Bazowy interwal cyklu workera synchronizacji zrodel (minuty). Per-zrodlo kontrolowane przez sync_interval_minutes.",
         "sync"),
        ("SOURCE_SYNC_MAX_DOCUMENTS_PER_CYCLE",
         "500",
         "Maksymalna liczba nowych dokumentow przetwarzanych przez workera w jednym cyklu na zrodlo.",
         "sync"),
        ("SOURCE_SYNC_MAX_RETRY_ATTEMPTS",
         "3",
         "Maksymalna liczba prob ponownej synchronizacji zrodla po bledzie.",
         "sync"),
        ("SOURCE_SYNC_RETRY_BACKOFF_SECONDS",
         "60",
         "Czas oczekiwania (sekundy) miedzy kolejnymi probami po bledzie synchronizacji.",
         "sync"),
        ("SOURCE_SYNC_ALERT_AFTER_FAILURES",
         "3",
         "Liczba kolejnych bledow synchronizacji po ktorych wysylany jest alert SSE CRITICAL do adminow.",
         "sync"),
        ("SOURCE_SYNC_TIMEOUT_SECONDS",
         "300",
         "Timeout pojedynczej operacji synchronizacji zrodla (sekundy). Po przekroczeniu — status error.",
         "sync"),

        # ── Auto-dispatch ─────────────────────────────────────────────────────
        ("AUTO_DISPATCH_ENABLED",
         "true",
         "Wlaczenie automatycznego przypisania dokumentow do sciezek obiegu.",
         "dispatch"),
        ("AUTO_DISPATCH_MAX_ATTEMPTS",
         "5",
         "Maksymalna liczba prob auto-dispatch przed ustawieniem statusu unassigned.",
         "dispatch"),
        ("AUTO_DISPATCH_RETRY_INTERVAL_MINUTES",
         "30",
         "Interwal miedzy probami auto-dispatch dla dokumentow ze statusem unassigned.",
         "dispatch"),

        # ── Detekcja duplikatow ───────────────────────────────────────────────
        ("DUPLICATE_DETECTION_ENABLED",
         "true",
         "Wlaczenie detekcji duplikatow podczas synchronizacji i webhookow.",
         "duplicates"),
        ("DUPLICATE_DETECTION_AMOUNT_TOLERANCE",
         "0.01",
         "Tolerancja kwoty brutto przy porownaniu duplikatow (PLN).",
         "duplicates"),
        ("DUPLICATE_DETECTION_DATE_RANGE_DAYS",
         "90",
         "Zakres dni wstecz przeszukiwanych przy detekcji duplikatow.",
         "duplicates"),

        # ── Hook Service ──────────────────────────────────────────────────────
        ("HOOK_DEFAULT_TIMEOUT_SECONDS",
         "30",
         "Domyslny timeout oczekiwania na odpowiedz zewnetrznego systemu (hook). Min 5, max 120.",
         "hooks"),
        ("HOOK_CRITICAL_MAX_LOCK_SECONDS",
         "130",
         "Maksymalny czas trzymania approval_lock przez hook krytyczny (timeout + margines).",
         "hooks"),
        ("HOOK_LOG_REQUEST_PAYLOAD",
         "true",
         "Logowanie request_payload do skw_source_action_log (false dla danych wrazliwych).",
         "hooks"),
        ("HOOK_LOG_RESPONSE_PAYLOAD",
         "true",
         "Logowanie response_payload do skw_source_action_log.",
         "hooks"),

        # ── Webhook endpoint ──────────────────────────────────────────────────
        ("WEBHOOK_ENABLED",
         "false",
         "Wlaczenie publicznego endpointu POST /webhooks/sources/{{token}}.",
         "webhooks"),
        ("WEBHOOK_HMAC_REQUIRED",
         "false",
         "Wymaganie podpisu HMAC-SHA256 dla webhookow. False = tylko token w URL.",
         "webhooks"),
        ("WEBHOOK_RATE_LIMIT_PER_MINUTE",
         "100",
         "Maksymalna liczba zadan per token w ciagu minuty (Redis rate limiting).",
         "webhooks"),

        # ── Teczki ───────────────────────────────────────────────────────────
        ("FOLDERS_ENABLED",
         "true",
         "Wlaczenie mechanizmu teczek dokumentow.",
         "folders"),
        ("FOLDERS_MAX_PER_DOCUMENT",
         "20",
         "Maksymalna liczba teczek do ktorych mozna przypisac jeden dokument.",
         "folders"),

        # ── KSeF 2.0 ─────────────────────────────────────────────────────────
        ("KSEF2_ENABLED",
         "false",
         "Wlaczenie adaptera KSeF 2.0 (XAdES, szyfrowane paczki AES). Wymaga certyfikatu testowego MF.",
         "ksef2"),
        ("KSEF2_DIAGNOSTIC_XML_ENABLED",
         "false",
         "Zapisywanie surowych pakietow XML KSeF do logow diagnostycznych.",
         "ksef2"),
        ("KSEF2_SESSION_POLL_INTERVAL_SECONDS",
         "30",
         "Interwal pollingu statusu sesji KSeF 2.0 (sekundy).",
         "ksef2"),
        ("KSEF2_HASH_MISMATCH_ACTION",
         "warn",
         "Akcja przy niezgodnosci hasha dokumentu KSeF: error (blokuj) lub warn (loguj i kontynuuj).",
         "ksef2"),

        # ── OCR ──────────────────────────────────────────────────────────────
        ("OCR_ENABLED",
         "false",
         "Wlaczenie pipeline OCR dla zalacznikow PDF (pytesseract + pdf2image).",
         "ocr"),
        ("OCR_LANGUAGE",
         "pol",
         "Jezyk OCR (kod Tesseract). pol = polski.",
         "ocr"),
        ("OCR_MIN_CONFIDENCE_SCORE",
         "0.6",
         "Minimalny confidence_score OCR do zapisania wynikow w extra_data.",
         "ocr"),
        ("OCR_MAX_PAGES",
         "3",
         "Maksymalna liczba stron PDF przetwarzanych przez OCR na dokument.",
         "ocr"),

        # ── Terminy i SMS ─────────────────────────────────────────────────────
        ("DEADLINE_REMINDER_HOURS_BEFORE",
         "24",
         "Liczba godzin przed terminem kiedy wysylane sa przypomnienia SSE/SMS.",
         "deadlines"),
        ("DEADLINE_REMINDER_INTERVAL_HOURS",
         "2",
         "Interwal (godziny) miedzy kolejnymi przypomnieniami po pierwszym.",
         "deadlines"),
        ("DEADLINE_SMS_ENABLED",
         "false",
         "Wlaczenie powiadomien SMS przy zblizajacym sie terminie (wymaga konta SMS).",
         "deadlines"),

        # ── Snapshot task ─────────────────────────────────────────────────────
        ("SNAPSHOT_INCLUDE_ETAP2_TABLES",
         "true",
         "Dolaczenie nowych tabel Etapu 2 do codziennego snapshotu (02:00 Warsaw).",
         "snapshot"),
    ]

    for key, value, description, category in config_entries:
        _execute(f"""
            MERGE [{SCHEMA}].[skw_SystemConfig] AS target
            USING (
                SELECT
                    N'{key}'         AS ConfigKey,
                    N'{value}'       AS ConfigValue,
                    N'{description}' AS Description
            ) AS source
            ON target.[ConfigKey] = source.[ConfigKey]
            WHEN NOT MATCHED THEN
                INSERT ([ConfigKey], [ConfigValue], [Description], [IsActive])
                VALUES (source.[ConfigKey], source.[ConfigValue], source.[Description], 1);
        """)

    _log("12", f"ZAKONCZONE — wstawiono {len(config_entries)} nowych kluczy SystemConfig (MERGE idempotentny)")


# =============================================================================
# KROK 13 — MERGE skw_SchemaChecksums
# =============================================================================

def _krok13_schema_checksums() -> None:
    """
    Rejestruje nowe obiekty schematu w skw_SchemaChecksums.

    Checksum obliczany przez MSSQL CHECKSUM() = INT (nie MD5 hex).
    Mechanizm schema_tamper_watchdog porownuje to przy starcie serwisu.

    Rejestrujemy:
      - 6 nowych tabel
      - Zmodyfikowane tabele (nowe kolumny zmieniaja ich definicje)
    """
    _log("13", "MERGE skw_SchemaChecksums — rejestracja nowych obiektow")

    objects_to_register = [
        "skw_source_hooks",
        "skw_source_actions",
        "skw_source_action_log",
        "skw_document_folders",
        "skw_document_folder_items",
        "skw_approval_filter_visibility",
        "skw_document_sources",
        "skw_document_approval_instances",
        "skw_approval_filters",
    ]

    for obj_name in objects_to_register:
        _execute(f"""
            MERGE [{SCHEMA}].[skw_SchemaChecksums] AS target
            USING (
                SELECT
                    N'{SCHEMA}'    AS SchemaName,
                    N'{obj_name}'  AS ObjectName,
                    CHECKSUM_AGG(CHECKSUM(
                        c.column_id,
                        c.name,
                        c.system_type_id,
                        c.max_length,
                        c.is_nullable
                    )) AS Checksum,
                    SYSUTCDATETIME() AS Now
                FROM sys.columns c
                JOIN sys.tables  t ON t.object_id = c.object_id
                JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE t.name   = N'{obj_name}'
                  AND s.name   = N'{SCHEMA}'
            ) AS source
            ON  target.[SchemaName] = source.[SchemaName]
            AND target.[ObjectName] = source.[ObjectName]
            WHEN MATCHED THEN
                UPDATE SET
                    target.[Checksum]       = source.[Checksum],
                    target.[LastVerifiedAt] = source.[Now]
            WHEN NOT MATCHED THEN
                INSERT ([SchemaName], [ObjectName], [Checksum], [LastVerifiedAt])
                VALUES (source.[SchemaName], source.[ObjectName], source.[Checksum], source.[Now]);
        """)

    _log("13", f"ZAKONCZONE — {len(objects_to_register)} obiektow zarejestrowanych w SchemaChecksums")


# =============================================================================
# DOWNGRADE
# =============================================================================

def downgrade() -> None:
    """
    Cofa migracje 0039.

    UWAGA: downgrade usuwa tabele z danymi.
    Jesli Krok 0 (migracja danych faktur) zostal juz wykonany,
    downgrade jest nieodwracalny — przywrocenie wymaga backup.

    Kolejnosc DROP: odwrotna do CREATE (FK dependencies).
    """
    logger.warning("[0039] ============================================================")
    logger.warning("[0039] START downgrade — cofanie F1 DDL infrastruktury Etapu 2")
    logger.warning("[0039] UWAGA: usuwa tabele z danymi jezeli juz istnieja")
    logger.warning("[0039] ============================================================")

    # ── DROP nowych tabel (odwrotna kolejnosc FK) ─────────────────────────────
    tables_to_drop = [
        "skw_approval_filter_visibility",
        "skw_document_folder_items",
        "skw_document_folders",
        "skw_source_action_log",
        "skw_source_actions",
        "skw_source_hooks",
    ]

    for table in tables_to_drop:
        _execute(f"""
            IF EXISTS (
                SELECT 1 FROM sys.tables t
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = N'{SCHEMA}' AND t.name = N'{table}'
            )
            DROP TABLE [{SCHEMA}].[{table}]
        """)
        _log("DOWN", f"DROP TABLE {table} OK")

    # ── Cofnij CHECK constraint status (wymiana z powrotem na 5 wartosci) ─────
    _execute(f"""
        IF EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_approval_instances]')
              AND name = N'CK_skw_dai_status_etap2'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_approval_instances]
            DROP CONSTRAINT [CK_skw_dai_status_etap2]
    """)

    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_approval_instances]')
              AND name = N'CK_skw_dai_status_pre_etap2'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_approval_instances]
            ADD CONSTRAINT [CK_skw_dai_status_pre_etap2]
                CHECK ([status] IN (
                    N'pending_dispatch',
                    N'in_progress',
                    N'approved',
                    N'cancelled',
                    N'rejected'
                ))
    """)
    _log("DOWN", "CHECK constraint status cofniety do 5 wartosci OK")

    # ── Cofnij kolumny dispatch_attempts i priority ───────────────────────────
    # MSSQL nie pozwoli DROP COLUMN gdy kolumna ma aktywny CHECK constraint.
    # Kolejnosc: najpierw DROP CHECK, potem DROP COLUMN.
    _execute(f"""
        IF EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_approval_instances]')
              AND name = N'CK_skw_dai_dispatch_attempts'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_approval_instances]
            DROP CONSTRAINT [CK_skw_dai_dispatch_attempts]
    """)
    _log("DOWN", "DROP CONSTRAINT CK_skw_dai_dispatch_attempts OK")

    for col in ("priority", "dispatch_attempts"):
        _execute(f"""
            IF EXISTS (
                SELECT 1 FROM sys.columns
                WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_approval_instances]')
                  AND name = N'{col}'
            )
            ALTER TABLE [{SCHEMA}].[skw_document_approval_instances]
                DROP COLUMN [{col}]
        """)
        _log("DOWN", f"DROP COLUMN {col} OK")

    # ── Cofnij visibility_mode z skw_approval_filters ────────────────────────
    _execute(f"""
        IF EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_approval_filters]')
              AND name = N'CK_skw_af_visibility_mode'
        )
        ALTER TABLE [{SCHEMA}].[skw_approval_filters]
            DROP CONSTRAINT [CK_skw_af_visibility_mode]
    """)

    _execute(f"""
        IF EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_approval_filters]')
              AND name = N'visibility_mode'
        )
        ALTER TABLE [{SCHEMA}].[skw_approval_filters]
            DROP CONSTRAINT [DF_skw_af_visibility_mode]
    """)

    _execute(f"""
        IF EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_approval_filters]')
              AND name = N'visibility_mode'
        )
        ALTER TABLE [{SCHEMA}].[skw_approval_filters]
            DROP COLUMN [visibility_mode]
    """)
    _log("DOWN", "DROP COLUMN visibility_mode z skw_approval_filters OK")

    # ── Kolumny skw_document_sources cofamy tylko te ktore nie maja danych ────
    # UWAGA: source_type i connection_mode maja dane (UPDATE w upgrade).
    # Jesli wiersze istnieja — DROP COLUMN zadziala ale dane sa stracone.
    new_ds_columns = [
        ("DF_skw_ds_updated_at",              "updated_at"),
        ("UQ_skw_ds_webhook_token",           None),             # constraint only
        (None,                                "webhook_token"),
        ("DF_skw_ds_is_test_mode",            "is_test_mode"),
        (None,                                "last_sync_message"),
        ("CK_skw_ds_last_sync_status",        None),
        (None,                                "last_sync_status"),
        (None,                                "last_sync_at"),
        ("DF_skw_ds_sync_interval_minutes",   "sync_interval_minutes"),
        (None,                                "connection_config"),
        ("CK_skw_ds_connection_mode",         None),
        (None,                                "connection_mode"),
        ("CK_skw_ds_source_type",             None),
        (None,                                "source_type"),
    ]

    for constraint, column in new_ds_columns:
        if constraint:
            _execute(f"""
                IF EXISTS (
                    SELECT 1 FROM sys.objects
                    WHERE name = N'{constraint}'
                      AND parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
                )
                ALTER TABLE [{SCHEMA}].[skw_document_sources]
                    DROP CONSTRAINT [{constraint}]
            """)
        if column:
            _execute(f"""
                IF EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
                      AND name = N'{column}'
                )
                ALTER TABLE [{SCHEMA}].[skw_document_sources]
                    DROP COLUMN [{column}]
            """)

    _log("DOWN", "Kolumny skw_document_sources cofniete OK")

    logger.warning("[0039] ============================================================")
    logger.warning("[0039] ZAKONCZONE downgrade — F1 DDL infrastruktura cofnieta")
    logger.warning("[0039] Sprawdz czy skw_SchemaChecksums wymaga recznej aktualizacji")
    logger.warning("[0039] ============================================================")