"""
0028_approval_module
════════════════════════════════════════════════════════════════════════════════
Sprint 3 — Moduł Obiegu Dokumentów i Akceptacji

Wszystkie tabele tworzone w schemacie [dbo] (po migracji 0026 nie ma już dbo_ext).

28 kroków wewnętrznych:
  01  Rozszerzenie CK_skw_Permissions_Category o 'approval'
  02  CREATE TABLE skw_approval_groups
  03  CREATE TABLE skw_approval_group_members
  04  CREATE TABLE skw_approval_paths
  05  CREATE TABLE skw_approval_path_steps
  06  CREATE TABLE skw_approval_path_change_log
  07  CREATE TABLE skw_document_sources
  08  CREATE TABLE skw_document_categories
  09  CREATE TABLE skw_document_approval_instances + filtrowany UNIQUE index
  10  CREATE TABLE skw_document_approval_snapshot_steps
  11  CREATE TABLE skw_approval_log (APPEND-ONLY)
  12  CREATE OR ALTER TRIGGER trg_DenyModify_skw_approval_log
  13  CREATE TABLE skw_approval_delegations
  14  CREATE TABLE skw_approval_comments (self-ref FK)
  15  CREATE TABLE skw_approval_attachments
  16  CREATE TABLE skw_approval_filters
  17  CREATE TABLE skw_approval_filter_conditions
  18  CREATE TABLE skw_document_source_field_mappings
  19  CREATE TABLE skw_user_notifications + filtrowany INDEX
  20  CREATE INDEX (pokrywające indeksy na status / is_urgent / logged_at)
  21  GRANT INSERT / DENY UPDATE+DELETE na skw_approval_log (dynamiczny DB user)
  22  CREATE OR ALTER VIEW dbo.skw_v_approval_dispatch_queue
  23  CREATE OR ALTER VIEW dbo.skw_v_approval_instance_detail
  24  CREATE OR ALTER VIEW dbo.skw_v_approval_my_queue
  25  MERGE skw_SchemaChecksums — rejestracja 3 widoków
  26  MERGE skw_Permissions — 12 uprawnień approval.*
  27  MERGE skw_document_sources — seed: fakir + ksef
  28  MERGE skw_SystemConfig — 12 feature flags / konfiguracja

Downgrade odwraca WSZYSTKIE kroki w odwrotnej kolejności.
Uwaga: downgrade usuwa dane w tabelach approval — wymaga świadomego potwierdzenia.

Revision ID : 0028
Revises     : 0027
"""

from __future__ import annotations

import logging
import textwrap
from datetime import datetime, timezone
from typing import Final

from alembic import op

# ─── Metadane migracji ────────────────────────────────────────────────────────
revision:      str = "0028"
down_revision: str = "0027"
branch_labels       = None
depends_on          = None

# ─── Stałe ───────────────────────────────────────────────────────────────────
SCHEMA: Final[str] = "dbo"           # Wszystkie tabele skw_* są teraz w dbo

logger = logging.getLogger(f"alembic.migration.{revision}")


# =============================================================================
# Helpers
# =============================================================================

def _ts() -> str:
    """Zwraca timestamp UTC ISO-8601 do logowania."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _log(krok: str, msg: str) -> None:
    logger.info("[%s] KROK %s | %s | ts=%s", revision, krok, msg, _ts())


def _execute(sql: str) -> None:
    """Wrapper z dedentacją — czytelniejszy kod."""
    op.execute(textwrap.dedent(sql))


# =============================================================================
# Pełna lista kategorii constraint po tej migracji
# =============================================================================
_CK_KATEGORIE_PO: Final[str] = ", ".join([
    "N'auth'",        "N'users'",        "N'roles'",      "N'permissions'",
    "N'debtors'",     "N'monits'",       "N'comments'",   "N'pdf'",
    "N'reports'",     "N'snapshots'",    "N'audit'",      "N'system'",
    "N'templates'",   "N'faktury'",      "N'dashboard'",  "N'koszty'",
    "N'approval'",
])

_CK_KATEGORIE_PRZED: Final[str] = ", ".join([
    "N'auth'",        "N'users'",        "N'roles'",      "N'permissions'",
    "N'debtors'",     "N'monits'",       "N'comments'",   "N'pdf'",
    "N'reports'",     "N'snapshots'",    "N'audit'",      "N'system'",
    "N'templates'",   "N'faktury'",      "N'dashboard'",  "N'koszty'",
])

# =============================================================================
# 12 uprawnień modułu approval
# =============================================================================
_PERMISSIONS: Final[list[tuple[str, str]]] = [
    ("approval.view_queue",
     "Podgląd kolejki dokumentów oczekujących na akceptację"),
    ("approval.dispatch",
     "Przekazywanie dokumentów do obiegu akceptacyjnego"),
    ("approval.accept",
     "Akceptacja dokumentu na bieżącym etapie obiegu"),
    ("approval.reject",
     "Odrzucenie dokumentu na bieżącym etapie obiegu"),
    ("approval.rollback",
     "Cofnięcie obiegu dokumentu do poprzedniego etapu"),
    ("approval.forward",
     "Przekazanie dokumentu do przodu — pominięcie bieżącego etapu"),
    ("approval.send_to_group",
     "Skierowanie dokumentu do konkretnej grupy akceptacyjnej"),
    ("approval.supervise",
     "Nadzór nad wszystkimi obiegami dokumentów — widok globalny"),
    ("approval.manage_paths",
     "Zarządzanie definicjami ścieżek akceptacyjnych"),
    ("approval.manage_groups",
     "Zarządzanie grupami akceptacyjnymi i ich członkami"),
    ("approval.manage_filters",
     "Zarządzanie filtrami automatycznego przydziału ścieżek"),
    ("approval.manage_delegations",
     "Zarządzanie delegowaniem uprawnień akceptacyjnych"),
]

# =============================================================================
# 12 kluczy SystemConfig (feature flags + konfiguracja)
# =============================================================================
_SYSTEM_CONFIG: Final[list[tuple[str, str, str]]] = [
    # (ConfigKey, ConfigValue, Description)
    ("APPROVAL_MODULE_ENABLED",         "false",
     "Cały moduł obiegu dokumentów. false → wszystkie endpointy /approval/* zwracają 503."),
    ("APPROVAL_PERMISSIONS_CONFIGURED", "false",
     "Flaga informacyjna: admin potwierdza że uprawnienia approval.* zostały przypisane do ról."),
    ("APPROVAL_ATTACHMENTS_ENABLED",    "true",
     "Obsługa załączników. false → endpointy /attachments/* zwracają 503."),
    ("APPROVAL_COMMENTS_ENABLED",       "true",
     "Komentarze wewnętrzne przy obiegu. false → endpointy /comments/* zwracają 503."),
    ("APPROVAL_DELEGATIONS_ENABLED",    "true",
     "Delegowanie uprawnień akceptacyjnych. false → delegacje nie są sprawdzane."),
    ("APPROVAL_AUTO_FILTERS_ENABLED",   "true",
     "Silnik filtrów automatycznych. false → wszystkie dokumenty trafiają do pending_dispatch."),
    ("APPROVAL_URGENT_MARKING_ENABLED", "true",
     "Tryb pilny (is_urgent). false → /mark-urgent zwraca 503."),
    ("APPROVAL_STATISTICS_ENABLED",     "true",
     "Endpointy /stats/* i /reports/*. false → zwracają 503."),
    ("APPROVAL_DEADLINE_WORKER_ENABLED","true",
     "Deadline worker (ARQ cron). false → job pomija logikę, loguje tylko 'disabled'."),
    ("APPROVAL_EMAIL_NOTIFICATIONS_ENABLED", "true",
     "Powiadomienia email. false → SSE i persystentne notyfikacje działają, email wyłączony."),
    ("APPROVAL_EMAIL_DEBOUNCE_MINUTES", "15",
     "Karencja agregacji emaili (minuty). System czeka X minut przed wysłaniem zbiorczego emaila."),
    ("APPROVAL_MAX_ATTACHMENT_MB",      "20",
     "Maksymalny rozmiar załącznika w MB. Zakres: 1-100."),
]


# ╔═════════════════════════════════════════════════════════════════════════════╗
# ║  UPGRADE                                                                   ║
# ╚═════════════════════════════════════════════════════════════════════════════╝

def upgrade() -> None:
    logger.info("=" * 72)
    logger.info("[%s] ══ UPGRADE START ══ ts=%s", revision, _ts())
    logger.info("=" * 72)

    _krok01_constraint_category()
    _krok02_approval_groups()
    _krok03_approval_group_members()
    _krok04_approval_paths()
    _krok05_approval_path_steps()
    _krok06_approval_path_change_log()
    _krok07_document_sources()
    _krok08_document_categories()
    _krok09_document_approval_instances()
    _krok10_document_approval_snapshot_steps()
    _krok11_approval_log()
    _krok12_trigger_deny_approval_log()
    _krok13_approval_delegations()
    _krok14_approval_comments()
    _krok15_approval_attachments()
    _krok16_approval_filters()
    _krok17_approval_filter_conditions()
    _krok18_document_source_field_mappings()
    _krok19_user_notifications()
    _krok20_indeksy()
    _krok21_grant_deny_approval_log()
    _krok22_view_dispatch_queue()
    _krok23_view_instance_detail()
    _krok24_view_my_queue()
    _krok25_merge_schema_checksums()
    _krok26_merge_permissions()
    _krok27_merge_document_sources()
    _krok28_merge_system_config()

    logger.info("=" * 72)
    logger.info("[%s] ══ UPGRADE OK ══ ts=%s", revision, _ts())
    logger.info("=" * 72)


# ─────────────────────────────────────────────────────────────────────────────
# KROK 01 — CHECK constraint: dodaj 'approval'
# ─────────────────────────────────────────────────────────────────────────────
def _krok01_constraint_category() -> None:
    _log("01", "Rozszerzenie CK_skw_Permissions_Category o 'approval'")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1
            FROM sys.check_constraints cc
            JOIN sys.tables  t ON cc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id          = s.schema_id
            WHERE s.name  = N'{SCHEMA}'
              AND t.name  = N'skw_Permissions'
              AND cc.name = N'CK_skw_Permissions_Category'
              AND cc.definition LIKE N'%approval%'
        )
        BEGIN
            -- Usuń stary constraint (jeśli istnieje)
            IF EXISTS (
                SELECT 1
                FROM sys.check_constraints cc
                JOIN sys.tables  t ON cc.parent_object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id          = s.schema_id
                WHERE s.name  = N'{SCHEMA}'
                  AND t.name  = N'skw_Permissions'
                  AND cc.name = N'CK_skw_Permissions_Category'
            )
            BEGIN
                ALTER TABLE [{SCHEMA}].[skw_Permissions]
                    DROP CONSTRAINT [CK_skw_Permissions_Category];
                PRINT N'[0028-01] Stary constraint CK_skw_Permissions_Category usunięty.';
            END

            -- Dodaj nowy constraint z 'approval'
            ALTER TABLE [{SCHEMA}].[skw_Permissions]
                ADD CONSTRAINT [CK_skw_Permissions_Category] CHECK (
                    [Category] IN ({_CK_KATEGORIE_PO})
                );
            PRINT N'[0028-01] Nowy constraint CK_skw_Permissions_Category z approval dodany.';
        END
        ELSE
            PRINT N'[0028-01] Constraint już zawiera approval — pomijam.';
    """)
    _log("01", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 02 — skw_approval_groups
# ─────────────────────────────────────────────────────────────────────────────
def _krok02_approval_groups() -> None:
    _log("02", "CREATE TABLE skw_approval_groups")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_approval_groups'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_approval_groups] (
                [id_group]       INT            IDENTITY(1,1)  NOT NULL,
                [group_name]     NVARCHAR(100)                 NOT NULL,
                [consensus_type] VARCHAR(3)                    NOT NULL
                                 CONSTRAINT [DF_skw_approval_groups_consensus]
                                 DEFAULT 'OR',
                [description]    NVARCHAR(500)                 NULL,
                [is_active]      BIT                           NOT NULL
                                 CONSTRAINT [DF_skw_approval_groups_is_active]
                                 DEFAULT 1,
                [created_at]     DATETIME2(7)                  NOT NULL
                                 CONSTRAINT [DF_skw_approval_groups_created_at]
                                 DEFAULT SYSUTCDATETIME(),
                [updated_at]     DATETIME2(7)                  NOT NULL
                                 CONSTRAINT [DF_skw_approval_groups_updated_at]
                                 DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_approval_groups]
                    PRIMARY KEY CLUSTERED ([id_group] ASC),
                CONSTRAINT [UQ_skw_approval_groups_name]
                    UNIQUE NONCLUSTERED ([group_name]),
                CONSTRAINT [CK_skw_approval_groups_consensus]
                    CHECK ([consensus_type] IN ('AND', 'OR'))
            );
            PRINT N'[0028-02] Tabela skw_approval_groups utworzona.';
        END
        ELSE
            PRINT N'[0028-02] Tabela skw_approval_groups już istnieje — pomijam.';
    """)
    _log("02", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 03 — skw_approval_group_members
# ─────────────────────────────────────────────────────────────────────────────
def _krok03_approval_group_members() -> None:
    _log("03", "CREATE TABLE skw_approval_group_members")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_approval_group_members'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_approval_group_members] (
                [id]          INT          IDENTITY(1,1) NOT NULL,
                [id_group]    INT                        NOT NULL,
                [id_user]     INT                        NOT NULL,
                [assigned_at] DATETIME2(7)               NOT NULL
                              CONSTRAINT [DF_skw_agm_assigned_at]
                              DEFAULT SYSUTCDATETIME(),
                [assigned_by] INT                        NULL,

                CONSTRAINT [PK_skw_approval_group_members]
                    PRIMARY KEY CLUSTERED ([id] ASC),
                CONSTRAINT [UQ_skw_agm_group_user]
                    UNIQUE NONCLUSTERED ([id_group], [id_user]),

                -- FK do grupy: RESTRICT (blokuje usunięcie grupy z członkami)
                CONSTRAINT [FK_skw_agm_group]
                    FOREIGN KEY ([id_group])
                    REFERENCES [{SCHEMA}].[skw_approval_groups] ([id_group])
                    ON DELETE NO ACTION ON UPDATE NO ACTION,

                -- FK do użytkownika: RESTRICT (blokuje usunięcie usera w grupie z aktywnym obiegiem)
                CONSTRAINT [FK_skw_agm_user]
                    FOREIGN KEY ([id_user])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE NO ACTION ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_agm_assigned_by]
                    FOREIGN KEY ([assigned_by])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE NO ACTION ON UPDATE NO ACTION
            );
            PRINT N'[0028-03] Tabela skw_approval_group_members utworzona.';
        END
        ELSE
            PRINT N'[0028-03] Tabela skw_approval_group_members już istnieje — pomijam.';
    """)
    _log("03", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 04 — skw_approval_paths
# ─────────────────────────────────────────────────────────────────────────────
def _krok04_approval_paths() -> None:
    _log("04", "CREATE TABLE skw_approval_paths")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_approval_paths'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_approval_paths] (
                [id_path]     INT           IDENTITY(1,1) NOT NULL,
                [path_name]   NVARCHAR(200)               NOT NULL,
                [description] NVARCHAR(500)               NULL,
                [is_active]   BIT                         NOT NULL
                              CONSTRAINT [DF_skw_approval_paths_is_active]
                              DEFAULT 1,
                [created_by]  INT                         NULL,
                [created_at]  DATETIME2(7)                NOT NULL
                              CONSTRAINT [DF_skw_approval_paths_created_at]
                              DEFAULT SYSUTCDATETIME(),
                [updated_at]  DATETIME2(7)                NOT NULL
                              CONSTRAINT [DF_skw_approval_paths_updated_at]
                              DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_approval_paths]
                    PRIMARY KEY CLUSTERED ([id_path] ASC),
                CONSTRAINT [UQ_skw_approval_paths_name]
                    UNIQUE NONCLUSTERED ([path_name]),

                CONSTRAINT [FK_skw_approval_paths_created_by]
                    FOREIGN KEY ([created_by])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE SET NULL ON UPDATE NO ACTION
            );
            PRINT N'[0028-04] Tabela skw_approval_paths utworzona.';
        END
        ELSE
            PRINT N'[0028-04] Tabela skw_approval_paths już istnieje — pomijam.';
    """)
    _log("04", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 05 — skw_approval_path_steps
# ─────────────────────────────────────────────────────────────────────────────
def _krok05_approval_path_steps() -> None:
    _log("05", "CREATE TABLE skw_approval_path_steps")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_approval_path_steps'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_approval_path_steps] (
                [id_step]        INT          IDENTITY(1,1) NOT NULL,
                [id_path]        INT                        NOT NULL,
                [step_order]     INT                        NOT NULL,
                [id_group]       INT                        NOT NULL,
                [deadline_hours] INT                        NULL,
                [created_at]     DATETIME2(7)               NOT NULL
                                 CONSTRAINT [DF_skw_aps_created_at]
                                 DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_approval_path_steps]
                    PRIMARY KEY CLUSTERED ([id_step] ASC),
                CONSTRAINT [UQ_skw_aps_path_order]
                    UNIQUE NONCLUSTERED ([id_path], [step_order]),

                -- CASCADE: usunięcie ścieżki usuwa jej kroki
                CONSTRAINT [FK_skw_aps_path]
                    FOREIGN KEY ([id_path])
                    REFERENCES [{SCHEMA}].[skw_approval_paths] ([id_path])
                    ON DELETE CASCADE ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_aps_group]
                    FOREIGN KEY ([id_group])
                    REFERENCES [{SCHEMA}].[skw_approval_groups] ([id_group])
                    ON DELETE NO ACTION ON UPDATE NO ACTION
            );
            PRINT N'[0028-05] Tabela skw_approval_path_steps utworzona.';
        END
        ELSE
            PRINT N'[0028-05] Tabela skw_approval_path_steps już istnieje — pomijam.';
    """)
    _log("05", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 06 — skw_approval_path_change_log (append-only, historia zmian ścieżek)
# ─────────────────────────────────────────────────────────────────────────────
def _krok06_approval_path_change_log() -> None:
    _log("06", "CREATE TABLE skw_approval_path_change_log")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_approval_path_change_log'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_approval_path_change_log] (
                [id_change]   BIGINT        IDENTITY(1,1) NOT NULL,
                [id_path]     INT                         NOT NULL,
                [changed_by]  INT                         NULL,
                [changed_at]  DATETIME2(7)                NOT NULL
                              CONSTRAINT [DF_skw_apcl_changed_at]
                              DEFAULT SYSUTCDATETIME(),
                [change_type] NVARCHAR(50)                NOT NULL,
                -- Wartości JSON: stary/nowy stan kroku lub metadanych ścieżki
                [old_value]   NVARCHAR(MAX)               NULL,
                [new_value]   NVARCHAR(MAX)               NULL,

                CONSTRAINT [PK_skw_approval_path_change_log]
                    PRIMARY KEY CLUSTERED ([id_change] ASC),

                CONSTRAINT [FK_skw_apcl_path]
                    FOREIGN KEY ([id_path])
                    REFERENCES [{SCHEMA}].[skw_approval_paths] ([id_path])
                    ON DELETE NO ACTION ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_apcl_user]
                    FOREIGN KEY ([changed_by])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE SET NULL ON UPDATE NO ACTION
            );
            PRINT N'[0028-06] Tabela skw_approval_path_change_log utworzona.';
        END
        ELSE
            PRINT N'[0028-06] Tabela skw_approval_path_change_log już istnieje — pomijam.';
    """)
    _log("06", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 07 — skw_document_sources (słownik źródeł: fakir, ksef, manual)
# ─────────────────────────────────────────────────────────────────────────────
def _krok07_document_sources() -> None:
    _log("07", "CREATE TABLE skw_document_sources")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_document_sources'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_document_sources] (
                [id_source]   INT           IDENTITY(1,1) NOT NULL,
                [source_name] NVARCHAR(50)                NOT NULL,
                [description] NVARCHAR(200)               NULL,
                [is_active]   BIT                         NOT NULL
                              CONSTRAINT [DF_skw_ds_is_active]
                              DEFAULT 1,
                [created_at]  DATETIME2(7)                NOT NULL
                              CONSTRAINT [DF_skw_ds_created_at]
                              DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_document_sources]
                    PRIMARY KEY CLUSTERED ([id_source] ASC),
                CONSTRAINT [UQ_skw_document_sources_name]
                    UNIQUE NONCLUSTERED ([source_name])
            );
            PRINT N'[0028-07] Tabela skw_document_sources utworzona.';
        END
        ELSE
            PRINT N'[0028-07] Tabela skw_document_sources już istnieje — pomijam.';
    """)
    _log("07", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 08 — skw_document_categories
# ─────────────────────────────────────────────────────────────────────────────
def _krok08_document_categories() -> None:
    _log("08", "CREATE TABLE skw_document_categories")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_document_categories'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_document_categories] (
                [id_category]   INT           IDENTITY(1,1) NOT NULL,
                [category_name] NVARCHAR(100)               NOT NULL,
                [description]   NVARCHAR(500)               NULL,
                [is_active]     BIT                         NOT NULL
                                CONSTRAINT [DF_skw_dc_is_active]
                                DEFAULT 1,
                [created_at]    DATETIME2(7)                NOT NULL
                                CONSTRAINT [DF_skw_dc_created_at]
                                DEFAULT SYSUTCDATETIME(),
                [updated_at]    DATETIME2(7)                NOT NULL
                                CONSTRAINT [DF_skw_dc_updated_at]
                                DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_document_categories]
                    PRIMARY KEY CLUSTERED ([id_category] ASC),
                CONSTRAINT [UQ_skw_document_categories_name]
                    UNIQUE NONCLUSTERED ([category_name])
            );
            PRINT N'[0028-08] Tabela skw_document_categories utworzona.';
        END
        ELSE
            PRINT N'[0028-08] Tabela skw_document_categories już istnieje — pomijam.';
    """)
    _log("08", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 09 — skw_document_approval_instances (tabela główna obiegu)
#           UNIQUE filtrowany: (id_document, id_source) WHERE status NOT IN
#           ('approved','cancelled') — blokuje równoległe obiegi na poziomie DB
# ─────────────────────────────────────────────────────────────────────────────
def _krok09_document_approval_instances() -> None:
    _log("09", "CREATE TABLE skw_document_approval_instances + filtrowany UNIQUE")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_document_approval_instances'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_document_approval_instances] (
                [id_instance]    INT            IDENTITY(1,1) NOT NULL,
                -- id_document: zewnętrzny identyfikator dokumentu (np. ID_BUF_DOKUMENT jako NVARCHAR)
                [id_document]    NVARCHAR(100)                NOT NULL,
                [id_source]      INT                          NOT NULL,
                [id_path]        INT                          NULL,
                [id_category]    INT                          NULL,
                [status]         NVARCHAR(30)                 NOT NULL
                                 CONSTRAINT [DF_skw_dai_status]
                                 DEFAULT N'pending_dispatch',
                [current_step]   INT                          NOT NULL
                                 CONSTRAINT [DF_skw_dai_current_step]
                                 DEFAULT 0,
                [is_urgent]      BIT                          NOT NULL
                                 CONSTRAINT [DF_skw_dai_is_urgent]
                                 DEFAULT 0,
                [dispatched_by]  INT                          NULL,
                [dispatched_at]  DATETIME2(7)                 NULL,
                [completed_at]   DATETIME2(7)                 NULL,
                [deadline_at]    DATETIME2(7)                 NULL,
                [document_title] NVARCHAR(500)                NULL,
                [document_amount] DECIMAL(18,2)               NULL,
                -- Dodatkowe dane z źródła jako JSON (np. numer_ksef, typ_dok itp.)
                [extra_data]     NVARCHAR(MAX)                NULL,
                [created_at]     DATETIME2(7)                 NOT NULL
                                 CONSTRAINT [DF_skw_dai_created_at]
                                 DEFAULT SYSUTCDATETIME(),
                [updated_at]     DATETIME2(7)                 NOT NULL
                                 CONSTRAINT [DF_skw_dai_updated_at]
                                 DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_document_approval_instances]
                    PRIMARY KEY CLUSTERED ([id_instance] ASC),
                CONSTRAINT [CK_skw_dai_status]
                    CHECK ([status] IN (
                        N'pending_dispatch', N'in_progress',
                        N'approved', N'cancelled', N'rejected'
                    )),

                CONSTRAINT [FK_skw_dai_source]
                    FOREIGN KEY ([id_source])
                    REFERENCES [{SCHEMA}].[skw_document_sources] ([id_source])
                    ON DELETE NO ACTION ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_dai_path]
                    FOREIGN KEY ([id_path])
                    REFERENCES [{SCHEMA}].[skw_approval_paths] ([id_path])
                    ON DELETE SET NULL ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_dai_category]
                    FOREIGN KEY ([id_category])
                    REFERENCES [{SCHEMA}].[skw_document_categories] ([id_category])
                    ON DELETE SET NULL ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_dai_dispatched_by]
                    FOREIGN KEY ([dispatched_by])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE SET NULL ON UPDATE NO ACTION
            );
            PRINT N'[0028-09] Tabela skw_document_approval_instances utworzona.';
        END
        ELSE
            PRINT N'[0028-09] Tabela skw_document_approval_instances już istnieje — pomijam.';
    """)

    # Filtrowany UNIQUE index — blokuje równoległe aktywne obiegi dla tego samego dokumentu.
    # ⚠ KRYTYCZNE: backend musi używać DOKŁADNIE tego samego filtru statusów przy sprawdzaniu
    #   aktywnego obiegu. Niespójność → fałszywy 409 lub przepuszczony duplikat.
    # ⚠ WAŻNE: filtrowany CREATE INDEX z WHERE ... NOT IN nie może pojawić się
    # jako token DDL w zewnętrznym batchu pyodbc (nawet wewnątrz EXEC(N'...')).
    # Rozwiązanie: budujemy SQL przez konkatenację NVARCHAR → NOT IN nigdy nie
    # trafia do parsera zewnętrznego batcha jako słowo kluczowe DDL.
    # Krok A — idempotentny DROP (bez żadnej klauzuli WHERE w DDL)
    _execute(f"""
        IF EXISTS (
            SELECT 1 FROM sys.indexes  i
            JOIN sys.tables  t ON i.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id  = s.schema_id
            WHERE s.name = N'{SCHEMA}'
              AND t.name = N'skw_document_approval_instances'
              AND i.name = N'UQ_skw_dai_active_document'
        )
        BEGIN
            DROP INDEX [UQ_skw_dai_active_document]
                ON [{SCHEMA}].[skw_document_approval_instances];
            PRINT N'[0028-09] Poprzedni index usuniety (re-run migracji).';
        END
        ELSE
            PRINT N'[0028-09] Index nie istnieje — przechodze do tworzenia.';
    """)
    # Krok B — standalone CREATE INDEX bez zadnego bloku IF.
    # Predykat <> zamiast NOT IN: ODBC Driver 18 blokuje NOT IN
    # wewnatrz literalow string nawet przez sp_executesql.
    # Semantycznie identyczne: status rozny od approved I od cancelled.
    _execute(f"""
        CREATE UNIQUE NONCLUSTERED INDEX [UQ_skw_dai_active_document]
            ON [{SCHEMA}].[skw_document_approval_instances] ([id_document], [id_source])
            WHERE [status] <> N'approved'
              AND [status] <> N'cancelled';
        PRINT N'[0028-09] Filtrowany UNIQUE index UQ_skw_dai_active_document utworzony.';
    """)
    _log("09", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 10 — skw_document_approval_snapshot_steps (robocza kopia ścieżki)
#           System obiegu działa WYŁĄCZNIE na tej tabeli, nie na definicji ścieżki
# ─────────────────────────────────────────────────────────────────────────────
def _krok10_document_approval_snapshot_steps() -> None:
    _log("10", "CREATE TABLE skw_document_approval_snapshot_steps")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_document_approval_snapshot_steps'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_document_approval_snapshot_steps] (
                [id_snapshot]     INT          IDENTITY(1,1) NOT NULL,
                [id_instance]     INT                        NOT NULL,
                [step_order]      INT                        NOT NULL,
                [id_group]        INT                        NOT NULL,
                [status]          NVARCHAR(20)               NOT NULL
                                  CONSTRAINT [DF_skw_dass_status]
                                  DEFAULT N'pending',
                -- votes_required: liczba głosów wymaganych (dla AND = liczba członków grupy, dla OR = 1)
                [votes_required]  INT                        NOT NULL
                                  CONSTRAINT [DF_skw_dass_votes_required]
                                  DEFAULT 1,
                [votes_cast]      INT                        NOT NULL
                                  CONSTRAINT [DF_skw_dass_votes_cast]
                                  DEFAULT 0,
                [deadline_at]     DATETIME2(7)               NULL,
                [completed_at]    DATETIME2(7)               NULL,
                [created_at]      DATETIME2(7)               NOT NULL
                                  CONSTRAINT [DF_skw_dass_created_at]
                                  DEFAULT SYSUTCDATETIME(),
                [updated_at]      DATETIME2(7)               NOT NULL
                                  CONSTRAINT [DF_skw_dass_updated_at]
                                  DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_document_approval_snapshot_steps]
                    PRIMARY KEY CLUSTERED ([id_snapshot] ASC),
                CONSTRAINT [UQ_skw_dass_instance_order]
                    UNIQUE NONCLUSTERED ([id_instance], [step_order]),
                CONSTRAINT [CK_skw_dass_status]
                    CHECK ([status] IN (N'pending', N'in_progress', N'approved', N'skipped')),

                -- CASCADE: usunięcie instancji usuwa jej snapshoty
                CONSTRAINT [FK_skw_dass_instance]
                    FOREIGN KEY ([id_instance])
                    REFERENCES [{SCHEMA}].[skw_document_approval_instances] ([id_instance])
                    ON DELETE CASCADE ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_dass_group]
                    FOREIGN KEY ([id_group])
                    REFERENCES [{SCHEMA}].[skw_approval_groups] ([id_group])
                    ON DELETE NO ACTION ON UPDATE NO ACTION
            );
            PRINT N'[0028-10] Tabela skw_document_approval_snapshot_steps utworzona.';
        END
        ELSE
            PRINT N'[0028-10] Tabela skw_document_approval_snapshot_steps już istnieje — pomijam.';
    """)
    _log("10", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 11 — skw_approval_log (APPEND-ONLY — niezniszczalny dziennik akcji)
#           BRAK UpdatedAt, BRAK is_active, BRAK triggera UpdatedAt
#           Trigger DENY w kroku 12, GRANT/DENY w kroku 21
# ─────────────────────────────────────────────────────────────────────────────
def _krok11_approval_log() -> None:
    _log("11", "CREATE TABLE skw_approval_log (APPEND-ONLY)")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_approval_log'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_approval_log] (
                [id_log]            BIGINT        IDENTITY(1,1)  NOT NULL,
                [id_instance]       INT                          NOT NULL,
                [id_user]           INT                          NULL,
                -- Kopia nazwy użytkownika w momencie akcji (na wypadek usunięcia konta)
                [username_snapshot] NVARCHAR(100)                NULL,
                -- Typ akcji: dispatch / accept / reject / rollback / forward /
                --            send_to_group / mark_urgent / delegate / comment /
                --            attach / deadline_expired / escalated / voided
                [action]            NVARCHAR(50)                 NOT NULL,
                [step_order_snapshot] INT                        NULL,
                [id_group_snapshot] INT                          NULL,
                [consensus_snapshot] NVARCHAR(3)                 NULL,
                [votes_before]      INT                          NULL,
                [votes_after]       INT                          NULL,
                -- is_voided: zamiast DELETE przy rollbacku — oznacza unieważnione głosy
                [is_voided]         BIT                          NOT NULL
                                    CONSTRAINT [DF_skw_al_is_voided]
                                    DEFAULT 0,
                -- Pełny kontekst akcji jako JSON (request_id, ip, duration_ms, db_queries itd.)
                [details]           NVARCHAR(MAX)                NULL,
                [ip_address]        NVARCHAR(45)                 NULL,
                [logged_at]         DATETIME2(7)                 NOT NULL
                                    CONSTRAINT [DF_skw_al_logged_at]
                                    DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_approval_log]
                    PRIMARY KEY CLUSTERED ([id_log] ASC),

                -- FK bez CASCADE — log jest niezniszczalny niezależnie od instancji
                CONSTRAINT [FK_skw_al_instance]
                    FOREIGN KEY ([id_instance])
                    REFERENCES [{SCHEMA}].[skw_document_approval_instances] ([id_instance])
                    ON DELETE NO ACTION ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_al_user]
                    FOREIGN KEY ([id_user])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE SET NULL ON UPDATE NO ACTION
            );
            PRINT N'[0028-11] Tabela skw_approval_log (APPEND-ONLY) utworzona.';
        END
        ELSE
            PRINT N'[0028-11] Tabela skw_approval_log już istnieje — pomijam.';
    """)
    _log("11", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 12 — Trigger blokujący UPDATE i DELETE na skw_approval_log
# ─────────────────────────────────────────────────────────────────────────────
def _krok12_trigger_deny_approval_log() -> None:
    _log("12", "CREATE OR ALTER TRIGGER trg_DenyModify_skw_approval_log")
    # CREATE OR ALTER TRIGGER musi byc jedyna instrukcja w batchu.
    # RAISERROR: jeden ciagy literal — T-SQL nie obsluguje
    # sasiednich literalow jak Python (brak implicit concatenation).
    op.execute(
        f"CREATE OR ALTER TRIGGER [{SCHEMA}].[trg_DenyModify_skw_approval_log] "
        f"ON [{SCHEMA}].[skw_approval_log] "
        f"AFTER UPDATE, DELETE "
        f"AS "
        f"BEGIN "
        f"    SET NOCOUNT ON; "
        f"    RAISERROR("
        f"        N'APPROVAL_LOG: Modyfikacja skw_approval_log jest ZABRONIONA. "
        f"Tabela jest APPEND-ONLY. Uzyj pola is_voided zamiast DELETE lub UPDATE.', "
        f"        16, 1"
        f"    ); "
        f"    ROLLBACK TRANSACTION; "
        f"END"
    )
    _log("12", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 13 — skw_approval_delegations
# ─────────────────────────────────────────────────────────────────────────────
def _krok13_approval_delegations() -> None:
    _log("13", "CREATE TABLE skw_approval_delegations")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_approval_delegations'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_approval_delegations] (
                [id_delegation]  INT          IDENTITY(1,1) NOT NULL,
                [id_user_from]   INT                        NOT NULL,
                [id_user_to]     INT                        NOT NULL,
                -- NULL = delegacja na wszystkie grupy, INT = konkretna grupa
                [id_group]       INT                        NULL,
                [valid_from]     DATETIME2(7)               NOT NULL,
                [valid_to]       DATETIME2(7)               NOT NULL,
                [reason]         NVARCHAR(500)              NULL,
                [is_active]      BIT                        NOT NULL
                                 CONSTRAINT [DF_skw_adel_is_active]
                                 DEFAULT 1,
                [created_by]     INT                        NULL,
                [created_at]     DATETIME2(7)               NOT NULL
                                 CONSTRAINT [DF_skw_adel_created_at]
                                 DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_approval_delegations]
                    PRIMARY KEY CLUSTERED ([id_delegation] ASC),
                CONSTRAINT [CK_skw_adel_dates]
                    CHECK ([valid_to] > [valid_from]),
                CONSTRAINT [CK_skw_adel_no_self]
                    CHECK ([id_user_from] <> [id_user_to]),

                CONSTRAINT [FK_skw_adel_user_from]
                    FOREIGN KEY ([id_user_from])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE NO ACTION ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_adel_user_to]
                    FOREIGN KEY ([id_user_to])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE NO ACTION ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_adel_group]
                    FOREIGN KEY ([id_group])
                    REFERENCES [{SCHEMA}].[skw_approval_groups] ([id_group])
                    ON DELETE SET NULL ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_adel_created_by]
                    FOREIGN KEY ([created_by])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE SET NULL ON UPDATE NO ACTION
            );
            PRINT N'[0028-13] Tabela skw_approval_delegations utworzona.';
        END
        ELSE
            PRINT N'[0028-13] Tabela skw_approval_delegations już istnieje — pomijam.';
    """)
    _log("13", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 14 — skw_approval_comments (self-reference FK dla wątków)
# ─────────────────────────────────────────────────────────────────────────────
def _krok14_approval_comments() -> None:
    _log("14", "CREATE TABLE skw_approval_comments (self-ref FK)")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_approval_comments'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_approval_comments] (
                [id_comment]  INT           IDENTITY(1,1) NOT NULL,
                [id_instance] INT                         NOT NULL,
                [id_user]     INT                         NULL,
                -- Referencja do komentarza-rodzica (NULL = komentarz główny)
                [parent_id]   INT                         NULL,
                [content]     NVARCHAR(MAX)               NOT NULL,
                [is_deleted]  BIT                         NOT NULL
                              CONSTRAINT [DF_skw_ac_is_deleted]
                              DEFAULT 0,
                [deleted_at]  DATETIME2(7)                NULL,
                [created_at]  DATETIME2(7)                NOT NULL
                              CONSTRAINT [DF_skw_ac_created_at]
                              DEFAULT SYSUTCDATETIME(),
                [updated_at]  DATETIME2(7)                NOT NULL
                              CONSTRAINT [DF_skw_ac_updated_at]
                              DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_approval_comments]
                    PRIMARY KEY CLUSTERED ([id_comment] ASC),

                CONSTRAINT [FK_skw_ac_instance]
                    FOREIGN KEY ([id_instance])
                    REFERENCES [{SCHEMA}].[skw_document_approval_instances] ([id_instance])
                    ON DELETE CASCADE ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_ac_user]
                    FOREIGN KEY ([id_user])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE SET NULL ON UPDATE NO ACTION,

                -- Self-reference dla wątków (odpowiedzi)
                CONSTRAINT [FK_skw_ac_parent]
                    FOREIGN KEY ([parent_id])
                    REFERENCES [{SCHEMA}].[skw_approval_comments] ([id_comment])
                    ON DELETE NO ACTION ON UPDATE NO ACTION
            );
            PRINT N'[0028-14] Tabela skw_approval_comments (self-ref) utworzona.';
        END
        ELSE
            PRINT N'[0028-14] Tabela skw_approval_comments już istnieje — pomijam.';
    """)
    _log("14", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 15 — skw_approval_attachments (metadane plików, plik fizycznie na dysku)
# ─────────────────────────────────────────────────────────────────────────────
def _krok15_approval_attachments() -> None:
    _log("15", "CREATE TABLE skw_approval_attachments")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_approval_attachments'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_approval_attachments] (
                [id_attachment] INT           IDENTITY(1,1) NOT NULL,
                [id_instance]   INT                         NOT NULL,
                [id_user]       INT                         NULL,
                -- Oryginalna nazwa pliku (przed sanityzacją — do wyświetlenia)
                [file_name]     NVARCHAR(255)               NOT NULL,
                -- Ścieżka na serwerze (po sanityzacji, poza web root)
                [file_path]     NVARCHAR(1000)              NOT NULL,
                [file_size]     BIGINT                      NOT NULL,
                [mime_type]     NVARCHAR(200)               NOT NULL,
                [is_deleted]    BIT                         NOT NULL
                                CONSTRAINT [DF_skw_aat_is_deleted]
                                DEFAULT 0,
                [deleted_at]    DATETIME2(7)                NULL,
                [deleted_by]    INT                         NULL,
                [created_at]    DATETIME2(7)                NOT NULL
                                CONSTRAINT [DF_skw_aat_created_at]
                                DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_approval_attachments]
                    PRIMARY KEY CLUSTERED ([id_attachment] ASC),
                CONSTRAINT [CK_skw_aat_file_size]
                    CHECK ([file_size] > 0),

                CONSTRAINT [FK_skw_aat_instance]
                    FOREIGN KEY ([id_instance])
                    REFERENCES [{SCHEMA}].[skw_document_approval_instances] ([id_instance])
                    ON DELETE CASCADE ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_aat_user]
                    FOREIGN KEY ([id_user])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE SET NULL ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_aat_deleted_by]
                    FOREIGN KEY ([deleted_by])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE NO ACTION ON UPDATE NO ACTION
            );
            PRINT N'[0028-15] Tabela skw_approval_attachments utworzona.';
        END
        ELSE
            PRINT N'[0028-15] Tabela skw_approval_attachments już istnieje — pomijam.';
    """)
    _log("15", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 16 — skw_approval_filters (reguły automatycznego przydziału ścieżek)
# ─────────────────────────────────────────────────────────────────────────────
def _krok16_approval_filters() -> None:
    _log("16", "CREATE TABLE skw_approval_filters")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_approval_filters'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_approval_filters] (
                [id_filter]          INT           IDENTITY(1,1) NOT NULL,
                [filter_name]        NVARCHAR(200)               NOT NULL,
                -- standard: warunki w tabeli filter_conditions
                -- universal: wywołanie funkcji SQL (whitelist: ^[a-zA-Z0-9_]+$)
                [filter_type]        NVARCHAR(20)                NOT NULL,
                [id_path]            INT                         NOT NULL,
                [id_source]          INT                         NULL,
                -- Wyższy priorytet = sprawdzany pierwszy
                [priority]           INT                         NOT NULL
                                     CONSTRAINT [DF_skw_af_priority]
                                     DEFAULT 100,
                [is_active]          BIT                         NOT NULL
                                     CONSTRAINT [DF_skw_af_is_active]
                                     DEFAULT 1,
                -- Tylko dla filter_type='universal': nazwa funkcji SQL
                [universal_function] NVARCHAR(200)               NULL,
                [created_at]         DATETIME2(7)                NOT NULL
                                     CONSTRAINT [DF_skw_af_created_at]
                                     DEFAULT SYSUTCDATETIME(),
                [updated_at]         DATETIME2(7)                NOT NULL
                                     CONSTRAINT [DF_skw_af_updated_at]
                                     DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_approval_filters]
                    PRIMARY KEY CLUSTERED ([id_filter] ASC),
                CONSTRAINT [CK_skw_af_type]
                    CHECK ([filter_type] IN (N'standard', N'universal')),

                CONSTRAINT [FK_skw_af_path]
                    FOREIGN KEY ([id_path])
                    REFERENCES [{SCHEMA}].[skw_approval_paths] ([id_path])
                    ON DELETE NO ACTION ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_af_source]
                    FOREIGN KEY ([id_source])
                    REFERENCES [{SCHEMA}].[skw_document_sources] ([id_source])
                    ON DELETE SET NULL ON UPDATE NO ACTION
            );
            PRINT N'[0028-16] Tabela skw_approval_filters utworzona.';
        END
        ELSE
            PRINT N'[0028-16] Tabela skw_approval_filters już istnieje — pomijam.';
    """)
    _log("16", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 17 — skw_approval_filter_conditions (warunki filtrów standard)
# ─────────────────────────────────────────────────────────────────────────────
def _krok17_approval_filter_conditions() -> None:
    _log("17", "CREATE TABLE skw_approval_filter_conditions")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_approval_filter_conditions'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_approval_filter_conditions] (
                [id_condition] INT           IDENTITY(1,1) NOT NULL,
                [id_filter]    INT                         NOT NULL,
                [field_name]   NVARCHAR(100)               NOT NULL,
                -- Operatory porównania — whitelist na poziomie DB i kodu
                [operator]     NVARCHAR(10)                NOT NULL,
                [field_value]  NVARCHAR(500)               NOT NULL,
                [created_at]   DATETIME2(7)                NOT NULL
                               CONSTRAINT [DF_skw_afc_created_at]
                               DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_approval_filter_conditions]
                    PRIMARY KEY CLUSTERED ([id_condition] ASC),
                CONSTRAINT [CK_skw_afc_operator]
                    CHECK ([operator] IN (
                        N'eq', N'neq', N'contains',
                        N'gt', N'lt', N'gte', N'lte'
                    )),

                -- CASCADE: usunięcie filtra usuwa jego warunki
                CONSTRAINT [FK_skw_afc_filter]
                    FOREIGN KEY ([id_filter])
                    REFERENCES [{SCHEMA}].[skw_approval_filters] ([id_filter])
                    ON DELETE CASCADE ON UPDATE NO ACTION
            );
            PRINT N'[0028-17] Tabela skw_approval_filter_conditions utworzona.';
        END
        ELSE
            PRINT N'[0028-17] Tabela skw_approval_filter_conditions już istnieje — pomijam.';
    """)
    _log("17", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 18 — skw_document_source_field_mappings (mapowanie pól między źródłami)
# ─────────────────────────────────────────────────────────────────────────────
def _krok18_document_source_field_mappings() -> None:
    _log("18", "CREATE TABLE skw_document_source_field_mappings")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_document_source_field_mappings'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_document_source_field_mappings] (
                [id_mapping]           INT           IDENTITY(1,1) NOT NULL,
                [id_source]            INT                         NOT NULL,
                -- Wspólna nazwa pola w systemie (np. 'document_amount', 'supplier_name')
                [common_field]         NVARCHAR(100)               NOT NULL,
                -- Pole w źródle (np. 'WARTOSC_BRUTTO', 'NazwaKontrahenta')
                [source_field]         NVARCHAR(200)               NOT NULL,
                [field_type]           NVARCHAR(20)                NOT NULL,
                -- Opcjonalne wyrażenie transformacji (np. 'DATEADD(DAY, val, 18991230)')
                [transform_expression] NVARCHAR(500)               NULL,
                [created_at]           DATETIME2(7)                NOT NULL
                                       CONSTRAINT [DF_skw_dsfm_created_at]
                                       DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_document_source_field_mappings]
                    PRIMARY KEY CLUSTERED ([id_mapping] ASC),
                CONSTRAINT [UQ_skw_dsfm_source_field]
                    UNIQUE NONCLUSTERED ([id_source], [common_field]),
                CONSTRAINT [CK_skw_dsfm_type]
                    CHECK ([field_type] IN (N'string', N'decimal', N'date', N'int')),

                CONSTRAINT [FK_skw_dsfm_source]
                    FOREIGN KEY ([id_source])
                    REFERENCES [{SCHEMA}].[skw_document_sources] ([id_source])
                    ON DELETE CASCADE ON UPDATE NO ACTION
            );
            PRINT N'[0028-18] Tabela skw_document_source_field_mappings utworzona.';
        END
        ELSE
            PRINT N'[0028-18] Tabela skw_document_source_field_mappings już istnieje — pomijam.';
    """)
    _log("18", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 19 — skw_user_notifications (persystentne powiadomienia biznesowe)
#           Filtrowany INDEX na (id_user, is_read) WHERE is_read = 0 — wydajny
#           odczyt liczby nieprzeczytanych
# ─────────────────────────────────────────────────────────────────────────────
def _krok19_user_notifications() -> None:
    _log("19", "CREATE TABLE skw_user_notifications + filtrowany index nieprzeczytanych")
    _execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_user_notifications'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_user_notifications] (
                [id_notification]   BIGINT        IDENTITY(1,1) NOT NULL,
                [id_user]           INT                         NOT NULL,
                [notification_type] NVARCHAR(50)                NOT NULL,
                [id_instance]       INT                         NULL,
                [title]             NVARCHAR(200)               NOT NULL,
                [message]           NVARCHAR(MAX)               NOT NULL,
                [is_read]           BIT                         NOT NULL
                                    CONSTRAINT [DF_skw_un_is_read]
                                    DEFAULT 0,
                [read_at]           DATETIME2(7)                NULL,
                [created_at]        DATETIME2(7)                NOT NULL
                                    CONSTRAINT [DF_skw_un_created_at]
                                    DEFAULT SYSUTCDATETIME(),

                CONSTRAINT [PK_skw_user_notifications]
                    PRIMARY KEY CLUSTERED ([id_notification] ASC),

                CONSTRAINT [FK_skw_un_user]
                    FOREIGN KEY ([id_user])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE CASCADE ON UPDATE NO ACTION,

                CONSTRAINT [FK_skw_un_instance]
                    FOREIGN KEY ([id_instance])
                    REFERENCES [{SCHEMA}].[skw_document_approval_instances] ([id_instance])
                    ON DELETE SET NULL ON UPDATE NO ACTION
            );
            PRINT N'[0028-19] Tabela skw_user_notifications utworzona.';
        END
        ELSE
            PRINT N'[0028-19] Tabela skw_user_notifications juz istnieje — pomijam.';
    """)

    # Krok A — idempotentny DROP filtrowanego indexu
    _execute(f"""
        IF EXISTS (
            SELECT 1 FROM sys.indexes  i
            JOIN sys.tables  t ON i.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id  = s.schema_id
            WHERE s.name = N'{SCHEMA}'
              AND t.name = N'skw_user_notifications'
              AND i.name = N'IX_skw_un_user_unread'
        )
        BEGIN
            DROP INDEX [IX_skw_un_user_unread]
                ON [{SCHEMA}].[skw_user_notifications];
            PRINT N'[0028-19] Poprzedni index usuniety (re-run migracji).';
        END
        ELSE
            PRINT N'[0028-19] Index nie istnieje — przechodze do tworzenia.';
    """)

    # Krok B — standalone CREATE INDEX bez IF, WHERE = 0 (prosta rownosc, brak NOT IN)
    _execute(f"""
        CREATE NONCLUSTERED INDEX [IX_skw_un_user_unread]
            ON [{SCHEMA}].[skw_user_notifications] ([id_user], [is_read])
            INCLUDE ([id_notification], [notification_type], [id_instance],
                     [title], [created_at])
            WHERE [is_read] = 0;
        PRINT N'[0028-19] Filtrowany index IX_skw_un_user_unread utworzony.';
    """)

    _log("19", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 20 — Indeksy pokrywające (wydajność zapytań API i workerów)
# ─────────────────────────────────────────────────────────────────────────────
def _krok20_indeksy() -> None:
    _log("20", "CREATE INDEX — status, is_urgent, logged_at, id_user")

    indeksy = [
        # Kolejka dyspozytora i ogólne filtrowanie po statusie
        (
            "IX_skw_dai_status_urgent",
            "skw_document_approval_instances",
            "[status], [is_urgent]",
            "INCLUDE ([id_document], [id_source], [current_step], "
            "[dispatched_at], [deadline_at], [document_title])",
        ),
        # Wyszukiwanie po dokumencie (dispatch / check duplicate)
        (
            "IX_skw_dai_document_source",
            "skw_document_approval_instances",
            "[id_document], [id_source]",
            "INCLUDE ([id_instance], [status])",
        ),
        # Log — przeszukiwanie po instancji (historia obiegu)
        (
            "IX_skw_al_instance_logged",
            "skw_approval_log",
            "[id_instance], [logged_at] DESC",
            "INCLUDE ([action], [id_user], [username_snapshot], "
            "[step_order_snapshot], [is_voided])",
        ),
        # Log — przeszukiwanie po użytkowniku (audyt akcji usera)
        (
            "IX_skw_al_user_logged",
            "skw_approval_log",
            "[id_user], [logged_at] DESC",
            "INCLUDE ([id_instance], [action])",
        ),
        # Snapshot steps — szybki dostęp po instancji i statusie
        (
            "IX_skw_dass_instance_status",
            "skw_document_approval_snapshot_steps",
            "[id_instance], [status]",
            "INCLUDE ([step_order], [id_group], [votes_cast], "
            "[votes_required], [deadline_at])",
        ),
        # Delegacje aktywne per grupa i data ważności
        (
            "IX_skw_adel_active",
            "skw_approval_delegations",
            "[id_group], [is_active], [valid_from], [valid_to]",
            "INCLUDE ([id_user_from], [id_user_to])",
        ),
        # Członkowie grupy (cache invalidation + walidacja)
        (
            "IX_skw_agm_group_user",
            "skw_approval_group_members",
            "[id_group]",
            "INCLUDE ([id_user])",
        ),
    ]

    for idx_name, tbl_name, cols, include in indeksy:
        _execute(f"""
            IF NOT EXISTS (
                SELECT 1 FROM sys.indexes i
                JOIN sys.tables  t ON i.object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id  = s.schema_id
                WHERE s.name = N'{SCHEMA}'
                  AND t.name = N'{tbl_name}'
                  AND i.name = N'{idx_name}'
            )
            BEGIN
                CREATE NONCLUSTERED INDEX [{idx_name}]
                    ON [{SCHEMA}].[{tbl_name}] ({cols})
                    {include};
                PRINT N'[0028-20] Index {idx_name} utworzony.';
            END
            ELSE
                PRINT N'[0028-20] Index {idx_name} już istnieje — pomijam.';
        """)

    _log("20", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 21 — GRANT INSERT / DENY UPDATE+DELETE na skw_approval_log
#           Użytkownik DB pobierany dynamicznie przez USER_NAME() — idempotentne
# ─────────────────────────────────────────────────────────────────────────────
def _krok21_grant_deny_approval_log() -> None:
    _log("21", "GRANT INSERT / DENY UPDATE+DELETE na skw_approval_log (dynamiczny user)")
    # Dwa osobne batche — kazdy z wlasnym DECLARE.
    # Brak polskich znakow i em-daszow w PRINT (ryzyko encodingu ODBC).
    _execute(f"""
        DECLARE @u NVARCHAR(128) = USER_NAME();
        DECLARE @s NVARCHAR(500);
        SET @s = N'DENY UPDATE, DELETE ON [{SCHEMA}].[skw_approval_log] TO [' + @u + N']';
        EXEC sp_executesql @s;
        PRINT N'[0028-21] DENY UPDATE+DELETE - OK.';
    """)
    _execute(f"""
        DECLARE @u NVARCHAR(128) = USER_NAME();
        DECLARE @s NVARCHAR(500);
        SET @s = N'GRANT INSERT ON [{SCHEMA}].[skw_approval_log] TO [' + @u + N']';
        EXEC sp_executesql @s;
        PRINT N'[0028-21] GRANT INSERT - OK.';
    """)
    _log("21", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 22 — VIEW: skw_v_approval_dispatch_queue
#           Kolejka dyspozytora — dokumenty pending_dispatch z danymi z WAPRO
# ─────────────────────────────────────────────────────────────────────────────
def _krok22_view_dispatch_queue() -> None:
    _log("22", "CREATE OR ALTER VIEW dbo.skw_v_approval_dispatch_queue")
    _execute(f"""
        CREATE OR ALTER VIEW [{SCHEMA}].[skw_v_approval_dispatch_queue] AS
        /*
         * Widok kolejki dyspozytora.
         * Zwraca wszystkie dokumenty ze statusem 'pending_dispatch'.
         * Dane dokumentu dołączane z odpowiedniego widoku źródłowego
         * na podstawie id_source (LEFT JOIN — bezpieczne dla nowych źródeł).
         *
         * ⚠ Rejestrowany w skw_SchemaChecksums — każda zmiana definicji
         *   MUSI być poprzedzona aktualizacją checksumy przez migrację Alembic.
         */
        SELECT
            dai.[id_instance],
            dai.[id_document],
            dai.[id_source],
            ds.[source_name],
            dai.[status],
            dai.[is_urgent],
            dai.[current_step],
            dai.[document_title],
            dai.[document_amount],
            dai.[extra_data],
            dai.[created_at]                    AS instance_created_at,
            dai.[updated_at]                    AS instance_updated_at,
            dai.[deadline_at],

            -- ── Dane z WAPRO (Fakir) gdy source_name = 'fakir' ──────────────
            fah.[NUMER]                         AS fakir_numer,
            fah.[WARTOSC_NETTO]                 AS fakir_wartosc_netto,
            fah.[WARTOSC_BRUTTO]                AS fakir_wartosc_brutto,
            fah.[KWOTA_VAT]                     AS fakir_kwota_vat,
            fah.[NazwaKontrahenta]              AS fakir_kontrahent,
            fah.[EmailKontrahenta]              AS fakir_email_kontrahenta,
            fah.[KOD_STATUSU]                   AS fakir_status_zewnetrzny,
            fah.[StatusOpis]                    AS fakir_status_opis,
            fah.[DataWystawienia]               AS fakir_data_wystawienia,
            fah.[DataOtrzymania]                AS fakir_data_otrzymania,
            fah.[TerminPlatnosci]               AS fakir_termin_platnosci,
            fah.[FORMA_PLATNOSCI]               AS fakir_forma_platnosci

        FROM [{SCHEMA}].[skw_document_approval_instances]  dai
        INNER JOIN [{SCHEMA}].[skw_document_sources]        ds
               ON ds.[id_source] = dai.[id_source]
        LEFT  JOIN [{SCHEMA}].[skw_faktury_akceptacja_naglowek] fah
               ON  ds.[source_name]     = N'fakir'
               AND fah.[ID_BUF_DOKUMENT] = TRY_CAST(dai.[id_document] AS INT)

        WHERE dai.[status] = N'pending_dispatch';
    """)
    _log("22", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 23 — VIEW: skw_v_approval_instance_detail
#           Szczegóły instancji obiegu z bieżącą grupą i danymi dokumentu
# ─────────────────────────────────────────────────────────────────────────────
def _krok23_view_instance_detail() -> None:
    _log("23", "CREATE OR ALTER VIEW dbo.skw_v_approval_instance_detail")
    _execute(f"""
        CREATE OR ALTER VIEW [{SCHEMA}].[skw_v_approval_instance_detail] AS
        /*
         * Widok szczegółów instancji obiegu.
         * Jeden wiersz per instancja — dołącza dane bieżącego kroku snapshot,
         * info o grupie oraz dane dyspozytora.
         * Używany przez endpoint GET /approval/instances/{{id}}/status.
         *
         * ⚠ Rejestrowany w skw_SchemaChecksums.
         */
        SELECT
            dai.[id_instance],
            dai.[id_document],
            dai.[id_source],
            ds.[source_name],
            dai.[status],
            dai.[current_step],
            dai.[is_urgent],
            dai.[document_title],
            dai.[document_amount],
            dai.[extra_data],
            dai.[created_at]                    AS instance_created_at,
            dai.[updated_at]                    AS instance_updated_at,
            dai.[dispatched_at],
            dai.[completed_at],
            dai.[deadline_at],

            -- ── Dyspozytor ───────────────────────────────────────────────────
            dai.[dispatched_by]                 AS dispatched_by_id,
            u_disp.[Username]                   AS dispatched_by_username,
            u_disp.[FullName]                   AS dispatched_by_fullname,

            -- ── Bieżący krok snapshot ────────────────────────────────────────
            snap.[id_snapshot]                  AS current_snapshot_id,
            snap.[id_group]                     AS current_id_group,
            snap.[status]                       AS current_step_status,
            snap.[votes_required]               AS current_votes_required,
            snap.[votes_cast]                   AS current_votes_cast,
            snap.[deadline_at]                  AS current_step_deadline,

            -- ── Bieżąca grupa akceptacyjna ───────────────────────────────────
            ag.[group_name]                     AS current_group_name,
            ag.[consensus_type]                 AS current_consensus_type,
            ag.[description]                    AS current_group_description,

            -- ── Postęp ogólny (całkowita liczba kroków w snapshot) ────────────
            (
                SELECT COUNT(*)
                FROM [{SCHEMA}].[skw_document_approval_snapshot_steps] s2
                WHERE s2.[id_instance] = dai.[id_instance]
            )                                   AS total_steps,

            -- ── Dane z WAPRO (Fakir) gdy source_name = 'fakir' ──────────────
            fah.[NUMER]                         AS fakir_numer,
            fah.[WARTOSC_NETTO]                 AS fakir_wartosc_netto,
            fah.[WARTOSC_BRUTTO]                AS fakir_wartosc_brutto,
            fah.[KWOTA_VAT]                     AS fakir_kwota_vat,
            fah.[NazwaKontrahenta]              AS fakir_kontrahent,
            fah.[KOD_STATUSU]                   AS fakir_status_zewnetrzny,
            fah.[StatusOpis]                    AS fakir_status_opis,
            fah.[DataWystawienia]               AS fakir_data_wystawienia,
            fah.[TerminPlatnosci]               AS fakir_termin_platnosci,
            fah.[FORMA_PLATNOSCI]               AS fakir_forma_platnosci,
            fah.[UWAGI]                         AS fakir_uwagi

        FROM [{SCHEMA}].[skw_document_approval_instances]  dai
        INNER JOIN [{SCHEMA}].[skw_document_sources]        ds
               ON ds.[id_source] = dai.[id_source]
        LEFT  JOIN [{SCHEMA}].[skw_Users]                   u_disp
               ON u_disp.[ID_USER] = dai.[dispatched_by]
        LEFT  JOIN [{SCHEMA}].[skw_document_approval_snapshot_steps] snap
               ON  snap.[id_instance] = dai.[id_instance]
               AND snap.[step_order]  = dai.[current_step]
        LEFT  JOIN [{SCHEMA}].[skw_approval_groups]         ag
               ON ag.[id_group] = snap.[id_group]
        LEFT  JOIN [{SCHEMA}].[skw_faktury_akceptacja_naglowek] fah
               ON  ds.[source_name]     = N'fakir'
               AND fah.[ID_BUF_DOKUMENT] = TRY_CAST(dai.[id_document] AS INT);
    """)
    _log("23", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 24 — VIEW: skw_v_approval_my_queue
#           "Moja kolejka" — dokumenty czekające na zalogowanego usera
#           przez członkostwo w grupie LUB aktywną delegację
# ─────────────────────────────────────────────────────────────────────────────
def _krok24_view_my_queue() -> None:
    _log("24", "CREATE OR ALTER VIEW dbo.skw_v_approval_my_queue")
    _execute(f"""
        CREATE OR ALTER VIEW [{SCHEMA}].[skw_v_approval_my_queue] AS
        /*
         * Widok "moja kolejka".
         * Zwraca instancje w toku (status = 'in_progress') razem z listą
         * użytkowników uprawnionych do akcji na bieżącym kroku:
         *   - przez bezpośrednie członkostwo w grupie (gm.id_user)
         *   - przez aktywną delegację (del.id_user_to)
         *
         * Backend filtruje po id_user = current_user.ID_USER używając
         * COALESCE(gm.id_user, del.id_user_to).
         *
         * ⚠ Rejestrowany w skw_SchemaChecksums.
         */
        SELECT
            dai.[id_instance],
            dai.[id_document],
            dai.[id_source],
            ds.[source_name],
            dai.[status],
            dai.[current_step],
            dai.[is_urgent],
            dai.[document_title],
            dai.[document_amount],
            dai.[deadline_at],
            dai.[created_at]                    AS instance_created_at,

            -- ── Bieżący krok snapshot ────────────────────────────────────────
            snap.[id_snapshot]                  AS snapshot_id,
            snap.[id_group],
            snap.[votes_cast],
            snap.[votes_required],
            snap.[deadline_at]                  AS step_deadline,

            -- ── Bieżąca grupa ────────────────────────────────────────────────
            ag.[group_name],
            ag.[consensus_type],

            -- ── Uprawniony użytkownik (przez członkostwo lub delegację) ───────
            gm.[id_user]                        AS member_id_user,
            del.[id_user_to]                    AS delegate_id_user,
            -- Wygodne pole: kto jest uprawnionym (jeden z dwóch)
            COALESCE(gm.[id_user], del.[id_user_to])
                                                AS authorized_id_user,
            -- Flaga: uprawnienie pochodzi z delegacji
            CASE
                WHEN del.[id_user_to] IS NOT NULL
                 AND gm.[id_user]     IS NULL     THEN 1
                ELSE 0
            END                                 AS via_delegation,
            del.[id_delegation],
            del.[id_user_from]                  AS delegated_from_id,

            -- ── Dane z WAPRO (Fakir) gdy source_name = 'fakir' ──────────────
            fah.[NUMER]                         AS fakir_numer,
            fah.[WARTOSC_BRUTTO]                AS fakir_wartosc_brutto,
            fah.[NazwaKontrahenta]              AS fakir_kontrahent,
            fah.[DataWystawienia]               AS fakir_data_wystawienia,
            fah.[TerminPlatnosci]               AS fakir_termin_platnosci,
            fah.[KOD_STATUSU]                   AS fakir_status_zewnetrzny

        FROM [{SCHEMA}].[skw_document_approval_instances]  dai
        INNER JOIN [{SCHEMA}].[skw_document_sources]        ds
               ON ds.[id_source] = dai.[id_source]
        INNER JOIN [{SCHEMA}].[skw_document_approval_snapshot_steps] snap
               ON  snap.[id_instance] = dai.[id_instance]
               AND snap.[step_order]  = dai.[current_step]
               AND snap.[status]      = N'in_progress'
        INNER JOIN [{SCHEMA}].[skw_approval_groups]         ag
               ON ag.[id_group] = snap.[id_group]

        -- Członkowie grupy bieżącego kroku
        LEFT  JOIN [{SCHEMA}].[skw_approval_group_members]  gm
               ON gm.[id_group] = snap.[id_group]

        -- Aktywne delegacje na tę grupę (lub globalne: id_group IS NULL)
        LEFT  JOIN [{SCHEMA}].[skw_approval_delegations]    del
               ON  del.[is_active]   = 1
               AND del.[valid_from] <= SYSUTCDATETIME()
               AND del.[valid_to]   >= SYSUTCDATETIME()
               AND (
                       del.[id_group] = snap.[id_group]
                    OR del.[id_group] IS NULL
                   )
               -- Delegacja musi być dla kogoś, kto jest w grupie
               AND EXISTS (
                   SELECT 1 FROM [{SCHEMA}].[skw_approval_group_members] gm2
                   WHERE gm2.[id_group] = snap.[id_group]
                     AND gm2.[id_user]  = del.[id_user_from]
               )

        -- Dane faktury z WAPRO
        LEFT  JOIN [{SCHEMA}].[skw_faktury_akceptacja_naglowek] fah
               ON  ds.[source_name]     = N'fakir'
               AND fah.[ID_BUF_DOKUMENT] = TRY_CAST(dai.[id_document] AS INT)

        WHERE dai.[status] = N'in_progress'
          -- Wyklucz wiersze gdzie user nie ma żadnego uprawnienia (oba NULL)
          AND (gm.[id_user] IS NOT NULL OR del.[id_user_to] IS NOT NULL);
    """)
    _log("24", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 25 — MERGE skw_SchemaChecksums — rejestracja 3 nowych widoków
#           Checksum pobierany dynamicznie z sys.sql_modules (aktualny po CREATE)
# ─────────────────────────────────────────────────────────────────────────────
def _krok25_merge_schema_checksums() -> None:
    _log("25", "MERGE skw_SchemaChecksums — 3 widoki approval")

    views = [
        "skw_v_approval_dispatch_queue",
        "skw_v_approval_instance_detail",
        "skw_v_approval_my_queue",
    ]

    for view_name in views:
        _execute(f"""
            MERGE [{SCHEMA}].[skw_SchemaChecksums] AS target
            USING (
                SELECT
                    N'{view_name}'  AS ObjectName,
                    N'{SCHEMA}'     AS SchemaName,
                    N'VIEW'         AS ObjectType,
                    (
                        SELECT CHECKSUM(m.definition)
                        FROM   sys.sql_modules  m
                        JOIN   sys.objects      o ON o.object_id = m.object_id
                        JOIN   sys.schemas      s ON s.schema_id = o.schema_id
                        WHERE  o.name   = N'{view_name}'
                          AND  s.name   = N'{SCHEMA}'
                    )               AS Checksum,
                    N'{revision}'   AS AlembicRevision,
                    NULL            AS LastVerifiedAt,
                    GETDATE()       AS Now
            ) AS source
            ON (
                    target.[ObjectName]  = source.[ObjectName]
                AND target.[SchemaName]  = source.[SchemaName]
                AND target.[ObjectType]  = source.[ObjectType]
            )
            WHEN MATCHED THEN
                UPDATE SET
                    [Checksum]        = source.[Checksum],
                    [AlembicRevision] = source.[AlembicRevision],
                    [LastVerifiedAt]  = source.[LastVerifiedAt],
                    [UpdatedAt]       = source.[Now]
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    [ObjectName], [SchemaName], [ObjectType],
                    [Checksum],   [AlembicRevision],
                    [LastVerifiedAt], [CreatedAt]
                )
                VALUES (
                    source.[ObjectName], source.[SchemaName], source.[ObjectType],
                    source.[Checksum],   source.[AlembicRevision],
                    source.[LastVerifiedAt], source.[Now]
                );
            PRINT N'[0028-25] SchemaChecksums MERGE: {view_name} — OK.';
        """)

    _log("25", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 26 — MERGE skw_Permissions — 12 uprawnień approval.*
#           WHEN NOT MATCHED → INSERT only (nie nadpisuje istniejących)
# ─────────────────────────────────────────────────────────────────────────────
def _krok26_merge_permissions() -> None:
    _log("26", f"MERGE skw_Permissions — {len(_PERMISSIONS)} uprawnień approval.*")

    for perm_name, description in _PERMISSIONS:
        # Escape apostrofów w opisie
        desc_escaped = description.replace("'", "''")
        _execute(f"""
            MERGE [{SCHEMA}].[skw_Permissions] AS target
            USING (
                SELECT
                    N'{perm_name}'    AS PermissionName,
                    N'{desc_escaped}' AS Description,
                    N'approval'       AS Category
            ) AS source
            ON target.[PermissionName] = source.[PermissionName]
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ([PermissionName], [Description], [Category], [IsActive], [CreatedAt])
                VALUES (
                    source.[PermissionName],
                    source.[Description],
                    source.[Category],
                    1,
                    GETDATE()
                );
            PRINT N'[0028-26] Permission {perm_name} — MERGE OK.';
        """)

    _log("26", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 27 — MERGE skw_document_sources — seed: fakir + ksef
# ─────────────────────────────────────────────────────────────────────────────
def _krok27_merge_document_sources() -> None:
    _log("27", "MERGE skw_document_sources — seed fakir + ksef")

    sources = [
        ("fakir", "Faktury zakupowe z systemu Fakir (WAPRO BUF_DOKUMENT)"),
        ("ksef",  "Faktury z Krajowego Systemu e-Faktur (KSeF)"),
    ]

    for source_name, description in sources:
        desc_escaped = description.replace("'", "''")
        _execute(f"""
            MERGE [{SCHEMA}].[skw_document_sources] AS target
            USING (
                SELECT
                    N'{source_name}'  AS source_name,
                    N'{desc_escaped}' AS description
            ) AS source
            ON target.[source_name] = source.[source_name]
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ([source_name], [description], [is_active], [created_at])
                VALUES (source.[source_name], source.[description], 1, SYSUTCDATETIME());
            PRINT N'[0028-27] DocumentSource "{source_name}" — MERGE OK.';
        """)

    _log("27", "OK")


# ─────────────────────────────────────────────────────────────────────────────
# KROK 28 — MERGE skw_SystemConfig — 12 kluczy feature flags i konfiguracji
#           WHEN NOT MATCHED → INSERT only (nie nadpisuje wartości admina)
# ─────────────────────────────────────────────────────────────────────────────
def _krok28_merge_system_config() -> None:
    _log("28", f"MERGE skw_SystemConfig — {len(_SYSTEM_CONFIG)} kluczy APPROVAL_*")

    for config_key, config_value, description in _SYSTEM_CONFIG:
        desc_escaped = description.replace("'", "''")
        _execute(f"""
            MERGE [{SCHEMA}].[skw_SystemConfig] AS target
            USING (
                SELECT
                    N'{config_key}'   AS ConfigKey,
                    N'{config_value}' AS ConfigValue,
                    N'{desc_escaped}' AS Description
            ) AS source
            ON target.[ConfigKey] = source.[ConfigKey]
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ([ConfigKey], [ConfigValue], [Description], [IsActive], [CreatedAt])
                VALUES (
                    source.[ConfigKey],
                    source.[ConfigValue],
                    source.[Description],
                    1,
                    GETDATE()
                );
            PRINT N'[0028-28] SystemConfig "{config_key}" — MERGE OK.';
        """)

    _log("28", "OK")


# ╔═════════════════════════════════════════════════════════════════════════════╗
# ║  DOWNGRADE                                                                 ║
# ║  Odwraca wszystkie 28 kroków. Kolejność: odwrotna do upgrade.              ║
# ║  ⚠ USUWA WSZYSTKIE DANE w tabelach approval — wymaga świadomego           ║
# ║    potwierdzenia administratora przed uruchomieniem.                       ║
# ╚═════════════════════════════════════════════════════════════════════════════╝

def downgrade() -> None:
    logger.warning("=" * 72)
    logger.warning("[%s] ══ DOWNGRADE START ══ ts=%s", revision, _ts())
    logger.warning(
        "[%s] ⚠  DOWNGRADE USUWA WSZYSTKIE DANE MODUŁU APPROVAL.", revision
    )
    logger.warning("=" * 72)

    _down01_system_config()
    _down02_document_sources()
    _down03_permissions()
    _down04_schema_checksums()
    _down05_views()
    _down06_tabele()
    _down07_constraint_category()

    logger.warning("[%s] ══ DOWNGRADE OK ══ ts=%s", revision, _ts())


def _down01_system_config() -> None:
    logger.warning("[%s] DOWNGRADE-01 | DELETE SystemConfig APPROVAL_*", revision)
    _execute(f"""
        DELETE FROM [{SCHEMA}].[skw_SystemConfig]
        WHERE [ConfigKey] LIKE N'APPROVAL_%';
        PRINT N'[0028-D1] SystemConfig APPROVAL_* usunięte.';
    """)


def _down02_document_sources() -> None:
    logger.warning("[%s] DOWNGRADE-02 | DELETE document_sources fakir+ksef (jeśli brak instancji)", revision)
    _execute(f"""
        -- Usuwamy źródła tylko jeśli nie ma żadnych instancji — bezpieczeństwo danych
        IF NOT EXISTS (
            SELECT 1 FROM [{SCHEMA}].[skw_document_approval_instances]
        )
        BEGIN
            DELETE FROM [{SCHEMA}].[skw_document_sources]
            WHERE [source_name] IN (N'fakir', N'ksef');
            PRINT N'[0028-D2] document_sources fakir+ksef usunięte (brak instancji).';
        END
        ELSE
            PRINT N'[0028-D2] UWAGA: Istnieją instancje — document_sources NIE usunięte.';
    """)


def _down03_permissions() -> None:
    logger.warning("[%s] DOWNGRADE-03 | DELETE Permissions approval.* + CASCADE RolePermissions", revision)
    _execute(f"""
        -- Najpierw usuń przypisania do ról (mogą nie mieć CASCADE)
        DELETE rp
        FROM [{SCHEMA}].[skw_RolePermissions] rp
        INNER JOIN [{SCHEMA}].[skw_Permissions] p
               ON p.[ID_PERMISSION] = rp.[ID_PERMISSION]
        WHERE p.[PermissionName] LIKE N'approval.%';
        PRINT N'[0028-D3a] RolePermissions approval.* usunięte: ' + CAST(@@ROWCOUNT AS NVARCHAR(20));

        DELETE FROM [{SCHEMA}].[skw_Permissions]
        WHERE [PermissionName] LIKE N'approval.%';
        PRINT N'[0028-D3b] Permissions approval.* usunięte: ' + CAST(@@ROWCOUNT AS NVARCHAR(20));
    """)


def _down04_schema_checksums() -> None:
    logger.warning("[%s] DOWNGRADE-04 | DELETE SchemaChecksums dla widoków approval", revision)
    _execute(f"""
        DELETE FROM [{SCHEMA}].[skw_SchemaChecksums]
        WHERE [ObjectName] IN (
            N'skw_v_approval_dispatch_queue',
            N'skw_v_approval_instance_detail',
            N'skw_v_approval_my_queue'
        );
        PRINT N'[0028-D4] SchemaChecksums — wpisy approval usunięte: '
              + CAST(@@ROWCOUNT AS NVARCHAR(20));
    """)


def _down05_views() -> None:
    logger.warning("[%s] DOWNGRADE-05 | DROP VIEWs approval", revision)
    for view_name in [
        "skw_v_approval_my_queue",
        "skw_v_approval_instance_detail",
        "skw_v_approval_dispatch_queue",
    ]:
        _execute(f"""
            IF OBJECT_ID(N'[{SCHEMA}].[{view_name}]', N'V') IS NOT NULL
            BEGIN
                DROP VIEW [{SCHEMA}].[{view_name}];
                PRINT N'[0028-D5] DROP VIEW {view_name} — OK.';
            END
            ELSE
                PRINT N'[0028-D5] VIEW {view_name} nie istnieje — pomijam.';
        """)


def _down06_tabele() -> None:
    """Usuwa tabele w odwrotnej kolejności FK."""
    logger.warning("[%s] DOWNGRADE-06 | DROP TABLEs approval (odwrotna kolejność FK)", revision)

    # Kolejność: najpierw dzieci, potem rodzice
    tabele = [
        "skw_user_notifications",
        "skw_document_source_field_mappings",
        "skw_approval_filter_conditions",
        "skw_approval_filters",
        "skw_approval_attachments",
        "skw_approval_comments",
        "skw_approval_delegations",
        "skw_approval_log",          # trigger usuwany razem z tabelą
        "skw_document_approval_snapshot_steps",
        "skw_document_approval_instances",
        "skw_document_categories",
        "skw_document_sources",
        "skw_approval_path_change_log",
        "skw_approval_path_steps",
        "skw_approval_paths",
        "skw_approval_group_members",
        "skw_approval_groups",
    ]

    for tbl in tabele:
        _execute(f"""
            IF OBJECT_ID(N'[{SCHEMA}].[{tbl}]', N'U') IS NOT NULL
            BEGIN
                DROP TABLE [{SCHEMA}].[{tbl}];
                PRINT N'[0028-D6] DROP TABLE {tbl} — OK.';
            END
            ELSE
                PRINT N'[0028-D6] TABLE {tbl} nie istnieje — pomijam.';
        """)


def _down07_constraint_category() -> None:
    logger.warning("[%s] DOWNGRADE-07 | Przywróć CK_skw_Permissions_Category bez 'approval'", revision)
    _execute(f"""
        IF EXISTS (
            SELECT 1
            FROM sys.check_constraints cc
            JOIN sys.tables  t ON cc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id          = s.schema_id
            WHERE s.name  = N'{SCHEMA}'
              AND t.name  = N'skw_Permissions'
              AND cc.name = N'CK_skw_Permissions_Category'
        )
        BEGIN
            ALTER TABLE [{SCHEMA}].[skw_Permissions]
                DROP CONSTRAINT [CK_skw_Permissions_Category];

            ALTER TABLE [{SCHEMA}].[skw_Permissions]
                ADD CONSTRAINT [CK_skw_Permissions_Category] CHECK (
                    [Category] IN ({_CK_KATEGORIE_PRZED})
                );
            PRINT N'[0028-D7] CK_skw_Permissions_Category przywrócony bez approval.';
        END
        ELSE
            PRINT N'[0028-D7] Constraint nie istnieje — pomijam.';
    """)