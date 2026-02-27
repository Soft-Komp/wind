"""Dodanie kolumny RequestID do dbo_ext.AuditLog

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-02-27

Przyczyna:
    Kolumna RequestID (NVARCHAR(36)) była zdefiniowana w dokumentacji
    audit_service.py oraz odpytywana w SELECT przez serwis, ale NIE
    została uwzględniona w migracji 001_create_dbo_ext_initial.py.

    Skutek: każde wywołanie GET /system/audit-log kończyło się:
        sqlalchemy.exc.ProgrammingError: [42S22] Invalid column name 'RequestID'

    Ponadto: INSERT w audit_service.py (_INSERT_AUDIT_SQL) próbował zapisać
    RequestID → cichy fail każdego wpisu audytowego od początku projektu.

Co robi ta migracja:
    1. Dodaje kolumnę [RequestID] NVARCHAR(36) NULL do dbo_ext.AuditLog
    2. Tworzy indeks IX_AuditLog_RequestID (filtrowany — WHERE NOT NULL)

Uwaga o SchemaChecksums:
    Tabela SchemaChecksums ma CHECK constraint dopuszczający ObjectType tylko
    dla: 'VIEW', 'PROCEDURE', 'INDEX'. Kolumna to inny typ obiektu — nie
    rejestrujemy jej w SchemaChecksums (nie ma sensu wymuszać integralności
    kolumny przez ten mechanizm — od tego jest właśnie Alembic).

Down_revision: d4e5f6a7b8c9  ← merge heads (HEAD po 2026-02-26)
"""
from __future__ import annotations

import logging

from alembic import op

# ─── Identyfikatory rewizji ───────────────────────────────────────────────────
revision: str      = "e5f6a7b8c9d0"
down_revision: str = "d4e5f6a7b8c9"
branch_labels      = None
depends_on         = None

logger = logging.getLogger("alembic.migration.005")

SCHEMA = "dbo_ext"


# ─── UPGRADE ─────────────────────────────────────────────────────────────────

def upgrade() -> None:
    logger.info("=== MIGRACJA 005 — UPGRADE START (RequestID w AuditLog) ===")
    _add_request_id_column()
    _create_request_id_index()
    logger.info("=== MIGRACJA 005 — UPGRADE ZAKOŃCZONY ===")


def downgrade() -> None:
    logger.warning("=== MIGRACJA 005 — DOWNGRADE START ===")
    # Kolejność: najpierw indeks (zależy od kolumny), potem kolumna
    _drop_request_id_index()
    _drop_request_id_column()
    logger.warning("=== MIGRACJA 005 — DOWNGRADE ZAKOŃCZONY ===")


# ─── HELPERS UPGRADE ─────────────────────────────────────────────────────────

def _add_request_id_column() -> None:
    """
    Dodaje [RequestID] NVARCHAR(36) NULL do dbo_ext.AuditLog.

    NULL — wymagane dla istniejących wierszy (nie mają RequestID).
    Nowe wiersze wypełniane przez audit_service.py z ContextVar.
    NVARCHAR(36) — UUID4: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    """
    op.execute(
        f"""
        IF NOT EXISTS (
            SELECT 1
            FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[AuditLog]')
              AND name = N'RequestID'
        )
        BEGIN
            ALTER TABLE [{SCHEMA}].[AuditLog]
            ADD [RequestID] NVARCHAR(36) NULL;

            PRINT 'Kolumna [{SCHEMA}].[AuditLog].[RequestID] dodana.';
        END
        ELSE
        BEGIN
            PRINT 'Kolumna [{SCHEMA}].[AuditLog].[RequestID] już istnieje — pominięto.';
        END
        """
    )
    logger.info("AuditLog.RequestID: NVARCHAR(36) NULL — OK")


def _create_request_id_index() -> None:
    """
    Indeks filtrowany na RequestID — korelacja logów HTTP z wpisami AuditLog.

    WHERE RequestID IS NOT NULL — nie indeksuje historycznych NULL-i,
    oszczędza miejsce i przyspiesza wyszukiwanie po UUID requestu.

    Nie UNIQUE — jeden request może wygenerować wiele wpisów (bulk ops).
    """
    op.execute(
        f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[AuditLog]')
              AND name = N'IX_AuditLog_RequestID'
        )
        BEGIN
            CREATE NONCLUSTERED INDEX [IX_AuditLog_RequestID]
                ON [{SCHEMA}].[AuditLog] ([RequestID] ASC)
                WHERE [RequestID] IS NOT NULL;

            PRINT 'Indeks IX_AuditLog_RequestID: OK';
        END
        ELSE
        BEGIN
            PRINT 'Indeks IX_AuditLog_RequestID już istnieje — pominięto.';
        END
        """
    )
    logger.info("Indeks IX_AuditLog_RequestID: OK")


# ─── HELPERS DOWNGRADE ───────────────────────────────────────────────────────

def _drop_request_id_index() -> None:
    op.execute(
        f"""
        IF EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[AuditLog]')
              AND name = N'IX_AuditLog_RequestID'
        )
        BEGIN
            DROP INDEX [IX_AuditLog_RequestID] ON [{SCHEMA}].[AuditLog];
            PRINT 'Indeks IX_AuditLog_RequestID usunięty (downgrade).';
        END
        """
    )
    logger.warning("Indeks IX_AuditLog_RequestID: usunięty (downgrade)")


def _drop_request_id_column() -> None:
    """
    UWAGA DOWNGRADE: usuwa kolumnę wraz z danymi.
    Wszystkie zapisane RequestID zostaną utracone bezpowrotnie.
    """
    op.execute(
        f"""
        IF EXISTS (
            SELECT 1
            FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[AuditLog]')
              AND name = N'RequestID'
        )
        BEGIN
            ALTER TABLE [{SCHEMA}].[AuditLog]
            DROP COLUMN [RequestID];

            PRINT 'Kolumna [{SCHEMA}].[AuditLog].[RequestID] usunięta (downgrade).';
        END
        """
    )
    logger.warning(
        "AuditLog.RequestID: USUNIĘTA (downgrade) — dane bezpowrotnie utracone!"
    )