"""0006_faktura_akceptacja

STUB SESJI 1: DDL trzech tabel modułu Akceptacji Faktur KSeF.

======================================================================================
ZAWARTOŚĆ TEGO STUBBA (Sesja 1)
======================================================================================
  ✅ dbo_ext.skw_faktura_akceptacja  — główna tabela modułu (1 faktura = 1 wiersz)
  ✅ dbo_ext.skw_faktura_przypisanie — przypisania pracowników + decyzje (indeksy!)
  ✅ dbo_ext.skw_faktura_log         — immutable audit trail modułu

======================================================================================
ZAPLANOWANE ROZSZERZENIA (kolejne sesje — NIE IMPLEMENTOWAĆ TUTAJ)
======================================================================================
  Sesja 2: widoki SQL dbo.skw_faktury_akceptacja_naglowek + _pozycje
           + INSERT do skw_SchemaChecksums
           + triggery UpdatedAt (020_faktura_triggers_updated_at.sql)
  Sesja 3: MERGE 14 uprawnień kategorii 'faktury' (08_faktura_permissions.sql)
           + macierz ról (09_faktura_role_permissions.sql)
           + 13 kluczy SystemConfig modułu (10_system_config_faktura.sql)

======================================================================================
DECYZJE PROJEKTOWE
======================================================================================
  D1: UpdatedAt w skw_faktura_przypisanie — spec pomija, ale:
      - TABELE_REFERENCJA zasada ogólna: "Każda tabela ma CreatedAt + UpdatedAt"
      - Trigger 020 musi mieć kolumnę do aktualizacji
      → Dodajemy UpdatedAt ŚWIADOMIE

  D2: skw_faktura_log — BRAK UpdatedAt (jak skw_AuditLog)
      Tabela immutable — tylko INSERT, nigdy UPDATE.

  D3: CHECK constraints na wszystkich NVARCHAR statusach
      Baza jako ostatnia linia obrony. Walidacja Pydantic na poziomie API
      nie zastępuje constraint na poziomie DB.

  D4: FK do skw_faktura_log.user_id → ON DELETE SET NULL
      Dezaktywacja usera nie może niszczyć historii faktury.

======================================================================================
WERYFIKACJA
======================================================================================
  SELECT t.name, s.name AS [schema]
  FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
  WHERE s.name = 'dbo_ext' AND t.name LIKE 'skw_faktura%'
  ORDER BY t.name;
  -- oczekiwany wynik: 3 wiersze

Revision ID: 0006
Revises: 0005
Create Date: 2026-03-26
Author: Sprint 2 — Sesja 1
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Final

from alembic import op

# ── Meta Alembic ─────────────────────────────────────────────────────────────
revision: str = "0006"
down_revision: str = "0005"
branch_labels = None
depends_on = None

# ── Logger ────────────────────────────────────────────────────────────────────
logger = logging.getLogger(f"alembic.migration.{revision}")

# ── Stałe ─────────────────────────────────────────────────────────────────────
SCHEMA: Final[str] = "dbo_ext"

# Kolejność DROP przy downgrade — odwrotna do FK dependencies
_DROP_ORDER: Final[list[str]] = [
    "skw_faktura_log",         # najpierw — FK do akceptacja i Users
    "skw_faktura_przypisanie", # potem   — FK do akceptacja i Users
    "skw_faktura_akceptacja",  # na końcu — referenced by dwie powyższe
]


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC — wymagane przez Alembic
# ══════════════════════════════════════════════════════════════════════════════

def upgrade() -> None:
    """
    Tworzy 3 tabele modułu Akceptacji Faktur KSeF.
    Każda operacja jest idempotentna (IF NOT EXISTS).
    """
    _ts = datetime.now(timezone.utc).isoformat()
    logger.info(
        "[%s] upgrade() START — DDL 3 tabel modułu faktur | revision=%s | ts=%s",
        revision, revision, _ts,
    )

    _create_faktura_akceptacja()
    _create_faktura_przypisanie()
    _create_faktura_log()

    logger.info(
        "[%s] upgrade() DONE — 3 tabele modułu faktur gotowe | ts=%s",
        revision,
        datetime.now(timezone.utc).isoformat(),
    )


def downgrade() -> None:
    """
    Usuwa 3 tabele w odwrotnej kolejności do FK dependencies.

    ⚠⚠⚠ NIEODWRACALNA UTRATA DANYCH.
         Tylko środowisko deweloperskie / emergency rollback.
         Wymaga pełnego backupu przed uruchomieniem.
    """
    _ts = datetime.now(timezone.utc).isoformat()
    logger.warning(
        "[%s] downgrade() START — USUWANIE %d tabel modułu faktur | ts=%s",
        revision, len(_DROP_ORDER), _ts,
    )

    for table_name in _DROP_ORDER:
        _drop_table_if_exists(table_name)

    logger.warning(
        "[%s] downgrade() DONE — wszystkie tabele modułu faktur usunięte | ts=%s",
        revision,
        datetime.now(timezone.utc).isoformat(),
    )


# ══════════════════════════════════════════════════════════════════════════════
# PRIVATE — DDL tabel
# ══════════════════════════════════════════════════════════════════════════════

def _create_faktura_akceptacja() -> None:
    """
    Tabela główna modułu Akceptacji Faktur.

    Zasada: jedna faktura KSeF = jeden wiersz.
    numer_ksef jest unikalny i immutable — twarda referencja do systemu KSeF.

    status_wewnetrzny lifecycle:
      nowe → w_toku → zaakceptowana
                    ↘ anulowana

    IsActive = 0 przy anulowaniu (soft-delete spójne z systemem).
    Archiwum JSON.gz tworzone przez application layer przy IsActive = 0.
    """
    logger.info("[%s] Tworzenie tabeli %s.skw_faktura_akceptacja...", revision, SCHEMA)

    op.execute(f"""
        IF NOT EXISTS (
            SELECT 1
            FROM   sys.tables  t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE  s.name = N'{SCHEMA}'
              AND  t.name = N'skw_faktura_akceptacja'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_faktura_akceptacja] (

                -- ── Identyfikacja ────────────────────────────────────────────
                id                  INT          IDENTITY(1,1) NOT NULL,
                numer_ksef          NVARCHAR(50)               NOT NULL,

                -- ── Status i priorytety ──────────────────────────────────────
                status_wewnetrzny   NVARCHAR(20)               NOT NULL,
                priorytet           NVARCHAR(15)               NOT NULL
                    CONSTRAINT [DF_sfa_priorytet]   DEFAULT N'normalny',

                -- ── Opis dokumentu (opcjonalny) ──────────────────────────────
                opis_dokumentu      NVARCHAR(MAX)                  NULL,
                uwagi               NVARCHAR(MAX)                  NULL,

                -- ── Metadane ─────────────────────────────────────────────────
                utworzony_przez     INT                        NOT NULL,
                IsActive            BIT                        NOT NULL
                    CONSTRAINT [DF_sfa_IsActive]    DEFAULT 1,
                CreatedAt           DATETIME2(7)               NOT NULL
                    CONSTRAINT [DF_sfa_CreatedAt]   DEFAULT GETDATE(),
                UpdatedAt           DATETIME2(7)                   NULL,

                -- ── Klucz główny ─────────────────────────────────────────────
                CONSTRAINT [PK_skw_faktura_akceptacja]
                    PRIMARY KEY CLUSTERED (id ASC),

                -- ── Unikalność KSeF — jedna faktura = jeden obieg ─────────────
                CONSTRAINT [UQ_skw_faktura_akceptacja_numer_ksef]
                    UNIQUE NONCLUSTERED (numer_ksef),

                -- ── Integralność referencyjna ────────────────────────────────
                CONSTRAINT [FK_sfa_utworzony_przez]
                    FOREIGN KEY (utworzony_przez)
                    REFERENCES [{SCHEMA}].[skw_Users] (ID_USER),

                -- ── Wartości dozwolone ────────────────────────────────────────
                CONSTRAINT [CHK_sfa_status_wewnetrzny] CHECK (
                    status_wewnetrzny IN (
                        N'nowe',
                        N'w_toku',
                        N'zaakceptowana',
                        N'anulowana'
                    )
                ),

                CONSTRAINT [CHK_sfa_priorytet] CHECK (
                    priorytet IN (
                        N'normalny',
                        N'pilny',
                        N'bardzo_pilny'
                    )
                )
            );

            PRINT N'[007] Tabela [{SCHEMA}].[skw_faktura_akceptacja] — UTWORZONA.';
        END
        ELSE
        BEGIN
            PRINT N'[007] Tabela [{SCHEMA}].[skw_faktura_akceptacja] — już istnieje, pomijam.';
        END
    """)

    logger.info(
        "[%s] skw_faktura_akceptacja — DDL zakończone.",
        revision,
    )


def _create_faktura_przypisanie() -> None:
    """
    Tabela przypisań pracowników do faktur.

    Semantyka: jeden wiersz = jeden pracownik przypisany do jednej faktury.
    is_active = 0 oznacza dezaktywację przez reset referenta (nie DELETE).
    decided_at = timestamp podjęcia decyzji przez pracownika.

    INDEKSY KRYTYCZNE dla wydajności (D2 z dokumentu):
      IX_sfp_user_active   → endpoint "moje faktury" — skanuje po user_id
      IX_sfp_faktura_active → sprawdzenie kompletności akceptacji — skanuje po faktura_id

    DECYZJA: UpdatedAt dodany mimo braku w specyfikacji (patrz D1 w docstringu modułu).
    """
    logger.info("[%s] Tworzenie tabeli %s.skw_faktura_przypisanie...", revision, SCHEMA)

    op.execute(f"""
        IF NOT EXISTS (
            SELECT 1
            FROM   sys.tables  t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE  s.name = N'{SCHEMA}'
              AND  t.name = N'skw_faktura_przypisanie'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_faktura_przypisanie] (

                -- ── Identyfikacja ────────────────────────────────────────────
                id          INT          IDENTITY(1,1) NOT NULL,

                -- ── Relacje ──────────────────────────────────────────────────
                faktura_id  INT                        NOT NULL,
                user_id     INT                        NOT NULL,

                -- ── Stan przypisania ─────────────────────────────────────────
                status      NVARCHAR(20)               NOT NULL
                    CONSTRAINT [DF_sfp_status]     DEFAULT N'oczekuje',
                komentarz   NVARCHAR(MAX)                  NULL,
                is_active   BIT                        NOT NULL
                    CONSTRAINT [DF_sfp_is_active]  DEFAULT 1,

                -- ── Timestampy ───────────────────────────────────────────────
                CreatedAt   DATETIME2(7)               NOT NULL
                    CONSTRAINT [DF_sfp_CreatedAt]  DEFAULT GETDATE(),
                UpdatedAt   DATETIME2(7)                   NULL,   -- trigger 020
                decided_at  DATETIME2(7)                   NULL,   -- kiedy podjął decyzję

                -- ── Klucz główny ─────────────────────────────────────────────
                CONSTRAINT [PK_skw_faktura_przypisanie]
                    PRIMARY KEY CLUSTERED (id ASC),

                -- ── Integralność referencyjna ────────────────────────────────
                CONSTRAINT [FK_sfp_faktura_id]
                    FOREIGN KEY (faktura_id)
                    REFERENCES [{SCHEMA}].[skw_faktura_akceptacja] (id),

                CONSTRAINT [FK_sfp_user_id]
                    FOREIGN KEY (user_id)
                    REFERENCES [{SCHEMA}].[skw_Users] (ID_USER),

                -- ── Wartości dozwolone ────────────────────────────────────────
                CONSTRAINT [CHK_sfp_status] CHECK (
                    status IN (
                        N'oczekuje',
                        N'zaakceptowane',
                        N'odrzucone',
                        N'nie_moje'
                    )
                )
            );

            -- ── Indeks krytyczny 1: "moje faktury" (endpoint pracownika) ──────
            -- Pokrywa: WHERE user_id=? AND is_active=1
            -- INCLUDE: unika lookupów dla najczęstszych pól SELECT
            CREATE NONCLUSTERED INDEX [IX_sfp_user_active]
                ON [{SCHEMA}].[skw_faktura_przypisanie]
                    (user_id ASC, is_active ASC, status ASC)
                INCLUDE (faktura_id, CreatedAt);

            -- ── Indeks krytyczny 2: kompletność akceptacji (trigger Fakira) ───
            -- Pokrywa: WHERE faktura_id=? AND is_active=1
            -- Używany przy sprawdzaniu "czy wszyscy zaakceptowali"
            CREATE NONCLUSTERED INDEX [IX_sfp_faktura_active]
                ON [{SCHEMA}].[skw_faktura_przypisanie]
                    (faktura_id ASC, is_active ASC)
                INCLUDE (user_id, status);

            PRINT N'[007] Tabela [{SCHEMA}].[skw_faktura_przypisanie] + 2 indeksy — UTWORZONE.';
        END
        ELSE
        BEGIN
            PRINT N'[007] Tabela [{SCHEMA}].[skw_faktura_przypisanie] — już istnieje, pomijam.';
        END
    """)

    logger.info(
        "[%s] skw_faktura_przypisanie — DDL + 2 indeksy zakończone.",
        revision,
    )


def _create_faktura_log() -> None:
    """
    Immutable audit trail modułu faktur.

    ZASADA: tylko INSERT — nigdy UPDATE ani DELETE.
    Analogia do skw_AuditLog — stąd brak UpdatedAt.

    szczegoly to JSON serializowany przez model FakturaLogDetails (Sesja 4).
    Bezpośrednie wstawianie dict do tej kolumny jest ZABRONIONE.

    user_id = NULL oznacza akcję systemową:
      - auto-akceptacja po "wszyscy zaakceptowali"
      - timeout / force-accept przez system

    FK user_id → ON DELETE SET NULL:
      Dezaktywacja usera nie może niszczyć historii faktury.
      Historia musi przetrwać usunięcie pracownika.
    """
    logger.info("[%s] Tworzenie tabeli %s.skw_faktura_log...", revision, SCHEMA)

    op.execute(f"""
        IF NOT EXISTS (
            SELECT 1
            FROM   sys.tables  t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE  s.name = N'{SCHEMA}'
              AND  t.name = N'skw_faktura_log'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_faktura_log] (

                -- ── Identyfikacja ────────────────────────────────────────────
                id          INT          IDENTITY(1,1) NOT NULL,

                -- ── Powiązania ───────────────────────────────────────────────
                faktura_id  INT                        NOT NULL,
                user_id     INT                            NULL,   -- NULL = akcja systemowa

                -- ── Treść logu ───────────────────────────────────────────────
                akcja       NVARCHAR(50)               NOT NULL,
                szczegoly   NVARCHAR(MAX)                  NULL,   -- JSON (FakturaLogDetails)

                -- ── Timestamp ────────────────────────────────────────────────
                -- BRAK UpdatedAt — tabela IMMUTABLE (analogia: skw_AuditLog)
                CreatedAt   DATETIME2(7)               NOT NULL
                    CONSTRAINT [DF_sfl_CreatedAt]  DEFAULT GETDATE(),

                -- ── Klucz główny ─────────────────────────────────────────────
                CONSTRAINT [PK_skw_faktura_log]
                    PRIMARY KEY CLUSTERED (id ASC),

                -- ── Integralność referencyjna ────────────────────────────────
                CONSTRAINT [FK_sfl_faktura_id]
                    FOREIGN KEY (faktura_id)
                    REFERENCES [{SCHEMA}].[skw_faktura_akceptacja] (id),

                -- ON DELETE SET NULL — historia przeżywa dezaktywację usera
                CONSTRAINT [FK_sfl_user_id]
                    FOREIGN KEY (user_id)
                    REFERENCES [{SCHEMA}].[skw_Users] (ID_USER)
                    ON DELETE SET NULL,

                -- ── Wartości dozwolone ────────────────────────────────────────
                CONSTRAINT [CHK_sfl_akcja] CHECK (
                    akcja IN (
                        N'przypisano',
                        N'zaakceptowano',
                        N'odrzucono',
                        N'zresetowano',
                        N'status_zmieniony',
                        N'priorytet_zmieniony',
                        N'fakir_update',
                        N'fakir_update_failed',
                        N'nie_moje',
                        N'force_akceptacja',
                        N'anulowano'
                    )
                )
            );

            -- ── Indeks: historia faktury (sortowanie DESC — najnowsze pierwsze) ──
            -- Pokrywa: WHERE faktura_id=? ORDER BY CreatedAt DESC
            -- Używany przez GET /{id}/historia
            CREATE NONCLUSTERED INDEX [IX_sfl_faktura_created]
                ON [{SCHEMA}].[skw_faktura_log]
                    (faktura_id ASC, CreatedAt DESC);

            PRINT N'[007] Tabela [{SCHEMA}].[skw_faktura_log] + indeks — UTWORZONE.';
        END
        ELSE
        BEGIN
            PRINT N'[007] Tabela [{SCHEMA}].[skw_faktura_log] — już istnieje, pomijam.';
        END
    """)

    logger.info(
        "[%s] skw_faktura_log — DDL + indeks zakończone.",
        revision,
    )


# ══════════════════════════════════════════════════════════════════════════════
# PRIVATE — helpers
# ══════════════════════════════════════════════════════════════════════════════

def _drop_table_if_exists(table_name: str) -> None:
    """Usuwa tabelę jeśli istnieje — helper dla downgrade()."""
    op.execute(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[{table_name}]', N'U') IS NOT NULL
        BEGIN
            DROP TABLE [{SCHEMA}].[{table_name}];
            PRINT N'[007] Tabela [{SCHEMA}].[{table_name}] — USUNIĘTA.';
        END
        ELSE
        BEGIN
            PRINT N'[007] Tabela [{SCHEMA}].[{table_name}] — nie istnieje, pomijam.';
        END
    """)
    logger.warning(
        "[%s] _drop_table_if_exists() — tabela '%s.%s' usunięta.",
        revision, SCHEMA, table_name,
    )