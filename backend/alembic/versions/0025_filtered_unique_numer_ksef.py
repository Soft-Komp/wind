"""
0025_filtered_unique_numer_ksef

Zamiana standardowego UNIQUE na filtered unique index dla numer_ksef
w tabeli dbo_ext.skw_faktura_akceptacja.

Problem: UQ_skw_faktura_akceptacja_numer_ksef (bez filtra) blokował
         ponowne wpuszczenie do obiegu faktur z IsActive=0 (anulowanych),
         zwracając HTTP 500 zamiast poprawnego INSERT nowego wiersza.

Rozwiązanie (Opcja A — re-entry dozwolony):
    DROP CONSTRAINT UQ_skw_faktura_akceptacja_numer_ksef
    CREATE UNIQUE INDEX ... WHERE IsActive = 1

Unikalność obowiązuje TYLKO wśród aktywnych rekordów.
Anulowane (IsActive=0) nie blokują nowego wpisu.

Revision: 0025
Revises:  0024
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from alembic import op

logger = logging.getLogger("alembic.0010_filtered_unique_numer_ksef")

# ─────────────────────────────────────────────────────────────────────────────
revision:       str       = "0025"
down_revision:  str       = "0024"
branch_labels:  None      = None
depends_on:     None      = None
# ─────────────────────────────────────────────────────────────────────────────

# Nazwa starego constraintu (UNIQUE niezfiltrowany)
_OLD_CONSTRAINT = "UQ_skw_faktura_akceptacja_numer_ksef"

# Nazwa nowego indeksu (filtered unique WHERE IsActive = 1)
_NEW_INDEX      = "UQ_skw_faktura_akceptacja_numer_ksef_active"

# Tabela
_TABLE          = "dbo_ext.skw_faktura_akceptacja"


def upgrade() -> None:
    logger.info(
        "[0010] START upgrade | ts=%s",
        datetime.now(timezone.utc).isoformat(),
    )

    # ── KROK 1: Usuń stary UNIQUE constraint (niezfiltrowany) ─────────────────
    # UWAGA: W MSSQL UNIQUE CONSTRAINT jest wewnętrznie indeksem.
    # Jeśli constraint nie istnieje (np. clean-install), blok jest idempotentny
    # dzięki warunkowemu DROP.
    op.execute(f"""
        IF EXISTS (
            SELECT 1
            FROM   sys.indexes i
            JOIN   sys.objects o ON i.object_id = o.object_id
            JOIN   sys.schemas s ON o.schema_id = s.schema_id
            WHERE  s.name   = 'dbo_ext'
              AND  o.name   = 'skw_faktura_akceptacja'
              AND  i.name   = '{_OLD_CONSTRAINT}'
        )
        BEGIN
            ALTER TABLE dbo_ext.skw_faktura_akceptacja
            DROP CONSTRAINT {_OLD_CONSTRAINT};
        END
    """)
    logger.info("[0010] KROK 1 OK — stary constraint usunięty (lub nie istniał)")

    # ── KROK 2: Utwórz filtered unique index (WHERE IsActive = 1) ────────────
    # Filtr MSSQL: IsActive jest kolumną BIT — wartość 1 odpowiada True.
    # Nowy indeks gwarantuje unikalność numer_ksef TYLKO wśród aktywnych wpisów.
    # Wiele anulowanych wpisów (IsActive=0) dla tego samego numer_ksef jest OK.
    op.execute(f"""
        IF NOT EXISTS (
            SELECT 1
            FROM   sys.indexes i
            JOIN   sys.objects o ON i.object_id = o.object_id
            JOIN   sys.schemas s ON o.schema_id = s.schema_id
            WHERE  s.name = 'dbo_ext'
              AND  o.name = 'skw_faktura_akceptacja'
              AND  i.name = '{_NEW_INDEX}'
        )
        BEGIN
            CREATE UNIQUE INDEX {_NEW_INDEX}
            ON dbo_ext.skw_faktura_akceptacja (numer_ksef)
            WHERE IsActive = 1;
        END
    """)
    logger.info("[0010] KROK 2 OK — filtered unique index utworzony")

    # ── KROK 3: Weryfikacja ───────────────────────────────────────────────────
    op.execute(f"""
        IF NOT EXISTS (
            SELECT 1
            FROM   sys.indexes i
            JOIN   sys.objects o ON i.object_id = o.object_id
            JOIN   sys.schemas s ON o.schema_id = s.schema_id
            WHERE  s.name   = 'dbo_ext'
              AND  o.name   = 'skw_faktura_akceptacja'
              AND  i.name   = '{_NEW_INDEX}'
              AND  i.is_unique = 1
              AND  i.filter_definition IS NOT NULL
        )
        BEGIN
            RAISERROR (
                'WERYFIKACJA NIEUDANA: filtered unique index nie istnieje po migracji 0010',
                16, 1
            );
        END
    """)
    logger.info("[0010] KROK 3 OK — weryfikacja filtered index passed")

    logger.info(
        "[0010] UPGRADE ZAKOŃCZONY POMYŚLNIE | ts=%s",
        datetime.now(timezone.utc).isoformat(),
    )


def downgrade() -> None:
    """
    Przywraca stary niezfiltrowany UNIQUE constraint.

    UWAGA: downgrade ZAKOŃCZY SIĘ BŁĘDEM jeśli w tabeli istnieją
    zduplikowane numer_ksef wśród IsActive=0 (co jest intencjonalnym stanem
    po migracji upgrade). Wyczyść duplikaty przed downgrade.
    """
    logger.warning(
        "[0010] DOWNGRADE — przywracanie niezfiltrowanego UNIQUE | ts=%s",
        datetime.now(timezone.utc).isoformat(),
    )

    # Usuń filtered index
    op.execute(f"""
        IF EXISTS (
            SELECT 1
            FROM   sys.indexes i
            JOIN   sys.objects o ON i.object_id = o.object_id
            JOIN   sys.schemas s ON o.schema_id = s.schema_id
            WHERE  s.name = 'dbo_ext'
              AND  o.name = 'skw_faktura_akceptacja'
              AND  i.name = '{_NEW_INDEX}'
        )
        BEGIN
            DROP INDEX {_NEW_INDEX}
            ON dbo_ext.skw_faktura_akceptacja;
        END
    """)

    # Przywróć stary constraint
    op.execute(f"""
        IF NOT EXISTS (
            SELECT 1
            FROM   sys.indexes i
            JOIN   sys.objects o ON i.object_id = o.object_id
            JOIN   sys.schemas s ON o.schema_id = s.schema_id
            WHERE  s.name = 'dbo_ext'
              AND  o.name = 'skw_faktura_akceptacja'
              AND  i.name = '{_OLD_CONSTRAINT}'
        )
        BEGIN
            ALTER TABLE dbo_ext.skw_faktura_akceptacja
            ADD CONSTRAINT {_OLD_CONSTRAINT}
            UNIQUE (numer_ksef);
        END
    """)

    logger.info("[0010] DOWNGRADE ZAKOŃCZONY")