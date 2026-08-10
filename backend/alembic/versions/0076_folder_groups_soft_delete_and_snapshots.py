# backend/alembic/versions/0076_folder_groups_soft_delete_and_snapshots.py
"""0076_folder_groups_soft_delete_and_snapshots

ZLECENIE FRONTU (06.08.2026, "Opcja A" — retroaktywnosc przy usunieciu
grupy wspoldzielonej): usuniecie przypisania grupy do teczki NIE MA byc
fizycznym DELETE — ma to byc soft-delete (removed_at), zeby byli
czlonkowie zachowali wglad w ZAMROZONA zawartosc teczki z momentu
usuniecia, ale NIE w aktualna, zywa zawartosc (ktora widza wylacznie
obecni czlonkowie).

Dwie zmiany:
  1. skw_document_folder_groups.removed_at (DATETIME NULL) — NULL =
     przypisanie aktywne, wypelnione = "usuniete" (ale wiersz zostaje
     w tabeli, nie jest kasowany).
  2. Nowa tabela skw_document_folder_snapshots — migawka listy
     id_instance w teczce W MOMENCIE removed_at. Bez tego "zamrozona
     zawartosc" nie miałaby z czego czerpac danych (aktualna
     skw_document_folder_items zmienia sie w czasie, nie reprezentuje
     stanu z przeszlosci).

Revision ID : 0076
Revises     : 0075
"""
import logging

from alembic import op
from sqlalchemy import text

logger = logging.getLogger(__name__)

revision = "0076"
down_revision = "0075"
branch_labels = None
depends_on = None

SCHEMA = "dbo"
GROUPS_TABLE = "skw_document_folder_groups"
SNAPSHOTS_TABLE = "skw_document_folder_snapshots"


def upgrade() -> None:
    bind = op.get_bind()

    # ── Krok 1/3: removed_at na skw_document_folder_groups ──────────────────
    logger.info("[0076] Krok 1/3 — kolumna removed_at na %s", GROUPS_TABLE)
    bind.execute(text(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{GROUPS_TABLE}]')
              AND name = N'removed_at'
        )
        BEGIN
            ALTER TABLE [{SCHEMA}].[{GROUPS_TABLE}] ADD [removed_at] DATETIME NULL;
            PRINT N'[0076] Kolumna removed_at dodana.';
        END
        ELSE
            PRINT N'[0076] Kolumna removed_at juz istnieje — pomijam.';
    """))

    # ── Krok 2/3: indeks wspierajacy domyslny filtr "aktywne" (removed_at IS NULL) ──
    logger.info("[0076] Krok 2/3 — indeks filtrowany")
    bind.execute(text(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{GROUPS_TABLE}]')
              AND name = N'IX_{GROUPS_TABLE}_active'
        )
        BEGIN
            CREATE NONCLUSTERED INDEX [IX_{GROUPS_TABLE}_active]
                ON [{SCHEMA}].[{GROUPS_TABLE}] ([id_folder], [id_group])
                WHERE [removed_at] IS NULL;
            PRINT N'[0076] Indeks IX_{GROUPS_TABLE}_active utworzony.';
        END
        ELSE
            PRINT N'[0076] Indeks juz istnieje — pomijam.';
    """))

    # ── Krok 3/3: tabela migawek historycznych ───────────────────────────────
    # UWAGA — DECYZJA TECHNICZNA CLAUDE (nie zamowiona explicite przez front
    # co do struktury): jedna migawka = jeden wiersz snapshotu z JSON-em
    # listy id_instance, nie N wierszy per dokument. Wybor uzasadniony:
    # migawka jest tworzona RAZ (w momencie removed_at), czytana rzadko
    # (tylko gdy byly czlonek odwiedza /items/historical) — normalizacja
    # do N wierszy nie dawałaby tu przewagi, tylko wiecej zlaczen.
    logger.info("[0076] Krok 3/3 — tabela %s", SNAPSHOTS_TABLE)
    bind.execute(text(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables
            WHERE schema_id = SCHEMA_ID(N'{SCHEMA}') AND name = N'{SNAPSHOTS_TABLE}'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[{SNAPSHOTS_TABLE}] (
                [id_snapshot]      INT IDENTITY(1,1) NOT NULL,
                [id_folder_group]  INT NOT NULL,
                [id_folder]        INT NOT NULL,
                [id_group]         INT NOT NULL,
                [snapshot_at]      DATETIME NOT NULL CONSTRAINT [DF_{SNAPSHOTS_TABLE}_at] DEFAULT (SYSUTCDATETIME()),
                [instance_ids_json] NVARCHAR(MAX) NOT NULL,
                CONSTRAINT [PK_{SNAPSHOTS_TABLE}] PRIMARY KEY CLUSTERED ([id_snapshot]),
                CONSTRAINT [FK_{SNAPSHOTS_TABLE}_folder_group] FOREIGN KEY ([id_folder_group])
                    REFERENCES [{SCHEMA}].[{GROUPS_TABLE}] ([id_folder_group])
                    ON DELETE NO ACTION
            );
            CREATE NONCLUSTERED INDEX [IX_{SNAPSHOTS_TABLE}_folder_group]
                ON [{SCHEMA}].[{SNAPSHOTS_TABLE}] ([id_folder_group]);
            PRINT N'[0076] Tabela {SNAPSHOTS_TABLE} utworzona.';
        END
        ELSE
            PRINT N'[0076] Tabela {SNAPSHOTS_TABLE} juz istnieje — pomijam.';
    """))
    logger.info("[0076] ZAKONCZONE")


def downgrade() -> None:
    bind = op.get_bind()
    logger.info("[0076] downgrade — usuwam tabele, indeksy, kolumne")
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID(N'{SCHEMA}') AND name = N'{SNAPSHOTS_TABLE}')
            DROP TABLE [{SCHEMA}].[{SNAPSHOTS_TABLE}]
    """))
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{GROUPS_TABLE}]') AND name = N'IX_{GROUPS_TABLE}_active')
            DROP INDEX [IX_{GROUPS_TABLE}_active] ON [{SCHEMA}].[{GROUPS_TABLE}]
    """))
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{GROUPS_TABLE}]') AND name = N'removed_at')
            ALTER TABLE [{SCHEMA}].[{GROUPS_TABLE}] DROP COLUMN [removed_at]
    """))
    logger.info("[0076] downgrade — ZAKONCZONY")