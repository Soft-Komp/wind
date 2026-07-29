# backend/alembic/versions/0068_duplicate_match_columns_and_source_cursor.py
"""0068_duplicate_match_columns_and_source_cursor

DECYZJA PRODUKTOWA (2026-07-28, jawnie potwierdzona przez wlasciciela projektu):
PORZUCAMY model D-09 (adnotacja duplikatu w extra_data: duplicate_of_id_instance/
duplicate_confidence/duplicate_score) na rzecz dedykowanych kolumn relacyjnych —
analogia do wzorca superseded_by_instance_id z migracji 0064.

Nowe kolumny na skw_document_approval_instances:
  matched_instance_id  INT NULL           — FK self-referencing (jak 0064)
  match_type           NVARCHAR(30) NULL  — CHECK: ksef_id|file_sha256|
                                             invoice_fingerprint|contractor_fallback
  match_reason         NVARCHAR(500) NULL — opis dla referenta/audytu
  file_sha256          NVARCHAR(64) NULL  — hash pliku (manual/ftp/email)
  ksef_id_lookup        — kolumna WYLICZANA (PERSISTED) z extra_data.ksef_id,
                           zeby dopasowanie po KSeF ID nie wymagalo skanowania
                           JSON_VALUE() na calej tabeli przy kazdym nowym dokumencie

ZASADA (na wyrazna prosbe frontu, 2026-07-28): wykrywanie duplikatow ma dzialac
MIEDZY WSZYSTKIMI ZRODLAMI, BEZ OGRANICZENIA CZASOWEGO, WLACZNIE z dokumentami
approved/cancelled/rejected. Dlatego indeksy nizej sa zaprojektowane pod
zapytania PO WSZYSTKICH wierszach (bez WHERE status/created_at), nie pod
dotychczasowe zawezenie z duplicate_detection_service.py (90 dni, ten sam
zrodlo, statusy aktywne) — to zawezenie jest w tej samej sesji usuwane z kodu
serwisu (patrz osobny plik).

Druga, niezalezna zmiana w tej migracji: kolumna sync_cursor na
skw_document_sources — oddzielenie "kiedy worker ostatnio probowal
synchronizowac" (last_sync_at, bez zmian) od "gdzie w danych zrodla worker
doszedl" (nowe pole, JSON, ksztalt zalezny od adaptera). Naprawia wielokrotne
pobieranie tych samych TOP N rekordow dla zrodel bez date_column (zgloszenie
2026-07-28: zrodlo #4, date_column=None).

Revision ID : 0068
Revises     : 0067
"""
import logging

from alembic import op
from sqlalchemy import text

logger = logging.getLogger(__name__)

revision = "0068"
down_revision = "0067"
branch_labels = None
depends_on = None

SCHEMA = "dbo"
TABLE_INSTANCES = "skw_document_approval_instances"
TABLE_SOURCES = "skw_document_sources"


def upgrade() -> None:
    bind = op.get_bind()

    # ── Krok 1/8: matched_instance_id ────────────────────────────────────────
    logger.info("[0068] Krok 1/8 — kolumna matched_instance_id")
    if not bind.execute(text(f"""
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_INSTANCES}]')
          AND name = N'matched_instance_id'
    """)).fetchone():
        bind.execute(text(f"""
            ALTER TABLE [{SCHEMA}].[{TABLE_INSTANCES}]
                ADD [matched_instance_id] INT NULL
        """))
        logger.info("[0068] Krok 1/8 — OK (kolumna dodana)")
    else:
        logger.info("[0068] Krok 1/8 — OK (juz istnieje, pomijam)")

    # ── Krok 2/8: FK self-referencing (wzorzec identyczny jak 0064) ─────────
    logger.info("[0068] Krok 2/8 — FK_skw_dai_matched_instance")
    if not bind.execute(text("""
        SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_skw_dai_matched_instance'
    """)).fetchone():
        bind.execute(text(f"""
            ALTER TABLE [{SCHEMA}].[{TABLE_INSTANCES}]
                ADD CONSTRAINT [FK_skw_dai_matched_instance]
                FOREIGN KEY ([matched_instance_id])
                REFERENCES [{SCHEMA}].[{TABLE_INSTANCES}] ([id_instance])
                ON DELETE NO ACTION ON UPDATE NO ACTION
        """))
        logger.info("[0068] Krok 2/8 — OK (FK dodany)")
    else:
        logger.info("[0068] Krok 2/8 — OK (juz istnieje, pomijam)")

    # ── Krok 3/8: match_type + CHECK ─────────────────────────────────────────
    logger.info("[0068] Krok 3/8 — kolumna match_type + CHECK")
    if not bind.execute(text(f"""
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_INSTANCES}]')
          AND name = N'match_type'
    """)).fetchone():
        bind.execute(text(f"""
            ALTER TABLE [{SCHEMA}].[{TABLE_INSTANCES}]
                ADD [match_type] NVARCHAR(30) NULL
        """))
        bind.execute(text(f"""
            ALTER TABLE [{SCHEMA}].[{TABLE_INSTANCES}]
                ADD CONSTRAINT [CHK_skw_dai_match_type] CHECK (
                    [match_type] IS NULL OR [match_type] IN (
                        N'ksef_id', N'file_sha256',
                        N'invoice_fingerprint', N'contractor_fallback'
                    )
                )
        """))
        logger.info("[0068] Krok 3/8 — OK (kolumna + CHECK dodane)")
    else:
        logger.info("[0068] Krok 3/8 — OK (juz istnieje, pomijam)")

    # ── Krok 4/8: match_reason ────────────────────────────────────────────────
    logger.info("[0068] Krok 4/8 — kolumna match_reason")
    if not bind.execute(text(f"""
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_INSTANCES}]')
          AND name = N'match_reason'
    """)).fetchone():
        bind.execute(text(f"""
            ALTER TABLE [{SCHEMA}].[{TABLE_INSTANCES}]
                ADD [match_reason] NVARCHAR(500) NULL
        """))
        logger.info("[0068] Krok 4/8 — OK (kolumna dodana)")
    else:
        logger.info("[0068] Krok 4/8 — OK (juz istnieje, pomijam)")

    # ── Krok 5/8: file_sha256 + indeks filtrowany ────────────────────────────
    logger.info("[0068] Krok 5/8 — kolumna file_sha256 + indeks")
    if not bind.execute(text(f"""
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_INSTANCES}]')
          AND name = N'file_sha256'
    """)).fetchone():
        bind.execute(text(f"""
            ALTER TABLE [{SCHEMA}].[{TABLE_INSTANCES}]
                ADD [file_sha256] NVARCHAR(64) NULL
        """))
        logger.info("[0068] Krok 5/8 — OK (kolumna dodana)")
    else:
        logger.info("[0068] Krok 5/8 — OK (kolumna juz istnieje, pomijam)")

    if not bind.execute(text(f"""
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_INSTANCES}]')
          AND name = N'IX_skw_dai_file_sha256'
    """)).fetchone():
        bind.execute(text(f"""
            CREATE NONCLUSTERED INDEX [IX_skw_dai_file_sha256]
                ON [{SCHEMA}].[{TABLE_INSTANCES}] ([file_sha256])
                WHERE [file_sha256] IS NOT NULL
        """))
        logger.info("[0068] Krok 5/8 — OK (indeks filtrowany utworzony)")
    else:
        logger.info("[0068] Krok 5/8 — OK (indeks juz istnieje, pomijam)")

    # ── Krok 6/8: ksef_id_lookup (PERSISTED, z extra_data) + indeks ─────────
    logger.info("[0068] Krok 6/8 — kolumna wyliczana ksef_id_lookup + indeks")
    if not bind.execute(text(f"""
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_INSTANCES}]')
          AND name = N'ksef_id_lookup'
    """)).fetchone():
        bind.execute(text(f"""
            ALTER TABLE [{SCHEMA}].[{TABLE_INSTANCES}]
                ADD [ksef_id_lookup] AS JSON_VALUE([extra_data], '$.ksef_id') PERSISTED
        """))
        logger.info("[0068] Krok 6/8 — OK (kolumna wyliczana dodana)")
    else:
        logger.info("[0068] Krok 6/8 — OK (kolumna juz istnieje, pomijam)")

    if not bind.execute(text(f"""
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_INSTANCES}]')
          AND name = N'IX_skw_dai_ksef_id_lookup'
    """)).fetchone():
        # NAPRAWA (2026-07-28, incydent podczas wdrozenia): SQL Server
        # (blad 10609) NIE POZWALA na indeks FILTROWANY, ktorego predykat
        # WHERE odwoluje sie do kolumny wyliczanej (nawet PERSISTED) —
        # "Rewrite the filter expression so that it does not include this
        # column". Ograniczenie silnika, nie da sie tego obejsc filtrem na
        # samej ksef_id_lookup. Rezygnujemy z filtra na TYM indeksie —
        # zwykly (niefiltrowany) indeks na kolumnie wyliczanej dziala bez
        # przeszkod. Kolumna i tak jest NULL dla wiekszosci wierszy (tylko
        # KSeF ja wypelnia), wiec strata na rozmiarze indeksu jest niewielka.
        bind.execute(text(f"""
            CREATE NONCLUSTERED INDEX [IX_skw_dai_ksef_id_lookup]
                ON [{SCHEMA}].[{TABLE_INSTANCES}] ([ksef_id_lookup])
        """))
        logger.info("[0068] Krok 6/8 — OK (indeks utworzony, BEZ filtra — patrz komentarz o bledzie 10609)")
    else:
        logger.info("[0068] Krok 6/8 — OK (indeks juz istnieje, pomijam)")

    # ── Krok 7/8: indeks pod JOIN po matched_instance_id ─────────────────────
    logger.info("[0068] Krok 7/8 — indeks IX_skw_dai_matched_instance_id")
    if not bind.execute(text(f"""
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_INSTANCES}]')
          AND name = N'IX_skw_dai_matched_instance_id'
    """)).fetchone():
        bind.execute(text(f"""
            CREATE NONCLUSTERED INDEX [IX_skw_dai_matched_instance_id]
                ON [{SCHEMA}].[{TABLE_INSTANCES}] ([matched_instance_id])
                WHERE [matched_instance_id] IS NOT NULL
        """))
        logger.info("[0068] Krok 7/8 — OK (indeks utworzony)")
    else:
        logger.info("[0068] Krok 7/8 — OK (indeks juz istnieje, pomijam)")

    # ── Krok 8/8: sync_cursor na skw_document_sources ───────────────────────
    logger.info("[0068] Krok 8/8 — kolumna sync_cursor (skw_document_sources)")
    if not bind.execute(text(f"""
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_SOURCES}]')
          AND name = N'sync_cursor'
    """)).fetchone():
        bind.execute(text(f"""
            ALTER TABLE [{SCHEMA}].[{TABLE_SOURCES}]
                ADD [sync_cursor] NVARCHAR(1000) NULL
        """))
        logger.info(
            "[0068] Krok 8/8 — OK (kolumna dodana; JSON, ksztalt zalezny od "
            "adaptera — patrz DatabaseAdapter.extract_cursor() w unified_document.py)"
        )
    else:
        logger.info("[0068] Krok 8/8 — OK (juz istnieje, pomijam)")

    logger.info("[0068] ZAKONCZONE")


def downgrade() -> None:
    bind = op.get_bind()
    logger.info("[0068] downgrade — usuwam wszystkie obiekty z tej migracji")

    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_SOURCES}]') AND name = N'sync_cursor')
            ALTER TABLE [{SCHEMA}].[{TABLE_SOURCES}] DROP COLUMN [sync_cursor]
    """))
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_INSTANCES}]') AND name = N'IX_skw_dai_matched_instance_id')
            DROP INDEX [IX_skw_dai_matched_instance_id] ON [{SCHEMA}].[{TABLE_INSTANCES}]
    """))
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_INSTANCES}]') AND name = N'IX_skw_dai_ksef_id_lookup')
            DROP INDEX [IX_skw_dai_ksef_id_lookup] ON [{SCHEMA}].[{TABLE_INSTANCES}]
    """))
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_INSTANCES}]') AND name = N'ksef_id_lookup')
            ALTER TABLE [{SCHEMA}].[{TABLE_INSTANCES}] DROP COLUMN [ksef_id_lookup]
    """))
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_INSTANCES}]') AND name = N'IX_skw_dai_file_sha256')
            DROP INDEX [IX_skw_dai_file_sha256] ON [{SCHEMA}].[{TABLE_INSTANCES}]
    """))
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_INSTANCES}]') AND name = N'file_sha256')
            ALTER TABLE [{SCHEMA}].[{TABLE_INSTANCES}] DROP COLUMN [file_sha256]
    """))
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_INSTANCES}]') AND name = N'match_reason')
            ALTER TABLE [{SCHEMA}].[{TABLE_INSTANCES}] DROP COLUMN [match_reason]
    """))
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = N'CHK_skw_dai_match_type')
            ALTER TABLE [{SCHEMA}].[{TABLE_INSTANCES}] DROP CONSTRAINT [CHK_skw_dai_match_type]
    """))
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_INSTANCES}]') AND name = N'match_type')
            ALTER TABLE [{SCHEMA}].[{TABLE_INSTANCES}] DROP COLUMN [match_type]
    """))
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_skw_dai_matched_instance')
            ALTER TABLE [{SCHEMA}].[{TABLE_INSTANCES}] DROP CONSTRAINT [FK_skw_dai_matched_instance]
    """))
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE_INSTANCES}]') AND name = N'matched_instance_id')
            ALTER TABLE [{SCHEMA}].[{TABLE_INSTANCES}] DROP COLUMN [matched_instance_id]
    """))
    logger.info("[0068] downgrade — ZAKONCZONY")