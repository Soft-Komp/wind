# backend/alembic/versions/0064_superseded_by_instance_id.py
"""0064_superseded_by_instance_id

Wariant A (ustalony z frontem, 2026-07-22) dla problemu widocznosci
duplikatow po cancel+redispatch (np. 717 cancelled + 719 in_progress
bez danych). Analogia do wzorca D-09 (duplicate_of) z listy decyzji
architektonicznych Etapu 2 — ale dla przypadku unassigned/source_orphaned,
ktory w tej samej liscie (D-08) mial jawnie zaznaczone "brak osobnego
mechanizmu".

Nowa kolumna superseded_by_instance_id (self-referencing FK, nullable):
  - NULL = instancja nie zostala zastapiona (normalny przypadek)
  - wypelnione = ta instancja (zazwyczaj cancelled) zostala zastapiona
    przez inna, nowsza instancje tego samego dokumentu

Backend: dispatch_document (instances.py) ustawia to pole automatycznie
po utworzeniu nowej instancji, jesli dla tego samego (id_document,
id_source) istnieje starsza instancja cancelled bez tego pola ustawionego.

Backfill: istniejace pary typu 717/719 sa laczone retroaktywnie —
kazda cancelled instancja bez superseded_by_instance_id zostaje
polaczona z najblizsza NASTEPNA (po czasie utworzenia) instancja tego
samego dokumentu/zrodla, jesli taka istnieje.

Revision ID : 0064
Revises     : 0063
"""
import logging

from alembic import op
from sqlalchemy import text

logger = logging.getLogger(__name__)

revision = "0064"
down_revision = "0063"
branch_labels = None
depends_on = None

SCHEMA = "dbo"
TABLE = "skw_document_approval_instances"


def upgrade() -> None:
    bind = op.get_bind()

    # ── Krok 1: kolumna (idempotentnie) ──────────────────────────────────────
    logger.info("[0064] Krok 1/5 — kolumna superseded_by_instance_id")
    col_exists = bind.execute(text(f"""
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE}]')
          AND name = N'superseded_by_instance_id'
    """)).fetchone()
    if not col_exists:
        bind.execute(text(f"""
            ALTER TABLE [{SCHEMA}].[{TABLE}]
                ADD [superseded_by_instance_id] INT NULL
        """))
        logger.info("[0064] Krok 1/5 — OK (kolumna dodana)")
    else:
        logger.info("[0064] Krok 1/5 — OK (kolumna juz istnieje, pomijam)")

    # ── Krok 2: FK self-referencing (NO ACTION — brak hard-delete w tabeli) ──
    logger.info("[0064] Krok 2/5 — FK_skw_dai_superseded_by")
    fk_exists = bind.execute(text(f"""
        SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_skw_dai_superseded_by'
    """)).fetchone()
    if not fk_exists:
        bind.execute(text(f"""
            ALTER TABLE [{SCHEMA}].[{TABLE}]
                ADD CONSTRAINT [FK_skw_dai_superseded_by]
                FOREIGN KEY ([superseded_by_instance_id])
                REFERENCES [{SCHEMA}].[{TABLE}] ([id_instance])
                ON DELETE NO ACTION ON UPDATE NO ACTION
        """))
        logger.info("[0064] Krok 2/5 — OK (FK dodany)")
    else:
        logger.info("[0064] Krok 2/5 — OK (FK juz istnieje, pomijam)")

    # ── Krok 3: indeks wspierajacy domyslny filtr IS NULL na liscie ──────────
    logger.info("[0064] Krok 3/5 — indeks IX_skw_dai_superseded_by")
    idx_exists = bind.execute(text(f"""
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE}]')
          AND name = N'IX_skw_dai_superseded_by'
    """)).fetchone()
    if not idx_exists:
        bind.execute(text(f"""
            CREATE NONCLUSTERED INDEX [IX_skw_dai_superseded_by]
                ON [{SCHEMA}].[{TABLE}] ([superseded_by_instance_id])
        """))
        logger.info("[0064] Krok 3/5 — OK (indeks utworzony)")
    else:
        logger.info("[0064] Krok 3/5 — OK (indeks juz istnieje, pomijam)")

    # ── Krok 4: BACKFILL — laczenie istniejacych par (np. 717 -> 719) ────────
    logger.info("[0064] Krok 4/5 — backfill istniejacych par cancelled -> kolejna instancja")
    result = bind.execute(text(f"""
        ;WITH ranked AS (
            SELECT
                i1.[id_instance] AS old_id,
                (
                    SELECT TOP 1 i2.[id_instance]
                    FROM [{SCHEMA}].[{TABLE}] i2
                    WHERE i2.[id_document] = i1.[id_document]
                      AND i2.[id_source]   = i1.[id_source]
                      AND i2.[id_instance] <> i1.[id_instance]
                      AND i2.[created_at]  > i1.[created_at]
                    ORDER BY i2.[created_at] ASC
                ) AS new_id
            FROM [{SCHEMA}].[{TABLE}] i1
            WHERE i1.[status] = N'cancelled'
              AND i1.[superseded_by_instance_id] IS NULL
        )
        UPDATE i
        SET i.[superseded_by_instance_id] = r.[new_id],
            i.[updated_at] = SYSUTCDATETIME()
        FROM [{SCHEMA}].[{TABLE}] i
        JOIN ranked r ON r.[old_id] = i.[id_instance]
        WHERE r.[new_id] IS NOT NULL
    """))
    logger.info("[0064] Krok 4/5 — OK (polaczono %s istniejacych par)", result.rowcount)

    # ── Krok 5: log weryfikacyjny — ktore pary zostaly polaczone ─────────────
    logger.info("[0064] Krok 5/5 — weryfikacja")
    rows = bind.execute(text(f"""
        SELECT [id_instance], [id_document], [id_source], [superseded_by_instance_id]
        FROM [{SCHEMA}].[{TABLE}]
        WHERE [superseded_by_instance_id] IS NOT NULL
        ORDER BY [id_instance]
    """)).fetchall()
    for id_instance, id_document, id_source, superseded_by in rows:
        logger.info(
            "[0064]   instancja %s (doc=%s, src=%s) -> zastapiona przez %s",
            id_instance, id_document, id_source, superseded_by,
        )
    logger.info("[0064] Krok 5/5 — OK (%d powiazan)", len(rows))


def downgrade() -> None:
    bind = op.get_bind()
    logger.info("[0064] downgrade — usuwam indeks, FK, kolumne")
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE}]') AND name = N'IX_skw_dai_superseded_by')
            DROP INDEX [IX_skw_dai_superseded_by] ON [{SCHEMA}].[{TABLE}]
    """))
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_skw_dai_superseded_by')
            ALTER TABLE [{SCHEMA}].[{TABLE}] DROP CONSTRAINT [FK_skw_dai_superseded_by]
    """))
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE}]') AND name = N'superseded_by_instance_id')
            ALTER TABLE [{SCHEMA}].[{TABLE}] DROP COLUMN [superseded_by_instance_id]
    """))
    logger.info("[0064] downgrade — ZAKONCZONY")