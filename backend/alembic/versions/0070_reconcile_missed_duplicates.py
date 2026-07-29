# backend/alembic/versions/0070_reconcile_missed_duplicates.py
"""0070_reconcile_missed_duplicates

Jednorazowa reconciliacja (2026-07-28) — dokumenty nieterminalne wstawione
PRZED wdrozeniem dzisiejszej poprawki DuplicateDetectionService (Metoda 1:
dopasowanie po id_document dla zrodla 'fakir') mogly przejsc pierwsze
sprawdzenie duplikatu, gdy mechanizm mial jeszcze luke — dokladnie jak
instancja 1509 (duplikat instancji 146, ktora do migracji 0069 miala
calkowicie puste extra_data).

Uruchamiane PO migracji 0069 (backfill danych fakir) — WYMAGA jej wczesniej,
bo Metoda 1 ponizej porownuje juz WYLACZNIE ksef_id_lookup (nie surowy
id_document) — po 0069 wszystkie 519 wierszy fakir ma ta kolumne wypelniona,
wiec dodatkowa galaz "source_name='fakir'" z poprawki kodu nie jest tu
potrzebna.

ZAKRES: metody 1 (ksef_id), 2 (file_sha256), 3 (invoice_fingerprint) —
w tej kolejnosci, kazda UPDATE dotyka wylacznie wierszy, ktorych NIE
zlapala metoda poprzednia (matched_instance_id IS NULL).

SWIADOMIE POMINIETA: metoda 4 (contractor_fallback, dopasowanie rozmyte
po nazwie kontrahenta, difflib.SequenceMatcher prog 0.85 w Pythonie) —
NIE jest tu reimplementowana w SQL, zeby nie tworzyc drugiej, niezaleznej
kopii tej samej logiki rozmytej, ktora z czasem zacznie sie rozjezdzac
z wersja w duplicate_detection_service.py. Krok 3 tej migracji WYPISUJE
do logu kandydatow spelniajacych czesciowe kryteria metody 4 (bez NIP,
zgodny numer/data/kwota) — do RECZNEGO przegladu, bez automatycznego
oznaczania.

DECYZJA: znalezione duplikaty oznaczane sa jako 'duplicate_pending' —
NIE anulowane automatycznie. Rozstrzygniecie (confirm/dismiss) pozostaje
w gestii referenta przez istniejacy POST /documents/{id}/duplicate-pending/
resolve, dokladnie tak jak dla duplikatow wykrywanych na biezaco. Jesli
w przyszlosci zapadnie decyzja o automatycznym anulowaniu trafien z metod
1-3 (deterministycznych) — to OSOBNA, swiadoma zmiana, nie ta migracja.

Revision ID : 0070
Revises     : 0069
"""
import logging

from alembic import op
from sqlalchemy import text

logger = logging.getLogger(__name__)

revision = "0070"
down_revision = "0069"
branch_labels = None
depends_on = None

SCHEMA = "dbo"
_NON_TERMINAL = ("pending_dispatch", "in_progress", "unassigned")


def upgrade() -> None:
    bind = op.get_bind()
    status_ph = ", ".join(f"N'{s}'" for s in _NON_TERMINAL)

    # ── Krok 0/4: stan przed ─────────────────────────────────────────────────
    logger.info("[0070] Krok 0/4 — diagnostyka przed reconciliacja")
    before = bind.execute(text(f"""
        SELECT COUNT(*) FROM [{SCHEMA}].[skw_document_approval_instances]
        WHERE [status] IN ({status_ph}) AND [matched_instance_id] IS NULL
    """)).scalar()
    logger.info("[0070] Krok 0/4 — %s instancji nieterminalnych, jeszcze bez dopasowania", before)

    # ── Krok 1/4: METODA 1 — ksef_id ─────────────────────────────────────────
    logger.info("[0070] Krok 1/4 — METODA 1 (ksef_id)")
    r1 = bind.execute(text(f"""
        UPDATE i
        SET i.[status] = N'duplicate_pending',
            i.[matched_instance_id] = m.[id_instance],
            i.[match_type] = N'ksef_id',
            i.[match_reason] = N'[reconciliacja 0070] Identyczny numer KSeF jak instancja #'
                                + CAST(m.[id_instance] AS NVARCHAR(20)) + N'.',
            i.[updated_at] = SYSUTCDATETIME()
        FROM [{SCHEMA}].[skw_document_approval_instances] i
        CROSS APPLY (
            SELECT TOP 1 i2.[id_instance]
            FROM [{SCHEMA}].[skw_document_approval_instances] i2
            WHERE i2.[id_instance] <> i.[id_instance]
              AND i2.[ksef_id_lookup] = i.[ksef_id_lookup]
            ORDER BY i2.[created_at] ASC
        ) m
        WHERE i.[status] IN ({status_ph})
          AND i.[matched_instance_id] IS NULL
          AND i.[ksef_id_lookup] IS NOT NULL
    """))
    logger.info("[0070] Krok 1/4 — OK (oznaczono %s instancji)", r1.rowcount)

    # ── Krok 2/4: METODA 2 — file_sha256 ─────────────────────────────────────
    logger.info("[0070] Krok 2/4 — METODA 2 (file_sha256)")
    r2 = bind.execute(text(f"""
        UPDATE i
        SET i.[status] = N'duplicate_pending',
            i.[matched_instance_id] = m.[id_instance],
            i.[match_type] = N'file_sha256',
            i.[match_reason] = N'[reconciliacja 0070] Identyczny hash SHA-256 pliku jak instancja #'
                                + CAST(m.[id_instance] AS NVARCHAR(20)) + N'.',
            i.[updated_at] = SYSUTCDATETIME()
        FROM [{SCHEMA}].[skw_document_approval_instances] i
        CROSS APPLY (
            SELECT TOP 1 i2.[id_instance]
            FROM [{SCHEMA}].[skw_document_approval_instances] i2
            WHERE i2.[id_instance] <> i.[id_instance]
              AND i2.[file_sha256] = i.[file_sha256]
            ORDER BY i2.[created_at] ASC
        ) m
        WHERE i.[status] IN ({status_ph})
          AND i.[matched_instance_id] IS NULL
          AND i.[file_sha256] IS NOT NULL
    """))
    logger.info("[0070] Krok 2/4 — OK (oznaczono %s instancji)", r2.rowcount)

    # ── Krok 3/4: METODA 3 — invoice_fingerprint ─────────────────────────────
    logger.info("[0070] Krok 3/4 — METODA 3 (invoice_fingerprint)")
    r3 = bind.execute(text(f"""
        UPDATE i
        SET i.[status] = N'duplicate_pending',
            i.[matched_instance_id] = m.[id_instance],
            i.[match_type] = N'invoice_fingerprint',
            i.[match_reason] = N'[reconciliacja 0070] Zgodnosc NIP, numeru, daty i kwoty z instancja #'
                                + CAST(m.[id_instance] AS NVARCHAR(20)) + N'.',
            i.[updated_at] = SYSUTCDATETIME()
        FROM [{SCHEMA}].[skw_document_approval_instances] i
        CROSS APPLY (
            SELECT TOP 1 i2.[id_instance]
            FROM [{SCHEMA}].[skw_document_approval_instances] i2
            WHERE i2.[id_instance] <> i.[id_instance]
              AND JSON_VALUE(i2.[extra_data], '$.nip')        = JSON_VALUE(i.[extra_data], '$.nip')
              AND JSON_VALUE(i2.[extra_data], '$.doc_number') = JSON_VALUE(i.[extra_data], '$.doc_number')
              AND JSON_VALUE(i2.[extra_data], '$.doc_date')   = JSON_VALUE(i.[extra_data], '$.doc_date')
              AND COALESCE(JSON_VALUE(i2.[extra_data], '$.currency'), N'PLN')
                = COALESCE(JSON_VALUE(i.[extra_data], '$.currency'), N'PLN')
              AND i2.[document_amount] IS NOT NULL AND i.[document_amount] IS NOT NULL
              AND ABS(i2.[document_amount] - i.[document_amount]) <= 0.01
            ORDER BY i2.[created_at] ASC
        ) m
        WHERE i.[status] IN ({status_ph})
          AND i.[matched_instance_id] IS NULL
          AND JSON_VALUE(i.[extra_data], '$.nip') IS NOT NULL
          AND JSON_VALUE(i.[extra_data], '$.doc_number') IS NOT NULL
          AND JSON_VALUE(i.[extra_data], '$.doc_date') IS NOT NULL
    """))
    logger.info("[0070] Krok 3/4 — OK (oznaczono %s instancji)", r3.rowcount)

    # ── Krok 4/4: METODA 4 — TYLKO raport, bez automatycznego oznaczania ────
    logger.info("[0070] Krok 4/4 — METODA 4 (contractor_fallback) — WYLACZNIE raport do przegladu")
    candidates = bind.execute(text(f"""
        SELECT i.[id_instance], m.[id_instance] AS candidate_id,
               JSON_VALUE(i.[extra_data], '$.contractor')   AS kontrahent_i,
               JSON_VALUE(m.[extra_data], '$.contractor')   AS kontrahent_m
        FROM [{SCHEMA}].[skw_document_approval_instances] i
        CROSS APPLY (
            SELECT TOP 1 i2.[id_instance], i2.[extra_data]
            FROM [{SCHEMA}].[skw_document_approval_instances] i2
            WHERE i2.[id_instance] <> i.[id_instance]
              AND JSON_VALUE(i2.[extra_data], '$.doc_number') = JSON_VALUE(i.[extra_data], '$.doc_number')
              AND JSON_VALUE(i2.[extra_data], '$.doc_date')   = JSON_VALUE(i.[extra_data], '$.doc_date')
              AND i2.[document_amount] IS NOT NULL AND i.[document_amount] IS NOT NULL
              AND ABS(i2.[document_amount] - i.[document_amount]) <= 0.01
            ORDER BY i2.[created_at] ASC
        ) m ([id_instance], [extra_data])
        WHERE i.[status] IN ({status_ph})
          AND i.[matched_instance_id] IS NULL
          AND JSON_VALUE(i.[extra_data], '$.nip') IS NULL
          AND JSON_VALUE(i.[extra_data], '$.doc_number') IS NOT NULL
    """)).fetchall()
    if candidates:
        logger.warning(
            "[0070] Krok 4/4 — %s par WYMAGA RECZNEGO PRZEGLADU (dopasowanie po numerze/"
            "dacie/kwocie, brak NIP po jednej stronie — podobienstwo nazwy NIE sprawdzone "
            "automatycznie w tej migracji):",
            len(candidates),
        )
        for id_instance, candidate_id, k_i, k_m in candidates:
            logger.warning(
                "[0070]   instancja #%s (%r) <-> #%s (%r) — sprawdz recznie w UI",
                id_instance, k_i, candidate_id, k_m,
            )
    else:
        logger.info("[0070] Krok 4/4 — OK (brak kandydatow do recznego przegladu)")

    logger.info(
        "[0070] ZAKONCZONE — lacznie oznaczono %s instancji jako duplicate_pending "
        "(metoda 1: %s, metoda 2: %s, metoda 3: %s). %s par czeka na reczny przeglad "
        "(metoda 4, nieautomatyzowana).",
        r1.rowcount + r2.rowcount + r3.rowcount, r1.rowcount, r2.rowcount, r3.rowcount,
        len(candidates),
    )


def downgrade() -> None:
    # Celowo NO-OP — jak w 0069. Cofanie oznaczen duplicate_pending nadanych
    # tu wymagaloby wiedziec, ktore z nich zostaly JUZ rozstrzygniete recznie
    # przez referenta w miedzyczasie (te NIE powinny wracac do pending_dispatch) —
    # bezpieczny rollback wymagalby osobnej, swiadomej analizy, nie automatu.
    logger.warning(
        "[0070] downgrade() jest celowo NO-OP — patrz uzasadnienie w docstringu migracji."
    )