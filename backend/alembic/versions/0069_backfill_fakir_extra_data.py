# backend/alembic/versions/0069_backfill_fakir_extra_data.py
"""0069_backfill_fakir_extra_data

Naprawa danych historycznych (2026-07-28, incydent: instancje 146/1509,
przepuszczony duplikat). 519 instancji zrodla 'fakir' ma extra_data
calkowicie puste (NULL lub '{}') — prawdopodobnie pochodza sprzed pelnej
migracji Etapu 2 (Krok 0: skw_faktura_akceptacja -> skw_document_approval_
instances) i nigdy nie przeszly przez UnifiedDocument.to_extra_data_json().

Skutek: DuplicateDetectionService (kaskada 4 metod, wersja z 2026-07-28)
nie ma dla tych wierszy ZADNYCH danych do porownania — sa niewidoczne dla
metod 1/3/4 (metoda 2 — hash pliku — i tak by ich nie objela, zrodlo
bazodanowe nie ma pliku).

Zakres naprawy TEJ migracji: WYLACZNIE zrodlo 'fakir' (potwierdzone
zapytaniem: 519/519 pustych wierszy nalezy do tego zrodla). Dociagane pola
identyczne z tym, co FakirDocumentAdapter.get_document() zapisalby normalnie
— WLACZNIE z nip=NULL, bo aktualny widok skw_faktury_akceptacja_naglowek
nie ma kolumny NIP (potwierdzone komentarzem w unified_document.py:
"nip=None  # NIP nie istnieje w aktualnym widoku"). Dokumenty bez NIP-u
nadal beda lapane przez metode 4 kaskady (contractor_fallback).

NIE JEST W ZAKRESIE tej migracji: wiersze, dla ktorych KSEF_ID nie ma juz
odpowiednika w skw_faktury_akceptacja_naglowek (np. przeterminowana
retencja WAPRO) — te zostaja z pusta extra_data, zabezpieczone WYLACZNIE
przez zmiane kodu w duplicate_detection_service.py (metoda 1 dopasowuje
teraz rowniez po surowym id_document dla zrodla 'fakir', niezaleznie od
extra_data). Liczba takich wierszy zalogowana ponizej do dalszej analizy,
nie naprawiana automatycznie — brak zrodla danych do wypelnienia.

Uzyto JSON_OBJECT (SQL Server 2022+, potwierdzona wersja silnika w tym
projekcie) zamiast recznej konkatenacji stringow — bezpieczne escapowanie
cudzyslowow/znakow specjalnych w nazwach kontrahentow (np. wystapienie
'"GRADZIEL"' w danych, ktore wywolalo ten incydent) bez recznego
dopisywania logiki ucieczki znakow.

Revision ID : 0069
Revises     : 0068
"""
import logging

from alembic import op
from sqlalchemy import text

logger = logging.getLogger(__name__)

revision = "0069"
down_revision = "0068"
branch_labels = None
depends_on = None

SCHEMA = "dbo"


def upgrade() -> None:
    bind = op.get_bind()

    # ── Krok 0/3: stan przed — ile mozliwe do naprawy, ile nie ──────────────
    logger.info("[0069] Krok 0/3 — diagnostyka przed backfillem")
    diag = bind.execute(text(f"""
        SELECT
            COUNT(*) AS total_puste,
            SUM(CASE WHEN fah.[KSEF_ID] IS NOT NULL THEN 1 ELSE 0 END) AS mozliwe,
            SUM(CASE WHEN fah.[KSEF_ID] IS NULL THEN 1 ELSE 0 END) AS niemozliwe
        FROM [{SCHEMA}].[skw_document_approval_instances] i
        JOIN [{SCHEMA}].[skw_document_sources] ds
             ON ds.[id_source] = i.[id_source] AND ds.[source_name] = N'fakir'
        LEFT JOIN [{SCHEMA}].[skw_faktury_akceptacja_naglowek] fah
             ON fah.[KSEF_ID] = i.[id_document]
        WHERE i.[extra_data] IS NULL OR i.[extra_data] = N'{{}}'
    """)).fetchone()
    logger.info(
        "[0069] Krok 0/3 — puste=%s, mozliwe_do_naprawy=%s, BEZ zrodla danych=%s "
        "(te ostatnie zostana z pusta extra_data — zabezpieczone wylacznie "
        "przez zmiane kodu w duplicate_detection_service.py, nie przez ta migracje)",
        diag[0], diag[1], diag[2],
    )

    # ── Krok 1/3: backfill extra_data + document_title + document_amount ────
    logger.info("[0069] Krok 1/3 — UPDATE (JSON_OBJECT, NULL ON NULL)")
    result = bind.execute(text(f"""
        UPDATE i
        SET
            i.[extra_data] = JSON_OBJECT(
                'ksef_id':       i.[id_document],
                'doc_number':    fah.[NUMER],
                'doc_date':      CONVERT(varchar(10), fah.[DataWystawienia], 23),
                'contractor':    fah.[NazwaKontrahenta],
                'nip':           CAST(NULL AS NVARCHAR(20)),
                'document_type': COALESCE(fah.[StatusOpis], fah.[KOD_STATUSU]),
                'source_name':   N'fakir',
                'currency':      N'PLN',
                'amount_gross':  CAST(fah.[WARTOSC_BRUTTO] AS DECIMAL(18,2)),
                'amount_net':    CAST(fah.[WARTOSC_NETTO] AS DECIMAL(18,2))
                NULL ON NULL
            ),
            i.[document_title]  = COALESCE(
                i.[document_title], fah.[NazwaKontrahenta], fah.[NUMER],
                N'Dokument #' + i.[id_document]
            ),
            i.[document_amount] = COALESCE(i.[document_amount], fah.[WARTOSC_BRUTTO]),
            i.[updated_at]      = SYSUTCDATETIME()
        FROM [{SCHEMA}].[skw_document_approval_instances] i
        JOIN [{SCHEMA}].[skw_document_sources] ds
             ON ds.[id_source] = i.[id_source] AND ds.[source_name] = N'fakir'
        JOIN [{SCHEMA}].[skw_faktury_akceptacja_naglowek] fah
             ON fah.[KSEF_ID] = i.[id_document]
        WHERE i.[extra_data] IS NULL OR i.[extra_data] = N'{{}}'
    """))
    logger.info("[0069] Krok 1/3 — OK (zaktualizowano %s wierszy)", result.rowcount)

    # ── Krok 2/3: weryfikacja koncowa — ile zostalo bez extra_data ───────────
    logger.info("[0069] Krok 2/3 — weryfikacja stanu po backfillu")
    remaining = bind.execute(text(f"""
        SELECT COUNT(*)
        FROM [{SCHEMA}].[skw_document_approval_instances] i
        JOIN [{SCHEMA}].[skw_document_sources] ds
             ON ds.[id_source] = i.[id_source] AND ds.[source_name] = N'fakir'
        WHERE i.[extra_data] IS NULL OR i.[extra_data] = N'{{}}'
    """)).scalar()
    if remaining:
        logger.warning(
            "[0069] Krok 2/3 — UWAGA: %s wierszy 'fakir' NADAL ma pusta extra_data "
            "(brak odpowiadajacego KSEF_ID w skw_faktury_akceptacja_naglowek — "
            "prawdopodobnie retencja WAPRO). Lista do dalszej analizy, patrz "
            "log powyzej z Kroku 0. Te wiersze polegaja wylacznie na naprawie "
            "kodu (metoda 1 w duplicate_detection_service.py), nie na tej migracji.",
            remaining,
        )
    else:
        logger.info("[0069] Krok 2/3 — OK (wszystkie mozliwe wiersze naprawione)")

    logger.info("[0069] ZAKONCZONE")


def downgrade() -> None:
    # SWIADOMIE NIE-ODWRACALNE: backfill danych historycznych, nie zmiana
    # schematu. Cofniecie (ustawienie z powrotem NULL) usunieloby realne,
    # poprawne dane odtworzone z WAPRO bez zadnej korzysci — ryzyko wieksze
    # niz brak downgrade. Jesli kiedys potrzebny bedzie prawdziwy rollback,
    # wymaga to osobnej, swiadomej decyzji z pelna lista dotknietych
    # id_instance (dostepna w logach tej migracji przy upgrade()).
    logger.warning(
        "[0069] downgrade() jest celowo NO-OP — to backfill danych "
        "historycznych, nie zmiana schematu. Cofanie go usunieloby "
        "poprawne dane bez korzysci. Patrz docstring migracji."
    )