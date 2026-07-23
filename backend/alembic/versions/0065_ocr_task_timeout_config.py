# backend/alembic/versions/0065_ocr_task_timeout_config.py
"""0065_ocr_task_timeout_config

Dodaje klucz SystemConfig OCR_TASK_TIMEOUT_SECONDS — maksymalny czas
(w sekundach) na przetworzenie jednego dokumentu przez ocr_task.py
(worker/tasks/ocr_task.py::ocr_task). Po przekroczeniu limitu task zapisuje
ocr_error w extra_data zamiast wisiec na czas nieokreslony.

Kontekst (2026-07-23): zrodlo FTP qa_b_ftp dostarczylo pliki 0-bajtowe,
ktore doprowadzily do zadan OCR utkniętych na stale w statusie 'running'
w skw_ArqJobRegistry (brak finished_at, brak error_message). Naprawa w
ocr_task.py dodaje guard na pusty plik + asyncio.wait_for() z timeoutem
wokol extract_fields(). Ten klucz konfiguruje wartosc tego timeoutu.

Wartosc domyslna 120s dobrana jako bezpieczny margines ponad typowy czas
przetwarzania pojedynczego dokumentu (patrz przyklady z skw_ArqJobRegistry:
duration_ms od ~8s do ~298s dla udanych zadan — 120s jest kompromisem,
NIE pokrywa najwolniejszych zaobserwowanych przypadkow (296s, 298s).
Jesli po wdrozeniu poprawne, wolne OCR zaczna byc masowo ucinane timeoutem,
nalezy podniesc te wartosc przez panel admina, nie przez kolejna migracje —
to jest wlasnie cel trzymania tego w SystemConfig zamiast w kodzie).

NAPRAWA (2026-07-23): pierwotna wersja tej migracji zakladala kolumne
'Category' w skw_SystemConfig przez analogie do skw_Permissions — BLEDNIE.
Rzeczywisty schemat (zweryfikowany zapytaniem sys.columns po nieudanej
pierwszej probie na STOMIL) to wylacznie: ID_CONFIG (IDENTITY), ConfigKey,
ConfigValue, Description, IsActive, CreatedAt (NOT NULL, brak potwierdzonego
DEFAULT — ustawiane jawnie), UpdatedAt (nullable). Zadnej kolumny Category
nie ma — usunieta calkowicie z INSERT/MERGE ponizej.

WHEN NOT MATCHED -> INSERT ONLY. Bez WHEN MATCHED UPDATE — nie nadpisujemy
wartosci ewentualnie juz zmienionej przez admina, zgodnie ze standardowym
wzorcem seedowania w tym projekcie (patrz migracja 0039, krok 12).

Revision ID : 0065
Revises     : 0064
"""
import logging

from alembic import op
from sqlalchemy import text

logger = logging.getLogger(__name__)

revision = "0065"
down_revision = "0064"
branch_labels = None
depends_on = None

SCHEMA = "dbo"
_CONFIG_KEY = "OCR_TASK_TIMEOUT_SECONDS"
_CONFIG_VALUE = "120"
_CONFIG_DESCRIPTION = (
    "Maksymalny czas (sekundy) na przetworzenie jednego dokumentu przez "
    "ocr_task — po przekroczeniu zapisywany jest ocr_error zamiast "
    "wiszacego bezterminowo taska. Wprowadzone po incydencie z plikami "
    "0-bajtowymi ze zrodla FTP (2026-07-23)."
)


def upgrade() -> None:
    bind = op.get_bind()

    logger.info(
        "[0065] Info — skw_SystemConfig NIE MA kolumny 'Category' "
        "(potwierdzone zapytaniem sys.columns po nieudanej pierwszej "
        "probie tej migracji) — pomijam ja calkowicie w INSERT/MERGE."
    )

    # ── Krok 1/2: SEED klucza (INSERT-only MERGE, idempotentny) ─────────────
    # CreatedAt jest NOT NULL bez potwierdzonego DEFAULT constraint —
    # ustawiane jawnie na SYSUTCDATETIME() zamiast polegac na baze.
    logger.info("[0065] Krok 1/2 — MERGE skw_SystemConfig: %s = %s", _CONFIG_KEY, _CONFIG_VALUE)
    result = bind.execute(text(f"""
        MERGE [{SCHEMA}].[skw_SystemConfig] AS target
        USING (
            SELECT
                N'{_CONFIG_KEY}'         AS ConfigKey,
                N'{_CONFIG_VALUE}'       AS ConfigValue,
                N'{_CONFIG_DESCRIPTION}' AS Description
        ) AS source
        ON target.[ConfigKey] = source.[ConfigKey]
        WHEN NOT MATCHED THEN
            INSERT ([ConfigKey], [ConfigValue], [Description], [IsActive], [CreatedAt])
            VALUES (source.[ConfigKey], source.[ConfigValue], source.[Description], 1, SYSUTCDATETIME());
    """))
    logger.info("[0065] Krok 2/2 — OK (rowcount=%s; 0 = klucz juz istnial, pomijam nadpisanie)", result.rowcount)

    # ── Weryfikacja koncowa ───────────────────────────────────────────────────
    logger.info("[0065] Weryfikacja koncowa")
    final_row = bind.execute(text(f"""
        SELECT [ConfigKey], [ConfigValue], [IsActive], [CreatedAt]
        FROM [{SCHEMA}].[skw_SystemConfig]
        WHERE [ConfigKey] = N'{_CONFIG_KEY}'
    """)).fetchone()
    if final_row:
        logger.info(
            "[0065]   %s = %s | is_active=%s | created_at=%s",
            final_row[0], final_row[1], final_row[2], final_row[3],
        )
    else:
        logger.error("[0065] NIEOCZEKIWANE: klucz nie istnieje po MERGE")
    logger.info("[0065] ZAKONCZONE")


def downgrade() -> None:
    bind = op.get_bind()
    logger.info("[0065] downgrade — usuwam klucz %s", _CONFIG_KEY)
    bind.execute(text(f"""
        DELETE FROM [{SCHEMA}].[skw_SystemConfig]
        WHERE [ConfigKey] = N'{_CONFIG_KEY}'
    """))
    logger.info("[0065] downgrade — ZAKONCZONY")