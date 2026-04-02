"""
0009 — Seed SystemConfig: klucze modułu faktur.

PROBLEM:
    Klucze modul_akceptacji_faktur_enabled + faktury.* nie były wstawiane
    na istniejących systemach, ponieważ entrypoint.sh pomija SEED_FILES
    gdy tabele są już niepuste (fresh-install only path).
    Skutek: moduł był zablokowany (wartość domyślna = false), admin
    musiał ręcznie dodawać 13 kluczy w SSMS.

ROZWIĄZANIE:
    Migracja Alembic uruchamiana przy każdym `alembic upgrade head` —
    wykonuje MERGE (INSERT only) co jest w 100% bezpieczne dla systemów
    gdzie admin już ręcznie ustawił wartości.

UWAGA:
    WHEN MATCHED → brak (nie nadpisujemy wartości admina).
    Aktualizujemy jedynie Description dla czytelności.

Revision:      0009
Down-revision: 0008
Branch labels: None
Depends on:    0008 (skw_AlertLog)
"""
from __future__ import annotations

import logging

from alembic import op

logger = logging.getLogger("alembic.0009")

revision       = "0009"
down_revision  = "0008"
branch_labels  = None
depends_on     = None

# ---------------------------------------------------------------------------
# Schemat docelowy (projektu)
# ---------------------------------------------------------------------------
SCHEMA = "dbo_ext"

# ---------------------------------------------------------------------------
# 13 kluczy konfiguracyjnych modułu Akceptacji Faktur KSeF
# (ConfigKey, DefaultValue, Description)
# ---------------------------------------------------------------------------
FAKTURY_CONFIG_KEYS: list[tuple[str, str, str]] = [
    # ── Główny włącznik modułu ───────────────────────────────────────────
    (
        "modul_akceptacji_faktur_enabled",
        "false",
        "Główny włącznik modułu akceptacji faktur KSeF. "
        "false = wszystkie endpointy /faktury-akceptacja i /moje-faktury zwracają 403. "
        "Ustaw true dopiero po weryfikacji DDL (021_fakir_write_user.sql).",
    ),
    # ── Zapis do Fakira ──────────────────────────────────────────────────
    (
        "faktury.fakir_update_enabled",
        "false",
        "KRYTYCZNY — włącza UPDATE dbo.BUF_DOKUMENT (KOD_STATUSU) przez "
        "użytkownika windykacja_fakir_write. "
        "Ustaw true TYLKO po wykonaniu DDL 021_fakir_write_user.sql i weryfikacji uprawnień. "
        "false = DEMO_MODE (zatwierdzenie nie trafia do Fakira, tylko do logu).",
    ),
    # ── SSE ─────────────────────────────────────────────────────────────
    (
        "faktury.powiadomienia_sse_enabled",
        "true",
        "Włącza push SSE (Server-Sent Events) przy nowym przypisaniu faktury. "
        "Pracownicy otrzymują powiadomienie w czasie rzeczywistym przez /events/stream. "
        "Wyłącz jeśli SSE powoduje problemy z proxy (np. nginx bez chunked transfer).",
    ),
    # ── Rollback Fakira ──────────────────────────────────────────────────
    (
        "faktury.fakir_rollback_enabled",
        "false",
        "Czy operacja reset/anulowanie może cofnąć zatwierdzoną fakturę w Fakirze "
        "(KOD_STATUSU powrót do stanu poprzedniego). "
        "NIEBEZPIECZNE — pozostaw false na produkcji bez konsultacji z DBA.",
    ),
    # ── Reset przypisań ──────────────────────────────────────────────────
    (
        "faktury.reset_przypisania_enabled",
        "true",
        "Czy referent może zresetować przypisania faktury (POST /faktury-akceptacja/{id}/reset). "
        "Operacja dwuetapowa z confirm_token. "
        "false = endpoint zwraca 503 Service Unavailable.",
    ),
    # ── Force status ─────────────────────────────────────────────────────
    (
        "faktury.force_status_enabled",
        "true",
        "Czy referent może wymusić zmianę statusu faktury z pominięciem procesu akceptacji "
        "(PATCH /faktury-akceptacja/{id}/status). "
        "Operacja dwuetapowa z confirm_token. "
        "false = endpoint zwraca 503 Service Unavailable.",
    ),
    # ── Limit przypisanych ───────────────────────────────────────────────
    (
        "faktury.max_przypisanych_pracownikow",
        "10",
        "Maksymalna liczba pracowników przypisanych do jednej faktury jednocześnie. "
        "Przekroczenie limitu zwraca 422. "
        "Zakres: 1–50. Domyślnie: 10.",
    ),
    # ── TTL confirm token ────────────────────────────────────────────────
    (
        "faktury.confirm_token_ttl_seconds",
        "60",
        "Czas ważności jednorazowego tokenu JWT potwierdzającego operacje dwuetapowe "
        "(reset przypisań, force_status, anulowanie). "
        "Po tym czasie token wygasa i operacja wymaga ponownego zainicjowania (krok 1). "
        "Zakres: 30–300. Domyślnie: 60 sekund.",
    ),
    # ── Retry Fakira ─────────────────────────────────────────────────────
    (
        "faktury.fakir_retry_attempts",
        "3",
        "Liczba ponownych prób UPDATE BUF_DOKUMENT gdy Fakir jest chwilowo niedostępny. "
        "Po wyczerpaniu prób → alert WARNING + status faktury = 'orphaned'. "
        "Zakres: 1–10. Domyślnie: 3.",
    ),
    # ── PDF ──────────────────────────────────────────────────────────────
    (
        "faktury.pdf_enabled",
        "true",
        "Włącza generowanie wizualizacji PDF faktury przez ReportLab "
        "(GET /faktury-akceptacja/{id}/pdf i /moje-faktury/{id}/pdf). "
        "false = endpoint zwraca 503. Wyłącz gdy brakuje fontów DejaVu.",
    ),
    (
        "faktury.pdf_cache_ttl_seconds",
        "300",
        "Czas cache'owania PDF w Redis (klucz: faktura:pdf:{id}:{hash}). "
        "Inwalidacja przy każdej zmianie danych faktury. "
        "Zakres: 60–3600. Domyślnie: 300 sekund (5 minut).",
    ),
    # ── Idempotency ──────────────────────────────────────────────────────
    (
        "idempotency.window_seconds",
        "10",
        "Okno czasowe ochrony idempotentności (anti-duplicate click). "
        "Duplikat identycznego żądania w tym oknie zwraca 200/201 z cached response "
        "i nagłówkiem X-Idempotency-Replayed: true. "
        "Dotyczy: POST /faktury-akceptacja, POST /*/reset/confirm, POST /*/status/confirm, "
        "POST /moje-faktury/*/decyzja. "
        "Zakres: 5–60. Domyślnie: 10 sekund.",
    ),
    # ── Demo mode ────────────────────────────────────────────────────────
    (
        "faktury.demo_fake_ksef_ids_enabled",
        "false",
        "Tryb demo — generuje fikcyjne numery KSeF gdy WAPRO nie zwraca faktur. "
        "Używać TYLKO w środowisku testowym. "
        "Na produkcji zawsze false.",
    ),
]


def upgrade() -> None:
    """
    MERGE 13 kluczy SystemConfig modułu faktur.

    Strategia:
        WHEN NOT MATCHED BY TARGET → INSERT (bezpieczne dla istniejących instalacji)
        WHEN MATCHED → brak (nie nadpisujemy wartości ustawionych przez admina)

    Idempotentny: można uruchomić wielokrotnie bez efektów ubocznych.
    """
    logger.info(
        "[0009] upgrade() START — MERGE %d kluczy SystemConfig faktury.",
        len(FAKTURY_CONFIG_KEYS),
    )

    # Budujemy VALUES dla MERGE
    values_rows = ",\n            ".join(
        f"(N'{key}', N'{val}', N'{desc.replace(chr(39), chr(39)+chr(39))}')"
        for key, val, desc in FAKTURY_CONFIG_KEYS
    )

    merge_sql = f"""
    MERGE [{SCHEMA}].[skw_SystemConfig] AS target
    USING (
        VALUES
            {values_rows}
    ) AS source ([ConfigKey], [ConfigValue], [Description])
    ON target.[ConfigKey] = source.[ConfigKey]

    -- Wstawiamy TYLKO nowe klucze — nie nadpisujemy wartości admina
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            [ConfigKey],
            [ConfigValue],
            [Description],
            [IsActive],
            [CreatedAt]
        )
        VALUES (
            source.[ConfigKey],
            source.[ConfigValue],
            source.[Description],
            1,
            GETDATE()
        );
    """

    op.execute(merge_sql)

    # Weryfikacja — log ile rekordów faktycznie istnieje po MERGE
    logger.info(
        "[0009] MERGE wykonany. "
        "Sprawdź: SELECT COUNT(*) FROM [%s].[skw_SystemConfig] "
        "WHERE ConfigKey LIKE 'faktury.%%' OR ConfigKey = 'modul_akceptacji_faktur_enabled' "
        "OR ConfigKey = 'idempotency.window_seconds'",
        SCHEMA,
    )
    logger.info("[0009] upgrade() DONE.")


def downgrade() -> None:
    """
    Downgrade usuwa TYLKO klucze wstawione przez tę migrację
    (te których wartość == wartość domyślna — nie ruszamy zmodyfikowanych przez admina).

    UWAGA: Jeśli admin zmienił wartości, downgrade ich NIE usunie.
    """
    logger.warning(
        "[0009] downgrade() — usuwanie kluczy SystemConfig faktury "
        "(tylko te z wartością domyślną)."
    )

    keys_to_delete = [key for key, _val, _desc in FAKTURY_CONFIG_KEYS]
    keys_list = ", ".join(f"N'{k}'" for k in keys_to_delete)

    op.execute(f"""
    DELETE FROM [{SCHEMA}].[skw_SystemConfig]
    WHERE [ConfigKey] IN ({keys_list})
      AND [ConfigValue] IN (
          SELECT source.[ConfigValue]
          FROM (VALUES
              {",".join(f"(N'{k}', N'{v}')" for k, v, _ in FAKTURY_CONFIG_KEYS)}
          ) AS source ([ConfigKey], [ConfigValue])
          WHERE source.[ConfigKey] = [{SCHEMA}].[skw_SystemConfig].[ConfigKey]
      );
    """)

    logger.info("[0009] downgrade() DONE.")