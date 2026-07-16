"""0054_widen_id_document

Poszerza dbo.skw_document_approval_instances.id_document z NVARCHAR(100)
do NVARCHAR(400).

=== KONTEKST (dlaczego to jest potrzebne) ===

id_document powstal jako synonim identyfikatora KSeF (decyzja F0.3,
Etap 1) — String(100) byl wymiarowany pod format KSEF_ID (NIP + data +
sufiks, ok. 30-40 znakow), z pewnym zapasem.

W Etapie 2 EmailAdapter (worker/services/email_adapter.py, UWAGA 2 w
naglowku pliku) zaczal budowac id_document jako
f"{message_id}::{nazwa_pliku}" dla zrodel typu 'email' — konkatenacja
IMAP Message-ID (typowo 40-60 znakow z nawiasami <>) i dowolnej nazwy
pliku zalacznika (bez gornego ograniczenia, obserwowane realne przypadki
> 100 znakow dla opisowych polskich nazw plikow z ERP-ow ksiegowych).
Nikt w tamtej sesji nie zweryfikowal tego zalozenia wzgledem fizycznego
rozmiaru kolumny — bo cala architektura pola do tej pory zakladala
milczaco "to zawsze wyglada jak KSEF_ID".

Efekt produkcyjny: pyodbc.DataError 22001 "String or binary data would
be truncated" przy kazdej probie synchronizacji zrodla email z dluzszym
Message-ID + nazwa pliku — dokument nigdy nie trafial do bazy, ginal
bez sladu poza logiem bledu source_sync_task.

=== ROZSTRZYGNIECIE WLASNE: dlaczego NVARCHAR(400), nie inna wartosc ===

400 zamiast np. "z zapasem 1000" celowo, z dwoch powodow:

1. Limit klucza indeksu. id_document jest czescia filtrowanego UNIQUE
   NONCLUSTERED INDEX UQ_skw_dai_active_document na (id_document,
   id_source) z migracji 0028. Maksymalny rozmiar klucza indeksu w SQL
   Server to 1700 bajtow (od SQL Server 2016, poziom zgodnosci >=130)
   lub 900 bajtow na starszych poziomach zgodnosci. NVARCHAR liczy 2
   bajty/znak, wiec NVARCHAR(400) = 800 bajtow + INT id_source (4 bajty)
   = 804 bajty — miesci sie z zapasem NAWET pod starszym, bardziej
   restrykcyjnym limitem 900B. NVARCHAR(1000) (1000*2=2000B) przekroczylby
   limit 1700B i migracja rzucalaby blad w trakcie CREATE INDEX gdyby
   trzeba go bylo przebudowac.
2. Realny margines. Najdluzszy zaobserwowany przypadek (Message-ID +
   "::" + polska nazwa pliku faktury) to ok. 120-140 znakow. 400 daje
   ~3x zapasu bez zblizania sie do twardego limitu indeksu.

Jesli w przyszlosci pojawi sie zrodlo generujace jeszcze dluzsze
identyfikatory — WLASCIWA odpowiedzia jest zmiana sposobu budowania
id_document w adapterze (np. hash o stalej dlugosci zamiast surowej
konkatenacji), NIE dalsze poszerzanie tej kolumny w kierunku limitu
indeksu. To osobna rekomendacja, poza zakresem tej migracji.

=== WERYFIKACJA ZALEZNOSCI (wykonana PRZED napisaniem tej migracji) ===

- UQ_skw_dai_active_document (filtrowany UNIQUE NONCLUSTERED INDEX):
  NIE wymaga DROP/CREATE — SQL Server pozwala poszerzac NVARCHAR bedacy
  czescia indeksu bez jego przebudowy, o ile nowy rozmiar miesci sie w
  limicie klucza (patrz wyzej — 804B, bezpieczne).
- FK_skw_dai_source, FK_skw_dai_path, FK_skw_dai_category,
  FK_skw_dai_dispatched_by: NIE dotycza id_document (inne kolumny).
- CK_skw_dai_status: NIE dotyczy id_document (pilnuje status).
- skw_v_approval_dispatch_queue, skw_v_approval_instance_detail,
  skw_v_approval_my_queue: JOIN na id_document, ale NIE sa
  WITH SCHEMABINDING w zrodle (migracja 0032) — migracja i tak
  weryfikuje to programowo w KROKU 01 zamiast zakladac, i PRZERYWA
  z czytelnym bledem jesli founder znajdzie schemabinding (zamiast
  cichego ALTER COLUMN failure z kryptycznym komunikatem SQL Server).
- Triggery: brak znalezionych triggerow na tej tabeli w bazie wiedzy
  projektu. Migracja nie modyfikuje danych, wiec nawet trigger typu
  AFTER INSERT/UPDATE nie powinien byc wywolany przez sam ALTER COLUMN.

Revision ID : 0054
Revises     : 0053
"""

import logging

from alembic import op
from sqlalchemy import text

revision = "0054"
down_revision = "0053"
branch_labels = None
depends_on = None

SCHEMA = "dbo"
TABLE = "skw_document_approval_instances"
COLUMN = "id_document"
OLD_LENGTH = 100
NEW_LENGTH = 400

logger = logging.getLogger(f"alembic.migration.{revision}")


def _log(krok: str, msg: str) -> None:
    logger.info("0054 [%s] %s", krok, msg)
    print(f"[0054-{krok}] {msg}")


def _execute(sql: str, params: dict | None = None):
    return op.get_bind().execute(text(sql), params or {})


# =============================================================================
# KROK 01 — weryfikacja schemabindingu widokow ZALEZNYCH od tej kolumny
# =============================================================================
# KRYTYCZNE: jesli ktorykolwiek z widokow uzywajacych id_document jest
# WITH SCHEMABINDING, ALTER COLUMN ponizej rzuci blad SQL Server ("object
# is dependent on column"). Zamiast pozwolic na kryptyczny blad w polowie
# migracji, sprawdzamy to jawnie i przerywamy z czytelnym komunikatem —
# administrator musi wtedy recznie DROP/CREATE dotknietych widokow w
# ramach tej samej migracji (nie robimy tego automatycznie, bo widoki
# moga miec zaleznosci ktorych nie znamy z tego poziomu).
def _krok01_sprawdz_schemabinding() -> None:
    _log("01", f"Sprawdzam schemabinding widokow zaleznych od {TABLE}.{COLUMN}")

    result = _execute(
        """
        SELECT v.name AS view_name
        FROM sys.views v
        JOIN sys.schemas s ON v.schema_id = s.schema_id
        WHERE s.name = :schema
          AND OBJECTPROPERTY(v.object_id, 'IsSchemaBound') = 1
          AND EXISTS (
              SELECT 1
              FROM sys.sql_expression_dependencies d
              WHERE d.referencing_id = v.object_id
                AND d.referenced_id = OBJECT_ID(:full_table_name)
          )
        """,
        {"schema": SCHEMA, "full_table_name": f"{SCHEMA}.{TABLE}"},
    )
    schemabound_views = [row[0] for row in result.fetchall()]

    if schemabound_views:
        raise RuntimeError(
            f"0054: znaleziono widoki WITH SCHEMABINDING zalezne od "
            f"{TABLE}: {schemabound_views}. ALTER COLUMN zostalby "
            f"zablokowany przez SQL Server. Wymagana reczna interwencja: "
            f"DROP tych widokow, ALTER COLUMN, CREATE OR ALTER widokow "
            f"z powrotem — w JEDNEJ migracji, zeby nie zostawic bazy "
            f"w niespojnym stanie miedzy krokami. Migracja przerwana "
            f"celowo, zero zmian wykonanych."
        )

    _log("01", "OK — zero widokow schemabound zaleznych od tej kolumny")


# =============================================================================
# KROK 02 — weryfikacja aktualnej dlugosci kolumny (idempotentnosc)
# =============================================================================
def _krok02_sprawdz_aktualna_dlugosc() -> int | None:
    result = _execute(
        """
        SELECT c.max_length / 2 AS max_chars  -- NVARCHAR: 2 bajty/znak
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = :schema AND t.name = :table AND c.name = :column
        """,
        {"schema": SCHEMA, "table": TABLE, "column": COLUMN},
    )
    row = result.fetchone()
    if row is None:
        raise RuntimeError(
            f"0054: kolumna {TABLE}.{COLUMN} nie istnieje — czy migracja "
            f"0028 zostala wczesniej zastosowana?"
        )
    return row[0]


# =============================================================================
# KROK 03 — ALTER COLUMN (idempotentny — pomija jesli juz poszerzone)
# =============================================================================
def _krok03_alter_column(current_length: int) -> None:
    if current_length >= NEW_LENGTH:
        _log(
            "03",
            f"Kolumna ma juz {current_length} znakow (>= {NEW_LENGTH}) — "
            f"pomijam ALTER COLUMN (migracja juz zastosowana lub kolumna "
            f"byla wczesniej poszerzona recznie).",
        )
        return

    _log("03", f"ALTER COLUMN {COLUMN}: NVARCHAR({current_length}) -> NVARCHAR({NEW_LENGTH})")
    _execute(
        f"""
        ALTER TABLE [{SCHEMA}].[{TABLE}]
            ALTER COLUMN [{COLUMN}] NVARCHAR({NEW_LENGTH}) NOT NULL
        """
    )
    _log("03", "OK")


# =============================================================================
# KROK 04 — weryfikacja koncowa
# =============================================================================
def _krok04_weryfikacja() -> None:
    new_length = _krok02_sprawdz_aktualna_dlugosc()
    if new_length != NEW_LENGTH:
        raise RuntimeError(
            f"0054: weryfikacja koncowa nieudana — kolumna ma {new_length} "
            f"znakow, oczekiwano {NEW_LENGTH}. Migracja NIE zostala "
            f"zastosowana poprawnie."
        )
    _log("04", f"Zweryfikowano: {TABLE}.{COLUMN} = NVARCHAR({new_length}) NOT NULL")

    # Weryfikacja poboczna — upewnij sie, ze filtrowany UNIQUE INDEX wciaz
    # istnieje i jest aktywny (SQL Server nie powinien go zepsuc przy
    # poszerzeniu NVARCHAR, ale sprawdzamy zamiast zakladac).
    result = _execute(
        """
        SELECT i.name, i.is_disabled
        FROM sys.indexes i
        JOIN sys.tables t ON i.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = :schema AND t.name = :table
          AND i.name = 'UQ_skw_dai_active_document'
        """,
        {"schema": SCHEMA, "table": TABLE},
    )
    row = result.fetchone()
    if row is None:
        raise RuntimeError(
            "0054: UQ_skw_dai_active_document zniknal po ALTER COLUMN — "
            "nieoczekiwane zachowanie SQL Server, wymaga natychmiastowej "
            "recznej weryfikacji przed kontynuacja."
        )
    if row[1]:  # is_disabled
        raise RuntimeError(
            "0054: UQ_skw_dai_active_document jest disabled po ALTER "
            "COLUMN — wymaga recznego REBUILD przed kontynuacja."
        )
    _log("04", "OK — UQ_skw_dai_active_document nienaruszony i aktywny")


def upgrade() -> None:
    logger.info("0054 upgrade — poszerzenie id_document NVARCHAR(100) -> NVARCHAR(400)")

    _krok01_sprawdz_schemabinding()
    current_length = _krok02_sprawdz_aktualna_dlugosc()
    _krok03_alter_column(current_length)
    _krok04_weryfikacja()

    logger.info("0054 upgrade ZAKONCZONE")


def downgrade() -> None:
    """
    UWAGA: downgrade do NVARCHAR(100) jest DESTRUKCYJNY, jesli w bazie
    istnieja juz wiersze z id_document > 100 znakow (a po tej migracji
    beda — to caly jej cel). SQL Server rzuci blad 8152 przy probie
    ALTER COLUMN na mniejszy rozmiar, jesli istnieja dane, ktore by sie
    nie zmiescily — co jest WLASCIWYM zachowaniem (odmowa cichej utraty
    danych). Nie probujemy tego obejsc automatycznym przycinaniem
    stringow — to wymagaloby decyzji biznesowej (ktore dane wazniejsze),
    nie powinno byc ukryte w downgrade migracji.
    """
    logger.warning(
        "0054 downgrade: NVARCHAR(400) -> NVARCHAR(100). Zawiedzie jesli "
        "istnieja wiersze z id_document > 100 znakow — to oczekiwane, "
        "SQL Server chroni przed cicha utrata danych. Rozwiaz recznie "
        "(usun/skroc dlugie rekordy) przed ponowna proba downgrade."
    )
    _execute(
        f"""
        ALTER TABLE [{SCHEMA}].[{TABLE}]
            ALTER COLUMN [{COLUMN}] NVARCHAR({OLD_LENGTH}) NOT NULL
        """
    )