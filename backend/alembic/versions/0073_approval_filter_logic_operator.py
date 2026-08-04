# backend/alembic/versions/0073_approval_filter_logic_operator.py
"""0073_approval_filter_logic_operator

Dodaje kolumne logic_operator do skw_approval_filters - pozwala administratorowi
wybrac, czy warunki filtra typu 'standard' maja byc laczone logika AND (wszystkie
musza byc spelnione - zachowanie dzisiejsze) czy OR (wystarczy jeden spelniony
warunek).

--------------------------------------------------------------------------
KONTEKST:
--------------------------------------------------------------------------
Zgloszenie z frontu: potrzeba filtrowania w filtrach automatycznych dodatkowo
po OR. Przyjety model (po analizie z wlascicielem projektu, 2026-07-31):
JEDEN operator globalny na caly filtr - bez grup, bez mieszania AND/OR
w obrebie jednego filtra, bez zagniezdzania nawiasami.

Dotyczy WYLACZNIE filtrow filter_type='standard'. Filtry typu 'universal'
(wywolanie funkcji SQL, patrz _evaluate_universal_filter w filter_engine.py)
nie maja warunkow w ogole i ignoruja te kolumne - kod ewaluacji nie odczytuje
jej dla tego typu filtra.

Zmiana NIE dotyczy petli resolve_path() (kolejnosc po priority, last-match-wins)
- ta petla juz dzis sprawdza kazdy aktywny filtr niezaleznie od wczesniejszych
dopasowan, wiec dodanie trybu AND/OR wewnatrz jednego filtra nie wymaga zadnej
zmiany w samej petli, wylacznie w _evaluate_standard_filter().

--------------------------------------------------------------------------
DEFAULT KOLUMNY - UWAGA, PUNKT WYMAGAJACY POTWIERDZENIA:
--------------------------------------------------------------------------
Wartosc ponizej (_DEFAULT_VALUE = "AND") to REKOMENDACJA INZYNIERSKA, nie
ostatecznie potwierdzona przez wlasciciela projektu decyzja. W toku ustalen
(2026-07-31) pojawila sie tez propozycja DEFAULT='OR', ktora nastepnie
zostala okreslona przez wlasciciela projektu jako "wstawka z frontu"
(nieautoryzowana, niebedaca jego faktyczna decyzja).

Uzasadnienie rekomendacji 'AND': zero zmiany zachowania dzisiaj dzialajacych
filtrow na produkcji (GPGKJASLO) w momencie wykonania ALTER - administrator
musi SWIADOMIE przelaczyc konkretny filtr na OR, zeby zmienic jego dzialanie.
Formularz tworzenia NOWEGO filtra po stronie API/frontu moze niezaleznie
podpowiadac 'OR' jako wartosc poczatkowa w UI - to decyzja UX na poziomie
Pydantic/frontu, BEZ zwiazku z DEFAULT tej kolumny SQL, i nie jest tu
implementowana.

JESLI wlasciciel projektu jawnie potwierdzi INNA wartosc DEFAULT niz 'AND':
  - jesli migracja NIE byla jeszcze zastosowana na zadnej instancji -
    wystarczy zmienic literal _DEFAULT_VALUE ponizej i wdrozyc ponownie.
  - jesli migracja JUZ byla zastosowana (STOMIL i/lub GPGKJASLO) - wymagana
    jest NOWA migracja korygujaca (analogicznie do wzorca 0071 -> 0072),
    NIE recznie edytowany plik 0073 po fakcie.

--------------------------------------------------------------------------
PUSTY FILTR (0 warunkow) - zachowanie docelowe w filter_engine.py:
--------------------------------------------------------------------------
    - tryb AND: bez zmian - catch-all (zawsze pasuje), zgodnie z dzisiejsza
      logika _evaluate_standard_filter().
    - tryb OR: NIGDY nie pasuje (pusta alternatywa logiczna = falsz).
Ta migracja to WYLACZNIE DDL (kolumna + constraint) - zmiana logiki
ewaluacji w filter_engine.py / worker/services/filter_engine.py to osobny,
kolejny krok dostawy (etapami, zgodnie z ustaleniem), NIE czesc tego pliku.

--------------------------------------------------------------------------
SchemaChecksums - NIE DOTYCZY tej migracji:
--------------------------------------------------------------------------
Mechanizm skw_SchemaChecksums (patrz _checksum_merge w migracjach 0061,
0062, 0066, 0067, 0071, 0072) liczy checksum WYLACZNIE z definicji WIDOKOW
poprzez sys.sql_modules. skw_approval_filters jest zwykla TABELA - nie ma
wpisu w sys.sql_modules. Brak kroku MERGE w tej migracji jest celowy,
zweryfikowany na podstawie tresci pliku 0072_fix_naglowek_regression_and_nip.py,
nie jest przeoczeniem.

Revision ID : 0073
Revises     : 0072
"""
from alembic import op

revision = "0073"
down_revision = "0072"
branch_labels = None
depends_on = None

SCHEMA = "dbo"
TABLE = "skw_approval_filters"
COLUMN = "logic_operator"
CHECK_NAME = "CHK_saf_logic_operator"
DEFAULT_NAME = "DF_saf_logic_operator"

# ── PENDING POTWIERDZENIA WLASCICIELA PROJEKTU (patrz docstring wyzej) ─────
# Rekomendacja: 'AND'. Zmienic TYLKO po jawnym, jednoznacznym potwierdzeniu
# (nie po komunikacie, ktory moze byc kolejna "wstawka z frontu").
_DEFAULT_VALUE = "AND"


def upgrade() -> None:
    # KROK 1 - dodanie kolumny z DEFAULT.
    # Konwencja projektu: ADD COLUMN i ADD CONSTRAINT w oddzielnych
    # wywolaniach op.execute() (nie w jednej instrukcji ALTER TABLE).
    op.execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE}]')
              AND name = N'{COLUMN}'
        )
        BEGIN
            ALTER TABLE [{SCHEMA}].[{TABLE}]
                ADD [{COLUMN}] NVARCHAR(3)
                    CONSTRAINT [{DEFAULT_NAME}] DEFAULT (N'{_DEFAULT_VALUE}')
                    NOT NULL;
        END
    """)

    # KROK 2 - CHECK constraint ograniczajacy kolumne do dokladnie dwoch
    # dopuszczalnych wartosci. Osobny krok - zgodnie z konwencja projektu.
    op.execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE name = N'{CHECK_NAME}'
              AND parent_object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE}]')
        )
        BEGIN
            ALTER TABLE [{SCHEMA}].[{TABLE}]
                ADD CONSTRAINT [{CHECK_NAME}]
                CHECK ([{COLUMN}] IN (N'AND', N'OR'));
        END
    """)


def downgrade() -> None:
    # Kolejnosc odwrotna wzgledem upgrade(): najpierw CHECK, potem
    # DEFAULT constraint, na koncu sama kolumna.
    op.execute(f"""
        IF EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE name = N'{CHECK_NAME}'
              AND parent_object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE}]')
        )
        BEGIN
            ALTER TABLE [{SCHEMA}].[{TABLE}] DROP CONSTRAINT [{CHECK_NAME}];
        END
    """)

    op.execute(f"""
        IF EXISTS (
            SELECT 1 FROM sys.default_constraints
            WHERE name = N'{DEFAULT_NAME}'
              AND parent_object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE}]')
        )
        BEGIN
            ALTER TABLE [{SCHEMA}].[{TABLE}] DROP CONSTRAINT [{DEFAULT_NAME}];
        END
    """)

    op.execute(f"""
        IF EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE}]')
              AND name = N'{COLUMN}'
        )
        BEGIN
            ALTER TABLE [{SCHEMA}].[{TABLE}] DROP COLUMN [{COLUMN}];
        END
    """)