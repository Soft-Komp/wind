# backend/alembic/versions/0060_source_type_add_ksef20.py
"""0060_source_type_add_ksef20

NAPRAWA 2026-07-17: CK_skw_ds_source_type (migracja 0039) nigdy nie zostal
rozszerzony o 'ksef20' — mimo ze model ORM (document_source.py::SOURCE_TYPES)
dopuszcza ta wartosc od czasu wdrozenia KSeF 2.0, z komentarzem twierdzacym
(blednie), ze jest "zsynchronizowane z CHECK constraintami w DB (migracja 0039)".

Efekt przed ta migracja: POST /admin/sources z source_type='ksef20' przechodzil
walidacje Pythona (validate()), ale INSERT padal na CHECK constraint w bazie —
IntegrityError zlapany w create_source() byl BLEDNIE zinterpretowany jako
konflikt source_name (catch-all except IntegrityError -> SourceNameConflictError,
bez sprawdzenia KTORY constraint sie nie zgodzil), co dawalo mylacy komunikat
"Zrodlo o nazwie '...' juz istnieje" przy zrodlach, ktore w ogole nie istnialy.

Ta migracja naprawia WYLACZNIE przyczyne zrodlowa (brakujaca wartosc w CHECK
constraint). Diagnostyczna poprawka w create_source()/update_source() (rozroznienie
typu bledu IntegrityError zamiast zgadywania) jest osobnym, jeszcze niewykonanym
zadaniem — patrz rozmowa robocza z 2026-07-17.

DROP + ADD (nie ALTER) — MSSQL nie pozwala modyfikowac istniejacego CHECK
constraint, trzeba go usunac i utworzyc na nowo z pelna, rozszerzona lista.
Bezpieczne dla istniejacych danych: nowa lista jest nadzbiorem starej
(dodajemy wartosc, nie usuwamy), wiec zaden istniejacy wiersz nie moze
naruszyc nowego constraintu.

Revision ID : 0060
Revises     : 0059
"""
from alembic import op
from sqlalchemy import text

revision = "0060"
down_revision = "0059"
branch_labels = None
depends_on = None

SCHEMA = "dbo"

# Pelna, rozszerzona lista — zgodna z document_source.py::SOURCE_TYPES
_SOURCE_TYPES = ("database", "api", "ftp", "email", "manual", "ksef20")


def upgrade() -> None:
    bind = op.get_bind()
    values_sql = ", ".join(f"N'{v}'" for v in _SOURCE_TYPES)

    # Krok 1: usun stary constraint (jesli istnieje)
    bind.execute(text(f"""
        IF EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'CK_skw_ds_source_type'
        )
        BEGIN
            ALTER TABLE [{SCHEMA}].[skw_document_sources]
                DROP CONSTRAINT [CK_skw_ds_source_type];
            PRINT N'[0060] Stary CK_skw_ds_source_type usuniety.';
        END
        ELSE
            PRINT N'[0060] CK_skw_ds_source_type nie istnial — tworze od nowa.';
    """))

    # Krok 2: utworz na nowo z pelna, rozszerzona lista (WITH NOCHECK — nie
    # wymusza retrospektywnie na istniejacych wierszach, choc i tak powinny
    # sie zgadzac, bo nowa lista jest nadzbiorem starej)
    bind.execute(text(f"""
        ALTER TABLE [{SCHEMA}].[skw_document_sources] WITH NOCHECK
            ADD CONSTRAINT [CK_skw_ds_source_type]
                CHECK ([source_type] IN ({values_sql}))
    """))
    print(f"[0060] CK_skw_ds_source_type utworzony na nowo z wartosciami: {_SOURCE_TYPES}")


def downgrade() -> None:
    bind = op.get_bind()
    old_values_sql = ", ".join(f"N'{v}'" for v in _SOURCE_TYPES if v != "ksef20")

    bind.execute(text(f"""
        IF EXISTS (
            SELECT 1 FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_sources]')
              AND name = N'CK_skw_ds_source_type'
        )
        ALTER TABLE [{SCHEMA}].[skw_document_sources]
            DROP CONSTRAINT [CK_skw_ds_source_type]
    """))

    # UWAGA: downgrade zawiedzie (IntegrityError), jesli istnieja juz wiersze
    # z source_type='ksef20' — to oczekiwane i poprawne zachowanie (nie da
    # sie bezpiecznie cofnac tego ograniczenia, gdy dane juz go naruszaja
    # wzgledem starej listy).
    bind.execute(text(f"""
        ALTER TABLE [{SCHEMA}].[skw_document_sources] WITH CHECK
            ADD CONSTRAINT [CK_skw_ds_source_type]
                CHECK ([source_type] IN ({old_values_sql}))
    """))