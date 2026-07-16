"""0056_add_line_items_permission

Dodaje uprawnienie documents.view_line_items — kontroluje widocznosc
pozycji dokumentu (nowy mechanizm, 2026-07-15). Kategoria 'documents' —
zakladam, ze juz jest dozwolona w CK_skw_Permissions_Category, bo
documents.view/documents.view_all juz istnieja z tym prefiksem. Migracja
NIE przypisuje uprawnienia do zadnej roli automatycznie — to swiadoma
decyzja administratora przez panel, zgodnie z ostroznym wzorcem juz
stosowanym w tym projekcie dla nowych uprawnien.

Revision ID : 0056
Revises     : 0055
"""
from alembic import op
from sqlalchemy import text

revision = "0056"
down_revision = "0055"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()

    # Krok 1: weryfikacja, ze kategoria 'documents' jest dozwolona —
    # PRZED insertem, zeby dostac czytelny blad zamiast surowego
    # naruszenia CHECK constraint.
    result = bind.execute(text(
        "SELECT definition FROM sys.check_constraints "
        "WHERE name = 'CK_skw_Permissions_Category'"
    ))
    row = result.fetchone()
    if row and "documents" not in row[0]:
        raise RuntimeError(
            "0056: kategoria 'documents' NIE jest dozwolona w "
            "CK_skw_Permissions_Category. Migracja przerwana — wymaga "
            "najpierw rozszerzenia constraintu (ten sam wzorzec co przy "
            "innych nowych kategoriach uprawnien w tym projekcie)."
        )

    bind.execute(text("""
        MERGE [dbo].[skw_Permissions] AS target
        USING (SELECT
            N'documents.view_line_items' AS PermissionName,
            N'Podglad pozycji (linii) dokumentu — numery, ilosci, ceny per pozycja faktury' AS Description,
            N'documents' AS Category
        ) AS source
        ON target.[PermissionName] = source.PermissionName
        WHEN NOT MATCHED THEN
            INSERT ([PermissionName], [Description], [Category], [IsActive])
            VALUES (source.PermissionName, source.Description, source.Category, 1);
    """))


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(text(
        "DELETE FROM [dbo].[skw_Permissions] WHERE [PermissionName] = N'documents.view_line_items'"
    ))