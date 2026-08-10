# backend/alembic/versions/0075_folder_admin_override_permission.py
"""0075_folder_admin_override_permission

Dodaje uprawnienie documents.manage_all_folders — pelne obejscie
sprawdzania czlonkostwa w grupie dla WSZYSTKICH operacji na teczkach
zespolowych (create/read/update/delete/dodanie-usuniecie dokumentu/
zarzadzanie grupami wspoldzielonymi).

KONTEKST (wrzutka z frontu, 06.08.2026): "Teczki tworzy administrator i
nie musi on byc czescia grupy, zeby tworzyc teczke zespolowa dla
konkretnych grup." Front potwierdzil (06.08.2026): to ma byc PELNE
obejscie, nie tylko dla create/delete.

DECYZJA (rozmowa robocza 06.08.2026): "administrator" w tym systemie to
NIEOKRESLONY zbior — ktos moze byc administratorem "z nazwy" (RoleName=
'Admin' w skw_Roles) albo "z uprawnien" (rola o innej nazwie, ktorej
recznie przypisano odpowiednie uprawnienia w skw_RolePermissions — ta
tabela jest edytowalna niezaleznie od nazwy roli). To nowe uprawnienie
MUSI dzialac dla obu przypadkow — stad sprawdzane WYLACZNIE przez
skw_RolePermissions, NIGDY przez RoleName='Admin' (ten drugi wzorzec
istnieje gdzie indziej w projekcie — worker/tasks/auto_dispatch_task.py::
_get_admin_user_ids() — ale to jest odosobniony wyjatek do wewnetrznego
uzytku workera, nie konwencja tego projektu do powielania przy nowych
uprawnieniach widocznych przez API).

NIE nadaje tego uprawnienia domyslnie zadnej roli (w tym Admin) —
w przeciwienstwie do migracji 0063 (documents.view_list), gdzie
automatyczne dziedziczenie po istniejacym uprawnieniu bylo uzasadnione
zapobieganiem utracie juz posiadanego dostepu. Tutaj nie ma analogicznego
"istniejacego dostepu" do zachowania — to jest NOWA, dodatkowa zdolnosc,
ktora front chce przypisywac RECZNIE (potwierdzone jawnie w rozmowie
roboczej: "Nowe recznie przypisywane uprawnienie").

Revision ID : 0075
Revises     : 0074
"""
import logging

from alembic import op
from sqlalchemy import text

logger = logging.getLogger(__name__)

revision = "0075"
down_revision = "0074"
branch_labels = None
depends_on = None

SCHEMA = "dbo"

_NEW_PERMISSION = "documents.manage_all_folders"
_PERMISSION_DESCRIPTION = (
    "Pelne obejscie sprawdzania czlonkostwa w grupie dla operacji na "
    "teczkach zespolowych (create/read/update/delete/dokumenty/grupy). "
    "Recznie przypisywane. Migracja 0075, wniosek frontu 06.08.2026."
)


def upgrade() -> None:
    bind = op.get_bind()

    logger.info("[0075] Krok 1/1 — MERGE skw_Permissions: %s", _NEW_PERMISSION)
    result = bind.execute(text(f"""
        MERGE [{SCHEMA}].[skw_Permissions] AS target
        USING (
            SELECT
                N'{_NEW_PERMISSION}' AS PermissionName,
                N'{_PERMISSION_DESCRIPTION}' AS Description,
                N'documents' AS Category
        ) AS source
        ON target.[PermissionName] = source.PermissionName
        WHEN NOT MATCHED THEN
            INSERT ([PermissionName], [Description], [Category], [IsActive])
            VALUES (source.PermissionName, source.Description, source.Category, 1);
    """))
    logger.info(
        "[0075] Krok 1/1 — OK (rowcount=%s; 0 = uprawnienie juz istnialo, idempotentnie). "
        "SWIADOMIE nie przypisano do zadnej roli — front przypisuje recznie.",
        result.rowcount,
    )
    logger.info("[0075] ZAKONCZONE")


def downgrade() -> None:
    bind = op.get_bind()

    logger.info("[0075] downgrade — usuwam uprawnienie %s (i przypisania do rol)", _NEW_PERMISSION)
    bind.execute(text(f"""
        DELETE rp
        FROM [{SCHEMA}].[skw_RolePermissions] rp
        JOIN [{SCHEMA}].[skw_Permissions] p ON p.[ID_PERMISSION] = rp.[ID_PERMISSION]
        WHERE p.[PermissionName] = N'{_NEW_PERMISSION}';
    """))
    bind.execute(text(f"""
        DELETE FROM [{SCHEMA}].[skw_Permissions]
        WHERE [PermissionName] = N'{_NEW_PERMISSION}';
    """))
    logger.info("[0075] downgrade — ZAKONCZONY")