# backend/alembic/versions/0063_documents_view_list_permission.py
"""0063_documents_view_list_permission

Rozdziela uprawnienie 'documents.view' na dwa niezalezne poziomy
granularnosci, na wniosek frontu (2026-07-22):

  documents.view_list — WYLACZNIE dostep do globalnej listy dokumentow
                         (GET /documents, GET /documents/unassigned)
  documents.view      — POZOSTAJE dla szczegolow pojedynczego dokumentu
                         (status-summary, actions/available, timeline)

Cel biznesowy: uzytkownik moze otworzyc znany dokument w
InstanceDetailDrawer (np. z linku/powiadomienia), ale nie moze
przegladac calej globalnej listy dokumentow.

DECYZJA PODJETA JAWNIE (nie domyslna): migracja nadaje documents.view_list
WSZYSTKIM rolom ktore w momencie uruchomienia migracji maja juz
documents.view. Zapobiega to utracie dotychczasowego dostepu do listy
zaraz po wdrozeniu. Ograniczenie dostepu wybranym rolom (np. samo
odebranie documents.view_list kierownikowi) to OSOBNA, swiadoma
operacja administracyjna PO tej migracji — NIE jest tu wykonywana.

Kategoria 'documents' jest juz dozwolona w CK_skw_Permissions_Category
(potwierdzone: documents.view / documents.view_all / documents.view_line_items
juz istnieja z tym prefiksem od migracji 0039/0056) — brak potrzeby ALTER TABLE.

Revision ID : 0063
Revises     : 0062
"""
import logging

from alembic import op
from sqlalchemy import text

logger = logging.getLogger(__name__)

revision = "0063"
down_revision = "0062"
branch_labels = None
depends_on = None

SCHEMA = "dbo"
_NEW_PERMISSION = "documents.view_list"
_SOURCE_PERMISSION = "documents.view"


def upgrade() -> None:
    bind = op.get_bind()

    # ── Krok 0: weryfikacja, ze kategoria 'documents' jest dozwolona ─────────
    # (ten sam wzorzec obronny co w migracji 0056 — czytelny blad zamiast
    # surowego naruszenia CHECK constraint)
    logger.info("[0063] Krok 0/3 — weryfikacja CK_skw_Permissions_Category")
    row = bind.execute(text(
        "SELECT definition FROM sys.check_constraints "
        "WHERE name = 'CK_skw_Permissions_Category'"
    )).fetchone()
    if row and "documents" not in row[0]:
        raise RuntimeError(
            "0063: kategoria 'documents' NIE jest dozwolona w "
            "CK_skw_Permissions_Category. Migracja przerwana."
        )
    logger.info("[0063] Krok 0/3 — OK (kategoria 'documents' dozwolona)")

    # ── Krok 1: SEED nowego uprawnienia (INSERT-only MERGE, idempotentne) ────
    logger.info("[0063] Krok 1/3 — MERGE skw_Permissions: %s", _NEW_PERMISSION)
    result = bind.execute(text(f"""
        MERGE [{SCHEMA}].[skw_Permissions] AS target
        USING (
            SELECT
                N'{_NEW_PERMISSION}' AS PermissionName,
                N'Dostep do globalnej listy dokumentow (GET /documents, GET /documents/unassigned). '
                + N'Rozdzielone od documents.view (szczegoly pojedynczego dokumentu) na wniosek frontu 2026-07-22.' AS Description,
                N'documents' AS Category
        ) AS source
        ON target.[PermissionName] = source.PermissionName
        WHEN NOT MATCHED THEN
            INSERT ([PermissionName], [Description], [Category], [IsActive])
            VALUES (source.PermissionName, source.Description, source.Category, 1);
    """))
    logger.info("[0063] Krok 1/3 — OK (rowcount=%s; 0 = juz istnialo, idempotentnie)", result.rowcount)

    # ── Krok 2: przypisanie documents.view_list KAZDEJ roli, ─────────────────
    # ktora obecnie ma documents.view (zapobiega utracie dostepu po wdrozeniu)
    logger.info("[0063] Krok 2/3 — przypisanie %s wszystkim rolom majacym %s",
                _NEW_PERMISSION, _SOURCE_PERMISSION)
    result = bind.execute(text(f"""
        INSERT INTO [{SCHEMA}].[skw_RolePermissions] ([ID_ROLE], [ID_PERMISSION])
        SELECT DISTINCT rp_old.[ID_ROLE], p_new.[ID_PERMISSION]
        FROM [{SCHEMA}].[skw_RolePermissions] rp_old
        JOIN [{SCHEMA}].[skw_Permissions] p_old
             ON p_old.[ID_PERMISSION] = rp_old.[ID_PERMISSION]
            AND p_old.[PermissionName] = N'{_SOURCE_PERMISSION}'
        CROSS JOIN (
            SELECT [ID_PERMISSION]
            FROM [{SCHEMA}].[skw_Permissions]
            WHERE [PermissionName] = N'{_NEW_PERMISSION}'
        ) p_new
        WHERE NOT EXISTS (
            SELECT 1 FROM [{SCHEMA}].[skw_RolePermissions] rp_check
            WHERE rp_check.[ID_ROLE] = rp_old.[ID_ROLE]
              AND rp_check.[ID_PERMISSION] = p_new.[ID_PERMISSION]
        );
    """))
    logger.info("[0063] Krok 2/3 — OK (przypisano %s nowych wpisow ID_ROLE x ID_PERMISSION)",
                result.rowcount)

    # ── Krok 3: log stanu koncowego (audytowalnosc — ktore role dostaly co) ──
    logger.info("[0063] Krok 3/3 — weryfikacja koncowa")
    rows = bind.execute(text(f"""
        SELECT r.[RoleName], p.[PermissionName]
        FROM [{SCHEMA}].[skw_RolePermissions] rp
        JOIN [{SCHEMA}].[skw_Roles] r ON r.[ID_ROLE] = rp.[ID_ROLE]
        JOIN [{SCHEMA}].[skw_Permissions] p ON p.[ID_PERMISSION] = rp.[ID_PERMISSION]
        WHERE p.[PermissionName] IN (N'{_SOURCE_PERMISSION}', N'{_NEW_PERMISSION}')
        ORDER BY r.[RoleName], p.[PermissionName];
    """)).fetchall()
    for role_name, perm_name in rows:
        logger.info("[0063]   %-30s -> %s", role_name, perm_name)
    logger.info("[0063] Krok 3/3 — OK (%d wierszy zweryfikowanych)", len(rows))


def downgrade() -> None:
    bind = op.get_bind()

    logger.info("[0063] downgrade — usuwam przypisania roli dla %s", _NEW_PERMISSION)
    bind.execute(text(f"""
        DELETE rp
        FROM [{SCHEMA}].[skw_RolePermissions] rp
        JOIN [{SCHEMA}].[skw_Permissions] p ON p.[ID_PERMISSION] = rp.[ID_PERMISSION]
        WHERE p.[PermissionName] = N'{_NEW_PERMISSION}';
    """))

    logger.info("[0063] downgrade — usuwam uprawnienie %s", _NEW_PERMISSION)
    bind.execute(text(f"""
        DELETE FROM [{SCHEMA}].[skw_Permissions]
        WHERE [PermissionName] = N'{_NEW_PERMISSION}';
    """))
    logger.info("[0063] downgrade — ZAKONCZONY")