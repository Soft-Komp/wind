# backend/alembic/versions/0074_document_folder_groups.py
"""0074_document_folder_groups

Rozszerza mechanizm teczek dokumentow (skw_document_folders, migracja 0039)
o widocznosc wielogrupowa — front zglosil (05.08.2026): obecny model
wymusza DOKLADNIE JEDNEGO wlasciciela (owner_user XOR owner_group,
patrz CHECK constraint na skw_document_folders i walidacja w
DocumentFolder.validate()). Front potrzebuje: teczka utworzona przez
jedna grupe (np. Zaopatrzenie) ma byc dodatkowo widoczna dla innych grup
(np. Ksiegowosc, Dzial Techniczny), bez zmiany wlasciciela.

DECYZJA PRODUKTOWA (potwierdzona przez front, 05.08.2026):
  - owner_user/owner_group NA SKW_DOCUMENT_FOLDERS POZOSTAJE BEZ ZMIAN —
    to wciaz jeden, jednoznaczny wlasciciel/tworca teczki (audytowo).
  - Nowa tabela ponizej to WYLACZNIE dodatkowa widocznosc (odczyt) dla
    grup spoza wlasciciela — analogiczny wzorzec do juz istniejacego
    skw_approval_filter_visibility (widocznosc filtrow per grupa/user).
  - Modyfikacja zawartosci teczki (dodanie/usuniecie dokumentu) przez
    grupe dostepna WYLACZNIE przez ta nowa tabele (nie bedaca wlascicielem)
    wymaga NOWEGO, ODDZIELNEGO uprawnienia 'documents.assign_shared_folder' —
    explicite oddzielonego od istniejacych documents.assign_own_folder /
    documents.assign_team_folder (te dwa pozostaja bez zmian znaczeniowych,
    dotycza wylacznie sciezki dostepu przez wlasciciela).

Tabela jest PLASKA (bez kolumny poziomu dostepu typu read/write) — front
jawnie zdecydowal, ze prawo modyfikacji zawartosci jest kontrolowane
WYLACZNIE przez osobne uprawnienie, nie przez atrybut na wierszu
przypisania grupy do teczki. Jesli w przyszlosci pojawi sie potrzeba
zroznicowania per-grupa (jedna grupa read-only, inna z prawem edycji) —
to osobna, swiadoma zmiana schematu, nie objeta ta migracja.

Revision ID : 0074
Revises     : 0073
"""
import logging

from alembic import op
from sqlalchemy import text

logger = logging.getLogger(__name__)

revision = "0074"
down_revision = "0073"
branch_labels = None
depends_on = None

SCHEMA = "dbo"
TABLE = "skw_document_folder_groups"

_NEW_PERMISSION = "documents.assign_shared_folder"
# NAPRAWA (06.08.2026, incydent przy wdrozeniu na GPGKJASLO): pierwsza wersja
# tego opisu (ponad 350 znakow) przekraczala limit dlugosci kolumny
# skw_Permissions.Description, co powodowalo blad 8152 "String or binary
# data would be truncated" i zatrzymywalo cala migracje. Skrocony do
# dlugosci zgodnej z pozostalymi opisami uprawnien w tym projekcie
# (patrz migracje 0059, 0063 — rzedu 100-200 znakow).
_PERMISSION_DESCRIPTION = (
    "Dodawanie/usuwanie dokumentow z teczki dostepnej WYLACZNIE przez grupe "
    "wspoldzielona (skw_document_folder_groups), nie przez wlasciciela. "
    "Migracja 0074, wniosek frontu 05.08.2026."
)


def upgrade() -> None:
    bind = op.get_bind()

    # ── Krok 1/3: tabela skw_document_folder_groups ─────────────────────────
    logger.info("[0074] Krok 1/3 — tworze tabele %s", TABLE)
    bind.execute(text(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables
            WHERE schema_id = SCHEMA_ID(N'{SCHEMA}') AND name = N'{TABLE}'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[{TABLE}] (
                [id_folder_group] INT IDENTITY(1,1) NOT NULL,
                [id_folder]       INT NOT NULL,
                [id_group]        INT NOT NULL,
                [added_by]        INT NULL,
                [added_at]        DATETIME NOT NULL CONSTRAINT [DF_{TABLE}_added_at] DEFAULT (SYSUTCDATETIME()),
                CONSTRAINT [PK_{TABLE}] PRIMARY KEY CLUSTERED ([id_folder_group]),
                CONSTRAINT [FK_{TABLE}_folder] FOREIGN KEY ([id_folder])
                    REFERENCES [{SCHEMA}].[skw_document_folders] ([id_folder])
                    ON DELETE CASCADE,
                CONSTRAINT [FK_{TABLE}_group] FOREIGN KEY ([id_group])
                    REFERENCES [{SCHEMA}].[skw_approval_groups] ([id_group])
                    ON DELETE NO ACTION,
                CONSTRAINT [FK_{TABLE}_added_by] FOREIGN KEY ([added_by])
                    REFERENCES [{SCHEMA}].[skw_Users] ([ID_USER])
                    ON DELETE NO ACTION,
                CONSTRAINT [UQ_{TABLE}_folder_group] UNIQUE ([id_folder], [id_group])
            );
            PRINT N'[0074] Tabela {TABLE} utworzona.';
        END
        ELSE
            PRINT N'[0074] Tabela {TABLE} juz istnieje — pomijam.';
    """))

    # ── Krok 2/3: indeks wspierajacy JOIN po id_group (odwrotny kierunek —
    #    "ktore teczki widzi ta grupa") ────────────────────────────────────
    logger.info("[0074] Krok 2/3 — indeks IX_%s_id_group", TABLE)
    bind.execute(text(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE}]')
              AND name = N'IX_{TABLE}_id_group'
        )
        BEGIN
            CREATE NONCLUSTERED INDEX [IX_{TABLE}_id_group]
                ON [{SCHEMA}].[{TABLE}] ([id_group]);
            PRINT N'[0074] Indeks IX_{TABLE}_id_group utworzony.';
        END
        ELSE
            PRINT N'[0074] Indeks IX_{TABLE}_id_group juz istnieje — pomijam.';
    """))

    # ── Krok 3/3: seed nowego uprawnienia (INSERT-only MERGE) ───────────────
    logger.info("[0074] Krok 3/3 — MERGE skw_Permissions: %s", _NEW_PERMISSION)
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
        "[0074] Krok 3/3 — OK (rowcount=%s; 0 = uprawnienie juz istnialo, idempotentnie)",
        result.rowcount,
    )
    logger.info("[0074] ZAKONCZONE")


def downgrade() -> None:
    bind = op.get_bind()

    logger.info("[0074] downgrade — usuwam uprawnienie %s (i przypisania do rol)", _NEW_PERMISSION)
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

    logger.info("[0074] downgrade — usuwam indeks i tabele %s", TABLE)
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[{TABLE}]') AND name = N'IX_{TABLE}_id_group')
            DROP INDEX [IX_{TABLE}_id_group] ON [{SCHEMA}].[{TABLE}]
    """))
    bind.execute(text(f"""
        IF EXISTS (SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID(N'{SCHEMA}') AND name = N'{TABLE}')
            DROP TABLE [{SCHEMA}].[{TABLE}]
    """))
    logger.info("[0074] downgrade — ZAKONCZONY")