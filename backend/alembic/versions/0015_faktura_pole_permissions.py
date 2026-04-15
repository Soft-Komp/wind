"""0015_faktura_pole_permissions

Dodaje 21 granularnych uprawnień faktury.pole.* dla endpointu
GET /faktury-akceptacja/ksef/{ksef_id}.

Logika: whitelist — pole zwracane tylko gdy rola MA uprawnienie.
Zawsze widoczne (bez uprawnień): numer_ksef, numer,
id_buf_dokument, numer_pozycji.

Pola nagłówka (12):
  status_wewnetrzny, priorytet, data_wystawienia, data_otrzymania,
  termin_platnosci, wartosc_netto, wartosc_brutto, kwota_vat,
  forma_platnosci, nazwa_kontrahenta, email_kontrahenta, telefon_kontrahenta

Pola pozycji (9):
  nazwa_towaru, ilosc, jednostka, cena_netto, cena_brutto,
  pozycja_wartosc_netto, pozycja_wartosc_brutto, stawka_vat, opis

Revision ID: 0015
Revises:     0014
Create Date: 2026-04-14
"""

from __future__ import annotations

import logging
import textwrap
from typing import Final

from alembic import op

revision:      str = "0015"
down_revision: str = "0014"
branch_labels       = None
depends_on          = None

logger = logging.getLogger(f"alembic.migration.{revision}")

# ---------------------------------------------------------------------------
# 21 uprawnień faktury.pole.*
# ---------------------------------------------------------------------------

_PERMISSIONS: Final[list[tuple[str, str]]] = [
    # Nagłówek (12)
    ("faktury.pole.status_wewnetrzny",  "Widoczność pola: status wewnętrzny faktury"),
    ("faktury.pole.priorytet",          "Widoczność pola: priorytet faktury"),
    ("faktury.pole.data_wystawienia",   "Widoczność pola: data wystawienia (WAPRO)"),
    ("faktury.pole.data_otrzymania",    "Widoczność pola: data otrzymania (WAPRO)"),
    ("faktury.pole.termin_platnosci",   "Widoczność pola: termin płatności (WAPRO)"),
    ("faktury.pole.wartosc_netto",      "Widoczność pola: wartość netto nagłówka"),
    ("faktury.pole.wartosc_brutto",     "Widoczność pola: wartość brutto nagłówka"),
    ("faktury.pole.kwota_vat",          "Widoczność pola: kwota VAT nagłówka"),
    ("faktury.pole.forma_platnosci",    "Widoczność pola: forma płatności"),
    ("faktury.pole.nazwa_kontrahenta",  "Widoczność pola: nazwa kontrahenta"),
    ("faktury.pole.email_kontrahenta",  "Widoczność pola: email kontrahenta (RODO)"),
    ("faktury.pole.telefon_kontrahenta","Widoczność pola: telefon kontrahenta (RODO)"),
    # Pozycje (9)
    ("faktury.pole.nazwa_towaru",           "Widoczność pola: nazwa towaru/usługi"),
    ("faktury.pole.ilosc",                  "Widoczność pola: ilość na pozycji"),
    ("faktury.pole.jednostka",              "Widoczność pola: jednostka miary"),
    ("faktury.pole.cena_netto",             "Widoczność pola: cena jednostkowa netto"),
    ("faktury.pole.cena_brutto",            "Widoczność pola: cena jednostkowa brutto"),
    ("faktury.pole.pozycja_wartosc_netto",  "Widoczność pola: wartość netto pozycji"),
    ("faktury.pole.pozycja_wartosc_brutto", "Widoczność pola: wartość brutto pozycji"),
    ("faktury.pole.stawka_vat",             "Widoczność pola: stawka VAT pozycji"),
    ("faktury.pole.opis",                   "Widoczność pola: opis pozycji"),
]

_ROLE_NAMES: Final[list[str]] = ["Admin", "Manager", "User", "ReadOnly"]


def upgrade() -> None:
    logger.info("[%s] UPGRADE → wstawiam 21 uprawnień faktury.pole.*", revision)

    # Krok 1: MERGE uprawnień
    for perm_name, description in _PERMISSIONS:
        op.execute(textwrap.dedent(f"""\
            MERGE [dbo_ext].[skw_Permissions] AS target
            USING (
                SELECT
                    N'{perm_name}'   AS PermissionName,
                    N'{description}' AS Description,
                    N'faktury'       AS Category
            ) AS source
                ON target.[PermissionName] = source.[PermissionName]
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ([PermissionName], [Description], [Category], [IsActive], [CreatedAt])
                VALUES (source.[PermissionName], source.[Description],
                        source.[Category], 1, GETDATE());
        """))

    logger.info("[%s] Krok 1/2 — uprawnienia wstawione", revision)

    # Krok 2: Przypisanie do ról
    for role_name in _ROLE_NAMES:
        op.execute(textwrap.dedent(f"""\
            MERGE [dbo_ext].[skw_RolePermissions] AS target
            USING (
                SELECT r.[ID_ROLE], p.[ID_PERMISSION]
                FROM [dbo_ext].[skw_Roles] r
                CROSS JOIN [dbo_ext].[skw_Permissions] p
                WHERE r.[RoleName]       = N'{role_name}'
                  AND p.[PermissionName] LIKE N'faktury.pole.%'
                  AND p.[IsActive]       = 1
            ) AS source
                ON  target.[ID_ROLE]       = source.[ID_ROLE]
                AND target.[ID_PERMISSION] = source.[ID_PERMISSION]
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ([ID_ROLE], [ID_PERMISSION], [CreatedAt])
                VALUES (source.[ID_ROLE], source.[ID_PERMISSION], GETDATE());
        """))
        logger.info("[%s] Rola %s — przypisania OK", revision, role_name)

    logger.info("[%s] UPGRADE zakończony — 21 uprawnień, 4 role", revision)


def downgrade() -> None:
    logger.warning("[%s] DOWNGRADE → usuwam uprawnienia faktury.pole.*", revision)

    # Usuń przypisania ról
    op.execute(textwrap.dedent("""\
        DELETE rp
        FROM [dbo_ext].[skw_RolePermissions] rp
        INNER JOIN [dbo_ext].[skw_Permissions] p
            ON rp.[ID_PERMISSION] = p.[ID_PERMISSION]
        WHERE p.[PermissionName] LIKE N'faktury.pole.%';
    """))

    # Usuń uprawnienia
    op.execute(textwrap.dedent("""\
        DELETE FROM [dbo_ext].[skw_Permissions]
        WHERE [PermissionName] LIKE N'faktury.pole.%';
    """))

    logger.warning("[%s] DOWNGRADE zakończony", revision)