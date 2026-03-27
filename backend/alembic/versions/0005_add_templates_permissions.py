"""0005_add_templates_permissions

NAPRAWA PILNA: Brakujące uprawnienia kategorii 'templates' + brakujące klucze SystemConfig.

======================================================================================
DIAGNOZA PROBLEMU
======================================================================================
Plik database/seeds/06_templates_permissions.sql NIE ISTNIEJE.
Przy aktualizacji istniejącego systemu (skw_Roles != EMPTY) entrypoint.sh pomija
WSZYSTKIE seedery — uprawnienia templates nigdy nie trafiają do bazy.
Skutek: każdy request na /templates kończy się 403 Forbidden.

DODATKOWY PROBLEM: 17 kluczy SystemConfig jest hardkodowanych w kodzie Python
zamiast czytanych z bazy. Przy aktualizacji brakuje ich w skw_SystemConfig.

======================================================================================
ROZWIĄZANIE
======================================================================================
Idempotentna migracja Alembic:
  - MERGE (nie INSERT) na skw_Permissions      → bezpieczna na duplikaty
  - INSERT NOT EXISTS na skw_RolePermissions   → bezpieczna na wielokrotne uruchomienie
  - MERGE na skw_SystemConfig                  → 17 brakujących kluczy konfiguracyjnych

======================================================================================
BEZPIECZEŃSTWO
======================================================================================
Bezpieczna na:
  ✅ Systemy świeże          (tabele puste — MERGE wstawi)
  ✅ Systemy po ręcznym SSMS  (MERGE nie zduplikuje)
  ✅ Wielokrotne uruchomienie (idempotentność przez NOT MATCHED)

======================================================================================
WERYFIKACJA PO WDROŻENIU
======================================================================================
  SELECT COUNT(*) FROM dbo_ext.skw_Permissions WHERE Category = 'templates';
  -- oczekiwany wynik: 5

  SELECT p.PermissionName, r.RoleName
  FROM dbo_ext.skw_RolePermissions rp
  JOIN dbo_ext.skw_Permissions p ON rp.ID_PERMISSION = p.ID_PERMISSION
  JOIN dbo_ext.skw_Roles r       ON rp.ID_ROLE       = r.ID_ROLE
  WHERE p.Category = 'templates'
  ORDER BY r.RoleName, p.PermissionName;
  -- oczekiwany wynik: 12 wierszy (Admin=5, Manager=4, User=2, ReadOnly=1)

  SELECT COUNT(*) FROM dbo_ext.skw_SystemConfig WHERE ConfigKey LIKE 'integrity_watchdog%';
  -- oczekiwany wynik: 3

Revision ID: 0005
Revises: 0004
Create Date: 2026-03-26
Author: Sprint 2 — Sesja 1
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Final

from alembic import op

# ── Meta Alembic ─────────────────────────────────────────────────────────────
revision: str = "0005"
down_revision: str = "0004"     # ⚠ UWAGA: spec mówi "0001" — ale real HEAD to 0004
branch_labels = None
depends_on = None

# ── Logger ────────────────────────────────────────────────────────────────────
# Alembic konfiguruje handlery logowania; używamy nazwy hierarchicznej.
logger = logging.getLogger(f"alembic.migration.{revision}")

# ── Stałe ─────────────────────────────────────────────────────────────────────
SCHEMA: Final[str] = "dbo_ext"
TEMPLATES_CATEGORY: Final[str] = "templates"

# ── Dane uprawnień ─────────────────────────────────────────────────────────────
# Tuple: (PermissionName, Description, Category)
TEMPLATE_PERMISSIONS: Final[list[tuple[str, str, str]]] = [
    (
        "templates.view_list",
        "Lista wszystkich szablonów monitów",
        TEMPLATES_CATEGORY,
    ),
    (
        "templates.view_details",
        "Szczegóły szablonu wraz z treścią Body",
        TEMPLATES_CATEGORY,
    ),
    (
        "templates.create",
        "Tworzenie nowego szablonu wiadomości",
        TEMPLATES_CATEGORY,
    ),
    (
        "templates.edit",
        "Edycja istniejącego szablonu",
        TEMPLATES_CATEGORY,
    ),
    (
        "templates.delete",
        "Dezaktywacja szablonu (soft-delete)",
        TEMPLATES_CATEGORY,
    ),
]

# Macierz uprawnień: RoleName → zestaw PermissionName
# Źródło prawdy: Sprint_2, Sekcja 1.3
ROLE_PERMISSION_MATRIX: Final[dict[str, list[str]]] = {
    "Admin": [
        "templates.view_list",
        "templates.view_details",
        "templates.create",
        "templates.edit",
        "templates.delete",
    ],
    "Manager": [
        "templates.view_list",
        "templates.view_details",
        "templates.create",
        "templates.edit",
    ],
    "User": [
        "templates.view_list",
        "templates.view_details",
    ],
    "ReadOnly": [
        "templates.view_list",
    ],
}

# ── Brakujące klucze SystemConfig ─────────────────────────────────────────────
# Tuple: (ConfigKey, ConfigValue, Description)
# Źródło: Sprint_2, Sekcja 4.2 — "Brakujące klucze istniejącego systemu"
MISSING_SYSTEM_CONFIG_KEYS: Final[list[tuple[str, str, str]]] = [
    # integrity_watchdog
    (
        "integrity_watchdog.enabled",
        "true",
        "Włącznik watchdoga integralności schematu DB",
    ),
    (
        "integrity_watchdog.interval_seconds",
        "300",
        "Interwał sprawdzania integralności schematu (sekundy)",
    ),
    (
        "integrity_watchdog.grace_period_s",
        "30",
        "Okres łaski przy starcie aplikacji przed pierwszym sprawdzeniem (sekundy)",
    ),
    # test_mode
    (
        "test_mode.enabled",
        "false",
        "Tryb testowy — email/SMS wysyłane na adresy testowe zamiast rzeczywistych",
    ),
    (
        "test_mode.email",
        "",
        "Adres email dla trybu testowego (nadpisuje odbiorcę)",
    ),
    (
        "test_mode.phone",
        "",
        "Numer telefonu dla trybu testowego (nadpisuje odbiorcę SMS)",
    ),
    # bcc
    (
        "bcc.enabled",
        "false",
        "Włącznik ślepej kopii (BCC) dla wysyłki email",
    ),
    (
        "bcc.emails",
        "",
        "Lista adresów BCC rozdzielona przecinkami",
    ),
    # rate_limit
    (
        "rate_limit.login_max_attempts",
        "5",
        "Maksymalna liczba nieudanych prób logowania przed blokadą konta",
    ),
    (
        "rate_limit.login_window_seconds",
        "300",
        "Okno czasowe dla zliczania nieudanych prób logowania (sekundy)",
    ),
    # maintenance_mode
    (
        "maintenance_mode.enabled",
        "false",
        "Tryb serwisowy — blokuje wszystkie requesty poza /health",
    ),
    (
        "maintenance_mode.message",
        "",
        "Komunikat wyświetlany użytkownikom podczas trybu serwisowego",
    ),
    # log
    (
        "log.level",
        "INFO",
        "Dynamiczny poziom logowania (DEBUG/INFO/WARNING/ERROR) bez restartu",
    ),
    # worker
    (
        "worker.max_email_per_bulk",
        "500",
        "Maksymalna liczba emaili w jednej operacji masowej wysyłki",
    ),
    (
        "worker.max_sms_per_bulk",
        "500",
        "Maksymalna liczba SMS w jednej operacji masowej wysyłki",
    ),
    # api
    (
        "api.pagination_max_per_page",
        "200",
        "Maksymalna dozwolona wartość parametru per_page w paginacji",
    ),
    (
        "api.pagination_default_per_page",
        "50",
        "Domyślna wartość per_page gdy parametr nie podany",
    ),
]


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC — wymagane przez Alembic
# ══════════════════════════════════════════════════════════════════════════════

def upgrade() -> None:
    """
    Wdraża:
      1. 5 uprawnień kategorii 'templates' (MERGE — idempotentne)
      2. 12 przypisań ról dla tych uprawnień (INSERT NOT EXISTS)
      3. 17 brakujących kluczy SystemConfig (MERGE — idempotentne)
    """
    _ts = datetime.now(timezone.utc).isoformat()
    logger.info(
        "[%s] upgrade() START | revision=%s | down_revision=%s | ts=%s",
        revision, revision, down_revision, _ts,
    )

    # ── Krok 1: uprawnienia templates ────────────────────────────────────────
    logger.info("[%s] Krok 1/3 — MERGE uprawnień templates...", revision)
    _merge_template_permissions()

    # ── Krok 2: przypisania ról ───────────────────────────────────────────────
    logger.info("[%s] Krok 2/3 — INSERT przypisań ról...", revision)
    _insert_role_permissions()

    # ── Krok 3: brakujące klucze SystemConfig ────────────────────────────────
    logger.info("[%s] Krok 3/3 — MERGE brakujących kluczy SystemConfig...", revision)
    _merge_missing_system_config()

    logger.info(
        "[%s] upgrade() DONE | ts=%s",
        revision,
        datetime.now(timezone.utc).isoformat(),
    )


def downgrade() -> None:
    """
    Odwraca migrację:
      1. Usuwa przypisania ról dla uprawnień templates
      2. Usuwa uprawnienia templates
      3. Usuwa brakujące klucze SystemConfig (tylko te wstawione przez tę migrację)

    ⚠⚠⚠ UWAGA: NIEODWRACALNA UTRATA DANYCH KONFIGURACJI.
         Wykonywać WYŁĄCZNIE w środowisku deweloperskim.
         NIE uruchamiać na produkcji bez pełnego backupu.
    """
    _ts = datetime.now(timezone.utc).isoformat()
    logger.warning(
        "[%s] downgrade() START — USUWANIE uprawnień i konfiguracji | ts=%s",
        revision, _ts,
    )

    # Krok 1: usuń przypisania ról (FK do skw_Permissions — musi być pierwsze)
    op.execute(
        f"""
        DELETE rp
        FROM [{SCHEMA}].[skw_RolePermissions] rp
        INNER JOIN [{SCHEMA}].[skw_Permissions] p
            ON rp.ID_PERMISSION = p.ID_PERMISSION
        WHERE p.Category = N'{TEMPLATES_CATEGORY}';
        """
    )
    logger.info(
        "[%s] downgrade() — usunięto przypisania ról dla kategorii='%s'.",
        revision, TEMPLATES_CATEGORY,
    )

    # Krok 2: usuń uprawnienia templates
    op.execute(
        f"""
        DELETE FROM [{SCHEMA}].[skw_Permissions]
        WHERE Category = N'{TEMPLATES_CATEGORY}';
        """
    )
    logger.info(
        "[%s] downgrade() — usunięto uprawnienia kategorii='%s'.",
        revision, TEMPLATES_CATEGORY,
    )

    # Krok 3: usuń klucze SystemConfig wstawione przez tę migrację
    config_keys_escaped = ", ".join(
        f"N'{key}'" for key, _, _ in MISSING_SYSTEM_CONFIG_KEYS
    )
    op.execute(
        f"""
        DELETE FROM [{SCHEMA}].[skw_SystemConfig]
        WHERE ConfigKey IN ({config_keys_escaped});
        """
    )
    logger.warning(
        "[%s] downgrade() — usunięto %d kluczy SystemConfig. DONE.",
        revision, len(MISSING_SYSTEM_CONFIG_KEYS),
    )


# ══════════════════════════════════════════════════════════════════════════════
# PRIVATE — logika migracji
# ══════════════════════════════════════════════════════════════════════════════

def _merge_template_permissions() -> None:
    """
    MERGE do skw_Permissions.

    Dlaczego MERGE zamiast INSERT?
      INSERT z IGNORE powodowałby błąd w MSSQL przy istniejącym UNIQUE constraint.
      MERGE z WHEN NOT MATCHED jest atomowy i idempotentny.

    Wynik: 5 wierszy wstawionych (lub 0 jeśli już istnieją).
    """
    # Budujemy VALUES blok dynamicznie — czytelniejsze i łatwiejsze do audytu
    values_rows = ",\n            ".join(
        f"(N'{pname}', N'{pdesc}', N'{pcat}')"
        for pname, pdesc, pcat in TEMPLATE_PERMISSIONS
    )

    sql = f"""
    MERGE [{SCHEMA}].[skw_Permissions] AS target
    USING (
        VALUES
            {values_rows}
    ) AS source (PermissionName, [Description], Category)
    ON target.PermissionName = source.PermissionName
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (PermissionName, [Description], Category, IsActive, CreatedAt)
        VALUES (
            source.PermissionName,
            source.[Description],
            source.Category,
            1,
            GETDATE()
        );
    """

    op.execute(sql)
    logger.info(
        "[%s] _merge_template_permissions() — MERGE wykonany dla %d uprawnień.",
        revision, len(TEMPLATE_PERMISSIONS),
    )


def _insert_role_permissions() -> None:
    """
    Wstawia przypisania uprawnień templates do ról.

    Używamy RoleName (nie hardkodowanego ID_ROLE) — odporność na różne środowiska.
    NOT EXISTS zapewnia idempotentność — wielokrotne uruchomienie jest bezpieczne.

    Oczekiwany rezultat:
      Admin=5, Manager=4, User=2, ReadOnly=1 → łącznie 12 przypisań
    """
    total_assignments = sum(len(p) for p in ROLE_PERMISSION_MATRIX.values())
    logger.info(
        "[%s] _insert_role_permissions() — %d ról, ~%d przypisań do wstawienia.",
        revision,
        len(ROLE_PERMISSION_MATRIX),
        total_assignments,
    )

    inserted_count = 0
    for role_name, permission_names in ROLE_PERMISSION_MATRIX.items():
        for permission_name in permission_names:
            sql = f"""
            INSERT INTO [{SCHEMA}].[skw_RolePermissions] (ID_ROLE, ID_PERMISSION, CreatedAt)
            SELECT
                r.ID_ROLE,
                p.ID_PERMISSION,
                GETDATE()
            FROM  [{SCHEMA}].[skw_Roles]       r
            CROSS JOIN [{SCHEMA}].[skw_Permissions]   p
            WHERE r.RoleName       = N'{role_name}'
              AND p.PermissionName = N'{permission_name}'
              AND NOT EXISTS (
                  SELECT 1
                  FROM [{SCHEMA}].[skw_RolePermissions] rp
                  WHERE rp.ID_ROLE       = r.ID_ROLE
                    AND rp.ID_PERMISSION = p.ID_PERMISSION
              );
            """
            op.execute(sql)
            inserted_count += 1
            logger.debug(
                "[%s]   INSERT RolePermission — rola='%s' | uprawnienie='%s'",
                revision, role_name, permission_name,
            )

    logger.info(
        "[%s] _insert_role_permissions() — %d instrukcji INSERT wykonanych.",
        revision, inserted_count,
    )


def _merge_missing_system_config() -> None:
    """
    MERGE 17 brakujących kluczy do skw_SystemConfig.

    Klucze te były hardkodowane w kodzie Python (auth_service.py, email_task.py itp.)
    zamiast czytane z bazy. Powoduje to niespójność przy zmianie przez /system/config.

    Używamy MERGE aby nie nadpisywać wartości zmodyfikowanych przez admina w istniejących
    systemach — WHEN NOT MATCHED wstawia TYLKO jeśli klucz nie istnieje.
    """
    values_rows = ",\n            ".join(
        f"(N'{ckey}', N'{cval}', N'{cdesc}')"
        for ckey, cval, cdesc in MISSING_SYSTEM_CONFIG_KEYS
    )

    sql = f"""
    MERGE [{SCHEMA}].[skw_SystemConfig] AS target
    USING (
        VALUES
            {values_rows}
    ) AS source (ConfigKey, ConfigValue, [Description])
    ON target.ConfigKey = source.ConfigKey
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (ConfigKey, ConfigValue, [Description], IsActive, CreatedAt)
        VALUES (
            source.ConfigKey,
            source.ConfigValue,
            source.[Description],
            1,
            GETDATE()
        );
    """

    op.execute(sql)
    logger.info(
        "[%s] _merge_missing_system_config() — MERGE wykonany dla %d kluczy.",
        revision, len(MISSING_SYSTEM_CONFIG_KEYS),
    )