# backend/alembic/versions/0052_watchdog_kolekcje.py
"""0052_watchdog_kolekcje

Watchdog (Etap 2.1 — komponent dopisany na końcu):
    - skw_WatchdogKolekcje  — grupa ustawień + OPCJONALNY warunek wyzwalający
    - skw_WatchdogPozycje   — pozycje wewnątrz kolekcji (obecność LUB wartość)
    - skw_WatchdogRunLog    — append-only log każdego przebiegu

POPRAWKA względem wersji roboczej (przed aplikacją na STOMIL — brak potrzeby 0053):
    1. Kolekcje 'env' mogą teraz sprawdzać WARTOŚĆ zmiennej, nie tylko
       obecność — realny przykład: MAINTENANCE_MODE=on wymaga DEMO_MODE=true
       itd. Tryb naprawy dla 'env' pozostaje ograniczony do block/log_only
       (CK_skw_WatchdogKolekcje_EnvNoAutoFix bez zmian — nadal obowiązuje).
    2. Nowe kolumny warunek_klucz/warunek_wartosc na skw_WatchdogKolekcje —
       reguła warunkowa "JEŚLI klucz=wartość, TO wymuś pozycje poniżej".
       NULL = kolekcja bezwarunkowa (zawsze sprawdzana), zachowanie
       kompatybilne z pierwotnym, prostym przypadkiem.
    3. Seed zastąpiony PRAWDZIWYM przypadkiem (tryb konserwacji / test mode),
       zamiast ilustracyjnego przykładu SMTP.

Revision ID: 0052
Revises:     0051
Create Date: 2026-07-07
"""
from __future__ import annotations

import logging
from typing import Final

from alembic import op
from sqlalchemy import text as sa_text

revision:      str = "0052"
down_revision: str = "0051"
branch_labels       = None
depends_on          = None

logger = logging.getLogger(f"alembic.migration.{revision}")

_CREATE_KOLEKCJE: Final[str] = """
IF OBJECT_ID(N'[dbo].[skw_WatchdogKolekcje]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[skw_WatchdogKolekcje] (
        [id_kolekcja]       INT IDENTITY(1,1) PRIMARY KEY,
        [nazwa]             NVARCHAR(100)  NOT NULL UNIQUE,
        [opis]              NVARCHAR(500)  NULL,
        [typ]               NVARCHAR(10)   NOT NULL
            CONSTRAINT CK_skw_WatchdogKolekcje_Typ CHECK ([typ] IN (N'env', N'db')),
        [tryb_naprawy]      NVARCHAR(10)   NOT NULL
            CONSTRAINT CK_skw_WatchdogKolekcje_Tryb
                CHECK ([tryb_naprawy] IN (N'block', N'auto_fix', N'log_only')),
        -- NOWE — warunek wyzwalający. NULL = kolekcja bezwarunkowa.
        [warunek_klucz]     NVARCHAR(200)  NULL,
        [warunek_wartosc]   NVARCHAR(MAX)  NULL,
        [is_active]         BIT            NOT NULL DEFAULT 1,
        [created_at]        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),

        -- Nie da się nadpisać żywego env w działającym kontenerze —
        -- kolekcje typu 'env' NIGDY nie mogą mieć trybu 'auto_fix'.
        CONSTRAINT CK_skw_WatchdogKolekcje_EnvNoAutoFix
            CHECK (NOT ([typ] = N'env' AND [tryb_naprawy] = N'auto_fix')),

        -- Warunek musi być podany w komplecie (klucz + wartość) albo wcale.
        CONSTRAINT CK_skw_WatchdogKolekcje_WarunekKomplet
            CHECK (
                ([warunek_klucz] IS NULL AND [warunek_wartosc] IS NULL)
                OR ([warunek_klucz] IS NOT NULL AND [warunek_wartosc] IS NOT NULL)
            )
    );
END
"""

_CREATE_POZYCJE: Final[str] = """
IF OBJECT_ID(N'[dbo].[skw_WatchdogPozycje]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[skw_WatchdogPozycje] (
        [id_pozycja]        INT IDENTITY(1,1) PRIMARY KEY,
        [id_kolekcja]       INT             NOT NULL,
        [typ_pozycji]       NVARCHAR(15)    NOT NULL
            CONSTRAINT CK_skw_WatchdogPozycje_Typ
                CHECK ([typ_pozycji] IN (N'env_var', N'db_setting')),
        [klucz]             NVARCHAR(200)   NOT NULL,
        -- Dla env_var: NULL = sprawdzaj TYLKO obecność.
        --              NOT NULL = sprawdzaj DOKŁADNĄ wartość (np. 'true').
        -- Dla db_setting: zawsze wartość docelowa (auto_fix ją zapisuje).
        [wartosc_wzorcowa]  NVARCHAR(MAX)   NULL,
        [created_at]        DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [FK_skw_WatchdogPozycje_Kolekcja]
            FOREIGN KEY ([id_kolekcja])
            REFERENCES [dbo].[skw_WatchdogKolekcje]([id_kolekcja])
            ON DELETE CASCADE
    );
    CREATE INDEX [IX_skw_WatchdogPozycje_Kolekcja] ON [dbo].[skw_WatchdogPozycje]([id_kolekcja]);
END
"""

_CREATE_RUNLOG: Final[str] = """
IF OBJECT_ID(N'[dbo].[skw_WatchdogRunLog]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[skw_WatchdogRunLog] (
        [id_log]                BIGINT IDENTITY(1,1) PRIMARY KEY,
        [id_kolekcja]           INT             NULL,
        [ts]                    DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        [watchdog_instance_id]  NVARCHAR(100)   NOT NULL,
        [wynik]                 NVARCHAR(20)    NOT NULL
            CONSTRAINT CK_skw_WatchdogRunLog_Wynik CHECK ([wynik] IN (
                N'ok', N'pominieto_warunek', N'niespojnosc', N'naprawiono',
                N'zablokowano', N'env_brak', N'blad_polaczenia',
                N'alert_wyslany', N'alert_blad'
            )),
        [szczegoly_json]        NVARCHAR(MAX)   NULL,
        [czas_trwania_ms]       INT             NULL,

        CONSTRAINT [FK_skw_WatchdogRunLog_Kolekcja]
            FOREIGN KEY ([id_kolekcja])
            REFERENCES [dbo].[skw_WatchdogKolekcje]([id_kolekcja])
            ON DELETE NO ACTION
    );
    CREATE INDEX [IX_skw_WatchdogRunLog_ts] ON [dbo].[skw_WatchdogRunLog]([ts] DESC);
END
"""

_CREATE_TRIGGER_RUNLOG: Final[str] = """
CREATE OR ALTER TRIGGER [dbo].[TRG_skw_WatchdogRunLog_AppendOnly]
ON [dbo].[skw_WatchdogRunLog]
INSTEAD OF UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    RAISERROR(N'skw_WatchdogRunLog jest append-only — UPDATE/DELETE zablokowane.', 16, 1);
END
"""

_SEED_KILL_SWITCH: Final[str] = """
MERGE [dbo].[skw_SystemConfig] AS target
USING (VALUES (N'ApplicationEnabled', N'1',
    N'Globalny wyłącznik aplikacji — sterowany przez Watchdog lub ręcznie w SSMS.'))
    AS source ([ConfigKey], [ConfigValue], [Description])
ON target.[ConfigKey] = source.[ConfigKey]
WHEN NOT MATCHED THEN
    INSERT ([ConfigKey], [ConfigValue], [Description], [IsActive])
    VALUES (source.[ConfigKey], source.[ConfigValue], source.[Description], 1);
"""

# ── SEED — PRAWDZIWY PRZYPADEK (zastępuje poprzedni ilustracyjny SMTP) ───────
# Tryb naprawy: 'block' — celowo, nie 'log_only'. Wysłanie realnego e-maila/SMS
# do prawdziwego klienta podczas okna konserwacyjnego to poważne ryzyko
# wizerunkowe/prawne — warto zablokować aplikację, a nie tylko zalogować.
# Zmiana trybu na log_only możliwa przez UPDATE (brak CRUD API na start —
# ustalone wcześniej), bez nowej migracji.
_SEED_KOLEKCJA_MAINTENANCE: Final[str] = """
MERGE [dbo].[skw_WatchdogKolekcje] AS target
USING (VALUES (
    N'Tryb_konserwacji_wymaga_trybu_testowego',
    N'Gdy MAINTENANCE_MODE=on, DEMO_MODE i TEST_MODE_* muszą być aktywne — ' +
    N'inaczej realne wiadomości mogą trafić do prawdziwych klientów podczas okna serwisowego.',
    N'env', N'block', N'MAINTENANCE_MODE', N'on'
)) AS source ([nazwa], [opis], [typ], [tryb_naprawy], [warunek_klucz], [warunek_wartosc])
ON target.[nazwa] = source.[nazwa]
WHEN NOT MATCHED THEN
    INSERT ([nazwa], [opis], [typ], [tryb_naprawy], [warunek_klucz], [warunek_wartosc], [is_active])
    VALUES (source.[nazwa], source.[opis], source.[typ], source.[tryb_naprawy],
            source.[warunek_klucz], source.[warunek_wartosc], 1);
"""

_SEED_POZYCJE_MAINTENANCE: Final[str] = """
INSERT INTO [dbo].[skw_WatchdogPozycje] ([id_kolekcja], [typ_pozycji], [klucz], [wartosc_wzorcowa])
SELECT k.[id_kolekcja], N'env_var', v.[klucz], v.[wartosc]
FROM [dbo].[skw_WatchdogKolekcje] k
CROSS JOIN (VALUES
    (N'DEMO_MODE',          N'true'),
    (N'TEST_MODE_ENABLED',  N'true'),
    (N'TEST_MODE_EMAIL',    N'wojtek@soft-komp.net'),
    (N'TEST_MODE_PHONE',    N'605223128')
) AS v([klucz], [wartosc])
WHERE k.[nazwa] = N'Tryb_konserwacji_wymaga_trybu_testowego'
  AND NOT EXISTS (
      SELECT 1 FROM [dbo].[skw_WatchdogPozycje] p
      WHERE p.[id_kolekcja] = k.[id_kolekcja] AND p.[klucz] = v.[klucz]
  );
"""


def upgrade() -> None:
    logger.info("[%s] UPGRADE START — tabele Watchdoga + kill-switch + seed rzeczywisty", revision)
    bind = op.get_bind()
    bind.execute(sa_text(_CREATE_KOLEKCJE))
    bind.execute(sa_text(_CREATE_POZYCJE))
    bind.execute(sa_text(_CREATE_RUNLOG))
    bind.execute(sa_text(_CREATE_TRIGGER_RUNLOG))
    bind.execute(sa_text(_SEED_KILL_SWITCH))
    bind.execute(sa_text(_SEED_KOLEKCJA_MAINTENANCE))
    bind.execute(sa_text(_SEED_POZYCJE_MAINTENANCE))
    logger.info("[%s] UPGRADE OK", revision)


def downgrade() -> None:
    logger.warning("[%s] DOWNGRADE — usuwam tabele Watchdoga (ApplicationEnabled w SystemConfig POZOSTAJE)", revision)
    bind = op.get_bind()
    bind.execute(sa_text("DROP TRIGGER IF EXISTS [dbo].[TRG_skw_WatchdogRunLog_AppendOnly]"))
    bind.execute(sa_text("DROP TABLE IF EXISTS [dbo].[skw_WatchdogRunLog]"))
    bind.execute(sa_text("DROP TABLE IF EXISTS [dbo].[skw_WatchdogPozycje]"))
    bind.execute(sa_text("DROP TABLE IF EXISTS [dbo].[skw_WatchdogKolekcje]"))