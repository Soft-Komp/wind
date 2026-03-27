"""Alembic migration: 007_faktura_akceptacja

Revision ID: 007
Revises    : 006
Create Date: 2026-03-26

Zawartość (kompletna — musi zawierać ZARÓWNO DDL jak i seeds):
    1. CREATE TABLE skw_faktura_akceptacja    (+ indeksy)
    2. CREATE TABLE skw_faktura_przypisanie   (+ 2 indeksy krytyczne)
    3. CREATE TABLE skw_faktura_log           (+ indeksy)
    4. CREATE/ALTER widoki dbo (skw_faktury_akceptacja_naglowek + pozycje)
    5. MERGE 14 uprawnień kategorii 'faktury' do skw_Permissions
    6. INSERT przypisań ról dla 14 uprawnień (Admin/Manager/User)
    7. MERGE 30 kluczy SystemConfig (13 nowych faktury + 17 brakujących systemu)
    8. MERGE checksums obu widoków do skw_SchemaChecksums

⚠️  Idempotentność:
    Każda operacja używa IF NOT EXISTS / MERGE / NOT EXISTS.
    Bezpieczne uruchomienie wielokrotne.

⚠️  Kolejność DDL jest krytyczna:
    skw_faktura_akceptacja PRZED skw_faktura_przypisanie i skw_faktura_log
    (FK dependencies).

⚠️  down_revision = "006":
    Migracja 006 (naprawa permissions templates) musi być wykonana wcześniej.
    Jeśli 006 nie istnieje w alembic_version — alembic upgrade head ją wykona.
"""
from __future__ import annotations

import logging

from alembic import op
import sqlalchemy as sa

# ---------------------------------------------------------------------------
revision      = "0007"
down_revision = "0006"
branch_labels = None
depends_on    = None
# ---------------------------------------------------------------------------

logger = logging.getLogger("alembic.migration.007")


# ===========================================================================
# UPGRADE
# ===========================================================================
def upgrade() -> None:
    logger.info("=== Migration 007: START upgrade ===")
    _create_table_faktura_akceptacja()
    _create_table_faktura_przypisanie()
    _create_table_faktura_log()
    _create_views_dbo()
    _seed_permissions()
    _seed_role_permissions()
    _seed_system_config()
    _seed_schema_checksums()
    logger.info("=== Migration 007: DONE upgrade ===")


# ===========================================================================
# DOWNGRADE
# ===========================================================================
def downgrade() -> None:
    logger.info("=== Migration 007: START downgrade ===")
    _drop_schema_checksums()
    _drop_system_config()
    _drop_role_permissions()
    _drop_permissions()
    _drop_views_dbo()
    _drop_table_faktura_log()
    _drop_table_faktura_przypisanie()
    _drop_table_faktura_akceptacja()
    logger.info("=== Migration 007: DONE downgrade ===")


# ===========================================================================
# UPGRADE HELPERS
# ===========================================================================

def _create_table_faktura_akceptacja() -> None:
    logger.info("[007] Tworzenie tabeli skw_faktura_akceptacja...")
    op.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.objects o
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = 'dbo_ext' AND o.name = 'skw_faktura_akceptacja'
        )
        BEGIN
            CREATE TABLE [dbo_ext].[skw_faktura_akceptacja] (
                [id]                INT         IDENTITY(1,1) NOT NULL,
                [numer_ksef]        NVARCHAR(50)              NOT NULL,
                [status_wewnetrzny] NVARCHAR(20)              NOT NULL,
                [priorytet]         NVARCHAR(10) DEFAULT 'normalny' NOT NULL,
                [opis_dokumentu]    NVARCHAR(MAX)             NULL,
                [uwagi]             NVARCHAR(MAX)             NULL,
                [utworzony_przez]   INT                       NOT NULL,
                [IsActive]          BIT          DEFAULT 1    NOT NULL,
                [CreatedAt]         DATETIME2    DEFAULT GETDATE() NOT NULL,
                [UpdatedAt]         DATETIME2                 NULL,
                CONSTRAINT [PK_skw_faktura_akceptacja]
                    PRIMARY KEY CLUSTERED ([id] ASC),
                CONSTRAINT [UQ_skw_faktura_akceptacja_numer_ksef]
                    UNIQUE ([numer_ksef]),
                CONSTRAINT [CK_skw_faktura_akceptacja_status]
                    CHECK ([status_wewnetrzny] IN
                        ('nowe','w_toku','zaakceptowana','anulowana')),
                CONSTRAINT [CK_skw_faktura_akceptacja_priorytet]
                    CHECK ([priorytet] IN ('normalny','pilny','bardzo_pilny')),
                CONSTRAINT [FK_skw_faktura_akceptacja_utworzony_przez]
                    FOREIGN KEY ([utworzony_przez])
                    REFERENCES [dbo_ext].[skw_Users]([ID_USER])
                    ON DELETE NO ACTION ON UPDATE NO ACTION
            )
        END
    """)
    # Indeksy
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
            WHERE name='IX_skw_faktura_akceptacja_status_active'
              AND object_id=OBJECT_ID('dbo_ext.skw_faktura_akceptacja'))
        CREATE NONCLUSTERED INDEX [IX_skw_faktura_akceptacja_status_active]
        ON [dbo_ext].[skw_faktura_akceptacja]([status_wewnetrzny],[IsActive])
        INCLUDE ([numer_ksef],[priorytet],[utworzony_przez],[CreatedAt])
    """)
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
            WHERE name='IX_skw_faktura_akceptacja_priorytet'
              AND object_id=OBJECT_ID('dbo_ext.skw_faktura_akceptacja'))
        CREATE NONCLUSTERED INDEX [IX_skw_faktura_akceptacja_priorytet]
        ON [dbo_ext].[skw_faktura_akceptacja]([priorytet],[IsActive])
        INCLUDE ([status_wewnetrzny],[CreatedAt])
    """)
    logger.info("[007] skw_faktura_akceptacja — OK")


def _create_table_faktura_przypisanie() -> None:
    logger.info("[007] Tworzenie tabeli skw_faktura_przypisanie...")
    op.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.objects o
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = 'dbo_ext' AND o.name = 'skw_faktura_przypisanie'
        )
        BEGIN
            CREATE TABLE [dbo_ext].[skw_faktura_przypisanie] (
                [id]          INT         IDENTITY(1,1) NOT NULL,
                [faktura_id]  INT                       NOT NULL,
                [user_id]     INT                       NOT NULL,
                [status]      NVARCHAR(20) DEFAULT 'oczekuje' NOT NULL,
                [komentarz]   NVARCHAR(MAX)             NULL,
                [is_active]   BIT          DEFAULT 1    NOT NULL,
                [CreatedAt]   DATETIME2    DEFAULT GETDATE() NOT NULL,
                [UpdatedAt]   DATETIME2                 NULL,
                [decided_at]  DATETIME2                 NULL,
                CONSTRAINT [PK_skw_faktura_przypisanie]
                    PRIMARY KEY CLUSTERED ([id] ASC),
                CONSTRAINT [CK_skw_faktura_przypisanie_status]
                    CHECK ([status] IN
                        ('oczekuje','zaakceptowane','odrzucone','nie_moje')),
                CONSTRAINT [FK_skw_faktura_przypisanie_faktura]
                    FOREIGN KEY ([faktura_id])
                    REFERENCES [dbo_ext].[skw_faktura_akceptacja]([id])
                    ON DELETE NO ACTION ON UPDATE NO ACTION,
                CONSTRAINT [FK_skw_faktura_przypisanie_user]
                    FOREIGN KEY ([user_id])
                    REFERENCES [dbo_ext].[skw_Users]([ID_USER])
                    ON DELETE NO ACTION ON UPDATE NO ACTION
            )
        END
    """)
    # Indeksy krytyczne
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
            WHERE name='IX_skw_faktura_przypisanie_user_active'
              AND object_id=OBJECT_ID('dbo_ext.skw_faktura_przypisanie'))
        CREATE NONCLUSTERED INDEX [IX_skw_faktura_przypisanie_user_active]
        ON [dbo_ext].[skw_faktura_przypisanie]([user_id],[is_active],[status])
        INCLUDE ([faktura_id],[CreatedAt])
    """)
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
            WHERE name='IX_skw_faktura_przypisanie_faktura_active'
              AND object_id=OBJECT_ID('dbo_ext.skw_faktura_przypisanie'))
        CREATE NONCLUSTERED INDEX [IX_skw_faktura_przypisanie_faktura_active]
        ON [dbo_ext].[skw_faktura_przypisanie]([faktura_id],[is_active])
        INCLUDE ([user_id],[status])
    """)
    logger.info("[007] skw_faktura_przypisanie — OK")


def _create_table_faktura_log() -> None:
    logger.info("[007] Tworzenie tabeli skw_faktura_log...")
    op.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.objects o
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = 'dbo_ext' AND o.name = 'skw_faktura_log'
        )
        BEGIN
            CREATE TABLE [dbo_ext].[skw_faktura_log] (
                [id]          BIGINT      IDENTITY(1,1) NOT NULL,
                [faktura_id]  INT                       NOT NULL,
                [user_id]     INT                       NULL,
                [akcja]       NVARCHAR(50)              NOT NULL,
                [szczegoly]   NVARCHAR(MAX)             NULL,
                [CreatedAt]   DATETIME2   DEFAULT GETDATE() NOT NULL,
                [UpdatedAt]   DATETIME2                 NULL,
                CONSTRAINT [PK_skw_faktura_log]
                    PRIMARY KEY CLUSTERED ([id] ASC),
                CONSTRAINT [CK_skw_faktura_log_akcja]
                    CHECK ([akcja] IN (
                        'przypisano','zaakceptowano','odrzucono','zresetowano',
                        'status_zmieniony','priorytet_zmieniony','fakir_update',
                        'fakir_update_failed','nie_moje','force_akceptacja','anulowano'
                    )),
                CONSTRAINT [CK_skw_faktura_log_szczegoly_json]
                    CHECK ([szczegoly] IS NULL OR ISJSON([szczegoly]) = 1),
                CONSTRAINT [FK_skw_faktura_log_faktura]
                    FOREIGN KEY ([faktura_id])
                    REFERENCES [dbo_ext].[skw_faktura_akceptacja]([id])
                    ON DELETE NO ACTION ON UPDATE NO ACTION,
                CONSTRAINT [FK_skw_faktura_log_user]
                    FOREIGN KEY ([user_id])
                    REFERENCES [dbo_ext].[skw_Users]([ID_USER])
                    ON DELETE SET NULL ON UPDATE NO ACTION
            )
        END
    """)
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
            WHERE name='IX_skw_faktura_log_faktura_czas'
              AND object_id=OBJECT_ID('dbo_ext.skw_faktura_log'))
        CREATE NONCLUSTERED INDEX [IX_skw_faktura_log_faktura_czas]
        ON [dbo_ext].[skw_faktura_log]([faktura_id],[CreatedAt] DESC)
        INCLUDE ([user_id],[akcja])
    """)
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
            WHERE name='IX_skw_faktura_log_akcja'
              AND object_id=OBJECT_ID('dbo_ext.skw_faktura_log'))
        CREATE NONCLUSTERED INDEX [IX_skw_faktura_log_akcja]
        ON [dbo_ext].[skw_faktura_log]([akcja],[CreatedAt] DESC)
        INCLUDE ([faktura_id],[user_id])
    """)
    logger.info("[007] skw_faktura_log — OK")


def _create_views_dbo() -> None:
    """Tworzy/aktualizuje oba widoki w schemacie dbo."""
    logger.info("[007] Tworzenie widoków dbo...")
    op.execute("""
        CREATE OR ALTER VIEW [dbo].[skw_faktury_akceptacja_naglowek]
        AS
        SELECT
            bd.[ID_BUF_DOKUMENT],
            bd.[KSEF_ID],
            bd.[NUMER],
            bd.[KOD_STATUSU],
            CASE bd.[KOD_STATUSU]
                WHEN NULL THEN N'NOWY'
                WHEN N'K' THEN N'ZATWIERDZONY'
                WHEN N'A' THEN N'ZAKSIEGOWANY'
                ELSE bd.[KOD_STATUSU]
            END AS [StatusOpis],
            dbo.RM_Func_ClarionDateToDateTime(bd.[DATA_WYSTAWIENIA], 0) AS [DataWystawienia],
            dbo.RM_Func_ClarionDateToDateTime(bd.[DATA_OTRZYMANIA], 0)  AS [DataOtrzymania],
            dbo.RM_Func_ClarionDateToDateTime(bd.[TERMIN_PLATNOSCI], 0) AS [TerminPlatnosci],
            ISNULL(bd.[WARTOSC_NETTO], 0)   AS [WARTOSC_NETTO],
            ISNULL(bd.[WARTOSC_BRUTTO], 0)  AS [WARTOSC_BRUTTO],
            ISNULL(bd.[KWOTA_VAT], 0)       AS [KWOTA_VAT],
            bd.[FORMA_PLATNOSCI],
            bd.[UWAGI],
            ISNULL(k.[NAZWA], N'')          AS [NazwaKontrahenta],
            ISNULL(k.[EMAIL], N'')          AS [EmailKontrahenta],
            ISNULL(k.[TELEFON], N'')        AS [TelefonKontrahenta],
            ISNULL(k.[NIP], N'')            AS [NIPKontrahenta],
            bd.[PRG_KOD]
        FROM [dbo].[BUF_DOKUMENT] bd
        LEFT JOIN [dbo].[KONTRAHENT] k ON bd.[ID_KONTRAHENT] = k.[ID_KONTRAHENT]
        WHERE bd.[PRG_KOD] = 3
          AND bd.[KSEF_ID] IS NOT NULL
          AND bd.[TYP] = N'Z'
    """)
    op.execute("""
        CREATE OR ALTER VIEW [dbo].[skw_faktury_akceptacja_pozycje]
        AS
        SELECT
            pos.[ID_BUF_DOKUMENT],
            pos.[LP]                            AS [NumerPozycji],
            ISNULL(pos.[NAZWA_TOWARU], N'')     AS [NazwaTowaru],
            ISNULL(pos.[ILOSC], 0)              AS [Ilosc],
            ISNULL(pos.[JEDNOSTKA], N'szt.')    AS [Jednostka],
            ISNULL(pos.[CENA_NETTO], 0)         AS [CenaNetto],
            ISNULL(pos.[CENA_BRUTTO], 0)        AS [CenaBrutto],
            ISNULL(pos.[WARTOSC_NETTO], 0)      AS [WartoscNetto],
            ISNULL(pos.[WARTOSC_BRUTTO], 0)     AS [WartoscBrutto],
            ISNULL(pos.[STAWKA_VAT], N'23%')    AS [StawkaVAT],
            pos.[OPIS]                          AS [Opis]
        FROM [dbo].[Api_V_BufferDocumentPosition] pos
        WHERE EXISTS (
            SELECT 1 FROM [dbo].[BUF_DOKUMENT] bd
            WHERE bd.[ID_BUF_DOKUMENT] = pos.[ID_BUF_DOKUMENT]
              AND bd.[PRG_KOD] = 3
              AND bd.[KSEF_ID] IS NOT NULL
              AND bd.[TYP] = N'Z'
        )
    """)
    logger.info("[007] Widoki dbo — OK")


def _seed_permissions() -> None:
    logger.info("[007] Seeding 14 uprawnień kategorii faktury...")
    op.execute("""
        MERGE INTO [dbo_ext].[skw_Permissions] AS target
        USING (VALUES
            (N'faktury.view_list',    N'Lista faktur w obiegu',                            N'faktury'),
            (N'faktury.view_details', N'Szczegóły faktury z pozycjami',                    N'faktury'),
            (N'faktury.create',       N'Wpuszczenie faktury do obiegu',                    N'faktury'),
            (N'faktury.edit',         N'Edycja priorytetu i opisu faktury',                N'faktury'),
            (N'faktury.reset',        N'Reset przypisań pracowników',                      N'faktury'),
            (N'faktury.force_status', N'Wymuszenie statusu faktury',                       N'faktury'),
            (N'faktury.view_historia',N'Historia zdarzeń faktury',                         N'faktury'),
            (N'faktury.view_pdf',     N'PDF wizualizacja faktury',                         N'faktury'),
            (N'faktury.moje_view',    N'Lista przypisanych faktur pracownika',             N'faktury'),
            (N'faktury.moje_details', N'Szczegóły przypisanej faktury',                    N'faktury'),
            (N'faktury.moje_decyzja', N'Akceptacja/odrzucenie/nie_moje faktury',           N'faktury'),
            (N'faktury.referent',     N'Rola modułowa: dostęp referenta',                  N'faktury'),
            (N'faktury.akceptant',    N'Rola modułowa: dostęp akceptanta',                 N'faktury'),
            (N'faktury.config_edit',  N'Edycja kluczy konfiguracji modułu faktur',         N'faktury')
        ) AS source (PermissionName, Description, Category)
        ON (target.PermissionName = source.PermissionName)
        WHEN NOT MATCHED THEN
            INSERT (PermissionName, Description, Category, IsActive, CreatedAt)
            VALUES (source.PermissionName, source.Description, source.Category, 1, GETDATE())
    """)
    logger.info("[007] Permissions faktury — OK")


def _seed_role_permissions() -> None:
    logger.info("[007] Seeding macierzy ról dla faktury...")
    op.execute("""
        INSERT INTO [dbo_ext].[skw_RolePermissions] ([ID_ROLE], [ID_PERMISSION], [CreatedAt])
        SELECT r.[ID_ROLE], p.[ID_PERMISSION], GETDATE()
        FROM (VALUES
            -- Admin — wszystkie 14
            (N'Admin',   N'faktury.view_list'),
            (N'Admin',   N'faktury.view_details'),
            (N'Admin',   N'faktury.create'),
            (N'Admin',   N'faktury.edit'),
            (N'Admin',   N'faktury.reset'),
            (N'Admin',   N'faktury.force_status'),
            (N'Admin',   N'faktury.view_historia'),
            (N'Admin',   N'faktury.view_pdf'),
            (N'Admin',   N'faktury.moje_view'),
            (N'Admin',   N'faktury.moje_details'),
            (N'Admin',   N'faktury.moje_decyzja'),
            (N'Admin',   N'faktury.referent'),
            (N'Admin',   N'faktury.akceptant'),
            (N'Admin',   N'faktury.config_edit'),
            -- Manager — 12 (bez force_status i config_edit)
            (N'Manager', N'faktury.view_list'),
            (N'Manager', N'faktury.view_details'),
            (N'Manager', N'faktury.create'),
            (N'Manager', N'faktury.edit'),
            (N'Manager', N'faktury.reset'),
            (N'Manager', N'faktury.view_historia'),
            (N'Manager', N'faktury.view_pdf'),
            (N'Manager', N'faktury.moje_view'),
            (N'Manager', N'faktury.moje_details'),
            (N'Manager', N'faktury.moje_decyzja'),
            (N'Manager', N'faktury.referent'),
            (N'Manager', N'faktury.akceptant'),
            -- User — 5 (tylko akceptant + PDF)
            (N'User',    N'faktury.view_pdf'),
            (N'User',    N'faktury.moje_view'),
            (N'User',    N'faktury.moje_details'),
            (N'User',    N'faktury.moje_decyzja'),
            (N'User',    N'faktury.akceptant')
        ) AS m (RoleName, PermissionName)
        JOIN [dbo_ext].[skw_Roles]       r ON r.[RoleName]       = m.[RoleName]
        JOIN [dbo_ext].[skw_Permissions] p ON p.[PermissionName] = m.[PermissionName]
        WHERE NOT EXISTS (
            SELECT 1 FROM [dbo_ext].[skw_RolePermissions] rp
            WHERE rp.[ID_ROLE] = r.[ID_ROLE] AND rp.[ID_PERMISSION] = p.[ID_PERMISSION]
        )
    """)
    logger.info("[007] Role permissions faktury — OK")


def _seed_system_config() -> None:
    logger.info("[007] Seeding 30 kluczy SystemConfig...")
    op.execute("""
        MERGE INTO [dbo_ext].[skw_SystemConfig] AS target
        USING (VALUES
            -- Moduł faktur (13)
            (N'modul_akceptacji_faktur_enabled', N'false',
             N'Główny włącznik modułu akceptacji faktur KSeF'),
            (N'faktury.powiadomienia_sse_enabled', N'true',
             N'SSE push przy przypisaniu faktury'),
            (N'faktury.fakir_update_enabled', N'false',
             N'KRYTYCZNY: włącznik zapisu do BUF_DOKUMENT'),
            (N'faktury.fakir_rollback_enabled', N'false',
             N'Czy reset może cofnąć zatwierdzoną fakturę'),
            (N'faktury.reset_przypisania_enabled', N'true',
             N'Czy referent może resetować przypisania'),
            (N'faktury.force_status_enabled', N'true',
             N'Czy referent może wymusić status'),
            (N'faktury.max_przypisanych_pracownikow', N'10',
             N'Limit pracowników przypisanych do jednej faktury'),
            (N'faktury.confirm_token_ttl_seconds', N'60',
             N'TTL tokenów potwierdzających operacje dwuetapowe'),
            (N'faktury.fakir_retry_attempts', N'3',
             N'Liczba prób UPDATE Fakira przed alertem'),
            (N'faktury.demo_fake_ksef_ids_enabled', N'false',
             N'DEMO: fikcyjne faktury zamiast WAPRO'),
            (N'faktury.pdf_enabled', N'true',
             N'Włącznik generowania PDF faktury'),
            (N'faktury.pdf_cache_ttl_seconds', N'300',
             N'TTL cache PDF faktury w Redis'),
            (N'idempotency.window_seconds', N'10',
             N'Okno czasowe idempotency w sekundach'),
            -- Brakujące klucze systemu (17)
            (N'integrity_watchdog.enabled', N'true',
             N'Włącznik watchdoga integralności schematu'),
            (N'integrity_watchdog.interval_seconds', N'300',
             N'Interwał watchdoga (sekundy)'),
            (N'integrity_watchdog.grace_period_s', N'30',
             N'Okres karencji watchdoga (sekundy)'),
            (N'test_mode.enabled', N'false',
             N'Tryb testowy: przekieruj email/SMS'),
            (N'test_mode.email', N'',
             N'Testowy email przy test_mode.enabled=true'),
            (N'test_mode.phone', N'',
             N'Testowy telefon przy test_mode.enabled=true'),
            (N'bcc.enabled', N'false',
             N'Dodawaj BCC do emaili'),
            (N'bcc.emails', N'',
             N'Adresy BCC oddzielone przecinkiem'),
            (N'rate_limit.login_max_attempts', N'5',
             N'Max nieudanych prób logowania'),
            (N'rate_limit.login_window_seconds', N'300',
             N'Okno rate limit logowania (sekundy)'),
            (N'maintenance_mode.enabled', N'false',
             N'Globalny tryb serwisowy'),
            (N'maintenance_mode.message',
             N'System jest chwilowo niedostępny.',
             N'Komunikat trybu serwisowego'),
            (N'log.level', N'INFO',
             N'Poziom logowania: DEBUG|INFO|WARNING|ERROR'),
            (N'worker.max_email_per_bulk', N'500',
             N'Max emaili w zadaniu masowym'),
            (N'worker.max_sms_per_bulk', N'500',
             N'Max SMS-ów w zadaniu masowym'),
            (N'api.pagination_max_per_page', N'200',
             N'Max rekordów na stronę'),
            (N'api.pagination_default_per_page', N'50',
             N'Domyślna liczba rekordów na stronę')
        ) AS source (ConfigKey, ConfigValue, Description)
        ON (target.ConfigKey = source.ConfigKey)
        WHEN NOT MATCHED THEN
            INSERT (ConfigKey, ConfigValue, Description, IsActive, CreatedAt)
            VALUES (source.ConfigKey, source.ConfigValue, source.Description, 1, GETDATE())
    """)
    logger.info("[007] SystemConfig — OK (30 kluczy)")


def _seed_schema_checksums() -> None:
    logger.info("[007] Rejestracja checksumów widoków...")
    op.execute("""
        MERGE INTO [dbo_ext].[skw_SchemaChecksums] AS target
        USING (VALUES
            (N'dbo.skw_faktury_akceptacja_naglowek', N'VIEW',
             CHECKSUM(OBJECT_DEFINITION(OBJECT_ID('dbo.skw_faktury_akceptacja_naglowek'))),
             N'007'),
            (N'dbo.skw_faktury_akceptacja_pozycje',  N'VIEW',
             CHECKSUM(OBJECT_DEFINITION(OBJECT_ID('dbo.skw_faktury_akceptacja_pozycje'))),
             N'007')
        ) AS source (ObjectName, ObjectType, Checksum, AlembicRevision)
        ON (target.ObjectName = source.ObjectName)
        WHEN MATCHED AND target.Checksum <> source.Checksum THEN
            UPDATE SET Checksum=source.Checksum,
                       AlembicRevision=source.AlembicRevision,
                       LastVerifiedAt=GETDATE(),
                       UpdatedAt=GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (ObjectName, ObjectType, Checksum, AlembicRevision, LastVerifiedAt, CreatedAt)
            VALUES (source.ObjectName, source.ObjectType, source.Checksum,
                    source.AlembicRevision, GETDATE(), GETDATE())
    """)
    logger.info("[007] Checksums widoków — OK")


# ===========================================================================
# DOWNGRADE HELPERS
# ===========================================================================

def _drop_schema_checksums() -> None:
    op.execute("""
        DELETE FROM [dbo_ext].[skw_SchemaChecksums]
        WHERE ObjectName IN (
            N'dbo.skw_faktury_akceptacja_naglowek',
            N'dbo.skw_faktury_akceptacja_pozycje'
        )
    """)


def _drop_system_config() -> None:
    op.execute("""
        DELETE FROM [dbo_ext].[skw_SystemConfig]
        WHERE ConfigKey IN (
            N'modul_akceptacji_faktur_enabled',
            N'faktury.powiadomienia_sse_enabled',
            N'faktury.fakir_update_enabled',
            N'faktury.fakir_rollback_enabled',
            N'faktury.reset_przypisania_enabled',
            N'faktury.force_status_enabled',
            N'faktury.max_przypisanych_pracownikow',
            N'faktury.confirm_token_ttl_seconds',
            N'faktury.fakir_retry_attempts',
            N'faktury.demo_fake_ksef_ids_enabled',
            N'faktury.pdf_enabled',
            N'faktury.pdf_cache_ttl_seconds',
            N'idempotency.window_seconds',
            N'integrity_watchdog.enabled',
            N'integrity_watchdog.interval_seconds',
            N'integrity_watchdog.grace_period_s',
            N'test_mode.enabled', N'test_mode.email', N'test_mode.phone',
            N'bcc.enabled', N'bcc.emails',
            N'rate_limit.login_max_attempts', N'rate_limit.login_window_seconds',
            N'maintenance_mode.enabled', N'maintenance_mode.message',
            N'log.level',
            N'worker.max_email_per_bulk', N'worker.max_sms_per_bulk',
            N'api.pagination_max_per_page', N'api.pagination_default_per_page'
        )
    """)


def _drop_role_permissions() -> None:
    op.execute("""
        DELETE rp FROM [dbo_ext].[skw_RolePermissions] rp
        JOIN [dbo_ext].[skw_Permissions] p ON rp.[ID_PERMISSION] = p.[ID_PERMISSION]
        WHERE p.[Category] = N'faktury'
    """)


def _drop_permissions() -> None:
    op.execute("""
        DELETE FROM [dbo_ext].[skw_Permissions] WHERE Category = N'faktury'
    """)


def _drop_views_dbo() -> None:
    op.execute("""
        IF OBJECT_ID('dbo.skw_faktury_akceptacja_pozycje', 'V') IS NOT NULL
            DROP VIEW [dbo].[skw_faktury_akceptacja_pozycje]
    """)
    op.execute("""
        IF OBJECT_ID('dbo.skw_faktury_akceptacja_naglowek', 'V') IS NOT NULL
            DROP VIEW [dbo].[skw_faktury_akceptacja_naglowek]
    """)


def _drop_table_faktura_log() -> None:
    op.execute("""
        IF OBJECT_ID('dbo_ext.skw_faktura_log', 'U') IS NOT NULL
            DROP TABLE [dbo_ext].[skw_faktura_log]
    """)


def _drop_table_faktura_przypisanie() -> None:
    op.execute("""
        IF OBJECT_ID('dbo_ext.skw_faktura_przypisanie', 'U') IS NOT NULL
            DROP TABLE [dbo_ext].[skw_faktura_przypisanie]
    """)


def _drop_table_faktura_akceptacja() -> None:
    op.execute("""
        IF OBJECT_ID('dbo_ext.skw_faktura_akceptacja', 'U') IS NOT NULL
            DROP TABLE [dbo_ext].[skw_faktura_akceptacja]
    """)