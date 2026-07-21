# backend/alembic/versions/0057_document_push_items.py
"""0057_document_push_items

Tier 1b (Recenzja Krytyczna Tier1/Tier2 + Rozstrzygniecia Koncowe, 2026-07-16):
Tabela pozycji dokumentu dla zrodel source_type='api' connection_mode='push'.

Jeden wiersz = jedna pozycja (item_data = surowy JSON, dynamiczne pola,
zero whitelisty — kazda integracja moze miec inny ksztalt pozycji).
FK na id_instance (nie para id_source+id_document) — id_instance juz
istnieje w momencie zapisu pozycji (ta sama transakcja co utworzenie
dokumentu przez webhook_service.receive_document()).

ON DELETE CASCADE — usuniecie instancji obiegu usuwa tez jej pozycje,
zgodnie ze wzorcem juz stosowanym w projekcie dla tabel podrzednych.

Rowniez: seed klucza SystemConfig WEBHOOK_MAX_ITEMS_PER_DOCUMENT
(domyslnie 500) — limit liczby pozycji na dokument w jednym webhooku,
walidowany w webhook_service._validate_items_payload().

Revision ID : 0057
Revises     : 0056
"""
import logging

from alembic import op
from sqlalchemy import text

revision = "0057"
down_revision = "0056"
branch_labels = None
depends_on = None

SCHEMA = "dbo"

logger = logging.getLogger(f"alembic.migration.{revision}")


def _log(krok: str, msg: str) -> None:
    print(f"[0057] KROK {krok} | {msg}")
    logger.info("[%s] %s", krok, msg)


def upgrade() -> None:
    bind = op.get_bind()

    # ── KROK 01: CREATE TABLE skw_document_push_items (idempotentnie) ───────
    _log("01", "CREATE TABLE skw_document_push_items")
    bind.execute(text(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_document_push_items'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_document_push_items] (
                [id_item]     INT IDENTITY(1,1) NOT NULL,
                [id_instance] INT               NOT NULL,
                [item_order]  INT               NOT NULL,
                [item_data]   NVARCHAR(MAX)     NOT NULL,
                [created_at]  DATETIME2         NOT NULL
                              CONSTRAINT [DF_skw_dpi_created_at] DEFAULT (SYSUTCDATETIME()),

                CONSTRAINT [PK_skw_document_push_items]
                    PRIMARY KEY CLUSTERED ([id_item] ASC),

                CONSTRAINT [FK_skw_dpi_instance]
                    FOREIGN KEY ([id_instance])
                    REFERENCES [{SCHEMA}].[skw_document_approval_instances] ([id_instance])
                    ON DELETE CASCADE
            );
            PRINT N'[0057-01] Tabela skw_document_push_items utworzona.';
        END
        ELSE
            PRINT N'[0057-01] Tabela skw_document_push_items juz istnieje — pomijam.';
    """))
    _log("01", "OK")

    # ── KROK 02: Indeks na id_instance (odczyt pozycji per dokument) ────────
    _log("02", "INDEX IX_skw_dpi_instance")
    bind.execute(text(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_document_push_items]')
              AND name = N'IX_skw_dpi_instance'
        )
        CREATE NONCLUSTERED INDEX [IX_skw_dpi_instance]
            ON [{SCHEMA}].[skw_document_push_items] ([id_instance] ASC, [item_order] ASC)
    """))
    _log("02", "OK")

    # ── KROK 03: Seed SystemConfig WEBHOOK_MAX_ITEMS_PER_DOCUMENT ───────────
    _log("03", "MERGE skw_SystemConfig WEBHOOK_MAX_ITEMS_PER_DOCUMENT")
    bind.execute(text(f"""
        MERGE [{SCHEMA}].[skw_SystemConfig] AS target
        USING (SELECT
            N'WEBHOOK_MAX_ITEMS_PER_DOCUMENT' AS ConfigKey,
            N'500' AS ConfigValue,
            N'Maksymalna liczba pozycji (items) w jednym payloadzie webhooka push. '
            + N'Przekroczenie = HTTP 422.' AS Description
        ) AS source
        ON target.[ConfigKey] = source.ConfigKey
        WHEN NOT MATCHED THEN
            INSERT ([ConfigKey], [ConfigValue], [Description], [IsActive])
            VALUES (source.ConfigKey, source.ConfigValue, source.Description, 1);
    """))
    _log("03", "OK")

    # ── KROK 04: Seed SystemConfig WEBHOOK_MODULE_ENABLED ────────────────────
    # NAPRAWA (self-review "brakujace mechanizmy z podstawowego projektu"):
    # kazdy wiekszy modul projektu ma administracyjny wylacznik awaryjny
    # (wzorzec: APPROVAL_MODULE_ENABLED, faktury.force_status_enabled,
    # maintenance_mode.enabled) — webhook_service.py nigdy go nie mial.
    _log("04", "MERGE skw_SystemConfig WEBHOOK_MODULE_ENABLED")
    bind.execute(text(f"""
        MERGE [{SCHEMA}].[skw_SystemConfig] AS target
        USING (SELECT
            N'WEBHOOK_MODULE_ENABLED' AS ConfigKey,
            N'true' AS ConfigValue,
            N'Wylacznik awaryjny calego mechanizmu webhook push. false = '
            + N'kazde POST /webhooks/sources/{{token}} zwraca HTTP 503, '
            + N'niezaleznie od tokenu/zrodla. Uzyj przy ataku/naduzyciu/awarii '
            + N'integratora zewnetrznego zamiast dezaktywowac kazde zrodlo osobno.' AS Description
        ) AS source
        ON target.[ConfigKey] = source.ConfigKey
        WHEN NOT MATCHED THEN
            INSERT ([ConfigKey], [ConfigValue], [Description], [IsActive])
            VALUES (source.ConfigKey, source.ConfigValue, source.Description, 1);
    """))
    _log("04", "OK — migracja 0057 zakonczona")

    # UWAGA: swiadomie BRAK rejestracji w skw_SchemaChecksums dla tej tabeli.
    # Ten mechanizm liczy CHECKSUM() z sys.sql_modules — definicji SQL
    # widokow/procedur/funkcji. Zwykla tabela fizyczna nie ma "modulu" do
    # checksumowania; dodanie tam recznego wpisu z Checksum=0 dla ObjectType
    # 'TABLE' wprowadziloby rekord, ktorego integrity watchdog
    # (app.core.schema_integrity) nigdy nie zweryfikuje ani nie zaktualizuje,
    # bo iteruje wylacznie po obiektach faktycznie obecnych w sys.sql_modules.


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(text(f"""
        IF OBJECT_ID(N'[{SCHEMA}].[skw_document_push_items]', N'U') IS NOT NULL
            DROP TABLE [{SCHEMA}].[skw_document_push_items]
    """))
    bind.execute(text(f"""
        DELETE FROM [{SCHEMA}].[skw_SystemConfig]
        WHERE [ConfigKey] = N'WEBHOOK_MAX_ITEMS_PER_DOCUMENT'
    """))
    bind.execute(text(f"""
        DELETE FROM [{SCHEMA}].[skw_SystemConfig]
        WHERE [ConfigKey] = N'WEBHOOK_MODULE_ENABLED'
    """))