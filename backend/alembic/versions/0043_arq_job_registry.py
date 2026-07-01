# backend/alembic/versions/0043_arq_job_registry.py
"""0043 — Uniwersalny rejestr zadan ARQ (skw_ArqJobRegistry)

Jeden rekord per wywolanie kazdego taska ARQ w systemie — niezaleznie od
typu (monity, source_sync, auto_dispatch, hooki, akcje, przyszly OCR).

Cel: odpowiedz na pytanie "czy zadanie X sie powiodlo" bez logowania sie
na serwer i przegladania logow kontenera.

Wypelniana przez dekorator @track_job (worker/core/job_tracker.py) owinajacy
kazda funkcje w WorkerSettings.functions — zero zmian w samych taskach.

Kolumny:
  id_job          PK
  job_id          ARQ job_id (UUID jako string) — moze byc NULL gdy enqueue
                  sie nie powiodlo, ale rekord jest tworzony przy starcie
  task_name       nazwa funkcji (np. 'send_bulk_emails')
  status          queued | running | success | failed
  enqueued_at     kiedy zadanie trafilo do kolejki (przed wykonaniem)
  started_at      kiedy worker faktycznie zaczal wykonywac
  finished_at     kiedy zakonczyl (sukces lub blad)
  duration_ms     finished_at - started_at w ms
  result_summary  NVARCHAR(MAX) JSON — skrocony wynik (np. {"success":45,"failed":2})
  error_message   NVARCHAR(500) — tylko gdy status=failed
  triggered_by    NVARCHAR(50) — 'cron' | 'manual' | id_user jako string

Retencja: brak automatycznego czyszczenia w tej migracji — rekordy
gromadzone bezterminowo. Jesli wolumen okaze sie duzy, dodac partycjonowanie
po enqueued_at w osobnej migracji.

Revision ID: 0043
Revises:     0042
Create Date: 2026-06-30
"""

from alembic import op
from sqlalchemy import text

revision      = "0043"
down_revision = "0042"
branch_labels = None
depends_on    = None

SCHEMA = "dbo"


def upgrade() -> None:
    op.execute(text(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_ArqJobRegistry'
        )
        BEGIN
            CREATE TABLE [{SCHEMA}].[skw_ArqJobRegistry] (
                [id_job]          INT           IDENTITY(1,1) NOT NULL,
                [job_id]          NVARCHAR(64)                NULL,
                [task_name]       NVARCHAR(100)               NOT NULL,
                [status]          NVARCHAR(20)                NOT NULL
                                  CONSTRAINT [DF_skw_ajr_status]
                                  DEFAULT N'queued',
                [enqueued_at]     DATETIME2(7)                NOT NULL
                                  CONSTRAINT [DF_skw_ajr_enqueued_at]
                                  DEFAULT SYSUTCDATETIME(),
                [started_at]      DATETIME2(7)                NULL,
                [finished_at]     DATETIME2(7)                NULL,
                [duration_ms]     INT                         NULL,
                [result_summary]  NVARCHAR(MAX)               NULL,
                [error_message]   NVARCHAR(500)               NULL,
                [triggered_by]    NVARCHAR(50)                NULL,

                CONSTRAINT [PK_skw_ArqJobRegistry]
                    PRIMARY KEY CLUSTERED ([id_job] ASC),

                CONSTRAINT [CK_skw_ajr_status]
                    CHECK ([status] IN (N'queued', N'running', N'success', N'failed')),

                CONSTRAINT [CK_skw_ajr_duration_ms]
                    CHECK ([duration_ms] IS NULL OR [duration_ms] >= 0)
            );
            PRINT N'[0043] Tabela skw_ArqJobRegistry utworzona.'
        END
        ELSE
            PRINT N'[0043] Tabela skw_ArqJobRegistry juz istnieje — pomijam.'
    """))

    # Indeks pod typowe zapytania: "ostatnie zadania danego typu", "wszystkie failed"
    op.execute(text(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_ArqJobRegistry]')
              AND name = N'IX_skw_ajr_task_enqueued'
        )
        CREATE NONCLUSTERED INDEX [IX_skw_ajr_task_enqueued]
            ON [{SCHEMA}].[skw_ArqJobRegistry] ([task_name], [enqueued_at] DESC)
            INCLUDE ([status], [duration_ms])
    """))

    op.execute(text(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'[{SCHEMA}].[skw_ArqJobRegistry]')
              AND name = N'IX_skw_ajr_status_enqueued'
        )
        CREATE NONCLUSTERED INDEX [IX_skw_ajr_status_enqueued]
            ON [{SCHEMA}].[skw_ArqJobRegistry] ([status], [enqueued_at] DESC)
    """))

    # Uprawnienie do podglądu rejestru — nowa, atomowa nazwa
    op.execute(text(f"""
        MERGE [{SCHEMA}].[skw_Permissions] AS target
        USING (
            SELECT
                N'system.view_job_queue' AS PermissionName,
                N'Podglad rejestru zadan ARQ (panel admina) — statusy wysylek i zadan tla' AS Description,
                N'system' AS Category
        ) AS source
        ON target.[PermissionName] = source.[PermissionName]
        WHEN NOT MATCHED THEN
            INSERT ([PermissionName], [Description], [Category], [IsActive])
            VALUES (source.[PermissionName], source.[Description], source.[Category], 1);
    """))

    op.execute(text(f"""
        INSERT INTO [{SCHEMA}].[skw_RolePermissions] ([ID_ROLE], [ID_PERMISSION])
        SELECT r.[ID_ROLE], p.[ID_PERMISSION]
        FROM [{SCHEMA}].[skw_Roles] r
        CROSS JOIN [{SCHEMA}].[skw_Permissions] p
        WHERE r.[RoleName] = N'admin'
          AND p.[PermissionName] = N'system.view_job_queue'
          AND NOT EXISTS (
              SELECT 1 FROM [{SCHEMA}].[skw_RolePermissions] rp
              WHERE rp.[ID_ROLE] = r.[ID_ROLE]
                AND rp.[ID_PERMISSION] = p.[ID_PERMISSION]
          );
    """))


def downgrade() -> None:
    op.execute(text(f"""
        DELETE rp
        FROM [{SCHEMA}].[skw_RolePermissions] rp
        JOIN [{SCHEMA}].[skw_Permissions] p ON p.[ID_PERMISSION] = rp.[ID_PERMISSION]
        WHERE p.[PermissionName] = N'system.view_job_queue';
    """))
    op.execute(text(f"""
        DELETE FROM [{SCHEMA}].[skw_Permissions]
        WHERE [PermissionName] = N'system.view_job_queue';
    """))
    op.execute(text(f"""
        IF EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'{SCHEMA}' AND t.name = N'skw_ArqJobRegistry'
        )
        DROP TABLE [{SCHEMA}].[skw_ArqJobRegistry]
    """))