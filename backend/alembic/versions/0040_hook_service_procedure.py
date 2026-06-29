# backend/alembic/versions/0040_hook_service_procedure.py
"""0040 — Procedura SQL dla hooka Fakira + seed hooka w skw_source_hooks

Kroki:
  1. CREATE OR ALTER PROCEDURE dbo.skw_AktualizujStatusFaktury
     Kontrakt: SELECT status, message, refresh_document (1 wiersz)
     Wywolywana przez HookService po akcji accepted/rejected na instancji
     ktora pochodzi ze zrodla fakir.

  2. Health check — test wywolania procedury z ksef_id=__health_check__
     Weryfikuje ze procedura zostala poprawnie zainstalowana.

  3. MERGE skw_source_hooks — seed hooka krytycznego dla Fakira
     trigger_action=accepted, severity=critical, operation_type=sql_procedure
     Idempotentny — bezpieczny przy wielokrotnym uruchomieniu.

Revision ID: 0040
Revises:     0039
Create Date: 2026-06-26
"""

from alembic import op
from sqlalchemy import text

revision      = "0040"
down_revision = "0039"
branch_labels = None
depends_on    = None

# Procedura musi zwracac dokladnie 3 kolumny w dokladnie 1 wierszu:
#   status            NVARCHAR — 'success' | 'error' | 'warning'
#   message           NVARCHAR — komunikat dla uzytkownika
#   refresh_document  BIT      — 1 = frontend powinien odswiezac dane dokumentu
#
# SET NOCOUNT ON jest wymagane — bez tego pyodbc liczy rowcount z UPDATE
# i EXEC zwraca niepoprawny wynik.

PROCEDURE_SQL = """
CREATE OR ALTER PROCEDURE [dbo].[skw_AktualizujStatusFaktury]
    @ksef_id NVARCHAR(200),
    @action  NVARCHAR(30)
AS
BEGIN
    SET NOCOUNT ON;

    IF @ksef_id IS NULL OR LEN(RTRIM(LTRIM(@ksef_id))) = 0
    BEGIN
        SELECT
            N'error'                        AS status,
            N'Parametr ksef_id jest pusty'  AS message,
            0                               AS refresh_document;
        RETURN;
    END;

    IF @action NOT IN (N'accepted', N'rejected')
    BEGIN
        SELECT
            N'error'                                        AS status,
            N'Nieznana akcja: ' + ISNULL(@action, N'')     AS message,
            0                                               AS refresh_document;
        RETURN;
    END;

    DECLARE @nowy_status NVARCHAR(30);
    SET @nowy_status = CASE @action
        WHEN N'accepted' THEN N'zaakceptowana'
        WHEN N'rejected' THEN N'w_toku'
        ELSE NULL
    END;

    BEGIN TRY
        UPDATE [dbo].[skw_faktura_akceptacja]
           SET [status_wewnetrzny] = @nowy_status,
               [UpdatedAt]         = GETDATE()
         WHERE [numer_ksef] = @ksef_id
           AND [IsActive]   = 1;

        SELECT
            N'success'                                  AS status,
            N'Status zaktualizowany: ' + @nowy_status  AS message,
            1                                           AS refresh_document;

    END TRY
    BEGIN CATCH
        SELECT
            N'error'                                    AS status,
            N'Blad UPDATE: ' + ERROR_MESSAGE()         AS message,
            0                                           AS refresh_document;
    END CATCH;
END;
"""


def upgrade() -> None:
    # ── 1. Procedura SQL ─────────────────────────────────────────────────────
    op.execute(text(PROCEDURE_SQL))

    # ── 2. Health check ───────────────────────────────────────────────────────
    # Testuje ze procedura odpowiada poprawnie dla nieistniejacego ksef_id.
    # Oczekiwany wynik: status='success', refresh_document=1
    # (UPDATE bez trafienia = 0 rows affected, ale procedura nie rozroznia)
    op.execute(text(
        "EXEC [dbo].[skw_AktualizujStatusFaktury] "
        "@ksef_id = N'__health_check__', @action = N'accepted'"
    ))

    # ── 3. Seed hooka Fakira ──────────────────────────────────────────────────
    # MERGE idempotentny — jeden aktywny hook per (id_source, trigger_action)
    # Placeholdery w operation_config:
    #   {extra.ksef_id} — KSEF_ID dokumentu z extra_data instancji
    #   {action}        — nazwa akcji ('accepted' lub 'rejected')
    op.execute(text("""
        MERGE [dbo].[skw_source_hooks] AS target
        USING (
            SELECT
                (
                    SELECT [id_source]
                    FROM   [dbo].[skw_document_sources]
                    WHERE  [source_name] = N'fakir'
                ) AS id_source,
                N'accepted'      AS trigger_action,
                N'sql_procedure' AS operation_type,
                N'{"procedure_name":"dbo.skw_AktualizujStatusFaktury","params":{"ksef_id":"{extra.ksef_id}","action":"{action}"},"timeout_seconds":30}' AS operation_config,
                N'critical'      AS severity,
                1                AS is_active
        ) AS source
        ON  target.[id_source]      = source.[id_source]
        AND target.[trigger_action] = source.[trigger_action]
        AND target.[is_active]      = 1
        WHEN NOT MATCHED THEN
            INSERT (
                [id_source], [trigger_action], [operation_type],
                [operation_config], [severity], [is_active]
            )
            VALUES (
                source.[id_source], source.[trigger_action], source.[operation_type],
                source.[operation_config], source.[severity], source.[is_active]
            );
    """))


def downgrade() -> None:
    # Dezaktywuj hook — nie usuwamy (historia logów w skw_source_action_log jest cenna)
    op.execute(text("""
        UPDATE [dbo].[skw_source_hooks]
        SET    [is_active] = 0
        WHERE  [trigger_action] = N'accepted'
          AND  [id_source] = (
                   SELECT [id_source]
                   FROM   [dbo].[skw_document_sources]
                   WHERE  [source_name] = N'fakir'
               )
    """))
    # Procedura celowo pozostawiona — DROP wymaga jawnej decyzji admina