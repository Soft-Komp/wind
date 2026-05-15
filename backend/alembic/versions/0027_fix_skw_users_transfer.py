"""
0027_fix_skw_users_transfer

Przeniesienie brakujacej tabeli skw_Users z dbo_ext do dbo.

PRZYCZYNA:
  Migracja 0026 przeniosla 18/19 tabel. skw_Users pozostala w dbo_ext
  poniewaz KROK F (odtworzenie FK) proba tworzenia FK wskazujacych na
  dbo.skw_Users (ktora wtedy jeszcze nie istniala w dbo) cicho sie nie powiodla.
  SQL Server z SET XACT_ABORT OFF kontynuowal petle — tabela nie zostala
  przeniesiona, ale migracja sie "udala".

ZAKRES:
  A. Zbierz FK scripts z/do skw_Users (dbo_ext.skw_Users)
  B. Drop FK constraints (jezeli jakies istnieja)
  C. ALTER SCHEMA dbo TRANSFER dbo_ext.skw_Users
  D. Odtworz FK constraints (dbo.skw_Users <-> dbo.skw_*)
  E. DROP SCHEMA dbo_ext jezeli teraz pusty
  F. Weryfikacja

Revision: 0027
Revises:  0026
"""

import logging
from datetime import datetime, timezone

from alembic import op

logger = logging.getLogger("alembic.0027_fix_skw_users_transfer")

revision      = "0027"
down_revision = "0026"
branch_labels = None
depends_on    = None


def _log(msg):
    logger.info("[0027] %s | ts=%s", msg, datetime.now(timezone.utc).isoformat())


def upgrade():
    _log("START upgrade — transfer brakujacej tabeli skw_Users")

    op.execute("""
        -- Idempotentnosc: jesli juz w dbo — konczymy
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'dbo_ext' AND t.name = N'skw_Users'
        )
        BEGIN
            PRINT '0027: skw_Users juz jest w dbo lub nie istnieje — pomijam';
            RETURN;
        END;

        PRINT '0027: Znaleziono dbo_ext.skw_Users — przenoszenie...';

        -- Temp tabela na FK scripts
        IF OBJECT_ID('tempdb..#fk27') IS NOT NULL DROP TABLE #fk27;

        CREATE TABLE #fk27 (
            id           INT IDENTITY(1,1) PRIMARY KEY,
            fk_name      NVARCHAR(256),
            child_table  NVARCHAR(256),
            recreate_sql NVARCHAR(MAX)
        );

        -- ── KROK A: Zbierz FK scripts ─────────────────────────────────────
        -- 1) FKs WYCHODZACE z skw_Users (np. RoleID -> skw_Roles)
        INSERT INTO #fk27 (fk_name, child_table, recreate_sql)
        SELECT
            fk.name,
            tp.name,
            N'ALTER TABLE [dbo].[' + tp.name + N'] ADD CONSTRAINT [' + fk.name + N'] ' +
            N'FOREIGN KEY (' +
                STUFF((
                    SELECT N',' + QUOTENAME(cp.name)
                    FROM sys.foreign_key_columns fkc2
                    JOIN sys.columns cp
                        ON fkc2.parent_object_id = cp.object_id
                       AND fkc2.parent_column_id = cp.column_id
                    WHERE fkc2.constraint_object_id = fk.object_id
                    ORDER BY fkc2.constraint_column_id
                    FOR XML PATH(N''), TYPE
                ).value(N'.', N'NVARCHAR(MAX)'), 1, 1, N'') +
            N') REFERENCES [dbo].[' + tr.name + N'] (' +
                STUFF((
                    SELECT N',' + QUOTENAME(cr.name)
                    FROM sys.foreign_key_columns fkc3
                    JOIN sys.columns cr
                        ON fkc3.referenced_object_id = cr.object_id
                       AND fkc3.referenced_column_id = cr.column_id
                    WHERE fkc3.constraint_object_id = fk.object_id
                    ORDER BY fkc3.constraint_column_id
                    FOR XML PATH(N''), TYPE
                ).value(N'.', N'NVARCHAR(MAX)'), 1, 1, N'') +
            N') ON DELETE ' +
                CASE fk.delete_referential_action
                    WHEN 0 THEN N'NO ACTION' WHEN 1 THEN N'CASCADE'
                    WHEN 2 THEN N'SET NULL'   WHEN 3 THEN N'SET DEFAULT'
                    ELSE N'NO ACTION' END +
            N' ON UPDATE ' +
                CASE fk.update_referential_action
                    WHEN 0 THEN N'NO ACTION' WHEN 1 THEN N'CASCADE'
                    WHEN 2 THEN N'SET NULL'   WHEN 3 THEN N'SET DEFAULT'
                    ELSE N'NO ACTION' END + N';'
        FROM sys.foreign_keys fk
        JOIN sys.tables  tp ON fk.parent_object_id     = tp.object_id
        JOIN sys.schemas sp ON tp.schema_id             = sp.schema_id
        JOIN sys.tables  tr ON fk.referenced_object_id = tr.object_id
        JOIN sys.schemas sr ON tr.schema_id             = sr.schema_id
        WHERE sp.name = N'dbo_ext' AND tp.name = N'skw_Users';

        -- 2) FKs z tabel dbo.* wskazujace na dbo_ext.skw_Users (cross-schema)
        INSERT INTO #fk27 (fk_name, child_table, recreate_sql)
        SELECT
            fk.name,
            tp.name,
            N'ALTER TABLE [dbo].[' + tp.name + N'] ADD CONSTRAINT [' + fk.name + N'] ' +
            N'FOREIGN KEY (' +
                STUFF((
                    SELECT N',' + QUOTENAME(cp.name)
                    FROM sys.foreign_key_columns fkc2
                    JOIN sys.columns cp
                        ON fkc2.parent_object_id = cp.object_id
                       AND fkc2.parent_column_id = cp.column_id
                    WHERE fkc2.constraint_object_id = fk.object_id
                    ORDER BY fkc2.constraint_column_id
                    FOR XML PATH(N''), TYPE
                ).value(N'.', N'NVARCHAR(MAX)'), 1, 1, N'') +
            N') REFERENCES [dbo].[skw_Users] (' +
                STUFF((
                    SELECT N',' + QUOTENAME(cr.name)
                    FROM sys.foreign_key_columns fkc3
                    JOIN sys.columns cr
                        ON fkc3.referenced_object_id = cr.object_id
                       AND fkc3.referenced_column_id = cr.column_id
                    WHERE fkc3.constraint_object_id = fk.object_id
                    ORDER BY fkc3.constraint_column_id
                    FOR XML PATH(N''), TYPE
                ).value(N'.', N'NVARCHAR(MAX)'), 1, 1, N'') +
            N') ON DELETE ' +
                CASE fk.delete_referential_action
                    WHEN 0 THEN N'NO ACTION' WHEN 1 THEN N'CASCADE'
                    WHEN 2 THEN N'SET NULL'   WHEN 3 THEN N'SET DEFAULT'
                    ELSE N'NO ACTION' END +
            N' ON UPDATE ' +
                CASE fk.update_referential_action
                    WHEN 0 THEN N'NO ACTION' WHEN 1 THEN N'CASCADE'
                    WHEN 2 THEN N'SET NULL'   WHEN 3 THEN N'SET DEFAULT'
                    ELSE N'NO ACTION' END + N';'
        FROM sys.foreign_keys fk
        JOIN sys.tables  tp ON fk.parent_object_id     = tp.object_id
        JOIN sys.schemas sp ON tp.schema_id             = sp.schema_id
        JOIN sys.tables  tr ON fk.referenced_object_id = tr.object_id
        JOIN sys.schemas sr ON tr.schema_id             = sr.schema_id
        WHERE sr.name = N'dbo_ext' AND tr.name = N'skw_Users'
          AND sp.name = N'dbo';

        PRINT '0027: Zebrano ' + CAST(@@ROWCOUNT AS NVARCHAR(20))
            + ' FK scripts (cross-schema)';

        -- ── KROK B: Drop FK constraints ───────────────────────────────────
        DECLARE @drop_sql NVARCHAR(MAX) = N'';

        -- FKs wychodzace z dbo_ext.skw_Users
        SELECT @drop_sql +=
            N'ALTER TABLE [dbo_ext].[skw_Users] DROP CONSTRAINT [' + fk.name + N'];' + CHAR(10)
        FROM sys.foreign_keys fk
        JOIN sys.tables  t ON fk.parent_object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id          = s.schema_id
        WHERE s.name = N'dbo_ext' AND t.name = N'skw_Users';

        -- FKs z dbo.* wskazujace na dbo_ext.skw_Users
        SELECT @drop_sql +=
            N'ALTER TABLE [dbo].[' + tp.name + N'] DROP CONSTRAINT [' + fk.name + N'];' + CHAR(10)
        FROM sys.foreign_keys fk
        JOIN sys.tables  tp ON fk.parent_object_id     = tp.object_id
        JOIN sys.schemas sp ON tp.schema_id             = sp.schema_id
        JOIN sys.tables  tr ON fk.referenced_object_id = tr.object_id
        JOIN sys.schemas sr ON tr.schema_id             = sr.schema_id
        WHERE sr.name = N'dbo_ext' AND tr.name = N'skw_Users'
          AND sp.name = N'dbo';

        IF LEN(ISNULL(@drop_sql, N'')) > 0
        BEGIN
            EXEC sp_executesql @drop_sql;
            PRINT '0027: FK constraints usuniete';
        END
        ELSE
            PRINT '0027: Brak FK constraints do usuniecia';

        -- ── KROK C: Transfer ──────────────────────────────────────────────
        ALTER SCHEMA [dbo] TRANSFER [dbo_ext].[skw_Users];
        PRINT '0027: dbo_ext.skw_Users przeniesione do dbo';

        -- ── KROK D: Odtworz FK constraints ────────────────────────────────
        DECLARE @fk_sql   NVARCHAR(MAX);
        DECLARE @fk_nm    NVARCHAR(256);
        DECLARE @fk_child NVARCHAR(256);
        DECLARE @restored INT = 0;

        DECLARE fk_cur CURSOR FAST_FORWARD FOR
            SELECT fk_name, child_table, recreate_sql FROM #fk27 ORDER BY id;

        OPEN fk_cur;
        FETCH NEXT FROM fk_cur INTO @fk_nm, @fk_child, @fk_sql;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRY
                EXEC sp_executesql @fk_sql;
                PRINT '0027: Odtworzono FK [' + @fk_nm + N'] na [' + @fk_child + N']';
                SET @restored = @restored + 1;
            END TRY
            BEGIN CATCH
                PRINT '0027: WARN — nie udalo sie odtworzyc FK [' + @fk_nm + N']: '
                    + ERROR_MESSAGE();
            END CATCH

            FETCH NEXT FROM fk_cur INTO @fk_nm, @fk_child, @fk_sql;
        END;
        CLOSE fk_cur;
        DEALLOCATE fk_cur;

        DROP TABLE #fk27;
        PRINT '0027: Odtworzono ' + CAST(@restored AS NVARCHAR(20)) + ' FK constraints';

        -- ── KROK E: DROP SCHEMA dbo_ext jezeli pusty ──────────────────────
        IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dbo_ext')
        BEGIN
            DECLARE @obj_cnt INT;
            SELECT @obj_cnt = COUNT(*)
            FROM sys.objects o
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = N'dbo_ext'
              AND o.type IN ('U','V','P','FN','IF','TF','TR');

            IF @obj_cnt = 0
            BEGIN
                DROP SCHEMA [dbo_ext];
                PRINT '0027: Schemat dbo_ext usuniety';
            END
            ELSE
                PRINT '0027: dbo_ext nie jest pusty ('
                    + CAST(@obj_cnt AS NVARCHAR(20)) + ' obj) — pomijam DROP';
        END

        -- ── KROK F: Weryfikacja ────────────────────────────────────────────
        DECLARE @in_dbo INT;
        DECLARE @in_ext INT;

        SELECT @in_dbo = COUNT(*) FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = N'dbo' AND t.name = N'skw_Users';

        SELECT @in_ext = COUNT(*) FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = N'dbo_ext' AND t.name = N'skw_Users';

        PRINT '=== WERYFIKACJA 0027 ===';
        PRINT '  dbo.skw_Users:     ' + CAST(@in_dbo AS NVARCHAR(20)) + ' (oczekiwane: 1)';
        PRINT '  dbo_ext.skw_Users: ' + CAST(@in_ext AS NVARCHAR(20)) + ' (oczekiwane: 0)';

        IF @in_dbo = 0
            RAISERROR('0027 NIEUDANA: skw_Users nie ma w dbo po transferze!', 16, 1);
        IF @in_ext > 0
            RAISERROR('0027 NIEUDANA: skw_Users nadal jest w dbo_ext!', 16, 1);

        PRINT '=== WERYFIKACJA 0027: PASSED ===';
        PRINT '0027: UPGRADE ZAKOŃCZONY';
    """)

    _log("UPGRADE ZAKOŃCZONY — skw_Users przeniesiona do dbo")


def downgrade():
    raise NotImplementedError(
        "Downgrade 0027 niedostepny — transfer skw_Users do dbo_ext nie jest automatyczny."
    )