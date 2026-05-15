"""
0026_schema_transfer

Przeniesienie wszystkich tabel skw_* ze schematu dbo_ext do dbo.

ZMIANA WZGLEDEM POPRZEDNIEJ WERSJI:
  Widoki sa DROPOWANE przed transferem tabel, a nastepnie odtwarzane
  po transferze. Cale execute w jednym bloku SQL (jeden op.execute),
  dzieki czemu MSSQL widzi wszystkie zmiany w tym samym kontekscie.

KOLEJNOSC:
  A. Zapisz definicje widokow referencujacych dbo_ext -> #vw_defs
  B. DROP tych widokow
  C. Zbierz FK recreate scripts -> #fk_scripts
  D. Drop FK constraints
  E. ALTER SCHEMA dbo TRANSFER dbo_ext.[skw_*]  (wszystkie tabele)
  F. Odtworz FK constraints
  G. Odtworz widoki (teraz referencujace dbo.skw_*)
  H. UPDATE skw_SchemaChecksums (SchemaName dbo_ext -> dbo)
  I. DROP SCHEMA dbo_ext (jesli pusty)
  J. Weryfikacja koncowa

Revision: 0026
Revises:  0025
"""

import logging
from datetime import datetime, timezone

from alembic import op

logger = logging.getLogger("alembic.0026_schema_transfer")

revision      = "0026"
down_revision = "0025"
branch_labels = None
depends_on    = None


def _log(msg):
    logger.info("[0026] %s | ts=%s", msg, datetime.now(timezone.utc).isoformat())


def upgrade():
    _log("START upgrade — DROP/TRANSFER/RECREATE w jednym bloku SQL")

    op.execute("""
        -- ── Idempotentnosc: jesli tabele juz przeniesione — konczymy ────────
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'dbo_ext' AND t.name LIKE N'skw_%'
        )
        BEGIN
            PRINT '0026: Tabele skw_* nie sa w dbo_ext — migracja juz wykonana lub niepotrzebna. Konczymy.';
            RETURN;
        END;

        PRINT '0026: START — wykryto tabele skw_* w dbo_ext, wykonuje transfer';

        -- ── Temp tabele ──────────────────────────────────────────────────────
        IF OBJECT_ID('tempdb..#vw_defs')    IS NOT NULL DROP TABLE #vw_defs;
        IF OBJECT_ID('tempdb..#fk_scripts') IS NOT NULL DROP TABLE #fk_scripts;

        CREATE TABLE #vw_defs (
            id           INT IDENTITY(1,1) PRIMARY KEY,
            vw_schema    NVARCHAR(128),
            vw_name      NVARCHAR(256),
            original_def NVARCHAR(MAX)
        );
        CREATE TABLE #fk_scripts (
            id           INT IDENTITY(1,1) PRIMARY KEY,
            fk_name      NVARCHAR(256),
            child_table  NVARCHAR(256),
            recreate_sql NVARCHAR(MAX)
        );

        -- ════════════════════════════════════════════════════════════════════
        -- KROK A: Zapisz definicje widokow referencujacych dbo_ext
        -- Szukamy w obu schematach (dbo i dbo_ext) na wszelki wypadek
        -- ════════════════════════════════════════════════════════════════════
        INSERT INTO #vw_defs (vw_schema, vw_name, original_def)
        SELECT
            SCHEMA_NAME(o.schema_id),
            o.name,
            sm.definition
        FROM sys.sql_modules sm
        JOIN sys.objects     o ON sm.object_id = o.object_id
        WHERE o.type = 'V'
          AND sm.definition LIKE N'%dbo_ext%';

        PRINT 'KROK A: Zapisano ' + CAST(@@ROWCOUNT AS NVARCHAR(20)) + ' definicji widokow do odtworzenia';

        -- ════════════════════════════════════════════════════════════════════
        -- KROK B: DROP widokow referencujacych dbo_ext
        -- DROP musi byc przed transferem tabel — unikamy problemow z
        -- WITH SCHEMABINDING i walidacja referencji przy CREATE OR ALTER VIEW
        -- ════════════════════════════════════════════════════════════════════
        DECLARE @vw_s   NVARCHAR(128);
        DECLARE @vw_n   NVARCHAR(256);
        DECLARE @vw_dropped INT = 0;

        DECLARE vw_drop_cur CURSOR FAST_FORWARD FOR
            SELECT vw_schema, vw_name FROM #vw_defs ORDER BY id;

        OPEN vw_drop_cur;
        FETCH NEXT FROM vw_drop_cur INTO @vw_s, @vw_n;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @drop_sql NVARCHAR(512) =
                N'IF OBJECT_ID(N''' + @vw_s + N'.' + @vw_n + N''', N''V'') IS NOT NULL '
                + N'DROP VIEW [' + @vw_s + N'].[' + @vw_n + N'];';
            EXEC sp_executesql @drop_sql;
            PRINT 'KROK B: Dropped [' + @vw_s + N'].[' + @vw_n + N']';
            SET @vw_dropped = @vw_dropped + 1;
            FETCH NEXT FROM vw_drop_cur INTO @vw_s, @vw_n;
        END;
        CLOSE vw_drop_cur;
        DEALLOCATE vw_drop_cur;

        PRINT 'KROK B: Usunieto ' + CAST(@vw_dropped AS NVARCHAR(20)) + ' widokow';

        -- ════════════════════════════════════════════════════════════════════
        -- KROK C: Zbierz FK recreate scripts
        -- ════════════════════════════════════════════════════════════════════
        INSERT INTO #fk_scripts (fk_name, child_table, recreate_sql)
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
        WHERE sp.name = N'dbo_ext' AND tp.name LIKE N'skw_%';

        PRINT 'KROK C: Zebrano ' + CAST(@@ROWCOUNT AS NVARCHAR(20)) + ' FK do odtworzenia';

        -- ════════════════════════════════════════════════════════════════════
        -- KROK D: Drop FK constraints
        -- ════════════════════════════════════════════════════════════════════
        DECLARE @drop_fks NVARCHAR(MAX) = N'';
        SELECT @drop_fks +=
            N'ALTER TABLE [dbo].[' + t.name + N'] DROP CONSTRAINT [' + fk.name + N'];' + CHAR(10)
        FROM sys.foreign_keys fk
        JOIN sys.tables  t ON fk.parent_object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id          = s.schema_id
        WHERE s.name = N'dbo_ext' AND t.name LIKE N'skw_%';

        IF LEN(ISNULL(@drop_fks, N'')) > 0
        BEGIN
            EXEC sp_executesql @drop_fks;
            PRINT 'KROK D: FK constraints usuniete';
        END
        ELSE
            PRINT 'KROK D: Brak FK do usuniecia';

        -- ════════════════════════════════════════════════════════════════════
        -- KROK E: ALTER SCHEMA dbo TRANSFER dla kazdej tabeli skw_*
        -- ════════════════════════════════════════════════════════════════════
        DECLARE @tbl_name    NVARCHAR(256);
        DECLARE @xfer_sql    NVARCHAR(512);
        DECLARE @transferred INT = 0;

        DECLARE tbl_cur CURSOR FAST_FORWARD FOR
            SELECT t.name
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'dbo_ext' AND t.name LIKE N'skw_%'
            ORDER BY t.name;

        OPEN tbl_cur;
        FETCH NEXT FROM tbl_cur INTO @tbl_name;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @xfer_sql = N'ALTER SCHEMA [dbo] TRANSFER [dbo].[' + @tbl_name + N'];';
            EXEC sp_executesql @xfer_sql;
            PRINT 'KROK E: Przeniesiono ' + @tbl_name;
            SET @transferred = @transferred + 1;
            FETCH NEXT FROM tbl_cur INTO @tbl_name;
        END;
        CLOSE tbl_cur;
        DEALLOCATE tbl_cur;

        PRINT 'KROK E: Przeniesiono ' + CAST(@transferred AS NVARCHAR(20)) + ' tabel do dbo';

        -- ════════════════════════════════════════════════════════════════════
        -- KROK F: Odtworz FK constraints (teraz wskazuja [dbo].[skw_*])
        -- ════════════════════════════════════════════════════════════════════
        DECLARE @fk_sql    NVARCHAR(MAX);
        DECLARE @fk_nm     NVARCHAR(256);
        DECLARE @fk_child  NVARCHAR(256);
        DECLARE @restored  INT = 0;

        DECLARE fk_cur CURSOR FAST_FORWARD FOR
            SELECT fk_name, child_table, recreate_sql FROM #fk_scripts ORDER BY id;

        OPEN fk_cur;
        FETCH NEXT FROM fk_cur INTO @fk_nm, @fk_child, @fk_sql;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXEC sp_executesql @fk_sql;
            PRINT 'KROK F: Odtworzono FK [' + @fk_nm + N'] na [' + @fk_child + N']';
            SET @restored = @restored + 1;
            FETCH NEXT FROM fk_cur INTO @fk_nm, @fk_child, @fk_sql;
        END;
        CLOSE fk_cur;
        DEALLOCATE fk_cur;

        PRINT 'KROK F: Odtworzono ' + CAST(@restored AS NVARCHAR(20)) + ' FK constraints';

        -- ════════════════════════════════════════════════════════════════════
        -- KROK G: Odtworz widoki (tabele sa juz w dbo)
        -- Zamieniamy [dbo]. na [dbo]. w definicjach i wykonujemy
        -- CREATE OR ALTER VIEW. Bez problemow z WITH SCHEMABINDING —
        -- tabele sa juz w dbo wiec walidacja przejdzie.
        -- ════════════════════════════════════════════════════════════════════
        DECLARE @vw_orig  NVARCHAR(MAX);
        DECLARE @vw_new   NVARCHAR(MAX);
        DECLARE @vw_s2    NVARCHAR(128);
        DECLARE @vw_n2    NVARCHAR(256);
        DECLARE @vw_done  INT = 0;

        DECLARE vw_create_cur CURSOR FAST_FORWARD FOR
            SELECT vw_schema, vw_name, original_def FROM #vw_defs ORDER BY id;

        OPEN vw_create_cur;
        FETCH NEXT FROM vw_create_cur INTO @vw_s2, @vw_n2, @vw_orig;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Zamien referencje dbo_ext -> dbo
            SET @vw_new = REPLACE(@vw_orig, N'[dbo].', N'[dbo].');
            SET @vw_new = REPLACE(@vw_new,  N'dbo_ext.',   N'dbo.');

            -- Zamien CREATE VIEW na CREATE OR ALTER VIEW
            IF @vw_new LIKE N'%CREATE VIEW%'
               AND @vw_new NOT LIKE N'%CREATE OR ALTER VIEW%'
                SET @vw_new = REPLACE(@vw_new, N'CREATE VIEW', N'CREATE OR ALTER VIEW');

            EXEC sp_executesql @vw_new;
            PRINT 'KROK G: Odtworzono widok [' + @vw_s2 + N'].[' + @vw_n2 + N']';
            SET @vw_done = @vw_done + 1;

            FETCH NEXT FROM vw_create_cur INTO @vw_s2, @vw_n2, @vw_orig;
        END;
        CLOSE vw_create_cur;
        DEALLOCATE vw_create_cur;

        PRINT 'KROK G: Odtworzono ' + CAST(@vw_done AS NVARCHAR(20)) + ' widokow';

        -- ════════════════════════════════════════════════════════════════════
        -- KROK H: Update skw_SchemaChecksums
        -- Tabela jest juz w dbo (KROK E ja przeniosl)
        -- ════════════════════════════════════════════════════════════════════
        IF OBJECT_ID(N'[dbo].[skw_SchemaChecksums]', N'U') IS NOT NULL
        BEGIN
            UPDATE [dbo].[skw_SchemaChecksums]
            SET    [SchemaName] = N'dbo'
            WHERE  [SchemaName] = N'dbo_ext';
            PRINT 'KROK H: SchemaChecksums zaktualizowano — '
                + CAST(@@ROWCOUNT AS NVARCHAR(20)) + ' wpisow';
        END
        ELSE
            PRINT 'KROK H: skw_SchemaChecksums nie znaleziono — pomijam';

        -- ════════════════════════════════════════════════════════════════════
        -- KROK I: DROP SCHEMA dbo_ext (tylko jesli pusty)
        -- ════════════════════════════════════════════════════════════════════
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
                PRINT 'KROK I: Schemat dbo_ext usuniety';
            END
            ELSE
                PRINT 'KROK I: dbo_ext nie jest pusty ('
                    + CAST(@obj_cnt AS NVARCHAR(20)) + ' obj) — pomijam DROP';
        END
        ELSE
            PRINT 'KROK I: Schemat dbo_ext juz nie istnieje';

        -- ════════════════════════════════════════════════════════════════════
        -- KROK J: Weryfikacja koncowa
        -- ════════════════════════════════════════════════════════════════════
        DECLARE @in_dbo  INT;
        DECLARE @in_ext  INT;
        DECLARE @broken  INT;

        SELECT @in_dbo = COUNT(*) FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = N'dbo' AND t.name LIKE N'skw_%';

        SELECT @in_ext = COUNT(*) FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = N'dbo_ext' AND t.name LIKE N'skw_%';

        SELECT @broken = COUNT(*) FROM sys.sql_modules sm
        JOIN sys.objects o ON sm.object_id = o.object_id
        JOIN sys.schemas s ON o.schema_id  = s.schema_id
        WHERE s.name = N'dbo' AND o.type = 'V'
          AND sm.definition LIKE N'%dbo_ext%';

        PRINT '=== WERYFIKACJA ===';
        PRINT '  Tabele skw_* w dbo:     ' + CAST(@in_dbo AS NVARCHAR(20));
        PRINT '  Tabele skw_* w dbo_ext: ' + CAST(@in_ext AS NVARCHAR(20)) + ' (oczekiwane: 0)';
        PRINT '  Widoki broken:          ' + CAST(@broken AS NVARCHAR(20)) + ' (oczekiwane: 0)';

        IF @in_ext > 0
            RAISERROR('WERYFIKACJA NIEUDANA: %d tabel skw_* pozostalo w dbo_ext.', 16, 1, @in_ext);
        IF @broken > 0
            RAISERROR('WERYFIKACJA NIEUDANA: %d widokow nadal referencuje dbo_ext.', 16, 1, @broken);
        IF @in_dbo = 0
            RAISERROR('WERYFIKACJA NIEUDANA: 0 tabel skw_* w dbo — transfer nie wykonal sie.', 16, 1);

        PRINT '=== WERYFIKACJA: PASSED ===';

        -- Cleanup
        DROP TABLE #vw_defs;
        DROP TABLE #fk_scripts;

        PRINT '0026: UPGRADE ZAKOŃCZONY POMYŚLNIE';
    """)

    _log("UPGRADE ZAKOŃCZONY — tabele skw_* przeniesione do dbo")


def downgrade():
    raise NotImplementedError(
        "\n"
        "================================================================\n"
        "DOWNGRADE 0026 JEST NIEDOSTEPNY\n"
        "================================================================\n"
        "Transfer dbo_ext -> dbo jest nieodwracalny automatycznie.\n"
        "DANE SA BEZPIECZNE — wszystkie tabele skw_* sa w dbo.\n"
        "\n"
        "Aby cofnac recznie (tylko dev/test):\n"
        "  1. CREATE SCHEMA dbo_ext\n"
        "  2. Odtworz widoki ze starymi referencjami (dbo_ext.skw_*)\n"
        "  3. ALTER SCHEMA dbo_ext TRANSFER dbo.[skw_*] dla kazdej tabeli\n"
        "  4. Usun rekord revision='0026' z dbo.alembic_version\n"
        "  5. Cofnij schema='dbo' -> 'dbo_ext' w modelach Python\n"
        "================================================================\n"
    )