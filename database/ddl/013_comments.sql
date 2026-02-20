-- ============================================================
-- Tabela: dbo_ext.Comments
-- Notatki pracowników do kontrahentów.
-- v1.5: kolumna Tresc (nie Content), UzytkownikID NOT NULL (nie ID_USER NULL)
-- ============================================================

USE [WAPRO];
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'dbo_ext' AND t.name = 'Comments'
)
BEGIN
    CREATE TABLE dbo_ext.Comments (
        ID_COMMENT      INT             NOT NULL IDENTITY(1,1),
        ID_KONTRAHENTA  INT             NOT NULL,
        Tresc           NVARCHAR(MAX)   NOT NULL,       -- ← polska nazwa v1.5
        UzytkownikID    INT             NOT NULL,       -- ← polska nazwa v1.5, NOT NULL
        IsActive        BIT             NOT NULL DEFAULT 1,
        CreatedAt       DATETIME        NOT NULL DEFAULT GETDATE(),
        UpdatedAt       DATETIME        NULL,

        CONSTRAINT PK_Comments PRIMARY KEY CLUSTERED (ID_COMMENT),

        -- RESTRICT: nie można usunąć usera który ma komentarze
        -- (NOT NULL wyklucza ON DELETE SET NULL)
        CONSTRAINT FK_Comments_User
            FOREIGN KEY (UzytkownikID)
            REFERENCES dbo_ext.Users(ID_USER)
            ON DELETE NO ACTION
            ON UPDATE NO ACTION

        -- Brak FK na ID_KONTRAHENTA — WAPRO jest read-only (inny schemat)
    );

    PRINT 'Tabela dbo_ext.Comments utworzona (v1.5: Tresc, UzytkownikID NOT NULL).';
END
ELSE
BEGIN
    PRINT 'Tabela dbo_ext.Comments już istnieje — pominięto.';
END
GO

-- Indeksy zgodne z dokumentacją v1.5
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Comments_Kontrahent')
    CREATE INDEX IX_Comments_Kontrahent ON dbo_ext.Comments (ID_KONTRAHENTA);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Comments_Uzytkownik')
    CREATE INDEX IX_Comments_Uzytkownik ON dbo_ext.Comments (UzytkownikID);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Comments_CreatedAt')
    CREATE INDEX IX_Comments_CreatedAt ON dbo_ext.Comments (CreatedAt DESC);
GO

-- Indeks partial: szybkie pobieranie aktywnych komentarzy dla kontrahenta
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Comments_Kontrahent_Active')
    CREATE INDEX IX_Comments_Kontrahent_Active
        ON dbo_ext.Comments (ID_KONTRAHENTA, CreatedAt DESC)
        WHERE IsActive = 1;
GO