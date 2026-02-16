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
        ID_USER         INT             NULL,
        Content         NVARCHAR(MAX)   NOT NULL,
        IsActive        BIT             NOT NULL DEFAULT 1,
        CreatedAt       DATETIME        NOT NULL DEFAULT GETDATE(),
        UpdatedAt       DATETIME        NULL,

        CONSTRAINT PK_Comments PRIMARY KEY CLUSTERED (ID_COMMENT),
        CONSTRAINT FK_Comments_User
            FOREIGN KEY (ID_USER)
            REFERENCES dbo_ext.Users(ID_USER)
            ON DELETE SET NULL
        -- Brak FK na ID_KONTRAHENTA — WAPRO jest read-only
    );
    PRINT 'Tabela dbo_ext.Comments utworzona.';
END
GO

CREATE INDEX IX_Comments_Kontrahent ON dbo_ext.Comments (ID_KONTRAHENTA);
CREATE INDEX IX_Comments_User       ON dbo_ext.Comments (ID_USER);
CREATE INDEX IX_Comments_CreatedAt  ON dbo_ext.Comments (CreatedAt DESC);
GO