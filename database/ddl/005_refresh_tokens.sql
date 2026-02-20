USE [WAPRO];
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'dbo_ext' AND t.name = 'RefreshTokens'
)
BEGIN
    CREATE TABLE dbo_ext.RefreshTokens (
        ID_TOKEN    INT             NOT NULL IDENTITY(1,1),
        ID_USER     INT             NOT NULL,
        Token       NVARCHAR(500)   NOT NULL,
        ExpiresAt   DATETIME        NOT NULL,
        CreatedAt   DATETIME        NOT NULL DEFAULT GETDATE(),
        IsRevoked   BIT             NOT NULL DEFAULT 0,
        RevokedAt   DATETIME        NULL,
        IPAddress   NVARCHAR(45)    NULL,
        UserAgent   NVARCHAR(500)   NULL,

        CONSTRAINT PK_RefreshTokens PRIMARY KEY CLUSTERED (ID_TOKEN),
        CONSTRAINT FK_RefreshTokens_User
            FOREIGN KEY (ID_USER)
            REFERENCES dbo_ext.Users(ID_USER)
            ON DELETE CASCADE
    );
    PRINT 'Tabela dbo_ext.RefreshTokens utworzona.';
END
GO

CREATE INDEX IX_RefreshTokens_User      ON dbo_ext.RefreshTokens (ID_USER);
CREATE INDEX IX_RefreshTokens_Token     ON dbo_ext.RefreshTokens (Token);
CREATE INDEX IX_RefreshTokens_ExpiresAt ON dbo_ext.RefreshTokens (ExpiresAt);
GO