USE [WAPRO];
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'dbo_ext' AND t.name = 'SystemConfig'
)
BEGIN
    CREATE TABLE dbo_ext.SystemConfig (
        ID_CONFIG   INT             NOT NULL IDENTITY(1,1),
        ConfigKey   NVARCHAR(100)   NOT NULL,
        ConfigValue NVARCHAR(MAX)   NOT NULL,
        Description NVARCHAR(500)   NULL,
        IsActive    BIT             NOT NULL DEFAULT 1,
        CreatedAt   DATETIME        NOT NULL DEFAULT GETDATE(),
        UpdatedAt   DATETIME        NULL,

        CONSTRAINT PK_SystemConfig PRIMARY KEY CLUSTERED (ID_CONFIG),
        CONSTRAINT UQ_SystemConfig_Key UNIQUE (ConfigKey)
    );
    PRINT 'Tabela dbo_ext.SystemConfig utworzona.';
END
GO