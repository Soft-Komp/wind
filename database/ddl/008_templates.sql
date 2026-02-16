USE [WAPRO];
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'dbo_ext' AND t.name = 'Templates'
)
BEGIN
    CREATE TABLE dbo_ext.Templates (
        ID_TEMPLATE     INT             NOT NULL IDENTITY(1,1),
        TemplateName    NVARCHAR(100)   NOT NULL,
        TemplateType    NVARCHAR(20)    NOT NULL,
        Subject         NVARCHAR(200)   NULL,
        Body            NVARCHAR(MAX)   NOT NULL,
        IsActive        BIT             NOT NULL DEFAULT 1,
        CreatedAt       DATETIME        NOT NULL DEFAULT GETDATE(),
        UpdatedAt       DATETIME        NULL,

        CONSTRAINT PK_Templates PRIMARY KEY CLUSTERED (ID_TEMPLATE),
        CONSTRAINT UQ_Templates_Name UNIQUE (TemplateName),
        CONSTRAINT CK_Templates_Type
            CHECK (TemplateType IN ('email', 'sms', 'print'))
    );
    PRINT 'Tabela dbo_ext.Templates utworzona.';
END
GO

CREATE INDEX IX_Templates_Type ON dbo_ext.Templates (TemplateType)
    WHERE IsActive = 1;
GO