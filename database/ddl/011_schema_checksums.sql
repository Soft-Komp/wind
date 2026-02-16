USE [WAPRO];
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'dbo_ext' AND t.name = 'SchemaChecksums'
)
BEGIN
    CREATE TABLE dbo_ext.SchemaChecksums (
        ID_CHECKSUM     INT             NOT NULL IDENTITY(1,1),
        ObjectName      NVARCHAR(200)   NOT NULL,
        ObjectType      NVARCHAR(50)    NOT NULL,
        Checksum        INT             NOT NULL,
        AlembicRevision NVARCHAR(50)    NULL,
        LastVerifiedAt  DATETIME        NULL,
        CreatedAt       DATETIME        NOT NULL DEFAULT GETDATE(),
        UpdatedAt       DATETIME        NULL,

        CONSTRAINT PK_SchemaChecksums PRIMARY KEY CLUSTERED (ID_CHECKSUM),
        CONSTRAINT CK_SchemaChecksums_Type
            CHECK (ObjectType IN ('VIEW', 'PROCEDURE'))
    );
    PRINT 'Tabela dbo_ext.SchemaChecksums utworzona.';
END
GO