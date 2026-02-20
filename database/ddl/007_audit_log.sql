USE [WAPRO];
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'dbo_ext' AND t.name = 'AuditLog'
)
BEGIN
    CREATE TABLE dbo_ext.AuditLog (
        ID_LOG          BIGINT          NOT NULL IDENTITY(1,1),
        ID_USER         INT             NULL,
        Username        NVARCHAR(50)    NULL,
        Action          NVARCHAR(100)   NOT NULL,
        ActionCategory  NVARCHAR(50)    NULL,
        EntityType      NVARCHAR(50)    NULL,
        EntityID        INT             NULL,
        OldValue        NVARCHAR(MAX)   NULL,
        NewValue        NVARCHAR(MAX)   NULL,
        Details         NVARCHAR(MAX)   NULL,
        IPAddress       NVARCHAR(45)    NULL,
        UserAgent       NVARCHAR(500)   NULL,
        RequestURL      NVARCHAR(500)   NULL,
        RequestMethod   NVARCHAR(10)    NULL,
        Timestamp       DATETIME        NOT NULL DEFAULT GETDATE(),
        Success         BIT             NOT NULL DEFAULT 1,
        ErrorMessage    NVARCHAR(500)   NULL,

        CONSTRAINT PK_AuditLog PRIMARY KEY CLUSTERED (ID_LOG),
        CONSTRAINT FK_AuditLog_User
            FOREIGN KEY (ID_USER)
            REFERENCES dbo_ext.Users(ID_USER)
            ON DELETE SET NULL,
        CONSTRAINT CK_AuditLog_Method
            CHECK (RequestMethod IN ('GET','POST','PUT','DELETE','PATCH', NULL))
    );
    PRINT 'Tabela dbo_ext.AuditLog utworzona.';
END
GO

CREATE INDEX IX_AuditLog_User       ON dbo_ext.AuditLog (ID_USER);
CREATE INDEX IX_AuditLog_Timestamp  ON dbo_ext.AuditLog (Timestamp DESC);
CREATE INDEX IX_AuditLog_Action     ON dbo_ext.AuditLog (Action);
CREATE INDEX IX_AuditLog_Entity     ON dbo_ext.AuditLog (EntityType, EntityID);
CREATE INDEX IX_AuditLog_Success    ON dbo_ext.AuditLog (Success)
    WHERE Success = 0;  -- Partial index — szybkie szukanie błędów
GO