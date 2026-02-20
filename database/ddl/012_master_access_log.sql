USE [WAPRO];
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'dbo_ext' AND t.name = 'MasterAccessLog'
)
BEGIN
    CREATE TABLE dbo_ext.MasterAccessLog (
        ID_LOG          BIGINT          NOT NULL IDENTITY(1,1),
        TargetUserID    INT             NULL,
        TargetUsername  NVARCHAR(50)    NOT NULL,
        IPAddress       NVARCHAR(45)    NOT NULL,
        UserAgent       NVARCHAR(500)   NULL,
        AccessedAt      DATETIME        NOT NULL DEFAULT GETDATE(),
        SessionEndedAt  DATETIME        NULL,
        Notes           NVARCHAR(500)   NULL,

        CONSTRAINT PK_MasterAccessLog PRIMARY KEY CLUSTERED (ID_LOG),
        CONSTRAINT FK_MasterAccessLog_User
            FOREIGN KEY (TargetUserID)
            REFERENCES dbo_ext.Users(ID_USER)
            ON DELETE SET NULL
    );
    PRINT 'Tabela dbo_ext.MasterAccessLog utworzona.';
END
GO

CREATE INDEX IX_MasterLog_AccessedAt ON dbo_ext.MasterAccessLog (AccessedAt DESC);
GO

-- WAŻNE: Zabezpieczenie uprawnień dla użytkownika aplikacji
-- Odkomentuj i dostosuj nazwę użytkownika:
-- DENY SELECT ON dbo_ext.MasterAccessLog TO [windykacja_app_user];
-- DENY UPDATE ON dbo_ext.MasterAccessLog TO [windykacja_app_user];
-- DENY DELETE ON dbo_ext.MasterAccessLog TO [windykacja_app_user];
-- GRANT INSERT ON dbo_ext.MasterAccessLog TO [windykacja_app_user];