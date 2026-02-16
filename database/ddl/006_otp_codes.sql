USE [WAPRO];
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'dbo_ext' AND t.name = 'OtpCodes'
)
BEGIN
    CREATE TABLE dbo_ext.OtpCodes (
        ID_OTP      INT             NOT NULL IDENTITY(1,1),
        ID_USER     INT             NOT NULL,
        Code        NVARCHAR(10)    NOT NULL,
        Purpose     NVARCHAR(20)    NOT NULL,
        ExpiresAt   DATETIME        NOT NULL,
        IsUsed      BIT             NOT NULL DEFAULT 0,
        CreatedAt   DATETIME        NOT NULL DEFAULT GETDATE(),
        IPAddress   NVARCHAR(45)    NULL,

        CONSTRAINT PK_OtpCodes PRIMARY KEY CLUSTERED (ID_OTP),
        CONSTRAINT FK_OtpCodes_User
            FOREIGN KEY (ID_USER)
            REFERENCES dbo_ext.Users(ID_USER)
            ON DELETE CASCADE,
        CONSTRAINT CK_OtpCodes_Purpose
            CHECK (Purpose IN ('password_reset', '2fa'))
    );
    PRINT 'Tabela dbo_ext.OtpCodes utworzona.';
END
GO

CREATE INDEX IX_OtpCodes_User      ON dbo_ext.OtpCodes (ID_USER);
CREATE INDEX IX_OtpCodes_ExpiresAt ON dbo_ext.OtpCodes (ExpiresAt);
GO