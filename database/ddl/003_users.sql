-- ============================================================
-- Tabela: dbo_ext.Users
-- Użytkownicy systemu. Hasła: argon2. RBAC przez RoleID.
-- ============================================================

USE [WAPRO];
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'dbo_ext' AND t.name = 'Users'
)
BEGIN
    CREATE TABLE dbo_ext.Users (
        ID_USER                 INT             NOT NULL IDENTITY(1,1),
        Username                NVARCHAR(50)    NOT NULL,
        Email                   NVARCHAR(100)   NOT NULL,
        PasswordHash            NVARCHAR(255)   NOT NULL,
        FullName                NVARCHAR(100)   NULL,
        IsActive                BIT             NOT NULL DEFAULT 1,
        RoleID                  INT             NOT NULL,
        CreatedAt               DATETIME        NOT NULL DEFAULT GETDATE(),
        UpdatedAt               DATETIME        NULL,
        LastLoginAt             DATETIME        NULL,
        FailedLoginAttempts     INT             NOT NULL DEFAULT 0,
        LockedUntil             DATETIME        NULL,

        CONSTRAINT PK_Users PRIMARY KEY CLUSTERED (ID_USER),
        CONSTRAINT UQ_Users_Username UNIQUE (Username),
        CONSTRAINT UQ_Users_Email UNIQUE (Email),
        CONSTRAINT FK_Users_Roles FOREIGN KEY (RoleID)
            REFERENCES dbo_ext.Roles(ID_ROLE)
            ON DELETE NO ACTION ON UPDATE NO ACTION,
        CONSTRAINT CK_Users_FailedAttempts
            CHECK (FailedLoginAttempts >= 0),
        CONSTRAINT CK_Users_Email
            CHECK (Email LIKE '%_@_%.__%')
    );

    PRINT 'Tabela dbo_ext.Users utworzona.';
END
GO

CREATE INDEX IX_Users_RoleID       ON dbo_ext.Users (RoleID);
CREATE INDEX IX_Users_IsActive     ON dbo_ext.Users (IsActive);
CREATE INDEX IX_Users_LastLoginAt  ON dbo_ext.Users (LastLoginAt DESC);
GO