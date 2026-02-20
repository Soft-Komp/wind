-- ============================================================
-- Tabela: dbo_ext.Roles
-- Rolle użytkowników systemu RBAC.
-- Predefiniowane: Admin, Manager, User, ReadOnly
-- ============================================================

USE [WAPRO];
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'dbo_ext' AND t.name = 'Roles'
)
BEGIN
    CREATE TABLE dbo_ext.Roles (
        ID_ROLE     INT             NOT NULL IDENTITY(1,1),
        RoleName    NVARCHAR(50)    NOT NULL,
        Description NVARCHAR(200)   NULL,
        IsActive    BIT             NOT NULL DEFAULT 1,
        CreatedAt   DATETIME        NOT NULL DEFAULT GETDATE(),
        UpdatedAt   DATETIME        NULL,

        CONSTRAINT PK_Roles PRIMARY KEY CLUSTERED (ID_ROLE),
        CONSTRAINT UQ_Roles_RoleName UNIQUE (RoleName),
        CONSTRAINT CK_Roles_IsActive CHECK (IsActive IN (0, 1))
    );

    PRINT 'Tabela dbo_ext.Roles utworzona.';
END
ELSE
BEGIN
    PRINT 'Tabela dbo_ext.Roles już istnieje — pominięto.';
END
GO

-- Indeksy
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Roles_IsActive')
    CREATE INDEX IX_Roles_IsActive ON dbo_ext.Roles (IsActive);
GO