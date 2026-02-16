-- ============================================================
-- Tabela: dbo_ext.Permissions
-- Granularne uprawnienia. Format nazwy: kategoria.akcja
-- ============================================================

USE [WAPRO];
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'dbo_ext' AND t.name = 'Permissions'
)
BEGIN
    CREATE TABLE dbo_ext.Permissions (
        ID_PERMISSION   INT             NOT NULL IDENTITY(1,1),
        PermissionName  NVARCHAR(100)   NOT NULL,
        Description     NVARCHAR(200)   NULL,
        Category        NVARCHAR(50)    NOT NULL,
        IsActive        BIT             NOT NULL DEFAULT 1,
        CreatedAt       DATETIME        NOT NULL DEFAULT GETDATE(),
        UpdatedAt       DATETIME        NULL,

        CONSTRAINT PK_Permissions PRIMARY KEY CLUSTERED (ID_PERMISSION),
        CONSTRAINT UQ_Permissions_Name UNIQUE (PermissionName),
        CONSTRAINT CK_Permissions_Category CHECK (
            Category IN (
                'auth', 'users', 'roles', 'permissions', 'debtors',
                'comments', 'monits', 'pdf', 'reports', 'audit',
                'snapshots', 'system'
            )
        ),
        CONSTRAINT CK_Permissions_NameFormat CHECK (
            PermissionName LIKE '%.%'  -- wymusza format kategoria.akcja
        )
    );

    PRINT 'Tabela dbo_ext.Permissions utworzona.';
END
GO

CREATE INDEX IX_Permissions_Category ON dbo_ext.Permissions (Category)
    WHERE IsActive = 1;
GO