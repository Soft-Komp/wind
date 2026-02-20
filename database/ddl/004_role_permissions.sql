USE [WAPRO];
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'dbo_ext' AND t.name = 'RolePermissions'
)
BEGIN
    CREATE TABLE dbo_ext.RolePermissions (
        ID_ROLE         INT      NOT NULL,
        ID_PERMISSION   INT      NOT NULL,
        CreatedAt       DATETIME NOT NULL DEFAULT GETDATE(),

        CONSTRAINT PK_RolePermissions
            PRIMARY KEY CLUSTERED (ID_ROLE, ID_PERMISSION),
        CONSTRAINT FK_RolePerm_Role
            FOREIGN KEY (ID_ROLE)
            REFERENCES dbo_ext.Roles(ID_ROLE)
            ON DELETE CASCADE,
        CONSTRAINT FK_RolePerm_Permission
            FOREIGN KEY (ID_PERMISSION)
            REFERENCES dbo_ext.Permissions(ID_PERMISSION)
            ON DELETE CASCADE
    );
    PRINT 'Tabela dbo_ext.RolePermissions utworzona.';
END
GO

CREATE INDEX IX_RolePerm_Permission ON dbo_ext.RolePermissions (ID_PERMISSION);
GO