-- =============================================================================
-- DODATEK DO SEEDA: Uprawnienia szablonów (templates)
-- =============================================================================
--
-- Kategoria "templates" — 5 uprawnień.
-- Po wykonaniu łączna liczba uprawnień: 83 + 5 = 88.
--
-- IDEMPOTENTNY — MERGE INSERT only.
-- =============================================================================

SET NOCOUNT ON;
GO

BEGIN TRANSACTION;
BEGIN TRY

    MERGE [dbo_ext].[skw_Permissions] AS target
    USING (VALUES
        (N'templates.view_list',
         N'Lista wszystkich szablonów monitów',
         N'templates'),
        (N'templates.view_details',
         N'Szczegóły szablonu wraz z treścią Body',
         N'templates'),
        (N'templates.create',
         N'Tworzenie nowego szablonu wiadomości',
         N'templates'),
        (N'templates.edit',
         N'Edycja istniejącego szablonu (nazwa, typ, treść)',
         N'templates'),
        (N'templates.delete',
         N'Dezaktywacja szablonu (soft-delete)',
         N'templates')
    ) AS source ([PermissionName], [Description], [Category])
    ON target.[PermissionName] = source.[PermissionName]
    WHEN NOT MATCHED THEN
        INSERT ([PermissionName], [Description], [Category], [IsActive], [CreatedAt])
        VALUES (source.[PermissionName], source.[Description], source.[Category], 1, GETDATE());

    PRINT 'Uprawnienia templates: OK (' + CAST(@@ROWCOUNT AS NVARCHAR) + ' wstawione)';

    COMMIT TRANSACTION;
    PRINT '=== Templates permissions — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT 'BŁĄD: ' + ERROR_MESSAGE();
    THROW;
END CATCH
GO


-- =============================================================================
-- PRZYPISANIE uprawnień do ról:
--   Admin     — wszystkie templates.*
--   Manager   — view_list, view_details, create, edit
--   User      — view_list, view_details
--   ReadOnly  — view_list
-- =============================================================================

BEGIN TRANSACTION;
BEGIN TRY

    -- ADMIN — wszystkie templates.*
    INSERT INTO [dbo_ext].[skw_RolePermissions] ([ID_ROLE], [ID_PERMISSION])
    SELECT r.[ID_ROLE], p.[ID_PERMISSION]
    FROM   [dbo_ext].[skw_Roles]       r
    JOIN   [dbo_ext].[skw_Permissions] p ON p.[Category] = N'templates' AND p.[IsActive] = 1
    WHERE  r.[RoleName] = N'Admin'
      AND  NOT EXISTS (
          SELECT 1 FROM [dbo_ext].[skw_RolePermissions] rp
          WHERE rp.[ID_ROLE] = r.[ID_ROLE] AND rp.[ID_PERMISSION] = p.[ID_PERMISSION]
      );

    -- MANAGER — view_list, view_details, create, edit
    INSERT INTO [dbo_ext].[skw_RolePermissions] ([ID_ROLE], [ID_PERMISSION])
    SELECT r.[ID_ROLE], p.[ID_PERMISSION]
    FROM   [dbo_ext].[skw_Roles]       r
    JOIN   [dbo_ext].[skw_Permissions] p ON p.[Category] = N'templates'
                                         AND p.[PermissionName] IN (
                                             N'templates.view_list',
                                             N'templates.view_details',
                                             N'templates.create',
                                             N'templates.edit'
                                         )
                                         AND p.[IsActive] = 1
    WHERE  r.[RoleName] = N'Manager'
      AND  NOT EXISTS (
          SELECT 1 FROM [dbo_ext].[skw_RolePermissions] rp
          WHERE rp.[ID_ROLE] = r.[ID_ROLE] AND rp.[ID_PERMISSION] = p.[ID_PERMISSION]
      );

    -- USER — view_list, view_details
    INSERT INTO [dbo_ext].[skw_RolePermissions] ([ID_ROLE], [ID_PERMISSION])
    SELECT r.[ID_ROLE], p.[ID_PERMISSION]
    FROM   [dbo_ext].[skw_Roles]       r
    JOIN   [dbo_ext].[skw_Permissions] p ON p.[Category] = N'templates'
                                         AND p.[PermissionName] IN (
                                             N'templates.view_list',
                                             N'templates.view_details'
                                         )
                                         AND p.[IsActive] = 1
    WHERE  r.[RoleName] = N'User'
      AND  NOT EXISTS (
          SELECT 1 FROM [dbo_ext].[skw_RolePermissions] rp
          WHERE rp.[ID_ROLE] = r.[ID_ROLE] AND rp.[ID_PERMISSION] = p.[ID_PERMISSION]
      );

    -- READONLY — view_list
    INSERT INTO [dbo_ext].[skw_RolePermissions] ([ID_ROLE], [ID_PERMISSION])
    SELECT r.[ID_ROLE], p.[ID_PERMISSION]
    FROM   [dbo_ext].[skw_Roles]       r
    JOIN   [dbo_ext].[skw_Permissions] p ON p.[Category] = N'templates'
                                         AND p.[PermissionName] = N'templates.view_list'
                                         AND p.[IsActive] = 1
    WHERE  r.[RoleName] = N'ReadOnly'
      AND  NOT EXISTS (
          SELECT 1 FROM [dbo_ext].[skw_RolePermissions] rp
          WHERE rp.[ID_ROLE] = r.[ID_ROLE] AND rp.[ID_PERMISSION] = p.[ID_PERMISSION]
      );

    PRINT 'Przypisania ról templates: OK';

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT 'BŁĄD: ' + ERROR_MESSAGE();
    THROW;
END CATCH
GO

-- Weryfikacja
SELECT
    r.[RoleName],
    p.[PermissionName]
FROM   [dbo_ext].[skw_RolePermissions] rp
JOIN   [dbo_ext].[skw_Roles]           r ON r.[ID_ROLE]       = rp.[ID_ROLE]
JOIN   [dbo_ext].[skw_Permissions]     p ON p.[ID_PERMISSION] = rp.[ID_PERMISSION]
WHERE  p.[Category] = N'templates'
ORDER BY r.[RoleName], p.[PermissionName];
GO

