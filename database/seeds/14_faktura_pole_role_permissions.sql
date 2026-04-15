-- =============================================================================
-- PLIK:    database/seeds/14_faktura_pole_role_permissions.sql
-- OPIS:    Przypisanie 21 uprawnień faktury.pole.* do ról.
--          Admin    → wszystkie 21
--          Manager  → wszystkie 21
--          User     → wszystkie 21 (admin może odebrać per rola w razie potrzeby)
--          ReadOnly → wszystkie 21 (tylko podgląd, bez akcji)
-- IDEMPOTENTNOŚĆ: MERGE INSERT-only
-- =============================================================================

SET NOCOUNT ON;

DECLARE @pole_perms TABLE ([ID_PERMISSION] INT);

INSERT INTO @pole_perms
SELECT [ID_PERMISSION]
FROM [dbo_ext].[skw_Permissions]
WHERE [PermissionName] LIKE N'faktury.pole.%'
  AND [IsActive] = 1;

-- Weryfikacja — musi być 21
DECLARE @cnt INT = (SELECT COUNT(*) FROM @pole_perms);
IF @cnt <> 21
BEGIN
    RAISERROR(
        N'[14] Oczekiwano 21 uprawnień faktury.pole.*, znaleziono %d. Uruchom seed 11 najpierw.',
        16, 1, @cnt
    );
    RETURN;
END

-- ── Admin ────────────────────────────────────────────────────────────────────
MERGE [dbo_ext].[skw_RolePermissions] AS target
USING (
    SELECT r.[ID_ROLE], p.[ID_PERMISSION]
    FROM [dbo_ext].[skw_Roles] r
    CROSS JOIN @pole_perms p
    WHERE r.[RoleName] = N'Admin'
) AS source
    ON target.[ID_ROLE] = source.[ID_ROLE]
   AND target.[ID_PERMISSION] = source.[ID_PERMISSION]
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([ID_ROLE], [ID_PERMISSION], [CreatedAt])
    VALUES (source.[ID_ROLE], source.[ID_PERMISSION], GETDATE());
PRINT N'[14] Admin: ' + CAST(@@ROWCOUNT AS NVARCHAR) + N' nowych przypisań.';

-- ── Manager ──────────────────────────────────────────────────────────────────
MERGE [dbo_ext].[skw_RolePermissions] AS target
USING (
    SELECT r.[ID_ROLE], p.[ID_PERMISSION]
    FROM [dbo_ext].[skw_Roles] r
    CROSS JOIN @pole_perms p
    WHERE r.[RoleName] = N'Manager'
) AS source
    ON target.[ID_ROLE] = source.[ID_ROLE]
   AND target.[ID_PERMISSION] = source.[ID_PERMISSION]
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([ID_ROLE], [ID_PERMISSION], [CreatedAt])
    VALUES (source.[ID_ROLE], source.[ID_PERMISSION], GETDATE());
PRINT N'[14] Manager: ' + CAST(@@ROWCOUNT AS NVARCHAR) + N' nowych przypisań.';

-- ── User ─────────────────────────────────────────────────────────────────────
MERGE [dbo_ext].[skw_RolePermissions] AS target
USING (
    SELECT r.[ID_ROLE], p.[ID_PERMISSION]
    FROM [dbo_ext].[skw_Roles] r
    CROSS JOIN @pole_perms p
    WHERE r.[RoleName] = N'User'
) AS source
    ON target.[ID_ROLE] = source.[ID_ROLE]
   AND target.[ID_PERMISSION] = source.[ID_PERMISSION]
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([ID_ROLE], [ID_PERMISSION], [CreatedAt])
    VALUES (source.[ID_ROLE], source.[ID_PERMISSION], GETDATE());
PRINT N'[14] User: ' + CAST(@@ROWCOUNT AS NVARCHAR) + N' nowych przypisań.';

-- ── ReadOnly ─────────────────────────────────────────────────────────────────
MERGE [dbo_ext].[skw_RolePermissions] AS target
USING (
    SELECT r.[ID_ROLE], p.[ID_PERMISSION]
    FROM [dbo_ext].[skw_Roles] r
    CROSS JOIN @pole_perms p
    WHERE r.[RoleName] = N'ReadOnly'
) AS source
    ON target.[ID_ROLE] = source.[ID_ROLE]
   AND target.[ID_PERMISSION] = source.[ID_PERMISSION]
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([ID_ROLE], [ID_PERMISSION], [CreatedAt])
    VALUES (source.[ID_ROLE], source.[ID_PERMISSION], GETDATE());
PRINT N'[14] ReadOnly: ' + CAST(@@ROWCOUNT AS NVARCHAR) + N' nowych przypisań.';

GO