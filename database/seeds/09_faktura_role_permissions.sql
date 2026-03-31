-- =============================================================================
-- PLIK  : database/seeds/09_faktura_role_permissions.sql
-- MODUŁ : Akceptacja Faktur KSeF
-- OPIS  : Przypisanie uprawnień kategorii 'faktury' do ról systemu.
--         Macierz uprawnień:
--           Admin   → wszystkie 14 uprawnień faktury
--           Manager → 12 (bez force_status i config_edit)
--           User    → 5  (moje_view, moje_details, moje_decyzja, akceptant, view_pdf)
-- IDEMPOTENTNY: TAK — INSERT z NOT EXISTS
-- =============================================================================

GO

-- ── Admin: wszystkie uprawnienia faktury ─────────────────────────────────────
INSERT INTO [dbo_ext].[skw_RolePermissions] ([ID_ROLE], [ID_PERMISSION])
SELECT r.[ID_ROLE], p.[ID_PERMISSION]
FROM [dbo_ext].[skw_Permissions] p
CROSS JOIN [dbo_ext].[skw_Roles] r
WHERE p.[Category] = N'faktury'
  AND p.[IsActive] = 1
  AND r.[RoleName] = N'Admin'
  AND r.[IsActive] = 1
  AND NOT EXISTS (
      SELECT 1 FROM [dbo_ext].[skw_RolePermissions] rp
      WHERE rp.[ID_ROLE] = r.[ID_ROLE]
        AND rp.[ID_PERMISSION] = p.[ID_PERMISSION]
  );

PRINT '[09] Admin — przypisano: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' uprawnień faktury';
GO

-- ── Manager: bez force_status i config_edit ───────────────────────────────────
INSERT INTO [dbo_ext].[skw_RolePermissions] ([ID_ROLE], [ID_PERMISSION])
SELECT r.[ID_ROLE], p.[ID_PERMISSION]
FROM [dbo_ext].[skw_Permissions] p
CROSS JOIN [dbo_ext].[skw_Roles] r
WHERE p.[Category] = N'faktury'
  AND p.[IsActive] = 1
  AND p.[PermissionName] NOT IN (
      N'faktury.force_status',
      N'faktury.config_edit'
  )
  AND r.[RoleName] = N'Manager'
  AND r.[IsActive] = 1
  AND NOT EXISTS (
      SELECT 1 FROM [dbo_ext].[skw_RolePermissions] rp
      WHERE rp.[ID_ROLE] = r.[ID_ROLE]
        AND rp.[ID_PERMISSION] = p.[ID_PERMISSION]
  );

PRINT '[09] Manager — przypisano: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' uprawnień faktury';
GO

-- ── User: tylko uprawnienia pracownika ────────────────────────────────────────
INSERT INTO [dbo_ext].[skw_RolePermissions] ([ID_ROLE], [ID_PERMISSION])
SELECT r.[ID_ROLE], p.[ID_PERMISSION]
FROM [dbo_ext].[skw_Permissions] p
CROSS JOIN [dbo_ext].[skw_Roles] r
WHERE p.[Category] = N'faktury'
  AND p.[IsActive] = 1
  AND p.[PermissionName] IN (
      N'faktury.moje_view',
      N'faktury.moje_details',
      N'faktury.moje_decyzja',
      N'faktury.akceptant',
      N'faktury.view_pdf'
  )
  AND r.[RoleName] = N'User'
  AND r.[IsActive] = 1
  AND NOT EXISTS (
      SELECT 1 FROM [dbo_ext].[skw_RolePermissions] rp
      WHERE rp.[ID_ROLE] = r.[ID_ROLE]
        AND rp.[ID_PERMISSION] = p.[ID_PERMISSION]
  );

PRINT '[09] User — przypisano: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' uprawnień faktury';
GO

-- ── Weryfikacja ───────────────────────────────────────────────────────────────
SELECT
    r.[RoleName],
    COUNT(*) AS LiczbaUprawnien
FROM [dbo_ext].[skw_RolePermissions] rp
JOIN [dbo_ext].[skw_Permissions]     p ON rp.[ID_PERMISSION] = p.[ID_PERMISSION]
JOIN [dbo_ext].[skw_Roles]           r ON rp.[ID_ROLE]       = r.[ID_ROLE]
WHERE p.[Category] = N'faktury'
GROUP BY r.[RoleName]
ORDER BY r.[RoleName];

PRINT '[09] === Skrypt 09_faktura_role_permissions.sql zakończony ===';
GO