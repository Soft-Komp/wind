-- ============================================================
-- Seed: predefiniowane role systemu
-- Idempotentny — bezpieczny do wielokrotnego uruchomienia
-- ============================================================

USE [WAPRO];
GO

MERGE dbo_ext.Roles AS target
USING (VALUES
    (N'Admin',    N'Pełne uprawnienia systemowe. Dostęp do wszystkich funkcji.'),
    (N'Manager',  N'Zarządza windykacją. Widzi wszystko, nie zarządza systemem.'),
    (N'User',     N'Podstawowy pracownik biurowy. Wysyłka monitów i komentarze.'),
    (N'ReadOnly', N'Tylko podgląd danych. Brak możliwości modyfikacji.')
) AS source (RoleName, Description)
ON target.RoleName = source.RoleName
WHEN NOT MATCHED THEN
    INSERT (RoleName, Description, IsActive, CreatedAt)
    VALUES (source.RoleName, source.Description, 1, GETDATE())
WHEN MATCHED THEN
    UPDATE SET Description = source.Description;
GO

PRINT 'Seed ról zakończony. Role: Admin, Manager, User, ReadOnly';
GO