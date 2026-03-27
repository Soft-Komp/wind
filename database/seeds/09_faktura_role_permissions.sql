-- =============================================================================
-- PLIK  : database/seeds/08_faktura_permissions.sql
-- MODUŁ : Akceptacja Faktur KSeF
-- OPIS  : Seed 14 uprawnień kategorii 'faktury'.
--         Uprawnienia są TYLKO tu definiowane — nie przez API (security design).
--         Wykonywany przez entrypoint.sh przy świeżej instalacji.
--         Wykonywany przez Alembic migration 007 przy aktualizacji systemu.
--
--         Dwie grupy uprawnień:
--           A) Uprawnienia referenta  (osoby wprowadzającej faktury do obiegu)
--           B) Uprawnienia pracownika (osoby akceptującej przypisane faktury)
--           C) Uprawnienia systemowe  (config, PDF, historia)
--
-- AUTOR : Sprint 2 — Sekcja 3
-- DATA  : 2026-03-26
-- WERSJA: 1.0
-- IDEMPOTENTNY: TAK — MERGE (INSERT only, nie modyfikuje istniejących)
-- =============================================================================

USE [WAPRO];
GO

-- ---------------------------------------------------------------------------
-- MERGE: wstaw brakujące uprawnienia, nie dotykaj istniejących
-- ---------------------------------------------------------------------------
MERGE INTO [dbo_ext].[skw_Permissions] AS target
USING (
    VALUES
    -- ==================================================================
    -- GRUPA A: Uprawnienia referenta — endpointy /faktury-akceptacja
    -- ==================================================================

    -- Lista faktur w obiegu (NOWE z WAPRO + W_TOKU) — główny widok referenta
    (N'faktury.view_list',
     N'Lista faktur zakupowych w obiegu akceptacji',
     N'faktury'),

    -- Szczegóły faktury: dane WAPRO + opis/uwagi referenta + przypisania
    (N'faktury.view_details',
     N'Szczegóły faktury wraz z pozycjami i historią przypisań',
     N'faktury'),

    -- Wpuszczenie faktury do obiegu: POST /faktury-akceptacja
    (N'faktury.create',
     N'Wpuszczenie faktury z KSeF do obiegu akceptacji',
     N'faktury'),

    -- Edycja priorytetu, opisu, uwag: PATCH /faktury-akceptacja/{id}
    (N'faktury.edit',
     N'Edycja priorytetu, opisu formalnego i uwag faktury',
     N'faktury'),

    -- Reset przypisań (dwuetapowy): POST /faktury-akceptacja/{id}/reset
    (N'faktury.reset',
     N'Reset przypisań pracowników do faktury (operacja dwuetapowa)',
     N'faktury'),

    -- Wymuszenie statusu (dwuetapowy): PATCH /faktury-akceptacja/{id}/status
    (N'faktury.force_status',
     N'Wymuszenie zmiany statusu faktury z pominięciem procesu akceptacji',
     N'faktury'),

    -- Historia zdarzeń faktury: GET /faktury-akceptacja/{id}/historia
    (N'faktury.view_historia',
     N'Dostęp do pełnej historii zdarzeń faktury (skw_faktura_log)',
     N'faktury'),

    -- ==================================================================
    -- GRUPA B: Uprawnienia pracownika — endpointy /moje-faktury
    -- ==================================================================

    -- Lista przypisanych faktur: GET /moje-faktury
    (N'faktury.moje_view',
     N'Lista faktur przypisanych do zalogowanego pracownika',
     N'faktury'),

    -- Szczegóły przypisanej faktury: GET /moje-faktury/{id}
    (N'faktury.moje_details',
     N'Szczegóły faktury przypisanej do pracownika wraz z pozycjami',
     N'faktury'),

    -- Podjęcie decyzji: POST /moje-faktury/{id}/decyzja
    (N'faktury.moje_decyzja',
     N'Akceptacja, odrzucenie lub oznaczenie faktury jako nie_moje',
     N'faktury'),

    -- ==================================================================
    -- GRUPA C: Role modułowe — granularne uprawnienia dostępu do modułu
    -- ==================================================================

    -- Rola referenta: dostęp do endpointów /faktury-akceptacja/*
    (N'faktury.referent',
     N'Rola modułowa: dostęp do endpointów referenta (zarządzanie obiegiem)',
     N'faktury'),

    -- Rola akceptanta: dostęp do endpointów /moje-faktury/*
    (N'faktury.akceptant',
     N'Rola modułowa: dostęp do endpointów akceptanta (podejmowanie decyzji)',
     N'faktury'),

    -- ==================================================================
    -- GRUPA D: Uprawnienia techniczne
    -- ==================================================================

    -- Generowanie PDF wizualizacji faktury
    (N'faktury.view_pdf',
     N'Generowanie i pobieranie wizualizacji PDF faktury',
     N'faktury'),

    -- Edycja kluczy konfiguracyjnych faktury.* w /system/config
    (N'faktury.config_edit',
     N'Edycja kluczy systemowych konfiguracji modułu faktur',
     N'faktury')

) AS source ([PermissionName], [Description], [Category])
ON (target.[PermissionName] = source.[PermissionName])
WHEN NOT MATCHED THEN
    INSERT (
        [PermissionName],
        [Description],
        [Category],
        [IsActive],
        [CreatedAt]
    )
    VALUES (
        source.[PermissionName],
        source.[Description],
        source.[Category],
        1,
        GETDATE()
    );

GO

-- ---------------------------------------------------------------------------
-- Weryfikacja — pokaż wstawione uprawnienia
-- ---------------------------------------------------------------------------
SELECT
    [ID_PERMISSION],
    [PermissionName],
    [Description],
    [Category],
    [IsActive],
    [CreatedAt]
FROM   [dbo_ext].[skw_Permissions]
WHERE  [Category] = N'faktury'
ORDER BY [PermissionName];

DECLARE @cnt INT;
SELECT @cnt = COUNT(*) FROM [dbo_ext].[skw_Permissions] WHERE [Category] = N'faktury';
PRINT '[08] Uprawnienia kategorii faktury: ' + CAST(@cnt AS VARCHAR(10)) + ' (oczekiwane: 14)';
IF @cnt <> 14
    PRINT '[08] ⚠️  UWAGA: Liczba uprawnień różni się od oczekiwanej!';
ELSE
    PRINT '[08] ✅ Wszystkie 14 uprawnień faktury zarejestrowanych poprawnie';
GO

PRINT '[08] === Skrypt 08_faktura_permissions.sql zakończony pomyślnie ===';
GO