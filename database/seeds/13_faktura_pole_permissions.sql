-- =============================================================================
-- PLIK:    database/seeds/13_faktura_pole_permissions.sql
-- MODUŁ:   Akceptacja Faktur KSeF — granularne uprawnienia pól
-- OPIS:    21 uprawnień faktury.pole.* — kontrola widoczności pól w
--          GET /faktury-akceptacja/ksef/{ksef_id}
--          Logika: whitelist — pole widoczne tylko gdy rola MA uprawnienie.
--          Zawsze widoczne (bez uprawnień): numer_ksef, numer,
--          id_buf_dokument, numer_pozycji.
-- IDEMPOTENTNOŚĆ: MERGE INSERT-only — bezpieczne przy wielokrotnym uruchomieniu
-- =============================================================================

SET NOCOUNT ON;

MERGE [dbo_ext].[skw_Permissions] AS target
USING (
    VALUES
    -- =========================================================================
    -- GRUPA: Pola nagłówka faktury (12)
    -- =========================================================================
    (N'faktury.pole.status_wewnetrzny',
     N'Widoczność pola: status wewnętrzny faktury w obiegu akceptacji',
     N'faktury'),
    (N'faktury.pole.priorytet',
     N'Widoczność pola: priorytet faktury (normalny/pilny/bardzo_pilny)',
     N'faktury'),
    (N'faktury.pole.data_wystawienia',
     N'Widoczność pola: data wystawienia faktury (z WAPRO)',
     N'faktury'),
    (N'faktury.pole.data_otrzymania',
     N'Widoczność pola: data otrzymania faktury (z WAPRO)',
     N'faktury'),
    (N'faktury.pole.termin_platnosci',
     N'Widoczność pola: termin płatności faktury (z WAPRO)',
     N'faktury'),
    (N'faktury.pole.wartosc_netto',
     N'Widoczność pola: wartość netto faktury (nagłówek)',
     N'faktury'),
    (N'faktury.pole.wartosc_brutto',
     N'Widoczność pola: wartość brutto faktury (nagłówek)',
     N'faktury'),
    (N'faktury.pole.kwota_vat',
     N'Widoczność pola: kwota VAT faktury (nagłówek)',
     N'faktury'),
    (N'faktury.pole.forma_platnosci',
     N'Widoczność pola: forma płatności faktury',
     N'faktury'),
    (N'faktury.pole.nazwa_kontrahenta',
     N'Widoczność pola: nazwa kontrahenta wystawcy faktury',
     N'faktury'),
    (N'faktury.pole.email_kontrahenta',
     N'Widoczność pola: adres email kontrahenta (RODO)',
     N'faktury'),
    (N'faktury.pole.telefon_kontrahenta',
     N'Widoczność pola: telefon kontrahenta (RODO)',
     N'faktury'),

    -- =========================================================================
    -- GRUPA: Pola pozycji faktury (9)
    -- =========================================================================
    (N'faktury.pole.nazwa_towaru',
     N'Widoczność pola: nazwa towaru/usługi na pozycji faktury',
     N'faktury'),
    (N'faktury.pole.ilosc',
     N'Widoczność pola: ilość na pozycji faktury',
     N'faktury'),
    (N'faktury.pole.jednostka',
     N'Widoczność pola: jednostka miary na pozycji faktury',
     N'faktury'),
    (N'faktury.pole.cena_netto',
     N'Widoczność pola: cena jednostkowa netto na pozycji faktury',
     N'faktury'),
    (N'faktury.pole.cena_brutto',
     N'Widoczność pola: cena jednostkowa brutto na pozycji faktury',
     N'faktury'),
    (N'faktury.pole.pozycja_wartosc_netto',
     N'Widoczność pola: wartość netto pozycji faktury',
     N'faktury'),
    (N'faktury.pole.pozycja_wartosc_brutto',
     N'Widoczność pola: wartość brutto pozycji faktury',
     N'faktury'),
    (N'faktury.pole.stawka_vat',
     N'Widoczność pola: stawka VAT na pozycji faktury',
     N'faktury'),
    (N'faktury.pole.opis',
     N'Widoczność pola: opis pozycji faktury',
     N'faktury')

) AS source ([PermissionName], [Description], [Category])
ON (target.[PermissionName] = source.[PermissionName])
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([PermissionName], [Description], [Category], [IsActive], [CreatedAt])
    VALUES (
        source.[PermissionName],
        source.[Description],
        source.[Category],
        1,
        GETDATE()
    );

PRINT N'[13] faktury.pole.* — ' + CAST(@@ROWCOUNT AS NVARCHAR) + N' nowych uprawnień wstawionych.';
GO