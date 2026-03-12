-- =============================================================================
-- 07_default_templates.sql
-- Domyślne szablony wiadomości dla systemu Windykacja
-- IDEMPOTENTNY — MERGE INSERT only
-- =============================================================================
SET NOCOUNT ON;
GO

BEGIN TRANSACTION;
BEGIN TRY

    MERGE [dbo_ext].[skw_Templates] AS target
    USING (VALUES
        (
            N'Wezwanie do zapłaty - Email',
            N'email',
            N'Wezwanie do zapłaty - {{ company_name }}',
            N'<html><body style="font-family: Arial, sans-serif; color: #333; max-width: 600px; margin: 0 auto;">
<h2 style="color: #c0392b;">Wezwanie do zapłaty</h2>
<p>Szanowni Państwo,</p>
<p>Zwracamy się z uprzejmą prośbą o uregulowanie zaległych należności.</p>
<table style="border-collapse: collapse; width: 100%; margin: 20px 0;">
  <tr style="background: #f8f8f8;">
    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Dłużnik:</strong></td>
    <td style="padding: 8px; border: 1px solid #ddd;">{{ debtor_name }}</td>
  </tr>
  <tr>
    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Łączna kwota zadłużenia:</strong></td>
    <td style="padding: 8px; border: 1px solid #ddd; color: #c0392b;"><strong>{{ total_debt }} PLN</strong></td>
  </tr>
  <tr style="background: #f8f8f8;">
    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Numery faktur:</strong></td>
    <td style="padding: 8px; border: 1px solid #ddd;">{{ invoice_list }}</td>
  </tr>
  <tr>
    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Termin zapłaty:</strong></td>
    <td style="padding: 8px; border: 1px solid #ddd;"><strong>{{ due_date }}</strong></td>
  </tr>
</table>
<p>Prosimy o dokonanie wpłaty w wyznaczonym terminie. Brak zapłaty może skutkować
   skierowaniem sprawy na drogę sądową.</p>
<p>W przypadku pytań prosimy o kontakt.</p>
<br>
<p>Z poważaniem,<br><strong>{{ company_name }}</strong><br>Dział Windykacji</p>
</body></html>'
        ),
        (
            N'Wezwanie do zapłaty - SMS',
            N'sms',
            NULL,
            N'{{ company_name }}: Zaleglosc {{ total_debt }} PLN. Faktury: {{ invoice_list }}. Prosimy o wplate do {{ due_date }}.'
        ),
        (
            N'Wezwanie do zapłaty - Druk',
            N'print',
            NULL,
            N'Szanowni Państwo,

wzywamy do zapłaty zaległości w kwocie {{ total_debt }} PLN.
Dłużnik: {{ debtor_name }}
Faktury: {{ invoice_list }}
Termin zapłaty: {{ due_date }}

Z poważaniem,
{{ company_name }}'
        )
    ) AS source ([TemplateName], [TemplateType], [Subject], [Body])
    ON target.[TemplateName] = source.[TemplateName]
    WHEN NOT MATCHED THEN
        INSERT ([TemplateName], [TemplateType], [Subject], [Body], [IsActive], [CreatedAt])
        VALUES (source.[TemplateName], source.[TemplateType], source.[Subject], source.[Body], 1, GETDATE());

    PRINT '[07] Szablony domyślne — OK';

    SELECT
        ID_TEMPLATE,
        TemplateName,
        TemplateType,
        IsActive
    FROM [dbo_ext].[skw_Templates]
    ORDER BY ID_TEMPLATE;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT 'BŁĄD: ' + ERROR_MESSAGE();
    THROW;
END CATCH;
GO