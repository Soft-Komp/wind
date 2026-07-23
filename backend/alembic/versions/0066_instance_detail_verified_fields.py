# backend/alembic/versions/0066_instance_detail_verified_fields.py
"""0066_instance_detail_verified_fields

Rozszerza widok skw_v_approval_instance_detail o drugi poziom fallbacku:
kanoniczne pola (fah.* / extra_data.doc_number|contractor|nip|doc_date)
-> verified_* (dane zweryfikowane przez operatora przy POST /documents/
{id}/ocr-review/resolve, decision='confirm' — patrz resolve_ocr_review
w documents_service.py).

KONTEKST (2026-07-23, zgloszenie: instancja 787): dokumenty przechodzace
przez OCR maja dane wylacznie pod kluczami ocr_* w extra_data, ktorych ten
widok nigdy nie czytal. Operator moze teraz jawnie zatwierdzic/poprawic
numer, kontrahenta, NIP i date dokumentu przy potwierdzeniu OCR — te
wartosci trafiaja do osobnych kluczy verified_* (NIE nadpisuja ocr_*,
ktore pozostaja jako surowy slad techniczny).

DECYZJA PRODUKTOWA (jawna, 2026-07-23): surowe ocr_* NIE sa fallbackiem
w tym widoku — tylko kanoniczne i verified_*. Dokumenty z OCR, ktore nigdy
nie zostaly jawnie potwierdzone (verified_* puste), pokazuja NULL w tych
polach — front wyswietla wtedy etykiete "Dane historyczne z OCR —
niezweryfikowane" zamiast podstawiac niepewne dane. Zaden automatyczny
backfill ocr_* -> verified_* dla dokumentow historycznych (w tym 787) nie
jest wykonywany w tej migracji — brak wiarygodnego sladu w audycie co
operator faktycznie zatwierdzil przed tym wdrozeniem (potwierdzone: stary
_audit_log przy ocr_review_resolved zapisywal tylko {decision, comment},
bez wartosci pol).

Dotyczy tylko 4 pol: numer, kontrahent, nip, data_wystawienia.
NIE dotyczy wartosc_netto/kwota_vat — kwota brutto pozostaje wylacznie
w document_amount (kolumna), bez odpowiednika verified_amount_gross
(decyzja: unikamy trzeciego, niezaleznego zrodla tej samej liczby, patrz
precedens w migracji 0061 dot. ksef_wartosc_brutto).

Revision ID : 0066
Revises     : 0065
"""
from alembic import op

revision = "0066"
down_revision = "0065"
branch_labels = None
depends_on = None

SCHEMA = "dbo"
VIEW = "skw_v_approval_instance_detail"

# ── Definicja NOWA (0067) — kanoniczne -> verified_*, bez ocr_* ────────────
_VIEW_DDL = f"""
CREATE OR ALTER VIEW [{SCHEMA}].[{VIEW}] AS
SELECT
    dai.[id_instance],
    dai.[id_document],
    dai.[id_source],
    ds.[source_name],
    dai.[status],
    dai.[current_step],
    dai.[is_urgent],
    dai.[document_title],
    dai.[document_amount],
    dai.[deadline_at],
    dai.[created_at]                    AS instance_created_at,
    dai.[updated_at]                    AS instance_updated_at,
    snap.[id_snapshot]                  AS snapshot_id,
    snap.[id_group],
    ag.[group_name],
    ag.[consensus_type],
    snap.[votes_cast],
    snap.[votes_required],
    snap.[deadline_at]                  AS step_deadline,
    u_disp.[Username]                   AS dispatched_by_username,
    dai.[id_category],
    dc.[category_name],
    -- ── NOWE 2026-07-23 (migracja 0067) — dodany kolejny poziom
    --    fallbacku: kanoniczne -> verified_*. Swiadomie BEZ ocr_*
    --    (surowe dane OCR nie sa tu czytane, patrz docstring). ──────────
    COALESCE(
        fah.[NUMER],
        JSON_VALUE(dai.[extra_data], '$.doc_number'),
        JSON_VALUE(dai.[extra_data], '$.verified_doc_number')
    )                                                                   AS numer,
    COALESCE(
        fah.[DataWystawienia],
        TRY_CONVERT(DATE, JSON_VALUE(dai.[extra_data], '$.doc_date')),
        TRY_CONVERT(DATE, JSON_VALUE(dai.[extra_data], '$.verified_doc_date'))
    )                                                                   AS data_wystawienia,
    COALESCE(
        fah.[NazwaKontrahenta],
        JSON_VALUE(dai.[extra_data], '$.contractor'),
        JSON_VALUE(dai.[extra_data], '$.verified_contractor')
    )                                                                   AS kontrahent,
    COALESCE(
        JSON_VALUE(dai.[extra_data], '$.nip'),
        JSON_VALUE(dai.[extra_data], '$.verified_nip')
    )                                                                   AS nip,
    COALESCE(
        fah.[WARTOSC_NETTO],
        TRY_CONVERT(DECIMAL(18,2), JSON_VALUE(dai.[extra_data], '$.amount_net'))
    )                                                                   AS wartosc_netto,
    fah.[KWOTA_VAT]                                                    AS kwota_vat,        -- Fakir-only, brak odpowiednika KSeF
    COALESCE(
        fah.[TerminPlatnosci],
        TRY_CONVERT(DATE, JSON_VALUE(dai.[extra_data], '$.payment_deadline'))
    )                                                                   AS termin_platnosci,
    -- UWAGA: polaczenie mimo roznicy slownikow (fakir vs KSeF) —
    -- swiadoma decyzja, patrz docstring migracji 0062.
    COALESCE(
        fah.[FORMA_PLATNOSCI],
        JSON_VALUE(dai.[extra_data], '$.payment_form')
    )                                                                   AS forma_platnosci,
    fah.[KOD_STATUSU]                                                  AS status_zewnetrzny, -- Fakir-only
    fah.[StatusOpis]                                                   AS status_opis,       -- Fakir-only
    fah.[UWAGI]                                                        AS uwagi              -- Fakir-only
FROM [{SCHEMA}].[skw_document_approval_instances] dai
INNER JOIN [{SCHEMA}].[skw_document_sources] ds
       ON ds.[id_source] = dai.[id_source]
LEFT  JOIN [{SCHEMA}].[skw_Users] u_disp
       ON u_disp.[ID_USER] = dai.[dispatched_by]
LEFT  JOIN [{SCHEMA}].[skw_document_approval_snapshot_steps] snap
       ON  snap.[id_instance] = dai.[id_instance]
       AND snap.[step_order]  = dai.[current_step]
LEFT  JOIN [{SCHEMA}].[skw_approval_groups] ag
       ON ag.[id_group] = snap.[id_group]
LEFT  JOIN [{SCHEMA}].[skw_document_categories] dc
       ON dc.[id_category] = dai.[id_category]
LEFT  JOIN [{SCHEMA}].[skw_faktury_akceptacja_naglowek] fah
       ON  ds.[source_name] = N'fakir'
       AND fah.[KSEF_ID]    = dai.[id_document]
"""

# ── Definicja POPRZEDNIA (0062) — dokladna kopia zweryfikowana zapytaniem
#    OBJECT_DEFINITION() na STOMIL przed napisaniem tej migracji, uzywana
#    w downgrade() ─────────────────────────────────────────────────────────
_VIEW_DDL_PREVIOUS = f"""
CREATE OR ALTER VIEW [{SCHEMA}].[{VIEW}] AS
SELECT
    dai.[id_instance],
    dai.[id_document],
    dai.[id_source],
    ds.[source_name],
    dai.[status],
    dai.[current_step],
    dai.[is_urgent],
    dai.[document_title],
    dai.[document_amount],
    dai.[deadline_at],
    dai.[created_at]                    AS instance_created_at,
    dai.[updated_at]                    AS instance_updated_at,
    snap.[id_snapshot]                  AS snapshot_id,
    snap.[id_group],
    ag.[group_name],
    ag.[consensus_type],
    snap.[votes_cast],
    snap.[votes_required],
    snap.[deadline_at]                  AS step_deadline,
    u_disp.[Username]                   AS dispatched_by_username,
    dai.[id_category],
    dc.[category_name],
    COALESCE(
        fah.[NUMER],
        JSON_VALUE(dai.[extra_data], '$.doc_number')
    )                                                                   AS numer,
    COALESCE(
        fah.[DataWystawienia],
        TRY_CONVERT(DATE, JSON_VALUE(dai.[extra_data], '$.doc_date'))
    )                                                                   AS data_wystawienia,
    COALESCE(
        fah.[NazwaKontrahenta],
        JSON_VALUE(dai.[extra_data], '$.contractor')
    )                                                                   AS kontrahent,
    JSON_VALUE(dai.[extra_data], '$.nip')                              AS nip,
    COALESCE(
        fah.[WARTOSC_NETTO],
        TRY_CONVERT(DECIMAL(18,2), JSON_VALUE(dai.[extra_data], '$.amount_net'))
    )                                                                   AS wartosc_netto,
    fah.[KWOTA_VAT]                                                    AS kwota_vat,
    COALESCE(
        fah.[TerminPlatnosci],
        TRY_CONVERT(DATE, JSON_VALUE(dai.[extra_data], '$.payment_deadline'))
    )                                                                   AS termin_platnosci,
    COALESCE(
        fah.[FORMA_PLATNOSCI],
        JSON_VALUE(dai.[extra_data], '$.payment_form')
    )                                                                   AS forma_platnosci,
    fah.[KOD_STATUSU]                                                  AS status_zewnetrzny,
    fah.[StatusOpis]                                                   AS status_opis,
    fah.[UWAGI]                                                        AS uwagi
FROM [{SCHEMA}].[skw_document_approval_instances] dai
INNER JOIN [{SCHEMA}].[skw_document_sources] ds
       ON ds.[id_source] = dai.[id_source]
LEFT  JOIN [{SCHEMA}].[skw_Users] u_disp
       ON u_disp.[ID_USER] = dai.[dispatched_by]
LEFT  JOIN [{SCHEMA}].[skw_document_approval_snapshot_steps] snap
       ON  snap.[id_instance] = dai.[id_instance]
       AND snap.[step_order]  = dai.[current_step]
LEFT  JOIN [{SCHEMA}].[skw_approval_groups] ag
       ON ag.[id_group] = snap.[id_group]
LEFT  JOIN [{SCHEMA}].[skw_document_categories] dc
       ON dc.[id_category] = dai.[id_category]
LEFT  JOIN [{SCHEMA}].[skw_faktury_akceptacja_naglowek] fah
       ON  ds.[source_name] = N'fakir'
       AND fah.[KSEF_ID]    = dai.[id_document]
"""


def _checksum_merge(revision_tag: str) -> str:
    return f"""
MERGE [{SCHEMA}].[skw_SchemaChecksums] AS target
USING (
    SELECT
        obj.[name]                  AS ObjectName,
        sch.[name]                  AS SchemaName,
        N'VIEW'                     AS ObjectType,
        CHECKSUM(mod.[definition])  AS Checksum,
        N'{revision_tag}'           AS AlembicRevision,
        NULL                        AS LastVerifiedAt,
        SYSUTCDATETIME()            AS Now
    FROM sys.objects  obj
    JOIN sys.schemas  sch ON sch.[schema_id] = obj.[schema_id]
    JOIN sys.sql_modules mod ON mod.[object_id] = obj.[object_id]
    WHERE obj.[type] = N'V'
      AND sch.[name] = N'{SCHEMA}'
      AND obj.[name] = N'{VIEW}'
) AS source
    ON  target.[ObjectName] = source.[ObjectName]
    AND target.[SchemaName] = source.[SchemaName]
WHEN MATCHED THEN
    UPDATE SET
        [Checksum]        = source.[Checksum],
        [AlembicRevision] = source.[AlembicRevision],
        [LastVerifiedAt]  = source.[LastVerifiedAt],
        [UpdatedAt]       = source.[Now]
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        [ObjectName], [SchemaName], [ObjectType],
        [Checksum], [AlembicRevision],
        [LastVerifiedAt], [CreatedAt]
    )
    VALUES (
        source.[ObjectName], source.[SchemaName], source.[ObjectType],
        source.[Checksum],   source.[AlembicRevision],
        source.[LastVerifiedAt], source.[Now]
    );
"""


def upgrade() -> None:
    op.execute(_VIEW_DDL)
    op.execute(_checksum_merge("0066"))


def downgrade() -> None:
    op.execute(_VIEW_DDL_PREVIOUS)
    op.execute(_checksum_merge("0062"))