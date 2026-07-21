# backend/alembic/versions/0061_instance_detail_ksef_columns.py
"""0061_instance_detail_ksef_columns

Dodanie kolumn ksef_* do widoku skw_v_approval_instance_detail, analogicznie
do juz istniejacych fakir_* (LEFT JOIN skw_faktury_akceptacja_naglowek).

KONTEKST: GET /approval/instances/{id} zwraca SELECT * z tego widoku
(potwierdzone w komentarzu migracji 0035: "pola pojawia sie automatycznie
po aktualizacji widoku" — backend/app/api/approval/instances.py::get_instance
buduje dict dynamicznie z kolumn widoku, zero zmian w Pythonie potrzebne).

Dla zrodel source_type='fakir' dane pochodza z LEFT JOIN do widoku WAPRO
(fakir_*). Dla zrodel KSeF (source_type='ksef20') analogiczne dane —
numer dokumentu, kontrahent, NIP, daty, kwota netto, forma platnosci —
NIGDY nie byly osobnymi kolumnami tabeli skw_document_approval_instances
(potwierdzone w source_sync_task.py::_upsert_instance — komentarz: "Kolumny
doc_number, contractor_name, document_date sa w extra_data, nie jako
osobne kolumny"). Trafiaja tam jako JSON (UnifiedDocument.to_extra_data_json()),
ale ZADEN dotychczasowy endpoint ich nie odczytywal z powrotem.

Mapowanie kluczy JSON -> kolumna widoku (zgodne z ksef20_adapter.py):
  doc_number        -> ksef_numer
  doc_date          -> ksef_data_wystawienia
  contractor         -> ksef_kontrahent   (UWAGA: klucz w JSON to "contractor",
                                            NIE "contractor_name" — latwa do
                                            pomylenia niespojnosc nazewnictwa
                                            miedzy atrybutem klasy a kluczem JSON)
  nip                -> ksef_nip
  amount_net         -> ksef_wartosc_netto
  payment_deadline   -> ksef_termin_platnosci
  payment_form       -> ksef_forma_platnosci

NIE dodajemy ksef_wartosc_brutto — document_amount (kolumna, nie JSON) juz
pokrywa te wartosc, duplikowanie tworzyloby dwa niezalezne zrodla tej samej
liczby, ktore moglyby sie rozjechac.

Brak odpowiednika dla fakir_kwota_vat / fakir_status_zewnetrzny /
fakir_status_opis / fakir_uwagi — te sa specyficzne dla integracji z
zewnetrznym systemem WAPRO (stan synchronizacji, uwagi operatora), KSeF
nie ma koncepcyjnego odpowiednika (brak dwustronnej integracji z systemem
ksiegowym, ktora wymagalaby takiego stanu).

ZALOZENIE WLASNE: TRY_CONVERT (nie CONVERT) dla dat/kwot — jesli JSON_VALUE
zwroci format niezgodny z oczekiwaniem (np. z powodu przyszlej zmiany w
adapterze), kolumna widoku daje NULL, nie wywala calego zapytania.

Revision ID : 0061
Revises     : 0060
"""
from alembic import op

revision = "0061"
down_revision = "0060"
branch_labels = None
depends_on = None

SCHEMA = "dbo"
VIEW = "skw_v_approval_instance_detail"

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
    fah.[NUMER]                         AS fakir_numer,
    fah.[WARTOSC_NETTO]                 AS fakir_wartosc_netto,
    fah.[WARTOSC_BRUTTO]                AS fakir_wartosc_brutto,
    fah.[KWOTA_VAT]                     AS fakir_kwota_vat,
    fah.[NazwaKontrahenta]              AS fakir_kontrahent,
    fah.[KOD_STATUSU]                   AS fakir_status_zewnetrzny,
    fah.[StatusOpis]                    AS fakir_status_opis,
    fah.[DataWystawienia]               AS fakir_data_wystawienia,
    fah.[TerminPlatnosci]               AS fakir_termin_platnosci,
    fah.[FORMA_PLATNOSCI]               AS fakir_forma_platnosci,
    fah.[UWAGI]                         AS fakir_uwagi,
    -- ── NOWE 2026-07-20 (migracja 0061) — analogiczne dane dla KSeF,
    --    czytane z extra_data (JSON) — patrz docstring migracji ──────────
    JSON_VALUE(dai.[extra_data], '$.doc_number')                       AS ksef_numer,
    TRY_CONVERT(DATE, JSON_VALUE(dai.[extra_data], '$.doc_date'))      AS ksef_data_wystawienia,
    JSON_VALUE(dai.[extra_data], '$.contractor')                       AS ksef_kontrahent,
    JSON_VALUE(dai.[extra_data], '$.nip')                              AS ksef_nip,
    TRY_CONVERT(DECIMAL(18,2), JSON_VALUE(dai.[extra_data], '$.amount_net')) AS ksef_wartosc_netto,
    TRY_CONVERT(DATE, JSON_VALUE(dai.[extra_data], '$.payment_deadline'))    AS ksef_termin_platnosci,
    JSON_VALUE(dai.[extra_data], '$.payment_form')                     AS ksef_forma_platnosci
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

# Wersja PRZED ta migracja (0035) — dokladna kopia, potrzebna do downgrade.
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
    fah.[NUMER]                         AS fakir_numer,
    fah.[WARTOSC_NETTO]                 AS fakir_wartosc_netto,
    fah.[WARTOSC_BRUTTO]                AS fakir_wartosc_brutto,
    fah.[KWOTA_VAT]                     AS fakir_kwota_vat,
    fah.[NazwaKontrahenta]              AS fakir_kontrahent,
    fah.[KOD_STATUSU]                   AS fakir_status_zewnetrzny,
    fah.[StatusOpis]                    AS fakir_status_opis,
    fah.[DataWystawienia]               AS fakir_data_wystawienia,
    fah.[TerminPlatnosci]               AS fakir_termin_platnosci,
    fah.[FORMA_PLATNOSCI]               AS fakir_forma_platnosci,
    fah.[UWAGI]                         AS fakir_uwagi
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


# NAPRAWA (self-review po incydencie SCHEMA TAMPER DETECTED, 2026-07-20):
# pierwsza wersja tej migracji miala WYLACZNIE CREATE OR ALTER VIEW, bez
# aktualizacji zarejestrowanego checksumu w skw_SchemaChecksums. Watchdog
# integralnosci schematu (app/core/schema_integrity.py) wykryl to jako
# "SCHEMA TAMPER" (niezgodnosc stored_checksum vs live_checksum) i
# ZABLOKOWAL start aplikacji (reaction=BLOCK). Wzorzec ponizej — dokladna
# replika _CHECKSUM_MERGE z migracji 0035 (ten sam widok), ktory
# przeoczylem przy pierwszym pisaniu tej migracji.
_CHECKSUM_MERGE = f"""
MERGE [{SCHEMA}].[skw_SchemaChecksums] AS target
USING (
    SELECT
        obj.[name]                  AS ObjectName,
        sch.[name]                  AS SchemaName,
        N'VIEW'                     AS ObjectType,
        CHECKSUM(mod.[definition])  AS Checksum,
        N'0061'                     AS AlembicRevision,
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

# Analogiczny MERGE dla downgrade — rejestruje checksum wersji 0035
# (przywracanej definicji), zeby stan po downgrade byl rowniez konsystentny.
_CHECKSUM_MERGE_DOWNGRADE = f"""
MERGE [{SCHEMA}].[skw_SchemaChecksums] AS target
USING (
    SELECT
        obj.[name]                  AS ObjectName,
        sch.[name]                  AS SchemaName,
        N'VIEW'                     AS ObjectType,
        CHECKSUM(mod.[definition])  AS Checksum,
        N'0035'                     AS AlembicRevision,
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
        [UpdatedAt]       = source.[Now];
"""


def upgrade() -> None:
    op.execute(_VIEW_DDL)
    op.execute(_CHECKSUM_MERGE)


def downgrade() -> None:
    op.execute(_VIEW_DDL_PREVIOUS)
    op.execute(_CHECKSUM_MERGE_DOWNGRADE)