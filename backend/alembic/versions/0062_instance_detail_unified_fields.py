# backend/alembic/versions/0062_instance_detail_unified_fields.py
"""0062_instance_detail_unified_fields

Ujednolicenie kolumn skw_v_approval_instance_detail — na prosbe frontu,
usuniecie przedrostkow fakir_/ksef_ (LAMIACA ZMIANA KONTRAKTU, swiadomie
zaakceptowana: 2026-07-20).

DECYZJE PODJETE JAWNIE (nie domyslne, nie zgadywane):
  1. Stare kolumny fakir_*/ksef_* USUNIETE calkowicie, nie zostaja obok
     nowych. Kazdy inny konsument API czytajacy te nazwy bezposrednio
     PRZESTANIE dzialac po tej migracji.
  2. forma_platnosci: POLACZONE mimo ze fakir_forma_platnosci i
     ksef_forma_platnosci pochodza z DWOCH ROZNYCH SLOWNIKOW kodow
     (WAPRO/Fakir wewnetrzny slownik vs. kod liczbowy KSeF wg specyfikacji
     FA(3), np. "2"=karta). Front ma swiadomosc tej roznicy i zdecydowal
     sie sam zmapowac kody po swojej stronie — NIE jest to przeoczenie,
     to jawna decyzja z rozmowy roboczej.

BEZPIECZENSTWO POLACZENIA (dlaczego COALESCE nie tworzy kolizji):
  fakir_* wypelnia sie WYLACZNIE gdy ds.source_name='fakir' (warunek w
  LEFT JOIN). ksef_* wypelnia sie z extra_data (JSON), ktore ma tresc
  tylko dla zrodel faktycznie zapisujacych te klucze (KSeF). Dla KAZDEGO
  pojedynczego wiersza tylko JEDNA z tych dwoch grup jest niepusta —
  zweryfikowane empirycznie na zywych danych (id_instance=574 Fakir,
  id_instance=586/590 KSeF) przed napisaniem tej migracji.

BRAK ODPOWIEDNIKA — pola pozostaja Fakir-only (bez przedrostka, NULL dla
KSeF), bo KSeF nie ma koncepcyjnego odpowiednika:
  kwota_vat, status_zewnetrzny, status_opis, uwagi
(patrz uzasadnienie w migracji 0061 — integracja dwustronna z WAPRO,
bez odpowiednika w jednostronnej synchronizacji KSeF).

document_amount NIE jest dublowane jako "wartosc_brutto" — ta kolumna juz
istnieje, jest juz source-agnostic i juz poprawnie wypelniona dla obu
zrodel (zweryfikowane na id_instance=586/590).

NAPRAWIONE OD RAZU (nie przeoczone jak w 0061): checksum widoku
zarejestrowany w tym samym upgrade(), zaraz po CREATE OR ALTER VIEW —
patrz incydent SCHEMA TAMPER DETECTED z migracji 0061 (2026-07-20),
ktory zablokowal start aplikacji przez brakujacy krok MERGE.

Revision ID : 0062
Revises     : 0061
"""
from alembic import op

revision = "0062"
down_revision = "0061"
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
    -- ── UJEDNOLICONE 2026-07-20 (migracja 0062) — bez przedrostkow
    --    fakir_/ksef_, patrz decyzje w docstringu tej migracji ───────────
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
    fah.[KWOTA_VAT]                                                    AS kwota_vat,        -- Fakir-only, brak odpowiednika KSeF
    COALESCE(
        fah.[TerminPlatnosci],
        TRY_CONVERT(DATE, JSON_VALUE(dai.[extra_data], '$.payment_deadline'))
    )                                                                   AS termin_platnosci,
    -- UWAGA: polaczenie mimo roznicy slownikow (fakir vs KSeF) —
    -- swiadoma decyzja, patrz docstring migracji.
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

# Poprzednia wersja (0061, z przedrostkami fakir_/ksef_) — potrzebna do downgrade.
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
    fah.[UWAGI]                         AS fakir_uwagi,
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


def _checksum_merge(alembic_revision: str) -> str:
    """Wzorzec identyczny jak w 0035/0061 — NIGDY nie pomijac tego kroku."""
    return f"""
MERGE [{SCHEMA}].[skw_SchemaChecksums] AS target
USING (
    SELECT
        obj.[name]                  AS ObjectName,
        sch.[name]                  AS SchemaName,
        N'VIEW'                     AS ObjectType,
        CHECKSUM(mod.[definition])  AS Checksum,
        N'{alembic_revision}'       AS AlembicRevision,
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
    op.execute(_checksum_merge("0062"))


def downgrade() -> None:
    op.execute(_VIEW_DDL_PREVIOUS)
    op.execute(_checksum_merge("0061"))