"""0016_dashboard_views

Tworzy trzy widoki SQL dla modułu dashboardu:

  1. dbo.skw_dashboard_debt_stats
     Agregaty zadłużenia dla StatsGrid + CategoryChart + TopDluznicy.
     Źródło: dbo.skw_kontrahenci (widok projektu — migracja 0011).

  2. dbo.skw_dashboard_monit_stats
     Statystyki monitów dla StatsGrid + ChannelChart + TrendChart.
     Źródło: dbo_ext.skw_MonitHistory.

  3. dbo.skw_dashboard_activity
     Oś czasu ostatniej aktywności (monity + komentarze).
     Źródło: dbo_ext.skw_MonitHistory + dbo_ext.skw_Comments
             + dbo.skw_kontrahenci.

Wymagania wstępne (upgrade przerywa z błędem jeśli brakuje):
  - dbo.skw_kontrahenci          (migracja 0011)
  - dbo_ext.skw_MonitHistory     (migracja 0001+)
  - dbo_ext.skw_Comments         (migracja 0001+)

Revision ID: 0016
Revises:     0015
Create Date: 2026-04-15
"""

from __future__ import annotations

import logging
import textwrap
from typing import Final

import sqlalchemy as sa
from alembic import op

# ---------------------------------------------------------------------------
# Metadane Alembic
# ---------------------------------------------------------------------------
revision:      str = "0016"
down_revision: str = "0015"
branch_labels       = None
depends_on          = None

logger = logging.getLogger(f"alembic.migration.{revision}")

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------
SCHEMA_DBO: Final[str] = "dbo"
SCHEMA_EXT: Final[str] = "dbo_ext"

VIEWS: Final[list[str]] = [
    "skw_dashboard_debt_stats",
    "skw_dashboard_monit_stats",
    "skw_dashboard_activity",
]

# ---------------------------------------------------------------------------
# DDL widoków
# ---------------------------------------------------------------------------

_VIEW_DEBT_STATS: Final[str] = """\
CREATE OR ALTER VIEW dbo.skw_dashboard_debt_stats
AS
-- Globalny wiersz statystyk
SELECT
    NULL                                        AS ID_KONTRAHENTA,
    NULL                                        AS NazwaKontrahenta,
    SUM(k.SumaDlugu)                            AS SumaDlugu,
    COUNT(*)                                    AS LiczbaKontrahentow,
    NULL                                        AS LiczbaFaktur,
    NULL                                        AS DniPrzeterminowania,
    SUM(CASE WHEN k.DniPrzeterminowania > 60
             THEN k.SumaDlugu ELSE 0 END)       AS Zagrozone,
    SUM(CASE WHEN k.DniPrzeterminowania <= 30
             THEN k.SumaDlugu ELSE 0 END)       AS Kat_0_30,
    SUM(CASE WHEN k.DniPrzeterminowania BETWEEN 31 AND 60
             THEN k.SumaDlugu ELSE 0 END)       AS Kat_31_60,
    SUM(CASE WHEN k.DniPrzeterminowania BETWEEN 61 AND 90
             THEN k.SumaDlugu ELSE 0 END)       AS Kat_61_90,
    SUM(CASE WHEN k.DniPrzeterminowania > 90
             THEN k.SumaDlugu ELSE 0 END)       AS Kat_Powyzej90
FROM dbo.skw_kontrahenci AS k

UNION ALL

-- Wiersze per kontrahent (do TopDluznicy)
SELECT
    k.ID_KONTRAHENTA,
    k.NazwaKontrahenta,
    k.SumaDlugu,
    NULL                                        AS LiczbaKontrahentow,
    k.LiczbaFaktur,
    k.DniPrzeterminowania,
    NULL                                        AS Zagrozone,
    NULL                                        AS Kat_0_30,
    NULL                                        AS Kat_31_60,
    NULL                                        AS Kat_61_90,
    NULL                                        AS Kat_Powyzej90
FROM dbo.skw_kontrahenci AS k;
"""

_VIEW_MONIT_STATS: Final[str] = """\
CREATE OR ALTER VIEW dbo.skw_dashboard_monit_stats
AS
-- A) Globalny agregat
SELECT
    NULL                                            AS Miesiac,
    NULL                                            AS Kanal,
    COUNT(*)                                        AS Wyslane,
    SUM(CASE WHEN m.OpenedAt IS NOT NULL
             THEN 1 ELSE 0 END)                     AS Otwarte,
    SUM(CASE WHEN m.ClickedAt IS NOT NULL
             THEN 1 ELSE 0 END)                     AS Klikniete,
    CAST(
        CASE WHEN COUNT(*) > 0
             THEN (SUM(CASE WHEN m.OpenedAt IS NOT NULL THEN 1.0 ELSE 0 END)
                   / COUNT(*)) * 100
             ELSE 0
        END
    AS DECIMAL(5,2))                                AS Skutecznosc,
    NULL                                            AS SumaDlugow,
    'global'                                        AS TypWiersza
FROM dbo_ext.skw_MonitHistory AS m
WHERE m.IsActive = 1
  AND m.Status   IN ('sent', 'delivered', 'opened', 'clicked')

UNION ALL

-- B) Per kanał (ostatnie 30 dni)
SELECT
    NULL                                            AS Miesiac,
    m.MonitType                                     AS Kanal,
    COUNT(*)                                        AS Wyslane,
    SUM(CASE WHEN m.OpenedAt IS NOT NULL
             THEN 1 ELSE 0 END)                     AS Otwarte,
    SUM(CASE WHEN m.ClickedAt IS NOT NULL
             THEN 1 ELSE 0 END)                     AS Klikniete,
    CAST(
        CASE WHEN COUNT(*) > 0
             THEN (SUM(CASE WHEN m.OpenedAt IS NOT NULL THEN 1.0 ELSE 0 END)
                   / COUNT(*)) * 100
             ELSE 0
        END
    AS DECIMAL(5,2))                                AS Skutecznosc,
    NULL                                            AS SumaDlugow,
    'kanal'                                         AS TypWiersza
FROM dbo_ext.skw_MonitHistory AS m
WHERE m.IsActive  = 1
  AND m.Status    IN ('sent', 'delivered', 'opened', 'clicked')
  AND m.CreatedAt >= DATEADD(DAY, -30, GETDATE())
GROUP BY m.MonitType

UNION ALL

-- C) Per miesiąc — trend ostatnie 6 miesięcy
SELECT
    CONVERT(NVARCHAR(7), m.CreatedAt, 120)          AS Miesiac,
    NULL                                            AS Kanal,
    COUNT(*)                                        AS Wyslane,
    SUM(CASE WHEN m.OpenedAt IS NOT NULL
             THEN 1 ELSE 0 END)                     AS Otwarte,
    SUM(CASE WHEN m.ClickedAt IS NOT NULL
             THEN 1 ELSE 0 END)                     AS Klikniete,
    NULL                                            AS Skutecznosc,
    SUM(ISNULL(m.TotalDebt, 0))                     AS SumaDlugow,
    'trend'                                         AS TypWiersza
FROM dbo_ext.skw_MonitHistory AS m
WHERE m.IsActive  = 1
  AND m.CreatedAt >= DATEADD(MONTH, -6, GETDATE())
GROUP BY CONVERT(NVARCHAR(7), m.CreatedAt, 120);
"""

_VIEW_ACTIVITY: Final[str] = """\
CREATE OR ALTER VIEW dbo.skw_dashboard_activity
AS
-- Monity
SELECT
    CAST(m.ID_MONIT AS BIGINT)                      AS ID_ZDARZENIA,
    CASE
        WHEN m.Status = 'bounced'  THEN 'monit_bounced'
        WHEN m.Status = 'opened'
          OR m.Status = 'clicked'  THEN 'monit_otwarty'
        ELSE                            'monit_wyslany'
    END                                             AS TypZdarzenia,
    CASE
        WHEN m.Status = 'bounced'
            THEN N'Email został odrzucony'
        WHEN m.Status IN ('opened', 'clicked')
            THEN N'Monit został otwarty'
        ELSE
            N'Wysłano monit ' + UPPER(m.MonitType)
    END                                             AS Opis,
    k.NazwaKontrahenta                              AS Kontrahent,
    m.TotalDebt                                     AS Kwota,
    ISNULL(m.SentAt, m.CreatedAt)                   AS DataZdarzenia,
    CASE
        WHEN m.Status = 'bounced'                   THEN 'red'
        WHEN m.Status IN ('opened', 'clicked')      THEN 'blue'
        WHEN m.MonitType = 'email'                  THEN 'blue'
        WHEN m.MonitType = 'sms'                    THEN 'green'
        ELSE                                             'purple'
    END                                             AS Kolor,
    'monit'                                         AS ZrodloDanych
FROM dbo_ext.skw_MonitHistory AS m
LEFT JOIN dbo.skw_kontrahenci AS k
    ON k.ID_KONTRAHENTA = m.ID_KONTRAHENTA
WHERE m.IsActive = 1

UNION ALL

-- Komentarze
SELECT
    CAST(c.ID_COMMENT AS BIGINT)                    AS ID_ZDARZENIA,
    'komentarz'                                     AS TypZdarzenia,
    N'Dodano komentarz do sprawy'                   AS Opis,
    k.NazwaKontrahenta                              AS Kontrahent,
    NULL                                            AS Kwota,
    c.CreatedAt                                     AS DataZdarzenia,
    'orange'                                        AS Kolor,
    'komentarz'                                     AS ZrodloDanych
FROM dbo_ext.skw_Comments AS c
LEFT JOIN dbo.skw_kontrahenci AS k
    ON k.ID_KONTRAHENTA = c.ID_KONTRAHENTA
WHERE c.IsActive = 1;
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _check_prerequisites(bind: sa.engine.Connection) -> None:
    """
    Weryfikuje wymagania wstępne. RAISERROR przerywa migrację jeśli brakuje.
    """
    sql = textwrap.dedent("""\
        -- skw_kontrahenci (migracja 0011)
        IF NOT EXISTS (
            SELECT 1 FROM sys.views v
            INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
            WHERE s.name = N'dbo' AND v.name = N'skw_kontrahenci'
        )
            RAISERROR(N'[0016] Wymagany widok dbo.skw_kontrahenci nie istnieje. Uruchom migrację 0011.', 16, 1);

        -- skw_MonitHistory (migracja 0001+)
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'dbo_ext' AND t.name = N'skw_MonitHistory'
        )
            RAISERROR(N'[0016] Wymagana tabela dbo_ext.skw_MonitHistory nie istnieje.', 16, 1);

        -- skw_Comments (migracja 0001+)
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = N'dbo_ext' AND t.name = N'skw_Comments'
        )
            RAISERROR(N'[0016] Wymagana tabela dbo_ext.skw_Comments nie istnieje.', 16, 1);

        PRINT N'[0016] Weryfikacja wymagań wstępnych — OK.';
    """)
    bind.execute(sa.text(sql))


def _merge_checksum(bind: sa.engine.Connection, view_name: str, revision_id: str) -> None:
    """Rejestruje lub aktualizuje checksum widoku w skw_SchemaChecksums."""
    sql = textwrap.dedent(f"""\
        MERGE [{SCHEMA_EXT}].[skw_SchemaChecksums] AS tgt
        USING (
            SELECT
                N'{SCHEMA_DBO}'   AS SchemaName,
                N'{view_name}'    AS ObjectName,
                N'VIEW'           AS ObjectType,
                CHECKSUM((SELECT definition FROM sys.sql_modules m
                      JOIN sys.objects o ON m.object_id = o.object_id
                      JOIN sys.schemas s ON o.schema_id = s.schema_id
                      WHERE s.name = N'{SCHEMA_DBO}' AND o.name = N'{view_name}'))
                                  AS Checksum,
                N'{revision_id}'  AS AlembicRevision,
                GETDATE()         AS UpdatedAt
        ) AS src
        ON  tgt.SchemaName = src.SchemaName
        AND tgt.ObjectName = src.ObjectName
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SchemaName, ObjectName, ObjectType, Checksum, AlembicRevision, UpdatedAt)
            VALUES (src.SchemaName, src.ObjectName, src.ObjectType,
                    src.Checksum,  src.AlembicRevision, src.UpdatedAt)
        WHEN MATCHED THEN
            UPDATE SET
                tgt.Checksum        = src.Checksum,
                tgt.AlembicRevision = src.AlembicRevision,
                tgt.UpdatedAt       = src.UpdatedAt;
    """)
    bind.execute(sa.text(sql))
    logger.info("[%s] SchemaChecksums MERGE OK — %s.%s", revision, SCHEMA_DBO, view_name)


def _drop_view(bind: sa.engine.Connection, view_name: str) -> None:
    bind.execute(sa.text(
        f"IF OBJECT_ID(N'[{SCHEMA_DBO}].[{view_name}]', N'V') IS NOT NULL "
        f"DROP VIEW [{SCHEMA_DBO}].[{view_name}];"
    ))
    logger.info("[%s] DROP VIEW IF EXISTS %s.%s — OK", revision, SCHEMA_DBO, view_name)


def _remove_checksum(bind: sa.engine.Connection, view_name: str) -> None:
    bind.execute(sa.text(
        f"DELETE FROM [{SCHEMA_EXT}].[skw_SchemaChecksums] "
        f"WHERE SchemaName = N'{SCHEMA_DBO}' AND ObjectName = N'{view_name}';"
    ))
    logger.info("[%s] SchemaChecksums DELETE — %s.%s", revision, SCHEMA_DBO, view_name)


# ===========================================================================
# UPGRADE
# ===========================================================================

def upgrade() -> None:
    """
    Tworzy trzy widoki dashboardu + rejestruje checksums.

    Kolejność:
      1. Weryfikacja wymagań wstępnych
      2. CREATE OR ALTER VIEW × 3
      3. MERGE SchemaChecksums × 3
    """
    logger.info("[%s] UPGRADE START — tworzenie widoków dashboardu", revision)

    bind = op.get_bind()

    # Krok 1: Weryfikacja
    logger.info("[%s] Krok 1/5 — weryfikacja wymagań wstępnych", revision)
    _check_prerequisites(bind)

    # Krok 2: skw_dashboard_debt_stats
    logger.info("[%s] Krok 2/5 — CREATE OR ALTER VIEW skw_dashboard_debt_stats", revision)
    bind.execute(sa.text(_VIEW_DEBT_STATS))
    logger.info("[%s] Krok 2/5 — OK", revision)

    # Krok 3: skw_dashboard_monit_stats
    logger.info("[%s] Krok 3/5 — CREATE OR ALTER VIEW skw_dashboard_monit_stats", revision)
    bind.execute(sa.text(_VIEW_MONIT_STATS))
    logger.info("[%s] Krok 3/5 — OK", revision)

    # Krok 4: skw_dashboard_activity
    logger.info("[%s] Krok 4/5 — CREATE OR ALTER VIEW skw_dashboard_activity", revision)
    bind.execute(sa.text(_VIEW_ACTIVITY))
    logger.info("[%s] Krok 4/5 — OK", revision)

    # Krok 5: Checksums
    logger.info("[%s] Krok 5/5 — MERGE SchemaChecksums × 3", revision)
    for view_name in VIEWS:
        _merge_checksum(bind, view_name, revision)
    logger.info("[%s] Krok 5/5 — OK", revision)

    logger.info(
        "[%s] UPGRADE zakończony — %d widoki dashboardu aktywne",
        revision, len(VIEWS),
    )


# ===========================================================================
# DOWNGRADE
# ===========================================================================

def downgrade() -> None:
    """
    Usuwa trzy widoki dashboardu + wpisy SchemaChecksums.

    Kolejność (odwrotna do upgrade):
      1. DELETE SchemaChecksums × 3
      2. DROP VIEW IF EXISTS × 3
    """
    logger.warning(
        "[%s] DOWNGRADE — usuwanie widoków dashboardu: %s",
        revision, VIEWS,
    )

    bind = op.get_bind()

    for view_name in reversed(VIEWS):
        _remove_checksum(bind, view_name)
        _drop_view(bind, view_name)

    logger.warning(
        "[%s] DOWNGRADE zakończony — widoki dashboardu usunięte",
        revision,
    )