# backend/alembic/versions/0072_fix_naglowek_regression_and_nip.py
"""0072_fix_naglowek_regression_and_nip

NAPRAWA KRYTYCZNEJ REGRESJI wprowadzonej migracja 0071.

--------------------------------------------------------------------------
JAK DO TEGO DOSZLO (pelna rekonstrukcja zdarzen, 2026-07-29):
--------------------------------------------------------------------------

Migracja 0071 dodala kolumny Nip/ADRES_EMAIL do widoku
skw_faktury_akceptacja_naglowek, opierajac sie na tresci pliku
database/ddl/018_faktura_widoki_dbo.sql z bazy wiedzy projektu (marzec 2026).
Ten plik okazal sie NIEAKTUALNY wzgledem tego, co faktycznie dzialalo na
produkcji od dawna.

Rzeczywista historia tego widoku (zrekonstruowana z migracji 0036/0037/0038):

  v3 (migracja 0018, 2026-04-22) — filtry: PRG_KOD=1, KIERUNEK_SYS='Z'
     (NIE PRG_KOD=3/TYP='Z' jak w 018_faktura_widoki_dbo.sql).
     JOIN: KONTRAHENT.KLUCZ = BUF_DOKUMENT.KONTRAHENT_KLUCZ
     (NIE KONTRAHENT.ID_KONTRAHENTA = BUF_DOKUMENT.ID_KONTRAHENTA).
     Konwersja dat: dbo.RM_Func_ClarionDateToDateTime()
     (NIE reczny CASE+DATEADD).

  v4 (migracja 0036, 2026-06-08) — maskowanie deterministyczne
     NazwaKontrahenta/NUMER przez HASHBYTES SHA2_256. FILTRY I JOIN
     BEZ ZMIAN wzgledem v3 — potwierdzone jawnie w docstringu 0036.

  migracja 0038 (2026-06-08) — odwraca maskowanie: upgrade() woloa
     _load("0036").downgrade(), co przywraca DOKLADNIE v3 (bez maskowania).
     Ten stan (v3, bez maskowania) byl zywy na produkcji od 0038 az do 0071.

Migracja 0071 NADPISALA ten stan filtrami/JOIN-em z 018_faktura_widoki_dbo.sql
(PRG_KOD=3, TYP='Z', ID_KONTRAHENTA) — czyli inna, przestarzala definicja,
ktora prawdopodobnie nigdy nie byla stanem produkcyjnym po kwietniu 2026,
albo byla stanem WCZESNIEJSZYM niz v3, zanim ktos (migracja 0018, poza
zakresem tego repo/bazy wiedzy) zmienil kolumny filtrujace.

SKUTEK: kazdy dokument, ktory pasowal do filtra v3 (PRG_KOD=1,
KIERUNEK_SYS='Z') ale NIE pasowal do filtra z 018 (PRG_KOD=3, TYP='Z'),
zniknal z widoku po migracji 0071 — nie tylko przypadek TYP='DH', ktory
sprowokowal to zgloszenie, ale POTENCJALNIE SZERSZY zestaw dokumentow.
Zalecana weryfikacja PO wdrozeniu tej migracji: policzyc wiersze widoku
przed/po i porownac z oczekiwana liczba dokumentow w BUF_DOKUMENT
spelniajacych PRG_KOD=1 AND KIERUNEK_SYS='Z'.

--------------------------------------------------------------------------
CO ROBI TA MIGRACJA:
--------------------------------------------------------------------------
1. Przywraca DOKLADNA strukture v3 (filtry, JOIN, konwersje dat,
   logike NazwaKontrahenta z fallbackiem na KONTRAHENT_KLUCZ) —
   BEZ zadnych dodatkowych "ulepszen" poza tym, co nizej w punkcie 2.
2. DOPIERO na tym poprawnym fundamencie dodaje Nip i ADRES_EMAIL
   (ten sam cel co 0071 mial realizowac, teraz na wlasciwej bazie).
3. Rejestruje checksum jako '0072'.

Zrodlo kolumny NIP: dbo.KONTRAHENT.NIP — potwierdzone recznie w SSMS
przez wlasciciela projektu (2026-07-29). Tabela KONTRAHENT jest tu
osiagana przez klucz KLUCZ (nie ID_KONTRAHENTA jak blednie zalozono
w 0071) — kolumna NIP istnieje na tym samym, fizycznym wierszu
niezaleznie od tego, przez ktory klucz do niego trafiamy.

DOWNGRADE: przywraca dokladnie to, co zrobila migracja 0071 (stan,
z ktorego ta migracja startuje w lancuchu Alembic) — czyli BLEDNA
wersje z PRG_KOD=3/TYP='Z'/ID_KONTRAHENTA. To jest poprawne zachowanie
downgrade w sensie technicznym (cofniecie o jeden krok w lancuchu),
ALE nie jest to "bezpieczny" stan biznesowo — jesli kiedykolwiek
zajdzie potrzeba zejscia ponizej 0072, nalezy od razu przejsc rowniez
ponizej 0071 (downgrade -2), nie zatrzymywac sie na samym 0071.

Revision ID : 0072
Revises     : 0071
"""
from alembic import op

revision = "0072"
down_revision = "0071"
branch_labels = None
depends_on = None

SCHEMA = "dbo"
VIEW = "skw_faktury_akceptacja_naglowek"

# ── Definicja NOWA (0072) — prawdziwa struktura v3 + Nip/ADRES_EMAIL ───────
_VIEW_DDL = f"""
CREATE OR ALTER VIEW [{SCHEMA}].[{VIEW}] AS
SELECT
    bd.[ID_BUF_DOKUMENT],
    bd.[KSEF_ID],
    bd.[NUMER],
    bd.[KOD_STATUSU],
    CASE
        WHEN bd.[KOD_STATUSU] IS NULL THEN N'NOWY'
        WHEN bd.[KOD_STATUSU] = N'K'  THEN N'ZATWIERDZONY'
        WHEN bd.[KOD_STATUSU] = N'A'  THEN N'ZAKSIEGOWANY'
        ELSE bd.[KOD_STATUSU]
    END AS StatusOpis,

    CAST([{SCHEMA}].RM_Func_ClarionDateToDateTime(bd.[DATA_WYSTAWIENIA])  AS DATE) AS DataWystawienia,
    CAST([{SCHEMA}].RM_Func_ClarionDateToDateTime(bd.[DATA_OTRZYMANIA])   AS DATE) AS DataOtrzymania,
    CAST([{SCHEMA}].RM_Func_ClarionDateToDateTime(bd.[TERMIN_PLATNOSCI])  AS DATE) AS TerminPlatnosci,

    bd.[WARTOSC_NETTO],
    bd.[WARTOSC_BRUTTO],
    bd.[KWOTA_VAT],
    bd.[FORMA_PLATNOSCI],
    bd.[UWAGI],
    k.[ID_KONTRAHENTA],

    CASE
        WHEN ISNULL(k.[NAZWA_PELNA], N'') = N''
        THEN bd.[KONTRAHENT_KLUCZ]
        ELSE k.[NAZWA_PELNA]
    END AS NazwaKontrahenta,

    k.[ADRES_EMAIL]      AS EmailKontrahenta,
    k.[TELEFON_FIRMOWY]  AS TelefonKontrahenta,
    -- NOWE (migracja 0072, 2026-07-29) — cel pierwotnie realizowany przez
    -- 0071, teraz na poprawnym fundamencie v3. Kolumna NIP zweryfikowana
    -- recznie w SSMS przez wlasciciela projektu.
    k.[NIP]              AS Nip,
    -- NOWE (migracja 0072) — surowa nazwa kolumny zrodlowej, dodatkowo
    -- obok istniejacego EmailKontrahenta (ktory pozostaje bez zmian).
    k.[ADRES_EMAIL]      AS ADRES_EMAIL

FROM [{SCHEMA}].[BUF_DOKUMENT] AS bd
LEFT JOIN [{SCHEMA}].[KONTRAHENT] AS k
    ON k.[KLUCZ] = bd.[KONTRAHENT_KLUCZ]
WHERE bd.[PRG_KOD]     = 1
  AND bd.[KSEF_ID]      IS NOT NULL
  AND bd.[KIERUNEK_SYS] = N'Z'
"""

# ── Definicja POPRZEDNIA (0071) — dokladna kopia bledenej wersji, uzywana
#    WYLACZNIE w downgrade(), zeby lancuch Alembic byl spojny ─────────────
_VIEW_DDL_PREVIOUS_0071 = f"""
CREATE OR ALTER VIEW [{SCHEMA}].[{VIEW}] AS
SELECT
    bd.[ID_BUF_DOKUMENT],
    bd.[KSEF_ID],
    bd.[NUMER],
    bd.[KOD_STATUSU],
    CASE
        WHEN bd.[KOD_STATUSU] IS NULL THEN N'NOWY'
        WHEN bd.[KOD_STATUSU] = 'K'   THEN N'ZATWIERDZONY'
        WHEN bd.[KOD_STATUSU] = 'A'   THEN N'ZAKSIEGOWANY'
        ELSE bd.[KOD_STATUSU]
    END AS StatusOpis,
    CASE
        WHEN bd.[DATA_WYSTAWIENIA] IS NULL OR bd.[DATA_WYSTAWIENIA] = 0 THEN NULL
        ELSE CAST(DATEADD(DAY, bd.[DATA_WYSTAWIENIA], '18991230') AS DATE)
    END AS DataWystawienia,
    CASE
        WHEN bd.[DATA_OTRZYMANIA] IS NULL OR bd.[DATA_OTRZYMANIA] = 0 THEN NULL
        ELSE CAST(DATEADD(DAY, bd.[DATA_OTRZYMANIA], '18991230') AS DATE)
    END AS DataOtrzymania,
    CASE
        WHEN bd.[TERMIN_PLATNOSCI] IS NULL OR bd.[TERMIN_PLATNOSCI] = 0 THEN NULL
        ELSE CAST(DATEADD(DAY, bd.[TERMIN_PLATNOSCI], '18991230') AS DATE)
    END AS TerminPlatnosci,
    bd.[WARTOSC_NETTO],
    bd.[WARTOSC_BRUTTO],
    bd.[KWOTA_VAT],
    bd.[FORMA_PLATNOSCI],
    bd.[UWAGI],
    k.[NAZWA]            AS NazwaKontrahenta,
    k.[ADRES_EMAIL]       AS EmailKontrahenta,
    k.[TELEFON_FIRMOWY]  AS TelefonKontrahenta,
    k.[NIP]              AS Nip,
    k.[ADRES_EMAIL]      AS ADRES_EMAIL
FROM [{SCHEMA}].[BUF_DOKUMENT] bd
LEFT JOIN [{SCHEMA}].[KONTRAHENT] k
    ON k.[ID_KONTRAHENTA] = bd.[ID_KONTRAHENTA]
WHERE
    bd.[PRG_KOD]    = 3
    AND bd.[KSEF_ID] IS NOT NULL
    AND bd.[TYP]    = 'Z'
"""


def _checksum_merge(revision_tag: str) -> str:
    """Wzorzec identyczny jak w migracjach 0061/0062/0066/0067/0071."""
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
    op.execute(_checksum_merge("0072"))


def downgrade() -> None:
    op.execute(_VIEW_DDL_PREVIOUS_0071)
    op.execute(_checksum_merge("0071"))