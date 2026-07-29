# backend/alembic/versions/0071_faktury_naglowek_nip_email.py
"""0071_faktury_naglowek_nip_email

Rozszerza widok skw_faktury_akceptacja_naglowek o dwa pola z dbo.KONTRAHENT,
na wniosek frontu (2026-07-29): brak dostepu do NIP kontrahenta.

NIP — kolumna NIGDY nie byla w tym widoku. Potwierdzone wczesniej w tej
samej sesji (i juz udokumentowane w Etap2_Instrukcja_Techniczna, sekcja
10.1 erraty, oraz w komentarzu unified_document.py: "nip=None # NIP nie
istnieje w aktualnym widoku"). Nazwa kolumny w dbo.KONTRAHENT zweryfikowana
recznie przez wlasciciela projektu w SSMS (2026-07-29): NIP, bez prefiksu/
sufiksu.

ADRES_EMAIL — kolumna JUZ byla eksponowana w tym widoku pod aliasem
EmailKontrahenta. Ta migracja DODAJE surowa nazwe kolumny ADRES_EMAIL
jako DRUGIE, dodatkowe pole obok istniejacego EmailKontrahenta (ktory
NIE jest usuwany — zero ryzyka regresji dla obecnych konsumentow API).
Jesli po wdrozeniu okaze sie, ze front nie chcial tego duplikatu,
usuniecie jednej linii z definicji widoku w kolejnej migracji.

UWAGA — bonusowy, ZWIAZANY problem NIE naprawiany w tej migracji:
FakirDocumentAdapter (backend/app/schemas/unified_document.py) hardkoduje
dzis nip=None przy budowaniu UnifiedDocument, z komentarzem odwolujacym
sie do braku tej kolumny w widoku (patrz tez migracja 0069, docstring:
"nip=NULL, bo aktualny widok... nie ma kolumny NIP"). Po tej migracji
kolumna JUZ istnieje, ale adapter jej jeszcze nie czyta — NIP dalej
bedzie NULL w UnifiedDocument.nip / extra_data.nip dla NOWYCH dokumentow
zrodla 'fakir', dopoki adapter nie zostanie osobno poprawiony. To
naprawiloby przy okazji martwe dzis warunki filtrow automatycznych na
polu 'nip' (4 sztuki, zidentyfikowane wczesniej w tej samej sesji) — ale
to OSOBNA, swiadoma zmiana kodu Python, nie zakres tej migracji SQL.

Podobnie: czy front faktycznie dostanie NIP w odpowiedzi API zalezy od
tego, z ktorego endpointu korzysta. Legacy /faktury-akceptacja filtruje
pola przez whitelist uprawnien faktury.pole.* (13_faktura_pole_permissions.sql)
- NIP nie ma tam dzis wpisu, wiec nawet po tej migracji serwis moze
nadal wycinac to pole z odpowiedzi, dopoki nie powstanie i nie zostanie
przypisane uprawnienie faktury.pole.nip. To rowniez OSOBNA zmiana,
zalezna od potwierdzenia ktorego endpointu dotyczy zgloszenie.

Revision ID : 0071
Revises     : 0070
"""
from alembic import op

revision = "0071"
down_revision = "0070"
branch_labels = None
depends_on = None

SCHEMA = "dbo"
VIEW = "skw_faktury_akceptacja_naglowek"

# ── Definicja NOWA (0071) — dodane Nip i ADRES_EMAIL ───────────────────────
_VIEW_DDL = f"""
CREATE OR ALTER VIEW [{SCHEMA}].[{VIEW}] AS
SELECT
    -- ── Identyfikatory ──────────────────────────────────────────────────────
    bd.[ID_BUF_DOKUMENT],
    bd.[KSEF_ID],
    bd.[NUMER],

    -- ── Status faktury w WAPRO ──────────────────────────────────────────────
    bd.[KOD_STATUSU],
    CASE
        WHEN bd.[KOD_STATUSU] IS NULL THEN N'NOWY'
        WHEN bd.[KOD_STATUSU] = 'K'   THEN N'ZATWIERDZONY'
        WHEN bd.[KOD_STATUSU] = 'A'   THEN N'ZAKSIEGOWANY'
        ELSE bd.[KOD_STATUSU]
    END AS StatusOpis,

    -- ── Daty (Clarion INT -> DATE) ───────────────────────────────────────────
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

    -- ── Wartości finansowe ──────────────────────────────────────────────────
    bd.[WARTOSC_NETTO],
    bd.[WARTOSC_BRUTTO],
    bd.[KWOTA_VAT],

    -- ── Płatność ────────────────────────────────────────────────────────────
    bd.[FORMA_PLATNOSCI],
    bd.[UWAGI],

    -- ── Dane kontrahenta (LEFT JOIN — może być NULL) ─────────────────────────
    k.[NAZWA]            AS NazwaKontrahenta,
    k.[ADRES_EMAIL]       AS EmailKontrahenta,
    k.[TELEFON_FIRMOWY]  AS TelefonKontrahenta,
    -- NOWE (migracja 0071, 2026-07-29) — NIP nigdy wczesniej nie byl w tym
    -- widoku. Nazwa kolumny w dbo.KONTRAHENT zweryfikowana recznie w SSMS.
    k.[NIP]              AS Nip,
    -- NOWE (migracja 0071) — surowa nazwa kolumny zrodlowej, DODATKOWO obok
    -- istniejacego EmailKontrahenta (ktory pozostaje bez zmian dla wstecznej
    -- kompatybilnosci). Jesli okaze sie zbedne — usunac jedna linia nizej.
    k.[ADRES_EMAIL]      AS ADRES_EMAIL

FROM [{SCHEMA}].[BUF_DOKUMENT] bd
LEFT JOIN [{SCHEMA}].[KONTRAHENT] k
    ON k.[ID_KONTRAHENTA] = bd.[ID_KONTRAHENTA]

WHERE
    bd.[PRG_KOD]    = 3             -- tylko Fakir
    AND bd.[KSEF_ID] IS NOT NULL    -- tylko faktury z KSeF
    AND bd.[TYP]    = 'Z'           -- tylko zakupowe
"""

# ── Definicja POPRZEDNIA (przed 0071) — dokladna kopia z database/ddl/
#    018_faktura_widoki_dbo.sql, uzywana wylacznie w downgrade() ───────────
_VIEW_DDL_PREVIOUS = f"""
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
    k.[ADRES_EMAIL]      AS EmailKontrahenta,
    k.[TELEFON_FIRMOWY]  AS TelefonKontrahenta

FROM [{SCHEMA}].[BUF_DOKUMENT] bd
LEFT JOIN [{SCHEMA}].[KONTRAHENT] k
    ON k.[ID_KONTRAHENTA] = bd.[ID_KONTRAHENTA]

WHERE
    bd.[PRG_KOD]    = 3
    AND bd.[KSEF_ID] IS NOT NULL
    AND bd.[TYP]    = 'Z'
"""


def _checksum_merge(revision_tag: str) -> str:
    """Wzorzec identyczny jak w migracjach 0061/0062/0066/0067 — MERGE do skw_SchemaChecksums."""
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
    op.execute(_checksum_merge("0071"))


def downgrade() -> None:
    op.execute(_VIEW_DDL_PREVIOUS)
    op.execute(_checksum_merge("0070"))