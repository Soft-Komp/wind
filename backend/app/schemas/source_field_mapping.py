# backend/app/schemas/source_field_mapping.py
"""
Schematy Pydantic dla mapowania pol zrodel dokumentow -> UnifiedDocument.

Kontrakt: GET/PUT /admin/sources/{id_source}/field-mappings

Ustalenia bezpieczenstwa (sesja 2026-07-07):
    - common_field: brak zamknietej listy kanonicznej (decyzja swiadoma —
      Michal: "zrob tak jak uwazasz"). Wybrano: dowolna nazwa snake_case,
      opcjonalny prefiks 'extra.' dla pol trafiajacych do extra_data.
    - source_field: nazwa pola w zrodle. WAPRO uzywa UPPER_SNAKE_CASE,
      niektore API moga zwracac camelCase lub pola zagniezdzone przez
      kropke — regex jest permisywny co do wielkosci liter, restrykcyjny
      co do dozwolonych znakow (zero SQL, zero whitespace, zero cudzyslowow).
    - transform_expression: NIGDY nie interpolowane surowo do SQL.
      Decyzja Michala: "jesli jest to teraz tak robione to musi zostac"
      (kolumna NVARCHAR(500) zostaje wolnym tekstem jako kontrakt danych),
      ALE kazda wartosc musi przejsc przez ZAMKNIETA whitelist wzorcow
      (_TRANSFORM_WHITELIST_PATTERNS) — cokolwiek poza nia jest odrzucane
      z 422, NIGDY nie trafia do zapytania SQL. To jest miejsce, w ktorym
      "bezpieczniejsza opcja" (wybor domyslny przy niejasnosciach) zostala
      zastosowana wprost.

Kazda zmiana _TRANSFORM_WHITELIST_PATTERNS wymaga swiadomego przegladu
bezpieczenstwa — to jest jedyna sciezka, przez ktora tekst od administratora
moze wplynac na strukture wykonywanego zapytania SQL.
"""
import re
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# =============================================================================
# Whitelist wzorcow transform_expression
# =============================================================================
# "val" jest literalnym placeholderem — worker synchronizacji (source_sync_task)
# podmienia go na rzeczywista referencje kolumny/pola tuz przed wykonaniem.
# Zaden z ponizszych wzorcow nie dopuszcza srednikow, komentarzy SQL (--, /* */),
# zagniezdzonych SELECT-ow ani cudzyslowow poza jednym, zaszytym na sztywno
# literalem daty Clarion.

_TRANSFORM_WHITELIST_PATTERNS: list[re.Pattern] = [
    # Data Clarion (dni od 1899-12-30) — jedyny dopuszczalny literal-string
    re.compile(r"^DATEADD\(\s*DAY\s*,\s*val\s*,\s*'18991230'\s*\)$", re.IGNORECASE),
    # Zaokraglenie liczby dziesietnej
    re.compile(r"^ROUND\(\s*val\s*,\s*\d{1,2}\s*\)$", re.IGNORECASE),
    # Rzutowanie typu (CAST)
    re.compile(
        r"^CAST\(\s*val\s+AS\s+"
        r"(DECIMAL\(\s*\d{1,2}\s*,\s*\d{1,2}\s*\)|INT|BIGINT|"
        r"NVARCHAR\(\s*\d{1,4}\s*\)|DATE|DATETIME2?)\s*\)$",
        re.IGNORECASE,
    ),
    # Rzutowanie typu (CONVERT)
    re.compile(
        r"^CONVERT\(\s*"
        r"(DECIMAL\(\s*\d{1,2}\s*,\s*\d{1,2}\s*\)|INT|BIGINT|"
        r"NVARCHAR\(\s*\d{1,4}\s*\)|DATE|DATETIME2?)\s*,\s*val\s*\)$",
        re.IGNORECASE,
    ),
    # Przycinanie bialych znakow
    re.compile(r"^TRIM\(\s*val\s*\)$", re.IGNORECASE),
    re.compile(r"^LTRIM\(\s*RTRIM\(\s*val\s*\)\s*\)$", re.IGNORECASE),
]

_COMMON_FIELD_RE = re.compile(r"^[a-z][a-z0-9_]{0,49}(\.[a-z][a-z0-9_]{0,49})?$")
_SOURCE_FIELD_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,99}(\.[A-Za-z][A-Za-z0-9_]{0,99}){0,3}$")

FieldType = Literal["string", "decimal", "date", "int"]


def is_transform_expression_safe(value: str) -> bool:
    """
    Jedyne miejsce prawdy o bezpieczenstwie transform_expression.
    Wywolywane zarowno przez walidator Pydantic (pierwsza linia obrony)
    jak i przez field_mapping_service tuz przed zapisem do bazy
    (druga, niezalezna linia obrony — defense in depth).
    """
    return any(p.fullmatch(value.strip()) for p in _TRANSFORM_WHITELIST_PATTERNS)


class FieldMappingItem(BaseModel):
    """Pojedyncza pozycja mapowania: pole zrodla -> pole UnifiedDocument."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    common_field: str = Field(
        ..., min_length=1, max_length=100,
        description=(
            "Nazwa docelowa w UnifiedDocument, snake_case. "
            "Prefiks 'extra.' dla pol trafiajacych do extra_data "
            "(np. 'document_amount', 'extra.ksef_id')."
        ),
    )
    source_field: str = Field(
        ..., min_length=1, max_length=200,
        description=(
            "Nazwa pola w zrodle — kolumna WAPRO (UPPER_SNAKE_CASE) "
            "lub klucz API (mozliwe zagniezdzenie przez kropke)."
        ),
    )
    field_type: FieldType = Field(
        ..., description="string | decimal | date | int — zgodne z CHECK w skw_document_source_field_mappings.",
    )
    transform_expression: Optional[str] = Field(
        default=None, max_length=500,
        description="Opcjonalna transformacja SQL — WYLACZNIE wzorce z zamknietej whitelisty.",
    )

    @field_validator("common_field")
    @classmethod
    def _validate_common_field(cls, v: str) -> str:
        if not _COMMON_FIELD_RE.match(v):
            raise ValueError(
                "common_field musi byc snake_case (male litery, cyfry, podkreslenia), "
                "opcjonalnie z prefiksem 'extra.' (np. 'document_amount', 'extra.ksef_id'). "
                f"Otrzymano: {v!r}"
            )
        return v

    @field_validator("source_field")
    @classmethod
    def _validate_source_field(cls, v: str) -> str:
        if not _SOURCE_FIELD_RE.match(v):
            raise ValueError(
                "source_field zawiera niedozwolone znaki. Dozwolone: litery, cyfry, "
                "podkreslenia, opcjonalne zagniezdzenie przez kropke (max 3 poziomy). "
                f"Otrzymano: {v!r}"
            )
        return v

    @field_validator("transform_expression")
    @classmethod
    def _validate_transform_expression(cls, v: Optional[str]) -> Optional[str]:
        if v is None or v.strip() == "":
            return None
        v_stripped = v.strip()
        if not is_transform_expression_safe(v_stripped):
            raise ValueError(
                "transform_expression nie pasuje do zadnego dozwolonego wzorca "
                "(ochrona przed wstrzyknieciem SQL). Dozwolone: DATEADD dla dat "
                "Clarion, ROUND, CAST, CONVERT, TRIM, LTRIM(RTRIM(...)) z "
                "placeholderem 'val'. Jesli potrzebujesz nowego wzorca — zgloszenie "
                "do zespolu backendu, nie obejscie walidacji. "
                f"Otrzymano: {v_stripped!r}"
            )
        return v_stripped


class FieldMappingsReplaceRequest(BaseModel):
    """
    Body dla PUT /admin/sources/{id_source}/field-mappings.

    Semantyka: PELNA ZAMIANA (replace-all) — wszystkie istniejace mapowania
    tego zrodla sa zastepowane podana lista, w jednej transakcji.
    Pusta lista = usuniecie wszystkich mapowan zrodla (swiadoma decyzja
    operatora, nie blad).
    """

    model_config = ConfigDict(extra="forbid")

    mappings: list[FieldMappingItem] = Field(
        default_factory=list, max_length=200,
        description="Pelna, docelowa lista mapowan zrodla.",
    )

    @model_validator(mode="after")
    def _validate_unique_common_fields(self) -> "FieldMappingsReplaceRequest":
        seen: set[str] = set()
        duplicates: set[str] = set()
        for item in self.mappings:
            key = item.common_field.lower()
            if key in seen:
                duplicates.add(item.common_field)
            seen.add(key)
        if duplicates:
            raise ValueError(
                f"Zduplikowane common_field w jednym zadaniu: {sorted(duplicates)}. "
                "Kazde pole docelowe moze byc zmapowane tylko raz na zrodlo."
            )
        return self


class FieldMappingOut(BaseModel):
    """Odpowiedz GET/PUT — pojedyncza pozycja z metadanymi bazy."""

    model_config = ConfigDict(from_attributes=True)

    id_mapping: int
    id_source: int
    common_field: str
    source_field: str
    field_type: str
    transform_expression: Optional[str] = None