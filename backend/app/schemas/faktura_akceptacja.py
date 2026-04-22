"""
app/schemas/faktura_akceptacja.py
=================================
Pydantic v2 schemas dla modułu Akceptacji Faktur KSeF.

Organizacja:
    1. Enums             — dozwolone wartości statusów, priorytetów, akcji
    2. Base schemas      — wspólne pola
    3. Request schemas   — walidacja danych wejściowych (POST/PATCH body)
    4. Response schemas  — odpowiedzi API
    5. FakturaLogDetails — model JSON dla skw_faktura_log.szczegoly
    6. Confirm schemas   — dwuetapowe operacje (reset, force_status)
    7. WAPRO schemas     — dane z widoków dbo (read-only)

WALIDACJA (zero-trust frontend):
    • Wszystkie string fields: strip whitespace, max length
    • Znaki kontrolne: odrzucone przez validator
    • status/priorytet: wyłącznie wartości z Enum (nie raw string)
    • numer_ksef: regex pattern + max 50 znaków
    • komentarz: max 2000 znaków (nie MAX — zapobiega DOS)
    • user_ids: deduplikacja, min 1 / max z SystemConfig

SANITYZACJA:
    • Każde pole tekstowe przechodzi przez _sanitize_string()
    • HTML stripped ze wszystkich pól opisowych
    • SQL injection nie możliwa (parametryzowane zapytania + ORM)

JSON serializacja:
    • model_config: populate_by_name=True (obsługa alias)
    • datetime: ISO 8601 z timezone UTC
    • Decimal: serialized as float (wartości finansowe)
"""

import hashlib
import re
import unicodedata
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Optional

from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
    ConfigDict,
)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Enums — dozwolone wartości (spójne z CHECK constraints w DB)
# ─────────────────────────────────────────────────────────────────────────────

class StatusWewnetrzny(str, Enum):
    """Status faktury w systemie akceptacji. Musi być spójny z CHK_sfa_status_wewnetrzny."""
    NOWE          = "nowe"
    W_TOKU        = "w_toku"
    ZAAKCEPTOWANA = "zaakceptowana"
    ANULOWANA     = "anulowana"
    ORPHANED      = "orphaned"   # ET-01: faktura zniknęła z WAPRO


class Priorytet(str, Enum):
    """Priorytet faktury. Musi być spójny z CHK_sfa_priorytet."""
    NORMALNY      = "normalny"
    PILNY         = "pilny"
    BARDZO_PILNY  = "bardzo_pilny"


class StatusPrzypisania(str, Enum):
    """Decyzja pracownika. Musi być spójny z CHK_sfp_status."""
    OCZEKUJE      = "oczekuje"
    ZAAKCEPTOWANE = "zaakceptowane"
    ODRZUCONE     = "odrzucone"
    NIE_MOJE      = "nie_moje"


class AkcjaLog(str, Enum):
    """Dozwolone akcje w skw_faktura_log. Musi być spójny z CHK_sfl_akcja."""
    PRZYPISANO          = "przypisano"
    ZAAKCEPTOWANO       = "zaakceptowano"
    ODRZUCONO           = "odrzucono"
    ZRESETOWANO         = "zresetowano"
    STATUS_ZMIENIONY    = "status_zmieniony"
    PRIORYTET_ZMIENIONY = "priorytet_zmieniony"
    FAKIR_UPDATE        = "fakir_update"
    FAKIR_UPDATE_FAILED = "fakir_update_failed"
    NIE_MOJE            = "nie_moje"
    FORCE_AKCEPTACJA    = "force_akceptacja"
    ANULOWANO           = "anulowano"


class PowodInterwencji(str, Enum):
    """Powód eventu SSE faktura_wymaga_interwencji."""
    WSZYSCY_ODMOWILI  = "wszyscy_odmowili"
    WSZYSCY_NIE_MOJE  = "wszyscy_nie_moje"
    MIESZANE          = "mieszane"


class ForceStatusScope(str, Enum):
    """Scope dla confirm_token force_status."""
    FORCE_AKCEPTACJA   = "confirm_force_akceptacja"
    ANULUJ_FAKTURA     = "confirm_anuluj_faktura"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers — sanityzacja
# ─────────────────────────────────────────────────────────────────────────────

def _sanitize_string(value: str) -> str:
    """
    Sanityzacja tekstu wejściowego — linia obrony #1.
    1. NFC unicode normalization
    2. Strip whitespace
    3. Usunięcie znaków kontrolnych (oprócz \t, \n, \r)
    4. Kolaps wielokrotnych spacji (opcjonalnie)
    """
    if not value:
        return value
    normalized = unicodedata.normalize("NFC", value)
    stripped   = normalized.strip()
    cleaned    = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", stripped)
    return cleaned


def _strip_html_basic(value: str) -> str:
    """Usunięcie tagów HTML z pól opisowych — prosta ochrona XSS."""
    return re.sub(r"<[^>]+>", "", value)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Base schemas
# ─────────────────────────────────────────────────────────────────────────────

class FakturaBase(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        str_strip_whitespace=True,
        use_enum_values=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# 3. Request schemas
# ─────────────────────────────────────────────────────────────────────────────

class FakturaCreateRequest(FakturaBase):
    """
    POST /faktury-akceptacja — wpuszczenie faktury do obiegu.

    numer_ksef: unikalny identyfikator z KSeF — nie może być zmieniony po utworzeniu.
    user_ids:   lista ID pracowników do przypisania (min 1, max 10 domyślnie).
    priorytet:  domyślnie normalny — może być zmieniony przez PATCH.
    """
    numer_ksef:     str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Unikalny identyfikator KSeF faktury. Immutable po utworzeniu.",
        examples=["PL7893215648-20260326-ABC123"],
    )
    priorytet:      Priorytet = Field(
        default=Priorytet.NORMALNY,
        description="Priorytet faktury w obiegu.",
    )
    opis_dokumentu: Optional[str] = Field(
        default=None,
        max_length=4000,
        description="Formalny opis faktury przez referenta (co to jest, czego dotyczy).",
    )
    uwagi:          Optional[str] = Field(
        default=None,
        max_length=2000,
        description="Nieformalne uwagi referenta. Nie trafia do AuditLog.",
    )
    user_ids:       list[int] = Field(
        ...,
        min_length=1,
        max_length=10,
        description="Lista ID pracowników do przypisania. Min 1, max wg SystemConfig.",
    )

    @field_validator("numer_ksef")
    @classmethod
    def validate_numer_ksef(cls, v: str) -> str:
        v = _sanitize_string(v)
        if not v:
            raise ValueError("numer_ksef nie może być pusty")
        # Odrzuć znaki które nie powinny być w KSEF_ID
        if re.search(r"[\x00-\x1f\x7f\"';<>\\]", v):
            raise ValueError("numer_ksef zawiera niedozwolone znaki")
        return v

    @field_validator("opis_dokumentu", "uwagi", mode="before")
    @classmethod
    def sanitize_text_fields(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = _sanitize_string(v)
        v = _strip_html_basic(v)
        return v or None

    @field_validator("user_ids")
    @classmethod
    def validate_user_ids(cls, v: list[int]) -> list[int]:
        if not v:
            raise ValueError("user_ids nie może być pusta")
        # Deduplikacja z zachowaniem kolejności
        seen: set[int] = set()
        deduped: list[int] = []
        for uid in v:
            if uid <= 0:
                raise ValueError(f"Nieprawidłowy user_id: {uid}")
            if uid not in seen:
                seen.add(uid)
                deduped.append(uid)
        return deduped


class FakturaPatchRequest(FakturaBase):
    """
    PATCH /faktury-akceptacja/{id} — edycja priorytetu, opisu, uwag.
    Wszystkie pola opcjonalne — partial update.
    Walidacja: co najmniej jedno pole musi być podane.
    """
    priorytet:      Optional[Priorytet] = Field(default=None)
    opis_dokumentu: Optional[str] = Field(default=None, max_length=4000)
    uwagi:          Optional[str] = Field(default=None, max_length=2000)

    @field_validator("opis_dokumentu", "uwagi", mode="before")
    @classmethod
    def sanitize_text(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        return _strip_html_basic(_sanitize_string(v)) or None

    @model_validator(mode="after")
    def at_least_one_field(self) -> "FakturaPatchRequest":
        if all(v is None for v in [self.priorytet, self.opis_dokumentu, self.uwagi]):
            raise ValueError("Co najmniej jedno pole musi być podane w PATCH")
        return self


class FakturaResetRequest(FakturaBase):
    """
    POST /faktury-akceptacja/{id}/reset — inicjacja resetu przypisań.
    Krok 1: zwraca confirm_token.
    """
    nowe_user_ids: list[int] = Field(
        ...,
        min_length=1,
        max_length=10,
        description="Nowe przypisania po resecie.",
    )
    powod: Optional[str] = Field(
        default=None,
        max_length=500,
        description="Powód resetu — opcjonalny, trafia do skw_faktura_log.",
    )

    @field_validator("nowe_user_ids")
    @classmethod
    def validate_user_ids(cls, v: list[int]) -> list[int]:
        seen: set[int] = set()
        deduped: list[int] = []
        for uid in v:
            if uid <= 0:
                raise ValueError(f"Nieprawidłowy user_id: {uid}")
            if uid not in seen:
                seen.add(uid)
                deduped.append(uid)
        return deduped

    @field_validator("powod", mode="before")
    @classmethod
    def sanitize_powod(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        return _sanitize_string(v)[:500] or None


class FakturaResetConfirmRequest(FakturaBase):
    """Krok 2 resetu: POST /faktury-akceptacja/{id}/reset/confirm."""
    confirm_token: str = Field(
        ...,
        min_length=10,
        max_length=2048,
        description="JWT confirm_token otrzymany w kroku 1.",
    )


class FakturaForceStatusRequest(FakturaBase):
    """
    PATCH /faktury-akceptacja/{id}/status — inicjacja force_status.
    Krok 1.
    """
    nowy_status: StatusWewnetrzny = Field(
        ...,
        description="Docelowy status: zaakceptowana (force) lub anulowana.",
    )
    powod: str = Field(
        ...,
        min_length=5,
        max_length=500,
        description="Powód wymuszonej zmiany statusu. Wymagany.",
    )

    @field_validator("nowy_status")
    @classmethod
    def validate_status(cls, v: StatusWewnetrzny) -> StatusWewnetrzny:
        allowed = {StatusWewnetrzny.ZAAKCEPTOWANA, StatusWewnetrzny.ANULOWANA}
        if v not in allowed:
            raise ValueError(
                f"force_status dozwolony tylko dla: {[s.value for s in allowed]}"
            )
        return v

    @field_validator("powod", mode="before")
    @classmethod
    def sanitize_powod(cls, v: str) -> str:
        return _sanitize_string(v)


class FakturaForceStatusConfirmRequest(FakturaBase):
    """Krok 2 force_status: POST /faktury-akceptacja/{id}/status/confirm."""
    confirm_token: str = Field(..., min_length=10, max_length=2048)


class DecyzjaRequest(FakturaBase):
    """
    POST /moje-faktury/{id}/decyzja — decyzja pracownika.

    WAŻNE: komentarz jest opcjonalny ale WYMAGANY przy odrzuceniu/nie_moje
    (walidacja: model_validator).
    Komentarz NIE trafia do AuditLog — tylko hash SHA256.
    """
    status:    StatusPrzypisania = Field(
        ...,
        description="Decyzja: zaakceptowane / odrzucone / nie_moje",
    )
    komentarz: Optional[str] = Field(
        default=None,
        max_length=2000,
        description="Komentarz pracownika. Trafia do skw_faktura_log (pełna treść). "
                    "Do AuditLog: tylko SHA256 hash.",
    )

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: StatusPrzypisania) -> StatusPrzypisania:
        # oczekuje nie jest dozwolone jako decyzja
        if v == StatusPrzypisania.OCZEKUJE:
            raise ValueError("Status 'oczekuje' nie jest prawidłową decyzją")
        return v

    @field_validator("komentarz", mode="before")
    @classmethod
    def sanitize_komentarz(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        return _sanitize_string(v)[:2000] or None

    @model_validator(mode="after")
    def komentarz_required_for_rejection(self) -> "DecyzjaRequest":
        if self.status in (StatusPrzypisania.ODRZUCONE, StatusPrzypisania.NIE_MOJE):
            if not self.komentarz:
                raise ValueError(
                    f"Komentarz jest wymagany przy decyzji '{self.status.value}'"
                )
        return self

    def komentarz_hash(self) -> Optional[str]:
        """SHA256 hash komentarza — do AuditLog (nie ujawniamy treści)."""
        if not self.komentarz:
            return None
        return hashlib.sha256(self.komentarz.encode("utf-8")).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# 4. Response schemas
# ─────────────────────────────────────────────────────────────────────────────

class PrzypisanieResponse(FakturaBase):
    """Pojedyncze przypisanie pracownika w odpowiedzi API."""
    id:         int
    user_id:    int
    status:     StatusPrzypisania
    is_active:  bool
    created_at: datetime
    decided_at: Optional[datetime] = None
    # Komentarz — widoczność kontrolowana w serwisie:
    #   referent (GET /faktury-akceptacja/{id}): wszystkich aktywnych
    #   pracownik (GET /moje-faktury/{id}):      tylko własny
    komentarz:       Optional[str] = None
    actor_username:  Optional[str] = None
    actor_full_name: Optional[str] = None


class FakturaListItem(FakturaBase):
    """
    Pozycja na liście faktur (GET /faktury-akceptacja).
    Dane łączone z widoku WAPRO + naszej tabeli.
    Bez pozycji faktury (te tylko w szczegółach).
    """
    # Nasze dane
    id:                int
    numer_ksef:        str
    status_wewnetrzny: StatusWewnetrzny
    priorytet:         Priorytet
    opis_skrocony:     Optional[str] = None  # opis_dokumentu[:120]
    created_at:        datetime
    updated_at:        Optional[datetime] = None
    is_active:         bool

    # Dane z WAPRO (mogą być None jeśli faktura zniknęła z BUF_DOKUMENT)
    numer:             Optional[str] = None
    data_wystawienia:  Optional[datetime] = None
    termin_platnosci:  Optional[datetime] = None
    wartosc_brutto:    Optional[Decimal] = None
    nazwa_kontrahenta: Optional[str] = None

    # Przypisania (skrótowo)
    przypisani_count:  int = 0
    zaakceptowali:     int = 0


class FakturaDetailResponse(FakturaBase):
    """
    Szczegóły faktury (GET /faktury-akceptacja/{id} lub /moje-faktury/{id}).
    Pełne dane z WAPRO + nasze + pozycje + przypisania.
    """
    # Nasze dane (pełne)
    id:                int
    numer_ksef:        str
    status_wewnetrzny: StatusWewnetrzny
    priorytet:         Priorytet
    opis_dokumentu:    Optional[str] = None
    uwagi:             Optional[str] = None
    utworzony_przez:   int
    is_active:         bool
    created_at:        datetime
    updated_at:        Optional[datetime] = None

    # Dane nagłówkowe WAPRO
    numer:             Optional[str] = None
    data_wystawienia:  Optional[datetime] = None
    data_otrzymania:   Optional[datetime] = None
    termin_platnosci:  Optional[datetime] = None
    wartosc_netto:     Optional[Decimal] = None
    wartosc_brutto:    Optional[Decimal] = None
    kwota_vat:         Optional[Decimal] = None
    forma_platnosci:   Optional[str] = None
    uwagi_wapro:       Optional[str] = None
    nazwa_kontrahenta: Optional[str] = None
    email_kontrahenta: Optional[str] = None
    telefon_kontrahenta: Optional[str] = None

    # Pozycje faktury (z dbo.skw_faktury_akceptacja_pozycje)
    pozycje: list["PozycjaFakturyResponse"] = []

    # Przypisania (pełne)
    przypisania: list[PrzypisanieResponse] = []

    @property
    def is_overdue(self) -> bool:
        """Czy termin płatności minął."""
        if not self.termin_platnosci:
            return False
        return datetime.utcnow() > self.termin_platnosci.replace(tzinfo=None)


class PozycjaFakturyResponse(FakturaBase):
    """Pozycja faktury z dbo.skw_faktury_akceptacja_pozycje."""
    id_buf_dokument: int
    numer_pozycji:   int
    nazwa_towaru:    str
    ilosc:           Optional[Decimal] = None
    jednostka:       Optional[str] = None
    cena_netto:      Optional[Decimal] = None
    cena_brutto:     Optional[Decimal] = None
    wartosc_netto:   Optional[Decimal] = None
    wartosc_brutto:  Optional[Decimal] = None
    stawka_vat:      Optional[str] = None
    opis:            Optional[str] = None


class FakturaCreateResponse(FakturaBase):
    """Odpowiedź POST /faktury-akceptacja (201 Created)."""
    id:           int
    numer_ksef:   str
    status:       StatusWewnetrzny
    priorytet:    Priorytet
    przypisano_do: list[int]   # user_ids którym wysłano SSE
    created_at:   datetime
    message:      str = "Faktura wpuszczona do obiegu akceptacji"


class ConfirmTokenResponse(FakturaBase):
    """
    Odpowiedź 202 dla operacji dwuetapowych (reset, force_status).
    confirm_token: JWT jednorazowy, TTL 60s (z SystemConfig).
    """
    confirm_token: str
    expires_in:    int = 60   # sekundy
    action:        str        # "reset_przypisania" lub "force_status"
    message:       str


class FakturaResetResponse(FakturaBase):
    """Odpowiedź po potwierdzeniu resetu."""
    faktura_id:       int
    dezaktywowane:    list[int]  # user_ids których is_active → 0
    nowe_przypisania: list[int]  # user_ids nowych przypisań
    message:          str = "Reset przypisań wykonany"


class FakturaHistoriaResponse(FakturaBase):
    """GET /faktury-akceptacja/{id}/historia — log zdarzeń faktury."""
    faktura_id: int
    total:      int
    items:      list["FakturaLogItemResponse"]


class FakturaLogItemResponse(FakturaBase):
    """Pojedynczy wpis w historii faktury."""
    id:         int
    faktura_id: int
    user_id:    Optional[int] = None
    akcja:      AkcjaLog
    created_at: datetime
    # szczegoly: nie zwracamy surowego JSON — parsujemy wybrane pola
    actor_username: Optional[str] = None
    actor_full_name: Optional[str] = None 
    before_status:  Optional[str] = None
    after_status:   Optional[str] = None


class DecyzjaResponse(FakturaBase):
    """Odpowiedź POST /moje-faktury/{id}/decyzja."""
    faktura_id:    int
    twoja_decyzja: StatusPrzypisania
    faktura_status: StatusWewnetrzny
    fakir_updated: bool = False   # czy UPDATE BUF_DOKUMENT wykonany
    message:       str


# ─────────────────────────────────────────────────────────────────────────────
# 5. FakturaLogDetails — model JSON dla skw_faktura_log.szczegoly
# ─────────────────────────────────────────────────────────────────────────────

class FakturaLogActor(FakturaBase):
    """Kto wykonał akcję."""
    user_id:   Optional[int] = None
    username:  Optional[str] = None
    full_name: Optional[str] = None   # ← NOWE: FullName z skw_Users
    ip:        Optional[str] = None


class FakturaLogDetails(FakturaBase):
    """
    Model JSON dla skw_faktura_log.szczegoly.
    ZAWSZE używać tego modelu — nigdy ręczny dict.
    Wersjonowanie: version=1 umożliwia przyszłą migrację struktury.
    """
    version:        int = 1
    timestamp_utc:  str = Field(
        default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    actor:          FakturaLogActor
    before:         Optional[dict[str, Any]] = None
    after:          Optional[dict[str, Any]] = None
    meta:           Optional[dict[str, Any]] = None
    system:         Optional[dict[str, Any]] = None

    def to_json_str(self) -> str:
        """Serializuje do JSON string dla kolumny NVARCHAR(MAX)."""
        import json
        return self.model_dump_json(exclude_none=True)

    @classmethod
    def build(
        cls,
        *,
        user_id:    Optional[int],
        username:   Optional[str],
        full_name:  Optional[str] = None,   # ← NOWE
        ip:         Optional[str],
        before:     Optional[dict[str, Any]] = None,
        after:      Optional[dict[str, Any]] = None,
        meta:       Optional[dict[str, Any]] = None,
        request_id: Optional[str] = None,
        endpoint:   Optional[str] = None,
    ) -> "FakturaLogDetails":
        """Factory method — czytelne tworzenie wpisu logu."""
        return cls(
            actor=FakturaLogActor(user_id=user_id, username=username, full_name=full_name, ip=ip),
            before=before,
            after=after,
            meta=meta,
            system={
                "request_id": request_id,
                "endpoint":   endpoint,
            } if (request_id or endpoint) else None,
        )


# ─────────────────────────────────────────────────────────────────────────────
# 6. WAPRO schemas — dane z widoków dbo (read-only)
# ─────────────────────────────────────────────────────────────────────────────

class WaproFakturaNaglowek(FakturaBase):
    """
    Dane z dbo.skw_faktury_akceptacja_naglowek.
    Używany do łączenia z danymi z naszej tabeli.
    """
    id_buf_dokument:  int
    ksef_id:          str
    numer:            Optional[str] = None
    kod_statusu:      Optional[str] = None
    status_opis:      Optional[str] = None
    data_wystawienia: Optional[datetime] = None
    data_otrzymania:  Optional[datetime] = None
    termin_platnosci: Optional[datetime] = None
    wartosc_netto:    Optional[Decimal] = None
    wartosc_brutto:   Optional[Decimal] = None
    kwota_vat:        Optional[Decimal] = None
    forma_platnosci:  Optional[str] = None
    uwagi:            Optional[str] = None
    nazwa_kontrahenta:  Optional[str] = None
    email_kontrahenta:  Optional[str] = None
    telefon_kontrahenta: Optional[str] = None


class WaproFakturaPozycja(FakturaBase):
    """Dane z dbo.skw_faktury_akceptacja_pozycje."""
    id_buf_dokument: int
    numer_pozycji:   int
    nazwa_towaru:    str
    ilosc:           Optional[Decimal] = None
    jednostka:       Optional[str] = None
    cena_netto:      Optional[Decimal] = None
    cena_brutto:     Optional[Decimal] = None
    wartosc_netto:   Optional[Decimal] = None
    wartosc_brutto:  Optional[Decimal] = None
    stawka_vat:      Optional[str] = None
    opis:            Optional[str] = None


# ─────────────────────────────────────────────────────────────────────────────
# 7. Schemas filtrów i paginacji
# ─────────────────────────────────────────────────────────────────────────────

class FakturaListFilter(FakturaBase):
    """Query params dla GET /faktury-akceptacja."""
    priorytet:    Optional[Priorytet]         = None
    status:       Optional[StatusWewnetrzny]  = None
    search:       Optional[str] = Field(default=None, max_length=100)
    date_from:    Optional[datetime]          = None
    date_to:      Optional[datetime]          = None

    @field_validator("search", mode="before")
    @classmethod
    def sanitize_search(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        return _sanitize_string(v)[:100] or None


class MojeFakturyFilter(FakturaBase):
    """Query params dla GET /moje-faktury."""
    status: Optional[str] = Field(
        default=None,
        description="'archiwum' = pokaż zdecydowane (nie tylko oczekujące)",
    )

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if v not in ("archiwum",):
            raise ValueError("Dozwolone wartości: 'archiwum'")
        return v


# ─────────────────────────────────────────────────────────────────────────────
# Rebuild forward references
# ─────────────────────────────────────────────────────────────────────────────

FakturaDetailResponse.model_rebuild()
FakturaHistoriaResponse.model_rebuild()