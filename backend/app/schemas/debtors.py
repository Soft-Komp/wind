# =============================================================================
# backend/app/schemas/debtors.py
# =============================================================================
# Schematy Pydantic v2 dla dłużników z WAPRO — read-only przez widoki pyodbc.
#
# WIDOKI ŹRÓDŁOWE:
#   dbo.skw_kontrahenci          — lista dłużników z agregacją długu
#   dbo.skw_rozrachunki_faktur   — szczegóły faktur dla dłużnika
#
# ENDPOINTY UŻYWAJĄCE:
#   GET  /api/v1/debtors                    → DebtorFilterRequest → DebtorListResponse
#   GET  /api/v1/debtors/{id}               → DebtorDetailResponse
#   GET  /api/v1/debtors/{id}/invoices      → InvoiceListResponse
#   POST /api/v1/debtors/validate-bulk      → BulkDebtorValidateRequest
#   POST /api/v1/debtors/{id}/preview-pdf   → PDF blob (używa DebtorDetail)
#
# =============================================================================

from __future__ import annotations

import logging
import unicodedata
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.schemas.common import BaseResponse, PaginatedData, PaginationParams

logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS — wartości słownikowe z widoków SQL
# =============================================================================

class AgeCategory(str, Enum):
    """
    Kategoria wiekowa długu — mapuje KategoriaWieku z skw_kontrahenci.

    Wartości DOKŁADNIE jak w widoku SQL (CASE w skw_kontrahenci.sql:216-226):
        biezace         — nie przeterminowane
        do_30_dni       — 1-30 dni po terminie
        dni_31_60       — 31-60 dni po terminie
        dni_61_90       — 61-90 dni po terminie
        powyzej_90_dni  — >90 dni po terminie
    """

    BIEZACE = "biezace"
    DO_30_DNI = "do_30_dni"
    DNI_31_60 = "31_60_dni"
    DNI_61_90 = "61_90_dni"
    POWYZEJ_90_DNI = "powyzej_90_dni"

    @property
    def severity(self) -> int:
        """
        Waga kategorii (0-4) — do sortowania w UI.
        Im wyższy numer, tym poważniejsze przeterminowanie.
        """
        order = {
            AgeCategory.BIEZACE: 0,
            AgeCategory.DO_30_DNI: 1,
            AgeCategory.DNI_31_60: 2,
            AgeCategory.DNI_61_90: 3,
            AgeCategory.POWYZEJ_90_DNI: 4,
        }
        return order[self]


class InvoiceAgeCategory(str, Enum):
    """
    Kategoria wiekowa faktury — mapuje KategoriaWieku z skw_rozrachunki_faktur.

    Wartości z widoku SQL (skw_rozrachunki_faktur.sql:139-164):
        zaplacona       — faktura zapłacona (CZY_ROZLICZONY=1)
        biezace         — nie przeterminowana
        do_30_dni       — 1-30 dni po terminie
        dni_31_60       — 31-60 dni po terminie
        dni_61_90       — 61-90 dni po terminie
        powyzej_90_dni  — >90 dni po terminie
    """

    ZAPLACONA = "zaplacona"
    BIEZACE = "biezace"
    DO_30_DNI = "do_30_dni"
    DNI_31_60 = "31_60_dni"
    DNI_61_90 = "61_90_dni"
    POWYZEJ_90_DNI = "powyzej_90_dni"


# =============================================================================
# INPUT — filtrowanie listy dłużników
# =============================================================================

class DebtorFilterRequest(PaginationParams):
    """
    Parametry filtrowania listy dłużników — używane jako query params w GET /debtors.

    Dziedziczy z PaginationParams: page, per_page, sort_order.
    Dodaje filtry biznesowe: kwota, kategoria wieku, wyszukiwanie tekstowe.

    Użycie w endpointcie:
        @router.get("/debtors")
        async def list_debtors(filters: DebtorFilterRequest = Depends()):
            debtors = await debtor_service.list_filtered(db, wapro, filters)
            ...

    Limity (wg PLAN_PRAC.md §1.1):
        - max 200 rekordów/stronę
        - sanityzacja NFC dla search_query
        - extra='forbid' — blokuj nieznane query params
    """

    model_config = ConfigDict(
        extra="forbid",  # Blokuj nieznane query params
        populate_by_name=True,
    )

    # ── Filtry kwotowe ────────────────────────────────────────────────────────
    min_debt: Decimal | None = Field(
        default=None,
        description=(
            "Minimalna suma długu (SumaDlugu >= min_debt). "
            "NULL = bez filtra dolnego."
        ),
        ge=0,
        decimal_places=2,
        examples=[1000.00, 5000.00],
    )
    max_debt: Decimal | None = Field(
        default=None,
        description=(
            "Maksymalna suma długu (SumaDlugu <= max_debt). "
            "NULL = bez filtra górnego."
        ),
        ge=0,
        decimal_places=2,
        examples=[10000.00, 50000.00],
    )

    # ── Filtr kategorii wieku ─────────────────────────────────────────────────
    age_category: AgeCategory | None = Field(
        default=None,
        description=(
            "Filtr po kategorii wiekowej długu (KategoriaWieku z skw_kontrahenci). "
            "NULL = wszystkie kategorie."
        ),
    )

    # ── Filtr przeterminowania ────────────────────────────────────────────────
    only_overdue: bool = Field(
        default=False,
        description=(
            "Tylko przeterminowani (MaPrzeterminowane = 1). "
            "False = wszyscy dłużnicy (również bieżący)."
        ),
    )

    # ── Wyszukiwanie tekstowe ─────────────────────────────────────────────────
    search_query: str | None = Field(
        default=None,
        description=(
            "Wyszukiwanie w: NazwaKontrahenta, NIP, KodKontrahenta, Email. "
            "Case-insensitive, sanityzowane NFC. "
            "NULL = bez wyszukiwania."
        ),
        min_length=1,
        max_length=100,
        examples=["Kowalski", "123-456-78-90", "firma@example.com"],
    )

    # ── Filtr dat ─────────────────────────────────────────────────────────────
    invoice_date_from: date | None = Field(
        default=None,
        description=(
            "Filtr: DataOstatniejFaktury >= invoice_date_from. "
            "NULL = bez filtra dolnego."
        ),
    )
    invoice_date_to: date | None = Field(
        default=None,
        description=(
            "Filtr: DataOstatniejFaktury <= invoice_date_to. "
            "NULL = bez filtra górnego."
        ),
    )

    @field_validator("search_query")
    @classmethod
    def sanitize_search_query(cls, v: str | None) -> str | None:
        """
        Sanityzacja NFC (Unicode Normalization Form C) — zapobiega atakom homoglifów.

        Przykład: "café" (e + combining acute) → "café" (single character é)

        Wg PLAN_PRAC.md §1.1: "sanityzacja stringów (NFC)".
        """
        if v is None:
            return None
        # Strip białych znaków z obu stron
        v = v.strip()
        if not v:
            return None
        # Normalizacja NFC
        normalized = unicodedata.normalize("NFC", v)
        return normalized

    @field_validator("min_debt", "max_debt", mode="before")
    @classmethod
    def coerce_to_decimal(cls, v) -> Decimal | None:
        """Konwersja query param string → Decimal."""
        if v is None or v == "":
            return None
        try:
            return Decimal(str(v))
        except Exception:
            raise ValueError("Wartość musi być liczbą dziesiętną")

    @field_validator("only_overdue", mode="before")
    @classmethod
    def coerce_to_bool(cls, v) -> bool:
        """
        Konwersja query param string → bool.
        Akceptuje: "true", "1", "yes" → True; "false", "0", "no" → False.
        """
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            v_lower = v.lower()
            if v_lower in ("true", "1", "yes"):
                return True
            if v_lower in ("false", "0", "no", ""):
                return False
        raise ValueError("Wartość musi być true/false, 1/0, yes/no")


# =============================================================================
# OUTPUT — lista dłużników (GET /debtors)
# =============================================================================

class DebtorListItem(BaseModel):
    """
    Pojedynczy dłużnik na liście — skrócone dane z skw_kontrahenci.

    Używany w DebtorListResponse = BaseResponse[PaginatedData[DebtorListItem]].

    Nazwy pól DOKŁADNIE jak w widoku SQL (alias AS) — snake_case z SQL → snake_case w Pythonie.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,  # Pydantic v2: pozwala na .from_orm() z pyodbc Row
    )

    # ── Identyfikacja (z skw_kontrahenci.sql:188-191) ───────────────────────
    id_kontrahenta: int = Field(
        alias="IdKontrahenta",
        description="ID kontrahenta z WAPRO (klucz główny).",
        ge=1,
    )
    kod_kontrahenta: str = Field(
        alias="KodKontrahenta",
        description="Kod kontrahenta w WAPRO (np. 'K001', 'FIRMA-123').",
        max_length=50,
    )
    nazwa_kontrahenta: str = Field(
        alias="NazwaKontrahenta",
        description="Pełna nazwa firmy/osoby.",
        max_length=200,
    )
    nip: str | None = Field(
        default=None,
        alias="NIP",
        description="NIP kontrahenta (może być NULL jeśli brak w WAPRO).",
        max_length=20,
    )

    # ── Dane kontaktowe (skw_kontrahenci.sql:195-196) ───────────────────────
    email: str | None = Field(
        default=None,
        alias="Email",
        description=(
            "Adres email kontrahenta (ADRES_EMAIL z WAPRO). "
            "NULL jeśli brak w WAPRO."
        ),
        max_length=100,
    )
    telefon: str | None = Field(
        default=None,
        alias="Telefon",
        description=(
            "Telefon firmowy (TELEFON_FIRMOWY z WAPRO). "
            "NULL jeśli brak w WAPRO."
        ),
        max_length=50,
    )

    # ── Dane długu (VIEW_kontrahenti.sql:204-207) ────────────────────────────
    suma_dlugu: Decimal = Field(
        alias="SumaDlugu",
        description=(
            "Łączna suma długu (wszystkie niezapłacone faktury). "
            "Obliczone w CTE widoku (skw_kontrahenci.sql:49-53)."
        ),
        ge=0,
        decimal_places=2,
        examples=[12345.67],
    )
    suma_dlugu_przeterminowanego: Decimal = Field(
        alias="SumaDlinguPrzeterminowanego",
        description=(
            "Suma długu przeterminowanego (po terminie płatności). "
            "Obliczone w CTE widoku (skw_kontrahenci.sql:56-67)."
        ),
        ge=0,
        decimal_places=2,
        examples=[5678.90],
    )
    liczba_faktur_niezaplaconych: int = Field(
        alias="LiczbaFakturNiezaplaconych",
        description="Liczba niezapłaconych faktur.",
        ge=0,
        examples=[3, 15],
    )
    ma_przeterminowane: bool = Field(
        alias="MaPrzeterminowane",
        description=(
            "Czy dłużnik ma JAKIEKOLWIEK faktury przeterminowane. "
            "BIT flag do szybkiego filtrowania w UI."
        ),
    )

    # ── Daty (skw_kontrahenci.sql:210-212) ──────────────────────────────────
    najstarszy_termin_platnosci: date | None = Field(
        default=None,
        alias="NajstarszyTerminPlatnosci",
        description=(
            "Najstarszy termin płatności (najdłużej zaległa faktura). "
            "NULL jeśli wszystkie zapłacone. "
            "Skonwertowane z INT w widoku (DATEADD)."
        ),
    )
    data_ostatniej_faktury: date | None = Field(
        default=None,
        alias="DataOstatniejFaktury",
        description=(
            "Data wystawienia ostatniej faktury (najnowsza). "
            "NULL jeśli brak faktur. "
            "Skonwertowane z INT w widoku."
        ),
    )
    max_dni_przeterminowania: int | None = Field(
        default=None,
        alias="MaxDniPrzeterminowania",
        description=(
            "Liczba dni od najstarszego terminu płatności do dzisiaj. "
            "NULL jeśli nie ma przeterminowanych. "
            "Obliczone w CTE (skw_kontrahenci.sql:81-91)."
        ),
        ge=0,
    )

    # ── Kategoria wieku (skw_kontrahenci.sql:216-226) ───────────────────────
    kategoria_wieku: AgeCategory = Field(
        alias="KategoriaWieku",
        description=(
            "Kategoria wiekowa długu — do sortowania i kolorowania w UI. "
            "Obliczone w widoku przez CASE (skw_kontrahenci.sql:216-226)."
        ),
    )


# =============================================================================
# OUTPUT — faktura dłużnika (GET /debtors/{id}/invoices)
# =============================================================================

class InvoiceItem(BaseModel):
    """
    Pojedyncza faktura dłużnika — mapuje skw_rozrachunki_faktur.

    Używany w InvoiceListResponse = BaseResponse[PaginatedData[InvoiceItem]]
    oraz w DebtorDetail.invoices (lista ostatnich 10 faktur).

    Mapuje kolumny z dbo.skw_rozrachunki_faktur (skw_rozrachunki_faktur.sql:64-164).
    """

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
    )

    # ── Dane faktury (skw_rozrachunki_faktur.sql:72-84) ─────────────────────
    numer_faktury: str = Field(
        alias="NumerFaktury",
        description="Numer faktury z WAPRO (NR_DOK). Przykład: 'FV/2024/01/0001'.",
        max_length=100,
    )
    data_wystawienia: date = Field(
        alias="DataWystawienia",
        description=(
            "Data wystawienia faktury. "
            "Skonwertowane z INT w widoku (DATEADD, skw_rozrachunki_faktur.sql:77-79)."
        ),
    )
    termin_platnosci: date = Field(
        alias="TerminPlatnosci",
        description=(
            "Termin płatności faktury. "
            "Skonwertowane z INT w widoku (skw_rozrachunki_faktur.sql:82-84)."
        ),
    )

    # ── Kwoty (skw_rozrachunki_faktur.sql:88-95) ────────────────────────────
    kwota_brutto: Decimal = Field(
        alias="KwotaBrutto",
        description="Kwota brutto faktury (pełna wartość z WAPRO.KWOTA).",
        ge=0,
        decimal_places=2,
        examples=[1234.56],
    )
    kwota_pozostala: Decimal = Field(
        alias="KwotaPozostala",
        description=(
            "Kwota jeszcze do zapłaty po częściowych płatnościach (POZOSTALO z WAPRO). "
            "0.00 jeśli zapłacona w całości."
        ),
        ge=0,
        decimal_places=2,
        examples=[1234.56, 0.00],
    )
    kwota_zaplacona: Decimal = Field(
        alias="KwotaZaplacona",
        description=(
            "Kwota już zapłacona. "
            "Obliczone w widoku: KWOTA - POZOSTALO (skw_rozrachunki_faktur.sql:95)."
        ),
        ge=0,
        decimal_places=2,
        examples=[0.00, 500.00],
    )

    # ── Status (skw_rozrachunki_faktur.sql:99-124) ──────────────────────────
    czy_zaplacona: bool = Field(
        alias="CzyZaplacona",
        description="Czy faktura zapłacona (CZY_ROZLICZONY BIT z WAPRO). True=zapłacona.",
    )
    czy_przeterminowana: bool = Field(
        alias="CzyPrzeterminowana",
        description=(
            "Czy faktura przeterminowana (termin < dzisiaj AND nie zapłacona). "
            "Obliczone w widoku (skw_rozrachunki_faktur.sql:103-111)."
        ),
    )
    dni_przeterminowania: int | None = Field(
        default=None,
        alias="DniPrzeterminowania",
        description=(
            "Liczba dni przeterminowania. "
            "NULL jeśli nie przeterminowana lub zapłacona. "
            "Obliczone w widoku (skw_rozrachunki_faktur.sql:114-124)."
        ),
        ge=0,
    )

    # ── Metadane (skw_rozrachunki_faktur.sql:128-135) ───────────────────────
    forma_platnosci: str | None = Field(
        default=None,
        alias="FormaPlatnosci",
        description=(
            "Metoda płatności (FORMA_PLATNOSCI z WAPRO). "
            "Przykłady: 'przelew', 'gotówka', 'karta'. "
            "NULL jeśli brak w WAPRO."
        ),
        max_length=50,
    )

    # ── Kategoria wieku (skw_rozrachunki_faktur.sql:139-164) ────────────────
    kategoria_wieku: InvoiceAgeCategory = Field(
        alias="KategoriaWieku",
        description=(
            "Kategoria wiekowa faktury — do sortowania i kolorowania. "
            "Obliczone w widoku przez CASE (skw_rozrachunki_faktur.sql:139-164). "
            "Wartość 'zaplacona' jeśli CZY_ROZLICZONY=1."
        ),
    )


# =============================================================================
# OUTPUT — szczegóły dłużnika (GET /debtors/{id})
# =============================================================================

class DebtorDetail(BaseModel):
    """
    Pełne dane dłużnika — wszystkie pola z skw_kontrahenci + lista ostatnich faktur.

    Używany w DebtorDetailResponse = BaseResponse[DebtorDetail].

    Rozszerza DebtorListItem o:
        - Adres (ulica, kod pocztowy, miejscowość)
        - Lista ostatnich 10 faktur (InvoiceItem)
        - Liczba komentarzy
        - Data ostatniego monitu
    """

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
    )

    # ── Wszystkie pola z DebtorListItem ───────────────────────────────────────
    # Kopiujemy explicite dla czytelności dokumentacji API (Swagger)
    id_kontrahenta: int = Field(alias="IdKontrahenta", ge=1)
    kod_kontrahenta: str = Field(alias="KodKontrahenta", max_length=50)
    nazwa_kontrahenta: str = Field(alias="NazwaKontrahenta", max_length=200)
    nip: str | None = Field(default=None, alias="NIP", max_length=20)
    email: str | None = Field(default=None, alias="Email", max_length=100)
    telefon: str | None = Field(default=None, alias="Telefon", max_length=50)
    suma_dlugu: Decimal = Field(alias="SumaDlugu", ge=0, decimal_places=2)
    suma_dlugu_przeterminowanego: Decimal = Field(
        alias="SumaDlinguPrzeterminowanego", ge=0, decimal_places=2
    )
    liczba_faktur_niezaplaconych: int = Field(alias="LiczbaFakturNiezaplaconych", ge=0)
    ma_przeterminowane: bool = Field(alias="MaPrzeterminowane")
    najstarszy_termin_platnosci: date | None = Field(
        default=None, alias="NajstarszyTerminPlatnosci"
    )
    data_ostatniej_faktury: date | None = Field(default=None, alias="DataOstatniejFaktury")
    max_dni_przeterminowania: int | None = Field(
        default=None, alias="MaxDniPrzeterminowania", ge=0
    )
    kategoria_wieku: AgeCategory = Field(alias="KategoriaWieku")

    # ── Adres (skw_kontrahenci.sql:199-201) ─────────────────────────────────
    ulica: str | None = Field(
        default=None,
        alias="Ulica",
        description="Ulica i numer (może być NULL jeśli brak w WAPRO).",
        max_length=100,
    )
    kod_pocztowy: str | None = Field(
        default=None,
        alias="KodPocztowy",
        description="Kod pocztowy (może być NULL).",
        max_length=10,
    )
    miejscowosc: str | None = Field(
        default=None,
        alias="Miejscowosc",
        description="Miejscowość (może być NULL).",
        max_length=100,
    )

    # ── Dane dodatkowe (z innych źródeł, nie z skw_kontrahenci) ─────────────
    invoices: list[InvoiceItem] = Field(
        default_factory=list,
        description=(
            "Lista ostatnich faktur dłużnika (max 10). "
            "Pobierane z skw_rozrachunki_faktur przez debtor_service."
        ),
    )
    comments_count: int = Field(
        default=0,
        description=(
            "Liczba komentarzy przypisanych do tego dłużnika. "
            "Pobierane z tabeli dbo_ext.Comments."
        ),
        ge=0,
    )
    last_monit_date: date | None = Field(
        default=None,
        description=(
            "Data ostatniego monitu wysłanego do tego dłużnika. "
            "Pobierane z dbo_ext.MonitHistory."
        ),
    )


# =============================================================================
# INPUT — walidacja masowa (POST /debtors/validate-bulk)
# =============================================================================

class BulkDebtorValidateRequest(BaseModel):
    """
    Żądanie walidacji masowej dłużników przed wysyłką monitów.

    Endpoint: POST /api/v1/debtors/validate-bulk
    Cel: Sprawdzić czy wszyscy dłużnicy z listy ID mają poprawne dane kontaktowe.

    Użycie w UI:
        User zaznacza 50 dłużników w tabeli → kliknij "Wyślij monity email" →
        Frontend wywołuje validate-bulk → pokazuje ile będzie sukces/błąd →
        User potwierdza → wywołuje POST /monits/send-bulk

    Walidacja sprawdza:
        - Czy dłużnik istnieje w WAPRO
        - Czy email/telefon nie jest NULL (w zależności od channel)
        - Czy nie jest zanonimizowany RODO
        - Czy nie jest zablokowany
    """

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
    )

    debtor_ids: list[int] = Field(
        description=(
            "Lista ID kontrahentów do walidacji. "
            "Max 500 dłużników na jedno wywołanie (limit bezpieczeństwa)."
        ),
        min_length=1,
        max_length=500,
    )
    channel: Literal["email", "sms"] = Field(
        description=(
            "Kanał komunikacji do sprawdzenia. "
            "email → sprawdź Email IS NOT NULL. "
            "sms → sprawdź Telefon IS NOT NULL."
        ),
    )

    @field_validator("debtor_ids")
    @classmethod
    def validate_unique_ids(cls, v: list[int]) -> list[int]:
        """Usuń duplikaty z listy ID."""
        if len(v) != len(set(v)):
            # Deduplikacja
            unique = list(dict.fromkeys(v))  # Zachowuje kolejność
            logger.warning(
                f"Znaleziono duplikaty w debtor_ids — zdeduplikowano {len(v)} → {len(unique)}"
            )
            return unique
        return v


class BulkDebtorValidateResponse(BaseModel):
    """
    Odpowiedź walidacji masowej — zwraca listy valid/invalid ID.

    Używany w BaseResponse[BulkDebtorValidateResponse].
    """

    model_config = ConfigDict(populate_by_name=True)

    valid: list[int] = Field(
        description="Lista ID dłużników które przeszły walidację.",
    )
    invalid: list[int] = Field(
        description="Lista ID dłużników które NIE przeszły walidacji.",
    )
    valid_count: int = Field(
        description="Liczba poprawnych dłużników (len(valid)).",
        ge=0,
    )
    invalid_count: int = Field(
        description="Liczba niepoprawnych dłużników (len(invalid)).",
        ge=0,
    )
    details: dict[int, str] = Field(
        default_factory=dict,
        description=(
            "Szczegóły błędów dla niepoprawnych ID. "
            "Klucz: ID_KONTRAHENTA, Wartość: komunikat błędu po polsku. "
            "Przykład: {123: 'Brak adresu email', 456: 'Zanonimizowany RODO'}"
        ),
    )


# =============================================================================
# TYPE ALIASES — odpowiedzi API (używane w endpointach)
# =============================================================================

DebtorListResponse = BaseResponse[PaginatedData[DebtorListItem]]
DebtorDetailResponse = BaseResponse[DebtorDetail]
InvoiceListResponse = BaseResponse[PaginatedData[InvoiceItem]]
BulkValidateResponse = BaseResponse[BulkDebtorValidateResponse]