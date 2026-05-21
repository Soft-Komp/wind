# backend/app/schemas/unified_document.py
"""
UnifiedDocument — wspolny format dokumentu dla silnika obiegu.

Caly silnik filtrow i logika obiegu operuje WYLACZNIE na tym schemacie.
Kazde zrodlo dostarcza adapter implementujacy interfejs BaseDocumentAdapter.

Wzorzec fabryki:
    adapter = get_adapter(id_source)
    doc = await adapter.get_document(db, id_document)

Nowe zrodlo = nowy adapter + rekordy w skw_document_sources + field_mappings
— zero zmian w logice obiegu.

UWAGA: from __future__ import annotations — NIGDY w tym pliku (FastAPI router).
"""

import logging
from abc import ABC, abstractmethod
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_SCHEMA = "dbo"


# =============================================================================
# UnifiedDocument — wspolny schemat Pydantic
# =============================================================================

class UnifiedDocument(BaseModel):
    """
    Wspolny format dokumentu dla silnika filtrów i obiegu.

    Pola sa wspolna abstrakcja — kazde zrodlo mapuje swoje kolumny
    na te pola przez document_source_field_mappings.

    raw_data: oryginalne dane zrodla bez przetwarzania (do extra_data w instancji).
    """

    id_document:      str            = Field(description="Klucz dokumentu w zrodle (jako str)")
    id_source:        int            = Field(description="FK do skw_document_sources")
    source_name:      str            = Field(description="Nazwa zrodla: fakir|ksef|manual")

    doc_number:       str | None     = Field(default=None, description="Numer dokumentu")
    doc_date:         date | None    = Field(default=None, description="Data wystawienia")
    amount_gross:     Decimal | None = Field(default=None, description="Wartosc brutto")
    amount_net:       Decimal | None = Field(default=None, description="Wartosc netto")
    amount_vat:       Decimal | None = Field(default=None, description="Kwota VAT")
    contractor_name:  str | None     = Field(default=None, description="Nazwa kontrahenta")
    nip:              str | None     = Field(default=None, description="NIP kontrahenta")
    document_type:    str | None     = Field(default=None, description="Typ dokumentu")
    currency:         str | None     = Field(default=None, description="Waluta (PLN, EUR itp.)")
    payment_term:     date | None    = Field(default=None, description="Termin platnosci")
    payment_method:   str | None     = Field(default=None, description="Forma platnosci")
    external_id:      str | None     = Field(default=None, description="ID w zrodle zewnetrznym (KSeF)")

    raw_data:         dict[str, Any] = Field(
        default_factory=dict,
        description="Surowe dane ze zrodla — zapisywane do extra_data instancji",
    )

    def to_filter_dict(self) -> dict:
        """
        Zwraca slownik gotowy dla filter_engine.resolve_path().
        Wartosci None sa wlaczone — engine sprawdza ich brak.
        """
        return {
            "id_document":     self.id_document,
            "id_source":       self.id_source,
            "source_name":     self.source_name,
            "doc_number":      self.doc_number,
            "doc_date":        self.doc_date,
            "amount_gross":    self.amount_gross,
            "amount_net":      self.amount_net,
            "amount_vat":      self.amount_vat,
            "contractor_name": self.contractor_name,
            "nip":             self.nip,
            "document_type":   self.document_type,
            "currency":        self.currency,
            "payment_term":    self.payment_term,
            "payment_method":  self.payment_method,
            "external_id":     self.external_id,
        }

    def to_extra_data_json(self) -> dict:
        """Dane do zapisania w DocumentApprovalInstance.extra_data."""
        return {
            "doc_number":    self.doc_number,
            "doc_date":      str(self.doc_date) if self.doc_date else None,
            "contractor":    self.contractor_name,
            "nip":           self.nip,
            "document_type": self.document_type,
            "source_name":   self.source_name,
            **self.raw_data,
        }


# =============================================================================
# Interfejs adaptera
# =============================================================================

class BaseDocumentAdapter(ABC):
    """
    Interfejs adaptera zrodla dokumentow.
    Kazde zrodlo implementuje ta klase i rejestruje sie w ADAPTER_REGISTRY.
    """

    source_name: str  # np. 'fakir', 'ksef'

    @abstractmethod
    async def get_document(
        self, db: AsyncSession, id_document: str
    ) -> UnifiedDocument | None:
        """
        Pobiera dokument ze zrodla i mapuje na UnifiedDocument.
        Zwraca None jesli dokument nie istnieje.
        """
        ...

    @abstractmethod
    def get_document_title(self, doc: UnifiedDocument) -> str:
        """Generuje tytul dokumentu do wyswietlenia w interfejsie."""
        ...


# =============================================================================
# Adapter Fakir (WAPRO — skw_faktury_akceptacja_naglowek)
# =============================================================================

class FakirDocumentAdapter(BaseDocumentAdapter):
    """
    Adapter dla dokumentow z Fakir/WAPRO.
    Zrodlo: skw_faktury_akceptacja_naglowek (widok BUF_DOKUMENT + KONTRAHENT).

    Mapowanie pol (z document_source_field_mappings seed):
        ID_BUF_DOKUMENT  → id_document
        NUMER            → doc_number
        DataWystawienia  → doc_date (DATE z Clarion INT przez TRY_CAST)
        WARTOSC_BRUTTO   → amount_gross
        WARTOSC_NETTO    → amount_net
        KWOTA_VAT        → amount_vat
        NazwaKontrahenta → contractor_name
        NIP              → nip
        KOD_STATUSU      → document_type (lub StatusOpis)
        FORMA_PLATNOSCI  → payment_method
        TerminPlatnosci  → payment_term
    """

    source_name = "fakir"

    async def get_document(
        self, db: AsyncSession, id_document: str
    ) -> UnifiedDocument | None:
        """
        Pobiera naglowek faktury z widoku skw_faktury_akceptacja_naglowek.
        id_document = ID_BUF_DOKUMENT jako string.
        """
        try:
            buf_id = int(id_document)
        except ValueError:
            logger.warning("FakirAdapter | niepoprawny id_document: %r", id_document)
            return None

        row = await db.execute(
            text(
                f"SELECT "
                f"  f.[ID_BUF_DOKUMENT], "
                f"  f.[NUMER], "
                f"  f.[DataWystawienia], "
                f"  f.[WARTOSC_BRUTTO], "
                f"  f.[WARTOSC_NETTO], "
                f"  f.[KWOTA_VAT], "
                f"  f.[NazwaKontrahenta], "
                f"  f.[EmailKontrahenta], "
                f"  f.[KOD_STATUSU], "
                f"  f.[StatusOpis], "
                f"  f.[TerminPlatnosci], "
                f"  f.[FORMA_PLATNOSCI], "
                f"  f.[UWAGI], "
                f"  f.[NIP] "
                f"FROM [{_SCHEMA}].[skw_faktury_akceptacja_naglowek] f "
                f"WHERE f.[ID_BUF_DOKUMENT] = :id"
            ),
            {"id": buf_id},
        )
        r = row.fetchone()
        if not r:
            return None

        (
            buf_id_val, numer, data_wyst, wartosc_brutto, wartosc_netto, kwota_vat,
            nazwa_kont, email_kont, kod_statusu, status_opis,
            termin_platnosci, forma_platnosci, uwagi, nip,
        ) = r

        # Przelicz date (moze byc DATE lub INT Clarion — widok juz konwertuje)
        doc_date: date | None = None
        if isinstance(data_wyst, (date, datetime)):
            doc_date = data_wyst.date() if isinstance(data_wyst, datetime) else data_wyst

        payment_term: date | None = None
        if isinstance(termin_platnosci, (date, datetime)):
            payment_term = (
                termin_platnosci.date()
                if isinstance(termin_platnosci, datetime) else termin_platnosci
            )

        # Wartosc brutto -> Decimal
        amount_gross = Decimal(str(wartosc_brutto)) if wartosc_brutto is not None else None
        amount_net   = Decimal(str(wartosc_netto))  if wartosc_netto  is not None else None
        amount_vat   = Decimal(str(kwota_vat))      if kwota_vat      is not None else None

        raw: dict = {
            "email_kontrahenta": email_kont,
            "status_zewnetrzny": kod_statusu,
            "status_opis":       status_opis,
            "uwagi":             uwagi,
        }

        # Pobierz id_source dla 'fakir' (z cache lub bazy — tutaj z bazy dla prostoty)
        src_row = await db.execute(
            text(
                f"SELECT [id_source] FROM [{_SCHEMA}].[skw_document_sources] "
                f"WHERE [source_name] = N'fakir'"
            )
        )
        src = src_row.fetchone()
        id_source = src[0] if src else 1  # fallback: 1 = fakir per seed

        return UnifiedDocument(
            id_document=str(buf_id_val),
            id_source=id_source,
            source_name="fakir",
            doc_number=numer,
            doc_date=doc_date,
            amount_gross=amount_gross,
            amount_net=amount_net,
            amount_vat=amount_vat,
            contractor_name=nazwa_kont,
            nip=str(nip).strip() if nip else None,
            document_type=status_opis or kod_statusu,
            payment_term=payment_term,
            payment_method=forma_platnosci,
            raw_data=raw,
        )

    def get_document_title(self, doc: UnifiedDocument) -> str:
        parts = []
        if doc.doc_number:
            parts.append(doc.doc_number)
        if doc.contractor_name:
            parts.append(doc.contractor_name)
        if doc.amount_gross is not None:
            parts.append(f"{doc.amount_gross:.2f} PLN")
        return " | ".join(parts) if parts else f"Dokument #{doc.id_document}"


# =============================================================================
# Fabryka adapterow
# =============================================================================

# Rejestr adapterow: source_name → adapter instance
# Rozszerzenie: dodaj nowy adapter i zarejestruj go tutaj
ADAPTER_REGISTRY: dict[str, BaseDocumentAdapter] = {
    "fakir": FakirDocumentAdapter(),
}


def get_adapter(source_name: str) -> BaseDocumentAdapter | None:
    """
    Zwraca adapter dla podanej nazwy zrodla.
    Zwraca None jesli adapter nie jest zarejestrowany.

    Uzycie:
        adapter = get_adapter(source_name)
        if not adapter:
            raise HTTPException(422, "Nieobslugiwane zrodlo dokumentow")
        doc = await adapter.get_document(db, id_document)
    """
    return ADAPTER_REGISTRY.get(source_name)


async def get_adapter_by_source_id(
    db: AsyncSession, id_source: int
) -> BaseDocumentAdapter | None:
    """
    Zwraca adapter na podstawie id_source (ID z skw_document_sources).
    """
    row = await db.execute(
        text(
            f"SELECT [source_name] FROM [{_SCHEMA}].[skw_document_sources] "
            f"WHERE [id_source] = :s AND [is_active] = 1"
        ),
        {"s": id_source},
    )
    r = row.fetchone()
    if not r:
        return None
    return get_adapter(r[0])