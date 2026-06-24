# backend/app/schemas/unified_document.py
"""
UnifiedDocument — wspolny format dokumentu dla silnika obiegu.

ETAP 2 — zmiany:
  - ADAPTER_REGISTRY zastapiony przez get_adapter(id_source, db)
    czytajacy source_type i connection_config z bazy (cache Redis TTL).
  - Nowe generyczne adaptery: DatabaseAdapter, RestApiAdapter.
  - FakirDocumentAdapter pozostaje jako legacy do czasu pelnej migracji.

Wzorzec fabryki:
    adapter = await get_adapter(id_source, db, redis)
    docs    = await adapter.fetch_new_documents(db, since=last_sync_at)

Nowe zrodlo = wpis w skw_document_sources + connection_config + field_mappings
— zero zmian w logice obiegu.

UWAGA: from __future__ import annotations — NIGDY w tym pliku (FastAPI router).
SQLAlchemy i Pydantic wymagaja resolved annotations.
"""

import json
import logging
import re
from abc import ABC, abstractmethod
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import pyodbc
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_SCHEMA = "dbo"

# Klucz Redis dla cache adaptera per id_source
_ADAPTER_CACHE_PREFIX = "adapter_config:"
_ADAPTER_CACHE_TTL    = 300  # 5 minut


# =============================================================================
# UnifiedDocument — wspolny schemat Pydantic
# =============================================================================

class UnifiedDocument(BaseModel):
    """
    Wspolny format dokumentu dla silnika filtrow i obiegu.

    Pola sa wspolna abstrakcja — kazde zrodlo mapuje swoje kolumny
    na te pola przez document_source_field_mappings lub wbudowane mapowanie.

    raw_data: oryginalne dane zrodla bez przetwarzania (do extra_data w instancji).
    """

    id_document:      str            = Field(description="Klucz dokumentu w zrodle (jako str)")
    id_source:        int            = Field(description="FK do skw_document_sources")
    source_name:      str            = Field(description="Nazwa zrodla: fakir|ksef|manual|...")

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
    external_id:      str | None     = Field(default=None, description="ID w zrodle zewnetrznym")

    raw_data:         dict[str, Any] = Field(
        default_factory=dict,
        description="Surowe dane ze zrodla — zapisywane do extra_data instancji",
    )

    def to_filter_dict(self) -> dict:
        """Slownik gotowy dla filter_engine.resolve_path()."""
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
            "ksef_id":       self.id_document,
            "doc_number":    self.doc_number,
            "doc_date":      str(self.doc_date) if self.doc_date else None,
            "contractor":    self.contractor_name,
            "nip":           self.nip,
            "document_type": self.document_type,
            "source_name":   self.source_name,
            "amount_gross":  float(self.amount_gross) if self.amount_gross else None,
            "amount_net":    float(self.amount_net) if self.amount_net else None,
            **self.raw_data,
        }


# =============================================================================
# Interfejs adaptera
# =============================================================================

class BaseDocumentAdapter(ABC):
    """
    Interfejs adaptera zrodla dokumentow.

    ETAP 2: Adaptery generyczne (DatabaseAdapter, RestApiAdapter) tworzone
    dynamicznie przez get_adapter() na podstawie source_type i connection_config.
    Istniejace FakirDocumentAdapter i KsefDocumentAdapter pozostaja do czasu
    pelnej migracji Krok 0 i weryfikacji workera synchronizacji.
    """

    source_name: str   # np. 'fakir', 'ksef', lub dynamicznie z bazy

    @abstractmethod
    async def get_document(
        self, db: AsyncSession, id_document: str
    ) -> UnifiedDocument | None:
        """
        Pobiera jeden dokument ze zrodla i mapuje na UnifiedDocument.
        Zwraca None jesli dokument nie istnieje.
        """
        ...

    @abstractmethod
    def get_document_title(self, doc: UnifiedDocument) -> str:
        """Generuje tytul dokumentu do wyswietlenia w interfejsie."""
        ...

    async def fetch_new_documents(
        self,
        db: AsyncSession,
        since: datetime | None,
        limit: int = 500,
    ) -> list[UnifiedDocument]:
        """
        Pobiera nowe/zmienione dokumenty od 'since'.
        Domyslna implementacja — adaptery generyczne nadpisuja.

        Args:
            db:    Sesja SQLAlchemy (async).
            since: Timestamp od kiedy pobierac (last_sync_at). None = wszystkie.
            limit: Max liczba dokumentow w jednym cyklu.

        Returns:
            Lista UnifiedDocument gotowych do zapisu w skw_document_approval_instances.
        """
        raise NotImplementedError(
            f"{type(self).__name__} nie implementuje fetch_new_documents. "
            "Adapter nie obsluguje synchronizacji cyklicznej."
        )


# =============================================================================
# DatabaseAdapter — generyczny adapter dla zrodel typu 'database'
# =============================================================================

class DatabaseAdapter(BaseDocumentAdapter):
    """
    Generyczny adapter dla zrodel polaczonych przez ODBC/pyodbc.

    connection_config (odszyfrowany JSON):
        connection_string:  ODBC connection string do zewnetrznej bazy
        view_name:          Nazwa widoku SQL z dokumentami (np. 'skw_faktury_akceptacja_naglowek')
        id_column:          Kolumna bedaca kluczem dokumentu (np. 'KSEF_ID')
        date_column:        Kolumna daty dla filtrowania od last_sync_at (opcjonalna)
        field_mappings:     Slownik {pole_zrodla: pole_UnifiedDocument} (opcjonalny, fallback)

    Uwaga dla Fakira:
        id_column = 'KSEF_ID' (decyzja F0.3 — stabilny identyfikator zewnetrzny)
        view_name = 'skw_faktury_akceptacja_naglowek' (istniejacy widok WAPRO)

    Paginacja po id_column (string): ORDER BY id_column, strony po 'limit' rekordow.
    """

    # Whitelist dozwolonych typow pyodbc — zapobiega SQL injection w connection_string
    _SAFE_DRIVERS = frozenset({
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
    })

    def __init__(
        self,
        id_source: int,
        source_name: str,
        config: dict[str, Any],
        field_mappings: list[dict[str, Any]] | None = None,
    ) -> None:
        """
        Args:
            id_source:      ID zrodla z skw_document_sources.
            source_name:    Nazwa zrodla.
            config:         Odszyfrowany connection_config (JSON).
            field_mappings: Lista rekordow z skw_document_source_field_mappings.
        """
        self.id_source    = id_source
        self.source_name  = source_name
        self._config      = config
        self._mappings    = field_mappings or []

        self._conn_str    = config.get("connection_string", "")
        self._view_name   = config.get("view_name", "")
        self._id_col      = config.get("id_column", "KSEF_ID")
        self._date_col    = config.get("date_column")  # None = brak filtrowania dat

        self._validate_config()

    def _validate_config(self) -> None:
        """Waliduje konfiguracje przy tworzeniu adaptera."""
        if not self._conn_str:
            raise ValueError(
                f"DatabaseAdapter [{self.source_name}]: brak 'connection_string' w config"
            )
        if not self._view_name:
            raise ValueError(
                f"DatabaseAdapter [{self.source_name}]: brak 'view_name' w config"
            )
        # Walidacja nazwy widoku — tylko litery, cyfry, podkreslenia, bez SQL injection
        if not re.match(r'^[a-zA-Z0-9_]+$', self._view_name):
            raise ValueError(
                f"DatabaseAdapter [{self.source_name}]: "
                f"view_name '{self._view_name}' zawiera niedozwolone znaki"
            )
        if not re.match(r'^[a-zA-Z0-9_]+$', self._id_col):
            raise ValueError(
                f"DatabaseAdapter [{self.source_name}]: "
                f"id_column '{self._id_col}' zawiera niedozwolone znaki"
            )

    def _get_pyodbc_conn(self) -> pyodbc.Connection:
        """Tworzy nowe polaczenie pyodbc. Nie cachujemy — kazda synchronizacja to osobna sesja."""
        conn = pyodbc.connect(self._conn_str, autocommit=True, timeout=30)
        conn.setdecoding(pyodbc.SQL_CHAR,  encoding="utf-8")
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-8")
        conn.setencoding(encoding="utf-8")
        return conn

    def _row_to_unified(self, row: dict[str, Any]) -> UnifiedDocument:
        """
        Mapuje rekord z bazy na UnifiedDocument uzywajac field_mappings.
        Jesli mappings puste — uzywa domyslnego mapowania dla Fakira.
        """
        mapped: dict[str, Any] = {}

        if self._mappings:
            for m in self._mappings:
                src_field    = m.get("source_field", "")
                common_field = m.get("common_field", "")
                field_type   = m.get("field_type", "string")
                transform    = m.get("transform_expression")

                raw_val = row.get(src_field)
                mapped[common_field] = self._cast_value(raw_val, field_type, transform)
        else:
            # Domyslne mapowanie dla Fakira (widok skw_faktury_akceptacja_naglowek)
            mapped = {
                "doc_number":      row.get("NUMER"),
                "amount_gross":    self._cast_value(row.get("WARTOSC_BRUTTO"), "decimal"),
                "amount_net":      self._cast_value(row.get("WARTOSC_NETTO"), "decimal"),
                "amount_vat":      self._cast_value(row.get("KWOTA_VAT"), "decimal"),
                "contractor_name": row.get("NazwaKontrahenta"),
                "nip":             row.get("NIP"),
                "payment_method":  row.get("FORMA_PLATNOSCI"),
                "doc_date":        self._cast_value(row.get("DataWystawienia"), "date"),
                "payment_term":    self._cast_value(row.get("TerminPlatnosci"), "date"),
                "document_type":   row.get("StatusOpis") or row.get("KOD_STATUSU"),
            }

        id_document = str(row.get(self._id_col, ""))
        if not id_document:
            raise ValueError(
                f"DatabaseAdapter [{self.source_name}]: "
                f"id_column '{self._id_col}' jest NULL lub puste w rekordzie: {row}"
            )

        return UnifiedDocument(
            id_document=id_document,
            id_source=self.id_source,
            source_name=self.source_name,
            doc_number=mapped.get("doc_number"),
            doc_date=mapped.get("doc_date"),
            amount_gross=mapped.get("amount_gross"),
            amount_net=mapped.get("amount_net"),
            amount_vat=mapped.get("amount_vat"),
            contractor_name=mapped.get("contractor_name"),
            nip=mapped.get("nip"),
            document_type=mapped.get("document_type"),
            payment_term=mapped.get("payment_term"),
            payment_method=mapped.get("payment_method"),
            raw_data={k: str(v) if v is not None else None for k, v in row.items()},
        )

    @staticmethod
    def _cast_value(val: Any, field_type: str, transform: str | None = None) -> Any:
        """Rzutuje wartosc z bazy na typ Pythona wg field_type."""
        if val is None:
            return None

        if field_type == "decimal":
            try:
                return Decimal(str(val))
            except Exception:
                return None

        if field_type == "date":
            if isinstance(val, date):
                return val
            if isinstance(val, datetime):
                return val.date()
            # Daty Clarion (INT = dni od 1899-12-30) — stosowane w WAPRO/Fakir
            if isinstance(val, int) and 0 < val < 200000:
                try:
                    from datetime import date as _date, timedelta
                    return _date(1899, 12, 30) + timedelta(days=val)
                except Exception:
                    pass
            try:
                return datetime.fromisoformat(str(val)).date()
            except Exception:
                return None

        if field_type == "int":
            try:
                return int(val)
            except Exception:
                return None

        # string (domyslny)
        return str(val) if val is not None else None

    async def get_document(
        self, db: AsyncSession, id_document: str
    ) -> UnifiedDocument | None:
        """
        Pobiera jeden dokument po id_column = id_document.
        Uzywa pyodbc (synchronicznie) — wrappowane w executor jesli potrzeba async.
        """
        try:
            with self._get_pyodbc_conn() as conn:
                cur = conn.cursor()
                # view_name i id_col sa zwalidowane w __init__ — bezpieczne f-string
                cur.execute(
                    f"SELECT * FROM [{self._view_name}] WHERE [{self._id_col}] = ?",
                    (id_document,),
                )
                row_raw = cur.fetchone()
                if not row_raw:
                    return None
                cols = [d[0] for d in cur.description]
                row = dict(zip(cols, row_raw))
                return self._row_to_unified(row)
        except Exception as exc:
            logger.error(
                "DatabaseAdapter.get_document blad | source=%s id=%s: %s",
                self.source_name, id_document, exc,
            )
            raise

    async def fetch_new_documents(
        self,
        db: AsyncSession,
        since: datetime | None,
        limit: int = 500,
    ) -> list[UnifiedDocument]:
        """
        Pobiera dokumenty z widoku — nowe lub zmienione od 'since'.

        Paginacja: ORDER BY id_column ASC, max 'limit' rekordow.
        Jesli date_column jest skonfigurowany — filtruje od since.
        Jesli nie — pobiera wszystkie (worker sprawdzi duplikaty przez MERGE).
        """
        results: list[UnifiedDocument] = []
        errors = 0

        try:
            with self._get_pyodbc_conn() as conn:
                cur = conn.cursor()

                if self._date_col and since:
                    since_str = since.strftime("%Y-%m-%dT%H:%M:%S")
                    # date_col jest zwalidowany w __init__
                    sql = (
                        f"SELECT TOP {int(limit)} * FROM [{self._view_name}] "
                        f"WHERE [{self._date_col}] >= ? "
                        f"ORDER BY [{self._id_col}] ASC"
                    )
                    cur.execute(sql, (since_str,))
                else:
                    sql = (
                        f"SELECT TOP {int(limit)} * FROM [{self._view_name}] "
                        f"ORDER BY [{self._id_col}] ASC"
                    )
                    cur.execute(sql)

                cols = [d[0] for d in cur.description]
                for row_raw in cur.fetchall():
                    row = dict(zip(cols, row_raw))
                    try:
                        results.append(self._row_to_unified(row))
                    except Exception as exc:
                        errors += 1
                        logger.warning(
                            "DatabaseAdapter.fetch_new_documents: pomijam rekord z bledem | "
                            "source=%s error=%s row_id=%s",
                            self.source_name, exc,
                            row.get(self._id_col, "?"),
                        )

        except Exception as exc:
            logger.error(
                "DatabaseAdapter.fetch_new_documents blad | source=%s since=%s: %s",
                self.source_name, since, exc,
            )
            raise

        logger.info(
            "DatabaseAdapter.fetch_new_documents | source=%s since=%s "
            "ok=%d errors=%d",
            self.source_name, since, len(results), errors,
        )
        return results

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
# RestApiAdapter — generyczny adapter dla zrodel REST API
# =============================================================================

class RestApiAdapter(BaseDocumentAdapter):
    """
    Generyczny adapter dla zrodel REST API.

    connection_config (odszyfrowany JSON):
        base_url:       Bazowy URL API (np. 'https://api.example.com/v1')
        auth_type:      'bearer_refresh' | 'api_key' | 'basic'
        auth_config:    Slownik z danymi auth (token/key/login/password)
        endpoint_list:  Sciezka do listowania nowych dokumentow (np. '/invoices')
        endpoint_detail: Sciezka do szczegolu /{id} (np. '/invoices/{id}')
        pagination:     Slownik konfiguracji paginacji (opcjonalny)
        field_mappings: Slownik {pole_json: pole_UnifiedDocument}

    Autoryzacja:
        bearer_refresh — JWT Bearer z automatycznym odswiezaniem tokenu
        api_key        — statyczny klucz w naglowku (X-Api-Key lub Authorization)
        basic          — HTTP Basic Auth

    UWAGA: Import httpx jest lazy (w metodach) — nie wymagamy go przy starcie jesli
    zrodlo nie jest uzywane.
    """

    def __init__(
        self,
        id_source: int,
        source_name: str,
        config: dict[str, Any],
        field_mappings: list[dict[str, Any]] | None = None,
    ) -> None:
        self.id_source    = id_source
        self.source_name  = source_name
        self._config      = config
        self._mappings    = field_mappings or []

        self._base_url    = config.get("base_url", "").rstrip("/")
        self._auth_type   = config.get("auth_type", "api_key")
        self._auth_config = config.get("auth_config", {})
        self._ep_list     = config.get("endpoint_list", "")
        self._ep_detail   = config.get("endpoint_detail", "")
        self._pagination  = config.get("pagination", {})
        self._json_mappings = config.get("field_mappings", {})

        self._validate_config()

    def _validate_config(self) -> None:
        if not self._base_url:
            raise ValueError(f"RestApiAdapter [{self.source_name}]: brak 'base_url'")
        if not self._ep_list:
            raise ValueError(f"RestApiAdapter [{self.source_name}]: brak 'endpoint_list'")
        valid_auth = {"bearer_refresh", "api_key", "basic"}
        if self._auth_type not in valid_auth:
            raise ValueError(
                f"RestApiAdapter [{self.source_name}]: "
                f"nieprawidlowy auth_type '{self._auth_type}'. Dozwolone: {valid_auth}"
            )

    def _get_auth_headers(self) -> dict[str, str]:
        """Buduje naglowki autoryzacyjne."""
        if self._auth_type == "api_key":
            key   = self._auth_config.get("api_key", "")
            hdr   = self._auth_config.get("header_name", "X-Api-Key")
            return {hdr: key}

        if self._auth_type == "basic":
            import base64
            login = self._auth_config.get("login", "")
            pwd   = self._auth_config.get("password", "")
            token = base64.b64encode(f"{login}:{pwd}".encode()).decode()
            return {"Authorization": f"Basic {token}"}

        if self._auth_type == "bearer_refresh":
            # Uzywamy tokenu z auth_config — odswiezanie w _refresh_bearer_token
            token = self._auth_config.get("access_token", "")
            return {"Authorization": f"Bearer {token}"}

        return {}

    async def _refresh_bearer_token(self) -> None:
        """Odswierza Bearer token jesli wymagangy."""
        try:
            import httpx
            refresh_url    = self._auth_config.get("token_url", "")
            refresh_token  = self._auth_config.get("refresh_token", "")
            client_id      = self._auth_config.get("client_id", "")
            client_secret  = self._auth_config.get("client_secret", "")

            if not refresh_url:
                return

            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    refresh_url,
                    data={
                        "grant_type":    "refresh_token",
                        "refresh_token": refresh_token,
                        "client_id":     client_id,
                        "client_secret": client_secret,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                self._auth_config["access_token"] = data.get("access_token", "")
                logger.debug("RestApiAdapter [%s]: token odswiezony", self.source_name)

        except Exception as exc:
            logger.error(
                "RestApiAdapter [%s]: blad odswiezania tokenu: %s",
                self.source_name, exc,
            )

    def _map_json_to_unified(self, item: dict[str, Any], id_document: str) -> UnifiedDocument:
        """Mapuje JSON z API na UnifiedDocument uzywajac _json_mappings."""
        mapped: dict[str, Any] = {}
        for json_key, common_field in self._json_mappings.items():
            # Obsluga zagniezdzonego klucza: "address.city" -> item["address"]["city"]
            parts = json_key.split(".")
            val = item
            for part in parts:
                if isinstance(val, dict):
                    val = val.get(part)
                else:
                    val = None
                    break
            mapped[common_field] = val

        def _to_decimal(v):
            try:
                return Decimal(str(v)) if v is not None else None
            except Exception:
                return None

        def _to_date(v):
            if isinstance(v, (date, datetime)):
                return v.date() if isinstance(v, datetime) else v
            try:
                return datetime.fromisoformat(str(v)).date() if v else None
            except Exception:
                return None

        return UnifiedDocument(
            id_document=id_document,
            id_source=self.id_source,
            source_name=self.source_name,
            doc_number=mapped.get("doc_number"),
            doc_date=_to_date(mapped.get("doc_date")),
            amount_gross=_to_decimal(mapped.get("amount_gross")),
            amount_net=_to_decimal(mapped.get("amount_net")),
            amount_vat=_to_decimal(mapped.get("amount_vat")),
            contractor_name=mapped.get("contractor_name"),
            nip=mapped.get("nip"),
            document_type=mapped.get("document_type"),
            payment_term=_to_date(mapped.get("payment_term")),
            payment_method=mapped.get("payment_method"),
            raw_data=item,
        )

    async def get_document(
        self, db: AsyncSession, id_document: str
    ) -> UnifiedDocument | None:
        if not self._ep_detail:
            return None

        try:
            import httpx
            url = f"{self._base_url}{self._ep_detail.format(id=id_document)}"
            headers = self._get_auth_headers()

            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(url, headers=headers)

                if resp.status_code == 401 and self._auth_type == "bearer_refresh":
                    await self._refresh_bearer_token()
                    headers = self._get_auth_headers()
                    resp = await client.get(url, headers=headers)

                if resp.status_code == 404:
                    return None

                resp.raise_for_status()
                item = resp.json()

                # Klucz id_document w odpowiedzi — konfigurowalny
                id_col = self._config.get("id_column", "id")
                id_val = str(item.get(id_col, id_document))
                return self._map_json_to_unified(item, id_val)

        except Exception as exc:
            logger.error(
                "RestApiAdapter.get_document blad | source=%s id=%s: %s",
                self.source_name, id_document, exc,
            )
            raise

    async def fetch_new_documents(
        self,
        db: AsyncSession,
        since: datetime | None,
        limit: int = 500,
    ) -> list[UnifiedDocument]:
        """
        Odpytuje endpoint_list z filtrem daty i zwraca liste dokumentow.
        Obsluguje paginacje page/page_size lub cursor.
        """
        try:
            import httpx
        except ImportError:
            raise RuntimeError("Brak httpx. Zainstaluj: pip install httpx")

        results: list[UnifiedDocument] = []
        errors  = 0
        headers = self._get_auth_headers()
        url     = f"{self._base_url}{self._ep_list}"
        id_col  = self._config.get("id_column", "id")

        # Parametry zapytania — konfigurowalny klucz dla filtra dat
        params: dict[str, Any] = {}
        if since:
            date_param = self._pagination.get("date_param", "updated_since")
            params[date_param] = since.isoformat()

        page_size = self._pagination.get("page_size", 100)
        page_param = self._pagination.get("page_param", "page")
        page = 1

        async with httpx.AsyncClient(timeout=60) as client:
            while len(results) < limit:
                params[page_param] = page
                params["page_size"] = min(page_size, limit - len(results))

                resp = await client.get(url, headers=headers, params=params)

                if resp.status_code == 401 and self._auth_type == "bearer_refresh":
                    await self._refresh_bearer_token()
                    headers = self._get_auth_headers()
                    resp = await client.get(url, headers=headers, params=params)

                if resp.status_code == 429:
                    import asyncio
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    logger.warning(
                        "RestApiAdapter [%s]: HTTP 429 — czekam %ds",
                        self.source_name, retry_after,
                    )
                    await asyncio.sleep(retry_after)
                    continue

                resp.raise_for_status()
                data = resp.json()

                # Odpowiedz moze byc lista lub slownikiem {data: [...], total: ...}
                items_key = self._pagination.get("items_key", "data")
                items = data if isinstance(data, list) else data.get(items_key, [])

                if not items:
                    break

                for item in items:
                    try:
                        id_val = str(item.get(id_col, ""))
                        if not id_val:
                            continue
                        results.append(self._map_json_to_unified(item, id_val))
                    except Exception as exc:
                        errors += 1
                        logger.warning(
                            "RestApiAdapter.fetch_new_documents: pomijam rekord | "
                            "source=%s error=%s",
                            self.source_name, exc,
                        )

                # Sprawdz czy sa kolejne strony
                has_more_key = self._pagination.get("has_more_key", "has_more")
                if isinstance(data, dict):
                    has_more = data.get(has_more_key, len(items) == page_size)
                else:
                    has_more = len(items) == page_size

                if not has_more or len(items) < page_size:
                    break

                page += 1

        logger.info(
            "RestApiAdapter.fetch_new_documents | source=%s since=%s ok=%d errors=%d",
            self.source_name, since, len(results), errors,
        )
        return results

    def get_document_title(self, doc: UnifiedDocument) -> str:
        parts = []
        if doc.doc_number:
            parts.append(doc.doc_number)
        if doc.contractor_name:
            parts.append(doc.contractor_name)
        return " | ".join(parts) if parts else f"Dokument #{doc.id_document}"


# =============================================================================
# FakirDocumentAdapter — legacy adapter (pozostaje do czasu weryfikacji F3)
# =============================================================================

class FakirDocumentAdapter(BaseDocumentAdapter):
    """
    Adapter dla dokumentow z Fakir/WAPRO.
    Zrodlo: skw_faktury_akceptacja_naglowek (widok BUF_DOKUMENT + KONTRAHENT).

    ETAP 2 STATUS: LEGACY — uzywany przez istniejacy endpoint /faktury-akceptacja
    do czasu przelaczenia na nowa implementacje (ETAP2_FAKTURA_ENDPOINT_NEW_IMPL=true).
    Po weryfikacji workera synchronizacji dla Fakira na DatabaseAdapter — do usunięcia.

    id_document = KSEF_ID (decyzja F0.3).
    """

    source_name = "fakir"

    async def get_document(
        self, db: AsyncSession, id_document: str
    ) -> UnifiedDocument | None:
        """Pobiera naglowek faktury z widoku skw_faktury_akceptacja_naglowek po KSEF_ID."""

        row_result = await db.execute(
            text(
                f"SELECT * FROM [{_SCHEMA}].[skw_faktury_akceptacja_naglowek] "
                f"WHERE [KSEF_ID] = :ksef_id"
            ),
            {"ksef_id": id_document},
        )
        row = row_result.fetchone()
        if not row:
            return None

        cols = list(row_result.keys()) if hasattr(row_result, 'keys') else []
        raw: dict[str, Any] = dict(zip(cols, row)) if cols else {}

        # Pobierz id_source dla 'fakir'
        src_row = await db.execute(
            text(
                f"SELECT [id_source] FROM [{_SCHEMA}].[skw_document_sources] "
                f"WHERE [source_name] = N'fakir'"
            )
        )
        src = src_row.fetchone()
        id_source = src[0] if src else 1

        def _to_decimal(v):
            try:
                return Decimal(str(v)) if v is not None else None
            except Exception:
                return None

        def _clarion_date(v):
            if v is None:
                return None
            if isinstance(v, (date, datetime)):
                return v.date() if isinstance(v, datetime) else v
            if isinstance(v, int) and 0 < v < 200000:
                try:
                    from datetime import date as _date, timedelta
                    return _date(1899, 12, 30) + timedelta(days=v)
                except Exception:
                    pass
            try:
                return datetime.fromisoformat(str(v)).date()
            except Exception:
                return None

        return UnifiedDocument(
            id_document=id_document,
            id_source=id_source,
            source_name="fakir",
            doc_number=raw.get("NUMER"),
            doc_date=_clarion_date(raw.get("DataWystawienia")),
            amount_gross=_to_decimal(raw.get("WARTOSC_BRUTTO")),
            amount_net=_to_decimal(raw.get("WARTOSC_NETTO")),
            amount_vat=_to_decimal(raw.get("KWOTA_VAT")),
            contractor_name=raw.get("NazwaKontrahenta"),
            nip=None,  # NIP nie istnieje w aktualnym widoku
            document_type=raw.get("StatusOpis") or raw.get("KOD_STATUSU"),
            payment_method=raw.get("FORMA_PLATNOSCI"),
            payment_term=_clarion_date(raw.get("TerminPlatnosci")),
            raw_data={k: str(v) if v is not None else None for k, v in raw.items()},
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
# Fabryka adapterow — get_adapter() zastepuje ADAPTER_REGISTRY singleton
# =============================================================================

# Legacy ADAPTER_REGISTRY — zachowany dla kompatybilnosci wstecznej.
# Istniejacy kod ktory woła get_adapter(source_name) nadal działa.
# ETAP 2: nowy kod uzywa get_adapter(id_source, db, redis).
ADAPTER_REGISTRY: dict[str, BaseDocumentAdapter] = {
    "fakir": FakirDocumentAdapter(),
}


def get_adapter(source_name: str) -> BaseDocumentAdapter | None:
    """
    Legacy: zwraca adapter po nazwie zrodla.
    Uzyj get_adapter_by_id() dla nowych zrodel Etapu 2.
    """
    return ADAPTER_REGISTRY.get(source_name)


async def get_adapter_by_source_id(
    db: AsyncSession,
    id_source: int,
    redis: Any | None = None,
) -> BaseDocumentAdapter | None:
    """
    ETAP 2: Zwraca adapter dla id_source — dynamicznie na podstawie source_type.

    Kolejnosc:
      1. Sprawdz cache Redis (klucz adapter_config:{id_source}, TTL 5 min)
      2. Pobierz DocumentSource z bazy
      3. Zbuduj odpowiedni adapter na podstawie source_type
      4. Zapisz config do cache

    Args:
        db:        Sesja SQLAlchemy (async).
        id_source: ID zrodla z skw_document_sources.
        redis:     Klient Redis (opcjonalny — jesli None, bez cache).

    Returns:
        Skonfigurowana instancja adaptera lub None jesli zrodlo nieaktywne/nieznane.
    """
    # Cache Redis
    cache_key = f"{_ADAPTER_CACHE_PREFIX}{id_source}"
    if redis:
        try:
            cached_raw = await redis.get(cache_key)
            if cached_raw:
                cached = json.loads(cached_raw)
                source_type  = cached.get("source_type", "database")
                source_name  = cached.get("source_name", "")
                config       = cached.get("config", {})
                field_maps   = cached.get("field_mappings", [])
                return _build_adapter(id_source, source_name, source_type, config, field_maps)
        except Exception as exc:
            logger.warning("get_adapter_by_source_id: blad odczytu cache Redis: %s", exc)

    # Pobierz z bazy
    src_result = await db.execute(
        text(
            f"SELECT [id_source], [source_name], [source_type], "
            f"       [connection_config], [is_active], [is_test_mode] "
            f"FROM [{_SCHEMA}].[skw_document_sources] "
            f"WHERE [id_source] = :s"
        ),
        {"s": id_source},
    )
    row = src_result.fetchone()
    if not row:
        logger.warning("get_adapter_by_source_id: zrodlo id=%s nie istnieje", id_source)
        return None

    _, source_name, source_type, connection_config_raw, is_active, is_test_mode = row

    if not is_active:
        logger.info("get_adapter_by_source_id: zrodlo id=%s nieaktywne", id_source)
        return None

    # Deszyfruj connection_config
    config: dict[str, Any] = {}
    if connection_config_raw:
        try:
            from app.core.encryption import decrypt_value
            config = json.loads(decrypt_value(connection_config_raw))
        except Exception as exc:
            logger.error(
                "get_adapter_by_source_id: blad deszyfrowania config zrodla id=%s: %s",
                id_source, exc,
            )
            return None

    # Pobierz field_mappings
    fm_result = await db.execute(
        text(
            f"SELECT [common_field], [source_field], [field_type], [transform_expression] "
            f"FROM [{_SCHEMA}].[skw_document_source_field_mappings] "
            f"WHERE [id_source] = :s "
            f"ORDER BY [id_mapping] ASC"
        ),
        {"s": id_source},
    )
    field_mappings = [
        {
            "common_field": r[0],
            "source_field": r[1],
            "field_type":   r[2],
            "transform_expression": r[3],
        }
        for r in fm_result.fetchall()
    ]

    adapter = _build_adapter(id_source, source_name, source_type, config, field_mappings)

    # Zapisz do cache Redis
    if redis and adapter:
        try:
            cache_payload = json.dumps({
                "source_type":   source_type,
                "source_name":   source_name,
                "config":        config,
                "field_mappings": field_mappings,
            }, ensure_ascii=False, default=str)
            await redis.set(cache_key, cache_payload, ex=_ADAPTER_CACHE_TTL)
        except Exception as exc:
            logger.warning("get_adapter_by_source_id: blad zapisu cache Redis: %s", exc)

    return adapter


def _build_adapter(
    id_source: int,
    source_name: str,
    source_type: str,
    config: dict[str, Any],
    field_mappings: list[dict[str, Any]],
) -> BaseDocumentAdapter | None:
    """
    Buduje instancje adaptera na podstawie source_type.
    Zwraca None jesli source_type nieznany.
    """
    try:
        if source_type == "database":
            return DatabaseAdapter(id_source, source_name, config, field_mappings)

        if source_type == "api":
            return RestApiAdapter(id_source, source_name, config, field_mappings)

        # FTP i Email — w kolejnej iteracji
        if source_type in ("ftp", "email"):
            logger.warning(
                "_build_adapter: adapter dla source_type='%s' jeszcze nie zaimplementowany",
                source_type,
            )
            return None

        # KSeF 2.0 — dedykowany adapter (osobny plik)
        if source_type == "ksef20":
            try:
                from app.adapters.ksef20_adapter import KSeF20Adapter
                return KSeF20Adapter(id_source, source_name, config)
            except ImportError:
                logger.error("KSeF20Adapter nie jest jeszcze zaimplementowany")
                return None

        # Manual — dokumenty wchodzace przez webhook/UI, brak fetch_new_documents
        if source_type == "manual":
            return None  # manual nie ma adaptera synchronizacji

        logger.warning("_build_adapter: nieznany source_type='%s'", source_type)
        return None

    except (ValueError, KeyError) as exc:
        logger.error(
            "_build_adapter: blad konfiguracji adaptera dla id=%s type=%s: %s",
            id_source, source_type, exc,
        )
        return None


async def invalidate_adapter_cache(redis: Any, id_source: int) -> None:
    """
    Uniewa¿nia cache konfiguracji adaptera po aktualizacji zrodla.
    Wywolywac po PUT /sources/{id} i PATCH /sources/{id}/test-mode.
    """
    if not redis:
        return
    try:
        await redis.delete(f"{_ADAPTER_CACHE_PREFIX}{id_source}")
        logger.debug("invalidate_adapter_cache: id_source=%s", id_source)
    except Exception as exc:
        logger.warning("invalidate_adapter_cache blad: %s", exc)