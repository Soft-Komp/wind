# worker/services/source_adapter.py
"""
Minimalny, samodzielny port logiki adaptera zrodel dla workera — ETAP 2.1.

DLACZEGO TEN PLIK ISTNIEJE (a nie import z backend/app/schemas/unified_document.py):
  Worker dziala w osobnym kontenerze (windykacja_worker) bez zamontowanego
  pakietu `app` z backendu — swiadoma decyzja izolacji, identyczna z ta juz
  zastosowana w worker/core/db.py ("Modele ORM workera — import subset").
  Ten plik jest kontynuacja tego samego wzorca dla logiki adaptera zrodel.

SYNC: ten plik jest recznie synchronizowany z backend/app/schemas/unified_document.py.
Jesli zmieniasz UnifiedDocument, DatabaseAdapter lub RestApiAdapter w backendzie —
zmien identycznie tutaj. Brak automatycznej synchronizacji miedzy kontenerami.

Co jest tu ZAWARTE (tylko to, czego uzywa worker/tasks/source_sync_task.py):
  - UnifiedDocument         — trymowany model Pydantic (te same pola co w backendzie)
  - BaseDocumentAdapter     — interfejs (tylko fetch_new_documents — worker nie
                              potrzebuje get_document/get_document_title)
  - DatabaseAdapter         — pelny port z backendu (source_type='database')
  - RestApiAdapter          — pelny port z backendu (source_type='api')
  - decrypt_connection_config — cienki wrapper Fernet, czyta ENCRYPTION_KEY
                              z worker/settings.py (NIE z app.core.config)
  - get_adapter_by_source_id  — lokalny odpowiednik fabryki adaptera, czyta
                              skw_document_sources + skw_document_source_field_mappings
                              przez worker/core/db.py::get_engine()

Co NIE jest tu zawarte BEZPOSREDNIO (ale JEST podlaczone przez leniwy import
w _build_adapter() ponizej — patrz linie ~628-660):
  - FtpAdapter    — worker/services/ftp_adapter.py    (source_type='ftp')
  - EmailAdapter  — worker/services/email_adapter.py  (source_type='email')
  - KSeF20Adapter — worker/services/ksef20_adapter.py (source_type='ksef20')
  Wszystkie trzy sa REALNIE ZAIMPLEMENTOWANE i wpiete od 2026-07-13 — ten
  naglowek byl przez pomylke nie zaktualizowany przy tamtej zmianie i do
  2026-07-14 falszywie sugerowal, ze zwracaja None. Poprawiono.

UWAGA: brak `from __future__ import annotations` — ten plik nie jest routerem
FastAPI, wiec technicznie moglby byc, ale zachowuje spojnosc stylu z
backend/app/schemas/unified_document.py, gdzie jest to zakazane z innego powodu
(SQLAlchemy Mapped[]). Tutaj nie ma SQLAlchemy ORM, wiec nieistotne — pomijamy
dla jednolitosci wizualnej z plikiem zrodlowym.
"""

import json
import logging
import re
from abc import ABC, abstractmethod
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from typing import Any, Optional

import pyodbc
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from worker.settings import get_settings

logger = logging.getLogger("worker.services.source_adapter")

_SCHEMA = "dbo"

# Prefiks zaszyfrowanych wartosci — musi byc IDENTYCZNY z backend/app/core/encryption.py
_ENCRYPTED_PREFIX = "enc:"


# =============================================================================
# Deszyfrowanie connection_config — lokalny wrapper Fernet
# =============================================================================

class WorkerEncryptionError(Exception):
    """Blad deszyfrowania connection_config po stronie workera."""


@lru_cache(maxsize=1)
def _get_fernet():
    """
    Zwraca instancje Fernet skonfigurowana kluczem z worker/settings.py.
    Cachowana — jeden obiekt przez caly czas zycia procesu workera.

    KRYTYCZNE: ENCRYPTION_KEY musi byc bit-identyczny z tym w .env kontenera api —
    to ten sam plik .env (env_file: .env w docker-compose.yml dla obu serwisow),
    wiec w normalnych warunkach zawsze sa zgodne. Gdyby ktos kiedys rozdzielil
    .env dla api i workera — to pierwsze miejsce do sprawdzenia przy bledach
    deszyfrowania.
    """
    try:
        from cryptography.fernet import Fernet
    except ImportError as exc:
        raise WorkerEncryptionError(
            "Brak biblioteki cryptography w kontenerze workera. "
            "Sprawdz worker/requirements.txt."
        ) from exc

    settings = get_settings()
    key = settings.ENCRYPTION_KEY
    if not key:
        raise WorkerEncryptionError(
            "ENCRYPTION_KEY nie jest ustawiony w .env (worker). "
            "Musi byc identyczny z ENCRYPTION_KEY uzywanym przez kontener api."
        )
    try:
        return Fernet(key.encode() if isinstance(key, str) else key)
    except Exception as exc:
        raise WorkerEncryptionError(
            f"ENCRYPTION_KEY w .env ma nieprawidlowy format: {type(exc).__name__}"
        ) from exc


def decrypt_connection_config(ciphertext: str) -> str:
    """
    Deszyfruje connection_config. Lustrzane odbicie app.core.encryption.decrypt_value.

    Kompatybilnosc wsteczna: jesli wartosc nie ma prefiksu 'enc:' i jest
    prawidlowym JSON-em — zwraca ja bez zmian (legacy, nieszyfrowane zrodla
    z seeda migracji 0028: 'fakir', 'ksef').
    """
    if not ciphertext:
        return ciphertext

    if not ciphertext.startswith(_ENCRYPTED_PREFIX):
        try:
            json.loads(ciphertext)
            logger.debug("decrypt_connection_config: wartosc niezaszyfrowana (legacy JSON)")
            return ciphertext
        except json.JSONDecodeError:
            raise WorkerEncryptionError(
                "Wartosc connection_config nie jest ani zaszyfrowana ('enc:' prefix) "
                "ani prawidlowym JSON-em"
            )

    token_str = ciphertext[len(_ENCRYPTED_PREFIX):]
    try:
        fernet = _get_fernet()
        plaintext = fernet.decrypt(token_str.encode("ascii"))
        return plaintext.decode("utf-8")
    except WorkerEncryptionError:
        raise
    except Exception as exc:
        raise WorkerEncryptionError(
            f"Blad deszyfrowania connection_config: {type(exc).__name__}. "
            "Sprawdz czy ENCRYPTION_KEY w .env workera jest identyczny z api."
        ) from exc


# =============================================================================
# UnifiedDocument — trymowany model (te same pola co backend, ta sama semantyka)
# =============================================================================

class UnifiedDocument(BaseModel):
    """Wspolny format dokumentu. SYNC z backend/app/schemas/unified_document.py."""

    id_document:      str            = Field(description="Klucz dokumentu w zrodle (jako str)")
    id_source:        int            = Field(description="FK do skw_document_sources")
    source_name:      str            = Field(description="Nazwa zrodla: fakir|ksef|manual|...")

    doc_number:       Optional[str]     = Field(default=None)
    doc_date:         Optional[date]    = Field(default=None)
    amount_gross:     Optional[Decimal] = Field(default=None)
    amount_net:       Optional[Decimal] = Field(default=None)
    amount_vat:       Optional[Decimal] = Field(default=None)
    contractor_name:  Optional[str]     = Field(default=None)
    nip:               Optional[str]     = Field(default=None)
    document_type:    Optional[str]     = Field(default=None)
    currency:         Optional[str]     = Field(default=None)
    payment_term:     Optional[date]    = Field(default=None)
    payment_method:   Optional[str]     = Field(default=None)
    external_id:      Optional[str]     = Field(default=None)
    # NOWE (2026-07-28) — SYNC z backend/app/schemas/unified_document.py.
    # Wypelniane WYLACZNIE przez FtpAdapter/EmailAdapter (manual upload
    # ustawia je bezposrednio w documents_service.py, poza ta klasa).
    file_sha256:      Optional[str]     = Field(default=None)

    raw_data: dict[str, Any] = Field(default_factory=dict)

    def to_extra_data_json(self) -> dict:
        """Dane do zapisania w DocumentApprovalInstance.extra_data. SYNC z backendem."""
        return {
            "ksef_id":       self.id_document,
            "doc_number":    self.doc_number,
            "doc_date":      str(self.doc_date) if self.doc_date else None,
            "contractor":    self.contractor_name,
            "nip":           self.nip,
            "document_type": self.document_type,
            "source_name":   self.source_name,
            # NAPRAWA (2026-07-28): 'currency' brakowalo calkowicie — patrz
            # identyczna naprawa w backend/app/schemas/unified_document.py.
            "currency":      self.currency or "PLN",
            # NAPRAWA (2026-07-28): Decimal("0.00") jest falsy w Pythonie —
            # `if self.amount_gross` zamienialo kwote 0,00 na NULL. Ten sam
            # bug jak w source_sync_task.py::_upsert_instance, tu w drugim
            # miejscu tego samego przeplywu danych.
            "amount_gross":  float(self.amount_gross) if self.amount_gross is not None else None,
            "amount_net":    float(self.amount_net) if self.amount_net is not None else None,
            **self.raw_data,
        }


# =============================================================================
# Interfejs adaptera — tylko fetch_new_documents (worker nie potrzebuje reszty)
# =============================================================================

class BaseDocumentAdapter(ABC):
    source_name: str

    @abstractmethod
    async def fetch_new_documents(
        self, since: Optional[datetime], limit: int = 500
    ) -> list[UnifiedDocument]:
        """Pobiera nowe/zmienione dokumenty ze zrodla od czasu `since`."""
        raise NotImplementedError

    def supports_compound_cursor(self) -> bool:
        """
        NOWE (2026-07-28). Domyslnie False — nadpisywane przez DatabaseAdapter
        gdy skonfigurowano cursor_date_column + cursor_id_column. source_sync_task.py
        sprawdza to PRZED przekazaniem parametru 'cursor' do fetch_new_documents().
        """
        return False


# =============================================================================
# DatabaseAdapter — port z backend/app/schemas/unified_document.py
# =============================================================================

class DatabaseAdapter(BaseDocumentAdapter):
    """
    Generyczny adapter dla zrodel bazodanowych (source_type='database').

    connection_config (odszyfrowany JSON) MUSI zawierac:
        connection_string:  Pelny connection string ODBC (NIE osobne
                             host/port/database/username/password — patrz
                             UWAGA nizej, to znana rozbieznosc z formularzem admina)
        view_name:           Nazwa widoku/tabeli zrodlowej
        id_column:            Kolumna klucza dokumentu (domyslnie 'KSEF_ID')
        date_column:          Kolumna daty do filtrowania od `since` (opcjonalna)

    connection_string moze byc podany:
      (a) wprost jako gotowy string ODBC w config['connection_string'], lub
      (b) zbudowany automatycznie z osobnych pol: host, port, database,
          username, password, driver, encrypt, trust_server_certificate
          (dokladnie to, co wysyla dzis panel admina). Wariant (a) ma
          pierwszenstwo, jesli oba sa obecne.
    SYNC: identyczna logika w backend/app/schemas/unified_document.py::DatabaseAdapter.
    """

    _SAFE_DRIVERS = frozenset({
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
    })

    _DRIVER_ALIASES = {
        "mssql":     "ODBC Driver 18 for SQL Server",
        "sqlserver": "ODBC Driver 18 for SQL Server",
        "odbc18":    "ODBC Driver 18 for SQL Server",
        "odbc17":    "ODBC Driver 17 for SQL Server",
    }

    def __init__(
        self,
        id_source: int,
        source_name: str,
        config: dict[str, Any],
        field_mappings: Optional[list[dict[str, Any]]] = None,
    ) -> None:
        self.id_source   = id_source
        self.source_name = source_name
        self._config     = config
        self._mappings   = field_mappings or []

        self._conn_str  = config.get("connection_string", "") or self._build_connection_string(config)
        self._view_name = config.get("view_name", "")
        self._id_col    = config.get("id_column", "KSEF_ID")
        self._date_col  = config.get("date_column")

        # NOWE (2026-07-28) — kursor zlozony (data+id), SYNC z backend/app/
        # schemas/unified_document.py::DatabaseAdapter. Patrz tamten plik
        # dla pelnego uzasadnienia (naprawa wielokrotnego pobierania tych
        # samych TOP N rekordow przy date_column=None).
        self._cursor_date_col = config.get("cursor_date_column")
        self._cursor_id_col   = config.get("cursor_id_column")
        # NOWE (2026-07-28) — opcjonalny punkt startowy dla PIERWSZEJ
        # synchronizacji w trybie kursora zlozonego. Bez tego pierwsza
        # synchronizacja zaczynala od najstarszego rekordu w widoku
        # (potencjalnie lata historii) zamiast od ustalonej daty startowej
        # zrodla. Format: string 'YYYY-MM-DD' lub 'YYYY-MM-DDTHH:MM:SS'.
        # Brak tego klucza = stare zachowanie (od poczatku widoku),
        # zachowane dla zrodel, ktore juz dzialaja bez niego.
        self._initial_cursor_date = config.get("initial_cursor_date")

        self._validate_config()

    def _build_connection_string(self, config: dict[str, Any]) -> str:
        """
        Buduje connection string ODBC z osobnych pol, gdy connection_string
        nie zostal podany wprost. Zwraca pusty string, jesli brakuje ktoregos
        z wymaganych pol — _validate_config() zglosi to jako czytelny blad.
        NIGDY nie loguje zwroconego stringa (zawiera haslo w plaintext).
        SYNC z backend/app/schemas/unified_document.py::DatabaseAdapter._build_connection_string.
        """
        host     = config.get("host")
        database = config.get("database")
        username = config.get("username")
        password = config.get("password")

        if not all([host, database, username, password]):
            return ""

        port       = config.get("port", 1433)
        driver_raw = str(config.get("driver", "mssql")).strip()
        driver     = self._DRIVER_ALIASES.get(driver_raw.lower(), driver_raw)
        encrypt    = config.get("encrypt", True)
        trust_cert = config.get("trust_server_certificate", False)

        if driver not in self._SAFE_DRIVERS:
            logger.error(
                "DatabaseAdapter [%s]: niedozwolony driver '%s' (po aliasowaniu: '%s')",
                self.source_name, driver_raw, driver,
            )
            return ""

        return (
            f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};"
            f"UID={username};PWD={password};"
            f"Encrypt={'yes' if encrypt else 'no'};"
            f"TrustServerCertificate={'yes' if trust_cert else 'no'};"
            "Connection Timeout=30;MARS_Connection=yes"
        )

    def _validate_config(self) -> None:
        if not self._conn_str:
            raise ValueError(
                f"DatabaseAdapter [{self.source_name}]: brak 'connection_string' w config "
                f"i nie udalo sie go zbudowac z osobnych pol (wymagane: host, database, "
                f"username, password, oraz driver z dozwolonej listy: {sorted(self._SAFE_DRIVERS)})"
            )
        if not self._view_name:
            raise ValueError(
                f"DatabaseAdapter [{self.source_name}]: brak 'view_name' w config"
            )
        if not re.match(r'^[a-zA-Z0-9_.]+$', self._view_name):
            raise ValueError(
                f"DatabaseAdapter [{self.source_name}]: "
                f"view_name '{self._view_name}' zawiera niedozwolone znaki"
            )
        if not re.match(r'^[a-zA-Z0-9_]+$', self._id_col):
            raise ValueError(
                f"DatabaseAdapter [{self.source_name}]: "
                f"id_column '{self._id_col}' zawiera niedozwolone znaki"
            )
        cursor_fields_present = bool(self._cursor_date_col) or bool(self._cursor_id_col)
        if cursor_fields_present and not (self._cursor_date_col and self._cursor_id_col):
            raise ValueError(
                f"DatabaseAdapter [{self.source_name}]: kursor zlozony wymaga "
                f"OBU pol jednoczesnie (cursor_date_column={self._cursor_date_col!r}, "
                f"cursor_id_column={self._cursor_id_col!r})"
            )
        if self._cursor_date_col and not re.match(r'^[a-zA-Z0-9_]+$', self._cursor_date_col):
            raise ValueError(
                f"DatabaseAdapter [{self.source_name}]: "
                f"cursor_date_column '{self._cursor_date_col}' zawiera niedozwolone znaki"
            )
        if self._cursor_id_col and not re.match(r'^[a-zA-Z0-9_]+$', self._cursor_id_col):
            raise ValueError(
                f"DatabaseAdapter [{self.source_name}]: "
                f"cursor_id_column '{self._cursor_id_col}' zawiera niedozwolone znaki"
            )

    @staticmethod
    def _bracket_qualify(name: str) -> str:
        """
        Konwertuje 'dbo.widok' na poprawne '[dbo].[widok]'.

        POPRAWKA: poprzedni kod robil f"[{view_name}]" co dla nazwy z kropka
        dawalo '[dbo.widok]' — SQL Server odczytuje to jako JEDEN identyfikator
        z kropka w nazwie (nie istnieje), zamiast dwuczesciowej nazwy
        schemat.obiekt. Stad falszywe "Invalid object name" nawet gdy widok
        istnieje. Obsluguje tez nazwy bez schematu ('widok' -> '[widok]').
        SYNC z backend/app/schemas/unified_document.py::DatabaseAdapter._bracket_qualify.
        """
        parts = [p for p in name.split(".") if p]
        return ".".join(f"[{p}]" for p in parts)

    def _get_pyodbc_conn(self) -> pyodbc.Connection:
        conn = pyodbc.connect(self._conn_str, autocommit=True, timeout=30)
        conn.setdecoding(pyodbc.SQL_CHAR,  encoding="utf-8")
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-8")
        conn.setencoding(encoding="utf-8")
        return conn

    @staticmethod
    def _cast_value(val: Any, field_type: str, transform: Optional[str] = None) -> Any:
        """Rzutuje wartosc z bazy na typ Pythona wg field_type."""
        if val is None:
            return None
        try:
            if field_type == "decimal":
                return Decimal(str(val))
            if field_type == "date":
                if isinstance(val, (date, datetime)):
                    return val.date() if isinstance(val, datetime) else val
                return datetime.fromisoformat(str(val)).date()
            if field_type == "int":
                return int(val)
            return str(val)
        except Exception:
            logger.warning(
                "DatabaseAdapter._cast_value: nie udalo sie rzutowac '%s' na %s",
                val, field_type,
            )
            return None

    def _row_to_unified(self, row: dict[str, Any]) -> UnifiedDocument:
        mapped: dict[str, Any] = {}

        if self._mappings:
            for m in self._mappings:
                src_field    = m.get("source_field", "")
                common_field = m.get("common_field", "")
                field_type   = m.get("field_type", "string")
                raw_val = row.get(src_field)
                mapped[common_field] = self._cast_value(raw_val, field_type)
        else:
            # Domyslne mapowanie dla Fakira (widok skw_faktury_akceptacja_naglowek /
            # skw_v_dokumenty_do_obiegu) — identyczne z backendem
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
                f"id_column '{self._id_col}' jest NULL lub puste w rekordzie"
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

    def supports_compound_cursor(self) -> bool:
        """NOWE (2026-07-28). SYNC z backend/app/schemas/unified_document.py."""
        return bool(self._cursor_date_col and self._cursor_id_col)

    async def fetch_new_documents(
        self, since: Optional[datetime], limit: int = 500,
        cursor: Optional[dict[str, Any]] = None,
    ) -> list[UnifiedDocument]:
        results: list[UnifiedDocument] = []
        errors = 0

        try:
            with self._get_pyodbc_conn() as conn:
                cur = conn.cursor()
                qualified_view = self._bracket_qualify(self._view_name)

                if self.supports_compound_cursor():
                    last_date = cursor.get("date") if cursor else None
                    last_id   = cursor.get("id") if cursor else None
                    if last_date and last_id is not None:
                        sql = (
                            f"SELECT TOP {int(limit)} * FROM {qualified_view} "
                            f"WHERE ([{self._cursor_date_col}] > ?) "
                            f"   OR ([{self._cursor_date_col}] = ? AND [{self._cursor_id_col}] > ?) "
                            f"ORDER BY [{self._cursor_date_col}] ASC, [{self._cursor_id_col}] ASC"
                        )
                        cur.execute(sql, (last_date, last_date, last_id))
                    elif self._initial_cursor_date:
                        # NOWE (2026-07-28) — pierwsza synchronizacja, ALE
                        # skonfigurowano punkt startowy. Zamiast od poczatku
                        # widoku, zaczynamy od tej daty wlacznie — dokladnie
                        # to samo zachowanie, co poprzednio osiagane recznym
                        # UPDATE sync_cursor, teraz jako jawna, dokumentowana
                        # konfiguracja zrodla zamiast operacyjnego obejscia.
                        sql = (
                            f"SELECT TOP {int(limit)} * FROM {qualified_view} "
                            f"WHERE [{self._cursor_date_col}] >= ? "
                            f"ORDER BY [{self._cursor_date_col}] ASC, [{self._cursor_id_col}] ASC"
                        )
                        cur.execute(sql, (self._initial_cursor_date,))
                        logger.info(
                            "DatabaseAdapter.fetch_new_documents [kursor zlozony, "
                            "PIERWSZA synchronizacja od skonfigurowanej daty "
                            "initial_cursor_date=%s] | source=%s",
                            self._initial_cursor_date, self.source_name,
                        )
                    else:
                        # Brak zapisanego kursora I brak initial_cursor_date —
                        # pobieramy od poczatku widoku (stare zachowanie,
                        # zachowane dla zrodel bez tej konfiguracji).
                        sql = (
                            f"SELECT TOP {int(limit)} * FROM {qualified_view} "
                            f"ORDER BY [{self._cursor_date_col}] ASC, [{self._cursor_id_col}] ASC"
                        )
                        cur.execute(sql)
                        logger.warning(
                            "DatabaseAdapter.fetch_new_documents [kursor zlozony, "
                            "PIERWSZA synchronizacja — BRAK initial_cursor_date, "
                            "pobieram od poczatku CALEJ historii widoku] | source=%s. "
                            "Jesli to nie bylo zamierzone, ustaw 'initial_cursor_date' "
                            "w konfiguracji zrodla PRZED aktywacja.",
                            self.source_name,
                        )
                elif self._date_col and since:
                    since_str = since.strftime("%Y-%m-%dT%H:%M:%S")
                    sql = (
                        f"SELECT TOP {int(limit)} * FROM {qualified_view} "
                        f"WHERE [{self._date_col}] >= ? "
                        f"ORDER BY [{self._id_col}] ASC"
                    )
                    cur.execute(sql, (since_str,))
                else:
                    sql = (
                        f"SELECT TOP {int(limit)} * FROM {qualified_view} "
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
                            self.source_name, exc, row.get(self._id_col, "?"),
                        )

        except Exception as exc:
            logger.error(
                "DatabaseAdapter.fetch_new_documents blad | source=%s since=%s cursor=%s: %s",
                self.source_name, since, cursor, exc,
            )
            raise

        logger.info(
            "DatabaseAdapter.fetch_new_documents | source=%s since=%s cursor=%s ok=%d errors=%d",
            self.source_name, since, cursor, len(results), errors,
        )
        return results

    def extract_cursor(self, docs: list[UnifiedDocument]) -> Optional[dict[str, Any]]:
        """NOWE (2026-07-28). SYNC z backend/app/schemas/unified_document.py."""
        if not self.supports_compound_cursor() or not docs:
            return None
        last = docs[-1]
        return {
            "date": last.raw_data.get(self._cursor_date_col),
            "id":   last.raw_data.get(self._cursor_id_col),
        }


# =============================================================================
# RestApiAdapter — port z backend/app/schemas/unified_document.py
# =============================================================================

class RestApiAdapter(BaseDocumentAdapter):
    """Generyczny adapter dla zrodel REST API (source_type='api')."""

    def __init__(
        self,
        id_source: int,
        source_name: str,
        config: dict[str, Any],
        field_mappings: Optional[list[dict[str, Any]]] = None,
    ) -> None:
        self.id_source   = id_source
        self.source_name = source_name
        self._config     = config
        self._mappings   = field_mappings or []

        self._base_url      = config.get("base_url", "").rstrip("/")
        self._auth_type     = config.get("auth_type", "api_key")
        self._auth_config   = config.get("auth_config", {})
        self._ep_list       = config.get("endpoint_list", "")
        self._pagination    = config.get("pagination", {})
        self._json_mappings = config.get("field_mappings", {})

        self._validate_config()

    def _validate_config(self) -> None:
        if not self._base_url:
            raise ValueError(f"RestApiAdapter [{self.source_name}]: brak 'base_url'")
        if not self._ep_list:
            raise ValueError(f"RestApiAdapter [{self.source_name}]: brak 'endpoint_list'")
        # POPRAWKA (2026-07-15): 'none' dla publicznych API bez autoryzacji.
        # Zsynchronizowane z backend/app/schemas/unified_document.py — patrz
        # tamtejsza identyczna zmiana z tej samej daty.
        valid_auth = {"bearer_refresh", "api_key", "basic", "none"}
        if self._auth_type not in valid_auth:
            raise ValueError(
                f"RestApiAdapter [{self.source_name}]: "
                f"nieprawidlowy auth_type '{self._auth_type}'. Dozwolone: {valid_auth}"
            )

    def _get_auth_headers(self) -> dict[str, str]:
        """Buduje naglowki autoryzacyjne."""
        if self._auth_type == "none":
            return {}

        if self._auth_type == "api_key":
            key = self._auth_config.get("api_key", "")
            hdr = self._auth_config.get("header_name", "X-Api-Key")
            return {hdr: key}
        if self._auth_type == "basic":
            import base64
            login = self._auth_config.get("login", "")
            pwd   = self._auth_config.get("password", "")
            token = base64.b64encode(f"{login}:{pwd}".encode()).decode()
            return {"Authorization": f"Basic {token}"}
        if self._auth_type == "bearer_refresh":
            token = self._auth_config.get("access_token", "")
            return {"Authorization": f"Bearer {token}"}
        return {}

    async def _refresh_bearer_token(self) -> None:
        try:
            import httpx
            refresh_url   = self._auth_config.get("token_url", "")
            refresh_token = self._auth_config.get("refresh_token", "")
            client_id     = self._auth_config.get("client_id", "")
            client_secret = self._auth_config.get("client_secret", "")
            if not refresh_url:
                return
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    refresh_url,
                    data={
                        "grant_type": "refresh_token",
                        "refresh_token": refresh_token,
                        "client_id": client_id,
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
        mapped: dict[str, Any] = {}
        for json_key, common_field in self._json_mappings.items():
            parts = json_key.split(".")
            val: Any = item
            for p in parts:
                val = val.get(p) if isinstance(val, dict) else None
            mapped[common_field] = val

        return UnifiedDocument(
            id_document=id_document,
            id_source=self.id_source,
            source_name=self.source_name,
            doc_number=mapped.get("doc_number"),
            contractor_name=mapped.get("contractor_name"),
            nip=mapped.get("nip"),
            document_type=mapped.get("document_type"),
            raw_data={k: str(v) if v is not None else None for k, v in item.items()},
        )

    async def fetch_new_documents(
        self, since: Optional[datetime], limit: int = 500
    ) -> list[UnifiedDocument]:
        try:
            import httpx
        except ImportError:
            raise RuntimeError("Brak httpx w kontenerze workera (powinien byc w requirements.txt)")

        results: list[UnifiedDocument] = []
        headers = self._get_auth_headers()
        url     = f"{self._base_url}{self._ep_list}"
        id_col  = self._config.get("id_column", "id")

        params: dict[str, Any] = {}
        if since:
            date_param = self._pagination.get("date_param", "updated_since")
            params[date_param] = since.isoformat()

        page_size  = self._pagination.get("page_size", 100)
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

                resp.raise_for_status()
                items = resp.json()
                if isinstance(items, dict):
                    items = items.get("items") or items.get("data") or []
                if not items:
                    break

                for item in items:
                    id_val = str(item.get(id_col, ""))
                    if id_val:
                        results.append(self._map_json_to_unified(item, id_val))

                if len(items) < page_size:
                    break
                page += 1

        logger.info(
            "RestApiAdapter.fetch_new_documents | source=%s since=%s ok=%d",
            self.source_name, since, len(results),
        )
        return results


# =============================================================================
# Fabryka adaptera — lokalny odpowiednik get_adapter_by_source_id z backendu
# =============================================================================

def _build_adapter(
    id_source: int,
    source_name: str,
    source_type: str,
    config: dict[str, Any],
    field_mappings: list[dict[str, Any]],
    redis: Any = None,
) -> Optional[BaseDocumentAdapter]:
    try:
        if source_type == "database":
            return DatabaseAdapter(id_source, source_name, config, field_mappings)
        if source_type == "api":
            return RestApiAdapter(id_source, source_name, config, field_mappings)
        if source_type == "ftp":
            from worker.services.ftp_adapter import FtpAdapter
            return FtpAdapter(id_source, source_name, config)

        if source_type == "email":
            from worker.services.email_adapter import EmailAdapter
            return EmailAdapter(id_source, source_name, config)
        if source_type == "ksef20":
            from worker.services.ksef20_adapter import KSeF20Adapter
            return KSeF20Adapter(id_source, source_name, config, redis=redis)
        if source_type == "manual":
            return None
        logger.warning("_build_adapter: nieznany source_type='%s'", source_type)
        return None
    except (ValueError, KeyError) as exc:
        logger.error(
            "_build_adapter: blad konfiguracji adaptera dla id=%s type=%s: %s",
            id_source, source_type, exc,
        )
        return None


async def get_adapter_by_source_id(
    db_conn: AsyncConnection,
    id_source: int,
    redis: Any = None,
) -> Optional[BaseDocumentAdapter]:
    """
    Lokalny (workerowy) odpowiednik app.schemas.unified_document.get_adapter_by_source_id.

    Roznice wzgledem backendu:
      - Brak cache Redis (worker juz ma dystrybucyjny lock per-zrodlo w
        source_sync_task.py — cache adaptera nie jest tu krytyczny, sync i tak
        odbywa sie raz na sync_interval_minutes)
      - Uzywa AsyncConnection (nie AsyncSession) — spojne z worker/core/db.py
      - Deszyfrowanie przez decrypt_connection_config() (lokalny Fernet wrapper)
        zamiast app.core.encryption.decrypt_value
    """
    src_result = await db_conn.execute(
        text(
            f"SELECT [id_source], [source_name], [source_type], "
            f"       [connection_config], [is_active] "
            f"FROM [{_SCHEMA}].[skw_document_sources] "
            f"WHERE [id_source] = :s"
        ),
        {"s": id_source},
    )
    row = src_result.fetchone()
    if not row:
        logger.warning("get_adapter_by_source_id: zrodlo id=%s nie istnieje", id_source)
        return None

    _, source_name, source_type, connection_config_raw, is_active = row

    if not is_active:
        logger.info("get_adapter_by_source_id: zrodlo id=%s nieaktywne", id_source)
        return None

    config: dict[str, Any] = {}
    if connection_config_raw:
        try:
            config = json.loads(decrypt_connection_config(connection_config_raw))
        except Exception as exc:
            logger.error(
                "get_adapter_by_source_id: blad deszyfrowania config zrodla id=%s: %s",
                id_source, exc,
            )
            return None

    fm_result = await db_conn.execute(
        text(
            f"SELECT [common_field], [source_field], [field_type], [transform_expression] "
            f"FROM [{_SCHEMA}].[skw_document_source_field_mappings] "
            f"WHERE [id_source] = :s "
            f"ORDER BY [id_mapping] ASC"
        ),
        {"s": id_source},
    )
    field_mappings = [
        {"common_field": r[0], "source_field": r[1], "field_type": r[2], "transform_expression": r[3]}
        for r in fm_result.fetchall()
    ]

    return _build_adapter(id_source, source_name, source_type, config, field_mappings, redis=redis)