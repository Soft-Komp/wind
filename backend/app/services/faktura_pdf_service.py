"""
Plik   : app/services/faktura_pdf_service.py
Moduł  : Akceptacja Faktur KSeF — Sprint 2 / S2-01
Serwis : Generowanie PDF Karty Akceptacji Faktury

Wzorzec:
    async generate_pdf()
        → zbiera dane async (pozycje WAPRO, nazwy userów, logo path)
        → buduje _PDFKontekst (serializowalny plain-data)
        → wywołuje _build_pdf_sync() w asyncio.run_in_executor()
        → ReportLab → BytesIO → bytes
        → NIE zapisuje na dysk

Polskie znaki : DejaVu Sans TTF (/usr/share/fonts/truetype/dejavu/)
Logo          : klucz SystemConfig 'faktury.pdf_logo_path'
               → ścieżka do pliku PNG/JPG na serwerze
               → fallback: settings.LOGO_PATH z .env
               → brak logo: pominięcie (nie błąd, tylko ostrzeżenie)
Cache         : zarządzany przez wywołującego (faktura_akceptacja_service.py)
               — ten serwis zawsze generuje od nowa
Bezpieczeństwo:
    • Brak zapisu na dysk (in-memory BytesIO)
    • Wszystkie pola sanityzowane przed wstawieniem do PDF
    • SHA-256 preview komentarzy (privacy by design — pełna treść nie trafia do AuditLog)
    • run_in_executor — ReportLab nie blokuje event loop FastAPI
Logowanie:
    logger "app.services.faktura_pdf" — structured JSON do logs/app_*.log
    Metryki: czas generowania [ms], rozmiar PDF [KB], liczba pozycji, liczba przypisań

ZALEŻNOŚCI:
    reportlab >= 3.6    (zainstalowane w backend/requirements.txt)
    orjson              (już używane w projekcie)
    DejaVu fonts        (fonts-dejavu-core w backend/Dockerfile)

AUTOR : Sprint 2 — S2-01
DATA  : 2026-03-30
WERSJA: 1.0
"""

# =============================================================================
# Importy — stdlib → third-party → lokalne
# =============================================================================
import asyncio
import hashlib
import logging
import threading
import time as _time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

import orjson
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.models.faktura_akceptacja import FakturaAkceptacja
from app.db.models.faktura_przypisanie import FakturaPrzypisanie
from app.schemas.faktura_akceptacja import WaproFakturaNaglowek
from app.services.config_service import get_config_value

# =============================================================================
# Logger
# =============================================================================
logger = logging.getLogger("app.services.faktura_pdf")

# =============================================================================
# Stałe — wersja, ścieżki, kolory (HEX)
# =============================================================================
_APP_VERSION   = "1.0"
_DEJAVU_DIR    = "/usr/share/fonts/truetype/dejavu"

# Paleta kolorów — spójna z marką systemu
_KOLOR_NAGLOWEK_BG   = "#1A365D"   # ciemny granat — nagłówki tabel, linie
_KOLOR_NORMALNY      = "#2980B9"   # niebieski — status NOWE, priorytet normalny
_KOLOR_PILNY         = "#E67E22"   # pomarańczowy — priorytet pilny
_KOLOR_BARDZO_PILNY  = "#C0392B"   # czerwony — priorytet bardzo_pilny
_KOLOR_ZAAKCEPTOWANA = "#27AE60"   # zielony — zaakceptowana
_KOLOR_W_TOKU        = "#E67E22"   # pomarańczowy — w toku
_KOLOR_ANULOWANA     = "#7F8C8D"   # szary — anulowana / orphaned
_KOLOR_NOWE          = "#2980B9"   # niebieski — nowe
_KOLOR_OVERDUE       = "#C0392B"   # czerwony — alert przeterminowania
_KOLOR_WIERSZ_PAIR   = "#F0F4FA"   # bardzo jasny niebieski — parzyste wiersze tabel

# =============================================================================
# Thread-safe rejestracja fontów — double-checked locking (Singleton pattern)
# Rejestrujemy raz przy pierwszym generowaniu PDF, nie przy starcie aplikacji.
# =============================================================================
_fonts_registered: bool           = False
_fonts_lock:       threading.Lock = threading.Lock()


def _ensure_fonts_registered() -> None:
    """
    Rejestruje fonty DejaVu Sans w ReportLab pdfmetrics.

    Idempotentna — bezpieczna przy wielokrotnym wywołaniu z wielu wątków.
    W przypadku braku pliku fontu: loguje WARN i kontynuuje (polskie znaki
    mogą nie być wyświetlane poprawnie, ale PDF zostanie wygenerowany).
    """
    global _fonts_registered
    if _fonts_registered:
        return
    with _fonts_lock:
        if _fonts_registered:          # podwójne sprawdzenie po locku
            return
        try:
            from reportlab.pdfbase import pdfmetrics
            from reportlab.pdfbase.ttfonts import TTFont

            pdfmetrics.registerFont(
                TTFont("DejaVu",         f"{_DEJAVU_DIR}/DejaVuSans.ttf"))
            pdfmetrics.registerFont(
                TTFont("DejaVu-Bold",    f"{_DEJAVU_DIR}/DejaVuSans-Bold.ttf"))
            pdfmetrics.registerFont(
                TTFont("DejaVu-Oblique", f"{_DEJAVU_DIR}/DejaVuSans-Oblique.ttf"))
            pdfmetrics.registerFontFamily(
                "DejaVu",
                normal     = "DejaVu",
                bold       = "DejaVu-Bold",
                italic     = "DejaVu-Oblique",
                boldItalic = "DejaVu-Bold",
            )
            _fonts_registered = True
            logger.info(
                "Fonty DejaVu zarejestrowane pomyślnie",
                extra={"dejavu_dir": _DEJAVU_DIR},
            )
        except Exception as exc:
            # Graceful degradation — nie rzucamy, tylko logujemy
            logger.warning(
                "Nie można zarejestrować fontów DejaVu — polskie znaki mogą nie wyświetlać się poprawnie",
                extra={"dejavu_dir": _DEJAVU_DIR, "error": str(exc)},
            )
            _fonts_registered = True   # zapobiegamy ponawianiu przy każdym PDF


# =============================================================================
# Kontekst danych — przekazywany do synchronicznej funkcji budowania PDF
# Wszystkie pola: typy proste (str, int, float, dict, list) — bezpieczne dla
# przekazania przez granicę asyncio ↔ thread executor.
# =============================================================================
@dataclass
class _PDFKontekst:
    """
    Serializowalny kontener danych dla _build_pdf_sync().

    Zasada: po zbudowaniu _PDFKontekst żadne async operacje nie są potrzebne.
    Cały I/O (DB, Redis, WAPRO) wykonywany PRZED budowaniem kontekstu.
    """
    # Dane z naszej tabeli (skw_faktura_akceptacja)
    faktura_id:        int
    numer_ksef:        str
    status_wewnetrzny: str
    priorytet:         str
    opis_dokumentu:    Optional[str]
    uwagi_referenta:   Optional[str]
    created_at:        Optional[str]        # ISO datetime string lub None
    is_orphaned:       bool

    # Dane z widoku WAPRO (skw_faktury_akceptacja_naglowek) — None jeśli brak
    wapro: Optional[dict[str, Any]]         # pole model_dump() z WaproFakturaNaglowek

    # Pozycje z widoku WAPRO (skw_faktury_akceptacja_pozycje)
    pozycje: list[dict[str, Any]]

    # Przypisania z danymi pracowników (user_id + full_name + decyzja)
    przypisania: list[dict[str, Any]]

    # Konfiguracja firmy i wygląd
    logo_path:    Optional[str]
    firma_nazwa:  str
    firma_nip:    str
    generated_at: str                       # ISO datetime string

    # Kontekst requestu (do logowania metryk po zakończeniu)
    request_context: dict[str, Any] = field(default_factory=dict)


# =============================================================================
# Helpery async — zbieranie danych przed budowaniem kontekstu
# =============================================================================

async def _fetch_pozycje(numer_ksef: str) -> list[dict[str, Any]]:
    """
    Pobiera pozycje faktury z widoku WAPRO (dbo.skw_faktury_akceptacja_pozycje).

    Zwraca pustą listę przy błędzie — PDF wygeneruje się bez sekcji pozycji.
    Loguje WARNING z pełnymi szczegółami przy każdym błędzie (redundancja logów).
    """
    try:
        from app.db.wapro import execute_query
        rows = await execute_query(
            query_type="faktura_pozycje",
            params={"ksef_id": numer_ksef},
        )

        def _to_float(v: Any) -> Optional[float]:
            if v is None:
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        result = []
        for r in rows:
            result.append({
                "numer_pozycji":  int(r.get("NumerPozycji") or 0),
                "nazwa_towaru":   str(r.get("NazwaTowaru") or ""),
                "ilosc":          _to_float(r.get("Ilosc")),
                "jednostka":      str(r.get("Jednostka") or ""),
                "cena_netto":     _to_float(r.get("CenaNetto")),
                "wartosc_netto":  _to_float(r.get("WartoscNetto")),
                "wartosc_brutto": _to_float(r.get("WartoscBrutto")),
                "stawka_vat":     str(r.get("StawkaVAT") or ""),
                "opis":           str(r.get("Opis") or ""),
            })

        logger.debug(
            "Pobrano pozycje WAPRO dla PDF",
            extra={"numer_ksef": numer_ksef, "count": len(result)},
        )
        return result

    except Exception as exc:
        logger.warning(
            "Błąd pobierania pozycji WAPRO — PDF wygeneruje się bez sekcji pozycji",
            extra={
                "numer_ksef":  numer_ksef,
                "error":       str(exc),
                "error_type":  type(exc).__name__,
            },
        )
        return []


async def _fetch_user_names(
    db:       AsyncSession,
    user_ids: list[int],
) -> dict[int, str]:
    """
    Pobiera imiona/nazwiska pracowników wskazanych przez user_ids.
    Jedno zapytanie batch — nie N zapytań.

    Zwraca: {user_id: "Imię Nazwisko"} lub "User #ID" gdy brak danych.
    Bezpieczne dla pustej listy (zwraca {} bez zapytania).
    """
    if not user_ids:
        return {}

    try:
        # Bezpieczne: user_ids to int (bez ryzyka SQL injection)
        unique_ids  = list(set(user_ids))
        placeholders = ",".join(str(uid) for uid in unique_ids)

        result = await db.execute(
            text(
                f"SELECT ID_USER, FullName, Username "
                f"FROM dbo_ext.skw_Users "
                f"WHERE ID_USER IN ({placeholders})"
            )
        )
        users_map: dict[int, str] = {}
        for row in result.mappings():
            uid  = int(row["ID_USER"])
            name = (
                row.get("FullName")
                or row.get("Username")
                or f"User #{uid}"
            )
            users_map[uid] = str(name)

        logger.debug(
            "Pobrano nazwy użytkowników dla historii akceptacji",
            extra={
                "requested": len(unique_ids),
                "found":     len(users_map),
                "missing":   [uid for uid in unique_ids if uid not in users_map],
            },
        )
        return users_map

    except Exception as exc:
        logger.warning(
            "Błąd pobierania nazw użytkowników dla PDF — zostaną użyte ID",
            extra={
                "user_ids":   user_ids,
                "error":      str(exc),
                "error_type": type(exc).__name__,
            },
        )
        return {}


async def _fetch_logo_path(
    redis: Redis,
    db:    AsyncSession,
) -> Optional[str]:
    """
    Pobiera ścieżkę do logo firmy.

    Kolejność próbowania:
    1. SystemConfig key: 'faktury.pdf_logo_path' (przez Redis cache + DB)
    2. Fallback: settings.LOGO_PATH z .env
    3. None — PDF bez logo (nie błąd)

    Zwraca ścieżkę jeśli plik istnieje na dysku, None w przeciwnym przypadku.
    """
    # ── Próba 1: SystemConfig przez config_service ────────────────────────────
    try:
        logo_path_cfg = await get_config_value(
            redis   = redis,
            key     = "faktury.pdf_logo_path",
            default = "",
        )
        if logo_path_cfg and logo_path_cfg.strip():
            p = Path(logo_path_cfg.strip())
            if p.exists() and p.is_file():
                logger.debug(
                    "Logo załadowane z SystemConfig 'faktury.pdf_logo_path'",
                    extra={"path": str(p)},
                )
                return str(p)
            else:
                logger.warning(
                    "Logo path z SystemConfig istnieje w konfiguracji ale plik nie istnieje na dysku",
                    extra={"path": str(p), "config_key": "faktury.pdf_logo_path"},
                )
    except Exception as exc:
        logger.warning(
            "Błąd odczytu 'faktury.pdf_logo_path' z config_service",
            extra={"error": str(exc)},
        )

    # ── Próba 2: Fallback .env LOGO_PATH ─────────────────────────────────────
    try:
        settings   = get_settings()
        env_logo   = getattr(settings, "LOGO_PATH", None)
        if env_logo:
            p = Path(env_logo)
            if p.exists() and p.is_file():
                logger.debug(
                    "Logo załadowane z .env LOGO_PATH (fallback)",
                    extra={"path": str(p)},
                )
                return str(p)
    except Exception as exc:
        logger.warning(
            "Błąd odczytu LOGO_PATH z .env settings",
            extra={"error": str(exc)},
        )

    logger.info(
        "Logo niedostępne — PDF wygeneruje się bez logo (bez wpływu na funkcjonalność)",
    )
    return None


# =============================================================================
# Helpery formatowania — używane wewnątrz _build_pdf_sync (sync context)
# =============================================================================

def _fmt_date(value: Any) -> str:
    """
    Formatuje datę/datetime jako DD.MM.RRRR.
    Obsługuje: datetime, date, str (ISO), None.
    Zwraca '—' dla None lub nieparsowalne.
    """
    if value is None:
        return "—"
    if isinstance(value, str):
        if not value or value == "None":
            return "—"
        try:
            value = datetime.fromisoformat(value)
        except (ValueError, TypeError):
            return str(value)[:10]
    if isinstance(value, datetime):
        return value.strftime("%d.%m.%Y")
    if isinstance(value, date):
        return value.strftime("%d.%m.%Y")
    return str(value)


def _fmt_money(value: Any, suffix: str = " zł") -> str:
    """
    Formatuje kwotę finansową z 2 miejscami po przecinku (polska konwencja).
    Przykład: 1234567.89 → "1 234 567,89 zł"
    Zwraca "0,00 zł" dla None/błędu.
    """
    if value is None:
        return f"0,00{suffix}"
    try:
        f = float(value)
        # Formatowanie z separatorem tysięcy (spacja) i przecinkiem
        formatted = f"{f:,.2f}"                    # "1,234,567.89"
        formatted = formatted.replace(",", " ")    # "1 234 567.89"
        formatted = formatted.replace(".", ",")    # "1 234 567,89"
        return formatted + suffix
    except (TypeError, ValueError):
        return f"—{suffix}"


def _fmt_ilosc(value: Any) -> str:
    """
    Formatuje ilość — usuwa zbędne zera po przecinku.
    1.0 → "1", 1.5 → "1,5", 1.500 → "1,5"
    """
    if value is None:
        return "—"
    try:
        f = float(value)
        if f == int(f):
            return str(int(f))
        # Max 3 miejsca po przecinku, bez trailing zeros
        s = f"{f:.3f}".rstrip("0")
        return s.replace(".", ",")
    except (TypeError, ValueError):
        return str(value)


def _sanitize_str(value: Any, max_len: int = 500) -> str:
    """
    Sanityzuje string do wyświetlenia w PDF.
    Usuwa znaki kontrolne (z wyjątkiem newline/tab), obcina do max_len.
    Nigdy nie rzuca wyjątku.
    """
    if value is None:
        return ""
    s = str(value)
    # Usuń znaki kontrolne oprócz \n i \t
    s = "".join(ch for ch in s if ch in ("\n", "\t") or ord(ch) >= 32)
    return s[:max_len]


def _sha256_preview(text_value: Optional[str], chars: int = 8) -> str:
    """
    Zwraca pierwsze `chars` znaków SHA-256 stringa.
    Używane do wyświetlenia preview komentarza bez ujawniania treści.
    Np. komentarz "Faktura poprawna" → "#a3f7b2c1"
    """
    if not text_value:
        return ""
    digest = hashlib.sha256(text_value.encode("utf-8")).hexdigest()
    return f"#{digest[:chars]}"


def _priorytet_kolor(priorytet: str) -> str:
    return {
        "normalny":     _KOLOR_NORMALNY,
        "pilny":        _KOLOR_PILNY,
        "bardzo_pilny": _KOLOR_BARDZO_PILNY,
    }.get(priorytet, _KOLOR_NORMALNY)


def _status_kolor(status: str) -> str:
    return {
        "nowe":          _KOLOR_NOWE,
        "w_toku":        _KOLOR_W_TOKU,
        "zaakceptowana": _KOLOR_ZAAKCEPTOWANA,
        "anulowana":     _KOLOR_ANULOWANA,
        "orphaned":      _KOLOR_ANULOWANA,
    }.get(status, _KOLOR_NOWE)


def _status_label(status: str) -> str:
    return {
        "nowe":          "NOWE",
        "w_toku":        "W TOKU",
        "zaakceptowana": "ZAAKCEPTOWANA",
        "anulowana":     "ANULOWANA",
        "orphaned":      "ZAGINIONA (ORPHANED)",
    }.get(status, status.upper())


def _priorytet_label(priorytet: str) -> str:
    return {
        "normalny":     "NORMALNY",
        "pilny":        "PILNY ⚡",
        "bardzo_pilny": "BARDZO PILNY 🔴",
    }.get(priorytet, priorytet.upper())


def _decyzja_symbol(status: str) -> str:
    """Unicode symbol dla statusu przypisania (wyświetlany w historii)."""
    return {
        "zaakceptowane": "✓",
        "odrzucone":     "✗",
        "nie_moje":      "↩",
        "oczekuje":      "⏳",
    }.get(status, "?")


def _decyzja_label(status: str) -> str:
    return {
        "zaakceptowane": "Zaakceptowano",
        "odrzucone":     "Odrzucono",
        "nie_moje":      "Nie moja",
        "oczekuje":      "Oczekuje",
    }.get(status, status)


def _decyzja_kolor(status: str) -> str:
    return {
        "zaakceptowane": _KOLOR_ZAAKCEPTOWANA,
        "odrzucone":     _KOLOR_OVERDUE,
        "nie_moje":      _KOLOR_PILNY,
        "oczekuje":      _KOLOR_NORMALNY,
    }.get(status, _KOLOR_NORMALNY)


# =============================================================================
# Główna funkcja budowania PDF — synchroniczna (uruchamiana w thread executor)
# =============================================================================

def _build_pdf_sync(ctx: _PDFKontekst) -> bytes:
    """
    Buduje PDF z danych zgromadzonych w _PDFKontekst.

    ⚠️  SYNCHRONICZNA — musi być wywołana przez asyncio.run_in_executor().
        Nigdy nie wywołuj bezpośrednio z async context (zablokuje event loop).

    Struktura dokumentu (A4, marginesy 2cm):
        1. Nagłówek      — logo (opcjonalne) + firma + tytuł + KSeF ID
        2. Badges        — status faktury + priorytet (kolor-coded)
        3. WYSTAWCA      — kontrahent, e-mail, telefon
        4. WARTOŚCI      — Netto / VAT / Brutto / forma płatności / termin
                           + alert przeterminowania (czerwony)
        5. POZYCJE       — tabela NazwaTowaru | Ilość | Jm | Cena | Wartość | VAT
        6. OPIS I UWAGI  — tekst od referenta (opis formalny + uwagi wewnętrzne)
        7. HISTORIA      — ✓/✗/⏳/↩ + pracownik + decyzja + data + SHA256 komentarza
        8. STOPKA        — timestamp generowania + wersja systemu + ID faktury

    Returns:
        Bajty PDF (content PDF w pamięci).

    Raises:
        RuntimeError: Gdy ReportLab nie jest zainstalowany.
        Exception:    Przy błędach ReportLab (z pełnym logowaniem przez wywołującego).
    """
    _ensure_fonts_registered()

    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import cm, mm
        from reportlab.platypus import (
            HRFlowable,
            Image,
            Paragraph,
            SimpleDocTemplate,
            Spacer,
            Table,
            TableStyle,
        )
    except ImportError as exc:
        raise RuntimeError(
            "ReportLab nie jest zainstalowany. "
            "Dodaj 'reportlab' do requirements.txt i przebuduj kontener Docker."
        ) from exc

    # ── Style tekstu (wszystkie z fontem DejaVu) ───────────────────────────────
    base_styles = getSampleStyleSheet()

    def _s(name: str, parent: str = "Normal", **kwargs) -> ParagraphStyle:
        """Skrócony konstruktor stylu — domyślnie DejaVu, można nadpisać przez kwargs."""
        if "fontName" not in kwargs:
            kwargs["fontName"] = "DejaVu"
        return ParagraphStyle(
            name,
            parent = base_styles[parent],
            **kwargs,
        )

    S_NORMAL    = _s("pdf_n",    fontSize=9,  leading=13)
    S_BOLD      = _s("pdf_b",    fontSize=9,  leading=13, fontName="DejaVu-Bold")
    S_TYTUL     = _s("pdf_t",    "Title",
                      fontSize=14, leading=18, fontName="DejaVu-Bold",
                      textColor=colors.HexColor(_KOLOR_NAGLOWEK_BG), spaceAfter=4)
    S_SEKCJA    = _s("pdf_sec",  fontSize=9,  leading=11, fontName="DejaVu-Bold",
                      textColor=colors.HexColor(_KOLOR_NAGLOWEK_BG),
                      spaceBefore=8, spaceAfter=3)
    S_MALY      = _s("pdf_sm",   fontSize=7,  leading=10,
                      textColor=colors.grey)
    S_OVERDUE   = _s("pdf_ov",   fontSize=9,  leading=13, fontName="DejaVu-Bold",
                      textColor=colors.HexColor(_KOLOR_OVERDUE))
    S_FIRMA     = _s("pdf_firma", fontSize=11, leading=14, fontName="DejaVu-Bold",
                      textColor=colors.HexColor(_KOLOR_NAGLOWEK_BG))
    S_BRAK_DANYCH = _s("pdf_bd", fontSize=8, leading=12,
                        textColor=colors.HexColor("#888888"))

    # ── Rozmiar strony i bufor ─────────────────────────────────────────────────
    page_w, page_h = A4
    buffer = BytesIO()

    # Callback numeracji stron — wywoływany przez ReportLab na każdej stronie
    def _on_page(canvas_obj: Any, doc_obj: Any) -> None:
        canvas_obj.saveState()
        canvas_obj.setFont("DejaVu", 7)
        canvas_obj.setFillColor(colors.HexColor("#888888"))
        canvas_obj.drawRightString(
            page_w - 2 * cm,
            1.2 * cm,
            f"Strona {doc_obj.page}  |  System Windykacja  |  "
            f"Faktura #{ctx.faktura_id}  |  "
            f"{ctx.generated_at[:10]}",
        )
        canvas_obj.restoreState()

    doc = SimpleDocTemplate(
        buffer,
        pagesize     = A4,
        rightMargin  = 2 * cm,
        leftMargin   = 2 * cm,
        topMargin    = 2.2 * cm,
        bottomMargin = 2 * cm,
        title        = f"Karta Akceptacji Faktury — {ctx.numer_ksef}",
        author       = "System Windykacja",
        subject      = f"Faktura ID={ctx.faktura_id}",
    )

    story: list[Any] = []

    # ═══════════════════════════════════════════════════════════════════════════
    # 1. NAGŁÓWEK — logo + firma + tytuł + KSeF ID
    # ═══════════════════════════════════════════════════════════════════════════
    logo_element: Any = None
    if ctx.logo_path:
        try:
            logo_element = Image(ctx.logo_path, width=4.5 * cm, height=1.5 * cm)
            logo_element.hAlign = "LEFT"
        except Exception as logo_exc:
            logger.warning(
                "Nie można załadować logo do PDF",
                extra={"logo_path": ctx.logo_path, "error": str(logo_exc)},
            )
            logo_element = None

    # Prawa strona nagłówka: firma + tytuł + KSeF
    w = ctx.wapro or {}
    wapro_numer = _sanitize_str(w.get("numer") or "", 60)

    prawa_naglowka: list[Any] = [
        Paragraph(_sanitize_str(ctx.firma_nazwa, 100), S_FIRMA),
        Spacer(1, 2 * mm),
        Paragraph("KARTA AKCEPTACJI FAKTURY", S_TYTUL),
        Paragraph(
            f'<font name="DejaVu-Bold">Nr KSeF:</font> '
            f'{_sanitize_str(ctx.numer_ksef, 100)}',
            S_NORMAL,
        ),
    ]
    if wapro_numer:
        prawa_naglowka.append(Paragraph(
            f'<font name="DejaVu-Bold">Nr dok. WAPRO:</font> {wapro_numer}',
            S_NORMAL,
        ))
    if ctx.firma_nip:
        prawa_naglowka.append(Paragraph(
            f'<font name="DejaVu-Bold">NIP:</font> '
            f'{_sanitize_str(ctx.firma_nip, 20)}',
            S_MALY,
        ))

    if logo_element:
        # Tabela 2-kolumnowa: logo | tekst
        naglowek_tab = Table(
            [[logo_element, prawa_naglowka]],
            colWidths=[5 * cm, None],
        )
        naglowek_tab.setStyle(TableStyle([
            ("VALIGN",       (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING",  (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("TOPPADDING",   (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 0),
        ]))
        story.append(naglowek_tab)
    else:
        for elem in prawa_naglowka:
            story.append(elem)

    story.append(Spacer(1, 3 * mm))

    # ═══════════════════════════════════════════════════════════════════════════
    # 2. BADGES — status faktury + priorytet (kolor-coded inline tabele)
    # ═══════════════════════════════════════════════════════════════════════════
    sk = _status_kolor(ctx.status_wewnetrzny)
    sl = _status_label(ctx.status_wewnetrzny)
    pk = _priorytet_kolor(ctx.priorytet)
    pl = _priorytet_label(ctx.priorytet)

    def _badge_table(label: str, hex_color: str, width: float) -> Table:
        """Tworzy kolorowy 'badge' z białym tekstem."""
        t = Table(
            [[Paragraph(
                f'<font name="DejaVu-Bold" color="white"> {label} </font>',
                _s(f"badge_{label[:4]}", fontSize=8, leading=11,
                   fontName="DejaVu-Bold"),
            )]],
            colWidths=[width * cm],
        )
        t.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), colors.HexColor(hex_color)),
            ("LEFTPADDING",   (0, 0), (-1, -1), 5),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 5),
            ("TOPPADDING",    (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        return t

    badges_row = Table(
        [[
            _badge_table(sl, sk, 4.0),
            Spacer(3 * mm, 1),
            _badge_table(f"PRIORYTET: {pl}", pk, 5.5),
        ]],
        colWidths=[4.3 * cm, 0.4 * cm, 6.0 * cm],
    )
    badges_row.setStyle(TableStyle([
        ("LEFTPADDING",   (0, 0), (-1, -1), 0),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
        ("TOPPADDING",    (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(badges_row)

    if ctx.created_at:
        story.append(Spacer(1, 2 * mm))
        story.append(Paragraph(
            f'<font name="DejaVu-Bold">Data wpuszczenia do obiegu:</font> '
            f'{_fmt_date(ctx.created_at)}',
            S_NORMAL,
        ))

    story.append(HRFlowable(
        width="100%", thickness=1.5,
        color=colors.HexColor(_KOLOR_NAGLOWEK_BG),
        spaceBefore=5, spaceAfter=5,
    ))

    # ═══════════════════════════════════════════════════════════════════════════
    # 3. SEKCJA WYSTAWCA
    # ═══════════════════════════════════════════════════════════════════════════
    story.append(Paragraph("WYSTAWCA", S_SEKCJA))

    wystawca_rows = [
        [
            Paragraph('<font name="DejaVu-Bold">Kontrahent:</font>', S_NORMAL),
            Paragraph(
                _sanitize_str(w.get("nazwa_kontrahenta") or "— brak danych —", 200),
                S_BOLD if w.get("nazwa_kontrahenta") else S_BRAK_DANYCH,
            ),
        ],
    ]
    if w.get("email_kontrahenta"):
        wystawca_rows.append([
            Paragraph('<font name="DejaVu-Bold">E-mail:</font>', S_NORMAL),
            Paragraph(_sanitize_str(w["email_kontrahenta"], 100), S_NORMAL),
        ])
    if w.get("telefon_kontrahenta"):
        wystawca_rows.append([
            Paragraph('<font name="DejaVu-Bold">Telefon:</font>', S_NORMAL),
            Paragraph(_sanitize_str(w["telefon_kontrahenta"], 50), S_NORMAL),
        ])

    tab_wystawca = Table(wystawca_rows, colWidths=[3.8 * cm, None])
    tab_wystawca.setStyle(TableStyle([
        ("VALIGN",       (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING",  (0, 0), (0, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING",   (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 2),
    ]))
    story.append(tab_wystawca)
    story.append(HRFlowable(
        width="100%", thickness=0.5,
        color=colors.HexColor("#DDDDDD"),
        spaceBefore=4, spaceAfter=4,
    ))

    # ═══════════════════════════════════════════════════════════════════════════
    # 4. SEKCJA WARTOŚCI FAKTURY
    # ═══════════════════════════════════════════════════════════════════════════
    story.append(Paragraph("WARTOŚCI FAKTURY", S_SEKCJA))

    netto  = w.get("wartosc_netto")
    vat    = w.get("kwota_vat")
    brutto = w.get("wartosc_brutto")
    forma  = _sanitize_str(w.get("forma_platnosci") or "—", 80)
    termin = w.get("termin_platnosci")

    # Oblicz is_overdue
    is_overdue = False
    if termin:
        try:
            if isinstance(termin, str) and termin not in ("None", ""):
                termin_dt = datetime.fromisoformat(termin)
            elif isinstance(termin, datetime):
                termin_dt = termin
            else:
                termin_dt = None
            if termin_dt:
                termin_date = termin_dt.date() if isinstance(termin_dt, datetime) else termin_dt
                is_overdue  = termin_date < date.today()
        except Exception:
            pass

    termin_str = _fmt_date(termin)
    if is_overdue:
        termin_str_wyswietlany = f'<font color="{_KOLOR_OVERDUE}"><b>{termin_str} ⚠</b></font>'
        termin_styl = S_OVERDUE
    else:
        termin_str_wyswietlany = termin_str
        termin_styl = S_NORMAL

    wartosci_data = [
        [
            Paragraph('<font name="DejaVu-Bold">Wartość netto:</font>',   S_NORMAL),
            Paragraph(_fmt_money(netto),                                   S_NORMAL),
            Paragraph('<font name="DejaVu-Bold">Kwota VAT:</font>',        S_NORMAL),
            Paragraph(_fmt_money(vat),                                     S_NORMAL),
        ],
        [
            Paragraph('<font name="DejaVu-Bold">Wartość brutto:</font>',  S_NORMAL),
            Paragraph(_fmt_money(brutto),                                  S_BOLD),
            Paragraph('<font name="DejaVu-Bold">Forma płatności:</font>',  S_NORMAL),
            Paragraph(forma,                                               S_NORMAL),
        ],
        [
            Paragraph('<font name="DejaVu-Bold">Termin płatności:</font>', S_NORMAL),
            Paragraph(termin_str_wyswietlany,                              termin_styl),
            Paragraph("",                                                  S_NORMAL),
            Paragraph("",                                                  S_NORMAL),
        ],
    ]

    tab_wartosci = Table(
        wartosci_data,
        colWidths=[4 * cm, 4 * cm, 4 * cm, None],
    )
    tab_wartosci.setStyle(TableStyle([
        ("VALIGN",       (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING",  (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING",   (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 2),
        # Parzysty wiersz (index 1 = brutto/forma) — lekkie tło
        ("BACKGROUND",   (0, 1), (-1, 1), colors.HexColor(_KOLOR_WIERSZ_PAIR)),
    ]))
    story.append(tab_wartosci)

    if is_overdue:
        story.append(Spacer(1, 2 * mm))
        story.append(Paragraph(
            f"⚠  UWAGA: Faktura PRZETERMINOWANA — "
            f"termin płatności ({termin_str}) minął.",
            S_OVERDUE,
        ))

    if not ctx.wapro:
        story.append(Spacer(1, 2 * mm))
        story.append(Paragraph(
            "ℹ️  Dane finansowe niedostępne — faktura nie jest już widoczna w systemie WAPRO.",
            S_BRAK_DANYCH,
        ))

    story.append(HRFlowable(
        width="100%", thickness=0.5,
        color=colors.HexColor("#DDDDDD"),
        spaceBefore=4, spaceAfter=4,
    ))

    # ═══════════════════════════════════════════════════════════════════════════
    # 5. TABELA POZYCJI FAKTURY
    # ═══════════════════════════════════════════════════════════════════════════
    story.append(Paragraph("POZYCJE FAKTURY", S_SEKCJA))

    if ctx.pozycje:
        # Nagłówek tabeli (biały tekst na granatowym tle)
        def _th(tekst: str) -> Paragraph:
            return Paragraph(
                f'<font color="white"><b>{tekst}</b></font>',
                _s(f"th_{tekst[:3]}", fontSize=8, leading=10, fontName="DejaVu-Bold"),
            )

        pozycje_wiersze = [[
            _th("Lp."),
            _th("Nazwa towaru / usługi"),
            _th("Ilość"),
            _th("Jm"),
            _th("Cena netto"),
            _th("Wart. netto"),
            _th("VAT"),
        ]]

        for i, poz in enumerate(ctx.pozycje):
            def _td(v: str, idx: int = i) -> Paragraph:
                return Paragraph(v, _s(f"td_{idx}_{v[:3]}", fontSize=8, leading=10))

            pozycje_wiersze.append([
                _td(str(i + 1)),
                _td(_sanitize_str(poz.get("nazwa_towaru", ""), 200)),
                _td(_fmt_ilosc(poz.get("ilosc"))),
                _td(_sanitize_str(poz.get("jednostka", ""), 10)),
                _td(_fmt_money(poz.get("cena_netto"), "")),
                _td(_fmt_money(poz.get("wartosc_netto"), "")),
                _td(_sanitize_str(poz.get("stawka_vat", ""), 10)),
            ])

        tab_pozycje = Table(
            pozycje_wiersze,
            colWidths=[
                0.8 * cm,   # Lp
                None,       # Nazwa (rozciągnięta)
                1.6 * cm,   # Ilość
                1.2 * cm,   # Jm
                2.6 * cm,   # Cena netto
                2.6 * cm,   # Wartość netto
                1.3 * cm,   # VAT
            ],
            repeatRows=1,   # nagłówek powtarzany na każdej stronie
        )

        pozycje_style = [
            ("BACKGROUND",    (0, 0), (-1, 0), colors.HexColor(_KOLOR_NAGLOWEK_BG)),
            ("TEXTCOLOR",     (0, 0), (-1, 0), colors.white),
            ("ALIGN",         (0, 0), (-1, -1), "LEFT"),
            ("ALIGN",         (2, 1), (2, -1), "RIGHT"),    # Ilość — prawy
            ("ALIGN",         (4, 1), (5, -1), "RIGHT"),    # Kwoty — prawy
            ("VALIGN",        (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING",    (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("LEFTPADDING",   (0, 0), (-1, -1), 4),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
            ("GRID",          (0, 0), (-1, -1), 0.3,
                               colors.HexColor("#CCCCCC")),
        ]
        # Alternujące tło wierszy (nieparzyste indeksy danych = parzyste wizualnie)
        for i in range(len(ctx.pozycje)):
            if i % 2 == 1:
                pozycje_style.append(
                    ("BACKGROUND", (0, i + 1), (-1, i + 1),
                     colors.HexColor(_KOLOR_WIERSZ_PAIR))
                )
        tab_pozycje.setStyle(TableStyle(pozycje_style))
        story.append(tab_pozycje)
    else:
        story.append(Paragraph(
            "Brak pozycji — dane pozycji z WAPRO niedostępne "
            "lub faktura nie posiada pozycji w systemie ERP.",
            S_BRAK_DANYCH,
        ))

    story.append(HRFlowable(
        width="100%", thickness=0.5,
        color=colors.HexColor("#DDDDDD"),
        spaceBefore=4, spaceAfter=4,
    ))

    # ═══════════════════════════════════════════════════════════════════════════
    # 6. OPIS I UWAGI REFERENTA
    # ═══════════════════════════════════════════════════════════════════════════
    has_text = ctx.opis_dokumentu or ctx.uwagi_referenta
    if has_text:
        story.append(Paragraph("INFORMACJE OD REFERENTA", S_SEKCJA))

        if ctx.opis_dokumentu:
            story.append(Paragraph(
                '<font name="DejaVu-Bold">Opis formalny dokumentu:</font>',
                S_NORMAL,
            ))
            story.append(Paragraph(
                _sanitize_str(ctx.opis_dokumentu, 2000),
                _s("opis_text", fontSize=9, leading=13, leftIndent=0.5 * cm),
            ))
            story.append(Spacer(1, 2 * mm))

        if ctx.uwagi_referenta:
            story.append(Paragraph(
                '<font name="DejaVu-Bold">Uwagi wewnętrzne referenta:</font>',
                S_NORMAL,
            ))
            story.append(Paragraph(
                _sanitize_str(ctx.uwagi_referenta, 2000),
                _s("uwagi_text", fontSize=9, leading=13, leftIndent=0.5 * cm,
                   textColor=colors.HexColor("#444444")),
            ))

        story.append(HRFlowable(
            width="100%", thickness=0.5,
            color=colors.HexColor("#DDDDDD"),
            spaceBefore=4, spaceAfter=4,
        ))

    # ═══════════════════════════════════════════════════════════════════════════
    # 7. HISTORIA AKCEPTACJI
    # ═══════════════════════════════════════════════════════════════════════════
    story.append(Paragraph("HISTORIA AKCEPTACJI", S_SEKCJA))

    if ctx.przypisania:
        def _th_h(tekst: str) -> Paragraph:
            return Paragraph(
                f'<font color="white"><b>{tekst}</b></font>',
                _s(f"thh_{tekst[:4]}", fontSize=8, leading=10, fontName="DejaVu-Bold"),
            )

        historia_wiersze = [[
            _th_h(""),
            _th_h("Pracownik"),
            _th_h("Decyzja"),
            _th_h("Data decyzji"),
            _th_h("Komentarz (SHA-256)"),
        ]]

        for i, p in enumerate(ctx.przypisania):
            status_p   = p.get("status", "oczekuje")
            symbol     = _decyzja_symbol(status_p)
            decyzja    = _decyzja_label(status_p)
            full_name  = _sanitize_str(p.get("full_name") or f"User #{p.get('user_id', '?')}", 80)
            decided_at = _fmt_date(p.get("decided_at"))
            komentarz  = p.get("komentarz")
            sha_prev   = _sha256_preview(komentarz) if komentarz else "—"
            sym_kolor  = _decyzja_kolor(status_p)

            # Nieaktywne przypisania (is_active=False) — szarszy tekst
            is_active  = p.get("is_active", True)
            row_alpha  = "" if is_active else ' color="#999999"'

            wiersz = [
                Paragraph(
                    f'<font name="DejaVu-Bold" color="{sym_kolor}" size="11">'
                    f'{symbol}</font>',
                    _s(f"sym_{i}", fontSize=11, leading=13, fontName="DejaVu-Bold"),
                ),
                Paragraph(
                    f'<font{row_alpha}>{full_name}</font>'
                    + ('' if is_active else ' <font color="#999999" size="7">(reset)</font>'),
                    _s(f"hn_{i}", fontSize=8, leading=10),
                ),
                Paragraph(
                    f'<font{row_alpha}>{decyzja}</font>',
                    _s(f"hd_{i}", fontSize=8, leading=10),
                ),
                Paragraph(
                    f'<font{row_alpha}>{decided_at}</font>',
                    _s(f"hdt_{i}", fontSize=8, leading=10),
                ),
                Paragraph(
                    sha_prev,
                    _s(f"hsha_{i}", fontSize=7, leading=9,
                       textColor=colors.HexColor("#777777")),
                ),
            ]
            historia_wiersze.append(wiersz)

        tab_historia = Table(
            historia_wiersze,
            colWidths=[1.0 * cm, None, 3.2 * cm, 2.8 * cm, 2.4 * cm],
            repeatRows=1,
        )

        historia_style = [
            ("BACKGROUND",    (0, 0), (-1, 0), colors.HexColor(_KOLOR_NAGLOWEK_BG)),
            ("TEXTCOLOR",     (0, 0), (-1, 0), colors.white),
            ("ALIGN",         (0, 0), (-1, -1), "LEFT"),
            ("ALIGN",         (0, 1), (0, -1), "CENTER"),   # symbol wyśrodkowany
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING",    (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("LEFTPADDING",   (0, 0), (-1, -1), 4),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
            ("GRID",          (0, 0), (-1, -1), 0.3,
                               colors.HexColor("#CCCCCC")),
        ]
        for i in range(len(ctx.przypisania)):
            if i % 2 == 1:
                historia_style.append(
                    ("BACKGROUND", (0, i + 1), (-1, i + 1),
                     colors.HexColor(_KOLOR_WIERSZ_PAIR))
                )
        tab_historia.setStyle(TableStyle(historia_style))
        story.append(tab_historia)

        # Legenda symboli
        story.append(Spacer(1, 2 * mm))
        story.append(Paragraph(
            "Legenda: ✓ Zaakceptowano  |  ✗ Odrzucono  |  "
            "↩ Nie moja  |  ⏳ Oczekuje",
            S_MALY,
        ))
        story.append(Paragraph(
            "SHA-256: skrócony hash komentarza pracownika (privacy by design — "
            "pełna treść w systemie).",
            S_MALY,
        ))
    else:
        story.append(Paragraph(
            "Brak przypisań dla tej faktury.",
            S_BRAK_DANYCH,
        ))

    # ═══════════════════════════════════════════════════════════════════════════
    # 8. STOPKA
    # ═══════════════════════════════════════════════════════════════════════════
    story.append(HRFlowable(
        width="100%", thickness=1.2,
        color=colors.HexColor(_KOLOR_NAGLOWEK_BG),
        spaceBefore=8, spaceAfter=4,
    ))

    generated_local = ctx.generated_at[:19].replace("T", " ")
    stopka_row = Table(
        [[
            Paragraph(
                f'Wygenerowano: {generated_local}',
                S_MALY,
            ),
            Paragraph(
                f'System Windykacja v{_APP_VERSION}  |  Faktura ID: #{ctx.faktura_id}',
                _s("stopka_r", fontSize=7, leading=9,
                   textColor=colors.grey, alignment=2),
            ),
        ]],
        colWidths=[None, 8 * cm],
    )
    stopka_row.setStyle(TableStyle([
        ("LEFTPADDING",  (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING",   (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 0),
        ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(stopka_row)

    # ── Budowanie dokumentu ────────────────────────────────────────────────────
    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)
    return buffer.getvalue()


# =============================================================================
# Publiczne API serwisu
# =============================================================================

async def generate_pdf(
    *,
    faktura:      FakturaAkceptacja,
    wapro:        Optional[WaproFakturaNaglowek],
    przypisania:  list[FakturaPrzypisanie],
    db:           AsyncSession,
    redis:        Redis,
) -> bytes:
    """
    Generuje PDF Karty Akceptacji Faktury (in-memory, bez zapisu na dysk).

    Przepływ:
        1. Pobierz pozycje WAPRO (async)
        2. Pobierz nazwy pracowników — batch SQL (async)
        3. Pobierz ścieżkę logo — SystemConfig → .env (async)
        4. Zbuduj _PDFKontekst (plain-data, serializowalny)
        5. Uruchom _build_pdf_sync() w thread executor (nie blokuje event loop)
        6. Zaloguj metryki (rozmiar KB, czas ms, liczba pozycji)

    Args:
        faktura:     ORM FakturaAkceptacja (dane z naszej tabeli)
        wapro:       Dane nagłówka z widoku WAPRO lub None (faktura orphaned)
        przypisania: Lista przypisań (aktywnych i historycznych) z skw_faktura_przypisanie
        db:          AsyncSession SQLAlchemy
        redis:       Klient Redis (do config_service dla logo path + TTL)

    Returns:
        Bajty PDF gotowe do StreamingResponse.

    Raises:
        RuntimeError:  ReportLab nie jest zainstalowany.
        Exception:     Błąd generowania — logowany z exc_info=True.

    Przykład użycia (faktura_akceptacja_service.py):
        from app.services.faktura_pdf_service import generate_pdf
        pdf_bytes = await generate_pdf(
            faktura=faktura, wapro=wapro,
            przypisania=przypisania, db=db, redis=redis,
        )
    """
    t_start  = _time.monotonic()
    settings = get_settings()

    logger.info(
        orjson.dumps({
            "event":             "pdf_generation_start",
            "faktura_id":        faktura.id,
            "numer_ksef":        faktura.numer_ksef,
            "status":            faktura.status_wewnetrzny,
            "priorytet":         faktura.priorytet,
            "przypisania_count": len(przypisania),
            "wapro_dostepne":    wapro is not None,
        }).decode()
    )

    # ── Krok 1: Pozycje WAPRO ─────────────────────────────────────────────────
    pozycje = await _fetch_pozycje(faktura.numer_ksef)

    # ── Krok 2: Nazwy pracowników (batch) ─────────────────────────────────────
    user_ids  = [p.user_id for p in przypisania if p.user_id]
    users_map = await _fetch_user_names(db, user_ids)

    # Serializacja przypisań do plain dict (bezpieczne przez granicę executor)
    przypisania_dicts: list[dict[str, Any]] = []
    for p in przypisania:
        przypisania_dicts.append({
            "user_id":    p.user_id,
            "full_name":  users_map.get(p.user_id or 0, f"User #{p.user_id}"),
            "status":     p.status,
            "komentarz":  p.komentarz,       # pełna treść — SHA256 liczymy w sync
            "decided_at": (
                p.decided_at.isoformat()
                if p.decided_at else None
            ),
            "is_active":  bool(p.is_active),
        })

    # ── Krok 3: Logo path ─────────────────────────────────────────────────────
    logo_path = await _fetch_logo_path(redis, db)

    # ── Krok 4: Buduj kontekst ────────────────────────────────────────────────
    ctx = _PDFKontekst(
        faktura_id        = faktura.id,
        numer_ksef        = faktura.numer_ksef,
        status_wewnetrzny = faktura.status_wewnetrzny,
        priorytet         = faktura.priorytet,
        opis_dokumentu    = faktura.opis_dokumentu,
        uwagi_referenta   = faktura.uwagi,
        created_at        = (
            faktura.CreatedAt.isoformat()
            if getattr(faktura, "CreatedAt", None) else
            faktura.created_at.isoformat()
            if getattr(faktura, "created_at", None) else None
        ),
        is_orphaned       = (faktura.status_wewnetrzny == "orphaned"),
        wapro             = (
            wapro.model_dump(mode="json")
            if wapro else None
        ),
        pozycje           = pozycje,
        przypisania       = przypisania_dicts,
        logo_path         = logo_path,
        firma_nazwa       = getattr(settings, "COMPANY_NAME", "GPGK Jasło"),
        firma_nip         = getattr(settings, "COMPANY_NIP", ""),
        generated_at      = datetime.now(timezone.utc).isoformat(),
        request_context   = {
            "faktura_id": faktura.id,
            "numer_ksef": faktura.numer_ksef,
        },
    )

    # ── Krok 5: Generowanie PDF w thread executor ─────────────────────────────
    # ReportLab jest synchroniczny i CPU-bound.
    # run_in_executor() przenosi go do thread pool, nie blokuje event loop.
    loop = asyncio.get_event_loop()
    try:
        pdf_bytes: bytes = await loop.run_in_executor(None, _build_pdf_sync, ctx)
    except Exception as exc:
        duration_ms = round((_time.monotonic() - t_start) * 1000, 1)
        logger.error(
            orjson.dumps({
                "event":       "pdf_generation_failed",
                "faktura_id":  faktura.id,
                "numer_ksef":  faktura.numer_ksef,
                "error":       str(exc),
                "error_type":  type(exc).__name__,
                "duration_ms": duration_ms,
            }).decode(),
            exc_info=True,
        )
        raise

    # ── Krok 6: Metryki ───────────────────────────────────────────────────────
    duration_ms = round((_time.monotonic() - t_start) * 1000, 1)
    pdf_size_kb = round(len(pdf_bytes) / 1024, 1)

    logger.info(
        orjson.dumps({
            "event":             "pdf_generation_success",
            "faktura_id":        faktura.id,
            "numer_ksef":        faktura.numer_ksef,
            "pdf_size_kb":       pdf_size_kb,
            "pozycje_count":     len(pozycje),
            "przypisania_count": len(przypisania),
            "logo_found":        logo_path is not None,
            "wapro_dostepne":    wapro is not None,
            "is_orphaned":       ctx.is_orphaned,
            "duration_ms":       duration_ms,
        }).decode()
    )

    return pdf_bytes