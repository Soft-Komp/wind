# backend/app/services/approval_history_pdf_service.py
"""
Serwis generowania PDF historii obiegu dokumentu (moduł Obiegu Dokumentów — Sprint 3).

Generuje dokument A4 zawierający:
  1. Nagłówek  — logo (opcjonalne) + firma + tytuł dokumentu + ID instancji
  2. Metadane  — status, pilność, dyspozytor, daty, ścieżka akceptacji
  3. Historia  — pełna tabela z approval_log (ALL rows, łącznie z is_voided=1)
  4. Stopka    — timestamp UTC + wersja systemu + ID instancji + użytkownik żądający

Wzorzec architektoniczny zgodny z faktura_pdf_service.py:
  - Dane zbierane async (DB + Redis) PRZED wejściem w executor
  - _build_pdf_sync() uruchamiana w run_in_executor() — nie blokuje event loop
  - Dane przekazywane przez @dataclass _PDFKontekst (plain types, thread-safe)
  - Cache Redis z TTL z SystemConfig (klucz faktury.pdf_cache_ttl_seconds)
  - Thread-safe rejestracja fontów DejaVu (double-checked locking)
  - Logowanie JSONL: start, cache-hit, cache-miss, sukces (rozmiar KB, czas ms),
    błąd (z exc_info=True)

Kolory:
  Niebieskie nagłówki tabel (#1A365D), zielony "zaakceptowany" (#27AE60),
  czerwony "odrzucony/anulowany" (#C0392B), szary "unieważniony" (#AAAAAA),
  pomarańczowy "w toku" (#E67E22), jasno-szare tło wierszy voided (#F5F5F5).

Uwaga: from __future__ import annotations NIGDY w tym pliku (importy w funkcjach
przez granicę executora wymagają działającego type resolver w runtime).
"""

import asyncio
import hashlib
import json
import logging
import threading
import time as _time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

import orjson
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.services.config_service import get_config_value

logger = logging.getLogger("app.services.approval_history_pdf")

# =============================================================================
# Stałe
# =============================================================================
_SCHEMA      = "dbo"
_APP_VERSION = "1.0"
_DEJAVU_DIR  = "/usr/share/fonts/truetype/dejavu"

# Paleta kolorów — spójna z faktura_pdf_service.py
_C_HEADER_BG    = "#1A365D"   # ciemny granat — tło nagłówków tabel
_C_HEADER_FG    = "#FFFFFF"   # biały — tekst nagłówków tabel
_C_APPROVED     = "#27AE60"   # zielony — akcja approved / zaakceptowano
_C_REJECTED     = "#C0392B"   # czerwony — rejected / cancelled / rollback
_C_IN_PROGRESS  = "#E67E22"   # pomarańczowy — dispatched / in_progress
_C_NEUTRAL      = "#2C3E50"   # ciemny — akcje neutralne
_C_VOIDED_BG    = "#F5F5F5"   # bardzo jasny szary — tło wierszy unieważnionych
_C_VOIDED_TEXT  = "#AAAAAA"   # szary — tekst wierszy unieważnionych
_C_ROW_EVEN     = "#F0F4FA"   # bardzo jasny niebieski — parzyste wiersze (nieV.)
_C_SECTION      = "#1A365D"   # kolor nagłówków sekcji

# Mapa akcja → polska nazwa (lustro z instances.py — utrzymujemy spójność)
_ACTION_DISPLAY: dict[str, str] = {
    "dispatched":         "Przekazano do obiegu",
    "accepted":           "Zaakceptowano",
    "rejected":           "Odrzucono",
    "rollback":           "Cofnięto obieg",
    "approved":           "Obieg zakończony — zaakceptowany",
    "cancelled":          "Anulowano obieg",
    "forwarded":          "Przekazano odpowiedzialność",
    "send_to_group":      "Wstawiono grupę do weryfikacji",
    "step_advanced":      "Przejście do kolejnego etapu",
    "marked_urgent":      "Oznaczono jako pilny",
    "unmarked_urgent":    "Usunięto oznaczenie pilny",
    "deadline_expired":   "Termin przekroczony",
    "deadline_warning":   "Ostrzeżenie o terminie",
    "deadline_escalated": "Eskalacja terminu",
    "path_modified":      "Zmodyfikowano ścieżkę",
    "reassigned":         "Zmieniono przypisanie",
}

# Mapa akcja → kolor tekstu w PDF
_ACTION_COLOR: dict[str, str] = {
    "dispatched":         _C_IN_PROGRESS,
    "accepted":           _C_APPROVED,
    "approved":           _C_APPROVED,
    "rejected":           _C_REJECTED,
    "rollback":           _C_REJECTED,
    "cancelled":          _C_REJECTED,
    "forwarded":          _C_IN_PROGRESS,
    "send_to_group":      _C_IN_PROGRESS,
    "step_advanced":      _C_APPROVED,
    "marked_urgent":      _C_REJECTED,
    "unmarked_urgent":    _C_NEUTRAL,
    "deadline_expired":   _C_REJECTED,
    "deadline_warning":   _C_IN_PROGRESS,
    "deadline_escalated": _C_REJECTED,
    "path_modified":      _C_NEUTRAL,
    "reassigned":         _C_NEUTRAL,
}

# Mapa status instancji → polska nazwa
_STATUS_DISPLAY: dict[str, str] = {
    "pending_dispatch": "Oczekuje na przydzielenie",
    "in_progress":      "W toku",
    "approved":         "Zaakceptowany",
    "cancelled":        "Anulowany",
    "rejected":         "Odrzucony",
}

# Mapa status instancji → kolor
_STATUS_COLOR: dict[str, str] = {
    "pending_dispatch": _C_IN_PROGRESS,
    "in_progress":      _C_IN_PROGRESS,
    "approved":         _C_APPROVED,
    "cancelled":        _C_REJECTED,
    "rejected":         _C_REJECTED,
}

# =============================================================================
# Thread-safe rejestracja fontów DejaVu (double-checked locking)
# =============================================================================
_fonts_registered: bool            = False
_fonts_lock:       threading.Lock  = threading.Lock()


def _ensure_fonts_registered() -> None:
    """
    Rejestruje fonty DejaVu Sans w ReportLab pdfmetrics.
    Idempotentna — bezpieczna przy wielokrotnym wywołaniu z wielu wątków.
    Graceful degradation: brak pliku fontu → log WARN, PDF nadal generowany
    (polskie znaki mogą nie być wyświetlane poprawnie).
    """
    global _fonts_registered
    if _fonts_registered:
        return
    with _fonts_lock:
        if _fonts_registered:
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
                orjson.dumps({
                    "event":      "fonts_registered",
                    "dejavu_dir": _DEJAVU_DIR,
                }).decode()
            )
        except Exception as exc:
            logger.warning(
                orjson.dumps({
                    "event":  "fonts_register_failed",
                    "error":  str(exc),
                    "impact": "polskie_znaki_moga_nie_dzialac",
                }).decode()
            )
            _fonts_registered = True   # blokuje ponawianie przy każdym PDF


# =============================================================================
# Dataclass kontekstu — przekazywana przez granicę async → thread executor
# Wszystkie pola: typy proste (str, int, float, dict, list, None).
# Żadnych ORM-owych obiektów, żadnych coroutine — bezpieczne dla threading.
# =============================================================================
@dataclass
class _PDFKontekst:
    """
    Serializowalny kontener danych dla _build_pdf_sync().

    Zasada: po zbudowaniu _PDFKontekst żaden async I/O nie jest potrzebny.
    Cały dostęp do DB i Redis wykonywany PRZED budowaniem kontekstu,
    wyłącznie w funkcji generate_approval_history_pdf().
    """
    # Identyfikator instancji obiegu
    id_instance:     int

    # Dane nagłówka dokumentu
    document_title:  str
    document_amount: Optional[float]    # None jeśli nie znany
    id_source:       int
    source_name:     str                # np. "Faktury KSeF"

    # Status i metadane instancji
    status:          str
    is_urgent:       bool
    id_path:         Optional[int]
    path_name:       Optional[str]      # nazwa ścieżki z approval_paths
    current_step:    int

    # Dyspozytor
    dispatched_by_username: Optional[str]
    dispatched_by_fullname: Optional[str]
    dispatched_at:           Optional[str]  # ISO string lub None
    completed_at:            Optional[str]  # ISO string lub None
    deadline_at:             Optional[str]  # ISO string lub None

    # Wpisy logu — lista plain dict, każdy dict to jeden wiersz approval_log
    # Klucze: id_log, username_snapshot, full_name, action, action_display,
    #         step_order_snapshot, id_group_snapshot, group_name,
    #         votes_before, votes_after, is_voided, comment, details, logged_at_str
    log_entries: list[dict[str, Any]] = field(default_factory=list)

    # Metadane generowania
    logo_path:      Optional[str]   = None
    firma_nazwa:    str             = "System Windykacja"
    firma_nip:      str             = ""
    generated_at:   str             = ""
    requested_by:   str             = "system"   # username osoby żądającej PDF

    # Flagi diagnostyczne — logowane przy generowaniu
    total_log_entries: int          = 0
    voided_entries:    int          = 0


# =============================================================================
# Helpery formatowania — używane wyłącznie w sync context (_build_pdf_sync)
# =============================================================================

def _fmt_dt(value: Any, include_time: bool = True) -> str:
    """
    Formatuje datę/datetime/ISO-str do czytelnego formatu polskiego.
    DD.MM.RRRR HH:MM lub DD.MM.RRRR (include_time=False).
    Zwraca '—' dla None lub wartości nieparsowalne.
    """
    if value is None:
        return "—"
    try:
        if isinstance(value, str):
            # Obsługa obu formatów: z 'T' i ze spacją, z/bez microseconds, z/bez Z
            value = value.replace("Z", "+00:00")
            dt = datetime.fromisoformat(value)
        elif isinstance(value, datetime):
            dt = value
        else:
            return str(value)
        if include_time:
            return dt.strftime("%d.%m.%Y %H:%M")
        return dt.strftime("%d.%m.%Y")
    except Exception:
        return str(value)[:19] if value else "—"


def _sanitize(text_val: Any, max_len: int = 500) -> str:
    """
    Sanityzuje wartość do bezpiecznego tekstu w PDF.
    Usuwa znaki NULL, obcina do max_len, zwraca '[BRAK]' dla None/pustych.
    Zastępuje '<', '>' encjami HTML (ReportLab parsuje XML w Paragraph).
    """
    if text_val is None:
        return "[BRAK]"
    s = str(text_val).replace("\x00", "").strip()
    if not s:
        return "[BRAK]"
    s = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    if len(s) > max_len:
        s = s[:max_len - 3] + "..."
    return s


def _votes_label(before: Any, after: Any) -> str:
    """Formatuje głosy jako 'X→Y' lub '' jeśli brak danych."""
    if before is None and after is None:
        return ""
    b = int(before) if before is not None else "?"
    a = int(after)  if after  is not None else "?"
    return f"głosy: {b}→{a}"


def _extract_comment_from_details(
    comment_direct: Optional[str],
    details: Optional[dict],
) -> Optional[str]:
    """
    Zwraca komentarz: najpierw z pola comment (approval_log.comment),
    fallback z details JSON (klucze: comment, reason, note).
    None jeśli brak.
    """
    if comment_direct and comment_direct.strip():
        return comment_direct.strip()
    if details and isinstance(details, dict):
        for key in ("comment", "reason", "note", "uwaga"):
            val = details.get(key)
            if val and str(val).strip():
                return str(val).strip()
    return None


# =============================================================================
# Główna funkcja sync (ReportLab) — wywoływana przez run_in_executor
# =============================================================================

def _build_pdf_sync(ctx: _PDFKontekst) -> bytes:
    """
    Buduje PDF historii obiegu dokumentu.

    ⚠️  SYNCHRONICZNA — musi być wywołana przez asyncio.run_in_executor().
    Nigdy nie wywołuj bezpośrednio z async context.

    Struktura dokumentu (A4, marginesy 2cm):
        1. Nagłówek    — logo + firma/NIP + tytuł "HISTORIA OBIEGU DOKUMENTU"
        2. Metadane    — ID instancji, dokument, status, pilność, ścieżka,
                         dyspozytor, daty przekazania/zakończenia/deadline
        3. Historia    — tabela z approval_log: lp. | data | użytkownik |
                         etap | akcja | komentarz | głosy
                         Wiersze is_voided=1: szare tło + prefix [UNIEWAŻNIONY]
        4. Stopka      — timestamp UTC generowania + wersja + ID instancji + użytkownik

    Returns:
        Bajty PDF gotowe do StreamingResponse.
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

    # ── Style ──────────────────────────────────────────────────────────────────
    base = getSampleStyleSheet()

    def _s(name: str, parent: str = "Normal", **kw) -> ParagraphStyle:
        kw.setdefault("fontName", "DejaVu")
        return ParagraphStyle(name, parent=base[parent], **kw)

    S_NORMAL   = _s("ah_n",    fontSize=9,  leading=13)
    S_BOLD     = _s("ah_b",    fontSize=9,  leading=13, fontName="DejaVu-Bold")
    S_TYTUL    = _s("ah_t",    "Title",
                    fontSize=14, leading=18, fontName="DejaVu-Bold",
                    textColor=colors.HexColor(_C_HEADER_BG), spaceAfter=2)
    S_SEKCJA   = _s("ah_sec",  fontSize=9,  leading=11, fontName="DejaVu-Bold",
                    textColor=colors.HexColor(_C_SECTION),
                    spaceBefore=8, spaceAfter=3)
    S_MALY     = _s("ah_sm",   fontSize=7,  leading=10,
                    textColor=colors.grey)
    S_FIRMA    = _s("ah_firma", fontSize=11, leading=14, fontName="DejaVu-Bold",
                    textColor=colors.HexColor(_C_HEADER_BG))
    S_CELL     = _s("ah_cell",  fontSize=8,  leading=11)
    S_CELL_B   = _s("ah_cellb", fontSize=8,  leading=11, fontName="DejaVu-Bold")
    S_CELL_V   = _s("ah_cellv", fontSize=8,  leading=11,
                    textColor=colors.HexColor(_C_VOIDED_TEXT))
    S_CELL_VB  = _s("ah_cellvb",fontSize=8,  leading=11, fontName="DejaVu-Bold",
                    textColor=colors.HexColor(_C_VOIDED_TEXT))
    S_COMMENT  = _s("ah_comm",  fontSize=7,  leading=10,
                    textColor=colors.HexColor("#555555"), leftIndent=4)
    S_COMMENT_V= _s("ah_commv", fontSize=7,  leading=10,
                    textColor=colors.HexColor(_C_VOIDED_TEXT), leftIndent=4)

    # ── Geometria strony ────────────────────────────────────────────────────────
    page_w, page_h = A4
    buffer = BytesIO()

    def _on_page(canvas_obj: Any, doc_obj: Any) -> None:
        """Callback numeracji stron — wywoływany przez ReportLab na każdej stronie."""
        canvas_obj.saveState()
        canvas_obj.setFont("DejaVu", 7)
        canvas_obj.setFillColor(colors.HexColor("#888888"))
        canvas_obj.drawRightString(
            page_w - 2 * cm,
            1.2 * cm,
            (
                f"Strona {doc_obj.page}  |  System Windykacja {_APP_VERSION}  |  "
                f"Obieg #{ctx.id_instance}  |  {ctx.generated_at[:10]}"
            ),
        )
        canvas_obj.restoreState()

    doc = SimpleDocTemplate(
        buffer,
        pagesize     = A4,
        rightMargin  = 2 * cm,
        leftMargin   = 2 * cm,
        topMargin    = 2.2 * cm,
        bottomMargin = 2 * cm,
        title        = f"Historia Obiegu — {_sanitize(ctx.document_title, 80)}",
        author       = "System Windykacja",
        subject      = f"ID instancji: {ctx.id_instance}",
        creator      = f"System Windykacja {_APP_VERSION}",
    )

    story: list[Any] = []

    # ═══════════════════════════════════════════════════════════════════════════
    # 1. NAGŁÓWEK — logo + firma + tytuł
    # ═══════════════════════════════════════════════════════════════════════════
    header_data: list[list[Any]] = [[]]

    # Logo (opcjonalne — graceful fallback)
    logo_cell: Any = Spacer(1, 1 * mm)
    if ctx.logo_path:
        try:
            logo_cell = Image(ctx.logo_path, width=3.5 * cm, height=1.5 * cm,
                              kind="proportional")
        except Exception as logo_exc:
            logger.warning(
                orjson.dumps({
                    "event":      "pdf_logo_load_failed",
                    "id_instance": ctx.id_instance,
                    "path":       ctx.logo_path,
                    "error":      str(logo_exc),
                }).decode()
            )

    firma_info: list[Paragraph] = [
        Paragraph(_sanitize(ctx.firma_nazwa, 100), S_FIRMA),
    ]
    if ctx.firma_nip:
        firma_info.append(Paragraph(f"NIP: {_sanitize(ctx.firma_nip, 30)}", S_NORMAL))

    header_data = [[
        logo_cell,
        firma_info,
        Paragraph(
            f'<font color="{_C_HEADER_BG}"><b>HISTORIA OBIEGU DOKUMENTU</b></font>',
            _s("ah_doc_title", fontSize=11, leading=14, fontName="DejaVu-Bold",
               alignment=2),   # 2 = TA_RIGHT
        ),
    ]]

    header_table = Table(header_data, colWidths=[4 * cm, 8 * cm, 5.5 * cm])
    header_table.setStyle(TableStyle([
        ("VALIGN",     (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING",  (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 4),
    ]))
    story.append(header_table)
    story.append(HRFlowable(
        width="100%", thickness=1.5,
        color=colors.HexColor(_C_HEADER_BG),
        spaceBefore=4, spaceAfter=6,
    ))

    # ═══════════════════════════════════════════════════════════════════════════
    # 2. METADANE INSTANCJI
    # ═══════════════════════════════════════════════════════════════════════════
    story.append(Paragraph("SZCZEGÓŁY OBIEGU", S_SEKCJA))

    status_disp  = _STATUS_DISPLAY.get(ctx.status, ctx.status)
    status_color = _STATUS_COLOR.get(ctx.status, _C_NEUTRAL)
    urgent_label = (
        '<font color="#C0392B"><b>TAK — PILNE</b></font>'
        if ctx.is_urgent else "Nie"
    )
    path_label   = (
        _sanitize(ctx.path_name, 100)
        if ctx.path_name else f"ID {ctx.id_path}" if ctx.id_path else "—"
    )
    amount_label = (
        f"{ctx.document_amount:,.2f} PLN".replace(",", " ")
        if ctx.document_amount is not None else "—"
    )

    meta_rows = [
        ["ID instancji obiegu:",
         Paragraph(f"<b>#{ctx.id_instance}</b>", S_BOLD),
         "Status:",
         Paragraph(
             f'<font color="{status_color}"><b>{_sanitize(status_disp)}</b></font>',
             S_BOLD,
         )],
        ["Dokument:",
         Paragraph(_sanitize(ctx.document_title, 200), S_NORMAL),
         "Pilny:",
         Paragraph(urgent_label, S_NORMAL)],
        ["Wartość dokumentu:",
         Paragraph(amount_label, S_NORMAL),
         "Ścieżka akceptacji:",
         Paragraph(path_label, S_NORMAL)],
        ["Dyspozytor:",
         Paragraph(
             _sanitize(
                 ctx.dispatched_by_fullname or ctx.dispatched_by_username or "—", 100
             ),
             S_NORMAL,
         ),
         "Etap bieżący:",
         Paragraph(str(ctx.current_step) if ctx.current_step else "—", S_NORMAL)],
        ["Przekazano:",
         Paragraph(_fmt_dt(ctx.dispatched_at), S_NORMAL),
         "Zakończono:",
         Paragraph(_fmt_dt(ctx.completed_at), S_NORMAL)],
        ["Źródło dokumentu:",
         Paragraph(_sanitize(ctx.source_name, 80), S_NORMAL),
         "Deadline:",
         Paragraph(_fmt_dt(ctx.deadline_at), S_NORMAL)],
    ]

    meta_table = Table(
        meta_rows,
        colWidths=[3.8 * cm, 5.7 * cm, 3.3 * cm, 4.7 * cm],
    )
    meta_table.setStyle(TableStyle([
        ("FONTNAME",    (0, 0), (0, -1), "DejaVu-Bold"),
        ("FONTNAME",    (2, 0), (2, -1), "DejaVu-Bold"),
        ("FONTSIZE",    (0, 0), (-1, -1), 8),
        ("LEADING",     (0, 0), (-1, -1), 11),
        ("VALIGN",      (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 2),
        ("RIGHTPADDING",(0, 0), (-1, -1), 4),
        ("TOPPADDING",  (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 2),
        # Linia oddzielająca kolumny
        ("LINEAFTER",   (1, 0), (1, -1), 0.3,
         colors.HexColor("#DDDDDD")),
        # Szachownica wierszy
        ("ROWBACKGROUNDS", (0, 0), (-1, -1),
         [colors.white, colors.HexColor("#F8F9FA")]),
    ]))
    story.append(meta_table)
    story.append(HRFlowable(
        width="100%", thickness=0.5,
        color=colors.HexColor("#DDDDDD"),
        spaceBefore=6, spaceAfter=6,
    ))

    # ═══════════════════════════════════════════════════════════════════════════
    # 3. HISTORIA AKCJI
    # ═══════════════════════════════════════════════════════════════════════════
    story.append(Paragraph(
        f"HISTORIA AKCJI  ({ctx.total_log_entries} wpisów"
        + (f", w tym {ctx.voided_entries} unieważnionych" if ctx.voided_entries else "")
        + ")",
        S_SEKCJA,
    ))

    if not ctx.log_entries:
        story.append(Paragraph(
            "Brak wpisów w logu dla tej instancji obiegu.",
            _s("ah_empty", fontSize=9, textColor=colors.grey),
        ))
    else:
        # ── Nagłówek tabeli ────────────────────────────────────────────────────
        def _th(tekst: str) -> Paragraph:
            return Paragraph(
                f'<font color="{_C_HEADER_FG}"><b>{tekst}</b></font>',
                _s(f"ah_th_{tekst[:6]}", fontSize=8, leading=10,
                   fontName="DejaVu-Bold"),
            )

        log_rows: list[list[Any]] = [[
            _th("Lp."),
            _th("Data i czas"),
            _th("Użytkownik"),
            _th("Et."),
            _th("Akcja / Komentarz"),
            _th("Głosy"),
        ]]

        # ── Wiersze danych ─────────────────────────────────────────────────────
        for idx, entry in enumerate(ctx.log_entries, start=1):
            is_voided   = bool(entry.get("is_voided", False))

            # Style zależne od voided
            S_c  = S_CELL_V  if is_voided else S_CELL
            S_cb = S_CELL_VB if is_voided else S_CELL_B
            S_cm = S_COMMENT_V if is_voided else S_COMMENT

            # Dane wiersza
            lp_str       = str(idx)
            data_str     = _fmt_dt(entry.get("logged_at_str"))
            actor_str    = _sanitize(
                entry.get("full_name") or entry.get("username_snapshot") or "system",
                60,
            )
            step_str     = str(entry.get("step_order_snapshot") or "—")
            action_raw   = entry.get("action", "")
            action_disp  = _sanitize(
                _ACTION_DISPLAY.get(action_raw, action_raw), 60
            )
            action_color = (
                _C_VOIDED_TEXT
                if is_voided
                else _ACTION_COLOR.get(action_raw, _C_NEUTRAL)
            )
            votes_str    = _votes_label(
                entry.get("votes_before"), entry.get("votes_after")
            )
            group_name   = entry.get("group_name")
            comment_text = _extract_comment_from_details(
                entry.get("comment"),
                entry.get("details") if isinstance(entry.get("details"), dict) else None,
            )

            # Kolumna "Akcja / Komentarz" — wielowierszowa
            akcja_content: list[Any] = []

            voided_prefix = ""
            if is_voided:
                voided_prefix = (
                    '<font color="#C0392B"><b>[UNIEWAŻNIONY]</b></font> '
                )

            akcja_content.append(Paragraph(
                f'{voided_prefix}'
                f'<font color="{action_color}"><b>{action_disp}</b></font>',
                S_cb,
            ))
            if group_name:
                akcja_content.append(Paragraph(
                    f"Grupa: {_sanitize(group_name, 60)}",
                    S_cm,
                ))
            if comment_text:
                # Komentarz w kursywie, obcięty do 300 znaków w PDF
                short_comment = _sanitize(comment_text, 300)
                akcja_content.append(Paragraph(
                    f'<i>"{short_comment}"</i>',
                    S_cm,
                ))

            log_rows.append([
                Paragraph(lp_str,    S_c),
                Paragraph(data_str,  S_c),
                Paragraph(actor_str, S_c),
                Paragraph(step_str,  S_c),
                akcja_content,
                Paragraph(votes_str, S_c),
            ])

        # ── Buduj tabelę ───────────────────────────────────────────────────────
        log_table = Table(
            log_rows,
            colWidths=[0.7 * cm, 2.8 * cm, 3.5 * cm, 0.6 * cm, 7.2 * cm, 2.7 * cm],
            repeatRows=1,   # nagłówek powtarza się na każdej stronie
        )

        # Zbuduj style — szachownica + osobne tło dla voided
        ts_cmds = [
            # Nagłówek tabeli
            ("BACKGROUND",  (0, 0), (-1, 0), colors.HexColor(_C_HEADER_BG)),
            ("TEXTCOLOR",   (0, 0), (-1, 0), colors.white),
            ("FONTNAME",    (0, 0), (-1, 0), "DejaVu-Bold"),
            ("FONTSIZE",    (0, 0), (-1, 0), 8),
            ("TOPPADDING",  (0, 0), (-1, 0), 4),
            ("BOTTOMPADDING",(0, 0), (-1, 0), 4),
            # Wszystkie wiersze danych
            ("FONTSIZE",    (0, 1), (-1, -1), 8),
            ("VALIGN",      (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 3),
            ("RIGHTPADDING",(0, 0), (-1, -1), 3),
            ("TOPPADDING",  (0, 1), (-1, -1), 3),
            ("BOTTOMPADDING",(0, 1),(-1, -1), 3),
            # Siatka
            ("GRID",        (0, 0), (-1, -1), 0.3, colors.HexColor("#DDDDDD")),
            ("LINEBELOW",   (0, 0), (-1, 0),  0.8, colors.HexColor(_C_HEADER_BG)),
        ]

        # Szachownica wierszy i szare tło dla voided
        for row_idx, entry in enumerate(ctx.log_entries, start=1):
            if entry.get("is_voided"):
                ts_cmds.append(
                    ("BACKGROUND", (0, row_idx), (-1, row_idx),
                     colors.HexColor(_C_VOIDED_BG))
                )
            elif row_idx % 2 == 0:
                ts_cmds.append(
                    ("BACKGROUND", (0, row_idx), (-1, row_idx),
                     colors.HexColor(_C_ROW_EVEN))
                )

        log_table.setStyle(TableStyle(ts_cmds))
        story.append(log_table)

    story.append(Spacer(1, 6 * mm))
    story.append(HRFlowable(
        width="100%", thickness=0.5,
        color=colors.HexColor("#DDDDDD"),
        spaceBefore=2, spaceAfter=4,
    ))

    # ═══════════════════════════════════════════════════════════════════════════
    # 4. STOPKA
    # ═══════════════════════════════════════════════════════════════════════════
    story.append(Paragraph(
        f"Dokument wygenerowany: {_fmt_dt(ctx.generated_at)}  UTC  |  "
        f"System Windykacja {_APP_VERSION}  |  "
        f"ID obiegu: {ctx.id_instance}  |  "
        f"Wygenerował: {_sanitize(ctx.requested_by, 50)}",
        S_MALY,
    ))

    # ── Buduj PDF ─────────────────────────────────────────────────────────────
    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)
    return buffer.getvalue()


# =============================================================================
# Helpery async — pobieranie danych z DB i Redis
# =============================================================================

async def _fetch_logo_path(redis: Redis, db: AsyncSession) -> Optional[str]:
    """
    Pobiera ścieżkę logo. Kolejność:
    1. SystemConfig 'faktury.pdf_logo_path' (przez Redis cache)
    2. Fallback settings.LOGO_PATH z .env
    3. None — PDF bez logo (nie błąd, graceful degradation)
    """
    try:
        logo_path_cfg = await get_config_value(
            redis=redis, key="faktury.pdf_logo_path", default=""
        )
        if logo_path_cfg and logo_path_cfg.strip():
            p = Path(logo_path_cfg.strip())
            if p.exists() and p.is_file():
                return str(p)
            logger.warning(
                orjson.dumps({
                    "event":  "logo_path_missing_on_disk",
                    "path":   str(p),
                    "key":    "faktury.pdf_logo_path",
                }).decode()
            )
    except Exception as exc:
        logger.warning(
            orjson.dumps({
                "event": "logo_config_read_error",
                "error": str(exc),
            }).decode()
        )

    try:
        settings  = get_settings()
        env_logo  = getattr(settings, "LOGO_PATH", None)
        if env_logo:
            p = Path(env_logo)
            if p.exists() and p.is_file():
                return str(p)
    except Exception:
        pass

    return None


async def _fetch_instance_meta(
    db: AsyncSession,
    id_instance: int,
) -> Optional[dict[str, Any]]:
    """
    Pobiera metadane instancji obiegu.

    Zwraca dict z kluczami:
        id_instance, document_title, document_amount, id_source, source_name,
        status, is_urgent, id_path, path_name, current_step,
        dispatched_by_username, dispatched_by_fullname,
        dispatched_at, completed_at, deadline_at

    Zwraca None jeśli instancja nie istnieje.
    """
    try:
        row = (await db.execute(
            text(
                f"SELECT "
                f"  i.[id_instance], "
                f"  i.[document_title], "
                f"  i.[document_amount], "
                f"  i.[id_source], "
                f"  ds.[source_name], "
                f"  i.[status], "
                f"  i.[is_urgent], "
                f"  i.[id_path], "
                f"  p.[path_name], "
                f"  i.[current_step], "
                f"  u.[Username], "
                f"  u.[FullName], "
                f"  i.[dispatched_at], "
                f"  i.[completed_at], "
                f"  i.[deadline_at] "
                f"FROM [{_SCHEMA}].[skw_document_approval_instances] i "
                f"LEFT JOIN [{_SCHEMA}].[skw_document_sources] ds "
                f"  ON ds.[id_source] = i.[id_source] "
                f"LEFT JOIN [{_SCHEMA}].[skw_approval_paths] p "
                f"  ON p.[id_path] = i.[id_path] "
                f"LEFT JOIN [{_SCHEMA}].[skw_Users] u "
                f"  ON u.[ID_USER] = i.[dispatched_by] "
                f"WHERE i.[id_instance] = :iid"
            ),
            {"iid": id_instance},
        )).fetchone()
    except Exception as exc:
        logger.error(
            orjson.dumps({
                "event":       "fetch_instance_meta_error",
                "id_instance": id_instance,
                "error":       str(exc),
                "error_type":  type(exc).__name__,
            }).decode(),
            exc_info=True,
        )
        return None

    if row is None:
        return None

    return {
        "id_instance":             row[0],
        "document_title":          str(row[1]) if row[1] else f"Dokument #{id_instance}",
        "document_amount":         float(row[2]) if row[2] is not None else None,
        "id_source":               row[3],
        "source_name":             str(row[4]) if row[4] else "—",
        "status":                  str(row[5]) if row[5] else "unknown",
        "is_urgent":               bool(row[6]),
        "id_path":                 row[7],
        "path_name":               str(row[8]) if row[8] else None,
        "current_step":            int(row[9]) if row[9] is not None else 0,
        "dispatched_by_username":  str(row[10]) if row[10] else None,
        "dispatched_by_fullname":  str(row[11]) if row[11] else None,
        "dispatched_at":           row[12].isoformat() if row[12] else None,
        "completed_at":            row[13].isoformat() if row[13] else None,
        "deadline_at":             row[14].isoformat() if row[14] else None,
    }


async def _fetch_log_entries(
    db: AsyncSession,
    id_instance: int,
) -> list[dict[str, Any]]:
    """
    Pobiera WSZYSTKIE wpisy approval_log dla instancji (łącznie z is_voided=1).

    LEFT JOIN na skw_approval_groups w celu pobrania nazwy grupy.
    LEFT JOIN na skw_Users w celu pobrania FullName aktora.

    Zwraca listę dict (plain), posortowaną chronologicznie (logged_at ASC).
    Przy błędzie DB: loguje error i zwraca [] (nie crashuje generowania PDF).
    """
    try:
        rows = (await db.execute(
            text(
                f"SELECT "
                f"  l.[id_log], "
                f"  l.[username_snapshot], "
                f"  u.[FullName], "
                f"  l.[action], "
                f"  l.[step_order_snapshot], "
                f"  l.[id_group_snapshot], "
                f"  g.[group_name], "
                f"  l.[votes_before], "
                f"  l.[votes_after], "
                f"  l.[is_voided], "
                f"  l.[comment], "
                f"  l.[details], "
                f"  l.[logged_at] "
                f"FROM [{_SCHEMA}].[skw_approval_log] l "
                f"LEFT JOIN [{_SCHEMA}].[skw_Users] u "
                f"  ON u.[ID_USER] = l.[id_user] "
                f"LEFT JOIN [{_SCHEMA}].[skw_approval_groups] g "
                f"  ON g.[id_group] = l.[id_group_snapshot] "
                f"WHERE l.[id_instance] = :iid "
                f"ORDER BY l.[logged_at] ASC"
            ),
            {"iid": id_instance},
        )).fetchall()
    except Exception as exc:
        logger.error(
            orjson.dumps({
                "event":       "fetch_log_entries_error",
                "id_instance": id_instance,
                "error":       str(exc),
                "error_type":  type(exc).__name__,
            }).decode(),
            exc_info=True,
        )
        return []

    entries: list[dict[str, Any]] = []
    for r in rows:
        # Parsuj details JSON (może być None, poprawny JSON lub śmieci)
        details_parsed: Optional[dict] = None
        if r[11]:
            try:
                details_parsed = json.loads(r[11])
            except Exception:
                details_parsed = {"raw": str(r[11])[:200]}

        entries.append({
            "id_log":              r[0],
            "username_snapshot":   str(r[1]) if r[1] else None,
            "full_name":           str(r[2]) if r[2] else None,
            "action":              str(r[3]) if r[3] else "unknown",
            "step_order_snapshot": r[4],
            "id_group_snapshot":   r[5],
            "group_name":          str(r[6]) if r[6] else None,
            "votes_before":        r[7],
            "votes_after":         r[8],
            "is_voided":           bool(r[9]),
            "comment":             str(r[10]) if r[10] else None,
            "details":             details_parsed,
            "logged_at_str":       r[12].isoformat() if r[12] else None,
        })

    return entries


# =============================================================================
# Publiczny punkt wejścia — wywoływany z routera
# =============================================================================

async def generate_approval_history_pdf(
    *,
    db:          AsyncSession,
    redis:       Redis,
    id_instance: int,
    requested_by: str = "system",
) -> bytes:
    """
    Generuje PDF historii obiegu dokumentu dla instancji `id_instance`.

    Przepływ:
        1. Sprawdź cache Redis (klucz zawiera hash updated_at instancji)
        2. Pobierz metadane instancji (+ JOIN na paths, sources, users)
        3. Pobierz wpisy approval_log (+ JOIN na groups, users)
        4. Pobierz ścieżkę logo
        5. Zbuduj _PDFKontekst (plain data)
        6. Uruchom _build_pdf_sync() w thread executor
        7. Zapisz wynik do cache Redis
        8. Zaloguj metryki (rozmiar KB, czas ms, liczba wpisów)

    Args:
        db:           AsyncSession SQLAlchemy
        redis:        Klient Redis (cache PDF + SystemConfig)
        id_instance:  ID instancji obiegu
        requested_by: Username osoby żądającej (do stopki PDF + logów)

    Returns:
        Bajty PDF gotowe do StreamingResponse.

    Raises:
        HTTPException 404: instancja nie istnieje
        HTTPException 500: błąd generowania PDF (po zalogowaniu)
    """
    from fastapi import HTTPException

    t_start = _time.monotonic()

    logger.info(
        orjson.dumps({
            "event":        "approval_history_pdf_start",
            "id_instance":  id_instance,
            "requested_by": requested_by,
        }).decode()
    )

    # ── Krok 1: Metadane instancji ─────────────────────────────────────────────
    meta = await _fetch_instance_meta(db, id_instance)
    if meta is None:
        logger.warning(
            orjson.dumps({
                "event":        "approval_history_pdf_not_found",
                "id_instance":  id_instance,
                "requested_by": requested_by,
            }).decode()
        )
        raise HTTPException(
            status_code=404,
            detail=f"Instancja obiegu ID={id_instance} nie istnieje.",
        )

    # ── Krok 2: Cache Redis ────────────────────────────────────────────────────
    # Klucz cache zawiera hash status+completed_at — inwalidacja po zmianie stanu.
    # Nie używamy samego id_instance, bo historia może rosnąć (nowe wpisy).
    cache_fingerprint = hashlib.md5(
        f"{id_instance}:{meta['status']}:{meta['completed_at']}:{meta['dispatched_at']}".encode()
    ).hexdigest()[:12]
    cache_key = f"approval_history_pdf:{id_instance}:{cache_fingerprint}"

    try:
        cached = await redis.get(cache_key)
        if cached:
            logger.debug(
                orjson.dumps({
                    "event":       "approval_history_pdf_cache_hit",
                    "id_instance": id_instance,
                    "cache_key":   cache_key,
                    "size_kb":     round(len(cached) / 1024, 1),
                }).decode()
            )
            return cached
    except Exception as exc:
        # Cache niedostępny — kontynuujemy bez cache (non-blocking)
        logger.warning(
            orjson.dumps({
                "event":       "approval_history_pdf_cache_read_error",
                "id_instance": id_instance,
                "error":       str(exc),
            }).decode()
        )

    logger.debug(
        orjson.dumps({
            "event":       "approval_history_pdf_cache_miss",
            "id_instance": id_instance,
            "cache_key":   cache_key,
        }).decode()
    )

    # ── Krok 3: Wpisy logu ────────────────────────────────────────────────────
    log_entries = await _fetch_log_entries(db, id_instance)
    voided_count = sum(1 for e in log_entries if e.get("is_voided"))

    logger.info(
        orjson.dumps({
            "event":          "approval_history_pdf_data_fetched",
            "id_instance":    id_instance,
            "status":         meta["status"],
            "log_entries":    len(log_entries),
            "voided_entries": voided_count,
            "requested_by":   requested_by,
        }).decode()
    )

    # ── Krok 4: Logo ──────────────────────────────────────────────────────────
    logo_path = await _fetch_logo_path(redis, db)

    # ── Krok 5: Buduj kontekst ────────────────────────────────────────────────
    settings = get_settings()
    ctx = _PDFKontekst(
        id_instance              = id_instance,
        document_title           = meta["document_title"],
        document_amount          = meta["document_amount"],
        id_source                = meta["id_source"],
        source_name              = meta["source_name"],
        status                   = meta["status"],
        is_urgent                = meta["is_urgent"],
        id_path                  = meta["id_path"],
        path_name                = meta["path_name"],
        current_step             = meta["current_step"],
        dispatched_by_username   = meta["dispatched_by_username"],
        dispatched_by_fullname   = meta["dispatched_by_fullname"],
        dispatched_at            = meta["dispatched_at"],
        completed_at             = meta["completed_at"],
        deadline_at              = meta["deadline_at"],
        log_entries              = log_entries,
        logo_path                = logo_path,
        firma_nazwa              = getattr(settings, "COMPANY_NAME", "System Windykacja"),
        firma_nip                = getattr(settings, "COMPANY_NIP", ""),
        generated_at             = datetime.now(timezone.utc).isoformat(),
        requested_by             = requested_by,
        total_log_entries        = len(log_entries),
        voided_entries           = voided_count,
    )

    # ── Krok 6: Generowanie PDF w thread executor ─────────────────────────────
    # ReportLab jest synchroniczny i CPU-bound.
    # run_in_executor() przenosi go do thread pool, nie blokuje event loop.
    loop = asyncio.get_running_loop()
    try:
        pdf_bytes: bytes = await loop.run_in_executor(None, _build_pdf_sync, ctx)
    except Exception as exc:
        logger.error(
            orjson.dumps({
                "event":        "approval_history_pdf_build_error",
                "id_instance":  id_instance,
                "error":        str(exc),
                "error_type":   type(exc).__name__,
                "requested_by": requested_by,
            }).decode(),
            exc_info=True,
        )
        raise HTTPException(
            status_code=500,
            detail="Błąd generowania PDF historii obiegu. Szczegóły w logach serwera.",
        )

    elapsed_ms = round((_time.monotonic() - t_start) * 1000, 1)
    size_kb    = round(len(pdf_bytes) / 1024, 1)

    # ── Krok 7: Zapis do cache Redis ──────────────────────────────────────────
    try:
        ttl = int(await get_config_value(
            redis=redis,
            key="faktury.pdf_cache_ttl_seconds",
            default="300",
        ))
        await redis.setex(cache_key, ttl, pdf_bytes)
        logger.debug(
            orjson.dumps({
                "event":       "approval_history_pdf_cached",
                "id_instance": id_instance,
                "cache_key":   cache_key,
                "ttl_s":       ttl,
                "size_kb":     size_kb,
            }).decode()
        )
    except Exception as exc:
        # Cache niedostępny — PDF zwracamy mimo to (non-blocking)
        logger.warning(
            orjson.dumps({
                "event":       "approval_history_pdf_cache_write_error",
                "id_instance": id_instance,
                "error":       str(exc),
            }).decode()
        )

    # ── Krok 8: Metryki ───────────────────────────────────────────────────────
    logger.info(
        orjson.dumps({
            "event":          "approval_history_pdf_generated",
            "id_instance":    id_instance,
            "requested_by":   requested_by,
            "size_kb":        size_kb,
            "elapsed_ms":     elapsed_ms,
            "log_entries":    len(log_entries),
            "voided_entries": voided_count,
            "status":         meta["status"],
            "is_urgent":      meta["is_urgent"],
            "path_name":      meta["path_name"],
            "cached_key":     cache_key,
        }).decode()
    )

    return pdf_bytes