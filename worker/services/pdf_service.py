# =============================================================================
# worker/services/pdf_service.py — Generowanie PDF przez WeasyPrint
# =============================================================================
# HTML (Jinja2) → WeasyPrint → PDF bytes
# Numer referencyjny formatu: MON/RRRR/MM/NNNNN
# Dane firmy z .env, logo z LOGO_PATH (opcjonalne)
# =============================================================================

from __future__ import annotations

import base64
import logging
import re
from datetime import datetime, date
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from jinja2 import Environment, BaseLoader
from weasyprint import HTML, CSS

from worker.settings import get_settings

logger = logging.getLogger("worker.pdf")
_WARSAW = ZoneInfo("Europe/Warsaw")

# =============================================================================
# Szablon HTML dla monitu windykacyjnego
# =============================================================================

_MONIT_HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="pl">
<head>
    <meta charset="UTF-8">
    <style>
        @page {
            size: A4;
            margin: 2cm 2.5cm;
            @bottom-right {
                content: "Strona " counter(page) " z " counter(pages);
                font-size: 8pt;
                color: #666;
            }
        }
        * { box-sizing: border-box; }
        body {
            font-family: "Liberation Sans", "DejaVu Sans", Arial, sans-serif;
            font-size: 10pt;
            color: #1a1a1a;
            line-height: 1.5;
        }
        /* Nagłówek firmy */
        .header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            border-bottom: 2px solid #1a365d;
            padding-bottom: 12px;
            margin-bottom: 20px;
        }
        .company-block { flex: 1; }
        .company-name {
            font-size: 16pt;
            font-weight: bold;
            color: #1a365d;
            margin: 0 0 4px 0;
        }
        .company-details { font-size: 8pt; color: #555; }
        .logo { max-height: 60px; max-width: 160px; }

        /* Numer dokumentu */
        .doc-number-block {
            text-align: right;
            margin-bottom: 24px;
        }
        .doc-number {
            font-size: 13pt;
            font-weight: bold;
            color: #1a365d;
        }
        .doc-date { font-size: 9pt; color: #666; }

        /* Adresat */
        .recipient-block {
            background: #f7fafc;
            border: 1px solid #e2e8f0;
            border-radius: 4px;
            padding: 14px 18px;
            margin-bottom: 24px;
        }
        .recipient-label {
            font-size: 8pt;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: #666;
            margin-bottom: 4px;
        }
        .recipient-name { font-size: 11pt; font-weight: bold; }
        .recipient-details { font-size: 9pt; color: #444; }

        /* Tytuł */
        .monit-title {
            font-size: 14pt;
            font-weight: bold;
            color: #c53030;
            margin: 0 0 8px 0;
        }
        .monit-subtitle { font-size: 10pt; color: #555; margin-bottom: 20px; }

        /* Tabela faktur */
        .invoices-table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 16px;
        }
        .invoices-table th {
            background: #1a365d;
            color: #fff;
            padding: 8px 10px;
            text-align: left;
            font-size: 9pt;
        }
        .invoices-table th.right,
        .invoices-table td.right { text-align: right; }
        .invoices-table td {
            padding: 7px 10px;
            border-bottom: 1px solid #e2e8f0;
            font-size: 9pt;
        }
        .invoices-table tr:nth-child(even) td { background: #f7fafc; }
        .invoices-table tr.overdue td { color: #c53030; }

        /* Suma */
        .total-row {
            background: #fff5f5;
            border: 2px solid #c53030;
            border-radius: 4px;
            padding: 12px 18px;
            margin-bottom: 24px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .total-label { font-size: 11pt; font-weight: bold; }
        .total-amount { font-size: 15pt; font-weight: bold; color: #c53030; }

        /* Treść pisma */
        .body-text { margin-bottom: 16px; text-align: justify; }
        .body-text p { margin: 0 0 10px 0; }

        /* Termin płatności */
        .payment-deadline {
            background: #fffbeb;
            border: 1px solid #f6ad55;
            border-radius: 4px;
            padding: 12px 18px;
            margin-bottom: 24px;
        }
        .deadline-label { font-size: 8pt; text-transform: uppercase; color: #b7791f; }
        .deadline-date { font-size: 13pt; font-weight: bold; color: #b7791f; }

        /* Dane do przelewu */
        .payment-info {
            background: #f0fff4;
            border: 1px solid #68d391;
            border-radius: 4px;
            padding: 12px 18px;
            margin-bottom: 24px;
            font-size: 9pt;
        }
        .payment-info-title {
            font-weight: bold;
            margin-bottom: 6px;
            color: #276749;
        }

        /* Stopka */
        .footer {
            border-top: 1px solid #e2e8f0;
            padding-top: 10px;
            font-size: 8pt;
            color: #888;
            text-align: center;
        }
        .legal-notice {
            font-size: 7.5pt;
            color: #aaa;
            margin-top: 8px;
            text-align: center;
        }
        /* Podpis */
        .signature-block {
            margin-top: 40px;
            text-align: right;
        }
        .signature-line {
            border-top: 1px solid #333;
            display: inline-block;
            min-width: 180px;
            padding-top: 4px;
            font-size: 9pt;
        }
    </style>
</head>
<body>

<!-- ── Nagłówek firmy ──────────────────────────────────────────────────────── -->
<div class="header">
    <div class="company-block">
        <div class="company-name">{{ company_name }}</div>
        <div class="company-details">
            {% if company_nip %}NIP: {{ company_nip }}{% endif %}
            {% if company_regon %} | REGON: {{ company_regon }}{% endif %}<br>
            {% if company_address %}{{ company_address }}<br>{% endif %}
            {% if company_phone %}Tel: {{ company_phone }}{% endif %}
            {% if company_email %} | {{ company_email }}{% endif %}
        </div>
    </div>
    {% if logo_base64 %}
    <img class="logo" src="data:image/png;base64,{{ logo_base64 }}" alt="Logo" />
    {% endif %}
</div>

<!-- ── Numer dokumentu ────────────────────────────────────────────────────── -->
<div class="doc-number-block">
    <div class="doc-number">{{ doc_number }}</div>
    <div class="doc-date">Miejscowość, dnia {{ issue_date }}</div>
</div>

<!-- ── Adresat ────────────────────────────────────────────────────────────── -->
<div class="recipient-block">
    <div class="recipient-label">Adresat</div>
    <div class="recipient-name">{{ debtor_name }}</div>
    <div class="recipient-details">
        {% if debtor_nip %}NIP: {{ debtor_nip }}<br>{% endif %}
        {% if debtor_address %}{{ debtor_address }}<br>{% endif %}
    </div>
</div>

<!-- ── Tytuł ──────────────────────────────────────────────────────────────── -->
<div class="monit-title">WEZWANIE DO ZAPŁATY</div>
<div class="monit-subtitle">Nr referencyjny: {{ doc_number }}</div>

<!-- ── Treść ──────────────────────────────────────────────────────────────── -->
<div class="body-text">
    <p>
        Szanowni Państwo,<br><br>
        Uprzejmie informujemy, że na Państwa koncie widnieją zaległości płatnicze
        z tytułu poniżej wymienionych należności. Wzywamy do ich niezwłocznego uregulowania.
    </p>
</div>

<!-- ── Tabela faktur ─────────────────────────────────────────────────────── -->
<table class="invoices-table">
    <thead>
        <tr>
            <th>Nr faktury</th>
            <th>Data wystawienia</th>
            <th>Termin płatności</th>
            <th class="right">Kwota brutto</th>
            <th class="right">Pozostało do zapłaty</th>
            <th>Dni po terminie</th>
        </tr>
    </thead>
    <tbody>
        {% for inv in invoices %}
        <tr {% if inv.days_overdue > 0 %}class="overdue"{% endif %}>
            <td>{{ inv.number }}</td>
            <td>{{ inv.issue_date }}</td>
            <td>{{ inv.due_date }}</td>
            <td class="right">{{ "%.2f"|format(inv.amount) }} PLN</td>
            <td class="right">{{ "%.2f"|format(inv.remaining) }} PLN</td>
            <td>{% if inv.days_overdue > 0 %}{{ inv.days_overdue }}{% else %}—{% endif %}</td>
        </tr>
        {% endfor %}
    </tbody>
</table>

<!-- ── Suma ───────────────────────────────────────────────────────────────── -->
<div class="total-row">
    <span class="total-label">ŁĄCZNA KWOTA DO ZAPŁATY:</span>
    <span class="total-amount">{{ "%.2f"|format(total_debt) }} PLN</span>
</div>

<!-- ── Termin ─────────────────────────────────────────────────────────────── -->
<div class="payment-deadline">
    <div class="deadline-label">Termin zapłaty</div>
    <div class="deadline-date">{{ payment_deadline }}</div>
</div>

<!-- ── Dane do przelewu ───────────────────────────────────────────────────── -->
{% if payment_account %}
<div class="payment-info">
    <div class="payment-info-title">Dane do przelewu</div>
    Odbiorca: <strong>{{ company_name }}</strong><br>
    Nr rachunku: <strong>{{ payment_account }}</strong><br>
    Tytuł przelewu: <strong>{{ doc_number }} — {{ debtor_name }}</strong>
</div>
{% endif %}

<!-- ── Treść końcowa ──────────────────────────────────────────────────────── -->
<div class="body-text">
    <p>
        W przypadku, gdy należność została już uregulowana, prosimy o zignorowanie
        niniejszego pisma. W razie pytań prosimy o kontakt pod adresem
        {% if company_email %}<strong>{{ company_email }}</strong>{% else %}podanym powyżej{% endif %}.
    </p>
    <p>
        Brak zapłaty w wyznaczonym terminie może skutkować skierowaniem sprawy
        na drogę postępowania sądowego lub do firmy windykacyjnej.
    </p>
</div>

<!-- ── Podpis ─────────────────────────────────────────────────────────────── -->
<div class="signature-block">
    <div class="signature-line">{{ company_name }}<br>Dział Windykacji</div>
</div>

<!-- ── Stopka ─────────────────────────────────────────────────────────────── -->
<div class="footer">
    Dokument wygenerowany automatycznie przez System Windykacja |
    Data generacji: {{ generated_at }}
</div>
<div class="legal-notice">
    Wierzytelność może być przedmiotem cesji zgodnie z art. 509 k.c.
</div>

</body>
</html>
"""


# =============================================================================
# Główne API serwisu PDF
# =============================================================================

def _load_logo_base64() -> Optional[str]:
    """Ładuje logo firmy jako base64 PNG (opcjonalne)."""
    settings = get_settings()
    if not settings.LOGO_PATH:
        return None
    path = Path(settings.LOGO_PATH)
    if not path.exists():
        logger.warning("Logo nie znalezione", extra={"path": str(path)})
        return None
    try:
        data = path.read_bytes()
        return base64.b64encode(data).decode("ascii")
    except Exception as exc:
        logger.error("Błąd ładowania logo", extra={"path": str(path), "error": str(exc)})
        return None


def generate_monit_reference(monit_id: int, dt: Optional[date] = None) -> str:
    """
    Generuje numer referencyjny formatu: MON/RRRR/MM/NNNNN

    Przykład: MON/2026/03/00042
    """
    if dt is None:
        dt = datetime.now(_WARSAW).date()
    return f"MON/{dt.year}/{dt.month:02d}/{monit_id:05d}"


async def generate_pdf(
    monit_id: int,
    debtor_name: str,
    debtor_nip: Optional[str],
    debtor_address: Optional[str],
    invoices: list[dict],
    total_debt: float,
    payment_deadline: str,
    payment_account: Optional[str] = None,
    issue_date: Optional[str] = None,
) -> bytes:
    """
    Generuje PDF monitu windykacyjnego.

    Args:
        monit_id:          ID monitu (do numeru referencyjnego)
        debtor_name:       Nazwa dłużnika
        debtor_nip:        NIP dłużnika (opcjonalne)
        debtor_address:    Adres dłużnika (opcjonalne)
        invoices:          Lista faktur: [{number, issue_date, due_date, amount, remaining, days_overdue}]
        total_debt:        Łączna kwota do zapłaty
        payment_deadline:  Termin płatności (string DD.MM.RRRR)
        payment_account:   Numer konta do przelewu (opcjonalne)
        issue_date:        Data wystawienia (domyślnie: dziś)

    Returns:
        Bajty PDF.
    """
    settings = get_settings()
    now = datetime.now(_WARSAW)

    if issue_date is None:
        issue_date = now.strftime("%d.%m.%Y")

    doc_number = generate_monit_reference(monit_id, now.date())
    logo_b64 = _load_logo_base64()

    # Renderuj szablon HTML
    env = Environment(loader=BaseLoader())
    template = env.from_string(_MONIT_HTML_TEMPLATE)

    html_str = template.render(
        # Firma
        company_name=settings.COMPANY_NAME,
        company_nip=settings.COMPANY_NIP,
        company_regon=settings.COMPANY_REGON,
        company_address=settings.COMPANY_ADDRESS,
        company_phone=settings.COMPANY_PHONE,
        company_email=settings.COMPANY_EMAIL,
        logo_base64=logo_b64,
        # Dokument
        doc_number=doc_number,
        issue_date=issue_date,
        generated_at=now.strftime("%d.%m.%Y %H:%M:%S"),
        # Dłużnik
        debtor_name=debtor_name,
        debtor_nip=debtor_nip or "",
        debtor_address=debtor_address or "",
        # Faktury
        invoices=invoices,
        total_debt=total_debt,
        # Płatność
        payment_deadline=payment_deadline,
        payment_account=payment_account or "",
    )

    # WeasyPrint → PDF
    try:
        html_obj = HTML(string=html_str, base_url=None)
        pdf_bytes = html_obj.write_pdf()

        logger.info(
            "PDF wygenerowany",
            extra={
                "monit_id": monit_id,
                "doc_number": doc_number,
                "pdf_size_kb": round(len(pdf_bytes) / 1024, 1),
                "invoices_count": len(invoices),
                "total_debt": total_debt,
            },
        )
        return pdf_bytes

    except Exception as exc:
        logger.error(
            "Błąd generowania PDF",
            extra={
                "monit_id": monit_id,
                "error": str(exc),
                "error_type": type(exc).__name__,
            },
            exc_info=True,
        )
        raise


def save_pdf_to_disk(
    pdf_bytes: bytes,
    monit_id: int,
    monit_type: str = "email",
) -> str:
    """
    Zapisuje PDF na dysk i zwraca ścieżkę.
    Katalog: /app/pdf_cache/YYYY-MM-DD/

    Returns:
        Ścieżka do pliku.
    """
    settings = get_settings()
    now = datetime.now(_WARSAW)
    date_dir = Path(settings.PDF_CACHE_DIR) / now.strftime("%Y-%m-%d")
    date_dir.mkdir(parents=True, exist_ok=True)

    filename = f"monit_{monit_id}_{monit_type}_{now.strftime('%H%M%S')}.pdf"
    filepath = date_dir / filename

    filepath.write_bytes(pdf_bytes)
    logger.debug(
        "PDF zapisany na dysk",
        extra={"path": str(filepath), "size_kb": round(len(pdf_bytes) / 1024, 1)},
    )
    return str(filepath)