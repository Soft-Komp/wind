# worker/services/ocr_service.py
"""
Pipeline OCR — F7 (sekcja 4.12).

Biblioteki: pytesseract + pdf2image (poppler-utils w Dockerfile).
Wyciaga pola strukturalne z PDF za pomoca regex i OCR:
  - numer dokumentu
  - NIP
  - data dokumentu
  - kwota brutto
  - nazwa kontrahenta

Asynchroniczny — wynik zapisywany do extra_data instancji obiegu
przez oddzielny task ARQ (ocr_task), nie blokuje synchronizacji.

Flaga OCR_ENABLED (SystemConfig) — domyslnie false, wlaczane przez admina
po zainstalowaniu tesseract w Dockerze i weryfikacji na STOMIL.

UWAGA: from __future__ import annotations OK (nie ORM, nie router).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger("worker.services.ocr")

# Jezyk OCR — domyslnie pol (jezyk polski z tesseract-ocr-pol)
_DEFAULT_LANG = "pol"

# Maksymalna liczba stron PDF do OCR — zabezpieczenie przed ogromnymi plikami
_DEFAULT_MAX_PAGES = 3


@dataclass
class OcrResult:
    """Wynik pipeline OCR dla jednego dokumentu."""
    raw_text:         str   = ""
    doc_number:       str | None = None
    nip:              str | None = None
    doc_date:         str | None = None
    amount_gross:     float | None = None
    contractor_name:  str | None = None
    confidence_score: float = 0.0
    pages_processed:  int   = 0
    error:            str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ocr_text":        self.raw_text[:5000],  # limit w extra_data
            "ocr_doc_number":  self.doc_number,
            "ocr_nip":         self.nip,
            "ocr_doc_date":    self.doc_date,
            "ocr_amount_gross": self.amount_gross,
            "ocr_contractor":  self.contractor_name,
            "ocr_confidence":  self.confidence_score,
            "ocr_pages":       self.pages_processed,
            "ocr_error":       self.error,
        }


async def extract_fields(
    file_path: str, *, lang: str = _DEFAULT_LANG, max_pages: int = _DEFAULT_MAX_PAGES,
    tesseract_timeout_seconds: int = 30,
) -> OcrResult:
    """
    Glowna funkcja pipeline OCR. Asynchroniczna — blokujaca czesc (pytesseract)
    delegowana do thread pool przez asyncio.to_thread().

    Args:
        file_path:  Sciezka do pliku PDF lub obrazu (PNG/JPG).
        lang:       Jezyk tesseract (domyslnie 'pol').
        max_pages:  Max liczba stron PDF do przetworzenia.

    Returns:
        OcrResult z wyodrebnionymi polami i confidence_score.
    """
    import asyncio

    path = Path(file_path)
    if not path.exists():
        return OcrResult(error=f"Plik nie istnieje: {file_path}")

    suffix = path.suffix.lower()
    try:
        if suffix == ".pdf":
            result = await asyncio.to_thread(
                _extract_from_pdf, str(path), lang=lang, max_pages=max_pages,
                tesseract_timeout_seconds=tesseract_timeout_seconds,
            )
        elif suffix in (".png", ".jpg", ".jpeg", ".tiff", ".bmp"):
            result = await asyncio.to_thread(
                _extract_from_image, str(path), lang=lang,
                tesseract_timeout_seconds=tesseract_timeout_seconds,
            )
        else:
            return OcrResult(error=f"Nieobslugiwany format pliku: {suffix}")
    except Exception as exc:
        logger.error("ocr_service.extract_fields: blad dla %s: %s", file_path, exc)
        return OcrResult(error=f"{type(exc).__name__}: {str(exc)[:200]}")

    return result


def _extract_from_pdf(
    file_path: str, *, lang: str, max_pages: int, tesseract_timeout_seconds: int = 30
) -> OcrResult:
    """
    Konwertuje PDF na obrazy (pdf2image/poppler) i uruchamia OCR na kazdej stronie.
    Wykonywany synchronicznie w thread pool.
    """
    try:
        from pdf2image import convert_from_path
    except ImportError:
        return OcrResult(error="pdf2image nie jest zainstalowany. Dodaj do requirements.txt.")

    try:
        pages = convert_from_path(
            file_path,
            dpi=200,
            first_page=1,
            last_page=max_pages,
            fmt="jpeg",
        )
    except Exception as exc:
        return OcrResult(error=f"pdf2image blad konwersji: {exc}")

    texts: list[str] = []
    for i, page in enumerate(pages):
        try:
            page_text = _run_tesseract(page, lang=lang, timeout_seconds=tesseract_timeout_seconds)
            texts.append(page_text)
            logger.debug("OCR strona %d/%d | chars=%d", i + 1, len(pages), len(page_text))
        except Exception as exc:
            logger.warning("OCR blad strony %d: %s", i + 1, exc)

    raw_text = "\n".join(texts)
    result = _parse_fields(raw_text)
    result.pages_processed = len(pages)
    return result


def _extract_from_image(
    file_path: str, *, lang: str, tesseract_timeout_seconds: int = 30
) -> OcrResult:
    """Uruchamia OCR bezposrednio na pliku obrazu."""
    try:
        from PIL import Image
        img = Image.open(file_path)
        raw_text = _run_tesseract(img, lang=lang, timeout_seconds=tesseract_timeout_seconds)
        result = _parse_fields(raw_text)
        result.pages_processed = 1
        return result
    except ImportError:
        return OcrResult(error="Pillow nie jest zainstalowany.")
    except Exception as exc:
        return OcrResult(error=f"{type(exc).__name__}: {exc}")


def _run_tesseract(image: Any, *, lang: str, timeout_seconds: int = 30) -> str:
    """
    Uruchamia pytesseract na obrazie PIL i zwraca wynikowy tekst.

    NAPRAWA (2026-07-23): timeout_seconds przekazywany do pytesseract jako
    natywny parametr `timeout` — w przeciwienstwie do asyncio.wait_for()
    (ktory tylko przestaje czekac, nie zabija podprocesu), pytesseract
    z ustawionym timeout FAKTYCZNIE terminuje proces tesseract po
    przekroczeniu limitu (RunTimeoutError). Bez tego kazdy zawieszony/
    bardzo wolny OCR zostawial osierocony proces tesseract zuzywajacy CPU
    bezterminowo (potwierdzone w docker top: procesy tesseract z rosnacym
    czasem CPU, > 2 minuty, nigdy niekonczace sie).
    """
    try:
        import pytesseract
        config = r"--oem 3 --psm 3"
        return pytesseract.image_to_string(
            image, lang=lang, config=config, timeout=timeout_seconds
        )
    except ImportError:
        raise RuntimeError("pytesseract nie jest zainstalowany.")
    except RuntimeError as exc:
        # pytesseract rzuca RunTimeoutError (subklasa RuntimeError) po
        # przekroczeniu timeout — proces tesseract zostaje juz zabity
        # przez sama biblioteke w tym momencie.
        raise RuntimeError(f"Tesseract przekroczyl limit {timeout_seconds}s: {exc}") from exc


def _parse_fields(text: str) -> OcrResult:
    """
    Wyciaga pola strukturalne z surowego tekstu OCR za pomoca regex.
    Wszystkie wzorce oparte na polskich dokumentach (faktury, umowy).
    """
    result = OcrResult(raw_text=text)
    fields_found = 0

    # Numer dokumentu — wzorce: FV/..., F/..., FA/..., NR ..., Nr ...,
    # typowe formaty polskich faktur
    m = re.search(
        r"(?:Faktura|Nr|Numer|FV|FA?)[^\w]*([\w/\-]{4,30})",
        text, re.IGNORECASE
    )
    if m:
        result.doc_number = m.group(1).strip()
        fields_found += 1

    # NIP — 10 cyfr, opcjonalnie z myslnikami lub spacjami
    m = re.search(r"NIP\s*:?\s*(\d{3}[-\s]?\d{3}[-\s]?\d{2}[-\s]?\d{2})", text, re.IGNORECASE)
    if m:
        result.nip = re.sub(r"[\s\-]", "", m.group(1))  # normalizuj do 10 cyfr
        fields_found += 1

    # Data — formaty: DD.MM.YYYY, YYYY-MM-DD, DD-MM-YYYY
    m = re.search(
        r"(?:Data|Wystawienia|Sprzedazy)[^\d]*(\d{2}[.\-]\d{2}[.\-]\d{4}|\d{4}-\d{2}-\d{2})",
        text, re.IGNORECASE
    )
    if not m:
        m = re.search(r"\b(\d{2}\.\d{2}\.\d{4})\b", text)
    if m:
        result.doc_date = m.group(1)
        fields_found += 1

    # Kwota brutto — szukaj "do zapłaty", "razem", "brutto" + liczba
    m = re.search(
        r"(?:do\s+zap.aty|Razem|RAZEM|Brutto|BRUTTO)[^\d]*(\d{1,8}[,\.]\d{2})",
        text, re.IGNORECASE
    )
    if m:
        try:
            amount_str = m.group(1).replace(",", ".")
            result.amount_gross = float(amount_str)
            fields_found += 1
        except ValueError:
            pass

    # Nazwa kontrahenta — linia po "Nabywca:", "Kupujacy:", "Odbiorca:"
    m = re.search(
        r"(?:Nabywca|Kupuj.cy|Odbiorca|Zamawiaj.cy)\s*:?\s*\n?\s*(.{5,80})",
        text, re.IGNORECASE
    )
    if m:
        contractor = m.group(1).strip().split("\n")[0].strip()
        if len(contractor) >= 5:
            result.contractor_name = contractor
            fields_found += 1

    # Confidence score — proporcja znalezionych pol (0.0-1.0)
    result.confidence_score = round(fields_found / 5, 2)

    logger.info(
        "_parse_fields | fields_found=%d/5 confidence=%.2f nip=%s",
        fields_found, result.confidence_score, result.nip,
    )
    return result