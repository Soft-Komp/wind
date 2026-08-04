# worker/services/email_adapter.py
"""
EmailAdapter — adapter zrodla source_type='email'.

Port specyfikacji z sekcji 4.3 dokumentu Etap2_Instrukcja_Techniczna, KTORA JEST
NIEDOPRECYZOWANA W KILKU MIEJSCACH. Kazda luka oznaczona "UWAGA: rozstrzygniecie
wlasne" ponizej.

=== ROZSTRZYGNIECIA WLASNE (brak w specyfikacji) ===

1. Brak pola szyfrowania transportu w connection_config (host, port, login,
   password, folder, filter_sender — zero pola SSL/TLS). Prawie kazda
   prawdziwa skrzynka wymaga IMAPS. ROZSTRZYGNIECIE: nowe pole
   connection_config["use_ssl"] (bool), domyslnie True — bezpieczniejszy
   domyslny wybor, jawnie mozna wylaczyc dla serwerow wewnetrznych bez TLS.

2. Relacja "jedna wiadomosc = ile dokumentow" — specyfikacja mowi liczba
   pojedyncza ("tworzy UnifiedDocument"), ale wiadomosc moze miec wiele
   zalacznikow PDF/XML jednoczesnie. ROZSTRZYGNIECIE: KAZDY zalacznik to
   OSOBNY UnifiedDocument (spojne z filozofia "jeden dokument = jedna
   faktura" w reszcie systemu). id_document = f"{message_id}::{nazwa_pliku}"
   dla unikalnosci przy wielu zalacznikach w jednej wiadomosci.

3. IMAP SEARCH SINCE ma rozdzielczosc DNIA, nie sekundy/godziny (ograniczenie
   samego protokolu IMAP, nie nasza decyzja) — kazdy cykl synchronizacji tego
   samego dnia zobaczy ponownie te same wiadomosci z rana. Specyfikacja nie
   wspomina o deduplikacji. ROZSTRZYGNIECIE: deduplikacja przez id_document
   deterministyczny (Message-ID + nazwa zalacznika) — MERGE w _upsert_instance
   (juz istniejacy mechanizm) zapewnia, ze ponowne zobaczenie tej samej
   wiadomosci nie tworzy duplikatu, tylko nieszkodliwie aktualizuje istniejacy
   wiersz (o ile nie jest w stanie terminalnym).

4. Gdzie fizycznie zapisywany jest pobrany zalacznik — nigdzie nie
   sprecyzowane. ROZSTRZYGNIECIE: ten sam wspolny wolumen /data/source_downloads
   co FtpAdapter (patrz ftp_adapter.py, UWAGA 2) — jeden wolumen, nie dwa
   osobne, zeby nie mnozyc zmian infrastrukturalnych bez potrzeby.

5. filter_sender — dokladne dopasowanie czy dopasowanie domeny? ROZSTRZYGNIECIE:
   dopasowanie "contains" (substring, case-insensitive) na adresie nadawcy —
   najbardziej elastyczne, pozwala administratorowi podac zarowno pelny adres
   jak i sama domene (np. "@dostawca.pl").

=== CO NIE JEST ROZWIAZANE (poza zakresem tego pliku) ===

- Tresc samej wiadomosci (body) jako potencjalne zrodlo danych faktury (np.
  gdy dostawca wysyla dane w tresci HTML, bez zalacznika) — specyfikacja mowi
  wylacznie o zalacznikach, wiec tresc wiadomosci jest ignorowana. Jesli to
  realny przypadek uzycia, wymaga osobnej decyzji projektowej.
- OCR triggering: jak w FtpAdapter — "file_path" w raw_data wystarcza,
  istniejacy _enqueue_ocr_for_new_docs() w source_sync_task.py robi reszte.

UWAGA: uzywa synchronicznego imaplib/email (biblioteka standardowa) — owiniete
w asyncio.to_thread(), ten sam wzorzec co FtpAdapter/KSeF20Adapter.
"""

import email as email_lib
import hashlib
import imaplib
import logging
import os
import uuid
from datetime import datetime
from email.header import decode_header
from pathlib import Path
from typing import Any, Optional

from worker.services.source_adapter import BaseDocumentAdapter, UnifiedDocument

logger = logging.getLogger("worker.services.email_adapter")

# POPRAWKA (2026-07-14): .xml zablokowane celowo. ocr_service.extract_fields()
# nie obsluguje XML (zwraca blad "Nieobslugiwany format pliku"), a od tej
# samej zmiany dokumenty ftp/email startuja jako status='ocr_review_pending'
# (patrz source_sync_task.py::_upsert_instance) — zalacznik XML nigdy nie
# doszedlby do _update_instance_with_ocr() (ocr_task zwraca sie wczesniej
# przy bledzie OCR), wiec utkwilby w ocr_review_pending NA ZAWSZE, bez
# zadnej sciezki wyjscia poza recznym rozwiazaniem admina co do KAZDEGO
# takiego dokumentu. Blokada tutaj = jasny blad we froncie/logu zamiast
# cichego, permanentnego zatoru. Do zdjecia gdy powstanie dedykowana
# obsluga XML (np. bezposredni parser e-faktury, bez przechodzenia przez OCR).
_ALLOWED_ATTACHMENT_EXTENSIONS = frozenset({".pdf"})


class EmailAdapterError(Exception):
    """Blad specyficzny dla adaptera e-mail."""


def _decode_mime_header(raw_value: Optional[str]) -> str:
    if not raw_value:
        return ""
    decoded_parts = decode_header(raw_value)
    result = []
    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            result.append(part.decode(encoding or "utf-8", errors="replace"))
        else:
            result.append(part)
    return "".join(result)


class EmailAdapter(BaseDocumentAdapter):
    """
    Adapter zrodla e-mail (IMAP). Implementuje BaseDocumentAdapter.fetch_new_documents().

    connection_config (odszyfrowany JSON):
        host:          Adres serwera IMAP
        port:          Port (domyslnie 993 dla SSL, 143 bez SSL)
        login:         Login skrzynki
        password:      Haslo
        folder:        Folder IMAP do przeszukania (domyslnie "INBOX")
        filter_sender: Filtr po nadawcy (substring, opcjonalny)
        use_ssl:       ROZSTRZYGNIECIE WLASNE — patrz naglowek pliku. Domyslnie True.
    """

    def __init__(self, id_source: int, source_name: str, config: dict[str, Any]) -> None:
        self.id_source = id_source
        self.source_name = source_name
        self._config = config

        self.host = config.get("host")
        self.login = config.get("login")
        self.password = config.get("password", "")
        self.folder = config.get("folder", "INBOX")
        self.filter_sender = (config.get("filter_sender") or "").lower().strip()
        self.use_ssl = bool(config.get("use_ssl", True))

        if not self.host or not self.login:
            raise ValueError(f"EmailAdapter [{source_name}]: brak 'host'/'login' w connection_config")

        default_port = 993 if self.use_ssl else 143
        self.port = int(config.get("port") or default_port)

        downloads_dir = os.environ.get("SOURCE_DOWNLOADS_DIR", "/data/source_downloads")
        self._downloads_dir = Path(downloads_dir) / f"email_{id_source}"

    # =========================================================================
    # IMAP — synchroniczne, wywolywane przez asyncio.to_thread
    # =========================================================================

    def _fetch_messages_sync(self, since: Optional[datetime]) -> list[dict[str, Any]]:
        """
        Zwraca liste {'message_id', 'subject', 'from', 'date', 'attachments': [(filename, bytes)]}.
        """
        if self.use_ssl:
            conn = imaplib.IMAP4_SSL(self.host, self.port)
        else:
            conn = imaplib.IMAP4(self.host, self.port)

        results: list[dict[str, Any]] = []
        try:
            conn.login(self.login, self.password)
            conn.select(self.folder, readonly=True)

            search_criteria = "ALL"
            if since:
                # IMAP SINCE ma rozdzielczosc dnia — patrz UWAGA 3 w naglowku pliku
                imap_date = since.strftime("%d-%b-%Y")
                search_criteria = f'(SINCE "{imap_date}")'

            status, data = conn.search(None, search_criteria)
            if status != "OK":
                raise EmailAdapterError(f"EmailAdapter [{self.source_name}]: blad IMAP SEARCH: {status}")

            message_numbers = data[0].split()

            for num in message_numbers:
                status, msg_data = conn.fetch(num, "(RFC822)")
                if status != "OK" or not msg_data or not msg_data[0]:
                    continue

                raw_email = msg_data[0][1]
                msg = email_lib.message_from_bytes(raw_email)

                from_addr = _decode_mime_header(msg.get("From", ""))
                if self.filter_sender and self.filter_sender not in from_addr.lower():
                    continue

                message_id = msg.get("Message-ID", "").strip() or f"noid-{num.decode()}"
                subject = _decode_mime_header(msg.get("Subject", ""))
                date_raw = msg.get("Date")

                attachments: list[tuple[str, bytes]] = []
                for part in msg.walk():
                    content_disposition = str(part.get("Content-Disposition", ""))
                    if "attachment" not in content_disposition:
                        continue
                    filename = _decode_mime_header(part.get_filename())
                    if not filename:
                        continue
                    ext = Path(filename).suffix.lower()
                    if ext not in _ALLOWED_ATTACHMENT_EXTENSIONS:
                        if ext == ".xml":
                            logger.info(
                                "EmailAdapter [%s]: pominieto zalacznik XML '%s' — "
                                "XML zablokowany do czasu dedykowanej obslugi (nie przechodzi "
                                "przez OCR), patrz komentarz przy _ALLOWED_ATTACHMENT_EXTENSIONS",
                                self.source_name, filename,
                            )
                        continue
                    payload = part.get_payload(decode=True)
                    if payload:
                        attachments.append((filename, payload))

                if attachments:
                    results.append({
                        "message_id": message_id,
                        "subject": subject,
                        "from": from_addr,
                        "date_raw": date_raw,
                        "attachments": attachments,
                    })
        finally:
            try:
                conn.close()
            except Exception:
                pass
            try:
                conn.logout()
            except Exception:
                pass

        return results

    # =========================================================================
    # INTERFEJS BaseDocumentAdapter
    # =========================================================================

    async def fetch_new_documents(self, since: Optional[datetime], limit: int = 500) -> list[UnifiedDocument]:
        import asyncio

        try:
            messages = await asyncio.to_thread(self._fetch_messages_sync, since)
        except imaplib.IMAP4.error as exc:
            raise EmailAdapterError(f"EmailAdapter [{self.source_name}]: blad IMAP: {exc}") from exc

        self._downloads_dir.mkdir(parents=True, exist_ok=True)

        documents: list[UnifiedDocument] = []
        for msg in messages:
            for filename, payload in msg["attachments"]:
                if len(documents) >= limit:
                    break

                local_path = self._downloads_dir / f"{uuid.uuid4().hex}_{filename}"
                try:
                    with open(local_path, "wb") as f:
                        f.write(payload)
                except OSError as exc:
                    logger.error(
                        "EmailAdapter [%s]: blad zapisu zalacznika '%s': %s",
                        self.source_name, filename, exc,
                    )
                    continue

                # id_document = Message-ID + nazwa zalacznika — patrz UWAGA 2 w naglowku
                id_document = f"{msg['message_id']}::{filename}"

                # NOWE (2026-07-28): SHA-256 zalacznika — liczone z 'payload'
                # (bajty juz w pamieci, przed zapisem na dysk — brak potrzeby
                # ponownego czytania pliku).
                try:
                    file_hash = hashlib.sha256(payload).hexdigest()
                except Exception as exc:
                    logger.warning(
                        "EmailAdapter [%s]: nie udalo sie policzyc SHA-256 dla "
                        "'%s' — duplikat nie zostanie wykryty metoda file_sha256 "
                        "dla tego dokumentu: %s",
                        self.source_name, filename, exc,
                    )
                    file_hash = None

                documents.append(UnifiedDocument(
                    id_document=id_document,
                    id_source=self.id_source,
                    source_name=self.source_name,
                    doc_number=None,  # brak ustrukturyzowanych danych — wypelni OCR
                    contractor_name=msg["from"] or None,  # heurystyka: adres nadawcy, niekoniecznie nazwa firmy
                    document_type=Path(filename).suffix.lstrip(".").lower() or None,
                    file_sha256=file_hash,
                    raw_data={
                        "original_filename": filename,
                        "file_path": str(local_path),
                        "email_subject": msg["subject"],
                        "email_from": msg["from"],
                        "email_message_id": msg["message_id"],
                    },
                ))
        logger.info(
            "EmailAdapter [%s]: fetch_new_documents zakonczone | wiadomosci=%d zalacznikow=%d",
            self.source_name, len(messages), len(documents),
        )
        return documents