# worker/services/ftp_adapter.py
"""
FtpAdapter — adapter zrodla source_type='ftp'.

Port specyfikacji z sekcji 4.3 dokumentu Etap2_Instrukcja_Techniczna, KTORA JEST
NIEDOPRECYZOWANA W KILKU MIEJSCACH. Ponizej kazda luka jest jawnie oznaczona
komentarzem "UWAGA: rozstrzygniecie wlasne" — zamiast cichego zgadywania.

=== ROZSTRZYGNIECIA WLASNE (brak w specyfikacji) ===

1. FTP vs SFTP — specyfikacja mowi "FTP/SFTP" jak o jednym protokole, ale to
   dwie rozne implementacje (ftplib vs paramiko/SSH), rozna autentykacja.
   ROZSTRZYGNIECIE (zatwierdzone): nowe pole connection_config["protocol"] =
   "ftp" | "sftp" — WYMAGANE, swiadomy wybor administratora przy deklaracji
   zrodla. Brak tego pola = ValueError przy tworzeniu adaptera, zero
   domyslnego zgadywania po porcie. Panel admina MUSI miec osobne pole
   wyboru (np. dropdown) dla zrodel typu ftp.

2. Gdzie fizycznie zapisywany jest pobrany plik — specyfikacja nigdzie tego
   nie precyzuje. ROZSTRZYGNIECIE: wspolny wolumen /data/source_downloads
   (analogiczny do manual_uploads z dzisiejszej sesji OCR) — WYMAGA NOWEGO
   wolumenu w docker-compose.yml, wspoldzielonego miedzy api i worker,
   dokladnie tym samym wzorcem co manual_uploads (patrz komentarz w
   docker-compose.yml o rozjezdzie approval_data, ktorego unikamy).

3. Sekcja 2.4 mowi OCR obsluguje "PDF/obraz", sekcja 4.3 zawęza dla FTP tylko
   do PDF — SPRZECZNOSC. ROZSTRZYGNIECIE: idziemy za szersza definicja z 2.4
   (PDF + obrazy PNG/JPG/TIFF/BMP) — spojne z tym, co juz dzis obsluguje
   worker/services/ocr_service.py::extract_fields() (rozgalezienie po
   rozszerzeniu pliku, oba warianty juz zaimplementowane).

4. Kasowanie/przenoszenie plikow po przetworzeniu na FTP — nigdzie nie
   specyfikowane. ROZSTRZYGNIECIE: NIE ruszamy plikow zrodlowych (zero DELETE/
   MOVE na serwerze FTP) — polegamy wylacznie na filtrowaniu po dacie
   modyfikacji (mtime > since). Bezpieczniejsze (nieodwracalne kasowanie
   cudzych plikow to zly pomysl bez jawnej zgody), ale oznacza że plik
   pozostaje tam na zawsze — do przemyslenia w przyszlosci jesli okaze sie
   problemem (np. przenoszenie do podfolderu processed/ zamiast kasowania).

5. file_pattern — format nieprecyzowany (glob czy regex). ROZSTRZYGNIECIE: glob
   (fnmatch), bo to bardziej intuicyjne dla administratora wpisujacego wzorzec
   w UI (np. "*.pdf") niz regex.

6. id_document dla pliku — nieprecyzowane. ROZSTRZYGNIECIE: pelna sciezka
   zdalna (directory + "/" + filename), bo to jedyny stabilny, unikalny
   identyfikator na serwerze FTP (sama nazwa pliku moglaby sie powtorzyc
   w innym podfolderze, gdyby ktos kiedys rozszerzyl adapter o rekursje).

=== CO NIE JEST ROZWIAZANE (poza zakresem tego pliku) ===

- OCR triggering: NIE robimy tego bezposrednio w adapterze. Wpisujemy
  "file_path" do UnifiedDocument.raw_data — istniejacy mechanizm
  worker/tasks/source_sync_task.py::_enqueue_ocr_for_new_docs() juz dzis
  automatycznie kolejkuje ocr_task dla instancji z "file_path" w extra_data,
  utworzonych w ostatnich 6 minutach. Zero dodatkowego kodu potrzebne.

UWAGA: uzywa synchronicznego ftplib/paramiko — owiniete w asyncio.to_thread(),
ten sam wzorzec co KSeF20Adapter i ocr_service.py.
"""

import fnmatch
import hashlib
import logging
import os
import uuid
from datetime import datetime, timezone
from ftplib import FTP, error_perm
from pathlib import Path
from typing import Any, Optional

from worker.services.source_adapter import BaseDocumentAdapter, UnifiedDocument

logger = logging.getLogger("worker.services.ftp_adapter")

_ALLOWED_OCR_EXTENSIONS = frozenset({".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".bmp"})


class FtpAdapterError(Exception):
    """Blad specyficzny dla adaptera FTP/SFTP."""


class FtpAdapter(BaseDocumentAdapter):
    """
    Adapter zrodla FTP/SFTP. Implementuje BaseDocumentAdapter.fetch_new_documents().

    connection_config (odszyfrowany JSON):
        host:      Adres serwera
        port:      Port (domyslnie 21 dla FTP, 22 dla SFTP)
        login:     Uzytkownik
        password:  Haslo
        directory: Katalog zdalny do przeszukania (bez rekursji w podfoldery — MVP)
        file_pattern: Wzorzec glob nazwy pliku (np. "*.pdf")
        protocol:  "ftp" | "sftp" — ROZSTRZYGNIECIE WLASNE, patrz naglowek pliku.
                   Jesli brak: wnioskowane z portu (22 -> sftp, inaczej ftp).
    """

    def __init__(self, id_source: int, source_name: str, config: dict[str, Any]) -> None:
        self.id_source = id_source
        self.source_name = source_name
        self._config = config

        self.host = config.get("host")
        self.login = config.get("login")
        self.password = config.get("password", "")
        self.directory = config.get("directory", "/")
        self.file_pattern = config.get("file_pattern", "*")

        if not self.host or not self.login:
            raise ValueError(f"FtpAdapter [{source_name}]: brak 'host'/'login' w connection_config")

        protocol_raw = config.get("protocol")
        if protocol_raw not in ("ftp", "sftp"):
            raise ValueError(
                f"FtpAdapter [{source_name}]: pole 'protocol' w connection_config jest "
                f"WYMAGANE i musi byc jawnie ustawione na 'ftp' albo 'sftp' "
                f"(otrzymano: {protocol_raw!r}). To swiadomy wybor administratora przy "
                f"deklaracji zrodla — brak domyslnego zgadywania po porcie."
            )
        self.protocol = protocol_raw
        default_port = 22 if protocol_raw == "sftp" else 21
        self.port = int(config.get("port") or default_port)

        downloads_dir = os.environ.get("SOURCE_DOWNLOADS_DIR", "/data/source_downloads")
        self._downloads_dir = Path(downloads_dir) / f"ftp_{id_source}"

    # =========================================================================
    # FTP (ftplib) — synchroniczne, wywolywane przez asyncio.to_thread
    # =========================================================================

    def _list_and_filter_ftp_sync(self, since_epoch: Optional[float]) -> list[dict[str, Any]]:
        """Zwraca liste {'name', 'mtime_epoch', 'size'} dla plikow pasujacych do wzorca i daty."""
        results = []
        with FTP() as ftp:
            ftp.connect(self.host, self.port, timeout=30)
            ftp.login(self.login, self.password)
            ftp.cwd(self.directory)

            filenames = ftp.nlst()
            for name in filenames:
                if not fnmatch.fnmatch(name, self.file_pattern):
                    continue
                try:
                    # MDTM zwraca "YYYYMMDDHHMMSS" — nie kazdy serwer to wspiera (RFC 3659)
                    mdtm_response = ftp.sendcmd(f"MDTM {name}")
                    mtime_str = mdtm_response.split()[-1]
                    mtime = datetime.strptime(mtime_str, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                except Exception as exc:
                    logger.warning(
                        "FtpAdapter [%s]: serwer nie wspiera MDTM dla '%s' (%s) — traktuje jako nowy plik",
                        self.source_name, name, exc,
                    )
                    mtime = datetime.now(timezone.utc)

                if since_epoch is not None and mtime.timestamp() <= since_epoch:
                    continue

                try:
                    size = ftp.size(name) or 0
                except Exception:
                    size = 0

                results.append({"name": name, "mtime": mtime, "size": size})
        return results

    def _download_ftp_file_sync(self, name: str, local_path: Path) -> None:
        with FTP() as ftp:
            ftp.connect(self.host, self.port, timeout=30)
            ftp.login(self.login, self.password)
            ftp.cwd(self.directory)
            with open(local_path, "wb") as f:
                ftp.retrbinary(f"RETR {name}", f.write)

    # =========================================================================
    # SFTP (paramiko) — synchroniczne, wywolywane przez asyncio.to_thread
    # =========================================================================

    def _list_and_filter_sftp_sync(self, since_epoch: Optional[float]) -> list[dict[str, Any]]:
        import paramiko

        results = []
        transport = paramiko.Transport((self.host, self.port))
        try:
            transport.connect(username=self.login, password=self.password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            try:
                for attr in sftp.listdir_attr(self.directory):
                    if not fnmatch.fnmatch(attr.filename, self.file_pattern):
                        continue
                    mtime = datetime.fromtimestamp(attr.st_mtime, tz=timezone.utc)
                    if since_epoch is not None and attr.st_mtime <= since_epoch:
                        continue
                    results.append({"name": attr.filename, "mtime": mtime, "size": attr.st_size})
            finally:
                sftp.close()
        finally:
            transport.close()
        return results

    def _download_sftp_file_sync(self, name: str, local_path: Path) -> None:
        import paramiko

        transport = paramiko.Transport((self.host, self.port))
        try:
            transport.connect(username=self.login, password=self.password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            try:
                remote_path = f"{self.directory.rstrip('/')}/{name}"
                sftp.get(remote_path, str(local_path))
            finally:
                sftp.close()
        finally:
            transport.close()

    # =========================================================================
    # INTERFEJS BaseDocumentAdapter
    # =========================================================================

    async def fetch_new_documents(self, since: Optional[datetime], limit: int = 500) -> list[UnifiedDocument]:
        import asyncio

        since_epoch = since.timestamp() if since else None

        try:
            if self.protocol == "sftp":
                files = await asyncio.to_thread(self._list_and_filter_sftp_sync, since_epoch)
            else:
                files = await asyncio.to_thread(self._list_and_filter_ftp_sync, since_epoch)
        except error_perm as exc:
            raise FtpAdapterError(f"FtpAdapter [{self.source_name}]: blad uprawnien FTP: {exc}") from exc

        files = files[:limit]
        self._downloads_dir.mkdir(parents=True, exist_ok=True)

        documents: list[UnifiedDocument] = []
        for file_info in files:
            name = file_info["name"]
            ext = Path(name).suffix.lower()
            local_path = self._downloads_dir / f"{uuid.uuid4().hex}_{name}"

            try:
                if self.protocol == "sftp":
                    await asyncio.to_thread(self._download_sftp_file_sync, name, local_path)
                else:
                    await asyncio.to_thread(self._download_ftp_file_sync, name, local_path)
            except Exception as exc:
                logger.error(
                    "FtpAdapter [%s]: blad pobierania pliku '%s': %s",
                    self.source_name, name, exc,
                )
                continue

            # id_document = pelna sciezka zdalna — patrz UWAGA 6 w naglowku pliku
            id_document = f"{self.directory.rstrip('/')}/{name}"

            # NOWE (2026-07-28): SHA-256 pliku — wymagane do metody 2 wykrywania
            # duplikatow (identyczny plik wgrany ponownie, niezaleznie od zrodla).
            # Liczone po pobraniu, z dysku (a nie w locie podczas transferu) —
            # prostsze i wystarczajaco szybkie dla typowych rozmiarow faktur.
            try:
                file_hash = hashlib.sha256(local_path.read_bytes()).hexdigest()
            except Exception as exc:
                logger.warning(
                    "FtpAdapter [%s]: nie udalo sie policzyc SHA-256 dla '%s' — "
                    "duplikat nie zostanie wykryty metoda file_sha256 dla tego "
                    "dokumentu, reszta kaskady (fingerprint) nadal zadziala: %s",
                    self.source_name, name, exc,
                )
                file_hash = None

            raw_data: dict[str, Any] = {
                "original_filename": name,
                "file_path": str(local_path),
                "file_size": file_info["size"],
            }
            if ext not in _ALLOWED_OCR_EXTENSIONS:
                logger.info(
                    "FtpAdapter [%s]: plik '%s' ma rozszerzenie '%s' spoza listy OCR — "
                    "zapisany bez automatycznego odczytu tresci",
                    self.source_name, name, ext,
                )

            documents.append(UnifiedDocument(
                id_document=id_document,
                id_source=self.id_source,
                source_name=self.source_name,
                doc_number=None,   # brak ustrukturyzowanych danych — wypelni OCR
                document_type=ext.lstrip(".") or None,
                file_sha256=file_hash,
                raw_data=raw_data,
            ))

        logger.info(
            "FtpAdapter [%s]: fetch_new_documents zakonczone | plikow=%d",
            self.source_name, len(documents),
        )
        return documents