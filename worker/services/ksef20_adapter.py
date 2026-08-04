# worker/services/ksef20_adapter.py
"""
KSeF20Adapter — adapter zrodla source_type='ksef20' (worker, synchronizacja cykliczna).

Port protokolu (XAdES, eksport paczek, AES-CBC, RSA-OAEP) z referencyjnego,
dzialajacego workera projektu SAMARYTANIN (worker_ksef20_export.py). Logika
kryptograficzna/protokolu KSeF przeniesiona niemal 1:1 — to jest przetestowany,
dzialajacy kod. NATOMIAST calkowicie przepisane zostaly: zrodlo konfiguracji
(connection_config zamiast tabel smm_*), format wyjscia (UnifiedDocument zamiast
bezposredniego zapisu SQL), szyfrowanie (nasz Fernet zamiast statycznego klucza
ccdr.py), cache sesji w Redis (referencja nie miala tego wcale — logowala sie
od zera przy kazdym uruchomieniu).

SYNC: brak odpowiednika w backend/app/adapters/ksef20_adapter.py — CELOWO NIE
IMPLEMENTUJEMY teraz strony backendu (akcje na zadanie: "sprawdz status",
"pobierz XML ponownie"). Zostawione jako swiadomie odlozone rozszerzenie.

ZALEZNOSCI (dopisac do worker/requirements.txt):
    signxml==5.1.0
    lxml==6.1.1
    (cryptography juz jest z dzisiejszej sesji OCR/manual_upload)

UWAGA: uzywa synchronicznych requests/lxml/signxml — kazde wywolanie tych
bibliotek jest owiniete w asyncio.to_thread(), tym samym wzorcem co
pytesseract w worker/services/ocr_service.py.

ZNANE ZALOZENIE DO ZWERYFIKOWANIA: format cert_content/key_content w
connection_config. Patrz _decode_cert_or_key() nizej — obsluguje dwa warianty,
ale nie mam prawdziwego pliku certyfikatu do przetestowania.

NASTEPNY KROK (nie w tym pliku): worker/tasks/ksef_sync_task.py — osobny task
ARQ z wlasnym cronem (co ok. 60 min, timeout 45 min), NIE dzielacy kolejki
z source_sync_task (eksport paczek moze trwac do 30 min).

from __future__ import annotations NIGDY w tym pliku — brak takiej potrzeby,
a chcemy zachowac styl zgodny z reszta plikow worker/services/.
"""

import asyncio
import base64
import hashlib
import json
import logging
import os
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Any, Optional

import requests
from lxml import etree
from cryptography import x509
from cryptography.hazmat.primitives import serialization, hashes, padding as sym_padding
from cryptography.hazmat.primitives.asymmetric import ec, rsa, padding as asym_padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from signxml import methods
from signxml.xades import XAdESSigner

from worker.services.source_adapter import BaseDocumentAdapter, UnifiedDocument

logger = logging.getLogger("worker.services.ksef20_adapter")

# Domyslne adresy bazowe per srodowisko — identyczne z referencja
_DEFAULT_URLS = {
    "PROD": "https://api.ksef.mf.gov.pl/api/v2",
    "PRD":  "https://api.ksef.mf.gov.pl/api/v2",
    "DEMO": "https://api-demo.ksef.mf.gov.pl/api/v2",
    "TEST": "https://api-test.ksef.mf.gov.pl/api/v2",
    "TE":   "https://api-test.ksef.mf.gov.pl/api/v2",
}

_MAX_AUTH_WAIT_SECONDS   = 180
_AUTH_POLL_INTERVAL      = 5
_REQUEST_TIMEOUT_SECONDS = 60
_MAX_EXPORT_WAIT_SECONDS = 1800
_EXPORT_POLL_INTERVAL    = 10
_MAX_WINDOW_DAYS         = 89

_SESSION_KEY_PREFIX = "ksef_session:"
_LOCK_KEY_PREFIX    = "ksef_lock:"
_LOCK_TTL_SECONDS   = 60 * 5  # 5 min — czas na cala autoryzacje, nie na caly eksport


@dataclass
class _KSeFTokens:
    reference_number: str
    access_token: str
    expires_at: datetime  # obliczone lokalnie — patrz uwaga w _authenticate()


class KSeF20AdapterError(Exception):
    """Blad specyficzny dla adaptera KSeF 2.0."""


class KSeF20Adapter(BaseDocumentAdapter):
    """
    Adapter zrodla KSeF 2.0. Implementuje BaseDocumentAdapter.fetch_new_documents().

    connection_config (odszyfrowany JSON):
        environment:   "PROD" | "DEMO" | "TEST"
        cert_content:  base64 — patrz _decode_cert_or_key() co do formatu
        key_content:   base64 — jw.
        key_password:  haslo do klucza prywatnego (plaintext po deszyfrowaniu configu)
        nip:           NIP firmy (10 cyfr, moze byc z separatorami — normalizujemy)
        dtstart:       data startu odczytu (YYYY-MM-DD), uzywana tylko gdy since=None
    """

    def __init__(
        self,
        id_source: int,
        source_name: str,
        config: dict[str, Any],
        redis: Any = None,
    ) -> None:
        self.id_source   = id_source
        self.source_name = source_name
        self._config     = config
        self._redis      = redis

        self.environment = str(config.get("environment", "PROD")).upper().strip()
        # NAPRAWA 2026-07-20 (KRYTYCZNA): usunieto os.environ.get("KSEF_BASE_URL")
        # jako priorytetowe nadpisanie. Ta zmienna srodowiskowa byla GLOBALNA
        # dla calego kontenera workera i miala priorytet nad connection_config.
        # environment per-zrodlo — jesli KTOKOLWIEK kiedykolwiek ustawil
        # KSEF_BASE_URL w .env (np. do wczesniejszych testow na DEMO), TO
        # ZRODLO wciaz wysylalo zadania na DEMO mimo environment="PROD" w
        # connection_config, PO CICHU, bez zadnego ostrzezenia. Certyfikat
        # autoryzowany dla PROD nie ma uprawnien w DEMO (i odwrotnie) — KSeF
        # odrzuca to komunikatem "415 Brak przypisanych uprawnien", myslacym
        # przyczyne bledu (wyglada jak problem uprawnien u wystawcy certyfikatu,
        # a jest to blad wyboru srodowiska w naszym kodzie). Zdiagnozowane w
        # sesji 2026-07-20 po niekonsekwentnym zachowaniu (jedna synchronizacja
        # tym samym certyfikatem sie powiodla, kolejna — nie).
        #
        # connection_config.environment jest teraz JEDYNYM zrodlem prawdy —
        # zgodnie z reszta architektury projektu (per-source config, nie
        # globalne zmienne kontenera).
        if os.environ.get("KSEF_BASE_URL"):
            logger.warning(
                "KSeF20Adapter [%s]: zmienna srodowiskowa KSEF_BASE_URL jest "
                "ustawiona (%s), ale ZOSTAJE ZIGNOROWANA — connection_config."
                "environment ('%s') jest jedynym uzywanym zrodlem adresu bazowego. "
                "Usun te zmienna z .env workera, jesli nie jest juz potrzebna, "
                "zeby uniknac tego ostrzezenia w przyszlosci.",
                self.source_name, os.environ.get("KSEF_BASE_URL"), self.environment,
            )
        self.base_url = _DEFAULT_URLS.get(self.environment, _DEFAULT_URLS["PROD"])
        self.base_url = self.base_url.rstrip("/")

        self.nip = re.sub(r"\D", "", str(config.get("nip", "")))
        if len(self.nip) != 10:
            raise ValueError(
                f"KSeF20Adapter [{source_name}]: NIP nieprawidlowy po normalizacji: '{self.nip}'"
            )

        raw_cert = config.get("cert_content")
        raw_key  = config.get("key_content")
        self._key_password = config.get("key_password") or ""
        if not raw_cert or not raw_key:
            raise ValueError(
                f"KSeF20Adapter [{source_name}]: brak cert_content/key_content w connection_config"
            )

        # Patrz UWAGA w naglowku pliku — format do zweryfikowania z prawdziwym certyfikatem
        self._cert_pem = self._decode_cert_or_key(raw_cert, "CERTIFICATE")
        self._key_pem  = self._decode_cert_or_key(raw_key, "ENCRYPTED PRIVATE KEY")

        self._dtstart_raw = config.get("dtstart")

        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Error-Format": "problem-details",
        })

    # =========================================================================
    # Dekodowanie certyfikatu/klucza — patrz UWAGA na gorze pliku
    # =========================================================================

    @staticmethod
    def _decode_cert_or_key(b64_value: str, pem_label: str) -> str:
        """
        Odkodowuje pole cert_content/key_content.

        NAPRAWA 2026-07-20: pierwotna wersja tej funkcji ZAWSZE najpierw
        robila base64.b64decode(b64_value) — zalozenie zgodne z komentarzem
        w tym pliku ("base64" — patrz docstring klasy) i z dokumentacja
        Etapu 2 ("cert_content: base64 .cert"). Ale referencyjny, oryginalny
        kod SAMARYTANIN (worker_ksef20_export.py::_ensure_pem/
        _ensure_private_key_pem), z ktorego ten adapter zostal przeniesiony,
        NIGDY nie robi base64.b64decode() na wejsciu — traktuje wartosc z
        bazy jako JUZ GOTOWY tekst (albo pelny PEM, albo sam base64-body
        do wstawienia wprost miedzy naglowki, bez dodatkowego dekodowania).

        Skoro nie wiadomo z pewnoscia, ktora z tych dwoch konwencji front
        faktycznie zastosowal przy budowaniu connection_config, ta wersja
        OBSLUGUJE OBIE, w nastepujacej kolejnosci priorytetu:

          1. Jesli b64_value JUZ zawiera '-----BEGIN' (zanim cokolwiek
             zdekodujemy) — to gotowy tekst PEM, uzyj wprost. Dokladnie tak
             jak robi to oryginalny, referencyjny kod. NAJWYZSZY priorytet,
             bo to jednoznaczny, bezpieczny sygnal — zaden dalszy krok nie
             moze tego bledne zinterpretowac.

          2. Jesli nie — sprobuj base64.b64decode(). Jesli wynikowe bajty
             sa poprawnym UTF-8 i zawieraja '-----BEGIN' — to byl to PEM
             zakodowany w base64 (zgodnie z dokumentacja Etapu 2). Uzyj
             tego tekstu.

          3. Jesli wynikowe bajty NIE sa tekstem PEM — zakladamy surowy
             binarny DER, zawijamy reczny w naglowki PEM (jak poprzednio).

          4. Jesli base64.b64decode() w ogole sie nie powiedzie (wyjatek)
             — ostatnia deska ratunku: traktujemy b64_value DOSLOWNIE jako
             juz-gotowy base64-body (bez zadnego dekodowania), dokladnie
             tak jak robi oryginalny kod referencyjny dla przypadku "nie
             jest jeszcze pelnym PEM" — zawijamy go wprost w naglowki.

        Ta kolejnosc gwarantuje poprawna obsluge NIEZALEZNIE od tego, ktora
        z dwoch konwencji (nasza dokumentacja vs. oryginal SAMARYTANIN)
        front zastosowal — nie trzeba zgadywac ani pytac.
        """
        stripped = b64_value.strip()

        # Poziom 1: juz gotowy PEM, zanim cokolwiek zdekodujemy — najwyzszy
        # priorytet, zgodnie z oryginalnym kodem referencyjnym.
        if "-----BEGIN" in stripped:
            return stripped

        # Poziom 2/3: sprobuj base64.b64decode — zgodnie z dokumentacja
        # Etapu 2 ("cert_content: base64 .cert").
        try:
            raw = base64.b64decode(stripped, validate=False)
        except Exception:
            raw = None

        if raw is not None:
            try:
                text = raw.decode("utf-8")
                if "-----BEGIN" in text:
                    return text  # Poziom 2: byl to PEM zakodowany w base64
            except UnicodeDecodeError:
                pass

            # Poziom 3: surowy binarny DER — zawijamy w PEM.
            # UWAGA: sprawdzamy czy raw faktycznie wyglada na binarne (nie
            # na przypadkowo poprawny base64 z czegos innego) — jesli
            # zdekodowane bajty sa bardzo krotkie/puste, to podejrzane,
            # ale nie blokujemy — cryptography i tak odrzuci nieprawidlowe dane
            # z czytelnym bledem przy load_pem_private_key/load_pem_x509_certificate.
            body_b64 = base64.b64encode(raw).decode("ascii")
            wrapped = "\n".join(body_b64[i:i + 64] for i in range(0, len(body_b64), 64))
            return f"-----BEGIN {pem_label}-----\n{wrapped}\n-----END {pem_label}-----"

        # Poziom 4: base64.b64decode() sie nie powiodlo — ostatnia deska
        # ratunku, zgodnie z oryginalnym kodem referencyjnym: traktujemy
        # wartosc DOSLOWNIE jako juz-gotowy base64-body, bez dekodowania.
        wrapped = "\n".join(stripped[i:i + 64] for i in range(0, len(stripped), 64))
        return f"-----BEGIN {pem_label}-----\n{wrapped}\n-----END {pem_label}-----"

    # =========================================================================
    # HTTP pomocnicze (synchroniczne — wywolywane przez asyncio.to_thread)
    # =========================================================================

    def _request_sync(self, method: str, path: str, *, headers: Optional[dict] = None, **kwargs: Any) -> requests.Response:
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        merged_headers = dict(headers or {})
        for attempt in range(1, 4):
            response = self._session.request(
                method=method, url=url, timeout=_REQUEST_TIMEOUT_SECONDS,
                headers=merged_headers or None, **kwargs,
            )
            if response.status_code == 429 and attempt < 3:
                retry_after = response.headers.get("Retry-After")
                wait_s = int(retry_after) if retry_after and retry_after.isdigit() else 10 * attempt
                logger.warning(
                    "KSeF20Adapter [%s]: HTTP 429 dla %s %s, czekam %ds (proba %d/3)",
                    self.source_name, method, path, wait_s, attempt + 1,
                )
                time.sleep(wait_s)
                continue
            return response
        return response  # nieosiagalne w praktyce, dla typechecka

    @staticmethod
    def _response_text(response: requests.Response) -> str:
        try:
            return response.text[:2000]
        except Exception:
            return ""

    # =========================================================================
    # Cache sesji w Redis + distributed lock
    # =========================================================================

    async def _load_cached_session(self) -> Optional[str]:
        """Zwraca access_token z cache, jesli wciaz wazny. None jesli brak/wygasl."""
        if not self._redis:
            return None
        try:
            raw = await self._redis.get(f"{_SESSION_KEY_PREFIX}{self.id_source}")
            if not raw:
                return None
            data = json.loads(raw)
            return data.get("access_token")
        except Exception as exc:
            logger.warning("KSeF20Adapter [%s]: blad odczytu cache sesji: %s", self.source_name, exc)
            return None

    async def _save_session_cache(self, tokens: _KSeFTokens) -> None:
        if not self._redis:
            return
        ttl_seconds = max(30, int((tokens.expires_at - datetime.now(timezone.utc)).total_seconds()) - 60)
        try:
            await self._redis.set(
                f"{_SESSION_KEY_PREFIX}{self.id_source}",
                json.dumps({
                    "access_token":     tokens.access_token,
                    "reference_number": tokens.reference_number,
                    "expires_at":       tokens.expires_at.isoformat(),
                }, ensure_ascii=False),
                ex=ttl_seconds,
            )
        except Exception as exc:
            logger.warning("KSeF20Adapter [%s]: blad zapisu cache sesji: %s", self.source_name, exc)

    async def _acquire_lock(self) -> bool:
        if not self._redis:
            return True  # brak Redis = brak ochrony, ale nie blokujemy dzialania
        try:
            acquired = await self._redis.set(
                f"{_LOCK_KEY_PREFIX}{self.id_source}", "1", ex=_LOCK_TTL_SECONDS, nx=True
            )
            return bool(acquired)
        except Exception as exc:
            logger.warning("KSeF20Adapter [%s]: blad Redis przy acquire_lock: %s", self.source_name, exc)
            return True  # fail-open — nie blokujemy synchronizacji z powodu Redis

    async def _release_lock(self) -> None:
        if not self._redis:
            return
        try:
            await self._redis.delete(f"{_LOCK_KEY_PREFIX}{self.id_source}")
        except Exception:
            pass

    # =========================================================================
    # AUTORYZACJA XAdES — port 1:1 z referencji (logika krypto/protokolu)
    # =========================================================================

    def _pobierz_challenge_sync(self) -> str:
        response = self._request_sync("POST", "/auth/challenge", json={})
        response.raise_for_status()
        data = response.json()
        challenge = data.get("challenge")
        if not challenge:
            raise KSeF20AdapterError(f"Brak challenge w odpowiedzi KSeF: {data}")
        return challenge

    def _generuj_i_podpisz_xades_sync(self, challenge: str) -> bytes:
        xml_str = f'''<?xml version="1.0" encoding="UTF-8"?>
<AuthTokenRequest xmlns="http://ksef.mf.gov.pl/auth/token/2.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <Challenge>{challenge}</Challenge>
    <ContextIdentifier>
        <Nip>{self.nip}</Nip>
    </ContextIdentifier>
    <SubjectIdentifierType>certificateSubject</SubjectIdentifierType>
</AuthTokenRequest>'''

        root = etree.fromstring(xml_str.encode("utf-8"))
        pass_bytes = self._key_password.encode("utf-8") if self._key_password else None
        private_key = serialization.load_pem_private_key(self._key_pem.encode("utf-8"), password=pass_bytes)

        if isinstance(private_key, ec.EllipticCurvePrivateKey):
            sig_alg = "ecdsa-sha256"
        elif isinstance(private_key, rsa.RSAPrivateKey):
            sig_alg = "rsa-sha256"
        else:
            raise KSeF20AdapterError(f"Nieobslugiwany typ klucza prywatnego: {type(private_key)}")

        signer = XAdESSigner(
            method=methods.enveloped,
            signature_algorithm=sig_alg,
            digest_algorithm="sha256",
            c14n_algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315",
        )
        signed_root = signer.sign(root, key=private_key, cert=self._cert_pem)
        return etree.tostring(signed_root, xml_declaration=True, encoding="utf-8", pretty_print=False)

    @staticmethod
    def _extract_token(token_obj: Any) -> Optional[str]:
        if not token_obj:
            return None
        if isinstance(token_obj, str):
            return token_obj
        if isinstance(token_obj, dict):
            return token_obj.get("token") or token_obj.get("accessToken") or token_obj.get("value")
        return None

    def _authenticate_sync(self) -> _KSeFTokens:
        """Pelny przeplyw XAdES: challenge -> podpis -> polling -> redeem tokenu."""
        challenge = self._pobierz_challenge_sync()
        signed_xml = self._generuj_i_podpisz_xades_sync(challenge)

        response = self._request_sync(
            "POST", "/auth/xades-signature", data=signed_xml,
            headers={"Content-Type": "application/xml", "Accept": "application/json",
                     "X-Error-Format": "problem-details"},
        )
        response.raise_for_status()
        init_data = response.json()

        reference_number = init_data.get("referenceNumber")
        authentication_token = self._extract_token(init_data.get("authenticationToken"))
        if not reference_number or not authentication_token:
            raise KSeF20AdapterError(f"Niepelna odpowiedz /auth/xades-signature: {init_data}")

        # Polling statusu autoryzacji
        headers = {"Authorization": f"Bearer {authentication_token}", "Accept": "application/json",
                   "X-Error-Format": "problem-details"}
        deadline = time.time() + _MAX_AUTH_WAIT_SECONDS
        while time.time() < deadline:
            response = self._request_sync("GET", f"/auth/{reference_number}", headers=headers)
            response.raise_for_status()
            body = response.json()
            status = body.get("status") or {}
            code = status.get("code")
            if code == 200:
                break
            if code and int(code) >= 400:
                raise KSeF20AdapterError(f"Uwierzytelnianie zakonczone niepowodzeniem: {body}")
            time.sleep(_AUTH_POLL_INTERVAL)
        else:
            raise TimeoutError(f"Przekroczono limit {_MAX_AUTH_WAIT_SECONDS}s oczekiwania na autoryzacje KSeF.")

        redeem_response = self._request_sync(
            "POST", "/auth/token/redeem",
            headers={"Authorization": f"Bearer {authentication_token}", "Accept": "application/json",
                     "Content-Type": "application/json", "X-Error-Format": "problem-details"},
        )
        redeem_response.raise_for_status()
        token_data = redeem_response.json()
        access_token = self._extract_token(token_data.get("accessToken"))
        if not access_token:
            raise KSeF20AdapterError(f"Brak accessToken w odpowiedzi /auth/token/redeem: {token_data}")

        self._session.headers.update({
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json", "Content-Type": "application/json",
            "X-Error-Format": "problem-details",
        })

        # UWAGA: KSeF nie zwraca jawnie expires_in w tym kroku w referencji —
        # przyjmujemy bezpieczny, konserwatywny czas zycia 20 min (typowy dla
        # tego typu sesji tokenowych). DO ZWERYFIKOWANIA z realna odpowiedzia
        # /auth/token/redeem — jesli zawiera pole np. "expiresIn"/"expiresAt",
        # nalezy tu podmienic na wartosc z odpowiedzi zamiast stalej.
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=20)

        return _KSeFTokens(reference_number=reference_number, access_token=access_token, expires_at=expires_at)

    async def _ensure_authenticated(self) -> None:
        """Uzywa cache Redis jesli wazny; w przeciwnym razie pelna autoryzacja pod lockiem."""
        cached_token = await self._load_cached_session()
        if cached_token:
            self._session.headers.update({"Authorization": f"Bearer {cached_token}"})
            logger.debug("KSeF20Adapter [%s]: uzyto sesji z cache Redis", self.source_name)
            return

        got_lock = await self._acquire_lock()
        if not got_lock:
            raise KSeF20AdapterError(
                f"KSeF20Adapter [{self.source_name}]: inna instancja workera wlasnie loguje sie do KSeF "
                f"(lock ksef_lock:{self.id_source} zajety) — pomijam ten cykl."
            )
        try:
            # Sprawdz jeszcze raz po zdobyciu locka — mogla sie pojawic w miedzyczasie
            cached_token = await self._load_cached_session()
            if cached_token:
                self._session.headers.update({"Authorization": f"Bearer {cached_token}"})
                return

            logger.info("KSeF20Adapter [%s]: rozpoczynam autoryzacje XAdES [%s]", self.source_name, self.environment)
            tokens = await asyncio.to_thread(self._authenticate_sync)
            await self._save_session_cache(tokens)
            logger.info("KSeF20Adapter [%s]: autoryzacja OK, referenceNumber=%s", self.source_name, tokens.reference_number)
        finally:
            await self._release_lock()

    # =========================================================================
    # EKSPORT PACZEK — port 1:1 z referencji
    # =========================================================================

    def _build_export_encryption_data_sync(self) -> dict[str, Any]:
        response = self._request_sync("GET", "/security/public-key-certificates")
        response.raise_for_status()
        certs = response.json()
        if not isinstance(certs, list):
            raise KSeF20AdapterError(f"Nieprawidlowa odpowiedz /security/public-key-certificates: {certs}")

        now = datetime.now(timezone.utc)
        candidates = []
        for cert in certs:
            usage = cert.get("usage") or []
            if "SymmetricKeyEncryption" not in usage:
                continue
            valid_from = self._parse_datetime(cert.get("validFrom"))
            valid_to = self._parse_datetime(cert.get("validTo"))
            if valid_from and valid_from > now:
                continue
            if valid_to and valid_to < now:
                continue
            candidates.append(cert)

        if not candidates:
            raise KSeF20AdapterError(f"Brak aktywnego certyfikatu SymmetricKeyEncryption. Odpowiedz: {certs}")

        candidates.sort(key=lambda c: self._parse_datetime(c.get("validFrom")) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        selected = candidates[0]

        certificate_b64 = selected.get("certificate")
        public_key_id = selected.get("publicKeyId")
        cert_der = base64.b64decode(certificate_b64)
        cert = x509.load_der_x509_certificate(cert_der)
        public_key = cert.public_key()

        aes_key = os.urandom(32)
        iv = os.urandom(16)
        encrypted_key = public_key.encrypt(
            aes_key,
            asym_padding.OAEP(mgf=asym_padding.MGF1(algorithm=hashes.SHA256()), algorithm=hashes.SHA256(), label=None),
        )
        return {
            "aes_key": aes_key,
            "iv": iv,
            "request_encryption": {
                "encryptedSymmetricKey": base64.b64encode(encrypted_key).decode("ascii"),
                "initializationVector": base64.b64encode(iv).decode("ascii"),
                "publicKeyId": public_key_id,
            },
        }

    def _wait_for_export_success_sync(self, reference_number: str) -> dict[str, Any]:
        deadline = time.time() + _MAX_EXPORT_WAIT_SECONDS
        while time.time() < deadline:
            response = self._request_sync("GET", f"/invoices/exports/{reference_number}")
            response.raise_for_status()
            body = response.json()
            status = body.get("status") or {}
            code = int(status.get("code") or 0)
            if code == 200:
                return body
            if code >= 400:
                raise KSeF20AdapterError(f"Eksport {reference_number} zakonczony bledem: {body}")
            time.sleep(_EXPORT_POLL_INTERVAL)
        raise TimeoutError(f"Przekroczono limit {_MAX_EXPORT_WAIT_SECONDS}s oczekiwania na eksport {reference_number}.")

    @staticmethod
    def _decrypt_aes_cbc_pkcs7(encrypted_bytes: bytes, key: bytes, iv: bytes) -> bytes:
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
        decryptor = cipher.decryptor()
        padded = decryptor.update(encrypted_bytes) + decryptor.finalize()
        unpadder = sym_padding.PKCS7(128).unpadder()
        return unpadder.update(padded) + unpadder.finalize()

    @staticmethod
    def _verify_part_hash(data: bytes, expected_b64: Optional[str], label: str, source_name: str) -> None:
        if not expected_b64:
            return
        actual_b64 = base64.b64encode(hashlib.sha256(data).digest()).decode("ascii")
        if actual_b64 != expected_b64:
            logger.warning("KSeF20Adapter [%s]: hash czesci %s nie zgadza sie z metadanymi KSeF.", source_name, label)

    def _download_decrypt_and_merge_parts_sync(self, parts: list[dict], aes_key: bytes, iv: bytes) -> bytes:
        decrypted_parts = []
        for part in sorted(parts, key=lambda p: int(p.get("ordinalNumber") or 0)):
            url = part.get("url")
            part_name = part.get("partName") or "part"
            if not url:
                raise KSeF20AdapterError(f"Czesc paczki nie zawiera URL: {part}")
            response = requests.get(url, timeout=300)
            response.raise_for_status()
            encrypted_bytes = response.content
            self._verify_part_hash(encrypted_bytes, part.get("encryptedPartHash"), f"encrypted {part_name}", self.source_name)
            decrypted = self._decrypt_aes_cbc_pkcs7(encrypted_bytes, aes_key, iv)
            self._verify_part_hash(decrypted, part.get("partHash"), f"decrypted {part_name}", self.source_name)
            decrypted_parts.append(decrypted)
        return b"".join(decrypted_parts)

    @staticmethod
    def _unzip_export_package(zip_bytes: bytes) -> dict[str, str]:
        result = {}
        with zipfile.ZipFile(BytesIO(zip_bytes), "r") as zf:
            for name in zf.namelist():
                if name.endswith("/"):
                    continue
                raw = zf.read(name)
                try:
                    result[name] = raw.decode("utf-8-sig")
                except UnicodeDecodeError:
                    result[name] = raw.decode("utf-8", errors="replace")
        return result

    @staticmethod
    def _read_export_metadata(files: dict[str, str]) -> list[dict[str, Any]]:
        for name, content in files.items():
            if name.lower().endswith("metadata.json") or name.lower().endswith(".json"):
                try:
                    data = json.loads(content)
                    invoices = data.get("invoices") if isinstance(data, dict) else None
                    if isinstance(invoices, list):
                        return [x for x in invoices if isinstance(x, dict)]
                except Exception as exc:
                    logger.warning("KSeF20Adapter: nie udalo sie odczytac metadanych z %s: %s", name, exc)
        return []

    @staticmethod
    def _extract_xml_files(files: dict[str, str]) -> list[tuple[str, str]]:
        result = []
        for name, content in files.items():
            if not name.lower().endswith(".xml"):
                continue
            base = os.path.basename(name)
            ksef_number = re.sub(r"\.xml$", "", base, flags=re.IGNORECASE).strip()
            if ksef_number:
                result.append((ksef_number, content))
        return result

    @staticmethod
    def _get_meta_value(meta: dict[str, Any], names: list[str]) -> Optional[str]:
        for name in names:
            val = meta.get(name)
            if val is not None and str(val).strip():
                return str(val).strip()
        return None

    @staticmethod
    def _is_fa3(meta: dict[str, Any]) -> bool:
        form_code = meta.get("formCode") or {}
        if not form_code:
            return True
        system_code = str(form_code.get("systemCode") or "")
        value = str(form_code.get("value") or "")
        return "FA (3)" in system_code or value == "FA"

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            parsed = value
        else:
            text = str(value).strip()
            if not text:
                return None
            text = text.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(text)
            except ValueError:
                return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @classmethod
    def _build_windows(cls, dt_from: datetime, dt_to: datetime):
        cursor = dt_from
        while cursor < dt_to:
            end = min(cursor + timedelta(days=_MAX_WINDOW_DAYS), dt_to)
            yield cursor, end
            cursor = end

    @staticmethod
    def _parse_fa3_xml_fields(xml_content: str, invoice_type: int) -> dict[str, Any]:
        """
        NAPRAWA 2026-07-20: fallback wyciagajacy wybrane pola BEZPOSREDNIO
        z tresci XML FA(3), gdy metadata.json (_meta_to_unified_document)
        nie ma ich pod oczekiwanymi kluczami (grossValue/issueDate itp.).

        Oparte na PRAWDZIWYM przykladzie XML z zywej synchronizacji
        (id_instance=574) — nie na zgadywanych nazwach pol metadata.json,
        ktorych zawartosci nigdy nie widzielismy wprost. Struktura FA(3)
        jest ustandaryzowanym formularzem MF, wiec sciezki ponizej sa
        stabilne niezaleznie od tego, co konkretnie zwraca metadata.json.

        Zwraca dict z kluczami: doc_number, doc_date (date|None),
        amount_gross (float|None), contractor_name, nip, payment_deadline
        (str|None — surowa data z XML, NIE mylic z deadline_at w
        skw_document_approval_instances, ktore jest terminem SLA obiegu
        akceptacji, calkowicie niepowiazanym z terminem platnosci faktury).

        Kazde brakujace/niesparsowane pole = None, nigdy wyjatek —
        wywolujacy uzywa tego wylacznie jako uzupelnienia brakow, nigdy
        nie nadpisuje juz obecnych wartosci z metadata.json.
        """
        result: dict[str, Any] = {
            "doc_number": None, "doc_date": None, "amount_gross": None,
            "amount_net": None, "contractor_name": None, "nip": None,
            "payment_deadline": None, "payment_form": None,
        }
        try:
            root = etree.fromstring(xml_content.encode("utf-8"))
        except Exception as exc:
            logger.warning("KSeF20Adapter: fallback XML parse nieudany: %s", exc)
            return result

        ns_uri = root.nsmap.get(None) or root.tag.split("}")[0].strip("{")
        ns = {"fa": ns_uri}

        def _text(path: str) -> Optional[str]:
            el = root.find(path, namespaces=ns)
            return el.text.strip() if el is not None and el.text else None

        result["doc_number"] = _text(".//fa:Fa/fa:P_2")

        doc_date_raw = _text(".//fa:Fa/fa:P_1")
        if doc_date_raw:
            try:
                result["doc_date"] = datetime.fromisoformat(doc_date_raw[:10]).date()
            except Exception:
                result["doc_date"] = None

        amount_raw = _text(".//fa:Fa/fa:P_15")
        if amount_raw:
            try:
                result["amount_gross"] = float(amount_raw)
            except ValueError:
                result["amount_gross"] = None

        # NOWE: kwota netto — P_13_1 (podstawa opodatkowania stawki podstawowej).
        # UWAGA: to NIE jest suma wszystkich stawek VAT na fakturze, tylko
        # pierwsza/glowna pozycja stawki — wystarczajace dla wiekszosci
        # prostych faktur jednostawkowych, ale NIE jest to w pelni ogolne
        # rozliczenie wielostawkowe. Jawnie oznaczone ograniczenie.
        net_raw = _text(".//fa:Fa/fa:P_13_1")
        if net_raw:
            try:
                result["amount_net"] = float(net_raw)
            except ValueError:
                result["amount_net"] = None

        # Kierunek kontrahenta: w FA(3) Podmiot1 = wystawca (formalnie
        # "sprzedawca"), Podmiot2 = nabywca — NIEZALEZNIE od tego, czy to
        # nasza sprzedaz czy zakup. invoice_type: 0=zakup (MY=Podmiot2,
        # kontrahent=Podmiot1/dostawca), 1=sprzedaz (MY=Podmiot1,
        # kontrahent=Podmiot2/odbiorca).
        # UWAGA: sciezka metadata-based (wyzej w tym pliku, seller.get("name"))
        # NIE robi tego rozroznienia — to swiadoma, DODATKOWA poprawka
        # wylacznie w tej sciezce fallback. Sciezka glowna metadata NIE jest
        # tu zmieniana bez osobnej decyzji.
        contractor_node = "Podmiot1" if invoice_type == 0 else "Podmiot2"
        result["contractor_name"] = _text(f".//fa:{contractor_node}/fa:DaneIdentyfikacyjne/fa:Nazwa")
        result["nip"] = _text(f".//fa:{contractor_node}/fa:DaneIdentyfikacyjne/fa:NIP")

        result["payment_deadline"] = _text(".//fa:Platnosc/fa:TerminPlatnosci/fa:Termin")

        # NOWE: forma platnosci — kod liczbowy wg slownika KSeF (1=gotowka,
        # 2=karta, 6=przelew, itd. — pelny slownik w specyfikacji FA(3),
        # nie mapujemy tu na etykiete tekstowa, zostaje surowy kod).
        result["payment_form"] = _text(".//fa:Platnosc/fa:FormaPlatnosci")

        return result

    def _meta_to_unified_document(self, ksef_number: str, xml_content: str, metadata: dict[str, Any], invoice_type: int) -> UnifiedDocument:
        """
        Mapowanie metadata.json -> UnifiedDocument, z fallbackiem do
        parsowania XML FA(3) (_parse_fa3_xml_fields) dla pol, ktorych
        metadata.json nie dostarczylo pod oczekiwanymi kluczami.

        NAPRAWA 2026-07-20: pierwotna wersja polegala WYLACZNIE na
        metadata.json (docstring mowil "nie parsujemy XML"). Na zywej
        synchronizacji (id_instance=574) grossValue/issueDate okazaly sie
        puste w prawdziwej odpowiedzi KSeF, dajac null document_amount/
        doc_date. Fallback uzupelnia WYLACZNIE brakujace pola z XML, ktory
        i tak jest juz pobrany (do raw_data) — NIGDY nie nadpisuje tego,
        co metadata.json poprawnie dostarczylo.
        """
        seller = metadata.get("seller") or {}
        buyer = metadata.get("buyer") or {}
        buyer_id = buyer.get("identifier") or {}

        doc_number = self._get_meta_value(metadata, ["invoiceNumber", "number"])
        nip1 = seller.get("nip") or seller.get("identifier")
        issue_date_raw = metadata.get("issueDate") or metadata.get("invoiceDate")
        issue_date = None
        if issue_date_raw:
            try:
                issue_date = datetime.fromisoformat(str(issue_date_raw)[:10]).date()
            except Exception:
                issue_date = None

        gross_raw = metadata.get("grossValue") or metadata.get("netValue")
        amount_gross = None
        if gross_raw is not None:
            try:
                amount_gross = float(gross_raw)
            except (TypeError, ValueError):
                amount_gross = None

        contractor_name = seller.get("name")

        # Fallback z prawdziwego XML — wolane ZAWSZE (nie tylko gdy cos
        # brakuje), bo payment_deadline nigdy nie pochodzi z metadata.json
        # niezaleznie od tego, czy reszta pol jest kompletna.
        xml_fields = self._parse_fa3_xml_fields(xml_content, invoice_type)
        doc_number      = doc_number      or xml_fields["doc_number"]
        issue_date      = issue_date      or xml_fields["doc_date"]
        amount_gross    = amount_gross    or xml_fields["amount_gross"]
        contractor_name = contractor_name or xml_fields["contractor_name"]
        nip1            = nip1            or xml_fields["nip"]

        return UnifiedDocument(
            id_document=ksef_number,
            id_source=self.id_source,
            source_name=self.source_name,
            doc_number=doc_number,
            doc_date=issue_date,
            amount_gross=amount_gross,
            contractor_name=contractor_name,
            nip=nip1,
            document_type="FA(3)" if self._is_fa3(metadata) else metadata.get("formCode", {}).get("value"),
            raw_data={
                "ksef_id":          ksef_number,
                "podmiot2":         buyer.get("name"),
                "nip2":             buyer_id.get("value") or buyer.get("nip"),
                "invoice_type":     invoice_type,  # 0=zakup, 1=sprzedaz -- patrz UWAGA w naglowku pliku
                "is_fa3":           self._is_fa3(metadata),
                "xml":              xml_content,
                # NOWE 2026-07-20: termin platnosci faktury (NIE mylic z
                # deadline_at instancji obiegu — to termin SLA akceptacji,
                # calkowicie inna, niepowiazana rzecz).
                "payment_deadline": xml_fields.get("payment_deadline"),
                # NOWE 2026-07-20 (na prosbe: "zmapuj wszystkie pola, ktore
                # mozesz"): forma platnosci (surowy kod liczbowy KSeF) i
                # kwota netto (P_13_1 — pierwsza/glowna stawka, NIE pelne
                # rozliczenie wielostawkowe, patrz komentarz w
                # _parse_fa3_xml_fields).
                "payment_form":     xml_fields.get("payment_form"),
                "amount_net":       xml_fields.get("amount_net"),
            },
        )

    def _export_subject_window_sync(self, subject_type: str, invoice_type: int, dt_from: datetime, dt_to: datetime) -> tuple[list[UnifiedDocument], bool, Optional[datetime]]:
        encryption_data = self._build_export_encryption_data_sync()

        def _fmt(d: datetime) -> str:
            return d.astimezone(timezone.utc).replace(microsecond=0).isoformat()

        payload = {
            "encryption": encryption_data["request_encryption"],
            "onlyMetadata": False,
            "filters": {
                "subjectType": subject_type,
                "dateRange": {
                    "dateType": "PermanentStorage",
                    "from": _fmt(dt_from), "to": _fmt(dt_to),
                    "restrictToPermanentStorageHwmDate": True,
                },
                "formType": "FA",
            },
            "compressionType": "Zip",
        }

        # NAPRAWA 2026-07-20 (diagnostyka tymczasowa): logujemy DOKLADNY
        # payload wysylany do KSeF — podejrzenie: restrictToPermanentStorageHwmDate=True
        # + dateType="PermanentStorage" moze przycinac zakres do punktu, do
        # ktorego KSeF oficjalnie potwierdza pelne, trwale zapisanie dokumentow —
        # jesli "to" siega za ten punkt, moze to tlumaczyc blad 420. Usunac
        # po zdiagnozowaniu.
        logger.info(
            "KSeF20Adapter [%s]: DIAG payload /invoices/exports | subject=%s "
            "dateRange.from=%s dateRange.to=%s restrictToPermanentStorageHwmDate=%s",
            self.source_name, subject_type,
            payload["filters"]["dateRange"]["from"],
            payload["filters"]["dateRange"]["to"],
            payload["filters"]["dateRange"]["restrictToPermanentStorageHwmDate"],
        )

        response = self._request_sync(
            "POST", "/invoices/exports", json=payload,
            headers={"Accept": "application/json", "Content-Type": "application/json", "X-Error-Format": "problem-details"},
        )
        response.raise_for_status()
        export_init = response.json()
        reference_number = export_init.get("referenceNumber")
        if not reference_number:
            raise KSeF20AdapterError(f"Brak referenceNumber w odpowiedzi /invoices/exports: {export_init}")

        status_data = self._wait_for_export_success_sync(reference_number)
        package = status_data.get("package") or {}
        parts = package.get("parts") or []
        if not parts:
            return [], False, None

        zip_bytes = self._download_decrypt_and_merge_parts_sync(parts, encryption_data["aes_key"], encryption_data["iv"])
        files = self._unzip_export_package(zip_bytes)

        metadata_list = self._read_export_metadata(files)
        metadata_by_ksef = {}
        for meta in metadata_list:
            ksef_number = self._get_meta_value(meta, ["ksefNumber", "invoiceReferenceNumber", "invoiceKsefNumber"])
            if ksef_number:
                metadata_by_ksef[ksef_number] = meta

        xml_items = self._extract_xml_files(files)
        documents: list[UnifiedDocument] = []

        for ksef_number, xml_content in xml_items:
            metadata = metadata_by_ksef.get(ksef_number) or {"ksefNumber": ksef_number}
            if not self._is_fa3(metadata):
                continue
            documents.append(self._meta_to_unified_document(ksef_number, xml_content, metadata, invoice_type))

        is_truncated = bool(package.get("isTruncated"))
        next_from = self._parse_datetime(package.get("lastPermanentStorageDate")) if is_truncated else None
        return documents, is_truncated, next_from

    # =========================================================================
    # INTERFEJS BaseDocumentAdapter
    # =========================================================================

    async def fetch_new_documents(self, since: Optional[datetime], limit: int = 500) -> list[UnifiedDocument]:
        """
        Pobiera nowe faktury z KSeF 2.0 za pomoca mechanizmu paczek eksportowych.

        since:  poprzedni last_sync_at zrodla. Jesli None — uzywamy dtstart z
                connection_config (pierwsza synchronizacja).
        limit:  UWAGA — mechanizm paczek KSeF nie wspiera limitu liczby rekordow
                w prosty sposob (paczka zwraca wszystko z okna dat). Parametr
                jest tu ignorowany — okno dat i tak jest dzielone po 89 dniach
                (_build_windows), co naturalnie ogranicza rozmiar pojedynczej
                paczki. Jesli to problem przy bardzo duzym wolumenie, wymaga
                dalszej rozbudowy (dzielenie okna takze na mniejsze przedzialy
                czasowe wewnatrz jednego dnia).
        """
        await self._ensure_authenticated()

        # NAPRAWA 2026-07-20: 'since' pochodzi z zewnatrz (source.last_sync_at
        # odczytane z MSSQL DATETIME2 przez pyodbc/SQLAlchemy — domyslnie
        # NAIVE, bez tzinfo), podczas gdy _parse_dtstart() i dt_to ponizej
        # sa ZAWSZE aware (timezone.utc). Bez tej normalizacji porownanie
        # 'dt_from >= dt_to' rzuca "can't compare offset-naive and
        # offset-aware datetimes" przy KAZDEJ synchronizacji NASTEPUJACEJ
        # po pierwszej udanej (bo dopiero wtedy last_sync_at != None).
        # Ten sam wzorzec normalizacji juz zastosowany w tym pliku
        # (_parse_datetime, linie 589-591) i w document_source.py::needs_sync
        # — swiadoma spojnosc z istniejaca konwencja projektu.
        if since is not None and since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)

        dt_from = since or self._parse_dtstart()
        dt_to = datetime.now(timezone.utc)

        # NAPRAWA 2026-07-20 (diagnostyka tymczasowa): logujemy DOKLADNE
        # wartosci zakresu przed wyslaniem do KSeF, zeby ustalic przyczyne
        # bledu 420 "Zakres filtrowania wykracza poza dostepny zakres danych"
        # bez dalszego zgadywania. Usunac po zdiagnozowaniu.
        logger.info(
            "KSeF20Adapter [%s]: DIAG zakres eksportu | since_raw=%s "
            "dtstart_raw=%s dt_from=%s dt_to=%s (dt_to - dt_from = %s)",
            self.source_name, since, self._dtstart_raw,
            dt_from.isoformat(), dt_to.isoformat(), dt_to - dt_from,
        )

        if dt_from >= dt_to:
            logger.warning(
                "KSeF20Adapter [%s]: DIAG dt_from >= dt_to — zwracam pusta liste, "
                "bez wysylania zadania do KSeF (dt_from=%s, dt_to=%s)",
                self.source_name, dt_from.isoformat(), dt_to.isoformat(),
            )
            return []

        all_documents: list[UnifiedDocument] = []

        # Subject1 = sprzedaz (invoice_type=1), Subject2 = zakup (invoice_type=0)
        for subject_type, invoice_type in (("Subject1", 1), ("Subject2", 0)):
            for win_from, win_to in self._build_windows(dt_from, dt_to):
                current_from = win_from
                while current_from < win_to:
                    docs, is_truncated, next_from = await asyncio.to_thread(
                        self._export_subject_window_sync, subject_type, invoice_type, current_from, win_to
                    )
                    all_documents.extend(docs)
                    if is_truncated and next_from and next_from > current_from:
                        logger.warning(
                            "KSeF20Adapter [%s]: paczka %s obcieta, kontynuuje od %s",
                            self.source_name, subject_type, next_from.isoformat(),
                        )
                        current_from = next_from
                        continue
                    break

        logger.info(
            "KSeF20Adapter [%s]: fetch_new_documents zakonczone | dokumentow=%d od=%s do=%s",
            self.source_name, len(all_documents), dt_from.isoformat(), dt_to.isoformat(),
        )
        return all_documents

    def _parse_dtstart(self) -> datetime:
        if not self._dtstart_raw:
            raise KSeF20AdapterError(
                f"KSeF20Adapter [{self.source_name}]: brak 'dtstart' w connection_config "
                f"i brak poprzedniego last_sync_at — nie wiem od jakiej daty zaczac."
            )
        parsed = self._parse_datetime(str(self._dtstart_raw))
        if not parsed:
            raise KSeF20AdapterError(f"KSeF20Adapter [{self.source_name}]: nieprawidlowy format dtstart: {self._dtstart_raw}")
        return parsed