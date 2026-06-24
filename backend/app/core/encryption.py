# backend/app/core/encryption.py
"""
Szyfrowanie/deszyfrowanie wartosci wrazliwych — Fernet (AES-128-CBC + HMAC).

Uzycie: connection_config w skw_document_sources (hasla, tokeny, certyfikaty).
Klucz: ENCRYPTION_KEY w .env — 32 bajty base64url (generuj: Fernet.generate_key()).

Funkcje publiczne:
    encrypt_value(plaintext: str) -> str   — zwraca zaszyfrowany base64 string
    decrypt_value(ciphertext: str) -> str  — zwraca odszyfrowany plaintext
    generate_key() -> str                  — generuje nowy klucz (do .env)

NIGDY nie loguj plaintext ani klucza. Bledy szyfrowania -> EncryptionError.

Kompatybilnosc wsteczna:
    Jesli pole connection_config zawiera prawidlowy JSON bez szyfrowania
    (legacy — przed Etapem 2), decrypt_value zwraca go tak jak jest.
    Pozwala to na plynna migracje istniejacych wpisow.
"""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from typing import Final

logger = logging.getLogger(__name__)

# Prefiks szyfrowanego stringa — pozwala odroznic zaszyfrowane od legacy JSON
_ENCRYPTED_PREFIX: Final[str] = "enc:"


class EncryptionError(Exception):
    """Blad szyfrowania lub deszyfrowania."""


@lru_cache(maxsize=1)
def _get_fernet():
    """
    Zwraca instancje Fernet skonfigurowana kluczem z settings.
    Lazy — nie laduje kryptografii przy imporcie modulu.
    Cachowana — jeden obiekt przez caly czas zycia procesu.
    """
    try:
        from cryptography.fernet import Fernet
        from app.core.config import get_settings

        settings = get_settings()
        key = settings.encryption_key

        if hasattr(key, "get_secret_value"):
            key = key.get_secret_value()

        if not key:
            raise EncryptionError(
                "ENCRYPTION_KEY nie jest ustawiony w .env. "
                "Wygeneruj klucz: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
            )

        return Fernet(key.encode() if isinstance(key, str) else key)

    except ImportError as exc:
        raise EncryptionError(
            "Brak biblioteki cryptography. Zainstaluj: pip install cryptography"
        ) from exc


def encrypt_value(plaintext: str) -> str:
    """
    Szyfruje string algorytmem Fernet.

    Args:
        plaintext: Dowolny string (typowo JSON z danymi polaczenia).

    Returns:
        Zaszyfrowany string z prefiksem 'enc:' + base64.

    Raises:
        EncryptionError: Jesli szyfrowanie sie nie powiedzie.
    """
    if not plaintext:
        return plaintext

    # Jesli juz zaszyfrowany — nie szyfruj ponownie
    if plaintext.startswith(_ENCRYPTED_PREFIX):
        logger.warning("encrypt_value: wartosc juz zaszyfrowana — pomijam")
        return plaintext

    try:
        fernet = _get_fernet()
        token = fernet.encrypt(plaintext.encode("utf-8"))
        return _ENCRYPTED_PREFIX + token.decode("ascii")
    except EncryptionError:
        raise
    except Exception as exc:
        # Nie logujemy plaintext
        raise EncryptionError(f"Blad szyfrowania: {type(exc).__name__}") from exc


def decrypt_value(ciphertext: str) -> str:
    """
    Deszyfruje string zaszyfrowany przez encrypt_value.

    Kompatybilnosc wsteczna: jesli wartosc nie ma prefiksu 'enc:'
    i jest prawidlowym JSON-em, zwraca ja bez zmian (legacy connection_config).

    Args:
        ciphertext: Zaszyfrowany string (z prefiksem 'enc:') lub legacy JSON.

    Returns:
        Odszyfrowany plaintext.

    Raises:
        EncryptionError: Jesli deszyfrowanie sie nie powiedzie.
    """
    if not ciphertext:
        return ciphertext

    # Kompatybilnosc wsteczna — niezaszyfrowany JSON (legacy)
    if not ciphertext.startswith(_ENCRYPTED_PREFIX):
        try:
            json.loads(ciphertext)
            logger.debug("decrypt_value: wartosc nie zaszyfrowana (legacy JSON) — zwracam as-is")
            return ciphertext
        except json.JSONDecodeError:
            raise EncryptionError(
                "Wartosc nie jest ani zaszyfrowana ('enc:' prefix) ani prawidlowym JSON-em"
            )

    token_str = ciphertext[len(_ENCRYPTED_PREFIX):]

    try:
        fernet = _get_fernet()
        plaintext = fernet.decrypt(token_str.encode("ascii"))
        return plaintext.decode("utf-8")
    except EncryptionError:
        raise
    except Exception as exc:
        raise EncryptionError(
            f"Blad deszyfrowania: {type(exc).__name__}. "
            "Sprawdz czy ENCRYPTION_KEY w .env jest poprawny."
        ) from exc


def generate_key() -> str:
    """
    Generuje nowy klucz Fernet. Wynik wklej do .env jako ENCRYPTION_KEY.
    Uzywac tylko raz przy konfiguracji srodowiska.
    """
    try:
        from cryptography.fernet import Fernet
        return Fernet.generate_key().decode("ascii")
    except ImportError as exc:
        raise EncryptionError("Brak biblioteki cryptography") from exc


def rotate_key(old_key: str, new_key: str, ciphertext: str) -> str:
    """
    Rotacja klucza: deszyfruje starym kluczem, szyfruje nowym.
    Uzywane przy zmianie ENCRYPTION_KEY (procedura migracji kluczy).

    Args:
        old_key:    Stary klucz Fernet (base64).
        new_key:    Nowy klucz Fernet (base64).
        ciphertext: Zaszyfrowana wartosc (starym kluczem).

    Returns:
        Wartosc zaszyfrowana nowym kluczem.
    """
    try:
        from cryptography.fernet import Fernet, MultiFernet

        old_fernet = Fernet(old_key.encode() if isinstance(old_key, str) else old_key)
        new_fernet = Fernet(new_key.encode() if isinstance(new_key, str) else new_key)
        multi = MultiFernet([new_fernet, old_fernet])

        token_str = ciphertext.removeprefix(_ENCRYPTED_PREFIX)
        plaintext = multi.decrypt(token_str.encode("ascii"))
        new_token = new_fernet.encrypt(plaintext)
        return _ENCRYPTED_PREFIX + new_token.decode("ascii")
    except Exception as exc:
        raise EncryptionError(f"Blad rotacji klucza: {type(exc).__name__}") from exc