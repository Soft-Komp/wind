# -*- coding: utf-8 -*-
"""
Test z WKLEJONA WARTOSCIA klucza (nie z pliku) — testuje NARAZ dwie rzeczy:

  1. Czy sam klucz + haslo sa poprawne (identycznie jak test_ksef_key_decrypt.py).
  2. Czy proces kodowania base64 -> _decode_cert_or_key() (DOKLADNIE ta sama
     funkcja co w ksef20_adapter.py, skopiowana verbatim ponizej) — czyli
     symulacja calego potoku "tekst PEM -> tak jak front zakodowalby to do
     connection_config -> tak jak adapter to odkoduje" — NIE wprowadza
     zadnej korupcji danych.

Eliminuje potrzebe docker cp (i zwiazane z tym ryzyko, ze transfer pliku
przez Windows/Docker cos po drodze zmienil, np. konce linii CRLF/LF).

UZYCIE:
  1. Otworz swoj plik .key w dowolnym edytorze tekstu.
  2. Zaznacz CALA zawartosc (od "-----BEGIN" do "-----END..." wlacznie).
  3. Wklej ja PONIZEJ, pomiedzy znaczniki PASTE_KEY_HERE_START/END,
     zachowujac oryginalne formatowanie/laczenie linii.
  4. Uruchom: python test_key_pasted_value.py
  5. Podaj haslo gdy zapyta (getpass — nie trafia do historii/logow).

NIC z tresci klucza ani hasla nie jest wypisywane w outpucie.
"""
import base64
import getpass

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa

# =============================================================================
# WKLEJ TU CALA ZAWARTOSC PLIKU .key (miedzy potrojnymi cudzyslowami)
# =============================================================================
PASTE_KEY_HERE_START = """
-----BEGIN ENCRYPTED PRIVATE KEY-----
MIHzMF4GCSqGSIb3DQEFDTBRMDAGCSqGSIb3DQEFDDAjBBAAYkLOsokJeR11opmR
k2ryAgMBhqAwCgYIKoZIhvcNAgkwHQYJYIZIAWUDBAEqBBAcBzTPU/JGY+V2eqlj
ZHv4BIGQJ1krZu0LUtgoWLWdgpLYC3/nQQ9JKJVLqEzobktXbdFJWBekC8Now4A3
FlO29q+l9+k/9ISK7DyDWXs8sSw4rB/WDR57D81hPeRU3EONpyo6fpUHXr75Ztkr
ocTucQtO51FH2JZuinL+OUCYhhG5Ez8+Qxy4ckUnZMlbZrLhzXKZvCZO6xLEaAIi
SnEY3PU3
-----END ENCRYPTED PRIVATE KEY-----
"""
# =============================================================================


def decode_cert_or_key(b64_value: str, pem_label: str) -> str:
    """
    KOPIA VERBATIM funkcji _decode_cert_or_key() z
    worker/services/ksef20_adapter.py — identyczna logika, zeby test
    faktycznie sprawdzal TEN SAM kod co produkcja, nie przyblizenie.
    """
    raw = base64.b64decode(b64_value, validate=False)
    try:
        text = raw.decode("utf-8")
        if "-----BEGIN" in text:
            return text
    except UnicodeDecodeError:
        pass
    body_b64 = base64.b64encode(raw).decode("ascii")
    wrapped = "\n".join(body_b64[i:i + 64] for i in range(0, len(body_b64), 64))
    return f"-----BEGIN {pem_label}-----\n{wrapped}\n-----END {pem_label}-----"


def try_load(label: str, pem_text: str, pass_bytes: bytes | None) -> bool:
    print(f"--- {label} ---")
    try:
        private_key = serialization.load_pem_private_key(
            pem_text.encode("utf-8"), password=pass_bytes,
        )
        if isinstance(private_key, ec.EllipticCurvePrivateKey):
            print("SUKCES — typ: EC (ECDSA)")
        elif isinstance(private_key, rsa.RSAPrivateKey):
            print("SUKCES — typ: RSA")
        else:
            print(f"SUKCES — nieoczekiwany typ: {type(private_key)}")
        return True
    except TypeError as exc:
        print(f"BLAD TypeError: {exc}")
    except ValueError as exc:
        print(f"BLAD ValueError: {exc}")
    return False


def main() -> None:
    original_text = PASTE_KEY_HERE_START.strip() + "\n"

    if "WKLEJ_TU_PELNA_TRESC_PLIKU_KEY" in original_text:
        print("STOP: nie wkleiles jeszcze prawdziwej tresci klucza do skryptu.")
        print("Edytuj plik, zamien placeholder miedzy potrojnymi cudzyslowami")
        print("na PELNA zawartosc Twojego pliku .key, i uruchom ponownie.")
        return

    print(f"Dlugosc wklejonego tekstu: {len(original_text)} znakow")
    print(f"Pierwsza linia: {original_text.splitlines()[0]!r}")
    print(f"Ostatnia linia: {original_text.splitlines()[-1]!r}")
    print()

    password = getpass.getpass("Podaj haslo do klucza (Enter jesli klucz NIE jest zaszyfrowany): ")
    pass_bytes = password.encode("utf-8") if password else None

    print()
    # Test 1: bezposrednio, bez zadnego kodowania — czy sam tekst + haslo dzialaja
    ok_direct = try_load("Test 1: wklejony tekst bezposrednio (bez base64 roundtrip)", original_text, pass_bytes)

    print()
    # Test 2: symulacja PELNEGO potoku — base64 encode (jak zrobilby front),
    # potem decode_cert_or_key() (dokladnie jak w produkcji)
    simulated_b64 = base64.b64encode(original_text.encode("utf-8")).decode("ascii")
    roundtripped_text = decode_cert_or_key(simulated_b64, "ENCRYPTED PRIVATE KEY")

    if roundtripped_text != original_text:
        print("UWAGA: tekst PO przejsciu przez base64 roundtrip + _decode_cert_or_key()")
        print("RÓŻNI SIE od oryginalu wklejonego tekstu!")
        print(f"  Dlugosc oryginalu:      {len(original_text)}")
        print(f"  Dlugosc po roundtripie: {len(roundtripped_text)}")
    else:
        print("Tekst po base64 roundtrip + _decode_cert_or_key() IDENTYCZNY z oryginalem.")

    ok_roundtrip = try_load(
        "Test 2: po symulacji pelnego potoku (base64 -> _decode_cert_or_key)",
        roundtripped_text, pass_bytes,
    )

    print()
    print("=" * 70)
    if ok_direct and ok_roundtrip:
        print("WNIOSEK: klucz+haslo sa poprawne, i potok kodowania NIE psuje danych.")
        print("Jesli adapter na prawdziwym zrodle nadal zwraca blad — szukaj w")
        print("deszyfrowaniu Fernet connection_config (miedzy zapisem do bazy")
        print("a odczytem przez adapter), nie w samym kluczu ani w _decode_cert_or_key().")
    elif ok_direct and not ok_roundtrip:
        print("WNIOSEK: klucz+haslo SA poprawne bezposrednio, ale POTOK KODOWANIA")
        print("(base64 -> _decode_cert_or_key) PSUJE dane! To jest realny bug")
        print("w _decode_cert_or_key() albo w zalozeniach co do formatu.")
    elif not ok_direct:
        print("WNIOSEK: nawet BEZPOSREDNIO (bez zadnego kodowania), ten klucz")
        print("z tym haslem sie nie odszyfrowuje. To wskazuje na haslo/plik,")
        print("nie na potok kodowania w naszym kodzie.")
    print("=" * 70)


if __name__ == "__main__":
    main()