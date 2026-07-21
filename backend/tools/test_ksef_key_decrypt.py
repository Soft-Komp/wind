# -*- coding: utf-8 -*-
"""
Test bezposredni: probuje odszyfrowac PRAWDZIWY plik .key (bez przechodzenia
przez base64/JSON/Fernet connection_config) dokladnie ta sama funkcja co
KSeF20Adapter (serialization.load_pem_private_key).

Izoluje pytanie: czy problem jest w SAMYM hasle/kluczu, czy w potoku
kodowania (base64 -> JSON -> Fernet -> deszyfrowanie) miedzy frontem
a adapterem.

UZYCIE:
    python test_key_direct.py /tmp/test.key

Haslo podawane INTERAKTYWNIE (getpass) — nie trafia do historii powlok,
nie jest logowane, nie jest wypisywane nigdzie w outpucie.

NIC z tresci klucza ani hasla nie jest wypisywane — tylko wynik sukces/porazka
i typ klucza (RSA/EC) w razie sukcesu.
"""
import sys
import getpass

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa


def main() -> None:
    if len(sys.argv) != 2:
        print("Uzycie: python test_key_direct.py /sciezka/do/pliku.key")
        sys.exit(1)

    path = sys.argv[1]
    with open(path, "rb") as f:
        raw = f.read()

    print(f"Plik: {path}")
    print(f"Rozmiar: {len(raw)} bajtow")

    # Sprawdzenie strukturalne — czy to w ogole wyglada jak PEM
    try:
        text = raw.decode("utf-8")
        first_line = text.split("\n")[0].strip()
        print(f"Pierwsza linia: {first_line}")
        is_pem = "-----BEGIN" in text
    except UnicodeDecodeError:
        print("BLAD: plik nie jest czystym tekstem UTF-8 (nie wyglada na PEM tekstowy).")
        is_pem = False

    if not is_pem:
        print("STOP: plik nie ma naglowka PEM ('-----BEGIN') — to nie jest zwykly")
        print("tekstowy plik .key, jakiego oczekuje adapter. To juz jest odpowiedz:")
        print("problem NIE jest w hasle, tylko w formacie/tresci samego pliku.")
        sys.exit(1)

    password = getpass.getpass("Podaj haslo do klucza (Enter jesli klucz NIE jest zaszyfrowany): ")
    pass_bytes = password.encode("utf-8") if password else None

    print()
    print("--- Proba 1: z podanym haslem ---")
    try:
        private_key = serialization.load_pem_private_key(raw, password=pass_bytes)
        if isinstance(private_key, ec.EllipticCurvePrivateKey):
            print("SUKCES — klucz odszyfrowany poprawnie. Typ: EC (ECDSA)")
        elif isinstance(private_key, rsa.RSAPrivateKey):
            print("SUKCES — klucz odszyfrowany poprawnie. Typ: RSA")
        else:
            print(f"SUKCES — klucz odszyfrowany, ale nieoczekiwany typ: {type(private_key)}")
        print()
        print("WNIOSEK: haslo i plik .key SA POPRAWNE, gdy uzywane bezposrednio.")
        print("Jesli adapter mimo to zwraca 'Incorrect password' na prawdziwym")
        print("zrodle — problem jest w POTOKU KODOWANIA (base64/JSON/Fernet),")
        print("NIE w samym hasle/kluczu.")
        return
    except TypeError as exc:
        print(f"BLAD TypeError: {exc}")
        print("(czesty przypadek: podano haslo, ale klucz NIE jest zaszyfrowany —")
        print(" sprobuj ponownie, nacisnij Enter bez hasla)")
    except ValueError as exc:
        print(f"BLAD ValueError: {exc}")
        print()
        print("--- Proba 2: bez hasla (na wypadek gdyby klucz NIE byl zaszyfrowany) ---")
        try:
            private_key = serialization.load_pem_private_key(raw, password=None)
            print("SUKCES bez hasla — klucz WCALE nie byl zaszyfrowany!")
            print("WNIOSEK: podawanie key_password w connection_config dla tego")
            print("klucza jest BLEDEM — powinno byc puste/None.")
        except Exception as exc2:
            print(f"Rowniez nieudane: {exc2}")
            print()
            print("WNIOSEK: ani podane haslo, ani brak hasla nie dziala na tym")
            print("PRAWDZIWYM pliku .key. To wskazuje na: (a) faktycznie zle haslo,")
            print("(b) uszkodzony/niekompletny plik .key, albo (c) plik zaszyfrowany")
            print("algorytmem nieobslugiwanym przez zainstalowana wersje OpenSSL.")


if __name__ == "__main__":
    main()