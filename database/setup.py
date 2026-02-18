#!/usr/bin/env python3
"""
database/setup.py — Skrypt konfiguracyjny środowiska windykacja-system.

Odpowiada za czynności, których NIE można zautomatyzować przez seed SQL:
  1. Hashowanie PIN-u Master Key (bcrypt rounds=12) → UPDATE SystemConfig
  2. Hashowanie hasła admina (argon2id) → UPDATE Users
  3. Weryfikacja połączenia z bazą danych
  4. Uruchomienie seedów SQL w poprawnej kolejności (opcjonalne)

Użycie:
    # Ustawienie PIN-u Master Key (interaktywne, bez echa terminala):
    python database/setup.py --set-master-pin

    # Ustawienie hasła admina:
    python database/setup.py --set-admin-password

    # Weryfikacja połączenia:
    python database/setup.py --verify

    # Uruchomienie wszystkich seedów (jeśli sqlcmd dostępny):
    python database/setup.py --run-seeds

    # Pełna inicjalizacja (verify + seeds + pin + admin-password):
    python database/setup.py --full-init

Wymagania:
    pip install pyodbc bcrypt argon2-cffi python-dotenv

Środowisko: wczytuje z .env w katalogu nadrzędnym lub ze zmiennych systemowych.

BEZPIECZEŃSTWO:
    - PIN i hasło NIGDY nie są logowane ani zapisywane w plikach
    - Hashe są przesyłane do bazy przez parametryzowane zapytania SQL
    - Połączenie z bazą przez ODBC Driver 18 (TLS 1.2+)
    - Skrypt sprawdza czy działa w trybie interaktywnym (nie przez CI/CD)
"""

from __future__ import annotations

import argparse
import getpass
import json
import logging
import os
import subprocess
import sys
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ─── Zależności opcjonalne ─────────────────────────────────────────────────────
# Sprawdzamy przed importem — dajemy czytelny komunikat jeśli brakuje pakietów
_missing: list[str] = []

try:
    import pyodbc
except ImportError:
    _missing.append("pyodbc")

try:
    import bcrypt
except ImportError:
    _missing.append("bcrypt")

try:
    from argon2 import PasswordHasher
    from argon2.exceptions import VerifyMismatchError
except ImportError:
    _missing.append("argon2-cffi")

try:
    from dotenv import load_dotenv
except ImportError:
    # python-dotenv opcjonalne — możemy bez niego
    def load_dotenv(*args, **kwargs) -> None:  # type: ignore[misc]
        pass

if _missing:
    print(f"BŁĄD: Brakujące pakiety: {', '.join(_missing)}")
    print(f"Uruchom: pip install {' '.join(_missing)}")
    sys.exit(1)


# ─── Konfiguracja logowania ────────────────────────────────────────────────────

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)

# Log do pliku setup.log obok skryptu
_log_file = Path(__file__).parent / "setup.log"
_file_handler = logging.FileHandler(_log_file, encoding="utf-8")
_file_handler.setFormatter(
    logging.Formatter(
        '{"time": "%(asctime)s", "level": "%(levelname)s", "msg": %(message)s}'
    )
)
logging.getLogger().addHandler(_file_handler)

logger = logging.getLogger("windykacja.setup")


# ─── Stałe ────────────────────────────────────────────────────────────────────

SCHEMA = "dbo_ext"
SEEDS_DIR = Path(__file__).parent / "seeds"
SEEDS_ORDER = [
    "01_roles.sql",
    "02_permissions.sql",
    "03_role_permissions.sql",
    "04_admin_user.sql",
    "05_system_config.sql",
]

# Argon2id — dla haseł użytkowników (zgodnie z USTALENIAMI)
ARGON2_HASHER = PasswordHasher(
    time_cost=2,
    memory_cost=65536,   # 64 MB
    parallelism=2,
    hash_len=32,
    salt_len=16,
)

# Bcrypt rounds dla PIN-u Master Key (zgodnie z AUDIT_ZGODNOSCI R6)
BCRYPT_ROUNDS = 12


# ─── Połączenie z bazą ────────────────────────────────────────────────────────


def _load_env() -> None:
    """Wczytuje .env z katalogu nadrzędnego (jeśli istnieje)."""
    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        load_dotenv(env_file)
        logger.info('"Załadowano .env z: %s"', env_file)
    else:
        logger.info('"Brak pliku .env — używam zmiennych środowiskowych"')


def _build_connection_string() -> str:
    """Buduje connection string ODBC z .env / zmiennych środowiskowych."""
    host   = os.environ.get("DB_HOST", "host.docker.internal")
    port   = os.environ.get("DB_PORT", "1433")
    db     = os.environ.get("DB_NAME", "WAPRO")
    user   = os.environ.get("DB_USER", "")
    passwd = os.environ.get("DB_PASSWORD", "")
    driver = os.environ.get("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

    if not user or not passwd:
        raise ValueError(
            "Brak DB_USER lub DB_PASSWORD w .env / zmiennych środowiskowych.\n"
            "Uzupełnij plik .env lub ustaw zmienne: DB_USER, DB_PASSWORD"
        )

    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={host},{port};"
        f"DATABASE={db};"
        f"UID={user};"
        f"PWD={passwd};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=10;"
    )


def get_connection() -> "pyodbc.Connection":
    """Otwiera połączenie z MSSQL. Wyrzuca wyjątek przy błędzie."""
    conn_str = _build_connection_string()
    logger.info(
        '"Łączenie z bazą: SERVER=%s DB=%s"',
        os.environ.get("DB_HOST", "?"),
        os.environ.get("DB_NAME", "?"),
    )
    try:
        conn = pyodbc.connect(conn_str, autocommit=False)
        conn.setdecoding(pyodbc.SQL_CHAR, encoding="utf-8")
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-8")
        conn.setencoding(encoding="utf-8")
        return conn
    except pyodbc.Error as exc:
        logger.error('"Błąd połączenia z bazą: %s"', str(exc))
        raise


# ─── Komendy ──────────────────────────────────────────────────────────────────


def cmd_verify() -> bool:
    """Weryfikuje połączenie z bazą i istnienie tabel dbo_ext."""
    print("\n── Weryfikacja połączenia ─────────────────────────────────────────")
    try:
        conn = get_connection()
        cur = conn.cursor()

        # Test podstawowy
        cur.execute("SELECT @@VERSION AS Version;")
        row = cur.fetchone()
        version_line = row[0].split("\n")[0] if row else "?"
        print(f"✅ Połączenie OK: {version_line}")

        # Sprawdź schemat
        cur.execute(
            "SELECT COUNT(*) FROM sys.schemas WHERE name = N'dbo_ext';"
        )
        schema_exists = cur.fetchone()[0] > 0
        print(f"{'✅' if schema_exists else '❌'} Schemat dbo_ext: "
              f"{'istnieje' if schema_exists else 'BRAK'}")

        # Sprawdź tabele
        expected_tables = [
            "Roles", "Permissions", "Users", "RolePermissions",
            "RefreshTokens", "OtpCodes", "Templates", "AuditLog",
            "MonitHistory", "SystemConfig", "SchemaChecksums",
            "MasterAccessLog", "Comments",
        ]
        cur.execute(
            f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = N'{SCHEMA}'
              AND TABLE_TYPE = N'BASE TABLE'
            ORDER BY TABLE_NAME;
            """
        )
        existing = {row[0] for row in cur.fetchall()}
        for table in expected_tables:
            mark = "✅" if table in existing else "❌"
            print(f"  {mark} {SCHEMA}.{table}")

        missing = set(expected_tables) - existing
        if missing:
            print(f"\n❌ Brakujące tabele ({len(missing)}): {', '.join(sorted(missing))}")
            print("   Uruchom migracje: alembic upgrade head")
            conn.close()
            return False

        # Sprawdź SystemConfig — klucz pin_hash
        cur.execute(
            f"SELECT [ConfigValue] FROM [{SCHEMA}].[SystemConfig] "
            f"WHERE [ConfigKey] = N'master_key.pin_hash';"
        )
        row = cur.fetchone()
        pin_set = row and row[0] and row[0].strip()
        print(
            f"\n{'✅' if pin_set else '⚠️ '} master_key.pin_hash: "
            f"{'ustawiony' if pin_set else 'PUSTY — wymagana konfiguracja'}"
        )
        if not pin_set:
            print("   Uruchom: python database/setup.py --set-master-pin")

        conn.close()
        print("\n✅ Weryfikacja zakończona.\n")
        return True

    except Exception as exc:
        print(f"\n❌ Błąd: {exc}")
        logger.error('"Weryfikacja nieudana: %s"', str(exc))
        return False


def cmd_set_master_pin() -> None:
    """
    Interaktywne ustawienie PIN-u Master Key.

    Pyta o PIN (getpass — bez echa), hashuje bcrypt, zapisuje do bazy.
    Nigdy nie loguje surowego PIN-u.
    """
    print("\n── Ustawianie PIN-u Master Key ────────────────────────────────────")
    print("PIN musi mieć 4–8 cyfr. Wartość nie będzie wyświetlana na ekranie.")
    print("Obecna wartość zostanie ZASTĄPIONA.\n")

    if not sys.stdin.isatty():
        print("BŁĄD: Skrypt musi być uruchomiony w trybie interaktywnym (terminal).")
        print("Nie można bezpiecznie pobrać PIN-u przez pipe/redirection.")
        sys.exit(1)

    # Pobierz PIN dwukrotnie (weryfikacja)
    pin1 = getpass.getpass("Podaj PIN (4–8 cyfr): ")
    pin2 = getpass.getpass("Powtórz PIN: ")

    if pin1 != pin2:
        print("❌ PINy nie są identyczne. Anulowano.")
        sys.exit(1)

    if not pin1.isdigit() or not (4 <= len(pin1) <= 8):
        print("❌ PIN musi składać się z 4–8 cyfr. Anulowano.")
        sys.exit(1)

    # Hash bcrypt — zgodnie z AUDIT_ZGODNOSCI R6 i USTALENIA §8
    print("\nHashowanie PIN-u (bcrypt rounds=12)...")
    pin_hash: bytes = bcrypt.hashpw(pin1.encode("utf-8"), bcrypt.gensalt(rounds=BCRYPT_ROUNDS))
    pin_hash_str: str = pin_hash.decode("utf-8")

    # Cleanup surowego PIN-u z pamięci (best-effort)
    del pin1, pin2

    # Weryfikacja własna przed zapisem
    logger.info('"master_key.pin_hash wygenerowany (długość: %d)"', len(pin_hash_str))

    # Zapis do bazy
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            f"""
            MERGE [{SCHEMA}].[SystemConfig] AS target
            USING (SELECT N'master_key.pin_hash' AS ConfigKey) AS source
                ON target.[ConfigKey] = source.[ConfigKey]
            WHEN MATCHED THEN
                UPDATE SET
                    [ConfigValue] = ?,
                    [UpdatedAt]   = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT ([ConfigKey], [ConfigValue], [Description], [IsActive], [CreatedAt])
                VALUES (
                    N'master_key.pin_hash',
                    ?,
                    N'Hash bcrypt PIN-u Master Key (auto-wygenerowany przez setup.py)',
                    1,
                    GETDATE()
                );
            """,
            pin_hash_str,
            pin_hash_str,
        )
        conn.commit()

        rows = cur.rowcount
        conn.close()

        if rows > 0:
            print("✅ PIN ustawiony pomyślnie.")
            logger.info('"master_key.pin_hash zaktualizowany w bazie (bcrypt, rounds=12)"')
        else:
            print("⚠️  MERGE nie zmodyfikował żadnego wiersza — sprawdź bazę.")

    except Exception as exc:
        print(f"❌ Błąd zapisu do bazy: {exc}")
        logger.error('"Błąd zapisu pin_hash: %s"', str(exc))
        sys.exit(1)

    del pin_hash, pin_hash_str  # Cleanup z pamięci


def cmd_set_admin_password() -> None:
    """
    Interaktywne ustawienie hasła administratora.

    Hashuje argon2id — ten sam hasher co auth_service.py.
    """
    print("\n── Ustawianie hasła administratora ────────────────────────────────")
    print("Hasło zastąpi PLACEHOLDER wstawiony przez seed 04_admin_user.sql.")
    print("Hasło nie będzie wyświetlane na ekranie.\n")

    if not sys.stdin.isatty():
        print("BŁĄD: Skrypt musi być uruchomiony interaktywnie.")
        sys.exit(1)

    username = input("Nazwa użytkownika admina [admin]: ").strip() or "admin"

    # Sprawdź czy user istnieje
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            f"SELECT [ID_USER] FROM [{SCHEMA}].[Users] WHERE [Username] = ?;",
            username,
        )
        row = cur.fetchone()
        if not row:
            print(f"❌ Użytkownik '{username}' nie istnieje w bazie.")
            print("   Uruchom najpierw: seed 04_admin_user.sql")
            conn.close()
            sys.exit(1)
        user_id = row[0]
        conn.close()
    except Exception as exc:
        print(f"❌ Błąd: {exc}")
        sys.exit(1)

    password1 = getpass.getpass(f"Nowe hasło dla '{username}': ")
    password2 = getpass.getpass("Powtórz hasło: ")

    if password1 != password2:
        print("❌ Hasła nie są identyczne. Anulowano.")
        sys.exit(1)

    # Polityka hasła — minimum 8 znaków, wielka litera, cyfra
    if len(password1) < 8:
        print("❌ Hasło musi mieć co najmniej 8 znaków.")
        sys.exit(1)
    if not any(c.isupper() for c in password1):
        print("❌ Hasło musi zawierać co najmniej jedną wielką literę.")
        sys.exit(1)
    if not any(c.isdigit() for c in password1):
        print("❌ Hasło musi zawierać co najmniej jedną cyfrę.")
        sys.exit(1)

    print("\nHashowanie hasła (argon2id)...")
    password_hash = ARGON2_HASHER.hash(password1)
    del password1, password2  # Cleanup

    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            f"""
            UPDATE [{SCHEMA}].[Users]
            SET [PasswordHash] = ?,
                [UpdatedAt]    = GETDATE()
            WHERE [ID_USER] = ?;
            """,
            password_hash,
            user_id,
        )
        conn.commit()
        rows = cur.rowcount
        conn.close()

        if rows > 0:
            print(f"✅ Hasło dla '{username}' (ID: {user_id}) ustawione.")
            logger.info('"admin password_hash zaktualizowany dla user_id=%d (argon2id)"', user_id)
        else:
            print("⚠️  UPDATE nie zmodyfikował żadnego wiersza.")

    except Exception as exc:
        print(f"❌ Błąd zapisu: {exc}")
        logger.error('"Błąd zapisu password_hash: %s"', str(exc))
        sys.exit(1)

    del password_hash  # Cleanup


def cmd_run_seeds() -> None:
    """
    Uruchamia seedy SQL przez sqlcmd w poprawnej kolejności.

    Wymaga zainstalowanego sqlcmd (Microsoft SQL Server Command Line Tools).
    Alternatywnie możesz uruchomić pliki ręcznie przez SSMS.
    """
    print("\n── Uruchamianie seedów SQL ─────────────────────────────────────────")

    # Sprawdź dostępność sqlcmd
    try:
        result = subprocess.run(["sqlcmd", "-?"], capture_output=True, text=True)
        if result.returncode not in (0, 1):
            raise FileNotFoundError
        print("✅ sqlcmd: dostępny")
    except FileNotFoundError:
        print("❌ sqlcmd nie jest dostępny w PATH.")
        print("   Zainstaluj: https://learn.microsoft.com/en-us/sql/tools/sqlcmd-utility")
        print("   Lub uruchom seedy ręcznie przez SSMS.")
        sys.exit(1)

    host   = os.environ.get("DB_HOST", "host.docker.internal")
    port   = os.environ.get("DB_PORT", "1433")
    db     = os.environ.get("DB_NAME", "WAPRO")
    user   = os.environ.get("DB_USER", "")
    passwd = os.environ.get("DB_PASSWORD", "")

    server = f"{host},{port}"

    for seed_file in SEEDS_ORDER:
        seed_path = SEEDS_DIR / seed_file
        if not seed_path.exists():
            print(f"❌ Brak pliku: {seed_path}")
            sys.exit(1)

        print(f"\nUruchamiam: {seed_file}")
        cmd = [
            "sqlcmd",
            "-S", server,
            "-d", db,
            "-U", user,
            "-P", passwd,
            "-i", str(seed_path),
            "-b",  # EXIT on error
            "-e",  # Echo input
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
        stdout = result.stdout.strip()
        stderr = result.stderr.strip()

        if stdout:
            for line in stdout.splitlines():
                print(f"  {line}")

        if result.returncode != 0:
            print(f"❌ Błąd wykonania {seed_file}:")
            if stderr:
                print(f"  {stderr}")
            logger.error('"Seed %s nieudany: returncode=%d"', seed_file, result.returncode)
            sys.exit(1)

        print(f"✅ {seed_file}: OK")
        logger.info('"Seed %s wykonany pomyślnie"', seed_file)

    print("\n✅ Wszystkie seedy wykonane pomyślnie.\n")


def cmd_full_init() -> None:
    """
    Pełna inicjalizacja: weryfikacja → seedy → master-pin → admin-password.
    """
    print("=" * 60)
    print("  WINDYKACJA SYSTEM — Pełna inicjalizacja")
    print("=" * 60)
    print(f"  Czas: {datetime.now(timezone.utc).isoformat()}")
    print()

    # 1. Weryfikacja połączenia
    if not cmd_verify():
        print("❌ Weryfikacja nieudana. Sprawdź połączenie i spróbuj ponownie.")
        sys.exit(1)

    # 2. Seedy
    answer = input("\nUruchomić seedy SQL? [t/N]: ").strip().lower()
    if answer == "t":
        cmd_run_seeds()
    else:
        print("Seedy pominięte.")

    # 3. Master PIN
    answer = input("\nUstawić PIN Master Key? [t/N]: ").strip().lower()
    if answer == "t":
        cmd_set_master_pin()
    else:
        print("Master PIN pominięty.")

    # 4. Hasło admina
    answer = input("\nUstawić hasło admina? [t/N]: ").strip().lower()
    if answer == "t":
        cmd_set_admin_password()
    else:
        print("Hasło admina pominięte.")

    print("\n" + "=" * 60)
    print("  Inicjalizacja zakończona.")
    print("  Następny krok: alembic upgrade head")
    print("=" * 60 + "\n")


# ─── CLI ──────────────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="setup.py",
        description="Skrypt konfiguracyjny systemu Windykacja",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """\
            Przykłady:
              python database/setup.py --verify
              python database/setup.py --set-master-pin
              python database/setup.py --set-admin-password
              python database/setup.py --run-seeds
              python database/setup.py --full-init
            """
        ),
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--verify",
        action="store_true",
        help="Weryfikacja połączenia z bazą i stanu tabel",
    )
    group.add_argument(
        "--set-master-pin",
        action="store_true",
        help="Interaktywne ustawienie PIN-u Master Key (bcrypt)",
    )
    group.add_argument(
        "--set-admin-password",
        action="store_true",
        help="Interaktywne ustawienie hasła administratora (argon2id)",
    )
    group.add_argument(
        "--run-seeds",
        action="store_true",
        help="Uruchomienie wszystkich seedów SQL przez sqlcmd",
    )
    group.add_argument(
        "--full-init",
        action="store_true",
        help="Pełna inicjalizacja interaktywna (verify + seeds + pin + admin)",
    )

    return parser


def main() -> None:
    _load_env()

    parser = build_parser()
    args = parser.parse_args()

    logger.info(
        '"setup.py uruchomiony: args=%s"',
        json.dumps(vars(args)),
    )

    try:
        if args.verify:
            success = cmd_verify()
            sys.exit(0 if success else 1)
        elif args.set_master_pin:
            cmd_set_master_pin()
        elif args.set_admin_password:
            cmd_set_admin_password()
        elif args.run_seeds:
            cmd_run_seeds()
        elif args.full_init:
            cmd_full_init()
    except KeyboardInterrupt:
        print("\n\n❌ Przerwano przez użytkownika (Ctrl+C).")
        logger.warning('"setup.py przerwany przez użytkownika"')
        sys.exit(130)
    except Exception as exc:
        print(f"\n❌ Nieoczekiwany błąd: {exc}")
        logger.exception('"Nieoczekiwany błąd: %s"', str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()