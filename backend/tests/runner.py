#!/usr/bin/env python3
# =============================================================================
# PLIK  : backend/tests/runner.py
# MODUŁ : Self-test runner — Windykacja Sprint 2.3
#
# URUCHOMIENIE:
#   docker exec windykacja_api python -m tests.runner
#   docker exec windykacja_api python -m tests.runner --verbose
#   docker exec windykacja_api python -m tests.runner --filter auth
#   docker exec windykacja_api python -m tests.runner --no-report
#
# RAPORT:
#   /app/logs/selftest_YYYY-MM-DD_HH-MM-SS.json
# =============================================================================
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("windykacja.selftest")


# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------
BANNER = """
╔══════════════════════════════════════════════════════════════╗
║          WINDYKACJA — SELF-TEST SUITE  Sprint 2.3            ║
║          Uruchom: docker exec windykacja_api                 ║
║                   python -m tests.runner                     ║
╚══════════════════════════════════════════════════════════════╝
"""

IKONY = {
    "PASSED":  "✅",
    "FAILED":  "❌",
    "ERROR":   "💥",
    "SKIPPED": "⏭️ ",
    "XFAIL":   "〰️ ",   # oczekiwany błąd — znany problem
    "XPASS":   "⚠️ ",   # nieoczekiwany sukces — coś się zmieniło
    "WARN":    "⚠️ ",
}


# ---------------------------------------------------------------------------
# Klasa raportu
# ---------------------------------------------------------------------------
class SelfTestRaport:
    def __init__(self) -> None:
        self.start_ts   = datetime.now(timezone.utc)
        self.wyniki: list[dict] = []
        self.sukcesy    = 0
        self.bledy      = 0
        self.pomiete    = 0
        self.xfail      = 0   # oczekiwane błędy (znane problemy)
        self.xpass      = 0   # nieoczekiwane sukcesy

    def dodaj(
        self,
        nazwa:   str,
        status:  str,
        czas_ms: float,
        blad:    str | None = None,
    ) -> None:
        ikona = IKONY.get(status, "?")
        czas_s = czas_ms / 1000
        print(f"  {ikona}  {nazwa:<60} [{czas_s:.2f}s]")
        if blad and status not in ("SKIPPED", "XFAIL"):
            pierwsze_linie = "\n".join(blad.splitlines()[:3])
            print(f"       └─ {pierwsze_linie}")

        self.wyniki.append({
            "nazwa":   nazwa,
            "status":  status,
            "czas_ms": round(czas_ms, 1),
            "blad":    blad,
            "ts":      datetime.now(timezone.utc).isoformat(),
        })

        if status == "PASSED":
            self.sukcesy += 1
        elif status == "SKIPPED":
            self.pomiete += 1
        elif status == "XFAIL":
            self.xfail += 1   # nie liczymy jako błąd
        elif status == "XPASS":
            self.xpass += 1   # sukces gdzie oczekiwano błędu — informacyjnie
        else:
            self.bledy += 1

    def podsumowanie(self) -> dict:
        czas_total = (datetime.now(timezone.utc) - self.start_ts).total_seconds()
        return {
            "meta": {
                "wersja":       "Sprint 2.3",
                "start":        self.start_ts.isoformat(),
                "koniec":       datetime.now(timezone.utc).isoformat(),
                "czas_total_s": round(czas_total, 2),
                "hostname":     os.uname().nodename,
                "python":       sys.version.split()[0],
            },
            "wyniki": {
                "sukcesy":   self.sukcesy,
                "bledy":     self.bledy,
                "pominiete": self.pomiete,
                "xfail":     self.xfail,
                "xpass":     self.xpass,
                "total":     len(self.wyniki),
            },
            "testy": self.wyniki,
        }

    def zapisz_raport(self, katalog: str = "/app/logs") -> Path:
        Path(katalog).mkdir(parents=True, exist_ok=True)
        ts  = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        pth = Path(katalog) / f"selftest_{ts}.json"
        pth.write_text(
            json.dumps(self.podsumowanie(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return pth

    def drukuj_podsumowanie(self) -> None:
        czas = (datetime.now(timezone.utc) - self.start_ts).total_seconds()
        total = len(self.wyniki)
        print()
        print("─" * 68)
        linia = (
            f"  WYNIKI: {self.sukcesy}/{total} OK  |  "
            f"{self.bledy} BŁĘDÓW  |  {self.pomiete} POMINIĘTYCH  |  "
            f"{czas:.1f}s"
        )
        if self.xfail:
            linia += f"  |  {self.xfail} XFAIL (znane problemy)"
        if self.xpass:
            linia += f"  |  {self.xpass} XPASS (naprawione?)"
        print(linia)
        print("─" * 68)

        if self.bledy == 0:
            print("  🎉  Wszystkie testy PRZESZŁY!")
            if self.xfail:
                print(f"  〰️   {self.xfail} znanych problemów nadal aktywnych (XFAIL)")
        else:
            print(f"  ⚠️   {self.bledy} test(ów) NIEUDANYCH — sprawdź raport")

        if self.xpass:
            print(f"  ⚠️   {self.xpass} testów XPASS — endpoint który był niezaimplementowany teraz działa!")

        if self.pomiete > 0:
            print(f"  ℹ️   {self.pomiete} test(ów) pominiętych "
                  f"(brak danych testowych lub konfiguracji)")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------
def uruchom_pytest(verbose: bool, filter_str: str | None) -> tuple[int, str]:
    """
    Uruchamia pytest jako subprocess i zwraca (returncode, output).
    Używamy subprocess żeby nie interferować z bieżącym procesem.
    """
    import subprocess

    PYTEST_BIN = "/home/appuser/.local/bin/pytest"
    cmd = [
        PYTEST_BIN,
        "/app/tests/",
        "--tb=short",
        "--no-header",
        "-q",
        "--timeout=30",
        f"--json-report",
        f"--json-report-file=/tmp/selftest_pytest.json",
    ]

    if verbose:
        cmd.extend(["-v", "--tb=long"])

    if filter_str:
        cmd.extend(["-k", filter_str])

    env = os.environ.copy()
    env["SELFTEST_BASE_URL"] = env.get("SELFTEST_BASE_URL", "http://localhost:8000/api/v1")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd="/app",
        env=env,
    )
    return result.returncode, result.stdout + result.stderr


def _czy_linia_wyniku_testu(linia: str) -> bool:
    """
    Zwraca True jeśli linia to faktyczny wynik testu pytest (format: path::Class::method STATUS).

    Pytest drukuje dwa rodzaje linii z wynikami:
    1. Wyniki testów (zliczamy):
       tests/test_auth.py::TestAuth::test_login_ok PASSED      [ 10%]
    2. Sekcja summary (pomijamy — duplikaty):
       FAILED tests/test_auth.py::TestAuth::test_login_ok - assert...
       _ ERROR at setup of TestAuth.test_login_ok _

    Reguła: linia wyniku zawsze ma '::' i NIE zaczyna się od słowa kluczowego.
    """
    return "::" in linia


def uruchom_bez_pytest(raport: SelfTestRaport, verbose: bool, filter_str: str | None, pytest_bin: str | None = None) -> None:
    """
    Uruchamia pytest i parsuje wyjście tekstowe.

    Obsługuje statusy: PASSED, FAILED, SKIPPED, ERROR, XFAIL, XPASS.

    WAŻNE: Parsujemy TYLKO linie z '::' (faktyczne wyniki testów).
    Linie sekcji summary ("ERROR at setup of...", "FAILED test/...") są pomijane
    — inaczej każdy błąd byłby zliczony 2-3 razy.
    """
    import subprocess
    import shutil

    _bin = pytest_bin or shutil.which("pytest") or sys.executable
    if _bin == sys.executable:
        cmd = [_bin, "-m", "pytest", "/app/tests/", "--tb=short", "--no-header", "-v"]
    else:
        cmd = [_bin, "/app/tests/", "--tb=short", "--no-header", "-v"]

    if filter_str:
        cmd.extend(["-k", filter_str])

    env = os.environ.copy()
    env["SELFTEST_BASE_URL"] = env.get("SELFTEST_BASE_URL", "http://localhost:8000/api/v1")

    t_start = time.perf_counter()
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd="/app",
        env=env,
    )
    czas_total = (time.perf_counter() - t_start) * 1000

    # ---------------------------------------------------------------------------
    # Parsowanie outputu pytest
    #
    # Format wyników z -v:
    #   tests/test_xxx.py::ClassName::test_method PASSED   [ xx%]
    #   tests/test_xxx.py::ClassName::test_method FAILED   [ xx%]
    #   tests/test_xxx.py::ClassName::test_method XFAIL    [ xx%]
    #   tests/test_xxx.py::ClassName::test_method XPASS    [ xx%]
    #   tests/test_xxx.py::ClassName::test_method SKIPPED  [ xx%]
    #   tests/test_xxx.py::ClassName::test_method ERROR    [ xx%]
    #
    # POMIJAMY linie z sekcji summary (nie zawierają '::' lub zaczynają od słowa kluczowego):
    #   FAILED tests/test_xxx.py::... - AssertionError: ...
    #   ERROR tests/test_xxx.py::... - ...
    #   _ ERROR at setup of TestXxx.test_yyy _
    # ---------------------------------------------------------------------------

    for linia in result.stdout.splitlines():
        linia = linia.strip()

        # Pomijamy puste linie, separatory
        if not linia:
            continue
        if linia.startswith("=") or linia.startswith("-"):
            continue

        # Pomijamy linie sekcji summary (zaczynają się od słowa kluczowego + spacja)
        if (linia.startswith("FAILED ") or linia.startswith("ERROR ")
                or linia.startswith("PASSED ") or linia.startswith("XFAIL ")
                or linia.startswith("XPASS ")):
            continue

        # Pomijamy linie nagłówków sekcji
        if linia in ("short test summary info", "warnings summary", "ERRORS", "FAILURES"):
            continue

        # Właściwe wyniki mają '::' — format: path::Class::method STATUS
        if not _czy_linia_wyniku_testu(linia):
            continue

        # Wyciągnij nazwę testu — ostatni segment po '::'
        # Format: tests/test_xxx.py::ClassName::test_method STATUS   [ xx%]
        czesci = linia.split("::")
        if len(czesci) < 2:
            continue

        # Ostatnia część: "test_method STATUS   [ xx%]" lub "test_method STATUS"
        ostatnia = czesci[-1]

        def _wyciagnij_nazwe(suffix: str) -> str:
            """Wyciąga nazwę testu przed STATUS i ewentualnym procentem."""
            nazwa = ostatnia.split(suffix)[0].strip()
            return nazwa.split("[")[0].strip()

        if " PASSED" in ostatnia:
            raport.dodaj(_wyciagnij_nazwe(" PASSED"), "PASSED", 0)
        elif " FAILED" in ostatnia:
            raport.dodaj(_wyciagnij_nazwe(" FAILED"), "FAILED", 0, "Sprawdź logi poniżej")
        elif " SKIPPED" in ostatnia:
            raport.dodaj(_wyciagnij_nazwe(" SKIPPED"), "SKIPPED", 0)
        elif " ERROR" in ostatnia:
            raport.dodaj(_wyciagnij_nazwe(" ERROR"), "ERROR", 0, "Błąd wykonania testu (patrz --verbose)")
        elif " XFAIL" in ostatnia:
            # XFAIL = oczekiwany błąd — znany problem, nie liczymy jako błąd
            raport.dodaj(_wyciagnij_nazwe(" XFAIL"), "XFAIL", 0, "Znany problem (endpoint niezaimplementowany)")
        elif " XPASS" in ostatnia:
            # XPASS = nieoczekiwany sukces — test który miał failować, przeszedł
            raport.dodaj(_wyciagnij_nazwe(" XPASS"), "XPASS", 0, "Nieoczekiwany sukces — endpoint teraz działa!")

    if verbose:
        print("\n── Pełne wyjście pytest ──────────────────────────────")
        print(result.stdout)
        if result.stderr:
            print("── STDERR ────────────────────────────────────────────")
            print(result.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Windykacja Self-Test Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Przykłady:
  python -m tests.runner                        # wszystkie testy
  python -m tests.runner --verbose              # szczegółowy output z traceback
  python -m tests.runner --filter auth          # tylko testy auth (Sprint 2.2)
  python -m tests.runner --filter faktury       # tylko testy faktur
  python -m tests.runner --filter infra         # tylko infrastruktura
  python -m tests.runner --filter users         # tylko /users (Sprint 2.3)
  python -m tests.runner --filter roles         # tylko /roles (Sprint 2.3)
  python -m tests.runner --filter permissions   # tylko /permissions (Sprint 2.3)
  python -m tests.runner --filter debtors       # tylko /debtors + komentarze (Sprint 2.3)
  python -m tests.runner --filter system        # tylko /system/config (Sprint 2.3)
  python -m tests.runner --filter snapshots     # tylko /snapshots (Sprint 2.3)
  python -m tests.runner --filter templates     # tylko /templates (Sprint 2.3)
  python -m tests.runner --filter auth_extended # auth rozszerzony: me, impersonate (Sprint 2.3)
  python -m tests.runner --filter test_lista_ok # pojedynczy test po nazwie metody

Statusy wyników:
  ✅ PASSED  — test przeszedł
  ❌ FAILED  — test nieudany — wymaga naprawy w kodzie
  💥 ERROR   — błąd konfiguracji/fixture — sprawdź setup
  ⏭️  SKIPPED — brak danych testowych lub konfiguracji
  〰️ XFAIL  — znany problem (endpoint niezaimplementowany) — nie liczy się jako błąd
  ⚠️  XPASS  — endpoint który był niezaimplementowany teraz działa — usuń xfail!
        """,
    )
    parser.add_argument("--verbose", "-V", action="store_true", help="Szczegółowy output")
    parser.add_argument("--filter", "-k", type=str, default=None, help="Filtr testów (pytest -k)")
    parser.add_argument("--no-report", action="store_true", help="Nie zapisuj raportu JSON")
    args = parser.parse_args()

    print(BANNER)
    print(f"  Base URL:  {os.environ.get('SELFTEST_BASE_URL', 'http://localhost:8000/api/v1')}")
    print(f"  Username:  {os.environ.get('SELFTEST_USERNAME', 'admin')}")
    print(f"  Filter:    {args.filter or 'wszystkie'}")
    print(f"  Verbose:   {'tak' if args.verbose else 'nie'}")
    print()

    raport = SelfTestRaport()

    import shutil
    import os.path as _osp
    pytest_bin = shutil.which("pytest") or "/home/appuser/.local/bin/pytest"
    if not _osp.exists(pytest_bin):
        pytest_bin = None

    print("  Uruchamianie testów...\n")

    uruchom_bez_pytest(raport, args.verbose, args.filter, pytest_bin=pytest_bin)

    raport.drukuj_podsumowanie()

    if not args.no_report:
        try:
            sciezka = raport.zapisz_raport("/app/logs")
            print(f"\n  📄 Raport: {sciezka}")
        except Exception as exc:
            print(f"\n  ⚠️  Nie udało się zapisać raportu: {exc}")

    return 0 if raport.bledy == 0 else 1


if __name__ == "__main__":
    sys.exit(main())