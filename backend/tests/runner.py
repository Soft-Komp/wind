#!/usr/bin/env python3
# =============================================================================
# PLIK  : backend/tests/runner.py
# MODUŁ : Self-test runner — Windykacja Sprint 2.2
#
# URUCHOMIENIE:
#   docker exec windykacja_api python -m tests.runner
#   docker exec windykacja_api python -m tests.runner --verbose
#   docker exec windykacja_api python -m tests.runner --filter auth
#   docker exec windykacja_api python -m tests.runner --no-cleanup
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
║          WINDYKACJA — SELF-TEST SUITE  Sprint 2.2            ║
║          Uruchom: docker exec windykacja_api                 ║
║                   python -m tests.runner                     ║
╚══════════════════════════════════════════════════════════════╝
"""

IKONY = {
    "PASSED":  "✅",
    "FAILED":  "❌",
    "ERROR":   "💥",
    "SKIPPED": "⏭️ ",
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
        if blad and status not in ("SKIPPED",):
            # Pokaż tylko pierwsze 3 linie błędu
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
        else:
            self.bledy += 1

    def podsumowanie(self) -> dict:
        czas_total = (datetime.now(timezone.utc) - self.start_ts).total_seconds()
        return {
            "meta": {
                "wersja":       "Sprint 2.2",
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
        print(f"  WYNIKI: {self.sukcesy}/{total} OK  |  "
              f"{self.bledy} BŁĘDÓW  |  {self.pomiete} POMINIĘTYCH  |  "
              f"{czas:.1f}s")
        print("─" * 68)

        if self.bledy == 0:
            print("  🎉  Wszystkie testy PRZESZŁY!")
        else:
            print(f"  ⚠️   {self.bledy} test(ów) NIEUDANYCH — sprawdź raport")

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


def uruchom_bez_pytest(raport: SelfTestRaport, verbose: bool, filter_str: str | None, pytest_bin: str | None = None) -> None:
    """
    Uruchamia pytest i parsuje wyjście tekstowe.
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

    # Parsuj output pytest
    for linia in result.stdout.splitlines():
        linia = linia.strip()
        if not linia or linia.startswith("=") or linia.startswith("-"):
            continue

        if " PASSED" in linia:
            nazwa = linia.split("::")[1].split(" PASSED")[0].strip() if "::" in linia else linia
            raport.dodaj(nazwa, "PASSED", 0)
        elif " FAILED" in linia:
            nazwa = linia.split("::")[1].split(" FAILED")[0].strip() if "::" in linia else linia
            raport.dodaj(nazwa, "FAILED", 0, "Sprawdź logi poniżej")
        elif " SKIPPED" in linia:
            nazwa = linia.split("::")[1].split(" SKIPPED")[0].strip() if "::" in linia else linia
            raport.dodaj(nazwa, "SKIPPED", 0)
        elif " ERROR" in linia:
            nazwa = linia.split("::")[1].split(" ERROR")[0].strip() if "::" in linia else linia
            raport.dodaj(nazwa, "ERROR", 0, "Błąd wykonania testu")

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
  python -m tests.runner                    # wszystkie testy
  python -m tests.runner --verbose          # szczegółowy output
  python -m tests.runner --filter auth      # tylko testy auth
  python -m tests.runner --filter faktury   # tylko testy faktur
  python -m tests.runner --filter infra     # tylko infrastruktura
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

    # Sprawdź czy pytest jest dostępny
    import shutil
    import os.path as _osp
    pytest_bin = shutil.which("pytest") or "/home/appuser/.local/bin/pytest"
    if not _osp.exists(pytest_bin):
        # Spróbuj przez python -m pytest
        pytest_bin = None

    print("  Uruchamianie testów...\n")

    uruchom_bez_pytest(raport, args.verbose, args.filter, pytest_bin=pytest_bin)

    # Podsumowanie
    raport.drukuj_podsumowanie()

    # Zapis raportu
    if not args.no_report:
        try:
            sciezka = raport.zapisz_raport("/app/logs")
            print(f"\n  📄 Raport: {sciezka}")
        except Exception as exc:
            print(f"\n  ⚠️  Nie udało się zapisać raportu: {exc}")

    return 0 if raport.bledy == 0 else 1


if __name__ == "__main__":
    sys.exit(main())