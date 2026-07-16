# -*- coding: utf-8 -*-
"""
Diagnostyka: dlaczego logger.error() w app.services.documents_service
nie pojawia sie w docker logs, mimo ze na pewno sie wykonuje (potwierdzone
przez poprawna odpowiedz 503 z tresci zaleznej od tej galezi kodu).

Sprawdza:
  1. Stan loggera "app.services.documents_service" na zywo (handlers,
     level, propagate) oraz loggera root.
  2. Czy wpis faktycznie trafil do pliku logs/app_*.log (file handler
     dziala niezaleznie od konsoli).
  3. Wysyla testowy logger.error() z tego samego loggera i natychmiast
     sprawdza oba miejsca.
"""
import logging
import glob
import os


def dump_logger_state(name: str) -> None:
    lg = logging.getLogger(name)
    print(f"=== Logger: {name!r} ===", flush=True)
    print(f"  level (wlasny)   = {logging.getLevelName(lg.level)} ({lg.level})", flush=True)
    print(f"  effective level  = {logging.getLevelName(lg.getEffectiveLevel())}", flush=True)
    print(f"  propagate        = {lg.propagate}", flush=True)
    print(f"  handlers wlasne  = {lg.handlers}", flush=True)
    print(f"  disabled         = {lg.disabled}", flush=True)


def main() -> None:
    print("=== DIAG LOGGING START ===", flush=True)

    dump_logger_state("app.services.documents_service")
    dump_logger_state("app.schemas.unified_document")
    dump_logger_state("app.main")
    dump_logger_state("app.core.dependencies")
    dump_logger_state("")  # root

    root = logging.getLogger()
    print(f"\nroot handlers: {root.handlers}", flush=True)
    for h in root.handlers:
        print(f"  handler={h!r} level={logging.getLevelName(h.level)}", flush=True)

    # Wyslij testowy log z documents_service loggera
    test_logger = logging.getLogger("app.services.documents_service")
    marker = "DIAG_MARKER_TEST_12345"
    print(f"\n[TEST] Wysylam logger.error() z markerem: {marker}", flush=True)
    test_logger.error("Test diagnostyczny: %s", marker)

    # Sprawdz pliki logow na dysku
    print("\n[PLIKI] Szukam plikow logow w /app/logs", flush=True)
    for path in sorted(glob.glob("/app/logs/*.log")) + sorted(glob.glob("/app/logs/*.jsonl")):
        size = os.path.getsize(path)
        print(f"  {path} ({size} bajtow)", flush=True)
        if size > 0:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
                if marker in content:
                    print(f"    -> MARKER ZNALEZIONY w tym pliku!", flush=True)

    print("=== DIAG LOGGING END ===", flush=True)


if __name__ == "__main__":
    main()
