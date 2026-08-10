#!/usr/bin/env python3
"""
diag_missing_document.py

Skrypt diagnostyczny — WYŁĄCZNIE ODCZYT, żadnej modyfikacji danych.

Cel: zebrać w jednym miejscu wszystko, co system wie o konkretnym
dokumencie/id_source, który "zniknął" lub nie zaimportował się poprawnie:
  1. Przeszukuje pliki logów JSONL (events_*.jsonl, auto_dispatch_*.jsonl)
     w podanym zakresie dat, szukając dowolnej linii zawierającej podany
     numer dokumentu LUB podane id_source.
  2. Odczytuje z bazy aktualny stan skw_document_sources dla podanego
     id_source (last_sync_at/status/message).
  3. Sprawdza, czy dokument o podanym id_document istnieje w
     skw_document_approval_instances (i z jakim statusem, jeśli tak).

WYMAGANE ŚRODOWISKO: uruchamiać WEWNĄTRZ kontenera workera
(np. `docker exec -it windykacja_worker python /app/diag_missing_document.py ...`),
bo skrypt importuje worker.settings / worker.core.db — identyczny
mechanizm konfiguracji, jaki używa cała reszta workera. Uruchomienie poza
kontenerem zakończy się czytelnym błędem importu, nie czymś niezrozumiałym.

Użycie:
    python diag_missing_document.py \\
        --id-document "8941022653-20260803-900CE3000002-2C" \\
        --id-source 1 \\
        --date-from 2026-08-03 \\
        --date-to 2026-08-04 \\
        --log-dir /app/logs \\
        --output /app/diag_result.json

Wszystkie argumenty opcjonalne mają sensowne domyślne wartości — patrz --help.
Wynik: jeden plik JSON (kompletne dane, do dalszej analizy) + czytelne
podsumowanie na stdout.
"""
import argparse
import asyncio
import json
import logging
import sys
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

# ── Logowanie — zgodnie z zasadą "pełna odtwarzalność decyzji" ─────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("diag_missing_document")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Diagnostyka: dlaczego dokument nie trafił / nie zaktualizował się w systemie.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--id-document", required=True,
        help="Numer dokumentu do wyszukania w logach i w bazie (np. numer KSeF).",
    )
    p.add_argument(
        "--id-source", type=int, required=True,
        help="ID źródła dokumentu (np. 1 = fakir) — do przeszukania logów i skw_document_sources.",
    )
    p.add_argument(
        "--date-from", type=str, required=True,
        help="Data początkowa zakresu logów, format YYYY-MM-DD.",
    )
    p.add_argument(
        "--date-to", type=str, required=True,
        help="Data końcowa zakresu logów (włącznie), format YYYY-MM-DD.",
    )
    p.add_argument(
        "--log-dir", type=str, default="/app/logs",
        help="Katalog z plikami logów JSONL.",
    )
    p.add_argument(
        "--output", type=str, default="./diag_result.json",
        help="Ścieżka pliku wynikowego JSON.",
    )
    return p.parse_args()


def _daterange(d_from: date, d_to: date):
    d = d_from
    while d <= d_to:
        yield d
        d += timedelta(days=1)


def _search_log_file(path: Path, needles: list[str]) -> list[dict[str, Any]]:
    """
    Przeszukuje jeden plik JSONL linia po linii. Każda linia, która
    zawiera JAKIKOLWIEK z podanych fragmentów tekstu (needles), jest
    parsowana jako JSON (jeśli się nie da — zwracana jako surowy tekst,
    NIE gubimy żadnej linii tylko bo nie jest poprawnym JSON-em) i
    dołączana do wyniku wraz z numerem linii i nazwą pliku źródłowego.
    """
    hits: list[dict[str, Any]] = []
    if not path.exists():
        logger.warning("Plik logu nie istnieje: %s", path)
        return hits

    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line_no, line in enumerate(f, start=1):
                if not any(needle in line for needle in needles):
                    continue
                entry: dict[str, Any]
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    entry = {"_unparsed_raw_line": line.rstrip("\n")}
                entry["_source_file"] = str(path.name)
                entry["_line_number"] = line_no
                hits.append(entry)
    except Exception as exc:
        logger.error("Błąd czytania %s: %s", path, exc)
        hits.append({
            "_source_file": str(path.name),
            "_error": f"Nie udało się przeczytać pliku: {exc}",
        })
    return hits


def search_all_logs(
    log_dir: Path,
    id_document: str,
    id_source: int,
    d_from: date,
    d_to: date,
) -> dict[str, Any]:
    needles = [id_document, f'"id_source": {id_source}', f'"id_source":{id_source}']
    logger.info(
        "Szukam w logach fragmentów: %s (zakres dat %s .. %s)",
        needles, d_from, d_to,
    )

    events_hits: list[dict[str, Any]] = []
    dispatch_hits: list[dict[str, Any]] = []
    files_checked: list[str] = []
    files_missing: list[str] = []

    for d in _daterange(d_from, d_to):
        date_str = d.strftime("%Y-%m-%d")

        events_path = log_dir / f"events_{date_str}.jsonl"
        files_checked.append(str(events_path))
        if events_path.exists():
            events_hits.extend(_search_log_file(events_path, needles))
        else:
            files_missing.append(str(events_path))

        dispatch_path = log_dir / f"auto_dispatch_{date_str}.jsonl"
        files_checked.append(str(dispatch_path))
        if dispatch_path.exists():
            dispatch_hits.extend(_search_log_file(dispatch_path, needles))
        else:
            files_missing.append(str(dispatch_path))

    return {
        "files_checked": files_checked,
        "files_missing": files_missing,
        "events_hits": events_hits,
        "auto_dispatch_hits": dispatch_hits,
        "events_hits_count": len(events_hits),
        "auto_dispatch_hits_count": len(dispatch_hits),
    }


async def query_database(id_document: str, id_source: int) -> dict[str, Any]:
    """
    Odczyt WYŁĄCZNIE (SELECT) — zero modyfikacji danych.
    Jeśli import worker.core.db się nie powiedzie — zwraca jasny komunikat
    błędu w wyniku, NIE crashuje całego skryptu (część logowa i tak
    powinna dać wynik nawet bez dostępu do bazy).
    """
    result: dict[str, Any] = {
        "db_reachable": False,
        "source_status": None,
        "instance_lookup": None,
        "error": None,
    }
    try:
        from worker.core.db import get_engine  # type: ignore
        from sqlalchemy import text
    except Exception as exc:
        result["error"] = (
            f"Nie udało się zaimportować worker.core.db — skrypt musi być "
            f"uruchomiony WEWNĄTRZ kontenera workera. Szczegóły: {exc}"
        )
        logger.error(result["error"])
        return result

    try:
        engine = get_engine()
        async with engine.connect() as conn:
            # 1) Stan źródła — ostatnia synchronizacja
            row = (await conn.execute(
                text(
                    "SELECT [id_source], [source_name], [last_sync_at], "
                    "       [last_sync_status], [last_sync_message], [is_active] "
                    "FROM [dbo].[skw_document_sources] "
                    "WHERE [id_source] = :s"
                ),
                {"s": id_source},
            )).fetchone()
            if row:
                result["source_status"] = {
                    "id_source":         row[0],
                    "source_name":       row[1],
                    "last_sync_at":      str(row[2]) if row[2] else None,
                    "last_sync_status":  row[3],
                    "last_sync_message": row[4],
                    "is_active":         bool(row[5]) if row[5] is not None else None,
                }
            else:
                result["source_status"] = {"_note": f"id_source={id_source} nie istnieje w skw_document_sources"}

            # 2) Czy dokument istnieje jako instancja obiegu (jakikolwiek status)
            inst_rows = (await conn.execute(
                text(
                    "SELECT [id_instance], [id_source], [status], "
                    "       [document_title], [dispatch_attempts], "
                    "       [created_at], [updated_at] "
                    "FROM [dbo].[skw_document_approval_instances] "
                    "WHERE [id_document] = :d"
                ),
                {"d": id_document},
            )).fetchall()

            result["instance_lookup"] = [
                {
                    "id_instance":       r[0],
                    "id_source":         r[1],
                    "status":            r[2],
                    "document_title":    r[3],
                    "dispatch_attempts": r[4],
                    "created_at":        str(r[5]) if r[5] else None,
                    "updated_at":        str(r[6]) if r[6] else None,
                }
                for r in inst_rows
            ]
            if not inst_rows:
                result["instance_lookup"] = {
                    "_note": (
                        f"id_document='{id_document}' NIE ISTNIEJE w "
                        f"skw_document_approval_instances — dokument nigdy "
                        f"nie trafił do systemu, niezależnie od statusu."
                    )
                }

        result["db_reachable"] = True

    except Exception as exc:
        result["error"] = f"Błąd zapytania do bazy: {exc}"
        result["_traceback"] = traceback.format_exc()
        logger.error("Błąd zapytania do bazy: %s", exc)

    return result


async def main() -> None:
    args = parse_args()

    try:
        d_from = datetime.strptime(args.date_from, "%Y-%m-%d").date()
        d_to = datetime.strptime(args.date_to, "%Y-%m-%d").date()
    except ValueError as exc:
        logger.error("Nieprawidłowy format daty (wymagane YYYY-MM-DD): %s", exc)
        sys.exit(1)

    if d_to < d_from:
        logger.error("--date-to nie może być wcześniejsze niż --date-from.")
        sys.exit(1)

    log_dir = Path(args.log_dir)
    if not log_dir.exists():
        logger.warning("Katalog logów nie istnieje: %s — logi nie zostaną znalezione.", log_dir)

    logger.info("=== START diagnostyki ===")
    logger.info("id_document=%s | id_source=%s | zakres=%s..%s | log_dir=%s",
                args.id_document, args.id_source, d_from, d_to, log_dir)

    log_results = search_all_logs(log_dir, args.id_document, args.id_source, d_from, d_to)
    db_results = await query_database(args.id_document, args.id_source)

    output = {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "params": {
            "id_document": args.id_document,
            "id_source":   args.id_source,
            "date_from":   str(d_from),
            "date_to":     str(d_to),
            "log_dir":     str(log_dir),
        },
        "logs": log_results,
        "database": db_results,
    }

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)

    # ── Czytelne podsumowanie na konsolę ─────────────────────────────────────
    print("\n" + "=" * 70)
    print("PODSUMOWANIE DIAGNOSTYKI")
    print("=" * 70)
    print(f"Dokument:  {args.id_document}")
    print(f"Źródło:    id_source={args.id_source}")
    print(f"Zakres:    {d_from} .. {d_to}")
    print("-" * 70)
    print(f"Pliki logów sprawdzone: {len(log_results['files_checked'])}")
    print(f"Pliki logów brakujące:  {len(log_results['files_missing'])}")
    if log_results["files_missing"]:
        for fp in log_results["files_missing"]:
            print(f"    - BRAK: {fp}")
    print(f"Trafienia w events_*.jsonl:        {log_results['events_hits_count']}")
    print(f"Trafienia w auto_dispatch_*.jsonl: {log_results['auto_dispatch_hits_count']}")
    print("-" * 70)
    if db_results.get("error"):
        print(f"BŁĄD BAZY: {db_results['error']}")
    else:
        print("Stan źródła (skw_document_sources):")
        print(f"    {json.dumps(db_results['source_status'], ensure_ascii=False, indent=4, default=str)}")
        print("Wyszukanie dokumentu w skw_document_approval_instances:")
        print(f"    {json.dumps(db_results['instance_lookup'], ensure_ascii=False, indent=4, default=str)}")
    print("-" * 70)
    print(f"Pełny wynik zapisany do: {out_path.resolve()}")
    print("=" * 70)
    print("\nPrzekaż PEŁNY plik JSON (nie tylko to podsumowanie) do dalszej analizy.\n")


if __name__ == "__main__":
    asyncio.run(main())