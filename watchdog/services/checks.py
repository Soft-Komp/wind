"""
watchdog/services/checks.py
Rdzeń logiki Watchdoga: sprawdzanie kolekcji ENV (obecność) i DB (wartość + auto-naprawa).
Każde wywołanie loguje absurdalnie dużo kontekstu do skw_WatchdogRunLog (JSON)
oraz do pliku JSONL — żeby każde zdarzenie dało się odtworzyć bez wątpliwości.
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

import pyodbc

from watchdog.config import get_settings

logger = logging.getLogger("watchdog.checks")

WynikType = Literal[
    "ok", "niespojnosc", "naprawiono", "zablokowano",
    "env_brak", "blad_polaczenia", "alert_wyslany", "alert_blad",
]


@dataclass(frozen=True)
class Pozycja:
    id_pozycja: int
    typ_pozycji: str          # 'env_var' | 'db_setting'
    klucz: str
    wartosc_wzorcowa: str | None


@dataclass(frozen=True)
class Kolekcja:
    id_kolekcja: int
    nazwa: str
    typ: str
    tryb_naprawy: str
    warunek_klucz: str | None = None       # ← NOWE
    warunek_wartosc: str | None = None     # ← NOWE
    pozycje: list[Pozycja] = field(default_factory=list)


@dataclass
class WynikSprawdzenia:
    kolekcja: Kolekcja
    wynik: WynikType
    szczegoly: dict
    czas_trwania_ms: float


def _jsonl_log(entry: dict) -> None:
    """Dodatkowy, niezależny log plikowy — na wypadek gdyby zapis do bazy zawiódł."""
    settings = get_settings()
    os.makedirs(settings.LOG_DIR, exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = os.path.join(settings.LOG_DIR, f"watchdog_{date_str}.jsonl")
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
    except Exception as exc:
        logger.error("watchdog: błąd zapisu JSONL — %s", exc, extra={"error": str(exc)})


def pobierz_kolekcje(conn: pyodbc.Connection) -> list[Kolekcja]:
    """Wczytuje wszystkie aktywne kolekcje wraz z pozycjami — jedno zapytanie na start cyklu."""
    cur = conn.cursor()
    cur.execute("""
        SELECT id_kolekcja, nazwa, typ, tryb_naprawy, warunek_klucz, warunek_wartosc
        FROM dbo.skw_WatchdogKolekcje
        WHERE is_active = 1
    """)
    kolekcje_raw = cur.fetchall()

    kolekcje: list[Kolekcja] = []
    for row in kolekcje_raw:
        cur.execute("""
            SELECT id_pozycja, typ_pozycji, klucz, wartosc_wzorcowa
            FROM dbo.skw_WatchdogPozycje
            WHERE id_kolekcja = ?
        """, row.id_kolekcja)
        pozycje = [
            Pozycja(p.id_pozycja, p.typ_pozycji, p.klucz, p.wartosc_wzorcowa)
            for p in cur.fetchall()
        ]
        kolekcje.append(Kolekcja(
            row.id_kolekcja, row.nazwa, row.typ, row.tryb_naprawy,
            row.warunek_klucz, row.warunek_wartosc, pozycje,
        ))

    logger.info(
        "watchdog: wczytano kolekcje",
        extra={"event": "watchdog.kolekcje_wczytane", "liczba_kolekcji": len(kolekcje)},
    )
    return kolekcje


def sprawdz_kolekcje_env(kolekcja: Kolekcja) -> WynikSprawdzenia:
    """
    Sprawdza kolekcje ENV — obecność ORAZ (jeśli podana) dokładną wartość.

    Obsługuje warunek wyzwalający (kolekcja.warunek_klucz/warunek_wartosc):
    jeśli podany, kolekcja jest egzekwowana TYLKO gdy bieżąca wartość
    warunek_klucz w os.environ równa się warunek_wartosc. W przeciwnym razie
    kolekcja jest pomijana (wynik 'pominieto_warunek') — to NIE jest błąd.

    tryb_naprawy dla typu 'env' ograniczony w bazie do block/log_only
    (CK_skw_WatchdogKolekcje_EnvNoAutoFix) — auto_fix tu nigdy nie wystąpi,
    bo nie da się nadpisać żywego env w działającym kontenerze.
    """
    t0 = time.monotonic()

    # ── KROK 1: warunek wyzwalający ──────────────────────────────────────────
    if kolekcja.warunek_klucz is not None:
        aktualna_warunku = os.environ.get(kolekcja.warunek_klucz)
        if aktualna_warunku != kolekcja.warunek_wartosc:
            czas_ms = (time.monotonic() - t0) * 1000
            szczegoly = {
                "kolekcja": kolekcja.nazwa,
                "typ": "env",
                "warunek_klucz": kolekcja.warunek_klucz,
                "warunek_wartosc_wymagana": kolekcja.warunek_wartosc,
                "warunek_wartosc_aktualna": aktualna_warunku,
                "powod": "warunek_niespelniony_pomijam_kolekcje",
            }
            logger.debug(
                "watchdog: warunek kolekcji %r niespełniony — pomijam sprawdzenie",
                kolekcja.nazwa,
                extra={"event": "watchdog.warunek_niespelniony", **szczegoly},
            )
            return WynikSprawdzenia(kolekcja, "pominieto_warunek", szczegoly, czas_ms)

    # ── KROK 2: sprawdzenie pozycji — obecność I/LUB wartość ─────────────────
    problemy: list[dict] = []
    for p in kolekcja.pozycje:
        aktualna = os.environ.get(p.klucz)
        if aktualna is None:
            problemy.append({"klucz": p.klucz, "problem": "brak_zmiennej", "wartosc_wzorcowa": p.wartosc_wzorcowa})
        elif p.wartosc_wzorcowa is not None and aktualna != p.wartosc_wzorcowa:
            problemy.append({
                "klucz": p.klucz, "problem": "niezgodna_wartosc",
                "wartosc_aktualna": aktualna, "wartosc_wzorcowa": p.wartosc_wzorcowa,
            })

    czas_ms = (time.monotonic() - t0) * 1000
    szczegoly = {
        "kolekcja": kolekcja.nazwa,
        "typ": "env",
        "tryb_naprawy": kolekcja.tryb_naprawy,
        "warunek_klucz": kolekcja.warunek_klucz,
        "warunek_wartosc": kolekcja.warunek_wartosc,
        "sprawdzone_pozycje": [p.klucz for p in kolekcja.pozycje],
        "problemy": problemy,
        "watchdog_env_snapshot_liczba_zmiennych": len(os.environ),
    }

    if not problemy:
        return WynikSprawdzenia(kolekcja, "ok", szczegoly, czas_ms)

    wynik: WynikType = "zablokowano" if kolekcja.tryb_naprawy == "block" else "env_brak"
    logger.warning(
        "watchdog: niespójność w kolekcji ENV %r: %s",
        kolekcja.nazwa, problemy,
        extra={"event": "watchdog.env_niespojnosc", **szczegoly},
    )
    return WynikSprawdzenia(kolekcja, wynik, szczegoly, czas_ms)


def sprawdz_kolekcje_db(conn: pyodbc.Connection, kolekcja: Kolekcja) -> WynikSprawdzenia:
    """
    Sprawdza wartości w skw_SystemConfig względem wartosc_wzorcowa.
    tryb_naprawy='auto_fix' → UPDATE do wzorca w tej samej transakcji + audyt.
    tryb_naprawy='block'    → tylko raportuje niespójność (blokadę wykonuje pętla główna).
    tryb_naprawy='log_only' → tylko loguje.
    """
    t0 = time.monotonic()
    cur = conn.cursor()
    niespojne: list[dict] = []

    for p in kolekcja.pozycje:
        cur.execute(
            "SELECT ConfigValue FROM dbo.skw_SystemConfig WHERE ConfigKey = ?", p.klucz
        )
        row = cur.fetchone()
        aktualna = row.ConfigValue if row else None

        if aktualna != p.wartosc_wzorcowa:
            niespojne.append({
                "klucz": p.klucz,
                "wartosc_aktualna": aktualna,
                "wartosc_wzorcowa": p.wartosc_wzorcowa,
            })

    czas_ms = (time.monotonic() - t0) * 1000
    szczegoly = {
        "kolekcja": kolekcja.nazwa,
        "typ": "db",
        "tryb_naprawy": kolekcja.tryb_naprawy,
        "niespojne": niespojne,
    }

    if not niespojne:
        return WynikSprawdzenia(kolekcja, "ok", szczegoly, czas_ms)

    logger.warning(
        "watchdog: niespójność w kolekcji DB %r: %s",
        kolekcja.nazwa, niespojne,
        extra={"event": "watchdog.db_niespojnosc", **szczegoly},
    )

    if kolekcja.tryb_naprawy == "auto_fix":
        for item in niespojne:
            cur.execute(
                "UPDATE dbo.skw_SystemConfig SET ConfigValue = ? WHERE ConfigKey = ?",
                item["wartosc_wzorcowa"], item["klucz"],
            )
        conn.commit()
        # ── Audyt naprawy — pełny kontekst, przed/po ────────────────────────
        cur.execute("""
            INSERT INTO dbo.skw_AuditLog
                (Timestamp, Action, ActionCategory, EntityType, EntityID,
                 OldValue, NewValue, Success, Details, Username)
            VALUES (SYSUTCDATETIME(), 'watchdog.auto_fix', 'Watchdog',
                    'skw_WatchdogKolekcje', ?, ?, ?, 1, ?, 'watchdog-system')
        """,
            str(kolekcja.id_kolekcja),
            json.dumps({i["klucz"]: i["wartosc_aktualna"] for i in niespojne}, ensure_ascii=False),
            json.dumps({i["klucz"]: i["wartosc_wzorcowa"] for i in niespojne}, ensure_ascii=False),
            json.dumps(szczegoly, ensure_ascii=False),
        )
        conn.commit()
        return WynikSprawdzenia(kolekcja, "naprawiono", szczegoly, czas_ms)

    if kolekcja.tryb_naprawy == "block":
        return WynikSprawdzenia(kolekcja, "zablokowano", szczegoly, czas_ms)

    return WynikSprawdzenia(kolekcja, "niespojnosc", szczegoly, czas_ms)


def zapisz_wynik(conn: pyodbc.Connection, wynik: WynikSprawdzenia, instance_id: str) -> None:
    """Zapis do append-only skw_WatchdogRunLog + niezależny plik JSONL."""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dbo.skw_WatchdogRunLog
            (id_kolekcja, watchdog_instance_id, wynik, szczegoly_json, czas_trwania_ms)
        VALUES (?, ?, ?, ?, ?)
    """,
        wynik.kolekcja.id_kolekcja, instance_id, wynik.wynik,
        json.dumps(wynik.szczegoly, ensure_ascii=False), int(wynik.czas_trwania_ms),
    )
    conn.commit()

    _jsonl_log({
        "ts": datetime.now(timezone.utc).isoformat(),
        "watchdog_instance_id": instance_id,
        "kolekcja": wynik.kolekcja.nazwa,
        "wynik": wynik.wynik,
        "czas_trwania_ms": round(wynik.czas_trwania_ms, 2),
        "szczegoly": wynik.szczegoly,
    })