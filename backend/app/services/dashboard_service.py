"""
dashboard_service.py
====================
Serwis danych dashboardu — System Windykacja.

Trzy źródła danych (widoki SQL w schemacie dbo):
  - skw_dashboard_debt_stats   → agregaty zadłużenia + top dłużnicy
  - skw_dashboard_monit_stats  → statystyki monitów (globalne, per kanał, trend)
  - skw_dashboard_activity     → oś czasu ostatniej aktywności

Wszystkie endpointy cachowane w Redis (TTL konfigurowalny).
Zapytania przez pyodbc pool (ten sam co wapro.py).
"""
from __future__ import annotations

import logging
import time
import traceback
import uuid
from typing import Any, Optional

import orjson
from redis.asyncio import Redis

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache TTL (sekundy)
# ---------------------------------------------------------------------------
_CACHE_TTL_DEBT_STATS:  int = 120   # 2 min — dane finansowe zmieniają się rzadko
_CACHE_TTL_MONIT_STATS: int = 60    # 1 min — statystyki monitów
_CACHE_TTL_ACTIVITY:    int = 30    # 30s  — aktywność zmienia się często

_CACHE_KEY_DEBT_STATS  = "dashboard:debt_stats"
_CACHE_KEY_MONIT_STATS = "dashboard:monit_stats"
_CACHE_KEY_ACTIVITY    = "dashboard:activity:{limit}"


# ---------------------------------------------------------------------------
# Helper: wykonaj query przez pyodbc pool
# ---------------------------------------------------------------------------

async def _run_query(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    """
    Wykonuje zapytanie SQL przez istniejący pool pyodbc z wapro.py.
    Zwraca listę słowników (rows).
    Przy błędzie rzuca wyjątek — caller obsługuje.
    """
    from app.db.wapro import _run_in_executor, _execute_query_sync

    query_id = str(uuid.uuid4())[:8]
    t_start = time.monotonic()

    try:
        rows = await _run_in_executor(
            _execute_query_sync,
            sql, params, query_id, "dashboard",
        )
        duration_ms = (time.monotonic() - t_start) * 1000
        logger.debug(
            orjson.dumps({
                "event":       "dashboard_query_ok",
                "query_id":    query_id,
                "rows":        len(rows),
                "duration_ms": round(duration_ms, 2),
            }).decode()
        )
        return rows
    except Exception as exc:
        duration_ms = (time.monotonic() - t_start) * 1000
        logger.error(
            orjson.dumps({
                "event":       "dashboard_query_error",
                "query_id":    query_id,
                "error":       str(exc),
                "duration_ms": round(duration_ms, 2),
                "traceback":   traceback.format_exc(),
            }).decode()
        )
        raise


# ---------------------------------------------------------------------------
# Helper: cache Redis
# ---------------------------------------------------------------------------

async def _cache_get(redis: Redis, key: str) -> Optional[dict]:
    try:
        cached = await redis.get(key)
        if cached:
            return orjson.loads(cached)
    except Exception as exc:
        logger.warning("dashboard cache read error key=%s: %s", key, exc)
    return None


async def _cache_set(redis: Redis, key: str, data: dict, ttl: int) -> None:
    try:
        await redis.setex(key, ttl, orjson.dumps(data, default=str))
    except Exception as exc:
        logger.warning("dashboard cache write error key=%s: %s", key, exc)


# ---------------------------------------------------------------------------
# Helper: konwersja row → dict (pyodbc zwraca Row, nie dict)
# ---------------------------------------------------------------------------

def _row_to_dict(row: Any) -> dict[str, Any]:
    """
    Konwertuje pyodbc Row na dict.
    Wartości Decimal i date → float/str dla JSON.
    """
    if isinstance(row, dict):
        return row
    # pyodbc Row ma cursor.description — obsługujemy przez keys jeśli dostępne
    try:
        return dict(zip(row.cursor_description, row))
    except Exception:
        return {}


# ===========================================================================
# 1. GET /dashboard/debt-stats
# ===========================================================================

async def get_debt_stats(redis: Redis) -> dict[str, Any]:
    """
    Agregaty zadłużenia z dbo.skw_dashboard_debt_stats.

    Zwraca:
      - stats:      wiersz globalny (CalkowiteZadluzenie, kategorie wiekowe, Zagrozone)
      - top_debtors: lista dłużników posortowana po SumaDlugu DESC (top 20)
    """
    cached = await _cache_get(redis, _CACHE_KEY_DEBT_STATS)
    if cached:
        logger.debug("dashboard debt_stats: cache hit")
        return cached

    rows = await _run_query(
        "SELECT * FROM dbo.skw_dashboard_debt_stats",
    )

    # Rozdziel globalny wiersz od wierszy per-kontrahent
    stats_row: Optional[dict] = None
    top_debtors: list[dict] = []

    for r in rows:
        if isinstance(r, dict):
            row = r
        else:
            # pyodbc Row → dict przez kolumny z cursor_description
            row = {col[0]: val for col, val in zip(
                getattr(r, "cursor_description", []),
                r if hasattr(r, "__iter__") else []
            )}

        id_kontrahenta = row.get("ID_KONTRAHENTA")
        if id_kontrahenta is None:
            # Wiersz globalny
            stats_row = {
                "calkowite_zadluzenie": float(row.get("SumaDlugu") or 0),
                "liczba_kontrahentow":  int(row.get("LiczbaKontrahentow") or 0),
                "zagrozone":            float(row.get("Zagrozone") or 0),
                "kategorie": {
                    "do_30_dni":    float(row.get("Kat_0_30") or 0),
                    "dni_31_60":    float(row.get("Kat_31_60") or 0),
                    "dni_61_90":    float(row.get("Kat_61_90") or 0),
                    "powyzej_90":   float(row.get("Kat_Powyzej90") or 0),
                },
            }
        else:
            # Wiersz per-kontrahent
            top_debtors.append({
                "id_kontrahenta":    int(id_kontrahenta),
                "nazwa_kontrahenta": row.get("NazwaKontrahenta"),
                "suma_dlugu":        float(row.get("SumaDlugu") or 0),
                "liczba_faktur":     int(row.get("LiczbaFaktur") or 0),
                "dni_przeterminowania": int(row.get("DniPrzeterminowania") or 0),
            })

    # Sortuj top dłużników malejąco po kwocie, ogranicz do 20
    top_debtors.sort(key=lambda x: x["suma_dlugu"], reverse=True)
    top_debtors = top_debtors[:20]

    result = {
        "stats":       stats_row or {},
        "top_debtors": top_debtors,
        "total_in_top": len(top_debtors),
    }

    await _cache_set(redis, _CACHE_KEY_DEBT_STATS, result, _CACHE_TTL_DEBT_STATS)

    logger.info(
        orjson.dumps({
            "event":          "dashboard_debt_stats_fetched",
            "top_debtors":    len(top_debtors),
            "total_rows":     len(rows),
        }).decode()
    )

    return result


# ===========================================================================
# 2. GET /dashboard/monit-stats
# ===========================================================================

async def get_monit_stats(redis: Redis) -> dict[str, Any]:
    """
    Statystyki monitów z dbo.skw_dashboard_monit_stats.

    Zwraca:
      - global:  agregat wszystkich monitów (WyslaneRazem, Skutecznosc)
      - kanaly:  per kanał (email, sms, print) — ostatnie 30 dni
      - trend:   per miesiąc — ostatnie 6 miesięcy
    """
    cached = await _cache_get(redis, _CACHE_KEY_MONIT_STATS)
    if cached:
        logger.debug("dashboard monit_stats: cache hit")
        return cached

    rows = await _run_query(
        "SELECT * FROM dbo.skw_dashboard_monit_stats",
    )

    globalny: Optional[dict] = None
    kanaly: list[dict] = []
    trend: list[dict] = []

    for r in rows:
        row = r if isinstance(r, dict) else {
            col[0]: val for col, val in zip(
                getattr(r, "cursor_description", []),
                r if hasattr(r, "__iter__") else []
            )
        }

        typ = row.get("TypWiersza", "")
        miesiac = row.get("Miesiac")
        kanal = row.get("Kanal")

        if typ == "global":
            globalny = {
                "wyslane":      int(row.get("Wyslane") or 0),
                "otwarte":      int(row.get("Otwarte") or 0),
                "klikniete":    int(row.get("Klikniete") or 0),
                "skutecznosc":  float(row.get("Skutecznosc") or 0),
            }
        elif typ == "kanal" and kanal:
            kanaly.append({
                "kanal":        kanal,
                "wyslane":      int(row.get("Wyslane") or 0),
                "otwarte":      int(row.get("Otwarte") or 0),
                "klikniete":    int(row.get("Klikniete") or 0),
                "skutecznosc":  float(row.get("Skutecznosc") or 0),
            })
        elif typ == "trend" and miesiac:
            trend.append({
                "miesiac":      miesiac,
                "wyslane":      int(row.get("Wyslane") or 0),
                "otwarte":      int(row.get("Otwarte") or 0),
                "suma_dlugow":  float(row.get("SumaDlugow") or 0),
            })

    # Trend posortowany chronologicznie
    trend.sort(key=lambda x: x["miesiac"])

    result = {
        "global": globalny or {},
        "kanaly": kanaly,
        "trend":  trend,
    }

    await _cache_set(redis, _CACHE_KEY_MONIT_STATS, result, _CACHE_TTL_MONIT_STATS)

    logger.info(
        orjson.dumps({
            "event":   "dashboard_monit_stats_fetched",
            "kanaly":  len(kanaly),
            "trend":   len(trend),
        }).decode()
    )

    return result


# ===========================================================================
# 3. GET /dashboard/activity
# ===========================================================================

async def get_activity(redis: Redis, limit: int = 20) -> dict[str, Any]:
    """
    Oś czasu ostatniej aktywności z dbo.skw_dashboard_activity.
    Sortowanie: DataZdarzenia DESC. Limit: max 100.
    """
    limit = min(max(limit, 1), 100)
    cache_key = _CACHE_KEY_ACTIVITY.format(limit=limit)

    cached = await _cache_get(redis, cache_key)
    if cached:
        logger.debug("dashboard activity: cache hit limit=%d", limit)
        return cached

    rows = await _run_query(
        f"""
        SELECT TOP {limit}
            ID_ZDARZENIA,
            TypZdarzenia,
            Opis,
            Kontrahent,
            Kwota,
            DataZdarzenia,
            Kolor,
            ZrodloDanych
        FROM dbo.skw_dashboard_activity
        ORDER BY DataZdarzenia DESC
        """,
    )

    items: list[dict] = []
    for r in rows:
        row = r if isinstance(r, dict) else {
            col[0]: val for col, val in zip(
                getattr(r, "cursor_description", []),
                r if hasattr(r, "__iter__") else []
            )
        }

        data_zdarzenia = row.get("DataZdarzenia")
        items.append({
            "id":             int(row.get("ID_ZDARZENIA") or 0),
            "typ":            row.get("TypZdarzenia"),
            "opis":           row.get("Opis"),
            "kontrahent":     row.get("Kontrahent"),
            "kwota":          float(row.get("Kwota")) if row.get("Kwota") is not None else None,
            "data_zdarzenia": data_zdarzenia.isoformat() if hasattr(data_zdarzenia, "isoformat") else str(data_zdarzenia) if data_zdarzenia else None,
            "kolor":          row.get("Kolor"),
            "zrodlo":         row.get("ZrodloDanych"),
        })

    result = {
        "items": items,
        "total": len(items),
        "limit": limit,
    }

    await _cache_set(redis, cache_key, result, _CACHE_TTL_ACTIVITY)

    logger.info(
        orjson.dumps({
            "event":  "dashboard_activity_fetched",
            "items":  len(items),
            "limit":  limit,
        }).decode()
    )

    return result