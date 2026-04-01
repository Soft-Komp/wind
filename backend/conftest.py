# =============================================================================
# PLIK  : backend/tests/conftest.py
# MODUŁ : Self-test — Windykacja Sprint 2.2
# OPIS  : Fixtures wspólne dla wszystkich testów.
#         Uruchomienie: docker exec windykacja_api python -m tests.runner
# =============================================================================
from __future__ import annotations

import logging
import os
import time
from typing import Generator

import httpx
import pytest

logger = logging.getLogger("windykacja.tests")

# ---------------------------------------------------------------------------
# Konfiguracja
# ---------------------------------------------------------------------------

BASE_URL = os.environ.get("SELFTEST_BASE_URL", "http://localhost:8000/api/v1")
ADMIN_USERNAME = os.environ.get("SELFTEST_USERNAME", "admin")
ADMIN_PASSWORD = os.environ.get("SELFTEST_PASSWORD", os.environ.get("ADMIN_PASSWORD", ""))
TIMEOUT = float(os.environ.get("SELFTEST_TIMEOUT", "30"))

# KSEF_ID używane wyłącznie przez testy — czyszczone po teście
TEST_KSEF_ID = "SELFTEST-KSEF-9999"
TEST_NUMER   = "SELFTEST/2026/9999"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def base_url() -> str:
    return BASE_URL


@pytest.fixture(scope="session")
def http_client() -> Generator[httpx.Client, None, None]:
    """Synchroniczny klient HTTP — współdzielony przez całą sesję testową."""
    with httpx.Client(base_url=BASE_URL, timeout=TIMEOUT) as client:
        yield client


@pytest.fixture(scope="session")
def auth_token(http_client: httpx.Client) -> str:
    """
    Loguje się jako admin i zwraca access_token.
    Jeśli logowanie się nie uda — wszystkie testy zależne są pomijane.
    """
    if not ADMIN_PASSWORD:
        pytest.skip(
            "Brak hasła admina — ustaw SELFTEST_PASSWORD lub ADMIN_PASSWORD w .env"
        )

    resp = http_client.post(
        "/auth/login",
        json={"username": ADMIN_USERNAME, "password": ADMIN_PASSWORD},
    )
    assert resp.status_code == 200, (
        f"Logowanie nieudane: {resp.status_code} — {resp.text[:200]}"
    )
    data = resp.json()
    token = data.get("access_token") or data.get("data", {}).get("access_token")
    assert token, f"Brak access_token w odpowiedzi: {data}"
    logger.info("[conftest] Zalogowano jako %s", ADMIN_USERNAME)
    return token


@pytest.fixture(scope="session")
def authed_client(http_client: httpx.Client, auth_token: str) -> httpx.Client:
    """Klient HTTP z nagłówkiem Authorization."""
    http_client.headers.update({"Authorization": f"Bearer {auth_token}"})
    return http_client


@pytest.fixture(scope="session")
def faktura_testowa_id(authed_client: httpx.Client) -> Generator[int, None, None]:
    """
    Tworzy fakturę testową przed testami modułu faktur.
    Sprząta (usuwa z obiegu przez force_status=anulowana) po wszystkich testach.

    Wymaga istnienia rekordu w dbo.BUF_DOKUMENT z KSEF_ID = TEST_KSEF_ID.
    Jeśli nie istnieje — test jest pomijany z czytelnym komunikatem.
    """
    # Próba wpuszczenia faktury do obiegu
    resp = authed_client.post(
        "/faktury-akceptacja",
        json={
            "numer_ksef":      TEST_KSEF_ID,
            "priorytet":       "normalny",
            "opis_dokumentu":  "SELFTEST — automatyczny test regresyjny",
            "uwagi":           f"Utworzono przez selftest o {time.strftime('%H:%M:%S')}",
            "user_ids":        [1],
        },
    )

    if resp.status_code == 409:
        # Faktura już w obiegu z poprzedniego przerwanego testu — pobierz ID
        lista = authed_client.get("/faktury-akceptacja", params={"search": TEST_KSEF_ID})
        items = lista.json().get("data", [])
        existing = [f for f in items if f.get("numer_ksef") == TEST_KSEF_ID]
        if existing:
            faktura_id = existing[0]["id"]
            logger.warning(
                "[conftest] Faktura testowa ID=%d już istnieje — używam istniejącej",
                faktura_id,
            )
            yield faktura_id
            _cleanup_faktura(authed_client, faktura_id)
            return
        pytest.skip(
            f"Faktura {TEST_KSEF_ID} w obiegu ale nie znaleziona na liście — "
            "sprawdź bazę ręcznie"
        )

    if resp.status_code == 422 or (
        resp.status_code == 404
        and "nie znaleziono" in resp.text.lower()
    ):
        pytest.skip(
            f"Brak rekordu {TEST_KSEF_ID} w dbo.BUF_DOKUMENT. "
            "Dodaj go w SSMS przed uruchomieniem selftestów:\n"
            f"  INSERT INTO dbo.BUF_DOKUMENT "
            f"(KSEF_ID, NUMER, PRG_KOD, TYP, KOD_STATUSU, ID_KONTRAHENTA) "
            f"VALUES ('{TEST_KSEF_ID}', '{TEST_NUMER}', 3, 'Z', NULL, 1)"
        )

    assert resp.status_code == 201, (
        f"Nie udało się utworzyć faktury testowej: {resp.status_code} — {resp.text[:300]}"
    )

    faktura_id: int = resp.json()["id"]
    logger.info("[conftest] Faktura testowa ID=%d utworzona (%s)", faktura_id, TEST_KSEF_ID)

    yield faktura_id

    # ── Cleanup ───────────────────────────────────────────────────────────────
    _cleanup_faktura(authed_client, faktura_id)


def _cleanup_faktura(client: httpx.Client, faktura_id: int) -> None:
    """Anuluje fakturę testową przez force_status."""
    try:
        # Krok 1 — pobierz token potwierdzający
        r1 = client.patch(
            f"/faktury-akceptacja/{faktura_id}/status",
            json={"nowy_status": "anulowana", "powod": "SELFTEST cleanup"},
        )
        if r1.status_code != 200:
            logger.warning(
                "[conftest] Cleanup krok 1 nieudany: %d — %s",
                r1.status_code,
                r1.text[:100],
            )
            return

        token = r1.json().get("confirm_token")
        if not token:
            logger.warning("[conftest] Brak confirm_token w odpowiedzi cleanup")
            return

        # Krok 2 — potwierdź anulowanie
        r2 = client.post(
            f"/faktury-akceptacja/{faktura_id}/status/confirm",
            json={"confirm_token": token},
        )
        if r2.status_code == 200:
            logger.info("[conftest] Faktura testowa ID=%d anulowana (cleanup OK)", faktura_id)
        else:
            logger.warning(
                "[conftest] Cleanup krok 2 nieudany: %d — %s",
                r2.status_code,
                r2.text[:100],
            )
    except Exception as exc:
        logger.warning("[conftest] Błąd cleanup faktury: %s", exc)