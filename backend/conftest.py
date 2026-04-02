# =============================================================================
# PLIK  : backend/conftest.py
# MODUŁ : Self-test — Windykacja Sprint 2.3
# OPIS  : Fixtures wspólne dla wszystkich testów.
#         Uruchomienie: docker exec windykacja_api python -m tests.runner
#
# KLUCZOWA ZASADA (naprawiona w Sprint 2.3):
#   authed_client tworzy NOWY klient httpx z tokenem — NIE mutuje http_client.
#   Dzięki temu http_client pozostaje "czysty" (bez tokenu) przez całą sesję,
#   co pozwala na poprawne testowanie odpowiedzi 401.
#
#   Poprzedni błąd:
#     authed_client = http_client + Authorization: Bearer xxx
#     → http_client stawał się klientem z tokenem
#     → wszystkie testy 401 używające http_client dostawały odpowiedź 200/etc.
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

BASE_URL       = os.environ.get("SELFTEST_BASE_URL", "http://localhost:8000/api/v1")
ADMIN_USERNAME = os.environ.get("SELFTEST_USERNAME", "admin")
ADMIN_PASSWORD = os.environ.get("SELFTEST_PASSWORD", os.environ.get("ADMIN_PASSWORD", ""))
TIMEOUT        = float(os.environ.get("SELFTEST_TIMEOUT", "30"))

# KSEF_ID używane wyłącznie przez testy — czyszczone po teście
TEST_KSEF_ID = "SELFTEST-KSEF-9999"
TEST_NUMER   = "SELFTEST/2026/9999"

# Dane testowe dla nowych modułów (Sprint 2.3)
TEST_USER_USERNAME = "selftest_user_9999"
TEST_USER_EMAIL    = "selftest9999@windykacja.test"
TEST_USER_PASSWORD = "SelfTest!9999"
TEST_ROLE_NAME     = "SELFTEST_Rola_9999"
TEST_PERM_NAME     = "system.selftest_auto_9999"
TEST_TEMPLATE_NAME = "SELFTEST_Szablon_9999"


# ---------------------------------------------------------------------------
# Fixtures bazowe
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def base_url() -> str:
    return BASE_URL


@pytest.fixture(scope="session")
def http_client() -> Generator[httpx.Client, None, None]:
    """
    Synchroniczny klient HTTP BEZ tokenu autoryzacji.
    Używaj do testów 401 (nieautoryzowany dostęp).
    Pozostaje czysty przez całą sesję — authed_client go NIE mutuje.
    """
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

    if resp.status_code != 200:
        pytest.fail(
            f"Logowanie nieudane: HTTP {resp.status_code}\n"
            f"URL: {BASE_URL}/auth/login\n"
            f"Body: {resp.text[:400]}"
        )

    data = resp.json()
    token = _wyciagnij_token(data)

    if not token:
        pytest.fail(
            f"Brak access_token/token w odpowiedzi /auth/login!\n"
            f"Struktura: {list(data.keys())}\n"
            f"Odpowiedź: {resp.text[:400]}"
        )

    logger.info("[conftest] Zalogowano jako %s (token len=%d)", ADMIN_USERNAME, len(token))
    return token


def _wyciagnij_token(data: dict) -> str | None:
    """Wyciąga token z różnych formatów odpowiedzi /auth/login."""
    if data.get("access_token"):
        return data["access_token"]
    if data.get("token"):
        return data["token"]
    nested = data.get("data")
    if isinstance(nested, dict):
        return nested.get("access_token") or nested.get("token")
    return None


@pytest.fixture(scope="session")
def authed_client(auth_token: str) -> Generator[httpx.Client, None, None]:
    """
    Klient HTTP Z tokenem autoryzacji admina.

    WAŻNE: Tworzy NOWY obiekt httpx.Client — NIE modyfikuje http_client.
    Dzięki temu http_client pozostaje bez tokenu przez całą sesję.

    Używaj do testów wymagających uprawnień admina (~90% testów).
    """
    with httpx.Client(
        base_url=BASE_URL,
        timeout=TIMEOUT,
        headers={"Authorization": f"Bearer {auth_token}"},
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# Fixture: faktura testowa (Sprint 2.2 — bez zmian)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def faktura_testowa_id(authed_client: httpx.Client) -> Generator[int, None, None]:
    """
    Tworzy fakturę testową przed testami modułu faktur.
    Sprząta (usuwa z obiegu przez force_status=anulowana) po wszystkich testach.

    Wymaga istnienia rekordu w dbo.BUF_DOKUMENT z KSEF_ID = TEST_KSEF_ID.
    Jeśli nie istnieje — test jest pomijany z czytelnym komunikatem.
    """
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

    _cleanup_faktura(authed_client, faktura_id)


def _cleanup_faktura(client: httpx.Client, faktura_id: int) -> None:
    """Anuluje fakturę testową przez force_status."""
    try:
        r1 = client.patch(
            f"/faktury-akceptacja/{faktura_id}/status",
            json={"nowy_status": "anulowana", "powod": "SELFTEST cleanup"},
        )
        if r1.status_code != 200:
            logger.warning(
                "[conftest] Cleanup krok 1 nieudany: %d — %s",
                r1.status_code, r1.text[:100],
            )
            return

        token = r1.json().get("confirm_token")
        if not token:
            logger.warning("[conftest] Brak confirm_token w odpowiedzi cleanup")
            return

        r2 = client.post(
            f"/faktury-akceptacja/{faktura_id}/status/confirm",
            json={"confirm_token": token},
        )
        if r2.status_code == 200:
            logger.info("[conftest] Faktura testowa ID=%d anulowana (cleanup OK)", faktura_id)
        else:
            logger.warning(
                "[conftest] Cleanup krok 2 nieudany: %d — %s",
                r2.status_code, r2.text[:100],
            )
    except Exception as exc:
        logger.warning("[conftest] Błąd cleanup faktury: %s", exc)


# ---------------------------------------------------------------------------
# Fixture: użytkownik testowy (Sprint 2.3)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def user_testowy_id(authed_client: httpx.Client) -> Generator[int, None, None]:
    """Tworzy użytkownika selftest_user_9999 i zwraca ID. Cleanup po sesji."""
    _cleanup_user_by_username(authed_client, TEST_USER_USERNAME)

    resp = authed_client.post(
        "/users",
        json={
            "username":  TEST_USER_USERNAME,
            "email":     TEST_USER_EMAIL,
            "password":  TEST_USER_PASSWORD,
            "role_id":   1,
            "full_name": "SELFTEST Użytkownik Automatyczny",
        },
    )

    if resp.status_code not in (200, 201):
        pytest.skip(
            f"Nie można utworzyć użytkownika testowego: "
            f"{resp.status_code} — {resp.text[:200]}"
        )
        return

    data = resp.json()
    uid = (
        data.get("id")
        or data.get("id_user")
        or data.get("data", {}).get("id")
        or data.get("data", {}).get("id_user")
    )
    assert uid, f"Brak ID w odpowiedzi POST /users: {resp.text[:200]}"
    logger.info("[conftest] User testowy ID=%d utworzony (%s)", uid, TEST_USER_USERNAME)

    yield uid

    _cleanup_user(authed_client, uid)
    logger.info("[conftest] User testowy ID=%d usunięty", uid)


def _cleanup_user_by_username(client: httpx.Client, username: str) -> None:
    """Szuka usera po username i usuwa jeśli istnieje."""
    try:
        lista = client.get("/users", params={"search": username})
        if lista.status_code != 200:
            return
        data = lista.json().get("data", {})
        items = data if isinstance(data, list) else data.get("items", [])
        if not isinstance(items, list):
            items = []
        for u in items:
            if u.get("username") == username:
                uid = u.get("id") or u.get("id_user")
                if uid:
                    _cleanup_user(client, uid)
    except Exception as exc:
        logger.warning("[conftest] _cleanup_user_by_username error: %s", exc)


def _cleanup_user(client: httpx.Client, uid: int) -> None:
    """Soft-delete użytkownika przez 2-krokowy flow."""
    try:
        r1 = client.delete(f"/users/{uid}/initiate")
        if r1.status_code == 202:
            token = r1.json().get("delete_token") or r1.json().get("confirm_token")
            if token:
                client.delete(f"/users/{uid}/confirm", json={"confirm_token": token})
    except Exception as exc:
        logger.warning("[conftest] _cleanup_user(%d) error: %s", uid, exc)


# ---------------------------------------------------------------------------
# Fixture: rola testowa (Sprint 2.3)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def rola_testowa_id(authed_client: httpx.Client) -> Generator[int, None, None]:
    """Tworzy rolę SELFTEST_Rola_9999 i zwraca ID. Cleanup po sesji."""
    resp = authed_client.post(
        "/roles",
        json={
            "name":        TEST_ROLE_NAME,
            "description": "Rola testowa — automat SELFTEST Sprint 2.3.",
        },
    )

    if resp.status_code == 409:
        lista = authed_client.get("/roles")
        if lista.status_code == 200:
            data = lista.json().get("data", {})
            roles = data if isinstance(data, list) else data.get("items", [])
            if not isinstance(roles, list):
                roles = []
            for r in roles:
                if r.get("name") == TEST_ROLE_NAME:
                    rid = r.get("id") or r.get("id_role")
                    logger.warning("[conftest] Rola testowa ID=%d już istnieje", rid)
                    yield rid
                    _cleanup_role(authed_client, rid)
                    return

    if resp.status_code not in (200, 201):
        pytest.skip(
            f"Nie można utworzyć roli testowej: "
            f"{resp.status_code} — {resp.text[:200]}"
        )
        return

    data = resp.json()
    rid = data.get("id") or data.get("id_role") or data.get("data", {}).get("id")
    assert rid, f"Brak ID w odpowiedzi POST /roles: {resp.text[:200]}"
    logger.info("[conftest] Rola testowa ID=%d utworzona (%s)", rid, TEST_ROLE_NAME)

    yield rid

    _cleanup_role(authed_client, rid)
    logger.info("[conftest] Rola testowa ID=%d usunięta", rid)


def _cleanup_role(client: httpx.Client, rid: int) -> None:
    try:
        r1 = client.delete(f"/roles/{rid}/initiate")
        if r1.status_code == 202:
            token = r1.json().get("delete_token") or r1.json().get("confirm_token")
            if token:
                client.delete(f"/roles/{rid}/confirm", json={"confirm_token": token})
    except Exception as exc:
        logger.warning("[conftest] _cleanup_role(%d) error: %s", rid, exc)


# ---------------------------------------------------------------------------
# Fixture: uprawnienie testowe (Sprint 2.3)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def permission_testowy_id(authed_client: httpx.Client) -> Generator[int, None, None]:
    """Tworzy uprawnienie system.selftest_auto_9999 i zwraca ID. Cleanup po sesji."""
    resp = authed_client.post(
        "/permissions",
        json={
            "name":        TEST_PERM_NAME,
            "description": "SELFTEST uprawnienie automatyczne Sprint 2.3",
            "category":    "system",
        },
    )

    if resp.status_code == 409:
        lista = authed_client.get("/permissions", params={"category": "system"})
        if lista.status_code == 200:
            items = _flatten_permissions(lista.json())
            for p in items:
                pname = p.get("name") or p.get("permission_name", "")
                if pname == TEST_PERM_NAME:
                    pid = p.get("id") or p.get("id_permission")
                    logger.warning("[conftest] Permission testowa ID=%d już istnieje", pid)
                    yield pid
                    _cleanup_permission(authed_client, pid)
                    return

    if resp.status_code not in (200, 201):
        pytest.skip(
            f"Nie można utworzyć uprawnienia testowego: "
            f"{resp.status_code} — {resp.text[:200]}"
        )
        return

    data = resp.json()
    pid = data.get("id") or data.get("id_permission") or data.get("data", {}).get("id")
    assert pid, f"Brak ID w odpowiedzi POST /permissions: {resp.text[:200]}"
    logger.info("[conftest] Permission testowa ID=%d utworzona (%s)", pid, TEST_PERM_NAME)

    yield pid

    _cleanup_permission(authed_client, pid)
    logger.info("[conftest] Permission testowa ID=%d usunięta", pid)


def _flatten_permissions(resp_json: dict) -> list:
    """Spłaszcza odpowiedź /permissions (może być dict po kategorii lub lista)."""
    data = resp_json.get("data", {})
    items = data.get("items", data) if isinstance(data, dict) else data
    if isinstance(items, list):
        return items
    if isinstance(items, dict):
        result: list = []
        for v in items.values():
            if isinstance(v, list):
                result.extend(v)
        return result
    return []


def _cleanup_permission(client: httpx.Client, pid: int) -> None:
    try:
        r1 = client.delete(f"/permissions/{pid}/initiate")
        if r1.status_code == 202:
            token = r1.json().get("delete_token") or r1.json().get("confirm_token")
            if token:
                client.delete(f"/permissions/{pid}/confirm", json={"confirm_token": token})
    except Exception as exc:
        logger.warning("[conftest] _cleanup_permission(%d) error: %s", pid, exc)


# ---------------------------------------------------------------------------
# Fixture: dłużnik testowy — read-only WAPRO (Sprint 2.3)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def debtor_testowy_id(authed_client: httpx.Client) -> int:
    """
    Pobiera ID pierwszego dostępnego dłużnika z WAPRO.
    Read-only — brak cleanup. Skip jeśli brak danych.

    Próbuje wiele możliwych nazw pola ID (format zależy od widoku WAPRO).
    """
    resp = authed_client.get("/debtors", params={"per_page": 1})
    if resp.status_code != 200:
        pytest.skip(f"GET /debtors zwrócił {resp.status_code} — skip testów dłużników")

    data = resp.json().get("data", {})
    items = data if isinstance(data, list) else data.get("items", [])
    if not isinstance(items, list):
        items = []
    if not items:
        pytest.skip("Brak dłużników w WAPRO — skip testów dłużników")

    first = items[0]

    # Próbujemy wiele możliwych nazw pola ID (WAPRO może używać różnych konwencji)
    id_candidates = [
        "id", "id_kontrahenta", "debtor_id", "kontrahent_id",
        "ID_KONTRAHENTA", "Id", "ID", "idKontrahenta",
    ]
    did = None
    for key in id_candidates:
        val = first.get(key)
        if val is not None and isinstance(val, (int, str)) and str(val).isdigit():
            did = int(val)
            break

    # Ostatnia deska ratunku — szukaj pierwszego pola z "id" w nazwie
    if not did:
        for key, val in first.items():
            if "id" in key.lower() and isinstance(val, int) and val > 0:
                did = val
                logger.warning(
                    "[conftest] debtor_testowy_id: użyto klucza '%s' = %d. "
                    "Dodaj ten klucz do id_candidates w conftest.py.",
                    key, val,
                )
                break

    if not did:
        pytest.skip(
            f"Nie można wyciągnąć ID dłużnika. "
            f"Dostępne klucze: {list(first.keys())}. "
            f"Przykładowe wartości: { {k: first[k] for k in list(first.keys())[:5]} }"
        )

    logger.info("[conftest] Dłużnik testowy ID=%d (WAPRO read-only)", did)
    return did


# ---------------------------------------------------------------------------
# Fixture: komentarz testowy (Sprint 2.3)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def comment_testowy_id(
    authed_client: httpx.Client,
    debtor_testowy_id: int,
) -> Generator[tuple, None, None]:
    """Tworzy komentarz do dłużnika testowego. Zwraca (debtor_id, comment_id)."""
    resp = authed_client.post(
        f"/debtors/{debtor_testowy_id}/comments",
        json={"tresc": "SELFTEST komentarz automatyczny 9999 — nie usuwać ręcznie"},
    )

    if resp.status_code not in (200, 201):
        pytest.skip(
            f"Nie można utworzyć komentarza testowego: "
            f"{resp.status_code} — {resp.text[:200]}"
        )
        return

    data = resp.json()
    # Rzeczywisty format API: {"data": {"id_comment": 6, "id_kontrahenta": 4, ...}}
    nested = data.get("data", {})
    cid = (
        nested.get("id_comment")        # ← aktualny klucz w API
        or nested.get("id")
        or nested.get("comment_id")
        or data.get("id_comment")
        or data.get("id")
        or data.get("comment_id")
    )
    assert cid, f"Brak ID komentarza: {resp.text[:200]}"
    logger.info("[conftest] Komentarz testowy ID=%d (debtor=%d)", cid, debtor_testowy_id)

    yield (debtor_testowy_id, cid)

    try:
        r1 = authed_client.delete(
            f"/debtors/{debtor_testowy_id}/comments/{cid}/initiate"
        )
        if r1.status_code == 202:
            token = r1.json().get("delete_token") or r1.json().get("confirm_token")
            if token:
                authed_client.delete(
                    f"/debtors/{debtor_testowy_id}/comments/{cid}/confirm",
                    json={"confirm_token": token},
                )
        logger.info("[conftest] Komentarz testowy ID=%d usunięty", cid)
    except Exception as exc:
        logger.warning("[conftest] Cleanup komentarza %d error: %s", cid, exc)


# ---------------------------------------------------------------------------
# Fixture: szablon testowy (Sprint 2.3)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def template_testowy_id(authed_client: httpx.Client) -> Generator[int, None, None]:
    """Tworzy szablon SELFTEST_Szablon_9999 i zwraca ID. Cleanup po sesji."""
    resp = authed_client.post(
        "/templates",
        json={
            "name":        TEST_TEMPLATE_NAME,
            "type":        "email",
            "subject":     "SELFTEST — Temat testowy",
            "body":        "Szanowny Kliencie SELFTEST, to jest wiadomość testowa.",
            "description": "Szablon automatyczny SELFTEST Sprint 2.3",
        },
    )

    if resp.status_code not in (200, 201):
        pytest.skip(
            f"Nie można utworzyć szablonu testowego: "
            f"{resp.status_code} — {resp.text[:200]}"
        )
        return

    data = resp.json()
    tid = data.get("id") or data.get("data", {}).get("id")
    assert tid, f"Brak ID szablonu: {resp.text[:200]}"
    logger.info("[conftest] Szablon testowy ID=%d utworzony (%s)", tid, TEST_TEMPLATE_NAME)

    yield tid

    try:
        r = authed_client.delete(f"/templates/{tid}")
        logger.info("[conftest] Szablon testowy ID=%d usunięty (status=%d)", tid, r.status_code)
    except Exception as exc:
        logger.warning("[conftest] Cleanup szablonu %d error: %s", tid, exc)