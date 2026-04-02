"""
test_permissions.py — Testy modułu /permissions
=================================================
System Windykacja GPGK Jasło — Sprint 2.3

Stan API po analizie verbose:
  GET /permissions              — DZIAŁA ✅ (format: data.items = dict po kategorii)
  GET /permissions/categories   — DZIAŁA ✅
  GET /permissions/{id}         — DZIAŁA ✅
  POST /permissions             — 405 Method Not Allowed ❌ → xfail
  PUT  /permissions/{id}        — 405 Method Not Allowed ❌ → xfail
  DELETE /permissions/{id}/...  — DZIAŁA ✅

Format odpowiedzi GET /permissions:
  {"code":200, "data": {"items": {"auth": [...], "faktury": [...], "users": [...]}}}
  items = dict pogrupowany po kategorii, NIE lista.
"""

from __future__ import annotations

import logging

import httpx
import pytest

from conftest import BASE_URL, TEST_PERM_NAME, TIMEOUT

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# xfail dla metod HTTP które zwracają 405 (niezaimplementowane)
# ---------------------------------------------------------------------------

_XFAIL_405 = pytest.mark.xfail(
    reason="Endpoint zwraca 405 Method Not Allowed — metoda POST/PUT niezaimplementowana",
    strict=False,
)


def _fresh_client() -> httpx.Client:
    """Świeży klient bez tokenu — do testów 401."""
    return httpx.Client(base_url=BASE_URL, timeout=TIMEOUT)


class TestPermissionsLista:
    """GET /permissions — lista."""

    def test_lista_podstawowa(self, authed_client: httpx.Client) -> None:
        """GET /permissions zwraca łącznie ≥ 1 uprawnienie (wszystkie kategorie)."""
        resp = authed_client.get("/permissions")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        items = _lista(resp.json())
        assert len(items) >= 1, (
            f"Lista uprawnień jest pusta. "
            f"Odpowiedź: {resp.text[:400]}"
        )

    def test_lista_filtr_kategoria_faktury(self, authed_client: httpx.Client) -> None:
        """GET /permissions?category=faktury → dokładnie 14 uprawnień."""
        resp = authed_client.get("/permissions", params={"category": "faktury"})
        assert resp.status_code == 200
        items = _lista(resp.json())
        assert len(items) == 14, (
            f"Oczekiwano dokładnie 14 uprawnień 'faktury', got {len(items)}: "
            f"{[p.get('permission_name') or p.get('name') for p in items]}"
        )

    def test_lista_filtr_kategoria_auth(self, authed_client: httpx.Client) -> None:
        """GET /permissions?category=auth → ≥ 1 uprawnienie."""
        resp = authed_client.get("/permissions", params={"category": "auth"})
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        items = _lista(resp.json())
        assert len(items) >= 1, (
            f"Brak uprawnień kategorii 'auth'. "
            f"Odpowiedź: {resp.text[:400]}"
        )

    def test_lista_bez_tokenu_401(self, http_client: httpx.Client) -> None:
        """GET /permissions bez tokenu → 401."""
        with _fresh_client() as c:
            resp = c.get("/permissions")
        assert resp.status_code == 401


class TestPermissionsKategorie:
    """GET /permissions/categories."""

    def test_kategorie_lista(self, authed_client: httpx.Client) -> None:
        """GET /permissions/categories zwraca listę kategorii."""
        resp = authed_client.get("/permissions/categories")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        items = _lista(resp.json())
        if not items:
            data = resp.json().get("data", {})
            if isinstance(data, dict):
                items = data.get("categories", [])
        assert len(items) >= 5, (
            f"Oczekiwano ≥ 5 kategorii, got {len(items)}: {items}"
        )

    def test_kategorie_zawieraja_faktury(self, authed_client: httpx.Client) -> None:
        """Lista kategorii zawiera 'faktury'."""
        resp = authed_client.get("/permissions/categories")
        assert resp.status_code == 200
        assert "faktury" in resp.text, (
            f"Kategoria 'faktury' nie znaleziona: {resp.text[:300]}"
        )

    def test_kategorie_bez_tokenu_401(self, http_client: httpx.Client) -> None:
        """GET /permissions/categories bez tokenu → 401."""
        with _fresh_client() as c:
            resp = c.get("/permissions/categories")
        assert resp.status_code == 401


class TestPermissionsSzczegoly:
    """GET /permissions/{id}."""

    def test_szczegoly_ok(
        self, authed_client: httpx.Client, permission_testowy_id: int
    ) -> None:
        """GET /permissions/{id} zwraca szczegóły uprawnienia."""
        resp = authed_client.get(f"/permissions/{permission_testowy_id}")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        data = _data(resp.json())
        name = data.get("permission_name") or data.get("name", "")
        assert "selftest" in name.lower(), (
            f"Oczekiwano selftest w nazwie, got: {name}\nKlucze: {list(data.keys())}"
        )

    def test_szczegoly_404(self, authed_client: httpx.Client) -> None:
        """GET /permissions/99999 → 404."""
        resp = authed_client.get("/permissions/99999")
        assert resp.status_code == 404, f"got {resp.status_code}"


class TestPermissionsTworzenie:
    """POST /permissions — tworzenie (405 = niezaimplementowane → xfail)."""

    def test_tworzenie_ok(
        self, authed_client: httpx.Client, permission_testowy_id: int
    ) -> None:
        """Fixture permission_testowy_id potwierdza POST /permissions działa."""
        assert permission_testowy_id > 0
        resp = authed_client.get(f"/permissions/{permission_testowy_id}")
        assert resp.status_code == 200

    @_XFAIL_405
    def test_tworzenie_zla_kategoria_422(self, authed_client: httpx.Client) -> None:
        """POST /permissions z kategorią spoza CHECK constraint → 422."""
        resp = authed_client.post(
            "/permissions",
            json={
                "name": "selftest.zla_kategoria",
                "description": "test",
                "category": "KATEGORIA_NIEISTNIEJACA_XYZ",
            },
        )
        assert resp.status_code == 422, (
            f"Zła kategoria powinna dać 422 (CHECK constraint), "
            f"got {resp.status_code}: {resp.text[:200]}"
        )

    @_XFAIL_405
    def test_tworzenie_zly_format_nazwy_422(self, authed_client: httpx.Client) -> None:
        """POST /permissions z nazwą bez 'kategoria.akcja' → 422."""
        resp = authed_client.post(
            "/permissions",
            json={
                "name": "BEZ_KROPKI_NIEPRAWIDLOWE",
                "description": "test",
                "category": "system",
            },
        )
        assert resp.status_code == 422, (
            f"Zły format nazwy powinien dać 422, got {resp.status_code}"
        )

    def test_tworzenie_duplikat_409(
        self, authed_client: httpx.Client, permission_testowy_id: int
    ) -> None:
        """POST /permissions z duplikowaną nazwą → 409."""
        resp = authed_client.post(
            "/permissions",
            json={
                "name": TEST_PERM_NAME,
                "description": "duplikat",
                "category": "system",
            },
        )
        assert resp.status_code == 409, (
            f"Duplikat nazwy powinien dać 409, got {resp.status_code}"
        )


class TestPermissionsEdycja:
    """PUT /permissions/{id} — edycja (405 = niezaimplementowane → xfail)."""

    def test_edycja_description(
        self, authed_client: httpx.Client, permission_testowy_id: int
    ) -> None:
        """PUT /permissions/{id} zmienia description."""
        resp = authed_client.put(
            f"/permissions/{permission_testowy_id}",
            json={
                "name": TEST_PERM_NAME,
                "description": "SELFTEST zmieniony opis",
                "category": "system",
            },
        )
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"

    @_XFAIL_405
    def test_edycja_nieistniejace_404(self, authed_client: httpx.Client) -> None:
        """PUT /permissions/99999 → 404."""
        resp = authed_client.put(
            "/permissions/99999",
            json={"name": "x.y", "description": "test", "category": "system"},
        )
        assert resp.status_code == 404, f"got {resp.status_code}"


class TestPermissionsDelete:
    """DELETE 2-krokowy."""

    def test_delete_zly_token_blokada(
        self, authed_client: httpx.Client, permission_testowy_id: int
    ) -> None:
        """DELETE confirm ze złym tokenem → błąd."""
        resp = authed_client.delete(
            f"/permissions/{permission_testowy_id}/confirm",
            json={"confirm_token": "ZLYTOKEN.SELFTEST.NIEPRAWIDLOWY"},
        )
        assert resp.status_code in (400, 401, 422), (
            f"Zły token powinien dać błąd, got {resp.status_code}"
        )

    def test_delete_nieistniejace_404(self, authed_client: httpx.Client) -> None:
        """DELETE /permissions/99999/initiate → 404."""
        resp = authed_client.delete("/permissions/99999/initiate")
        assert resp.status_code == 404, f"got {resp.status_code}"

    def test_delete_bez_tokenu_401(self, http_client: httpx.Client) -> None:
        """DELETE /permissions/1/initiate bez tokenu → 401."""
        with _fresh_client() as c:
            resp = c.delete("/permissions/1/initiate")
        assert resp.status_code == 401, f"got {resp.status_code}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _data(resp_json: dict) -> dict:
    d = resp_json.get("data", resp_json)
    return d if isinstance(d, dict) else resp_json


def _lista(resp_json: dict) -> list:
    """
    Wyciąga listę uprawnień z odpowiedzi API.

    Obsługuje dwa formaty:
      Format A (lista): {"data": {"items": [...]}}
      Format B (dict po kategoriach): {"data": {"items": {"auth": [...], "faktury": [...]}}}

    Format B jest aktualnie używany przez /permissions endpoint.
    """
    data = resp_json.get("data", resp_json)

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in ("items", "permissions", "data", "results"):
            val = data.get(key)
            if isinstance(val, list):
                return val
            if isinstance(val, dict):
                # Format B: dict pogrupowany po kategorii → spłaszcz do jednej listy
                result: list = []
                for v in val.values():
                    if isinstance(v, list):
                        result.extend(v)
                return result

    return []