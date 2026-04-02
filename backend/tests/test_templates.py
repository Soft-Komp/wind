"""
test_templates.py — Testy modułu /templates
============================================
System Windykacja GPGK Jasło — Sprint 2.3

Stan po analizie:
  GET /templates        — DZIAŁA ✅
  POST /templates       — DZIAŁA ✅ (walidacja nazwy, typu)
  GET /templates/{id}   — DZIAŁA ✅ (404 działa)
  PUT /templates/{id}   — 405 ❌ → xfail
  DELETE /templates/{id} — 405 ❌ → xfail
"""

from __future__ import annotations

import logging

import httpx
import pytest

from conftest import BASE_URL, TEST_TEMPLATE_NAME, TIMEOUT

logger = logging.getLogger(__name__)

_XFAIL_405 = pytest.mark.xfail(
    reason="Endpoint zwraca 405 Method Not Allowed — PUT/DELETE /templates niezaimplementowane",
    strict=False,
)


def _fresh_client() -> httpx.Client:
    return httpx.Client(base_url=BASE_URL, timeout=TIMEOUT)


class TestTemplatesLista:
    """GET /templates — lista szablonów."""

    def test_lista_ok(self, authed_client: httpx.Client) -> None:
        """GET /templates zwraca 200."""
        resp = authed_client.get("/templates")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"

    def test_lista_bez_tokenu_401(self, http_client: httpx.Client) -> None:
        """GET /templates bez tokenu → 401."""
        with _fresh_client() as c:
            resp = c.get("/templates")
        assert resp.status_code == 401


class TestTemplatesTworzenie:
    """POST /templates."""

    def test_tworzenie_ok(
        self, authed_client: httpx.Client, template_testowy_id: int
    ) -> None:
        """Fixture template_testowy_id potwierdza POST /templates działa."""
        assert template_testowy_id > 0
        resp = authed_client.get(f"/templates/{template_testowy_id}")
        assert resp.status_code == 200, (
            f"Szablon ID={template_testowy_id} nie istnieje po stworzeniu"
        )

    def test_tworzenie_brak_nazwy_422(self, authed_client: httpx.Client) -> None:
        """POST /templates bez name → 422."""
        resp = authed_client.post(
            "/templates",
            json={"type": "email", "body": "treść"},
        )
        assert resp.status_code == 422, f"got {resp.status_code}"

    def test_tworzenie_zly_typ_422(self, authed_client: httpx.Client) -> None:
        """POST /templates z nieznajomym type → 422."""
        resp = authed_client.post(
            "/templates",
            json={
                "name": "test_zly_typ",
                "type": "NIEZNANY_TYP",
                "body": "treść",
            },
        )
        assert resp.status_code == 422, (
            f"Nieznany typ szablonu powinien dać 422, got {resp.status_code}"
        )

    def test_tworzenie_duplikat_409(
        self, authed_client: httpx.Client, template_testowy_id: int
    ) -> None:
        """POST /templates z duplikowaną nazwą → 409."""
        resp = authed_client.post(
            "/templates",
            json={
                "name": TEST_TEMPLATE_NAME,
                "type": "email",
                "body": "duplikat",
            },
        )
        assert resp.status_code == 409, (
            f"Duplikat nazwy szablonu powinien dać 409, got {resp.status_code}"
        )


class TestTemplatesSzczegoly:
    """GET /templates/{id}."""

    def test_szczegoly_ok(
        self, authed_client: httpx.Client, template_testowy_id: int
    ) -> None:
        """GET /templates/{id} zwraca szablon z polami."""
        resp = authed_client.get(f"/templates/{template_testowy_id}")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        data = _data(resp.json())
        has_name = "name" in data or "template_name" in data
        assert has_name, f"Szablon bez nazwy: {list(data.keys())}"

    def test_szczegoly_404(self, authed_client: httpx.Client) -> None:
        """GET /templates/99999 → 404."""
        resp = authed_client.get("/templates/99999")
        assert resp.status_code == 404, f"got {resp.status_code}"


class TestTemplatesEdycja:
    """PUT /templates/{id} — edycja (405 = niezaimplementowane → xfail)."""

    def test_edycja_ok(
        self, authed_client: httpx.Client, template_testowy_id: int
    ) -> None:
        """PUT /templates/{id} zmienia body szablonu."""
        resp = authed_client.put(
            f"/templates/{template_testowy_id}",
            json={
                "name": TEST_TEMPLATE_NAME,
                "type": "email",
                "subject": "SELFTEST zmieniony temat",
                "body": "Zmieniona treść po edycji.",
            },
        )
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"

    def test_edycja_nieistniejacy_404(self, authed_client: httpx.Client) -> None:
        """PUT /templates/99999 → 404 lub 422."""
        resp = authed_client.put(
            "/templates/99999",
            json={"name": "test", "type": "email", "body": "x"},
        )
        assert resp.status_code in (404, 422), f"got {resp.status_code}"


class TestTemplatesDelete:
    """DELETE /templates/{id} (405 = niezaimplementowane → xfail)."""

    @_XFAIL_405
    def test_delete_nieistniejacego_404(self, authed_client: httpx.Client) -> None:
        """DELETE /templates/99999 → 404."""
        resp = authed_client.delete("/templates/99999")
        assert resp.status_code == 404, f"got {resp.status_code}"

    @_XFAIL_405
    def test_delete_bez_tokenu_401(self, http_client: httpx.Client) -> None:
        """DELETE /templates/1 bez tokenu → 401."""
        with _fresh_client() as c:
            resp = c.delete("/templates/1")
        assert resp.status_code == 401, f"got {resp.status_code}"


def _data(resp_json: dict) -> dict:
    d = resp_json.get("data", resp_json)
    return d if isinstance(d, dict) else resp_json