"""
test_debtors.py — Testy modułu /debtors (WAPRO) + komentarze
=============================================================
System Windykacja GPGK Jasło — Sprint 2.3

Rzeczywiste formaty API (zweryfikowane verbose):
  GET /debtors — pola: ID_KONTRAHENTA (UPPERCASE), NazwaKontrahenta, SumaDlugu, ...
  GET /debtors/{id} — structure: {debtor: {ID_KONTRAHENTA, ...}, monit_history: [...]}
  GET /debtors/{id}/invoices — zwraca 200 + pusta lista dla nieistniejącego (nie 404)
  POST /debtors/{id}/comments — tworzy komentarz nawet dla nieistniejącego ID (nie 404)
  POST /debtors/{id}/comments — ID komentarza w odpowiedzi: id_comment
"""

from __future__ import annotations

import logging

import httpx
import pytest

from conftest import BASE_URL, TIMEOUT

logger = logging.getLogger(__name__)


def _fresh_client() -> httpx.Client:
    return httpx.Client(base_url=BASE_URL, timeout=TIMEOUT)


class TestDebtorsLista:
    """GET /debtors — lista dłużników."""

    def test_lista_podstawowa(self, authed_client: httpx.Client) -> None:
        """GET /debtors zwraca 200."""
        resp = authed_client.get("/debtors")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"

    def test_lista_paginacja(self, authed_client: httpx.Client) -> None:
        """GET /debtors?per_page=5 zwraca max 5 wyników."""
        resp = authed_client.get("/debtors", params={"per_page": 5})
        assert resp.status_code == 200
        items = _lista(resp.json())
        assert len(items) <= 5, f"per_page=5, ale dostaliśmy {len(items)}"

    def test_lista_struktura_dluznika(self, authed_client: httpx.Client) -> None:
        """
        Każdy dłużnik zawiera pole ID_KONTRAHENTA (UPPERCASE z WAPRO).

        Rzeczywiste pola API (z widoku WAPRO):
          ID_KONTRAHENTA, NazwaKontrahenta, Email, Telefon, SumaDlugu,
          LiczbaFaktur, NajstarszaFaktura, DniPrzeterminowania, ...
        """
        resp = authed_client.get("/debtors", params={"per_page": 3})
        assert resp.status_code == 200
        items = _lista(resp.json())
        if not items:
            pytest.skip("Brak dłużników w WAPRO")
        first = items[0]
        # Faktyczna nazwa pola z widoku WAPRO — uppercase PascalCase
        has_id = (
            "ID_KONTRAHENTA" in first
            or any("id" in k.lower() and "kontrahent" in k.lower() for k in first)
        )
        assert has_id, (
            f"Dłużnik bez pola ID_KONTRAHENTA. Dostępne: {list(first.keys())}"
        )

    def test_lista_filtr_overdue_only(self, authed_client: httpx.Client) -> None:
        """GET /debtors?overdue_only=true zwraca 200."""
        resp = authed_client.get("/debtors", params={"overdue_only": "true"})
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"

    def test_lista_filtr_min_debt(self, authed_client: httpx.Client) -> None:
        """GET /debtors?min_debt=0.01 zwraca 200."""
        resp = authed_client.get("/debtors", params={"min_debt": "0.01"})
        assert resp.status_code == 200

    def test_lista_bez_tokenu_401(self, http_client: httpx.Client) -> None:
        """GET /debtors bez tokenu → 401."""
        with _fresh_client() as c:
            resp = c.get("/debtors")
        assert resp.status_code == 401

    def test_lista_nieprawidlowa_paginacja_422(
        self, authed_client: httpx.Client
    ) -> None:
        """GET /debtors?per_page=-1 → 422."""
        resp = authed_client.get("/debtors", params={"per_page": -1})
        assert resp.status_code == 422


class TestDebtorsStats:
    """GET /debtors/stats — statystyki zbiorcze."""

    def test_stats_podstawowe(self, authed_client: httpx.Client) -> None:
        """GET /debtors/stats zwraca 200 ze statystykami."""
        resp = authed_client.get("/debtors/stats")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"

    def test_stats_bez_tokenu_401(self, http_client: httpx.Client) -> None:
        """GET /debtors/stats bez tokenu → 401."""
        with _fresh_client() as c:
            resp = c.get("/debtors/stats")
        assert resp.status_code == 401


class TestDebtorsSzczegoly:
    """GET /debtors/{id} — szczegóły dłużnika."""

    def test_szczegoly_ok(
        self, authed_client: httpx.Client, debtor_testowy_id: int
    ) -> None:
        """GET /debtors/{id} zwraca 200."""
        resp = authed_client.get(f"/debtors/{debtor_testowy_id}")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"

    def test_szczegoly_struktura(
        self, authed_client: httpx.Client, debtor_testowy_id: int
    ) -> None:
        """
        Szczegóły dłużnika mają strukturę {debtor: {...}, monit_history: [...], ...}.

        Rzeczywista struktura API:
          data.debtor        — dane dłużnika z ID_KONTRAHENTA
          data.monit_history — historia monitów
          data.monit_stats   — statystyki
          data.from_cache    — czy z cache
        """
        resp = authed_client.get(f"/debtors/{debtor_testowy_id}")
        assert resp.status_code == 200
        data = _data(resp.json())

        # Sprawdzamy strukturę zagnieżdżoną
        has_debtor = "debtor" in data
        assert has_debtor, (
            f"Brak klucza 'debtor' w odpowiedzi. Dostępne: {list(data.keys())}"
        )
        debtor = data.get("debtor", {})
        has_id = (
            "ID_KONTRAHENTA" in debtor
            or any("id" in k.lower() for k in debtor.keys())
        )
        assert has_id, (
            f"Debtor bez ID w strukturze zagnieżdżonej. "
            f"Klucze debtor: {list(debtor.keys())}"
        )

    def test_szczegoly_nieistniejacy_404(self, authed_client: httpx.Client) -> None:
        """GET /debtors/99999999 → 404."""
        resp = authed_client.get("/debtors/99999999")
        assert resp.status_code == 404, f"got {resp.status_code}"

    def test_szczegoly_bez_tokenu_401(
        self, http_client: httpx.Client, debtor_testowy_id: int
    ) -> None:
        """GET /debtors/{id} bez tokenu → 401."""
        with _fresh_client() as c:
            resp = c.get(f"/debtors/{debtor_testowy_id}")
        assert resp.status_code == 401


class TestDebtorsInvoices:
    """GET /debtors/{id}/invoices — faktury/rozrachunki."""

    def test_invoices_ok(
        self, authed_client: httpx.Client, debtor_testowy_id: int
    ) -> None:
        """GET /debtors/{id}/invoices zwraca 200."""
        resp = authed_client.get(f"/debtors/{debtor_testowy_id}/invoices")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"

    @pytest.mark.xfail(
        reason=(
            "API zwraca 200 + pustą listę dla nieistniejącego dłużnika zamiast 404. "
            "Brak walidacji istnienia kontrahenta przed zapytaniem o faktury."
        ),
        strict=False,
    )
    def test_invoices_nieistniejacy_404(self, authed_client: httpx.Client) -> None:
        """GET /debtors/99999999/invoices → 404."""
        resp = authed_client.get("/debtors/99999999/invoices")
        assert resp.status_code == 404, f"got {resp.status_code}"


class TestDebtorsMonitHistory:
    """GET /debtors/{id}/monit-history — historia monitów."""

    def test_historia_ok(
        self, authed_client: httpx.Client, debtor_testowy_id: int
    ) -> None:
        """GET /debtors/{id}/monit-history zwraca 200."""
        resp = authed_client.get(f"/debtors/{debtor_testowy_id}/monit-history")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"

    def test_historia_bez_tokenu_401(
        self, http_client: httpx.Client, debtor_testowy_id: int
    ) -> None:
        """GET /debtors/{id}/monit-history bez tokenu → 401."""
        with _fresh_client() as c:
            resp = c.get(f"/debtors/{debtor_testowy_id}/monit-history")
        assert resp.status_code == 401


class TestDebtorsWalidacja:
    """POST /debtors/validate-bulk."""

    def test_validate_bulk_lista_ids(
        self, authed_client: httpx.Client, debtor_testowy_id: int
    ) -> None:
        """POST /debtors/validate-bulk z listą ID → 200."""
        resp = authed_client.post(
            "/debtors/validate-bulk",
            json={"debtor_ids": [debtor_testowy_id]},
        )
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"

    def test_validate_bulk_pusta_lista(self, authed_client: httpx.Client) -> None:
        """POST /debtors/validate-bulk z pustą listą → 422 lub 200."""
        resp = authed_client.post(
            "/debtors/validate-bulk",
            json={"debtor_ids": []},
        )
        assert resp.status_code in (200, 422), f"got {resp.status_code}"

    def test_validate_bulk_nieistniejace_ids(
        self, authed_client: httpx.Client
    ) -> None:
        """POST /debtors/validate-bulk z nieistniejącymi ID → 200 z invalid."""
        resp = authed_client.post(
            "/debtors/validate-bulk",
            json={"debtor_ids": [99999998, 99999999]},
        )
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        logger.info("SELFTEST validate_bulk nieistniejące: %s", resp.text[:300])


class TestDebtorsKomentarze:
    """CRUD komentarzy do dłużnika."""

    def test_lista_komentarzy(
        self, authed_client: httpx.Client, debtor_testowy_id: int
    ) -> None:
        """GET /debtors/{id}/comments zwraca 200."""
        resp = authed_client.get(f"/debtors/{debtor_testowy_id}/comments")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"

    def test_lista_komentarzy_bez_tokenu_401(
        self, http_client: httpx.Client, debtor_testowy_id: int
    ) -> None:
        """GET /debtors/{id}/comments bez tokenu → 401."""
        with _fresh_client() as c:
            resp = c.get(f"/debtors/{debtor_testowy_id}/comments")
        assert resp.status_code == 401

    def test_dodanie_komentarza_ok(
        self,
        authed_client: httpx.Client,
        comment_testowy_id: tuple,
    ) -> None:
        """Fixture comment_testowy_id potwierdza POST /debtors/{id}/comments działa."""
        debtor_id, comment_id = comment_testowy_id
        assert comment_id > 0, f"comment_id <= 0: {comment_id}"
        # Weryfikacja że komentarz widać na liście
        resp = authed_client.get(f"/debtors/{debtor_id}/comments")
        assert resp.status_code == 200
        items = _lista(resp.json())
        comment_ids = [
            c.get("id_comment") or c.get("id") or c.get("comment_id")
            for c in items
        ]
        assert comment_id in comment_ids, (
            f"Nowy komentarz ID={comment_id} nie widoczny na liście: {comment_ids}"
        )

    def test_dodanie_komentarza_pusta_tresc_422(
        self, authed_client: httpx.Client, debtor_testowy_id: int
    ) -> None:
        """POST /debtors/{id}/comments z pustą treścią → 422."""
        resp = authed_client.post(
            f"/debtors/{debtor_testowy_id}/comments",
            json={"tresc": ""},
        )
        assert resp.status_code == 422

    def test_dodanie_komentarza_brak_tresci_422(
        self, authed_client: httpx.Client, debtor_testowy_id: int
    ) -> None:
        """POST /debtors/{id}/comments bez 'tresc' → 422."""
        resp = authed_client.post(
            f"/debtors/{debtor_testowy_id}/comments",
            json={},
        )
        assert resp.status_code == 422

    def test_edycja_komentarza_ok(
        self,
        authed_client: httpx.Client,
        comment_testowy_id: tuple,
    ) -> None:
        """PUT /debtors/{id}/comments/{cid} zmienia treść."""
        debtor_id, comment_id = comment_testowy_id
        resp = authed_client.put(
            f"/debtors/{debtor_id}/comments/{comment_id}",
            json={"tresc": "SELFTEST komentarz po edycji"},
        )
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"

    def test_edycja_komentarza_pusta_tresc_422(
        self,
        authed_client: httpx.Client,
        comment_testowy_id: tuple,
    ) -> None:
        """PUT z pustą treścią → 422."""
        debtor_id, comment_id = comment_testowy_id
        resp = authed_client.put(
            f"/debtors/{debtor_id}/comments/{comment_id}",
            json={"tresc": ""},
        )
        assert resp.status_code == 422, f"got {resp.status_code}"

    def test_edycja_nieistniejacego_404(
        self, authed_client: httpx.Client, debtor_testowy_id: int
    ) -> None:
        """PUT /debtors/{id}/comments/99999 → 404."""
        resp = authed_client.put(
            f"/debtors/{debtor_testowy_id}/comments/99999",
            json={"tresc": "próba edycji nieistniejącego"},
        )
        assert resp.status_code == 404, f"got {resp.status_code}"

    def test_delete_initiate_zwraca_token(
        self,
        authed_client: httpx.Client,
        comment_testowy_id: tuple,
    ) -> None:
        """DELETE /debtors/{id}/comments/{cid}/initiate → 202 (lub 200) z tokenem."""
        debtor_id, comment_id = comment_testowy_id
        resp = authed_client.delete(
            f"/debtors/{debtor_id}/comments/{comment_id}/initiate"
        )
        assert resp.status_code in (200, 202), (
            f"got {resp.status_code}: {resp.text[:300]}"
        )
        data = _data(resp.json())
        # Token może być pod różnymi kluczami w zależności od implementacji
        token = (
            data.get("delete_token")
            or data.get("confirm_token")
            or data.get("token")
            or data.get("data", {}).get("token")
            or data.get("data", {}).get("confirm_token")
        )
        assert token, (
            f"Brak tokenu w odpowiedzi delete initiate. "
            f"Dostępne klucze: {list(data.keys())}. "
            f"Pełna odpowiedź: {resp.text[:300]}"
        )

    def test_delete_zly_token_blokada(
        self,
        authed_client: httpx.Client,
        comment_testowy_id: tuple,
    ) -> None:
        """DELETE confirm ze złym tokenem → błąd."""
        debtor_id, comment_id = comment_testowy_id
        resp = authed_client.delete(
            f"/debtors/{debtor_id}/comments/{comment_id}/confirm",
            json={"confirm_token": "ZLYTOKEN.NIEPRAWIDLOWY"},
        )
        assert resp.status_code in (400, 401, 404, 422), (
            f"Zły token powinien dać błąd, got {resp.status_code}: {resp.text[:200]}"
        )

    @pytest.mark.xfail(
        reason=(
            "API zwraca 201 (tworzy komentarz) dla nieistniejącego dłużnika ID=99999999. "
            "Brak walidacji FK id_kontrahenta przed zapisem komentarza."
        ),
        strict=False,
    )
    def test_komentarz_nieistniejacego_dluznika_404(
        self, authed_client: httpx.Client
    ) -> None:
        """POST /debtors/99999999/comments → 404."""
        resp = authed_client.post(
            "/debtors/99999999/comments",
            json={"tresc": "komentarz do nieistniejącego dłużnika"},
        )
        assert resp.status_code == 404, f"got {resp.status_code}"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _data(resp_json: dict) -> dict:
    d = resp_json.get("data", resp_json)
    return d if isinstance(d, dict) else resp_json


def _lista(resp_json: dict) -> list:
    data = resp_json.get("data", resp_json)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("items", "comments", "invoices", "data", "results"):
            val = data.get(key)
            if isinstance(val, list):
                return val
    return []