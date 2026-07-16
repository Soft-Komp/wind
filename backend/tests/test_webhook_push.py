# =============================================================================
# PLIK  : backend/tests/test_webhook_push.py
# MODUL : Testy integracyjne — webhook push (Tier 1a/1b/1c) + walidacja
#         connection_config pull/push (Tier 2)
# OPIS  : Uzywa fixture push_source (conftest.py) — zrodlo source_type='api'
#         connection_mode='push', tworzone/dezaktywowane per sesja testowa.
#         Webhook jest PUBLICZNY (brak JWT) — testy webhooka uzywaja
#         http_client (bez tokenu), nie authed_client.
# =============================================================================
from __future__ import annotations

import concurrent.futures
import logging
import uuid

import httpx
import pytest

logger = logging.getLogger("windykacja.tests.webhook_push")


def _unique_id_document() -> str:
    """Unikalny id_document per test — zero kolizji miedzy testami/przebiegami."""
    return f"SELFTEST-PUSH-{uuid.uuid4().hex[:16]}"


def _cleanup_source_by_name(authed_client: httpx.Client, name: str) -> None:
    """Dezaktywuje zrodlo o danej nazwie, jesli istnieje (sprzatanie testow negatywnych)."""
    try:
        lista = authed_client.get("/admin/sources", params={"source_type": "api"})
        if lista.status_code != 200:
            return
        items = lista.json().get("data", {}).get("items", [])
        for s in items:
            if s.get("source_name") == name:
                authed_client.put(f"/admin/sources/{s['id_source']}", json={"is_active": False})
    except Exception as exc:
        logger.warning("[test_webhook_push] _cleanup_source_by_name(%s) error: %s", name, exc)


# =============================================================================
# Tier 1a — Idempotencja
# =============================================================================

@pytest.mark.webhook
@pytest.mark.db
class TestWebhookIdempotency:

    def test_nowy_dokument_zwraca_202(self, http_client: httpx.Client, push_source: dict):
        """Pierwsze wyslanie nowego id_document musi dac 202, idempotent_hit=False."""
        id_doc = _unique_id_document()
        resp = http_client.post(
            f"/webhooks/sources/{push_source['webhook_token']}",
            json={"id_document": id_doc, "doc_number": "TEST/1"},
        )
        assert resp.status_code == 202, f"{resp.status_code}: {resp.text[:300]}"

        body = resp.json()
        assert body["code"] == 202, (
            "Tresc JSON 'code' musi byc spojna z realnym HTTP status (naprawiona "
            "niespojnosc — patrz webhooks.py)"
        )
        data = body["data"]
        assert data["idempotent_hit"] is False
        assert isinstance(data["id_instance"], int)
        assert data["status"] in ("pending_dispatch", "duplicate_pending")
        push_source["created_instances"].append(data["id_instance"])

    def test_retry_innym_payloadem_zwraca_200_i_ten_sam_id_instance(
        self, http_client: httpx.Client, push_source: dict,
    ):
        """
        Retry tego samego id_document z INNYM payloadem (zmieniona kwota,
        zmieniony numer) musi: zwrocic 200, idempotent_hit=True, DOKLADNIE
        ten sam id_instance co pierwsze wywolanie, i CALKOWICIE zignorowac
        nowe dane (czysta idempotencja — decyzja zatwierdzona bez zmian).
        """
        id_doc = _unique_id_document()
        token = push_source["webhook_token"]

        first = http_client.post(
            f"/webhooks/sources/{token}",
            json={"id_document": id_doc, "doc_number": "ORYGINALNY", "amount_gross": 100.00},
        )
        assert first.status_code == 202, f"{first.status_code}: {first.text[:300]}"
        first_id_instance = first.json()["data"]["id_instance"]
        push_source["created_instances"].append(first_id_instance)

        second = http_client.post(
            f"/webhooks/sources/{token}",
            json={"id_document": id_doc, "doc_number": "ZMIENIONY", "amount_gross": 999999.99},
        )
        assert second.status_code == 200, f"{second.status_code}: {second.text[:300]}"

        body = second.json()
        assert body["code"] == 200
        data = body["data"]
        assert data["idempotent_hit"] is True
        assert data["id_instance"] == first_id_instance, (
            "Retry musi zwrocic TEN SAM id_instance — nie tworzyc nowego dokumentu "
            "ani aktualizowac istniejacego nowym payloadem."
        )

    def test_trzykrotny_retry_wciaz_ten_sam_id_instance(
        self, http_client: httpx.Client, push_source: dict,
    ):
        """Idempotencja musi dzialac powtarzalnie, nie tylko przy drugim wywolaniu."""
        id_doc = _unique_id_document()
        token = push_source["webhook_token"]
        payload = {"id_document": id_doc}

        first = http_client.post(f"/webhooks/sources/{token}", json=payload)
        assert first.status_code == 202
        id_instance = first.json()["data"]["id_instance"]
        push_source["created_instances"].append(id_instance)

        for i in range(3):
            retry = http_client.post(f"/webhooks/sources/{token}", json=payload)
            assert retry.status_code == 200, f"retry #{i}: {retry.status_code} {retry.text[:200]}"
            assert retry.json()["data"]["id_instance"] == id_instance


# =============================================================================
# Tier 1a — Race condition
# =============================================================================

@pytest.mark.webhook
@pytest.mark.db
@pytest.mark.slow
class TestWebhookRaceCondition:

    def test_rownolegle_zadania_ten_sam_dokument_bez_500(
        self, push_source: dict, base_url: str, http_timeout: float,
    ):
        """
        Wysyla N rownoleglych zadan z tym samym id_document. Oczekiwane:
        zero HTTP 500 (naprawiony IntegrityError -> rollback -> recovery),
        wszystkie odpowiedzi to 200 lub 202, i WSZYSTKIE wskazuja na ten
        sam id_instance (dokladnie jedna operacja tworzenia wygrala wyscig,
        reszta trafila w idempotentna sciezke — bezposrednio przez
        wczesniejszy SELECT albo przez recovery po IntegrityError).
        """
        id_doc = _unique_id_document()
        token = push_source["webhook_token"]
        payload = {"id_document": id_doc, "doc_number": "RACE/1"}
        url = f"{base_url}/webhooks/sources/{token}"

        def _send() -> httpx.Response:
            with httpx.Client(timeout=http_timeout) as c:
                return c.post(url, json=payload)

        n_requests = 8
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_requests) as executor:
            futures = [executor.submit(_send) for _ in range(n_requests)]
            responses = [f.result() for f in futures]

        statuses = [r.status_code for r in responses]
        assert 500 not in statuses, (
            f"Otrzymano HTTP 500 przy rownoleglych zadaniach (race condition "
            f"NIE zostal poprawnie obsluzony): {statuses}"
        )
        assert all(s in (200, 202) for s in statuses), f"Nieoczekiwane statusy: {statuses}"
        assert statuses.count(202) >= 1, (
            "Co najmniej jedno z rownoleglych zadan musialo faktycznie "
            "utworzyc dokument (202) — jesli wszystkie dostaly 200, oznacza "
            "to ze idempotentny SELECT sam w sobie tworzy fantomowe trafienia."
        )

        id_instances = {r.json()["data"]["id_instance"] for r in responses}
        assert len(id_instances) == 1, (
            f"Wszystkie rownolegle zadania musza wskazywac na TEN SAM "
            f"id_instance — race condition stworzyl duplikaty: {id_instances}"
        )
        push_source["created_instances"].extend(id_instances)


# =============================================================================
# Tier 1b/1c — Pozycje dokumentu (items)
# =============================================================================

@pytest.mark.webhook
@pytest.mark.db
class TestWebhookPushItems:

    def test_brak_items_line_items_zwraca_200_i_pusta_liste(
        self, http_client: httpx.Client, authed_client: httpx.Client, push_source: dict,
    ):
        """
        Rozstrzygniecia Koncowe #13: brak pozycji dla push = 200 + [],
        NIE 409 not_configured (brak pozycji to cecha dokumentu, nie brak
        konfiguracji zrodla).
        """
        id_doc = _unique_id_document()
        resp = http_client.post(
            f"/webhooks/sources/{push_source['webhook_token']}",
            json={"id_document": id_doc},
        )
        assert resp.status_code == 202, f"{resp.status_code}: {resp.text[:300]}"
        id_instance = resp.json()["data"]["id_instance"]
        push_source["created_instances"].append(id_instance)

        items_resp = authed_client.get(f"/documents/{id_instance}/line-items")
        assert items_resp.status_code == 200, f"{items_resp.status_code}: {items_resp.text[:300]}"
        data = items_resp.json()["data"]
        assert data["format"] == "structured"
        assert data["items"] == []

    def test_z_items_line_items_zwraca_te_same_dynamiczne_pozycje(
        self, http_client: httpx.Client, authed_client: httpx.Client, push_source: dict,
    ):
        """
        Pozycje wracaja SUROWO, bez mapowania — dowolne, dynamiczne pola
        per integracja (Rozstrzygniecia Koncowe #6).
        """
        id_doc = _unique_id_document()
        items_payload = [
            {"nazwa": "Pozycja pierwsza", "ilosc": 2, "cena_netto": 10.50},
            {"nazwa": "Pozycja druga", "dowolne_pole_integracji": True, "cos_innego": [1, 2, 3]},
        ]
        resp = http_client.post(
            f"/webhooks/sources/{push_source['webhook_token']}",
            json={"id_document": id_doc, "items": items_payload},
        )
        assert resp.status_code == 202, f"{resp.status_code}: {resp.text[:300]}"
        id_instance = resp.json()["data"]["id_instance"]
        push_source["created_instances"].append(id_instance)

        items_resp = authed_client.get(f"/documents/{id_instance}/line-items")
        assert items_resp.status_code == 200
        data = items_resp.json()["data"]
        assert data["format"] == "structured"
        assert data["items"] == items_payload, (
            f"Pozycje musza wrocic dokladnie w tej samej postaci "
            f"(zero whitelisty pol): oczekiwano {items_payload}, "
            f"otrzymano {data['items']}"
        )

    def test_items_nie_tablica_zwraca_422(self, http_client: httpx.Client, push_source: dict):
        id_doc = _unique_id_document()
        resp = http_client.post(
            f"/webhooks/sources/{push_source['webhook_token']}",
            json={"id_document": id_doc, "items": "to nie jest tablica"},
        )
        assert resp.status_code == 422, f"{resp.status_code}: {resp.text[:300]}"

    def test_items_element_nie_jest_obiektem_zwraca_422(
        self, http_client: httpx.Client, push_source: dict,
    ):
        id_doc = _unique_id_document()
        resp = http_client.post(
            f"/webhooks/sources/{push_source['webhook_token']}",
            json={"id_document": id_doc, "items": [{"ok": 1}, "zly_element_string", 123]},
        )
        assert resp.status_code == 422, f"{resp.status_code}: {resp.text[:300]}"

    def test_items_przekracza_limit_zwraca_422(
        self, http_client: httpx.Client, push_source: dict, webhook_max_items_limit: int,
    ):
        limit = webhook_max_items_limit
        id_doc = _unique_id_document()
        too_many_items = [{"i": i} for i in range(limit + 1)]

        resp = http_client.post(
            f"/webhooks/sources/{push_source['webhook_token']}",
            json={"id_document": id_doc, "items": too_many_items},
        )
        assert resp.status_code == 422, (
            f"Oczekiwano 422 dla {limit + 1} pozycji (limit={limit}): "
            f"{resp.status_code} — {resp.text[:300]}"
        )

    def test_items_dokladnie_na_limicie_jest_akceptowane(
        self, http_client: httpx.Client, push_source: dict, webhook_max_items_limit: int,
    ):
        """Wartosc graniczna: dokladnie limit pozycji musi przejsc (nie limit-1 czy limit+1)."""
        limit = webhook_max_items_limit
        id_doc = _unique_id_document()
        exactly_limit_items = [{"i": i} for i in range(limit)]

        resp = http_client.post(
            f"/webhooks/sources/{push_source['webhook_token']}",
            json={"id_document": id_doc, "items": exactly_limit_items},
        )
        assert resp.status_code == 202, (
            f"Dokladnie {limit} pozycji (na granicy limitu) powinno przejsc: "
            f"{resp.status_code} — {resp.text[:300]}"
        )
        push_source["created_instances"].append(resp.json()["data"]["id_instance"])


# =============================================================================
# Tier 2 — Walidacja connection_config zalezna od connection_mode
# =============================================================================

@pytest.mark.webhook
@pytest.mark.db
class TestSourcePushValidation:

    def test_utworzenie_zrodla_push_z_polem_pull_zwraca_422(self, authed_client: httpx.Client):
        source_name = "selftest_push_bad_9999"
        try:
            resp = authed_client.post(
                "/admin/sources",
                json={
                    "source_name":     source_name,
                    "source_type":     "api",
                    "connection_mode": "push",
                    "connection_config": {"base_url": "https://przykladowy-zewnetrzny-system.pl"},
                },
            )
            assert resp.status_code == 422, f"{resp.status_code}: {resp.text[:300]}"
        finally:
            _cleanup_source_by_name(authed_client, source_name)

    def test_edycja_istniejacego_zrodla_push_dodanie_pola_pull_zwraca_422(
        self, authed_client: httpx.Client, push_source: dict,
    ):
        resp = authed_client.put(
            f"/admin/sources/{push_source['id_source']}",
            json={"connection_config": {"endpoint_list": "/faktury"}},
        )
        assert resp.status_code == 422, f"{resp.status_code}: {resp.text[:300]}"

    @pytest.mark.parametrize(
        "pull_field",
        ["base_url", "endpoint_list", "endpoint_detail", "endpoint_line_items"],
    )
    def test_kazde_pole_pull_osobno_odrzucane_dla_push(
        self, authed_client: httpx.Client, push_source: dict, pull_field: str,
    ):
        resp = authed_client.put(
            f"/admin/sources/{push_source['id_source']}",
            json={"connection_config": {pull_field: "dowolna_wartosc"}},
        )
        assert resp.status_code == 422, (
            f"Pole '{pull_field}' powinno byc odrzucone dla push: "
            f"{resp.status_code} — {resp.text[:300]}"
        )

    def test_test_connection_dla_zrodla_push_zwraca_409(
        self, authed_client: httpx.Client, push_source: dict,
    ):
        resp = authed_client.post(f"/admin/sources/{push_source['id_source']}/test-connection")
        assert resp.status_code == 409, f"{resp.status_code}: {resp.text[:300]}"
        assert "test_connection_not_applicable" in resp.text, (
            f"Oczekiwano kodu biznesowego 'source.test_connection_not_applicable' "
            f"w tresci odpowiedzi: {resp.text[:300]}"
        )

    def test_sync_interval_minutes_dla_zrodla_push_jest_null_w_api(
        self, authed_client: httpx.Client, push_source: dict,
    ):
        """
        Rozstrzygniecia Koncowe #1: kolumna w bazie NIE jest ruszana
        (NOT NULL, zero migracji) — maskowanie WYLACZNIE w odpowiedzi API.
        """
        resp = authed_client.get(f"/admin/sources/{push_source['id_source']}")
        assert resp.status_code == 200, f"{resp.status_code}: {resp.text[:300]}"
        data = resp.json()["data"]
        assert data["sync_interval_minutes"] is None, (
            f"Dla connection_mode='push' API musi zwracac null dla "
            f"sync_interval_minutes, otrzymano: {data['sync_interval_minutes']!r}"
        )