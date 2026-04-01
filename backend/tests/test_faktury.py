# =============================================================================
# PLIK  : backend/tests/test_faktury.py
# MODUŁ : Self-test — moduł akceptacji faktur
# =============================================================================
from __future__ import annotations

import time

import httpx
import pytest


class TestFakturyLista:
    """Testy endpointów listy i szczegółów faktury."""

    def test_lista_faktury_ok(self, authed_client: httpx.Client) -> None:
        """GET /faktury-akceptacja zwraca 200 i strukturę paginacji."""
        resp = authed_client.get("/faktury-akceptacja")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:200]}"
        data = resp.json()
        assert "data" in data, "Brak klucza 'data' w odpowiedzi"
        assert "total" in data, "Brak klucza 'total' w odpowiedzi"
        assert "page" in data, "Brak klucza 'page' w odpowiedzi"
        assert isinstance(data["data"], list), "'data' nie jest listą"

    def test_lista_faktury_paginacja(self, authed_client: httpx.Client) -> None:
        """Parametry paginacji działają poprawnie."""
        resp = authed_client.get("/faktury-akceptacja", params={"page": 1, "per_page": 5})
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:200]}"
        data = resp.json()
        assert len(data["data"]) <= 5, "Paginacja nie działa — za dużo wyników"

    def test_lista_faktury_bez_uprawnien(self, http_client: httpx.Client) -> None:
        """Lista faktur bez tokenu zwraca 401."""
        with httpx.Client(base_url=http_client.base_url, timeout=30) as c:
            resp = c.get("/faktury-akceptacja")
        assert resp.status_code == 401, f"got {resp.status_code}"

    def test_szczegoly_faktury_ok(
        self, authed_client: httpx.Client, faktura_testowa_id: int
    ) -> None:
        """GET /faktury-akceptacja/{id} zwraca pełne dane faktury."""
        resp = authed_client.get(f"/faktury-akceptacja/{faktura_testowa_id}")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        data = resp.json()
        assert data.get("id") == faktura_testowa_id
        assert "numer_ksef" in data
        assert "status_wewnetrzny" in data
        assert "przypisania" in data
        assert "pozycje" in data
        assert "utworzony_przez" in data

    def test_szczegoly_faktury_nieistniejaca(self, authed_client: httpx.Client) -> None:
        """GET /faktury-akceptacja/99999 zwraca 404."""
        resp = authed_client.get("/faktury-akceptacja/99999")
        assert resp.status_code == 404, f"got {resp.status_code}"

    def test_historia_faktury(
        self, authed_client: httpx.Client, faktura_testowa_id: int
    ) -> None:
        """GET /faktury-akceptacja/{id}/historia zwraca listę zdarzeń."""
        resp = authed_client.get(f"/faktury-akceptacja/{faktura_testowa_id}/historia")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:200]}"
        data = resp.json()
        assert "items" in data, "Brak 'items' w odpowiedzi historii"
        assert data["total"] >= 1, "Historia pusta — powinna mieć min. 1 zdarzenie (przypisano)"
        akcje = [item.get("akcja") for item in data["items"]]
        assert "przypisano" in akcje, (
            f"Brak zdarzenia 'przypisano' w historii. Znalezione akcje: {akcje}"
        )


class TestFakturyEdycja:
    """Testy edycji faktury."""

    def test_patch_priorytet(
        self, authed_client: httpx.Client, faktura_testowa_id: int
    ) -> None:
        """PATCH /faktury-akceptacja/{id} zmienia priorytet i uwagi."""
        resp = authed_client.patch(
            f"/faktury-akceptacja/{faktura_testowa_id}",
            json={"priorytet": "pilny", "uwagi": "SELFTEST patch test"},
        )
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:200]}"
        data = resp.json()
        assert "changes" in data or "priorytet" in str(data)

    def test_patch_nieprawidlowy_priorytet(
        self, authed_client: httpx.Client, faktura_testowa_id: int
    ) -> None:
        """PATCH z nieprawidłowym priorytetem zwraca 422."""
        resp = authed_client.patch(
            f"/faktury-akceptacja/{faktura_testowa_id}",
            json={"priorytet": "NIEPRAWIDLOWY_PRIORYTET_XYZ"},
        )
        assert resp.status_code == 422, f"got {resp.status_code}"

    def test_patch_puste_body(
        self, authed_client: httpx.Client, faktura_testowa_id: int
    ) -> None:
        """PATCH z pustym body zwraca 422."""
        resp = authed_client.patch(
            f"/faktury-akceptacja/{faktura_testowa_id}",
            json={},
        )
        assert resp.status_code == 422, f"got {resp.status_code}"


class TestFakturyPDF:
    """Testy generowania PDF."""

    def test_pdf_referent(
        self, authed_client: httpx.Client, faktura_testowa_id: int
    ) -> None:
        """GET /faktury-akceptacja/{id}/pdf zwraca PDF (Content-Type: application/pdf)."""
        resp = authed_client.get(f"/faktury-akceptacja/{faktura_testowa_id}/pdf")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:200]}"
        ct = resp.headers.get("content-type", "")
        assert "application/pdf" in ct, f"Zły Content-Type: {ct}"
        assert len(resp.content) > 1000, "PDF za mały — prawdopodobnie pusty"
        # Sprawdź magic bytes PDF
        assert resp.content[:4] == b"%PDF", "Odpowiedź nie jest plikiem PDF"

    def test_pdf_moje_faktury(
        self, authed_client: httpx.Client, faktura_testowa_id: int
    ) -> None:
        """GET /moje-faktury/{id}/pdf zwraca PDF."""
        resp = authed_client.get(f"/moje-faktury/{faktura_testowa_id}/pdf")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:200]}"
        assert resp.content[:4] == b"%PDF", "Odpowiedź nie jest plikiem PDF"


class TestFakturyReset:
    """Testy resetu przypisań (2-krokowy)."""

    def test_reset_krok1_i_krok2(
        self, authed_client: httpx.Client, faktura_testowa_id: int
    ) -> None:
        """Reset przypisań: krok 1 generuje token, krok 2 wykonuje reset."""
        # Krok 1
        r1 = authed_client.post(
            f"/faktury-akceptacja/{faktura_testowa_id}/reset",
            json={
                "powod": "SELFTEST reset",
                "nowe_user_ids": [1],
            },
        )
        assert r1.status_code in (200, 202), f"Reset krok 1: got {r1.status_code}: {r1.text[:200]}"
        data1 = r1.json()
        assert "confirm_token" in data1, "Brak confirm_token w odpowiedzi krok 1"
        assert data1.get("expires_in", 0) > 0, "expires_in powinno być > 0"

        token = data1["confirm_token"]

        # Krok 2 — wykonaj w ciągu 60 sekund
        r2 = authed_client.post(
            f"/faktury-akceptacja/{faktura_testowa_id}/reset/confirm",
            json={"confirm_token": token},
        )
        assert r2.status_code == 200, f"Reset krok 2: got {r2.status_code}: {r2.text[:200]}"
        data2 = r2.json()
        assert "dezaktywowane" in data2, "Brak 'dezaktywowane' w odpowiedzi krok 2"
        assert "nowe_przypisania" in data2, "Brak 'nowe_przypisania' w odpowiedzi krok 2"

    def test_reset_zly_token(
        self, authed_client: httpx.Client, faktura_testowa_id: int
    ) -> None:
        """Confirm reset z nieprawidłowym tokenem zwraca 400."""
        resp = authed_client.post(
            f"/faktury-akceptacja/{faktura_testowa_id}/reset/confirm",
            json={"confirm_token": "ZLYTOKEN.SELFTEST.XYZ"},
        )
        assert resp.status_code in (400, 401, 422), (
            f"Oczekiwano błędu dla złego tokenu, got {resp.status_code}"
        )

    def test_reset_brak_user_ids(
        self, authed_client: httpx.Client, faktura_testowa_id: int
    ) -> None:
        """Reset bez nowe_user_ids zwraca 422."""
        resp = authed_client.post(
            f"/faktury-akceptacja/{faktura_testowa_id}/reset",
            json={"powod": "SELFTEST brak user_ids"},
        )
        assert resp.status_code == 422, f"got {resp.status_code}"


class TestMojeFaktury:
    """Testy endpointów pracownika /moje-faktury."""

    def test_lista_moje_faktury(
        self, authed_client: httpx.Client, faktura_testowa_id: int
    ) -> None:
        """GET /moje-faktury zwraca listę przypisanych faktur."""
        resp = authed_client.get("/moje-faktury")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:200]}"
        data = resp.json()
        assert "data" in data
        assert isinstance(data["data"], list)

    def test_szczegoly_moje_faktury(
        self, authed_client: httpx.Client, faktura_testowa_id: int
    ) -> None:
        """GET /moje-faktury/{id} zwraca szczegóły przypisanej faktury."""
        resp = authed_client.get(f"/moje-faktury/{faktura_testowa_id}")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:200]}"
        data = resp.json()
        assert data.get("id") == faktura_testowa_id
        assert "moj_status" in data

    def test_decyzja_akceptacja(
        self, authed_client: httpx.Client, faktura_testowa_id: int
    ) -> None:
        """POST /moje-faktury/{id}/decyzja — akceptacja faktury."""
        # Najpierw reset żeby mieć pewność że status to 'oczekuje'
        r_reset = authed_client.post(
            f"/faktury-akceptacja/{faktura_testowa_id}/reset",
            json={"powod": "SELFTEST przed decyzją", "nowe_user_ids": [1]},
        )
        if r_reset.status_code in (200, 202):
            token = r_reset.json().get("confirm_token")
            if token:
                authed_client.post(
                    f"/faktury-akceptacja/{faktura_testowa_id}/reset/confirm",
                    json={"confirm_token": token},
                )

        # Decyzja
        resp = authed_client.post(
            f"/moje-faktury/{faktura_testowa_id}/decyzja",
            json={
                "status": "zaakceptowane",
                "komentarz": "SELFTEST — akceptacja automatyczna",
            },
        )
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:200]}"
        data = resp.json()
        assert data.get("twoja_decyzja") == "zaakceptowane", (
            f"Oczekiwano 'zaakceptowane', got {data.get('twoja_decyzja')}"
        )

    def test_decyzja_duplikat_blokada(
        self, authed_client: httpx.Client, faktura_testowa_id: int
    ) -> None:
        """Druga decyzja dla tej samej faktury zwraca 409 Conflict."""
        # Zakładamy że test_decyzja_akceptacja już wykonał decyzję
        resp = authed_client.post(
            f"/moje-faktury/{faktura_testowa_id}/decyzja",
            json={
                "status": "odrzucone",
                "komentarz": "SELFTEST — próba duplikatu",
            },
        )
        assert resp.status_code == 409, (
            f"Oczekiwano 409 dla duplikatu decyzji, got {resp.status_code}: {resp.text[:200]}"
        )

    def test_decyzja_nieprawidlowy_status(
        self, authed_client: httpx.Client, faktura_testowa_id: int
    ) -> None:
        """Decyzja z nieprawidłowym statusem zwraca 422."""
        resp = authed_client.post(
            f"/moje-faktury/{faktura_testowa_id}/decyzja",
            json={"status": "NIEPRAWIDLOWY_STATUS_XYZ"},
        )
        assert resp.status_code == 422, f"got {resp.status_code}"


class TestUprawnieniaFaktury:
    """Testy że uprawnienia faktury są poprawnie przypisane."""

    def test_14_uprawnien_faktury(self, authed_client: httpx.Client) -> None:
        """W bazie jest dokładnie 14 uprawnień kategorii 'faktury'."""
        resp = authed_client.get("/permissions", params={"category": "faktury"})
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:200]}"
        data = resp.json()
        raw = data.get("data", data)
        # Obsługa formatu: {"items": {"faktury": [...]}, "total": 14}
        if isinstance(raw, dict):
            inner = raw.get("items", raw)
            if isinstance(inner, dict):
                # grouped_by_category — spłaszcz do jednej listy
                items = [p for sublist in inner.values() for p in sublist]
            elif isinstance(inner, list):
                items = inner
            else:
                items = []
        elif isinstance(raw, list):
            items = raw
        else:
            items = []
        assert len(items) == 14, (
            f"Oczekiwano 14 uprawnień faktury, znaleziono {len(items)}: "
            f"{[p.get('permission_name') or p.get('name') for p in items]}"
        )

    def test_uprawnienia_admin_ma_faktury(self, authed_client: httpx.Client) -> None:
        """Rola Admin ma przypisane uprawnienia faktury."""
        # Sprawdzamy przez to że admin może wywołać endpoint faktury
        resp = authed_client.get("/faktury-akceptacja")
        assert resp.status_code == 200, (
            f"Admin nie ma dostępu do /faktury-akceptacja: {resp.status_code}"
        )

    def test_config_modul_enabled(self, authed_client: httpx.Client) -> None:
        """Moduł faktur jest włączony w konfiguracji systemu."""
        resp = authed_client.get("/system/config")
        assert resp.status_code == 200
        items = resp.json().get("data", {}).get("items", {})
        val = items.get("modul_akceptacji_faktur_enabled")
        assert val == "true", (
            f"modul_akceptacji_faktur_enabled = '{val}', oczekiwano 'true'"
        )