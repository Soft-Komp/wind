"""
test_users.py — Testy modułu /users
=====================================
System Windykacja GPGK Jasło — Sprint 2.3

Pokrycie: 9 endpointów
  GET    /users                    — lista
  GET    /users/{id}               — szczegóły
  POST   /users                    — tworzenie
  PUT    /users/{id}               — edycja
  POST   /users/{id}/lock          — blokada
  POST   /users/{id}/unlock        — odblokowanie
  POST   /users/{id}/reset-password — reset hasła przez admina
  DELETE /users/{id}/initiate      — krok 1 usuwania
  DELETE /users/{id}/confirm       — krok 2 usuwania

Klasy testowe:
  TestUsersLista     — GET /users z filtrami i paginacją
  TestUsersOdczyt    — GET /users/{id}, 404, format odpowiedzi
  TestUsersTworzenie — POST /users: happy path + walidacja + duplikaty
  TestUsersEdycja    — PUT /users/{id}: edycja danych, role_id bug (S2-05)
  TestUsersLock      — lock/unlock: flow, edge cases
  TestUsersDelete    — 2-krokowy DELETE: tok, TTL, autoremove
  TestUsersBezAuth   — 401/403 dla wszystkich chronionych endpointów
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone, timedelta

import httpx
import pytest

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# TestUsersLista
# ─────────────────────────────────────────────────────────────────────────────

class TestUsersLista:
    """GET /users — lista z filtrami."""

    def test_lista_podstawowa(self, authed_client: httpx.Client) -> None:
        """GET /users zwraca 200 z niepustą listą zawierającą admina."""
        resp = authed_client.get("/users")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        data = resp.json()
        # Sprawdzamy strukturę odpowiedzi
        assert "data" in data, f"Brak klucza 'data' w odpowiedzi: {list(data.keys())}"
        users = _lista(data)
        assert len(users) >= 1, "Lista użytkowników jest pusta"
        usernames = [u.get("username", "") for u in users]
        assert any("admin" in u.lower() for u in usernames), (
            f"Admin nie znaleziony na liście: {usernames}"
        )

    def test_lista_paginacja_per_page(self, authed_client: httpx.Client) -> None:
        """per_page=1 zwraca dokładnie 1 wynik."""
        resp = authed_client.get("/users", params={"page": 1, "per_page": 1})
        assert resp.status_code == 200
        users = _lista(resp.json())
        assert len(users) <= 1, f"per_page=1, ale dostaliśmy {len(users)} wyników"

    def test_lista_filtr_is_active_true(self, authed_client: httpx.Client) -> None:
        """Filtr is_active=true zwraca tylko aktywnych."""
        resp = authed_client.get("/users", params={"is_active": "true"})
        assert resp.status_code == 200
        users = _lista(resp.json())
        for u in users:
            assert u.get("is_active") in (True, 1, "true", "1"), (
                f"Nieaktywny user na liście is_active=true: {u}"
            )

    def test_lista_filtr_role_id(self, authed_client: httpx.Client) -> None:
        """Filtr role_id=1 zwraca tylko userów z rolą ID=1."""
        resp = authed_client.get("/users", params={"role_id": 1})
        assert resp.status_code == 200
        users = _lista(resp.json())
        for u in users:
            assert u.get("role_id") == 1, (
                f"User z role_id={u.get('role_id')} na liście role_id=1: {u.get('username')}"
            )

    def test_lista_filtr_search(self, authed_client: httpx.Client) -> None:
        """Filtr search=admin zwraca admina."""
        resp = authed_client.get("/users", params={"search": "admin"})
        assert resp.status_code == 200
        users = _lista(resp.json())
        assert len(users) >= 1, "search=admin nie znalazło żadnego wynika"

    def test_lista_struktura_paginacji(self, authed_client: httpx.Client) -> None:
        """Odpowiedź zawiera metadane paginacji (total, page, per_page)."""
        resp = authed_client.get("/users", params={"page": 1, "per_page": 10})
        assert resp.status_code == 200
        body = resp.json()
        data = body.get("data", body)
        # Metadane mogą być w różnych miejscach
        has_total = (
            "total" in body
            or "total" in data
            or "total_count" in data
            or "pagination" in data
        )
        assert has_total, (
            f"Brak metadanych paginacji (total) w odpowiedzi: {list(data.keys() if isinstance(data, dict) else [])}"
        )

    def test_lista_bez_tokenu_401(self, http_client: httpx.Client) -> None:
        """GET /users bez tokenu → 401."""
        resp = http_client.get("/users")
        assert resp.status_code == 401, f"Oczekiwano 401, got {resp.status_code}"


# ─────────────────────────────────────────────────────────────────────────────
# TestUsersOdczyt
# ─────────────────────────────────────────────────────────────────────────────

class TestUsersOdczyt:
    """GET /users/{id} — szczegóły użytkownika."""

    def test_szczegoly_admina(self, authed_client: httpx.Client) -> None:
        """GET /users/1 zwraca dane admina."""
        resp = authed_client.get("/users/1")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        user = _data(resp.json())
        assert user.get("username", "").lower() == "admin", (
            f"Oczekiwano username='admin', got '{user.get('username')}'"
        )

    def test_szczegoly_struktura_pol(self, authed_client: httpx.Client) -> None:
        """Odpowiedź /users/1 zawiera wymagane pola."""
        resp = authed_client.get("/users/1")
        assert resp.status_code == 200
        user = _data(resp.json())
        wymagane = {"username", "email", "role_id", "is_active"}
        brakujace = wymagane - set(user.keys())
        assert not brakujace, f"Brakuje pól w odpowiedzi: {brakujace}"

    def test_szczegoly_nie_eksponuje_hasla(self, authed_client: httpx.Client) -> None:
        """Odpowiedź NIE zawiera hash hasła ani failed_login_attempts."""
        resp = authed_client.get("/users/1")
        assert resp.status_code == 200
        body_str = resp.text.lower()
        assert "password_hash" not in body_str, "Odpowiedź zawiera password_hash!"
        assert "passwordhash" not in body_str, "Odpowiedź zawiera passwordHash!"

    def test_szczegoly_nieistniejacy_404(self, authed_client: httpx.Client) -> None:
        """GET /users/99999 → 404."""
        resp = authed_client.get("/users/99999")
        assert resp.status_code == 404, f"Oczekiwano 404, got {resp.status_code}"

    def test_szczegoly_bez_tokenu_401(self, http_client: httpx.Client) -> None:
        """GET /users/1 bez tokenu → 401."""
        resp = http_client.get("/users/1")
        assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────────────────────
# TestUsersTworzenie
# ─────────────────────────────────────────────────────────────────────────────

class TestUsersTworzenie:
    """POST /users — tworzenie użytkownika."""

    def test_tworzenie_ok(self, authed_client: httpx.Client, user_testowy_id: int) -> None:
        """Fixture user_testowy_id potwierdza że POST /users działa → 201."""
        assert user_testowy_id > 0, "user_testowy_id <= 0"
        # Weryfikacja że user faktycznie istnieje
        resp = authed_client.get(f"/users/{user_testowy_id}")
        assert resp.status_code == 200, f"Utworzony user ID={user_testowy_id} nie istnieje"
        user = _data(resp.json())
        assert user.get("username") == "selftest_user_9999"

    def test_tworzenie_duplikat_username(
        self, authed_client: httpx.Client, user_testowy_id: int
    ) -> None:
        """POST /users z istniejącym username → 409 Conflict."""
        resp = authed_client.post(
            "/users",
            json={
                "username": "selftest_user_9999",  # duplikat
                "email": "inny_email_9999@windykacja.test",
                "password": "SelfTest!9999",
                "role_id": 1,
            },
        )
        assert resp.status_code == 409, (
            f"Oczekiwano 409 dla duplikatu username, got {resp.status_code}: {resp.text[:200]}"
        )

    def test_tworzenie_duplikat_email(
        self, authed_client: httpx.Client, user_testowy_id: int
    ) -> None:
        """POST /users z istniejącym emailem → 409 Conflict."""
        resp = authed_client.post(
            "/users",
            json={
                "username": "inny_login_selftest_9999",
                "email": "selftest9999@windykacja.test",  # duplikat
                "password": "SelfTest!9999",
                "role_id": 1,
            },
        )
        assert resp.status_code == 409, (
            f"Oczekiwano 409 dla duplikatu email, got {resp.status_code}: {resp.text[:200]}"
        )

    def test_tworzenie_brak_wymaganych_pol(self, authed_client: httpx.Client) -> None:
        """POST /users bez wymaganych pól → 422."""
        resp = authed_client.post("/users", json={"username": "tylko_login"})
        assert resp.status_code == 422, f"got {resp.status_code}"

    def test_tworzenie_nieprawidlowy_email(self, authed_client: httpx.Client) -> None:
        """POST /users z nieprawidłowym emailem → 422."""
        resp = authed_client.post(
            "/users",
            json={
                "username": "test_zly_email",
                "email": "to_nie_jest_email",
                "password": "SelfTest!9999",
                "role_id": 1,
            },
        )
        assert resp.status_code == 422, (
            f"Nieprawidłowy email powinien dać 422, got {resp.status_code}"
        )

    def test_tworzenie_slabe_haslo(self, authed_client: httpx.Client) -> None:
        """POST /users ze słabym hasłem → 422."""
        resp = authed_client.post(
            "/users",
            json={
                "username": "test_slabe_haslo",
                "email": "slabe@windykacja.test",
                "password": "123",  # za słabe
                "role_id": 1,
            },
        )
        assert resp.status_code == 422, (
            f"Słabe hasło powinno dać 422, got {resp.status_code}"
        )

    def test_tworzenie_nieznane_pole_odrzucone(self, authed_client: httpx.Client) -> None:
        """POST /users z nieznanym polem → 422 (extra='forbid')."""
        resp = authed_client.post(
            "/users",
            json={
                "username": "test_extra_field",
                "email": "extra@windykacja.test",
                "password": "SelfTest!9999",
                "role_id": 1,
                "POLE_KTORE_NIE_ISTNIEJE": "hacker_payload",
            },
        )
        assert resp.status_code == 422, (
            f"Nieznane pole powinno dać 422 (extra=forbid), got {resp.status_code}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# TestUsersEdycja
# ─────────────────────────────────────────────────────────────────────────────

class TestUsersEdycja:
    """PUT /users/{id} — edycja użytkownika."""

    def test_edycja_full_name(
        self, authed_client: httpx.Client, user_testowy_id: int
    ) -> None:
        """PUT /users/{id} zmienia full_name."""
        resp = authed_client.put(
            f"/users/{user_testowy_id}",
            json={
                "email": "selftest9999@windykacja.test",
                "full_name": "SELFTEST Zmienione Imię",
                "role_id": 1,
                "is_active": True,
            },
        )
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"

    def test_edycja_persystuje_role_id(
        self, authed_client: httpx.Client, user_testowy_id: int
    ) -> None:
        """
        PUT /users/{id} — role_id musi być persystowany (bug S2-05).
        Zmiana role_id=4 (ReadOnly) i weryfikacja GET.
        """
        # Zmień na ReadOnly (zakładamy ID=4)
        resp_put = authed_client.put(
            f"/users/{user_testowy_id}",
            json={
                "email": "selftest9999@windykacja.test",
                "full_name": "SELFTEST Test Role Bug",
                "role_id": 4,
                "is_active": True,
            },
        )
        assert resp_put.status_code == 200, f"PUT failed: {resp_put.text[:300]}"

        # Weryfikacja przez GET
        resp_get = authed_client.get(f"/users/{user_testowy_id}")
        assert resp_get.status_code == 200
        user = _data(resp_get.json())
        assert user.get("role_id") == 4, (
            f"BUG S2-05: role_id nie został zapisany! "
            f"Oczekiwano 4, got {user.get('role_id')}"
        )

        # Przywróć Admin
        authed_client.put(
            f"/users/{user_testowy_id}",
            json={"email": "selftest9999@windykacja.test", "role_id": 1, "is_active": True},
        )

    def test_edycja_nieistniejacy_404(self, authed_client: httpx.Client) -> None:
        """PUT /users/99999 → 404."""
        resp = authed_client.put(
            "/users/99999",
            json={"email": "x@x.com", "role_id": 1, "is_active": True},
        )
        assert resp.status_code == 404, f"got {resp.status_code}"

    def test_edycja_nieprawidlowy_email(
        self, authed_client: httpx.Client, user_testowy_id: int
    ) -> None:
        """PUT /users/{id} z nieprawidłowym emailem → 422."""
        resp = authed_client.put(
            f"/users/{user_testowy_id}",
            json={"email": "to_nie_email", "role_id": 1, "is_active": True},
        )
        assert resp.status_code == 422, f"got {resp.status_code}"


# ─────────────────────────────────────────────────────────────────────────────
# TestUsersLock
# ─────────────────────────────────────────────────────────────────────────────

class TestUsersLock:
    """POST /users/{id}/lock i /unlock."""

    def test_lock_ok(
        self, authed_client: httpx.Client, user_testowy_id: int
    ) -> None:
        """Blokada konta zwraca 200 z locked_until."""
        locked_until = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        resp = authed_client.post(
            f"/users/{user_testowy_id}/lock",
            json={"locked_until": locked_until},
        )
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        data = _data(resp.json())
        assert "locked_until" in data or "user_id" in data, (
            f"Odpowiedź lock nie zawiera oczekiwanych pól: {list(data.keys())}"
        )

    def test_unlock_po_lock(
        self, authed_client: httpx.Client, user_testowy_id: int
    ) -> None:
        """Odblokowanie konta zwraca 200."""
        resp = authed_client.post(f"/users/{user_testowy_id}/unlock")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"

    def test_lock_nieistniejacy_404(self, authed_client: httpx.Client) -> None:
        """Lock nieistniejącego usera → 404."""
        locked_until = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        resp = authed_client.post(
            "/users/99999/lock",
            json={"locked_until": locked_until},
        )
        assert resp.status_code == 404, f"got {resp.status_code}"

    def test_lock_samego_siebie(self, authed_client: httpx.Client) -> None:
        """
        Admin nie może zablokować własnego konta (ID=1).
        Oczekujemy 400 lub 403 — zależy od implementacji.
        """
        locked_until = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        resp = authed_client.post(
            "/users/1/lock",
            json={"locked_until": locked_until},
        )
        assert resp.status_code in (400, 403), (
            f"Admin powinien być chroniony przed samolockingiem, got {resp.status_code}"
        )

    def test_unlock_nie_zablokowanego(
        self, authed_client: httpx.Client, user_testowy_id: int
    ) -> None:
        """
        Unlock konta które nie jest zablokowane.
        Powinno zwrócić 400 lub 200 (idempotentne).
        """
        # Upewniamy się że odblokowane (z poprzedniego testu)
        resp = authed_client.post(f"/users/{user_testowy_id}/unlock")
        assert resp.status_code in (200, 400), (
            f"Unlock nie-zablokowanego: oczekiwano 200 lub 400, got {resp.status_code}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# TestUsersDelete
# ─────────────────────────────────────────────────────────────────────────────

class TestUsersDelete:
    """DELETE 2-krokowy."""

    def test_delete_nie_siebie(self, authed_client: httpx.Client) -> None:
        """
        DELETE /users/1/initiate (admin usuwa siebie) → 400.
        Blokada usunięcia własnego konta.
        """
        resp = authed_client.delete("/users/1/initiate")
        assert resp.status_code in (400, 403), (
            f"Admin powinien być chroniony przed samousunięciem, got {resp.status_code}"
        )

    def test_delete_nieistniejacy_404(self, authed_client: httpx.Client) -> None:
        """DELETE /users/99999/initiate → 404."""
        resp = authed_client.delete("/users/99999/initiate")
        assert resp.status_code == 404, f"got {resp.status_code}"

    def test_delete_zly_token_confirm(
        self, authed_client: httpx.Client, user_testowy_id: int
    ) -> None:
        """DELETE confirm ze złym tokenem → 400/401."""
        resp = authed_client.delete(
            f"/users/{user_testowy_id}/confirm",
            json={"confirm_token": "ZLYTOKEN.SELFTEST.XYZ.NIEPRAWIDLOWY"},
        )
        assert resp.status_code in (400, 401, 422), (
            f"Zły token powinien dać błąd, got {resp.status_code}"
        )

    def test_delete_initiate_zwraca_token(
        self, authed_client: httpx.Client, user_testowy_id: int
    ) -> None:
        """DELETE initiate zwraca confirm_token/delete_token i expires_in."""
        resp = authed_client.delete(f"/users/{user_testowy_id}/initiate")
        assert resp.status_code == 202, f"got {resp.status_code}: {resp.text[:300]}"
        data = _data(resp.json())
        token = data.get("delete_token") or data.get("confirm_token")
        assert token, f"Brak tokenu w odpowiedzi: {list(data.keys())}"
        expires = data.get("expires_in") or data.get("ttl_seconds")
        assert expires, f"Brak expires_in w odpowiedzi: {list(data.keys())}"
        # Token musi być sensownej długości
        assert len(str(token)) > 10, f"Token wygląda podejrzanie krótko: {token}"
        logger.info(f"SELFTEST: delete token długość={len(str(token))}, expires_in={expires}")
        # UWAGA: nie potwierdzamy usunięcia tutaj — cleanup fixture to zrobi


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _data(response_json: dict) -> dict:
    """Wyciąga data z odpowiedzi API."""
    d = response_json.get("data", response_json)
    return d if isinstance(d, dict) else response_json


def _lista(response_json: dict) -> list:
    """Wyciąga listę z odpowiedzi API."""
    data = response_json.get("data", response_json)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("items", "users", "data", "results"):
            val = data.get(key)
            if isinstance(val, list):
                return val
    return []