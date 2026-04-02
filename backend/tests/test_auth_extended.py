"""
test_auth_extended.py — Rozszerzone testy modułu /auth
========================================================
System Windykacja GPGK Jasło — Sprint 2.3

WAŻNA REGUŁA (zgodna z Sprint 2.2):
  Testy weryfikujące 401 (brak tokenu) MUSZĄ tworzyć świeżego klienta httpx.
  Fixture http_client jest mutowany przez authed_client (dodaje Bearer token),
  więc po inicjalizacji sesji http_client też ma token.

Stan implementacji endpointów:
  /auth/me               — ZAIMPLEMENTOWANY ✅
  /auth/change-password  — NIEZAIMPLEMENTOWANY ❌ → wszystkie testy xfail
  /auth/forgot-password  — NIEZAIMPLEMENTOWANY ❌ → wszystkie testy xfail
  /auth/impersonate      — NIEZAIMPLEMENTOWANY ❌ → testy xfail
"""

from __future__ import annotations

import logging
import os

import httpx
import pytest

from conftest import BASE_URL, TIMEOUT

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Dekoratory xfail dla niezaimplementowanych endpointów.
# Gdy endpoint zostanie dodany → testy staną się XPASS → usuń dekorator.
# ---------------------------------------------------------------------------

_XFAIL_CHANGE_PASSWORD = pytest.mark.xfail(
    reason="Endpoint /auth/change-password zwraca 404 — niezaimplementowany w tej wersji",
    strict=False,
)

_XFAIL_IMPERSONATE = pytest.mark.xfail(
    reason="Endpoint /auth/impersonate zwraca 404 — niezaimplementowany w tej wersji",
    strict=False,
)


def _fresh_client() -> httpx.Client:
    """
    Tworzy świeżego klienta HTTP BEZ tokenu autoryzacji.

    Używać w testach weryfikujących 401 — NIE używaj fixture http_client,
    który jest mutowany przez authed_client i może mieć Bearer token.
    """
    return httpx.Client(base_url=BASE_URL, timeout=TIMEOUT)


class TestAuthMe:
    """GET /auth/me — dane zalogowanego użytkownika."""

    def test_me_ok(self, authed_client: httpx.Client) -> None:
        """GET /auth/me zwraca dane zalogowanego admina."""
        resp = authed_client.get("/auth/me")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        data = _extract_user(resp.json())
        assert data.get("username", "").lower() == "admin", (
            f"Oczekiwano username='admin', got '{data.get('username')}'\n"
            f"Pełna odpowiedź: {resp.text[:300]}"
        )

    def test_me_zawiera_wymagane_pola(self, authed_client: httpx.Client) -> None:
        """GET /auth/me zawiera: username, email."""
        resp = authed_client.get("/auth/me")
        assert resp.status_code == 200
        data = _extract_user(resp.json())
        wymagane = {"username", "email"}
        brakujace = wymagane - set(data.keys())
        assert not brakujace, (
            f"Brakuje pól w /auth/me: {brakujace}\n"
            f"Dostępne: {list(data.keys())}"
        )

    def test_me_nie_eksponuje_hasla(self, authed_client: httpx.Client) -> None:
        """GET /auth/me NIE zwraca hash hasła."""
        resp = authed_client.get("/auth/me")
        assert resp.status_code == 200
        assert "password_hash" not in resp.text.lower(), (
            "BEZPIECZEŃSTWO: /auth/me eksponuje password_hash!"
        )
        assert "passwordhash" not in resp.text.lower(), (
            "BEZPIECZEŃSTWO: /auth/me eksponuje passwordHash!"
        )

    def test_me_bez_tokenu_401(self, http_client: httpx.Client) -> None:
        """
        GET /auth/me bez tokenu → 401.

        UWAGA: Tworzymy świeżego klienta — NIE używamy http_client bezpośrednio,
        bo po inicjalizacji authed_client fixtures http_client ma już Bearer token.
        """
        with _fresh_client() as c:
            resp = c.get("/auth/me")
        assert resp.status_code == 401, (
            f"Oczekiwano 401 bez tokenu, got {resp.status_code}: {resp.text[:200]}"
        )

    def test_me_zgodny_z_users_id1(self, authed_client: httpx.Client) -> None:
        """
        /auth/me i /users/1 muszą zwracać tego samego usera (admin).
        Sprawdzamy username (case-insensitive).
        """
        resp_me = authed_client.get("/auth/me")
        resp_user = authed_client.get("/users/1")
        assert resp_me.status_code == 200, f"/auth/me: {resp_me.status_code}"
        assert resp_user.status_code == 200, f"/users/1: {resp_user.status_code}"

        me = _extract_user(resp_me.json())
        user = _extract_user(resp_user.json())

        me_username = me.get("username", "").lower()
        user_username = user.get("username", "").lower()
        assert me_username == user_username, (
            f"Username niezgodny: /auth/me='{me_username}' vs /users/1='{user_username}'\n"
            f"/auth/me keys: {list(me.keys())}\n"
            f"/users/1 keys: {list(user.keys())}"
        )


# ---------------------------------------------------------------------------
# XFAIL — endpoint /auth/change-password nie jest zaimplementowany
# ---------------------------------------------------------------------------

class TestAuthChangePassword:
    """
    POST /auth/change-password — zmiana własnego hasła.
    Endpoint zwraca 404 (niezaimplementowany) — wszystkie testy xfail.
    """

    @_XFAIL_CHANGE_PASSWORD
    def test_zmiana_hasla_zle_stare_haslo_401(
        self, authed_client: httpx.Client
    ) -> None:
        """Zmiana hasła ze złym starym hasłem → 400/401/422."""
        resp = authed_client.post(
            "/auth/change-password",
            json={
                "old_password": "ZUPELNIE_NIEPRAWIDLOWE_HASLO_XYZ_!@#$",
                "new_password": "NoweHaslo!2026",
            },
        )
        assert resp.status_code in (400, 401, 422), (
            f"Złe stare hasło powinno dać błąd, got {resp.status_code}: {resp.text[:200]}"
        )

    @_XFAIL_CHANGE_PASSWORD
    def test_zmiana_hasla_nowe_za_slabe_422(
        self, authed_client: httpx.Client
    ) -> None:
        """Zmiana hasła na zbyt słabe → 422."""
        password = os.environ.get("SELFTEST_PASSWORD", "admin")
        resp = authed_client.post(
            "/auth/change-password",
            json={"old_password": password, "new_password": "123"},
        )
        assert resp.status_code == 422, (
            f"Słabe nowe hasło powinno dać 422, got {resp.status_code}: {resp.text[:200]}"
        )

    @_XFAIL_CHANGE_PASSWORD
    def test_zmiana_hasla_takie_samo_jak_stare(
        self, authed_client: httpx.Client
    ) -> None:
        """Zmiana hasła na to samo → 400/422."""
        password = os.environ.get("SELFTEST_PASSWORD", "")
        if not password:
            pytest.skip("SELFTEST_PASSWORD nie ustawione")
        resp = authed_client.post(
            "/auth/change-password",
            json={"old_password": password, "new_password": password},
        )
        assert resp.status_code in (400, 422), (
            f"Zmiana na to samo hasło powinna być zablokowana, got {resp.status_code}"
        )

    @_XFAIL_CHANGE_PASSWORD
    def test_zmiana_hasla_bez_tokenu_401(self, http_client: httpx.Client) -> None:
        """POST /auth/change-password bez tokenu → 401."""
        with _fresh_client() as c:
            resp = c.post(
                "/auth/change-password",
                json={"old_password": "x", "new_password": "y"},
            )
        assert resp.status_code == 401, (
            f"Oczekiwano 401 bez tokenu, got {resp.status_code}: {resp.text[:200]}"
        )

    @_XFAIL_CHANGE_PASSWORD
    def test_zmiana_hasla_brak_pol_422(self, authed_client: httpx.Client) -> None:
        """POST /auth/change-password z brakującymi polami → 422."""
        resp = authed_client.post(
            "/auth/change-password",
            json={"old_password": "tylko_stare"},
        )
        assert resp.status_code == 422, (
            f"Brak new_password powinien dać 422, got {resp.status_code}"
        )


# ---------------------------------------------------------------------------
# XFAIL — endpoint /auth/forgot-password nie jest zaimplementowany
# ---------------------------------------------------------------------------

class TestAuthForgotPassword:
    """
    POST /auth/forgot-password — reset przez email.
    Endpoint zwraca 404 (niezaimplementowany) — wszystkie testy xfail.
    """

    @pytest.mark.xfail(
        reason="Endpoint /auth/forgot-password zwraca 404 — niezaimplementowany",
        strict=False,
    )
    def test_forgot_password_nieistniejacy_email(
        self, http_client: httpx.Client
    ) -> None:
        """forgot-password z nieistniejącym emailem → 200 (security: nie ujawnia istnienia)."""
        with _fresh_client() as c:
            resp = c.post(
                "/auth/forgot-password",
                json={"email": "selftest_nieistniejacy_xyz@windykacja.test"},
            )
        assert resp.status_code == 200, (
            f"forgot-password powinno zawsze zwracać 200, got {resp.status_code}"
        )

    @pytest.mark.xfail(
        reason="Endpoint /auth/forgot-password zwraca 404 — niezaimplementowany",
        strict=False,
    )
    def test_forgot_password_nieprawidlowy_email_422(
        self, http_client: httpx.Client
    ) -> None:
        """forgot-password z nieprawidłowym emailem → 422."""
        with _fresh_client() as c:
            resp = c.post("/auth/forgot-password", json={"email": "to_nie_email"})
        assert resp.status_code == 422, (
            f"Nieprawidłowy email powinien dać 422, got {resp.status_code}"
        )

    @pytest.mark.xfail(
        reason="Endpoint /auth/forgot-password zwraca 404 — niezaimplementowany",
        strict=False,
    )
    def test_forgot_password_brak_emaila_422(
        self, http_client: httpx.Client
    ) -> None:
        """forgot-password bez email → 422."""
        with _fresh_client() as c:
            resp = c.post("/auth/forgot-password", json={})
        assert resp.status_code == 422, f"got {resp.status_code}"


# ---------------------------------------------------------------------------
# XFAIL — endpoint /auth/impersonate nie jest zaimplementowany
# ---------------------------------------------------------------------------

class TestAuthImpersonate:
    """
    POST /auth/impersonate/{id} — impersonacja użytkownika.
    Endpoint zwraca 404 (niezaimplementowany) — większość testów xfail.
    """

    @_XFAIL_IMPERSONATE
    def test_impersonate_status_bez_impersonacji(
        self, authed_client: httpx.Client
    ) -> None:
        """GET /auth/impersonate/status → 200, is_impersonating=false."""
        resp = authed_client.get("/auth/impersonate/status")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        data = _extract_user(resp.json())
        is_active = (
            data.get("impersonating")
            or data.get("active")
            or data.get("is_impersonating")
        )
        assert not is_active, f"Impersonacja nie powinna być aktywna: {data}"

    @_XFAIL_IMPERSONATE
    def test_impersonate_siebie_400(
        self, authed_client: httpx.Client
    ) -> None:
        """POST /auth/impersonate/1 (siebie) → 400/403."""
        resp = authed_client.post("/auth/impersonate/1")
        assert resp.status_code in (400, 403), (
            f"Impersonacja samego siebie powinna być zablokowana, got {resp.status_code}"
        )

    @_XFAIL_IMPERSONATE
    def test_impersonate_nieistniejacego_404(
        self, authed_client: httpx.Client
    ) -> None:
        """POST /auth/impersonate/99999 → 404."""
        resp = authed_client.post("/auth/impersonate/99999")
        assert resp.status_code == 404, f"got {resp.status_code}: {resp.text[:200]}"

    @_XFAIL_IMPERSONATE
    def test_impersonate_flow_kompletny(
        self,
        authed_client: httpx.Client,
        user_testowy_id: int,
    ) -> None:
        """Pełny flow impersonacji: start → status → me → end → me (powrót do admina)."""
        resp_start = authed_client.post(f"/auth/impersonate/{user_testowy_id}")
        assert resp_start.status_code == 200, (
            f"Impersonacja nieudana: {resp_start.status_code}: {resp_start.text[:300]}"
        )

        resp_status = authed_client.get("/auth/impersonate/status")
        assert resp_status.status_code == 200
        data_status = _extract_user(resp_status.json())
        is_active = (
            data_status.get("impersonating")
            or data_status.get("active")
            or data_status.get("is_impersonating")
        )
        assert is_active, f"Status impersonacji powinien być aktywny: {data_status}"

        resp_end = authed_client.post("/auth/impersonate/end")
        assert resp_end.status_code == 200, (
            f"End impersonacji nieudane: {resp_end.status_code}: {resp_end.text[:300]}"
        )

        resp_me_po = authed_client.get("/auth/me")
        assert resp_me_po.status_code == 200
        me_po = _extract_user(resp_me_po.json())
        assert me_po.get("username", "").lower() == "admin", (
            f"Po zakończeniu impersonacji oczekiwano admina, got '{me_po.get('username')}'"
        )

    @_XFAIL_IMPERSONATE
    def test_impersonate_end_bez_aktywnej_impersonacji(
        self, authed_client: httpx.Client
    ) -> None:
        """POST /auth/impersonate/end bez aktywnej impersonacji → 200/400."""
        resp = authed_client.post("/auth/impersonate/end")
        assert resp.status_code in (200, 400), (
            f"End bez aktywnej impersonacji: oczekiwano 200/400, got {resp.status_code}"
        )

    def test_impersonate_bez_tokenu_401(self, http_client: httpx.Client) -> None:
        """
        POST /auth/impersonate/1 bez tokenu → 401.

        UWAGA: Tworzymy świeżego klienta — http_client może mieć Bearer token.
        Jeśli endpoint nie istnieje (404) — xfail.
        """
        with _fresh_client() as c:
            resp = c.post("/auth/impersonate/1")

        if resp.status_code == 404:
            pytest.xfail(
                "Endpoint /auth/impersonate zwraca 404 (niezaimplementowany). "
                "Gdy zostanie dodany — zweryfikuj 401 bez tokenu."
            )
        assert resp.status_code == 401, (
            f"Oczekiwano 401 bez tokenu dla /auth/impersonate/1, "
            f"got {resp.status_code}: {resp.text[:200]}"
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_user(resp_json: dict) -> dict:
    """
    Wyciąga dane usera z odpowiedzi API w różnych formatach:
      Format A: {"username": "admin", "email": "..."}
      Format B: {"data": {"username": "admin", ...}}
      Format C: {"data": {"user": {"username": "admin", ...}}}
    """
    if "username" in resp_json:
        return resp_json

    data = resp_json.get("data")
    if isinstance(data, dict):
        if "username" in data:
            return data
        user = data.get("user")
        if isinstance(user, dict) and "username" in user:
            return user

    return data if isinstance(data, dict) else resp_json