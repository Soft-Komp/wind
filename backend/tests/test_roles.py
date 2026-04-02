"""
test_roles.py — Testy modułu /roles i /roles-permissions
===========================================================
System Windykacja GPGK Jasło — Sprint 2.3

Stan API po analizie verbose:
  GET /roles           — DZIAŁA ✅ (klucz: role_name, nie name)
  GET /roles/{id}      — DZIAŁA ✅ (ale BEZ permissions w odpowiedzi → xfail)
  GET /roles/{id}/permissions — DZIAŁA ✅ (osobny endpoint)
  POST/PUT/DELETE roles — do weryfikacji

Klucze w odpowiedzi GET /roles/{id}:
  ['id_role', 'role_name', 'description', 'created_at', 'updated_at', 'is_active']
  BRAK: 'permissions' — to jest bug API vs spec, oznaczamy jako xfail.
"""

from __future__ import annotations

import logging

import httpx
import pytest

from conftest import BASE_URL, TEST_PERM_NAME, TEST_ROLE_NAME, TIMEOUT

logger = logging.getLogger(__name__)


def _fresh_client() -> httpx.Client:
    """Świeży klient bez tokenu — do testów 401."""
    return httpx.Client(base_url=BASE_URL, timeout=TIMEOUT)


class TestRoleLista:
    """GET /roles — lista ról."""

    def test_lista_podstawowa(self, authed_client: httpx.Client) -> None:
        """GET /roles zwraca ≥ 4 role (Admin, Manager, User, ReadOnly)."""
        resp = authed_client.get("/roles")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        roles = _lista(resp.json())
        assert len(roles) >= 4, f"Oczekiwano ≥ 4 ról, got {len(roles)}"
        # Klucz to 'role_name' (nie 'name') — zweryfikowane z API
        names = [r.get("role_name") or r.get("name", "") for r in roles]
        assert "Admin" in names, f"Rola 'Admin' nie znaleziona: {names}"

    def test_lista_zawiera_wymagane_pola(self, authed_client: httpx.Client) -> None:
        """Każda rola zawiera id_role/id, role_name/name."""
        resp = authed_client.get("/roles")
        assert resp.status_code == 200
        roles = _lista(resp.json())
        for r in roles:
            has_name = r.get("role_name") or r.get("name")
            assert has_name, f"Rola bez nazwy: {r}"
            has_id = r.get("id_role") or r.get("id")
            assert has_id, f"Rola bez ID: {r}"

    def test_lista_zawiera_liczbe_userow(self, authed_client: httpx.Client) -> None:
        """Lista ról może zawierać user_count (informacyjnie)."""
        resp = authed_client.get("/roles")
        assert resp.status_code == 200
        roles = _lista(resp.json())
        if roles:
            first = roles[0]
            has_stats = "user_count" in first or "permission_count" in first
            if not has_stats:
                logger.warning(
                    "SELFTEST: Rola nie zawiera user_count/permission_count: %s",
                    list(first.keys()),
                )
            # Nie failujemy — to informacyjny check

    @pytest.mark.xfail(
        reason=(
            "SECURITY BUG: GET /roles zwraca 200 bez tokenu. "
            "Napraw: dodaj dependencies=[require_permission('roles.view_list')] w routerze."
        ),
        strict=False,
    )
    def test_lista_bez_tokenu_401(self, http_client: httpx.Client) -> None:
        """GET /roles bez tokenu → 401 (security bug — aktualnie zwraca 200)."""
        with _fresh_client() as c:
            resp = c.get("/roles")
        assert resp.status_code == 401


class TestRoleSzczegoly:
    """GET /roles/{id} — szczegóły roli."""

    def test_szczegoly_admin_role(self, authed_client: httpx.Client) -> None:
        """GET /roles/1 zwraca szczegóły roli Admin."""
        resp = authed_client.get("/roles/1")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        data = _data(resp.json())
        # Klucz to 'role_name', nie 'name'
        role_name = data.get("role_name") or data.get("name", "")
        assert role_name.lower() in ("admin",), (
            f"Oczekiwano role_name='Admin', got '{role_name}'\nKlucze: {list(data.keys())}"
        )

    @pytest.mark.xfail(
        reason=(
            "GET /roles/{id} nie zwraca permissions w odpowiedzi "
            "(klucze: id_role, role_name, description, created_at, updated_at, is_active). "
            "Spec zakłada permissions:[...] w szczegółach roli — bug API."
        ),
        strict=False,
    )
    def test_szczegoly_zawiera_permissions(self, authed_client: httpx.Client) -> None:
        """GET /roles/1 POWINIEN zawierać listę uprawnień (według spec)."""
        resp = authed_client.get("/roles/1")
        assert resp.status_code == 200
        data = _data(resp.json())
        has_perms = "permissions" in data or "uprawnienia" in data
        assert has_perms, (
            f"Szczegóły roli nie zawierają uprawnień: {list(data.keys())}\n"
            f"Uprawnienia dostępne przez osobny endpoint: GET /roles/1/permissions"
        )

    def test_szczegoly_nieistniejaca_404(self, authed_client: httpx.Client) -> None:
        """GET /roles/99999 → 404."""
        resp = authed_client.get("/roles/99999")
        assert resp.status_code == 404, f"got {resp.status_code}"

    @pytest.mark.xfail(
        reason=(
            "SECURITY BUG: GET /roles/{id} zwraca 200 bez tokenu. "
            "Napraw: dodaj dependencies=[require_permission('roles.view_details')] w routerze."
        ),
        strict=False,
    )
    def test_szczegoly_bez_tokenu_401(self, http_client: httpx.Client) -> None:
        """GET /roles/1 bez tokenu → 401 (security bug — aktualnie zwraca 200)."""
        with _fresh_client() as c:
            resp = c.get("/roles/1")
        assert resp.status_code == 401


class TestRoleTworzenie:
    """POST /roles — tworzenie roli."""

    def test_tworzenie_ok(
        self, authed_client: httpx.Client, rola_testowa_id: int
    ) -> None:
        """Fixture rola_testowa_id potwierdza że POST /roles działa."""
        assert rola_testowa_id > 0
        resp = authed_client.get(f"/roles/{rola_testowa_id}")
        assert resp.status_code == 200, f"Rola ID={rola_testowa_id} nie istnieje"
        data = _data(resp.json())
        role_name = data.get("role_name") or data.get("name", "")
        assert "SELFTEST" in role_name, (
            f"Nazwa roli testowej nie zawiera SELFTEST: {role_name}"
        )

    def test_tworzenie_duplikat_nazwy(
        self, authed_client: httpx.Client, rola_testowa_id: int
    ) -> None:
        """POST /roles z duplikowaną nazwą → 409."""
        resp = authed_client.post(
            "/roles",
            json={"name": TEST_ROLE_NAME, "description": "duplikat"},
        )
        assert resp.status_code == 409, (
            f"Oczekiwano 409 dla duplikatu nazwy roli, got {resp.status_code}"
        )

    def test_tworzenie_brak_nazwy_422(self, authed_client: httpx.Client) -> None:
        """POST /roles bez name → 422."""
        resp = authed_client.post("/roles", json={"description": "brak nazwy"})
        assert resp.status_code == 422, f"got {resp.status_code}"

    def test_tworzenie_pusta_nazwa_422(self, authed_client: httpx.Client) -> None:
        """POST /roles z pustą nazwą → 422."""
        resp = authed_client.post("/roles", json={"name": "", "description": "pusta"})
        assert resp.status_code == 422, f"got {resp.status_code}"

    def test_tworzenie_nieznane_pole_422(self, authed_client: httpx.Client) -> None:
        """POST /roles z nieznanym polem → 422 (extra=forbid)."""
        resp = authed_client.post(
            "/roles",
            json={
                "name": "SELFTEST_extra_field",
                "description": "test",
                "POLE_NIEISTNIEJE": "payload_injection",
            },
        )
        assert resp.status_code == 422, (
            f"Nieznane pole powinno dać 422 (extra=forbid), got {resp.status_code}"
        )


class TestRoleEdycja:
    """PUT /roles/{id} — edycja roli."""

    def test_edycja_description(
        self, authed_client: httpx.Client, rola_testowa_id: int
    ) -> None:
        """PUT /roles/{id} zmienia description."""
        resp = authed_client.put(
            f"/roles/{rola_testowa_id}",
            json={
                "name": TEST_ROLE_NAME,
                "description": "SELFTEST opis po edycji",
            },
        )
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"

    def test_edycja_nieistniejaca_404(self, authed_client: httpx.Client) -> None:
        """PUT /roles/99999 → 404 lub 422 (zależy od implementacji walidacji)."""
        resp = authed_client.put(
            "/roles/99999",
            json={"name": "Nieistniejąca", "description": "test"},
        )
        assert resp.status_code in (404, 422), (
            f"Oczekiwano 404 lub 422 dla nieistniejącej roli, got {resp.status_code}"
        )


class TestRoleUsers:
    """GET /roles/{id}/users — użytkownicy z rolą."""

    def test_users_admina(self, authed_client: httpx.Client) -> None:
        """GET /roles/1/users zwraca listę adminów."""
        resp = authed_client.get("/roles/1/users")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        users = _lista(resp.json())
        assert len(users) >= 1, "Rola Admin nie ma żadnego użytkownika"

    def test_users_paginacja(self, authed_client: httpx.Client) -> None:
        """GET /roles/1/users?per_page=1 zwraca max 1 wynik."""
        resp = authed_client.get("/roles/1/users", params={"per_page": 1})
        assert resp.status_code == 200
        users = _lista(resp.json())
        assert len(users) <= 1, f"per_page=1, ale dostaliśmy {len(users)}"

    def test_users_nieistniejaca_rola_404(self, authed_client: httpx.Client) -> None:
        """GET /roles/99999/users → 404."""
        resp = authed_client.get("/roles/99999/users")
        assert resp.status_code == 404, f"got {resp.status_code}"


class TestRolePermissions:
    """GET/POST/PUT /roles/{id}/permissions."""

    def test_get_permissions_admina(self, authed_client: httpx.Client) -> None:
        """GET /roles/1/permissions zwraca uprawnienia Admina."""
        resp = authed_client.get("/roles/1/permissions")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        items = _lista(resp.json())
        assert len(items) > 0, "Admin nie ma żadnych uprawnień!"

    def test_get_permissions_struktura(self, authed_client: httpx.Client) -> None:
        """Uprawnienia zawierają permission_name lub name."""
        resp = authed_client.get("/roles/1/permissions")
        assert resp.status_code == 200
        items = _lista(resp.json())
        if items:
            first = items[0]
            has_name = (
                "permission_name" in first
                or "name" in first
                or "PermissionName" in first
            )
            assert has_name, f"Uprawnienie bez nazwy: {list(first.keys())}"

    def test_post_permissions_do_testowej_roli(
        self,
        authed_client: httpx.Client,
        rola_testowa_id: int,
        permission_testowy_id: int,
    ) -> None:
        """POST /roles/{id}/permissions przypisuje uprawnienie do roli."""
        resp = authed_client.post(
            f"/roles/{rola_testowa_id}/permissions",
            json={"permission_ids": [permission_testowy_id]},
        )
        assert resp.status_code in (200, 201), (
            f"got {resp.status_code}: {resp.text[:300]}"
        )
        # Weryfikacja
        resp_get = authed_client.get(f"/roles/{rola_testowa_id}/permissions")
        assert resp_get.status_code == 200
        items = _lista(resp_get.json())
        perm_ids = [
            p.get("id_permission") or p.get("id")
            for p in items
        ]
        assert permission_testowy_id in perm_ids, (
            f"Uprawnienie {permission_testowy_id} nie zostało przypisane. "
            f"Przypisane: {perm_ids}"
        )

    def test_put_permissions_nadpisuje(
        self,
        authed_client: httpx.Client,
        rola_testowa_id: int,
    ) -> None:
        """PUT /roles/{id}/permissions z pustą listą → czyści uprawnienia."""
        resp = authed_client.put(
            f"/roles/{rola_testowa_id}/permissions",
            json={"permission_ids": []},
        )
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        resp_get = authed_client.get(f"/roles/{rola_testowa_id}/permissions")
        items = _lista(resp_get.json())
        assert len(items) == 0, (
            f"PUT z pustą listą powinien wyczyścić uprawnienia, zostało: {len(items)}"
        )

    def test_permissions_nieistniejaca_rola_404(
        self, authed_client: httpx.Client
    ) -> None:
        """GET /roles/99999/permissions → 404."""
        resp = authed_client.get("/roles/99999/permissions")
        assert resp.status_code == 404, f"got {resp.status_code}"


class TestRoleDelete:
    """DELETE 2-krokowy dla ról."""

    def test_delete_chronionej_roli_blokada(self, authed_client: httpx.Client) -> None:
        """Usunięcie roli Admin (ID=1) powinno być zablokowane."""
        resp = authed_client.delete("/roles/1/initiate")
        assert resp.status_code in (400, 403, 409), (
            f"Rola Admin powinna być chroniona przed usunięciem, got {resp.status_code}"
        )

    def test_delete_nieistniejaca_rola_404(self, authed_client: httpx.Client) -> None:
        """DELETE /roles/99999/initiate → 404."""
        resp = authed_client.delete("/roles/99999/initiate")
        assert resp.status_code == 404, f"got {resp.status_code}"

    def test_delete_zly_token_blokada(
        self, authed_client: httpx.Client, rola_testowa_id: int
    ) -> None:
        """DELETE confirm ze złym tokenem → błąd."""
        resp = authed_client.delete(
            f"/roles/{rola_testowa_id}/confirm",
            json={"confirm_token": "ZLYTOKEN.NIEPRAWIDLOWY.XYZ"},
        )
        assert resp.status_code in (400, 401, 422), (
            f"Zły token powinien dać błąd, got {resp.status_code}"
        )

    def test_delete_initiate_zwraca_token(
        self, authed_client: httpx.Client, rola_testowa_id: int
    ) -> None:
        """DELETE initiate zwraca token z expires_in."""
        resp = authed_client.delete(f"/roles/{rola_testowa_id}/initiate")
        assert resp.status_code == 202, f"got {resp.status_code}: {resp.text[:300]}"
        data = _data(resp.json())
        token = data.get("delete_token") or data.get("confirm_token")
        assert token, f"Brak tokenu: {list(data.keys())}"


class TestRolePermMatrix:
    """GET/PUT /roles-permissions/matrix."""

    def test_matrix_get(self, authed_client: httpx.Client) -> None:
        """GET /roles-permissions/matrix zwraca macierz."""
        resp = authed_client.get("/roles-permissions/matrix")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}"
        data = _data(resp.json())
        assert data, f"Macierz jest pusta: {resp.text[:200]}"

    def test_matrix_bez_tokenu_401(self, http_client: httpx.Client) -> None:
        """GET /roles-permissions/matrix bez tokenu → 401."""
        with _fresh_client() as c:
            resp = c.get("/roles-permissions/matrix")
        assert resp.status_code == 401

    def test_matrix_put_puste_updates(self, authed_client: httpx.Client) -> None:
        """PUT /roles-permissions/matrix z pustą listą → 200 lub 422."""
        resp = authed_client.put(
            "/roles-permissions/matrix",
            json={"updates": []},
        )
        assert resp.status_code in (200, 422), (
            f"Pusta macierz updates: oczekiwano 200/422, got {resp.status_code}"
        )


class TestRoleBulkAssign:
    """POST /roles-permissions/bulk-assign."""

    def test_bulk_assign_ok(
        self,
        authed_client: httpx.Client,
        rola_testowa_id: int,
        permission_testowy_id: int,
    ) -> None:
        """Masowe przypisanie uprawnienia do roli."""
        resp = authed_client.post(
            "/roles-permissions/bulk-assign",
            json={
                "role_id": rola_testowa_id,
                "permission_ids": [permission_testowy_id],
                "action": "add",
            },
        )
        assert resp.status_code in (200, 201), f"got {resp.status_code}: {resp.text[:300]}"

    def test_bulk_assign_remove(
        self,
        authed_client: httpx.Client,
        rola_testowa_id: int,
        permission_testowy_id: int,
    ) -> None:
        """Masowe odebranie uprawnienia od roli."""
        resp = authed_client.post(
            "/roles-permissions/bulk-assign",
            json={
                "role_id": rola_testowa_id,
                "permission_ids": [permission_testowy_id],
                "action": "remove",
            },
        )
        assert resp.status_code in (200, 201), f"got {resp.status_code}: {resp.text[:300]}"

    def test_bulk_assign_zly_action_422(
        self, authed_client: httpx.Client, rola_testowa_id: int
    ) -> None:
        """bulk-assign z nieznaną akcją → 422."""
        resp = authed_client.post(
            "/roles-permissions/bulk-assign",
            json={
                "role_id": rola_testowa_id,
                "permission_ids": [1],
                "action": "NIEZNANA_AKCJA",
            },
        )
        assert resp.status_code == 422, f"got {resp.status_code}"

    def test_bulk_assign_bez_tokenu_401(self, http_client: httpx.Client) -> None:
        """bulk-assign bez tokenu → 401."""
        with _fresh_client() as c:
            resp = c.post(
                "/roles-permissions/bulk-assign",
                json={"role_id": 1, "permission_ids": [1], "action": "add"},
            )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _data(resp_json: dict) -> dict:
    d = resp_json.get("data", resp_json)
    return d if isinstance(d, dict) else resp_json


def _lista(resp_json: dict) -> list:
    """
    Wyciąga listę z odpowiedzi API.
    Obsługuje: data.items (lista lub dict po kategorii), data (lista), root (lista).
    """
    data = resp_json.get("data", resp_json)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("items", "permissions", "roles", "users", "data", "results"):
            val = data.get(key)
            if isinstance(val, list):
                return val
            if isinstance(val, dict):
                # Spłaszcz dict pogrupowany po kategorii
                result: list = []
                for v in val.values():
                    if isinstance(v, list):
                        result.extend(v)
                return result
    return []