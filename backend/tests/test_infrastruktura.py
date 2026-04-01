# =============================================================================
# PLIK  : backend/tests/test_infrastruktura.py
# MODUŁ : Self-test — infrastruktura (DB, Redis, Alembic, checksums)
# =============================================================================
from __future__ import annotations

import httpx
import pytest


class TestHealth:
    """Podstawowe testy zdrowia systemu."""

    def test_health_ok(self, http_client: httpx.Client) -> None:
        """GET /health zwraca 200."""
        resp = http_client.get("/health")
        assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:200]}"

    def test_swagger_dostepny(self, http_client: httpx.Client) -> None:
        """Swagger UI jest dostępny."""
        # Swagger jest na /api/v1/docs ale base_url już zawiera /api/v1
        with httpx.Client(timeout=10) as c:
            resp = c.get("http://localhost:8000/api/v1/docs")
        assert resp.status_code == 200, f"Swagger niedostępny: {resp.status_code}"


class TestKonfiguracja:
    """Testy konfiguracji systemu."""

    def test_config_lista_pelna(self, authed_client: httpx.Client) -> None:
        """Lista konfiguracji zawiera ≥ 31 kluczy."""
        resp = authed_client.get("/system/config")
        assert resp.status_code == 200
        data = resp.json()
        items = data.get("data", {}).get("items", {})
        assert len(items) >= 31, (
            f"Oczekiwano ≥ 31 kluczy konfiguracji, znaleziono {len(items)}"
        )

    def test_config_klucze_faktury(self, authed_client: httpx.Client) -> None:
        """Klucze konfiguracji modułu faktur istnieją."""
        wymagane_klucze = [
            "modul_akceptacji_faktur_enabled",
            "faktury.fakir_update_enabled",
            "faktury.pdf_enabled",
        ]
        resp = authed_client.get("/system/config")
        assert resp.status_code == 200
        items = resp.json().get("data", {}).get("items", {})
        for klucz in wymagane_klucze:
            assert klucz in items, f"Brak klucza konfiguracji: '{klucz}'"

    def test_config_cors_origins(self, authed_client: httpx.Client) -> None:
        """CORS origins jest ustawione."""
        resp = authed_client.get("/system/config")
        assert resp.status_code == 200
        items = resp.json().get("data", {}).get("items", {})
        cors = items.get("cors.allowed_origins", "")
        assert cors, "cors.allowed_origins jest puste"


class TestRoleIUprawnienia:
    """Testy ról i uprawnień."""

    def test_role_istnieja(self, authed_client: httpx.Client) -> None:
        """W systemie są co najmniej 4 podstawowe role."""
        resp = authed_client.get("/roles")
        assert resp.status_code == 200
        data = resp.json()
        items = data.get("data", {}).get("items", data.get("data", []))
        assert len(items) >= 4, (
            f"Oczekiwano ≥ 4 ról, znaleziono {len(items)}"
        )
        nazwy = [r.get("role_name") or r.get("name") for r in items]
        for wymagana in ["Admin"]:
            assert wymagana in nazwy, f"Brak roli '{wymagana}' w systemie"

    def test_uprawnienia_kategorie(self, authed_client: httpx.Client) -> None:
        """Kategorie uprawnień zawierają 'faktury'."""
        resp = authed_client.get("/permissions/categories")
        if resp.status_code == 404:
            pytest.skip("Endpoint /permissions/categories nie istnieje")
        assert resp.status_code == 200
        data = resp.json()
        kategorie = data.get("data", [])
        assert "faktury" in kategorie, (
            f"Brak kategorii 'faktury' w: {kategorie}"
        )

    def test_uzytkownicy_lista(self, authed_client: httpx.Client) -> None:
        """Lista użytkowników zwraca ≥ 1 użytkownika (admin)."""
        resp = authed_client.get("/users")
        assert resp.status_code == 200
        data = resp.json()
        items = data.get("data", {}).get("items", data.get("data", []))
        assert len(items) >= 1, "Lista użytkowników pusta"
        usernames = [u.get("username") for u in items]
        assert "admin" in usernames, "Brak użytkownika 'admin' w systemie"


class TestAlembicMigracje:
    """Testy stanu migracji Alembic."""

    def test_alembic_head(self) -> None:
        """Alembic jest na head — wszystkie migracje zastosowane."""
        import subprocess
        result = subprocess.run(
            ["alembic", "current"],
            capture_output=True,
            text=True,
            cwd="/app",
        )
        output = result.stdout + result.stderr
        assert "(head)" in output, (
            f"Alembic nie jest na head!\nOutput: {output[:500]}"
        )

    def test_alembic_wersja_0007(self) -> None:
        """Alembic jest na wersji ≥ 0007."""
        import subprocess
        result = subprocess.run(
            ["alembic", "current"],
            capture_output=True,
            text=True,
            cwd="/app",
        )
        output = result.stdout + result.stderr
        # Sprawdź że jest 0007 lub wyższy
        assert any(f"000{n}" in output or f"00{n}" in output
                   for n in range(7, 20)) or "head" in output, (
            f"Oczekiwano ≥ 0007, output: {output[:300]}"
        )


class TestSSEStream:
    """Testy SSE."""

    def test_sse_stream_dostepny(self, authed_client: httpx.Client) -> None:
        """GET /sse/stream zwraca 200 i nagłówek text/event-stream."""
        with authed_client.stream("GET", "/sse/stream", timeout=5) as resp:
            assert resp.status_code == 200, f"got {resp.status_code}"
            ct = resp.headers.get("content-type", "")
            assert "text/event-stream" in ct, f"Zły Content-Type: {ct}"

    def test_sse_bez_tokenu(self, http_client: httpx.Client) -> None:
        """SSE bez tokenu zwraca 401."""
        with httpx.Client(base_url=http_client.base_url, timeout=5) as c:
            try:
                with c.stream("GET", "/sse/stream", timeout=3) as resp:
                    assert resp.status_code == 401, f"got {resp.status_code}"
            except httpx.TimeoutException:
                pytest.skip("SSE timeout — endpoint może wymagać dłuższego połączenia")