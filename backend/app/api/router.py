"""
Centralny agregator routerów API — System Windykacja.

Wszystkie sub-routery rejestrowane tutaj z prefixem /api/v1/.
main.py importuje wyłącznie `api_router` z tego pliku.

Rejestracja w kolejności priorytetów:
  1. auth          — logowanie, tokeny, OTP, impersonacja
  2. users         — zarządzanie użytkownikami
  3. roles         — zarządzanie rolami
  4. roles_perms   — macierz uprawnień (osobny prefix /roles-permissions)
  5. permissions   — przeglądanie i sprawdzanie uprawnień
  6. debtors       — dłużnicy (WAPRO read-only + operacje)
  7. monits        — historia monitów, retry, PDF
  8. comments      — komentarze do dłużników
  9. snapshots     — snapshoty bazy danych
  10. events       — SSE Server-Sent Events
  11. system       — konfiguracja, health, audit log

"""
from __future__ import annotations

from fastapi import APIRouter

# ─────────────────────────────────────────────────────────────────────────────
# Główny router z prefixem /api/v1/
# ─────────────────────────────────────────────────────────────────────────────

api_router = APIRouter(prefix="/api/v1")

# ─────────────────────────────────────────────────────────────────────────────
# Rejestracja sub-routerów
# Każdy import opakowany w try/except — niezaimplementowane routery
# nie blokują startu aplikacji (przydatne podczas stopniowego wdrażania)
# ─────────────────────────────────────────────────────────────────────────────

def _register_router(
    api_router: APIRouter,
    module_path: str,
    attr: str,
    prefix: str,
    tags: list[str],
) -> None:
    """
    Importuje sub-router i rejestruje go w api_router.
    Przy błędzie importu (moduł nie istnieje) loguje ostrzeżenie — nie crashuje.
    """
    import logging
    logger = logging.getLogger(__name__)

    try:
        import importlib
        module = importlib.import_module(module_path)
        router = getattr(module, attr)
        api_router.include_router(router, prefix=prefix, tags=tags)
    except (ImportError, AttributeError) as exc:
        logger.warning(
            f"Router '{prefix}' niedostępny: {exc} — "
            f"pomiń do czasu implementacji {module_path}"
        )


# 1. AUTH
_register_router(
    api_router,
    module_path="app.api.auth",
    attr="router",
    prefix="/auth",
    tags=["Uwierzytelnianie"],
)

# 2. USERS
_register_router(
    api_router,
    module_path="app.api.users",
    attr="router",
    prefix="/users",
    tags=["Użytkownicy"],
)

# 3. ROLES
_register_router(
    api_router,
    module_path="app.api.roles",
    attr="router",
    prefix="/roles",
    tags=["Role"],
)

_register_router(
    api_router,
    module_path="app.api.roles_permissions",
    attr="roles_router",
    prefix="/roles",
    tags=["Role — Uprawnienia"],
)

# 4. ROLES-PERMISSIONS 
_register_router(
    api_router,
    module_path="app.api.roles_permissions",
    attr="router",
    prefix="/roles-permissions",
    tags=["Role — Uprawnienia"],
)

# 5. PERMISSIONS
_register_router(
    api_router,
    module_path="app.api.permissions",
    attr="router",
    prefix="/permissions",
    tags=["Uprawnienia"],
)

# 6. DEBTORS
_register_router(
    api_router,
    module_path="app.api.debtors",
    attr="router",
    prefix="/debtors",
    tags=["Dłużnicy"],
)

# 7. MONITS 
_register_router(
    api_router,
    module_path="app.api.monits",
    attr="router",
    prefix="/monits",
    tags=["Monity"],
)

# 8. COMMENTS 
_register_router(
    api_router,
    module_path="app.api.comments",
    attr="router",
    prefix="/comments",
    tags=["Komentarze"],
)

# 9. SNAPSHOTS
_register_router(
    api_router,
    module_path="app.api.snapshots",
    attr="router",
    prefix="/snapshots",
    tags=["Snapshoty"],
)

# 10. EVENTS (SSE)
_register_router(
    api_router,
    module_path="app.api.events",
    attr="router",
    prefix="/events",
    tags=["Zdarzenia SSE"],
)

# 11. SYSTEM
_register_router(
    api_router,
    module_path="app.api.system",
    attr="router",
    prefix="/system",
    tags=["System"],
)

# 12. TEMPLATES
_register_router(
    api_router,
    module_path="app.api.templates",
    attr="router",
    prefix="/templates",
    tags=["Szablony"],
)

# 13. FAKTURY — REFERENT
_register_router(
    api_router,
    module_path="app.api.faktury_akceptacja",
    attr="router",
    prefix="/faktury-akceptacja",
    tags=["Faktury — Referent"],
)

# 14. FAKTURY — PRACOWNIK
_register_router(
    api_router,
    module_path="app.api.moje_faktury",
    attr="router",
    prefix="/moje-faktury",
    tags=["Faktury — Pracownik"],
)