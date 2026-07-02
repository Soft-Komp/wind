# backend/app/core/endpoint_toggle.py
"""
Dekorator @endpoint_toggle i mechanizm rejestracji wlacznikow endpointow.

UZYCIE (opcjonalne — tylko gdy chcesz nadac czytelna nazwe):

    from app.core.endpoint_toggle import endpoint_toggle

    @router.get("/documents/{id_instance}/status-summary")
    @endpoint_toggle("Podglad statusu dokumentu")
    async def get_status_summary(...):
        ...

Bez dekoratora: endpoint rejestruje sie automatycznie przy pierwszym
wywolaniu przez middleware z kluczem "METHOD:/sciezka".

Dekorator zapisuje label w atrybucie __endpoint_label__ funkcji —
middleware i startup scan uzywaja tego atrybutu.

UWAGA: from __future__ import annotations OK (nie ORM, nie router).
"""

from __future__ import annotations

import logging
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])

# Rejestr in-memory: endpoint_key -> label
# Wypelniany przy starcie przez scan_and_register_routes()
# i przy kazdym wywolaniu przez middleware (lazy)
_LABEL_REGISTRY: dict[str, str] = {}


def endpoint_toggle(label: str) -> Callable[[F], F]:
    """
    Opcjonalny dekorator nadajacy czytelna nazwe endpointowi.

    Args:
        label: Czytelna nazwa wyswietlana w panelu admina,
               np. "Podglad statusu dokumentu".

    Przyklad:
        @router.get("/documents/{id}/status-summary")
        @endpoint_toggle("Podglad statusu dokumentu")
        async def get_status_summary(...):
            ...
    """
    def decorator(fn: F) -> F:
        fn.__endpoint_label__ = label  # type: ignore[attr-defined]
        return fn
    return decorator


def get_label(endpoint_key: str, fn: Any = None) -> str | None:
    """
    Zwraca label dla endpointu. Priorytet:
      1. Cache in-memory (_LABEL_REGISTRY)
      2. Atrybut __endpoint_label__ na funkcji
      3. None (middleware uzyje endpoint_key jako etykiety)
    """
    if endpoint_key in _LABEL_REGISTRY:
        return _LABEL_REGISTRY[endpoint_key]
    if fn is not None:
        label = getattr(fn, "__endpoint_label__", None)
        if label:
            _LABEL_REGISTRY[endpoint_key] = label
            return label
    return None


def scan_and_register_routes(app: Any) -> dict[str, str | None]:
    """
    Skanuje wszystkie trasy FastAPI przy starcie i zwraca slownik
    {endpoint_key: label | None}.

    Wywolywany z on_startup w main.py — zapewnia ze wszystkie endpointy
    sa widoczne w panelu admina od razu, nawet przed pierwszym wywolaniem.

    Args:
        app: Instancja FastAPI.

    Returns:
        Slownik {endpoint_key: label} do uzycia przez endpoint_registry_service
        przy eager registration.
    """
    from fastapi.routing import APIRoute

    result: dict[str, str | None] = {}

    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        path = route.path
        endpoint_fn = route.endpoint

        for method in route.methods or []:
            key = f"{method.upper()}:{path}"
            label = getattr(endpoint_fn, "__endpoint_label__", None)
            if label:
                _LABEL_REGISTRY[key] = label
            result[key] = label

    logger.info(
        "endpoint_toggle.scan: znaleziono %d endpointow (%d z labelem)",
        len(result),
        sum(1 for v in result.values() if v),
    )
    return result