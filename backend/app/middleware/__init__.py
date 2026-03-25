"""
Pakiet middleware — System Windykacja
=====================================
Rejestruje i eksportuje wszystkie komponenty middleware backendu.

Kolejność rejestracji w main.py (WAŻNA — od zewnątrz do wewnątrz):
    1. DynamicCORSMiddleware   — musi być PIERWSZY (obsługuje preflight przed auth)
    2. AuditMiddleware         — drugi (loguje każdy request z request_id)

Przykład rejestracji w main.py::

    from app.middleware import AuditMiddleware, DynamicCORSMiddleware

    # Kolejność add_middleware jest odwrócona (Starlette LIFO):
    app.add_middleware(AuditMiddleware)
    app.add_middleware(DynamicCORSMiddleware)
    # Efekt: DynamicCORSMiddleware → AuditMiddleware → handler

"""

from app.middleware.audit_middleware import AuditMiddleware
from app.middleware.cors_middleware import DynamicCORSMiddleware

__all__ = [
    "AuditMiddleware",
    "DynamicCORSMiddleware",
]

__version__ = "1.0.0"