# backend/app/api/admin/__init__.py
"""
Zbiorczy router panelu administracyjnego — F6.

Sciezka katalogu: backend/app/api/admin/
Rejestracja w: backend/app/api/router.py przez _register_router()

_register_router(
    api_router,
    module_path="app.api.admin",
    attr="router",
    prefix="",  # podsciezki maja juz swoj prefix (/admin/sources)
    tags=["Admin — Panel Administracyjny"],
)

Wszystkie endpointy dostepne pod /api/v1/admin/...

UWAGA: ten katalog jest oddzielony od app/api/approval/ — zaden plik
tutaj NIE koliduje z istniejacymi endpointami approval (np.
app/api/approval/sources.py obsluguje /approval/sources, ten katalog
obsluguje /admin/sources — różne prefixy, różne przeznaczenie).

Zaden plik routera NIE ma from __future__ import annotations.
"""
from fastapi import APIRouter

from app.api.admin.sources import router as sources_router
from app.api.admin.source_hooks_actions import router as hooks_actions_router

router = APIRouter()

router.include_router(sources_router)
router.include_router(hooks_actions_router)