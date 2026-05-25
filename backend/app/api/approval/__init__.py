# backend/app/api/approval/__init__.py
"""
Zbiorczy router modulu Obiegu Dokumentow i Akceptacji.

Sciezka katalogu: backend/app/api/approval/
Rejestracja w: backend/app/api/router.py przez _register_router()

_register_router(
    api_router,
    module_path="app.api.approval",
    attr="router",
    prefix="/approval",
    tags=["Approval — Obieg Dokumentow"],
)

Wszystkie endpointy dostepne pod /api/v1/approval/...
Zadny plik routera NIE ma from __future__ import annotations.
"""
from fastapi import APIRouter

from app.api.approval.instances     import router as instances_router
from app.api.approval.groups        import router as groups_router
from app.api.approval.paths         import router as paths_router
from app.api.approval.categories    import router as categories_router
from app.api.approval.filters       import router as filters_router
from app.api.approval.comments      import router as comments_router
from app.api.approval.attachments   import router as attachments_router
from app.api.approval.delegations   import router as delegations_router
from app.api.approval.stats         import router as stats_router
from app.api.approval.notifications import router as notifications_router
from app.api.approval.admin         import router as admin_router
from app.api.approval.sources       import router as sources_router

router = APIRouter()

router.include_router(instances_router)
router.include_router(groups_router)
router.include_router(paths_router)
router.include_router(categories_router)
router.include_router(filters_router)
router.include_router(comments_router)
router.include_router(attachments_router)
router.include_router(delegations_router)
router.include_router(stats_router)
router.include_router(notifications_router)
router.include_router(admin_router)
router.include_router(sources_router)