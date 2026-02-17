from fastapi import APIRouter

from .roles import router as roles_router
from .permissions import router as permissions_router
from .system import router as system_router

api_router = APIRouter()
api_router.include_router(roles_router)
api_router.include_router(permissions_router)
api_router.include_router(system_router)
