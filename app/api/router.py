from fastapi import APIRouter

from app.api.routers import health, process, root

api_router = APIRouter()
api_router.include_router(root.router)
api_router.include_router(health.router)
api_router.include_router(process.router)
