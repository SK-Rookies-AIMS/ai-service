from fastapi import APIRouter

from app.api.routers import health, manufacturing_event, process, root

api_router = APIRouter()
api_router.include_router(root.router)
api_router.include_router(health.router)
api_router.include_router(process.router)
api_router.include_router(manufacturing_event.router)
