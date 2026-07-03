from fastapi import APIRouter

from app.api.routers import (
    defect_transfer,
    health,
    manufacturing_event,
    ml_dataset,
    process,
    process_analysis_ws,
    root,
    manual,
)

api_router = APIRouter()
api_router.include_router(root.router)
api_router.include_router(health.router)
api_router.include_router(process.router)
api_router.include_router(defect_transfer.router)
api_router.include_router(process_analysis_ws.router)
api_router.include_router(manufacturing_event.router)
api_router.include_router(ml_dataset.router)
api_router.include_router(manual.router)
