# app/api/routers/events.py

from fastapi import APIRouter

router = APIRouter(prefix="/events", tags=["events"])

@router.get("")
def get_events():
    return [
        {
            "id": 1,
            "title": "Robot Collision",
            "severity": "위험",
            "area": "BODY",
            "subArea": "Robot 2",
            "equipmentNo": "RB-201",
            "aiScore": 95,
            "status": "조치 필요"
        }
    ]