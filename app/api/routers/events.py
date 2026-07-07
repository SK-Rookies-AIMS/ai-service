from fastapi import APIRouter

from app.ai_manual.repository.alert_event_repository import AlertEventRepository

router = APIRouter(
    prefix="/events",
    tags=["Events"]
)

repository = AlertEventRepository()


@router.get("")
def get_events():

    return repository.get_events()