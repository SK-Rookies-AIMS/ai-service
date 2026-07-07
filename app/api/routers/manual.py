# app/api/routers/manual.py

from fastapi import APIRouter, HTTPException

from app.ai_manual.service.manual_service import ManualService

router = APIRouter(
    prefix="/manual",
    tags=["AI Manual"]
)

service = ManualService()


@router.get("/{user_id}")
def generate_manual(user_id: int):

    result = service.generate_manual(
        user_id=user_id
    )

    if result is None:
        raise HTTPException(
            status_code=404,
            detail="처리할 이벤트가 없습니다."
        )

    return result

