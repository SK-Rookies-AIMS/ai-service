# app/api/routers/manual.py
from app.ai_manual.service.manual_service import ManualService
from fastapi import APIRouter, HTTPException, Header
from jose import jwt
import os

router = APIRouter(
    prefix="/manual",
    tags=["AI Manual"]
)

service = ManualService()

@router.get("")
def generate_manual(
    authorization: str = Header(...)
):
    token = authorization.replace("Bearer ", "")

    payload = jwt.decode(
        token,
        os.getenv("JWT_SECRET_KEY"),
        algorithms=["HS384"]
    )

    user_id = int(payload["sub"])
    print(payload)

    result = service.generate_manual(user_id)

    if result is None:
        raise HTTPException(
            status_code=404,
            detail="처리할 이벤트가 없습니다."
        )

    return result
