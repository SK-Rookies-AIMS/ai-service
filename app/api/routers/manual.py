# app/api/routers/manual.py
from app.ai_manual.service.manual_service import ManualService
from fastapi import APIRouter, HTTPException, Header, Request
from jose import jwt
import os

router = APIRouter(
    prefix="/manual",
    tags=["AI Manual"]
)

service = ManualService()

@router.get("")
def generate_manual(request: Request):

    authorization = request.headers.get("authorization")

    if not authorization:
        raise HTTPException(
            status_code=401,
            detail="Authorization header missing"
        )

    token = authorization.replace("Bearer ", "")

    print("TOKEN:", token)

    payload = jwt.decode(
        token,
        os.getenv("JWT_SECRET_KEY"),
        algorithms=["HS384"]
    )

    print(payload)

    user_id = int(payload["id"])

    result = service.generate_manual(user_id)

    if result is None:
        raise HTTPException(
            status_code=404,
            detail="처리할 이벤트가 없습니다."
        )

    return result