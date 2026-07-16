# app/api/routers/manual.py
from app.ai_manual.service.manual_service import ManualService
from fastapi import APIRouter, HTTPException, Header, Request
from jose import jwt
from app.core.config import settings

router = APIRouter(
    prefix="/api/ai/manual",
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
        settings.jwt_secret_key,
        algorithms=[settings.jwt_algorithm]
    )

    print(payload)

    user_id = int(payload["id"])

    result = service.generate_manual(user_id)

    if result is None:
        return {
            "success": True,
            "message": "처리할 이벤트가 없습니다.",
            "data": None
        }

    return result
