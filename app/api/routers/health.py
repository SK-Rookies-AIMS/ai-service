from fastapi import APIRouter

from app.core.config import settings
from app.dto.response import CommonResponse
from app.utils.response_utils import success_response

router = APIRouter(tags=["health"])


@router.get("/health")
def health_check() -> CommonResponse[dict[str, str | bool]]:
    return success_response(
        data={
            "status": "ok",
            "service": settings.app_name,
            "debug": settings.debug,
        },
        message="헬스 체크 성공",
    )
