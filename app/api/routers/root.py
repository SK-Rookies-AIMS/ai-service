from fastapi import APIRouter

from app.core.config import settings
from app.dto.response import CommonResponse
from app.utils.response_utils import success_response

router = APIRouter(tags=["root"])


@router.get("/")
def read_root() -> CommonResponse[dict[str, str]]:
    return success_response(
        data={"version": settings.app_version},
        message=f"{settings.app_name} is running",
    )
