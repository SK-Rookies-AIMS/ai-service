from typing import Any

from app.dto.response import CommonResponse
from app.utils.datetime_utils import utc_now_iso


def success_response(
    data: Any = None,
    message: str = "요청 성공",
) -> CommonResponse[Any]:
    return CommonResponse(
        success=True,
        data=data,
        message=message,
        timestamp=utc_now_iso(),
    )


def error_response(
    message: str,
    data: Any = None,
) -> CommonResponse[Any]:
    return CommonResponse(
        success=False,
        data=data,
        message=message,
        timestamp=utc_now_iso(),
    )

