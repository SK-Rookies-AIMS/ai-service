import logging

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.utils.response_utils import error_response

logger = logging.getLogger(__name__)


class AppException(Exception):
    """HTTP 상태 코드를 함께 전달하는 애플리케이션 예외"""

    def __init__(
        self,
        message: str,
        *,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
    ) -> None:
        self.message = message
        self.status_code = status_code


def register_exception_handlers(app: FastAPI) -> None:
    """예외를 공통 API 응답으로 변환하는 핸들러를 등록"""

    @app.exception_handler(AppException)
    async def app_exception_handler(
        request: Request,
        exc: AppException,
    ) -> JSONResponse:
        """예상 가능한 애플리케이션 오류를 공통 에러 응답으로 반환"""
        return JSONResponse(
            status_code=exc.status_code,
            content=error_response(
                message=exc.message,
            ).model_dump(exclude_none=True),
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(
        request: Request,
        exc: Exception,
    ) -> JSONResponse:
        """예상하지 못한 서버 오류를 공통 에러 응답으로 반환"""
        logger.exception("처리되지 않은 예외가 발생했습니다: %s", request.url.path)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=error_response(
                message="서버 내부 오류가 발생했습니다.",
            ).model_dump(exclude_none=True),
        )
