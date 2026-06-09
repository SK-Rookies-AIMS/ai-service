from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.utils.response_utils import error_response


class AppException(Exception):
    def __init__(
        self,
        message: str,
        *,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
    ) -> None:
        self.message = message
        self.status_code = status_code


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppException)
    async def app_exception_handler(
        request: Request,
        exc: AppException,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content=error_response(
                message=exc.message,
                data={
                    "path": request.url.path,
                },
            ).model_dump(),
        )
