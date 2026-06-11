import logging

from fastapi import FastAPI

from app.api.router import api_router
from app.core.config import settings
from app.core.exceptions import register_exception_handlers
from app.core.logging import configure_logging
from app.repository.sampledb_repository import initialize_sampledb


logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    configure_logging()

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        debug=settings.debug,
    )

    app.include_router(api_router)
    register_exception_handlers(app)

    @app.on_event("startup")
    def initialize_sampledb_schema() -> None:
        """SAMPLE_DATABASE_URL이 설정된 경우 sampledb 엔티티를 생성한"""
        if not settings.sample_database_connection_url:
            return

        try:
            initialize_sampledb(settings.sample_database_connection_url)
        except Exception:
            logger.exception("sampledb 스키마 초기화에 실패했습니다.")

    return app


app = create_app()
