import logging

from fastapi import FastAPI

from app.api.router import api_router
from app.core.config import settings
from app.core.exceptions import register_exception_handlers
from app.core.logging import configure_logging
from app.repository.sampledb_repository import initialize_sampledb
from app.service.manufacturing_event_scheduler import (
    start_manufacturing_event_scheduler,
    stop_manufacturing_event_scheduler,
)
from app.service.manufacturing_event_json_service import resume_incomplete_generation_jobs


logger = logging.getLogger(__name__)

OPENAPI_TAGS = [
    {
        "name": "제조 관제 이벤트",
        "description": (
            "CSV 원천 데이터를 전처리/정제해 제조 관제 이벤트 JSON을 생성하고, "
            "템플릿 replay 또는 실물 테이블 적재 방식으로 조회하는 API입니다."
        ),
    },
]


def create_app() -> FastAPI:
    configure_logging()

    app = FastAPI(
        title=settings.app_name,
        description=(
            "AIMS AI Service API 문서입니다.\n\n"
            "제조 관제 이벤트 API는 프레스, 차체, 도장, 의장 공정 데이터를 기반으로 "
            "원천 이벤트 JSON을 생성하고 조회합니다."
        ),
        version=settings.app_version,
        debug=settings.debug,
        openapi_tags=OPENAPI_TAGS,
    )

    app.include_router(api_router)
    register_exception_handlers(app)

    @app.on_event("startup")
    def initialize_sampledb_schema() -> None:
        """SAMPLE_DATABASE_URL이 설정된 경우 sampledb 엔티티를 생성한다."""
        if not settings.sample_database_connection_url:
            return

        try:
            initialize_sampledb(settings.sample_database_connection_url)
        except Exception:
            logger.exception("sampledb 스키마 초기화에 실패했습니다.")

    @app.on_event("startup")
    async def start_background_schedulers() -> None:
        if settings.sample_database_connection_url:
            try:
                resume_incomplete_generation_jobs(settings.sample_database_connection_url)
            except Exception:
                logger.exception("미완료 제조 이벤트 생성 job 복구에 실패했습니다.")
        start_manufacturing_event_scheduler(app)

    @app.on_event("shutdown")
    async def stop_background_schedulers() -> None:
        await stop_manufacturing_event_scheduler(app)

    return app


app = create_app()
