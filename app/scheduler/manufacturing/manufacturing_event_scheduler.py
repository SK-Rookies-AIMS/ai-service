from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from fastapi import FastAPI

from app.core.config import settings
from app.repository.sampledb_repository import SampleDbRepository
from app.service.manufacturing import (
    DEFAULT_TEMPLATE_NAME,
    ManufacturingEventJsonService,
)


logger = logging.getLogger(__name__)
SEOUL_TZ = ZoneInfo("Asia/Seoul")


def start_manufacturing_event_scheduler(app: FastAPI) -> None:
    """제조 이벤트 템플릿 기반 다음날 데이터 적재 스케줄러를 시작한다."""
    if not settings.manufacturing_event_scheduler_enabled:
        return
    if not settings.sample_database_connection_url:
        logger.info(
            "SAMPLE_DATABASE_URL이 없어 제조 이벤트 스케줄러를 시작하지 않습니다.",
        )
        return

    task = asyncio.create_task(_run_daily_generation_loop())
    app.state.manufacturing_event_scheduler_task = task


async def stop_manufacturing_event_scheduler(app: FastAPI) -> None:
    task = getattr(app.state, "manufacturing_event_scheduler_task", None)
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def _run_daily_generation_loop() -> None:
    while True:
        await asyncio.sleep(_seconds_until_next_run())
        try:
            await asyncio.to_thread(_materialize_tomorrow_events)
        except Exception:
            logger.exception(
                "제조 이벤트 다음날 데이터 적재 스케줄러 실행에 실패했습니다.",
            )


def _materialize_tomorrow_events() -> dict[str, object] | None:
    if not settings.sample_database_connection_url:
        return None

    service = ManufacturingEventJsonService(
        SampleDbRepository(settings.sample_database_connection_url),
    )
    result = service.generate_tomorrow(
        template_name=DEFAULT_TEMPLATE_NAME,
        events_per_day=settings.manufacturing_event_scheduler_events_per_day,
        car_pool_size=settings.manufacturing_event_car_pool_size,
        insert_chunk_size=settings.manufacturing_event_insert_chunk_size,
        update_existing=True,
    )
    logger.info(
        "제조 이벤트 다음날 데이터 적재 완료: %s",
        {
            "templateName": result["templateName"],
            "targetDate": result["targetDate"],
            "generatedCount": result["generatedCount"],
            "storedCountInDate": result["storedCountInDate"],
        },
    )
    return result


def _seconds_until_next_run() -> float:
    now = datetime.now(SEOUL_TZ)
    next_run = datetime.combine(now.date(), time(hour=17), tzinfo=SEOUL_TZ)
    if next_run <= now:
        next_run += timedelta(days=1)
    return max(60.0, (next_run - now).total_seconds())
