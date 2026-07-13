from __future__ import annotations

import copy
import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta
from typing import Any, Callable
from uuid import uuid4

from fastapi import status

from app.core.config import settings
from app.core.exceptions import AppException
from app.data_generation.manufacturing_event_json_builder import (
    ABNORMAL_RATIO,
    EventBuildRequest,
    ManufacturingEventJsonBuilder,
    initial_dispatch_status,
    is_abnormal_operation_status,
    normalize_event_json,
    validate_process_data,
)
from app.repository.sampledb_repository import SampleDbRepository


# 실제 생성 수량은 vehicle_id의 생산일자에 해당하는 car_master 수로 결정한다.
DEFAULT_EVENTS_PER_DAY: int | None = None
DEFAULT_CAR_POOL_SIZE: int | None = None
DEFAULT_INSERT_CHUNK_SIZE = 1_000
DEFAULT_TEMPLATE_NAME = "default"
TEMPLATE_ANCHOR_DATE = date(2000, 1, 1)
ProgressCallback = Callable[[dict[str, Any]], None]
JOB_TYPE_GENERATE_RANGE = "GENERATE_RANGE"
JOB_TYPE_GENERATE_TOMORROW = "GENERATE_TOMORROW"
JOB_TYPE_GENERATE_TEMPLATE = "GENERATE_TEMPLATE"
_generation_job_executor = ThreadPoolExecutor(
    # 대량 생성 job끼리 DB insert 부하가 겹치지 않도록 단일 worker로 직렬 처리한다.
    max_workers=1,
    thread_name_prefix="manufacturing-event-generation-job",
)
logger = logging.getLogger(__name__)


class ManufacturingEventJsonService:
    """CSV 원천 데이터를 통합 제조 이벤트 JSON으로 생성하고 sampledb에 저장한다."""

    def __init__(
        self,
        repository: SampleDbRepository,
        builder: ManufacturingEventJsonBuilder | None = None,
    ) -> None:
        self.repository = repository
        self.builder = builder or ManufacturingEventJsonBuilder()

    def generate_range(
        self,
        *,
        start_date: date,
        end_date: date,
        events_per_day: int | None = DEFAULT_EVENTS_PER_DAY,
        car_pool_size: int | None = DEFAULT_CAR_POOL_SIZE,
        insert_chunk_size: int = DEFAULT_INSERT_CHUNK_SIZE,
        update_existing: bool = True,
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        """생산일자별 car_master 차량 전체에 4공정 이벤트를 생성한다."""
        if start_date > end_date:
            raise AppException(
                "start_date는 end_date보다 이후일 수 없습니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        vehicle_limit = _resolve_vehicle_limit(events_per_day, car_pool_size)
        if insert_chunk_size < 1:
            raise AppException(
                "insert_chunk_size는 1 이상이어야 합니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        # 생성 전에 PRD 스키마와 공정별 기본 설비 5개를 보장한다.
        self.repository.schema.ensure_schema()
        self.repository.equipment.seed_defaults()
        equipment_map = self.repository.equipment.get_map()
        production_dates = list(_date_range(start_date, end_date))
        daily_car_maps = {
            production_date: self.repository.cars.get_map_by_production_date(
                production_date,
                limit=vehicle_limit,
            )
            for production_date in production_dates
        }
        total_vehicle_count = sum(len(car_map) for car_map in daily_car_maps.values())
        abnormal_vehicle_count = sum(
            round(len(car_map) * ABNORMAL_RATIO)
            for car_map in daily_car_maps.values()
        )
        normal_vehicle_count = total_vehicle_count - abnormal_vehicle_count
        total_events = total_vehicle_count * 4
        affected_rows = 0
        generated_count = 0
        distribution = {"PRESS": 0, "BODY": 0, "PAINT": 0, "ASSEMBLY": 0}
        abnormal_distribution = {"PRESS": 0, "BODY": 0, "PAINT": 0, "ASSEMBLY": 0}
        # 날짜별 차량 수가 달라도 공통 chunk 단위로 이어서 저장한다.
        chunk: list[dict[str, Any]] = []
        for production_date, car_id_map in daily_car_maps.items():
            daily_event_count = len(car_id_map) * 4
            request = EventBuildRequest(
                start_date=production_date,
                end_date=production_date,
                events_per_day=daily_event_count,
                car_id_map=car_id_map,
                equipment_map=equipment_map,
            )
            for row in self.builder.iter_rows(request):
                chunk.append(row)
                generated_count += 1
                distribution[str(row["process_code"])] += 1
                if is_abnormal_operation_status(
                    row["event_json"]["equipmentStatus"]["operationStatus"],
                ):
                    abnormal_distribution[str(row["process_code"])] += 1
                if len(chunk) >= insert_chunk_size:
                    affected_rows += self.repository.events.insert_rows(
                        chunk,
                        update_existing=update_existing,
                    )
                    chunk = []
                    if progress_callback:
                        progress_callback(
                            {
                                "generatedCount": generated_count,
                                "affectedRows": affected_rows,
                                "totalExpectedEvents": total_events,
                            },
                        )
        if chunk:
            affected_rows += self.repository.events.insert_rows(
                chunk,
                update_existing=update_existing,
            )
            if progress_callback:
                progress_callback(
                    {
                        "generatedCount": generated_count,
                        "affectedRows": affected_rows,
                        "totalExpectedEvents": total_events,
                    },
                )

        return {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "requestedEventCountPerDay": events_per_day,
            "totalExpectedEvents": total_events,
            "generatedCount": generated_count,
            "affectedRows": affected_rows,
            # event_time을 NULL로 저장하므로 현재 작업에서 생성한 건수를 반환한다.
            "storedCountInRange": generated_count,
            "carPoolSize": total_vehicle_count,
            "normalVehicleCount": normal_vehicle_count,
            "discardVehicleCount": abnormal_vehicle_count,
            "dailyVehicleCounts": {
                production_date.isoformat(): len(car_id_map)
                for production_date, car_id_map in daily_car_maps.items()
            },
            "insertChunkSize": insert_chunk_size,
            "processDistribution": distribution,
            "abnormalDistribution": abnormal_distribution,
            "normalEventCount": generated_count - sum(abnormal_distribution.values()),
            "abnormalEventCount": sum(abnormal_distribution.values()),
        }

    def enqueue_generate_range_job(
        self,
        *,
        start_date: date,
        end_date: date,
        events_per_day: int | None = DEFAULT_EVENTS_PER_DAY,
        car_pool_size: int | None = DEFAULT_CAR_POOL_SIZE,
        insert_chunk_size: int = DEFAULT_INSERT_CHUNK_SIZE,
    ) -> dict[str, Any]:
        """기간 생성 요청을 DB job으로 등록하고 background worker에 전달한다."""
        if start_date > end_date:
            raise AppException(
                "start_date는 end_date보다 이후일 수 없습니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        vehicle_limit = _resolve_vehicle_limit(events_per_day, car_pool_size)
        if insert_chunk_size < 1:
            raise AppException(
                "insert_chunk_size는 1 이상이어야 합니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        request_json = {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "eventCount": events_per_day,
            "carPoolSize": car_pool_size,
            "insertChunkSize": insert_chunk_size,
        }
        total_expected_events = sum(
            _selected_vehicle_count(
                self.repository.cars.count_by_production_date(production_date),
                vehicle_limit,
                production_date,
            )
            for production_date in _date_range(start_date, end_date)
        ) * 4
        if total_expected_events < 1:
            raise AppException(
                "요청 날짜에 해당하는 car_master 차량이 없습니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        return self._create_and_submit_generation_job(
            job_type=JOB_TYPE_GENERATE_RANGE,
            request_json=request_json,
            total_expected_events=total_expected_events,
        )

    def generate_template(
        self,
        *,
        template_name: str = DEFAULT_TEMPLATE_NAME,
        production_date: date = date(2026, 6, 1),
        event_count: int | None = None,
        car_pool_size: int | None = None,
        insert_chunk_size: int = DEFAULT_INSERT_CHUNK_SIZE,
        replace: bool = False,
        update_existing: bool = False,
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        """날짜를 제외한 하루 기준 제조 이벤트 패턴을 템플릿으로 저장한다."""
        vehicle_limit = _resolve_vehicle_limit(event_count, car_pool_size)
        if insert_chunk_size < 1:
            raise AppException(
                "insert_chunk_size는 1 이상이어야 합니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        normalized_template_name = _normalize_template_name(template_name)
        self.repository.schema.ensure_schema()
        self.repository.equipment.seed_defaults()
        if replace:
            self.repository.templates.delete(normalized_template_name)

        # 템플릿도 production_date와 vehicle_id가 일치하는 실제 차량 PK만 참조한다.
        car_id_map = self.repository.cars.get_map_by_production_date(
            production_date,
            limit=vehicle_limit,
        )
        resolved_event_count = len(car_id_map) * 4
        equipment_map = self.repository.equipment.get_map()
        request = EventBuildRequest(
            start_date=TEMPLATE_ANCHOR_DATE,
            end_date=TEMPLATE_ANCHOR_DATE,
            events_per_day=resolved_event_count,
            car_id_map=car_id_map,
            equipment_map=equipment_map,
        )

        affected_rows = 0
        generated_count = 0
        distribution = {"PRESS": 0, "BODY": 0, "PAINT": 0, "ASSEMBLY": 0}
        abnormal_distribution = {"PRESS": 0, "BODY": 0, "PAINT": 0, "ASSEMBLY": 0}
        chunk: list[dict[str, Any]] = []
        # 실제 날짜 대신 고정 기준일과 하루 시작 기준 offset을 저장한다.
        # 이후 어느 날짜에도 동일한 생산 패턴을 재사용할 수 있다.
        anchor_datetime = datetime.combine(TEMPLATE_ANCHOR_DATE, time.min)
        for row in self.builder.iter_rows(request):
            generated_count += 1
            distribution[str(row["process_code"])] += 1
            if is_abnormal_operation_status(
                row["event_json"]["equipmentStatus"]["operationStatus"],
            ):
                abnormal_distribution[str(row["process_code"])] += 1
            template_event_id = (
                f"TMPL-{normalized_template_name.upper()}-{generated_count:06d}"
            )
            event_json = copy.deepcopy(row["event_json"])
            event_json["event"]["eventId"] = template_event_id
            event_offset_us = int(
                (row["event_time"] - anchor_datetime).total_seconds() * 1_000_000,
            )
            chunk.append(
                {
                    "template_name": normalized_template_name,
                    "template_event_id": template_event_id,
                    "event_offset_us": event_offset_us,
                    "car_master_id": row["car_master_id"],
                    "equipment_id": row["equipment_id"],
                    "process_code": row["process_code"],
                    "station_code": row["station_code"],
                    "equipment_code": row["equipment_code"],
                    "equipment_type": row["equipment_type"],
                    "equipment_status": row["equipment_status"],
                    "event_type": row["event_type"],
                    "event_json": event_json,
                },
            )
            if len(chunk) >= insert_chunk_size:
                affected_rows += self.repository.templates.insert_rows(
                    chunk,
                    update_existing=update_existing,
                )
                chunk = []
                if progress_callback:
                    progress_callback(
                        {
                            "generatedCount": generated_count,
                            "affectedRows": affected_rows,
                            "totalExpectedEvents": resolved_event_count,
                        },
                    )
        if chunk:
            affected_rows += self.repository.templates.insert_rows(
                chunk,
                update_existing=update_existing,
            )
            if progress_callback:
                progress_callback(
                    {
                        "generatedCount": generated_count,
                        "affectedRows": affected_rows,
                        "totalExpectedEvents": resolved_event_count,
                    },
                )

        stored_count = self.repository.templates.count(normalized_template_name)
        return {
            "templateName": normalized_template_name,
            "productionDate": production_date.isoformat(),
            "eventCount": resolved_event_count,
            "generatedCount": generated_count,
            "affectedRows": affected_rows,
            "storedCount": stored_count,
            "carPoolSize": len(car_id_map),
            "insertChunkSize": insert_chunk_size,
            "processDistribution": distribution,
            "abnormalDistribution": abnormal_distribution,
            "normalEventCount": generated_count - sum(abnormal_distribution.values()),
            "abnormalEventCount": sum(abnormal_distribution.values()),
            "timeDistribution": [
                {"range": "00:00-05:00", "density": "LOW"},
                {"range": "05:00-08:00", "density": "MEDIUM"},
                {"range": "08:00-12:00", "density": "HIGH"},
                {"range": "12:00-13:00", "density": "LOW"},
                {"range": "13:00-18:00", "density": "HIGH"},
                {"range": "18:00-22:00", "density": "MEDIUM"},
                {"range": "22:00-24:00", "density": "LOW"},
            ],
        }

    def enqueue_generate_template_job(
        self,
        *,
        template_name: str = DEFAULT_TEMPLATE_NAME,
        production_date: date = date(2026, 6, 1),
        event_count: int | None = None,
        car_pool_size: int | None = None,
        insert_chunk_size: int = DEFAULT_INSERT_CHUNK_SIZE,
        replace: bool = False,
    ) -> dict[str, Any]:
        vehicle_limit = _resolve_vehicle_limit(event_count, car_pool_size)
        if insert_chunk_size < 1:
            raise AppException(
                "insert_chunk_size는 1 이상이어야 합니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        available_vehicle_count = self.repository.cars.count_by_production_date(
            production_date,
        )
        selected_vehicle_count = _selected_vehicle_count(
            available_vehicle_count,
            vehicle_limit,
            production_date,
        )
        request_json = {
            "templateName": template_name,
            "productionDate": production_date.isoformat(),
            "eventCount": event_count,
            "carPoolSize": car_pool_size,
            "insertChunkSize": insert_chunk_size,
            "replace": replace,
        }
        return self._create_and_submit_generation_job(
            job_type=JOB_TYPE_GENERATE_TEMPLATE,
            request_json=request_json,
            total_expected_events=selected_vehicle_count * 4,
        )

    def list_template_events(
        self,
        *,
        template_name: str = DEFAULT_TEMPLATE_NAME,
        limit: int,
        offset: int = 0,
        process_code: str | None = None,
    ) -> list[dict[str, Any]]:
        return self.repository.templates.list_rows(
            template_name=_normalize_template_name(template_name),
            limit=limit,
            offset=offset,
            process_code=process_code,
        )

    def replay_template_events(
        self,
        *,
        template_name: str = DEFAULT_TEMPLATE_NAME,
        target_date: date,
        limit: int,
        offset: int = 0,
        process_code: str | None = None,
    ) -> list[dict[str, Any]]:
        template_rows = self.list_template_events(
            template_name=template_name,
            limit=limit,
            offset=offset,
            process_code=process_code,
        )
        return [
            _materialize_template_row(row, target_date=target_date)
            for row in template_rows
        ]

    def materialize_template_events(
        self,
        *,
        template_name: str = DEFAULT_TEMPLATE_NAME,
        target_date: date,
        insert_chunk_size: int = DEFAULT_INSERT_CHUNK_SIZE,
        update_existing: bool = True,
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        """저장된 템플릿에 목표 날짜와 결정적 변동을 입혀 실제 이벤트로 적재한다."""
        if insert_chunk_size < 1:
            raise AppException(
                "insert_chunk_size는 1 이상이어야 합니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        normalized_template_name = _normalize_template_name(template_name)
        self.repository.schema.ensure_schema()
        total_template_events = self.repository.templates.count(
            normalized_template_name,
        )
        if total_template_events < 1:
            raise AppException(
                "저장된 제조 이벤트 템플릿이 없습니다. 먼저 템플릿을 생성해주세요.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        affected_rows = 0
        generated_count = 0
        distribution = {"PRESS": 0, "BODY": 0, "PAINT": 0, "ASSEMBLY": 0}
        abnormal_distribution = {"PRESS": 0, "BODY": 0, "PAINT": 0, "ASSEMBLY": 0}
        # 템플릿 역시 페이지 단위로 읽어 대량 materialize 시 메모리 사용량을 제한한다.
        offset = 0
        while offset < total_template_events:
            template_rows = self.repository.templates.list_rows(
                template_name=normalized_template_name,
                limit=insert_chunk_size,
                offset=offset,
            )
            if not template_rows:
                break

            materialized_rows = [
                _materialize_template_row(row, target_date=target_date)
                for row in template_rows
            ]
            affected_rows += self.repository.events.insert_rows(
                materialized_rows,
                update_existing=update_existing,
            )
            for row in materialized_rows:
                distribution[str(row["process_code"])] += 1
                if is_abnormal_operation_status(
                    row["event_json"]["equipmentStatus"]["operationStatus"],
                ):
                    abnormal_distribution[str(row["process_code"])] += 1

            generated_count += len(materialized_rows)
            offset += len(template_rows)
            if progress_callback:
                progress_callback(
                    {
                        "generatedCount": generated_count,
                        "affectedRows": affected_rows,
                        "totalExpectedEvents": total_template_events,
                    },
                )

        return {
            "templateName": normalized_template_name,
            "targetDate": target_date.isoformat(),
            "templateEventCount": total_template_events,
            "generatedCount": generated_count,
            "affectedRows": affected_rows,
            # event_time을 NULL로 저장하므로 현재 materialize한 건수를 반환한다.
            "storedCountInDate": generated_count,
            "insertChunkSize": insert_chunk_size,
            "updateExisting": update_existing,
            "processDistribution": distribution,
            "abnormalDistribution": abnormal_distribution,
            "normalEventCount": generated_count - sum(abnormal_distribution.values()),
            "abnormalEventCount": sum(abnormal_distribution.values()),
            "variationMode": "target_date_and_template_event_id_seed",
        }

    def ensure_template(
        self,
        *,
        template_name: str = DEFAULT_TEMPLATE_NAME,
        production_date: date = date(2026, 6, 1),
        event_count: int | None = None,
        car_pool_size: int | None = None,
        insert_chunk_size: int = DEFAULT_INSERT_CHUNK_SIZE,
    ) -> dict[str, Any]:
        """요청 수량과 정확히 일치하는 템플릿이 없으면 전체를 다시 생성한다."""
        normalized_template_name = _normalize_template_name(template_name)
        self.repository.schema.ensure_schema()
        vehicle_limit = _resolve_vehicle_limit(event_count, car_pool_size)
        available_vehicle_count = self.repository.cars.count_by_production_date(
            production_date,
        )
        selected_vehicle_count = _selected_vehicle_count(
            available_vehicle_count,
            vehicle_limit,
            production_date,
        )
        resolved_event_count = selected_vehicle_count * 4
        stored_count = self.repository.templates.count(normalized_template_name)
        # 차량당 4공정 보장이 중요하므로 "이상"이 아니라 정확히 같은 건수만 재사용한다.
        if stored_count == resolved_event_count:
            return {
                "templateName": normalized_template_name,
                "eventCount": resolved_event_count,
                "storedCount": stored_count,
                "created": False,
            }

        result = self.generate_template(
            template_name=normalized_template_name,
            production_date=production_date,
            event_count=event_count,
            car_pool_size=car_pool_size,
            insert_chunk_size=insert_chunk_size,
            replace=stored_count > 0,
            update_existing=stored_count > 0,
        )
        return {
            **result,
            "created": True,
        }

    def generate_initial_demo_range(
        self,
        *,
        events_per_day: int | None = DEFAULT_EVENTS_PER_DAY,
        car_pool_size: int | None = DEFAULT_CAR_POOL_SIZE,
        insert_chunk_size: int = DEFAULT_INSERT_CHUNK_SIZE,
        update_existing: bool = True,
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        return self.generate_range(
            start_date=date(2026, 6, 1),
            end_date=date(2026, 6, 16),
            events_per_day=events_per_day,
            car_pool_size=car_pool_size,
            insert_chunk_size=insert_chunk_size,
            update_existing=update_existing,
            progress_callback=progress_callback,
        )

    def generate_tomorrow(
        self,
        *,
        base_date: date | None = None,
        template_name: str = DEFAULT_TEMPLATE_NAME,
        events_per_day: int | None = DEFAULT_EVENTS_PER_DAY,
        car_pool_size: int | None = DEFAULT_CAR_POOL_SIZE,
        insert_chunk_size: int = DEFAULT_INSERT_CHUNK_SIZE,
        update_existing: bool = True,
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        """기준일 다음날 vehicle_id 생산일자의 차량 전체로 이벤트를 생성한다."""
        target_date = (base_date or date.today()) + timedelta(days=1)
        result = self.generate_range(
            start_date=target_date,
            end_date=target_date,
            events_per_day=events_per_day,
            car_pool_size=car_pool_size,
            insert_chunk_size=insert_chunk_size,
            update_existing=update_existing,
            progress_callback=progress_callback,
        )
        return {
            **result,
            "baseDate": (base_date or date.today()).isoformat(),
            "targetDate": target_date.isoformat(),
            "templateName": template_name,
            "generationMode": "CAR_MASTER_PRODUCTION_DATE",
        }

    def enqueue_generate_tomorrow_job(
        self,
        *,
        base_date: date | None = None,
        template_name: str = DEFAULT_TEMPLATE_NAME,
        events_per_day: int | None = DEFAULT_EVENTS_PER_DAY,
        car_pool_size: int | None = DEFAULT_CAR_POOL_SIZE,
        insert_chunk_size: int = DEFAULT_INSERT_CHUNK_SIZE,
    ) -> dict[str, Any]:
        vehicle_limit = _resolve_vehicle_limit(events_per_day, car_pool_size)
        if insert_chunk_size < 1:
            raise AppException(
                "insert_chunk_size는 1 이상이어야 합니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        request_json = {
            "baseDate": base_date.isoformat() if base_date else None,
            "templateName": template_name,
            "eventCount": events_per_day,
            "carPoolSize": car_pool_size,
            "insertChunkSize": insert_chunk_size,
        }
        target_date = (base_date or date.today()) + timedelta(days=1)
        available_vehicle_count = self.repository.cars.count_by_production_date(
            target_date,
        )
        selected_vehicle_count = _selected_vehicle_count(
            available_vehicle_count,
            vehicle_limit,
            target_date,
        )
        return self._create_and_submit_generation_job(
            job_type=JOB_TYPE_GENERATE_TOMORROW,
            request_json=request_json,
            total_expected_events=selected_vehicle_count * 4,
        )

    def get_generation_job(self, job_id: str) -> dict[str, Any]:
        self.repository.schema.ensure_schema()
        job = self.repository.jobs.get(job_id)
        if not job:
            raise AppException(
                "제조 이벤트 생성 job을 찾을 수 없습니다.",
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return job

    def _create_and_submit_generation_job(
        self,
        *,
        job_type: str,
        request_json: dict[str, Any],
        total_expected_events: int,
    ) -> dict[str, Any]:
        """job 상태 row를 먼저 남긴 뒤 단일 background worker에 실행을 위임한다."""
        self.repository.schema.ensure_schema()
        job_id = str(uuid4())
        job = self.repository.jobs.create(
            job_id=job_id,
            job_type=job_type,
            request_json=request_json,
            total_expected_events=total_expected_events,
        )
        _generation_job_executor.submit(
            _run_generation_job,
            self.repository.database_url,
            job_id,
        )
        return job

    def list_events(
        self,
        *,
        limit: int,
        offset: int,
        start_date: date | None = None,
        end_date: date | None = None,
        process_code: str | None = None,
        is_sent: bool | None = None,
    ) -> list[dict[str, Any]]:
        return self.repository.events.list_rows(
            limit=limit,
            offset=offset,
            start_date=start_date,
            end_date=end_date,
            process_code=process_code,
            is_sent=is_sent,
        )

def get_manufacturing_event_json_service() -> ManufacturingEventJsonService:
    if not settings.sample_database_connection_url:
        raise AppException(
            "MAIN_DATABASE_URL + SAMPLE_DB_NAME 설정이 필요합니다.",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    return ManufacturingEventJsonService(
        SampleDbRepository(settings.sample_database_connection_url),
    )


def _run_generation_job(database_url: str, job_id: str) -> None:
    """비동기 생성 job의 RUNNING/SUCCEEDED/FAILED 상태 전이를 관리한다."""
    repository = SampleDbRepository(database_url)
    service = ManufacturingEventJsonService(repository)
    # uvicorn --reload의 이전/신규 프로세스나 다중 worker가 같은 미완료 job을
    # 동시에 복구하지 못하도록 DB advisory lock으로 생성 작업 전체를 직렬화한다.
    with repository.jobs.execution_lock() as acquired:
        if not acquired:
            logger.error("제조 이벤트 생성 전역 잠금을 획득하지 못했습니다: job_id=%s", job_id)
            return

        # 잠금을 기다리는 동안 다른 프로세스가 완료했을 수 있으므로 상태를 다시 읽는다.
        job = repository.jobs.get(job_id)
        if not job:
            logger.warning("제조 이벤트 생성 job을 찾을 수 없습니다: job_id=%s", job_id)
            return
        if job["status"] in {"SUCCEEDED", "FAILED"}:
            logger.info(
                "이미 종료된 제조 이벤트 생성 job을 건너뜁니다: job_id=%s status=%s",
                job_id,
                job["status"],
            )
            return

        try:
            repository.jobs.mark_running(job_id)
            result = _execute_generation_job(service, repository, job)
            repository.jobs.mark_succeeded(job_id, result)
            logger.info(
                "제조 이벤트 생성 job 완료: job_id=%s job_type=%s generated=%s affected=%s",
                job_id,
                job["jobType"],
                result.get("generatedCount"),
                result.get("affectedRows"),
            )
        except Exception as exc:
            message = getattr(exc, "message", str(exc))
            repository.jobs.mark_failed(job_id, message)
            logger.exception(
                "제조 이벤트 생성 job 실패: job_id=%s job_type=%s",
                job_id,
                job.get("jobType"),
            )


def _execute_generation_job(
    service: ManufacturingEventJsonService,
    repository: SampleDbRepository,
    job: dict[str, Any],
) -> dict[str, Any]:
    """저장된 job 요청을 종류별 동기 서비스 메서드 호출로 변환한다."""
    request = job["request"]
    progress_callback = lambda progress: repository.jobs.update_progress(
        job["jobId"],
        progress,
    )

    if job["jobType"] == JOB_TYPE_GENERATE_RANGE:
        return service.generate_range(
            start_date=date.fromisoformat(request["startDate"]),
            end_date=date.fromisoformat(request["endDate"]),
            events_per_day=_optional_int(
                request.get("eventCount", request.get("eventsPerDay")),
            ),
            car_pool_size=_optional_int(request.get("carPoolSize")),
            insert_chunk_size=int(request["insertChunkSize"]),
            update_existing=True,
            progress_callback=progress_callback,
        )

    if job["jobType"] == JOB_TYPE_GENERATE_TEMPLATE:
        return service.generate_template(
            template_name=str(request["templateName"]),
            production_date=date.fromisoformat(request["productionDate"]),
            event_count=_optional_int(request.get("eventCount")),
            car_pool_size=_optional_int(request.get("carPoolSize")),
            insert_chunk_size=int(request["insertChunkSize"]),
            replace=bool(request["replace"]),
            update_existing=bool(request["replace"]),
            progress_callback=progress_callback,
        )

    if job["jobType"] == JOB_TYPE_GENERATE_TOMORROW:
        base_date = (
            date.fromisoformat(request["baseDate"])
            if request.get("baseDate")
            else None
        )
        return service.generate_tomorrow(
            base_date=base_date,
            template_name=str(request["templateName"]),
            events_per_day=_optional_int(
                request.get("eventCount", request.get("eventsPerDay")),
            ),
            car_pool_size=_optional_int(request.get("carPoolSize")),
            insert_chunk_size=int(request["insertChunkSize"]),
            update_existing=True,
            progress_callback=progress_callback,
        )

    raise AppException(
        "지원하지 않는 제조 이벤트 생성 job 유형입니다.",
        status_code=status.HTTP_400_BAD_REQUEST,
    )


def resume_incomplete_generation_jobs(database_url: str) -> int:
    repository = SampleDbRepository(database_url)
    repository.schema.ensure_schema()
    jobs = repository.jobs.list_resumable()
    for job in jobs:
        _generation_job_executor.submit(
            _run_generation_job,
            database_url,
            job["jobId"],
        )
    if jobs:
        logger.info("미완료 제조 이벤트 생성 job %s건을 worker에 재등록했습니다.", len(jobs))
    return len(jobs)


def _normalize_template_name(template_name: str) -> str:
    normalized = "".join(
        char.lower() if char.isalnum() else "_"
        for char in template_name.strip()
    ).strip("_")
    return normalized or DEFAULT_TEMPLATE_NAME


def _resolve_vehicle_limit(
    event_count: int | None,
    car_pool_size: int | None,
) -> int | None:
    """선택적 수량 제한을 차량 수로 정규화한다."""
    if event_count is not None:
        if event_count < 4 or event_count % 4 != 0:
            raise AppException(
                "event_count는 차량당 4공정 기준으로 4의 배수여야 합니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        event_vehicle_count = event_count // 4
        if car_pool_size is not None and car_pool_size != event_vehicle_count:
            raise AppException(
                "event_count와 car_pool_size가 일치하지 않습니다: "
                f"event_count/4={event_vehicle_count}, "
                f"car_pool_size={car_pool_size}",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        return event_vehicle_count

    if car_pool_size is not None:
        if car_pool_size < 1:
            raise AppException(
                "car_pool_size는 1 이상이어야 합니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        return car_pool_size
    return None


def _selected_vehicle_count(
    available_count: int,
    vehicle_limit: int | None,
    production_date: date,
) -> int:
    if available_count < 1:
        raise AppException(
            f"{production_date.isoformat()} 생산 차량이 car_master에 없습니다.",
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    if vehicle_limit is not None and available_count < vehicle_limit:
        raise AppException(
            "요청한 차량 수보다 해당 날짜의 car_master가 부족합니다: "
            f"date={production_date.isoformat()}, required={vehicle_limit}, "
            f"actual={available_count}",
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    return vehicle_limit if vehicle_limit is not None else available_count


def _date_range(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def _optional_int(value: Any) -> int | None:
    return int(value) if value is not None else None


def _materialize_template_row(
    row: dict[str, Any],
    *,
    target_date: date,
) -> dict[str, Any]:
    """템플릿 한 행을 목표 날짜의 manufacturing_event_json 행으로 변환한다."""
    event_time = datetime.combine(target_date, time.min) + timedelta(
        microseconds=int(row["event_offset_us"]),
    )
    sequence_no = int(str(row["template_event_id"]).rsplit("-", 1)[-1])
    event_id = f"EVT-{target_date:%Y%m%d}-{sequence_no:06d}"
    event_json = copy.deepcopy(row["event_json"])
    event_json["event"]["eventId"] = event_id
    event_json["event"]["eventTime"] = None
    event_json["equipmentStatus"]["lastNormalTime"] = None
    event_json["equipmentStatus"]["statusChangedTime"] = None
    # 날짜와 template_event_id가 같으면 항상 같은 값이 나오도록 결정적으로 변동한다.
    _apply_date_variation(
        event_json,
        target_date=target_date,
        template_event_id=str(row["template_event_id"]),
    )
    event_json = normalize_event_json(event_json)
    # 날짜별 변동 후에도 해당 공정의 processData 블록 하나와 필수 필드를 보장한다.
    validate_process_data(
        str(row["process_code"]),
        event_json.get("processData", {}),
    )
    return {
        "event_id": event_id,
        "event_time": event_time,
        "car_master_id": row["car_master_id"],
        "equipment_id": row["equipment_id"],
        "process_code": row["process_code"],
        "station_code": row["station_code"],
        "equipment_code": row["equipment_code"],
        "equipment_type": row["equipment_type"],
        "equipment_status": event_json["equipmentStatus"]["operationStatus"],
        "event_type": row["event_type"],
        "event_json": event_json,
        # 재생된 이벤트도 최초 공정 제어 규칙을 그대로 따른다.
        "dispatch_status": initial_dispatch_status(str(row["process_code"])),
        "analysis_status": "NOT_ANALYZED",
        "bottleneck_analysis_done": False,
        "defect_transfer_analysis_done": False,
        "retry_count": 0,
        "error_message": None,
    }


def _apply_date_variation(
    event_json: dict[str, Any],
    *,
    target_date: date,
    template_event_id: str,
) -> None:
    """정상/이상 클래스는 보존하면서 날짜별 센서·공정 수치에 작은 변화를 준다."""
    seed_key = f"{target_date.isoformat()}:{template_event_id}"
    sensor = event_json.get("sensor", {})
    process_metrics = event_json.get("processMetrics", {})
    process_data = event_json.get("processData", {})

    # 각 센서 값을 먼저 바꾼 뒤 processData와 설비 상태를 다시 동기화한다.
    _vary_current(sensor.get("current", {}), seed_key)
    _vary_vibration(sensor.get("vibration", {}), seed_key)
    _vary_robot_arm_vibration(sensor.get("robotArmVibration", {}), seed_key)
    _vary_thermal(sensor.get("thermal", {}), seed_key)
    _vary_process_metrics(process_metrics, seed_key)
    _sync_process_data(process_data, sensor, process_metrics, seed_key)
    _refresh_equipment_status(event_json, sensor, process_metrics)


def _vary_current(current: dict[str, Any], seed_key: str) -> None:
    if not current:
        return
    original_rms = _as_float(current.get("rmsAmpere"))
    original_max = _as_float(current.get("maxAmpere"), original_rms)
    original_min = _as_float(current.get("minAmpere"), original_rms)

    rms = _jitter_numeric(
        original_rms,
        seed_key,
        "current.rmsAmpere",
        pct=0.035,
        precision=9,
        min_value=0.0,
    )
    upper_spread = max(original_max - original_rms, rms * 0.035, 0.001)
    lower_spread = max(original_rms - original_min, rms * 0.035, 0.001)
    upper_spread *= _factor(seed_key, "current.upperSpread", 0.12)
    lower_spread *= _factor(seed_key, "current.lowerSpread", 0.12)

    current["rmsAmpere"] = rms
    current["maxAmpere"] = _round(rms + upper_spread, 9)
    current["minAmpere"] = _round(max(0.0, rms - lower_spread), 9)


def _vary_vibration(vibration: dict[str, Any], seed_key: str) -> None:
    if not vibration:
        return
    vibration["accelerationG"] = _jitter_numeric(
        _as_float(vibration.get("accelerationG")),
        seed_key,
        "vibration.accelerationG",
        pct=0.08,
        precision=9,
        min_value=0.0,
    )
    vibration["vibrationRms"] = _jitter_numeric(
        _as_float(vibration.get("vibrationRms")),
        seed_key,
        "vibration.vibrationRms",
        pct=0.065,
        precision=9,
        min_value=0.0,
    )
    vibration["vibrationPeak"] = _jitter_numeric(
        _as_float(vibration.get("vibrationPeak")),
        seed_key,
        "vibration.vibrationPeak",
        pct=0.075,
        precision=9,
        min_value=0.0,
    )
    vibration["vibrationScore"] = _round(
        _clamp(
            _as_float(vibration.get("vibrationScore"))
            + _noise(seed_key, "vibration.vibrationScore", -0.025, 0.025),
            0.0,
            0.99,
        ),
        6,
    )


def _vary_robot_arm_vibration(robot: dict[str, Any], seed_key: str) -> None:
    if not robot:
        return
    robot["frequencyHz"] = _jitter_numeric(
        _as_float(robot.get("frequencyHz")),
        seed_key,
        "robot.frequencyHz",
        pct=0.035,
        precision=3,
        min_value=0.0,
    )
    for field in ("amplitude", "vibrationRms", "vibrationPeak"):
        robot[field] = _jitter_numeric(
            _as_float(robot.get(field)),
            seed_key,
            f"robot.{field}",
            pct=0.07,
            precision=9,
            min_value=0.0,
        )
    robot["vibrationScore"] = _round(
        _clamp(
            _as_float(robot.get("vibrationScore"))
            + _noise(seed_key, "robot.vibrationScore", -0.03, 0.03),
            0.0,
            0.99,
        ),
        6,
    )


def _vary_thermal(thermal: dict[str, Any], seed_key: str) -> None:
    if not thermal:
        return
    original_avg = _as_float(thermal.get("avgTemperature"))
    original_max = _as_float(thermal.get("maxTemperature"), original_avg)
    original_min = _as_float(thermal.get("minTemperature"), original_avg)

    avg_temp = _round(
        original_avg + _noise(seed_key, "thermal.avgTemperature", -0.85, 0.85),
        3,
    )
    upper_spread = max(original_max - original_avg, 0.25)
    lower_spread = max(original_avg - original_min, 0.25)
    upper_spread *= _factor(seed_key, "thermal.upperSpread", 0.08)
    lower_spread *= _factor(seed_key, "thermal.lowerSpread", 0.08)

    thermal["avgTemperature"] = avg_temp
    thermal["thermalScore"] = avg_temp
    thermal["maxTemperature"] = _round(max(avg_temp, avg_temp + upper_spread), 3)
    thermal["minTemperature"] = _round(min(avg_temp, avg_temp - lower_spread), 3)


def _vary_process_metrics(metrics: dict[str, Any], seed_key: str) -> None:
    if not metrics:
        return
    cycle_time = _jitter_numeric(
        _as_float(metrics.get("cycleTimeSec"), 1.0),
        seed_key,
        "metrics.cycleTimeSec",
        pct=0.045,
        precision=3,
        min_value=1.0,
    )
    processing_time = _jitter_numeric(
        _as_float(metrics.get("processingTimeSec"), cycle_time * 0.85),
        seed_key,
        "metrics.processingTimeSec",
        pct=0.04,
        precision=3,
        min_value=0.5,
    )
    processing_time = _round(min(processing_time, max(0.5, cycle_time - 0.5)), 3)
    waiting_time = _round(
        max(
            0.5,
            cycle_time
            - processing_time
            + _noise(seed_key, "metrics.waitingTimeSec", -0.35, 1.15),
        ),
        3,
    )
    station_delay = _jitter_numeric(
        _as_float(metrics.get("stationDelaySec")),
        seed_key,
        "metrics.stationDelaySec",
        pct=0.12,
        precision=3,
        min_value=0.0,
    )
    idle_time = _jitter_numeric(
        _as_float(metrics.get("equipmentIdleTimeSec")),
        seed_key,
        "metrics.equipmentIdleTimeSec",
        pct=0.13,
        precision=3,
        min_value=0.0,
    )

    metrics["cycleTimeSec"] = cycle_time
    metrics["processingTimeSec"] = processing_time
    metrics["waitingTimeSec"] = waiting_time
    metrics["stationDelaySec"] = station_delay
    metrics["throughputPerMin"] = _round(60 / cycle_time, 3)
    metrics["queueLength"] = max(
        0,
        _as_int(metrics.get("queueLength")) + _noise_int(seed_key, "metrics.queue", 2),
    )
    metrics["wipCount"] = max(
        0,
        _as_int(metrics.get("wipCount")) + _noise_int(seed_key, "metrics.wip", 3),
    )
    metrics["equipmentIdleTimeSec"] = idle_time


def _press_count_increase_flag(
    *,
    station_delay_sec: float,
    equipment_idle_time_sec: float,
) -> bool | None:
    if station_delay_sec <= 2.0 and equipment_idle_time_sec < 6.0:
        return True
    if station_delay_sec <= 3.0:
        return None
    return False


def _body_robot_motion_status(vibration_score: float) -> str:
    if vibration_score >= 0.45:
        return "ABNORMAL"
    if vibration_score >= 0.40:
        return "WARNING"
    return "NORMAL"


def _body_robot_operation_mode(vibration_score: float) -> str:
    return "STOPPED" if vibration_score >= 0.45 else "AUTO"


def _sync_process_data(
    process_data: dict[str, Any],
    sensor: dict[str, Any],
    metrics: dict[str, Any],
    seed_key: str,
) -> None:
    """변동된 센서/공정 지표와 공정별 processData의 파생값을 일치시킨다."""
    if not process_data:
        return

    press = process_data.get("press")
    if isinstance(press, dict):
        press["timestampDelaySec"] = metrics.get("stationDelaySec", 0.0)
        press["countIncreaseYn"] = _press_count_increase_flag(
            station_delay_sec=_as_float(metrics.get("stationDelaySec")),
            equipment_idle_time_sec=_as_float(metrics.get("equipmentIdleTimeSec")),
        )

    body = process_data.get("body")
    if isinstance(body, dict):
        robot_score = _as_float(
            sensor.get("robotArmVibration", {}).get("vibrationScore"),
        )
        body["robotMotionStatus"] = _body_robot_motion_status(robot_score)
        body["robotOperationMode"] = _body_robot_operation_mode(robot_score)

    paint = process_data.get("paint")
    if isinstance(paint, dict):
        thermal_std_temp = _jitter_numeric(
            _as_float(paint.get("thermalStdTemp")),
            seed_key,
            "paint.thermalStdTemp",
            pct=0.05,
            absolute=0.18,
            precision=3,
            min_value=0.0,
        )
        original_thermal_std_temp = _as_float(paint.get("thermalStdTemp"))
        if original_thermal_std_temp < 2.0:
            thermal_std_temp = min(1.99, thermal_std_temp)
        elif original_thermal_std_temp < 5.0:
            thermal_std_temp = min(4.99, max(2.0, thermal_std_temp))
        else:
            thermal_std_temp = max(5.0, thermal_std_temp)
        paint["thermalStdTemp"] = _round(thermal_std_temp, 3)

        thickness_value = _jitter_numeric(
            _as_float(paint.get("thicknessValue")),
            seed_key,
            "paint.thicknessValue",
            pct=0.01,
            absolute=0.9,
            precision=3,
            min_value=0.0,
        )
        original_thickness_value = _as_float(paint.get("thicknessValue"))
        if original_thickness_value < 80.0:
            thickness_value = min(79.9, thickness_value)
        elif original_thickness_value < 90.0:
            thickness_value = min(89.9, max(80.0, thickness_value))
        elif original_thickness_value <= 120.0:
            thickness_value = min(120.0, max(90.0, thickness_value))
        elif original_thickness_value <= 130.0:
            thickness_value = min(130.0, max(120.0, thickness_value))
        else:
            thickness_value = max(130.1, thickness_value)
        paint["thicknessValue"] = _round(thickness_value, 3)

        original_defect_score = _as_float(paint.get("defectScore"))
        defect_score = _round(
            _clamp(
                original_defect_score
                + _noise(seed_key, "paint.defectScore", -0.025, 0.025),
                0.0,
                0.99,
            ),
            4,
        )
        if original_defect_score < 0.4:
            defect_score = min(0.39, defect_score)
        elif original_defect_score < 0.6:
            defect_score = min(0.59, max(0.4, defect_score))
        else:
            defect_score = max(0.6, defect_score)
        paint["defectScore"] = defect_score

        surface_quality_score = _jitter_numeric(
            _as_float(paint.get("surfaceQualityScore")),
            seed_key,
            "paint.surfaceQualityScore",
            pct=0.02,
            absolute=1.5,
            precision=3,
            min_value=0.0,
        )
        original_surface_quality_score = _as_float(paint.get("surfaceQualityScore"))
        if original_surface_quality_score >= 80.0:
            surface_quality_score = max(80.0, surface_quality_score)
        elif original_surface_quality_score >= 60.0:
            surface_quality_score = min(79.9, max(60.0, surface_quality_score))
        else:
            surface_quality_score = min(59.9, surface_quality_score)
        paint["surfaceQualityScore"] = _round(surface_quality_score, 3)
        paint["visionLabel"] = "DEFECT" if defect_score >= 0.4 else "NORMAL"

    assembly = process_data.get("assembly")
    if isinstance(assembly, dict):
        delta = 1 if _noise(seed_key, "assembly.errorFlip", 0.0, 1.0) > 0.92 else 0
        base_sequence_error = _as_int(assembly.get("sequenceErrorCount"))
        sequence_error_count = max(0, min(2, base_sequence_error + delta))
        assembly["sequenceErrorCount"] = sequence_error_count
        assembly["fasteningErrorCount"] = max(
            0,
            min(2, _as_int(assembly.get("fasteningErrorCount")) + delta),
        )
        assembly["missingPartCount"] = max(
            0,
            min(2, _as_int(assembly.get("missingPartCount")) + (1 if delta else 0)),
        )
        expected_sequence = str(assembly.get("expectedSequence", ""))
        expected_steps = expected_sequence.split(">")
        if len(expected_steps) == 4:
            assembly["actualSequence"] = (
                ">".join(
                    [
                        expected_steps[0],
                        expected_steps[2],
                        expected_steps[1],
                        expected_steps[3],
                    ],
                )
                if sequence_error_count
                else expected_sequence
            )


def _refresh_equipment_status(
    event_json: dict[str, Any],
    sensor: dict[str, Any],
    metrics: dict[str, Any],
) -> None:
    """날짜별 수치 변동 후에도 생성된 상태와 이상 분류를 그대로 유지한다."""
    equipment_status = event_json.get("equipmentStatus", {})
    # 센서와 지표는 날짜별로 변동하지만 정상/이상 7:3 분포와 최초 랜덤
    # operationStatus는 변경하지 않는다.
    _ = sensor, metrics
    equipment_status["lastNormalTime"] = None
    equipment_status["statusChangedTime"] = None


def _jitter_numeric(
    value: float,
    seed_key: str,
    field: str,
    *,
    pct: float,
    precision: int,
    min_value: float | None = None,
    max_value: float | None = None,
    absolute: float | None = None,
) -> float:
    base = _as_float(value)
    delta = _noise(seed_key, field, -absolute, absolute) if absolute else 0.0
    varied = base * _factor(seed_key, field, pct) + delta
    if min_value is not None:
        varied = max(min_value, varied)
    if max_value is not None:
        varied = min(max_value, varied)
    return _round(varied, precision)


def _factor(seed_key: str, field: str, pct: float) -> float:
    return 1.0 + _noise(seed_key, field, -pct, pct)


def _noise(seed_key: str, field: str, low: float, high: float) -> float:
    """해시 기반 의사 난수로 같은 날짜·이벤트·필드에 같은 변동값을 반환한다."""
    digest = hashlib.blake2b(
        f"{seed_key}:{field}".encode("utf-8"),
        digest_size=8,
    ).digest()
    ratio = int.from_bytes(digest, "big") / ((1 << 64) - 1)
    return low + (high - low) * ratio


def _noise_int(seed_key: str, field: str, spread: int) -> int:
    if spread <= 0:
        return 0
    return int(round(_noise(seed_key, field, -spread, spread)))


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _clamp(value: float, low: float, high: float) -> float:
    return min(high, max(low, value))


def _round(value: float, precision: int) -> float:
    return round(float(value), precision)
