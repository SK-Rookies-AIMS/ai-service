from __future__ import annotations

import copy
import hashlib
from datetime import date, datetime, time, timedelta
from typing import Any, Callable

from fastapi import status

from app.core.config import settings
from app.core.exceptions import AppException
from app.ml.preprocessing.manufacturing_event_json_builder import (
    EventBuildRequest,
    ManufacturingEventJsonBuilder,
)
from app.repository.sampledb_repository import SampleDbRepository


DEFAULT_EVENTS_PER_DAY = 86_400
DEFAULT_CAR_POOL_SIZE = 10_000
DEFAULT_INSERT_CHUNK_SIZE = 1_000
DEFAULT_TEMPLATE_NAME = "default"
TEMPLATE_ANCHOR_DATE = date(2000, 1, 1)
ProgressCallback = Callable[[dict[str, Any]], None]


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
        events_per_day: int = DEFAULT_EVENTS_PER_DAY,
        car_pool_size: int = DEFAULT_CAR_POOL_SIZE,
        insert_chunk_size: int = DEFAULT_INSERT_CHUNK_SIZE,
        update_existing: bool = True,
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        if start_date > end_date:
            raise AppException(
                "start_date는 end_date보다 이후일 수 없습니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        if events_per_day < len(("PRESS", "BODY", "PAINT", "ASSEMBLY")):
            raise AppException(
                "events_per_day는 최소 4 이상이어야 공정별 이벤트를 생성할 수 있습니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        if car_pool_size < 1:
            raise AppException(
                "car_pool_size는 1 이상이어야 합니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        if insert_chunk_size < 1:
            raise AppException(
                "insert_chunk_size는 1 이상이어야 합니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        self.repository.ensure_schema()
        self.repository.seed_equipment()
        days = (end_date - start_date).days + 1
        total_events = days * events_per_day
        car_id_map = self.repository.ensure_car_master_rows(car_pool_size)
        equipment_map = self.repository.get_equipment_map()

        request = EventBuildRequest(
            start_date=start_date,
            end_date=end_date,
            events_per_day=events_per_day,
            car_id_map=car_id_map,
            equipment_map=equipment_map,
        )
        affected_rows = 0
        generated_count = 0
        distribution = {"PRESS": 0, "BODY": 0, "PAINT": 0, "ASSEMBLY": 0}
        chunk: list[dict[str, Any]] = []
        for row in self.builder.iter_rows(request):
            chunk.append(row)
            generated_count += 1
            distribution[str(row["process_code"])] += 1
            if len(chunk) >= insert_chunk_size:
                affected_rows += self.repository.insert_event_json_rows(
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
            affected_rows += self.repository.insert_event_json_rows(
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

        stored_count = self.repository.count_event_json_between(start_date, end_date)
        return {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "eventsPerDay": events_per_day,
            "totalExpectedEvents": total_events,
            "generatedCount": generated_count,
            "affectedRows": affected_rows,
            "storedCountInRange": stored_count,
            "carPoolSize": len(car_id_map),
            "insertChunkSize": insert_chunk_size,
            "processDistribution": distribution,
        }

    def generate_template(
        self,
        *,
        template_name: str = DEFAULT_TEMPLATE_NAME,
        event_count: int = DEFAULT_EVENTS_PER_DAY,
        car_pool_size: int = DEFAULT_CAR_POOL_SIZE,
        insert_chunk_size: int = DEFAULT_INSERT_CHUNK_SIZE,
        replace: bool = False,
        update_existing: bool = False,
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        if event_count < len(("PRESS", "BODY", "PAINT", "ASSEMBLY")):
            raise AppException(
                "event_count는 최소 4 이상이어야 공정별 템플릿을 생성할 수 있습니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        if car_pool_size < 1:
            raise AppException(
                "car_pool_size는 1 이상이어야 합니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        if insert_chunk_size < 1:
            raise AppException(
                "insert_chunk_size는 1 이상이어야 합니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        normalized_template_name = _normalize_template_name(template_name)
        self.repository.ensure_schema()
        self.repository.seed_equipment()
        if replace:
            self.repository.delete_template_events(normalized_template_name)

        car_id_map = self.repository.ensure_car_master_rows(car_pool_size)
        equipment_map = self.repository.get_equipment_map()
        request = EventBuildRequest(
            start_date=TEMPLATE_ANCHOR_DATE,
            end_date=TEMPLATE_ANCHOR_DATE,
            events_per_day=event_count,
            car_id_map=car_id_map,
            equipment_map=equipment_map,
        )

        affected_rows = 0
        generated_count = 0
        distribution = {"PRESS": 0, "BODY": 0, "PAINT": 0, "ASSEMBLY": 0}
        chunk: list[dict[str, Any]] = []
        anchor_datetime = datetime.combine(TEMPLATE_ANCHOR_DATE, time.min)
        for row in self.builder.iter_rows(request):
            generated_count += 1
            distribution[str(row["process_code"])] += 1
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
                affected_rows += self.repository.insert_template_event_rows(
                    chunk,
                    update_existing=update_existing,
                )
                chunk = []
                if progress_callback:
                    progress_callback(
                        {
                            "generatedCount": generated_count,
                            "affectedRows": affected_rows,
                            "totalExpectedEvents": event_count,
                        },
                    )
        if chunk:
            affected_rows += self.repository.insert_template_event_rows(
                chunk,
                update_existing=update_existing,
            )
            if progress_callback:
                progress_callback(
                    {
                        "generatedCount": generated_count,
                        "affectedRows": affected_rows,
                        "totalExpectedEvents": event_count,
                    },
                )

        stored_count = self.repository.count_template_events(normalized_template_name)
        return {
            "templateName": normalized_template_name,
            "eventCount": event_count,
            "generatedCount": generated_count,
            "affectedRows": affected_rows,
            "storedCount": stored_count,
            "carPoolSize": len(car_id_map),
            "insertChunkSize": insert_chunk_size,
            "processDistribution": distribution,
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

    def list_template_events(
        self,
        *,
        template_name: str = DEFAULT_TEMPLATE_NAME,
        limit: int,
        offset: int = 0,
        process_code: str | None = None,
    ) -> list[dict[str, Any]]:
        return self.repository.list_template_event_rows(
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
        if insert_chunk_size < 1:
            raise AppException(
                "insert_chunk_size는 1 이상이어야 합니다.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        normalized_template_name = _normalize_template_name(template_name)
        self.repository.ensure_schema()
        total_template_events = self.repository.count_template_events(
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
        offset = 0
        while offset < total_template_events:
            template_rows = self.repository.list_template_event_rows(
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
            affected_rows += self.repository.insert_event_json_rows(
                materialized_rows,
                update_existing=update_existing,
            )
            for row in materialized_rows:
                distribution[str(row["process_code"])] += 1

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

        stored_count = self.repository.count_event_json_between(
            target_date,
            target_date,
        )
        return {
            "templateName": normalized_template_name,
            "targetDate": target_date.isoformat(),
            "templateEventCount": total_template_events,
            "generatedCount": generated_count,
            "affectedRows": affected_rows,
            "storedCountInDate": stored_count,
            "insertChunkSize": insert_chunk_size,
            "updateExisting": update_existing,
            "processDistribution": distribution,
            "variationMode": "target_date_and_template_event_id_seed",
        }

    def ensure_template(
        self,
        *,
        template_name: str = DEFAULT_TEMPLATE_NAME,
        event_count: int = DEFAULT_EVENTS_PER_DAY,
        car_pool_size: int = DEFAULT_CAR_POOL_SIZE,
        insert_chunk_size: int = DEFAULT_INSERT_CHUNK_SIZE,
    ) -> dict[str, Any]:
        normalized_template_name = _normalize_template_name(template_name)
        self.repository.ensure_schema()
        stored_count = self.repository.count_template_events(normalized_template_name)
        if stored_count >= event_count:
            return {
                "templateName": normalized_template_name,
                "eventCount": event_count,
                "storedCount": stored_count,
                "created": False,
            }

        result = self.generate_template(
            template_name=normalized_template_name,
            event_count=event_count,
            car_pool_size=car_pool_size,
            insert_chunk_size=insert_chunk_size,
            replace=False,
            update_existing=False,
        )
        return {
            **result,
            "created": True,
        }

    def generate_initial_demo_range(
        self,
        *,
        events_per_day: int = DEFAULT_EVENTS_PER_DAY,
        car_pool_size: int = DEFAULT_CAR_POOL_SIZE,
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
        events_per_day: int = DEFAULT_EVENTS_PER_DAY,
        car_pool_size: int = DEFAULT_CAR_POOL_SIZE,
        insert_chunk_size: int = DEFAULT_INSERT_CHUNK_SIZE,
        update_existing: bool = True,
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        target_date = (base_date or date.today()) + timedelta(days=1)
        template_result = self.ensure_template(
            template_name=template_name,
            event_count=events_per_day,
            car_pool_size=car_pool_size,
            insert_chunk_size=insert_chunk_size,
        )
        materialized_result = self.materialize_template_events(
            template_name=template_name,
            target_date=target_date,
            insert_chunk_size=insert_chunk_size,
            update_existing=update_existing,
            progress_callback=progress_callback,
        )
        return {
            **materialized_result,
            "baseDate": (base_date or date.today()).isoformat(),
            "templatePrepared": template_result,
        }

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
        return self.repository.list_event_json_rows(
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
            "SAMPLE_DATABASE_URL 설정이 필요합니다.",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    return ManufacturingEventJsonService(
        SampleDbRepository(settings.sample_database_connection_url),
    )


def _normalize_template_name(template_name: str) -> str:
    normalized = "".join(
        char.lower() if char.isalnum() else "_"
        for char in template_name.strip()
    ).strip("_")
    return normalized or DEFAULT_TEMPLATE_NAME


def _materialize_template_row(
    row: dict[str, Any],
    *,
    target_date: date,
) -> dict[str, Any]:
    event_time = datetime.combine(target_date, time.min) + timedelta(
        microseconds=int(row["event_offset_us"]),
    )
    sequence_no = int(str(row["template_event_id"]).rsplit("-", 1)[-1])
    event_id = f"EVT-{target_date:%Y%m%d}-{sequence_no:06d}"
    event_json = copy.deepcopy(row["event_json"])
    event_json["event"]["eventId"] = event_id
    event_json["event"]["eventTime"] = event_time.isoformat()
    event_json["equipmentStatus"]["statusChangedTime"] = event_time.isoformat()
    event_json["equipmentStatus"]["lastNormalTime"] = (
        event_time - timedelta(seconds=31)
    ).isoformat()
    _apply_date_variation(
        event_json,
        target_date=target_date,
        template_event_id=str(row["template_event_id"]),
    )
    materialized_status = event_json["equipmentStatus"]["operationStatus"]
    return {
        "event_id": event_id,
        "event_time": event_time,
        "template_event_id": row["template_event_id"],
        "template_name": row["template_name"],
        "event_offset_us": row["event_offset_us"],
        "car_master_id": row["car_master_id"],
        "equipment_id": row["equipment_id"],
        "process_code": row["process_code"],
        "station_code": row["station_code"],
        "equipment_code": row["equipment_code"],
        "equipment_type": row["equipment_type"],
        "equipment_status": materialized_status,
        "event_type": row["event_type"],
        "event_json": event_json,
    }


def _apply_date_variation(
    event_json: dict[str, Any],
    *,
    target_date: date,
    template_event_id: str,
) -> None:
    seed_key = f"{target_date.isoformat()}:{template_event_id}"
    sensor = event_json.get("sensor", {})
    process_metrics = event_json.get("processMetrics", {})
    process_data = event_json.get("processData", {})

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


def _sync_process_data(
    process_data: dict[str, Any],
    sensor: dict[str, Any],
    metrics: dict[str, Any],
    seed_key: str,
) -> None:
    if not process_data:
        return

    press = process_data.get("press")
    if isinstance(press, dict):
        press["timestampDelaySec"] = metrics.get("stationDelaySec", 0.0)
        press["countIncreaseYn"] = _as_float(
            metrics.get("equipmentIdleTimeSec"),
        ) < 18.0

    body = process_data.get("body")
    if isinstance(body, dict):
        robot_score = _as_float(
            sensor.get("robotArmVibration", {}).get("vibrationScore"),
        )
        body["robotMotionStatus"] = "WARNING" if robot_score >= 0.68 else "NORMAL"

    paint = process_data.get("paint")
    if isinstance(paint, dict):
        paint["thermalStdTemp"] = _jitter_numeric(
            _as_float(paint.get("thermalStdTemp")),
            seed_key,
            "paint.thermalStdTemp",
            pct=0.04,
            absolute=0.12,
            precision=3,
            min_value=0.0,
        )
        paint["thicknessValue"] = _jitter_numeric(
            _as_float(paint.get("thicknessValue")),
            seed_key,
            "paint.thicknessValue",
            pct=0.004,
            absolute=0.45,
            precision=3,
            min_value=0.0,
        )
        defect_score = _round(
            _clamp(
                _as_float(paint.get("defectScore"))
                + _noise(seed_key, "paint.defectScore", -0.025, 0.025),
                0.0,
                0.99,
            ),
            4,
        )
        paint["defectScore"] = defect_score
        paint["surfaceQualityScore"] = _round(max(0.0, 100.0 - defect_score * 32), 3)
        paint["visionLabel"] = "DEFECT" if defect_score >= 0.45 else "NORMAL"

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
        assembly["actualSequence"] = (
            "A01>A03>A02>A04"
            if sequence_error_count
            else "A01>A02>A03>A04"
        )


def _refresh_equipment_status(
    event_json: dict[str, Any],
    sensor: dict[str, Any],
    metrics: dict[str, Any],
) -> None:
    equipment_status = event_json.get("equipmentStatus", {})
    idle_time = _as_float(metrics.get("equipmentIdleTimeSec"))
    station_delay = _as_float(metrics.get("stationDelaySec"))
    vibration_score = max(
        _as_float(sensor.get("vibration", {}).get("vibrationScore")),
        _as_float(sensor.get("robotArmVibration", {}).get("vibrationScore")),
    )
    defect_score = _as_float(
        event_json.get("processData", {}).get("paint", {}).get("defectScore"),
    )

    if idle_time >= 20.0:
        operation_status = "IDLE"
    elif station_delay >= 12.0 or vibration_score >= 0.82 or defect_score >= 0.75:
        operation_status = "ERROR"
    else:
        operation_status = "RUNNING"

    health_status = (
        "WARNING"
        if operation_status != "RUNNING"
        or station_delay >= 8.0
        or vibration_score >= 0.65
        or defect_score >= 0.45
        else "NORMAL"
    )
    equipment_status["operationStatus"] = operation_status
    equipment_status["healthStatus"] = health_status


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
