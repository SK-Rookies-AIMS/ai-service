from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


# PRD의 차량 생산 순서다. 한 차량마다 아래 4개 공정 이벤트를 정확히 한 건씩 만든다.
PROCESS_SEQUENCE = ("PRESS", "BODY", "PAINT", "ASSEMBLY")
PROCESS_ROUTE_PREFIX = {
    "PRESS": "P",
    "BODY": "B",
    "PAINT": "PA",
    "ASSEMBLY": "A",
}

# PRD 기준 공정별 설비 수와 목표 이상 데이터 비율이다.
LINE_STATION_COUNT = 5
ABNORMAL_RATIO = 0.30
PROCESS_DATA_REQUIRED_FIELDS: dict[str, frozenset[str]] = {
    "PRESS": frozenset(
        {
            "countIncreaseYn",
            "targetCycleTimeSec",
            "timestampDelaySec",
        },
    ),
    "BODY": frozenset(
        {
            "robotMotionStatus",
            "robotOperationMode",
            "frequencyPeakBand",
            "frequencyBands",
        },
    ),
    "PAINT": frozenset(
        {
            "imagePosition",
            "thermalStdTemp",
            "thicknessValue",
            "defectScore",
            "visionLabel",
            "surfaceQualityScore",
        },
    ),
    "ASSEMBLY": frozenset(
        {
            "expectedSequence",
            "actualSequence",
            "missingPartCount",
            "fasteningErrorCount",
            "sequenceErrorCount",
        },
    ),
}
PROCESS_DATA_KEY = {
    "PRESS": "press",
    "BODY": "body",
    "PAINT": "paint",
    "ASSEMBLY": "assembly",
}


def initial_dispatch_status(process_code: str) -> str:
    """원천 이벤트 최초 발행 상태를 반환한다.

    차량 생산 흐름은 PRESS부터 시작하므로 PRESS만 READY이며, BODY/PAINT/
    ASSEMBLY는 이전 공정의 정상 분석 결과가 오기 전까지 PENDING이다.
    """
    return "READY" if process_code == "PRESS" else "PENDING"


def is_abnormal_operation_status(operation_status: Any) -> bool:
    """이벤트 JSON의 운전 상태가 이상 상태인지 반환한다."""
    return operation_status in {"FAULT", "STOPPED", "MAINTENANCE"}


def normalize_event_json(event_json: dict[str, Any]) -> dict[str, Any]:
    """제조 이벤트 JSON을 PRD에 정의된 필드만 남긴 구조로 정규화한다."""
    event = event_json.get("event", {})
    equipment = event_json.get("equipment", {})
    equipment_status = event_json.get("equipmentStatus", {})
    product = event_json.get("product", {})
    sensor = event_json.get("sensor", {})
    current = sensor.get("current", {})
    vibration = sensor.get("vibration", {})
    robot = sensor.get("robotArmVibration", {})
    thermal = sensor.get("thermal", {})
    metrics = event_json.get("processMetrics", {})
    source_trace = event_json.get("sourceTrace", {})

    return {
        "event": {
            "eventId": event.get("eventId"),
            "eventTime": event.get("eventTime"),
            "eventType": event.get("eventType"),
            "eventName": event.get("eventName"),
        },
        "equipment": {
            "equipmentCode": equipment.get("equipmentCode"),
            "equipmentName": equipment.get("equipmentName"),
            "equipmentType": equipment.get("equipmentType"),
        },
        "equipmentStatus": {
            "operationStatus": equipment_status.get("operationStatus"),
            "lastNormalTime": equipment_status.get("lastNormalTime"),
            "statusChangedTime": equipment_status.get("statusChangedTime"),
        },
        "product": {
            "carMasterId": product.get("carMasterId"),
        },
        "sensor": {
            "sensorType": sensor.get("sensorType"),
            "current": {
                "rmsAmpere": current.get("rmsAmpere"),
                "maxAmpere": current.get("maxAmpere"),
                "minAmpere": current.get("minAmpere"),
            },
            "vibration": {
                "accelerationG": vibration.get("accelerationG"),
                "vibrationScore": vibration.get("vibrationScore"),
                "vibrationRms": vibration.get("vibrationRms"),
                "vibrationPeak": vibration.get("vibrationPeak"),
            },
            "robotArmVibration": {
                "robotId": robot.get("robotId"),
                "axis": robot.get("axis"),
                "frequencyHz": robot.get("frequencyHz"),
                "amplitude": robot.get("amplitude"),
                "vibrationRms": robot.get("vibrationRms"),
                "vibrationPeak": robot.get("vibrationPeak"),
                "vibrationScore": robot.get("vibrationScore"),
            },
            "thermal": {
                "thermalScore": thermal.get("thermalScore"),
                "avgTemperature": thermal.get("avgTemperature"),
                "maxTemperature": thermal.get("maxTemperature"),
                "minTemperature": thermal.get("minTemperature"),
            },
        },
        "processMetrics": {
            "cycleTimeSec": metrics.get("cycleTimeSec"),
            "waitingTimeSec": metrics.get("waitingTimeSec"),
            "processingTimeSec": metrics.get("processingTimeSec"),
            "stationDelaySec": metrics.get("stationDelaySec"),
            "throughputPerMin": metrics.get("throughputPerMin"),
            "queueLength": metrics.get("queueLength"),
            "wipCount": metrics.get("wipCount"),
            "equipmentIdleTimeSec": metrics.get("equipmentIdleTimeSec"),
        },
        "sourceTrace": {
            "fordRowId": source_trace.get("fordRowId"),
            "formingRowId": source_trace.get("formingRowId"),
            "robotArmVibrationRowId": source_trace.get(
                "robotArmVibrationRowId",
            ),
            "machineVisionRowId": source_trace.get("machineVisionRowId"),
            "boschId": source_trace.get("boschId"),
        },
        "processData": event_json.get("processData", {}),
    }


PROCESS_META: dict[str, dict[str, Any]] = {
    "PRESS": {
        "equipmentType": "HYDRAULIC_PRESS",
        "eventType": "PROCESS_STATUS",
        "eventName": "프레스 공정 통합 관제 이벤트",
        "targetCycleTimeSec": 40.0,
    },
    "BODY": {
        "equipmentType": "ROBOT_ARM",
        "eventType": "EQUIPMENT_SENSOR",
        "eventName": "차체 공정 로봇 관제 이벤트",
        "targetCycleTimeSec": 52.0,
    },
    "PAINT": {
        "equipmentType": "CAMERA",
        "eventType": "QUALITY_CHECK",
        "eventName": "도장 공정 품질 관제 이벤트",
        "targetCycleTimeSec": 64.0,
    },
    "ASSEMBLY": {
        "equipmentType": "CONVEYOR",
        "eventType": "PROCESS_STATUS",
        "eventName": "의장 공정 조립 관제 이벤트",
        "targetCycleTimeSec": 58.0,
    },
}

DATASET_ROOT = Path(__file__).resolve().parents[1] / "ml" / "datasets" / "process"
EVENT_DENSITY_WINDOWS: tuple[tuple[int, int, float], ...] = (
    (0, 5, 0.35),
    (5, 8, 0.8),
    (8, 12, 1.6),
    (12, 13, 0.65),
    (13, 18, 1.8),
    (18, 22, 1.0),
    (22, 24, 0.45),
)


@dataclass(frozen=True)
class EventBuildRequest:
    """한 번의 제조 이벤트 생성 작업에 필요한 범위와 마스터 데이터."""

    start_date: date
    end_date: date
    # 기존 API 이름을 유지하지만 현재 의미는 기간 전체 이벤트 수다.
    events_per_day: int
    # dict 삽입 순서가 car_master.id 오름차순을 보존한다.
    car_id_map: dict[str, int]
    equipment_map: dict[str, dict[str, Any]]


class ManufacturingEventJsonBuilder:
    """CSV 원천 데이터를 PRD의 통합 제조 이벤트 JSON으로 변환한다."""

    def __init__(self, dataset_root: Path = DATASET_ROOT) -> None:
        self.dataset_root = dataset_root
        self._cache: dict[str, Any] = {}
        self._feature_cache: dict[tuple[Any, ...], dict[str, Any]] = {}

    def build_rows(self, request: EventBuildRequest) -> list[dict[str, Any]]:
        return list(self.iter_rows(request))

    def iter_rows(self, request: EventBuildRequest) -> Iterator[dict[str, Any]]:
        """차량 단위로 4공정 row를 순차 생성한다."""
        # Repository가 car_master.id 오름차순으로 만든 순서를 그대로 사용해야
        # PRD의 "id 1번 차량부터 생성" 조건을 지킬 수 있다.
        car_ids = list(request.car_id_map)
        if not car_ids:
            return

        days = (request.end_date - request.start_date).days + 1
        if days <= 0:
            return

        # 차량마다 4건이므로 이벤트 수와 차량 수를 독립적으로 받을 수 없다.
        # 여기서 다시 검증해 서비스 외부에서 Builder를 직접 호출해도 구조가 깨지지 않게 한다.
        total_events = len(car_ids) * len(PROCESS_SEQUENCE)
        if request.events_per_day != total_events:
            raise ValueError(
                "event_count는 car_pool_size * 4와 같아야 합니다: "
                f"event_count={request.events_per_day}, expected={total_events}",
            )

        # 날짜별 실제 차량 수를 기준으로 정상 70% / 폐기(이상) 30%를 배치한다.
        # 이상 차량도 4공정을 모두 가지므로 공정별 이상 건수가 자동으로 동일해진다.
        abnormal_vehicle_count = round(len(car_ids) * ABNORMAL_RATIO)
        normal_vehicle_count = len(car_ids) - abnormal_vehicle_count

        for production_sequence_index, car_id in enumerate(car_ids):
            car_master_id = request.car_id_map[car_id]
            is_abnormal_vehicle = production_sequence_index >= normal_vehicle_count
            # 같은 차량의 이벤트는 PRESS -> BODY -> PAINT -> ASSEMBLY 순서를 유지한다.
            for process_index, process_code in enumerate(PROCESS_SEQUENCE):
                global_index = (
                    production_sequence_index * len(PROCESS_SEQUENCE) + process_index
                )
                event_time = _event_time_for_range_slot(
                    start_date=request.start_date,
                    end_date=request.end_date,
                    slot=global_index,
                    total_events=total_events,
                )
                event_id = f"EVT-{event_time:%Y%m%d}-{global_index + 1:06d}"
                event = self._build_event(
                    event_id=event_id,
                    event_time=event_time,
                    process_code=process_code,
                    global_index=global_index,
                    production_sequence_index=production_sequence_index,
                    car_id=car_id,
                    car_master_id=car_master_id,
                    is_abnormal=is_abnormal_vehicle,
                    equipment_map=request.equipment_map,
                )
                equipment_code = event["equipment"]["equipmentCode"]
                equipment_row = request.equipment_map[equipment_code]
                yield {
                    "event_id": event_id,
                    "event_time": event_time,
                    "car_master_id": car_master_id,
                    "equipment_id": int(equipment_row["id"]),
                    "process_code": process_code,
                    "station_code": f"{process_code}_STATION_{int(equipment_code.rsplit('_', 1)[1]):02d}",
                    "equipment_code": equipment_code,
                    "equipment_type": event["equipment"]["equipmentType"],
                    "equipment_status": event["equipmentStatus"]["operationStatus"],
                    "event_type": event["event"]["eventType"],
                    "event_json": _json_safe(event),
                    # 최초에는 PRESS만 발행할 수 있다. 후속 공정은 이전 공정의
                    # 정상 분석 결과를 받은 뒤 Consumer가 READY로 전환한다.
                    "dispatch_status": initial_dispatch_status(process_code),
                    "analysis_status": "NOT_ANALYZED",
                    "retry_count": 0,
                    "error_message": None,
                }

    def _build_event(
        self,
        *,
        event_id: str,
        event_time: datetime,
        process_code: str,
        global_index: int,
        production_sequence_index: int,
        car_id: str,
        car_master_id: int,
        is_abnormal: bool,
        equipment_map: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        meta = PROCESS_META[process_code]
        # 같은 차량도 공정마다 서로 다른 설비를 사용할 수 있도록 차량 PK와
        # 공정 코드를 함께 사용해 1~5호기를 독립적으로 배정한다.
        # 해시 기반이라 분포는 랜덤하지만 재생성·템플릿 replay 결과는 동일하다.
        line_station_no = _equipment_no_for_process(
            car_master_id=car_master_id,
            process_code=process_code,
        )
        equipment_code = f"EQ_{process_code}_{line_station_no:03d}"
        equipment_row = equipment_map[equipment_code]

        # 공개 데이터셋의 실제 행을 읽어 원천 추적 정보와 기본 센서 특성을 만든다.
        forming = self._forming_row(production_sequence_index)
        current = self._current_features(process_code, global_index)
        ford = self._ford_features(global_index)
        vision = self._vision_features(global_index)
        bosch = self._bosch_features(global_index)
        process_metrics = self._process_metrics(
            process_code=process_code,
            global_index=global_index,
            current=current,
            ford=ford,
            vision=vision,
            bosch=bosch,
        )
        # 원천 데이터의 실제 클래스 비율은 데이터셋마다 다르므로, PRD가 요구한
        # 7:3 비율과 Java 판정 임계값을 안정적으로 만족하도록 프로필을 적용한다.
        self._apply_detection_profile(
            process_code=process_code,
            is_abnormal=is_abnormal,
            current=current,
            ford=ford,
            vision=vision,
            bosch=bosch,
            process_metrics=process_metrics,
        )
        process_data = self._process_data(
            process_code=process_code,
            car_master_id=car_master_id,
            current=current,
            ford=ford,
            vision=vision,
            bosch=bosch,
            process_metrics=process_metrics,
        )
        validate_process_data(process_code, process_data)
        equipment_status = _equipment_status_for_event(
            car_master_id=car_master_id,
            process_code=process_code,
            is_abnormal=is_abnormal,
        )

        return {
            "event": {
                "eventId": event_id,
                # 원천 이벤트 생성 시점에는 실제 발생 시각이 확정되지 않았으므로
                "eventTime": event_time.isoformat(),
                "eventType": meta["eventType"],
                "eventName": meta["eventName"],
            },
            "equipment": {
                "equipmentCode": equipment_code,
                "equipmentName": equipment_row["equipment_name"],
                "equipmentType": equipment_row["equipment_type"],
            },
            "equipmentStatus": {
                # 운전 상태는 정상/이상 프로필 안에서 차량·공정별로 랜덤 생성한다.
                # 시간 필드는 실제 이벤트 전송 전까지 미확정이므로 NULL로 둔다.
                "operationStatus": equipment_status["operationStatus"],
                "lastNormalTime": (
                    (event_time - timedelta(seconds=31)).isoformat()
                    if is_abnormal
                    else None
                ),
                "statusChangedTime": event_time.isoformat() if is_abnormal else None,
            },
            "product": {
                "carMasterId": car_master_id,
            },
            "sensor": self._sensor_payload(current, ford, vision, process_code),
            "processMetrics": process_metrics,
            "sourceTrace": {
                "fordRowId": ford["rowId"],
                "formingRowId": forming["idx"],
                "robotArmVibrationRowId": current["rowId"],
                "machineVisionRowId": vision["rowId"],
                "boschId": bosch["id"],
            },
            # process_code에 해당하는 블록 하나만 포함한다.
            # PRESS면 press, BODY면 body, PAINT면 paint, ASSEMBLY면 assembly만 존재한다.
            "processData": process_data,
        }

    def _apply_detection_profile(
        self,
        *,
        process_code: str,
        is_abnormal: bool,
        current: dict[str, Any],
        ford: dict[str, Any],
        vision: dict[str, Any],
        bosch: dict[str, Any],
        process_metrics: dict[str, Any],
    ) -> None:
        """PRD의 Java 판정식에서 정상/이상이 명확히 갈리도록 입력값을 보정한다.

        이상 프로필은 전류·진동·열화상·지연·조립 오류를 함께 높여 모든 공정의
        전용 위험도가 임계값을 넘게 한다. 정상 프로필은 반대로 충분한 여유를 둬
        날짜별 작은 변동이 적용되어도 정상 범위를 벗어나지 않게 한다.
        """
        target = float(PROCESS_META[process_code]["targetCycleTimeSec"])
        if is_abnormal:
            # 설비 위험 및 불량 전이 위험을 높이는 공통 센서 프로필.
            current.update(
                rmsAmpere=4.2,
                maxAmpere=4.8,
                minAmpere=3.7,
                accelerationG=0.085,
            )
            ford.update(
                label=-1,
                vibrationScore=0.90,
                vibrationRms=2.88,
                vibrationPeak=4.10,
            )
            vision.update(
                label=1,
                avgTemperature=54.0,
                maxTemperature=62.0,
                minTemperature=45.0,
                thermalStdTemp=6.0,
                defectScore=0.90,
                thicknessValue=132.0,
                surfaceQualityScore=60.0,
            )
            bosch["response"] = 1
            # 병목 위험도도 함께 높아지도록 지연, WIP, 유휴 시간을 보정한다.
            process_metrics.update(
                cycleTimeSec=target + 16.0,
                waitingTimeSec=18.0,
                processingTimeSec=target - 2.0,
                stationDelaySec=16.0,
                throughputPerMin=round(60 / (target + 16.0), 3),
                queueLength=14,
                wipCount=38,
                equipmentIdleTimeSec=22.0,
            )
            return

        # 정상 프로필은 PRD 기준 cycle time과 낮은 센서 위험도를 사용한다.
        current.update(
            rmsAmpere=1.7,
            maxAmpere=1.9,
            minAmpere=1.5,
            accelerationG=0.006,
        )
        ford.update(
            label=1,
            vibrationScore=0.12,
            vibrationRms=0.32,
            vibrationPeak=0.58,
        )
        vision.update(
            label=0,
            avgTemperature=39.0,
            maxTemperature=41.0,
            minTemperature=37.0,
            thermalStdTemp=0.8,
            defectScore=0.08,
            thicknessValue=116.0,
            surfaceQualityScore=97.0,
        )
        bosch["response"] = 0
        process_metrics.update(
            cycleTimeSec=target,
            waitingTimeSec=2.0,
            processingTimeSec=target - 2.0,
            stationDelaySec=0.0,
            throughputPerMin=round(60 / target, 3),
            queueLength=1,
            wipCount=4,
            equipmentIdleTimeSec=0.0,
        )

    def _forming_row(self, index: int) -> dict[str, Any]:
        df = self._load_forming()
        row = df.iloc[index % len(df)]
        return {
            "idx": _safe_int(row.get("idx"), index + 1),
            "itemno": _safe_str(row.get("itemno"), "76211-A3010-100"),
            "quantity": _safe_int(row.get("quantity"), 1),
            "cnt": _safe_int(row.get("cnt"), 18000000 + index),
        }

    def _current_features(self, process_code: str, index: int) -> dict[str, Any]:
        if process_code == "BODY":
            df = self._load_robot_current(index)
            source_type = "ROBOT_CURRENT"
            file_index = index % 2 + 1
        else:
            df = self._load_press_current(index)
            source_type = "PRESS_CURRENT"
            file_index = index % 4 + 1

        row_index = index % len(df)
        cache_key = ("current", process_code, file_index, row_index, index % 7)
        if cache_key in self._feature_cache:
            return self._feature_cache[cache_key]

        row = df.iloc[row_index]
        value_column = "RMS[A]" if "RMS[A]" in df.columns else "Acceleration[g]"
        raw_value = _safe_float(row.get(value_column), 0.0)
        if value_column == "Acceleration[g]":
            rms_ampere = 1.65 + abs(raw_value) * 15
            acceleration_g = raw_value
        else:
            rms_ampere = raw_value if raw_value > 0.01 else 1.75 + (index % 17) * 0.04
            acceleration_g = abs(rms_ampere - 1.9) / 18

        spread = 0.11 + (index % 7) * 0.012
        result = {
            "rowId": _safe_int(row.get("Unnamed: 0"), index + 1),
            "sourceType": source_type,
            "rmsAmpere": round(rms_ampere, 9),
            "maxAmpere": round(rms_ampere + spread, 9),
            "minAmpere": round(max(0.0, rms_ampere - spread), 9),
            "accelerationG": round(acceleration_g, 9),
        }
        self._feature_cache[cache_key] = result
        return result

    def _ford_features(self, index: int) -> dict[str, Any]:
        rows = self._load_ford_rows()
        row_index = index % len(rows)
        cache_key = ("ford", row_index)
        if cache_key in self._feature_cache:
            return self._feature_cache[cache_key]

        values = rows[row_index]
        label = int(values[0])
        signal = [float(value) for value in values[1:501]]
        abs_values = [abs(value) for value in signal]
        rms = math.sqrt(sum(value * value for value in signal) / len(signal))
        peak = max(abs_values)
        bands = _frequency_bands(signal)
        peak_band = max(bands, key=bands.get)
        result = {
            "rowId": row_index + 1,
            "label": label,
            "vibrationRms": round(rms, 9),
            "vibrationPeak": round(peak, 9),
            "vibrationScore": round(min(0.99, rms / 3.2), 6),
            "frequencyBands": bands,
            "frequencyPeakBand": peak_band.replace("freq_", "").upper(),
        }
        self._feature_cache[cache_key] = result
        return result

    def _vision_features(self, index: int) -> dict[str, Any]:
        side = "left" if index % 2 == 0 else "right"
        df, labels = self._load_vision(side)
        row_index = index % len(df)
        cache_key = ("vision", side, row_index, index % 17)
        if cache_key in self._feature_cache:
            return self._feature_cache[cache_key]

        row = pd.to_numeric(df.iloc[row_index], errors="coerce").dropna().tolist()
        label = int(float(labels[row_index % len(labels)]))
        avg_temp = statistics.fmean(row)
        max_temp = max(row)
        min_temp = min(row)
        std_temp = statistics.pstdev(row) if len(row) > 1 else 0.0
        defect_score = min(0.99, (std_temp / 5.0) + (0.35 if label else 0.05))
        result = {
            "rowId": row_index + 1,
            "imagePosition": "LEFT" if side == "left" else "RIGHT",
            "label": label,
            "avgTemperature": round(avg_temp, 3),
            "maxTemperature": round(max_temp, 3),
            "minTemperature": round(min_temp, 3),
            "thermalStdTemp": round(std_temp, 3),
            "defectScore": round(defect_score, 4),
            "thicknessValue": round(112.0 + (index % 17) * 0.7 + std_temp, 3),
            "surfaceQualityScore": round(max(0.0, 100.0 - defect_score * 32), 3),
        }
        self._feature_cache[cache_key] = result
        return result

    def _bosch_features(self, index: int) -> dict[str, Any]:
        df = self._load_bosch_numeric()
        row_index = index % len(df)
        cache_key = ("bosch", row_index)
        if cache_key in self._feature_cache:
            return self._feature_cache[cache_key]

        row = df.iloc[row_index]
        numeric_values = pd.to_numeric(row.drop(labels=["Id", "Response"], errors="ignore"), errors="coerce")
        numeric_values = numeric_values.dropna().tolist()
        response = _safe_int(row.get("Response"), 0)
        mean_value = statistics.fmean(numeric_values) if numeric_values else 0.0
        result = {
            "id": _safe_int(row.get("Id"), index + 1),
            "response": response,
            "meanNumericFeature": round(mean_value, 6),
            "nonNullFeatureCount": len(numeric_values),
        }
        self._feature_cache[cache_key] = result
        return result

    def _process_metrics(
        self,
        *,
        process_code: str,
        global_index: int,
        current: dict[str, Any],
        ford: dict[str, Any],
        vision: dict[str, Any],
        bosch: dict[str, Any],
    ) -> dict[str, Any]:
        target = float(PROCESS_META[process_code]["targetCycleTimeSec"])
        anomaly_weight = 0.0
        if process_code in {"PRESS", "BODY"}:
            anomaly_weight = ford["vibrationScore"] * 12
        elif process_code == "PAINT":
            anomaly_weight = vision["defectScore"] * 10
        else:
            anomaly_weight = bosch["response"] * 9 + (global_index % 4)

        current_weight = min(6.0, abs(current["rmsAmpere"] - 1.9) * 1.5)
        cycle_time = target + anomaly_weight + current_weight + (global_index % 5) * 0.3
        processing_time = max(1.0, cycle_time - (5.0 + global_index % 6))
        waiting_time = max(0.5, cycle_time - processing_time + (global_index % 3))
        station_delay = max(0.0, cycle_time - target)
        throughput = round(60 / cycle_time, 3)
        return {
            "cycleTimeSec": round(cycle_time, 3),
            "waitingTimeSec": round(waiting_time, 3),
            "processingTimeSec": round(processing_time, 3),
            "stationDelaySec": round(station_delay, 3),
            "throughputPerMin": throughput,
            "queueLength": int(3 + station_delay // 2 + global_index % 4),
            "wipCount": int(16 + station_delay // 1.5 + global_index % 9),
            "equipmentIdleTimeSec": round(max(0.0, station_delay * 1.8), 3),
        }

    def _sensor_payload(
        self,
        current: dict[str, Any],
        ford: dict[str, Any],
        vision: dict[str, Any],
        process_code: str,
    ) -> dict[str, Any]:
        return {
            "sensorType": "MULTI_SENSOR",
            "current": {
                "rmsAmpere": current["rmsAmpere"],
                "maxAmpere": current["maxAmpere"],
                "minAmpere": current["minAmpere"],
            },
            "vibration": {
                "accelerationG": current["accelerationG"],
                "vibrationScore": ford["vibrationScore"],
                "vibrationRms": ford["vibrationRms"],
                "vibrationPeak": ford["vibrationPeak"],
            },
            "robotArmVibration": {
                "robotId": "ROBOT_ARM_01",
                "axis": f"J{(current['rowId'] % 6) + 1}",
                "frequencyHz": round(40.0 + ford["vibrationScore"] * 220, 3),
                "amplitude": round(ford["vibrationRms"] / 1000, 9),
                "vibrationRms": round(ford["vibrationRms"] / 900, 9),
                "vibrationPeak": round(ford["vibrationPeak"] / 700, 9),
                "vibrationScore": ford["vibrationScore"],
            },
            "thermal": {
                "thermalScore": vision["avgTemperature"],
                "avgTemperature": vision["avgTemperature"],
                "maxTemperature": vision["maxTemperature"],
                "minTemperature": vision["minTemperature"],
            },
        }

    def _process_data(
        self,
        *,
        process_code: str,
        car_master_id: int,
        current: dict[str, Any],
        ford: dict[str, Any],
        vision: dict[str, Any],
        bosch: dict[str, Any],
        process_metrics: dict[str, Any],
    ) -> dict[str, Any]:
        if process_code == "PRESS":
            return {
                "press": {
                    "countIncreaseYn": process_metrics["equipmentIdleTimeSec"] < 18,
                    "targetCycleTimeSec": PROCESS_META["PRESS"]["targetCycleTimeSec"],
                    "timestampDelaySec": process_metrics["stationDelaySec"],
                },
            }
        if process_code == "BODY":
            return {
                "body": {
                    "robotMotionStatus": "WARNING" if ford["label"] < 0 else "NORMAL",
                    "robotOperationMode": "AUTO",
                    "frequencyPeakBand": ford["frequencyPeakBand"],
                    "frequencyBands": ford["frequencyBands"],
                },
            }
        if process_code == "PAINT":
            label = "DEFECT" if vision["label"] else "NORMAL"
            return {
                "paint": {
                    "imagePosition": vision["imagePosition"],
                    "thermalStdTemp": vision["thermalStdTemp"],
                    "thicknessValue": vision["thicknessValue"],
                    "defectScore": vision["defectScore"],
                    "visionLabel": label,
                    "surfaceQualityScore": vision["surfaceQualityScore"],
                },
            }

        if process_code == "ASSEMBLY":
            has_sequence_error = bool(bosch["response"])
            expected_sequence = _equipment_route_for_car(car_master_id)
            expected_steps = expected_sequence.split(">")
            # 이상 데이터는 BODY와 PAINT 통과 순서를 바꿔 순서 오류를 표현한다.
            abnormal_steps = [
                expected_steps[0],
                expected_steps[2],
                expected_steps[1],
                expected_steps[3],
            ]
            return {
                "assembly": {
                    "expectedSequence": expected_sequence,
                    "actualSequence": (
                        ">".join(abnormal_steps)
                        if has_sequence_error
                        else expected_sequence
                    ),
                    "missingPartCount": (
                        1
                        if has_sequence_error and current["rmsAmpere"] > 2.3
                        else 0
                    ),
                    "fasteningErrorCount": 1 if has_sequence_error else 0,
                    "sequenceErrorCount": 1 if has_sequence_error else 0,
                },
            }
        raise ValueError(f"지원하지 않는 process_code입니다: {process_code}")

    def _load_forming(self) -> pd.DataFrame:
        return self._cached_csv(
            "forming",
            self.dataset_root / "소성가공 자원최적화 AI 데이터셋" / "공정_데이터_2022년_8월.csv",
            nrows=4096,
        )

    def _load_press_current(self, index: int) -> pd.DataFrame:
        file_index = index % 4 + 1
        return self._cached_csv(
            f"press_current_{file_index}",
            self.dataset_root
            / "소성가공 자원최적화 AI 데이터셋"
            / f"프레스_{file_index}호-유압모터_전류데이터.csv",
            nrows=4096,
        )

    def _load_robot_current(self, index: int) -> pd.DataFrame:
        file_index = index % 2 + 1
        return self._cached_csv(
            f"robot_current_{file_index}",
            self.dataset_root
            / "소성가공 자원최적화 AI 데이터셋"
            / f"로봇_{file_index}호-전류_데이터.csv",
            nrows=4096,
        )

    def _load_vision(self, side: str) -> tuple[pd.DataFrame, list[float]]:
        cache_key = f"vision_{side}"
        if cache_key not in self._cache:
            base = self.dataset_root / "머신비전 AI 데이터셋 (열화상 기반 품질 검사 데이터)"
            df = pd.read_csv(base / f"2nd_process_{side}_data.csv", nrows=4096)
            with (base / f"2nd_process_{side}_label.json").open(
                encoding="utf-8",
            ) as label_file:
                labels = json.load(label_file)
            self._cache[cache_key] = (df, labels)
        return self._cache[cache_key]

    def _load_bosch_numeric(self) -> pd.DataFrame:
        return self._cached_csv(
            "bosch_numeric",
            self.dataset_root / "bosch-production-line-performance" / "train_numeric.csv",
            nrows=4096,
        )

    def _load_ford_rows(self) -> list[list[float]]:
        if "ford_train" not in self._cache:
            base = self.dataset_root / "Ford 엔진 진동 데이터셋"
            path = base / "FordA_TRAIN.txt"
            rows: list[list[float]] = []
            with path.open(encoding="utf-8", errors="ignore") as file:
                for line in file:
                    line = line.replace("\x00", " ").strip()
                    if not line:
                        continue
                    values = [float(part) for part in line.split()]
                    if len(values) >= 501:
                        rows.append(values[:501])
                    if len(rows) >= 4096:
                        break
            if not rows:
                rows = self._load_ford_arff_rows(base / "FordA_TRAIN.arff")
            self._cache["ford_train"] = rows
        return self._cache["ford_train"]

    def _load_ford_arff_rows(self, path: Path) -> list[list[float]]:
        rows: list[list[float]] = []
        in_data = False
        with path.open(encoding="utf-8", errors="ignore") as file:
            for line in file:
                stripped = line.strip()
                if not stripped or stripped.startswith("%"):
                    continue
                if stripped.lower() == "@data":
                    in_data = True
                    continue
                if not in_data:
                    continue
                values = [float(part) for part in stripped.split(",")]
                if len(values) >= 501:
                    # ARFF stores class as the last value; TXT stores class first.
                    rows.append([values[-1], *values[:500]])
                if len(rows) >= 4096:
                    break
        return rows

    def _cached_csv(self, cache_key: str, path: Path, *, nrows: int) -> pd.DataFrame:
        if cache_key not in self._cache:
            self._cache[cache_key] = pd.read_csv(path, nrows=nrows)
        return self._cache[cache_key]


def _frequency_bands(signal: list[float]) -> dict[str, float]:
    band_names = [
        "freq_0_100_hz",
        "freq_101_200_hz",
        "freq_201_300_hz",
        "freq_301_400_hz",
        "freq_401_500_hz",
        "freq_501_600_hz",
        "freq_601_700_hz",
        "freq_701_800_hz",
        "freq_801_900_hz",
        "freq_901_1000_hz",
        "freq_1001_1100_hz",
        "freq_1101_1200_hz",
        "freq_1201_1300_hz",
        "freq_1301_1400_hz",
        "freq_1401_1500_hz",
        "freq_1501_1600_hz",
    ]
    chunk_size = max(1, len(signal) // len(band_names))
    bands: dict[str, float] = {}
    for index, band_name in enumerate(band_names):
        chunk = signal[index * chunk_size : (index + 1) * chunk_size]
        if not chunk:
            bands[band_name] = 0.0
            continue
        rms = math.sqrt(sum(value * value for value in chunk) / len(chunk))
        bands[band_name] = round(rms / 700, 9)
    return bands


def _event_time_for_slot(
    *,
    current_date: date,
    slot: int,
    events_per_day: int,
) -> datetime:
    day_start = datetime.combine(current_date, time.min)
    offset_microseconds = _weighted_event_offset_us(slot, events_per_day)
    return day_start + timedelta(microseconds=offset_microseconds)


def _event_time_for_range_slot(
    *,
    start_date: date,
    end_date: date,
    slot: int,
    total_events: int,
) -> datetime:
    """전체 이벤트를 날짜 범위에 균등 분할한 뒤 일별 생산 밀도를 적용한다."""
    days = (end_date - start_date).days + 1
    # 나머지는 앞 날짜부터 한 건씩 배분해 전체 건수가 정확히 유지되게 한다.
    base_count, remainder = divmod(total_events, days)
    day_offset = 0
    day_start_slot = 0
    for candidate_day in range(days):
        events_on_day = base_count + (1 if candidate_day < remainder else 0)
        if slot < day_start_slot + events_on_day:
            day_offset = candidate_day
            local_slot = slot - day_start_slot
            return _event_time_for_slot(
                current_date=start_date + timedelta(days=day_offset),
                slot=local_slot,
                events_per_day=events_on_day,
            )
        day_start_slot += events_on_day
    return datetime.combine(end_date, time.max)


def _weighted_event_offset_us(slot: int, events_per_day: int) -> int:
    """일별 slot을 시간대별 생산 밀도에 맞는 microsecond offset으로 변환한다."""
    windows = _weighted_event_windows(events_per_day)
    remaining_slot = slot
    for start_us, end_us, count in windows:
        if remaining_slot >= count:
            remaining_slot -= count
            continue
        span_us = end_us - start_us
        if count <= 1:
            return start_us + span_us // 2
        base_offset = int(remaining_slot * span_us / count)
        interval_us = max(1, span_us // count)
        jitter_us = _deterministic_jitter_us(slot, interval_us)
        return min(end_us - 1, max(start_us, start_us + base_offset + jitter_us))
    return 24 * 60 * 60 * 1_000_000 - 1


def _weighted_event_windows(events_per_day: int) -> list[tuple[int, int, int]]:
    raw_weights = [
        ((end_hour - start_hour) * density, start_hour, end_hour)
        for start_hour, end_hour, density in EVENT_DENSITY_WINDOWS
    ]
    total_weight = sum(weight for weight, _, _ in raw_weights)
    counts = [
        max(1, int(events_per_day * weight / total_weight))
        for weight, _, _ in raw_weights
    ]
    while sum(counts) > events_per_day:
        largest_index = max(range(len(counts)), key=counts.__getitem__)
        counts[largest_index] -= 1
    while sum(counts) < events_per_day:
        largest_fraction_index = _largest_fraction_window_index(
            raw_weights=raw_weights,
            counts=counts,
            events_per_day=events_per_day,
            total_weight=total_weight,
        )
        counts[largest_fraction_index] += 1

    windows: list[tuple[int, int, int]] = []
    for count, (_, start_hour, end_hour) in zip(counts, raw_weights):
        windows.append(
            (
                start_hour * 60 * 60 * 1_000_000,
                end_hour * 60 * 60 * 1_000_000,
                count,
            ),
        )
    return windows


def _largest_fraction_window_index(
    *,
    raw_weights: list[tuple[float, int, int]],
    counts: list[int],
    events_per_day: int,
    total_weight: float,
) -> int:
    fractions = [
        events_per_day * weight / total_weight - count
        for count, (weight, _, _) in zip(counts, raw_weights)
    ]
    return max(range(len(fractions)), key=fractions.__getitem__)


def _deterministic_jitter_us(slot: int, interval_us: int) -> int:
    """재실행 결과는 같게 유지하면서 이벤트 시각의 기계적인 등간격을 완화한다."""
    jitter_window = max(1, interval_us // 5)
    pseudo_random = (slot * 1103515245 + 12345) & 0x7FFFFFFF
    return pseudo_random % (2 * jitter_window + 1) - jitter_window


def _equipment_no_for_process(*, car_master_id: int, process_code: str) -> int:
    """차량·공정 조합별로 독립적인 설비 번호(1~5)를 결정한다.

    일반 random 모듈을 사용하면 배치를 다시 실행할 때 설비가 달라질 수 있다.
    이벤트 재생성과 upsert가 안정적으로 동작하도록 같은 입력에는 항상 같은
    번호가 나오는 해시 기반 결정적 랜덤 방식을 사용한다.
    """
    digest = hashlib.blake2b(
        f"{car_master_id}:{process_code}:equipment".encode("utf-8"),
        digest_size=8,
    ).digest()
    return int.from_bytes(digest, "big") % LINE_STATION_COUNT + 1


def _equipment_route_for_car(car_master_id: int) -> str:
    """차량이 통과할 4개 공정의 실제 설비 경로를 문자열로 만든다.

    예: PRESS 2호, BODY 3호, PAINT 1호, ASSEMBLY 4호
    -> P02>B03>PA01>A04
    """
    return ">".join(
        (
            f"{PROCESS_ROUTE_PREFIX[process_code]}"
            f"{_equipment_no_for_process(car_master_id=car_master_id, process_code=process_code):02d}"
        )
        for process_code in PROCESS_SEQUENCE
    )


def _equipment_status_for_event(
    *,
    car_master_id: int,
    process_code: str,
    is_abnormal: bool,
) -> dict[str, str]:
    """정상/이상 유형에 맞는 설비 상태를 결정적 랜덤으로 선택한다.

    정상 데이터는 RUNNING/IDLE, 이상 데이터는 FAULT/STOPPED/MAINTENANCE
    상태군에서 선택한다. 같은 차량·공정은 재생성해도 같은 상태를 갖는다.
    """
    digest = hashlib.blake2b(
        f"{car_master_id}:{process_code}:status".encode("utf-8"),
        digest_size=8,
    ).digest()
    ratio = int.from_bytes(digest, "big") % 100

    if not is_abnormal:
        operation_status = "RUNNING" if ratio < 85 else "IDLE"
    elif ratio < 50:
        operation_status = "FAULT"
    elif ratio < 80:
        operation_status = "STOPPED"
    else:
        operation_status = "MAINTENANCE"

    return {"operationStatus": operation_status}


def validate_process_data(
    process_code: str,
    process_data: dict[str, Any],
) -> None:
    """processData가 공정별 전용 JSON 구조를 정확히 따르는지 검증한다."""
    expected_key = PROCESS_DATA_KEY.get(process_code)
    required_fields = PROCESS_DATA_REQUIRED_FIELDS.get(process_code)
    if expected_key is None or required_fields is None:
        raise ValueError(f"지원하지 않는 process_code입니다: {process_code}")

    if set(process_data) != {expected_key}:
        raise ValueError(
            f"{process_code} processData는 {expected_key} 블록만 포함해야 합니다: "
            f"actual={sorted(process_data)}",
        )

    process_payload = process_data.get(expected_key)
    if not isinstance(process_payload, dict):
        raise ValueError(
            f"{process_code} processData.{expected_key}는 JSON object여야 합니다.",
        )

    missing_fields = required_fields - set(process_payload)
    if missing_fields:
        raise ValueError(
            f"{process_code} processData 필수 필드가 누락되었습니다: "
            f"{sorted(missing_fields)}",
        )


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(result) or math.isinf(result):
        return default
    return result


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_str(value: Any, default: str) -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text and text.lower() != "nan" else default


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    return value
