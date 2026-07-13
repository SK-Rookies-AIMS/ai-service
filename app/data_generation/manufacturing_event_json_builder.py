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


# PRD??李⑤웾 ?앹궛 ?쒖꽌?? ??李⑤웾留덈떎 ?꾨옒 4媛?怨듭젙 ?대깽?몃? ?뺥솗????嫄댁뵫 留뚮뱺??
PROCESS_SEQUENCE = ("PRESS", "BODY", "PAINT", "ASSEMBLY")
PROCESS_ROUTE_PREFIX = {
    "PRESS": "P",
    "BODY": "B",
    "PAINT": "PA",
    "ASSEMBLY": "A",
}

# PRD 湲곗? 怨듭젙蹂??ㅻ퉬 ?섏? 紐⑺몴 ?댁긽 ?곗씠??鍮꾩쑉?대떎.
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
    """?먯쿇 ?대깽??理쒖큹 諛쒗뻾 ?곹깭瑜?諛섑솚?쒕떎.

    李⑤웾 ?앹궛 ?먮쫫? PRESS遺???쒖옉?섎?濡?PRESS留?READY?대ŉ, BODY/PAINT/
    ASSEMBLY???댁쟾 怨듭젙???뺤긽 遺꾩꽍 寃곌낵媛 ?ㅺ린 ?꾧퉴吏 PENDING?대떎.
    """
    return "READY" if process_code == "PRESS" else "PENDING"


def is_abnormal_operation_status(operation_status: Any) -> bool:
    """?대깽??JSON???댁쟾 ?곹깭媛 ?댁긽 ?곹깭?몄? 諛섑솚?쒕떎."""
    return operation_status in {"FAULT", "STOPPED"}


def _press_count_increase_flag(
    *,
    station_delay_sec: float,
    equipment_idle_time_sec: float,
) -> bool | None:
    """?꾨젅???앹궛 移댁슫??利앷? ?щ?瑜??뺤긽/寃쎄퀬/?꾪뿕 湲곗??쇰줈 遺꾨━?쒕떎."""
    if station_delay_sec <= 2.0 and equipment_idle_time_sec < 6.0:
        return True
    if station_delay_sec <= 3.0:
        return None
    return False


def _body_robot_motion_status(vibration_score: float) -> str:
    """李⑥껜 濡쒕큸 吏꾨룞 ?먯닔瑜?諛뷀깢?쇰줈 ?곹깭瑜??먯젙?쒕떎."""
    if vibration_score >= 0.45:
        return "ABNORMAL"
    if vibration_score >= 0.40:
        return "WARNING"
    return "NORMAL"


def _body_robot_operation_mode(vibration_score: float) -> str:
    """李⑥껜 濡쒕큸 ?댁쟾 紐⑤뱶瑜?吏꾨룞 ?먯닔 湲곗??쇰줈 援ъ꽦?쒕떎."""
    return "STOPPED" if vibration_score >= 0.45 else "AUTO"


def normalize_event_json(event_json: dict[str, Any]) -> dict[str, Any]:
    """?쒖“ ?대깽??JSON??PRD???뺤쓽???꾨뱶留??④릿 援ъ“濡??뺢퇋?뷀븳??"""
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
        "eventName": "도장 공정 비전 관제 이벤트",
        "targetCycleTimeSec": 64.0,
    },
    "ASSEMBLY": {
        "equipmentType": "CONVEYOR",
        "eventType": "PROCESS_STATUS",
        "eventName": "조립 공정 통합 관제 이벤트",
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
    """??踰덉쓽 ?쒖“ ?대깽???앹꽦 ?묒뾽???꾩슂??踰붿쐞? 留덉뒪???곗씠??"""

    start_date: date
    end_date: date
    # 湲곗〈 API ?대쫫???좎??섏?留??꾩옱 ?섎???湲곌컙 ?꾩껜 ?대깽???섎떎.
    events_per_day: int
    # dict ?쎌엯 ?쒖꽌媛 car_master.id ?ㅻ쫫李⑥닚??蹂댁〈?쒕떎.
    car_id_map: dict[str, int]
    equipment_map: dict[str, dict[str, Any]]


class ManufacturingEventJsonBuilder:
    """CSV ?먯쿇 ?곗씠?곕? PRD???듯빀 ?쒖“ ?대깽??JSON?쇰줈 蹂?섑븳??"""

    def __init__(self, dataset_root: Path = DATASET_ROOT) -> None:
        self.dataset_root = dataset_root
        self._cache: dict[str, Any] = {}
        self._feature_cache: dict[tuple[Any, ...], dict[str, Any]] = {}

    def build_rows(self, request: EventBuildRequest) -> list[dict[str, Any]]:
        return list(self.iter_rows(request))

    def iter_rows(self, request: EventBuildRequest) -> Iterator[dict[str, Any]]:
        """李⑤웾 ?⑥쐞濡?4怨듭젙 row瑜??쒖감 ?앹꽦?쒕떎."""
        # Repository媛 car_master.id ?ㅻ쫫李⑥닚?쇰줈 留뚮뱺 ?쒖꽌瑜?洹몃?濡??ъ슜?댁빞
        # PRD??"id 1踰?李⑤웾遺???앹꽦" 議곌굔??吏?????덈떎.
        car_ids = list(request.car_id_map)
        if not car_ids:
            return

        days = (request.end_date - request.start_date).days + 1
        if days <= 0:
            return

        # 李⑤웾留덈떎 4嫄댁씠誘濡??대깽???섏? 李⑤웾 ?섎? ?낅┰?곸쑝濡?諛쏆쓣 ???녿떎.
        # ?ш린???ㅼ떆 寃利앺빐 ?쒕퉬???몃??먯꽌 Builder瑜?吏곸젒 ?몄텧?대룄 援ъ“媛 源⑥?吏 ?딄쾶 ?쒕떎.
        total_events = len(car_ids) * len(PROCESS_SEQUENCE)
        if request.events_per_day != total_events:
            raise ValueError(
                "event_count??car_pool_size * 4? 媛숈븘???⑸땲?? "
                f"event_count={request.events_per_day}, expected={total_events}",
            )

        # ?좎쭨蹂??ㅼ젣 李⑤웾 ?섎? 湲곗??쇰줈 ?뺤긽 70% / ?먭린(?댁긽) 30%瑜?諛곗튂?쒕떎.
        # ?댁긽 李⑤웾??4怨듭젙??紐⑤몢 媛吏誘濡?怨듭젙蹂??댁긽 嫄댁닔媛 ?먮룞?쇰줈 ?숈씪?댁쭊??
        abnormal_vehicle_count = round(len(car_ids) * ABNORMAL_RATIO)
        normal_vehicle_count = len(car_ids) - abnormal_vehicle_count

        for production_sequence_index, car_id in enumerate(car_ids):
            car_master_id = request.car_id_map[car_id]
            is_abnormal_vehicle = production_sequence_index >= normal_vehicle_count
            # 媛숈? 李⑤웾???대깽?몃뒗 PRESS -> BODY -> PAINT -> ASSEMBLY ?쒖꽌瑜??좎??쒕떎.
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
                event_date = (
                    _production_date_from_vehicle_id(car_id) or event_time.date()
                )
                event_id = f"EVT-{event_date:%Y%m%d}-{global_index + 1:06d}"
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
                    # 理쒖큹?먮뒗 PRESS留?諛쒗뻾?????덈떎. ?꾩냽 怨듭젙? ?댁쟾 怨듭젙??
                    # ?뺤긽 遺꾩꽍 寃곌낵瑜?諛쏆? ??Consumer媛 READY濡??꾪솚?쒕떎.
                    "dispatch_status": initial_dispatch_status(process_code),
                    "analysis_status": "NOT_ANALYZED",
                    "bottleneck_analysis_done": False,
                    "defect_transfer_analysis_done": False,
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
        # ?ъ슜?먯쓽 ?붿껌???곕씪 PRESS???댁긽 鍮덈룄瑜???텛怨?PAINT, ASSEMBLY???믪씤??
        # global_index瑜??쒖슜??寃곗젙濡좎쟻(deterministic) ?쒖닔瑜??앹꽦?쒕떎.
        if is_abnormal:
            if process_code == "PRESS":
                # PRESS???먮옒 ?댁긽??20%留??좎?
                is_abnormal = (global_index % 10) < 2
            elif process_code in {"PAINT", "ASSEMBLY"}:
                # PAINT, ASSEMBLY???먮옒 ?댁긽??80%瑜??좎? (鍮덈룄 ?믪엫)
                is_abnormal = (global_index % 10) < 8

        meta = PROCESS_META[process_code]
        # 媛숈? 李⑤웾??怨듭젙留덈떎 ?쒕줈 ?ㅻⅨ ?ㅻ퉬瑜??ъ슜?????덈룄濡?李⑤웾 PK?
        # 怨듭젙 肄붾뱶瑜??④퍡 ?ъ슜??1~5?멸린瑜??낅┰?곸쑝濡?諛곗젙?쒕떎.
        # ?댁떆 湲곕컲?대씪 遺꾪룷???쒕뜡?섏?留??ъ깮?굿룻뀥?뚮┸ replay 寃곌낵???숈씪?섎떎.
        line_station_no = _equipment_no_for_process(
            car_master_id=car_master_id,
            process_code=process_code,
        )
        equipment_code = f"EQ_{process_code}_{line_station_no:03d}"
        equipment_row = equipment_map[equipment_code]

        # 怨듦컻 ?곗씠?곗뀑???ㅼ젣 ?됱쓣 ?쎌뼱 ?먯쿇 異붿쟻 ?뺣낫? 湲곕낯 ?쇱꽌 ?뱀꽦??留뚮뱺??
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
        # ?먯쿇 ?곗씠?곗쓽 ?ㅼ젣 ?대옒??鍮꾩쑉? ?곗씠?곗뀑留덈떎 ?ㅻⅤ誘濡? PRD媛 ?붽뎄??
        # 7:3 鍮꾩쑉怨?Java ?먯젙 ?꾧퀎媛믪쓣 ?덉젙?곸쑝濡?留뚯”?섎룄濡??꾨줈?꾩쓣 ?곸슜?쒕떎.
        self._apply_detection_profile(
            process_code=process_code,
            is_abnormal=is_abnormal,
            global_index=global_index,
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
        operation_status = equipment_status["operationStatus"]
        is_equipment_abnormal = is_abnormal_operation_status(operation_status)

        return {
            "event": {
                "eventId": event_id,
                # ?먯쿇 ?대깽???앹꽦 ?쒖젏?먮뒗 ?ㅼ젣 諛쒖깮 ?쒓컖???뺤젙?섏? ?딆븯?쇰?濡?
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
                # Java enum EquipmentOperationStatus 媛믩쭔 ?ъ슜?쒕떎.
                "operationStatus": operation_status,
                "lastNormalTime": (
                    (event_time - timedelta(seconds=31)).isoformat()
                    if is_equipment_abnormal
                    else None
                ),
                "statusChangedTime": (
                    event_time.isoformat() if is_equipment_abnormal else None
                ),
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
            # process_code???대떦?섎뒗 釉붾줉 ?섎굹留??ы븿?쒕떎.
            # PRESS硫?press, BODY硫?body, PAINT硫?paint, ASSEMBLY硫?assembly留?議댁옱?쒕떎.
            "processData": process_data,
        }

    def _apply_detection_profile(
        self,
        *,
        process_code: str,
        is_abnormal: bool,
        global_index: int,
        current: dict[str, Any],
        ford: dict[str, Any],
        vision: dict[str, Any],
        bosch: dict[str, Any],
        process_metrics: dict[str, Any],
    ) -> None:
        target = float(PROCESS_META[process_code]["targetCycleTimeSec"])
        current_variation = global_index % 11
        vibration_variation = global_index % 13
        paint_variation = global_index % 10
        metric_variation = global_index % 8

        if process_code == "PRESS":
            if is_abnormal:
                rms_ampere = 3.05 + current_variation * 0.16
                acceleration_g = 0.022 + current_variation * 0.0015
                vibration_score = min(0.99, 0.44 + vibration_variation * 0.015)
                vibration_rms = 1.65 + vibration_variation * 0.06
                vibration_peak = 2.45 + vibration_variation * 0.08
                cycle_time = target + 4.5 + metric_variation * 0.95
                station_delay = cycle_time - target

                current.update(
                    rmsAmpere=round(rms_ampere, 3),
                    maxAmpere=round(rms_ampere + 0.38 + current_variation * 0.02, 3),
                    minAmpere=round(max(0.0, rms_ampere - 0.28), 3),
                    accelerationG=round(acceleration_g, 4),
                )
                ford.update(
                    label=1,
                    vibrationScore=round(vibration_score, 4),
                    vibrationRms=round(vibration_rms, 3),
                    vibrationPeak=round(vibration_peak, 3),
                )
                vision.update(
                    label=0,
                    avgTemperature=round(38.2 + paint_variation * 0.12, 3),
                    maxTemperature=round(40.2 + paint_variation * 0.15, 3),
                    minTemperature=round(36.8 + paint_variation * 0.09, 3),
                    thermalStdTemp=round(0.62 + paint_variation * 0.03, 3),
                    defectScore=round(0.05 + paint_variation * 0.006, 4),
                    thicknessValue=round(113.5 + paint_variation * 0.35, 3),
                    surfaceQualityScore=round(98.0 - paint_variation * 0.25, 3),
                )
                bosch["response"] = 0
                process_metrics.update(
                    cycleTimeSec=round(cycle_time, 3),
                    waitingTimeSec=round(5.5 + metric_variation * 0.45, 3),
                    processingTimeSec=round(max(1.0, target - 1.5 + metric_variation * 0.25), 3),
                    stationDelaySec=round(station_delay, 3),
                    throughputPerMin=round(60 / cycle_time, 3),
                    queueLength=6 + metric_variation,
                    wipCount=18 + metric_variation * 2,
                    equipmentIdleTimeSec=round(7.0 + metric_variation * 0.9, 3),
                )
                return

            rms_ampere = 1.68 + current_variation * 0.04
            acceleration_g = 0.004 + current_variation * 0.0006
            vibration_score = 0.14 + vibration_variation * 0.014
            vibration_rms = 0.28 + vibration_variation * 0.025
            vibration_peak = 0.48 + vibration_variation * 0.035
            cycle_time = target + metric_variation * 0.28
            station_delay = max(0.0, cycle_time - target)

            current.update(
                rmsAmpere=round(rms_ampere, 3),
                maxAmpere=round(rms_ampere + 0.17 + current_variation * 0.01, 3),
                minAmpere=round(max(0.0, rms_ampere - 0.15), 3),
                accelerationG=round(acceleration_g, 5),
            )
            ford.update(
                label=1,
                vibrationScore=round(vibration_score, 4),
                vibrationRms=round(vibration_rms, 3),
                vibrationPeak=round(vibration_peak, 3),
            )
            vision.update(
                label=0,
                avgTemperature=round(37.8 + paint_variation * 0.18, 3),
                maxTemperature=round(39.8 + paint_variation * 0.2, 3),
                minTemperature=round(36.5 + paint_variation * 0.12, 3),
                thermalStdTemp=round(0.55 + paint_variation * 0.05, 3),
                defectScore=round(0.03 + paint_variation * 0.006, 4),
                thicknessValue=round(113.0 + paint_variation * 0.4, 3),
                surfaceQualityScore=round(98.6 - paint_variation * 0.2, 3),
            )
            bosch["response"] = 0
            process_metrics.update(
                cycleTimeSec=round(cycle_time, 3),
                waitingTimeSec=round(1.4 + metric_variation * 0.2, 3),
                processingTimeSec=round(max(1.0, target - 2.0 + metric_variation * 0.12), 3),
                stationDelaySec=round(station_delay, 3),
                throughputPerMin=round(60 / cycle_time, 3),
                queueLength=1 + metric_variation % 3,
                wipCount=3 + metric_variation,
                equipmentIdleTimeSec=round(metric_variation * 0.18, 3),
            )
            return

        if process_code == "BODY":
            if is_abnormal:
                rms_ampere = 1.95 + current_variation * 0.03
                acceleration_g = 0.006 + current_variation * 0.0004
                vibration_score = min(0.99, 0.46 + vibration_variation * 0.012)
                vibration_rms = 1.55 + vibration_variation * 0.055
                vibration_peak = 2.15 + vibration_variation * 0.075
                cycle_time = target + 6.0 + metric_variation * 0.85
                station_delay = cycle_time - target

                current.update(
                    rmsAmpere=round(rms_ampere, 3),
                    maxAmpere=round(rms_ampere + 0.16 + current_variation * 0.008, 3),
                    minAmpere=round(max(0.0, rms_ampere - 0.13), 3),
                    accelerationG=round(acceleration_g, 5),
                )
                ford.update(
                    label=-1,
                    vibrationScore=round(vibration_score, 4),
                    vibrationRms=round(vibration_rms, 3),
                    vibrationPeak=round(vibration_peak, 3),
                )
                vision.update(
                    label=0,
                    avgTemperature=round(38.0 + paint_variation * 0.1, 3),
                    maxTemperature=round(40.0 + paint_variation * 0.12, 3),
                    minTemperature=round(36.8 + paint_variation * 0.08, 3),
                    thermalStdTemp=round(0.58 + paint_variation * 0.02, 3),
                    defectScore=round(0.04 + paint_variation * 0.005, 4),
                    thicknessValue=round(113.8 + paint_variation * 0.3, 3),
                    surfaceQualityScore=round(98.2 - paint_variation * 0.18, 3),
                )
                bosch["response"] = 0
                process_metrics.update(
                    cycleTimeSec=round(cycle_time, 3),
                    waitingTimeSec=round(7.5 + metric_variation * 0.5, 3),
                    processingTimeSec=round(max(1.0, target - 2.0 + metric_variation * 0.2), 3),
                    stationDelaySec=round(station_delay, 3),
                    throughputPerMin=round(60 / cycle_time, 3),
                    queueLength=7 + metric_variation,
                    wipCount=20 + metric_variation * 2,
                    equipmentIdleTimeSec=round(8.5 + metric_variation * 0.95, 3),
                )
                return

            rms_ampere = 1.72 + current_variation * 0.035
            acceleration_g = 0.005 + current_variation * 0.00045
            vibration_score = 0.24 + vibration_variation * 0.013
            vibration_rms = 0.36 + vibration_variation * 0.03
            vibration_peak = 0.62 + vibration_variation * 0.04
            cycle_time = target + metric_variation * 0.38
            station_delay = max(0.0, cycle_time - target)

            current.update(
                rmsAmpere=round(rms_ampere, 3),
                maxAmpere=round(rms_ampere + 0.14 + current_variation * 0.009, 3),
                minAmpere=round(max(0.0, rms_ampere - 0.13), 3),
                accelerationG=round(acceleration_g, 5),
            )
            ford.update(
                label=1,
                vibrationScore=round(vibration_score, 4),
                vibrationRms=round(vibration_rms, 3),
                vibrationPeak=round(vibration_peak, 3),
            )
            vision.update(
                label=0,
                avgTemperature=round(37.9 + paint_variation * 0.14, 3),
                maxTemperature=round(39.9 + paint_variation * 0.16, 3),
                minTemperature=round(36.6 + paint_variation * 0.1, 3),
                thermalStdTemp=round(0.56 + paint_variation * 0.04, 3),
                defectScore=round(0.03 + paint_variation * 0.005, 4),
                thicknessValue=round(113.2 + paint_variation * 0.32, 3),
                surfaceQualityScore=round(98.4 - paint_variation * 0.16, 3),
            )
            bosch["response"] = 0
            process_metrics.update(
                cycleTimeSec=round(cycle_time, 3),
                waitingTimeSec=round(2.1 + metric_variation * 0.22, 3),
                processingTimeSec=round(max(1.0, target - 1.8 + metric_variation * 0.15), 3),
                stationDelaySec=round(station_delay, 3),
                throughputPerMin=round(60 / cycle_time, 3),
                queueLength=2 + metric_variation % 4,
                wipCount=5 + metric_variation,
                equipmentIdleTimeSec=round(metric_variation * 0.22, 3),
            )
            return

        if process_code == "PAINT":
            paint_is_warning = is_abnormal and (global_index % 3 == 0)
            low_thickness_band = (global_index % 2) == 0

            if is_abnormal and paint_is_warning:
                rms_ampere = 1.82 + current_variation * 0.045
                acceleration_g = 0.0065 + current_variation * 0.00045
                vibration_score = min(0.55, 0.42 + vibration_variation * 0.012)
                vibration_rms = 0.55 + vibration_variation * 0.028
                vibration_peak = 0.82 + vibration_variation * 0.035
                thermal_std_temp = round(2.25 + paint_variation * 0.18, 3)
                defect_score = round(min(0.59, 0.42 + paint_variation * 0.018), 4)
                thickness_value = round(
                    85.0 + paint_variation * 0.45 if low_thickness_band else 121.0 + paint_variation * 0.85,
                    3,
                )
                surface_quality_score = round(max(60.0, 78.0 - paint_variation * 1.05), 3)
                cycle_time = target + 2.8 + metric_variation * 0.42
                station_delay = cycle_time - target

                current.update(
                    rmsAmpere=round(rms_ampere, 3),
                    maxAmpere=round(rms_ampere + 0.19 + current_variation * 0.012, 3),
                    minAmpere=round(max(0.0, rms_ampere - 0.14), 3),
                    accelerationG=round(acceleration_g, 5),
                )
                ford.update(
                    label=1,
                    vibrationScore=round(vibration_score, 4),
                    vibrationRms=round(vibration_rms, 3),
                    vibrationPeak=round(vibration_peak, 3),
                )
                vision.update(
                    label=1,
                    avgTemperature=round(41.5 + paint_variation * 0.42, 3),
                    maxTemperature=round(46.0 + paint_variation * 0.48, 3),
                    minTemperature=round(37.5 + paint_variation * 0.26, 3),
                    thermalStdTemp=thermal_std_temp,
                    defectScore=defect_score,
                    thicknessValue=thickness_value,
                    surfaceQualityScore=surface_quality_score,
                )
                bosch["response"] = 1
                process_metrics.update(
                    cycleTimeSec=round(cycle_time, 3),
                    waitingTimeSec=round(4.2 + metric_variation * 0.32, 3),
                    processingTimeSec=round(max(1.0, target - 1.2 + metric_variation * 0.16), 3),
                    stationDelaySec=round(station_delay, 3),
                    throughputPerMin=round(60 / cycle_time, 3),
                    queueLength=4 + metric_variation,
                    wipCount=12 + metric_variation * 2,
                    equipmentIdleTimeSec=round(3.0 + metric_variation * 0.55, 3),
                )
                return

            if is_abnormal:
                rms_ampere = 2.08 + current_variation * 0.075
                acceleration_g = 0.012 + current_variation * 0.0008
                vibration_score = min(0.83, 0.65 + vibration_variation * 0.015)
                vibration_rms = 1.1 + vibration_variation * 0.04
                vibration_peak = 1.65 + vibration_variation * 0.05
                thermal_std_temp = round(5.2 + paint_variation * 0.28, 3)
                defect_score = round(min(0.99, 0.66 + paint_variation * 0.022), 4)
                thickness_value = round(
                    78.0 - paint_variation * 0.45 if low_thickness_band else 131.5 + paint_variation * 1.05,
                    3,
                )
                surface_quality_score = round(max(0.0, 58.0 - paint_variation * 1.45), 3)
                cycle_time = target + 6.8 + metric_variation * 0.92
                station_delay = cycle_time - target

                current.update(
                    rmsAmpere=round(rms_ampere, 3),
                    maxAmpere=round(rms_ampere + 0.29 + current_variation * 0.018, 3),
                    minAmpere=round(max(0.0, rms_ampere - 0.22), 3),
                    accelerationG=round(acceleration_g, 5),
                )
                ford.update(
                    label=1,
                    vibrationScore=round(vibration_score, 4),
                    vibrationRms=round(vibration_rms, 3),
                    vibrationPeak=round(vibration_peak, 3),
                )
                vision.update(
                    label=1,
                    avgTemperature=round(49.5 + paint_variation * 0.72, 3),
                    maxTemperature=round(57.5 + paint_variation * 0.82, 3),
                    minTemperature=round(42.0 + paint_variation * 0.48, 3),
                    thermalStdTemp=thermal_std_temp,
                    defectScore=defect_score,
                    thicknessValue=thickness_value,
                    surfaceQualityScore=surface_quality_score,
                )
                bosch["response"] = 1
                process_metrics.update(
                    cycleTimeSec=round(cycle_time, 3),
                    waitingTimeSec=round(10.5 + metric_variation * 0.95, 3),
                    processingTimeSec=round(max(1.0, target - 2.8 + metric_variation * 0.35), 3),
                    stationDelaySec=round(station_delay, 3),
                    throughputPerMin=round(60 / cycle_time, 3),
                    queueLength=8 + metric_variation,
                    wipCount=22 + metric_variation * 2,
                    equipmentIdleTimeSec=round(10.0 + metric_variation * 1.1, 3),
                )
                return

            rms_ampere = 1.62 + current_variation * 0.028
            acceleration_g = 0.0045 + current_variation * 0.0003
            vibration_score = 0.11 + vibration_variation * 0.008
            vibration_rms = 0.24 + vibration_variation * 0.018
            vibration_peak = 0.41 + vibration_variation * 0.022
            thermal_std_temp = round(0.68 + paint_variation * 0.12, 3)
            defect_score = round(min(0.39, 0.10 + paint_variation * 0.027), 4)
            thickness_value = round(99.0 + paint_variation * 1.5, 3)
            surface_quality_score = round(max(80.0, 92.0 - paint_variation * 1.25), 3)
            cycle_time = target + 0.35 + metric_variation * 0.14
            station_delay = max(0.0, cycle_time - target)

            current.update(
                rmsAmpere=round(rms_ampere, 3),
                maxAmpere=round(rms_ampere + 0.13 + current_variation * 0.008, 3),
                minAmpere=round(max(0.0, rms_ampere - 0.11), 3),
                accelerationG=round(acceleration_g, 5),
            )
            ford.update(
                label=0,
                vibrationScore=round(vibration_score, 4),
                vibrationRms=round(vibration_rms, 3),
                vibrationPeak=round(vibration_peak, 3),
            )
            vision.update(
                label=0,
                avgTemperature=round(38.6 + paint_variation * 0.22, 3),
                maxTemperature=round(40.8 + paint_variation * 0.2, 3),
                minTemperature=round(36.9 + paint_variation * 0.12, 3),
                thermalStdTemp=thermal_std_temp,
                defectScore=defect_score,
                thicknessValue=thickness_value,
                surfaceQualityScore=surface_quality_score,
            )
            bosch["response"] = 0
            process_metrics.update(
                cycleTimeSec=round(cycle_time, 3),
                waitingTimeSec=round(1.8 + metric_variation * 0.12, 3),
                processingTimeSec=round(max(1.0, target - 2.2 + metric_variation * 0.1), 3),
                stationDelaySec=round(station_delay, 3),
                throughputPerMin=round(60 / cycle_time, 3),
                queueLength=2 + metric_variation % 3,
                wipCount=5 + metric_variation,
                equipmentIdleTimeSec=round(metric_variation * 0.18, 3),
            )
            return

        if is_abnormal:
            current_variation = global_index % 11
            vibration_variation = global_index % 13
            paint_variation = global_index % 10
            metric_variation = global_index % 8

            rms_ampere = 3.2 + current_variation * 0.12
            acceleration_g = 0.055 + current_variation * 0.003
            vibration_score = min(0.99, 0.72 + vibration_variation * 0.018)
            vibration_rms = 2.35 + vibration_variation * 0.075
            vibration_peak = 3.55 + vibration_variation * 0.095
            cycle_time = target + 12.0 + metric_variation * 1.4
            station_delay = cycle_time - target

            current.update(
                rmsAmpere=round(rms_ampere, 3),
                maxAmpere=round(rms_ampere + 0.45 + current_variation * 0.025, 3),
                minAmpere=round(max(0.0, rms_ampere - 0.35), 3),
                accelerationG=round(acceleration_g, 4),
            )
            ford.update(
                label=-1,
                vibrationScore=round(vibration_score, 4),
                vibrationRms=round(vibration_rms, 3),
                vibrationPeak=round(vibration_peak, 3),
            )
            vision.update(
                label=1,
                avgTemperature=round(50.0 + paint_variation * 0.8, 3),
                maxTemperature=round(58.0 + paint_variation * 0.9, 3),
                minTemperature=round(42.0 + paint_variation * 0.5, 3),
                thermalStdTemp=round(4.5 + paint_variation * 0.25, 3),
                defectScore=round(min(0.99, 0.65 + paint_variation * 0.025), 4),
                thicknessValue=round(124.0 + paint_variation * 1.3, 3),
                surfaceQualityScore=round(max(0.0, 72.0 - paint_variation * 1.8), 3),
            )
            bosch["response"] = 1
            process_metrics.update(
                cycleTimeSec=round(cycle_time, 3),
                waitingTimeSec=round(14.0 + metric_variation * 1.1, 3),
                processingTimeSec=round(max(1.0, target - 4.0 + metric_variation * 0.4), 3),
                stationDelaySec=round(station_delay, 3),
                throughputPerMin=round(60 / cycle_time, 3),
                queueLength=10 + metric_variation,
                wipCount=30 + metric_variation * 2,
                equipmentIdleTimeSec=round(18.0 + metric_variation * 1.7, 3),
            )
            return

        current_variation = global_index % 9
        vibration_variation = global_index % 7
        paint_variation = global_index % 8
        metric_variation = global_index % 6

        rms_ampere = 1.55 + current_variation * 0.035
        cycle_time = target + metric_variation * 0.25

        current.update(
            rmsAmpere=round(rms_ampere, 3),
            maxAmpere=round(rms_ampere + 0.18 + current_variation * 0.01, 3),
            minAmpere=round(max(0.0, rms_ampere - 0.16), 3),
            accelerationG=round(0.004 + current_variation * 0.0007, 5),
        )
        ford.update(
            label=1,
            vibrationScore=round(0.08 + vibration_variation * 0.015, 4),
            vibrationRms=round(0.22 + vibration_variation * 0.035, 3),
            vibrationPeak=round(0.42 + vibration_variation * 0.045, 3),
        )
        vision.update(
            label=0,
            avgTemperature=round(37.8 + paint_variation * 0.25, 3),
            maxTemperature=round(40.0 + paint_variation * 0.22, 3),
            minTemperature=round(36.5 + paint_variation * 0.18, 3),
            thermalStdTemp=round(0.55 + paint_variation * 0.06, 3),
            defectScore=round(0.03 + paint_variation * 0.007, 4),
            thicknessValue=round(113.0 + paint_variation * 0.45, 3),
            surfaceQualityScore=round(98.5 - paint_variation * 0.3, 3),
        )
        bosch["response"] = 0
        process_metrics.update(
            cycleTimeSec=round(cycle_time, 3),
            waitingTimeSec=round(1.5 + metric_variation * 0.25, 3),
            processingTimeSec=round(max(1.0, target - 2.5 + metric_variation * 0.15), 3),
            stationDelaySec=round(max(0.0, cycle_time - target), 3),
            throughputPerMin=round(60 / cycle_time, 3),
            queueLength=1 + metric_variation % 3,
            wipCount=3 + metric_variation,
            equipmentIdleTimeSec=round(metric_variation * 0.2, 3),
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
            count_increase = _press_count_increase_flag(
                station_delay_sec=float(process_metrics["stationDelaySec"]),
                equipment_idle_time_sec=float(process_metrics["equipmentIdleTimeSec"]),
            )
            return {
                "press": {
                    "countIncreaseYn": count_increase,
                    "targetCycleTimeSec": PROCESS_META["PRESS"]["targetCycleTimeSec"],
                    "timestampDelaySec": process_metrics["stationDelaySec"],
                },
            }
        if process_code == "BODY":
            robot_score = float(ford["vibrationScore"])
            return {
                "body": {
                    "robotMotionStatus": _body_robot_motion_status(robot_score),
                    "robotOperationMode": _body_robot_operation_mode(robot_score),
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
            # ?댁긽 ?곗씠?곕뒗 BODY? PAINT ?듦낵 ?쒖꽌瑜?諛붽퓭 ?쒖꽌 ?ㅻ쪟瑜??쒗쁽?쒕떎.
            abnormal_steps = [
                expected_steps[0],
                expected_steps[2],
                expected_steps[1],
                expected_steps[3],
            ]
            assembly_variation = car_master_id % 4
            missing_part_count = (assembly_variation % 3) if has_sequence_error else 0
            fastening_error_count = (1 + assembly_variation) if has_sequence_error else 0
            sequence_error_count = (1 + (assembly_variation % 2)) if has_sequence_error else 0

            return {
                "assembly": {
                    "expectedSequence": expected_sequence,
                    "actualSequence": (
                        ">".join(abnormal_steps)
                        if has_sequence_error
                        else expected_sequence
                    ),
                    "missingPartCount": missing_part_count,
                    "fasteningErrorCount": fastening_error_count,
                    "sequenceErrorCount": sequence_error_count,
                },
            }
        raise ValueError(f"吏?먰븯吏 ?딅뒗 process_code?낅땲?? {process_code}")

    def _load_forming(self) -> pd.DataFrame:
        return self._cached_csv(
            "forming",
            self.dataset_root / "?뚯꽦媛怨??먯썝理쒖쟻??AI ?곗씠?곗뀑" / "怨듭젙_?곗씠??2022??8??csv",
            nrows=4096,
        )

    def _load_press_current(self, index: int) -> pd.DataFrame:
        file_index = index % 4 + 1
        return self._cached_csv(
            f"press_current_{file_index}",
            self.dataset_root
            / "?뚯꽦媛怨??먯썝理쒖쟻??AI ?곗씠?곗뀑"
            / f"?꾨젅??{file_index}???좎븬紐⑦꽣_?꾨쪟?곗씠??csv",
            nrows=4096,
        )

    def _load_robot_current(self, index: int) -> pd.DataFrame:
        file_index = index % 2 + 1
        return self._cached_csv(
            f"robot_current_{file_index}",
            self.dataset_root
            / "?뚯꽦媛怨??먯썝理쒖쟻??AI ?곗씠?곗뀑"
            / f"濡쒕큸_{file_index}???꾨쪟_?곗씠??csv",
            nrows=4096,
        )

    def _load_vision(self, side: str) -> tuple[pd.DataFrame, list[float]]:
        cache_key = f"vision_{side}"
        if cache_key not in self._cache:
            base = self.dataset_root / "癒몄떊鍮꾩쟾 AI ?곗씠?곗뀑 (?댄솕??湲곕컲 ?덉쭏 寃???곗씠??"
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
            base = self.dataset_root / "Ford ?붿쭊 吏꾨룞 ?곗씠?곗뀑"
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
    """?꾩껜 ?대깽?몃? ?좎쭨 踰붿쐞??洹좊벑 遺꾪븷?????쇰퀎 ?앹궛 諛?꾨? ?곸슜?쒕떎."""
    days = (end_date - start_date).days + 1
    # ?섎㉧吏?????좎쭨遺????嫄댁뵫 諛곕텇???꾩껜 嫄댁닔媛 ?뺥솗???좎??섍쾶 ?쒕떎.
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
    """?쇰퀎 slot???쒓컙?蹂??앹궛 諛?꾩뿉 留욌뒗 microsecond offset?쇰줈 蹂?섑븳??"""
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
    """?ъ떎??寃곌낵??媛숆쾶 ?좎??섎㈃???대깽???쒓컖??湲곌퀎?곸씤 ?깃컙寃⑹쓣 ?꾪솕?쒕떎."""
    jitter_window = max(1, interval_us // 5)
    pseudo_random = (slot * 1103515245 + 12345) & 0x7FFFFFFF
    return pseudo_random % (2 * jitter_window + 1) - jitter_window


def _equipment_no_for_process(*, car_master_id: int, process_code: str) -> int:
    """李⑤웾쨌怨듭젙 議고빀蹂꾨줈 ?낅┰?곸씤 ?ㅻ퉬 踰덊샇(1~5)瑜?寃곗젙?쒕떎.

    ?쇰컲 random 紐⑤뱢???ъ슜?섎㈃ 諛곗튂瑜??ㅼ떆 ?ㅽ뻾?????ㅻ퉬媛 ?щ씪吏????덈떎.
    ?대깽???ъ깮?깃낵 upsert媛 ?덉젙?곸쑝濡??숈옉?섎룄濡?媛숈? ?낅젰?먮뒗 ??긽 媛숈?
    踰덊샇媛 ?섏삤???댁떆 湲곕컲 寃곗젙???쒕뜡 諛⑹떇???ъ슜?쒕떎.
    """
    digest = hashlib.blake2b(
        f"{car_master_id}:{process_code}:equipment".encode("utf-8"),
        digest_size=8,
    ).digest()
    return int.from_bytes(digest, "big") % LINE_STATION_COUNT + 1


def _equipment_route_for_car(car_master_id: int) -> str:
    """李⑤웾???듦낵??4媛?怨듭젙???ㅼ젣 ?ㅻ퉬 寃쎈줈瑜?臾몄옄?대줈 留뚮뱺??

    ?? PRESS 2?? BODY 3?? PAINT 1?? ASSEMBLY 4??
    -> P02>B03>PA01>A04
    """
    return ">".join(
        (
            f"{PROCESS_ROUTE_PREFIX[process_code]}"
            f"{_equipment_no_for_process(car_master_id=car_master_id, process_code=process_code):02d}"
        )
        for process_code in PROCESS_SEQUENCE
    )


def _production_date_from_vehicle_id(vehicle_id: str) -> date | None:
    for token in str(vehicle_id).split("-"):
        if len(token) != 8 or not token.isdigit():
            continue
        try:
            return date(
                int(token[0:4]),
                int(token[4:6]),
                int(token[6:8]),
            )
        except ValueError:
            return None
    return None


def _equipment_status_for_event(
    *,
    car_master_id: int,
    process_code: str,
    is_abnormal: bool,
) -> dict[str, str]:
    """Java enum 湲곗? ?댁쟾 ?곹깭瑜?議곗젙?섏뿬 ?λ퉬 ?댁긽(STOPPED/FAULT)??以꾩씤??"""
    _ = is_abnormal
    digest = hashlib.blake2b(
        f"{car_master_id}:{process_code}:status".encode("utf-8"),
        digest_size=8,
    ).digest()
    ratio = int.from_bytes(digest, "big") % 100

    if ratio < 85:
        operation_status = "RUNNING"
    elif ratio < 95:
        operation_status = "WARNING"
    elif ratio < 98:
        operation_status = "STOPPED"
    else:
        operation_status = "FAULT"

    return {"operationStatus": operation_status}


def validate_process_data(
    process_code: str,
    process_data: dict[str, Any],
) -> None:
    """processData媛 怨듭젙蹂??꾩슜 JSON 援ъ“瑜??뺥솗???곕Ⅴ?붿? 寃利앺븳??"""
    expected_key = PROCESS_DATA_KEY.get(process_code)
    required_fields = PROCESS_DATA_REQUIRED_FIELDS.get(process_code)
    if expected_key is None or required_fields is None:
        raise ValueError(f"吏?먰븯吏 ?딅뒗 process_code?낅땲?? {process_code}")

    if set(process_data) != {expected_key}:
        raise ValueError(
            f"{process_code} processData??{expected_key} 釉붾줉留??ы븿?댁빞 ?⑸땲?? "
            f"actual={sorted(process_data)}",
        )

    process_payload = process_data.get(expected_key)
    if not isinstance(process_payload, dict):
        raise ValueError(
            f"{process_code} processData.{expected_key}??JSON object?ъ빞 ?⑸땲??",
        )

    missing_fields = required_fields - set(process_payload)
    if missing_fields:
        raise ValueError(
            f"{process_code} processData ?꾩닔 ?꾨뱶媛 ?꾨씫?섏뿀?듬땲?? "
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

