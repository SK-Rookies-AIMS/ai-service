from __future__ import annotations

import json
import math
import statistics
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


PROCESS_SEQUENCE = ("PRESS", "BODY", "PAINT", "ASSEMBLY")
LINE_STATION_COUNT = 4
PROCESS_META: dict[str, dict[str, Any]] = {
    "PRESS": {
        "lineCode": "PRESS_LINE_01",
        "stationCode": "PRESS_STATION_01",
        "equipmentType": "HYDRAULIC_PRESS",
        "eventType": "PROCESS_STATUS",
        "eventName": "프레스 공정 통합 관제 이벤트",
        "targetCycleTimeSec": 40.0,
    },
    "BODY": {
        "lineCode": "BODY_LINE_01",
        "stationCode": "BODY_STATION_01",
        "equipmentType": "ROBOT_ARM",
        "eventType": "EQUIPMENT_SENSOR",
        "eventName": "차체 공정 로봇 관제 이벤트",
        "targetCycleTimeSec": 52.0,
    },
    "PAINT": {
        "lineCode": "PAINT_LINE_01",
        "stationCode": "PAINT_STATION_01",
        "equipmentType": "CAMERA",
        "eventType": "QUALITY_CHECK",
        "eventName": "도장 공정 품질 관제 이벤트",
        "targetCycleTimeSec": 64.0,
    },
    "ASSEMBLY": {
        "lineCode": "ASSEMBLY_LINE_01",
        "stationCode": "ASSEMBLY_STATION_01",
        "equipmentType": "CONVEYOR",
        "eventType": "PROCESS_STATUS",
        "eventName": "의장 공정 조립 관제 이벤트",
        "targetCycleTimeSec": 58.0,
    },
}

DATASET_ROOT = Path(__file__).resolve().parents[1] / "datasets" / "process"
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
    start_date: date
    end_date: date
    events_per_day: int
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
        car_ids = sorted(request.car_id_map)
        if not car_ids:
            return

        days = (request.end_date - request.start_date).days + 1
        if days <= 0:
            return

        global_index = 0
        for day_offset in range(days):
            current_date = request.start_date + timedelta(days=day_offset)
            for slot in range(request.events_per_day):
                process_index = global_index % len(PROCESS_SEQUENCE)
                production_sequence_index = global_index // len(PROCESS_SEQUENCE)
                process_code = PROCESS_SEQUENCE[process_index]
                event_time = _event_time_for_slot(
                    current_date=current_date,
                    slot=slot,
                    events_per_day=request.events_per_day,
                )
                sequence_no = slot + 1
                event_id = f"EVT-{current_date:%Y%m%d}-{sequence_no:06d}"
                car_id = car_ids[production_sequence_index % len(car_ids)]
                event = self._build_event(
                    event_id=event_id,
                    event_time=event_time,
                    process_code=process_code,
                    global_index=global_index,
                    production_sequence_index=production_sequence_index,
                    car_id=car_id,
                    equipment_map=request.equipment_map,
                )
                equipment_code = event["equipment"]["equipmentCode"]
                equipment_row = request.equipment_map[equipment_code]
                yield {
                    "event_id": event_id,
                    "event_time": event_time,
                    "car_master_id": request.car_id_map[car_id],
                    "equipment_id": int(equipment_row["id"]),
                    "process_code": process_code,
                    "station_code": event["location"]["stationCode"],
                    "equipment_code": equipment_code,
                    "equipment_type": event["equipment"]["equipmentType"],
                    "equipment_status": event["equipmentStatus"]["operationStatus"],
                    "event_type": event["event"]["eventType"],
                    "event_json": _json_safe(event),
                }
                global_index += 1

    def _build_event(
        self,
        *,
        event_id: str,
        event_time: datetime,
        process_code: str,
        global_index: int,
        production_sequence_index: int,
        car_id: str,
        equipment_map: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        meta = PROCESS_META[process_code]
        line_station_no = production_sequence_index % LINE_STATION_COUNT + 1
        equipment_code = f"EQ_{process_code}_{line_station_no:03d}"
        equipment_row = equipment_map[equipment_code]

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
        is_abnormal = self._is_abnormal(process_code, ford, vision, bosch, current)
        operation_status = self._operation_status(is_abnormal, process_metrics)

        return {
            "event": {
                "eventId": event_id,
                "eventTime": event_time.isoformat(),
                "eventCategory": "MANUFACTURING",
                "eventType": meta["eventType"],
                "eventName": meta["eventName"],
            },
            "location": {
                "factoryCode": "AIMS_FACTORY_01",
                "lineCode": f"{process_code}_LINE_{line_station_no:02d}",
                "processCode": process_code,
                "stationCode": f"{process_code}_STATION_{line_station_no:02d}",
            },
            "equipment": {
                "equipmentCode": equipment_code,
                "equipmentName": equipment_row["equipment_name"],
                "equipmentType": equipment_row["equipment_type"],
            },
            "equipmentStatus": {
                "operationStatus": operation_status,
                "lastNormalTime": (event_time - timedelta(seconds=31)).isoformat(),
                "statusChangedTime": event_time.isoformat(),
                "healthStatus": "WARNING" if is_abnormal else "NORMAL",
            },
            "product": {
                "carId": car_id,
                "productId": f"PRODUCT-{int(car_id.rsplit('-', 1)[1]):06d}",
                "itemNo": forming["itemno"],
                "quantity": forming["quantity"],
                "productionCount": forming["cnt"],
            },
            "sensor": self._sensor_payload(current, ford, vision, process_code),
            "manufacturing": self._manufacturing_payload(process_code, forming),
            "processMetrics": process_metrics,
            "sourceTrace": {
                "fordRowId": ford["rowId"],
                "formingRowId": forming["idx"],
                "robotArmVibrationRowId": current["rowId"],
                "machineVisionRowId": vision["rowId"],
                "boschId": bosch["id"],
            },
            "processData": self._process_data(
                process_code=process_code,
                current=current,
                ford=ford,
                vision=vision,
                bosch=bosch,
                process_metrics=process_metrics,
            ),
        }

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

    def _is_abnormal(
        self,
        process_code: str,
        ford: dict[str, Any],
        vision: dict[str, Any],
        bosch: dict[str, Any],
        current: dict[str, Any],
    ) -> bool:
        if process_code in {"PRESS", "BODY"}:
            return ford["label"] < 0 or current["rmsAmpere"] > 2.8
        if process_code == "PAINT":
            return vision["label"] == 1
        return bosch["response"] == 1

    def _operation_status(
        self,
        is_abnormal: bool,
        process_metrics: dict[str, Any],
    ) -> str:
        if process_metrics["equipmentIdleTimeSec"] >= 20:
            return "IDLE"
        if is_abnormal and process_metrics["stationDelaySec"] >= 10:
            return "ERROR"
        return "RUNNING"

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
            "primarySensorSource": _primary_sensor_source(process_code),
        }

    def _manufacturing_payload(
        self,
        process_code: str,
        forming: dict[str, Any],
    ) -> dict[str, Any]:
        sequence_no = PROCESS_SEQUENCE.index(process_code) + 1
        return {
            "processSequence": sequence_no,
            "previousProcessCode": PROCESS_SEQUENCE[sequence_no - 2] if sequence_no > 1 else None,
            "nextProcessCode": PROCESS_SEQUENCE[sequence_no] if sequence_no < len(PROCESS_SEQUENCE) else None,
            "targetQuantity": 200,
            "completedQuantity": min(200, forming["quantity"] + 140),
        }

    def _process_data(
        self,
        *,
        process_code: str,
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

        has_sequence_error = bool(bosch["response"])
        return {
            "assembly": {
                "expectedSequence": "A01>A02>A03>A04",
                "actualSequence": "A01>A03>A02>A04" if has_sequence_error else "A01>A02>A03>A04",
                "missingPartCount": 1 if has_sequence_error and current["rmsAmpere"] > 2.3 else 0,
                "fasteningErrorCount": 1 if has_sequence_error else 0,
                "sequenceErrorCount": 1 if has_sequence_error else 0,
            },
        }

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


def _weighted_event_offset_us(slot: int, events_per_day: int) -> int:
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
    jitter_window = max(1, interval_us // 5)
    pseudo_random = (slot * 1103515245 + 12345) & 0x7FFFFFFF
    return pseudo_random % (2 * jitter_window + 1) - jitter_window


def _primary_sensor_source(process_code: str) -> str:
    if process_code == "PRESS":
        return "PRESS_CURRENT_AND_FORD_VIBRATION"
    if process_code == "BODY":
        return "ROBOT_CURRENT_AND_FORD_VIBRATION"
    if process_code == "PAINT":
        return "MACHINE_VISION_THERMAL"
    return "BOSCH_PRODUCTION_LINE"


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
