from __future__ import annotations

import csv
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


PROCESS_ORDER = ("PRESS", "BODY", "PAINT", "ASSEMBLY")
TRANSFER_FLOWS = (("PRESS", "BODY"), ("BODY", "PAINT"), ("PAINT", "ASSEMBLY"))
RAW_EVENT_COLUMNS = [
    "raw_event_id",
    "event_id",
    "event_time",
    "car_master_id_raw",
    "equipment_id",
    "process_code_raw",
    "station_code_raw",
    "equipment_code_raw",
    "equipment_type_raw",
    "operation_status_raw",
    "event_type_raw",
    "event_json",
    "is_sent",
    "sent_at",
    "created_at",
    "updated_at",
    "defect_yn",
    "defect_reason",
    "dataset_split",
]
DEFAULT_DATASET_ROOT = Path(__file__).resolve().parents[1] / "ml" / "datasets" / "process"
DEFAULT_OUTPUT_DIRNAME = "generated"


@dataclass(frozen=True)
class GeneratedDatasetPaths:
    output_dir: Path
    defect_train: Path
    defect_test: Path
    transfer_train: Path
    transfer_test: Path
    metadata: Path
    event_rows: int
    transition_rows: int
    train_cars: int
    test_cars: int


def generate_defect_transfer_datasets(
    dataset_root: Path = DEFAULT_DATASET_ROOT,
    output_dir: Path | None = None,
    car_count: int = 12_000,
    train_ratio: float = 0.8,
    start_time: datetime | None = None,
) -> GeneratedDatasetPaths:
    """Build ML train/test CSVs from files under app/ml/datasets/process.

    The generated event JSON follows the Kafka payload shape used by
    manufacturing_raw_event:
    event/equipment/equipmentStatus/product/sensor/processMetrics/sourceTrace/processData.
    """

    dataset_root = Path(dataset_root)
    output_dir = Path(output_dir) if output_dir else dataset_root / DEFAULT_OUTPUT_DIRNAME
    output_dir.mkdir(parents=True, exist_ok=True)
    car_count = max(100, int(car_count))
    train_car_count = max(1, min(car_count - 1, int(car_count * train_ratio)))
    start_time = start_time or datetime(2026, 6, 16, 10, 0, 0)

    source = _SourceSamples(dataset_root)
    event_rows: list[dict[str, Any]] = []
    event_by_car_process: dict[tuple[int, str], dict[str, Any]] = {}
    raw_event_id = 1

    for car_index in range(car_count):
        car_master_id = car_index + 1
        split = "train" if car_master_id <= train_car_count else "test"
        defect_profile = _defect_profile(car_master_id)
        for process_index, process_code in enumerate(PROCESS_ORDER):
            event_time = start_time + timedelta(seconds=car_index * 12 + process_index * 3)
            event_id = f"EVT-{event_time:%Y%m%d}-{raw_event_id:06d}"
            sample_index = car_index * len(PROCESS_ORDER) + process_index
            event_json, defect_yn, defect_reason = _build_event_json(
                source=source,
                car_master_id=car_master_id,
                event_id=event_id,
                event_time=event_time,
                process_code=process_code,
                process_index=process_index,
                sample_index=sample_index,
                is_defect=defect_profile[process_code],
            )
            row = {
                "raw_event_id": raw_event_id,
                "event_id": event_id,
                "event_time": event_time.isoformat(timespec="seconds"),
                "car_master_id_raw": car_master_id,
                "equipment_id": _equipment_id(process_code, car_master_id),
                "process_code_raw": process_code,
                "station_code_raw": f"{process_code}_STATION_{_line_no(car_master_id, process_code):02d}",
                "equipment_code_raw": event_json["equipment"]["equipmentCode"],
                "equipment_type_raw": event_json["equipment"]["equipmentType"],
                "operation_status_raw": event_json["equipmentStatus"]["operationStatus"],
                "event_type_raw": event_json["event"]["eventType"],
                "event_json": json.dumps(event_json, ensure_ascii=False),
                "is_sent": 0,
                "sent_at": "",
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
                "defect_yn": int(defect_yn),
                "defect_reason": defect_reason,
                "dataset_split": split,
            }
            event_rows.append(row)
            event_by_car_process[(car_master_id, process_code)] = row
            raw_event_id += 1

    transition_rows = _build_transition_rows(event_by_car_process, car_count, train_car_count)

    defect_train_path = output_dir / "defect_detection_train.csv"
    defect_test_path = output_dir / "defect_detection_test.csv"
    transfer_train_path = output_dir / "transfer_prediction_train.csv"
    transfer_test_path = output_dir / "transfer_prediction_test.csv"
    metadata_path = output_dir / "defect_transfer_dataset_metadata.json"

    _write_csv(defect_train_path, [r for r in event_rows if r["dataset_split"] == "train"], RAW_EVENT_COLUMNS)
    _write_csv(defect_test_path, [r for r in event_rows if r["dataset_split"] == "test"], RAW_EVENT_COLUMNS)

    transition_columns = list(transition_rows[0]) if transition_rows else []
    _write_csv(transfer_train_path, [r for r in transition_rows if r["dataset_split"] == "train"], transition_columns)
    _write_csv(transfer_test_path, [r for r in transition_rows if r["dataset_split"] == "test"], transition_columns)

    metadata = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "dataset_root": str(dataset_root),
        "output_dir": str(output_dir),
        "car_count": car_count,
        "train_ratio": train_ratio,
        "train_cars": train_car_count,
        "test_cars": car_count - train_car_count,
        "event_rows": len(event_rows),
        "transition_rows": len(transition_rows),
        "dataset_files": {
            "defect_train": str(defect_train_path),
            "defect_test": str(defect_test_path),
            "transfer_train": str(transfer_train_path),
            "transfer_test": str(transfer_test_path),
        },
        "source_files": source.source_files,
    }
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    return GeneratedDatasetPaths(
        output_dir=output_dir,
        defect_train=defect_train_path,
        defect_test=defect_test_path,
        transfer_train=transfer_train_path,
        transfer_test=transfer_test_path,
        metadata=metadata_path,
        event_rows=len(event_rows),
        transition_rows=len(transition_rows),
        train_cars=train_car_count,
        test_cars=car_count - train_car_count,
    )


class _SourceSamples:
    def __init__(self, dataset_root: Path, sample_rows: int = 4096) -> None:
        self.dataset_root = Path(dataset_root)
        self.sample_rows = sample_rows
        self.forming_rows = self._read_forming_rows()
        self.press_current_rows = self._read_current_rows("press")
        self.robot_current_rows = self._read_current_rows("robot")
        self.ford_rows = self._read_ford_rows()
        self.vision_rows = self._read_vision_rows()
        self.bosch_rows = self._read_bosch_rows()
        self.source_files = {
            "forming": str(self._find_forming_file()),
            "press_current": [str(p) for p in self._find_current_files("press")],
            "robot_current": [str(p) for p in self._find_current_files("robot")],
            "ford": str(self._find_file(lambda p: p.name == "FordA_TRAIN.txt")),
            "vision": [str(p) for p in self.dataset_root.rglob("2nd_process_*_data.csv")],
            "bosch": str(self._find_file(lambda p: p.name == "train_numeric.csv")),
        }

    def forming(self, index: int) -> dict[str, Any]:
        return self.forming_rows[index % len(self.forming_rows)]

    def current(self, process_code: str, index: int) -> dict[str, float]:
        rows = self.robot_current_rows if process_code == "BODY" else self.press_current_rows
        return rows[index % len(rows)]

    def ford(self, index: int) -> dict[str, Any]:
        return self.ford_rows[index % len(self.ford_rows)]

    def vision(self, index: int) -> dict[str, Any]:
        return self.vision_rows[index % len(self.vision_rows)]

    def bosch(self, index: int) -> dict[str, Any]:
        return self.bosch_rows[index % len(self.bosch_rows)]

    def _find_file(self, predicate) -> Path:
        for path in self.dataset_root.rglob("*"):
            if path.is_file() and predicate(path):
                return path
        raise FileNotFoundError(f"Required source file was not found under {self.dataset_root}")

    def _find_forming_file(self) -> Path:
        return self._find_file(lambda p: p.suffix.lower() == ".csv" and "2022" in p.name)

    def _find_current_files(self, kind: str) -> list[Path]:
        files: list[Path] = []
        for path in self.dataset_root.rglob("*.csv"):
            if kind == "press" and "프레스" in path.name:
                files.append(path)
            if kind == "robot" and "로봇" in path.name:
                files.append(path)
        if not files:
            raise FileNotFoundError(f"{kind} current CSV files were not found")
        return sorted(files)

    def _read_forming_rows(self) -> list[dict[str, Any]]:
        path = self._find_forming_file()
        rows: list[dict[str, Any]] = []
        with path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                rows.append(row)
                if len(rows) >= self.sample_rows:
                    break
        return rows or [{"idx": "1", "itemno": "ITEM-001", "quantity": "1", "cnt": "1"}]

    def _read_current_rows(self, kind: str) -> list[dict[str, float]]:
        rows: list[dict[str, float]] = []
        for path in self._find_current_files(kind):
            with path.open(encoding="utf-8-sig", newline="") as f:
                for row_no, row in enumerate(csv.DictReader(f), start=1):
                    value = _to_float(row.get("RMS[A]"), 1.8)
                    if value < 0.01:
                        value = 1.65 + (row_no % 31) * 0.035
                    rows.append(
                        {
                            "rowId": float(row.get("", row_no) or row_no),
                            "rmsAmpere": value,
                            "maxAmpere": value + 0.16,
                            "minAmpere": max(0.0, value - 0.16),
                            "accelerationG": abs(value - 1.8) / 25,
                        },
                    )
                    if len(rows) >= self.sample_rows:
                        return rows
        return rows or [{"rowId": 1, "rmsAmpere": 1.8, "maxAmpere": 1.95, "minAmpere": 1.65, "accelerationG": 0.006}]

    def _read_ford_rows(self) -> list[dict[str, Any]]:
        path = self._find_file(lambda p: p.name == "FordA_TRAIN.txt")
        rows: list[dict[str, Any]] = []
        with path.open(encoding="utf-8", errors="ignore") as f:
            for row_no, line in enumerate(f, start=1):
                values = [_to_float(v, 0.0) for v in line.split()]
                if len(values) < 20:
                    continue
                signal = values[1:501]
                rms = math.sqrt(sum(v * v for v in signal) / len(signal))
                peak = max(abs(v) for v in signal)
                bands = _frequency_bands(signal)
                rows.append(
                    {
                        "rowId": row_no,
                        "label": int(values[0]),
                        "vibrationRms": rms,
                        "vibrationPeak": peak,
                        "vibrationScore": min(0.99, rms / 3.2),
                        "frequencyBands": bands,
                        "frequencyPeakBand": max(bands, key=bands.get).replace("freq_", "").upper(),
                    },
                )
                if len(rows) >= self.sample_rows:
                    break
        return rows or [{"rowId": 1, "label": 1, "vibrationRms": 0.2, "vibrationPeak": 0.4, "vibrationScore": 0.1, "frequencyBands": {}, "frequencyPeakBand": "0_100_HZ"}]

    def _read_vision_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for data_path in sorted(self.dataset_root.rglob("2nd_process_*_data.csv")):
            side = "LEFT" if "left" in data_path.name else "RIGHT"
            label_path = data_path.with_name(data_path.name.replace("_data.csv", "_label.json"))
            labels = json.loads(label_path.read_text(encoding="utf-8")) if label_path.exists() else []
            with data_path.open(encoding="utf-8-sig", newline="") as f:
                reader = csv.reader(f)
                for row_no, row in enumerate(reader, start=1):
                    values = [_to_float(v, 0.0) for v in row if str(v).strip()]
                    if not values:
                        continue
                    avg = sum(values) / len(values)
                    variance = sum((v - avg) ** 2 for v in values) / len(values)
                    std = math.sqrt(variance)
                    label = int(float(labels[(row_no - 1) % len(labels)])) if labels else 0
                    defect_score = min(0.99, std / 7.0 + (0.35 if label else 0.04))
                    rows.append(
                        {
                            "rowId": row_no,
                            "imagePosition": side,
                            "label": label,
                            "avgTemperature": avg,
                            "maxTemperature": max(values),
                            "minTemperature": min(values),
                            "thermalStdTemp": std,
                            "defectScore": defect_score,
                            "thicknessValue": 114.0 + (row_no % 19) * 0.45 + std,
                            "surfaceQualityScore": max(0.0, 100.0 - defect_score * 38),
                        },
                    )
                    if len(rows) >= self.sample_rows:
                        return rows
        return rows or [{"rowId": 1, "imagePosition": "LEFT", "label": 0, "avgTemperature": 42.0, "maxTemperature": 45.0, "minTemperature": 40.0, "thermalStdTemp": 1.0, "defectScore": 0.1, "thicknessValue": 116.0, "surfaceQualityScore": 96.0}]

    def _read_bosch_rows(self) -> list[dict[str, Any]]:
        path = self._find_file(lambda p: p.name == "train_numeric.csv")
        rows: list[dict[str, Any]] = []
        with path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                numeric = [_to_float(v, math.nan) for k, v in row.items() if k not in {"Id", "Response"} and v not in {"", None}]
                numeric = [v for v in numeric if not math.isnan(v)]
                rows.append(
                    {
                        "id": int(_to_float(row.get("Id"), len(rows) + 1)),
                        "response": int(_to_float(row.get("Response"), 0)),
                        "meanNumericFeature": sum(numeric) / len(numeric) if numeric else 0.0,
                        "nonNullFeatureCount": len(numeric),
                    },
                )
                if len(rows) >= self.sample_rows:
                    break
        return rows or [{"id": 1, "response": 0, "meanNumericFeature": 0.0, "nonNullFeatureCount": 0}]


def _build_event_json(
    *,
    source: _SourceSamples,
    car_master_id: int,
    event_id: str,
    event_time: datetime,
    process_code: str,
    process_index: int,
    sample_index: int,
    is_defect: bool,
) -> tuple[dict[str, Any], int, str]:
    forming = source.forming(car_master_id - 1)
    current = dict(source.current(process_code, sample_index))
    ford = dict(source.ford(sample_index))
    vision = dict(source.vision(sample_index))
    bosch = dict(source.bosch(sample_index))
    _apply_defect_profile(process_code, car_master_id, is_defect, current, ford, vision, bosch)
    metrics = _process_metrics(process_code, sample_index, current, ford, vision, bosch, is_defect)
    process_data = _process_data(process_code, car_master_id, current, ford, vision, bosch, metrics, is_defect)
    defect_reason = _defect_reason(process_code, process_data, metrics, current, ford, vision, bosch, is_defect)
    line_no = _line_no(car_master_id, process_code)
    equipment = _equipment(process_code, line_no)
    status_rand = _stable_float(car_master_id, process_code, "operation_status_rand")
    if is_defect:
        operation_status = "ERROR" if status_rand < 0.15 else "RUNNING"
    else:
        operation_status = "ERROR" if status_rand < 0.01 else "RUNNING"
    is_error = (operation_status == "ERROR")
    last_normal_time = event_time - timedelta(seconds=31 if is_error else 3)
    event_json = {
        "event": {
            "eventId": event_id,
            "eventTime": event_time.isoformat(timespec="seconds"),
            "eventType": _event_type(process_code),
            "eventName": _event_name(process_code),
        },
        "equipment": equipment,
        "equipmentStatus": {
            "operationStatus": operation_status,
            "lastNormalTime": last_normal_time.isoformat(timespec="seconds") if is_error else None,
            "statusChangedTime": event_time.isoformat(timespec="seconds") if is_error else None,
        },
        "product": {"carMasterId": car_master_id},
        "sensor": _sensor(current, ford, vision),
        "processMetrics": metrics,
        "sourceTrace": {
            "fordRowId": ford["rowId"],
            "formingRowId": int(_to_float(forming.get("idx"), car_master_id)),
            "robotArmVibrationRowId": int(_to_float(current.get("rowId"), sample_index + 1)),
            "machineVisionRowId": vision["rowId"],
            "boschId": bosch["id"],
        },
        "processData": process_data,
    }
    return _json_safe(event_json), int(is_defect), defect_reason


def _build_transition_rows(event_by_car_process: dict[tuple[int, str], dict[str, Any]], car_count: int, train_car_count: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for car_master_id in range(1, car_count + 1):
        split = "train" if car_master_id <= train_car_count else "test"
        for source_process, target_process in TRANSFER_FLOWS:
            source = event_by_car_process[(car_master_id, source_process)]
            target = event_by_car_process[(car_master_id, target_process)]
            source_payload = json.loads(source["event_json"])
            target_payload = json.loads(target["event_json"])
            source_metrics = source_payload["processMetrics"]
            source_sensor = source_payload["sensor"]
            row = {
                "car_master_id": car_master_id,
                "source_event_id": source["event_id"],
                "target_event_id": target["event_id"],
                "source_process_code": source_process,
                "target_process_code": target_process,
                "source_defect_yn": int(source["defect_yn"]),
                "target_defect_yn": int(target["defect_yn"]),
                "target_defect_reason": target["defect_reason"],
                "source_cycle_time_sec": source_metrics["cycleTimeSec"],
                "source_station_delay_sec": source_metrics["stationDelaySec"],
                "source_queue_length": source_metrics["queueLength"],
                "source_wip_count": source_metrics["wipCount"],
                "source_current_rms_ampere": source_sensor["current"]["rmsAmpere"],
                "source_vibration_score": source_sensor["vibration"]["vibrationScore"],
                "source_thermal_score": source_sensor["thermal"]["thermalScore"],
                "source_event_json": source["event_json"],
                "target_event_json": target["event_json"],
                "dataset_split": split,
            }
            rows.append(row)
    return rows


def _defect_profile(car_master_id: int) -> dict[str, bool]:
    press = _stable_int(car_master_id, "PRESS") % 100 < 9
    body = (_stable_int(car_master_id, "BODY") % 100 < 7) or (press and _stable_int(car_master_id, "PRESS_BODY") % 100 < 55)
    paint = (_stable_int(car_master_id, "PAINT") % 100 < 8) or (body and _stable_int(car_master_id, "BODY_PAINT") % 100 < 48)
    assembly = (_stable_int(car_master_id, "ASSEMBLY") % 100 < 6) or (paint and _stable_int(car_master_id, "PAINT_ASSEMBLY") % 100 < 45)
    return {"PRESS": press, "BODY": body, "PAINT": paint, "ASSEMBLY": assembly}


def _apply_defect_profile(
    process_code: str,
    car_master_id: int,
    is_defect: bool,
    current: dict[str, Any],
    ford: dict[str, Any],
    vision: dict[str, Any],
    bosch: dict[str, Any],
) -> None:
    r1 = _stable_float(car_master_id, process_code, "defect_rand_1")
    r2 = _stable_float(car_master_id, process_code, "defect_rand_2")
    r3 = _stable_float(car_master_id, process_code, "defect_rand_3")

    if is_defect:
        current["rmsAmpere"] = round(3.0 + r1 * 1.5, 9)
        current["maxAmpere"] = round(current["rmsAmpere"] + 0.2 + r2 * 0.2, 9)
        current["minAmpere"] = round(max(0.0, current["rmsAmpere"] - 0.2 - r3 * 0.2), 9)
        current["accelerationG"] = round(0.04 + r1 * 0.06, 9)
        ford["label"] = -1
        ford["vibrationScore"] = round(0.55 + r1 * 0.30, 6)
        ford["vibrationRms"] = round(1.2 + r2 * 1.5, 9)
        ford["vibrationPeak"] = round(2.0 + r3 * 2.0, 9)
        vision["label"] = 1
        vision["thermalStdTemp"] = round(3.5 + r1 * 3.5, 9)
        vision["defectScore"] = round(0.50 + r2 * 0.30, 6)
        vision["surfaceQualityScore"] = round(65.0 + r3 * 15.0, 3)
        bosch["response"] = 1
    else:
        if r1 < 0.03:
            current["rmsAmpere"] = round(2.8 + r2 * 0.8, 9)
            current["maxAmpere"] = round(current["rmsAmpere"] + 0.2, 9)
            current["minAmpere"] = round(max(0.0, current["rmsAmpere"] - 0.2), 9)
        else:
            current["rmsAmpere"] = round(float(current["rmsAmpere"]) * (0.9 + r2 * 0.2), 9)
            current["maxAmpere"] = round(float(current["maxAmpere"]) * (0.9 + r2 * 0.2), 9)
            current["minAmpere"] = round(float(current["minAmpere"]) * (0.9 + r2 * 0.2), 9)
            
        if r2 < 0.03:
            ford["vibrationScore"] = round(0.45 + r3 * 0.20, 6)
            ford["vibrationRms"] = round(1.0 + r1 * 0.8, 9)
        else:
            ford["vibrationScore"] = round(float(ford["vibrationScore"]) * (0.8 + r3 * 0.4), 6)
            
        if r3 < 0.03:
            vision["defectScore"] = round(0.40 + r1 * 0.20, 6)
            vision["surfaceQualityScore"] = round(75.0 + r2 * 10.0, 3)
        else:
            vision["defectScore"] = round(float(vision["defectScore"]) * (0.8 + r1 * 0.4), 6)
            
        bosch["response"] = 0


def _process_metrics(process_code: str, index: int, current: dict[str, Any], ford: dict[str, Any], vision: dict[str, Any], bosch: dict[str, Any], is_defect: bool) -> dict[str, Any]:
    target = {"PRESS": 40.0, "BODY": 52.0, "PAINT": 64.0, "ASSEMBLY": 58.0}[process_code]
    anomaly = 0.0
    if process_code in {"PRESS", "BODY"}:
        anomaly = float(ford["vibrationScore"]) * 9
    elif process_code == "PAINT":
        anomaly = float(vision["defectScore"]) * 8
    else:
        anomaly = float(bosch["response"]) * 8
    if is_defect:
        anomaly += 8.0
    cycle_time = target + anomaly + (index % 5) * 0.4
    processing = max(1.0, cycle_time - (5 + index % 4))
    waiting = cycle_time - processing + (index % 3)
    delay = max(0.0, cycle_time - target)
    return {
        "cycleTimeSec": round(cycle_time, 3),
        "waitingTimeSec": round(waiting, 3),
        "processingTimeSec": round(processing, 3),
        "stationDelaySec": round(delay, 3),
        "throughputPerMin": round(60 / cycle_time, 3),
        "queueLength": int(3 + delay // 2 + index % 5),
        "wipCount": int(14 + delay // 1.5 + index % 9),
        "equipmentIdleTimeSec": round(delay * 1.6, 3),
    }


def _process_data(
    process_code: str,
    car_master_id: int,
    current: dict[str, Any],
    ford: dict[str, Any],
    vision: dict[str, Any],
    bosch: dict[str, Any],
    metrics: dict[str, Any],
    is_defect: bool,
) -> dict[str, Any]:
    if process_code == "PRESS":
        return {"press": {"countIncreaseYn": metrics["stationDelaySec"] < 10, "targetCycleTimeSec": 40.0, "timestampDelaySec": metrics["stationDelaySec"]}}
    if process_code == "BODY":
        body_rand = _stable_float(car_master_id, process_code, "body_motion_rand")
        if is_defect:
            robot_motion_status = "WARNING" if body_rand < 0.70 else "NORMAL"
        else:
            robot_motion_status = "WARNING" if body_rand < 0.05 else "NORMAL"
        return {"body": {"robotMotionStatus": robot_motion_status, "robotOperationMode": "AUTO", "frequencyPeakBand": ford["frequencyPeakBand"], "frequencyBands": ford["frequencyBands"]}}
    if process_code == "PAINT":
        vision_rand = _stable_float(car_master_id, process_code, "vision_label_rand")
        if is_defect:
            vision_label = "DEFECT" if vision_rand < 0.70 else "NORMAL"
        else:
            vision_label = "DEFECT" if vision_rand < 0.05 else "NORMAL"
        return {
            "paint": {
                "imagePosition": vision["imagePosition"],
                "thermalStdTemp": round(vision["thermalStdTemp"], 3),
                "thicknessValue": round(vision["thicknessValue"], 3),
                "defectScore": round(vision["defectScore"], 4),
                "visionLabel": vision_label,
                "surfaceQualityScore": round(vision["surfaceQualityScore"], 3),
            }
        }
    expected = "A01>A02>A03>A04"
    seq_rand = _stable_float(car_master_id, process_code, "seq_rand")
    missing_rand = _stable_float(car_master_id, process_code, "missing_rand")
    fasten_rand = _stable_float(car_master_id, process_code, "fasten_rand")
    
    if is_defect:
        seq_err = 1 if seq_rand < 0.65 else 0
        missing_err = 1 if missing_rand < 0.60 else 0
        fasten_err = 1 if fasten_rand < 0.70 else 0
    else:
        seq_err = 1 if seq_rand < 0.04 else 0
        missing_err = 1 if missing_rand < 0.03 else 0
        fasten_err = 1 if fasten_rand < 0.05 else 0
        
    actual = "A01>A03>A02>A04" if seq_err > 0 else expected
    return {
        "assembly": {
            "expectedSequence": expected,
            "actualSequence": actual,
            "missingPartCount": missing_err,
            "fasteningErrorCount": fasten_err,
            "sequenceErrorCount": seq_err,
        }
    }


def _defect_reason(process_code: str, process_data: dict[str, Any], metrics: dict[str, Any], current: dict[str, Any], ford: dict[str, Any], vision: dict[str, Any], bosch: dict[str, Any], is_defect: bool) -> str:
    if not is_defect:
        return "normal"
    if process_code == "PRESS":
        return "press_count_or_delay"
    if process_code == "BODY":
        return "body_robot_vibration"
    if process_code == "PAINT":
        return "paint_vision_or_thermal"
    return "assembly_sequence_or_fastening"


def _sensor(current: dict[str, Any], ford: dict[str, Any], vision: dict[str, Any]) -> dict[str, Any]:
    return {
        "sensorType": "MULTI_SENSOR",
        "current": {"rmsAmpere": round(current["rmsAmpere"], 9), "maxAmpere": round(current["maxAmpere"], 9), "minAmpere": round(current["minAmpere"], 9)},
        "vibration": {"accelerationG": round(current["accelerationG"], 9), "vibrationScore": round(ford["vibrationScore"], 6), "vibrationRms": round(ford["vibrationRms"], 9), "vibrationPeak": round(ford["vibrationPeak"], 9)},
        "robotArmVibration": {"robotId": "ROBOT_ARM_01", "axis": f"J{(int(current['rowId']) % 6) + 1}", "frequencyHz": round(40 + float(ford["vibrationScore"]) * 220, 3), "amplitude": round(float(ford["vibrationRms"]) / 1000, 9), "vibrationRms": round(float(ford["vibrationRms"]) / 900, 9), "vibrationPeak": round(float(ford["vibrationPeak"]) / 700, 9), "vibrationScore": round(ford["vibrationScore"], 6)},
        "thermal": {"thermalScore": round(vision["avgTemperature"], 3), "avgTemperature": round(vision["avgTemperature"], 3), "maxTemperature": round(vision["maxTemperature"], 3), "minTemperature": round(vision["minTemperature"], 3)},
    }


def _event_type(process_code: str) -> str:
    return {"PRESS": "PROCESS_STATUS", "BODY": "EQUIPMENT_SENSOR", "PAINT": "QUALITY_CHECK", "ASSEMBLY": "PROCESS_STATUS"}[process_code]


def _event_name(process_code: str) -> str:
    return {"PRESS": "프레스 공정 통합 관제 이벤트", "BODY": "차체 공정 로봇 관제 이벤트", "PAINT": "도장 공정 품질 관제 이벤트", "ASSEMBLY": "의장 공정 조립 관제 이벤트"}[process_code]


def _equipment(process_code: str, line_no: int) -> dict[str, str]:
    equipment_type = {"PRESS": "HYDRAULIC_PRESS", "BODY": "ROBOT_ARM", "PAINT": "CAMERA", "ASSEMBLY": "CONVEYOR"}[process_code]
    name = {"PRESS": "프레스 유압모터", "BODY": "차체 용접 로봇", "PAINT": "도장 열화상 카메라", "ASSEMBLY": "의장 조립 컨베이어"}[process_code]
    return {"equipmentCode": f"EQ_{process_code}_{line_no:03d}", "equipmentName": f"{name} {line_no}호", "equipmentType": equipment_type}


def _equipment_id(process_code: str, car_master_id: int) -> int:
    return (PROCESS_ORDER.index(process_code) * 10) + _line_no(car_master_id, process_code)


def _line_no(car_master_id: int, process_code: str) -> int:
    return _stable_int(car_master_id, process_code, "line") % 5 + 1


def _frequency_bands(signal: list[float]) -> dict[str, float]:
    names = ["freq_0_100_hz", "freq_101_200_hz", "freq_201_300_hz", "freq_301_400_hz", "freq_401_500_hz", "freq_501_600_hz", "freq_601_700_hz", "freq_701_800_hz", "freq_801_900_hz", "freq_901_1000_hz", "freq_1001_1100_hz", "freq_1101_1200_hz", "freq_1201_1300_hz", "freq_1301_1400_hz", "freq_1401_1500_hz", "freq_1501_1600_hz"]
    chunk_size = max(1, len(signal) // len(names))
    bands = {}
    for idx, name in enumerate(names):
        chunk = signal[idx * chunk_size : (idx + 1) * chunk_size] or [0.0]
        bands[name] = round(math.sqrt(sum(v * v for v in chunk) / len(chunk)) / 700, 9)
    return bands


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _stable_int(*parts: Any) -> int:
    digest = hashlib.blake2b(":".join(map(str, parts)).encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big")


def _stable_float(*parts: Any) -> float:
    return (_stable_int(*parts) % 100000) / 100000.0


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if not math.isnan(result) and not math.isinf(result) else default


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    return value
