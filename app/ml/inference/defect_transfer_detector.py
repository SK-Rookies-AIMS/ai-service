from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd 


logger = logging.getLogger(__name__)

DEFECT_ARTIFACT_DIR = Path(__file__).resolve().parents[1] / "artifacts" / "defect"
PROCESS_SEQUENCE = ("PRESS", "BODY", "PAINT", "ASSEMBLY")
NEXT_PROCESS = {
    "PRESS": "BODY",
    "BODY": "PAINT",
    "PAINT": "ASSEMBLY",
}


@dataclass(frozen=True)
class DefectCause:
    rank: int
    feature: str
    label: str
    value: Any
    impact: float
    message: str


@dataclass(frozen=True)
class DefectTransferPrediction:
    defect_probability: float
    defect_threshold: float
    is_quality_defect: bool
    current_process_code: str
    predicted_process_code: str | None
    transfer_probability: float | None
    transfer_threshold: float | None
    expected_steps_after: int | None
    risk_level: str
    causes: list[DefectCause]
    feature_values: dict[str, Any]


class DefectTransferDetector:
    """Run event-level defect detection and adjacent-process transfer prediction."""

    def __init__(
        self,
        *,
        artifact_dir: str | Path = DEFECT_ARTIFACT_DIR,
    ) -> None:
        self.artifact_dir = Path(artifact_dir)
        self.model_path = self.artifact_dir / "lightgbm_defect_detector.joblib"
        self.feature_path = self.artifact_dir / "defect_model_features.json"
        self.metrics_path = self.artifact_dir / "defect_model_metrics.json"
        self.transfer_model_path = self.artifact_dir / "adjacent_transfer_models.joblib"
        self.transfer_metadata_path = (
            self.artifact_dir / "adjacent_transfer_model_metadata.json"
        )
        self._defect_model: Any | None = None
        self._transfer_models: dict[str, Any] | None = None
        self._feature_columns: list[str] | None = None
        self._metrics: dict[str, Any] | None = None
        self._transfer_metadata: dict[str, Any] | None = None
        self._defect_shap_explainer: Any | None = None

    def predict_event(
        self,
        event_json: dict[str, Any],
        process_code: str,
    ) -> DefectTransferPrediction:
        normalized_process = str(process_code or "").strip().upper()
        features = self.extract_event_features(event_json, normalized_process)
        defect_probability = self._predict_probability(
            self.defect_model,
            features,
            self.feature_columns,
        )
        defect_threshold = float(self.metrics.get("threshold", 0.5))
        transfer_key = self._transfer_key(normalized_process)
        transfer_probability = None
        transfer_threshold = None
        predicted_process = NEXT_PROCESS.get(normalized_process)
        if transfer_key and transfer_key in self.transfer_models:
            metadata = self.transfer_metadata.get(transfer_key, {})
            transfer_columns = list(metadata.get("feature_columns") or [])
            transfer_features = self._transfer_features(features, transfer_columns)
            transfer_probability = self._predict_probability(
                self.transfer_models[transfer_key],
                transfer_features,
                transfer_columns,
            )
            transfer_threshold = float(metadata.get("threshold", 0.5))

        risk_probability = max(
            defect_probability,
            transfer_probability if transfer_probability is not None else 0.0,
        )
        causes = self._rank_causes(features, normalized_process, risk_probability)
        next_process = NEXT_PROCESS.get(normalized_process)
        if transfer_probability is not None and next_process is not None:
            predicted_process_code = (
                next_process if transfer_probability > 0 else None
            )
        elif defect_probability > 0:
            predicted_process_code = normalized_process
        else:
            predicted_process_code = None
        return DefectTransferPrediction(
            defect_probability=round(defect_probability, 4),
            defect_threshold=round(defect_threshold, 4),
            is_quality_defect=defect_probability >= defect_threshold,
            current_process_code=normalized_process,
            predicted_process_code=predicted_process_code,
            transfer_probability=(
                round(transfer_probability, 4)
                if transfer_probability is not None
                else None
            ),
            transfer_threshold=(
                round(transfer_threshold, 4)
                if transfer_threshold is not None
                else None
            ),
            expected_steps_after=self._expected_steps_after(normalized_process),
            risk_level=self._risk_level(risk_probability),
            causes=causes,
            feature_values=features,
        )

    @property
    def defect_model(self) -> Any:
        if self._defect_model is None:
            self._defect_model = self._load_joblib(self.model_path)
            self._patch_loaded_model(self._defect_model)
        return self._defect_model

    @property
    def transfer_models(self) -> dict[str, Any]:
        if self._transfer_models is None:
            loaded = self._load_joblib(self.transfer_model_path)
            if not isinstance(loaded, dict):
                raise TypeError("adjacent_transfer_models.joblib must contain a dict.")
            for model in loaded.values():
                self._patch_loaded_model(model)
            self._transfer_models = loaded
        return self._transfer_models

    @property
    def feature_columns(self) -> list[str]:
        if self._feature_columns is None:
            self._feature_columns = list(
                json.loads(self.feature_path.read_text(encoding="utf-8")),
            )
        return self._feature_columns

    @property
    def metrics(self) -> dict[str, Any]:
        if self._metrics is None:
            self._metrics = json.loads(self.metrics_path.read_text(encoding="utf-8"))
        return self._metrics

    @property
    def transfer_metadata(self) -> dict[str, Any]:
        if self._transfer_metadata is None:
            self._transfer_metadata = json.loads(
                self.transfer_metadata_path.read_text(encoding="utf-8"),
            )
        return self._transfer_metadata

    def _load_joblib(self, path: Path) -> Any:
        try:
            import sklearn.compose._column_transformer as column_transformer

            if not hasattr(column_transformer, "_RemainderColsList"):
                column_transformer._RemainderColsList = type(  # type: ignore[attr-defined]
                    "_RemainderColsList",
                    (list,),
                    {},
                )

            import joblib
        except ModuleNotFoundError as exc:
            raise RuntimeError("joblib, scikit-learn and LightGBM are required.") from exc

        if not path.exists():
            raise FileNotFoundError(f"Defect model artifact not found: {path}")
        return joblib.load(path)

    def _predict_probability(
        self,
        model: Any,
        features: dict[str, Any],
        columns: list[str],
    ) -> float:
        frame = pd.DataFrame([{column: features.get(column) for column in columns}])
        if hasattr(model, "predict_proba"):
            probabilities = model.predict_proba(frame)
            return float(probabilities[0][1])
        prediction = model.predict(frame)
        return float(prediction[0])

    def extract_event_features(
        self,
        event_json: dict[str, Any],
        process_code: str,
    ) -> dict[str, Any]:
        equipment = _dict(event_json.get("equipment"))
        sensor = _dict(event_json.get("sensor"))
        metrics = _dict(event_json.get("processMetrics"))
        process_data = _dict(event_json.get("processData"))
        current = _dict(sensor.get("current"))
        vibration = _dict(sensor.get("vibration"))
        robot = _dict(sensor.get("robotArmVibration"))
        thermal = _dict(sensor.get("thermal"))
        press = _dict(process_data.get("press"))
        body = _dict(process_data.get("body"))
        paint = _dict(process_data.get("paint"))
        bands = _dict(body.get("frequencyBands"))
        band_values = [_safe_float(value, default=np.nan) for value in bands.values()]
        band_values = [value for value in band_values if not np.isnan(value)]

        equipment_code = str(equipment.get("equipmentCode") or "")
        return {
            "process_code": process_code,
            "station_code": _station_code(equipment_code),
            "equipment_code": equipment_code,
            "equipment_type": equipment.get("equipmentType"),
            "current_rms_ampere": _safe_float(current.get("rmsAmpere")),
            "current_max_ampere": _safe_float(current.get("maxAmpere")),
            "current_min_ampere": _safe_float(current.get("minAmpere")),
            "vibration_acceleration_g": _safe_float(vibration.get("accelerationG")),
            "vibration_score": _safe_float(vibration.get("vibrationScore")),
            "vibration_rms": _safe_float(vibration.get("vibrationRms")),
            "vibration_peak": _safe_float(vibration.get("vibrationPeak")),
            "robot_axis": robot.get("axis"),
            "robot_frequency_hz": _safe_float(robot.get("frequencyHz")),
            "robot_amplitude": _safe_float(robot.get("amplitude")),
            "robot_vibration_score": _safe_float(robot.get("vibrationScore")),
            "thermal_score": _safe_float(thermal.get("thermalScore")),
            "avg_temperature": _safe_float(thermal.get("avgTemperature")),
            "max_temperature": _safe_float(thermal.get("maxTemperature")),
            "min_temperature": _safe_float(thermal.get("minTemperature")),
            "cycle_time_sec": _safe_float(metrics.get("cycleTimeSec")),
            "waiting_time_sec": _safe_float(metrics.get("waitingTimeSec")),
            "processing_time_sec": _safe_float(metrics.get("processingTimeSec")),
            "station_delay_sec": _safe_float(metrics.get("stationDelaySec")),
            "throughput_per_min": _safe_float(metrics.get("throughputPerMin")),
            "queue_length": _safe_float(metrics.get("queueLength")),
            "wip_count": _safe_float(metrics.get("wipCount")),
            "equipment_idle_time_sec": _safe_float(metrics.get("equipmentIdleTimeSec")),
            "press_target_cycle_time_sec": _safe_float(
                press.get("targetCycleTimeSec"),
            ),
            "body_robot_operation_mode": body.get("robotOperationMode"),
            "body_frequency_peak_band": body.get("frequencyPeakBand"),
            "body_frequency_band_max": max(band_values) if band_values else 0.0,
            "body_frequency_band_mean": (
                float(np.mean(band_values)) if band_values else 0.0
            ),
            "paint_image_position": paint.get("imagePosition"),
            "paint_thermal_std_temp": _safe_float(paint.get("thermalStdTemp")),
            "paint_thickness_value": _safe_float(paint.get("thicknessValue")),
        }

    def _transfer_features(
        self,
        event_features: dict[str, Any],
        columns: list[str],
    ) -> dict[str, Any]:
        source_map = {
            "source_cycle_time_sec": "cycle_time_sec",
            "source_station_delay_sec": "station_delay_sec",
            "source_queue_length": "queue_length",
            "source_wip_count": "wip_count",
            "source_current_rms_ampere": "current_rms_ampere",
            "source_vibration_score": "vibration_score",
            "source_thermal_score": "thermal_score",
        }
        return {
            column: event_features.get(source_map.get(column, column), 0.0)
            for column in columns
        }

    def _rank_causes(
        self,
        features: dict[str, Any],
        process_code: str,
        risk_probability: float,
    ) -> list[DefectCause]:
        shap_impacts = self._shap_feature_impacts(features)
        candidates = [
            ("station_delay_sec", "공정 지연", 12.0, "초"),
            ("cycle_time_sec", "Cycle Time 증가", 55.0, "초"),
            ("queue_length", "대기열 증가", 8.0, "대"),
            ("wip_count", "WIP 증가", 24.0, "대"),
            ("current_rms_ampere", "전류 RMS 편차", 2.2, "A"),
            ("vibration_score", "진동 Score 상승", 0.45, ""),
            ("robot_vibration_score", "로봇 진동 Score 상승", 0.45, ""),
            ("thermal_score", "열화상 Score 상승", 55.0, ""),
            ("max_temperature", "최고 온도 상승", 58.0, "°C"),
            ("paint_thermal_std_temp", "도장 온도 편차", 4.0, "°C"),
            ("paint_thickness_value", "도막 두께 편차", 130.0, ""),
        ]
        scored: list[tuple[str, str, Any, float, str]] = []
        for feature, label, baseline, unit in candidates:
            value = features.get(feature)
            numeric = _safe_float(value, default=0.0)
            if feature == "paint_thickness_value":
                impact = abs(numeric - 116.0) / 18.0
            else:
                impact = max(0.0, numeric - baseline) / max(abs(baseline), 1.0)
            if process_code == "PAINT" and feature.startswith("paint_"):
                impact *= 1.35
            if shap_impacts:
                impact = max(impact * 0.35, shap_impacts.get(feature, 0.0))
            scored.append((feature, label, value, impact, unit))

        scored.sort(key=lambda item: item[3], reverse=True)
        top = scored[:4]
        if not top or top[0][3] <= 0:
            top = [
                (
                    "model_probability",
                    "모델 위험 확률",
                    risk_probability,
                    risk_probability,
                    "",
                ),
            ]
        return [
            DefectCause(
                rank=index,
                feature=feature,
                label=label,
                value=value,
                impact=round(float(min(max(impact, 0.0), 1.0)), 4),
                message=_cause_message(label, value, unit),
            )
            for index, (feature, label, value, impact, unit) in enumerate(top, 1)
        ]

    def _shap_feature_impacts(self, features: dict[str, Any]) -> dict[str, float]:
        try:
            model = self.defect_model
            steps = getattr(model, "named_steps", {})
            preprocess = steps.get("preprocess")
            estimator = steps.get("model")
            if preprocess is None or estimator is None:
                return {}

            frame = pd.DataFrame(
                [{column: features.get(column) for column in self.feature_columns}],
            )
            transformed = preprocess.transform(frame)
            if hasattr(transformed, "toarray"):
                transformed = transformed.toarray()

            if self._defect_shap_explainer is None:
                import shap

                self._defect_shap_explainer = shap.TreeExplainer(estimator)
            shap_values = self._defect_shap_explainer.shap_values(transformed)
            if isinstance(shap_values, list):
                values = shap_values[-1][0]
            else:
                values = np.asarray(shap_values)
                if values.ndim == 3:
                    values = values[0, :, -1]
                else:
                    values = values[0]

            try:
                transformed_names = list(preprocess.get_feature_names_out())
            except Exception:
                transformed_names = [f"feature_{index}" for index in range(len(values))]

            raw_scores = {feature: 0.0 for feature in self.feature_columns}
            for name, value in zip(transformed_names, values, strict=False):
                normalized_name = str(name)
                for feature in raw_scores:
                    if normalized_name.endswith(feature) or f"__{feature}" in normalized_name:
                        raw_scores[feature] += abs(float(value))
                        break

            max_score = max(raw_scores.values(), default=0.0)
            if max_score <= 0:
                return {}
            return {
                feature: round(score / max_score, 4)
                for feature, score in raw_scores.items()
                if score > 0
            }
        except Exception:
            logger.debug("SHAP cause ranking failed; using fallback causes.", exc_info=True)
            return {}

    @staticmethod
    def _transfer_key(process_code: str) -> str | None:
        target = NEXT_PROCESS.get(process_code)
        if target is None:
            return None
        return f"{process_code.lower()}_to_{target.lower()}"

    @staticmethod
    def _expected_steps_after(process_code: str) -> int | None:
        if process_code not in PROCESS_SEQUENCE:
            return None
        steps = len(PROCESS_SEQUENCE) - PROCESS_SEQUENCE.index(process_code) - 1
        return steps or None

    @staticmethod
    def _risk_level(probability: float) -> str:
        if probability >= 0.75:
            return "CRITICAL"
        if probability >= 0.55:
            return "WARNING"
        return "LOW"

    @staticmethod
    def _patch_loaded_model(model: Any) -> None:
        seen: set[int] = set()

        def visit(node: Any) -> None:
            node_id = id(node)
            if node_id in seen:
                return
            seen.add(node_id)

            if node.__class__.__name__ == "SimpleImputer" and not hasattr(
                node,
                "_fill_dtype",
            ):
                node._fill_dtype = getattr(node, "_fit_dtype", None)

            for child in getattr(node, "named_steps", {}).values():
                visit(child)
            for transformer in getattr(node, "transformers_", []):
                if len(transformer) >= 2:
                    visit(transformer[1])
            for _, value in getattr(node, "steps", []):
                visit(value)

        visit(model)


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_float(value: Any, *, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _station_code(equipment_code: str) -> str:
    if not equipment_code:
        return "UNKNOWN"
    parts = equipment_code.split("_")
    return parts[-1] if parts else equipment_code


def _cause_message(label: str, value: Any, unit: str) -> str:
    if isinstance(value, (int, float, np.number)):
        return f"{label} {float(value):.2f}{unit}"
    return f"{label} {value}"
