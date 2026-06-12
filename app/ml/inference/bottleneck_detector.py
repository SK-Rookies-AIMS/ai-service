from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from app.ml.features.bottleneck_features import (
    build_bottleneck_features,
    default_model_feature_columns,
    parse_bottleneck_station,
    parse_date_columns,
)


LINE_BY_PROCESS_CODE = {
    "PRESS": "L0",
    "BODY": "L1",
    "ASSEMBLY": "L2",
    "PAINT": "L3",
}


class BottleneckDetector:
    """병목 탐지 모델 추론"""

    def __init__(self, model_path: str | Path) -> None:
        """모델 경로 초기화"""
        self.model_path = Path(model_path)
        self._model: Any | None = None

    @property
    def model(self) -> Any:
        """모델 지연 로딩"""
        if self._model is None:
            if not self.model_path.exists():
                raise FileNotFoundError(f"병목 탐지 모델 파일을 찾을 수 없습니다: {self.model_path}")
            self._model = joblib.load(self.model_path)
            self._patch_loaded_model(self._model)
        return self._model

    def analyze(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """원본 데이터프레임 추론"""
        # 모델 입력 피처 생성
        column_meta = parse_date_columns(list(raw_df.columns))
        features, _ = build_bottleneck_features(raw_df, column_meta)
        model_features = self._resolve_model_features(features)

        # Isolation Forest 예측
        X = features.reindex(columns=model_features)
        pred_raw = self.model.predict(X)
        anomaly_scores = -self.model.decision_function(X)

        # 예측 결과 병합
        result = features.copy()
        result["iforest_bottleneck"] = (pred_raw == -1).astype(int)
        result["iforest_anomaly_score"] = anomaly_scores
        result["iforest_risk_score"] = pd.Series(anomaly_scores).rank(pct=True).to_numpy()
        return result

    def summarize(
        self,
        analysis_df: pd.DataFrame,
        top_n: int = 20,
        *,
        product_process_history_id: int | None = None,
        manufacturing_event_id: int | None = None,
        car_master_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """분석 결과 요약"""
        # 이상치가 없으면 위험도 상위 항목 사용
        target_df = analysis_df[analysis_df["iforest_bottleneck"].eq(1)].copy()
        if target_df.empty:
            target_df = analysis_df.nlargest(top_n, "iforest_risk_score").copy()

        # 병목 지점별 집계
        grouped = (
            target_df.groupby("bottleneck_station", dropna=False)
            .agg(
                affected_vehicle_count=("Id", "count"),
                avg_delay_time=("max_station_span", "mean"),
                risk_score=("iforest_risk_score", "mean"),
            )
            .reset_index()
            .sort_values(
                ["risk_score", "avg_delay_time", "affected_vehicle_count"],
                ascending=False,
            )
            .head(top_n)
            .reset_index(drop=True)
        )

        # 영향 차량 수와 모델 위험도 기반 등급화
        max_affected = max(int(grouped["affected_vehicle_count"].max()), 1)
        grouped["risk_level"] = grouped.apply(
            lambda row: self._to_risk_level(
                risk_score=self._safe_float(row["risk_score"], default=0.0),
                affected_vehicle_count=int(row["affected_vehicle_count"]),
                max_affected=max_affected,
            ),
            axis=1,
        )
        grouped = grouped.sort_values(
            ["risk_level", "risk_score", "avg_delay_time", "affected_vehicle_count"],
            ascending=False,
        ).reset_index(drop=True)

        # API 저장 형식 변환
        summaries: list[dict[str, Any]] = []
        for index, row in grouped.iterrows():
            station = parse_bottleneck_station(row["bottleneck_station"])
            avg_delay_time = self._safe_float(row["avg_delay_time"], default=0.0)
            risk_level = int(row["risk_level"])
            summaries.append(
                {
                    "product_process_history_id": product_process_history_id,
                    "manufacturing_event_id": manufacturing_event_id,
                    "car_master_id": car_master_id,
                    "process_code": station.process_code,
                    "equipment_code": station.equipment_code,
                    "station_code": station.station,
                    "rank_no": index + 1,
                    "avg_delay_time": round(avg_delay_time, 1),
                    "affected_vehicle_count": int(row["affected_vehicle_count"]),
                    "risk_score": float(risk_level),
                },
            )

        return summaries

    def summarize_product_process_histories(
        self,
        histories: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """공정 이력 병목 순위 계산"""
        # 이력 기반 모델 입력 피처 생성
        features = self._build_product_process_history_features(histories)
        model_features = self._resolve_model_features(features)
        X = features.reindex(columns=model_features)

        # Isolation Forest 점수 계산
        pred_raw = self.model.predict(X)
        anomaly_scores = -self.model.decision_function(X)
        features["iforest_bottleneck"] = (pred_raw == -1).astype(int)
        features["iforest_anomaly_score"] = anomaly_scores
        features["iforest_risk_score"] = pd.Series(anomaly_scores).rank(pct=True).to_numpy()

        # 지점 대표 이력 선정을 위한 정렬
        ranked_features = features.sort_values(
            ["iforest_risk_score", "waiting_time", "process_time", "product_process_history_id"],
            ascending=[False, False, False, True],
        )
        # 공정/설비/지점 단위 집계
        grouped = (
            ranked_features.groupby(["process_code", "equipment_code", "station_code"], dropna=False)
            .agg(
                product_process_history_id=("product_process_history_id", "first"),
                manufacturing_event_id=("manufacturing_event_id", "first"),
                car_master_id=("car_master_id", "first"),
                avg_delay_time=("waiting_time", "mean"),
                max_delay_time=("waiting_time", "max"),
                avg_process_time=("process_time", "mean"),
                affected_vehicle_count=("product_process_history_id", "count"),
                risk_score=("iforest_risk_score", "mean"),
                max_risk_score=("iforest_risk_score", "max"),
            )
            .reset_index()
        )
        # 위험 등급 산정
        max_affected = max(grouped["affected_vehicle_count"].astype(int).max(), 1)
        grouped["risk_level"] = grouped.apply(
            lambda row: self._to_risk_level(
                risk_score=self._safe_float(row["risk_score"], default=0.0),
                affected_vehicle_count=int(row["affected_vehicle_count"]),
                max_affected=max_affected,
            ),
            axis=1,
        )
        grouped = grouped.sort_values(
            [
                "risk_level",
                "max_risk_score",
                "risk_score",
                "max_delay_time",
                "avg_delay_time",
                "affected_vehicle_count",
            ],
            ascending=[False, False, False, False, False, False],
        ).reset_index(drop=True)

        # DB 저장 형식 변환
        summaries: list[dict[str, Any]] = []
        for index, row in grouped.iterrows():
            summaries.append(
                {
                    "product_process_history_id": int(row["product_process_history_id"]),
                    "manufacturing_event_id": self._safe_int(row["manufacturing_event_id"]),
                    "car_master_id": self._safe_int(row["car_master_id"]),
                    "process_code": str(row["process_code"]),
                    "equipment_code": str(row["equipment_code"]),
                    "station_code": str(row["station_code"]),
                    "rank_no": index + 1,
                    "avg_delay_time": round(self._safe_float(row["avg_delay_time"], default=0.0), 1),
                    "affected_vehicle_count": int(row["affected_vehicle_count"]),
                    "risk_score": float(row["risk_level"]),
                },
            )

        return summaries

    def _build_product_process_history_features(
        self,
        histories: list[dict[str, Any]],
    ) -> pd.DataFrame:
        """공정 이력 피처 생성"""
        source_df = pd.DataFrame(histories)
        process_time = source_df["process_time"].fillna(0).astype(float)
        waiting_time = source_df["waiting_time"].fillna(0).astype(float)
        total_duration = process_time + waiting_time

        # 기본 시간/식별자 피처
        feature_df = pd.DataFrame(
            {
                "Id": source_df["id"].astype(int),
                "product_process_history_id": source_df["id"].astype(int),
                "manufacturing_event_id": source_df["manufacturing_event_id"],
                "car_master_id": source_df["car_master_id"],
                "process_code": source_df["process_code"].astype(str),
                "equipment_code": source_df["equipment_code"].astype(str),
                "station_code": source_df["station_code"].astype(str),
                "process_time": process_time,
                "waiting_time": waiting_time,
                "process_start_time": 0.0,
                "process_end_time": total_duration,
                "total_duration": total_duration,
                "observed_date_count": 1,
                "date_missing_ratio": 0.0,
                "active_station_count": 1,
                "max_station_span": process_time,
                "mean_station_span": process_time,
                "std_station_span": 0.0,
            },
        )

        # 모델 학습 컬럼 기본값 보정
        for line in ("L0", "L1", "L2", "L3"):
            feature_df[f"{line}_seen"] = 0
            feature_df[f"{line}_duration"] = 0.0
            feature_df[f"{line}_date_count"] = 0
            feature_df[f"{line}_date_missing_ratio"] = 1.0

        # 공정 코드 기반 라인/스테이션 피처 매핑
        for index, row in feature_df.iterrows():
            line = LINE_BY_PROCESS_CODE.get(str(row["process_code"]))
            if line is None:
                continue

            station = str(row["station_code"])
            station_prefix = f"{line}_{station}"
            feature_df.at[index, f"{line}_seen"] = 1
            feature_df.at[index, f"{line}_duration"] = row["process_time"]
            feature_df.at[index, f"{line}_date_count"] = 1
            feature_df.at[index, f"{line}_date_missing_ratio"] = 0.0
            feature_df.at[index, f"{station_prefix}_seen"] = 1
            feature_df.at[index, f"{station_prefix}_span"] = row["process_time"]
            feature_df.at[index, f"{station_prefix}_mean_time"] = row["process_time"]
            feature_df.at[index, f"{station_prefix}_std_time"] = 0.0
            feature_df.at[index, "bottleneck_station"] = station_prefix

        return feature_df

    @staticmethod
    def _safe_float(value: Any, *, default: float) -> float:
        """float 변환"""
        if value is None or pd.isna(value):
            return default
        return float(value)

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        """int 변환"""
        if value is None or pd.isna(value):
            return None
        return int(value)

    def _resolve_model_features(self, features: pd.DataFrame) -> list[str]:
        """모델 피처 목록 확인"""
        model_feature_names = getattr(self.model, "feature_names_in_", None)
        if model_feature_names is not None:
            return list(model_feature_names)

        model_steps = getattr(self.model, "named_steps", {})
        for step in model_steps.values():
            step_feature_names = getattr(step, "feature_names_in_", None)
            if step_feature_names is not None:
                return list(step_feature_names)

        return default_model_feature_columns(features)

    def _patch_loaded_model(self, model: Any) -> None:
        """구버전 sklearn 모델 보정"""
        steps = getattr(model, "named_steps", {})
        for step in steps.values():
            if step.__class__.__name__ == "SimpleImputer" and not hasattr(step, "_fill_dtype"):
                step._fill_dtype = getattr(step, "_fit_dtype", None)

    @staticmethod
    def _to_risk_level(
        *,
        risk_score: float,
        affected_vehicle_count: int,
        max_affected: int,
    ) -> int:
        """위험 등급 계산"""
        affected_ratio = affected_vehicle_count / max_affected
        combined_score = 0.75 * risk_score + 0.25 * affected_ratio
        return int(np.clip(np.ceil(combined_score * 5), 1, 5))
