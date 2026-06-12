import re
from dataclasses import dataclass

import pandas as pd

DATE_COL_PATTERN = re.compile(r"^(L\d+)_S(\d+)_D\d+$")

PROCESS_CODE_BY_LINE = {
    "L0": "PRESS",
    "L1": "BODY",
    "L2": "ASSEMBLY",
    "L3": "PAINT",
}


@dataclass(frozen=True)
class BottleneckStation:
    line: str
    station: str

    @property
    def equipment_code(self) -> str:
        return f"{self.line}_{self.station}"

    @property
    def process_code(self) -> str:
        return PROCESS_CODE_BY_LINE.get(self.line, "INSPECTION")


def parse_date_columns(columns: list[str]) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for column in columns:
        match = DATE_COL_PATTERN.match(column)
        if match:
            rows.append(
                {
                    "column": column,
                    "line": match.group(1),
                    "station": f"S{match.group(2)}",
                },
            )

    return pd.DataFrame(rows)


def parse_bottleneck_station(value: str | None) -> BottleneckStation:
    if value is None or pd.isna(value) or not str(value):
        return BottleneckStation(line="L0", station="UNKNOWN")

    normalized_value = str(value)
    match = re.match(r"^(L\d+)_(S\d+)$", normalized_value)
    if not match:
        return BottleneckStation(line="L0", station=normalized_value)

    return BottleneckStation(line=match.group(1), station=match.group(2))


def build_bottleneck_features(
    raw_df: pd.DataFrame,
    column_meta: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    if "Id" not in raw_df.columns:
        raise ValueError("병목 탐지 입력 데이터에는 Id 컬럼이 필요합니다.")

    if column_meta is None:
        column_meta = parse_date_columns(list(raw_df.columns))
    if column_meta.empty:
        raise ValueError("L*_S*_D* 형식의 date feature 컬럼을 찾지 못했습니다.")

    feature_df = pd.DataFrame({"Id": raw_df["Id"]})
    date_cols = column_meta["column"].tolist()
    date_values = raw_df[date_cols]

    row_min = date_values.min(axis=1)
    row_max = date_values.max(axis=1)
    feature_df["process_start_time"] = row_min
    feature_df["process_end_time"] = row_max
    feature_df["total_duration"] = row_max - row_min
    feature_df["observed_date_count"] = date_values.notna().sum(axis=1)
    feature_df["date_missing_ratio"] = date_values.isna().mean(axis=1)

    station_span_cols: list[str] = []
    for (line, station), group in column_meta.groupby(["line", "station"], sort=True):
        cols = group["column"].tolist()
        prefix = f"{line}_{station}"
        station_values = raw_df[cols]
        station_min = station_values.min(axis=1)
        station_max = station_values.max(axis=1)

        feature_df[f"{prefix}_seen"] = station_values.notna().any(axis=1).astype("int8")
        feature_df[f"{prefix}_span"] = station_max - station_min
        feature_df[f"{prefix}_mean_time"] = station_values.mean(axis=1)
        feature_df[f"{prefix}_std_time"] = station_values.std(axis=1)
        station_span_cols.append(f"{prefix}_span")

    for line, group in column_meta.groupby("line", sort=True):
        cols = group["column"].tolist()
        line_values = raw_df[cols]
        feature_df[f"{line}_seen"] = line_values.notna().any(axis=1).astype("int8")
        feature_df[f"{line}_duration"] = line_values.max(axis=1) - line_values.min(axis=1)
        feature_df[f"{line}_date_count"] = line_values.notna().sum(axis=1)
        feature_df[f"{line}_date_missing_ratio"] = line_values.isna().mean(axis=1)

    station_spans = feature_df[station_span_cols]
    station_seen_cols = [c for c in feature_df.columns if re.match(r"^L\d+_S\d+_seen$", c)]
    feature_df["max_station_span"] = station_spans.max(axis=1)
    feature_df["mean_station_span"] = station_spans.mean(axis=1)
    feature_df["std_station_span"] = station_spans.std(axis=1)
    feature_df["active_station_count"] = feature_df[station_seen_cols].sum(axis=1)
    has_station_span = station_spans.notna().any(axis=1)
    bottleneck_station = station_spans.fillna(float("-inf")).idxmax(axis=1)
    feature_df["bottleneck_station"] = (
        bottleneck_station.where(has_station_span)
        .str.replace("_span", "", regex=False)
    )

    return feature_df, station_span_cols


def default_model_feature_columns(features: pd.DataFrame) -> list[str]:
    base_columns = [
        "total_duration",
        "observed_date_count",
        "date_missing_ratio",
        "active_station_count",
        "max_station_span",
        "mean_station_span",
        "std_station_span",
    ]
    pattern_columns = [
        c
        for c in features.columns
        if re.match(r"^L\d+_duration$", c)
        or re.match(r"^L\d+_date_count$", c)
        or re.match(r"^L\d+_date_missing_ratio$", c)
    ]

    return [c for c in base_columns + pattern_columns if c in features.columns]
