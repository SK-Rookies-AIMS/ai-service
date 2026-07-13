from __future__ import annotations

import logging
import re
from collections import Counter
from datetime import date as DateType
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import status

from app.core.config import settings
from app.core.exceptions import AppException
from app.dto.response import (
    AnalysisDateOption,
    BottleneckAnalysisItem,
    BottleneckAnalysisPage,
)
from app.ml.inference.bottleneck_detector import BottleneckDetector
from app.repository.bottleneck_analysis_repository import BottleneckAnalysisRepository
from app.search.process_analysis_search import ProcessAnalysisSearchRepository
from app.utils.datetime_utils import seoul_now
from app.utils.json_utils import from_json, to_json


DEFAULT_BOTTLENECK_MODEL_PATH = Path("app/ml/artifacts/bottleneck/bottleneck_iforest_model.pkl")
BOTTLENECK_CACHE_VERSION = "v14"
PROCESS_CODE_LABELS = {
    "PRESS": "프레스",
    "BODY": "차체",
    "PAINT": "도장",
    "ASSEMBLY": "의장",
    "INSPECTION": "검사",
}
PROCESS_EQUIPMENT_PREFIXES = {
    "PRESS": "P",
    "BODY": "S",
    "PAINT": "L",
    "ASSEMBLY": "A",
    "INSPECTION": "I",
}
logger = logging.getLogger(__name__)


class BottleneckAnalysisService:
    def __init__(
        self,
        *,
        model_path: str | Path | None = None,
        database_url: str | None = None,
    ) -> None:
        self.model_path = Path(model_path or DEFAULT_BOTTLENECK_MODEL_PATH)
        self.repository = BottleneckAnalysisRepository(
            database_url or settings.bottleneck_database_url,
            event_database_url=settings.sample_database_connection_url,
        )
        self.search_repository = self._create_search_repository()
        self.detector = BottleneckDetector(self.model_path)
        self._redis_client: Any | None = None

    def get_realtime_bottlenecks(
        self,
        *,
        cursor: int | None,
        size: int,
        date: DateType | None = None,
    ) -> BottleneckAnalysisPage:
        size = max(1, min(size, 100))
        page = max(cursor or 0, 0)
        cache_key = self._bottleneck_cache_key(
            cursor=cursor,
            size=size,
            date=date,
        )
        date_options = self._safe_get_date_options()
        selected_date = self._resolve_date(date, date_options)
        rows: list[dict[str, Any]]
        has_next = False
        if self.search_repository is not None:
            try:
                rows, has_next = self.search_repository.list_bottleneck_page(
                    cursor=page,
                    size=size,
                    analysis_date=selected_date,
                )
            except Exception:
                logger.exception("Elasticsearch bottleneck query failed. Falling back to DB.")
                cached_value = self._cache_get(cache_key)
                if cached_value:
                    return BottleneckAnalysisPage.model_validate(from_json(cached_value))
                rows = self.repository.list_results(
                    cursor=page,
                    size=size,
                    analysis_date=selected_date,
                )
                has_next = self.repository.count_results(
                    analysis_date=selected_date,
                ) > (page + 1) * size
        else:
            cached_value = self._cache_get(cache_key)
            if cached_value:
                return BottleneckAnalysisPage.model_validate(from_json(cached_value))
            rows = self.repository.list_results(
                cursor=page,
                size=size,
                analysis_date=selected_date,
            )
            has_next = self.repository.count_results(
                analysis_date=selected_date,
            ) > (page + 1) * size
        if not rows:
            return BottleneckAnalysisPage(
                mostBottleneckProcess=None,
                mostBottleneckRiskLevel=None,
                date=selected_date,
                dateOptions=date_options,
                content=[],
                hasNext=False,
                nextCursor=None,
            )

        top_row = rows[0]

        return BottleneckAnalysisPage(
            mostBottleneckProcess=self._format_process_label(top_row["process_code"]),
            mostBottleneckRiskLevel=self._risk_level_label(float(top_row["risk_score"])),
            date=selected_date,
            dateOptions=date_options,
            content=[
                BottleneckAnalysisItem(
                    rankNo=int(row["rank_no"]),
                    processCode=self._format_process_code(
                        row["process_code"],
                        row.get("equipment_code"),
                    ),
                    delayTime=round(float(row["avg_delay_time"]), 2),
                    affectedVehicleCount=int(row["affected_vehicle_count"]),
                    riskScore=float(row["risk_score"]),
                    riskLevel=self._risk_level_label(float(row["risk_score"])),
                )
                for row in rows
            ],
            hasNext=has_next,
            nextCursor=page + 1 if has_next else None,
        )

    def get_cached_realtime_bottlenecks(
        self,
        *,
        cursor: int | None,
        size: int,
        date: DateType | None = None,
    ) -> BottleneckAnalysisPage:
        cache_key = self._bottleneck_cache_key(
            cursor=cursor,
            size=size,
            date=date,
        )
        page = self.get_realtime_bottlenecks(
            cursor=cursor,
            size=size,
            date=date,
        )
        self._cache_set(cache_key, page.model_dump(by_alias=False))
        return page

    def run_analysis_and_save(self, *, cursor: int, size: int) -> tuple[int, bool]:
        merged_summaries = self.refresh_results_from_events()
        offset = cursor * size
        page_summaries = merged_summaries[offset : offset + size]
        has_next = len(merged_summaries) > offset + size
        return len(page_summaries), has_next

    def refresh_results_from_events(self) -> list[dict[str, Any]]:
        histories = self.repository.list_pending_manufacturing_event_histories()
        if not histories:
            return self._current_bottleneck_results()

        if not self.model_path.exists():
            raise AppException(
                f"Bottleneck model not found: {self.model_path}",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        new_summaries = self.detector.summarize_manufacturing_event_histories(histories)
        merged_summaries = self._rank_bottleneck_summaries(
            self._current_bottleneck_results() + new_summaries,
        )
        self.repository.replace_results(
            merged_summaries,
            detected_at=seoul_now().replace(tzinfo=None),
            start_rank=1,
            end_rank=max(len(merged_summaries), 1),
        )
        self.repository.mark_bottleneck_analysis_done(
            [
                int(row["manufacturing_event_id"])
                for row in histories
                if row.get("manufacturing_event_id") is not None
            ],
        )
        return merged_summaries

    def _redis(self) -> Any:
        if self._redis_client is None:
            try:
                from redis import Redis
            except ModuleNotFoundError as exc:
                raise RuntimeError("redis package is required for bottleneck cache.") from exc

            self._redis_client = Redis.from_url(
                settings.redis_connection_url,
                decode_responses=True,
            )
        return self._redis_client

    def _cache_get(self, cache_key: str) -> str | None:
        if not settings.redis_url:
            return None
        try:
            return self._redis().get(cache_key)
        except Exception:
            self._redis_client = None
            logger.exception("Bottleneck Redis cache lookup failed.")
            return None

    def _cache_set(self, cache_key: str, data: dict[str, Any]) -> None:
        if not settings.redis_url:
            return
        try:
            self._redis().setex(
                cache_key,
                settings.redis_cache_ttl_seconds,
                to_json(data),
            )
        except Exception:
            self._redis_client = None
            logger.exception("Bottleneck Redis cache write failed.")

    def _bottleneck_cache_key(
        self,
        *,
        cursor: int | None,
        size: int,
        date: DateType | None,
    ) -> str:
        page = max(cursor or 0, 0)
        safe_size = max(1, min(size, 100))
        return (
            f"{settings.redis_key_prefix}:process:bottleneck:"
            f"{BOTTLENECK_CACHE_VERSION}:{self._safe_cache_part(date.isoformat() if date else None)}:{page}:{safe_size}"
        )

    @staticmethod
    def _safe_cache_part(value: str | None) -> str:
        text = str(value or "__default__").strip()
        return text.replace(":", "_").replace("/", "_").replace("\\", "_")

    @staticmethod
    def _format_process_code(
        process_code: Any,
        equipment_code: Any | None = None,
    ) -> str:
        normalized = str(process_code or "").strip().upper()
        label = PROCESS_CODE_LABELS.get(normalized, normalized)
        prefix = PROCESS_EQUIPMENT_PREFIXES.get(normalized)
        equipment_no = BottleneckAnalysisService._equipment_number(equipment_code)
        if prefix and equipment_no is not None:
            return f"{label} ({prefix}{equipment_no})"
        return label

    @staticmethod
    def _format_process_label(process_code: Any) -> str:
        normalized = str(process_code or "").strip().upper()
        return PROCESS_CODE_LABELS.get(normalized, normalized)

    @staticmethod
    def _risk_level_label(risk_score: float) -> str:
        return "HIGH" if risk_score >= 3.0 else "NORMAL"

    def _current_bottleneck_results(self) -> list[dict[str, Any]]:
        total_count = self.repository.count_results()
        if total_count <= 0:
            return []
        return self.repository.list_results(
            cursor=0,
            size=total_count,
        )

    def _get_date_options(self) -> list[AnalysisDateOption]:
        options = self.repository.list_date_options()
        return [
            AnalysisDateOption.model_validate(
                {
                    "date": row["date"],
                    "sampleEventId": row.get("sample_event_id"),
                },
            )
            for row in options
            if row.get("date") is not None
        ]

    def _safe_get_date_options(self) -> list[AnalysisDateOption]:
        try:
            return self._get_date_options()
        except Exception:
            logger.exception("Failed to load bottleneck date options.")
            return []

    @staticmethod
    def _resolve_date(
        requested_date: DateType | None,
        date_options: list[AnalysisDateOption],
    ) -> DateType | None:
        if requested_date is not None:
            return requested_date
        if date_options:
            return date_options[0].date
        return None

    @staticmethod
    def _create_search_repository() -> ProcessAnalysisSearchRepository | None:
        if not settings.elasticsearch_url:
            return None
        try:
            repository = ProcessAnalysisSearchRepository()
            repository.ensure_indices()
            return repository
        except Exception:
            logger.exception("Elasticsearch bottleneck repository is unavailable.")
            return None

    @staticmethod
    def _rank_bottleneck_summaries(
        summaries: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        ranked = sorted(
            summaries,
            key=lambda row: (
                float(row.get("risk_score") or 0.0),
                float(row.get("avg_delay_time") or 0.0),
                int(row.get("affected_vehicle_count") or 0),
                int(row.get("manufacturing_event_id") or 0),
                int(row.get("car_master_id") or 0),
                str(row.get("process_code") or "").strip().upper(),
                str(row.get("equipment_code") or "").strip().upper(),
            ),
            reverse=True,
        )
        deduped: list[dict[str, Any]] = []
        seen_keys: set[tuple[str, str]] = set()

        for row in ranked:
            key = (
                str(row.get("process_code") or "").strip().upper(),
                str(row.get("equipment_code") or "").strip().upper(),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(row)

        return [{**row, "rank_no": index + 1} for index, row in enumerate(deduped)]

    @staticmethod
    def _equipment_number(equipment_code: Any | None) -> int | None:
        if equipment_code is None:
            return None
        match = re.search(r"(\d+)$", str(equipment_code).strip())
        if not match:
            return None
        return int(match.group(1))


def get_bottleneck_analysis_service() -> BottleneckAnalysisService:
    return BottleneckAnalysisService()
