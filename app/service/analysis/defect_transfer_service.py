from __future__ import annotations

import logging
from functools import lru_cache
from datetime import date as DateType
from typing import Any

from fastapi import status
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import settings
from app.core.exceptions import AppException
from app.dto.response import AnalysisDateOption
from app.dto.response.defect_transfer_response import (
    DefectTransferCauseItem,
    DefectTransferCausePage,
    DefectTransferPredictionItem,
    DefectTransferPredictionPage,
)
from app.repository.defect_transfer_prediction_repository import (
    DefectTransferPredictionRepository,
)
from app.search.process_analysis_search import ProcessAnalysisSearchRepository
from app.utils.json_utils import from_json, to_json
from app.utils.process_label_utils import (
    NEXT_PROCESS,
    equipment_code_for_car_process,
    format_process_with_line,
)


DEFECT_TRANSFER_CACHE_VERSION = "v11"
logger = logging.getLogger(__name__)


class DefectTransferAnalysisService:
    """불량 전이 예측 결과와 원인 분석을 ES 우선으로 조회한다."""
    def __init__(
        self,
        *,
        database_url: str | None = None,
        event_database_url: str | None = None,
        repository: DefectTransferPredictionRepository | None = None,
    ) -> None:
        main_database_url = database_url or settings.main_database_connection_url
        sample_database_url = event_database_url or settings.sample_database_connection_url
        if not sample_database_url:
            raise AppException(
                "sample database connection URL is required.",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        self.repository = repository or DefectTransferPredictionRepository(
            main_database_url,
            event_database_url=sample_database_url,
        )
        self.search_repository = self._create_search_repository()
        self._redis_client: Any | None = None

    def get_cached_predictions(
        self,
        *,
        cursor: int | None,
        size: int,
        date: DateType | None = None,
    ) -> DefectTransferPredictionPage:
        cache_key = self._prediction_cache_key(
            cursor=cursor,
            size=size,
            date=date,
        )
        page = self.get_predictions(
            cursor=cursor,
            size=size,
            date=date,
        )
        self._cache_set(cache_key, page.model_dump(by_alias=False))
        return page

    def get_cached_cause_analysis(
        self,
        *,
        vehicle_id: str | None,
        cursor: int | None,
        size: int,
        date: DateType | None = None,
    ) -> DefectTransferCausePage:
        cache_key = self._cause_cache_key(
            vehicle_id=vehicle_id,
            cursor=cursor,
            size=size,
            date=date,
        )
        page = self.get_cause_analysis(
            vehicle_id=vehicle_id,
            cursor=cursor,
            size=size,
            date=date,
        )
        self._cache_set(cache_key, page.model_dump(by_alias=False))
        return page

    def clear_cache(self) -> None:
        """불량 전이 조회 캐시를 모두 삭제한다."""
        if not settings.redis_url:
            return
        try:
            redis_client = self._redis()
            pattern = f"{settings.redis_key_prefix}:process:defect-transfer:*"
            keys = list(redis_client.scan_iter(match=pattern))
            if keys:
                redis_client.delete(*keys)
        except Exception as exc:
            self._redis_client = None
            raise AppException(
                "Defect transfer Redis cache clear failed.",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            ) from exc

    def get_predictions(
        self,
        *,
        cursor: int | None,
        size: int,
        date: DateType | None = None,
    ) -> DefectTransferPredictionPage:
        """불량 전이 목록을 ES 우선으로 조회하고, 실패 시 DB로 내려간다."""
        """조회한 불량 전이 목록을 Redis 캐시에 저장한다."""
        # ES를 먼저 보고, 실패하면 Redis 캐시와 DB 결과로 이어서 반환한다.
        page = max(cursor or 0, 0)
        safe_size = max(1, min(size, 100))
        cache_key = self._prediction_cache_key(
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
                rows, has_next = self.search_repository.list_defect_prediction_page(
                    cursor=page,
                    size=safe_size,
                    analysis_date=selected_date,
                )
            except Exception:
                logger.exception("Elasticsearch defect prediction query failed. Falling back to DB.")
                cached_value = self._cache_get(cache_key)
                if cached_value:
                    cached_page = DefectTransferPredictionPage.model_validate(from_json(cached_value))
                    if cached_page.content:
                        return cached_page
                try:
                    rows, has_next = self.repository.list_prediction_page(
                        cursor=page,
                        size=safe_size,
                        analysis_date=selected_date,
                    )
                except SQLAlchemyError as exc:
                    raise self._db_exception() from exc
        else:
            cached_value = self._cache_get(cache_key)
            if cached_value:
                cached_page = DefectTransferPredictionPage.model_validate(from_json(cached_value))
                if cached_page.content:
                    return cached_page
            try:
                rows, has_next = self.repository.list_prediction_page(
                    cursor=page,
                    size=safe_size,
                    analysis_date=selected_date,
                )
            except SQLAlchemyError as exc:
                raise self._db_exception() from exc

        return DefectTransferPredictionPage(
            content=[self._to_prediction_item(row) for row in rows],
            date=selected_date,
            dateOptions=date_options,
            hasNext=has_next,
            nextCursor=page + 1 if has_next else None,
        )

    def get_diagnostics(self) -> dict[str, Any]:
        try:
            diagnostics = self.repository.diagnostics()
        except SQLAlchemyError as exc:
            raise self._db_exception() from exc

        if diagnostics["sourceEventCount"] == 0:
            status_text = "NO_SOURCE_EVENTS"
            message = "sampledb.manufacturing_event_json source events not found."
        elif diagnostics["sentSourceEventCount"] == 0:
            status_text = "NO_SENT_SOURCE_EVENTS"
            message = "No sent source events were found."
        elif diagnostics["predictionResultRowCount"] == 0:
            status_text = "NO_PREDICTION_RESULTS"
            message = "No defect transfer prediction results were found."
        elif diagnostics["visiblePredictionCarCount"] == 0:
            status_text = "NO_VISIBLE_PREDICTIONS"
            message = "Prediction rows exist, but none are visible in the list."
        else:
            status_text = "OK"
            message = "Defect transfer analysis data is available."

        return {
            **diagnostics,
            "status": status_text,
            "message": message,
        }

    def get_cause_analysis(
        self,
        *,
        vehicle_id: str | None,
        cursor: int | None,
        size: int,
        date: DateType | None = None,
    ) -> DefectTransferCausePage:
        """대표 원인과 상세 원인을 함께 반환하는 불량 전이 원인 분석을 조회한다."""
        """조회한 불량 전이 원인 분석을 Redis 캐시에 저장한다."""
        # 원인 분석은 대표 원인과 상세 원인을 분리해서 화면에 맞게 구성한다.
        page = max(cursor or 0, 0)
        safe_size = max(1, min(size, 100))
        cache_key = self._cause_cache_key(
            vehicle_id=vehicle_id,
            cursor=cursor,
            size=size,
            date=date,
        )
        date_options = self._safe_get_date_options(vehicle_id=vehicle_id)
        selected_date = self._resolve_date(date, date_options)
        selected: dict[str, Any] | None
        if self.search_repository is not None:
            try:
                selected = self.search_repository.get_latest_defect_cause_document(
                    vehicle_id=vehicle_id,
                    analysis_date=selected_date,
                )
                if selected is None:
                    selected, _rows, _has_next = self.repository.list_cause_page(
                        vehicle_id=vehicle_id,
                        cursor=page,
                        size=safe_size,
                        analysis_date=selected_date,
                    )
            except Exception:
                logger.exception("Elasticsearch defect cause query failed. Falling back to DB.")
                cached_value = self._cache_get(cache_key)
                if cached_value:
                    cached_page = DefectTransferCausePage.model_validate(from_json(cached_value))
                    if cached_page.content:
                        return cached_page
                try:
                    selected, _rows, _has_next = self.repository.list_cause_page(
                        vehicle_id=vehicle_id,
                        cursor=page,
                        size=safe_size,
                        analysis_date=selected_date,
                    )
                except SQLAlchemyError as exc:
                    raise self._db_exception() from exc
        else:
            cached_value = self._cache_get(cache_key)
            if cached_value:
                cached_page = DefectTransferCausePage.model_validate(from_json(cached_value))
                if cached_page.content:
                    return cached_page
            try:
                selected, _rows, _has_next = self.repository.list_cause_page(
                    vehicle_id=vehicle_id,
                    cursor=page,
                    size=safe_size,
                    analysis_date=selected_date,
                )
            except SQLAlchemyError as exc:
                raise self._db_exception() from exc

        if selected is None:
            return DefectTransferCausePage(
                vehicleId=vehicle_id,
                carMasterId=None,
                predictedDefectProbability=None,
                riskLevel=None,
                currentProcess=None,
                predictedDefectProcess=None,
                transferProbability=None,
                date=selected_date,
                dateOptions=date_options,
                content=[],
                hasNext=False,
                nextCursor=None,
            )

        representative_cause, detail_causes = self._build_cause_sections(selected)
        offset = page * safe_size
        display_detail_causes = detail_causes[offset : offset + safe_size]
        has_next = len(detail_causes) > offset + safe_size

        return DefectTransferCausePage(
            vehicleId=str(selected.get("vehicle_id")),
            carMasterId=int(selected["car_master_id"]),
            predictedDefectProbability=self._normalize_probability(
                selected.get("target_defect_probability"),
            ),
            riskLevel=selected.get("risk_grade"),
            currentProcess=format_process_with_line(
                selected.get("source_process_code"),
                selected.get("source_equipment_code"),
            ),
            predictedDefectProcess=self._resolve_predicted_defect_process(selected),
            transferProbability=self._normalize_probability(selected.get("target_defect_probability")),
            date=selected_date,
            dateOptions=date_options,
            content=[representative_cause],
            representativeCause=representative_cause,
            detailCauses=display_detail_causes,
            hasNext=has_next,
            nextCursor=page + 1 if has_next else None,
        )

    def _to_prediction_item(
        self,
        row: dict[str, Any],
    ) -> DefectTransferPredictionItem:
        """DB row를 불량 전이 목록 응답 DTO로 변환한다."""
        # 저장된 결과 row를 목록 카드용 DTO로 변환한다.
        return DefectTransferPredictionItem(
            vehicleId=str(row.get("vehicle_id")),
            carMasterId=int(row["car_master_id"]),
            currentProcess=format_process_with_line(
                row.get("source_process_code"),
                row.get("source_equipment_code"),
            ),
            predictedDefectProcess=self._resolve_predicted_defect_process(row),
            defectProbability=self._result_probability(row),
            expectedTime=(
                f"{int(row['expected_occurrence_step'])}단계 후"
                if row.get("expected_occurrence_step") is not None
                else None
            ),
            riskLevel=str(row.get("risk_grade") or "LOW"),
        )

    @staticmethod
    def _normalize_probability(value: Any) -> float:
        # 99.24 또는 0.9924처럼 들어와도 화면에서는 동일한 확률로 맞춘다.
        if value is None:
            return 0.0
        normalized = float(value)
        if abs(normalized) > 1.0:
            normalized /= 100.0
        return round(normalized, 4)

    @classmethod
    def _result_probability(cls, row: dict[str, Any]) -> float:
        # 현재 화면 기준 확률은 current -> defect -> target 순으로 확인한다.
        value = row.get("current_defect_probability")
        if value is None:
            value = row.get("defect_probability")
        if value is None:
            value = row.get("target_defect_probability")
        return cls._normalize_probability(value)

    @classmethod
    def _resolve_predicted_defect_process(cls, row: dict[str, Any]) -> str | None:
        # 목표 공정이 있으면 우선 사용하고, 없으면 다음 공정을 계산한다.
        if cls._result_probability(row) <= 0:
            return None

        source_code = str(row.get("source_process_code") or "").strip().upper()
        process_code = row.get("target_process_code") or NEXT_PROCESS.get(source_code)
        if not process_code:
            return None
        process_code = str(process_code).strip().upper()
        if process_code == source_code:
            equipment_code = row.get("source_equipment_code")
        else:
            equipment_code = row.get("target_equipment_code") or equipment_code_for_car_process(
                car_master_id=int(row["car_master_id"]),
                process_code=process_code,
            )

        db_val = row.get("predicted_defect_process")
        if db_val and "(" in str(db_val) and ")" in str(db_val):
            return str(db_val)

        return format_process_with_line(process_code, equipment_code)

    @staticmethod
    def _display_value(value: Any) -> str:
        if isinstance(value, float):
            return f"{value:.2f}"
        return str(value)

    @classmethod
    def _is_displayable_cause(cls, row: dict[str, Any]) -> bool:
        message = cls._primary_cause_message(row).strip()
        if not message:
            return False

        value = cls._first_number(message)
        if value is None:
            return True

        thresholds = {
            "pressure": 0.0,
            "temperature": 4.0,
            "vibration": 0.45,
            "cycle time": 55.0,
            "wip": 24.0,
            "rms": 2.2,
        }
        lower_message = message.lower()
        for prefix, threshold in thresholds.items():
            if prefix in lower_message:
                return value > threshold
        return value > 0.0

    @staticmethod
    def _primary_cause_message(row: dict[str, Any]) -> str:
        main_causes = row.get("main_causes")
        if isinstance(main_causes, list) and main_causes:
            first = main_causes[0]
            if isinstance(first, dict):
                message = first.get("message") or first.get("label") or ""
                return str(message).strip()

        return ""

    @classmethod
    def _build_cause_sections(
        cls,
        row: dict[str, Any],
    ) -> tuple[DefectTransferCauseItem, list[DefectTransferCauseItem]]:
        """대표 원인 1개와 상세 원인 리스트를 분리해 구성한다."""
        # 대표 원인 1개와 보조 원인 리스트를 분리해 응답 구조를 만든다.
        main_causes = cls._normalize_main_causes(row.get("main_causes") or row.get("causes"))
        if main_causes:
            representative_cause = cls._to_cause_item(
                {
                    "rank": main_causes[0].get("rank") or 1,
                    "feature": main_causes[0].get("feature") or "main_causes",
                    "label": main_causes[0].get("label") or main_causes[0].get("message") or "",
                    "value": main_causes[0].get("value") or "",
                    "impact": main_causes[0].get("impact") or 0.0,
                    "message": main_causes[0].get("message") or main_causes[0].get("label") or "",
                },
                rank=1,
            )
            detail_causes = [
                cls._to_cause_item(
                    {
                        "rank": cause.get("rank") or index,
                        "feature": cause.get("feature") or "main_causes",
                        "label": cause.get("label") or cause.get("message") or "",
                        "value": cause.get("value") or "",
                        "impact": cause.get("impact") or 0.0,
                        "message": cause.get("message") or cause.get("label") or "",
                    },
                    rank=index,
                )
                for index, cause in enumerate(main_causes[1:], start=2)
            ]
            return representative_cause, detail_causes

        summary_message = cls._primary_cause_message(row)
        if not summary_message:
            summary_message = "no main cause available"
        representative_cause = cls._to_cause_item(
            {
                "rank": 1,
                "feature": "main_causes",
                "label": summary_message,
                "value": "",
                "impact": float(row.get("influence_score") or 0.0),
                "message": summary_message,
            },
            rank=1,
        )
        return representative_cause, []

    @staticmethod
    def _normalize_main_causes(causes: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for cause in causes[:5]:
            message = str(cause.get("message") or cause.get("label") or "").strip()
            if not message:
                continue
            try:
                impact = float(cause.get("impact") or 0.0)
            except (TypeError, ValueError):
                impact = 0.0
            normalized.append(
                {
                    "message": message,
                    "impact": impact,
                },
            )

        if normalized:
            return normalized

        return [
            {
                "message": "no main cause available",
                "impact": 0.0,
            },
        ]

    @classmethod
    def _to_cause_item(
        cls,
        row: dict[str, Any],
        *,
        rank: int,
    ) -> DefectTransferCauseItem:
        message = str(row.get("message") or row.get("label") or "").strip()
        if not message:
            message = cls._primary_cause_message(row)
        if not message:
            message = "no main cause available"

        return DefectTransferCauseItem(
            rank=int(row.get("rank") or rank),
            feature=str(row.get("feature") or "main_causes"),
            label=str(row.get("label") or message),
            value=str(row.get("value") or ""),
            impact=float(row.get("impact") or 0.0),
            message=message,
        )

    @staticmethod
    def _first_number(text: str) -> float | None:
        import re

        match = re.search(r"-?\d+(?:\.\d+)?", text)
        return float(match.group(0)) if match else None

    @staticmethod
    def _db_exception() -> AppException:
        return AppException(
            "Defect transfer analysis query failed. Please check DB settings and permissions.",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    @staticmethod
    def _create_search_repository() -> ProcessAnalysisSearchRepository | None:
        if not settings.elasticsearch_url:
            return None
        try:
            repository = ProcessAnalysisSearchRepository()
            repository.ensure_indices()
            return repository
        except Exception:
            logger.exception("Elasticsearch defect transfer repository is unavailable.")
            return None

    def _cache_get(self, cache_key: str) -> str | None:
        if not settings.redis_url:
            return None
        try:
            return self._redis().get(cache_key)
        except Exception:
            self._redis_client = None
            logger.exception("Defect transfer Redis cache lookup failed.")
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
            logger.exception("Defect transfer Redis cache write failed.")

    def _redis(self) -> Any:
        if self._redis_client is None:
            try:
                from redis import Redis
            except ModuleNotFoundError as exc:
                raise RuntimeError("redis package is required for defect transfer cache.") from exc

            self._redis_client = Redis.from_url(
                settings.redis_connection_url,
                decode_responses=True,
            )
        return self._redis_client

    @staticmethod
    def _safe_cache_part(value: Any) -> str:
        text = str(value or "__default__").strip()
        return text.replace(":", "_").replace("/", "_").replace("\\", "_")

    def _prediction_cache_key(
        self,
        *,
        cursor: int | None,
        size: int,
        date: DateType | None,
    ) -> str:
        page = max(cursor or 0, 0)
        safe_size = max(1, min(size, 100))
        return (
            f"{settings.redis_key_prefix}:process:defect-transfer:"
            f"predictions:{DEFECT_TRANSFER_CACHE_VERSION}:{self._safe_cache_part(date.isoformat() if date else None)}:{page}:{safe_size}"
        )

    def _cause_cache_key(
        self,
        *,
        vehicle_id: str | None,
        cursor: int | None,
        size: int,
        date: DateType | None,
    ) -> str:
        page = max(cursor or 0, 0)
        safe_size = max(1, min(size, 100))
        return (
            f"{settings.redis_key_prefix}:process:defect-transfer:"
            f"causes:{DEFECT_TRANSFER_CACHE_VERSION}:{self._safe_cache_part(vehicle_id)}:{self._safe_cache_part(date.isoformat() if date else None)}:{page}:{safe_size}"
        )

    def _get_date_options(
        self,
        *,
        vehicle_id: str | None = None,
    ) -> list[AnalysisDateOption]:
        options = self.repository.list_date_options(vehicle_id=vehicle_id)
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

    def _safe_get_date_options(
        self,
        *,
        vehicle_id: str | None = None,
    ) -> list[AnalysisDateOption]:
        try:
            return self._get_date_options(vehicle_id=vehicle_id)
        except Exception:
            logger.exception("Failed to load defect transfer date options.")
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


@lru_cache(maxsize=1)
def get_defect_transfer_analysis_service() -> DefectTransferAnalysisService:
    return DefectTransferAnalysisService()
