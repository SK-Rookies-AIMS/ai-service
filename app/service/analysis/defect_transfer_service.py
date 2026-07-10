from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any

from fastapi import status
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import settings
from app.core.exceptions import AppException
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
from app.utils.process_label_utils import NEXT_PROCESS, format_process_with_line


DEFECT_TRANSFER_CACHE_VERSION = "v8"
logger = logging.getLogger(__name__)


class DefectTransferAnalysisService:
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
    ) -> DefectTransferPredictionPage:
        cache_key = self._prediction_cache_key(
            cursor=cursor,
            size=size,
        )
        cached_value = self._cache_get(cache_key)
        if cached_value:
            cached_page = DefectTransferPredictionPage.model_validate(from_json(cached_value))
            if cached_page.content:
                return cached_page

        page = self.get_predictions(
            cursor=cursor,
            size=size,
        )
        self._cache_set(cache_key, page.model_dump(by_alias=False))
        return page

    def get_cached_cause_analysis(
        self,
        *,
        vehicle_id: str | None,
        cursor: int | None,
        size: int,
    ) -> DefectTransferCausePage:
        cache_key = self._cause_cache_key(
            vehicle_id=vehicle_id,
            cursor=cursor,
            size=size,
        )
        cached_value = self._cache_get(cache_key)
        if cached_value:
            cached_page = DefectTransferCausePage.model_validate(from_json(cached_value))
            if cached_page.content:
                return cached_page

        page = self.get_cause_analysis(
            vehicle_id=vehicle_id,
            cursor=cursor,
            size=size,
        )
        self._cache_set(cache_key, page.model_dump(by_alias=False))
        return page

    def clear_cache(self) -> None:
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
    ) -> DefectTransferPredictionPage:
        page = max(cursor or 0, 0)
        safe_size = max(1, min(size, 100))
        rows: list[dict[str, Any]]
        has_next = False
        if self.search_repository is not None:
            try:
                rows, has_next = self.search_repository.list_defect_prediction_page(
                    cursor=page,
                    size=safe_size,
                )
            except Exception:
                logger.exception("Elasticsearch defect prediction query failed. Falling back to DB.")
                try:
                    rows, has_next = self.repository.list_prediction_page(
                        cursor=page,
                        size=safe_size,
                    )
                except SQLAlchemyError as exc:
                    raise self._db_exception() from exc
        else:
            try:
                rows, has_next = self.repository.list_prediction_page(
                    cursor=page,
                    size=safe_size,
                )
            except SQLAlchemyError as exc:
                raise self._db_exception() from exc

        return DefectTransferPredictionPage(
            content=[self._to_prediction_item(row) for row in rows],
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
    ) -> DefectTransferCausePage:
        page = max(cursor or 0, 0)
        safe_size = max(1, min(size, 100))
        selected: dict[str, Any] | None
        rows: list[dict[str, Any]]
        has_next = False
        if self.search_repository is not None:
            try:
                selected = self.search_repository.get_latest_defect_cause_document(
                    vehicle_id=vehicle_id,
                )
                rows = [selected] if selected is not None else []
            except Exception:
                logger.exception("Elasticsearch defect cause query failed. Falling back to DB.")
                try:
                    selected, rows, has_next = self.repository.list_cause_page(
                        vehicle_id=vehicle_id,
                        cursor=page,
                        size=safe_size,
                    )
                except SQLAlchemyError as exc:
                    raise self._db_exception() from exc
        else:
            try:
                selected, rows, has_next = self.repository.list_cause_page(
                    vehicle_id=vehicle_id,
                    cursor=page,
                    size=safe_size,
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
                content=[],
                hasNext=False,
                nextCursor=None,
            )

        offset = page * safe_size
        paged_rows = rows[offset : offset + safe_size]
        display_rows = [row for row in paged_rows if self._is_displayable_cause(row)]
        if has_next is False:
            has_next = len(rows) > offset + safe_size

        return DefectTransferCausePage(
            vehicleId=str(selected.get("vehicle_id")),
            carMasterId=int(selected["car_master_id"]),
            predictedDefectProbability=self._result_probability(selected),
            riskLevel=selected.get("risk_grade"),
            currentProcess=format_process_with_line(
                selected.get("source_process_code"),
                selected.get("source_equipment_code"),
            ),
            predictedDefectProcess=self._resolve_predicted_defect_process(selected),
            transferProbability=self._percent(selected.get("target_defect_probability")),
            content=[
                self._to_cause_item(
                    row,
                    rank=index,
                )
                for index, row in enumerate(display_rows, page * safe_size + 1)
            ],
            hasNext=has_next,
            nextCursor=page + 1 if has_next else None,
        )

    def _to_prediction_item(
        self,
        row: dict[str, Any],
    ) -> DefectTransferPredictionItem:
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
    def _percent(value: Any) -> int:
        if value is None:
            return 0
        return round(float(value) * 100)

    @classmethod
    def _result_probability(cls, row: dict[str, Any]) -> int:
        value = row.get("target_defect_probability")
        if value is None:
            value = row.get("current_defect_probability")
        return cls._percent(value)

    @classmethod
    def _resolve_predicted_defect_process(cls, row: dict[str, Any]) -> str | None:
        if cls._result_probability(row) <= 0:
            return None

        db_val = row.get("predicted_defect_process")
        if db_val:
            return db_val

        source_code = str(row.get("source_process_code") or "").strip().upper()
        if row.get("target_defect_probability") is not None:
            process_code = row.get("target_process_code") or NEXT_PROCESS.get(source_code)
            equipment_code = row.get("target_equipment_code")
        else:
            process_code = row.get("source_process_code")
            equipment_code = row.get("source_equipment_code")

        if not process_code:
            return None
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
    def _to_cause_item(
        cls,
        row: dict[str, Any],
        *,
        rank: int,
    ) -> DefectTransferCauseItem:
        main_causes = row.get("main_causes")
        normalized_main_causes: list[dict[str, Any]] = []
        if isinstance(main_causes, list):
            for cause in main_causes:
                if not isinstance(cause, dict):
                    continue
                message = str(cause.get("message") or cause.get("label") or "").strip()
                if not message:
                    continue
                try:
                    impact = float(cause.get("impact") or 0.0)
                except (TypeError, ValueError):
                    impact = 0.0
                normalized_main_causes.append(
                    {
                        "message": message,
                        "impact": impact,
                    },
                )

        return DefectTransferCauseItem(
            rank=rank,
            feature="main_causes",
            label=cls._primary_cause_message(row),
            value="",
            impact=float(row.get("influence_score") or 0.0),
            message=cls._primary_cause_message(row),
            main_causes=normalized_main_causes,
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
        except Exception as exc:
            self._redis_client = None
            raise AppException(
                "Defect transfer Redis cache lookup failed.",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            ) from exc

    def _cache_set(self, cache_key: str, data: dict[str, Any]) -> None:
        if not settings.redis_url:
            return
        try:
            self._redis().setex(
                cache_key,
                settings.redis_cache_ttl_seconds,
                to_json(data),
            )
        except Exception as exc:
            self._redis_client = None
            raise AppException(
                "Defect transfer Redis cache write failed.",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            ) from exc

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
    ) -> str:
        page = max(cursor or 0, 0)
        safe_size = max(1, min(size, 100))
        return (
            f"{settings.redis_key_prefix}:process:defect-transfer:"
            f"predictions:{DEFECT_TRANSFER_CACHE_VERSION}:{page}:{safe_size}"
        )

    def _cause_cache_key(
        self,
        *,
        vehicle_id: str | None,
        cursor: int | None,
        size: int,
    ) -> str:
        page = max(cursor or 0, 0)
        safe_size = max(1, min(size, 100))
        return (
            f"{settings.redis_key_prefix}:process:defect-transfer:"
            f"causes:{DEFECT_TRANSFER_CACHE_VERSION}:{self._safe_cache_part(vehicle_id)}:{page}:{safe_size}"
        )


@lru_cache(maxsize=1)
def get_defect_transfer_analysis_service() -> DefectTransferAnalysisService:
    return DefectTransferAnalysisService()
