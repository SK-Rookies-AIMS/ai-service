from __future__ import annotations

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
from app.utils.json_utils import from_json, to_json
from app.utils.process_label_utils import NEXT_PROCESS, format_process_with_line


DEFECT_TRANSFER_CACHE_VERSION = "v4"


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
        self._redis_client: Any | None = None

    def get_cached_predictions(
        self,
        *,
        cursor: int | None,
        size: int,
    ) -> DefectTransferPredictionPage:
        cache_key = self._prediction_cache_key(cursor=cursor, size=size)
        cached_value = self._cache_get(cache_key)
        if cached_value:
            return DefectTransferPredictionPage.model_validate(from_json(cached_value))

        page = self.get_predictions(cursor=cursor, size=size)
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
            return DefectTransferCausePage.model_validate(from_json(cached_value))

        page = self.get_cause_analysis(
            vehicle_id=vehicle_id,
            cursor=cursor,
            size=size,
        )
        self._cache_set(cache_key, page.model_dump(by_alias=False))
        return page

    def get_predictions(
        self,
        *,
        cursor: int | None,
        size: int,
    ) -> DefectTransferPredictionPage:
        page = max(cursor or 0, 0)
        safe_size = max(1, min(size, 100))
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

    def get_cause_analysis(
        self,
        *,
        vehicle_id: str | None,
        cursor: int | None,
        size: int,
    ) -> DefectTransferCausePage:
        page = max(cursor or 0, 0)
        safe_size = max(1, min(size, 100))
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
                DefectTransferCauseItem(
                    rank=index,
                    feature="main_cause",
                    label=str(row.get("main_cause") or ""),
                    value=self._display_value(row.get("influence_score")),
                    impact=float(row.get("influence_score") or 0.0),
                    message=str(row.get("main_cause") or ""),
                )
                for index, row in enumerate(rows, page * safe_size + 1)
            ],
            hasNext=has_next,
            nextCursor=page + 1 if has_next else None,
        )

    def _to_prediction_item(
        self,
        row: dict[str, Any],
    ) -> DefectTransferPredictionItem:
        vehicle_id = str(row.get("vehicle_id"))
        return DefectTransferPredictionItem(
            vehicleId=vehicle_id,
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

    @staticmethod
    def _db_exception() -> AppException:
        return AppException(
            "불량 전이 예측 결과 데이터베이스 조회에 실패했습니다. "
            "MAIN_DB_NAME/SAMPLE_DB_NAME/DB 계정 권한을 확인해주세요.",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    def _cache_get(self, cache_key: str) -> str | None:
        if not settings.redis_url:
            return None
        try:
            return self._redis().get(cache_key)
        except Exception as exc:
            self._redis_client = None
            raise AppException(
                "불량 전이 예측 Redis 캐시 조회에 실패했습니다.",
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
                "불량 전이 예측 Redis 캐시 저장에 실패했습니다.",
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

    def _prediction_cache_key(self, *, cursor: int | None, size: int) -> str:
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
        safe_vehicle_id = self._safe_cache_part(vehicle_id)
        return (
            f"{settings.redis_key_prefix}:process:defect-transfer:"
            f"causes:{DEFECT_TRANSFER_CACHE_VERSION}:{safe_vehicle_id}:{page}:{safe_size}"
        )


@lru_cache(maxsize=1)
def get_defect_transfer_analysis_service() -> DefectTransferAnalysisService:
    return DefectTransferAnalysisService()
