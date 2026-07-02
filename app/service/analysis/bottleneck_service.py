import logging
import re
from collections import Counter
from pathlib import Path
from typing import Any

from fastapi import status

from app.core.config import settings
from app.core.exceptions import AppException
from app.dto.response import BottleneckAnalysisItem, BottleneckAnalysisPage
from app.ml.inference.bottleneck_detector import BottleneckDetector
from app.repository.bottleneck_analysis_repository import BottleneckAnalysisRepository
from app.utils.datetime_utils import seoul_now
from app.utils.json_utils import from_json, to_json


DEFAULT_BOTTLENECK_MODEL_PATH = Path("app/ml/artifacts/bottleneck/bottleneck_iforest_model.pkl")
BOTTLENECK_CACHE_VERSION = "v9"
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
    """공정 이력 데이터를 분석해 병목 순위 결과를 제공하는 서비스"""

    def __init__(
        self,
        *,
        model_path: str | Path | None = None,
        database_url: str | None = None,
    ) -> None:
        """병목 분석에 필요한 저장소, 모델, Redis 클라이언트 상태를 초기화"""
        self.model_path = Path(model_path or DEFAULT_BOTTLENECK_MODEL_PATH)
        self.repository = BottleneckAnalysisRepository(
            database_url or settings.bottleneck_database_url,
            event_database_url=settings.sample_database_connection_url,
        )
        self.detector = BottleneckDetector(self.model_path)
        self._redis_client: Any | None = None

    def get_realtime_bottlenecks(
        self,
        *,
        cursor: int | None,
        size: int,
    ) -> BottleneckAnalysisPage:
        """현재 공정 이력을 분석하고 순위가 매겨진 결과 페이지를 반환"""
        size = max(1, min(size, 100))
        page = max(cursor or 0, 0)

        # 저장소에서 요청 페이지 범위만 조회
        rows = self.repository.list_results(cursor=page, size=size)
        if not rows:
            return BottleneckAnalysisPage(
                content=[],
                hasNext=False,
                nextCursor=None,
            )

        total_count = self.repository.count_results()
        has_next = total_count > (page + 1) * size
        next_cursor = page + 1 if has_next else None

        return BottleneckAnalysisPage(
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
                )
                for row in rows
            ],
            hasNext=has_next,
            nextCursor=next_cursor,
        )

    def get_cached_realtime_bottlenecks(
        self,
        *,
        cursor: int | None,
        size: int,
    ) -> BottleneckAnalysisPage:
        """Redis에 결과가 있으면 반환하고, 없으면 분석 후 캐시에 저장"""
        cache_key = self._bottleneck_cache_key(cursor=cursor, size=size)
        try:
            redis_client = self._redis()
            # cursor와 size를 key에 포함해 페이지별 캐시를 분리
            cached_value = redis_client.get(cache_key)
            if cached_value:
                logger.info(
                    "Bottleneck analysis cache hit: key=%s source=redis",
                    cache_key,
                )
                return BottleneckAnalysisPage.model_validate(from_json(cached_value))
        except Exception as exc:
            self._redis_client = None
            raise AppException(
                "Redis 캐시 조회에 실패했습니다.",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            ) from exc

        logger.info(
            "Bottleneck analysis cache miss: key=%s source=kafka_raw_db",
            cache_key,
        )
        page = self.get_realtime_bottlenecks(cursor=cursor, size=size)
        try:
            # 오래된 결과가 과도하게 남지 않도록 설정된 TTL로 저장
            self._redis().setex(
                cache_key,
                settings.redis_cache_ttl_seconds,
                to_json(page.model_dump(by_alias=False)),
            )
        except Exception as exc:
            self._redis_client = None
            raise AppException(
                "Redis 캐시 저장에 실패했습니다.",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            ) from exc
        return page

    def run_analysis_and_save(self, *, cursor: int, size: int) -> tuple[int, bool]:
        """전체 공정 이력을 분석하고 요청한 순위 페이지 결과만 저장"""
        histories = self.repository.list_manufacturing_event_histories()
        if not histories:
            logger.info(
                "Bottleneck analysis skipped: source=kafka_raw_db "
                "table=manufacturing_event_json filter=is_sent:true total=0 "
                "cursor=%s size=%s",
                cursor,
                size,
            )
            self.repository.prune_results_after_rank(0)
            return 0, False

        process_counts = Counter(str(row.get("process_code")) for row in histories)
        event_ids = [
            int(row["manufacturing_event_id"])
            for row in histories
            if row.get("manufacturing_event_id") is not None
        ]
        delay_times = [
            float(row.get("delay_time") or 0.0)
            for row in histories
        ]
        logger.info(
            "Bottleneck analysis input loaded: source=kafka_raw_db "
            "table=manufacturing_event_json filter=is_sent:true total=%s "
            "process_counts=%s delay_time_range=%.3f..%.3f "
            "manufacturing_event_id_range=%s..%s cursor=%s size=%s",
            len(histories),
            dict(sorted(process_counts.items())),
            min(delay_times) if delay_times else 0.0,
            max(delay_times) if delay_times else 0.0,
            min(event_ids) if event_ids else None,
            max(event_ids) if event_ids else None,
            cursor,
            size,
        )

        if not self.model_path.exists():
            raise AppException(
                f"병목 탐지 모델을 찾을 수 없습니다: {self.model_path}",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        summaries = self.detector.summarize_manufacturing_event_histories(histories)
        offset = cursor * size
        page_summaries = summaries[offset : offset + size]
        has_next = len(summaries) > offset + size
        start_rank = offset + 1
        end_rank = offset + size

        self.repository.replace_results(
            page_summaries,
            detected_at=seoul_now().replace(tzinfo=None),
            start_rank=start_rank,
            end_rank=end_rank,
        )
        if not has_next:
            self.repository.prune_results_after_rank(len(summaries))

        logger.info(
            "Bottleneck analysis results saved: source=kafka_raw_db "
            "result_count=%s rank_range=%s..%s has_next=%s",
            len(page_summaries),
            start_rank,
            end_rank,
            has_next,
        )
        return len(page_summaries), has_next

    def refresh_results_from_events(self) -> list[dict[str, Any]]:
        """Kafka raw 이벤트 수신 후 전체 병목 분석 결과를 즉시 갱신한다."""
        histories = self.repository.list_manufacturing_event_histories()
        if not histories:
            logger.info(
                "Bottleneck analysis skipped: source=kafka_raw_db "
                "table=manufacturing_event_json filter=is_sent:true total=0 "
                "trigger=kafka_raw_event",
            )
            self.repository.prune_results_after_rank(0)
            return []

        process_counts = Counter(str(row.get("process_code")) for row in histories)
        event_ids = [
            int(row["manufacturing_event_id"])
            for row in histories
            if row.get("manufacturing_event_id") is not None
        ]
        delay_times = [
            float(row.get("delay_time") or 0.0)
            for row in histories
        ]
        logger.info(
            "Bottleneck analysis input loaded: source=kafka_raw_db "
            "table=manufacturing_event_json filter=is_sent:true total=%s "
            "process_counts=%s delay_time_range=%.3f..%.3f "
            "manufacturing_event_id_range=%s..%s trigger=kafka_raw_event",
            len(histories),
            dict(sorted(process_counts.items())),
            min(delay_times) if delay_times else 0.0,
            max(delay_times) if delay_times else 0.0,
            min(event_ids) if event_ids else None,
            max(event_ids) if event_ids else None,
        )

        if not self.model_path.exists():
            raise AppException(
                f"병목 탐지 모델을 찾을 수 없습니다: {self.model_path}",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        summaries = self.detector.summarize_manufacturing_event_histories(histories)
        self.repository.replace_results(
            summaries,
            detected_at=seoul_now().replace(tzinfo=None),
            start_rank=1,
            end_rank=max(len(summaries), 1),
        )
        self.repository.prune_results_after_rank(len(summaries))
        logger.info(
            "Bottleneck analysis results saved: source=kafka_raw_db "
            "trigger=kafka_raw_event result_count=%s rank_range=1..%s",
            len(summaries),
            len(summaries),
        )
        return summaries

    def _redis(self) -> Any:
        """병목 캐시 작업에 사용할 Redis 클라이언트를 생성하고 재사용"""
        if self._redis_client is None:
            try:
                from redis import Redis
            except ModuleNotFoundError as exc:
                raise RuntimeError("병목 캐시를 사용하려면 redis 패키지가 필요합니다.") from exc

            self._redis_client = Redis.from_url(
                settings.redis_connection_url,
                decode_responses=True,
            )
        return self._redis_client

    def _bottleneck_cache_key(self, *, cursor: int | None, size: int) -> str:
        """요청한 병목 결과 페이지에 대한 Redis key를 생성"""
        page = max(cursor or 0, 0)
        safe_size = max(1, min(size, 100))
        return (
            f"{settings.redis_key_prefix}:process:bottleneck:"
            f"{BOTTLENECK_CACHE_VERSION}:{page}:{safe_size}"
        )

    @staticmethod
    def _format_process_code(
        process_code: Any,
        equipment_code: Any | None = None,
    ) -> str:
        normalized = str(process_code or "").strip().upper()
        label = PROCESS_CODE_LABELS.get(normalized, normalized)
        prefix = PROCESS_EQUIPMENT_PREFIXES.get(normalized)
        equipment_no = BottleneckAnalysisService._equipment_number(
            equipment_code,
        )
        if prefix and equipment_no is not None:
            return f"{label} ({prefix}{equipment_no})"
        return label

    @staticmethod
    def _equipment_number(equipment_code: Any | None) -> int | None:
        if equipment_code is None:
            return None

        match = re.search(r"(\d+)$", str(equipment_code).strip())
        if not match:
            return None
        return int(match.group(1))

def get_bottleneck_analysis_service() -> BottleneckAnalysisService:
    """FastAPI 의존성 주입에 사용할 병목 분석 서비스를 생성"""
    return BottleneckAnalysisService()
