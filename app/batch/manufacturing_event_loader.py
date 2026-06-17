from __future__ import annotations

import argparse
import logging
import time
from datetime import date, timedelta
from typing import Any

from app.core.config import settings
from app.repository.sampledb_repository import SampleDbRepository
from app.service.manufacturing_event_json_service import (
    DEFAULT_CAR_POOL_SIZE,
    DEFAULT_EVENTS_PER_DAY,
    DEFAULT_INSERT_CHUNK_SIZE,
    DEFAULT_TEMPLATE_NAME,
    ManufacturingEventJsonService,
)


logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    args = _parse_args()

    if not settings.sample_database_connection_url:
        raise SystemExit("SAMPLE_DATABASE_URL 설정이 필요합니다.")

    repository = SampleDbRepository(settings.sample_database_connection_url)
    service = ManufacturingEventJsonService(repository)
    started_at = time.monotonic()
    last_progress_at = {"value": 0.0}

    if args.template:
        _run_template_batch(
            args=args,
            service=service,
            started_at=started_at,
            last_progress_at=last_progress_at,
        )
        return

    start_date, end_date = _resolve_date_range(args)
    logger.info(
        "제조 이벤트 JSON 배치 적재 시작: start=%s end=%s events_per_day=%s "
        "car_pool_size=%s chunk_size=%s update_existing=%s",
        start_date.isoformat(),
        end_date.isoformat(),
        args.events_per_day,
        args.car_pool_size,
        args.chunk_size,
        args.update_existing,
    )

    result = service.generate_range(
        start_date=start_date,
        end_date=end_date,
        events_per_day=args.events_per_day,
        car_pool_size=args.car_pool_size,
        insert_chunk_size=args.chunk_size,
        update_existing=args.update_existing,
        progress_callback=_progress_logger(
            started_at=started_at,
            last_progress_at=last_progress_at,
            interval_seconds=args.progress_interval_seconds,
        ),
    )

    elapsed = max(0.001, time.monotonic() - started_at)
    logger.info(
        "제조 이벤트 JSON 배치 적재 완료: generated=%s affected=%s stored=%s "
        "elapsed=%.1fs rate=%.1f rows/s distribution=%s",
        result["generatedCount"],
        result["affectedRows"],
        result["storedCountInRange"],
        elapsed,
        result["generatedCount"] / elapsed,
        result["processDistribution"],
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "CSV 원천 데이터에서 제조 이벤트 템플릿 또는 "
            "sampledb.manufacturing_event_json을 배치 적재합니다."
        ),
    )
    range_group = parser.add_mutually_exclusive_group(required=True)
    range_group.add_argument(
        "--template",
        action="store_true",
        help="날짜별 물리 적재 대신 하루 재생 템플릿을 생성합니다.",
    )
    range_group.add_argument(
        "--initial",
        action="store_true",
        help="2026-06-01부터 2026-06-16까지 초기 시연 데이터를 적재합니다.",
    )
    range_group.add_argument(
        "--tomorrow",
        action="store_true",
        help="오늘 기준 다음날 데이터를 적재합니다.",
    )
    range_group.add_argument(
        "--start-date",
        type=_date_arg,
        help="적재 시작일입니다. 예: 2026-06-01",
    )
    parser.add_argument(
        "--end-date",
        type=_date_arg,
        help="적재 종료일입니다. --start-date와 함께 사용합니다.",
    )
    parser.add_argument(
        "--events-per-day",
        type=int,
        default=DEFAULT_EVENTS_PER_DAY,
        help="하루 생성 이벤트 수입니다. 기본값은 템플릿 기준 86400입니다.",
    )
    parser.add_argument(
        "--template-name",
        default=DEFAULT_TEMPLATE_NAME,
        help="생성할 템플릿 이름입니다.",
    )
    parser.add_argument(
        "--car-pool-size",
        type=int,
        default=DEFAULT_CAR_POOL_SIZE,
        help="재사용할 차량 마스터 풀 크기입니다.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_INSERT_CHUNK_SIZE,
        help="DB에 한 번에 insert할 row 수입니다.",
    )
    parser.add_argument(
        "--update-existing",
        action="store_true",
        help=(
            "중복 event_id/template_event_id가 있으면 기존 row를 업데이트합니다. "
            "기본은 INSERT IGNORE입니다."
        ),
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="템플릿 생성 시 기존 템플릿 row를 삭제하고 다시 만듭니다.",
    )
    parser.add_argument(
        "--progress-interval-seconds",
        type=float,
        default=10.0,
        help="진행 로그 출력 간격입니다.",
    )
    return parser.parse_args()


def _resolve_date_range(args: argparse.Namespace) -> tuple[date, date]:
    if args.initial:
        return date(2026, 6, 1), date(2026, 6, 16)
    if args.tomorrow:
        target_date = date.today() + timedelta(days=1)
        return target_date, target_date
    if not args.start_date:
        raise SystemExit("--start-date가 필요합니다.")
    return args.start_date, args.end_date or args.start_date


def _run_template_batch(
    *,
    args: argparse.Namespace,
    service: ManufacturingEventJsonService,
    started_at: float,
    last_progress_at: dict[str, float],
) -> None:
    logger.info(
        "제조 이벤트 템플릿 배치 생성 시작: template=%s event_count=%s "
        "car_pool_size=%s chunk_size=%s replace=%s",
        args.template_name,
        args.events_per_day,
        args.car_pool_size,
        args.chunk_size,
        args.replace,
    )
    result = service.generate_template(
        template_name=args.template_name,
        event_count=args.events_per_day,
        car_pool_size=args.car_pool_size,
        insert_chunk_size=args.chunk_size,
        replace=args.replace,
        update_existing=args.update_existing or args.replace,
        progress_callback=_progress_logger(
            started_at=started_at,
            last_progress_at=last_progress_at,
            interval_seconds=args.progress_interval_seconds,
        ),
    )
    elapsed = max(0.001, time.monotonic() - started_at)
    logger.info(
        "제조 이벤트 템플릿 배치 생성 완료: template=%s generated=%s "
        "affected=%s stored=%s elapsed=%.1fs rate=%.1f rows/s distribution=%s",
        result["templateName"],
        result["generatedCount"],
        result["affectedRows"],
        result["storedCount"],
        elapsed,
        result["generatedCount"] / elapsed,
        result["processDistribution"],
    )


def _date_arg(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "날짜는 YYYY-MM-DD 형식이어야 합니다.",
        ) from exc


def _progress_logger(
    *,
    started_at: float,
    last_progress_at: dict[str, float],
    interval_seconds: float,
):
    def log_progress(progress: dict[str, Any]) -> None:
        now = time.monotonic()
        if now - last_progress_at["value"] < interval_seconds:
            return
        last_progress_at["value"] = now
        generated_count = int(progress["generatedCount"])
        total_expected = int(progress["totalExpectedEvents"])
        elapsed = max(0.001, now - started_at)
        percent = generated_count / total_expected * 100 if total_expected else 100.0
        logger.info(
            "제조 이벤트 JSON 배치 진행: generated=%s/%s %.2f%% affected=%s "
            "elapsed=%.1fs rate=%.1f rows/s",
            generated_count,
            total_expected,
            percent,
            progress["affectedRows"],
            elapsed,
            generated_count / elapsed,
        )

    return log_progress


if __name__ == "__main__":
    main()
