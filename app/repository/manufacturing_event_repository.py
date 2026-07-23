from __future__ import annotations

import copy
import logging
import time
from collections.abc import Iterable
from datetime import date, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError

from app.data_generation.manufacturing_event_json_builder import (
    initial_dispatch_status,
    normalize_event_json,
)
from app.repository.sampledb_schema import manufacturing_event_json


logger = logging.getLogger(__name__)
MYSQL_LOCK_RETRY_ERROR_CODES = {1205, 1213}
MYSQL_LOCK_RETRY_DELAYS_SEC = (0.2, 0.5, 1.0, 2.0, 4.0)


class ManufacturingEventRepository:
    """manufacturing_event_json 원천 이벤트의 저장과 조회를 담당한다."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    def count_between(self, start_date: date, end_date: date) -> int:
        event_date_key = func.substr(manufacturing_event_json.c.event_id, 5, 8)
        query = select(func.count()).select_from(manufacturing_event_json).where(
            event_date_key >= start_date.strftime("%Y%m%d"),
            event_date_key <= end_date.strftime("%Y%m%d"),
        )
        with self.engine.connect() as conn:
            return int(conn.execute(query).scalar_one())

    def insert_rows(
        self,
        rows: Iterable[dict[str, Any]],
        *,
        update_existing: bool = True,
    ) -> int:
        now = datetime.now()
        payload = []
        for row in rows:
            event_json = normalize_event_json(copy.deepcopy(row["event_json"]))
            payload.append(
                {
                    "event_id": row["event_id"],
                    "event_time": None,
                    "car_master_id": row["car_master_id"],
                    "process_code": row["process_code"],
                    "equipment_id": row["equipment_id"],
                    "event_json": event_json,
                    "dispatch_status": initial_dispatch_status(
                        str(row["process_code"]),
                    ),
                    "analysis_status": row.get("analysis_status", "NOT_ANALYZED"),
                    "bottleneck_analysis_done": _analysis_flag_value(
                        row.get("bottleneck_analysis_done", False),
                    ),
                    "defect_transfer_analysis_done": _analysis_flag_value(
                        row.get("defect_transfer_analysis_done", False),
                    ),
                    "is_sent": row.get("is_sent", False),
                    "retry_count": row.get("retry_count", 0),
                    "error_message": row.get("error_message"),
                    "updated_at": now,
                },
            )
        if not payload:
            return 0

        # 동시 작업도 같은 event_id 순서로 행 잠금을 획득하도록 고정한다.
        payload.sort(key=lambda row: str(row["event_id"]))
        return self._insert_payload_with_retry(
            payload,
            update_existing=update_existing,
        )

    def insert_raw_rows(
        self,
        rows: Iterable[dict[str, Any]],
        *,
        update_existing: bool = True,
    ) -> int:
        now = datetime.now()
        payload = []
        for row in rows:
            event_json = normalize_event_json(copy.deepcopy(row["event_json"]))
            payload.append(
                {
                    "event_id": row["event_id"],
                    "event_time": row.get("event_time"),
                    "car_master_id": row["car_master_id"],
                    "process_code": row["process_code"],
                    "equipment_id": row["equipment_id"],
                    "event_json": event_json,
                    "dispatch_status": "SENT",
                    "bottleneck_analysis_done": _analysis_flag_value(
                        row.get("bottleneck_analysis_done", False),
                    ),
                    "defect_transfer_analysis_done": _analysis_flag_value(
                        row.get("defect_transfer_analysis_done", False),
                    ),
                    "is_sent": _is_true_flag(row.get("is_sent", True)),
                    "retry_count": row.get("retry_count", 0),
                    "error_message": row.get("error_message"),
                    "updated_at": now,
                },
            )
        if not payload:
            return 0

        payload.sort(key=lambda row: str(row["event_id"]))
        return self._insert_payload_with_retry(
            payload,
            update_existing=update_existing,
        )

    def _insert_payload_with_retry(
        self,
        payload: list[dict[str, Any]],
        *,
        update_existing: bool,
    ) -> int:
        attempts = len(MYSQL_LOCK_RETRY_DELAYS_SEC) + 1
        for attempt in range(attempts):
            try:
                return self._insert_payload_once(
                    payload,
                    update_existing=update_existing,
                )
            except OperationalError as exc:
                error_code = _mysql_error_code(exc)
                can_retry = (
                    self.engine.dialect.name == "mysql"
                    and error_code in MYSQL_LOCK_RETRY_ERROR_CODES
                    and attempt < attempts - 1
                )
                if not can_retry:
                    raise

                delay = MYSQL_LOCK_RETRY_DELAYS_SEC[attempt]
                logger.warning(
                    "manufacturing_event_json chunk 저장 잠금 충돌 재시도: "
                    "error_code=%s attempt=%s/%s delay=%.1fs",
                    error_code,
                    attempt + 1,
                    attempts - 1,
                    delay,
                )
                time.sleep(delay)

        raise RuntimeError("manufacturing_event_json chunk 저장 재시도에 실패했습니다.")

    def _insert_payload_once(
        self,
        payload: list[dict[str, Any]],
        *,
        update_existing: bool,
    ) -> int:
        with self.engine.begin() as conn:
            event_ids = [row["event_id"] for row in payload]
            existing_ids = {
                row["event_id"]
                for row in conn.execute(
                    select(manufacturing_event_json.c.event_id).where(
                        manufacturing_event_json.c.event_id.in_(event_ids),
                    ),
                ).mappings()
            }
            existing_rows = {
                row["event_id"]: row
                for row in conn.execute(
                    select(manufacturing_event_json).where(
                        manufacturing_event_json.c.event_id.in_(event_ids),
                    ),
                ).mappings()
            }

            updated_rows = 0
            if update_existing:
                for row in payload:
                    if row["event_id"] not in existing_ids:
                        continue
                    existing_row = existing_rows.get(row["event_id"])
                    if existing_row is not None:
                        row = {
                            **row,
                            "analysis_status": existing_row.get(
                                "analysis_status",
                                row.get("analysis_status"),
                            ),
                            "bottleneck_analysis_done": existing_row.get(
                                "bottleneck_analysis_done",
                                row.get("bottleneck_analysis_done"),
                            ),
                            "defect_transfer_analysis_done": existing_row.get(
                                "defect_transfer_analysis_done",
                                row.get("defect_transfer_analysis_done"),
                            ),
                        }
                    result = conn.execute(
                        manufacturing_event_json.update()
                        .where(
                            manufacturing_event_json.c.event_id
                            == row["event_id"],
                        )
                        .values(**row),
                    )
                    updated_rows += int(result.rowcount or 0)

            new_rows = [
                row for row in payload if row["event_id"] not in existing_ids
            ]
            if new_rows and self.engine.dialect.name != "mysql":
                next_id = int(
                    conn.execute(
                        select(func.max(manufacturing_event_json.c.id)),
                    ).scalar()
                    or 0,
                )
                new_rows = [
                    {**row, "id": next_id + index}
                    for index, row in enumerate(new_rows, 1)
                ]
            if new_rows:
                conn.execute(manufacturing_event_json.insert(), new_rows)
            return len(new_rows) + updated_rows

    def list_rows(
        self,
        *,
        limit: int,
        offset: int = 0,
        start_date: date | None = None,
        end_date: date | None = None,
        process_code: str | None = None,
        is_sent: bool | None = None,
    ) -> list[dict[str, Any]]:
        query = select(manufacturing_event_json)
        event_date_key = func.substr(manufacturing_event_json.c.event_id, 5, 8)
        if start_date:
            query = query.where(
                event_date_key >= start_date.strftime("%Y%m%d"),
            )
        if end_date:
            query = query.where(
                event_date_key <= end_date.strftime("%Y%m%d"),
            )
        if process_code:
            query = query.where(
                manufacturing_event_json.c.process_code == process_code,
            )
        if is_sent is not None:
            query = query.where(manufacturing_event_json.c.is_sent == is_sent)

        query = query.order_by(
            manufacturing_event_json.c.event_id.asc(),
            manufacturing_event_json.c.id.asc(),
        ).offset(offset).limit(limit)

        with self.engine.connect() as conn:
            return [dict(row) for row in conn.execute(query).mappings()]

    def get_analysis_flags(self, event_id: str) -> dict[str, bool] | None:
        query = select(
            manufacturing_event_json.c.event_id,
            manufacturing_event_json.c.bottleneck_analysis_done,
            manufacturing_event_json.c.defect_transfer_analysis_done,
        ).where(manufacturing_event_json.c.event_id == event_id)
        with self.engine.connect() as conn:
            row = conn.execute(query).mappings().first()
        if row is None:
            return None
        return {
            "bottleneck_analysis_done": _is_true_flag(
                row["bottleneck_analysis_done"],
            ),
            "defect_transfer_analysis_done": _is_true_flag(
                row["defect_transfer_analysis_done"],
            ),
        }

    def is_bottleneck_analysis_done(self, event_id: str) -> bool:
        query = select(manufacturing_event_json.c.bottleneck_analysis_done).where(
            manufacturing_event_json.c.event_id == event_id,
        )
        with self.engine.connect() as conn:
            row = conn.execute(query).mappings().first()
        return False if row is None else _is_true_flag(row["bottleneck_analysis_done"])

    def is_defect_transfer_analysis_done(self, event_id: str) -> bool:
        query = select(manufacturing_event_json.c.defect_transfer_analysis_done).where(
            manufacturing_event_json.c.event_id == event_id,
        )
        with self.engine.connect() as conn:
            row = conn.execute(query).mappings().first()
        return False if row is None else _is_true_flag(row["defect_transfer_analysis_done"])

    def mark_bottleneck_analysis_done(self, event_id: str) -> int:
        return self._mark_analysis_done(
            event_id,
            bottleneck_analysis_done=True,
        )

    def mark_defect_transfer_analysis_done(self, event_id: str) -> int:
        return self._mark_analysis_done(
            event_id,
            defect_transfer_analysis_done=True,
        )

    def _mark_analysis_done(self, event_id: str, **values: Any) -> int:
        from sqlalchemy import func

        with self.engine.begin() as conn:
            result = conn.execute(
                manufacturing_event_json.update()
                .where(manufacturing_event_json.c.event_id == event_id)
                .values(**values, updated_at=func.current_timestamp()),
            )
        return int(result.rowcount or 0)


def _analysis_flag_value(value: Any) -> bool:
    return _is_true_flag(value)


def _is_true_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() == "true"


def _mysql_error_code(exc: OperationalError) -> int | None:
    original = getattr(exc, "orig", None)
    args = getattr(original, "args", ())
    return args[0] if args and isinstance(args[0], int) else None
