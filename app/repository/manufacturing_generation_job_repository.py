from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

from sqlalchemy import func, select, text

from app.repository.sampledb_schema import manufacturing_event_generation_job


MANUFACTURING_GENERATION_LOCK_NAME = "manufacturing_event_generation"


class ManufacturingGenerationJobRepository:
    """제조 이벤트 비동기 job의 상태와 전역 실행 잠금을 관리한다."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    @contextmanager
    def execution_lock(self, timeout_seconds: int = 600) -> Iterator[bool]:
        if self.engine.dialect.name != "mysql":
            yield True
            return

        with self.engine.connect() as conn:
            acquired = (
                conn.execute(
                    text("SELECT GET_LOCK(:lock_name, :timeout_seconds)"),
                    {
                        "lock_name": MANUFACTURING_GENERATION_LOCK_NAME,
                        "timeout_seconds": timeout_seconds,
                    },
                ).scalar()
                == 1
            )
            try:
                yield acquired
            finally:
                if acquired:
                    conn.execute(
                        text("SELECT RELEASE_LOCK(:lock_name)"),
                        {"lock_name": MANUFACTURING_GENERATION_LOCK_NAME},
                    )

    def create(
        self,
        *,
        job_id: str,
        job_type: str,
        request_json: dict[str, Any],
        total_expected_events: int = 0,
    ) -> dict[str, Any]:
        now = datetime.now()
        row = {
            "job_id": job_id,
            "job_type": job_type,
            "status": "PENDING",
            "request_json": request_json,
            "total_expected_events": total_expected_events,
            "generated_count": 0,
            "affected_rows": 0,
            "created_at": now,
            "updated_at": now,
        }
        with self.engine.begin() as conn:
            if self.engine.dialect.name != "mysql":
                next_id = int(
                    conn.execute(
                        select(func.max(manufacturing_event_generation_job.c.id)),
                    ).scalar()
                    or 0,
                )
                row = {**row, "id": next_id + 1}
            conn.execute(manufacturing_event_generation_job.insert(), row)
        return self.get(job_id) or {}

    def get(self, job_id: str) -> dict[str, Any] | None:
        query = select(manufacturing_event_generation_job).where(
            manufacturing_event_generation_job.c.job_id == job_id,
        )
        with self.engine.connect() as conn:
            row = conn.execute(query).mappings().first()
            return _format_job_row(dict(row)) if row else None

    def list_resumable(self) -> list[dict[str, Any]]:
        query = (
            select(manufacturing_event_generation_job)
            .where(
                manufacturing_event_generation_job.c.status.in_(
                    ["PENDING", "RUNNING"],
                ),
            )
            .order_by(
                manufacturing_event_generation_job.c.created_at.asc(),
                manufacturing_event_generation_job.c.id.asc(),
            )
        )
        with self.engine.connect() as conn:
            return [
                _format_job_row(dict(row))
                for row in conn.execute(query).mappings()
            ]

    def mark_running(self, job_id: str) -> None:
        now = datetime.now()
        self._update(
            job_id,
            status="RUNNING",
            started_at=now,
            updated_at=now,
        )

    def update_progress(
        self,
        job_id: str,
        progress: dict[str, Any],
    ) -> None:
        self._update(
            job_id,
            generated_count=int(progress.get("generatedCount", 0)),
            affected_rows=int(progress.get("affectedRows", 0)),
            total_expected_events=int(progress.get("totalExpectedEvents", 0)),
            updated_at=datetime.now(),
        )

    def mark_succeeded(
        self,
        job_id: str,
        result_json: dict[str, Any],
    ) -> None:
        now = datetime.now()
        self._update(
            job_id,
            status="SUCCEEDED",
            result_json=result_json,
            error_message=None,
            generated_count=int(result_json.get("generatedCount", 0)),
            affected_rows=int(result_json.get("affectedRows", 0)),
            total_expected_events=int(
                result_json.get(
                    "totalExpectedEvents",
                    result_json.get(
                        "templateEventCount",
                        result_json.get("eventCount", 0),
                    ),
                ),
            ),
            finished_at=now,
            updated_at=now,
        )

    def mark_failed(self, job_id: str, error_message: str) -> None:
        now = datetime.now()
        self._update(
            job_id,
            status="FAILED",
            error_message=error_message[:1000],
            finished_at=now,
            updated_at=now,
        )

    def _update(self, job_id: str, **values: Any) -> None:
        statement = (
            manufacturing_event_generation_job.update()
            .where(manufacturing_event_generation_job.c.job_id == job_id)
            .values(**values)
        )
        with self.engine.begin() as conn:
            conn.execute(statement)


def _format_job_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "jobId": row["job_id"],
        "jobType": row["job_type"],
        "status": row["status"],
        "request": row["request_json"],
        "result": row["result_json"],
        "errorMessage": row["error_message"],
        "totalExpectedEvents": int(row["total_expected_events"] or 0),
        "generatedCount": int(row["generated_count"] or 0),
        "affectedRows": int(row["affected_rows"] or 0),
        "createdAt": row["created_at"],
        "startedAt": row["started_at"],
        "finishedAt": row["finished_at"],
        "updatedAt": row["updated_at"],
    }
