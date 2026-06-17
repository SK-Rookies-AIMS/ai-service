from collections.abc import Iterable
from datetime import date, datetime
from typing import Any

from sqlalchemy import create_engine, func, inspect, select, text
from sqlalchemy.dialects.mysql import insert

from app.repository.sampledb_schema import (
    car_master,
    equipment,
    manufacturing_event_generation_job,
    manufacturing_event_json,
    manufacturing_event_template,
    metadata,
)
from app.utils.database_utils import mysql_connect_args_for_seoul


DEFAULT_EQUIPMENT_ROWS: list[dict[str, Any]] = [
    {
        "process_code": process_code,
        "equipment_code": f"EQ_{process_code}_{index:03d}",
        "equipment_name": f"{equipment_name_prefix} {index}호",
        "equipment_type": equipment_type,
    }
    for process_code, equipment_type, equipment_name_prefix in (
        ("PRESS", "HYDRAULIC_PRESS", "프레스 유압모터"),
        ("BODY", "ROBOT_ARM", "차체 용접 로봇"),
        ("PAINT", "CAMERA", "도장 열화상 카메라"),
        ("ASSEMBLY", "CONVEYOR", "의장 조립 컨베이어"),
    )
    for index in range(1, 5)
]

DEFAULT_CAR_TYPES = ("SEDAN", "SUV", "EV", "HEV")
DEFAULT_ENGINE_TYPES = ("GASOLINE", "DIESEL", "ELECTRIC", "HYBRID")
DEFAULT_CAR_COLORS = ("WHITE", "BLACK", "SILVER", "BLUE")


class SampleDbRepository:
    """sampledb 스키마와 제조 원천 이벤트 JSON 저장소."""

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.engine = create_engine(
            database_url,
            connect_args=mysql_connect_args_for_seoul(database_url),
            pool_pre_ping=True,
            future=True,
        )

    def ensure_schema(self) -> None:
        """sampledb 엔티티가 없으면 생성한다."""
        metadata.create_all(self.engine)
        self.drop_equipment_status_column()
        self.add_manufacturing_event_json_updated_at_column()

    def seed_equipment(self) -> None:
        """기본 설비 데이터를 입력하고 기존 설비 정보는 갱신한다."""
        with self.engine.begin() as conn:
            if self.engine.dialect.name == "mysql":
                statement = insert(equipment).values(DEFAULT_EQUIPMENT_ROWS)
                statement = statement.on_duplicate_key_update(
                    equipment_name=statement.inserted.equipment_name,
                    process_code=statement.inserted.process_code,
                    equipment_type=statement.inserted.equipment_type,
                )
                conn.execute(statement)
                return

            existing_codes = {
                row["equipment_code"]
                for row in conn.execute(select(equipment.c.equipment_code)).mappings()
            }
            rows = [
                row
                for row in DEFAULT_EQUIPMENT_ROWS
                if row["equipment_code"] not in existing_codes
            ]
            if rows:
                next_id = int(
                    conn.execute(select(func.max(equipment.c.id))).scalar() or 0,
                )
                rows = [{**row, "id": next_id + index} for index, row in enumerate(rows, 1)]
                conn.execute(equipment.insert(), rows)

    def drop_equipment_status_column(self) -> bool:
        if "status" not in self._table_columns("equipment"):
            return False
        with self.engine.begin() as conn:
            conn.execute(text("ALTER TABLE equipment DROP COLUMN status"))
        return True

    def add_manufacturing_event_json_updated_at_column(self) -> bool:
        if "updated_at" in self._table_columns("manufacturing_event_json"):
            return False

        with self.engine.begin() as conn:
            if self.engine.dialect.name == "mysql":
                conn.execute(
                    text(
                        "ALTER TABLE manufacturing_event_json "
                        "ADD COLUMN updated_at DATETIME NOT NULL "
                        "DEFAULT CURRENT_TIMESTAMP",
                    ),
                )
            else:
                conn.execute(
                    text(
                        "ALTER TABLE manufacturing_event_json "
                        "ADD COLUMN updated_at DATETIME",
                    ),
                )
                conn.execute(
                    text(
                        "UPDATE manufacturing_event_json "
                        "SET updated_at = CURRENT_TIMESTAMP "
                        "WHERE updated_at IS NULL",
                    ),
                )
        return True

    def _table_columns(self, table_name: str) -> set[str]:
        inspector = inspect(self.engine)
        if not inspector.has_table(table_name):
            return set(metadata.tables[table_name].c.keys())
        return {
            column["name"]
            for column in inspector.get_columns(table_name)
        }

    def ensure_car_master_rows(self, count: int) -> dict[str, int]:
        """이벤트 생성에 필요한 차량 마스터 데이터를 보장한다."""
        vehicles = [f"CAR-{index:06d}" for index in range(1, count + 1)]
        with self.engine.begin() as conn:
            existing = {
                row["vehicle_id"]: int(row["id"])
                for row in conn.execute(
                    select(car_master.c.id, car_master.c.vehicle_id).where(
                        car_master.c.vehicle_id.in_(vehicles),
                    ),
                ).mappings()
            }

            missing_rows = [
                {
                    "vehicle_id": vehicle_id,
                    "car_type": DEFAULT_CAR_TYPES[(index - 1) % len(DEFAULT_CAR_TYPES)],
                    "engine_type": DEFAULT_ENGINE_TYPES[
                        (index - 1) % len(DEFAULT_ENGINE_TYPES)
                    ],
                    "car_color": DEFAULT_CAR_COLORS[
                        (index - 1) % len(DEFAULT_CAR_COLORS)
                    ],
                    "fuel_efficiency": 11 + (index % 9),
                    "created_at": datetime.now(),
                }
                for index, vehicle_id in enumerate(vehicles, start=1)
                if vehicle_id not in existing
            ]
            if missing_rows:
                next_id = int(
                    conn.execute(select(func.max(car_master.c.id))).scalar() or 0,
                )
                missing_rows = [
                    {**row, "id": next_id + index}
                    for index, row in enumerate(missing_rows, 1)
                ]
                conn.execute(car_master.insert(), missing_rows)

            return {
                row["vehicle_id"]: int(row["id"])
                for row in conn.execute(
                    select(car_master.c.id, car_master.c.vehicle_id).where(
                        car_master.c.vehicle_id.in_(vehicles),
                    ),
                ).mappings()
            }

    def get_equipment_map(self) -> dict[str, dict[str, Any]]:
        """equipment_code 기준 설비 행을 반환한다."""
        query = select(
            equipment.c.id,
            equipment.c.process_code,
            equipment.c.equipment_code,
            equipment.c.equipment_name,
            equipment.c.equipment_type,
        )
        with self.engine.connect() as conn:
            return {
                row["equipment_code"]: dict(row)
                for row in conn.execute(query).mappings()
            }

    def count_event_json_between(self, start_date: date, end_date: date) -> int:
        """기간 내 생성된 원천 이벤트 JSON 수를 반환한다."""
        query = select(func.count()).select_from(manufacturing_event_json).where(
            func.date(manufacturing_event_json.c.event_time) >= start_date.isoformat(),
            func.date(manufacturing_event_json.c.event_time) <= end_date.isoformat(),
        )
        with self.engine.connect() as conn:
            return int(conn.execute(query).scalar_one())

    def count_template_events(self, template_name: str) -> int:
        """템플릿에 저장된 이벤트 수를 반환한다."""
        query = select(func.count()).select_from(manufacturing_event_template).where(
            manufacturing_event_template.c.template_name == template_name,
        )
        with self.engine.connect() as conn:
            return int(conn.execute(query).scalar_one())

    def delete_template_events(self, template_name: str) -> int:
        """템플릿 이벤트를 삭제한다."""
        statement = manufacturing_event_template.delete().where(
            manufacturing_event_template.c.template_name == template_name,
        )
        with self.engine.begin() as conn:
            result = conn.execute(statement)
            return int(result.rowcount or 0)

    def create_generation_job(
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
        return self.get_generation_job(job_id) or {}

    def get_generation_job(self, job_id: str) -> dict[str, Any] | None:
        query = select(manufacturing_event_generation_job).where(
            manufacturing_event_generation_job.c.job_id == job_id,
        )
        with self.engine.connect() as conn:
            row = conn.execute(query).mappings().first()
            return _format_generation_job_row(dict(row)) if row else None

    def list_resumable_generation_jobs(self) -> list[dict[str, Any]]:
        query = (
            select(manufacturing_event_generation_job)
            .where(manufacturing_event_generation_job.c.status.in_(["PENDING", "RUNNING"]))
            .order_by(
                manufacturing_event_generation_job.c.created_at.asc(),
                manufacturing_event_generation_job.c.id.asc(),
            )
        )
        with self.engine.connect() as conn:
            return [
                _format_generation_job_row(dict(row))
                for row in conn.execute(query).mappings()
            ]

    def mark_generation_job_running(self, job_id: str) -> None:
        now = datetime.now()
        self._update_generation_job(
            job_id,
            status="RUNNING",
            started_at=now,
            updated_at=now,
        )

    def update_generation_job_progress(
        self,
        job_id: str,
        progress: dict[str, Any],
    ) -> None:
        self._update_generation_job(
            job_id,
            generated_count=int(progress.get("generatedCount", 0)),
            affected_rows=int(progress.get("affectedRows", 0)),
            total_expected_events=int(progress.get("totalExpectedEvents", 0)),
            updated_at=datetime.now(),
        )

    def mark_generation_job_succeeded(
        self,
        job_id: str,
        result_json: dict[str, Any],
    ) -> None:
        now = datetime.now()
        self._update_generation_job(
            job_id,
            status="SUCCEEDED",
            result_json=result_json,
            error_message=None,
            generated_count=int(result_json.get("generatedCount", 0)),
            affected_rows=int(result_json.get("affectedRows", 0)),
            total_expected_events=int(
                result_json.get(
                    "totalExpectedEvents",
                    result_json.get("templateEventCount", result_json.get("eventCount", 0)),
                ),
            ),
            finished_at=now,
            updated_at=now,
        )

    def mark_generation_job_failed(self, job_id: str, error_message: str) -> None:
        now = datetime.now()
        self._update_generation_job(
            job_id,
            status="FAILED",
            error_message=error_message[:1000],
            finished_at=now,
            updated_at=now,
        )

    def _update_generation_job(self, job_id: str, **values: Any) -> None:
        statement = (
            manufacturing_event_generation_job.update()
            .where(manufacturing_event_generation_job.c.job_id == job_id)
            .values(**values)
        )
        with self.engine.begin() as conn:
            conn.execute(statement)

    def insert_event_json_rows(
        self,
        rows: Iterable[dict[str, Any]],
        *,
        update_existing: bool = True,
    ) -> int:
        """원천 이벤트 JSON을 저장한다. event_id 기준으로 재실행해도 중복 저장하지 않는다."""
        now = datetime.now()
        payload = [{**row, "updated_at": now} for row in rows]
        if not payload:
            return 0

        with self.engine.begin() as conn:
            if self.engine.dialect.name == "mysql":
                statement = insert(manufacturing_event_json).values(payload)
                if update_existing:
                    statement = statement.on_duplicate_key_update(
                        event_time=statement.inserted.event_time,
                        car_master_id=statement.inserted.car_master_id,
                        equipment_id=statement.inserted.equipment_id,
                        process_code=statement.inserted.process_code,
                        station_code=statement.inserted.station_code,
                        equipment_code=statement.inserted.equipment_code,
                        equipment_type=statement.inserted.equipment_type,
                        equipment_status=statement.inserted.equipment_status,
                        event_type=statement.inserted.event_type,
                        event_json=statement.inserted.event_json,
                        updated_at=statement.inserted.updated_at,
                    )
                else:
                    statement = statement.prefix_with("IGNORE")
                result = conn.execute(statement)
                return int(result.rowcount or 0)

            event_ids = [row["event_id"] for row in payload]
            existing_ids = {
                row["event_id"]
                for row in conn.execute(
                    select(manufacturing_event_json.c.event_id).where(
                        manufacturing_event_json.c.event_id.in_(event_ids),
                    ),
                ).mappings()
            }
            updated_rows = 0
            if update_existing:
                for row in payload:
                    if row["event_id"] not in existing_ids:
                        continue
                    result = conn.execute(
                        manufacturing_event_json.update()
                        .where(manufacturing_event_json.c.event_id == row["event_id"])
                        .values(
                            event_time=row["event_time"],
                            car_master_id=row["car_master_id"],
                            equipment_id=row["equipment_id"],
                            process_code=row["process_code"],
                            station_code=row["station_code"],
                            equipment_code=row["equipment_code"],
                            equipment_type=row["equipment_type"],
                            equipment_status=row["equipment_status"],
                            event_type=row["event_type"],
                            event_json=row["event_json"],
                            updated_at=row["updated_at"],
                        ),
                    )
                    updated_rows += int(result.rowcount or 0)

            new_rows = [row for row in payload if row["event_id"] not in existing_ids]
            if new_rows:
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
                conn.execute(manufacturing_event_json.insert(), new_rows)
            return len(new_rows) + updated_rows

    def insert_template_event_rows(
        self,
        rows: Iterable[dict[str, Any]],
        *,
        update_existing: bool = False,
    ) -> int:
        """하루 재생 템플릿 이벤트를 저장한다."""
        payload = list(rows)
        if not payload:
            return 0

        with self.engine.begin() as conn:
            if self.engine.dialect.name == "mysql":
                statement = insert(manufacturing_event_template).values(payload)
                if update_existing:
                    statement = statement.on_duplicate_key_update(
                        event_offset_us=statement.inserted.event_offset_us,
                        car_master_id=statement.inserted.car_master_id,
                        equipment_id=statement.inserted.equipment_id,
                        process_code=statement.inserted.process_code,
                        station_code=statement.inserted.station_code,
                        equipment_code=statement.inserted.equipment_code,
                        equipment_type=statement.inserted.equipment_type,
                        equipment_status=statement.inserted.equipment_status,
                        event_type=statement.inserted.event_type,
                        event_json=statement.inserted.event_json,
                    )
                else:
                    statement = statement.prefix_with("IGNORE")
                result = conn.execute(statement)
                return int(result.rowcount or 0)

            template_event_ids = [row["template_event_id"] for row in payload]
            existing_ids = {
                row["template_event_id"]
                for row in conn.execute(
                    select(manufacturing_event_template.c.template_event_id).where(
                        manufacturing_event_template.c.template_event_id.in_(
                            template_event_ids,
                        ),
                    ),
                ).mappings()
            }
            new_rows = [
                row
                for row in payload
                if row["template_event_id"] not in existing_ids
            ]
            if new_rows:
                next_id = int(
                    conn.execute(
                        select(func.max(manufacturing_event_template.c.id)),
                    ).scalar()
                    or 0,
                )
                new_rows = [
                    {**row, "id": next_id + index}
                    for index, row in enumerate(new_rows, 1)
                ]
                conn.execute(manufacturing_event_template.insert(), new_rows)
            return len(new_rows)

    def list_template_event_rows(
        self,
        *,
        template_name: str,
        limit: int,
        offset: int = 0,
        process_code: str | None = None,
    ) -> list[dict[str, Any]]:
        """템플릿 이벤트를 offset 순서로 조회한다."""
        query = select(manufacturing_event_template).where(
            manufacturing_event_template.c.template_name == template_name,
        )
        if process_code:
            query = query.where(
                manufacturing_event_template.c.process_code == process_code,
            )
        query = query.order_by(
            manufacturing_event_template.c.event_offset_us.asc(),
            manufacturing_event_template.c.id.asc(),
        ).offset(offset).limit(limit)

        with self.engine.connect() as conn:
            return [dict(row) for row in conn.execute(query).mappings()]

    def list_event_json_rows(
        self,
        *,
        limit: int,
        offset: int = 0,
        start_date: date | None = None,
        end_date: date | None = None,
        process_code: str | None = None,
        is_sent: bool | None = None,
    ) -> list[dict[str, Any]]:
        """생성된 원천 이벤트 JSON을 조회한다."""
        query = select(manufacturing_event_json)
        if start_date:
            query = query.where(
                func.date(manufacturing_event_json.c.event_time) >= start_date.isoformat(),
            )
        if end_date:
            query = query.where(
                func.date(manufacturing_event_json.c.event_time) <= end_date.isoformat(),
            )
        if process_code:
            query = query.where(manufacturing_event_json.c.process_code == process_code)
        if is_sent is not None:
            query = query.where(manufacturing_event_json.c.is_sent == is_sent)

        query = query.order_by(
            manufacturing_event_json.c.event_time.asc(),
            manufacturing_event_json.c.id.asc(),
        ).offset(offset).limit(limit)

        with self.engine.connect() as conn:
            return [dict(row) for row in conn.execute(query).mappings()]

    def initialize(self) -> None:
        """스키마를 생성하고 PRD에 필요한 참조 데이터를 입력한다."""
        self.ensure_schema()
        self.seed_equipment()


def initialize_sampledb(database_url: str) -> None:
    """sampledb 테이블과 참조 설비 데이터를 초기화한다."""
    SampleDbRepository(database_url).initialize()


def _format_generation_job_row(row: dict[str, Any]) -> dict[str, Any]:
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
