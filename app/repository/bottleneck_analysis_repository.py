from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from app.repository.sampledb_schema import manufacturing_event_json
from app.utils.database_utils import mysql_connect_args_for_seoul


class BottleneckAnalysisRepository:
    """Store bottleneck results and read source events from manufacturing_event_json."""

    def __init__(
        self,
        database_url: str,
        *,
        event_database_url: str | None = None,
    ) -> None:
        self.database_url = database_url
        self.event_database_url = event_database_url or database_url
        self.engine: Any | None = None
        self.event_engine: Any | None = None
        self.table: Any | None = None
        self.metadata: Any | None = None

        self._init_sqlalchemy()
        self.ensure_schema()

    def ensure_schema(self) -> None:
        self.metadata.create_all(self.engine)
        self._align_result_schema()

    def replace_results(
        self,
        rows: Iterable[dict[str, Any]],
        detected_at: Any,
        *,
        start_rank: int,
        end_rank: int,
    ) -> None:
        payload = [
            {
                key: value
                for key, value in {**row, "detected_at": detected_at}.items()
                if key != "id"
            }
            for row in rows
        ]
        if not payload:
            return

        with self.engine.begin() as conn:
            conn.execute(self.table.insert(), payload)

    def prune_results_after_rank(self, max_rank: int) -> None:
        from sqlalchemy import func
        with self.engine.begin() as conn:
            conn.execute(
                self.table.delete()
                .where(self.table.c.rank_no > max_rank)
                .where(func.date(self.table.c.detected_at) == func.current_date())
            )

    def list_results(
        self,
        *,
        cursor: int,
        size: int,
    ) -> list[dict[str, Any]]:
        snapshot_detected_at = self._latest_snapshot_detected_at()
        if snapshot_detected_at is None:
            return []

        from sqlalchemy import select

        query = (
            select(
                self.table.c.id,
                self.table.c.manufacturing_event_id,
                self.table.c.car_master_id,
                self.table.c.process_code,
                self.table.c.equipment_code,
                self.table.c.rank_no,
                self.table.c.avg_delay_time,
                self.table.c.affected_vehicle_count,
                self.table.c.risk_score,
                self.table.c.detected_at,
            )
            .where(self.table.c.detected_at == snapshot_detected_at)
            .order_by(
                self.table.c.rank_no.asc(),
                self.table.c.id.asc(),
            )
        )

        with self.engine.connect() as conn:
            rows = [dict(row) for row in conn.execute(query).mappings()]

        deduped = self._dedupe_rows(rows)
        offset = cursor * size
        return deduped[offset : offset + size]

    def count_results(
        self,
    ) -> int:
        snapshot_detected_at = self._latest_snapshot_detected_at()
        if snapshot_detected_at is None:
            return 0

        from sqlalchemy import func, select

        query = (
            select(func.count())
            .select_from(self.table)
            .where(self.table.c.detected_at == snapshot_detected_at)
        )
        with self.engine.connect() as conn:
            raw_count = int(conn.execute(query).scalar_one())

        if raw_count <= 0:
            return 0

        return len(self.list_results(cursor=0, size=raw_count))

    def list_manufacturing_event_histories(self) -> list[dict[str, Any]]:
        from sqlalchemy import select, func

        query = (
            select(
                manufacturing_event_json.c.id,
                manufacturing_event_json.c.car_master_id,
                manufacturing_event_json.c.process_code,
                manufacturing_event_json.c.equipment_id,
                manufacturing_event_json.c.event_time,
                manufacturing_event_json.c.event_json,
            )
            .where(manufacturing_event_json.c.dispatch_status == "SENT")
            .where(manufacturing_event_json.c.is_sent.is_(True))
            .where(func.date(manufacturing_event_json.c.event_time) == func.current_date())
            .order_by(manufacturing_event_json.c.id.asc())
        )
        with self.event_engine.connect() as conn:
            rows = conn.execute(query).mappings()
            return [
                self._to_bottleneck_history(row)
                for row in rows
            ]

    def list_pending_manufacturing_event_histories(self) -> list[dict[str, Any]]:
        from sqlalchemy import select, func

        query = (
            select(
                manufacturing_event_json.c.id,
                manufacturing_event_json.c.car_master_id,
                manufacturing_event_json.c.process_code,
                manufacturing_event_json.c.equipment_id,
                manufacturing_event_json.c.event_time,
                manufacturing_event_json.c.event_json,
            )
            .where(manufacturing_event_json.c.dispatch_status == "SENT")
            .where(manufacturing_event_json.c.is_sent.is_(True))
            .where(manufacturing_event_json.c.bottleneck_analysis_done.is_(False))
            .where(func.date(manufacturing_event_json.c.event_time) == func.current_date())
            .order_by(manufacturing_event_json.c.id.asc())
        )
        with self.event_engine.connect() as conn:
            rows = conn.execute(query).mappings()
            return [
                self._to_bottleneck_history(row)
                for row in rows
            ]

    def _event_ids_for_today(self) -> list[int]:
        from sqlalchemy import func, select

        query = (
            select(manufacturing_event_json.c.id)
            .where(manufacturing_event_json.c.dispatch_status == "SENT")
            .where(manufacturing_event_json.c.is_sent.is_(True))
            .where(func.date(manufacturing_event_json.c.event_time) == func.current_date())
        )
        with self.event_engine.connect() as conn:
            return [int(row[0]) for row in conn.execute(query).all()]

    def _latest_snapshot_detected_at(self) -> datetime | None:
        from sqlalchemy import func, select

        query = (
            select(func.max(self.table.c.detected_at))
            .where(func.date(self.table.c.detected_at) == func.current_date())
        )
        with self.engine.connect() as conn:
            value = conn.execute(query).scalar_one()
        return value

    def mark_bottleneck_analysis_done(self, event_ids: Iterable[int]) -> int:
        event_ids = [int(event_id) for event_id in event_ids]
        if not event_ids:
            return 0

        from sqlalchemy import func

        with self.event_engine.begin() as conn:
            result = conn.execute(
                manufacturing_event_json.update()
                .where(manufacturing_event_json.c.id.in_(event_ids))
                .values(
                    bottleneck_analysis_done=True,
                    updated_at=func.current_timestamp(),
                ),
            )
        return int(result.rowcount or 0)

    def _to_bottleneck_history(self, row: dict[str, Any]) -> dict[str, Any]:
        event_json = self._event_json_dict(row["event_json"])
        equipment = event_json.get("equipment", {})
        equipment_status = event_json.get("equipmentStatus", {})
        metrics = event_json.get("processMetrics", {})
        equipment_code = str(
            equipment.get("equipmentCode")
            or row.get("equipment_id")
            or "UNKNOWN",
        )
        process_data = event_json.get("processData", {})
        process_code = str(row["process_code"])
        operation_status = str(
            equipment_status.get("operationStatus") or "",
        ).upper()
        process_time = self._safe_float(metrics.get("processingTimeSec"))
        waiting_time = self._safe_float(metrics.get("waitingTimeSec"))
        cycle_time = self._safe_float(metrics.get("cycleTimeSec"))
        station_delay_time = self._safe_float(metrics.get("stationDelaySec"))
        timestamp_delay_time = self._process_timestamp_delay(
            process_data,
            process_code,
        )
        equipment_stop_delay_time = self._equipment_stop_delay(
            equipment_status,
            event_json,
            row.get("event_time"),
        )
        delay_time = (
            station_delay_time
            if station_delay_time > 0
            else timestamp_delay_time
            if timestamp_delay_time > 0
            else equipment_stop_delay_time
            if operation_status in {"FAULT", "STOPPED", "ERROR", "DOWN"}
            else 0.0
        )

        return {
            "id": int(row["id"]),
            "manufacturing_event_id": int(row["id"]),
            "car_master_id": row["car_master_id"],
            "process_code": process_code,
            "equipment_code": equipment_code,
            "equipment_status": operation_status,
            "station_key": equipment_code,
            "process_time": process_time,
            "waiting_time": waiting_time,
            "cycle_time": cycle_time,
            "delay_time": delay_time,
            "queue_length": self._safe_float(metrics.get("queueLength")),
            "wip_count": self._safe_float(metrics.get("wipCount")),
        }

    @staticmethod
    def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []

        deduped: list[dict[str, Any]] = []
        seen_keys: set[tuple[str, str]] = set()
        for row in sorted(
            rows,
            key=lambda item: (
                float(item.get("risk_score") or 0.0),
                float(item.get("avg_delay_time") or 0.0),
                int(item.get("affected_vehicle_count") or 0),
                int(item.get("manufacturing_event_id") or 0),
                int(item.get("car_master_id") or 0),
                str(item.get("process_code") or "").strip().upper(),
                str(item.get("equipment_code") or "").strip().upper(),
                int(item.get("rank_no") or 0),
                int(item.get("id") or 0),
            ),
            reverse=True,
        ):
            key = (
                str(row.get("process_code") or "").strip().upper(),
                str(row.get("equipment_code") or "").strip().upper(),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(row)

        deduped.sort(
            key=lambda item: (
                int(item.get("rank_no") or 0),
                int(item.get("id") or 0),
            ),
        )
        return deduped

    @staticmethod
    def _event_json_dict(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            return json.loads(value)
        return {}

    @staticmethod
    def _safe_float(value: Any) -> float:
        if value is None:
            return 0.0
        return float(value)

    @classmethod
    def _process_timestamp_delay(
        cls,
        process_data: Any,
        process_code: str,
    ) -> float:
        if not isinstance(process_data, dict):
            return 0.0

        candidates = [
            process_code,
            process_code.lower(),
            process_code.upper(),
        ]
        for key in candidates:
            value = process_data.get(key)
            if isinstance(value, dict):
                return cls._safe_float(value.get("timestampDelaySec"))
        return 0.0

    @classmethod
    def _equipment_stop_delay(
        cls,
        equipment_status: dict[str, Any],
        event_json: dict[str, Any],
        event_time_value: Any,
    ) -> float:
        status_changed_at = cls._parse_datetime(
            equipment_status.get("statusChangedTime"),
        )
        event_time = cls._parse_datetime(
            event_time_value,
        ) or cls._parse_datetime(
            cls._nested_value(event_json, "event", "eventTime"),
        )
        if status_changed_at is None:
            return 0.0

        last_normal_time = cls._parse_datetime(
            equipment_status.get("lastNormalTime"),
        )

        if event_time is not None and event_time > status_changed_at:
            return (event_time - status_changed_at).total_seconds()

        if last_normal_time is not None:
            return max((status_changed_at - last_normal_time).total_seconds(), 0.0)

        if event_time is None:
            event_time = datetime.now()
        else:
            return 30.0

        return max((event_time - status_changed_at).total_seconds(), 0.0)

    @staticmethod
    def _parse_datetime(value: Any) -> Any | None:
        if value is None:
            return None
        if hasattr(value, "isoformat") and hasattr(value, "tzinfo"):
            return value.replace(tzinfo=None)
        text = str(value).strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(
                text.replace("Z", "+00:00"),
            ).replace(tzinfo=None)
        except ValueError:
            return None

    @staticmethod
    def _nested_value(payload: dict[str, Any], *path: str) -> Any:
        current: Any = payload
        for key in path:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
        return current

    def _init_sqlalchemy(self) -> None:
        try:
            from sqlalchemy import BigInteger, Column, DateTime, Double, Enum
            from sqlalchemy import Index, Integer, MetaData, String, Table, func
            from sqlalchemy import create_engine
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "SQLAlchemy and PyMySQL are required for bottleneck analysis storage.",
            ) from exc

        self.metadata = MetaData()
        process_code_enum = Enum("PRESS", "BODY", "PAINT", "ASSEMBLY")
        self.table = Table(
            "bottleneck_analysis_result",
            self.metadata,
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("manufacturing_event_id", BigInteger),
            Column("car_master_id", BigInteger),
            Column("process_code", process_code_enum),
            Column("equipment_code", String(50)),
            Column("rank_no", Integer),
            Column("avg_delay_time", Double),
            Column("affected_vehicle_count", Integer),
            Column("risk_score", Double),
            Column("detected_at", DateTime),
            Column("created_at", DateTime, nullable=False, server_default=func.current_timestamp()),
            Column(
                "updated_at",
                DateTime,
                nullable=False,
                server_default=func.current_timestamp(),
                server_onupdate=func.current_timestamp(),
            ),
            Index("idx_bottleneck_analysis_result_rank", "rank_no"),
        )
        self.engine = create_engine(
            self.database_url,
            connect_args=mysql_connect_args_for_seoul(self.database_url),
            pool_pre_ping=True,
            future=True,
        )
        self.event_engine = create_engine(
            self.event_database_url,
            connect_args=mysql_connect_args_for_seoul(self.event_database_url),
            pool_pre_ping=True,
            future=True,
        )

    def _align_result_schema(self) -> None:
        if self.engine.dialect.name != "mysql":
            return

        from sqlalchemy import inspect, text

        table_name = "bottleneck_analysis_result"
        inspector = inspect(self.engine)
        if not inspector.has_table(table_name):
            return

        columns = {
            column["name"]
            for column in inspector.get_columns(table_name)
        }
        expected_columns = {
            "manufacturing_event_id": "BIGINT NULL",
            "car_master_id": "BIGINT NULL",
            "process_code": "ENUM('PRESS','BODY','PAINT','ASSEMBLY') NULL",
            "equipment_code": "VARCHAR(50) NULL",
            "rank_no": "INT NULL",
            "avg_delay_time": "DOUBLE NULL",
            "affected_vehicle_count": "INT NULL",
            "risk_score": "DOUBLE NULL",
            "detected_at": "DATETIME NULL",
            "created_at": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
            "updated_at": (
                "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP "
                "ON UPDATE CURRENT_TIMESTAMP"
            ),
        }
        removed_columns = ("product_process_history_id", "station_code")

        with self.engine.begin() as conn:
            for column_name in removed_columns:
                if column_name not in columns:
                    continue
                self._drop_column_constraints(conn, inspector, table_name, column_name)
                conn.execute(
                    text(
                        f"ALTER TABLE {table_name} "
                        f"DROP COLUMN {column_name}",
                    ),
                )
                columns.remove(column_name)

            for column_name, definition in expected_columns.items():
                if column_name not in columns:
                    conn.execute(
                        text(
                            f"ALTER TABLE {table_name} "
                            f"ADD COLUMN {column_name} {definition}",
                        ),
                    )
                    columns.add(column_name)

            conn.execute(
                text(
                    f"ALTER TABLE {table_name} "
                    "MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT",
                ),
            )
            if "detected_at" in columns:
                conn.execute(
                    text(
                        f"UPDATE {table_name} "
                        "SET detected_at = STR_TO_DATE("
                        "LEFT(SUBSTRING_INDEX(REPLACE(detected_at, 'T', ' '), '+', 1), 19), "
                        "'%Y-%m-%d %H:%i:%s'"
                        ") "
                        "WHERE detected_at IS NOT NULL "
                        "AND CAST(detected_at AS CHAR) LIKE '%T%'",
                    ),
                )
            conn.execute(
                text(
                    f"UPDATE {table_name} "
                    "SET created_at = COALESCE(created_at, detected_at, CURRENT_TIMESTAMP), "
                    "updated_at = COALESCE(updated_at, detected_at, CURRENT_TIMESTAMP)",
                ),
            )

            for column_name, definition in expected_columns.items():
                conn.execute(
                    text(
                        f"ALTER TABLE {table_name} "
                        f"MODIFY COLUMN {column_name} {definition}",
                    ),
                )

    @staticmethod
    def _drop_column_constraints(
        conn: Any,
        inspector: Any,
        table_name: str,
        column_name: str,
    ) -> None:
        from sqlalchemy import text

        for foreign_key in inspector.get_foreign_keys(table_name):
            if column_name in foreign_key.get("constrained_columns", []):
                conn.execute(
                    text(
                        f"ALTER TABLE {table_name} "
                        f"DROP FOREIGN KEY {foreign_key['name']}",
                    ),
                )

        index_names = {
            index["name"]
            for index in inspector.get_indexes(table_name)
            if column_name in index.get("column_names", [])
        }
        for index_name in index_names:
            conn.execute(text(f"DROP INDEX {index_name} ON {table_name}"))
