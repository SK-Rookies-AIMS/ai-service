from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from app.repository.sampledb_schema import car_master, equipment, manufacturing_event_json
from app.utils.database_utils import mysql_connect_args_for_seoul
from app.utils.process_label_utils import NEXT_PROCESS, equipment_code_for_car_process


class DefectTransferPredictionRepository:
    """Store and read defect-transfer prediction results in main_db."""

    def __init__(
        self,
        database_url: str,
        *,
        event_database_url: str,
    ) -> None:
        self.database_url = database_url
        self.event_database_url = event_database_url
        self.engine: Any | None = None
        self.event_engine: Any | None = None
        self.table: Any | None = None
        self.metadata: Any | None = None

        self._init_sqlalchemy()
        self.ensure_schema()

    def ensure_schema(self) -> None:
        self.metadata.create_all(self.engine)
        self._align_result_schema()

    def replace_prediction_result(
        self,
        *,
        event_id: str,
        car_master_id: int,
        source_process_code: str,
        target_process_code: str | None,
        current_defect_probability: float,
        target_defect_probability: float | None,
        predicted_defect_process: str | None,
        expected_occurrence_step: int | None,
        risk_grade: str,
        causes: list[dict[str, Any]],
        predicted_at: datetime,
    ) -> int:
        manufacturing_event_id = self._manufacturing_event_id(event_id)
        rows = []
        cause_rows = causes or [
            {
                "message": "모델 기반 주요 원인이 산출되지 않았습니다.",
                "impact": 0.0,
            },
        ]
        for cause in cause_rows[:5]:
            rows.append(
                {
                    "manufacturing_event_id": manufacturing_event_id,
                    "car_master_id": car_master_id,
                    "source_process_code": source_process_code,
                    "target_process_code": target_process_code,
                    "current_defect_probability": current_defect_probability,
                    "target_defect_probability": target_defect_probability,
                    "predicted_defect_process": predicted_defect_process,
                    "expected_occurrence_step": expected_occurrence_step,
                    "risk_grade": risk_grade,
                    "main_cause": str(cause.get("message") or cause.get("label") or "")[:200],
                    "influence_score": float(cause.get("impact") or 0.0),
                    "predicted_at": predicted_at,
                },
            )

        with self.engine.begin() as conn:
            if manufacturing_event_id is not None:
                conn.execute(
                    self.table.delete().where(
                        self.table.c.manufacturing_event_id == manufacturing_event_id,
                    ),
                )
            if self.engine.dialect.name != "mysql":
                from sqlalchemy import func, select

                next_id = int(
                    conn.execute(select(func.max(self.table.c.id))).scalar() or 0,
                )
                rows = [
                    {**row, "id": next_id + index}
                    for index, row in enumerate(rows, 1)
                ]
            conn.execute(self.table.insert(), rows)
        return len(rows)

    def list_prediction_page(self, *, cursor: int, size: int) -> tuple[list[dict[str, Any]], bool]:
        from sqlalchemy import select

        rows = self._all_prediction_rows()
        latest_by_car: dict[int, dict[str, Any]] = {}
        for row in rows:
            car_id = int(row["car_master_id"])
            if car_id not in latest_by_car:
                latest_by_car[car_id] = row

        items = [
            row
            for row in latest_by_car.values()
            if self._result_probability_percent(row) > 0
        ]
        items.sort(
            key=lambda row: (
                float(row.get("target_defect_probability") or 0.0),
                float(row.get("current_defect_probability") or 0.0),
                row.get("predicted_at") or datetime.min,
                int(row.get("id") or 0),
            ),
            reverse=True,
        )
        self._attach_vehicle_ids(items)
        self._attach_process_equipment_codes(items)

        offset = cursor * size
        page = items[offset : offset + size]
        return page, len(items) > offset + size

    def list_cause_page(
        self,
        *,
        vehicle_id: str | None,
        cursor: int,
        size: int,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]], bool]:
        car_master_id = self._car_master_id(vehicle_id) if vehicle_id else None
        rows = self._all_prediction_rows(car_master_id=car_master_id)
        if not rows:
            return None, [], False

        latest_event_id = rows[0].get("manufacturing_event_id")
        latest_rows = [
            row
            for row in rows
            if row.get("manufacturing_event_id") == latest_event_id
        ]
        latest_rows.sort(
            key=lambda row: (
                float(row.get("influence_score") or 0.0),
                int(row.get("id") or 0),
            ),
            reverse=True,
        )
        self._attach_vehicle_ids(latest_rows)
        self._attach_process_equipment_codes(latest_rows)

        offset = cursor * size
        page = latest_rows[offset : offset + size]
        return latest_rows[0], page, len(latest_rows) > offset + size

    def _all_prediction_rows(
        self,
        *,
        car_master_id: int | None = None,
    ) -> list[dict[str, Any]]:
        from sqlalchemy import select

        query = select(self.table)
        if car_master_id is not None:
            query = query.where(self.table.c.car_master_id == car_master_id)
        query = query.order_by(
            self.table.c.predicted_at.desc(),
            self.table.c.id.desc(),
        )
        with self.engine.connect() as conn:
            return [dict(row) for row in conn.execute(query).mappings()]

    def _manufacturing_event_id(self, event_id: str) -> int | None:
        from sqlalchemy import select

        query = select(manufacturing_event_json.c.id).where(
            manufacturing_event_json.c.event_id == event_id,
        )
        with self.event_engine.connect() as conn:
            value = conn.execute(query).scalar()
        return int(value) if value is not None else None

    def _car_master_id(self, vehicle_id: str | None) -> int | None:
        if not vehicle_id:
            return None
        from sqlalchemy import select

        query = select(car_master.c.id).where(car_master.c.vehicle_id == vehicle_id)
        with self.event_engine.connect() as conn:
            value = conn.execute(query).scalar()
        return int(value) if value is not None else None

    @staticmethod
    def _result_probability_percent(row: dict[str, Any]) -> int:
        value = row.get("target_defect_probability")
        if value is None:
            value = row.get("current_defect_probability")
        if value is None:
            return 0
        return round(float(value) * 100)

    def _attach_process_equipment_codes(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        from sqlalchemy import select

        event_ids = {
            int(row["manufacturing_event_id"])
            for row in rows
            if row.get("manufacturing_event_id") is not None
        }
        source_equipment_by_event: dict[int, str] = {}
        if event_ids:
            query = (
                select(
                    manufacturing_event_json.c.id,
                    equipment.c.equipment_code,
                )
                .select_from(
                    manufacturing_event_json.join(
                        equipment,
                        equipment.c.id == manufacturing_event_json.c.equipment_id,
                    ),
                )
                .where(manufacturing_event_json.c.id.in_(event_ids))
            )
            with self.event_engine.connect() as conn:
                for event_row in conn.execute(query).mappings():
                    source_equipment_by_event[int(event_row["id"])] = str(
                        event_row["equipment_code"],
                    )

        target_keys: set[tuple[int, str]] = set()
        for row in rows:
            target_code = self._target_process_code(row)
            if target_code:
                target_keys.add((int(row["car_master_id"]), target_code))

        target_equipment_by_key: dict[tuple[int, str], str] = {}
        if target_keys:
            car_ids = sorted({car_id for car_id, _ in target_keys})
            query = (
                select(
                    manufacturing_event_json.c.car_master_id,
                    manufacturing_event_json.c.process_code,
                    equipment.c.equipment_code,
                    manufacturing_event_json.c.id,
                )
                .select_from(
                    manufacturing_event_json.join(
                        equipment,
                        equipment.c.id == manufacturing_event_json.c.equipment_id,
                    ),
                )
                .where(manufacturing_event_json.c.car_master_id.in_(car_ids))
                .order_by(
                    manufacturing_event_json.c.car_master_id,
                    manufacturing_event_json.c.process_code,
                    manufacturing_event_json.c.id.desc(),
                )
            )
            with self.event_engine.connect() as conn:
                for event_row in conn.execute(query).mappings():
                    key = (
                        int(event_row["car_master_id"]),
                        str(event_row["process_code"]).strip().upper(),
                    )
                    if key in target_keys and key not in target_equipment_by_key:
                        target_equipment_by_key[key] = str(event_row["equipment_code"])

        for row in rows:
            event_id = row.get("manufacturing_event_id")
            row["source_equipment_code"] = (
                source_equipment_by_event.get(int(event_id))
                if event_id is not None
                else None
            )

            car_id = int(row["car_master_id"])
            target_code = self._target_process_code(row)
            if target_code:
                row["target_equipment_code"] = target_equipment_by_key.get(
                    (car_id, target_code),
                    equipment_code_for_car_process(
                        car_master_id=car_id,
                        process_code=target_code,
                    ),
                )
            else:
                row["target_equipment_code"] = None

    @staticmethod
    def _target_process_code(row: dict[str, Any]) -> str | None:
        target_code = row.get("target_process_code")
        if target_code:
            return str(target_code).strip().upper()
        source = str(row.get("source_process_code") or "").strip().upper()
        return NEXT_PROCESS.get(source)

    def _attach_vehicle_ids(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        from sqlalchemy import select

        car_ids = sorted({int(row["car_master_id"]) for row in rows})
        query = select(car_master.c.id, car_master.c.vehicle_id).where(
            car_master.c.id.in_(car_ids),
        )
        with self.event_engine.connect() as conn:
            vehicle_by_id = {
                int(row["id"]): str(row["vehicle_id"])
                for row in conn.execute(query).mappings()
            }
        for row in rows:
            row["vehicle_id"] = vehicle_by_id.get(
                int(row["car_master_id"]),
                f"VIN-{int(row['car_master_id']):06d}",
            )

    def _init_sqlalchemy(self) -> None:
        try:
            from sqlalchemy import BigInteger, Column, DateTime, Double, Enum
            from sqlalchemy import Index, Integer, MetaData, String, Table, func
            from sqlalchemy import create_engine
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "SQLAlchemy and PyMySQL are required for defect transfer result storage.",
            ) from exc

        self.metadata = MetaData()
        process_code_enum = Enum("PRESS", "BODY", "PAINT", "ASSEMBLY")
        self.table = Table(
            "defect_transfer_prediction_result",
            self.metadata,
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("manufacturing_event_id", BigInteger),
            Column("car_master_id", BigInteger),
            Column("source_process_code", process_code_enum),
            Column("target_process_code", process_code_enum),
            Column("current_defect_probability", Double),
            Column("target_defect_probability", Double),
            Column("predicted_defect_process", String(50)),
            Column("expected_occurrence_step", Integer),
            Column("risk_grade", String(20)),
            Column("main_cause", String(200)),
            Column("influence_score", Double),
            Column("predicted_at", DateTime, nullable=False),
            Column("created_at", DateTime, nullable=False, server_default=func.current_timestamp()),
            Column(
                "updated_at",
                DateTime,
                nullable=False,
                server_default=func.current_timestamp(),
                server_onupdate=func.current_timestamp(),
            ),
            Index("idx_defect_transfer_car_predicted", "car_master_id", "predicted_at"),
            Index("idx_defect_transfer_event", "manufacturing_event_id"),
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

        table_name = "defect_transfer_prediction_result"
        inspector = inspect(self.engine)
        if not inspector.has_table(table_name):
            return

        columns = {column["name"] for column in inspector.get_columns(table_name)}
        expected_columns = {
            "manufacturing_event_id": "BIGINT NULL",
            "car_master_id": "BIGINT NULL",
            "source_process_code": "ENUM('PRESS','BODY','PAINT','ASSEMBLY') NULL",
            "target_process_code": "ENUM('PRESS','BODY','PAINT','ASSEMBLY') NULL",
            "current_defect_probability": "DOUBLE NULL",
            "target_defect_probability": "DOUBLE NULL",
            "predicted_defect_process": "VARCHAR(50) NULL",
            "expected_occurrence_step": "INT NULL",
            "risk_grade": "VARCHAR(20) NULL",
            "main_cause": "VARCHAR(200) NULL",
            "influence_score": "DOUBLE NULL",
            "predicted_at": "DATETIME NOT NULL",
            "created_at": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
            "updated_at": (
                "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP "
                "ON UPDATE CURRENT_TIMESTAMP"
            ),
        }
        with self.engine.begin() as conn:
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
            for column_name, definition in expected_columns.items():
                conn.execute(
                    text(
                        f"ALTER TABLE {table_name} "
                        f"MODIFY COLUMN {column_name} {definition}",
                    ),
                )
