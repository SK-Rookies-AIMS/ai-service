from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from app.repository.sampledb_schema import car_master, equipment, manufacturing_event_json
from app.utils.database_utils import mysql_connect_args_for_seoul
from app.utils.process_label_utils import NEXT_PROCESS, equipment_code_for_car_process


class DefectTransferPredictionRepository:
    """불량 전이 예측 결과를 main_db에 저장하고 sampledb 원천 이벤트를 읽는다."""

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
        """불량 전이 결과 테이블의 스키마를 DB에 맞게 생성하거나 보정한다."""
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
        """하나의 제조 이벤트에 대한 예측 결과를 최신 값으로 다시 저장한다."""
        """하나의 제조 이벤트에 대한 예측 결과를 최신 값으로 다시 저장한다."""
        # 하나의 제조 이벤트에 대해 예측 결과 1건만 유지한다.
        manufacturing_event_id = self._manufacturing_event_id(event_id)
        main_causes = self._normalize_main_causes(causes)
        cause_rows = causes or [
            {
                "message": "no main cause available",
                "impact": 0.0,
            },
        ]
        top_cause = cause_rows[0] if cause_rows else {}
        row = {
            "manufacturing_event_id": manufacturing_event_id,
            "car_master_id": car_master_id,
            "source_process_code": source_process_code,
            "target_process_code": target_process_code,
            "current_defect_probability": current_defect_probability,
            "target_defect_probability": target_defect_probability,
            "predicted_defect_process": predicted_defect_process,
            "expected_occurrence_step": expected_occurrence_step,
            "risk_grade": risk_grade,
            "main_causes": main_causes,
            "influence_score": float(top_cause.get("impact") or 0.0),
            "predicted_at": predicted_at,
        }

        with self.engine.begin() as conn:
            conn.execute(
                self.table.delete().where(
                    self.table.c.manufacturing_event_id == manufacturing_event_id,
                ),
            )
            if self.engine.dialect.name != "mysql":
                from sqlalchemy import func, select

                next_id = int(conn.execute(select(func.max(self.table.c.id))).scalar() or 0)
                row = {**row, "id": next_id + 1}
            conn.execute(self.table.insert(), [row])
        return 1

    def has_prediction_for_event(self, event_id: str) -> bool:
        """이미 해당 이벤트의 예측 결과가 저장되어 있는지 확인한다."""
        manufacturing_event_id = self._manufacturing_event_id(event_id)
        if manufacturing_event_id is None:
            return False

        from sqlalchemy import func, select

        query = (
            select(func.count())
            .select_from(self.table)
            .where(self.table.c.manufacturing_event_id == manufacturing_event_id)
        )
        with self.engine.connect() as conn:
            return int(conn.execute(query).scalar_one()) > 0

    def list_prediction_page(
        self,
        *,
        cursor: int,
        size: int,
        analysis_date: date | None = None,
    ) -> tuple[list[dict[str, Any]], bool]:
        """차량별 최신 예측 결과를 모아 목록 페이지를 만든다."""
        # 차량별 최신 예측만 추려서 목록 페이지를 만든다.
        rows = self._all_prediction_rows(analysis_date=analysis_date)
        latest_by_car: dict[int, dict[str, Any]] = {}
        for row in rows:
            car_id = int(row["car_master_id"])
            if car_id not in latest_by_car:
                latest_by_car[car_id] = row

        items = [
            row
            for row in latest_by_car.values()
            if self._result_probability(row) > 0
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

    def list_prediction_rows(
        self,
        *,
        analysis_date: date | None = None,
        car_master_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """조건에 맞는 불량 전이 결과 원본 row를 모두 반환한다."""
        return self._all_prediction_rows(
            car_master_id=car_master_id,
            analysis_date=analysis_date,
        )

    def list_cause_page(
        self,
        *,
        vehicle_id: str | None,
        cursor: int,
        size: int,
        analysis_date: date | None = None,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]], bool]:
        """대표 원인 1개와 상세 원인 리스트를 함께 반환한다."""
        # 대표 원인 1개와 상세 원인 목록을 같은 이벤트 묶음으로 반환한다.
        car_master_id = self._car_master_id(vehicle_id) if vehicle_id else None
        rows = self._all_prediction_rows(
            car_master_id=car_master_id,
            analysis_date=analysis_date,
        )
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

    def list_date_options(self, *, vehicle_id: str | None = None) -> list[dict[str, Any]]:
        """predicted_at 기준으로 날짜 선택 옵션을 생성한다."""
        # 날짜 선택용 옵션은 predicted_at 기준으로 만든다.
        from sqlalchemy import func, select

        car_master_id = self._car_master_id(vehicle_id) if vehicle_id else None

        query = (
            select(
                func.date(self.table.c.predicted_at).label("date"),
                func.min(self.table.c.manufacturing_event_id).label(
                    "sample_manufacturing_event_id",
                ),
            )
            .where(self.table.c.predicted_at.is_not(None))
            .group_by(func.date(self.table.c.predicted_at))
            .order_by(func.date(self.table.c.predicted_at).desc())
        )
        if car_master_id is not None:
            query = query.where(self.table.c.car_master_id == car_master_id)
        with self.engine.connect() as conn:
            date_rows = [dict(row) for row in conn.execute(query).mappings()]

        sample_event_ids = sorted(
            {
                int(row["sample_manufacturing_event_id"])
                for row in date_rows
                if row.get("sample_manufacturing_event_id") is not None
            },
        )
        event_id_by_id: dict[int, str] = {}
        if sample_event_ids:
            event_query = select(
                manufacturing_event_json.c.id,
                manufacturing_event_json.c.event_id,
            ).where(manufacturing_event_json.c.id.in_(sample_event_ids))
            with self.event_engine.connect() as conn:
                for event_row in conn.execute(event_query).mappings():
                    event_id_by_id[int(event_row["id"])] = str(event_row["event_id"])

        return [
            {
                "date": row["date"],
                "sample_event_id": (
                    event_id_by_id.get(int(row["sample_manufacturing_event_id"]))
                    if row.get("sample_manufacturing_event_id") is not None
                    else None
                ),
            }
            for row in date_rows
        ]

    def diagnostics(self) -> dict[str, Any]:
        """원천 이벤트와 예측 결과의 현재 적재 상태를 점검한다."""
        # 원천 이벤트, 예측 결과, 최신 상태를 한 번에 점검한다.
        from sqlalchemy import distinct, func, select

        with self.event_engine.connect() as conn:
            source_event_count = int(
                conn.execute(select(func.count()).select_from(manufacturing_event_json)).scalar()
                or 0,
            )
            sent_event_count = int(
                conn.execute(
                    select(func.count())
                    .select_from(manufacturing_event_json)
                    .where(manufacturing_event_json.c.dispatch_status == "SENT")
                    .where(manufacturing_event_json.c.is_sent.is_(True)),
                ).scalar()
                or 0,
            )
            latest_source_event = conn.execute(
                select(
                    manufacturing_event_json.c.id,
                    manufacturing_event_json.c.event_id,
                    manufacturing_event_json.c.car_master_id,
                    manufacturing_event_json.c.process_code,
                    manufacturing_event_json.c.is_sent,
                    manufacturing_event_json.c.created_at,
                ).order_by(manufacturing_event_json.c.id.desc()),
            ).mappings().first()

        all_rows = self._all_prediction_rows()
        latest_by_car: dict[int, dict[str, Any]] = {}
        for row in all_rows:
            car_id = int(row["car_master_id"])
            if car_id not in latest_by_car:
                latest_by_car[car_id] = row

        visible_latest_rows = [
            row
            for row in latest_by_car.values()
            if self._result_probability(row) > 0
        ]

        with self.engine.connect() as conn:
            result_count = int(
                conn.execute(select(func.count()).select_from(self.table)).scalar() or 0,
            )
            result_car_count = int(
                conn.execute(
                    select(func.count(distinct(self.table.c.car_master_id))),
                ).scalar()
                or 0,
            )
            null_event_link_count = int(
                conn.execute(
                    select(func.count())
                    .select_from(self.table)
                    .where(self.table.c.manufacturing_event_id.is_(None)),
                ).scalar()
                or 0,
            )
            latest_prediction = conn.execute(
                select(
                    self.table.c.id,
                    self.table.c.manufacturing_event_id,
                    self.table.c.car_master_id,
                    self.table.c.source_process_code,
                    self.table.c.target_process_code,
                    self.table.c.current_defect_probability,
                    self.table.c.target_defect_probability,
                    self.table.c.risk_grade,
                    self.table.c.predicted_at,
                ).order_by(self.table.c.predicted_at.desc(), self.table.c.id.desc()),
            ).mappings().first()

        return {
            "sourceEventCount": source_event_count,
            "sentSourceEventCount": sent_event_count,
            "predictionResultRowCount": result_count,
            "predictionResultCarCount": result_car_count,
            "visiblePredictionCarCount": len(visible_latest_rows),
            "nullManufacturingEventLinkCount": null_event_link_count,
            "latestSourceEvent": self._json_ready_row(latest_source_event),
            "latestPredictionResult": self._json_ready_row(latest_prediction),
        }

    def delete_predictions_by_date(self, analysis_date: date) -> int:
        from sqlalchemy import func

        with self.engine.begin() as conn:
            result = conn.execute(
                self.table.delete().where(func.date(self.table.c.predicted_at) == analysis_date),
            )
        return int(result.rowcount or 0)

    def _all_prediction_rows(
        self,
        *,
        car_master_id: int | None = None,
        analysis_date: date | None = None,
    ) -> list[dict[str, Any]]:
        """필요한 조건에 맞는 예측 결과 원본 row를 조회한다."""
        from sqlalchemy import func, select

        query = select(self.table)
        if car_master_id is not None:
            query = query.where(self.table.c.car_master_id == car_master_id)
        if analysis_date is not None:
            query = query.where(func.date(self.table.c.predicted_at) == analysis_date)
        query = query.order_by(
            self.table.c.predicted_at.desc(),
            self.table.c.id.desc(),
        )
        with self.engine.connect() as conn:
            return [dict(row) for row in conn.execute(query).mappings()]

    def list_prediction_source_events(
        self,
        *,
        analysis_date: date | None = None,
        car_master_id: int | None = None,
    ) -> list[dict[str, Any]]:
        from sqlalchemy import func, select

        query = (
            select(
                manufacturing_event_json.c.id,
                manufacturing_event_json.c.event_id,
                manufacturing_event_json.c.event_time,
                manufacturing_event_json.c.car_master_id,
                manufacturing_event_json.c.process_code,
                manufacturing_event_json.c.event_json,
            )
            .where(manufacturing_event_json.c.dispatch_status == "SENT")
            .where(manufacturing_event_json.c.is_sent.is_(True))
        )
        if analysis_date is None:
            query = query.where(func.date(manufacturing_event_json.c.event_time) == func.current_date())
        else:
            query = query.where(func.date(manufacturing_event_json.c.event_time) == analysis_date)
        if car_master_id is not None:
            query = query.where(manufacturing_event_json.c.car_master_id == car_master_id)
        query = query.order_by(manufacturing_event_json.c.id.asc())
        with self.event_engine.connect() as conn:
            return [dict(row) for row in conn.execute(query).mappings()]

    def delete_source_analysis_done_flags(
        self,
        *,
        event_ids: list[int],
        column_name: str,
    ) -> int:
        if not event_ids:
            return 0

        from sqlalchemy import func

        if column_name not in {"bottleneck_analysis_done", "defect_transfer_analysis_done"}:
            raise ValueError(f"Unsupported analysis flag column: {column_name}")

        with self.event_engine.begin() as conn:
            result = conn.execute(
                manufacturing_event_json.update()
                .where(manufacturing_event_json.c.id.in_(event_ids))
                .values(**{column_name: False, "updated_at": func.current_timestamp()}),
            )
        return int(result.rowcount or 0)

    @staticmethod
    def _json_ready_row(row: Any | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            key: value.isoformat() if isinstance(value, datetime) else value
            for key, value in dict(row).items()
        }

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

    def _prediction_event_ids(
        self,
        *,
        car_master_id: int | None = None,
        analysis_date: date | None = None,
    ) -> list[int]:
        """분석 대상 manufacturing_event_json id를 필터링해서 반환한다."""
        from sqlalchemy import func, select

        query = (
            select(manufacturing_event_json.c.id)
            .where(manufacturing_event_json.c.dispatch_status == "SENT")
            .where(manufacturing_event_json.c.is_sent.is_(True))
        )
        if analysis_date is None:
            query = query.where(func.date(manufacturing_event_json.c.event_time) == func.current_date())
        else:
            query = query.where(func.date(manufacturing_event_json.c.event_time) == analysis_date)
        if car_master_id is not None:
            query = query.where(manufacturing_event_json.c.car_master_id == car_master_id)
        with self.event_engine.connect() as conn:
            return [int(row[0]) for row in conn.execute(query).all()]

    @staticmethod
    def _normalize_probability(value: Any) -> float:
        """확률값이 0~1 또는 0~100 형태여도 화면용 소수로 맞춘다."""
        if value is None:
            return 0.0
        normalized = float(value)
        if abs(normalized) > 1.0:
            normalized /= 100.0
        return round(normalized, 4)

    @classmethod
    def _result_probability(cls, row: dict[str, Any]) -> float:
        """현재 화면에서 사용할 대표 확률값을 선택한다."""
        value = row.get("current_defect_probability")
        if value is None:
            value = row.get("target_defect_probability")
        if value is None:
            value = row.get("defect_probability")
        if value is None:
            return 0.0
        return cls._normalize_probability(value)

    def _attach_process_equipment_codes(self, rows: list[dict[str, Any]]) -> None:
        """화면 표시에 필요한 공정명과 설비 코드를 보강한다."""
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
            source_code = str(row.get("source_process_code") or "").strip().upper()
            if not row["source_equipment_code"] and source_code:
                row["source_equipment_code"] = equipment_code_for_car_process(
                    car_master_id=car_id,
                    process_code=source_code,
                )

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
        """manufacturing_event_id를 기반으로 vehicle_id를 붙인다."""
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
        """예측 결과 저장소의 SQLAlchemy 테이블과 엔진을 준비한다."""
        try:
            from sqlalchemy import BigInteger, Column, DateTime, Double, Enum, JSON
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
            Column("main_causes", JSON),
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
        """기존 결과 테이블과 현재 코드의 컬럼 구조를 맞춘다."""
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
            "main_causes": "JSON NULL",
            "influence_score": "DOUBLE NULL",
            "predicted_at": "DATETIME NOT NULL",
            "created_at": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
            "updated_at": (
                "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP "
                "ON UPDATE CURRENT_TIMESTAMP"
            ),
        }
        removed_columns = ("main_cause",)
        with self.engine.begin() as conn:
            for column_name in removed_columns:
                if column_name not in columns:
                    continue
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
            for column_name, definition in expected_columns.items():
                conn.execute(
                    text(
                        f"ALTER TABLE {table_name} "
                        f"MODIFY COLUMN {column_name} {definition}",
                    ),
                )

    @staticmethod
    def _normalize_main_causes(causes: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """상위 원인 목록을 저장용 간단한 구조로 정리한다."""
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
