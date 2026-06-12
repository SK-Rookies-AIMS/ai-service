from collections.abc import Iterable
from typing import Any

from app.utils.database_utils import mysql_connect_args_for_seoul


class BottleneckAnalysisRepository:
    """병목 분석 결과를 DB에 저장하고 조회하는 저장소 계층"""

    def __init__(self, database_url: str) -> None:
        """MySQL 연결을 초기화하고 테이블 스키마를 보장한다."""
        self.database_url = database_url
        self.engine: Any | None = None
        self.table: Any | None = None
        self.product_process_history_table: Any | None = None
        self.metadata: Any | None = None

        self._init_sqlalchemy()
        self.ensure_schema()

    def ensure_schema(self) -> None:
        """병목 분석 결과 테이블과 조회용 인덱스를 생성"""
        self.metadata.create_all(self.engine)

    def replace_results(
        self,
        rows: Iterable[dict[str, Any]],
        detected_at: str,
        *,
        start_rank: int,
        end_rank: int,
    ) -> None:
        """요청 순위 구간 교체"""
        payload = [{**row, "detected_at": detected_at} for row in rows]
        self._validate_product_process_history_ids(payload)

        with self.engine.begin() as conn:
            # 요청한 rank 범위만 갱신
            conn.execute(
                self.table.delete().where(
                    self.table.c.rank_no.between(start_rank, end_rank),
                ),
            )
            if payload:
                conn.execute(self.table.insert(), payload)

    def list_results(self, *, cursor: int, size: int) -> list[dict[str, Any]]:
        """요청 순위 페이지 조회"""
        from sqlalchemy import select

        start_rank = cursor * size + 1
        end_rank = start_rank + size - 1

        query = select(self.table)
        query = (
            # 같은 순위가 있어도 조회 순서가 흔들리지 않도록 보조 정렬
            query.where(self.table.c.rank_no.between(start_rank, end_rank))
            .order_by(self.table.c.rank_no.asc(), self.table.c.id.asc())
            .limit(size)
        )

        with self.engine.connect() as conn:
            return [dict(row) for row in conn.execute(query).mappings()]

    def count_results(self) -> int:
        """저장된 병목 분석 결과 수를 반환"""
        from sqlalchemy import func, select

        query = select(func.count()).select_from(self.table)
        with self.engine.connect() as conn:
            return int(conn.execute(query).scalar_one())

    def list_product_process_histories(self) -> list[dict[str, Any]]:
        """분석에 사용할 product_process_history 전체를 조회"""
        from sqlalchemy import select

        query = (
            select(
                self.product_process_history_table.c.id,
                self.product_process_history_table.c.manufacturing_event_id,
                self.product_process_history_table.c.car_master_id,
                self.product_process_history_table.c.process_code,
                self.product_process_history_table.c.equipment_code,
                self.product_process_history_table.c.station_code,
                self.product_process_history_table.c.process_time,
                self.product_process_history_table.c.waiting_time,
            )
            .order_by(self.product_process_history_table.c.id.asc())
        )
        with self.engine.connect() as conn:
            return [dict(row) for row in conn.execute(query).mappings()]

    def _validate_product_process_history_ids(self, rows: list[dict[str, Any]]) -> None:
        """병목 결과의 product_process_history_id가 실제 공정 이력 PK인지 검증"""
        history_ids = {
            int(row["product_process_history_id"])
            for row in rows
            if row.get("product_process_history_id") is not None
        }
        if not history_ids:
            return

        from sqlalchemy import select

        query = select(self.product_process_history_table.c.id).where(
            self.product_process_history_table.c.id.in_(history_ids),
        )
        with self.engine.connect() as conn:
            existing_ids = {int(row["id"]) for row in conn.execute(query).mappings()}

        missing_ids = sorted(history_ids - existing_ids)
        if missing_ids:
            raise ValueError(
                f"Invalid product_process_history_id values: {missing_ids}",
            )

    def _init_sqlalchemy(self) -> None:
        """MySQL 연결을 위한 SQLAlchemy 엔진과 테이블 메타데이터를 구성"""
        try:
            from sqlalchemy import BigInteger, Column, DateTime, Double, Float, ForeignKey
            from sqlalchemy import Index, Integer, MetaData, String, Table
            from sqlalchemy import create_engine
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "외부 DB를 사용하려면 requirements.txt의 sqlalchemy, pymysql 패키지가 필요합니다.",
            ) from exc

        self.metadata = MetaData()
        self.product_process_history_table = Table(
            "product_process_history",
            self.metadata,
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            # sampledb manufacturing_event.id에 대한 논리 참조이며 DB 간 FK는 두지 않음
            Column("manufacturing_event_id", BigInteger),
            # sampledb car_master.id에 대한 논리 참조이며 DB 간 FK는 두지 않음
            Column("car_master_id", BigInteger),
            Column("process_code", String(20), nullable=False),
            Column("equipment_code", String(50), nullable=False),
            Column("station_code", String(50), nullable=False),
            Column("lot_code", String(50)),
            Column("started_at", DateTime, nullable=False),
            Column("ended_at", DateTime, nullable=False),
            Column("process_time", Double),
            Column("waiting_time", Double),
            Column("result_status", String(20), nullable=False),
            Column("sequence_no", Integer, nullable=False),
            Column("previous_process_code", String(20)),
            Column("created_at", DateTime, nullable=False),
            Index("idx_product_process_history_event_id", "manufacturing_event_id"),
            Index("idx_product_process_history_car_master_id", "car_master_id"),
            Index("idx_product_process_history_process_code", "process_code"),
        )
        # SQLAlchemy Core 테이블 정의를 사용해 MySQL DDL/CRUD를 DB 방언에 맞게 생성
        self.table = Table(
            "bottleneck_analysis_result",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("product_process_history_id", BigInteger, ForeignKey("product_process_history.id")),
            Column("manufacturing_event_id", Integer),
            Column("car_master_id", Integer),
            Column("process_code", String(64), nullable=False),
            Column("equipment_code", String(64), nullable=False),
            Column("station_code", String(64), nullable=False),
            Column("rank_no", Integer, nullable=False),
            Column("avg_delay_time", Float, nullable=False),
            Column("affected_vehicle_count", Integer, nullable=False),
            Column("risk_score", Float, nullable=False),
            Column("detected_at", String(64), nullable=False),
            Index("idx_bottleneck_analysis_result_rank", "rank_no"),
            Index("idx_bottleneck_analysis_result_history_id", "product_process_history_id"),
        )
        self.engine = create_engine(
            self.database_url,
            # MySQL 세션 함수(now 등)를 서비스 기준 타임존과 맞춤
            connect_args=mysql_connect_args_for_seoul(self.database_url),
            pool_pre_ping=True,
            future=True,
        )
