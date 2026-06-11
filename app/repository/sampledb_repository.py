from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert

from app.repository.sampledb_schema import equipment, metadata
from app.utils.database_utils import mysql_connect_args_for_seoul


DEFAULT_EQUIPMENT_ROWS: list[dict[str, Any]] = [
    {
        "process_code": process_code,
        "equipment_code": f"EQ_{process_code}_{index:03d}",
        "equipment_name": f"{process_code.title()} equipment {index}",
        "equipment_type": equipment_type,
        "status": "NORMAL",
    }
    for process_code, equipment_type in (
        ("PRESS", "HYDRAULIC_PRESS"),
        ("BODY", "ROBOT_ARM"),
        ("PAINT", "CAMERA"),
        ("ASSEMBLY", "CONVEYOR"),
    )
    for index in range(1, 6)
]


class SampleDbRepository:
    """제조 샘플 PRD에 정의된 sampledb 테이블을 생성하고 기본 데이터를 입력"""

    def __init__(self, database_url: str) -> None:
        """sampledb 연결에 사용할 SQLAlchemy 엔진을 생성."""
        self.engine = create_engine(
            database_url,
            connect_args=mysql_connect_args_for_seoul(database_url),
            pool_pre_ping=True,
            future=True,
        )

    def ensure_schema(self) -> None:
        """sampledb 엔티티가 없으면 생성"""
        metadata.create_all(self.engine)

    def seed_equipment(self) -> None:
        """기본 설비 데이터를 입력하고 기존 데이터는 유지"""
        statement = insert(equipment).values(DEFAULT_EQUIPMENT_ROWS)
        update_columns = {
            "equipment_name": statement.inserted.equipment_name,
            "process_code": statement.inserted.process_code,
            "equipment_type": statement.inserted.equipment_type,
            "status": statement.inserted.status,
        }
        statement = statement.on_duplicate_key_update(**update_columns)

        with self.engine.begin() as conn:
            conn.execute(statement)

    def initialize(self) -> None:
        """스키마를 생성하고 PRD에 필요한 참조 데이터를 입력"""
        self.ensure_schema()
        self.seed_equipment()


def initialize_sampledb(database_url: str) -> None:
    """sampledb 테이블과 참조 설비 데이터를 초기화"""
    SampleDbRepository(database_url).initialize()
