from __future__ import annotations

from sqlalchemy import create_engine

from app.repository.car_master_repository import CarMasterRepository
from app.repository.equipment_repository import EquipmentRepository
from app.repository.manufacturing_event_repository import (
    ManufacturingEventRepository,
)
from app.repository.manufacturing_event_template_repository import (
    ManufacturingEventTemplateRepository,
)
from app.repository.manufacturing_generation_job_repository import (
    ManufacturingGenerationJobRepository,
)
from app.repository.sampledb_schema_manager import SampleDbSchemaManager
from app.utils.database_utils import mysql_connect_args_for_seoul


class SampleDbRepository:
    """sampledb 전용 repository들을 같은 DB 연결로 묶는 facade다.

    테이블별 SQL과 상태 관리 책임은 각 repository에 있고, 이 클래스는 서비스가
    한 객체를 주입받아 사용할 수 있도록 구성 요소만 제공한다.
    """

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.engine = create_engine(
            database_url,
            connect_args=mysql_connect_args_for_seoul(database_url),
            pool_pre_ping=True,
            future=True,
        )
        self.schema = SampleDbSchemaManager(self.engine)
        self.cars = CarMasterRepository(self.engine)
        self.equipment = EquipmentRepository(self.engine)
        self.events = ManufacturingEventRepository(self.engine)
        self.templates = ManufacturingEventTemplateRepository(self.engine)
        self.jobs = ManufacturingGenerationJobRepository(self.engine)

    def initialize(self) -> None:
        self.schema.ensure_schema()
        self.equipment.seed_defaults()


def initialize_sampledb(database_url: str) -> None:
    """sampledb 스키마와 기본 설비 데이터를 초기화한다."""
    SampleDbRepository(database_url).initialize()
