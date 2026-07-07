from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    JSON,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
)

metadata = MetaData()

# 제조 이벤트 흐름에서 공통으로 사용하는 PRD 상태 코드.
process_code_enum = Enum("PRESS", "BODY", "PAINT", "ASSEMBLY")
equipment_type_enum = Enum("HYDRAULIC_PRESS", "ROBOT_ARM", "CAMERA", "CONVEYOR")
equipment_current_status_enum = Enum(
    "RUNNING",
    "IDLE",
    "STOPPED",
    "FAULT",
    "MAINTENANCE",
)
dispatch_status_enum = Enum(
    "PENDING",
    "READY",
    "SENT",
    "BLOCKED",
)
analysis_status_enum = Enum("NOT_ANALYZED", "NORMAL", "ABNORMAL")

car_master = Table(
    "car_master",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("vehicle_id", String(50), nullable=False),
    Column("car_type", String(20), nullable=False),
    Column("engine_type", String(30), nullable=False),
    Column("car_color", String(30), nullable=False),
    Column("fuel_efficiency", Integer, nullable=False),
    # 차량 생성 시각은 원천 시스템에서 제공되지 않을 수 있으므로 NULL을 허용한다.
    Column("created_at", DateTime, nullable=True),
    UniqueConstraint("vehicle_id", name="uq_car_master_vehicle_id"),
)

equipment = Table(
    "equipment",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("process_code", process_code_enum, nullable=False),
    Column("equipment_code", String(50), nullable=False, unique=True),
    Column("equipment_name", String(100), nullable=False),
    Column("equipment_type", equipment_type_enum, nullable=False),
    Column(
        "current_status",
        equipment_current_status_enum,
        nullable=False,
        server_default="RUNNING",
    ),
    Column("last_fault_time", DateTime),
    Column("last_recovered_time", DateTime),
    Column("reason", String(255)),
    Column("created_at", DateTime, nullable=False, server_default=func.current_timestamp()),
    Column("updated_at", DateTime, nullable=False, server_default=func.current_timestamp()),
    Index("idx_equipment_process_code", "process_code"),
)

manufacturing_event_json = Table(
    "manufacturing_event_json",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("event_id", String(100), nullable=False),
    Column("event_time", DateTime, nullable=True),
    Column("car_master_id", BigInteger, nullable=False),
    Column("process_code", process_code_enum, nullable=False),
    Column("equipment_id", BigInteger, nullable=False),
    Column("event_json", JSON, nullable=False),
    # Scheduler가 조회하는 발행 가능 상태. 최초 생성 시 PRESS만 READY가 된다.
    Column(
        "dispatch_status",
        dispatch_status_enum,
        nullable=False,
        server_default="PENDING",
    ),
    # Kafka Consumer의 공정 분석 결과가 기록되기 전에는 NOT_ANALYZED다.
    Column(
        "analysis_status",
        analysis_status_enum,
        nullable=False,
        server_default="NOT_ANALYZED",
    ),
    Column(
        "bottleneck_analysis_done",
        Boolean,
        nullable=False,
        server_default="0",
    ),
    Column(
        "defect_transfer_analysis_done",
        Boolean,
        nullable=False,
        server_default="0",
    ),
    Column("is_sent", Boolean, nullable=False, server_default="0"),
    Column("retry_count", Integer, nullable=False, server_default="0"),
    Column("error_message", Text),
    Column("created_at", DateTime, nullable=False, server_default=func.current_timestamp()),
    Column(
        "updated_at",
        DateTime,
        nullable=False,
        server_default=func.current_timestamp(),
        server_onupdate=func.current_timestamp(),
    ),
    # Scheduler의 READY/미전송 조회와 차량별 공정 진행 갱신을 위한 핵심 인덱스.
    Index("idx_dispatch", "dispatch_status", "is_sent", "id"),
    Index(
        "idx_bottleneck_analysis_pending",
        "is_sent",
        "bottleneck_analysis_done",
        "id",
    ),
    Index(
        "idx_defect_transfer_analysis_pending",
        "is_sent",
        "defect_transfer_analysis_done",
        "id",
    ),
    Index("idx_car_process", "car_master_id", "process_code"),
    Index("idx_process_status", "process_code", "dispatch_status"),
    Index("idx_equipment_status", "equipment_id", "dispatch_status"),
    Index("idx_event_id", "event_id"),
)

manufacturing_event_template = Table(
    "manufacturing_event_template",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("template_name", String(50), nullable=False),
    Column("template_event_id", String(100), nullable=False, unique=True),
    Column("event_offset_us", BigInteger, nullable=False),
    Column("car_master_id", BigInteger, ForeignKey("car_master.id"), nullable=False),
    Column("equipment_id", BigInteger, ForeignKey("equipment.id"), nullable=False),
    Column("process_code", process_code_enum, nullable=False),
    Column("station_code", String(50)),
    Column("equipment_code", String(50), nullable=False),
    Column("equipment_type", String(50)),
    Column("equipment_status", String(30)),
    Column("event_type", String(50)),
    Column("event_json", JSON, nullable=False),
    Column("created_at", DateTime, nullable=False, server_default=func.current_timestamp()),
    UniqueConstraint("template_name", "event_offset_us", name="uq_template_offset"),
    Index("idx_template_name_offset", "template_name", "event_offset_us"),
    Index("idx_template_process_offset", "template_name", "process_code", "event_offset_us"),
    Index("idx_template_equipment_offset", "template_name", "equipment_code", "event_offset_us"),
)

manufacturing_event_generation_job = Table(
    "manufacturing_event_generation_job",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("job_id", String(100), nullable=False, unique=True),
    Column("job_type", String(50), nullable=False),
    Column("status", String(30), nullable=False),
    Column("request_json", JSON, nullable=False),
    Column("result_json", JSON),
    Column("error_message", String(1000)),
    Column("total_expected_events", BigInteger, nullable=False, server_default="0"),
    Column("generated_count", BigInteger, nullable=False, server_default="0"),
    Column("affected_rows", BigInteger, nullable=False, server_default="0"),
    Column("created_at", DateTime, nullable=False, server_default=func.current_timestamp()),
    Column("started_at", DateTime),
    Column("finished_at", DateTime),
    Column("updated_at", DateTime, nullable=False, server_default=func.current_timestamp()),
    Index("idx_generation_job_status_created", "status", "created_at"),
    Index("idx_generation_job_type_created", "job_type", "created_at"),
)
