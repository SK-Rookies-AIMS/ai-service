from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Double,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    func,
)

metadata = MetaData()

process_code_enum = Enum("PRESS", "BODY", "PAINT", "ASSEMBLY")
equipment_type_enum = Enum("HYDRAULIC_PRESS", "ROBOT_ARM", "CAMERA", "CONVEYOR")
equipment_status_enum = Enum("NORMAL", "WARNING", "FAULT", "MAINTENANCE")
source_type_enum = Enum("BOSCH", "FORD", "PRESS_CURRENT", "ROBOT_CURRENT", "THERMAL_VISION")
data_type_enum = Enum("PROCESS", "SENSOR", "QUALITY", "THERMAL")
quality_result_enum = Enum("NORMAL", "DEFECT")
expected_label_enum = Enum("NORMAL", "WARNING", "FAULT", "DEFECT")

car_master = Table(
    "car_master",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("vehicle_id", String(50), nullable=False),
    Column("car_type", String(20), nullable=False),
    Column("engine_type", String(30), nullable=False),
    Column("car_color", String(30), nullable=False),
    Column("fuel_efficiency", Integer, nullable=False),
    Column("created_at", DateTime, nullable=False),
)

equipment = Table(
    "equipment",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("process_code", process_code_enum, nullable=False),
    Column("equipment_code", String(50), nullable=False, unique=True),
    Column("equipment_name", String(100), nullable=False),
    Column("equipment_type", equipment_type_enum, nullable=False),
    Column("status", equipment_status_enum, nullable=False, server_default="NORMAL"),
    Column("created_at", DateTime, nullable=False, server_default=func.current_timestamp()),
    Index("idx_equipment_process_code", "process_code"),
)

manufacturing_event = Table(
    "manufacturing_event",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("car_master_id", BigInteger, ForeignKey("car_master.id")),
    Column("sample_manufacturing_event_id", String(50), nullable=False, unique=True),
    Column("equipment_code", String(50), ForeignKey("equipment.equipment_code")),
    Column("source_dataset", String(100)),
    Column("source_type", source_type_enum, nullable=False),
    Column("data_type", data_type_enum, nullable=False),
    Column("process_code", process_code_enum, nullable=False),
    Column("equipment_type", equipment_type_enum, nullable=False),
    Column("station_code", String(50)),
    Column("event_time", DateTime, nullable=False),
    Column("metric_code", String(50)),
    Column("metric_value", Double),
    Column("unit", String(20)),
    Column("process_time", Double),
    Column("waiting_time", Double),
    Column("quality_result", quality_result_enum),
    Column("defect_type", String(50)),
    Column("expected_is_abnormal", Boolean),
    Column("expected_abnormal_type", String(50)),
    Column("expected_severity", String(20)),
    Column("expected_label", expected_label_enum),
    Column("created_at", DateTime, nullable=False, server_default=func.current_timestamp()),
    Index("idx_manufacturing_event_car_master_id", "car_master_id"),
    Index("idx_manufacturing_event_equipment_code", "equipment_code"),
    Index("idx_manufacturing_event_event_time", "event_time"),
)

thermal_vision = Table(
    "thermal_vision",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("manufacturing_event_id", BigInteger, ForeignKey("manufacturing_event.id"), nullable=False),
    Column("car_master_id", BigInteger, ForeignKey("car_master.id")),
    Column("image_position", String(50)),
    Column("thermal_avg_temp", Double),
    Column("thermal_max_temp", Double),
    Column("thermal_min_temp", Double),
    Column("thermal_std_temp", Double),
    Column("thickness_value", Double),
    Column("defect_score", Double),
    Column("expected_vision_label", String(30)),
    Column("created_at", DateTime, nullable=False, server_default=func.current_timestamp()),
    UniqueConstraint("manufacturing_event_id", name="uq_thermal_vision_manufacturing_event_id"),
    Index("idx_thermal_vision_car_master_id", "car_master_id"),
)

robot_arm_vibration = Table(
    "robot_arm_vibration",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("manufacturing_event_id", BigInteger, ForeignKey("manufacturing_event.id"), nullable=False),
    Column("equipment_id", BigInteger, ForeignKey("equipment.id"), nullable=False),
    Column("measured_at", DateTime, nullable=False),
    Column("freq_0_100_hz", Float),
    Column("freq_101_200_hz", Float),
    Column("freq_201_300_hz", Float),
    Column("freq_301_400_hz", Float),
    Column("freq_401_500_hz", Float),
    Column("freq_501_600_hz", Float),
    Column("freq_601_700_hz", Float),
    Column("freq_701_800_hz", Float),
    Column("freq_801_900_hz", Float),
    Column("freq_901_1000_hz", Float),
    Column("freq_1001_1100_hz", Float),
    Column("freq_1101_1200_hz", Float),
    Column("freq_1201_1300_hz", Float),
    Column("freq_1301_1400_hz", Float),
    Column("freq_1401_1500_hz", Float),
    Column("freq_1501_1600_hz", Float),
    Index("idx_robot_arm_vibration_event_id", "manufacturing_event_id"),
    Index("idx_robot_arm_vibration_equipment_id", "equipment_id"),
    Index("idx_robot_arm_vibration_measured_at", "measured_at"),
)
