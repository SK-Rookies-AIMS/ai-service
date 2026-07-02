from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.mysql import insert

from app.repository.sampledb_schema import equipment


DEFAULT_EQUIPMENT_ROWS: list[dict[str, Any]] = [
    {
        "process_code": process_code,
        "equipment_code": f"EQ_{process_code}_{index:03d}",
        "equipment_name": f"{equipment_name_prefix} {index}호",
        "equipment_type": equipment_type,
        "current_status": "RUNNING",
    }
    for process_code, equipment_type, equipment_name_prefix in (
        ("PRESS", "HYDRAULIC_PRESS", "프레스 유압모터"),
        ("BODY", "ROBOT_ARM", "차체 용접 로봇"),
        ("PAINT", "CAMERA", "도장 열화상 카메라"),
        ("ASSEMBLY", "CONVEYOR", "의장 조립 컨베이어"),
    )
    for index in range(1, 6)
]


class EquipmentRepository:
    """equipment 마스터 데이터의 초기화와 조회를 담당한다."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    def seed_defaults(self) -> None:
        with self.engine.begin() as conn:
            if self.engine.dialect.name == "mysql":
                statement = insert(equipment).values(DEFAULT_EQUIPMENT_ROWS)
                statement = statement.on_duplicate_key_update(
                    equipment_name=statement.inserted.equipment_name,
                    process_code=statement.inserted.process_code,
                    equipment_type=statement.inserted.equipment_type,
                    current_status=statement.inserted.current_status,
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
                rows = [
                    {**row, "id": next_id + index}
                    for index, row in enumerate(rows, 1)
                ]
                conn.execute(equipment.insert(), rows)

    def get_map(self) -> dict[str, dict[str, Any]]:
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
