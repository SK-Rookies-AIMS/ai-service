from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.mysql import insert

from app.repository.sampledb_schema import manufacturing_event_template


class ManufacturingEventTemplateRepository:
    """manufacturing_event_template의 저장·조회·삭제를 담당한다."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    def count(self, template_name: str) -> int:
        query = select(func.count()).select_from(
            manufacturing_event_template,
        ).where(
            manufacturing_event_template.c.template_name == template_name,
        )
        with self.engine.connect() as conn:
            return int(conn.execute(query).scalar_one())

    def delete(self, template_name: str) -> int:
        statement = manufacturing_event_template.delete().where(
            manufacturing_event_template.c.template_name == template_name,
        )
        with self.engine.begin() as conn:
            result = conn.execute(statement)
            return int(result.rowcount or 0)

    def insert_rows(
        self,
        rows: Iterable[dict[str, Any]],
        *,
        update_existing: bool = False,
    ) -> int:
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
                    select(
                        manufacturing_event_template.c.template_event_id,
                    ).where(
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

    def list_rows(
        self,
        *,
        template_name: str,
        limit: int,
        offset: int = 0,
        process_code: str | None = None,
    ) -> list[dict[str, Any]]:
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
