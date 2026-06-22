from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import func, select

from app.repository.sampledb_schema import car_master


class CarMasterRepository:
    """기존 car_master 차량을 제조 이벤트의 carMasterId로 매핑한다."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    def get_existing_map(self, count: int) -> dict[str, int]:
        with self.engine.connect() as conn:
            rows = list(
                conn.execute(
                    select(car_master.c.id, car_master.c.vehicle_id)
                    .order_by(car_master.c.id.asc())
                    .limit(count),
                ).mappings(),
            )
        return self._map_first_rows(rows, count)

    def get_map_by_production_date(
        self,
        production_date: date,
        *,
        limit: int | None = None,
    ) -> dict[str, int]:
        """vehicle_id의 생산일자에 해당하는 차량을 id 순서로 반환한다."""
        pattern = f"%-{production_date:%Y%m%d}-%"
        query = (
            select(car_master.c.id, car_master.c.vehicle_id)
            .where(car_master.c.vehicle_id.like(pattern))
            .order_by(car_master.c.id.asc())
        )
        if limit is not None:
            query = query.limit(limit)

        with self.engine.connect() as conn:
            rows = list(conn.execute(query).mappings())

        if not rows:
            raise RuntimeError(
                f"{production_date.isoformat()} 생산 차량이 car_master에 없습니다.",
            )
        if limit is not None and len(rows) < limit:
            raise RuntimeError(
                "요청한 차량 수보다 해당 날짜의 car_master가 부족합니다: "
                f"date={production_date.isoformat()}, required={limit}, "
                f"actual={len(rows)}",
            )
        return {
            str(row["vehicle_id"]): int(row["id"])
            for row in rows
        }

    def count_by_production_date(self, production_date: date) -> int:
        """vehicle_id 생산일자 기준 차량 수를 반환한다."""
        pattern = f"%-{production_date:%Y%m%d}-%"
        query = select(func.count()).select_from(car_master).where(
            car_master.c.vehicle_id.like(pattern),
        )
        with self.engine.connect() as conn:
            return int(conn.execute(query).scalar_one())

    @staticmethod
    def _map_first_rows(rows: list[Any], count: int) -> dict[str, int]:
        if len(rows) < count:
            raise RuntimeError(
                f"car_master row가 부족합니다: required={count}, actual={len(rows)}",
            )
        if int(rows[0]["id"]) != 1:
            raise RuntimeError(
                "제조 이벤트는 car_master.id=1부터 생성되어야 합니다.",
            )
        return {
            str(row["vehicle_id"]): int(row["id"])
            for row in rows[:count]
        }
