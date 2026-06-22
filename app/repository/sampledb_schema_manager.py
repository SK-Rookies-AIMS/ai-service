from __future__ import annotations

from typing import Any

from sqlalchemy import inspect, text

from app.repository.sampledb_schema import metadata


class SampleDbSchemaManager:
    """sampledb 테이블 생성과 점진적 스키마 마이그레이션을 담당한다."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    def ensure_schema(self) -> None:
        metadata.create_all(self.engine)
        self._migrate_prd_columns()

    def _migrate_prd_columns(self) -> None:
        equipment_columns = {
            "current_status": "VARCHAR(20) NOT NULL DEFAULT 'RUNNING'",
            "health_status": "VARCHAR(20) NOT NULL DEFAULT 'NORMAL'",
            "last_fault_time": "DATETIME NULL",
            "last_recovered_time": "DATETIME NULL",
            "reason": "VARCHAR(255) NULL",
            "updated_at": (
                "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"
                if self.engine.dialect.name == "mysql"
                else "DATETIME NULL"
            ),
        }
        self._add_missing_columns("equipment", equipment_columns)
        self._align_equipment_mysql_types()
        self._align_manufacturing_event_json_mysql()
        self._allow_car_master_created_at_null()
        self._create_missing_indexes("equipment")
        self._create_missing_indexes("manufacturing_event_json")

    def _allow_car_master_created_at_null(self) -> None:
        columns = inspect(self.engine).get_columns("car_master")
        created_at = next(
            (column for column in columns if column["name"] == "created_at"),
            None,
        )
        if created_at is None or created_at["nullable"]:
            return
        if self.engine.dialect.name == "mysql":
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        "ALTER TABLE car_master "
                        "MODIFY COLUMN created_at DATETIME NULL",
                    ),
                )

    def _align_equipment_mysql_types(self) -> None:
        if self.engine.dialect.name != "mysql":
            return
        equipment_types = {
            column["name"]: column["type"].__class__.__name__.upper()
            for column in inspect(self.engine).get_columns("equipment")
        }
        statements: list[str] = []
        if equipment_types.get("current_status") != "ENUM":
            statements.append(
                "ALTER TABLE equipment MODIFY COLUMN current_status "
                "ENUM('RUNNING','IDLE','STOPPED','FAULT','MAINTENANCE') "
                "NOT NULL DEFAULT 'RUNNING'",
            )
        if equipment_types.get("health_status") != "ENUM":
            statements.append(
                "ALTER TABLE equipment MODIFY COLUMN health_status "
                "ENUM('NORMAL','WARNING','CRITICAL') NOT NULL DEFAULT 'NORMAL'",
            )
        if statements:
            with self.engine.begin() as conn:
                for statement in statements:
                    conn.execute(text(statement))

    def _align_manufacturing_event_json_mysql(self) -> None:
        if self.engine.dialect.name != "mysql":
            return
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "ALTER TABLE manufacturing_event_json "
                    "MODIFY COLUMN event_time DATETIME NULL, "
                    "MODIFY COLUMN dispatch_status "
                    "ENUM('PENDING','READY','SENT','BLOCKED') "
                    "NOT NULL DEFAULT 'PENDING', "
                    "MODIFY COLUMN analysis_status "
                    "ENUM('NOT_ANALYZED','NORMAL','ABNORMAL') "
                    "NOT NULL DEFAULT 'NOT_ANALYZED', "
                    "MODIFY COLUMN updated_at DATETIME NOT NULL "
                    "DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
                ),
            )

    def _add_missing_columns(
        self,
        table_name: str,
        definitions: dict[str, str],
    ) -> None:
        existing_columns = self._table_columns(table_name)
        missing = [
            (column_name, definition)
            for column_name, definition in definitions.items()
            if column_name not in existing_columns
        ]
        if not missing:
            return
        with self.engine.begin() as conn:
            for column_name, definition in missing:
                conn.execute(
                    text(
                        f"ALTER TABLE {table_name} "
                        f"ADD COLUMN {column_name} {definition}",
                    ),
                )

    def _table_columns(self, table_name: str) -> set[str]:
        inspector = inspect(self.engine)
        if not inspector.has_table(table_name):
            return set(metadata.tables[table_name].c.keys())
        return {
            column["name"]
            for column in inspector.get_columns(table_name)
        }

    def _create_missing_indexes(self, table_name: str) -> None:
        table = metadata.tables[table_name]
        existing_names = {
            index["name"]
            for index in inspect(self.engine).get_indexes(table_name)
            if index.get("name")
        }
        for index in table.indexes:
            if index.name and index.name not in existing_names:
                index.create(self.engine, checkfirst=True)
