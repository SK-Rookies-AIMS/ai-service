from __future__ import annotations

import logging
import json
from datetime import datetime
from typing import Any

from sqlalchemy import create_engine, select

from app.core.config import settings
from app.ml.inference.defect_transfer_detector import DefectTransferDetector
from app.repository.defect_transfer_prediction_repository import (
    DefectTransferPredictionRepository,
)
from app.repository.sampledb_schema import manufacturing_event_json
from app.utils.database_utils import mysql_connect_args_for_seoul


logger = logging.getLogger(__name__)


def backfill_defect_transfer_predictions(*, limit: int | None = None) -> dict[str, int]:
    """Backfill defect-transfer predictions from sent manufacturing raw events."""
    if not settings.sample_database_connection_url:
        raise RuntimeError("SAMPLE_DB_NAME is required for defect transfer backfill.")

    event_engine = create_engine(
        settings.sample_database_connection_url,
        connect_args=mysql_connect_args_for_seoul(settings.sample_database_connection_url),
        pool_pre_ping=True,
        future=True,
    )
    result_repository = DefectTransferPredictionRepository(
        settings.main_database_connection_url,
        event_database_url=settings.sample_database_connection_url,
    )
    detector = DefectTransferDetector()

    query = (
        select(
            manufacturing_event_json.c.id,
            manufacturing_event_json.c.event_id,
            manufacturing_event_json.c.car_master_id,
            manufacturing_event_json.c.process_code,
            manufacturing_event_json.c.event_json,
        )
        .where(manufacturing_event_json.c.dispatch_status == "SENT")
        .where(manufacturing_event_json.c.is_sent.is_(True))
        .order_by(manufacturing_event_json.c.id.asc())
    )
    if limit is not None:
        query = query.limit(limit)

    with event_engine.connect() as conn:
        rows = [dict(row) for row in conn.execute(query).mappings()]

    processed = 0
    saved_rows = 0
    failed = 0
    for row in rows:
        try:
            prediction = detector.predict_event(
                _event_json(row["event_json"]),
                str(row["process_code"]),
            )
            saved_rows += result_repository.replace_prediction_result(
                event_id=str(row["event_id"]),
                car_master_id=int(row["car_master_id"]),
                source_process_code=prediction.current_process_code,
                target_process_code=prediction.predicted_process_code,
                current_defect_probability=prediction.defect_probability,
                target_defect_probability=prediction.transfer_probability,
                predicted_defect_process=_format_predicted_defect_process(
                    prediction.predicted_process_code,
                    row,
                ),
                expected_occurrence_step=prediction.expected_steps_after,
                risk_grade=prediction.risk_level,
                causes=[
                    {
                        "message": cause.message,
                        "label": cause.label,
                        "impact": cause.impact,
                    }
                    for cause in prediction.causes
                ],
                predicted_at=datetime.now(),
            )
            processed += 1
        except Exception:
            failed += 1
            logger.exception(
                "Failed to backfill defect transfer prediction: event_id=%s",
                row.get("event_id"),
            )

    return {
        "source_events": len(rows),
        "processed_events": processed,
        "saved_rows": saved_rows,
        "failed_events": failed,
    }


def _event_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return {}


def _format_predicted_defect_process(
    process_code: str | None,
    row: dict[str, Any],
) -> str | None:
    if process_code is None:
        return None
    from app.utils.process_label_utils import (
        equipment_code_for_car_process,
        format_process_with_line,
    )

    source_code = str(row.get("process_code") or "").strip().upper()
    normalized = str(process_code).strip().upper()
    if normalized == source_code:
        event_json = _event_json(row.get("event_json"))
        equipment = event_json.get("equipment", {})
        equipment_code = str(equipment.get("equipmentCode") or "")
    else:
        equipment_code = equipment_code_for_car_process(
            car_master_id=int(row["car_master_id"]),
            process_code=normalized,
        )
    return format_process_with_line(normalized, equipment_code)


def run() -> None:
    print(backfill_defect_transfer_predictions())


if __name__ == "__main__":
    run()
