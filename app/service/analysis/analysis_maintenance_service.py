from __future__ import annotations

import logging
from datetime import date as DateType
from datetime import datetime
from pathlib import Path
from uuid import uuid4
from typing import Any

from sqlalchemy import select

from app.core.config import settings
from app.dto.response.analysis_maintenance_response import (
    AnalysisMaintenanceResponse,
    AnalysisMaintenanceSummary,
)
from app.ml.inference.bottleneck_detector import BottleneckDetector
from app.ml.inference.defect_transfer_detector import (
    DefectTransferDetector,
    has_only_model_probability_cause,
)
from app.repository.bottleneck_analysis_repository import BottleneckAnalysisRepository
from app.repository.defect_transfer_prediction_repository import (
    DefectTransferPredictionRepository,
)
from app.repository.sampledb_schema import car_master
from app.search.process_analysis_search import ProcessAnalysisSearchRepository
from app.utils.database_utils import mysql_connect_args_for_seoul
from app.utils.datetime_utils import SEOUL_TZ, seoul_now
from app.utils.process_label_utils import NEXT_PROCESS, format_process_with_line
from app.utils.process_label_utils import equipment_code_for_car_process


logger = logging.getLogger(__name__)


class AnalysisMaintenanceService:
    def __init__(self) -> None:
        self.bottleneck_repository = BottleneckAnalysisRepository(
            settings.bottleneck_database_url,
            event_database_url=settings.sample_database_connection_url,
        )
        self.defect_repository = DefectTransferPredictionRepository(
            settings.main_database_connection_url,
            event_database_url=settings.sample_database_connection_url or settings.main_database_connection_url,
        )
        self.search_repository = self._create_search_repository()
        self.bottleneck_detector = BottleneckDetector(
            Path("app/ml/artifacts/bottleneck/bottleneck_iforest_model.pkl"),
        )
        self.defect_detector = DefectTransferDetector()

    def backfill_bottleneck(
        self,
        *,
        from_date: DateType,
        to_date: DateType,
        reindex_es: bool = True,
        reset_flags: bool = True,
        dry_run: bool = False,
    ) -> AnalysisMaintenanceResponse:
        items: list[AnalysisMaintenanceSummary] = []
        totals = self._empty_totals()

        for current_date in self._date_range(from_date, to_date):
            histories = self.bottleneck_repository.list_manufacturing_event_histories(
                analysis_date=current_date,
            )
            source_count = len(histories)
            if source_count == 0:
                items.append(
                    self._summary(
                        "bottleneck",
                        current_date,
                        source_count=0,
                        processed=0,
                        saved=0,
                        skipped=0,
                        failed=0,
                        deleted=0,
                        reindexed=0,
                        notes=["no source events"],
                    ),
                )
                continue

            if reset_flags and not dry_run:
                self.bottleneck_repository.reset_bottleneck_analysis_done(
                    [
                        int(row["manufacturing_event_id"])
                        for row in histories
                        if row.get("manufacturing_event_id") is not None
                    ],
                )

            if dry_run:
                items.append(
                    self._summary(
                        "bottleneck",
                        current_date,
                        source_count=source_count,
                        processed=source_count,
                        saved=source_count,
                        skipped=0,
                        failed=0,
                        deleted=0,
                        reindexed=0,
                        notes=["dry-run"],
                    ),
                )
                totals["source_count"] += source_count
                totals["processed_count"] += source_count
                totals["saved_count"] += source_count
                continue

            deleted_count = self.bottleneck_repository.delete_results_by_date(current_date)
            new_summaries = self.bottleneck_detector.summarize_manufacturing_event_histories(histories)
            ranked_summaries = self._rank_bottleneck_summaries(new_summaries)
            detected_at = self._analysis_detected_at(histories)
            self.bottleneck_repository.replace_results(
                ranked_summaries,
                detected_at=detected_at,
                start_rank=1,
                end_rank=max(len(ranked_summaries), 1),
            )

            es_reindexed_count = 0
            if reindex_es:
                es_reindexed_count = self._reindex_bottleneck_day(
                    current_date=current_date,
                    histories=histories,
                    summaries=ranked_summaries,
                    detected_at=detected_at,
                )

            self.bottleneck_repository.mark_bottleneck_analysis_done(
                [
                    int(row["manufacturing_event_id"])
                    for row in histories
                    if row.get("manufacturing_event_id") is not None
                ],
            )

            items.append(
                self._summary(
                    "bottleneck",
                    current_date,
                    source_count=source_count,
                    processed=source_count,
                    saved=len(ranked_summaries),
                    skipped=0,
                    failed=0,
                    deleted=deleted_count,
                    reindexed=es_reindexed_count,
                    notes=[],
                ),
            )
            totals["source_count"] += source_count
            totals["processed_count"] += source_count
            totals["saved_count"] += len(ranked_summaries)
            totals["deleted_count"] += deleted_count
            totals["es_reindexed_count"] += es_reindexed_count
            self._clear_analysis_caches()

        return self._response("backfill", items, totals)

    def backfill_defect_transfer(
        self,
        *,
        from_date: DateType,
        to_date: DateType,
        reindex_es: bool = True,
        reset_flags: bool = True,
        dry_run: bool = False,
    ) -> AnalysisMaintenanceResponse:
        items: list[AnalysisMaintenanceSummary] = []
        totals = self._empty_totals()

        for current_date in self._date_range(from_date, to_date):
            source_rows = self.defect_repository.list_prediction_source_events(
                analysis_date=current_date,
            )
            source_count = len(source_rows)
            if source_count == 0:
                items.append(
                    self._summary(
                        "defect-transfer",
                        current_date,
                        source_count=0,
                        processed=0,
                        saved=0,
                        skipped=0,
                        failed=0,
                        deleted=0,
                        reindexed=0,
                        notes=["no source events"],
                    ),
                )
                continue

            if reset_flags and not dry_run:
                self.defect_repository.delete_source_analysis_done_flags(
                    event_ids=[int(row["id"]) for row in source_rows],
                    column_name="defect_transfer_analysis_done",
                )

            if dry_run:
                items.append(
                    self._summary(
                        "defect-transfer",
                        current_date,
                        source_count=source_count,
                        processed=source_count,
                        saved=source_count,
                        skipped=0,
                        failed=0,
                        deleted=0,
                        reindexed=0,
                        notes=["dry-run"],
                    ),
                )
                totals["source_count"] += source_count
                totals["processed_count"] += source_count
                totals["saved_count"] += source_count
                continue

            deleted_count = self.defect_repository.delete_predictions_by_date(current_date)
            processed = 0
            saved = 0
            skipped = 0
            failed = 0
            for row in source_rows:
                try:
                    prediction = self.defect_detector.predict_event(
                        self._event_json(row.get("event_json")),
                        str(row.get("process_code") or ""),
                    )
                    if has_only_model_probability_cause(prediction.causes):
                        skipped += 1
                        continue
                    predicted_at = self._predict_at(row, current_date)
                    saved += self.defect_repository.replace_prediction_result(
                        event_id=str(row["event_id"]),
                        car_master_id=int(row["car_master_id"]),
                        source_process_code=prediction.current_process_code,
                        target_process_code=prediction.predicted_process_code,
                        current_defect_probability=prediction.defect_probability,
                        target_defect_probability=prediction.transfer_probability,
                        predicted_defect_process=self._format_predicted_defect_process(
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
                        predicted_at=predicted_at,
                    )
                    processed += 1
                    self.defect_repository.mark_defect_transfer_analysis_done(str(row["event_id"]))
                except Exception:
                    failed += 1
                    logger.exception(
                        "Failed to backfill defect transfer prediction: event_id=%s",
                        row.get("event_id"),
                    )

            es_reindexed_count = 0
            if reindex_es:
                es_reindexed_count = self._reindex_defect_transfer_day(
                    current_date=current_date,
                    source_rows=source_rows,
                )

            items.append(
                self._summary(
                    "defect-transfer",
                    current_date,
                    source_count=source_count,
                    processed=processed,
                    saved=saved,
                    skipped=skipped,
                    failed=failed,
                    deleted=deleted_count,
                    reindexed=es_reindexed_count,
                    notes=[],
                ),
            )
            totals["source_count"] += source_count
            totals["processed_count"] += processed
            totals["saved_count"] += saved
            totals["skipped_count"] += skipped
            totals["failed_count"] += failed
            totals["deleted_count"] += deleted_count
            totals["es_reindexed_count"] += es_reindexed_count
            self._clear_analysis_caches()

        return self._response("backfill", items, totals)

    def reindex_bottleneck(
        self,
        *,
        from_date: DateType,
        to_date: DateType,
        dry_run: bool = False,
    ) -> AnalysisMaintenanceResponse:
        items: list[AnalysisMaintenanceSummary] = []
        totals = self._empty_totals()

        for current_date in self._date_range(from_date, to_date):
            histories = self.bottleneck_repository.list_manufacturing_event_histories(
                analysis_date=current_date,
            )
            source_count = len(histories)
            if source_count == 0:
                items.append(
                    self._summary(
                        "bottleneck",
                        current_date,
                        source_count=0,
                        processed=0,
                        saved=0,
                        skipped=0,
                        failed=0,
                        deleted=0,
                        reindexed=0,
                        notes=["no source events"],
                    ),
                )
                continue

            if dry_run:
                items.append(
                    self._summary(
                        "bottleneck",
                        current_date,
                        source_count=source_count,
                        processed=source_count,
                        saved=source_count,
                        skipped=0,
                        failed=0,
                        deleted=0,
                        reindexed=0,
                        notes=["dry-run"],
                    ),
                )
                totals["source_count"] += source_count
                totals["processed_count"] += source_count
                totals["saved_count"] += source_count
                continue

            summaries = self._rank_bottleneck_summaries(
                self.bottleneck_detector.summarize_manufacturing_event_histories(histories),
            )
            detected_at = self._analysis_detected_at(histories)
            es_reindexed_count = self._reindex_bottleneck_day(
                current_date=current_date,
                histories=histories,
                summaries=summaries,
                detected_at=detected_at,
            )

            items.append(
                self._summary(
                    "bottleneck",
                    current_date,
                    source_count=source_count,
                    processed=source_count,
                    saved=len(summaries),
                    skipped=0,
                    failed=0,
                    deleted=0,
                    reindexed=es_reindexed_count,
                    notes=[],
                ),
            )
            totals["source_count"] += source_count
            totals["processed_count"] += source_count
            totals["saved_count"] += len(summaries)
            totals["es_reindexed_count"] += es_reindexed_count
            self._clear_analysis_caches()

        return self._response("reindex", items, totals)

    def reindex_defect_transfer(
        self,
        *,
        from_date: DateType,
        to_date: DateType,
        dry_run: bool = False,
    ) -> AnalysisMaintenanceResponse:
        items: list[AnalysisMaintenanceSummary] = []
        totals = self._empty_totals()

        for current_date in self._date_range(from_date, to_date):
            source_rows = self.defect_repository.list_prediction_source_events(
                analysis_date=current_date,
            )
            source_count = len(source_rows)
            if source_count == 0:
                items.append(
                    self._summary(
                        "defect-transfer",
                        current_date,
                        source_count=0,
                        processed=0,
                        saved=0,
                        skipped=0,
                        failed=0,
                        deleted=0,
                        reindexed=0,
                        notes=["no source events"],
                    ),
                )
                continue

            if dry_run:
                items.append(
                    self._summary(
                        "defect-transfer",
                        current_date,
                        source_count=source_count,
                        processed=source_count,
                        saved=source_count,
                        skipped=0,
                        failed=0,
                        deleted=0,
                        reindexed=0,
                        notes=["dry-run"],
                    ),
                )
                totals["source_count"] += source_count
                totals["processed_count"] += source_count
                totals["saved_count"] += source_count
                continue

            es_reindexed_count = self._reindex_defect_transfer_day(
                current_date=current_date,
                source_rows=source_rows,
            )
            items.append(
                self._summary(
                    "defect-transfer",
                    current_date,
                    source_count=source_count,
                    processed=source_count,
                    saved=0,
                    skipped=0,
                    failed=0,
                    deleted=0,
                    reindexed=es_reindexed_count,
                    notes=[],
                ),
            )
            totals["source_count"] += source_count
            totals["processed_count"] += source_count
            totals["es_reindexed_count"] += es_reindexed_count
            self._clear_analysis_caches()

        return self._response("reindex", items, totals)

    def _reindex_bottleneck_day(
        self,
        *,
        current_date: DateType,
        histories: list[dict[str, Any]],
        summaries: list[dict[str, Any]],
        detected_at: datetime,
    ) -> int:
        if self.search_repository is None:
            return 0
        self.search_repository.delete_bottleneck_documents_by_date(current_date)
        sync_event = self._build_bottleneck_sync_event(
            histories=histories,
            summaries=summaries,
            detected_at=detected_at,
        )
        self.search_repository.index_bottleneck_snapshot(sync_event)
        return len(summaries)

    def _reindex_defect_transfer_day(
        self,
        *,
        current_date: DateType,
        source_rows: list[dict[str, Any]],
    ) -> int:
        if self.search_repository is None:
            return 0
        self.search_repository.delete_defect_transfer_documents_by_date(current_date)
        indexed = 0
        for row in source_rows:
            try:
                prediction = self.defect_detector.predict_event(
                    self._event_json(row.get("event_json")),
                    str(row.get("process_code") or ""),
                )
                if has_only_model_probability_cause(prediction.causes):
                    continue
                sync_event = self._build_defect_transfer_sync_event(
                    row=row,
                    prediction=prediction,
                )
                self.search_repository.index_defect_transfer_prediction(sync_event)
                indexed += 1
            except Exception:
                logger.exception(
                    "Failed to reindex defect transfer document: event_id=%s",
                    row.get("event_id"),
                )
        return indexed

    def _build_bottleneck_sync_event(
        self,
        *,
        histories: list[dict[str, Any]],
        summaries: list[dict[str, Any]],
        detected_at: datetime,
    ) -> dict[str, Any]:
        sync_id = f"SNAP-{uuid4()}"
        first_history = histories[0]
        first_summary = summaries[0] if summaries else {}
        detected_at_iso = self._to_seoul_iso(detected_at)
        return {
            "syncId": sync_id,
            "analysisType": "BOTTLENECK_ANALYSIS_SYNC",
            "sourceService": "AI_SERVICE",
            "detectedAt": detected_at_iso,
            "analyzedAt": detected_at_iso,
            "eventId": first_history.get("event_id"),
            "carMasterId": first_history.get("car_master_id"),
            "mostBottleneckProcess": first_summary.get("process_code"),
            "mostBottleneckRiskLevel": self._risk_level(float(first_summary.get("risk_score") or 0.0)),
            "items": [
                {
                    "manufacturingEventId": summary.get("manufacturing_event_id"),
                    "carMasterId": summary.get("car_master_id") or first_history.get("car_master_id"),
                    "processCode": summary.get("process_code"),
                    "equipmentCode": summary.get("equipment_code"),
                    "rankNo": summary.get("rank_no"),
                    "avgDelayTime": summary.get("avg_delay_time"),
                    "affectedVehicleCount": summary.get("affected_vehicle_count"),
                    "riskScore": summary.get("risk_score"),
                    "riskLevel": self._risk_level(float(summary.get("risk_score") or 0.0)),
                }
                for summary in summaries
            ],
        }

    def _build_defect_transfer_sync_event(
        self,
        *,
        row: dict[str, Any],
        prediction: Any,
    ) -> dict[str, Any]:
        sync_id = f"SYNC-{uuid4()}"
        event_json = self._event_json(row.get("event_json"))
        vehicle_id = self._vehicle_id_for_car_master_id(int(row["car_master_id"]))
        source_equipment_code = self._source_equipment_code(row, event_json)
        current_process = format_process_with_line(
            str(row.get("process_code") or ""),
            source_equipment_code,
        )
        predicted_process = self._format_predicted_defect_process(
            prediction.predicted_process_code,
            row,
        )
        target_equipment_code = self._target_equipment_code(
            prediction.predicted_process_code,
            row,
            event_json,
        )
        fallback_date = self._analysis_date(row)
        predicted_at = self._predict_at(row, fallback_date)
        predicted_at_iso = self._to_seoul_iso(predicted_at)
        causes = [
            {
                "rank": cause.rank,
                "feature": cause.feature,
                "label": cause.label,
                "value": cause.value,
                "impact": cause.impact,
                "message": cause.message,
            }
            for cause in prediction.causes
        ]
        return {
            "syncId": sync_id,
            "analysisType": "DEFECT_TRANSFER_ANALYSIS_SYNC",
            "sourceService": "AI_SERVICE",
            "eventId": row["event_id"],
            "carMasterId": row["car_master_id"],
            "vehicleId": vehicle_id,
            "currentProcessCode": prediction.current_process_code,
            "currentProcess": current_process,
            "sourceEquipmentCode": source_equipment_code,
            "targetEquipmentCode": target_equipment_code,
            "predictedDefectProcess": predicted_process,
            "defectProbability": round(prediction.defect_probability, 4),
            "currentDefectProbability": prediction.defect_probability,
            "transferProbability": prediction.transfer_probability,
            "defectThreshold": prediction.defect_threshold,
            "transferThreshold": prediction.transfer_threshold,
            "expectedStepsAfter": prediction.expected_steps_after,
            "expectedTime": (
                f"{prediction.expected_steps_after} steps later"
                if prediction.expected_steps_after is not None
                else None
            ),
            "riskLevel": prediction.risk_level,
            "predictedAt": predicted_at_iso,
            "createdAt": predicted_at_iso,
            "mainCauses": causes,
            "causes": causes,
            "featureValues": prediction.feature_values,
        }

    @staticmethod
    def _source_equipment_code(row: dict[str, Any], event_json: dict[str, Any]) -> str | None:
        equipment = event_json.get("equipment", {}) if isinstance(event_json, dict) else {}
        code = equipment.get("equipmentCode") if isinstance(equipment, dict) else None
        if code:
            return str(code)
        equipment_id = row.get("equipment_id")
        return str(equipment_id) if equipment_id is not None else None

    def _vehicle_id_for_car_master_id(self, car_master_id: int) -> str | None:
        with self.defect_repository.event_engine.connect() as conn:
            value = conn.execute(
                select(car_master.c.vehicle_id).where(car_master.c.id == car_master_id),
            ).scalar()
        return str(value) if value is not None else None

    @staticmethod
    def _analysis_date(row: dict[str, Any]) -> DateType:
        value = row.get("event_time")
        if isinstance(value, datetime):
            return AnalysisMaintenanceService._to_seoul_naive(value).date()
        return seoul_now().date()

    @staticmethod
    def _predict_at(row: dict[str, Any], fallback_date: DateType) -> datetime:
        value = row.get("event_time")
        if isinstance(value, datetime):
            return AnalysisMaintenanceService._to_seoul_naive(value)
        return datetime.combine(fallback_date, datetime.min.time())

    @staticmethod
    def _format_predicted_defect_process(
        process_code: str | None,
        row: dict[str, Any],
    ) -> str | None:
        if process_code is None:
            return None
        normalized = str(process_code).strip().upper()
        source_code = str(row.get("process_code") or "").strip().upper()
        if normalized == source_code:
            event_json = row.get("event_json")
            if isinstance(event_json, dict):
                equipment = event_json.get("equipment", {})
                equipment_code = str(equipment.get("equipmentCode") or "")
            else:
                equipment_code = ""
        else:
            equipment_code = equipment_code_for_car_process(
                car_master_id=int(row["car_master_id"]),
                process_code=normalized,
            )
        return format_process_with_line(normalized, equipment_code)

    @staticmethod
    def _target_equipment_code(
        process_code: str | None,
        row: dict[str, Any],
        event_json: dict[str, Any],
    ) -> str | None:
        if process_code is None:
            return None
        normalized = str(process_code).strip().upper()
        source_code = str(row.get("process_code") or "").strip().upper()
        if normalized == source_code:
            return AnalysisMaintenanceService._source_equipment_code(row, event_json)
        return equipment_code_for_car_process(
            car_master_id=int(row["car_master_id"]),
            process_code=normalized,
        )

    @staticmethod
    def _date_range(from_date: DateType, to_date: DateType):
        current = from_date
        while current <= to_date:
            yield current
            current = current.fromordinal(current.toordinal() + 1)

    @staticmethod
    def _risk_level(risk_score: float) -> str:
        return "HIGH" if risk_score >= 3.0 else "NORMAL"

    @staticmethod
    def _analysis_detected_at(histories: list[dict[str, Any]]) -> datetime:
        candidates = [
            AnalysisMaintenanceService._to_seoul_naive(value)
            for value in (row.get("event_time") for row in histories)
            if isinstance(value, datetime)
        ]
        if candidates:
            return max(candidates)
        return seoul_now().replace(tzinfo=None)

    @staticmethod
    def _to_seoul_naive(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value
        return value.astimezone(SEOUL_TZ).replace(tzinfo=None)

    @staticmethod
    def _to_seoul_iso(value: datetime) -> str:
        if value.tzinfo is None:
            return value.replace(tzinfo=SEOUL_TZ).isoformat()
        return value.astimezone(SEOUL_TZ).isoformat()

    @staticmethod
    def _rank_bottleneck_summaries(
        summaries: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        ranked = sorted(
            summaries,
            key=lambda row: (
                float(row.get("risk_score") or 0.0),
                float(row.get("avg_delay_time") or 0.0),
                int(row.get("affected_vehicle_count") or 0),
                int(row.get("manufacturing_event_id") or 0),
                int(row.get("car_master_id") or 0),
                str(row.get("process_code") or "").strip().upper(),
                str(row.get("equipment_code") or "").strip().upper(),
            ),
            reverse=True,
        )
        deduped: list[dict[str, Any]] = []
        seen_keys: set[tuple[str, str]] = set()
        for row in ranked:
            key = (
                str(row.get("process_code") or "").strip().upper(),
                str(row.get("equipment_code") or "").strip().upper(),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(row)
        return [{**row, "rank_no": index + 1} for index, row in enumerate(deduped)]

    @staticmethod
    def _event_json(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        return {}

    @staticmethod
    def _empty_totals() -> dict[str, int]:
        return {
            "source_count": 0,
            "processed_count": 0,
            "saved_count": 0,
            "skipped_count": 0,
            "failed_count": 0,
            "deleted_count": 0,
            "es_reindexed_count": 0,
        }

    @staticmethod
    def _summary(
        analysis_name: str,
        source_from: DateType,
        *,
        source_count: int,
        processed: int,
        saved: int,
        skipped: int,
        failed: int,
        deleted: int,
        reindexed: int,
        notes: list[str],
    ) -> AnalysisMaintenanceSummary:
        return AnalysisMaintenanceSummary(
            analysisName=analysis_name,
            sourceFrom=source_from,
            sourceTo=source_from,
            sourceCount=source_count,
            processedCount=processed,
            savedCount=saved,
            skippedCount=skipped,
            failedCount=failed,
            deletedCount=deleted,
            esReindexedCount=reindexed,
            notes=notes,
        )

    @staticmethod
    def _response(
        mode: str,
        items: list[AnalysisMaintenanceSummary],
        totals: dict[str, int],
    ) -> AnalysisMaintenanceResponse:
        return AnalysisMaintenanceResponse(
            mode=mode,
            items=items,
            totalSourceCount=totals["source_count"],
            totalProcessedCount=totals["processed_count"],
            totalSavedCount=totals["saved_count"],
            totalSkippedCount=totals["skipped_count"],
            totalFailedCount=totals["failed_count"],
            totalDeletedCount=totals["deleted_count"],
            totalEsReindexedCount=totals["es_reindexed_count"],
            extra={},
        )

    @staticmethod
    def _create_search_repository() -> ProcessAnalysisSearchRepository | None:
        if not settings.elasticsearch_url:
            return None
        try:
            repository = ProcessAnalysisSearchRepository()
            repository.ensure_indices()
            return repository
        except Exception:
            logger.exception("Elasticsearch maintenance repository is unavailable.")
            return None

    @staticmethod
    def _clear_analysis_caches() -> None:
        if not settings.redis_url:
            return
        try:
            from redis import Redis

            redis_client = Redis.from_url(
                settings.redis_connection_url,
                decode_responses=True,
            )
            patterns = [
                f"{settings.redis_key_prefix}:process:bottleneck:*",
                f"{settings.redis_key_prefix}:process:defect-transfer:*",
            ]
            keys: list[str] = []
            for pattern in patterns:
                keys.extend(list(redis_client.scan_iter(match=pattern)))
            if keys:
                redis_client.delete(*keys)
        except Exception:
            logger.exception("Failed to clear analysis caches after maintenance.")
