from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from app.core.config import settings

logger = logging.getLogger(__name__)


class ProcessAnalysisSearchRepository:
    def __init__(self) -> None:
        self._client: Any | None = None

    @property
    def enabled(self) -> bool:
        return bool(settings.elasticsearch_url)

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = self._create_client()
        return self._client

    def ensure_indices(self) -> None:
        if not self.enabled:
            return

        client = self.client
        for index_name, body in self._index_definitions().items():
            try:
                if client.indices.exists(index=index_name):
                    continue
                client.indices.create(index=index_name, body=body)
                logger.info("Elasticsearch index created: %s", index_name)
            except Exception:
                logger.exception("Failed to ensure Elasticsearch index: %s", index_name)
                raise

    def index_sync_event(self, event: dict[str, Any]) -> None:
        analysis_type = str(event.get("analysisType") or "").strip().upper()
        if analysis_type == "BOTTLENECK_ANALYSIS_SYNC":
            self.index_bottleneck_snapshot(event)
            return
        if analysis_type == "DEFECT_TRANSFER_ANALYSIS_SYNC":
            self.index_defect_transfer_prediction(event)
            return
        # The sync consumer reuses the existing analysis topic, so non-sync
        # analysis events can arrive here as well. They are intentionally ignored.
        return

    def index_bottleneck_snapshot(self, event: dict[str, Any]) -> None:
        if not self.enabled:
            return

        snapshot_id = str(event.get("snapshotId") or event.get("syncId") or "").strip()
        items = event.get("items") or []
        if not snapshot_id or not isinstance(items, list) or not items:
            return

        detected_at = self._normalize_datetime(event.get("detectedAt"))
        analyzed_at = self._normalize_datetime(event.get("analyzedAt"))
        base_doc = {
            "analysisType": "BOTTLENECK_ANALYSIS",
            "syncId": str(event.get("syncId") or snapshot_id),
            "snapshotId": snapshot_id,
            "detectedAt": detected_at,
            "analyzedAt": analyzed_at,
            "eventId": event.get("eventId"),
            "carMasterId": event.get("carMasterId"),
            "mostBottleneckProcess": event.get("mostBottleneckProcess"),
            "mostBottleneckRiskLevel": event.get("mostBottleneckRiskLevel"),
            "sourceService": event.get("sourceService") or "AI_SERVICE",
        }
        actions: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            doc = {
                **base_doc,
                "rankNo": self._safe_int(item.get("rankNo")),
                "manufacturingEventId": item.get("manufacturingEventId"),
                "carMasterId": item.get("carMasterId") or event.get("carMasterId"),
                "processCode": item.get("processCode"),
                "equipmentCode": item.get("equipmentCode"),
                "avgDelayTime": self._safe_float(item.get("avgDelayTime")),
                "affectedVehicleCount": self._safe_int(item.get("affectedVehicleCount")),
                "riskScore": self._safe_float(item.get("riskScore")),
                "riskLevel": item.get("riskLevel"),
            }
            doc_id = self._bottleneck_doc_id(snapshot_id, doc)
            actions.append(
                {
                    "_index": settings.elasticsearch_bottleneck_index,
                    "_id": doc_id,
                    "_source": doc,
                },
            )
        self._bulk_index(actions)

    def index_defect_transfer_prediction(self, event: dict[str, Any]) -> None:
        if not self.enabled:
            return

        sync_id = str(event.get("syncId") or "").strip()
        if not sync_id:
            return

        doc = {
            "analysisType": "DEFECT_TRANSFER_ANALYSIS",
            "syncId": sync_id,
            "eventId": event.get("eventId"),
            "carMasterId": event.get("carMasterId"),
            "vehicleId": event.get("vehicleId"),
            "currentProcess": event.get("currentProcess"),
            "currentProcessCode": event.get("currentProcessCode"),
            "sourceProcessCode": event.get("sourceProcessCode"),
            "sourceEquipmentCode": event.get("sourceEquipmentCode"),
            "predictedDefectProcess": event.get("predictedDefectProcess"),
            "currentDefectProbability": self._safe_float(event.get("currentDefectProbability")),
            "transferProbability": self._safe_float(event.get("transferProbability")),
            "defectThreshold": self._safe_float(event.get("defectThreshold")),
            "transferThreshold": self._safe_float(event.get("transferThreshold")),
            "defectProbability": self._safe_int(event.get("defectProbability")),
            "expectedTime": event.get("expectedTime"),
            "expectedStepsAfter": self._safe_int(event.get("expectedStepsAfter")),
            "riskLevel": event.get("riskLevel"),
            "predictedAt": self._normalize_datetime(event.get("predictedAt")),
            "createdAt": self._normalize_datetime(event.get("createdAt") or event.get("predictedAt")),
            "mainCauses": self._normalize_main_causes(event.get("mainCauses") or event.get("causes")),
            "featureValues": event.get("featureValues") or {},
            "sourceService": event.get("sourceService") or "AI_SERVICE",
        }
        doc_id = f"{sync_id}:{doc.get('eventId') or doc.get('carMasterId')}"
        self._bulk_index(
            [
                {
                    "_index": settings.elasticsearch_defect_transfer_index,
                    "_id": doc_id,
                    "_source": doc,
                },
            ],
        )

    def list_bottleneck_page(
        self,
        *,
        cursor: int,
        size: int,
    ) -> tuple[list[dict[str, Any]], bool]:
        latest_snapshot_id = self._latest_bottleneck_snapshot_id()
        if not latest_snapshot_id:
            return [], False

        page = max(cursor, 0)
        safe_size = max(1, min(size, 100))
        query = {
            "query": {
                "term": {
                    "snapshotId": latest_snapshot_id,
                },
            },
            "sort": [
                {"rankNo": {"order": "asc"}},
                {"processCode": {"order": "asc"}},
                {"equipmentCode": {"order": "asc"}},
            ],
            "from": page * safe_size,
            "size": safe_size + 1,
            "track_total_hits": True,
        }
        response = self.client.search(
            index=settings.elasticsearch_bottleneck_index,
            body=query,
        )
        rows = [
            self._map_bottleneck_source(hit.get("_source") or {})
            for hit in response.get("hits", {}).get("hits", [])
        ]
        has_next = len(rows) > safe_size
        return rows[:safe_size], has_next

    def count_bottleneck_page(self) -> int:
        latest_snapshot_id = self._latest_bottleneck_snapshot_id()
        if not latest_snapshot_id:
            return 0

        query = {
            "query": {
                "term": {
                    "snapshotId": latest_snapshot_id,
                },
            },
            "size": 0,
            "track_total_hits": True,
        }
        response = self.client.search(
            index=settings.elasticsearch_bottleneck_index,
            body=query,
        )
        total = response.get("hits", {}).get("total", 0)
        if isinstance(total, dict):
            return int(total.get("value") or 0)
        return int(total or 0)

    def list_defect_prediction_page(
        self,
        *,
        cursor: int,
        size: int,
    ) -> tuple[list[dict[str, Any]], bool]:
        page = max(cursor, 0)
        safe_size = max(1, min(size, 100))
        query = {
            "query": {"match_all": {}},
            "collapse": {"field": "carMasterId"},
            "sort": [
                {"transferProbability": {"order": "desc", "missing": "_last"}},
                {"currentDefectProbability": {"order": "desc", "missing": "_last"}},
                {"predictedAt": {"order": "desc"}},
                {"syncId": {"order": "desc"}},
            ],
            "from": page * safe_size,
            "size": safe_size + 1,
            "track_total_hits": True,
        }
        response = self.client.search(
            index=settings.elasticsearch_defect_transfer_index,
            body=query,
        )
        rows = [
            self._map_defect_source(hit.get("_source") or {})
            for hit in response.get("hits", {}).get("hits", [])
        ]
        has_next = len(rows) > safe_size
        return rows[:safe_size], has_next

    def get_latest_defect_cause_document(
        self,
        *,
        vehicle_id: str | None,
    ) -> dict[str, Any] | None:
        query: dict[str, Any] = {
            "size": 1,
            "sort": [
                {"predictedAt": {"order": "desc"}},
                {"syncId": {"order": "desc"}},
            ],
        }
        if vehicle_id:
            query["query"] = {"term": {"vehicleId": vehicle_id}}
        else:
            query["query"] = {"match_all": {}}

        response = self.client.search(
            index=settings.elasticsearch_defect_transfer_index,
            body=query,
        )
        hits = response.get("hits", {}).get("hits", [])
        if not hits:
            return None
        return self._map_defect_source(hits[0].get("_source") or {})

    def _latest_bottleneck_snapshot_id(self) -> str | None:
        query = {
            "size": 1,
            "query": {"match_all": {}},
            "sort": [
                {"detectedAt": {"order": "desc"}},
                {"rankNo": {"order": "asc"}},
                {"syncId": {"order": "desc"}},
            ],
        }
        response = self.client.search(
            index=settings.elasticsearch_bottleneck_index,
            body=query,
        )
        hits = response.get("hits", {}).get("hits", [])
        if not hits:
            return None
        source = hits[0].get("_source") or {}
        snapshot_id = source.get("snapshotId")
        return str(snapshot_id) if snapshot_id is not None else None

    def _bulk_index(self, actions: list[dict[str, Any]]) -> None:
        if not actions:
            return
        from opensearchpy import helpers

        helpers.bulk(self.client, actions, raise_on_error=True)

    def _create_client(self) -> Any:
        if not settings.elasticsearch_url:
            raise RuntimeError("Elasticsearch URL is not configured.")

        try:
            from opensearchpy import OpenSearch
        except ModuleNotFoundError as exc:
            raise RuntimeError("opensearch-py is required for Elasticsearch integration.") from exc

        parsed = urlparse(settings.elasticsearch_url)
        if not parsed.scheme or not parsed.hostname:
            raise ValueError(f"Invalid Elasticsearch URL: {settings.elasticsearch_url}")

        hosts = [
            {
                "host": parsed.hostname,
                "port": parsed.port or (443 if parsed.scheme == "https" else 80),
                "scheme": parsed.scheme,
            },
        ]
        kwargs: dict[str, Any] = {
            "hosts": hosts,
            "use_ssl": parsed.scheme == "https",
            "verify_certs": settings.elasticsearch_verify_certs,
            "ssl_show_warn": False,
            "request_timeout": 30,
            "retry_on_timeout": True,
            "max_retries": 3,
        }
        if settings.elasticsearch_username:
            kwargs["http_auth"] = (
                settings.elasticsearch_username,
                settings.elasticsearch_password or "",
            )
        return OpenSearch(**kwargs)

    def _index_definitions(self) -> dict[str, dict[str, Any]]:
        return {
            settings.elasticsearch_bottleneck_index: {
                "settings": {
                    "index": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0,
                    },
                },
                "mappings": {
                    "dynamic": True,
                    "properties": {
                        "analysisType": {"type": "keyword"},
                        "syncId": {"type": "keyword"},
                        "snapshotId": {"type": "keyword"},
                        "detectedAt": {"type": "date"},
                        "analyzedAt": {"type": "date"},
                        "eventId": {"type": "keyword"},
                        "carMasterId": {"type": "long"},
                        "mostBottleneckProcess": {"type": "keyword"},
                        "mostBottleneckRiskLevel": {"type": "keyword"},
                        "rankNo": {"type": "integer"},
                        "manufacturingEventId": {"type": "long"},
                        "processCode": {"type": "keyword"},
                        "equipmentCode": {"type": "keyword"},
                        "avgDelayTime": {"type": "double"},
                        "affectedVehicleCount": {"type": "integer"},
                        "riskScore": {"type": "double"},
                        "riskLevel": {"type": "keyword"},
                        "sourceService": {"type": "keyword"},
                    },
                },
            },
            settings.elasticsearch_defect_transfer_index: {
                "settings": {
                    "index": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0,
                    },
                },
                "mappings": {
                    "dynamic": True,
                    "properties": {
                        "analysisType": {"type": "keyword"},
                        "syncId": {"type": "keyword"},
                        "eventId": {"type": "keyword"},
                        "carMasterId": {"type": "long"},
                        "vehicleId": {"type": "keyword"},
                        "currentProcess": {"type": "keyword"},
                        "currentProcessCode": {"type": "keyword"},
                        "sourceProcessCode": {"type": "keyword"},
                        "sourceEquipmentCode": {"type": "keyword"},
                        "predictedDefectProcess": {"type": "keyword"},
                        "currentDefectProbability": {"type": "double"},
                        "transferProbability": {"type": "double"},
                        "defectThreshold": {"type": "double"},
                        "transferThreshold": {"type": "double"},
                        "defectProbability": {"type": "integer"},
                        "expectedTime": {"type": "keyword"},
                        "expectedStepsAfter": {"type": "integer"},
                        "riskLevel": {"type": "keyword"},
                        "predictedAt": {"type": "date"},
                        "createdAt": {"type": "date"},
                        "sourceService": {"type": "keyword"},
                        "mainCauses": {
                            "type": "nested",
                            "properties": {
                                "rank": {"type": "integer"},
                                "feature": {"type": "keyword"},
                                "label": {"type": "text"},
                                "value": {"type": "keyword"},
                                "impact": {"type": "double"},
                                "message": {"type": "text"},
                            },
                        },
                    },
                },
            },
        }

    @staticmethod
    def _normalize_datetime(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat()
        text = str(value).strip()
        return text or None

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_main_causes(value: Any) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            return []
        normalized: list[dict[str, Any]] = []
        for cause in value:
            if not isinstance(cause, dict):
                continue
            message = str(cause.get("message") or cause.get("label") or "").strip()
            if not message:
                continue
            normalized.append(
                {
                    "rank": ProcessAnalysisSearchRepository._safe_int(cause.get("rank")) or 0,
                    "feature": str(cause.get("feature") or ""),
                    "label": str(cause.get("label") or message),
                    "value": str(cause.get("value") or ""),
                    "impact": ProcessAnalysisSearchRepository._safe_float(cause.get("impact")) or 0.0,
                    "message": message,
                },
            )
        return normalized

    @staticmethod
    def _bottleneck_doc_id(snapshot_id: str, doc: dict[str, Any]) -> str:
        rank_no = doc.get("rankNo")
        process_code = str(doc.get("processCode") or "").strip().upper() or "UNKNOWN"
        equipment_code = str(doc.get("equipmentCode") or "").strip().upper() or "UNKNOWN"
        return f"{snapshot_id}:{rank_no}:{process_code}:{equipment_code}"

    @staticmethod
    def _map_bottleneck_source(source: dict[str, Any]) -> dict[str, Any]:
        return {
            "analysis_type": source.get("analysisType"),
            "sync_id": source.get("syncId"),
            "snapshot_id": source.get("snapshotId"),
            "detected_at": source.get("detectedAt"),
            "analyzed_at": source.get("analyzedAt"),
            "event_id": source.get("eventId"),
            "car_master_id": source.get("carMasterId"),
            "most_bottleneck_process": source.get("mostBottleneckProcess"),
            "most_bottleneck_risk_level": source.get("mostBottleneckRiskLevel"),
            "manufacturing_event_id": source.get("manufacturingEventId"),
            "process_code": source.get("processCode"),
            "equipment_code": source.get("equipmentCode"),
            "rank_no": source.get("rankNo"),
            "avg_delay_time": source.get("avgDelayTime"),
            "affected_vehicle_count": source.get("affectedVehicleCount"),
            "risk_score": source.get("riskScore"),
            "risk_level": source.get("riskLevel"),
            "source_service": source.get("sourceService"),
        }

    @staticmethod
    def _map_defect_source(source: dict[str, Any]) -> dict[str, Any]:
        current_defect_probability = source.get("currentDefectProbability")
        transfer_probability = source.get("transferProbability")
        defect_probability = source.get("defectProbability")
        if defect_probability is None and current_defect_probability is not None:
            defect_probability = round(float(current_defect_probability) * 100)
        return {
            "analysis_type": source.get("analysisType"),
            "sync_id": source.get("syncId"),
            "event_id": source.get("eventId"),
            "vehicle_id": source.get("vehicleId"),
            "car_master_id": source.get("carMasterId"),
            "current_process": source.get("currentProcess"),
            "current_process_code": source.get("currentProcessCode"),
            "source_process_code": source.get("sourceProcessCode") or source.get("currentProcessCode"),
            "source_equipment_code": source.get("sourceEquipmentCode"),
            "predicted_defect_process": source.get("predictedDefectProcess"),
            "target_defect_probability": transfer_probability,
            "defect_probability": defect_probability,
            "current_defect_probability": current_defect_probability,
            "transfer_probability": transfer_probability,
            "defect_threshold": source.get("defectThreshold"),
            "transfer_threshold": source.get("transferThreshold"),
            "expected_time": source.get("expectedTime"),
            "expected_steps_after": source.get("expectedStepsAfter"),
            "expected_occurrence_step": source.get("expectedStepsAfter"),
            "risk_level": source.get("riskLevel"),
            "risk_grade": source.get("riskLevel"),
            "predicted_at": source.get("predictedAt"),
            "created_at": source.get("createdAt"),
            "main_causes": source.get("mainCauses") or source.get("causes") or [],
            "causes": source.get("causes") or source.get("mainCauses") or [],
            "feature_values": source.get("featureValues") or {},
            "source_service": source.get("sourceService"),
            "influence_score": (
                float((source.get("mainCauses") or [{}])[0].get("impact") or 0.0)
                if isinstance(source.get("mainCauses"), list) and source.get("mainCauses")
                else 0.0
            ),
        }
