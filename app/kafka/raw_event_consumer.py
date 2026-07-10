# -*- coding: utf-8 -*-
from __future__ import annotations
import asyncio
import json
import logging
import ssl
from datetime import datetime
from threading import Event, Lock
from typing import Any
from uuid import uuid4
from fastapi import FastAPI
from app.core.config import settings
from app.kafka import config as kafka_config
from app.kafka.iam_provider import MSKTokenProvider
from app.ml.inference.defect_transfer_detector import (
    DefectTransferDetector,
    DefectTransferPrediction,
    has_only_model_probability_cause,
)
from app.repository.defect_transfer_prediction_repository import (
    DefectTransferPredictionRepository,
)
from app.repository.sampledb_schema import car_master
from app.repository.sampledb_repository import SampleDbRepository
from app.service.analysis.bottleneck_service import BottleneckAnalysisService
from app.utils.datetime_utils import seoul_now_iso
from app.websocket.analysis_manager import analysis_websocket_manager
logger = logging.getLogger(__name__)
PROCESS_SEQUENCE = ("PRESS", "BODY", "PAINT", "ASSEMBLY")
# assembly-service의 app.kafka.topics.raw.name과 동일한 raw 토픽.
RAW_TOPIC = kafka_config.RAW_TOPIC
ANALYSIS_TOPIC = kafka_config.ANALYSIS_TOPIC
ANALYSIS_SYNC_TOPIC = kafka_config.ANALYSIS_SYNC_TOPIC
RAW_CONSUMER_GROUP_ID = kafka_config.RAW_CONSUMER_GROUP_ID
RAW_AUTO_OFFSET_RESET = kafka_config.AUTO_OFFSET_RESET
RAW_CONSUMER_CONCURRENCY = kafka_config.RAW_CONSUMER_CONCURRENCY
_bottleneck_analysis_lock = Lock()


def start_raw_event_consumer(app: FastAPI) -> None:
    """FastAPI startup 시 raw Kafka consumer를 백그라운드 thread로 실행한다."""
    bootstrap_servers = _bootstrap_servers()
    if not bootstrap_servers:
        logger.info("Kafka bootstrap servers are not set. Raw Kafka consumer is disabled.")
        return
    if not settings.sample_database_connection_url:
        logger.info("SAMPLE_DB_NAME is not set. Raw Kafka consumer is disabled.")
        return
    stop_event = Event()
    tasks = [
        asyncio.create_task(
            asyncio.to_thread(
                _run_raw_event_consumer,
                stop_event,
                bootstrap_servers,
                consumer_index,
            ),
        )
        for consumer_index in range(1, RAW_CONSUMER_CONCURRENCY + 1)
    ]
    # shutdown 시 백그라운드 thread consumer loop를 종료하기 위한 상태를 저장한다.
    app.state.raw_event_consumer_stop_event = stop_event
    app.state.raw_event_consumer_tasks = tasks


async def stop_raw_event_consumer(app: FastAPI) -> None:
    """FastAPI shutdown 시 consumer loop 종료 신호를 보내고 thread 작업을 정리한다."""
    tasks = getattr(app.state, "raw_event_consumer_tasks", None)
    if not tasks:
        return
    stop_event = getattr(app.state, "raw_event_consumer_stop_event", None)
    if stop_event is not None:
        stop_event.set()
    await asyncio.gather(*tasks, return_exceptions=True)


def _run_raw_event_consumer(
    stop_event: Event,
    bootstrap_servers: list[str],
    consumer_index: int,
) -> None:
    try:
        from kafka import KafkaConsumer, KafkaProducer
        from kafka.errors import CommitFailedError
    except ModuleNotFoundError:
        logger.exception("kafka-python is required to consume manufacturing raw events.")
        return
    # 운영 Kafka 연결에서는 MSK IAM 인증(SASL_SSL/OAUTHBEARER)을 사용한다.
    consumer = KafkaConsumer(
        RAW_TOPIC,
        ssl_context=ssl.create_default_context(),
        bootstrap_servers=bootstrap_servers,
        group_id=RAW_CONSUMER_GROUP_ID,
        auto_offset_reset=RAW_AUTO_OFFSET_RESET,
        enable_auto_commit=False,
        max_poll_interval_ms=kafka_config.MAX_POLL_INTERVAL_MS,
        session_timeout_ms=kafka_config.SESSION_TIMEOUT_MS,
        heartbeat_interval_ms=kafka_config.HEARTBEAT_INTERVAL_MS,
        max_poll_records=kafka_config.MAX_POLL_RECORDS,
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=MSKTokenProvider(),
        consumer_timeout_ms=kafka_config.CONSUMER_TIMEOUT_MS,
    )
    repository = SampleDbRepository(settings.sample_database_connection_url)
    repository.schema.ensure_schema()
    analysis_service = _create_bottleneck_analysis_service()
    defect_detector = _create_defect_transfer_detector()
    defect_result_repository = _create_defect_transfer_result_repository()
    analysis_producer = _create_analysis_producer(
        KafkaProducer,
        bootstrap_servers,
    )
    logger.info(
        "Raw Kafka consumer started: topic=%s group=%s concurrency=%s/%s bootstrap=%s auth=SASL_SSL/OAUTHBEARER",
        RAW_TOPIC,
        RAW_CONSUMER_GROUP_ID,
        consumer_index,
        RAW_CONSUMER_CONCURRENCY,
        bootstrap_servers,
    )
    try:
        while not stop_event.is_set():
            for record in consumer:
                if stop_event.is_set():
                    break
                try:
                    _consume_record(
                        repository,
                        analysis_service,
                        defect_detector,
                        defect_result_repository,
                        analysis_producer,
                        record,
                    )
                except Exception:
                    logger.exception(
                        "Failed to consume raw event: topic=%s partition=%s offset=%s",
                        record.topic,
                        record.partition,
                        record.offset,
                    )
                finally:
                    # DB 저장/분석 실패 여부와 관계없이 offset을 커밋해 같은 메시지의 무한 재처리를 막는다.
                    try:
                        consumer.commit()
                    except CommitFailedError:
                        logger.warning(
                            "Offset commit skipped because the consumer group was rebalanced: "
                            "topic=%s partition=%s offset=%s consumer_index=%s",
                            record.topic,
                            record.partition,
                            record.offset,
                            consumer_index,
                        )
    except Exception:
        logger.exception("Raw Kafka consumer failed.")
    finally:
        if analysis_producer is not None:
            analysis_producer.close(timeout=5)
        consumer.close()
        logger.info(
            "Raw Kafka consumer stopped: topic=%s group=%s concurrency=%s/%s",
            RAW_TOPIC,
            RAW_CONSUMER_GROUP_ID,
            consumer_index,
            RAW_CONSUMER_CONCURRENCY,
        )


def _consume_record(
    repository: SampleDbRepository,
    analysis_service: BottleneckAnalysisService | None,
    defect_detector: DefectTransferDetector | None,
    defect_result_repository: DefectTransferPredictionRepository | None,
    analysis_producer: Any | None,
    record: Any,
) -> None:
    """Kafka raw 메시지를 manufacturing_event_json row로 변환해 sampledb에 upsert한다."""
    raw_event = _parse_raw_event(record)
    row = _raw_event_to_row(raw_event)
    affected_rows = repository.events.insert_raw_rows(
        [row],
        update_existing=True,
    )
    if not _should_analyze_row(row):
        logger.info(
            "Kafka raw event skipped because dispatch_status/is_sent do not match analysis condition: "
            "topic=%s partition=%s offset=%s key=%s event_id=%s dispatch_status=%s is_sent=%s",
            record.topic,
            record.partition,
            record.offset,
            raw_event.get("_kafka_key"),
            row["event_id"],
            row.get("dispatch_status"),
            row.get("is_sent"),
        )
        return
    bottleneck_summaries: list[dict[str, Any]] = []
    defect_prediction: DefectTransferPrediction | None = None
    defect_rows_saved = 0
    analysis_ran = False
    if not repository.events.is_bottleneck_analysis_done(row["event_id"]):
        bottleneck_summaries = _refresh_bottleneck_analysis(
            analysis_service,
            record,
            row,
        )
        if bottleneck_summaries is not None:
            repository.events.mark_bottleneck_analysis_done(row["event_id"])
            analysis_ran = True
    if not repository.events.is_defect_transfer_analysis_done(row["event_id"]):
        if (
            defect_result_repository is not None
            and defect_result_repository.has_prediction_for_event(row["event_id"])
        ):
            repository.events.mark_defect_transfer_analysis_done(row["event_id"])
        else:
            defect_prediction = _predict_defect_transfer(defect_detector, row)
            if (
                defect_prediction is not None
                and has_only_model_probability_cause(defect_prediction.causes)
            ):
                logger.info(
                    "Skipped defect transfer storage and analysis publish because only fallback model probability cause was produced: "
                    "topic=%s partition=%s offset=%s key=%s event_id=%s process_code=%s",
                    record.topic,
                    record.partition,
                    record.offset,
                    raw_event.get("_kafka_key"),
                    row["event_id"],
                    row["process_code"],
                )
                return
            defect_rows_saved = _save_defect_transfer_prediction(
                defect_result_repository,
                row,
                defect_prediction,
            )
            if defect_rows_saved > 0:
                repository.events.mark_defect_transfer_analysis_done(row["event_id"])
                analysis_ran = True
    if not analysis_ran:
        logger.info(
            "Kafka raw event skipped because it was already analyzed: "
            "topic=%s partition=%s offset=%s key=%s event_id=%s",
            record.topic,
            record.partition,
            record.offset,
            raw_event.get("_kafka_key"),
            row["event_id"],
        )
        return
    analysis_published = _publish_bottleneck_analysis_event(
        analysis_producer,
        raw_event,
        row,
        bottleneck_summaries,
        defect_prediction,
    )
    sync_published = _publish_process_analysis_sync_events(
        analysis_producer,
        repository,
        raw_event,
        row,
        bottleneck_summaries,
        defect_prediction,
        defect_rows_saved,
    )
    # raw 이벤트 연동 분석 결과가 반영되면 Redis 캐시를 비운다.
    _clear_bottleneck_cache()
    _clear_defect_transfer_cache()
    _broadcast_process_analysis_updates(
        repository,
        row,
        bottleneck_summaries,
        defect_prediction,
        defect_rows_saved,
    )
    logger.info(
        "Kafka raw event consumed and stored: topic=%s partition=%s offset=%s "
        "key=%s event_id=%s manufacturing_event_id_source=manufacturing_event_json.id "
        "car_master_id=%s process_code=%s affected_rows=%s "
        "bottleneck_result_count=%s defect_result_saved=%s analysis_topic_published=%s "
        "analysis_sync_published=%s",
        record.topic,
        record.partition,
        record.offset,
        raw_event.get("_kafka_key"),
        row["event_id"],
        row["car_master_id"],
        row["process_code"],
        affected_rows,
        len(bottleneck_summaries),
        defect_rows_saved,
        analysis_published,
        sync_published,
    )


def _broadcast_process_analysis_updates(
    repository: SampleDbRepository,
    row: dict[str, Any],
    bottleneck_summaries: list[dict[str, Any]],
    defect_prediction: DefectTransferPrediction | None,
    defect_rows_saved: int,
) -> None:
    vehicle_id = _vehicle_id_for_car_master_id(repository, int(row["car_master_id"]))
    base_message = {
        "eventId": row["event_id"],
        "carMasterId": row["car_master_id"],
        "vehicleId": vehicle_id,
        "processCode": row["process_code"],
        "updatedAt": seoul_now_iso(),
    }
    analysis_websocket_manager.broadcast_from_thread(
        {
            **base_message,
            "type": "BOTTLENECK_UPDATED",
            "resultCount": len(bottleneck_summaries),
        },
    )
    analysis_websocket_manager.broadcast_from_thread(
        {
            **base_message,
            "type": "DEFECT_TRANSFER_UPDATED",
            "defectProbability": (
                round(defect_prediction.defect_probability * 100.0, 1)
                if defect_prediction is not None
                else None
            ),
            "transferProbability": (
                round(defect_prediction.transfer_probability * 100.0, 1)
                if defect_prediction is not None
                and defect_prediction.transfer_probability is not None
                else None
            ),
            "riskGrade": (
                defect_prediction.risk_level
                if defect_prediction is not None
                else None
            ),
            "predictedDefectProcess": (
                _format_predicted_defect_process(
                    defect_prediction.predicted_process_code,
                    row,
                )
                if defect_prediction is not None
                else None
            ),
            "resultSaved": defect_rows_saved,
        },
    )


def _vehicle_id_for_car_master_id(
    repository: SampleDbRepository,
    car_master_id: int,
) -> str | None:
    from sqlalchemy import select
    try:
        query = select(car_master.c.vehicle_id).where(car_master.c.id == car_master_id)
        with repository.engine.connect() as conn:
            value = conn.execute(query).scalar()
        return str(value) if value is not None else None
    except Exception:
        logger.exception(
            "Failed to resolve vehicle_id for websocket update: car_master_id=%s",
            car_master_id,
        )
        return None


def _parse_raw_event(record: Any) -> dict[str, Any]:
    """Kafka record의 value JSON과 key(carId)를 파싱해 dict로 반환한다."""
    payload = json.loads(record.value.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Manufacturing raw event must be a JSON object.")
    event_json = _parse_event_json(payload.get("eventJson") or payload.get("event_json"))
    if event_json is None:
        raise ValueError("Manufacturing raw event must include eventJson object.")
    key_text = record.key.decode("utf-8") if record.key else None
    payload["eventJson"] = event_json
    payload["_kafka_key"] = key_text
    return payload


def _raw_event_to_row(raw_event: dict[str, Any]) -> dict[str, Any]:
    """assembly-service raw envelope를 manufacturing_event_json 저장 형식으로 변환한다."""
    event_json = raw_event["eventJson"]
    process_code = _normalize_process_code(
        _first_present(raw_event, "processCode", "process_code", "PROCESS_CODE"),
        event_json,
    )
    event_id = (
        _first_present(raw_event, "eventId", "event_id")
        or _nested_text(event_json, "event", "eventId")
    )
    if not event_id:
        raise ValueError("Manufacturing raw event requires eventId.")
    car_master_id = _nullable_int(
        _first_present(raw_event, "carMasterId", "car_master_id", "carId", "car_id"),
    )
    if car_master_id is None:
        car_master_id = _nullable_int(_nested_value(event_json, "product", "carMasterId"))
    if car_master_id is None:
        # raw 토픽 key가 carId이므로 payload에 차량 ID가 없을 때 key를 최종 fallback으로 사용한다.
        car_master_id = _nullable_int(raw_event.get("_kafka_key"))
    if car_master_id is None:
        raise ValueError(f"Manufacturing raw event requires carId key or carMasterId. event_id={event_id}")
    equipment_id = _nullable_int(
        _first_present(raw_event, "equipmentId", "equipment_id"),
    ) or 0
    return {
        "event_id": str(event_id),
        "event_time": _parse_datetime(
            _first_present(raw_event, "eventTime", "event_time")
            or _nested_text(event_json, "event", "eventTime"),
        ),
        "car_master_id": car_master_id,
        "process_code": process_code,
        "equipment_id": equipment_id,
        "event_json": event_json,
        "dispatch_status": "SENT",
        "is_sent": True,
    }


def _normalize_process_code(value: Any, event_json: dict[str, Any]) -> str:
    """PRESS/BODY/PAINT/ASSEMBLY 중 하나의 공정 코드로 정규화한다."""
    process_code = str(value or "").strip().upper()
    if not process_code:
        process_data = event_json.get("processData")
        if isinstance(process_data, dict):
            process_code = next(iter(process_data.keys()), "").upper()
    if process_code not in PROCESS_SEQUENCE:
        raise ValueError(f"Unsupported processCode for raw event: {value}")
    return process_code


def _parse_event_json(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return None
        return decoded if isinstance(decoded, dict) else None
    return None


def _first_present(payload: dict[str, Any], *fields: str) -> Any:
    for field in fields:
        value = payload.get(field)
        if value is not None:
            return value
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        logger.warning("Cannot parse raw event time: %s", text)
        return None


def _nested_value(payload: dict[str, Any], *path: str) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _nested_text(payload: dict[str, Any], *path: str) -> str | None:
    value = _nested_value(payload, *path)
    return str(value) if value is not None else None


def _nullable_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bootstrap_servers() -> list[str]:
    """환경변수 BROKER_URL_1/2에서 MSK bootstrap 서버 목록을 만든다."""
    servers = [settings.broker_url_1 or "", settings.broker_url_2 or ""]
    return [server.strip() for server in servers if server.strip()]


def _create_bottleneck_analysis_service() -> BottleneckAnalysisService | None:
    try:
        return BottleneckAnalysisService()
    except Exception:
        logger.exception(
            "Bottleneck analysis service is unavailable. "
            "Raw Kafka events will still be stored.",
        )
        return None


def _create_defect_transfer_detector() -> DefectTransferDetector | None:
    try:
        return DefectTransferDetector()
    except Exception:
        logger.exception(
            "Defect transfer detector is unavailable. "
            "Raw Kafka events will still be stored.",
        )
        return None


def _create_defect_transfer_result_repository() -> DefectTransferPredictionRepository | None:
    if not settings.sample_database_connection_url:
        return None
    try:
        return DefectTransferPredictionRepository(
            settings.main_database_connection_url,
            event_database_url=settings.sample_database_connection_url,
        )
    except Exception:
        logger.exception(
            "Defect transfer result repository is unavailable. "
            "Raw Kafka events will still be stored.",
        )
        return None


def _create_analysis_producer(
    producer_cls: Any,
    bootstrap_servers: list[str],
) -> Any | None:
    try:
        producer = producer_cls(
            ssl_context=ssl.create_default_context(),
            bootstrap_servers=bootstrap_servers,
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
            sasl_oauth_token_provider=MSKTokenProvider(),
            key_serializer=lambda value: str(value).encode("utf-8"),
            value_serializer=lambda value: json.dumps(
                value,
                ensure_ascii=False,
                default=_json_default,
            ).encode("utf-8"),
            retries=kafka_config.PRODUCER_RETRIES,
            linger_ms=kafka_config.PRODUCER_LINGER_MS,
        )
    except Exception:
        logger.exception(
            "Kafka analysis producer is unavailable. "
            "Bottleneck results will still be stored in DB.",
        )
        return None
    logger.info(
        "Kafka analysis producer started: topic=%s bootstrap=%s auth=SASL_SSL/OAUTHBEARER",
        ANALYSIS_TOPIC,
        bootstrap_servers,
    )
    return producer


def _refresh_bottleneck_analysis(
    analysis_service: BottleneckAnalysisService | None,
    record: Any,
    row: dict[str, Any],
) -> list[dict[str, Any]]:
    if analysis_service is None:
        return []
    with _bottleneck_analysis_lock:
        try:
            summaries = analysis_service.refresh_results_from_events()
        except Exception:
            logger.exception(
                "Failed to refresh bottleneck analysis after raw event: "
                "topic=%s partition=%s offset=%s event_id=%s process_code=%s",
                record.topic,
                record.partition,
                record.offset,
                row.get("event_id"),
                row.get("process_code"),
            )
            return []
    logger.info(
        "Bottleneck analysis refreshed after raw Kafka event: "
        "topic=%s partition=%s offset=%s event_id=%s process_code=%s result_count=%s",
        record.topic,
        record.partition,
        record.offset,
        row.get("event_id"),
        row.get("process_code"),
        len(summaries),
    )
    return summaries or []


def _publish_bottleneck_analysis_event(
    producer: Any | None,
    raw_event: dict[str, Any],
    row: dict[str, Any],
    summaries: list[dict[str, Any]],
    defect_prediction: DefectTransferPrediction | None,
) -> bool:
    if producer is None:
        return False
    event = _build_bottleneck_analysis_event(
        raw_event,
        row,
        summaries,
        defect_prediction,
    )
    key = str(row["car_master_id"])
    try:
        result = producer.send(ANALYSIS_TOPIC, key=key, value=event).get(timeout=10)
    except Exception:
        logger.exception(
            "Failed to publish bottleneck analysis event: topic=%s key=%s event_id=%s",
            ANALYSIS_TOPIC,
            key,
            row.get("event_id"),
        )
        return False
    logger.info(
        "Bottleneck analysis event published: topic=%s partition=%s offset=%s "
        "key=%s event_id=%s analysis_id=%s analysis_type=%s",
        result.topic,
        result.partition,
        result.offset,
        key,
        row.get("event_id"),
        event["analysisId"],
        event["analysisType"],
    )
    return True


def _publish_process_analysis_sync_events(
    producer: Any | None,
    repository: SampleDbRepository,
    raw_event: dict[str, Any],
    row: dict[str, Any],
    bottleneck_summaries: list[dict[str, Any]],
    defect_prediction: DefectTransferPrediction | None,
    defect_rows_saved: int,
) -> bool:
    if producer is None:
        return False

    published = False
    if bottleneck_summaries:
        bottleneck_event = _build_bottleneck_sync_event(
            repository=repository,
            raw_event=raw_event,
            row=row,
            summaries=bottleneck_summaries,
        )
        try:
            producer.send(
                ANALYSIS_SYNC_TOPIC,
                key=f"bottleneck:{row['event_id']}",
                value=bottleneck_event,
            ).get(timeout=10)
            published = True
        except Exception:
            logger.exception(
                "Failed to publish bottleneck sync event: topic=%s event_id=%s",
                ANALYSIS_SYNC_TOPIC,
                row.get("event_id"),
            )

    if defect_prediction is not None and defect_rows_saved > 0:
        defect_event = _build_defect_transfer_sync_event(
            repository=repository,
            raw_event=raw_event,
            row=row,
            prediction=defect_prediction,
        )
        try:
            producer.send(
                ANALYSIS_SYNC_TOPIC,
                key=f"defect:{row['event_id']}",
                value=defect_event,
            ).get(timeout=10)
            published = True
        except Exception:
            logger.exception(
                "Failed to publish defect transfer sync event: topic=%s event_id=%s",
                ANALYSIS_SYNC_TOPIC,
                row.get("event_id"),
            )

    return published


def _build_bottleneck_sync_event(
    *,
    repository: SampleDbRepository,
    raw_event: dict[str, Any],
    row: dict[str, Any],
    summaries: list[dict[str, Any]],
) -> dict[str, Any]:
    sync_id = f"SNAP-{uuid4()}"
    detected_at = seoul_now_iso()
    first_summary = summaries[0] if summaries else {}
    return {
        "syncId": sync_id,
        "analysisType": "BOTTLENECK_ANALYSIS_SYNC",
        "sourceService": "AI_SERVICE",
        "detectedAt": detected_at,
        "analyzedAt": detected_at,
        "eventId": row["event_id"],
        "carMasterId": row["car_master_id"],
        "mostBottleneckProcess": first_summary.get("process_code"),
        "mostBottleneckRiskLevel": _risk_level(float(first_summary.get("risk_score") or 0.0)),
        "items": [
            {
                "manufacturingEventId": summary.get("manufacturing_event_id"),
                "carMasterId": summary.get("car_master_id") or row["car_master_id"],
                "processCode": summary.get("process_code"),
                "equipmentCode": summary.get("equipment_code"),
                "rankNo": summary.get("rank_no"),
                "avgDelayTime": summary.get("avg_delay_time"),
                "affectedVehicleCount": summary.get("affected_vehicle_count"),
                "riskScore": summary.get("risk_score"),
                "riskLevel": _risk_level(float(summary.get("risk_score") or 0.0)),
            }
            for summary in summaries
        ],
        "rawEvent": {
            "eventId": row["event_id"],
            "carMasterId": row["car_master_id"],
            "processCode": row["process_code"],
            "equipmentId": row.get("equipment_id"),
            "vehicleId": _vehicle_id_for_car_master_id(repository, int(row["car_master_id"])),
        },
    }


def _build_defect_transfer_sync_event(
    *,
    repository: SampleDbRepository,
    raw_event: dict[str, Any],
    row: dict[str, Any],
    prediction: DefectTransferPrediction,
) -> dict[str, Any]:
    sync_id = f"SYNC-{uuid4()}"
    predicted_at = seoul_now_iso()
    vehicle_id = _vehicle_id_for_car_master_id(repository, int(row["car_master_id"]))
    source_equipment_code = _source_equipment_code(row)
    current_process = _format_current_process(row, source_equipment_code)
    predicted_process = _format_predicted_defect_process(
        prediction.predicted_process_code,
        row,
    )
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
        "predictedDefectProcess": predicted_process,
        "defectProbability": round(prediction.defect_probability * 100.0),
        "currentDefectProbability": prediction.defect_probability,
        "transferProbability": prediction.transfer_probability,
        "defectThreshold": prediction.defect_threshold,
        "transferThreshold": prediction.transfer_threshold,
        "expectedStepsAfter": prediction.expected_steps_after,
        "expectedTime": (
            f"{prediction.expected_steps_after}단계 후"
            if prediction.expected_steps_after is not None
            else None
        ),
        "riskLevel": prediction.risk_level,
        "predictedAt": predicted_at,
        "createdAt": predicted_at,
        "mainCauses": causes,
        "causes": causes,
        "featureValues": prediction.feature_values,
        "rawEvent": {
            "eventId": row["event_id"],
            "carMasterId": row["car_master_id"],
            "processCode": row["process_code"],
            "equipmentId": row.get("equipment_id"),
            "vehicleId": vehicle_id,
        },
    }


def _build_bottleneck_analysis_event(
    raw_event: dict[str, Any],
    row: dict[str, Any],
    summaries: list[dict[str, Any]],
    defect_prediction: DefectTransferPrediction | None = None,
) -> dict[str, Any]:
    event_json = row["event_json"]
    equipment = event_json.get("equipment", {})
    product = event_json.get("product", {})
    metrics = event_json.get("processMetrics", {})
    equipment_status = event_json.get("equipmentStatus", {})
    equipment_code = str(
        equipment.get("equipmentCode")
        or row.get("equipment_id")
        or "UNKNOWN",
    )
    process_code = str(row["process_code"])
    summary = _matching_bottleneck_summary(
        summaries,
        process_code=process_code,
        equipment_code=equipment_code,
    )
    raw_delay_time = _safe_float(
        metrics.get("stationDelaySec"),
        default=_safe_float(metrics.get("waitingTimeSec"), default=0.0),
    )
    bottleneck_delay_time = _safe_float(
        summary.get("avg_delay_time") if summary else None,
        default=raw_delay_time,
    )
    risk_score = _safe_float(
        summary.get("risk_score") if summary else None,
        default=0.0,
    )
    overall_risk_score = _risk_score_to_percent(risk_score)
    risk_level = _risk_level(overall_risk_score)
    is_bottleneck = summary is not None and risk_score >= 3
    if defect_prediction is None:
        defect_probability = _defect_probability(event_json, process_code)
        transfer_predicted_process = _transfer_predicted_process(
            process_code,
            defect_probability,
        )
        transfer_probability = None
        defect_causes: list[dict[str, Any]] = []
        is_quality_defect = defect_probability >= 0.6
    else:
        defect_probability = defect_prediction.defect_probability
        transfer_predicted_process = defect_prediction.predicted_process_code
        transfer_probability = defect_prediction.transfer_probability
        defect_causes = [
            {
                "rank": cause.rank,
                "feature": cause.feature,
                "label": cause.label,
                "value": cause.value,
                "impact": cause.impact,
                "message": cause.message,
            }
            for cause in defect_prediction.causes
        ]
        is_quality_defect = defect_prediction.is_quality_defect
    is_equipment_fault = str(
        equipment_status.get("operationStatus") or "",
    ).upper() in {"FAULT", "STOPPED", "ERROR", "DOWN"}
    is_sequence_error = (
        process_code == "ASSEMBLY"
        and _safe_float(
            _nested_value(event_json, "processData", "assembly", "sequenceErrorCount"),
            default=0.0,
        )
        > 0
    )
    return {
        "analysisId": f"ANL-{uuid4()}",
        "eventId": row["event_id"],
        "eventTime": _iso_or_none(row.get("event_time"))
        or _nested_text(event_json, "event", "eventTime"),
        "analyzedAt": seoul_now_iso(),
        "factoryCode": _nested_text(event_json, "location", "factoryCode"),
        "lineCode": _nested_text(event_json, "location", "lineCode"),
        "processCode": process_code,
        "equipmentId": row.get("equipment_id"),
        "equipmentCode": equipment_code,
        "equipmentName": equipment.get("equipmentName"),
        "equipmentType": equipment.get("equipmentType"),
        "productId": product.get("productId"),
        "carId": raw_event.get("_kafka_key") or product.get("carId") or row["car_master_id"],
        "carMasterId": row["car_master_id"],
        "analysisType": "BOTTLENECK_ANALYSIS",
        "sourceService": "AI_SERVICE",
        "riskScore": risk_score,
        "riskScoreScale": "1-5",
        "riskScores": {
            "overallRiskScore": overall_risk_score,
            "bottleneckRiskScore": overall_risk_score,
            "defectTransferRiskScore": round(defect_probability * 100.0, 1),
            "equipmentRiskScore": None,
            "processRisk": {
                "pressRiskScore": overall_risk_score if process_code == "PRESS" else None,
                "bodyRiskScore": overall_risk_score if process_code == "BODY" else None,
                "paintRiskScore": overall_risk_score if process_code == "PAINT" else None,
                "assemblyRiskScore": (
                    overall_risk_score if process_code == "ASSEMBLY" else None
                ),
            },
        },
        "operationRate": _operation_rate(metrics),
        "riskLevel": risk_level,
        "analysisResult": {
            "isAbnormal": (
                is_bottleneck
                or is_quality_defect
                or is_equipment_fault
                or is_sequence_error
            ),
            "isBottleneck": is_bottleneck,
            "isQualityDefect": is_quality_defect,
            "isEquipmentFault": is_equipment_fault,
            "isSequenceError": is_sequence_error,
        },
        "reason": {
            "mainReason": _bottleneck_reason(
                risk_score=risk_score,
                delay_time=bottleneck_delay_time,
                is_equipment_fault=is_equipment_fault,
                is_sequence_error=is_sequence_error,
            ),
            "detailReasons": [
                f"processCode={process_code}",
                f"equipmentCode={equipment_code}",
                f"bottleneckDelayTime={round(bottleneck_delay_time, 3)}",
                f"riskScore={risk_score}",
            ],
        },
        "recommendation": {
            "type": _recommendation_type(process_code),
            "message": _recommendation_message(process_code),
        },
        "manufacturingAnalysisData": {
            "originalEventId": row["event_id"],
            "carMasterId": row["car_master_id"],
            "equipmentId": row.get("equipment_id"),
            "processCode": process_code,
            "analysisData": {
                "cycleTimeSec": metrics.get("cycleTimeSec"),
                "waitingTimeSec": metrics.get("waitingTimeSec"),
                "processingTimeSec": metrics.get("processingTimeSec"),
                "stationDelaySec": metrics.get("stationDelaySec"),
                "queueLength": metrics.get("queueLength"),
                "wipCount": metrics.get("wipCount"),
            },
            "riskScore": risk_score,
        },
        "aiAnalysisData": {
            "originalEventId": row["event_id"],
            "carMasterId": row["car_master_id"],
            "equipmentId": row.get("equipment_id"),
            "processCode": process_code,
            "bottleneckDelayTime": bottleneck_delay_time,
            "defectProbability": defect_probability,
            "transferProbability": transfer_probability,
            "transferPredictedProcess": transfer_predicted_process,
            "defectCauses": defect_causes,
            "riskScore": risk_score,
        },
    }


def _predict_defect_transfer(
    detector: DefectTransferDetector | None,
    row: dict[str, Any],
) -> DefectTransferPrediction | None:
    if detector is None:
        return None
    try:
        return detector.predict_event(row["event_json"], str(row["process_code"]))
    except Exception:
        logger.exception(
            "Failed to run defect transfer prediction: event_id=%s process_code=%s",
            row.get("event_id"),
            row.get("process_code"),
        )
        return None


def _save_defect_transfer_prediction(
    repository: DefectTransferPredictionRepository | None,
    row: dict[str, Any],
    prediction: DefectTransferPrediction | None,
) -> int:
    if repository is None:
        logger.warning(
            "Defect transfer prediction was not saved because result repository is unavailable: "
            "event_id=%s process_code=%s",
            row.get("event_id"),
            row.get("process_code"),
        )
        return 0
    if prediction is None:
        logger.warning(
            "Defect transfer prediction was not saved because prediction is unavailable: "
            "event_id=%s process_code=%s",
            row.get("event_id"),
            row.get("process_code"),
        )
        return 0
    try:
        causes = [
            {
                "message": cause.message,
                "label": cause.label,
                "impact": cause.impact,
            }
            for cause in prediction.causes
        ]
        return repository.replace_prediction_result(
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
            causes=causes,
            predicted_at=datetime.now(),
        )
    except Exception:
        logger.exception(
            "Failed to save defect transfer prediction result: event_id=%s process_code=%s",
            row.get("event_id"),
            row.get("process_code"),
        )
        return 0


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
        equipment_code = _source_equipment_code(row)
    else:
        equipment_code = equipment_code_for_car_process(
            car_master_id=int(row["car_master_id"]),
            process_code=normalized,
        )
    return format_process_with_line(normalized, equipment_code)


def _format_current_process(
    row: dict[str, Any],
    equipment_code: str | None,
) -> str:
    from app.utils.process_label_utils import format_process_with_line

    return format_process_with_line(row.get("process_code"), equipment_code)


def _should_analyze_row(row: dict[str, Any]) -> bool:
    return str(row.get("dispatch_status") or "").upper() == "SENT" and _analysis_flag_is_true(
        row.get("is_sent"),
    )


def _analysis_flag_is_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() == "true"


def _source_equipment_code(row: dict[str, Any]) -> str | None:
    event_json = row.get("event_json") or {}
    equipment = event_json.get("equipment", {}) if isinstance(event_json, dict) else {}
    equipment_code = str(equipment.get("equipmentCode") or row.get("equipment_id") or "")
    return equipment_code or None


def _matching_bottleneck_summary(
    summaries: list[dict[str, Any]],
    *,
    process_code: str,
    equipment_code: str,
) -> dict[str, Any] | None:
    for summary in summaries or []:
        if (
            str(summary.get("process_code")) == process_code
            and str(summary.get("equipment_code")) == equipment_code
        ):
            return summary
    return None


def _risk_score_to_percent(risk_score: float) -> float:
    return round(max(0.0, min(risk_score, 5.0)) * 20.0, 1)


def _risk_level(score: float) -> str:
    if score >= 80:
        return "CRITICAL"
    if score >= 60:
        return "WARNING"
    return "LOW"


def _operation_rate(metrics: dict[str, Any]) -> float:
    cycle_time = _safe_float(metrics.get("cycleTimeSec"), default=0.0)
    processing_time = _safe_float(metrics.get("processingTimeSec"), default=0.0)
    idle_time = _safe_float(metrics.get("equipmentIdleTimeSec"), default=0.0)
    planned_time = cycle_time if cycle_time > 0 else processing_time + idle_time
    if planned_time <= 0:
        return 0.0
    return round(max(0.0, min(processing_time / planned_time * 100.0, 100.0)), 1)


def _defect_probability(event_json: dict[str, Any], process_code: str) -> float:
    process_data = event_json.get("processData", {})
    sensor = event_json.get("sensor", {})
    vibration = sensor.get("vibration", {})
    robot = sensor.get("robotArmVibration", {})
    thermal = sensor.get("thermal", {})
    if process_code == "PAINT":
        paint = process_data.get("paint", {})
        defect_score = _safe_float(paint.get("defectScore"), default=0.0)
        if str(paint.get("visionLabel") or "").upper() == "DEFECT":
            defect_score = max(defect_score, 0.75)
        surface_quality = _safe_float(
            paint.get("surfaceQualityScore"),
            default=100.0,
        )
        return round(max(defect_score, max(0.0, 100.0 - surface_quality) / 100.0), 4)
    if process_code == "ASSEMBLY":
        assembly = process_data.get("assembly", {})
        error_count = (
            _safe_float(assembly.get("sequenceErrorCount"), default=0.0)
            + _safe_float(assembly.get("missingPartCount"), default=0.0)
            + _safe_float(assembly.get("fasteningErrorCount"), default=0.0)
        )
        if error_count == 0:
            return 0.0
        # 오류 건수에 따라 불량 확률을 현실적으로 조정한다.
        # 오류 1건당 약 8%를 추가하고, 기본 50% 위험도를 부여해 최대 98% 내외로 산출한다.
        return round(min(error_count * 0.08 + 0.50, 0.98), 4)
    vibration_score = max(
        _safe_float(vibration.get("vibrationScore"), default=0.0),
        _safe_float(robot.get("vibrationScore"), default=0.0),
    )
    thermal_score = max(
        _safe_float(thermal.get("thermalScore"), default=0.0) / 100.0,
        _safe_float(thermal.get("maxTemperature"), default=0.0) / 100.0,
    )
    return round(min(max(vibration_score, thermal_score), 1.0), 4)


def _transfer_predicted_process(
    process_code: str,
    defect_probability: float,
) -> str | None:
    if defect_probability < 0.5:
        return None
    return {
        "PRESS": "BODY",
        "BODY": "PAINT",
        "PAINT": "ASSEMBLY",
        "ASSEMBLY": None,
    }.get(process_code)


def _bottleneck_reason(
    *,
    risk_score: float,
    delay_time: float,
    is_equipment_fault: bool,
    is_sequence_error: bool,
) -> str:
    if is_equipment_fault:
        return "설비 상태값에서 고장 또는 정지 위험이 감지되었습니다."
    if is_sequence_error:
        return "의장 공정에서 작업 순서 오류가 감지되었습니다."
    if risk_score >= 3:
        return "Rule Engine과 Isolation Forest 기준으로 병목 위험이 감지되었습니다."
    if delay_time > 0:
        return "공정 지연 시간이 감지되었지만 위험도는 낮습니다."
    return "주요 병목 지표가 정상 범위입니다."


def _recommendation_type(process_code: str) -> str:
    return {
        "PRESS": "CHECK_PRESS_EQUIPMENT_AND_QUEUE",
        "BODY": "CHECK_ROBOT_VIBRATION",
        "PAINT": "CHECK_PAINT_QUALITY",
        "ASSEMBLY": "CHECK_ASSEMBLY_SEQUENCE",
    }.get(process_code, "CHECK_PROCESS")


def _recommendation_message(process_code: str) -> str:
    return {
        "PRESS": "프레스 설비 상태, 전류 RMS 값과 대기열을 확인하세요.",
        "BODY": "로봇 암 진동과 충돌 위험을 확인하세요.",
        "PAINT": "열화상, 도막 두께와 비전 불량 결과를 확인하세요.",
        "ASSEMBLY": "작업 순서, 누락 부품과 체결 오류를 확인하세요.",
    }.get(process_code, "공정 지표와 설비 상태를 확인하세요.")


def _safe_float(value: Any, *, default: float) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _iso_or_none(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value) if value is not None else None


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _clear_bottleneck_cache() -> None:
    if not settings.redis_url:
        return
    try:
        from redis import Redis
        redis_client = Redis.from_url(settings.redis_connection_url, decode_responses=True)
        pattern = f"{settings.redis_key_prefix}:process:bottleneck:*"
        keys = list(redis_client.scan_iter(match=pattern))
        if keys:
            redis_client.delete(*keys)
    except Exception:
        logger.exception("Failed to clear bottleneck cache after raw event consumption.")


def _clear_defect_transfer_cache() -> None:
    if not settings.redis_url:
        return
    try:
        from redis import Redis
        redis_client = Redis.from_url(settings.redis_connection_url, decode_responses=True)
        pattern = f"{settings.redis_key_prefix}:process:defect-transfer:*"
        keys = list(redis_client.scan_iter(match=pattern))
        if keys:
            redis_client.delete(*keys)
    except Exception:
        logger.exception("Failed to clear defect transfer cache after raw event consumption.")
