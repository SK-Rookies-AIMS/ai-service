from __future__ import annotations

import asyncio
import json
import logging
import ssl
from datetime import datetime
from threading import Event
from typing import Any

from fastapi import FastAPI

from app.core.config import settings
from app.kafka.iam_provider import MSKTokenProvider
from app.repository.sampledb_repository import SampleDbRepository


logger = logging.getLogger(__name__)
PROCESS_SEQUENCE = ("PRESS", "BODY", "PAINT", "ASSEMBLY")

# assembly-service의 app.kafka.topics.raw.name과 동일한 실제 raw 토픽명.
RAW_TOPIC = "factory.manufacturing.raw"
RAW_CONSUMER_GROUP_ID = "ai-consumer-group"
RAW_AUTO_OFFSET_RESET = "earliest"
RAW_CONSUMER_CONCURRENCY = 2


def start_raw_event_consumer(app: FastAPI) -> None:
    """FastAPI startup 시 제조 raw Kafka consumer를 백그라운드 thread로 시작한다."""
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
    # shutdown 이벤트에서 thread consumer loop를 안전하게 종료하기 위한 공유 신호.
    app.state.raw_event_consumer_stop_event = stop_event
    app.state.raw_event_consumer_tasks = tasks


async def stop_raw_event_consumer(app: FastAPI) -> None:
    """FastAPI shutdown 시 consumer loop 종료 신호를 보내고 thread 종료를 기다린다."""
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
        from kafka import KafkaConsumer
    except ModuleNotFoundError:
        logger.exception("kafka-python is required to consume manufacturing raw events.")
        return

    # 기존 품질 Kafka 연동과 동일하게 MSK IAM 인증(SASL_SSL/OAUTHBEARER)을 사용한다.
    consumer = KafkaConsumer(
        RAW_TOPIC,
        ssl_context=ssl.create_default_context(),
        bootstrap_servers=bootstrap_servers,
        group_id=RAW_CONSUMER_GROUP_ID,
        auto_offset_reset=RAW_AUTO_OFFSET_RESET,
        enable_auto_commit=False,
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=MSKTokenProvider(),
        consumer_timeout_ms=1000,
    )
    repository = SampleDbRepository(settings.sample_database_connection_url)
    repository.schema.ensure_schema()

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
                    _consume_record(repository, record)
                except Exception:
                    logger.exception(
                        "Failed to consume raw event: topic=%s partition=%s offset=%s",
                        record.topic,
                        record.partition,
                        record.offset,
                    )
                finally:
                    # DB 저장 시도 후 offset을 커밋해 재기동 시 같은 메시지의 무한 재처리를 막는다.
                    consumer.commit()
    except Exception:
        logger.exception("Raw Kafka consumer failed.")
    finally:
        consumer.close()
        logger.info(
            "Raw Kafka consumer stopped: topic=%s group=%s concurrency=%s/%s",
            RAW_TOPIC,
            RAW_CONSUMER_GROUP_ID,
            consumer_index,
            RAW_CONSUMER_CONCURRENCY,
        )


def _consume_record(repository: SampleDbRepository, record: Any) -> None:
    """Kafka raw 메시지를 manufacturing_event_json row로 변환해 sampledb에 upsert한다."""
    raw_event = _parse_raw_event(record)
    row = _raw_event_to_row(raw_event)
    affected_rows = repository.events.insert_raw_rows(
        [row],
        update_existing=True,
    )
    # 새 raw 이벤트가 들어오면 다음 병목 API 호출에서 재분석하도록 Redis 캐시를 비운다.
    _clear_bottleneck_cache()
    logger.info(
        "Kafka raw event consumed and stored: topic=%s partition=%s offset=%s "
        "key=%s event_id=%s manufacturing_event_id_source=manufacturing_event_json.id "
        "car_master_id=%s process_code=%s affected_rows=%s",
        record.topic,
        record.partition,
        record.offset,
        raw_event.get("_kafka_key"),
        row["event_id"],
        row["car_master_id"],
        row["process_code"],
        affected_rows,
    )


def _parse_raw_event(record: Any) -> dict[str, Any]:
    """Kafka record의 value JSON과 key(carId)를 분석하기 쉬운 dict로 정규화한다."""
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
    """assembly-service raw envelope를 manufacturing_event_json 테이블 컬럼으로 매핑한다."""
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
    """PRESS/BODY/PAINT/ASSEMBLY 중 하나로 공정 코드를 표준화한다."""
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
