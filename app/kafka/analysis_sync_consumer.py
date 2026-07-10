from __future__ import annotations

import asyncio
import logging
from threading import Event
from typing import Any

from fastapi import FastAPI

from app.core.config import settings
from app.kafka import config as kafka_config
from app.kafka.consumer import create_consumer
from app.search.process_analysis_search import ProcessAnalysisSearchRepository

logger = logging.getLogger(__name__)


def start_analysis_sync_consumer(app: FastAPI) -> None:
    if not settings.elasticsearch_url:
        logger.info("Elasticsearch is not configured. Analysis sync consumer is disabled.")
        return
    if not (settings.broker_url_1 or settings.broker_url_2):
        logger.info("Kafka bootstrap servers are not set. Analysis sync consumer is disabled.")
        return

    stop_event = Event()
    tasks = [
        asyncio.create_task(
            asyncio.to_thread(
                _run_analysis_sync_consumer,
                stop_event,
                1,
            ),
        )
    ]
    app.state.analysis_sync_consumer_stop_event = stop_event
    app.state.analysis_sync_consumer_tasks = tasks


async def stop_analysis_sync_consumer(app: FastAPI) -> None:
    tasks = getattr(app.state, "analysis_sync_consumer_tasks", None)
    if not tasks:
        return
    stop_event = getattr(app.state, "analysis_sync_consumer_stop_event", None)
    if stop_event is not None:
        stop_event.set()
    await asyncio.gather(*tasks, return_exceptions=True)


def _run_analysis_sync_consumer(
    stop_event: Event,
    consumer_index: int,
) -> None:
    consumer = create_consumer(
        kafka_config.ANALYSIS_SYNC_TOPIC,
        kafka_config.ANALYSIS_SYNC_CONSUMER_GROUP_ID,
        enable_auto_commit=False,
        consumer_timeout_ms=kafka_config.CONSUMER_TIMEOUT_MS,
    )
    search_repository = ProcessAnalysisSearchRepository()
    try:
        search_repository.ensure_indices()
    except Exception:
        logger.exception("Failed to ensure Elasticsearch indices for analysis sync.")
        consumer.close()
        return

    logger.info(
        "Analysis sync consumer started: topic=%s group=%s consumer_index=%s",
        kafka_config.ANALYSIS_SYNC_TOPIC,
        kafka_config.ANALYSIS_SYNC_CONSUMER_GROUP_ID,
        consumer_index,
    )
    try:
        while not stop_event.is_set():
            for message in consumer:
                if stop_event.is_set():
                    break
                try:
                    payload = message.value
                    if not isinstance(payload, dict):
                        logger.warning("Skipping non-dict analysis sync payload.")
                        continue
                    search_repository.index_sync_event(payload)
                    consumer.commit()
                except Exception:
                    logger.exception(
                        "Failed to index analysis sync event: topic=%s partition=%s offset=%s",
                        message.topic,
                        message.partition,
                        message.offset,
                    )
    finally:
        consumer.close()
        logger.info(
            "Analysis sync consumer stopped: topic=%s group=%s consumer_index=%s",
            kafka_config.ANALYSIS_SYNC_TOPIC,
            kafka_config.ANALYSIS_SYNC_CONSUMER_GROUP_ID,
            consumer_index,
        )
