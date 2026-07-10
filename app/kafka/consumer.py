import json
import ssl

from kafka import KafkaConsumer

from app.core.config import settings
from app.kafka.iam_provider import MSKTokenProvider


def _bootstrap_servers() -> list[str]:
    servers = [settings.broker_url_1 or "", settings.broker_url_2 or ""]
    return [server.strip() for server in servers if server.strip()]


def create_consumer(
    topic: str,
    group_id: str,
    *,
    enable_auto_commit: bool = True,
    consumer_timeout_ms: int | None = None,
) -> KafkaConsumer:
    consumer_kwargs = dict(
        ssl_context=ssl.create_default_context(),
        bootstrap_servers=_bootstrap_servers(),
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=enable_auto_commit,
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=MSKTokenProvider(),
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    if consumer_timeout_ms is not None:
        consumer_kwargs["consumer_timeout_ms"] = consumer_timeout_ms
    consumer = KafkaConsumer(
        topic,
        **consumer_kwargs,
    )
    return consumer
