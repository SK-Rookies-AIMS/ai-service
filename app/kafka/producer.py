import json
import ssl

from kafka import KafkaProducer

from app.core.config import settings
from app.kafka.iam_provider import MSKTokenProvider


def _bootstrap_servers() -> list[str]:
    servers = [settings.broker_url_1 or "", settings.broker_url_2 or ""]
    return [server.strip() for server in servers if server.strip()]


def create_producer():
    return KafkaProducer(
        ssl_context=ssl.create_default_context(),
        bootstrap_servers=_bootstrap_servers(),
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=MSKTokenProvider(),
        value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8"),
    )
