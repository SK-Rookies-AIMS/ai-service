# app/kafka/consumer.py

import json
import ssl
from kafka import KafkaConsumer
from app.kafka.iam_provider import MSKTokenProvider

def create_consumer(topic, group_id):

    consumer = KafkaConsumer(
        topic,
        ssl_context=ssl.create_default_context(),

        bootstrap_servers=[
            "b-1.aimsdevmsk.3g8nqa.c2.kafka.ap-northeast-2.amazonaws.com:9098",
            "b-2.aimsdevmsk.3g8nqa.c2.kafka.ap-northeast-2.amazonaws.com:9098"
        ],

        group_id=group_id,

        auto_offset_reset="earliest",

        enable_auto_commit=True,

        security_protocol="SASL_SSL",

        sasl_mechanism="OAUTHBEARER",

        sasl_oauth_token_provider=MSKTokenProvider(),

        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    return consumer
