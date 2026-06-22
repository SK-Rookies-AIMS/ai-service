# app/kafka/consumer.py

import json

from kafka import KafkaConsumer

from app.kafka.iam_provider import MSKTokenProvider


def create_consumer(topic, group_id):

    consumer = KafkaConsumer(
        topic,

        bootstrap_servers=[
            "127.0.0.2:9098",
            "127.0.0.3:9098"
        ],

        group_id=group_id,

        auto_offset_reset="earliest",

        enable_auto_commit=True,

        security_protocol="SASL_SSL",

        sasl_mechanism="OAUTHBEARER",

        sasl_oauth_token_provider=MSKTokenProvider(),

        value_deserializer=lambda x:
            json.loads(x.decode("utf-8"))
    )

    return consumer