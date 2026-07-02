from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import json


def create_producer():

    load_dotenv()

    return KafkaProducer(
        bootstrap_servers=[
            os.getenv("BROKER_URL_1"),
            os.getenv("BROKER_URL_2")
        ],
        value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8")
    )