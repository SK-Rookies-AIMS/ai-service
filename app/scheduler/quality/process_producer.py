from sqlalchemy import create_engine, text
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

import json
import time

from app.kafka.iam_provider import MSKTokenProvider

load_dotenv()
SAMPLE_DATABASE_URL = (
    os.getenv("SAMPLE_DATABASE_END")
)

sample_engine = create_engine(
    SAMPLE_DATABASE_URL,
    pool_pre_ping=True
)

producer = KafkaProducer(
    bootstrap_servers=[
        os.getenv("BROKER_URL_1"),
        os.getenv("BROKER_URL_2")
    ],

    security_protocol="SASL_SSL",

    sasl_mechanism="OAUTHBEARER",

    sasl_oauth_token_provider=MSKTokenProvider(),

    value_serializer=lambda x:
        json.dumps(x, default=str).encode("utf-8")
)


with sample_engine.connect() as conn:
    process_rows = conn.execute(
        text("""
            SELECT
                DATE(created_at) AS process_date,
                COUNT(*) AS vehicle_count
            FROM car_master
            GROUP BY DATE(created_at)
            ORDER BY process_date
        """)
    ).mappings().all()


for row in process_rows:

    message = {
        "process_date": str(row["process_date"]),
        "vehicle_count": row["vehicle_count"]
    }

    producer.send(
        "quality.inspection.process",
        value=message
    )

    print(f"Kafka 전송 완료 : {message}")

    time.sleep(1)

producer.flush()
print("모든 데이터 Kafka 전송 완료")