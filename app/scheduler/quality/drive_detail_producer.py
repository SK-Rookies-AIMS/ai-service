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
    drive_rows = conn.execute(
        text("""
            SELECT *
            FROM car_drive
            ORDER BY created_at, vehicle_id
        """)
    ).mappings().all()


for row in drive_rows:

    message = {
        "vehicle_id": row["vehicle_id"],

        "throttle_position": round(
            float(row["throttle_position"]), 2
        ),

        "brake_pressure": round(
            float(row["brake_pressure"]), 2
        ),

        "steering_angle": round(
            float(row["steering_angle"]), 2
        ),

        "created_at": row["created_at"]
    }

    producer.send(
        "quality.inspection.drive_detail",
        value=message
    )

    print(f"Kafka 전송 완료 : {message}")

    # 실시간처럼 보내고 싶으면 사용
    time.sleep(1)


producer.flush()

print("모든 데이터 Kafka 전송 완료")
