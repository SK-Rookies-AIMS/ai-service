from sqlalchemy import create_engine, text
from kafka import KafkaProducer
from dotenv import load_dotenv
from urllib.parse import quote_plus
import os
import json
import time

from app.kafka.iam_provider import MSKTokenProvider


def run():

    load_dotenv()

    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = quote_plus(os.getenv("DB_PASSWORD"))
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")

    SAMPLE_DATABASE_URL = (
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/sampledb"
    )

    MAIN_DATABASE_URL = (
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/maindb"
    )

    sample_engine = create_engine(
        SAMPLE_DATABASE_URL,
        pool_pre_ping=True
    )

    main_engine = create_engine(
        MAIN_DATABASE_URL,
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

    last_id = 0

    while True:

        # 새로 생산된 차량 조회
        with main_engine.connect() as conn:

            cars = conn.execute(
                text("""
                    SELECT id, vehicle_id
                    FROM inspection_master
                    WHERE id > :last_id
                    ORDER BY id
                """),
                {"last_id": last_id}
            ).mappings().all()

        if not cars:
            time.sleep(1)
            continue

        for car in cars:

            vehicle_id = car["vehicle_id"]

            # 해당 차량의 주행 데이터 조회
            with sample_engine.connect() as conn:

                drive_rows = conn.execute(
                    text("""
                        SELECT *
                        FROM car_drive
                        WHERE vehicle_id = :vehicle_id
                        ORDER BY created_at
                    """),
                    {"vehicle_id": vehicle_id}
                ).mappings().all()

            if not drive_rows:
                print(
                    f"[Drive] {vehicle_id} 데이터 없음"
                )

                last_id = car["id"]
                continue

            print(
                f"[Drive] {vehicle_id} 분석 시작"
            )

            for row in drive_rows:

                message = {

                    "vehicle_id":
                        row["vehicle_id"],

                    "throttle_position":
                        round(
                            float(row["throttle_position"]),
                            2
                        ),

                    "brake_pressure":
                        round(
                            float(row["brake_pressure"]),
                            2
                        ),

                    "steering_angle":
                        round(
                            float(row["steering_angle"]),
                            2
                        ),

                    "created_at":
                        row["created_at"]
                }

                producer.send(
                    "quality.inspection.drive_detail",
                    value=message
                )

            producer.flush()

            print(
                f"[Drive] {vehicle_id} Kafka 전송 완료"
            )

            # 처리 완료 차량 갱신
            last_id = car["id"]

        time.sleep(1)


if __name__ == "__main__":
    run()