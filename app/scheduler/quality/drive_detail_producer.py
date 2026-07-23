from sqlalchemy import text
from kafka import KafkaProducer

import os
import json
import threading

from app.db import (
    main_engine,
    sample_engine,
)

from app.kafka.iam_provider import MSKTokenProvider


def run(stop_event):

    producer = KafkaProducer(
        bootstrap_servers=[
            os.getenv("BROKER_URL_1"),
            os.getenv("BROKER_URL_2")
        ],

        security_protocol="SASL_SSL",

        sasl_mechanism="OAUTHBEARER",

        sasl_oauth_token_provider=MSKTokenProvider(),

        value_serializer=lambda x:
            json.dumps(
                x,
                default=str
            ).encode("utf-8")
    )

    last_id = 0

    try:

        while not stop_event.is_set():

            # 새로 생산된 차량 조회
            with main_engine.connect() as conn:

                cars = conn.execute(
                    text("""
                        SELECT
                            id,
                            vehicle_id
                        FROM inspection_master
                        WHERE id > :last_id
                        ORDER BY id
                    """),
                    {
                        "last_id": last_id
                    }
                ).mappings().all()

            if not cars:
                stop_event.wait(1)
                continue

            for car in cars:

                if stop_event.is_set():
                    break

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
                        {
                            "vehicle_id": vehicle_id
                        }
                    ).mappings().all()

                if not drive_rows:

                    print(
                        f"[Drive] {vehicle_id} 데이터 없음"
                    )

                    last_id = car["id"]
                    continue

                for row in drive_rows:

                    message = {

                        "vehicle_id":
                            row["vehicle_id"],

                        "throttle_position":
                            round(
                                float(
                                    row["throttle_position"]
                                ),
                                2
                            ),

                        "brake_pressure":
                            round(
                                float(
                                    row["brake_pressure"]
                                ),
                                2
                            ),

                        "steering_angle":
                            round(
                                float(
                                    row["steering_angle"]
                                ),
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

                # 처리 완료 차량 갱신
                last_id = car["id"]

            stop_event.wait(1)

    except Exception as e:

        print(f"Drive Producer 오류 : {e}")

    finally:

        producer.flush()
        producer.close()

        print("Drive Producer 종료")


if __name__ == "__main__":

    run(threading.Event())