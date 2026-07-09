from sqlalchemy import text
from kafka import KafkaProducer

import os
import json
import threading

from app.db import main_engine
from app.kafka.iam_provider import MSKTokenProvider

TOTAL_TARGET = 100


def run(stop_event):

    producer = KafkaProducer(
        bootstrap_servers=[
            os.getenv("BROKER_URL_1"),
            os.getenv("BROKER_URL_2")
        ],
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=MSKTokenProvider(),
        value_serializer=lambda x: json.dumps(
            x,
            default=str
        ).encode("utf-8")
    )

    last_id = 0

    try:

        while not stop_event.is_set():

            with main_engine.connect() as conn:

                new_cars = conn.execute(
                    text("""
                        SELECT
                            id,
                            vehicle_id,
                            created_at
                        FROM inspection_master
                        WHERE id > :last_id
                        ORDER BY id
                    """),
                    {
                        "last_id": last_id
                    }
                ).mappings().all()

            if not new_cars:
                stop_event.wait(1)
                continue

            for car in new_cars:

                if stop_event.is_set():
                    break

                # ============================
                # 해당 날짜 생산 차량 개수 계산
                # ============================
                with main_engine.connect() as conn:

                    current_count = conn.execute(
                        text("""
                            SELECT COUNT(*)
                            FROM inspection_master
                            WHERE DATE(created_at) = DATE(:created_at)
                              AND id <= :current_id
                        """),
                        {
                            "created_at": car["created_at"],
                            "current_id": car["id"]
                        }
                    ).scalar()

                # 생산 완료
                if current_count >= TOTAL_TARGET:

                    process_list = [
                        ("VISUAL", TOTAL_TARGET),
                        ("FUNCTION", TOTAL_TARGET),
                        ("DRIVE", TOTAL_TARGET),
                        ("FINAL", TOTAL_TARGET)
                    ]

                else:

                    process_list = [

                        (
                            "VISUAL",
                            min(current_count, TOTAL_TARGET)
                        ),

                        (
                            "FUNCTION",
                            min(max(current_count - 1, 0), TOTAL_TARGET)
                        ),

                        (
                            "DRIVE",
                            min(max(current_count - 2, 0), TOTAL_TARGET)
                        ),

                        (
                            "FINAL",
                            min(max(current_count - 3, 0), TOTAL_TARGET)
                        )
                    ]

                for process_name, completed in process_list:

                    waiting = max(
                        TOTAL_TARGET - completed,
                        0
                    )

                    progress_rate = round(
                        completed / TOTAL_TARGET * 100,
                        2
                    )

                    if progress_rate >= 100:
                        process_status = "COMPLETE"
                    elif progress_rate == 0:
                        process_status = "WAIT"
                    else:
                        process_status = "RUNNING"

                    message = {

                        "process_name": process_name,

                        "total_vehicle_count": TOTAL_TARGET,

                        "completed_count": completed,

                        "waiting_count": waiting,

                        "progress_rate": progress_rate,

                        "process_status": process_status,

                        "created_at": car["created_at"]

                    }

                    producer.send(
                        "quality.inspection.process",
                        value=message
                    )

                last_id = car["id"]

            producer.flush()

            stop_event.wait(1)

    except Exception as e:

        print(f"Process Producer 오류 : {e}")

    finally:

        producer.flush()
        producer.close()

        print("Process Producer 종료")


if __name__ == "__main__":

    run(threading.Event())