from sqlalchemy import create_engine, text
from kafka import KafkaProducer
from dotenv import load_dotenv
from urllib.parse import quote_plus

import os
import json
import time

from app.kafka.iam_provider import MSKTokenProvider

TOTAL_TARGET = 100


def run():

    load_dotenv()

    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = quote_plus(
        os.getenv("DB_PASSWORD")
    )
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")

    MAIN_DATABASE_URL = (
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/maindb"
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
                {"last_id": last_id}
            ).mappings().all()

        if not new_cars:
            time.sleep(1)
            continue

        for car in new_cars:

            current_count = car["id"]

            # 생산이 모두 끝난 경우
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
                        min(
                            max(current_count - 1, 0),
                            TOTAL_TARGET
                        )
                    ),

                    (
                        "DRIVE",
                        min(
                            max(current_count - 2, 0),
                            TOTAL_TARGET
                        )
                    ),

                    (
                        "FINAL",
                        min(
                            max(current_count - 3, 0),
                            TOTAL_TARGET
                        )
                    )
                ]

            for process_name, completed in process_list:

                waiting = max(
                    0,
                    TOTAL_TARGET - completed
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
                    "process_name":
                        process_name,

                    "total_vehicle_count":
                        TOTAL_TARGET,

                    "completed_count":
                        completed,

                    "waiting_count":
                        waiting,

                    "progress_rate":
                        progress_rate,

                    "process_status":
                        process_status,

                    "created_at":
                        car["created_at"]
                }

                producer.send(
                    "quality.inspection.process",
                    value=message
                )

                print(
                    f"[{process_name}] "
                    f"{completed}/{TOTAL_TARGET} "
                    f"({progress_rate}%) "
                    f"[{process_status}]"
                )

            producer.flush()

            last_id = car["id"]

        time.sleep(1)


if __name__ == "__main__":
    run()