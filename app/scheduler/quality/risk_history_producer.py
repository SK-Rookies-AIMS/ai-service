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
    DB_PASSWORD = quote_plus(
        os.getenv("DB_PASSWORD")
    )
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")

    SAMPLE_DB_NAME = os.getenv(
        "SAMPLE_DB_NAME"
    )

    MAIN_DATABASE_URL = (
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/maindb"
    )

    SAMPLE_DATABASE_URL = (
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{SAMPLE_DB_NAME}"
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

        sasl_oauth_token_provider=
            MSKTokenProvider(),

        value_serializer=lambda x:
            json.dumps(
                x,
                default=str
            ).encode("utf-8")
    )

    last_id = 0

    while True:

        with main_engine.connect() as conn:

            new_cars = conn.execute(
                text("""
                    SELECT
                        id,
                        vehicle_id,
                        DATE(created_at)
                            AS inspection_date
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

            vehicle_id = car["vehicle_id"]
            inspection_date = str(
                car["inspection_date"]
            )

            scores = {}

            with sample_engine.connect() as conn:

                # DRIVE
                row = conn.execute(
                    text("""
                        SELECT *
                        FROM car_drive
                        WHERE vehicle_id =
                            :vehicle_id
                        LIMIT 1
                    """),
                    {"vehicle_id": vehicle_id}
                ).mappings().first()

                if row:

                    score = 100

                    if float(
                        row["throttle_position"]
                    ) > 90:
                        score -= 20

                    if float(
                        row["brake_pressure"]
                    ) > 45:
                        score -= 20

                    if abs(float(
                        row["steering_angle"]
                    )) > 40:
                        score -= 20

                    scores["DRIVE"] = max(
                        score,
                        0
                    )

                # CONTROL
                row = conn.execute(
                    text("""
                        SELECT *
                        FROM car_control
                        WHERE vehicle_id =
                            :vehicle_id
                        LIMIT 1
                    """),
                    {"vehicle_id": vehicle_id}
                ).mappings().first()

                if row:

                    score = 100

                    if row[
                        "collision_warning"
                    ] == 1:
                        score -= 40

                    if row[
                        "lane_departure"
                    ] == 1:
                        score -= 20

                    if row[
                        "traction_control"
                    ] == 1:
                        score -= 10

                    if row[
                        "abs_active"
                    ] == 1:
                        score -= 10

                    scores["CONTROL"] = max(
                        score,
                        0
                    )

                # DYNAMICS
                row = conn.execute(
                    text("""
                        SELECT *
                        FROM car_dynamics
                        WHERE vehicle_id =
                            :vehicle_id
                        LIMIT 1
                    """),
                    {"vehicle_id": vehicle_id}
                ).mappings().first()

                if row:

                    score = 100

                    if abs(
                        float(
                            row["yaw_rate"]
                        )
                    ) > 7:
                        score -= 20

                    if abs(
                        float(row["roll"])
                    ) > 4:
                        score -= 20

                    if abs(
                        float(row["pitch"])
                    ) > 4:
                        score -= 20

                    scores["DYNAMICS"] = max(
                        score,
                        0
                    )

                # STATUS
                row = conn.execute(
                    text("""
                        SELECT *
                        FROM car_status
                        WHERE vehicle_id =
                            :vehicle_id
                        LIMIT 1
                    """),
                    {"vehicle_id": vehicle_id}
                ).mappings().first()

                if row:

                    score = 100

                    if float(
                        row["speed"]
                    ) > 120:
                        score -= 20

                    if int(
                        row["att"]
                    ) > 4000:
                        score -= 20

                    if float(
                        row[
                            "battery_voltage"
                        ]
                    ) < 12:
                        score -= 10

                    scores["STATUS"] = max(
                        score,
                        0
                    )

            for inspection_type, risk_score in (
                scores.items()
            ):

                message = {

                    "inspection_type":
                        inspection_type,

                    "inspection_date":
                        inspection_date,

                    "risk_score":
                        risk_score
                }

                producer.send(
                    "quality.inspection.risk_history",
                    value=message
                )

            producer.flush()

            last_id = car["id"]

        time.sleep(1)

    producer.close()


if __name__ == "__main__":
    run()