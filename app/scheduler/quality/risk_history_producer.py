from sqlalchemy import create_engine, text
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus

from datetime import datetime, timedelta

import json
import time

from app.kafka.iam_provider import MSKTokenProvider

load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = quote_plus(
    os.getenv("DB_PASSWORD")
)
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
SAMPLE_DB_NAME = os.getenv("SAMPLE_DB_NAME")

SAMPLE_DATABASE_URL = (
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{SAMPLE_DB_NAME}"
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


def calculate_status_risk(row):
    score = 100

    if float(row["speed"]) > 120:
        score -= 20

    if int(row["att"]) > 4000:
        score -= 20

    if float(row["battery_voltage"]) < 12:
        score -= 10

    return max(score, 0)


def calculate_control_risk(row):
    score = 100

    if row["collision_warning"] == 1:
        score -= 40

    if row["lane_departure"] == 1:
        score -= 20

    if row["traction_control"] == 1:
        score -= 10

    if row["abs_active"] == 1:
        score -= 10

    return max(score, 0)


def calculate_drive_risk(row):
    score = 100

    if float(row["throttle_position"]) > 90:
        score -= 20

    if float(row["brake_pressure"]) > 45:
        score -= 20

    if abs(float(row["steering_angle"])) > 40:
        score -= 20

    return max(score, 0)


def calculate_dynamics_risk(row):
    score = 100

    if abs(float(row["yaw_rate"])) > 7:
        score -= 20

    if abs(float(row["roll"])) > 4:
        score -= 20

    if abs(float(row["pitch"])) > 4:
        score -= 20

    return max(score, 0)


risk_id = 1

stage_plan = [
    ("DRIVE", 6),
    ("CONTROL", 5),
    ("DYNAMICS", 3),
    ("STATUS", 3)
]

start_date = datetime.strptime(
    "2026-06-01 01:00",
    "%Y-%m-%d %H:%M"
)

with sample_engine.connect() as conn:

    # 7일 데이터 생성
    for day in range(7):

        day_start = start_date + timedelta(days=day)

        # 하루 생산 차량 100대
        offset = day * 100

        vehicles = conn.execute(
            text("""
                SELECT DISTINCT vehicle_id
                FROM car_status
                ORDER BY vehicle_id
                LIMIT 100 OFFSET :offset
            """),
            {"offset": offset}
        ).mappings().all()

        vehicle_ids = [
            vehicle["vehicle_id"]
            for vehicle in vehicles
        ]

        current_time = day_start

        for stage_name, duration in stage_plan:

            stage_start = current_time

            stage_end = (
                current_time
                + timedelta(hours=duration)
            )

            scores = []

            for vehicle_id in vehicle_ids:

                if stage_name == "DRIVE":

                    row = conn.execute(
                        text("""
                            SELECT *
                            FROM car_drive
                            WHERE vehicle_id = :vehicle_id
                            LIMIT 1
                        """),
                        {"vehicle_id": vehicle_id}
                    ).mappings().first()

                    if row:
                        scores.append(
                            calculate_drive_risk(row)
                        )

                elif stage_name == "CONTROL":

                    row = conn.execute(
                        text("""
                            SELECT *
                            FROM car_control
                            WHERE vehicle_id = :vehicle_id
                            LIMIT 1
                        """),
                        {"vehicle_id": vehicle_id}
                    ).mappings().first()

                    if row:
                        scores.append(
                            calculate_control_risk(row)
                        )

                elif stage_name == "DYNAMICS":

                    row = conn.execute(
                        text("""
                            SELECT *
                            FROM car_dynamics
                            WHERE vehicle_id = :vehicle_id
                            LIMIT 1
                        """),
                        {"vehicle_id": vehicle_id}
                    ).mappings().first()

                    if row:
                        scores.append(
                            calculate_dynamics_risk(row)
                        )

                elif stage_name == "STATUS":

                    row = conn.execute(
                        text("""
                            SELECT *
                            FROM car_status
                            WHERE vehicle_id = :vehicle_id
                            LIMIT 1
                        """),
                        {"vehicle_id": vehicle_id}
                    ).mappings().first()

                    if row:
                        scores.append(
                            calculate_status_risk(row)
                        )

            avg_score = (
                round(sum(scores) / len(scores), 2)
                if scores else 0
            )

            message = {
                "id": risk_id,
                "inspection_type": stage_name,
                "inspection_round": day + 1,
                "risk_score": avg_score,
                "start_time": stage_start.strftime(
                    "%Y-%m-%d %H:%M"
                ),
                "end_time": stage_end.strftime(
                    "%Y-%m-%d %H:%M"
                )
            }

            producer.send(
                "quality.inspection.risk_history",
                value=message
            )

            print(
                f"Kafka 전송 완료 : {message}"
            )

            risk_id += 1

            current_time = stage_end

            time.sleep(1)

producer.flush()

print("모든 데이터 Kafka 전송 완료")