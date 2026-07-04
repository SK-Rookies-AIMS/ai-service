from sqlalchemy import text
from kafka import KafkaProducer

import os
import json
import time

from app.db import (
    main_engine,
    sample_engine
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

    def get_risk_level(score):

        if score >= 80:
            return "LOW"

        elif score >= 50:
            return "MEDIUM"

        return "HIGH"

    last_id = 0
    try:
        while not stop_event.is_set():

            with main_engine.connect() as conn:

                cars = conn.execute(
                    text("""
                        SELECT *
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

                created_date = (
                    car["created_at"]
                    .strftime("%Y-%m-%d 00:00:00")
                )

                low = 0
                medium = 0
                high = 0

                with main_engine.connect() as conn:

                    today_cars = conn.execute(
                        text("""
                            SELECT vehicle_id
                            FROM inspection_master
                            WHERE DATE(created_at)
                            =
                            DATE(:created_at)
                        """),
                        {
                            "created_at":
                                car["created_at"]
                        }
                    ).mappings().all()

                for row in today_cars:

                    vid = row["vehicle_id"]

                    scores = []

                    with sample_engine.connect() as conn:

                        status = conn.execute(
                            text("""
                                SELECT *
                                FROM car_status
                                WHERE vehicle_id=:vid
                                LIMIT 1
                            """),
                            {"vid": vid}
                        ).mappings().first()

                        if status:
                            scores.append(
                                calculate_status_risk(status)
                            )

                        control = conn.execute(
                            text("""
                                SELECT *
                                FROM car_control
                                WHERE vehicle_id=:vid
                                LIMIT 1
                            """),
                            {"vid": vid}
                        ).mappings().first()

                        if control:
                            scores.append(
                                calculate_control_risk(control)
                            )

                        drive = conn.execute(
                            text("""
                                SELECT *
                                FROM car_drive
                                WHERE vehicle_id=:vid
                                LIMIT 1
                            """),
                            {"vid": vid}
                        ).mappings().first()

                        if drive:
                            scores.append(
                                calculate_drive_risk(drive)
                            )

                        dynamics = conn.execute(
                            text("""
                                SELECT *
                                FROM car_dynamics
                                WHERE vehicle_id=:vid
                                LIMIT 1
                            """),
                            {"vid": vid}
                        ).mappings().first()

                        if dynamics:
                            scores.append(
                                calculate_dynamics_risk(dynamics)
                            )

                    if not scores:
                        continue

                    avg_score = (
                        sum(scores) / len(scores)
                    )

                    level = get_risk_level(
                        avg_score
                    )

                    if level == "LOW":
                        low += 1

                    elif level == "MEDIUM":
                        medium += 1

                    else:
                        high += 1

                total = low + medium + high

                for level, count in [
                    ("LOW", low),
                    ("MEDIUM", medium),
                    ("HIGH", high)
                ]:

                    producer.send(
                        "quality.inspection.risk_trend",
                        value={
                            "risk_level": level,

                            "risk_count": count,

                            "risk_ratio": round(
                                count / total * 100, 2
                            ) if total else 0,

                            "created_at":
                                created_date
                        }
                    )

                producer.flush()

                last_id = car["id"]

            time.sleep(1)

    except Exception as e:
        print(f"오류 발생 : {e}")

    finally:
        producer.close()
        print("risk-trend producer 종료")