from sqlalchemy import text
from kafka import KafkaProducer

import os
import json
import time

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
        value_serializer=lambda x: json.dumps(
            x,
            default=str
        ).encode("utf-8")
    )

    def calculate_status_score(status, control):

        score = 100
        issues = []

        speed = float(status["speed"])
        rpm = int(status["att"])
        battery = float(status["battery_voltage"])

        if speed > 120:
            score -= 20
            issues.append("over speed")

        if rpm > 4000:
            score -= 20
            issues.append("RPM Error")

        if battery < 12:
            score -= 10
            issues.append("battery drop")

        if control["collision_warning"] == 1:
            score -= 40
            issues.append("crash warning")

        if status["gear"] == "P" and speed > 20:
            score -= 30
            issues.append("Parking")

        return max(score, 0), issues

    def get_result(score):

        if score >= 90:
            return "PASS"

        elif score >= 70:
            return "WARN"

        return "FAIL"

    last_id = 0

    try:

        while not stop_event.is_set():

            with main_engine.connect() as conn:

                new_cars = conn.execute(
                    text("""
                        SELECT *
                        FROM inspection_master
                        WHERE id > :last_id
                        ORDER BY id
                    """),
                    {
                        "last_id": last_id
                    }
                ).mappings().all()

            if not new_cars:
                time.sleep(1)
                continue

            with sample_engine.connect() as conn:

                for car in new_cars:

                    if stop_event.is_set():
                        break

                    vehicle_id = car["vehicle_id"]

                    status_row = conn.execute(
                        text("""
                            SELECT *
                            FROM car_status
                            WHERE vehicle_id = :vehicle_id
                            ORDER BY created_at DESC
                            LIMIT 1
                        """),
                        {
                            "vehicle_id": vehicle_id
                        }
                    ).mappings().first()

                    control_row = conn.execute(
                        text("""
                            SELECT *
                            FROM car_control
                            WHERE vehicle_id = :vehicle_id
                            ORDER BY created_at DESC
                            LIMIT 1
                        """),
                        {
                            "vehicle_id": vehicle_id
                        }
                    ).mappings().first()

                    if not status_row or not control_row:
                        continue

                    score, issues = calculate_status_score(
                        status_row,
                        control_row
                    )

                    message = {
                        "car_code":
                            vehicle_id.split("-")[0],

                        "inspection_no":
                            f"STATUS-{car['id']:05d}",

                        "vehicle_id":
                            vehicle_id,

                        "speed":
                            float(status_row["speed"]),

                        "att":
                            int(status_row["att"]),

                        "gear":
                            status_row["gear"],

                        "battery_voltage":
                            float(
                                status_row["battery_voltage"]
                            ),

                        "fuel_rate":
                            float(
                                status_row["fuel_rate"]
                            ),

                        "status_score":
                            score,

                        "inspection_result":
                            get_result(score),

                        "issue_message":
                            ", ".join(issues)
                            if issues
                            else "정상",

                        "created_at":
                            status_row["created_at"]
                    }

                    producer.send(
                        "quality.inspection.status_detail",
                        value=message
                    )

                    producer.flush()

                    last_id = car["id"]

            time.sleep(1)

    except Exception as e:

        print(f"오류 발생 : {e}")

    finally:

        print("status-detail producer 종료")

        try:
            producer.flush()
        except Exception:
            pass

        producer.close()


if __name__ == "__main__":
    from threading import Event

    run(Event())