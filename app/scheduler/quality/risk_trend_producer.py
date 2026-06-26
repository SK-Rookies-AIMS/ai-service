from sqlalchemy import create_engine, text
from kafka import KafkaProducer

from datetime import datetime, timedelta

import json
import time

from app.kafka.iam_provider import MSKTokenProvider

SAMPLE_DATABASE_URL = (
    "mysql+pymysql://admin:j8XKJ9?vbR>v5Mysc0_5zk-zMDnO@aims-dev-mysql.c7yyi6w0ch43.ap-northeast-2.amazonaws.com:3306/sampledb"
)

sample_engine = create_engine(
    SAMPLE_DATABASE_URL,
    pool_pre_ping=True
)

producer = KafkaProducer(
    bootstrap_servers=[
        "b-1.aimsdevmsk.3g8nqa.c2.kafka.ap-northeast-2.amazonaws.com:9098",
        "b-2.aimsdevmsk.3g8nqa.c2.kafka.ap-northeast-2.amazonaws.com:9098"
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


risk_id = 1

base_date = datetime.strptime(
    "2026-06-01",
    "%Y-%m-%d"
)

with sample_engine.connect() as conn:

    for day in range(7):

        current_date = (
            base_date +
            timedelta(days=day)
        )

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
            v["vehicle_id"]
            for v in vehicles
        ]

        low_count = 0
        medium_count = 0
        high_count = 0

        for vehicle_id in vehicle_ids:

            scores = []

            status = conn.execute(
                text("""
                    SELECT *
                    FROM car_status
                    WHERE vehicle_id=:vehicle_id
                    LIMIT 1
                """),
                {"vehicle_id": vehicle_id}
            ).mappings().first()

            if status:
                scores.append(
                    calculate_status_risk(status)
                )

            control = conn.execute(
                text("""
                    SELECT *
                    FROM car_control
                    WHERE vehicle_id=:vehicle_id
                    LIMIT 1
                """),
                {"vehicle_id": vehicle_id}
            ).mappings().first()

            if control:
                scores.append(
                    calculate_control_risk(control)
                )

            drive = conn.execute(
                text("""
                    SELECT *
                    FROM car_drive
                    WHERE vehicle_id=:vehicle_id
                    LIMIT 1
                """),
                {"vehicle_id": vehicle_id}
            ).mappings().first()

            if drive:
                scores.append(
                    calculate_drive_risk(drive)
                )

            dynamics = conn.execute(
                text("""
                    SELECT *
                    FROM car_dynamics
                    WHERE vehicle_id=:vehicle_id
                    LIMIT 1
                """),
                {"vehicle_id": vehicle_id}
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
                low_count += 1

            elif level == "MEDIUM":
                medium_count += 1

            else:
                high_count += 1

        daily_result = [
            ("LOW", low_count),
            ("MEDIUM", medium_count),
            ("HIGH", high_count)
        ]

        for level, count in daily_result:

            message = {
                "id": risk_id,
                "risk_level": level,
                "risk_count": count,

                # 필요하면 ratio 계산 수정
                "risk_ratio": round(
                    count / 100 * 100,
                    2
                ),

                "created_at":
                    current_date.strftime(
                        "%Y-%m-%d 00:00:00"
                    )
            }

            producer.send(
                "quality.inspection.risk_trend",
                value=message
            )

            print(
                f"Kafka 전송 완료 : {message}"
            )

            risk_id += 1

            time.sleep(1)

producer.flush()

print("모든 데이터 Kafka 전송 완료")