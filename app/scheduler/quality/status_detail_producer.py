from sqlalchemy import create_engine, text
from kafka import KafkaProducer

import json
import time

from app.kafka.iam_provider import MSKTokenProvider

SAMPLE_DATABASE_URL = (
    "mysql+pymysql://admin:j8XKJ9?vbR>v5Mysc0_5zk-zMDnO@aims-dev-mysql.c7yyi6w0ch43.ap-northeast-2.rds.amazonaws.com:3306/sampledb"
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

    if battery < 12.0:
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

    if score >= 70:
        return "WARN"

    return "FAIL"


with sample_engine.connect() as conn:

    master_rows = conn.execute(
        text("""
            SELECT *
            FROM car_master
            ORDER BY id
        """)
    ).mappings().all()

    for master_row in master_rows:

        vehicle_id = master_row["vehicle_id"]

        status_row = conn.execute(
            text("""
                SELECT *
                FROM car_status
                WHERE vehicle_id = :vehicle_id
                ORDER BY created_at DESC
                LIMIT 1
            """),
            {"vehicle_id": vehicle_id}
        ).mappings().first()

        control_row = conn.execute(
            text("""
                SELECT *
                FROM car_control
                WHERE vehicle_id = :vehicle_id
                ORDER BY created_at DESC
                LIMIT 1
            """),
            {"vehicle_id": vehicle_id}
        ).mappings().first()

        if not status_row or not control_row:
            continue

        score, issues = calculate_status_score(
            status_row,
            control_row
        )

        message = {
            "car_code": vehicle_id.split("-")[0],

            "inspection_no":
                f"STATUS-{master_row['id']:05d}",

            "vehicle_id": vehicle_id,

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
                float(status_row["fuel_rate"]),

            "status_score":
                float(score),

            "inspection_result":
                get_result(score),

            "issue_message":
                ", ".join(issues)
                if issues else "정상",

            "created_at":
                status_row["created_at"]
        }

        producer.send(
            "quality.inspection.status_detail",
            value=message
        )

        print(
            f"Kafka 전송 완료 : {message}"
        )

        time.sleep(1)

producer.flush()

print("모든 데이터 Kafka 전송 완료")