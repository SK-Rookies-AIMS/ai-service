from sqlalchemy import create_engine, text
from decimal import Decimal
import pandas as pd

SAMPLE_DATABASE_URL = (
    "mysql+pymysql://admin:j8XKJ9?vbR>v5Mysc0_5zk-zMDnO@127.0.0.1:13306/sampledb"
)

MAIN_DATABASE_URL = (
    "mysql+pymysql://admin:j8XKJ9?vbR>v5Mysc0_5zk-zMDnO@127.0.0.1:13306/maindb"
)

sample_engine = create_engine(
    SAMPLE_DATABASE_URL,
    pool_pre_ping=True
)

main_engine = create_engine(
    MAIN_DATABASE_URL,
    pool_pre_ping=True
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
        """)
    ).mappings().all()

    inspection_status_detail_list = []

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

        inspection_status_detail = {
            "car_code": vehicle_id.split("-")[0],
            "inspection_no": f"STATUS-{master_row['id']:05d}",
            "vehicle_id": vehicle_id,
            "speed": float(status_row["speed"]),
            "att": int(status_row["att"]),
            "gear": status_row["gear"],
            "battery_voltage": float(status_row["battery_voltage"]),
            "fuel_rate": float(status_row["fuel_rate"]),
            "status_score": float(score),
            "inspection_result": get_result(score),
            "issue_message": ", ".join(issues) if issues else "정상",
            "created_at": status_row["created_at"]
        }

        inspection_status_detail_list.append(
            inspection_status_detail
        )

df = pd.DataFrame(
    inspection_status_detail_list
)

df.to_sql(
    name="inspection_status_detail",
    con=main_engine,
    if_exists="append",
    index=False
)

print(
    f"inspection_status_detail table 전송 완료"
)