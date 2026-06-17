from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime, timedelta

DATABASE_URL = (
    "mysql+pymysql://admin:K.d?S|46~($$z~.J2W)~W!aMEG)-"
    "@127.0.0.1:13306/sampledb"
)

# -------------------
# Risk 계산 함수
# -------------------

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


# -------------------
# DB
# -------------------

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

result = []
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

with engine.connect() as conn:

    # 7일
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

        vehicle_ids = [v["vehicle_id"] for v in vehicles]

        current_time = day_start

        for stage_name, duration in stage_plan:

            stage_start = current_time
            stage_end = current_time + timedelta(hours=duration)

            scores = []

            for vehicle_id in vehicle_ids:

                if stage_name == "DRIVE":

                    row = conn.execute(
                        text("""
                            SELECT *
                            FROM car_drive
                            WHERE vehicle_id=:vehicle_id
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
                            WHERE vehicle_id=:vehicle_id
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
                            WHERE vehicle_id=:vehicle_id
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
                            WHERE vehicle_id=:vehicle_id
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

            result.append({
                "id": risk_id,
                "inspection_type": stage_name,
                "risk_score": avg_score,
                "start_time": stage_start.strftime("%Y-%m-%d %H:%M"),
                "end_time": stage_end.strftime("%Y-%m-%d %H:%M")
            })

            risk_id += 1

            current_time = stage_end

df = pd.DataFrame(result)

df.to_csv(
    "inspection_risk_history.csv",
    index=False,
    encoding="utf-8-sig"
)

print(df)
print(f"총 생성 건수 : {len(df)}")