from sqlalchemy import create_engine, text
import pandas as pd

SAMPLE_DATABASE_URL = (
    "mysql+pymysql://admin:K.d?S|46~($$z~.J2W)~W!aMEG)-@127.0.0.1:13306/sampledb"
)

MAIN_DATABASE_URL = (
    "mysql+pymysql://admin:K.d?S|46~($$z~.J2W)~W!aMEG)-@127.0.0.1:13306/maindb"
)

sample_engine = create_engine(
    SAMPLE_DATABASE_URL,
    pool_pre_ping=True
)

main_engine = create_engine(
    MAIN_DATABASE_URL,
    pool_pre_ping=True
)

def calculate_drive_score(row):


    score = 100

    if float(row["throttle_position"]) > 90:
        score -= 20

    if float(row["brake_pressure"]) > 45:
        score -= 20

    if abs(float(row["steering_angle"])) > 40:
        score -= 20

    return round(score, 2)


def get_driving_pattern(row):


    throttle = float(row["throttle_position"])
    brake = float(row["brake_pressure"])
    steering = abs(float(row["steering_angle"]))

    if throttle > 75:
        return "RAPID_ACCEL"

    if brake > 35:
        return "HARD_BRAKE"

    if steering > 40:
        return "SHARP_TURN"

    return "NORMAL"


inspection_drive_detail_list = []

with sample_engine.connect() as conn:


    drive_rows = conn.execute(
        text("""
            SELECT *
            FROM car_drive
            ORDER BY created_at, vehicle_id
        """)
).mappings().all()

detail_id = 1

for row in drive_rows:

    vehicle_id = row["vehicle_id"]

    car_code = vehicle_id.split("-")[0]

    inspection_no = (
        f"DRIVE-{detail_id:05d}"
    )

    drive_score = calculate_drive_score(row)

    driving_pattern = get_driving_pattern(row)

    if drive_score >= 80:
        inspection_result = "NORMAL"
        issue_message = "NORMAL"
    else:
        inspection_result = "WARNING"
        issue_message = "ACCEL_ALERT"

    inspection_drive_detail_list.append({

        "id": detail_id,
        "car_code": car_code,
        "inspection_no": inspection_no,
        "vehicle_id": vehicle_id,

        "throttle_position":
            round(float(row["throttle_position"]), 2),

        "brake_pressure":
            round(float(row["brake_pressure"]), 2),

        "steering_angle":
            round(float(row["steering_angle"]), 2),

        "drive_score": drive_score,

        "inspection_result":
            inspection_result,

        "driving_pattern":
            driving_pattern,

        "issue_message":
            issue_message,

        "created_at":
            row["created_at"]
    })

    detail_id += 1


df = pd.DataFrame(
    inspection_drive_detail_list
)

df.to_sql(
    name="inspection_drive_detail",
    con=main_engine,
    if_exists="append",
    index=False
)

print(
    f"drive_detail table 전송 완료"
)