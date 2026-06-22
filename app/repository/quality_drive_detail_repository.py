from sqlalchemy import create_engine
import pandas as pd
import json

from app.kafka.consumer import create_consumer
from app.kafka.topics import (
    QUALITY_INSPECTION_DRIVE_DETAIL
)

MAIN_DATABASE_URL = (
    "mysql+pymysql://admin:K.d?S|46~($$z~.J2W)~W!aMEG)-@127.0.0.1:13306/maindb"
)

main_engine = create_engine(
    MAIN_DATABASE_URL,
    pool_pre_ping=True
)

consumer = create_consumer(
    topic=QUALITY_INSPECTION_DRIVE_DETAIL,
    group_id="ai-drive-detail-group"
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


detail_id = 1
inspection_drive_detail_list = []

try:
    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Kafka Error : {msg.error()}")
            continue

        row = json.loads(
            msg.value().decode("utf-8")
        )

        vehicle_id = row["vehicle_id"]
        car_code = vehicle_id.split("-")[0]

        inspection_no = f"DRIVE-{detail_id:05d}"

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
            "throttle_position": round(
                float(row["throttle_position"]), 2
            ),
            "brake_pressure": round(
                float(row["brake_pressure"]), 2
            ),
            "steering_angle": round(
                float(row["steering_angle"]), 2
            ),
            "drive_score": drive_score,
            "inspection_result": inspection_result,
            "driving_pattern": driving_pattern,
            "issue_message": issue_message,
            "created_at": row["created_at"]
        })

        if len(inspection_drive_detail_list) >= 100:

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
                f"{len(inspection_drive_detail_list)}건 저장 완료"
            )

            inspection_drive_detail_list.clear()

        detail_id += 1

except Exception as e:
    print(f"오류 발생 : {e}")

finally:

    if inspection_drive_detail_list:

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
            f"{len(inspection_drive_detail_list)}건 최종 저장 완료"
        )

    consumer.close()