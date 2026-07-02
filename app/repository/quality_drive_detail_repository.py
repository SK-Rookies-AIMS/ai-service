from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus

from app.kafka.consumer import create_consumer
from app.kafka.topics import QUALITY_INSPECTION_DRIVE_DETAIL
from app.kafka.options import DRIVE_DETAIL_GROUP

def run():

    load_dotenv()

    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = quote_plus(
        os.getenv("DB_PASSWORD")
    )
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    MAIN_DB_NAME = os.getenv("MAIN_DB_NAME")

    MAIN_DATABASE_URL = (
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{MAIN_DB_NAME}"
    )

    main_engine = create_engine(
        MAIN_DATABASE_URL,
        pool_pre_ping=True
    )

    consumer = create_consumer(
        topic=QUALITY_INSPECTION_DRIVE_DETAIL,
        group_id=DRIVE_DETAIL_GROUP
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

    try:

        for msg in consumer:

            row = msg.value

            if not row.get("created_at"):
                print(
                    f"폐기 차량 제외 : "
                    f"{row.get('vehicle_id')}"
                )
                continue

            vehicle_id = row["vehicle_id"]

            # ==========================
            # 중복 저장 방지
            # ==========================
            exists = pd.read_sql(
                text("""
                    SELECT COUNT(*) AS cnt
                    FROM inspection_drive_detail
                    WHERE vehicle_id = :vehicle_id
                """),
                con=main_engine,
                params={
                    "vehicle_id": vehicle_id
                }
            )

            if exists.iloc[0]["cnt"] > 0:

                print(
                    f"{vehicle_id} 이미 저장됨"
                )

                continue
            # ==========================

            car_code = vehicle_id.split("-")[0]

            inspection_no = (
                f"DRIVE-{detail_id:05d}"
            )

            drive_score = calculate_drive_score(row)

            driving_pattern = (
                get_driving_pattern(row)
            )

            if drive_score >= 80:

                inspection_result = "NORMAL"
                issue_message = "NORMAL"

            else:

                inspection_result = "WARNING"
                issue_message = "ACCEL_ALERT"

            df = pd.DataFrame([{
                "car_code": car_code,
                "inspection_no": inspection_no,
                "vehicle_id": vehicle_id,

                "throttle_position":
                    round(
                        float(
                            row["throttle_position"]
                        ),
                        2
                    ),

                "brake_pressure":
                    round(
                        float(
                            row["brake_pressure"]
                        ),
                        2
                    ),

                "steering_angle":
                    round(
                        float(
                            row["steering_angle"]
                        ),
                        2
                    ),

                "drive_score":
                    drive_score,

                "inspection_result":
                    inspection_result,

                "driving_pattern":
                    driving_pattern,

                "issue_message":
                    issue_message,

                "created_at":
                    row["created_at"]
            }])

            df.to_sql(
                name="inspection_drive_detail",
                con=main_engine,
                if_exists="append",
                index=False
            )

            detail_id += 1

    except Exception as e:

        print(
            f"오류 발생 : {e}"
        )

    finally:

        consumer.close()


if __name__ == "__main__":
    run()