from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime, timedelta

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

inspection_summary_list = []

summary_id = 1

base_date = datetime.strptime(
    "2026-06-01",
    "%Y-%m-%d"
)

with sample_engine.connect() as conn:

    # 7일
    for day in range(7):

        current_date = (
            base_date +
            timedelta(days=day)
        )

        offset = day * 100

        vehicles = conn.execute(
            text("""
                SELECT vehicle_id
                FROM (
                    SELECT DISTINCT vehicle_id
                    FROM car_drive
                    ORDER BY vehicle_id
                    LIMIT 100 OFFSET :offset
                ) t
            """),
            {
                "offset": offset
            }
        ).mappings().all()

        vehicle_ids = [
            row["vehicle_id"]
            for row in vehicles
        ]

        checkpoints = [

            (25, "01:00"),
            (50, "01:15"),
            (75, "01:30"),
            (100, "01:45")

        ]

        for target_count, time_str in checkpoints:

            current_vehicle_ids = (
                vehicle_ids[:target_count]
            )

            normal_count = 0
            abnormal_count = 0

            for vehicle_id in current_vehicle_ids:

                drive_row = conn.execute(
                    text("""
                        SELECT *
                        FROM car_drive
                        WHERE vehicle_id=:vehicle_id
                        LIMIT 1
                    """),
                    {
                        "vehicle_id": vehicle_id
                    }
                ).mappings().first()

                if not drive_row:
                    continue

                score = 100

                if float(
                    drive_row["throttle_position"]
                ) > 90:
                    score -= 20

                if float(
                    drive_row["brake_pressure"]
                ) > 45:
                    score -= 20

                if abs(float(
                    drive_row["steering_angle"]
                )) > 40:
                    score -= 20

                if score >= 80:
                    normal_count += 1
                else:
                    abnormal_count += 1

            total_count = target_count

            standby_count = (
                100 - total_count
            )

            created_at = datetime.strptime(
                f"{current_date.strftime('%Y-%m-%d')} {time_str}",
                "%Y-%m-%d %H:%M"
            )

            inspection_summary_list.append({

                "id": summary_id,

                "total_count": total_count,

                "normal_count": normal_count,

                "normal_rate":
                    round(
                        normal_count /
                        total_count * 100,
                        2
                    ),

                "abnormal_count":
                    abnormal_count,

                "abnormal_rate":
                    round(
                        abnormal_count /
                        total_count * 100,
                        2
                    ),

                "stanby_count":
                    standby_count,

                "created_at":
                    created_at

            })

            summary_id += 1

df = pd.DataFrame(
    inspection_summary_list
)

df.to_sql(
    name="inspection_summary",
    con=main_engine,
    if_exists="append",
    index=False
)

print(
    f"summary table 전송 완료"
)