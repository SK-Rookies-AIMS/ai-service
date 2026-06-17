from sqlalchemy import create_engine, text
from datetime import datetime
import pandas as pd
import random

DATABASE_URL = (
    "mysql+pymysql://admin:K.d?S|46~($$z~.J2W)~W!aMEG)-"
    "@127.0.0.1:13306/sampledb"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

inspection_process_list = []

process_names = [
    "Visual",
    "Function",
    "Drive",
    "Final"
]

with engine.connect() as conn:

    total_vehicle_count = conn.execute(
        text("""
            SELECT COUNT(*)
            FROM car_master
        """)
    ).scalar()

    date_rows = conn.execute(
        text("""
            SELECT
                DATE(created_at) AS process_date,
                COUNT(*) AS vehicle_count
            FROM car_master
            GROUP BY DATE(created_at)
            ORDER BY process_date
        """)
    ).mappings().all()

    process_id = 1

for date_row in date_rows:

    process_date = date_row["process_date"]

    total_vehicle_count = date_row["vehicle_count"]

    for process_name in process_names:

        completed_count = random.randint(
            int(total_vehicle_count * 0.5),
            total_vehicle_count
        )

        waiting_count = (
            total_vehicle_count
            - completed_count
        )

        progress_rate = round(
            completed_count
            / total_vehicle_count
            * 100,
            0
        )

        if progress_rate >= 90:
            process_status = "COMPLETE"

        elif progress_rate >= 70:
            process_status = "RUNNING"

        else:
            process_status = "WAIT"

        inspection_process = {
            "id": process_id,
            "process_name": process_name,
            "total_vehicle_count": total_vehicle_count,
            "completed_count": completed_count,
            "waiting_count": waiting_count,
            "progress_rate": progress_rate,
            "process_status": process_status,
            "created_at": process_date
        }

        inspection_process_list.append(
            inspection_process
        )

        process_id += 1

csv_file = "inspection_process.csv"
df = pd.DataFrame(
    inspection_process_list,
    columns=[
        "id",
        "process_name",
        "total_vehicle_count",
        "completed_count",
        "waiting_count",
        "progress_rate",
        "process_status",
        "created_at"
    ]
)

df.to_csv(
    "inspection_process.csv",
    index=False,
    encoding="utf-8-sig"
)

print(df.head(20))
print()
print(f"CSV 저장 완료: {csv_file}")
print(f"총 {len(df)}건")