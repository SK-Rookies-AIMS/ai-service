from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus

from app.kafka.consumer import create_consumer
from app.kafka.topics import (
    QUALITY_INSPECTION_STATUS_DETAIL
)

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
    topic=QUALITY_INSPECTION_STATUS_DETAIL,
    group_id="ai-status-detail-group"
)

inspection_status_detail_list = []

try:

    print("Consumer 시작")
    print("구독 토픽 :", consumer.subscription())

    for msg in consumer:

        row = msg.value

        print("=" * 50)
        print("[Kafka 메시지 수신]")
        print(row)
        print("=" * 50)

        inspection_status_detail_list.append({
            "car_code":
                row["car_code"],

            "inspection_no":
                row["inspection_no"],

            "vehicle_id":
                row["vehicle_id"],

            "speed":
                row["speed"],

            "att":
                row["att"],

            "gear":
                row["gear"],

            "battery_voltage":
                row["battery_voltage"],

            "fuel_rate":
                row["fuel_rate"],

            "status_score":
                row["status_score"],

            "inspection_result":
                row["inspection_result"],

            "issue_message":
                row["issue_message"],

            "created_at":
                row["created_at"]
        })

        # 100건씩 저장
        if len(
            inspection_status_detail_list
        ) >= 100:

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
                f"{len(inspection_status_detail_list)}건 저장 완료"
            )

            inspection_status_detail_list.clear()

except Exception as e:

    print(f"오류 발생 : {e}")

finally:

    if inspection_status_detail_list:

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
            f"{len(inspection_status_detail_list)}건 최종 저장 완료"
        )

    consumer.close()