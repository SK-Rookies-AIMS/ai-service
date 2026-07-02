from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus

from app.kafka.consumer import create_consumer
from app.kafka.topics import QUALITY_INSPECTION_STATUS_DETAIL
from app.kafka.options import STATUS_DETAIL_GROUP


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
        topic=QUALITY_INSPECTION_STATUS_DETAIL,
        group_id=STATUS_DETAIL_GROUP
    )

    try:

        for msg in consumer:

            row = msg.value

            vehicle_id = row["vehicle_id"]

            # 이미 저장된 차량인지 확인
            exists = pd.read_sql(
                text("""
                    SELECT COUNT(*) AS cnt
                    FROM inspection_status_detail
                    WHERE vehicle_id = :vehicle_id
                """),
                con=main_engine,
                params={"vehicle_id": vehicle_id}
            )

            if exists.iloc[0]["cnt"] > 0:

                continue

            inspection_status_detail = [{
                "car_code":
                    row["car_code"],

                "inspection_no":
                    row["inspection_no"],

                "vehicle_id":
                    vehicle_id,

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
            }]

            df = pd.DataFrame(
                inspection_status_detail
            )

            df.to_sql(
                name="inspection_status_detail",
                con=main_engine,
                if_exists="append",
                index=False
            )


    except Exception as e:

        print(f"오류 발생 : {e}")

    finally:

        print(
            "status-detail Consumer 종료"
        )

        consumer.close()


if __name__ == "__main__":
    run()