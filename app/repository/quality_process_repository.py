from sqlalchemy import create_engine
import pandas as pd
import random
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus

from app.kafka.consumer import create_consumer
from app.kafka.topics import QUALITY_INSPECTION_PROCESS

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
        topic=QUALITY_INSPECTION_PROCESS,
        group_id="ai-process-group"
    )

    process_names = [
        "Visual",
        "Function",
        "Drive",
        "Final"
    ]

    process_id = 1
    inspection_process_list = []

    try:
        for msg in consumer:

            row = msg.value

            process_date = row["process_date"]
            total_vehicle_count = row["vehicle_count"]

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

                inspection_process_list.append({
                    "id": process_id,
                    "process_name": process_name,
                    "total_vehicle_count": total_vehicle_count,
                    "completed_count": completed_count,
                    "waiting_count": waiting_count,
                    "progress_rate": progress_rate,
                    "process_status": process_status,
                    "created_at": process_date
                })

                process_id += 1

            # 날짜 하나당 4개 공정 생성
            df = pd.DataFrame(
                inspection_process_list
            )

            df.to_sql(
                name="inspection_process",
                con=main_engine,
                if_exists="append",
                index=False
            )

            inspection_process_list.clear()

    except Exception as e:
        print(f"오류 발생 : {e}")

    finally:
        print(
            f"process 최종 저장 완료"
        )
        consumer.close()

if __name__ == "__main__":
    run()