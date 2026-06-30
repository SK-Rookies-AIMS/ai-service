from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus

from app.kafka.consumer import create_consumer
from app.kafka.topics import QUALITY_INSPECTION_PROCESS
from app.kafka.options import (PROCESS_GROUP)


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
        group_id=PROCESS_GROUP
    )

    try:

        for msg in consumer:

            row = msg.value

            process_name = row["process_name"]
            completed_count = row["completed_count"]
            waiting_count = row["waiting_count"]
            progress_rate = row["completion_rate"]
            created_at = row["created_at"]

            # 같은 날짜 + 같은 공정 존재 여부 확인
            exists = pd.read_sql(
                """
                SELECT COUNT(*) AS cnt
                FROM inspection_process
                WHERE process_name=%s
                  AND DATE(created_at)=DATE(%s)
                """,
                con=main_engine,
                params=[
                    process_name,
                    created_at
                ]
            )

            if exists.iloc[0]["cnt"] > 0:

                # UPDATE
                with main_engine.begin() as conn:

                    conn.execute(
                        """
                        UPDATE inspection_process
                        SET
                            completed_count=%s,
                            waiting_count=%s,
                            progress_rate=%s,
                            process_status=%s
                        WHERE process_name=%s
                          AND DATE(created_at)=DATE(%s)
                        """,
                        (
                            completed_count,
                            waiting_count,
                            progress_rate,

                            "COMPLETE"
                            if progress_rate == 100
                            else "RUNNING",

                            process_name,
                            created_at
                        )
                    )

                print(
                    f"[UPDATE] {process_name} "
                    f"{progress_rate}%"
                )

            else:

                # INSERT

                df = pd.DataFrame([{
                    "process_name": process_name,

                    "total_vehicle_count":
                        completed_count
                        + waiting_count,

                    "completed_count":
                        completed_count,

                    "waiting_count":
                        waiting_count,

                    "progress_rate":
                        progress_rate,

                    "process_status":
                        "COMPLETE"
                        if progress_rate == 100
                        else "RUNNING",

                    "created_at":
                        created_at
                }])

                df.to_sql(
                    name="inspection_process",
                    con=main_engine,
                    if_exists="append",
                    index=False
                )

                print(
                    f"[INSERT] {process_name} "
                    f"{progress_rate}%"
                )

    except Exception as e:
        print(f"오류 발생 : {e}")

    finally:

        print("process 종료")
        consumer.close()


if __name__ == "__main__":
    run()