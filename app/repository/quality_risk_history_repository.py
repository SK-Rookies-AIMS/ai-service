from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus

from app.kafka.consumer import create_consumer
from app.kafka.topics import (
    QUALITY_INSPECTION_RISK_HISTORY
)


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
        topic=QUALITY_INSPECTION_RISK_HISTORY,
        group_id="ai-risk-history-group"
    )

    try:

        for msg in consumer:

            row = msg.value

            inspection_type = row["inspection_type"]
            inspection_round = row["inspection_round"]
            risk_score = row["risk_score"]
            start_time = row["start_time"]
            end_time = row["end_time"]

            # 같은 날짜 + 같은 검사 타입 존재 여부 확인
            exists = pd.read_sql(
                """
                SELECT COUNT(*) AS cnt
                FROM inspection_risk_history
                WHERE inspection_type=%s
                AND DATE(start_time)=DATE(%s)
                """,
                con=main_engine,
                params=[
                    inspection_type,
                    start_time
                ]
            )

            # 존재하면 UPDATE
            if exists.iloc[0]["cnt"] > 0:

                with main_engine.begin() as conn:

                    conn.execute(
                        text("""
                            UPDATE inspection_risk_history
                            SET
                                inspection_round=:inspection_round,
                                risk_score=:risk_score,
                                end_time=:end_time
                            WHERE inspection_type=:inspection_type
                            AND DATE(start_time)=DATE(:start_time)
                        """),
                        {
                            "inspection_round":
                                inspection_round,

                            "risk_score":
                                risk_score,

                            "end_time":
                                end_time,

                            "inspection_type":
                                inspection_type,

                            "start_time":
                                start_time
                        }
                    )

                print(
                    f"[UPDATE] "
                    f"{inspection_type} "
                    f"{risk_score}"
                )

            # 없으면 INSERT
            else:

                df = pd.DataFrame([{
                    "inspection_type":
                        inspection_type,

                    "inspection_round":
                        inspection_round,

                    "risk_score":
                        risk_score,

                    "start_time":
                        start_time,

                    "end_time":
                        end_time
                }])

                df.to_sql(
                    name="inspection_risk_history",
                    con=main_engine,
                    if_exists="append",
                    index=False
                )

                print(
                    f"[INSERT] "
                    f"{inspection_type} "
                    f"{risk_score}"
                )

    except Exception as e:

        print(f"오류 발생 : {e}")

    finally:

        print(
            "risk-history 종료"
        )

        consumer.close()


if __name__ == "__main__":
    run()