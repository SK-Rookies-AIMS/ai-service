from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus

from app.kafka.consumer import create_consumer
from app.kafka.topics import (
    QUALITY_INSPECTION_RISK_TREND
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
        topic=QUALITY_INSPECTION_RISK_TREND,
        group_id="ai-risk-trend-group"
    )

    inspection_risk_trend_list = []

    try:
        for msg in consumer:

            row = msg.value

            inspection_risk_trend_list.append({
                "id": row["id"],
                "risk_level":
                    row["risk_level"],

                "risk_count":
                    row["risk_count"],

                "risk_ratio":
                    row["risk_ratio"],

                "created_at":
                    row["created_at"]
            })

            if len(
                inspection_risk_trend_list
            ) >= 3:

                df = pd.DataFrame(
                    inspection_risk_trend_list
                )

                df.to_sql(
                    name="inspection_risk_trend",
                    con=main_engine,
                    if_exists="append",
                    index=False
                )

                print(
                    f"{len(inspection_risk_trend_list)}건 저장 완료"
                )

                inspection_risk_trend_list.clear()

    except Exception as e:

        print(f"오류 발생 : {e}")

    finally:

        if inspection_risk_trend_list:

            df = pd.DataFrame(
                inspection_risk_trend_list
            )

            df.to_sql(
                name="inspection_risk_trend",
                con=main_engine,
                if_exists="append",
                index=False
            )

            print(
                f"{len(inspection_risk_trend_list)}건 최종 저장 완료"
            )

        consumer.close()

if __name__ == "__main__":
    run()