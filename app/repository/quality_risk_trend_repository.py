from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus

from app.kafka.consumer import create_consumer
from app.kafka.topics import QUALITY_INSPECTION_RISK_TREND
from app.kafka.options import RISK_TREND_GROUP


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
        group_id=RISK_TREND_GROUP
    )

    try:

        for msg in consumer:

            row = msg.value

            exists = pd.read_sql(
                text("""
                    SELECT COUNT(*) AS cnt
                    FROM inspection_risk_trend
                    WHERE risk_level = :risk_level
                    AND DATE(created_at) = DATE(:created_at)
                """),
                con=main_engine,
                params={
                    "risk_level": row["risk_level"],
                    "created_at": row["created_at"]
                }
            )

            if exists.iloc[0]["cnt"] > 0:

                with main_engine.begin() as conn:

                    conn.execute(
                        text("""
                            UPDATE
                                inspection_risk_trend
                            SET
                                risk_count=:risk_count,
                                risk_ratio=:risk_ratio
                            WHERE
                                risk_level=:risk_level
                            AND DATE(created_at)
                                =
                                DATE(:created_at)
                        """),
                        row
                    )

            else:

                df = pd.DataFrame([row])

                df.to_sql(
                    name="inspection_risk_trend",
                    con=main_engine,
                    if_exists="append",
                    index=False
                )

    except Exception as e:

        print(f"오류 발생 : {e}")

    finally:

        print("risk-trend 종료")

        consumer.close()


if __name__ == "__main__":
    run()