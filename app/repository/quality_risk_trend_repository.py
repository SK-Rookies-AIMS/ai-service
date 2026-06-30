from sqlalchemy import create_engine, text
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

    try:

        for msg in consumer:

            row = msg.value

            exists = pd.read_sql(
                """
                SELECT COUNT(*) AS cnt
                FROM inspection_risk_trend
                WHERE risk_level=%s
                AND DATE(created_at)=DATE(%s)
                """,
                con=main_engine,
                params=[
                    row["risk_level"],
                    row["created_at"]
                ]
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

                print(
                    f"[UPDATE] "
                    f"{row['risk_level']}"
                )

            else:

                df = pd.DataFrame([row])

                df.to_sql(
                    name="inspection_risk_trend",
                    con=main_engine,
                    if_exists="append",
                    index=False
                )

                print(
                    f"[INSERT] "
                    f"{row['risk_level']}"
                )

    except Exception as e:

        print(f"오류 발생 : {e}")

    finally:

        print("risk-trend 종료")

        consumer.close()


if __name__ == "__main__":
    run()