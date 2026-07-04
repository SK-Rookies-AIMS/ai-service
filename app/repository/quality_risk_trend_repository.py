from sqlalchemy import text
import pandas as pd

from app.db import main_engine
from app.kafka.consumer import create_consumer
from app.kafka.topics import QUALITY_INSPECTION_RISK_TREND
from app.kafka.options import RISK_TREND_GROUP


def run(stop_event):

    consumer = create_consumer(
        topic=QUALITY_INSPECTION_RISK_TREND,
        group_id=RISK_TREND_GROUP
    )

    try:

        while not stop_event.is_set():

            # 1초마다 종료 여부 확인
            records = consumer.poll(timeout_ms=1000)

            if not records:
                continue

            for _, messages in records.items():

                for msg in messages:

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
                                    UPDATE inspection_risk_trend
                                    SET
                                        risk_count = :risk_count,
                                        risk_ratio = :risk_ratio
                                    WHERE
                                        risk_level = :risk_level
                                    AND DATE(created_at)
                                        = DATE(:created_at)
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

    import threading

    run(threading.Event())