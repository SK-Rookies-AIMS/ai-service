from sqlalchemy import text
import pandas as pd

from app.db import main_engine
from app.kafka.consumer import create_consumer
from app.kafka.topics import QUALITY_INSPECTION_RISK_HISTORY
from app.kafka.options import RISK_HISTORY_GROUP


def run(stop_event):

    consumer = create_consumer(
        topic=QUALITY_INSPECTION_RISK_HISTORY,
        group_id=RISK_HISTORY_GROUP
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

                    # Producer에서 전달되는 값
                    inspection_type = row["inspection_type"]
                    inspection_date = row["inspection_date"]
                    risk_score = row["risk_score"]

                    # Repository에서 생성
                    start_time = f"{inspection_date} 00:00:00"
                    end_time = f"{inspection_date} 23:59:59"

                    # 같은 날짜 + 같은 검사 타입 존재 여부 확인
                    exists = pd.read_sql(
                        text("""
                            SELECT COUNT(*) AS cnt
                            FROM inspection_risk_history
                            WHERE inspection_type = :inspection_type
                              AND DATE(start_time) = DATE(:start_time)
                        """),
                        con=main_engine,
                        params={
                            "inspection_type": inspection_type,
                            "start_time": start_time
                        }
                    )

                    # 이미 존재하면 UPDATE
                    if exists.iloc[0]["cnt"] > 0:

                        with main_engine.begin() as conn:

                            conn.execute(
                                text("""
                                    UPDATE inspection_risk_history
                                    SET
                                        risk_score = :risk_score,
                                        end_time = :end_time
                                    WHERE inspection_type = :inspection_type
                                      AND DATE(start_time) = DATE(:start_time)
                                """),
                                {
                                    "risk_score": risk_score,
                                    "end_time": end_time,
                                    "inspection_type": inspection_type,
                                    "start_time": start_time
                                }
                            )

                    else:

                        # inspection_round 자동 생성
                        with main_engine.begin() as conn:

                            inspection_round = conn.execute(
                                text("""
                                    SELECT COALESCE(MAX(inspection_round), 0) + 1
                                    FROM inspection_risk_history
                                    WHERE inspection_type = :inspection_type
                                """),
                                {
                                    "inspection_type": inspection_type
                                }
                            ).scalar()

                        df = pd.DataFrame([{
                            "inspection_type": inspection_type,
                            "inspection_round": inspection_round,
                            "risk_score": risk_score,
                            "start_time": start_time,
                            "end_time": end_time
                        }])

                        df.to_sql(
                            name="inspection_risk_history",
                            con=main_engine,
                            if_exists="append",
                            index=False
                        )

    except Exception as e:

        print(f"오류 발생 : {e}")

    finally:

        print("risk-history 종료")
        consumer.close()


if __name__ == "__main__":

    import threading

    run(threading.Event())