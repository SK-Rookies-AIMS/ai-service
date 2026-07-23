from sqlalchemy import text
import time

from app.db import main_engine


def run(stop_event):

    last_total_count = -1

    try:

        while not stop_event.is_set():

            with main_engine.begin() as conn:

                result = conn.execute(
                    text("""
                        SELECT
                            COUNT(*) AS total_count,

                            SUM(
                                CASE
                                    WHEN UPPER(inspection_result) = 'NORMAL'
                                    THEN 1
                                    ELSE 0
                                END
                            ) AS normal_count,

                            SUM(
                                CASE
                                    WHEN UPPER(inspection_result) = 'WARNING'
                                    THEN 1
                                    ELSE 0
                                END
                            ) AS abnormal_count,

                            MAX(created_at) AS created_at

                        FROM inspection_drive_detail

                        WHERE DATE(created_at) = (
                            SELECT DATE(MAX(created_at))
                            FROM inspection_drive_detail
                        )
                    """)
                ).mappings().first()

                total_count = result["total_count"] or 0

                # 데이터 변화가 없으면 건너뜀
                if total_count == last_total_count:
                    time.sleep(1)
                    continue

                normal_count = result["normal_count"] or 0
                abnormal_count = result["abnormal_count"] or 0

                standby_count = max(0, 100 - total_count)

                if total_count == 0:

                    normal_rate = 0
                    abnormal_rate = 0

                else:

                    normal_rate = round(
                        normal_count / total_count * 100,
                        2
                    )

                    abnormal_rate = round(
                        abnormal_count / total_count * 100,
                        2
                    )

                created_at = result["created_at"]

                # inspection_summary가 비어있는지 확인
                exists = conn.execute(
                    text("""
                        SELECT COUNT(*)
                        FROM inspection_summary
                    """)
                ).scalar()

                if exists == 0:

                    conn.execute(
                        text("""
                            INSERT INTO inspection_summary
                            (
                                total_count,
                                normal_count,
                                normal_rate,
                                abnormal_count,
                                abnormal_rate,
                                standby_count,
                                created_at,
                                updated_at
                            )
                            VALUES
                            (
                                :total_count,
                                :normal_count,
                                :normal_rate,
                                :abnormal_count,
                                :abnormal_rate,
                                :standby_count,
                                :created_at,
                                NOW()
                            )
                        """),
                        {
                            "total_count": total_count,
                            "normal_count": normal_count,
                            "normal_rate": normal_rate,
                            "abnormal_count": abnormal_count,
                            "abnormal_rate": abnormal_rate,
                            "standby_count": standby_count,
                            "created_at": created_at
                        }
                    )

                else:

                    conn.execute(
                        text("""
                            UPDATE inspection_summary
                            SET
                                total_count = :total_count,
                                normal_count = :normal_count,
                                normal_rate = :normal_rate,
                                abnormal_count = :abnormal_count,
                                abnormal_rate = :abnormal_rate,
                                standby_count = :standby_count,
                                created_at = :created_at,
                                updated_at = NOW()
                        """),
                        {
                            "total_count": total_count,
                            "normal_count": normal_count,
                            "normal_rate": normal_rate,
                            "abnormal_count": abnormal_count,
                            "abnormal_rate": abnormal_rate,
                            "standby_count": standby_count,
                            "created_at": created_at
                        }
                    )

                last_total_count = total_count

            # stop_event를 고려하여 1초 대기
            stop_event.wait(1)

    except Exception as e:

        print(f"Summary Aggregator 오류 발생 : {e}")

    finally:

        print("summary 종료")


if __name__ == "__main__":
    import threading

    run(threading.Event())