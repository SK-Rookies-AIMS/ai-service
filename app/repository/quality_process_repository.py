from sqlalchemy import create_engine
import pandas as pd
import random
from dotenv import load_dotenv
import os

from app.kafka.consumer import create_consumer
from app.kafka.topics import QUALITY_INSPECTION_PROCESS

MAIN_DATABASE_URL = (
    os.getenv("MAIN_DATABASE_END")
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
    print("Consumer 시작")
    print("구독 토픽", consumer.subscription())
    for msg in consumer:

        row = msg.value

        print(f"Kafka 수신 : {row}")

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

        print(
            f"{len(inspection_process_list)}건 저장 완료"
        )

        inspection_process_list.clear()

except Exception as e:
    print(f"오류 발생 : {e}")

finally:
    consumer.close()