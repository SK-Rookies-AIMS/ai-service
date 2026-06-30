from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import quote_plus
import os
import time

load_dotenv()

# Sample DB
SAMPLE_DATABASE_URL = (
    f"mysql+pymysql://{os.getenv('DB_USER')}:"
    f"{quote_plus(os.getenv('DB_PASSWORD'))}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/sampledb"
)

# Main DB
MAIN_DATABASE_URL = (
    f"mysql+pymysql://{os.getenv('DB_USER')}:"
    f"{quote_plus(os.getenv('DB_PASSWORD'))}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/maindb"
)

sample_engine = create_engine(
    SAMPLE_DATABASE_URL,
    pool_pre_ping=True
)

main_engine = create_engine(
    MAIN_DATABASE_URL,
    pool_pre_ping=True
)

last_id = 0

while True:

    # Sample DB에서 아직 복사하지 않은 차량 5건 조회
    with sample_engine.connect() as conn:

        rows = conn.execute(
            text("""
                SELECT *
                FROM car_master
                WHERE id > :last_id
                ORDER BY id
                LIMIT 1
            """),
            {"last_id": last_id}
        ).mappings().all()

    # 더 이상 복사할 데이터가 없으면 종료
    if not rows:
        print("모든 차량 복사 완료")
        break

    # Main DB로 저장
    with main_engine.begin() as conn:

        for row in rows:

            conn.execute(
                text("""
                    INSERT INTO inspection_master
                    (
                        id,
                        vehicle_id,
                        car_type,
                        engine_type,
                        car_color,
                        fuel_efficiency,
                        created_at
                    )
                    VALUES
                    (
                        :id,
                        :vehicle_id,
                        :car_type,
                        :engine_type,
                        :car_color,
                        :fuel_efficiency,
                        :created_at
                    )
                """),
                {
                    "id": row["id"],
                    "vehicle_id": row["vehicle_id"],
                    "car_type": row["car_type"],
                    "engine_type": row["engine_type"],
                    "car_color": row["car_color"],
                    "fuel_efficiency": row["fuel_efficiency"],
                    "created_at": row["created_at"]
                }
            )

            last_id = row["id"]

    print(
        f"{len(rows)}건 복사 완료 "
        f"(마지막 ID: {last_id})"
    )

    # 1초 대기
    time.sleep(1)