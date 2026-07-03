# ai_manual/repository/alert_event_repository.py

import os
from decimal import Decimal
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


class AlertEventRepository:

    def __init__(self):
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

        self.engine = create_engine(
            MAIN_DATABASE_URL,
            pool_pre_ping=True
        )

    def get_highest_priority_event(self):
        """
        현재 처리해야 하는 가장 높은 우선순위의
        Critical Event를 조회한다.

        조건
        - action_status = PENDING
        - resolved_at IS NULL
        - priority_score DESC
        - created_at ASC
        """

        query = text("""
            SELECT
                log_no,
                event_id,
                alert_type,
                process_code,
                equipment_id,
                event_key,
                risk_score,
                occurrence_score,
                detection_score,
                priority_score,
                severity,
                title,
                contents,
                action_by,
                action_status,
                reason,
                score_calculated_at,
                created_at,
                resolved_at
            FROM alert_event
            WHERE action_status = 'PENDING'
              AND resolved_at IS NULL
            ORDER BY priority_score DESC,
                     created_at ASC
            LIMIT 1
        """)

        with self.engine.connect() as conn:
            row = conn.execute(query).mappings().first()

        if row is None:
            return None

        return self._convert(row)

    def _convert(self, row):
        """
        SQLAlchemy RowMapping -> dict
        Decimal과 datetime을 JSON 직렬화 가능한 형태로 변환
        """

        result = {}

        for key, value in row.items():

            if isinstance(value, Decimal):
                result[key] = float(value)

            elif hasattr(value, "isoformat"):
                result[key] = value.isoformat()

            else:
                result[key] = value

        return result