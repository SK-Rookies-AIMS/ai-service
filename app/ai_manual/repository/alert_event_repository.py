from sqlalchemy import text
from app.db import main_engine


class AlertEventRepository:

    def get_highest_risk_event(self):
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
            WHERE severity = 'DANGER'
              AND action_status = 'PENDING'
              AND resolved_at IS NULL
            ORDER BY risk_score DESC
            LIMIT 1
        """)

        with main_engine.connect() as conn:
            row = conn.execute(query).mappings().first()

        if not row:
            return None

        return dict(row)
    
    def get_events(self):

        sql = """
        SELECT
            event_id,
            title,
            contents,
            severity,
            priority_score,
            risk_score,
            process_code,
            equipment_id,
            created_at
        FROM alert_event
        ORDER BY priority_score DESC
        """

        with main_engine.connect() as conn:

            result = conn.execute(text(sql))

            return [dict(row._mapping) for row in result]
        
    def get_user_role(self, user_id: int):

        sql = text("""
            SELECT role
            FROM users
            WHERE id = :user_id
        """)

        with main_engine.connect() as conn:
            role = conn.execute(
                sql,
                {"user_id": user_id}
            ).scalar()

        return role or "Junior"