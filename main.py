# app/main.py
from app.main import app

import threading
import time

from app.scheduler.quality.drive_detail_producer import run as drive_producer
from app.scheduler.quality.status_detail_producer import run as status_producer
from app.scheduler.quality.risk_history_producer import run as history_producer
from app.scheduler.quality.risk_trend_producer import run as trend_producer
from app.scheduler.quality.process_producer import run as process_producer
from app.scheduler.quality.stomp_client import run as stomp_client

from app.repository.quality_drive_detail_repository import (
    run as drive_repository
)

from app.repository.quality_status_detail_repository import (
    run as status_repository
)

from app.repository.quality_risk_history_repository import (
    run as history_repository
)

from app.repository.quality_risk_trend_repository import (
    run as trend_repository
)

from app.repository.quality_process_repository import (
    run as process_repository
)

from app.repository.quality_summary_repository import (
    run as summary_repository
)

def start_thread(name, target):

    print(f"[START] {name} 스레드 시작")

    thread = threading.Thread(
        target=target,
        daemon=True,
        name=name
    )

    thread.start()

    return thread


if __name__ == "__main__":

    print("=" * 60)
    print("AI-Service 시작")
    print("=" * 60)

    threads = []

    # Producer
    threads.append(
        start_thread(
            "Drive Detail Kafka 전송",
            drive_producer
        )
    )

    threads.append(
        start_thread(
            "Status Detail Kafka 전송",
            status_producer
        )
    )

    threads.append(
        start_thread(
            "Risk History Kafka 전송",
            history_producer
        )
    )

    threads.append(
        start_thread(
            "Risk Trend Kafka 전송",
            trend_producer
        )
    )

    threads.append(
        start_thread(
            "Process Kafka 전송",
            process_producer
        )
    )

    # Consumer
    threads.append(
        start_thread(
            "Drive Detail 메시지 처리",
            drive_repository
        )
    )

    threads.append(
        start_thread(
            "Status Detail 메시지 처리",
            status_repository
        )
    )

    threads.append(
        start_thread(
            "Risk History 메시지 처리",
            history_repository
        )
    )

    threads.append(
        start_thread(
            "Risk Trend 메시지 처리",
            trend_repository
        )
    )

    threads.append(
        start_thread(
            "Process 메시지 처리",
            process_repository
        )
    )

    threads.append(
        start_thread(
            "Inspection Summary 실시간 집계",
            summary_repository
        )
    )

    threads.append(
        start_thread(
            "WebSocket STOMP Listener",
            stomp_client
        )
    )

    print("\n실행 중인 서비스")
    print("- Drive Detail Kafka 전송")
    print("- Status Detail Kafka 전송")
    print("- Risk History Kafka 전송")
    print("- Risk Trend Kafka 전송")
    print("- Process Kafka 전송")
    print("- summary data 전송")

    print("- Drive Detail 메시지 처리")
    print("- Status Detail 메시지 처리")
    print("- Risk History 메시지 처리")
    print("- Risk Trend 메시지 처리")
    print("- Process 메시지 처리")

    print("\nAI-Service 정상 실행 완료")
    print("=" * 60)

    while True:
        time.sleep(60)