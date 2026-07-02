# app/main.py

import threading
import time
import signal
import sys

from app.scheduler.quality.drive_detail_producer import run as drive_producer
from app.scheduler.quality.status_detail_producer import run as status_producer
from app.scheduler.quality.risk_history_producer import run as history_producer
from app.scheduler.quality.risk_trend_producer import run as trend_producer
from app.scheduler.quality.process_producer import run as process_producer
from app.scheduler.quality.stomp_client import run as stomp_client

from app.repository.quality_drive_detail_repository import run as drive_repository
from app.repository.quality_status_detail_repository import run as status_repository
from app.repository.quality_risk_history_repository import run as history_repository
from app.repository.quality_risk_trend_repository import run as trend_repository
from app.repository.quality_process_repository import run as process_repository
from app.repository.quality_summary_repository import run as summary_repository

# DB engine (너 프로젝트에 있는 위치로 수정 필요)
from app.db import engine


# =========================
# STOP FLAG (핵심)
# =========================
stop_event = threading.Event()
threads = []


# =========================
# THREAD WRAPPER
# =========================
def start_thread(name, target):
    print(f"[START] {name}")

    def wrapped():
        try:
            # 각 worker가 stop_event를 받도록 확장 가능
            target(stop_event)
        except TypeError:
            # 기존 코드 호환 (stop_event 안 받는 경우)
            target()

    thread = threading.Thread(
        target=wrapped,
        daemon=True,
        name=name
    )

    thread.start()
    return thread


# =========================
# CLEAN SHUTDOWN
# =========================
def cleanup():
    print("\n🧹 Graceful Shutdown 시작...")

    # 1. stop signal 전달
    stop_event.set()

    # 2. DB connection pool 종료
    try:
        engine.dispose()
        print("🗄️ DB engine disposed")
    except Exception as e:
        print("DB dispose error:", e)

    # 3. thread 종료 대기
    print("⏳ threads join 중...")
    for t in threads:
        try:
            t.join(timeout=5)
        except Exception:
            pass

    print("✅ Shutdown 완료")


# =========================
# SIGNAL HANDLER
# =========================
def handle_exit(signum, frame):
    print("\n⚠️ 종료 신호 감지 (Ctrl+C)")
    cleanup()
    sys.exit(0)


signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


# =========================
# MAIN
# =========================
if __name__ == "__main__":

    print("=" * 60)
    print("🚀 AI-Service 시작")
    print("=" * 60)

    # =====================
    # PRODUCERS
    # =====================
    threads.append(start_thread("Drive Producer", drive_producer))
    threads.append(start_thread("Status Producer", status_producer))
    threads.append(start_thread("History Producer", history_producer))
    threads.append(start_thread("Trend Producer", trend_producer))
    threads.append(start_thread("Process Producer", process_producer))

    # =====================
    # CONSUMERS / REPOSITORY
    # =====================
    threads.append(start_thread("Drive Consumer", drive_repository))
    threads.append(start_thread("Status Consumer", status_repository))
    threads.append(start_thread("History Consumer", history_repository))
    threads.append(start_thread("Trend Consumer", trend_repository))
    threads.append(start_thread("Process Consumer", process_repository))
    threads.append(start_thread("Summary Aggregator", summary_repository))

    # =====================
    # STOMP
    # =====================
    threads.append(start_thread("STOMP Client", stomp_client))

    print("\n📡 서비스 실행 중... (Ctrl+C로 종료)")

    # =====================
    # BLOCKING LOOP (STOP EVENT 기반)
    # =====================
    try:
        while not stop_event.is_set():
            time.sleep(1)

    except KeyboardInterrupt:
        handle_exit(None, None)

    finally:
        cleanup()