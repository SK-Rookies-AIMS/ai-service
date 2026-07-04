# app/worker_main.py

import threading
import time
import signal
import sys

from app.db import (
    main_dispose_engine,
    sample_dispose_engine,
)

from app.scheduler.quality.drive_detail_producer import run as drive_producer
from app.scheduler.quality.status_detail_producer import run as status_producer
from app.scheduler.quality.risk_history_producer import run as history_producer
from app.scheduler.quality.risk_trend_producer import run as trend_producer
from app.scheduler.quality.process_producer import run as process_producer
#from app.scheduler.quality.stomp_client import run as stomp_client

from app.repository.quality_drive_detail_repository import run as drive_repository
from app.repository.quality_status_detail_repository import run as status_repository
from app.repository.quality_risk_history_repository import run as history_repository
from app.repository.quality_risk_trend_repository import run as trend_repository
from app.repository.quality_process_repository import run as process_repository
from app.repository.quality_summary_repository import run as summary_repository

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

    thread = threading.Thread(
        target=lambda: target(stop_event),
        daemon=True,
        name=name
    )

    thread.start()
    return thread


# =========================
# CLEAN SHUTDOWN
# =========================
def cleanup():
    global cleanup_done

    if cleanup_done:
        return

    cleanup_done = True

    print("\n🧹 Graceful Shutdown 시작...")

    # 1. stop signal 전달
    stop_event.set()

    # 2. thread 종료 대기
    print("⏳ threads join 중...")
    for t in threads:
        try:
            t.join(timeout=5)
        except Exception:
            pass

    # 3. DB connection pool 종료
    try:
        main_dispose_engine()
        sample_dispose_engine()
    except Exception as e:
        print("DB dispose error:", e)

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
    #threads.append(start_thread("STOMP Client", stomp_client))

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