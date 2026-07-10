# app/db.py

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from app.core.config import settings

main_engine = create_engine(
    settings.main_database_connection_url,
    # ===== connection pool =====
    pool_size=10,            # 기본 유지 커넥션
    max_overflow=20,         # 추가 커넥션 허용
    pool_timeout=30,         # 대기 시간
    pool_recycle=3600,       # 1시간마다 재생성 (MySQL 안정성)
    pool_pre_ping=True       # 죽은 connection 체크

    # (옵션) 멀티스레드 안정성
    # connect_args={"check_same_thread": False}  # SQLite일 때만
)

sample_engine = create_engine(
    settings.sample_database_connection_url,
    echo=False,

    # ===== connection pool =====
    pool_size=10,            # 기본 유지 커넥션
    max_overflow=20,         # 추가 커넥션 허용
    pool_timeout=30,         # 대기 시간
    pool_recycle=3600,       # 1시간마다 재생성 (MySQL 안정성)
    pool_pre_ping=True       # 죽은 connection 체크

    # (옵션) 멀티스레드 안정성
    # connect_args={"check_same_thread": False}  # SQLite일 때만
)

# =========================
# SESSION FACTORY
# =========================
Main_SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=main_engine
)

Sample_SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=sample_engine
)
# thread-safe session (Kafka / scheduler 환경에서 중요)
main_Session = scoped_session(Main_SessionLocal)
sample_Session = scoped_session(Sample_SessionLocal)

# =========================
# DEPENDENCY HELPERS
# =========================

def get_main_session():
    """
    권장 사용 방식:
    with get_session() as session:
        session.execute(...)
    """
    return main_Session()

def get_sample_session():
    """
    권장 사용 방식:
    with get_session() as session:
        session.execute(...)
    """
    return sample_Session()

def main_dispose_engine():
    """
    graceful shutdown 시 사용
    connection pool 전체 종료
    """
    try:
        main_Session.remove()
        main_engine.dispose()
        print("🗄️ DB engine disposed successfully")
    except Exception as e:
        print("DB dispose error:", e)

def sample_dispose_engine():
    """
    graceful shutdown 시 사용
    connection pool 전체 종료
    """
    try:
        sample_Session.remove()
        sample_engine.dispose()
        print("🗄️ DB engine disposed successfully")
    except Exception as e:
        print("DB dispose error:", e)