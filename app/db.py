# app/db.py

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

# =========================
# DATABASE URL
# =========================
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = quote_plus(
    os.getenv("DB_PASSWORD")
)
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
MAIN_DB_NAME = os.getenv("MAIN_DB_NAME")

DATABASE_URL = (
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{MAIN_DB_NAME}"
)

if not DATABASE_URL:
    raise Exception("MAIN_DATABASE_URL is not set")


# =========================
# ENGINE (SINGLETON)
# =========================
engine = create_engine(
    DATABASE_URL,
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
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# thread-safe session (Kafka / scheduler 환경에서 중요)
Session = scoped_session(SessionLocal)


# =========================
# DEPENDENCY HELPERS
# =========================

def get_session():
    """
    권장 사용 방식:
    with get_session() as session:
        session.execute(...)
    """
    return Session()


def dispose_engine():
    """
    graceful shutdown 시 사용
    connection pool 전체 종료
    """
    try:
        Session.remove()
        engine.dispose()
        print("🗄️ DB engine disposed successfully")
    except Exception as e:
        print("DB dispose error:", e)