from datetime import UTC, datetime
from zoneinfo import ZoneInfo


SEOUL_TZ = ZoneInfo("Asia/Seoul")


def utc_now() -> datetime:
    """현재 UTC 시간을 반환"""
    return datetime.now(UTC)


def utc_now_iso() -> str:
    """현재 UTC 시간을 ISO-8601 문자열로 반환"""
    return utc_now().isoformat()


def seoul_now() -> datetime:
    """현재 Asia/Seoul 시간을 반환"""
    return datetime.now(SEOUL_TZ)


def seoul_now_iso() -> str:
    """현재 Asia/Seoul 시간을 ISO-8601 문자열로 반환"""
    return seoul_now().isoformat()

