from typing import Any


MYSQL_SEOUL_CONNECT_ARGS: dict[str, Any] = {
    "init_command": "SET time_zone = '+09:00'",
}


def mysql_connect_args_for_seoul(database_url: str) -> dict[str, Any]:
    """MySQL의 now()가 Asia/Seoul 기준으로 동작하도록 연결 옵션을 반환"""
    if database_url.startswith("mysql"):
        return MYSQL_SEOUL_CONNECT_ARGS.copy()
    return {}
