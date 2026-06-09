from typing import Any

import orjson


def to_json(data: Any) -> str:
    return orjson.dumps(data).decode("utf-8")


def from_json(data: str | bytes) -> Any:
    return orjson.loads(data)

