from __future__ import annotations

import hashlib
import re
from typing import Any

PROCESS_LABELS = {
    "PRESS": "프레스",
    "BODY": "차체",
    "PAINT": "도장",
    "ASSEMBLY": "의장",
}

PROCESS_EQUIPMENT_PREFIXES = {
    "PRESS": "P",
    "BODY": "S",
    "PAINT": "L",
    "ASSEMBLY": "A",
}

NEXT_PROCESS = {
    "PRESS": "BODY",
    "BODY": "PAINT",
    "PAINT": "ASSEMBLY",
}

LINE_STATION_COUNT = 5


def equipment_number(equipment_code: Any | None) -> int | None:
    if equipment_code is None:
        return None
    match = re.search(r"(\d+)$", str(equipment_code).strip())
    if not match:
        return None
    return int(match.group(1))


def equipment_no_for_car_process(*, car_master_id: int, process_code: str) -> int:
    normalized = str(process_code or "").strip().upper()
    digest = hashlib.blake2b(
        f"{car_master_id}:{normalized}:equipment".encode("utf-8"),
        digest_size=8,
    ).digest()
    return int.from_bytes(digest, "big") % LINE_STATION_COUNT + 1


def equipment_code_for_car_process(*, car_master_id: int, process_code: str) -> str:
    normalized = str(process_code or "").strip().upper()
    equipment_no = equipment_no_for_car_process(
        car_master_id=car_master_id,
        process_code=normalized,
    )
    return f"EQ_{normalized}_{equipment_no:03d}"


def format_process_with_line(
    process_code: Any,
    equipment_code: Any | None = None,
) -> str:
    normalized = str(process_code or "").strip().upper()
    label = PROCESS_LABELS.get(normalized, normalized)
    prefix = PROCESS_EQUIPMENT_PREFIXES.get(normalized)
    equipment_no = equipment_number(equipment_code)
    if prefix and equipment_no is not None:
        return f"{label} ({prefix}{equipment_no})"
    return label
