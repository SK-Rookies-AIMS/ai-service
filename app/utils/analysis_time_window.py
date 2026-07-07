from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

from app.utils.datetime_utils import seoul_now


@dataclass(frozen=True)
class AnalysisTimeWindow:
    selected_date: date | None
    start_at: datetime | None
    end_at: datetime | None


def build_analysis_time_window(
    *,
    date_value: date | None = None,
    from_at: datetime | None = None,
    to_at: datetime | None = None,
    end_at: datetime | None = None,
    default_to_today: bool = True,
) -> AnalysisTimeWindow:
    if date_value is not None:
        start_at = datetime.combine(date_value, time.min)
        return AnalysisTimeWindow(
            selected_date=date_value,
            start_at=start_at,
            end_at=start_at + timedelta(days=1),
        )

    normalized_from = _normalize_datetime(from_at)
    normalized_to = _normalize_datetime(to_at)
    normalized_end = _normalize_datetime(end_at)
    final_end = normalized_to or normalized_end

    if normalized_from is None and final_end is None and default_to_today:
        today = seoul_now().date()
        start_at = datetime.combine(today, time.min)
        return AnalysisTimeWindow(
            selected_date=today,
            start_at=start_at,
            end_at=start_at + timedelta(days=1),
        )

    return AnalysisTimeWindow(
        selected_date=None,
        start_at=normalized_from,
        end_at=final_end,
    )


def _normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.replace(tzinfo=None)
    return value
