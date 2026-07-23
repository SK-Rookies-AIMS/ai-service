from __future__ import annotations

from datetime import date as DateType

from pydantic import BaseModel, ConfigDict, Field


class AnalysisDateOption(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    date: DateType
    sample_event_id: str | None = Field(alias="sampleEventId")
