from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

class BottleneckAnalysisItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    rank_no: int = Field(alias="rankNo")
    process_code: str = Field(alias="processCode")
    delay_time: float = Field(alias="delayTime")
    affected_vehicle_count: int = Field(alias="affectedVehicleCount")
    risk_score: float = Field(alias="riskScore")
    risk_level: str = Field(alias="riskLevel")


class BottleneckAnalysisPage(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    most_bottleneck_process: str | None = Field(alias="mostBottleneckProcess")
    most_bottleneck_risk_level: str | None = Field(alias="mostBottleneckRiskLevel")
    content: list[BottleneckAnalysisItem]
    has_next: bool = Field(alias="hasNext")
    next_cursor: int | None = Field(alias="nextCursor")
