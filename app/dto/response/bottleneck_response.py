from pydantic import BaseModel, ConfigDict, Field


class BottleneckAnalysisItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    rank_no: int = Field(alias="rankNo")
    process_code: str = Field(alias="processCode")
    station_code: str = Field(alias="stationCode")
    avg_delay_time: float = Field(alias="avgDelayTime")
    affected_vehicle_count: int = Field(alias="affectedVehicleCount")
    risk_score: int = Field(alias="riskScore")


class BottleneckAnalysisPage(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    content: list[BottleneckAnalysisItem]
    has_next: bool = Field(alias="hasNext")
    next_cursor: int | None = Field(alias="nextCursor")

