from pydantic import BaseModel, ConfigDict, Field


class DefectTransferPredictionItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    vehicle_id: str = Field(alias="vehicleId")
    car_master_id: int = Field(alias="carMasterId")
    current_process: str = Field(alias="currentProcess")
    predicted_defect_process: str | None = Field(alias="predictedDefectProcess")
    defect_probability: int = Field(alias="defectProbability")
    expected_time: str | None = Field(alias="expectedTime")
    risk_level: str = Field(alias="riskLevel")


class DefectTransferPredictionPage(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    content: list[DefectTransferPredictionItem]
    has_next: bool = Field(alias="hasNext")
    next_cursor: int | None = Field(alias="nextCursor")


class DefectTransferCauseItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    rank: int
    feature: str
    label: str
    value: str
    impact: float
    message: str


class DefectTransferCausePage(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    vehicle_id: str | None = Field(alias="vehicleId")
    car_master_id: int | None = Field(alias="carMasterId")
    predicted_defect_probability: int | None = Field(alias="predictedDefectProbability")
    risk_level: str | None = Field(alias="riskLevel")
    current_process: str | None = Field(alias="currentProcess")
    predicted_defect_process: str | None = Field(alias="predictedDefectProcess")
    transfer_probability: int | None = Field(alias="transferProbability")
    content: list[DefectTransferCauseItem]
    has_next: bool = Field(alias="hasNext")
    next_cursor: int | None = Field(alias="nextCursor")
