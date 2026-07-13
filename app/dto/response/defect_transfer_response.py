from __future__ import annotations

from datetime import date as DateType, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.dto.response.analysis_common import AnalysisDateOption


class DefectTransferPredictionItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    vehicle_id: str = Field(alias="vehicleId")
    car_master_id: int = Field(alias="carMasterId")
    current_process: str = Field(alias="currentProcess")
    predicted_defect_process: str | None = Field(alias="predictedDefectProcess")
    defect_probability: float = Field(alias="defectProbability")
    expected_time: str | None = Field(alias="expectedTime")
    risk_level: str = Field(alias="riskLevel")


class DefectTransferPredictionPage(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    content: list[DefectTransferPredictionItem]
    date: DateType | None = None
    from_: datetime | None = Field(default=None, alias="from")
    to: datetime | None = None
    date_options: list[AnalysisDateOption] = Field(default_factory=list, alias="dateOptions")
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
    predicted_defect_probability: float | None = Field(alias="predictedDefectProbability")
    risk_level: str | None = Field(alias="riskLevel")
    current_process: str | None = Field(alias="currentProcess")
    predicted_defect_process: str | None = Field(alias="predictedDefectProcess")
    transfer_probability: float | None = Field(alias="transferProbability")
    content: list[DefectTransferCauseItem]
    representative_cause: DefectTransferCauseItem | None = Field(default=None, alias="representativeCause")
    detail_causes: list[DefectTransferCauseItem] = Field(default_factory=list, alias="detailCauses")
    date: DateType | None = None
    from_: datetime | None = Field(default=None, alias="from")
    to: datetime | None = None
    date_options: list[AnalysisDateOption] = Field(default_factory=list, alias="dateOptions")
    has_next: bool = Field(alias="hasNext")
    next_cursor: int | None = Field(alias="nextCursor")
