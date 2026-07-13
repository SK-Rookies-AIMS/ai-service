from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class AnalysisMaintenanceSummary(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    analysis_name: str = Field(alias="analysisName")
    source_from: date | None = Field(default=None, alias="sourceFrom")
    source_to: date | None = Field(default=None, alias="sourceTo")
    source_count: int = Field(alias="sourceCount")
    processed_count: int = Field(alias="processedCount")
    saved_count: int = Field(alias="savedCount")
    skipped_count: int = Field(alias="skippedCount")
    failed_count: int = Field(alias="failedCount")
    deleted_count: int = Field(alias="deletedCount")
    es_reindexed_count: int = Field(alias="esReindexedCount")
    notes: list[str] = Field(default_factory=list)


class AnalysisMaintenanceResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    mode: str
    items: list[AnalysisMaintenanceSummary]
    total_source_count: int = Field(alias="totalSourceCount")
    total_processed_count: int = Field(alias="totalProcessedCount")
    total_saved_count: int = Field(alias="totalSavedCount")
    total_skipped_count: int = Field(alias="totalSkippedCount")
    total_failed_count: int = Field(alias="totalFailedCount")
    total_deleted_count: int = Field(alias="totalDeletedCount")
    total_es_reindexed_count: int = Field(alias="totalEsReindexedCount")
    extra: dict[str, Any] = Field(default_factory=dict)
