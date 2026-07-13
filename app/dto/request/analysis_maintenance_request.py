from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field, model_validator


class AnalysisMaintenanceRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_date: date | None = Field(default=None, alias="fromDate")
    to_date: date | None = Field(default=None, alias="toDate")
    limit: int | None = Field(default=None, ge=1, le=100_000)
    reindex_es: bool = Field(default=True, alias="reindexEs")
    reset_flags: bool = Field(default=True, alias="resetFlags")
    dry_run: bool = Field(default=False, alias="dryRun")

    @model_validator(mode="after")
    def validate_date_range(self) -> "AnalysisMaintenanceRequest":
        if self.from_date and self.to_date and self.from_date > self.to_date:
            raise ValueError("fromDate must be less than or equal to toDate.")
        return self
