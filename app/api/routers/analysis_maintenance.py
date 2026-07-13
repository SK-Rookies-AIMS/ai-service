from __future__ import annotations

from datetime import date as DateType

from fastapi import APIRouter, Depends
from app.dto.request import AnalysisMaintenanceRequest
from app.dto.response import AnalysisMaintenanceResponse, CommonResponse
from app.service.analysis.analysis_maintenance_service import AnalysisMaintenanceService
from app.utils.datetime_utils import seoul_now
from app.utils.response_utils import success_response


router = APIRouter(prefix="/api/ai/admin/analysis", tags=["admin-analysis"])


def _resolve_dates(payload: AnalysisMaintenanceRequest) -> tuple[DateType, DateType]:
    start = payload.from_date or payload.to_date or seoul_now().date()
    end = payload.to_date or payload.from_date or start
    return start, end


def _service() -> AnalysisMaintenanceService:
    return AnalysisMaintenanceService()


@router.post(
    "/backfill/bottleneck",
    response_model=CommonResponse[AnalysisMaintenanceResponse],
    summary="병목 백필 및 ES 재반영",
)
def backfill_bottleneck(
    payload: AnalysisMaintenanceRequest,
    service: AnalysisMaintenanceService = Depends(_service),
) -> CommonResponse[AnalysisMaintenanceResponse]:
    from_date, to_date = _resolve_dates(payload)
    response = service.backfill_bottleneck(
        from_date=from_date,
        to_date=to_date,
        reindex_es=payload.reindex_es,
        reset_flags=payload.reset_flags,
        dry_run=payload.dry_run,
    )
    return success_response(data=response, message="병목 백필이 완료되었습니다.")


@router.post(
    "/backfill/defect-transfer",
    response_model=CommonResponse[AnalysisMaintenanceResponse],
    summary="불량 전이/SHAP 백필 및 ES 재반영",
)
def backfill_defect_transfer(
    payload: AnalysisMaintenanceRequest,
    service: AnalysisMaintenanceService = Depends(_service),
) -> CommonResponse[AnalysisMaintenanceResponse]:
    from_date, to_date = _resolve_dates(payload)
    response = service.backfill_defect_transfer(
        from_date=from_date,
        to_date=to_date,
        reindex_es=payload.reindex_es,
        reset_flags=payload.reset_flags,
        dry_run=payload.dry_run,
    )
    return success_response(data=response, message="불량 전이 백필이 완료되었습니다.")


@router.post(
    "/reindex/bottleneck",
    response_model=CommonResponse[AnalysisMaintenanceResponse],
    summary="병목 ES 재색인",
)
def reindex_bottleneck(
    payload: AnalysisMaintenanceRequest,
    service: AnalysisMaintenanceService = Depends(_service),
) -> CommonResponse[AnalysisMaintenanceResponse]:
    from_date, to_date = _resolve_dates(payload)
    response = service.reindex_bottleneck(
        from_date=from_date,
        to_date=to_date,
        dry_run=payload.dry_run,
    )
    return success_response(data=response, message="병목 ES 재색인이 완료되었습니다.")


@router.post(
    "/reindex/defect-transfer",
    response_model=CommonResponse[AnalysisMaintenanceResponse],
    summary="불량 전이/SHAP ES 재색인",
)
def reindex_defect_transfer(
    payload: AnalysisMaintenanceRequest,
    service: AnalysisMaintenanceService = Depends(_service),
) -> CommonResponse[AnalysisMaintenanceResponse]:
    from_date, to_date = _resolve_dates(payload)
    response = service.reindex_defect_transfer(
        from_date=from_date,
        to_date=to_date,
        dry_run=payload.dry_run,
    )
    return success_response(data=response, message="불량 전이 ES 재색인이 완료되었습니다.")
