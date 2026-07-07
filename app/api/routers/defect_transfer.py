from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from app.dto.response import CommonResponse
from app.dto.response.defect_transfer_response import (
    DefectTransferCausePage,
    DefectTransferPredictionPage,
)
from app.service.analysis.defect_transfer_service import (
    DefectTransferAnalysisService,
    get_defect_transfer_analysis_service,
)
from app.utils.response_utils import success_response


router = APIRouter(prefix="/api/ai/process/defect-transfer", tags=["process"])

DefectTransferPredictionResponse = CommonResponse[DefectTransferPredictionPage]
DefectTransferCauseResponse = CommonResponse[DefectTransferCausePage]
DefectTransferDiagnosticsResponse = CommonResponse[dict[str, Any]]


@router.get(
    "/predictions",
    response_model=DefectTransferPredictionResponse,
    summary="불량 전이 목록 조회",
)
def get_defect_transfer_predictions(
    cursor: int | None = Query(default=None, ge=0, examples=[0]),
    size: int = Query(default=5, ge=1, le=100, examples=[5]),
    service: DefectTransferAnalysisService = Depends(get_defect_transfer_analysis_service),
) -> DefectTransferPredictionResponse:
    return success_response(
        data=service.get_cached_predictions(
            cursor=cursor,
            size=size,
        ),
        message="불량 전이 목록 조회가 완료되었습니다.",
    )


@router.get(
    "/diagnostics",
    response_model=DefectTransferDiagnosticsResponse,
    summary="불량 전이 데이터 진단",
)
def get_defect_transfer_diagnostics(
    service: DefectTransferAnalysisService = Depends(get_defect_transfer_analysis_service),
) -> DefectTransferDiagnosticsResponse:
    return success_response(
        data=service.get_diagnostics(),
        message="불량 전이 데이터 진단이 완료되었습니다.",
    )


@router.get(
    "/causes",
    response_model=DefectTransferCauseResponse,
    summary="SHAP 기반 AI 원인 분석 조회",
)
def get_defect_transfer_causes(
    vehicle_id: str | None = Query(
        default=None,
        alias="vehicleId",
        description="car_master.vehicle_id. If omitted, the latest vehicle is used.",
    ),
    cursor: int | None = Query(default=None, ge=0, examples=[0]),
    size: int = Query(default=5, ge=1, le=100, examples=[5]),
    service: DefectTransferAnalysisService = Depends(get_defect_transfer_analysis_service),
) -> DefectTransferCauseResponse:
    return success_response(
        data=service.get_cached_cause_analysis(
            vehicle_id=vehicle_id,
            cursor=cursor,
            size=size,
        ),
        message="SHAP 기반 AI 원인 분석 조회가 완료되었습니다.",
    )
