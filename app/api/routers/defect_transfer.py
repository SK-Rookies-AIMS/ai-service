from __future__ import annotations

from datetime import date as DateType
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

PREDICTION_DESCRIPTION = """
불량 예측 및 전이 예측 조회 API입니다.

무엇을 반환하나요
- 차량별 불량 예측 결과
- 현재 공정과 예측 공정
- 전이 확률과 위험도
- 날짜 옵션과 페이지 정보

어떤 데이터를 사용하나요
- `sampledb.manufacturing_event_json`의 SENT 이벤트
- ES 인덱스의 `predictedAt` 기준 날짜 옵션
- 불량 예측 결과 저장 테이블 `defect_transfer_prediction_result`

조회 방식
- 기본적으로 ES를 우선 조회합니다.
- ES가 비어 있거나 실패하면 Redis 캐시를 확인하고, 캐시가 없으면 DB로 fallback합니다.
- 날짜를 주지 않으면 최신 가능한 날짜를 자동 선택합니다.
- 목록은 차량별 최신 1건만 보여줍니다.
"""

CAUSE_DESCRIPTION = """
원인 분석 조회 API입니다.

무엇을 반환하나요
- 불량 예측 결과에 대한 대표 원인
- SHAP 기반 상세 원인
- 차량별 최신 원인 분석 결과

조회 방식
- 차량 ID가 있으면 해당 차량을 우선 조회합니다.
- 차량 ID가 없으면 최신 차량 기준으로 조회합니다.
- ES의 `predictedAt` 기준 날짜를 사용합니다.

응답 의미
- `main_causes`: 화면에 먼저 보여줄 대표 원인
- `detailCauses`: 추가로 확인할 상세 원인
"""

DIAGNOSTICS_DESCRIPTION = """
불량 예측 조회 상태를 점검하는 진단 API입니다.

무엇을 확인하나요
- 현재 ES 연결 상태
- 캐시 및 조회 가능 여부
- 예측 데이터의 간단한 상태 정보
"""


@router.get(
    "/predictions",
    response_model=DefectTransferPredictionResponse,
    summary="불량 예측 및 전이 예측 목록 조회",
    description=PREDICTION_DESCRIPTION,
)
def get_defect_transfer_predictions(
    date: DateType | None = Query(
        default=None,
        description="조회할 날짜입니다. 미지정 시 최신 가능한 날짜를 사용합니다.",
    ),
    cursor: int | None = Query(default=None, ge=0, examples=[0]),
    size: int = Query(default=5, ge=1, le=100, examples=[5]),
    service: DefectTransferAnalysisService = Depends(get_defect_transfer_analysis_service),
) -> DefectTransferPredictionResponse:
    return success_response(
        data=service.get_cached_predictions(
            cursor=cursor,
            size=size,
            date=date,
        ),
        message="불량 예측 및 전이 예측 목록 조회가 완료되었습니다.",
    )


@router.get(
    "/diagnostics",
    response_model=DefectTransferDiagnosticsResponse,
    summary="불량 예측 진단 정보 조회",
    description=DIAGNOSTICS_DESCRIPTION,
)
def get_defect_transfer_diagnostics(
    service: DefectTransferAnalysisService = Depends(get_defect_transfer_analysis_service),
) -> DefectTransferDiagnosticsResponse:
    return success_response(
        data=service.get_diagnostics(),
        message="불량 예측 진단 정보 조회가 완료되었습니다.",
    )


@router.get(
    "/causes",
    response_model=DefectTransferCauseResponse,
    summary="불량 예측 원인 분석 조회",
    description=CAUSE_DESCRIPTION,
)
def get_defect_transfer_causes(
    vehicle_id: str | None = Query(
        default=None,
        alias="vehicleId",
        description="차량 ID입니다. 미지정 시 최신 차량을 사용합니다.",
    ),
    date: DateType | None = Query(
        default=None,
        description="조회할 날짜입니다. 미지정 시 최신 가능한 날짜를 사용합니다.",
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
            date=date,
        ),
        message="불량 예측 원인 분석 조회가 완료되었습니다.",
    )
