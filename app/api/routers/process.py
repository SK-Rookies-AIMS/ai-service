from __future__ import annotations

from datetime import date as DateType

from fastapi import APIRouter, Depends, Query

from app.dto.response import BottleneckAnalysisPage, CommonResponse
from app.service.analysis.bottleneck_service import (
    BottleneckAnalysisService,
    get_bottleneck_analysis_service,
)
from app.utils.response_utils import success_response

router = APIRouter(prefix="/api/ai/process", tags=["process"])

BottleneckAnalysisResponse = CommonResponse[BottleneckAnalysisPage]

BottleneckAnalysisDescription = """
병목 분석 조회 API입니다.

무엇을 반환하나요
- 공정별 병목 순위
- 지연 시간
- 영향 차량 수
- 위험도와 위험 점수
- 다음 페이지 여부와 다음 커서

어떤 데이터를 사용하나요
- `sampledb.manufacturing_event_json`의 `is_sent = true` 이벤트
- ES 인덱스의 `detectedAt` 기준 날짜 옵션
- 병목 결과 저장 테이블 `bottleneck_analysis_result`

조회 방식
- 기본적으로 ES를 우선 조회합니다.
- ES가 비어 있거나 실패하면 Redis 캐시를 확인하고, 캐시가 없으면 DB로 fallback합니다.
- 날짜를 주지 않으면 최신 가능한 날짜를 자동 선택합니다.
- `cursor`와 `size`로 페이지를 제어합니다.

주의 사항
- 병목은 공정 단위로 집계됩니다.
- 날짜 옵션은 ES 기준으로 생성됩니다.
- 재색인 / 백필 시 해당 날짜의 기존 결과를 다시 계산합니다.
"""

BottleneckAnalysisExample = {
    "success": True,
    "data": {
        "mostBottleneckProcess": "차체",
        "mostBottleneckRiskLevel": "HIGH",
        "content": [
            {
                "rankNo": 1,
                "processCode": "차체 (L3)",
                "delayTime": 12.4,
                "affectedVehicleCount": 128,
                "riskScore": 5.0,
                "riskLevel": "HIGH",
            },
            {
                "rankNo": 2,
                "processCode": "의장 (S12)",
                "delayTime": 9.8,
                "affectedVehicleCount": 92,
                "riskScore": 4.0,
                "riskLevel": "HIGH",
            },
        ],
        "hasNext": True,
        "nextCursor": 1,
    },
    "message": "병목 분석 조회가 완료되었습니다.",
    "timestamp": "2026-06-30T11:10:00+09:00",
}


@router.get(
    "/bottleneck",
    response_model=BottleneckAnalysisResponse,
    summary="병목 분석 결과 조회",
    description=BottleneckAnalysisDescription,
    response_description="병목 순위 페이지",
    responses={
        200: {
            "description": "병목 분석 결과 조회 성공",
            "content": {
                "application/json": {
                    "example": BottleneckAnalysisExample,
                },
            },
        },
        500: {
            "description": "Redis 캐시, ES 인덱스, DB 처리 중 오류가 발생한 경우",
        },
    },
)
def get_bottleneck_analysis(
    date: DateType | None = Query(
        default=None,
        description="조회할 날짜입니다. 미지정 시 최신 가능한 날짜를 사용합니다.",
    ),
    cursor: int | None = Query(
        default=None,
        ge=0,
        description=(
            "조회할 페이지 번호입니다. 미지정 시 0으로 처리합니다. "
            "cursor=0,size=5는 1~5건, cursor=1,size=5는 6~10건을 의미합니다."
        ),
        examples=[0],
    ),
    size: int = Query(
        default=5,
        ge=1,
        le=100,
        description="한 페이지에 반환할 병목 결과 수입니다. 기본값은 5입니다.",
        examples=[5],
    ),
    service: BottleneckAnalysisService = Depends(get_bottleneck_analysis_service),
) -> BottleneckAnalysisResponse:
    """Redis 캐시를 우선 사용해 병목 분석 페이지를 반환합니다."""
    page: BottleneckAnalysisPage = service.get_cached_realtime_bottlenecks(
        cursor=cursor,
        size=size,
        date=date,
    )
    return success_response(
        data=page,
        message="병목 분석 조회가 완료되었습니다.",
    )
