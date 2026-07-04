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
Kafka raw 제조 이벤트를 기반으로 현재 제조 공정의 병목 순위를 조회합니다.

분석 입력 데이터:
- `sample_db.manufacturing_event_json` 테이블의 `is_sent = true` 이벤트만 사용합니다.
- Kafka consumer가 `factory.manufacturing.raw` 토픽에서 받은 record value의 `eventJson`을 저장한 데이터입니다.
- `manufacturing_event_id`는 `manufacturing_event_json.id`를 의미합니다.
- `analysis_status`는 병목 분석 상태가 아니라 다른 이상 탐지 로직용 값이므로 병목 분석 필터로 사용하지 않습니다.

분석 방식:
- PRESS, BODY, PAINT, ASSEMBLY 이벤트를 Kafka에서 계속 수신해 DB에 적재합니다.
- Kafka raw 이벤트가 저장되면 Rule Engine + Isolation Forest 모델로 병목 결과를 자동 갱신합니다.
- AI 병목 분석 결과는 `factory.manufacturing.analysis` 토픽으로도 발행합니다.
- 분석 결과 토픽의 Message Key는 raw 토픽과 동일하게 `carId`입니다.
- 발행 payload에는 `manufacturingAnalysisData`와 `aiAnalysisData`를 포함합니다.
- 병목 조회 시에도 저장된 raw 이벤트 기준으로 최신 결과를 다시 확인합니다.
- 결과는 `bottleneck_analysis_result` 테이블에 저장됩니다.
- `delayTime`은 DB에는 계산된 원본 double 값으로 저장하고, API 응답에서는 소수점 둘째 자리까지 반환합니다.
- `processCode`는 장비 코드 기준으로 `도장 (L1)`, `프레스 (P4)` 형식으로 반환합니다.

페이지네이션:
- `cursor`는 페이지 번호입니다. 생략하면 `0`으로 처리됩니다.
- `size=5`, `cursor=0`이면 1~5위, `cursor=1`이면 6~10위를 반환합니다.
- `hasNext=false`이면 다음 페이지를 호출하지 않아야 합니다.
- 분석 가능한 이벤트가 없거나 cursor가 마지막 페이지를 넘으면 404가 아니라 빈 `content`와 `hasNext=false`를 반환합니다.

캐시:
- Redis에 cursor/size별 결과를 캐시합니다.
- Kafka raw 이벤트가 새로 수신되면 병목 캐시를 무효화합니다.
"""

BottleneckAnalysisExample = {
    "success": True,
    "data": {
        "mostBottleneckProcess": "도장",
        "mostBottleneckRiskLevel": "위험",
        "content": [
            {
                "rankNo": 1,
                "processCode": "도장 (L3)",
                "delayTime": 12.4,
                "affectedVehicleCount": 128,
                "riskScore": 5.0,
                "riskLevel": "위험",
            },
            {
                "rankNo": 2,
                "processCode": "차체 (S12)",
                "delayTime": 9.8,
                "affectedVehicleCount": 92,
                "riskScore": 4.0,
                "riskLevel": "위험",
            },
        ],
        "hasNext": True,
        "nextCursor": 1,
    },
    "message": "병목 분석이 완료되었습니다.",
    "timestamp": "2026-06-30T11:10:00+09:00",
}


@router.get(
    "/bottleneck",
    response_model=BottleneckAnalysisResponse,
    summary="제조 공정 병목 분석 결과 조회",
    description=BottleneckAnalysisDescription,
    response_description="제조 공정 병목 순위 페이지",
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
            "description": "Redis 캐시, 모델 파일, DB 처리 중 오류가 발생한 경우",
        },
    },
)
def get_bottleneck_analysis(
    cursor: int | None = Query(
        default=None,
        ge=0,
        description=(
            "조회할 페이지 번호입니다. 생략하면 0으로 처리합니다. "
            "cursor=0,size=5는 1~5위, cursor=1,size=5는 6~10위를 반환합니다."
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
    """Redis 캐시를 통해 병목 분석 결과 페이지를 반환합니다."""
    page: BottleneckAnalysisPage = service.get_cached_realtime_bottlenecks(
        cursor=cursor,
        size=size,
    )
    return success_response(
        data=page,
        message="병목 분석이 완료되었습니다.",
    )
