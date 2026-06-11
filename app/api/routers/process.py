from fastapi import APIRouter, Depends, Query

from app.dto.response import BottleneckAnalysisPage, CommonResponse
from app.service.analysis.bottleneck_service import (
    BottleneckAnalysisService,
    get_bottleneck_analysis_service,
)
from app.utils.response_utils import success_response

router = APIRouter(prefix="/api/process", tags=["process"])


@router.get("/bottleneck")
def get_bottleneck_analysis(
    cursor: int | None = Query(default=None, ge=0),
    size: int = Query(default=10, ge=1, le=100),
    service: BottleneckAnalysisService = Depends(get_bottleneck_analysis_service),
) -> CommonResponse[dict]:
    """Redis 캐시를 통해 병목 분석 결과 한 페이지를 반환"""
    page: BottleneckAnalysisPage = service.get_cached_realtime_bottlenecks(
        cursor=cursor,
        size=size,
    )
    return success_response(
        data=page.model_dump(by_alias=True),
        message="병목 분석이 완료되었습니다.",
    )
