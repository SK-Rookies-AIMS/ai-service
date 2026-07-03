from fastapi import APIRouter, HTTPException

from ai_manual.service.manual_service import ManualService

router = APIRouter(
    prefix="/manual",
    tags=["AI Manual"]
)

manual_service = ManualService()


@router.get("/current")
def get_current_manual():
    """
    현재 가장 높은 우선순위의 Critical Event를 조회하여
    AI에게 전달할 Request 데이터를 생성한다.
    """

    result = manual_service.get_current_manual_request()

    if result is None:
        raise HTTPException(
            status_code=404,
            detail="현재 처리할 Critical Event가 없습니다."
        )

    return result