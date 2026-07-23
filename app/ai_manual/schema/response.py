# ai_manual/schema/response.py

from typing import List

from pydantic import BaseModel, Field


class ManualStep(BaseModel):
    """
    조치 절차
    """

    step: int = Field(
        description="순서"
    )

    action: str = Field(
        description="수행할 작업"
    )

    reason: str = Field(
        description="작업 이유"
    )

    warning: str = Field(
        description="주의사항"
    )


class CompletionCheck(BaseModel):
    """
    조치 완료 확인 항목
    """

    item: str = Field(
        description="확인 항목"
    )

    expected_result: str = Field(
        description="정상 상태"
    )


class ManualResponse(BaseModel):
    """
    GPT가 반환하는 최종 Manual
    """

    title: str = Field(
        description="매뉴얼 제목"
    )

    summary: str = Field(
        description="현재 상황 요약"
    )

    difficulty: str = Field(
        description="Junior 또는 Senior"
    )

    estimated_time: str = Field(
        description="예상 작업 시간"
    )

    precautions: List[str] = Field(
        default_factory=list,
        description="작업 전 주의사항"
    )

    steps: List[ManualStep] = Field(
        default_factory=list,
        description="조치 절차"
    )

    completion_check: List[CompletionCheck] = Field(
        default_factory=list,
        description="조치 완료 확인"
    )

    escalation: str = Field(
        description="상위 담당자 호출 기준"
    )

    prevention: List[str] = Field(
        default_factory=list,
        description="재발 방지 방법"
    )