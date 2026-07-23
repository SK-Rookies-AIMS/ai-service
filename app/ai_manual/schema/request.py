# ai_manual/schema/request.py

from typing import List, Optional

from pydantic import BaseModel, Field


class EquipmentInfo(BaseModel):
    """
    설비 정보
    """
    id: Optional[int] = Field(
        default=None,
        description="설비 ID"
    )

    name: Optional[str] = Field(
        default=None,
        description="설비명"
    )

    type: Optional[str] = Field(
        default=None,
        description="설비 종류"
    )


class CriticalEvent(BaseModel):
    """
    AI가 처리해야 하는 현재 Critical Event
    """

    event_id: str = Field(
        description="Kafka Event ID"
    )

    priority_score: float = Field(
        description="우선순위 점수"
    )

    risk_score: float = Field(
        description="위험도 점수"
    )

    severity: str = Field(
        description="심각도"
    )

    alert_type: str = Field(
        description="PROCESS / EQUIPMENT"
    )

    process_code: str = Field(
        description="공정 코드"
    )

    equipment: Optional[EquipmentInfo] = None

    title: str = Field(
        description="알람 제목"
    )

    description: str = Field(
        description="알람 상세 내용"
    )

    occurred_at: str = Field(
        description="발생 시간"
    )


class OperatorInfo(BaseModel):
    """
    담당자 정보
    """

    grade: str = Field(
        description="Junior 또는 Senior"
    )


class RagContext(BaseModel):
    """
    RAG 검색 결과
    """

    documents: List[str] = Field(
        default_factory=list,
        description="Vector Search 결과"
    )


class FactoryContext(BaseModel):
    """
    공장 기본 정보
    """

    system_name: str = Field(
        description="시스템 이름"
    )

    description: str = Field(
        description="스마트팩토리 설명"
    )


class ManualRequest(BaseModel):
    """
    GPT에게 전달하는 최종 Request
    """

    critical_event: CriticalEvent

    operator: OperatorInfo

    factory_context: FactoryContext

    rag_context: RagContext