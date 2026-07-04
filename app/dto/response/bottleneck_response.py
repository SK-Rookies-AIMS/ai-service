from pydantic import BaseModel, ConfigDict, Field


class BottleneckAnalysisItem(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "example": {
                "rankNo": 1,
                "processCode": "도장 (L3)",
                "delayTime": 12.4,
                "affectedVehicleCount": 128,
                "riskScore": 5.0,
                "riskLevel": "위험",
            },
        },
    )

    rank_no: int = Field(
        alias="rankNo",
        description="병목 위험 순위입니다. 1이 가장 위험도가 높은 결과입니다.",
        examples=[1],
    )
    process_code: str = Field(
        alias="processCode",
        description=(
            "공정 표시명입니다. 공정명과 설비 번호를 조합해 "
            "`도장 (L3)`, `프레스 (P4)` 형식으로 반환합니다."
        ),
        examples=["도장 (L3)"],
    )
    delay_time: float = Field(
        alias="delayTime",
        description=(
            "평균 지연 시간(초)입니다. DB에서는 원본 double 값으로 저장하고 "
            "API 응답에서는 소수 둘째 자리까지 반환합니다."
        ),
        examples=[12.4],
    )
    affected_vehicle_count: int = Field(
        alias="affectedVehicleCount",
        description="해당 병목 후보에 영향을 받은 차량 수입니다.",
        examples=[128],
    )
    risk_score: float = Field(
        alias="riskScore",
        description=(
            "Rule Engine과 Isolation Forest 결과를 결합해 산출한 병목 위험도입니다. "
            "값이 클수록 위험도가 높습니다."
        ),
        examples=[5.0],
    )
    risk_level: str = Field(
        alias="riskLevel",
        description="병목 위험도 요약입니다. 보통 또는 위험으로 반환됩니다.",
        examples=["위험"],
    )


class BottleneckAnalysisPage(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "example": {
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
                ],
                "hasNext": True,
                "nextCursor": 1,
            },
        },
    )

    most_bottleneck_process: str | None = Field(
        alias="mostBottleneckProcess",
        description="가장 병목인 공정명입니다.",
        examples=["도장"],
    )
    most_bottleneck_risk_level: str | None = Field(
        alias="mostBottleneckRiskLevel",
        description="가장 병목인 공정의 위험도 요약입니다. 보통 또는 위험으로 반환됩니다.",
        examples=["위험"],
    )
    content: list[BottleneckAnalysisItem] = Field(
        description="병목 분석 결과 목록입니다. rankNo 오름차순으로 정렬됩니다.",
    )
    has_next: bool = Field(
        alias="hasNext",
        description="다음 페이지 존재 여부입니다. false이면 다음 무한 스크롤을 호출하지 않아야 합니다.",
        examples=[True],
    )
    next_cursor: int | None = Field(
        alias="nextCursor",
        description="다음 페이지 cursor입니다. hasNext가 false이면 null입니다.",
        examples=[1],
    )
