from typing import Generic, TypeVar

from pydantic import BaseModel, Field

DataT = TypeVar("DataT")


class CommonResponse(BaseModel, Generic[DataT]):
    success: bool = Field(description="요청 성공 여부")
    data: DataT | None = Field(default=None, description="응답 데이터")
    message: str = Field(description="응답 메시지")
    timestamp: str = Field(description="응답 생성 시간")

