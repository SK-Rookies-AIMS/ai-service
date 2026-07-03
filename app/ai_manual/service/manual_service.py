# ai_manual/service/manual_service.py

import os

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

from app.ai_manual.prompt.prompt_template import manual_prompt
from app.ai_manual.rag.vector_store import VectorStore

from app.ai_manual.repository.alert_event_repository import (
    AlertEventRepository
)

from app.ai_manual.schema.request import (
    CriticalEvent,
    EquipmentInfo,
    FactoryContext,
    ManualRequest,
    OperatorInfo,
    RagContext,
)

from app.ai_manual.schema.response import ManualResponse


class ManualService:

    def __init__(self):

        load_dotenv()

        self.repository = AlertEventRepository()

        self.vector_store = VectorStore()

        self.llm = ChatOpenAI(
            api_key=os.getenv("OPENAI_API_KEY"),
            model="gpt-4.1",
            temperature=0.2
        ).with_structured_output(
            ManualResponse
        )

    def generate_manual(
        self,
        operator_grade: str = "Junior"
    ) -> ManualResponse | None:

        event = self.repository.get_highest_priority_event()

        if event is None:
            return None

        request = self._build_request(
            event,
            operator_grade
        )

        prompt = manual_prompt.invoke(
            {
                "critical_event":
                    request.critical_event.model_dump_json(
                        indent=2,
                        ensure_ascii=False
                    ),

                "operator":
                    request.operator.model_dump_json(
                        indent=2,
                        ensure_ascii=False
                    ),

                "factory_context":
                    request.factory_context.model_dump_json(
                        indent=2,
                        ensure_ascii=False
                    ),

                "rag_context":
                    "\n".join(
                        request.rag_context.documents
                    )
            }
        )

        response = self.llm.invoke(prompt)

        return response

    def _build_request(
        self,
        event: dict,
        operator_grade: str
    ) -> ManualRequest:

        equipment = None

        if event["equipment_id"] is not None:

            equipment = EquipmentInfo(
                id=event["equipment_id"],
                name=f"Equipment-{event['equipment_id']}",
                type="Industrial Equipment"
            )

        critical_event = CriticalEvent(
            event_id=event["event_id"],
            priority_score=event["priority_score"],
            risk_score=event["risk_score"],
            severity=event["severity"],
            alert_type=event["alert_type"],
            process_code=event["process_code"],
            equipment=equipment,
            title=event["title"],
            description=event["contents"],
            occurred_at=event["created_at"]
        )

        rag_documents = self.vector_store.search(
            critical_event
        )

        return ManualRequest(

            critical_event=critical_event,

            operator=OperatorInfo(
                grade=operator_grade
            ),

            factory_context=FactoryContext(
                system_name="AIMS Smart Factory",
                description="""
자동차 제조 스마트팩토리입니다.

PRESS
BODY
PAINT
ASSEMBLY

MES
Kafka
AI-Service
Main-Service
PLC
Robot
Vision Inspection
"""
            ),

            rag_context=RagContext(
                documents=rag_documents
            )
        )