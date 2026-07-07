import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

from app.ai_manual.prompt.prompt_template import manual_prompt
from app.ai_manual.rag.vector_store import VectorStore
from app.ai_manual.repository.alert_event_repository import AlertEventRepository

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
            model="gpt-5-mini",
            temperature=0.2
        ).with_structured_output(ManualResponse)

    # ======================================================
    # MAIN ENTRY
    # ======================================================
    def generate_manual(
        self,
        user_id: int
    ) -> ManualResponse | None:

        # 1. EVENT FETCH
        event = self.repository.get_highest_risk_event()

        if event is None:
            return None

        # 2. BUILD STRONG REQUEST OBJECT
        operator_grade = self.repository.get_user_role(user_id)

        if operator_grade is None:
            operator_grade = "Junior"
        request = self._build_request(event, operator_grade)

        # 3. PROMPT BUILD
        prompt = manual_prompt.invoke({
            "critical_event": self._format_json(request.critical_event),
            "operator": self._format_json(request.operator),
            "factory_context": self._format_json(request.factory_context),
            "rag_context": self._format_rag(request.rag_context)
        })

        # 4. LLM CALL
        response = self.llm.invoke(prompt)

        return {
            "event": {
                "eventId": event["event_id"],
                "title": event["title"],
                "severity": event["severity"],
                "process": event["process_code"],
                "equipmentId": event["equipment_id"],
                "riskScore": event["risk_score"],
            },
            "manual": response.model_dump()
        }

    # ======================================================
    # REQUEST BUILDER
    # ======================================================
    def _build_request(
        self,
        event: dict,
        operator_grade: str
    ) -> ManualRequest:

        # --------------------------
        # 1. Equipment mapping (safe)
        # --------------------------
        equipment = None

        if event.get("equipment_id") is not None:
            equipment = EquipmentInfo(
                id=event["equipment_id"],
                name=f"EQ-{event['equipment_id']}",  # fallback only
                type="Industrial Equipment"
            )

        # --------------------------
        # 2. Critical Event DTO
        # --------------------------
        critical_event = CriticalEvent(
            event_id=event["event_id"],
            priority_score=float(event.get("priority_score") or 0),
            risk_score=float(event.get("risk_score") or 0),
            severity=event["severity"],
            alert_type=event["alert_type"],
            process_code=event["process_code"],
            equipment=equipment,
            title=event["title"],
            description=event["contents"],
            occurred_at=str(event["created_at"])
        )

        # --------------------------
        # 3. RAG SEARCH
        # --------------------------
        rag_documents = self.vector_store.search(critical_event)

        # --------------------------
        # 4. FINAL REQUEST
        # --------------------------
        return ManualRequest(
            critical_event=critical_event,
            operator=OperatorInfo(
                grade=operator_grade
            ),
            factory_context=FactoryContext(
                system_name="AIMS Smart Factory",
                description=(
                    "자동차 제조 스마트팩토리 시스템\n\n"
                    "공정: PRESS / BODY / PAINT / ASSEMBLY\n"
                    "시스템: Kafka / AI-Service / Main-Service / PLC / Robot / Vision"
                )
            ),
            rag_context=RagContext(
                documents=rag_documents
            )
        )

    # ======================================================
    # HELPERS
    # ======================================================
    def _format_json(self, obj) -> str:
        """
        LLM readability 개선용 JSON formatter
        """
        return obj.model_dump_json(
            indent=2,
            ensure_ascii=False
        )

    def _format_rag(self, rag: RagContext) -> str:
        """
        RAG 문서 readable string 변환
        """
        return "\n".join(
            f"- {doc}" for doc in rag.documents
        )