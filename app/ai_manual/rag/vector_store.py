# ai_manual/rag/vector_store.py

from typing import List

from app.ai_manual.schema.request import CriticalEvent


class VectorStore:
    """
    RAG 검색 클래스

    현재는 Mock 데이터를 반환하며,
    추후 FAISS / Chroma / OpenSearch 등으로
    교체할 수 있도록 인터페이스 역할을 수행한다.
    """

    def __init__(self):
        pass

    def search(
        self,
        event: CriticalEvent,
        top_k: int = 5
    ) -> List[str]:
        """
        Critical Event를 기반으로
        관련 문서를 검색한다.
        """

        documents = []

        # 공정 정보
        documents.append(
            f"{event.process_code} 공정은 자동차 제조의 핵심 공정입니다."
        )

        # 설비 정보
        if event.equipment is not None:
            documents.append(
                f"{event.equipment.name} 설비 매뉴얼입니다."
            )

        # 이벤트 정보
        documents.append(
            f"{event.title} 발생 시 안전 절차를 우선 수행합니다."
        )

        # 위험도
        documents.append(
            f"Risk Score는 {event.risk_score}입니다."
        )

        # 심각도
        documents.append(
            f"Severity는 {event.severity}입니다."
        )

        return documents[:top_k]