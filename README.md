# AI Service

FastAPI 기반 AI 서비스 API Gateway / Orchestrator입니다.

`ai-services` Namespace 내에서 Pod 형태로 배포되며, ChatGPT API, AI 매뉴얼 API, colleague-skill(dot-skill) 등 외부 AI/API 및 내부 지식 서비스를 통합 연계합니다.

## 기술 스택

- Python
- FastAPI
- Uvicorn
- Kubernetes Pod 배포

## 프로젝트 구조

```text
ai-service/
├─ app/
│  ├─ core/
│  ├─ api/
│  │  └─ routers/
│  ├─ service/
│  │  ├─ orchestrator/
│  │  ├─ llm/
│  │  └─ analysis/
│  ├─ ml/
│  │  ├─ datasets/
│  │  ├─ preprocessing/
│  │  ├─ features/
│  │  ├─ training/
│  │  ├─ evaluation/
│  │  ├─ inference/
│  │  ├─ registry/
│  │  └─ artifacts/
│  ├─ kafka/
│  ├─ dto/
│  │  ├─ request/
│  │  └─ response/
│  ├─ utils/
│  └─ main.py
├─ main.py
├─ requirements.txt
└─ README.md
```

### 패키지 역할

- `app/core`: 환경 설정, 공통 설정, 애플리케이션 초기화 구성
- `app/api`: FastAPI API 라우터 집계 및 HTTP API 구성
- `app/api/routers`: 기능별 FastAPI 라우터 모듈
- `app/service/orchestrator`: ChatGPT API, AI 매뉴얼 API, colleague-skill 연계 흐름 제어
- `app/service/llm`: LLM API 연동 및 프롬프트 처리
- `app/service/analysis`: 요청 분석, 응답 후처리, 분석 로직
- `app/ml/datasets`: 학습 및 평가 데이터셋 관리
- `app/ml/preprocessing`: 데이터 전처리 로직
- `app/ml/features`: 피처 생성 및 변환 로직
- `app/ml/training`: 모델 학습 로직
- `app/ml/evaluation`: 모델 평가 로직
- `app/ml/inference`: 모델 추론 로직
- `app/ml/registry`: 모델 버전 및 메타데이터 관리
- `app/ml/artifacts`: 모델 산출물 및 관련 파일 관리
- `app/kafka`: Kafka 메시지 발행 및 구독 연동
- `app/dto/request`: 요청 DTO 정의
- `app/dto/response`: 응답 DTO 정의
- `app/utils`: 공통 유틸리티 함수


## 가상환경 생성 및 실행

PowerShell 기준:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

가상환경이 정상적으로 활성화되면 프롬프트 앞에 `(venv)`가 표시됩니다.

```powershell
(venv) PS C:\rookies\aims\ai-service>
```

의존성 설치:

```powershell
pip install -r requirements.txt
```

`requirements.txt`에는 FastAPI, LLM/API 연동, Kafka, 데이터 처리, ML 관련 의존성을 모두 포함합니다.

Windows에서 Python 3.14를 사용하는 경우 일부 패키지의 사전 빌드 wheel이 없으면 `pydantic-core`, `orjson`, `pandas`, `scipy`, `scikit-learn` 등이 소스 빌드를 시도할 수 있습니다. 이 경우 Visual Studio Build Tools가 필요할 수 있으므로, 설치 문제가 반복되면 Python 3.12 또는 3.13 사용을 권장합니다.

개발 서버 실행:

```powershell
uvicorn main:app --reload
```

또는 패키지 경로를 직접 지정해 실행할 수 있습니다.

```powershell
uvicorn app.main:app --reload
```

서버 실행 후 아래 주소에서 확인할 수 있습니다.

- API Root: `http://127.0.0.1:8000/`
- Health Check: `http://127.0.0.1:8000/health`
- Swagger UI: `http://127.0.0.1:8000/docs`
- OpenAPI Schema: `http://127.0.0.1:8000/openapi.json`

가상환경 비활성화:

```powershell
deactivate
```

## FastAPI 역할

FastAPI는 AI 서비스의 API Gateway 및 Orchestrator 역할을 수행합니다.

주요 역할:

- ChatGPT API 연동
- AI 매뉴얼 API 연동
- colleague-skill(dot-skill) 연동
- API Orchestration 및 서비스 연계
- 사용자 요청의 중앙 집중 처리
- 외부 AI 서비스와 내부 지식 서비스의 응답 조합

배포 형태:

- Kubernetes `ai-services` Namespace 내 Pod 형태로 배포

## ai-services Namespace 구성

### 1. FastAPI

FastAPI는 API Gateway / Orchestrator 역할을 담당합니다.

주요 역할:

- 사용자 요청 수신
- ChatGPT API 호출
- AI 매뉴얼 API 호출
- colleague-skill(dot-skill) 연계
- 각 서비스 응답 조합 및 최종 응답 반환

배포 형태:

- Pod 형태로 배포

### 2. 외부 AI/API 연동 영역

#### ChatGPT API

- 자연어 질의 처리
- AI 응답 생성
- FastAPI와 연동

#### AI 매뉴얼 API

- 매뉴얼 및 문서 기반 질의응답 제공
- ChatGPT API와 연계하여 결과 생성

#### colleague-skill(dot-skill)

- 사내 업무 지식 및 스킬셋 제공
- AI 매뉴얼 API와 연동

### 3. 데이터 흐름

```text
사용자 요청
    ↓
FastAPI
(API Gateway / Orchestrator)
    ↓
┌─────────────────────┐
│     ChatGPT API     │
└─────────────────────┘
           ↕
┌─────────────────────┐
│    AI 매뉴얼 API     │
└─────────────────────┘
           ↕
┌─────────────────────┐
│ colleague-skill     │
│    (dot-skill)      │
└─────────────────────┘
           ↓
FastAPI
           ↓
응답 반환
```

### 4. 아키텍처 요약

- FastAPI가 AI 서비스의 API Gateway 및 Orchestrator 역할을 수행합니다.
- FastAPI는 ChatGPT API, AI 매뉴얼 API, colleague-skill 서비스를 통합 관리합니다.
- 외부 AI 서비스와 내부 지식 서비스를 조합하여 응답을 생성합니다.
- 모든 요청 흐름은 FastAPI를 통해 중앙 집중적으로 처리됩니다.
- Kubernetes 환경에서는 `ai-services` Namespace 내 Pod로 배포됩니다.
- 향후 AI 서비스 추가 시 FastAPI에서 Orchestration만 확장하면 되므로 확장성이 높습니다.
