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
│  │  ├─ config.py
│  │  ├─ exceptions.py
│  │  └─ logging.py
│  ├─ api/
│  │  ├─ router.py
│  │  └─ routers/
│  │     ├─ health.py
│  │     └─ root.py
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
│  │     └─ common_response.py
│  ├─ utils/
│  │  ├─ datetime_utils.py
│  │  ├─ json_utils.py
│  │  └─ response_utils.py
│  └─ main.py
├─ main.py
├─ requirements.txt
└─ README.md
```

### 패키지 역할

- `app/core/config.py`: `.env` 기반 애플리케이션 설정 관리
- `app/core/logging.py`: 공통 로깅 설정
- `app/core/exceptions.py`: 공통 예외 클래스 및 FastAPI 예외 핸들러 등록
- `app/api/router.py`: 전체 API 라우터 집계
- `app/api/routers`: 기능별 FastAPI 라우터 모듈
- `app/api/routers/root.py`: 루트 상태 응답 라우터
- `app/api/routers/health.py`: 헬스 체크 라우터
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
- `app/dto/response/common_response.py`: 공통 API 응답 DTO 정의
- `app/utils/datetime_utils.py`: UTC 날짜/시간 공통 함수
- `app/utils/json_utils.py`: JSON 직렬화 및 역직렬화 공통 함수
- `app/utils/response_utils.py`: 공통 성공/실패 응답 생성 함수

## 공통 응답 형식

모든 API 응답은 아래 구조를 기본 형식으로 사용합니다.

```json
{
  "success": true,
  "data": {},
  "message": "로그인 성공",
  "timestamp": "2026-06-08T16:00:00"
}
```

필드 설명:

- `success`: 요청 성공 여부
- `data`: 응답 데이터
- `message`: 응답 메시지
- `timestamp`: 응답 생성 시간

관련 코드:

- `app/dto/response/common_response.py`: `CommonResponse` DTO
- `app/utils/response_utils.py`: `success_response`, `error_response` 헬퍼


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
- Health Check: `http://127.0.0.1:8000/api/health`
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

## 제조 이벤트 데이터 생성

제조 이벤트 생성 기능은 기존 `car_master`와 `equipment` 마스터를 참조하여
`manufacturing_event_json`에 원천 이벤트를 저장합니다. 차량 마스터를 생성하거나
수정하지 않으며, `car_master.id=1`부터 요청한 차량 수만큼 순서대로 매핑합니다.

### 기본 생성 규칙

- 차량 수: `vehicle_id`의 생산일자가 요청 날짜와 일치하는 `car_master` 전체
- 차량당 이벤트 수: 4건
- 전체 이벤트 수: 해당 날짜 차량 수 × 4
- 공정 순서: `PRESS -> BODY -> PAINT -> ASSEMBLY`
- 공정별 이벤트 수: 해당 날짜 차량 수와 동일
- 정상/이상 비율: 약 7:3
- 공정별 이상 이벤트: 각 공정에 균등 분배
- 설비: 차량의 공정마다 1~5호기 중 독립적으로 결정
- 최초 발행 상태: `PRESS=READY`, 나머지 공정은 `PENDING`

예를 들어 `AVANTE-20260601-10474`처럼 `vehicle_id`에 `20260601`이 포함된
차량이 10,700대라면 정상 7,490대, 폐기(이상) 3,210대로 구성하고 총
42,800건의 이벤트를 생성합니다.

`event_count`와 `car_pool_size`의 기본값은 `null`입니다. 값을 생략하면 해당
생산일자의 차량 전체를 사용합니다. 제한값을 지정한다면 `event_count`는
`car_pool_size * 4`와 같아야 합니다.

### 이벤트 JSON 구조

`manufacturing_event_json.event_json`에는 아래 최상위 필드만 저장합니다.
아래 JSON은 필드 위치를 보여주는 축약 예시이며, 실제 생성 시 `sensor`,
`processMetrics`, `sourceTrace`, `processData`의 공정별 상세 값이 채워집니다.

```json
{
  "event": {
    "eventId": "EVT-20260601-000001",
    "eventTime": null,
    "eventType": "PROCESS_STATUS",
    "eventName": "프레스 공정 통합 관제 이벤트"
  },
  "equipment": {
    "equipmentCode": "EQ_PRESS_001",
    "equipmentName": "프레스 유압모터 1호",
    "equipmentType": "HYDRAULIC_PRESS"
  },
  "equipmentStatus": {
    "operationStatus": "RUNNING",
    "lastNormalTime": null,
    "statusChangedTime": null
  },
  "product": {
    "carMasterId": 1
  },
  "sensor": {},
  "processMetrics": {},
  "sourceTrace": {},
  "processData": {
    "press": {
      "countIncreaseYn": true,
      "targetCycleTimeSec": 40.0,
      "timestampDelaySec": 5.7
    }
  }
}
```

- `product.carMasterId`는 기존 `car_master.id`입니다.
- DB 컬럼 `event_time`과 JSON의 `event.eventTime`은 최초 생성 시 `NULL`입니다.
- `lastNormalTime`, `statusChangedTime`도 최초 생성 시 `NULL`입니다.
- `operationStatus`는 정상 데이터의 경우 `RUNNING`/`IDLE`, 이상 데이터의 경우
  `FAULT`/`STOPPED`/`MAINTENANCE` 중에서 결정됩니다.
- 같은 차량과 공정은 재생성해도 동일한 상태를 갖도록 결정적 난수를 사용합니다.
- `processData`에는 현재 공정에 해당하는 `press`, `body`, `paint`, `assembly`
  블록 중 하나만 포함됩니다.

### 생성 API

모든 제조 이벤트 API prefix는 `/api/manufacturing/events`입니다.

| Method | Endpoint | 설명 |
| --- | --- | --- |
| `POST` | `/templates/generate` | 날짜 재생용 템플릿 생성 |
| `GET` | `/templates/{template_name}` | 저장된 템플릿 조회 |
| `GET` | `/templates/{template_name}/replay` | DB 저장 없이 특정 날짜 기준 replay 조회 |
| `POST` | `/generate` | 지정 날짜의 실제 이벤트 생성 및 적재 |
| `POST` | `/generate/tomorrow` | 템플릿을 이용해 다음날 이벤트 적재 |
| `GET` | `/generate/jobs/{job_id}` | 비동기 생성 job 상태 조회 |
| `GET` | `/` | 저장된 이벤트 조회 |

예를 들어 2026년 6월 1일 차량 전체의 이벤트는 Swagger에서 `POST /generate`를
아래처럼 호출하여 생성할 수 있습니다.

```text
start_date=2026-06-01
end_date=2026-06-01
event_count=null
car_pool_size=null
insert_chunk_size=1000
```

API는 즉시 `jobId`를 반환합니다. 이후
`GET /api/manufacturing/events/generate/jobs/{jobId}`에서 `SUCCEEDED` 여부를
확인합니다. 실패한 job을 다시 실행할 때는 `/generate`를 새로 호출하면 되며,
동일한 `event_id`는 중복 생성하지 않고 갱신합니다.

템플릿 생성도 `event_count`와 `car_pool_size`의 기본값은 `null`입니다.
`production_date`와 `vehicle_id` 생산일자가 일치하는 차량 전체를 사용합니다.

### 관련 테이블

| 테이블 | 용도 |
| --- | --- |
| `car_master` | 기존 차량 마스터. 생성 로직은 조회만 수행 |
| `equipment` | 공정별 설비 마스터 |
| `manufacturing_event_json` | 실제 원천 이벤트 JSON |
| `manufacturing_event_template` | 날짜별 재생에 사용하는 이벤트 템플릿 |
| `manufacturing_event_generation_job` | 비동기 생성 작업 상태와 진행률 |

### Repository 구조

`SampleDbRepository`는 DB 연결과 아래 repository를 묶는 얇은 facade입니다.
테이블별 SQL과 상태 관리 로직은 각각의 repository에 위치합니다.

```text
app/repository/
├── sampledb_repository.py
├── sampledb_schema.py
├── sampledb_schema_manager.py
├── car_master_repository.py
├── equipment_repository.py
├── manufacturing_event_repository.py
├── manufacturing_event_template_repository.py
└── manufacturing_generation_job_repository.py
```

- `CarMasterRepository`: 기존 차량 조회 및 ID 매핑
- `EquipmentRepository`: 기본 설비 초기화 및 조회
- `ManufacturingEventRepository`: 이벤트 저장·갱신·조회 및 데드락 재시도
- `ManufacturingEventTemplateRepository`: 템플릿 저장·조회·삭제
- `ManufacturingGenerationJobRepository`: job 상태·진행률 및 DB 전역 실행 잠금
- `SampleDbSchemaManager`: 테이블 생성과 점진적 스키마 마이그레이션

MySQL 오류 `1205` 또는 `1213`이 발생하면 이벤트 저장 청크를 자동으로
재시도합니다. 다중 프로세스나 `uvicorn --reload` 환경에서는 DB advisory lock으로
생성 job이 동시에 실행되지 않도록 직렬화합니다.

### 환경변수

아래 값은 코드 기본값이 있으므로 `.env`에 없더라도 동일하게 동작합니다.
운영 환경에서 값을 변경할 때만 설정하면 됩니다.

```dotenv
MANUFACTURING_EVENT_SCHEDULER_ENABLED=true
MANUFACTURING_EVENT_INSERT_CHUNK_SIZE=1000
```

`MANUFACTURING_EVENT_SCHEDULER_EVENTS_PER_DAY`과
`MANUFACTURING_EVENT_CAR_POOL_SIZE`를 생략하면 대상 날짜의 차량 전체를
사용합니다.

스케줄러를 사용하지 않으려면 다음과 같이 설정합니다.

```dotenv
MANUFACTURING_EVENT_SCHEDULER_ENABLED=false
```
