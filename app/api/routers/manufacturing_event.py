from datetime import date
from typing import Literal

from fastapi import APIRouter, Depends, Query, status

from app.dto.response import CommonResponse
from app.service.manufacturing_event_json_service import (
    DEFAULT_CAR_POOL_SIZE,
    DEFAULT_EVENTS_PER_DAY,
    DEFAULT_INSERT_CHUNK_SIZE,
    DEFAULT_TEMPLATE_NAME,
    ManufacturingEventJsonService,
    get_manufacturing_event_json_service,
)
from app.utils.response_utils import success_response


ProcessCode = Literal["PRESS", "BODY", "PAINT", "ASSEMBLY"]
PROCESS_CODE_DESCRIPTION = (
    "조회할 공정 코드입니다. 미입력 시 프레스, 차체, 도장, 의장 전체 공정을 조회합니다."
)
INSERT_CHUNK_SIZE_DESCRIPTION = (
    "DB에 한 번에 적재할 batch insert row 수입니다. "
    "서비스는 생성된 row를 메모리 chunk에 모은 뒤 이 값에 도달할 때마다 한 번의 INSERT를 실행합니다. "
    "MySQL에서는 중복 키 발생 시 generate 계열은 기존 row를 갱신하고, 템플릿 생성은 replace/update 옵션에 따라 갱신 또는 무시합니다."
)
EVENT_JSON_TABLE_DESCRIPTION = """
### 저장 테이블: `manufacturing_event_json`

| 컬럼 | 설명 |
| --- | --- |
| `id` | DB 내부 PK, 자동 증가 |
| `event_id` | 이벤트 고유 ID. 예: `EVT-20260616-000001` |
| `event_time` | 실제 이벤트 발생 시각 |
| `car_master_id` | `car_master.id` FK |
| `equipment_id` | `equipment.id` FK |
| `process_code` | 공정 코드. `PRESS`, `BODY`, `PAINT`, `ASSEMBLY` |
| `station_code` | 공정 내 스테이션 코드 |
| `equipment_code` | 설비 코드. 예: `EQ_PRESS_001` |
| `equipment_type` | 설비 유형. 예: `HYDRAULIC_PRESS`, `ROBOT_ARM`, `CAMERA`, `CONVEYOR` |
| `equipment_status` | 설비 운전 상태. 예: `RUNNING`, `IDLE`, `ERROR` |
| `event_type` | 이벤트 유형. 예: `PROCESS_STATUS`, `EQUIPMENT_SENSOR`, `QUALITY_CHECK` |
| `event_json` | 관제 화면/연계 시스템에 전달할 원본 JSON payload |
| `is_sent` | 외부 시스템 전송 여부. 기본값 `false` |
| `sent_at` | 외부 시스템 전송 완료 시각 |
| `created_at` | DB row 생성 시각 |
| `updated_at` | DB row 마지막 수정 시각. 신규 insert와 중복 `event_id` upsert 시 갱신 |

`event_json`에는 `event`, `location`, `equipment`, `equipmentStatus`, `product`,
`sensor`, `manufacturing`, `processMetrics`, `sourceTrace`, `processData`가 포함됩니다.
"""
TEMPLATE_TABLE_DESCRIPTION = """
### 저장 테이블: `manufacturing_event_template`

| 컬럼 | 설명 |
| --- | --- |
| `id` | DB 내부 PK, 자동 증가 |
| `template_name` | 템플릿 이름 |
| `template_event_id` | 템플릿 이벤트 고유 ID. 예: `TMPL-DEFAULT-000001` |
| `event_offset_us` | 하루 시작 시각 기준 이벤트 발생 offset, microsecond 단위 |
| `car_master_id` | `car_master.id` FK |
| `equipment_id` | `equipment.id` FK |
| `process_code` | 공정 코드. `PRESS`, `BODY`, `PAINT`, `ASSEMBLY` |
| `station_code` | 공정 내 스테이션 코드 |
| `equipment_code` | 설비 코드 |
| `equipment_type` | 설비 유형 |
| `equipment_status` | 템플릿 기준 설비 운전 상태 |
| `event_type` | 이벤트 유형 |
| `event_json` | 날짜를 입히기 전 기준 JSON payload |
| `created_at` | DB row 생성 시각 |

템플릿은 특정 날짜의 실제 이벤트가 아니라 하루 기준 패턴입니다. 실제 적재 시
`event_offset_us`를 target date에 더해 `event_time`으로 변환합니다.
"""

router = APIRouter(prefix="/api/manufacturing/events", tags=["제조 관제 이벤트"])


@router.post(
    "/templates/generate",
    summary="제조 이벤트 템플릿 생성",
    operation_id="generateManufacturingEventTemplate",
    status_code=status.HTTP_202_ACCEPTED,
    description=(
        "CSV 원천 데이터를 전처리/정제해 하루 기준 제조 관제 이벤트 템플릿을 생성하고 "
        "`manufacturing_event_template` 테이블에 저장하는 비동기 job을 생성하는 API입니다.\n\n"
        "### 동작 방식\n"
        "- API는 job row를 생성한 뒤 즉시 `jobId`를 반환합니다.\n"
        "- 실제 템플릿 생성과 DB 저장은 앱 내부 background worker가 수행합니다.\n"
        "- 프레스(PRESS), 차체(BODY), 도장(PAINT), 의장(ASSEMBLY) 공정이 순환 생성됩니다.\n"
        "- 날짜가 고정된 row가 아니라 하루 안에서의 이벤트 발생 위치를 `event_offset_us`로 저장합니다.\n"
        "- `replace=true`이면 같은 `template_name`의 기존 row를 삭제한 뒤 새로 생성합니다.\n"
        "- `insert_chunk_size` 단위로 row를 모아 batch insert합니다.\n\n"
        "### 즉시 응답 데이터\n"
        "- `jobId`: 비동기 job 고유 ID\n"
        "- `jobType`: `GENERATE_TEMPLATE`\n"
        "- `status`: 최초 상태는 `PENDING`, worker 실행 후 `RUNNING`, `SUCCEEDED`, `FAILED`로 변경됩니다.\n"
        "- `totalExpectedEvents`: 예상 생성 건수\n"
        "- `generatedCount`, `affectedRows`: worker 진행 중 갱신되는 처리 건수\n"
        "- `result`: 완료 전에는 `null`, 완료 후 job 상태 조회 API에서 최종 생성 결과가 채워집니다.\n\n"
        f"{TEMPLATE_TABLE_DESCRIPTION}"
    ),
    responses={202: {"description": "제조 이벤트 템플릿 생성 job 생성 결과"}},
)
def generate_manufacturing_event_template(
    template_name: str = Query(
        default=DEFAULT_TEMPLATE_NAME,
        description="생성할 템플릿 이름입니다. replay와 tomorrow 적재 시 이 이름으로 템플릿을 선택합니다.",
    ),
    event_count: int = Query(
        default=DEFAULT_EVENTS_PER_DAY,
        ge=4,
        le=300000,
        description="하루 기준 템플릿 이벤트 수입니다. 기본값은 86,400건입니다.",
    ),
    car_pool_size: int = Query(
        default=DEFAULT_CAR_POOL_SIZE,
        ge=1,
        le=100000,
        description="이벤트에 분배할 차량 마스터 풀 크기입니다.",
    ),
    insert_chunk_size: int = Query(
        default=DEFAULT_INSERT_CHUNK_SIZE,
        ge=100,
        le=10000,
        description=INSERT_CHUNK_SIZE_DESCRIPTION,
    ),
    replace: bool = Query(
        default=False,
        description="true이면 동일 템플릿명을 가진 기존 데이터를 삭제하고 다시 생성합니다.",
    ),
    service: ManufacturingEventJsonService = Depends(
        get_manufacturing_event_json_service,
    ),
) -> CommonResponse[dict]:
    result = service.enqueue_generate_template_job(
        template_name=template_name,
        event_count=event_count,
        car_pool_size=car_pool_size,
        insert_chunk_size=insert_chunk_size,
        replace=replace,
    )
    return success_response(
        data=result,
        message="제조 관제 이벤트 템플릿 생성 job이 생성되었습니다.",
    )


@router.get(
    "/templates/{template_name}",
    summary="제조 이벤트 템플릿 조회",
    operation_id="listManufacturingEventTemplate",
    description=(
        "`manufacturing_event_template`에 저장된 기준 템플릿 이벤트를 조회합니다.\n\n"
        "### 동작 방식\n"
        "- 이 API는 날짜별 변동을 적용하지 않은 템플릿 원본을 반환합니다.\n"
        "- `template_name`, `process_code`, `limit`, `offset` 기준으로 조회합니다.\n"
        "- 실제 관제 화면 날짜별 이벤트 형태를 확인하려면 replay API를 사용합니다.\n\n"
        f"{TEMPLATE_TABLE_DESCRIPTION}"
    ),
    responses={200: {"description": "제조 이벤트 템플릿 목록"}},
)
def list_manufacturing_event_template(
    template_name: str,
    limit: int = Query(default=20, ge=1, le=200, description="조회할 최대 건수입니다."),
    offset: int = Query(default=0, ge=0, description="조회 시작 위치입니다."),
    process_code: ProcessCode | None = Query(
        default=None,
        description=PROCESS_CODE_DESCRIPTION,
    ),
    service: ManufacturingEventJsonService = Depends(
        get_manufacturing_event_json_service,
    ),
) -> CommonResponse[dict]:
    events = service.list_template_events(
        template_name=template_name,
        limit=limit,
        offset=offset,
        process_code=process_code,
    )
    return success_response(
        data={
            "items": events,
            "limit": limit,
            "offset": offset,
        },
        message="제조 관제 이벤트 템플릿 조회가 완료되었습니다.",
    )


@router.get(
    "/templates/{template_name}/replay",
    summary="템플릿 기반 날짜별 이벤트 replay 조회",
    operation_id="replayManufacturingEventTemplate",
    description=(
        "저장된 템플릿을 지정한 날짜의 제조 관제 이벤트처럼 재생성해서 조회합니다.\n\n"
        "### 동작 방식\n"
        "- `manufacturing_event_template` row를 읽어 `target_date` 기준 이벤트처럼 materialize합니다.\n"
        "- `event_offset_us`를 `target_date` 00:00:00에 더해 `event_time`을 계산합니다.\n"
        "- `template_event_id` 순번을 이용해 `EVT-{target_date}-000001` 형태의 `event_id`를 생성합니다.\n"
        "- 전류, 진동, 로봇암 진동, 열화상, 공정 지표는 날짜별 변동 레이어가 적용됩니다.\n"
        "- 같은 날짜와 같은 템플릿 이벤트는 항상 같은 값으로 replay되며, 날짜가 바뀌면 수치가 달라집니다.\n"
        "- 조회 전용 API이므로 `manufacturing_event_json` 테이블에는 저장하지 않습니다.\n\n"
        "### 반환 데이터 형태\n"
        "반환 item은 실제 저장 row와 같은 필드 구조를 갖지만 DB에 insert되지는 않습니다.\n\n"
        f"{EVENT_JSON_TABLE_DESCRIPTION}"
    ),
    responses={200: {"description": "날짜별 replay 이벤트 목록"}},
)
def replay_manufacturing_event_template(
    template_name: str,
    target_date: date = Query(
        default=date(2026, 6, 16),
        description="이벤트를 replay할 기준 날짜입니다.",
    ),
    limit: int = Query(default=20, ge=1, le=200, description="조회할 최대 건수입니다."),
    offset: int = Query(default=0, ge=0, description="조회 시작 위치입니다."),
    process_code: ProcessCode | None = Query(
        default=None,
        description=PROCESS_CODE_DESCRIPTION,
    ),
    service: ManufacturingEventJsonService = Depends(
        get_manufacturing_event_json_service,
    ),
) -> CommonResponse[dict]:
    events = service.replay_template_events(
        template_name=template_name,
        target_date=target_date,
        limit=limit,
        offset=offset,
        process_code=process_code,
    )
    return success_response(
        data={
            "items": events,
            "targetDate": target_date.isoformat(),
            "limit": limit,
            "offset": offset,
        },
        message="템플릿 기반 날짜별 제조 관제 이벤트 replay 조회가 완료되었습니다.",
    )


@router.post(
    "/generate",
    summary="기간별 제조 이벤트 JSON 생성 및 적재",
    operation_id="generateManufacturingEventJson",
    status_code=status.HTTP_202_ACCEPTED,
    description=(
        "지정한 날짜 범위의 제조 관제 이벤트 JSON을 실제 일자 데이터로 생성해 "
        "`manufacturing_event_json` 테이블에 저장하는 비동기 job을 생성하는 API입니다.\n\n"
        "### 언제 사용하는 API인가요?\n"
        "- 초기 시연 데이터 또는 특정 기간의 샘플 제조 이벤트를 DB에 실제 row로 적재할 때 사용합니다.\n"
        "- 템플릿 replay가 아니라 CSV 기반 생성기를 직접 실행합니다.\n"
        "- `start_date`부터 `end_date`까지 양 끝 날짜를 모두 포함해 생성합니다.\n\n"
        "### 생성/저장 방식\n"
        "- API는 job row를 생성한 뒤 즉시 `jobId`를 반환합니다.\n"
        "- 실제 이벤트 생성과 DB 저장은 앱 내부 background worker가 수행합니다.\n"
        "- 진행 상태는 job 상태 조회 API로 확인합니다.\n"
        "- 전체 생성 건수는 `(end_date - start_date + 1) * events_per_day`입니다.\n"
        "- 공정은 `PRESS -> BODY -> PAINT -> ASSEMBLY` 순서로 순환 배치됩니다.\n"
        "- 이벤트 시간은 하루 안에서 생산 밀도가 높은 시간대에 더 많이 분포되도록 계산됩니다.\n"
        "- 필요한 schema를 보장하고, 기본 설비와 차량 마스터 row를 준비한 뒤 이벤트를 생성합니다.\n"
        "- 생성된 row는 `insert_chunk_size` 단위로 모아 batch insert합니다.\n"
        "- 기본 batch insert 단위는 `1,000`건이며, 요청 파라미터로 `100~10,000` 사이에서 조정할 수 있습니다.\n"
        "- MySQL에서는 `event_id` 중복 시 기존 row의 이벤트 시간, 설비, 상태, JSON payload 등을 갱신합니다.\n\n"
        "### 즉시 응답 데이터\n"
        "- `jobId`: 비동기 job 고유 ID\n"
        "- `jobType`: `GENERATE_RANGE`\n"
        "- `status`: 최초 상태는 `PENDING`, worker 실행 후 `RUNNING`, `SUCCEEDED`, `FAILED`로 변경됩니다.\n"
        "- `totalExpectedEvents`: 예상 전체 생성 건수\n"
        "- `generatedCount`, `affectedRows`: worker 진행 중 갱신되는 처리 건수\n"
        "- `result`: 완료 전에는 `null`, 완료 후 job 상태 조회 API에서 최종 생성 결과가 채워집니다.\n\n"
        f"{EVENT_JSON_TABLE_DESCRIPTION}"
    ),
    responses={202: {"description": "기간별 제조 이벤트 JSON 생성 job 생성 결과"}},
)
def generate_manufacturing_event_json(
    start_date: date = Query(
        default=date(2026, 6, 1),
        description="생성 시작 날짜입니다.",
    ),
    end_date: date = Query(
        default=date(2026, 6, 16),
        description="생성 종료 날짜입니다.",
    ),
    events_per_day: int = Query(
        default=DEFAULT_EVENTS_PER_DAY,
        ge=4,
        le=300000,
        description="하루에 생성할 이벤트 수입니다. 기본값은 86,400건입니다.",
    ),
    car_pool_size: int = Query(
        default=DEFAULT_CAR_POOL_SIZE,
        ge=1,
        le=100000,
        description="이벤트에 분배할 차량 마스터 풀 크기입니다.",
    ),
    insert_chunk_size: int = Query(
        default=DEFAULT_INSERT_CHUNK_SIZE,
        ge=100,
        le=10000,
        description=INSERT_CHUNK_SIZE_DESCRIPTION,
    ),
    service: ManufacturingEventJsonService = Depends(
        get_manufacturing_event_json_service,
    ),
) -> CommonResponse[dict]:
    result = service.enqueue_generate_range_job(
        start_date=start_date,
        end_date=end_date,
        events_per_day=events_per_day,
        car_pool_size=car_pool_size,
        insert_chunk_size=insert_chunk_size,
    )
    return success_response(
        data=result,
        message="제조 원천 이벤트 JSON 생성 job이 생성되었습니다.",
    )


@router.post(
    "/generate/tomorrow",
    summary="템플릿 기반 다음날 제조 이벤트 적재",
    operation_id="generateTomorrowManufacturingEventJson",
    status_code=status.HTTP_202_ACCEPTED,
    description=(
        "저장된 템플릿을 기준으로 다음날 제조 관제 이벤트를 생성하고 "
        "`manufacturing_event_json` 테이블에 저장하는 비동기 job을 생성하는 API입니다.\n\n"
        "### 언제 사용하는 API인가요?\n"
        "- 매일 다음날 관제 이벤트 데이터를 미리 생성/적재할 때 사용합니다.\n"
        "- 스케줄러도 이 API와 같은 내부 서비스 로직을 사용합니다.\n"
        "- `base_date`를 입력하지 않으면 서버 실행일 기준 다음날 데이터를 생성합니다.\n\n"
        "### 생성/저장 방식\n"
        "- API는 job row를 생성한 뒤 즉시 `jobId`를 반환합니다.\n"
        "- 실제 이벤트 생성과 DB 저장은 앱 내부 background worker가 수행합니다.\n"
        "- 진행 상태는 job 상태 조회 API로 확인합니다.\n"
        "- target date는 `(base_date 또는 서버 현재 날짜) + 1일`입니다.\n"
        "- 지정한 `template_name`의 템플릿이 없거나 `events_per_day`보다 부족하면 템플릿을 먼저 생성합니다.\n"
        "- 템플릿의 `event_offset_us`를 target date에 더해 실제 `event_time`을 만듭니다.\n"
        "- replay와 동일한 날짜별 수치 변동 레이어가 적용되어 날짜마다 다른 데이터가 저장됩니다.\n"
        "- 템플릿 row를 `insert_chunk_size` 단위로 읽고, materialize된 이벤트를 같은 단위로 batch insert합니다.\n"
        "- 기본 batch insert 단위는 `1,000`건이며, 요청 파라미터로 `100~10,000` 사이에서 조정할 수 있습니다.\n"
        "- MySQL에서는 `event_id` 중복 시 기존 row의 이벤트 시간, 설비, 상태, JSON payload 등을 갱신합니다.\n\n"
        "### 즉시 응답 데이터\n"
        "- `jobId`: 비동기 job 고유 ID\n"
        "- `jobType`: `GENERATE_TOMORROW`\n"
        "- `status`: 최초 상태는 `PENDING`, worker 실행 후 `RUNNING`, `SUCCEEDED`, `FAILED`로 변경됩니다.\n"
        "- `totalExpectedEvents`: 예상 전체 생성 건수\n"
        "- `generatedCount`, `affectedRows`: worker 진행 중 갱신되는 처리 건수\n"
        "- `result`: 완료 전에는 `null`, 완료 후 job 상태 조회 API에서 최종 생성 결과가 채워집니다.\n\n"
        f"{EVENT_JSON_TABLE_DESCRIPTION}"
    ),
    responses={202: {"description": "템플릿 기반 다음날 제조 이벤트 적재 job 생성 결과"}},
)
def generate_tomorrow_manufacturing_event_json(
    base_date: date | None = Query(
        default=None,
        description="다음날 계산 기준 날짜입니다. 미입력 시 서버 현재 날짜를 사용합니다.",
    ),
    template_name: str = Query(
        default=DEFAULT_TEMPLATE_NAME,
        description="다음날 데이터 적재에 사용할 템플릿 이름입니다.",
    ),
    events_per_day: int = Query(
        default=DEFAULT_EVENTS_PER_DAY,
        ge=4,
        le=300000,
        description="템플릿이 없거나 부족할 때 준비할 하루 기준 이벤트 수입니다.",
    ),
    car_pool_size: int = Query(
        default=DEFAULT_CAR_POOL_SIZE,
        ge=1,
        le=100000,
        description="템플릿 생성이 필요할 때 이벤트에 분배할 차량 마스터 풀 크기입니다.",
    ),
    insert_chunk_size: int = Query(
        default=DEFAULT_INSERT_CHUNK_SIZE,
        ge=100,
        le=10000,
        description=INSERT_CHUNK_SIZE_DESCRIPTION,
    ),
    service: ManufacturingEventJsonService = Depends(
        get_manufacturing_event_json_service,
    ),
) -> CommonResponse[dict]:
    result = service.enqueue_generate_tomorrow_job(
        base_date=base_date,
        template_name=template_name,
        events_per_day=events_per_day,
        car_pool_size=car_pool_size,
        insert_chunk_size=insert_chunk_size,
    )
    return success_response(
        data=result,
        message="템플릿 기반 다음날 제조 원천 이벤트 JSON 적재 job이 생성되었습니다.",
    )


@router.get(
    "/generate/jobs/{job_id}",
    summary="제조 이벤트 생성 job 상태 조회",
    operation_id="getManufacturingEventGenerationJob",
    description=(
        "`/generate`, `/generate/tomorrow`, `/templates/generate`에서 생성한 "
        "비동기 제조 이벤트 생성 job의 진행 상태를 조회합니다.\n\n"
        "### 상태 값\n"
        "- `PENDING`: job이 생성되었고 worker 실행을 기다리는 상태\n"
        "- `RUNNING`: worker가 생성/저장을 수행 중인 상태\n"
        "- `SUCCEEDED`: 생성/저장이 완료된 상태\n"
        "- `FAILED`: 생성/저장 중 오류가 발생한 상태\n\n"
        "### 주요 응답 데이터\n"
        "- `jobId`: job 고유 ID\n"
        "- `jobType`: `GENERATE_RANGE`, `GENERATE_TOMORROW`, `GENERATE_TEMPLATE`\n"
        "- `totalExpectedEvents`: 예상 전체 처리 건수\n"
        "- `generatedCount`: 현재까지 생성한 이벤트 수\n"
        "- `affectedRows`: 현재까지 DB insert/update 영향 row 수\n"
        "- `result`: 성공 시 기존 동기 API가 반환하던 생성 결과\n"
        "- `errorMessage`: 실패 시 오류 메시지"
    ),
    responses={200: {"description": "제조 이벤트 생성 job 상태"}},
)
def get_manufacturing_event_generation_job(
    job_id: str,
    service: ManufacturingEventJsonService = Depends(
        get_manufacturing_event_json_service,
    ),
) -> CommonResponse[dict]:
    result = service.get_generation_job(job_id)
    return success_response(
        data=result,
        message="제조 이벤트 생성 job 상태 조회가 완료되었습니다.",
    )


@router.get(
    "",
    summary="저장된 제조 이벤트 JSON 조회",
    operation_id="listManufacturingEventJson",
    description=(
        "`manufacturing_event_json` 테이블에 저장된 제조 관제 이벤트 JSON을 조회합니다.\n\n"
        "### 동작 방식\n"
        "- 기간, 공정 코드, 전송 여부 기준으로 필터링할 수 있습니다.\n"
        "- 템플릿 replay 결과가 아니라 실제로 테이블에 적재된 일자별 이벤트를 반환합니다.\n"
        "- `event_time`, `id` 오름차순으로 정렬해 `limit`, `offset` 페이지를 반환합니다.\n\n"
        f"{EVENT_JSON_TABLE_DESCRIPTION}"
    ),
    responses={200: {"description": "저장된 제조 이벤트 JSON 목록"}},
)
def list_manufacturing_event_json(
    limit: int = Query(default=20, ge=1, le=200, description="조회할 최대 건수입니다."),
    offset: int = Query(default=0, ge=0, description="조회 시작 위치입니다."),
    start_date: date | None = Query(default=None, description="조회 시작 날짜입니다."),
    end_date: date | None = Query(default=None, description="조회 종료 날짜입니다."),
    process_code: ProcessCode | None = Query(
        default=None,
        description=PROCESS_CODE_DESCRIPTION,
    ),
    is_sent: bool | None = Query(
        default=None,
        description="외부 시스템 전송 여부입니다. 미입력 시 전체를 조회합니다.",
    ),
    service: ManufacturingEventJsonService = Depends(
        get_manufacturing_event_json_service,
    ),
) -> CommonResponse[dict]:
    events = service.list_events(
        limit=limit,
        offset=offset,
        start_date=start_date,
        end_date=end_date,
        process_code=process_code,
        is_sent=is_sent,
    )
    return success_response(
        data={
            "items": events,
            "limit": limit,
            "offset": offset,
        },
        message="제조 원천 이벤트 JSON 조회가 완료되었습니다.",
    )
