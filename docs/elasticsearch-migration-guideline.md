# 병목 및 불량전이 조회의 Elasticsearch 전환 가이드

## 1. 목적

현재 병목 조회와 불량전이 조회는 MySQL 결과 테이블과 Redis 캐시를 중심으로 동작한다.
이 문서는 조회 계층을 Elasticsearch(OpenSearch 포함) 기반으로 전환할 때의 설계 원칙, 마이그레이션 순서, 운영 체크포인트를 정리한 가이드다.

핵심 목표는 다음과 같다.

- 조회 성능을 안정화한다.
- 최신 결과를 빠르게 검색 가능하게 만든다.
- 기존 API 응답 형식은 유지한다.
- DB는 원장(source of truth), ES는 조회용 read model로 분리한다.

## 2. 현재 구조 요약

### 병목 조회

현재 병목 조회는 다음 흐름이다.

- Kafka raw 이벤트 수신
- 병목 분석 수행
- `bottleneck_analysis_result` 테이블에 저장
- Redis 캐시로 페이지 응답 캐싱
- API는 Redis 캐시를 우선 조회하고, 없으면 DB에서 조회

관련 코드:

- [`app/api/routers/process.py`](../app/api/routers/process.py)
- [`app/service/analysis/bottleneck_service.py`](../app/service/analysis/bottleneck_service.py)
- [`app/repository/bottleneck_analysis_repository.py`](../app/repository/bottleneck_analysis_repository.py)

### 불량전이 조회

현재 불량전이 조회는 다음 흐름이다.

- raw 이벤트 또는 배치에서 불량전이 예측 수행
- `defect_transfer_prediction_result` 테이블에 저장
- Redis 캐시로 predictions / causes 페이지 캐싱
- API는 캐시를 우선 조회하고, 없으면 DB와 이벤트 DB를 조합해 조회

관련 코드:

- [`app/api/routers/defect_transfer.py`](../app/api/routers/defect_transfer.py)
- [`app/service/analysis/defect_transfer_service.py`](../app/service/analysis/defect_transfer_service.py)
- [`app/repository/defect_transfer_prediction_repository.py`](../app/repository/defect_transfer_prediction_repository.py)

## 3. 전환 원칙

### 3.1 유지할 것

- API 경로와 응답 DTO
- 분석/예측의 계산 로직
- MySQL 결과 테이블의 저장 책임
- Redis 캐시의 선택적 사용

### 3.2 바꿀 것

- 조회 대상의 1차 소스를 MySQL에서 ES로 이동
- 페이지 조회 방식에 맞는 ES 문서 구조 설계
- 인덱싱/재색인/동기화 파이프라인 추가

### 3.3 절대 분리할 것

- MySQL은 결과 저장과 정합성 기준으로 유지
- ES는 검색과 리스트 응답 전용으로 사용
- ES 장애 시에는 DB fallback 또는 캐시 fallback 전략을 둔다

## 4. 목표 아키텍처

권장 구조는 아래와 같다.

```text
Kafka/raw event -> analysis service -> MySQL result table
                               \-> outbox/event topic -> ES indexer -> Elasticsearch

API -> Redis cache -> Elasticsearch -> fallback MySQL
```

권장 역할 분리:

- MySQL
  - 분석 결과의 원장 저장
  - 재처리/검증 기준
- Elasticsearch
  - 조회용 read model
  - 최신 결과 검색, 정렬, 필터, collapse, highlight
- Redis
  - 짧은 TTL 응답 캐시
  - 동일 페이지 반복 요청 완화
- Kafka 또는 outbox
  - ES 동기화 이벤트 전달

## 5. 인덱스 설계

## 5.1 병목 인덱스

권장 인덱스 예시:

- `ai-bottleneck-result-v1`

문서 단위는 다음 둘 중 하나를 추천한다.

1. `rank` 기준 스냅샷 문서
2. `manufacturing_event_id` 기준 원천 이벤트 문서

현재 API가 “병목 순위 페이지”를 반환하므로, 실무적으로는 스냅샷 단위 문서가 단순하다.

예시 필드:

- `snapshotId`
- `detectedAt`
- `rankNo`
- `processCode`
- `equipmentCode`
- `avgDelayTime`
- `affectedVehicleCount`
- `riskScore`
- `riskLevel`
- `mostBottleneckProcess`
- `mostBottleneckRiskLevel`

권장 정렬:

- `rankNo asc`
- 필요 시 `riskScore desc`
- 필요 시 `detectedAt desc`

### 병목 문서 ID

권장 ID는 다음 중 하나다.

- `detectedAt + rankNo`
- `snapshotId + rankNo`

이렇게 하면 재색인 시 중복 문서를 쉽게 방지할 수 있다.

## 5.2 불량전이 인덱스

권장 인덱스 예시:

- `ai-defect-transfer-result-v1`

불량전이는 조회 유형이 두 가지다.

1. predictions: 차량/이벤트 단위 목록
2. causes: 특정 차량의 최신 이벤트에 대한 원인 목록

권장 문서 구조:

- `eventId`
- `manufacturingEventId`
- `carMasterId`
- `vehicleId`
- `sourceProcessCode`
- `sourceEquipmentCode`
- `targetProcessCode`
- `targetEquipmentCode`
- `currentDefectProbability`
- `targetDefectProbability`
- `predictedDefectProcess`
- `expectedOccurrenceStep`
- `riskGrade`
- `predictedAt`
- `influenceScore`
- `mainCauses` as nested or object array

### 불량전이 문서 ID

권장 ID는 `manufacturingEventId` 또는 `eventId`다.

이유:

- 같은 이벤트에 대해 예측 결과는 최신 값 1개가 적합하다.
- upsert가 쉽다.
- 중복 적재를 피하기 쉽다.

## 6. 매핑 가이드

### 6.1 병목 매핑 포인트

현재 `bottleneck_analysis_result` 테이블의 컬럼을 ES 필드로 1:1 매핑한다.

중요한 점:

- `riskScore`, `avgDelayTime`는 numeric type으로 저장
- `rankNo`는 integer
- `detectedAt`는 date
- `processCode`는 keyword
- `equipmentCode`는 keyword

### 6.2 불량전이 매핑 포인트

불량전이는 검색 조건이 다양하므로 다음처럼 나누는 것이 좋다.

- 식별자: `keyword`
- 수치: `double` or `integer`
- 시간: `date`
- 원인 목록: `nested` 권장

`mainCauses`가 단순 배열이 아니라 원인별 필터링/정렬의 대상이 된다면 `nested`로 설계하는 편이 안전하다.

## 7. 조회 로직 변경 방식

## 7.1 병목 조회

현재 로직은 DB에서 최신 스냅샷을 읽고 페이지를 자른다.
ES 전환 후에는 다음과 같이 바꾼다.

### 추천 방식

- 최신 스냅샷을 ES에서 찾는다.
- `detectedAt`이 가장 최신인 문서 집합만 조회한다.
- `rankNo asc`로 페이지를 자른다.

### 구현 포인트

- `cursor`가 페이지 번호이므로 우선 `from + size` 방식으로 시작할 수 있다.
- 데이터량이 많아지면 `search_after`로 바꾸는 것이 좋다.
- 응답의 `mostBottleneckProcess`와 `mostBottleneckRiskLevel`은 첫 문서 또는 별도 summary 문서에서 가져온다.

### 서비스 변경 방향

- `BottleneckAnalysisService.get_realtime_bottlenecks()`가 ES repository를 호출하도록 변경
- Redis 캐시는 유지 가능
- DB fallback은 장애 대응용으로 둔다

## 7.2 불량전이 predictions

현재는 결과 테이블의 전체 rows를 읽고 차량별 최신 결과를 뽑는다.
ES에서는 다음 둘 중 하나를 추천한다.

### 옵션 A: 최신 결과만 저장

- 차량 또는 이벤트별 최신 결과 1건만 ES에 저장
- `collapse` 없이 바로 정렬 가능

장점:

- 조회가 단순하다
- 응답 속도가 좋다

단점:

- 이력 조회가 어려워진다

### 옵션 B: 모든 결과 저장 후 최신 문서 선별

- `carMasterId`로 collapse
- `predictedAt desc`로 최신값 선택

장점:

- 이력 보존이 쉽다

단점:

- 쿼리가 다소 복잡하다

실무적으로는 predictions는 옵션 A, 이력/감사 목적은 MySQL 원장 유지가 가장 무난하다.

## 7.3 불량전이 causes

현재는 특정 vehicleId의 최신 이벤트를 기준으로 cause list를 만든다.
ES 전환 후에는 다음과 같이 설계한다.

- `vehicleId` 또는 `carMasterId`로 필터
- `predictedAt desc`로 최신 이벤트 선택
- `mainCauses`는 nested query 또는 source filtering으로 조회

`get_cached_cause_analysis()`는 ES 문서를 읽어 DTO로 변환하는 역할만 하도록 단순화한다.

## 8. 동기화 전략

ES 전환의 핵심은 “어떻게 최신성을 보장할 것인가”다.

### 8.1 권장 순서

1. MySQL에 결과 저장
2. 저장 완료 후 outbox 또는 Kafka 이벤트 발행
3. ES indexer가 이벤트 소비
4. ES에 upsert

### 8.2 동기화 방식 선택

#### 방식 1. 애플리케이션 dual write

- 저장 서비스에서 MySQL 저장 후 ES도 직접 저장

장점:

- 구현이 빠르다

단점:

- 부분 실패와 재시도 처리 복잡
- 정합성 이슈 가능

#### 방식 2. Kafka/outbox 기반 비동기 동기화

- 저장과 발행을 분리
- ES indexer가 별도 소비자 역할 수행

장점:

- 운영 안정성이 높다
- 재처리와 재색인이 쉽다

단점:

- 구성 요소가 하나 더 필요하다

권장:

- 장기적으로는 방식 2
- 단기 POC는 방식 1도 가능

## 9. 마이그레이션 단계

### 1단계. ES 설정 추가

- ES/OpenSearch URL
- 인증 정보
- 인덱스 이름
- timeouts / retry / bulk size

### 2단계. Read repository 추가

- `BottleneckSearchRepository`
- `DefectTransferSearchRepository`

### 3단계. Indexer 추가

- 병목 결과 upsert
- 불량전이 결과 upsert
- bulk indexing 지원

### 4단계. 백필 작업 추가

- 기존 MySQL 결과를 ES에 한 번 채운다
- 문서 ID 기준으로 재실행 가능해야 한다

### 5단계. API read path 전환

- Redis -> ES -> DB fallback
- 또는 Redis -> ES만 먼저 적용

### 6단계. 정합성 검증

- DB와 ES 결과 수 비교
- 상위 N개 순위 비교
- 차량별 최신 예측 비교

### 7단계. 캐시 버전 업

- ES 전환 시 캐시 키 버전을 올려 구 응답과 섞이지 않게 한다

## 10. 코드 변경 포인트

### 10.1 설정

`app/core/config.py`에 ES 관련 설정을 추가한다.

예시:

- `ELASTICSEARCH_URL`
- `ELASTICSEARCH_USERNAME`
- `ELASTICSEARCH_PASSWORD`
- `ELASTICSEARCH_BOTTLENECK_INDEX`
- `ELASTICSEARCH_DEFECT_TRANSFER_INDEX`

### 10.2 서비스

다음 서비스가 핵심 변경 지점이다.

- [`app/service/analysis/bottleneck_service.py`](../app/service/analysis/bottleneck_service.py)
- [`app/service/analysis/defect_transfer_service.py`](../app/service/analysis/defect_transfer_service.py)

현재는 repository를 통해 DB를 읽는데, 이후에는 search repository를 주입받도록 바꾼다.

### 10.3 저장 로직

다음 저장 지점에서 ES 이벤트를 함께 발행하거나 outbox를 남긴다.

- 병목 분석 저장
- 불량전이 예측 저장

### 10.4 캐시 무효화

현재 Redis 캐시 무효화는 raw event 수신 시점에 수행된다.
ES 전환 후에는 다음 중 하나를 선택한다.

- 캐시 유지 + ES 반영 시점에 키 무효화
- 캐시 축소 + 짧은 TTL만 유지

## 11. 성능 가이드

### 권장 기준

- page size는 100 이하 유지
- bulk size는 500~2000 사이에서 조정
- index refresh interval은 write 빈도에 맞춘다
- shard 수는 초기에는 최소화한다

### 검색 팁

- keyword 필드는 `term` / `terms`
- 날짜 범위는 `range`
- 최신 1건은 `sort + size=1`
- 차량별 최신 결과는 `collapse` 또는 미리 최신 문서만 저장

## 12. 장애 및 롤백 전략

### 장애 대응

- ES 조회 실패 시 DB fallback
- ES indexer 실패 시 재시도 큐에 적재
- bulk 실패는 item 단위 실패 로그를 남긴다

### 롤백

- read path를 다시 DB로 되돌릴 수 있어야 한다
- ES를 단순 보조 인프라로 취급한다
- 인덱스 버전은 `v1`, `v2`처럼 명시한다

## 13. 검증 항목

전환 후 아래를 반드시 비교한다.

- 병목 상위 5개 순위가 DB와 동일한가
- 병목 `mostBottleneckProcess`가 동일한가
- 불량전이 predictions 결과 건수가 동일한가
- vehicleId별 cause 페이지가 동일한가
- 최신성 지연이 허용 범위 이내인가

## 14. 추천 구현 순서

1. ES 설정과 client 추가
2. search repository 작성
3. 기존 DB 결과를 ES로 백필
4. 병목 조회부터 ES read path로 전환
5. 불량전이 predictions 전환
6. 불량전이 causes 전환
7. 캐시와 fallback 정리
8. DB-only 조회 코드 정리

## 15. 결론

가장 중요한 원칙은 두 가지다.

1. 저장의 기준은 MySQL로 유지한다.
2. 조회의 기준은 Elasticsearch로 옮긴다.

이렇게 하면 정합성은 유지하면서도, 병목/불량전이 같은 조회성 API를 훨씬 유연하게 확장할 수 있다.

