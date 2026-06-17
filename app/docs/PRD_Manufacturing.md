# 데이터셋

## 1.1. Ford 엔진 진동 데이터셋 - 프레스&차체

### 1.1.1. ARFF 파일 구조

ARFF는 WEKA 머신러닝 도구용 데이터 포맷입니다.

### 구성 요소

| 구성 | 설명 |
| --- | --- |
| `@RELATION` | 데이터셋 이름 |
| `@ATTRIBUTE` | 컬럼 정의 |
| `@DATA` | 실제 데이터 |

### 1.1.2. ARFF 컬럼 구조

| 컬럼 | 설명 |
| --- | --- |
| `att1 ~ att500` | 시계열 센서값 |
| `class` | 정상/이상 라벨 |

02. Dataset_FordEngine.zip

- 전체 데이터 구조
    
    ## 전체 데이터셋 구성
    
    | 파일명 | 데이터 유형 | 설명 |
    | --- | --- | --- |
    | `FordA.txt` | 전체 데이터 | 시계열 센서 데이터 |
    | `FordA_TEST.txt` | 테스트 데이터 | 테스트용 시계열 데이터 |
    | `FordA_TRAIN.arff` | 학습 데이터 | WEKA 형식 학습 데이터 |
    | `FordA_TEST.arff` | 테스트 데이터 | WEKA 형식 테스트 데이터 |
    
    ---
    
    ## 데이터 구조 특징
    
    | 항목 | 내용 |
    | --- | --- |
    | 데이터 형태 | 시계열(Time-Series) |
    | 데이터 길이 | 샘플당 500개 측정값 |
    | 목적 | 자동차 엔진 이상 탐지 |
    | 입력 데이터 | 엔진 소음 측정값 |
    | 출력 라벨 | 정상 / 이상 |
    | 문제 유형 | 이진 분류(Binary Classification) |
    | 추천 활용 | 이상 탐지, 예지보전, 시계열 분류 |
    
    ---
    
    ## TXT 파일 데이터 구조
    
    TXT 파일은 아래와 같은 구조를 가집니다.
    
    | 위치 | 의미 |
    | --- | --- |
    | 첫 번째 값 | 클래스(Label) |
    | 이후 500개 값 | 엔진 소음 시계열 데이터 |
    
    ---
    
    ## 데이터 샘플 구조 예시
    
    | label | t1 | t2 | t3 | ... | t500 |
    | --- | --- | --- | --- | --- | --- |
    | -1 | -0.14 | 0.17 | 0.30 | ... | -0.79 |
    | 1 | 0.71 | 0.74 | 0.72 | ... | -4.33 |
    
    ※ 실제 데이터는 실수(float) 기반 연속값으로 구성됨
    
    ---
    
    ## Label 의미
    
    | 값 | 의미 |
    | --- | --- |
    | `1` | 정상(Normal) |
    | `-1` | 이상(Fault/Abnormal) |
    
    ---
    
    ## ARFF 파일 구조
    
    ARFF는 WEKA 머신러닝 도구용 데이터 포맷입니다.
    
    ### 구성 요소
    
    | 구성 | 설명 |
    | --- | --- |
    | `@RELATION` | 데이터셋 이름 |
    | `@ATTRIBUTE` | 컬럼 정의 |
    | `@DATA` | 실제 데이터 |
    
    ---
    
    ## ARFF 컬럼 구조
    
    | 컬럼 | 설명 |
    | --- | --- |
    | `att1 ~ att500` | 시계열 센서값 |
    | `class` | 정상/이상 라벨 |
    
    ---
    
    ## 데이터셋 특징 요약
    
    | 항목 | 설명 |
    | --- | --- |
    | 산업 분야 | 자동차 제조 |
    | 데이터 종류 | 엔진 소음 시계열 |
    | 센서 유형 | 진동/소음 계열로 추정 |
    | 샘플 길이 | 고정 길이 500 |
    | 전처리 여부 | 정규화 미적용(original raw signal) |
    | 학습 유형 | 지도학습(Supervised Learning) |
- https://www.kamp-ai.kr/aidataDetail?AI_SEARCH=%EC%98%88%EC%A7%80%EB%B3%B4%EC%A0%84&page=1&DATASET_SEQ=2&DISPLAY_MODE_SEL=CARD&EQUIP_SEL=&GUBUN_SEL=&FILE_TYPE_SEL=&WDATE_SEL=
- Ford사의 자동차 엔진 및 모터 센서의 시계열 진동 데이터를 기반으로 정상/비정상 상태를 분류하는 예지보전(Predictive Maintenance) 데이터셋
- 데이터 개수 : 4,921 개 / 확장자 : ARFF, text
- Sensor 1~500의 시계열 데이터를 활용하여 설비 이상 탐지 및 고장 예측 모델 학습 가능
- CNN, RNN, XGBoost 등의 알고리즘을 활용하여 제조 설비 상태 분석 및 장애 탐지 수행 가능
- Kafka 기반 실시간 이벤트 스트리밍, Redis Queue 모니터링, AI 기반 Predictive Alert 기능 구현에 적합
- 스마트팩토리 환경에서 모터 진동 증가, RPM 이상, 설비 소음 증가 등의 이벤트를 시뮬레이션하는 용도로 활용 가능
- 모빌리티 스마트팩토리 관제 시스템의 실시간 설비 이상 탐지 및 시계열 기반 AI 분석 구조와 높은 연관성을 가짐

## **1.2. 소성가공 자원최적화 AI 데이터셋** - 프레스&차체&도장

### 1.2.1. 공정 데이터 2022년 8월 구조

| 컬럼명 | 의미 | 데이터 타입 | 예시 |
| --- | --- | --- | --- |
| idx | 생산 데이터 고유 ID | Integer | 55644626 |
| lineno | 생산 라인 번호 | Integer | 1200 |
| itemno | 생산 제품 코드 | String | 76211-A3010-100 |
| Order_date | 주문 날짜 값 | Integer(Date Serial) | 44783 |
| day_night_type | 주/야간 작업 구분 | Integer | 1 |
| product_date | 실제 생산 시간 | Datetime/String | 2022-08-10 7:57 |
| quantity | 생산 수량 | Integer | 5 |
| cnt | 생산 카운트 값 | Integer | 18133963 |

### 1.2.2. 프레스 유압모터 / 로봇 전류 데이터 구조

| 컬럼명 | 의미 | 데이터 타입 | 예시 |
| --- | --- | --- | --- |
| Unnamed: 0 | 데이터 인덱스 번호 | Integer | 1 |
| Time_s[s] | 센서 수집 시간(Timestamp) | Datetime/String | 2022-07-17 12:09 |
| RMS[A] | 전류 RMS(Root Mean Square) 값 | Float | 1.971987844 |

※ 일부 데이터셋은 `RMS[A]` 대신 `Acceleration[g]` 컬럼 사용

### 1.2.3. 프레스 2호 데이터 구조 (진동/가속도 기반)

| 컬럼명 | 의미 | 데이터 타입 | 예시 |
| --- | --- | --- | --- |
| Unnamed: 0 | 데이터 인덱스 번호 | Integer | 1 |
| Time_s[s] | 센서 수집 시간 | Datetime/String | 2022-07-17 12:10 |
| Acceleration[g] | 가속도/진동 값 | Float | 0.007593981 |

### 1.2.4. 데이터셋 파일

공정 데이터 2022년 8월.csv

로봇 1호-전류 데이터.csv

로봇 2호-전류 데이터.csv

프레스 1호-유압모터 전류데이터.csv

프레스 2호-유압모터 전류데이터.csv

프레스 3호-유압모터 전류데이터.csv

프레스 4호-유압모터 전류데이터.csv

- https://www.kamp-ai.kr/aidataDetail?AI_SEARCH=%EC%86%8C%EC%84%B1%EA%B0%80%EA%B3%B5+%EC%9E%90%EC%9B%90%EC%B5%9C%EC%A0%81%ED%99%94+AI+%EB%8D%B0%EC%9D%B4%ED%84%B0%EC%85%8B&page=1&DATASET_SEQ=46&DISPLAY_MODE_SEL=CARD&EQUIP_SEL=&GUBUN_SEL=&FILE_TYPE_SEL=&WDATE_SEL=
- 데이터 개수: 717,774개 / 데이터셋 파일 확장자: .xls
- 생산 부품에 따라 변동되는 에너지 소모량 예측을 위한 전류 데이터
- 단조프레스 연속공정은 자동차 부품 생산을 위해 여러 대의 프레스 설비가 연속적으로 동작하는 공정으로, 많은 전기에너지를 소모한다.
- 생산되는 부품 종류와 공정 상태에 따라 전력 사용량이 달라질 수 있으며, 이를 예측하기 위한 AI 기반 에너지 분석이 요구된다.
- 본 데이터셋은 단조프레스 공정의 전력 사용량을 분석하여 에너지 자원 최적화 및 운영 효율 향상을 위한 AI 모델 개발에 활용된다.

## 1.3. 머신비전 AI 데이터셋 (열화상 기반 품질 검사 데이터) - 차체&도장&의장

### 1.3.1. 데이터 구조

해당 머신비전 학습통합 데이터셋은 크게 전류/센서 시계열 데이터(`*_data.csv`)와 라벨 데이터(`*_label.json`)로 구성되어 있습니다.

### 데이터 파일 구조

| 파일명 | 데이터 유형 | 설명 | 행(Row) 의미 | 열(Column) 의미 |
| --- | --- | --- | --- | --- |
| `2nd_process_left_data.csv` | 학습 데이터 | 좌측 공정(Left) 센서/전류 시계열 데이터 | 하나의 측정 샘플 | 시간 순서에 따른 센서값 |
| `2nd_process_right_data.csv` | 학습 데이터 | 우측 공정(Right) 센서/전류 시계열 데이터 | 하나의 측정 샘플 | 시간 순서에 따른 센서값 |
| `2nd_process_left_label.json` | 라벨 데이터 | Left 데이터의 정상/불량 여부 | 각 샘플의 정답(Label) | 0 또는 1 |
| `2nd_process_right_label.json` | 라벨 데이터 | Right 데이터의 정상/불량 여부 | 각 샘플의 정답(Label) | 0 또는 1 |

---

## CSV 데이터 구조 (`_data.csv`)

CSV 파일은 시계열(Time-Series) 형태의 데이터로 구성되어 있으며, 약 80개의 연속 측정값 컬럼을 포함하고 있습니다.

### 컬럼 구조 예시

| 컬럼명 | 의미 |
| --- | --- |
| `42.19` | 특정 시점의 센서값 |
| `42.465` | 다음 시점 센서값 |
| `42.877` | 다음 시점 센서값 |
| `...` | ... |
| `52.777` | 마지막 시점 센서값 |

※ 실제 컬럼명은 측정값 자체가 헤더로 잘못 저장된 형태로 보이며, 일반적으로는 `t1`, `t2`, `t3` 같은 시계열 인덱스로 변경하여 사용하는 것이 좋습니다.

---

## 데이터 샘플 형태

| t1 | t2 | t3 | t4 | ... | t80 |
| --- | --- | --- | --- | --- | --- |
| 42.534 | 42.689 | 42.877 | 43.594 | ... | 52.999 |
| 42.259 | 42.689 | 42.791 | 43.338 | ... | 52.856 |
- 하나의 행(Row)은 설비/공정의 1회 측정 데이터
- 값은 시간 흐름에 따른 센서값(전류, 진동, 압력 등으로 추정)
- 시계열 기반 이상탐지(AI 이상 감지) 학습에 적합한 구조

---

## Label JSON 구조

라벨 파일은 리스트(List) 형태로 저장되어 있습니다.

### 예시

```
[0.0,1.0,0.0,1.0]
```

---

## 라벨 의미 예시

| 값 | 의미 |
| --- | --- |
| `0` | 정상(Normal) |
| `1` | 이상/불량(Abnormal) |

※ 실제 의미는 데이터셋 제공 문서 기준으로 최종 확인 필요

---

## 전체 데이터셋 특징 요약

| 항목 | 내용 |
| --- | --- |
| 데이터 형태 | 시계열(Time-Series) |
| 주요 목적 | 설비 이상 탐지 / 머신비전 AI 학습 |
| 입력 데이터 | 연속 센서값 |
| 출력 데이터 | 정상/불량 라벨 |
| 활용 가능 분야 | 스마트팩토리, 예지보전(Predictive Maintenance), 품질 검사 |
| 학습 방식 | 지도학습(Supervised Learning) 가능 |
| 추천 모델 | LSTM, CNN-1D, Transformer, AutoEncoder |

06. Dataset_Machinevision.zip

- https://www.kamp-ai.kr/aidataDetail?AI_SEARCH=%EB%A8%B8%EC%8B%A0%EB%B9%84%EC%A0%84+AI+%EB%8D%B0%EC%9D%B4%ED%84%B0%EC%85%8B&page=1&DATASET_SEQ=6&DISPLAY_MODE_SEL=CARD&EQUIP_SEL=&GUBUN_SEL=&FILE_TYPE_SEL=&WDATE_SEL=
- 열화상 이미지를 이용한 양/불량 판정을 위한 머신비전 데이터, 자동차 윈드실드 사이드 몰딩 사출품의 양품/불량품 판정을 위해 적외선 카메라를 사용하여 제품의 온도 분포에 따라 품질을 선별하기위한 제조 AI분석과정을 담은 데이터셋
- 사출 제품을 일정시간 안에 열화상 카메라로 촬영한 이미지 데이터를 수집하고 서포트벡터머신(SVM) 알고리즘을 사용하여 정확한 불량품 선별을 도모
- 데이터 개수 : 1,674개 / 확장자 : csv, json
    - 분석에 사용된 변수명 : 320x256의 열화상 이미지의 모든 온도 raw data, 제품 두께 데이터
    - 수집 방법 : 학습 데이터는 사출 제품을 일정시간안에 열화상 카메라로 촬영하여 취득. 라벨 데이터는 수기 기록(두께)
- 열화상 카메라를 통해 수집한 온도 분포 데이터를 기반으로 양품/불량품을 판별하는 머신비전 기반 품질 검사 데이터셋
- 자동차 윈드실드 사이드 몰딩 사출품의 열화상 이미지와 두께 데이터를 활용하여 품질 예측 수행
- 비선형 SVM 기반 AI 모델을 사용하여 불량품 자동 판정 및 품질 검사 자동화 가능
- OpenCV, Computer Vision, Thermal Vision 기반 AI 품질 검사 기능 구현에 적합
- 생산 라인의 품질 검사 이벤트를 실시간 관제 시스템과 연계하여 불량률 분석 및 제조 품질 모니터링 가능
- 스마트팩토리 통합 관제 환경에서 설비 이벤트와 품질 검사 이벤트를 함께 분석하는 구조로 확장 가능

### 1.3.2. Bosch Production Line Performance(kaggle) 데이터 구조

bosch-production-line-performance.zip

| 데이터 파일 | 전체 컬럼 수 | 주요 컬럼 구조 | 컬럼 패턴 | 설명 |
| --- | --- | --- | --- | --- |
| train_numeric.csv | 970개 | Id, L*_S*_F*, Response | L0_S0_F0 형태 | 제조 공정의 수치형 센서/측정 데이터 |
| train_categorical.csv | 2,141개 | Id, L*_S*_F* | L0_S1_F25 형태 | 제조 공정의 범주형/상태성 데이터 |
| train_date.csv | 1,157개 | Id, L*_S*_D* | L0_S0_D1 형태 | 제조 공정 단계별 시간/날짜성 데이터 |

| 공통 구조 | 의미 |
| --- | --- |
| Id | 제품 또는 생산 샘플 식별자 |
| L0, L1, L2, L3 | 생산 라인 또는 공정 구간 |
| S0, S1, S2 ... | 해당 라인 내 세부 스테이션/설비 |
| F0, F2 ... | Feature 컬럼, 측정값 또는 상태값 |
| D1, D3 ... | Date 컬럼, 공정 시간 정보 |
| Response | 불량 여부 라벨. train_numeric.csv에만 포함됨 |

| 데이터 파일 | 라인별 컬럼 수 |
| --- | --- |
| train_numeric.csv | L0: 168개, L1: 513개, L2: 42개, L3: 245개 |
| train_categorical.csv | L0: 323개, L1: 1,227개, L2: 159개, L3: 431개 |
| train_date.csv | L0: 184개, L1: 621개, L2: 78개, L3: 273개 |

| 데이터 파일 | 예시 컬럼 |
| --- | --- |
| train_numeric.csv | Id, L0_S0_F0, L0_S0_F2, L0_S0_F4, ..., Response |
| train_categorical.csv | Id, L0_S1_F25, L0_S1_F27, L0_S1_F29, ... |
| train_date.csv | Id, L0_S0_D1, L0_S0_D3, L0_S0_D5, ... |
- Bosch Production Line Performance (Kaggle)
- 자동차 부품 및 전자 부품 제조 공정에서 수집된 대규모 생산라인 데이터를 기반으로 불량 제품을 예측하는 제조 AI 데이터셋
- 독일 제조 기업 Bosch의 실제 생산라인 측정 데이터를 기반으로 구성된 Kaggle 제조 AI Competition 데이터셋
- 제조 공정 각 단계(Line / Station / Feature)에서 수집된 수천 개의 센서·측정 데이터를 활용하여 품질 불량 여부(Response)를 예측하는 목적의 데이터셋
- 스마트팩토리 제조 공정에서 가장 현실적인 “대규모 생산라인 이벤트 기반 데이터셋” 중 하나로 평가됨
- Bosch 데이터 상세
    
    # 분류 카테고리
    
    - 제조 품질 예측(Quality Prediction)
    - 생산라인 불량 탐지(Defect Detection)
    - 제조 공정 이상 탐지(Manufacturing Anomaly Detection)
    - 스마트팩토리 생산 공정 분석
    - 대규모 제조 시계열/이벤트 분석
    - Industrial AI / Manufacturing AI
    - 예지 품질 관리(Predictive Quality)
    
    ---
    
    # 데이터셋 구성
    
    이 데이터셋은 크게 7개 파일로 구성됩니다.
    
    | 데이터셋 파일 | 설명 | 확장자 |
    | --- | --- | --- |
    | train_numeric | 학습용 수치형 센서 데이터 + Response(Label 포함) | CSV |
    | test_numeric | 테스트용 수치형 센서 데이터 | CSV |
    | train_categorical | 학습용 범주형 데이터 | CSV |
    | test_categorical | 테스트용 범주형 데이터 | CSV |
    | train_date | 학습용 시간(Date/Timestamp) 데이터 | CSV |
    | test_date | 테스트용 시간(Date/Timestamp) 데이터 | CSV |
    | sample_submission | 제출 예시 파일 | CSV |
    
    ---
    
    # 데이터 규모
    
    | 항목 | 내용 |
    | --- | --- |
    | 학습 데이터 | 약 1,183,747개 |
    | 테스트 데이터 | 약 1,183,748개 |
    | 전체 Feature 수 | 약 3,000개 이상 |
    | Numeric Feature | 약 968개 |
    | Date Feature | 약 1,156개 |
    | Categorical Feature | 약 2,140개 |
    | 데이터 크기 | 약 14GB 이상 |
    | 라벨 | Response (0=정상, 1=불량) |
    
    ---
    
    # 데이터 특징
    
    ## 1. Numeric 데이터
    
    - 센서 측정값
    - 공정 수치 데이터
    - 압력/온도/측정값 계열
    - 연속형 데이터
    
    예:
    
    - L0_S0_F0
    - L3_S36_F3939
    
    (Line / Station / Feature 구조)
    
    ---
    
    ## 2. Categorical 데이터
    
    - 공정 상태
    - 설비 상태
    - 특정 공정 결과
    - 문자열 기반 제조 이벤트
    
    ---
    
    ## 3. Date 데이터
    
    - 공정 수행 시간
    - Station 통과 Timestamp
    - 생산 순서 분석 가능
    
    예:
    
    - L0_S0_D1
    
    ---
    
    # 모빌리티 스마트팩토리 관제 시스템 적합성
    
    매우 적합합니다.
    
    오히려 사용자가 현재 만들고 있는:
    
    - 모빌리티 스마트팩토리 관제
    - Kafka 기반 이벤트 수집
    - Redis Queue 처리
    - Elasticsearch 기반 검색
    - AI 이상 탐지
    - 실시간 생산라인 모니터링
    
    구조와 가장 유사한 공개 데이터셋 중 하나입니다.
    
    ---
    
    # 사용자 프로젝트와 연결 가능한 요소
    
    | Bosch 데이터 | 사용자 관제 시스템 |
    | --- | --- |
    | 생산라인(Line) | Factory / Line 구조 |
    | Station | 설비(Equipment) |
    | Timestamp | Kafka Event Time |
    | Numeric Sensor | IoT 센서 데이터 |
    | Response | 장애/불량 이벤트 |
    | 공정 통과 여부 | 생산 흐름 추적 |
    | Feature 수천개 | 대규모 이벤트 처리 |
    | 불균형 데이터 | 이상 탐지 AI |
    | 제조 이벤트 | 실시간 관제 이벤트 |

# 프레스&차체&도장&의장 기능

# 2. 제조 공정별 기능 상세화 + 데이터셋 활용 구조

## 2.1. 프레스 (차체 외판, 문, 보닛, 루프 등)

### 2.1.1. 프레스 공정

철판을 눌러 차체 부품을 만드는 공정

## 주요 기능

| 기능 | 상세 설명 | 활용 데이터셋 | 모델 적용 여부 | 적용 알고리즘 / 모델 | 알고리즘 설명 | 판단 기준 예시 |
| --- | --- | --- | --- | --- | --- | --- |
| 프레스 이상 정지 탐지 | 프레스 설비가 생산 중 갑자기 멈추거나 생산 주기가 비정상적으로 길어지는 상황을 탐지함 | 공정 데이터 2022년 8월, 프레스 전류 데이터, Bosch Dataset | AI 미적용 (룰 기반) | Kafka Stream + Rule Engine | 실시간 생산 이벤트와 전류 데이터를 기반으로 cnt 증가 여부, 생산 주기 지연, 전류 급감 패턴을 분석하여 설비 이상 정지를 탐지함 | cnt 증가 정체, 전류값 급감, Timestamp 지연 |

---

## 실제 활용 방식

### Ford Dataset 활용

시계열 진동 패턴 기반:

- 금형 진동 변화 탐지
- 모터 진동 증가 분석
- 프레스 충격 패턴 추적
- 반복 이상 진동 이벤트 분석

수행.

---

### 소성가공 데이터 활용

`RMS[A]` 기반:

- 설비 부하 상태 분석
- 모터 과부하 탐지
- 전류 Peak 이벤트 감지
- 설비 정지 상태 추적

수행.

---

### Bosch Dataset 활용

Line / Station / Timestamp 기반:

- 생산라인 이벤트 흐름 분석
- 설비 이상 이벤트 추적
- 생산 지연 및 병목 분석
- 반복 이상 패턴 탐지

수행.

## 2.2 차체

### 2.2.1. 차체 공정

용접 로봇 및 차체 조립 설비를 통해 차량 차체를 생산하는 공정

## 주요 기능

| 기능 | 상세 설명 | 활용 데이터셋 | 모델 적용 여부 | 적용 알고리즘 / 모델 | 알고리즘 설명 | 판단 기준 예시 |
| --- | --- | --- | --- | --- | --- | --- |
| 로봇 이상 동작 및 충돌 위험 탐지 | 용접 로봇의 반복 이동 패턴 이상 및 충돌 위험 상황을 탐지함 | Ford Dataset, 로봇 전류 데이터 | AI 미적용 (통계/룰 기반) | 시계열 패턴 분석 + Rule Engine | 로봇 암 이동 패턴, 진동 변화, 전류 RMS 변화를 기반으로 반복적인 비정상 움직임 및 충돌 위험 이벤트를 탐지함 | 반복 이동 오류, 진동 급증, 전류 RMS 급증 |

---

## 실제 활용 방식

### 로봇 전류 데이터 활용

`RMS[A]` 기반:

- 다중 로봇 간 전류 패턴 비교
- 특정 로봇 이상 상태 탐지
- 모터 과부하 및 전류 Peak 분석
- 생산 주기 이상 탐지

수행.

---

### Ford Dataset 활용

시계열 진동 패턴 기반:

- 로봇 암 진동 증가 탐지
- 충돌 위험 이벤트 감지
- 반복 이동 패턴 이상 분석
- 비정상 동작 탐지

수행.

---

### Bosch Dataset 활용

Station / Timestamp 기반:

- 생산 공정 흐름 추적
- 공정 병목 탐지
- 생산 지연 분석
- 품질 이벤트 및 반복 불량 추적

수행.

### 2.2.2 조립 관련 로봇팔 데이터

- **로봇팔 용접 전력사용 및 시간**
    
    링크 : https://www.kamp-ai.kr/aidataDetail?AI_SEARCH=&page=1&DATASET_SEQ=51&DISPLAY_MODE_SEL=CARD&EQUIP_SEL=&GUBUN_SEL=C004025&FILE_TYPE_SEL=C005002&WDATE_SEL=
    
    데이터셋
    
    | 컬럼명 | 의미 | 예시값 |
    | --- | --- | --- |
    | WK_DT | 작업 일자 | 2.02201E+16 |
    | PIPE_NO | 파이프 제품 번호 | PP22041200707 |
    | DV_R | 직경 편차 | 308 |
    | DA_R | 평균 직경 | 7962 |
    | AV_R | 검사 항목 평균값 | 364 |
    | AA_R | 평균 면적 | 5975 |
    | PM_R | 공정 측정 종학값 | 9270 |
    | FIN_JGMT | 최종 판정 결과 | 1 |
    
    데이터량 : 654,200(4.153mb)
    
- **로봇팔 용접기 진동 예지**
    
    링크 : https://www.kamp-ai.kr/aidataDetail?AI_SEARCH=진동&page=1&DATASET_SEQ=45&DISPLAY_MODE_SEL=CARD&EQUIP_SEL=&GUBUN_SEL=&FILE_TYPE_SEL=&WDATE_SEL=
    
    데이터셋
    
    | 컬럼명 | 의미 | 예시값 |
    | --- | --- | --- |
    | Time | 측정 시간 | 2021/08/02 6:47 |
    | 0 | 0hz | 0.00044808 |
    | 3.12 | 3.12hz | 0.000633875 |
    | 6.25 | 6.25hz | 0.000895333 |
    | 9.38 | 9.38hz | 0.001123601 |
    | 12.5 | 12.5hz | 0.000332504 |
    | 15.62 | 15.62hz | 0.000583095 |
    | 18.75 | 18.75hz | 0.000241952 |
    | 21.88 | 21.88hz | 0.000742734 |
    | 25 | 25hz | 0.001193284 |
    | 28.12 | 28.12hz | 0.001019982 |
    | 31.25 | 31.25hz | 0.000571357 |
    | 34.38 | 34.38hz | 0.000844553 |
    | 37.5 | 37.5hz | 0.000377998 |
    | 40.62 | 40.62hz | 0.00056753 |
    
    데이터량 : 9,358,120(105mb)
    

## 2.3. 도장(외부 도색)

### 2.3.1. 도장 공정

차량 외부를 도색하여 품질과 외관을 완성하는 공정

| 기능 | 상세 설명 | 활용 데이터셋 | 모델 적용 여부 | 적용 알고리즘 / 모델 | 알고리즘 설명 | 판단 기준 예시 |
| --- | --- | --- | --- | --- | --- | --- |
| 도장 품질 이상 탐지 | 차량 외부 도장 과정에서 도장 불균형, 도장 누락, 표면 품질 저하 등이 발생하는 상황을 탐지함 | 머신비전 데이터, Bosch Dataset | AI 미적용 (통계/룰 기반) | 시계열 통계 분석 + Rule Engine | 도장 공정의 품질 이벤트, 불량률 변화, 생산 이벤트 흐름, 센서값 변화를 기반으로 반복적인 품질 이상 패턴을 탐지함. 공정별 품질 이벤트 로그와 생산 흐름 중심의 실시간 품질 관제를 수행함 | 품질 이벤트 증가, 불량률 증가, Response 증가, 공정별 반복 이상 발생 |

---

## 실제 활용 방식

### 머신비전 데이터 활용

품질 이벤트 및 시계열 패턴 기반:

- 도장 품질 이상 탐지
- 반복 불량 이벤트 분석
- 공정별 품질 변화 추적
- 생산 품질 이벤트 모니터링

수행.

---

### Bosch Dataset 활용

Numeric / Response / Timestamp 기반:

- 생산라인 품질 흐름 분석
- 공정별 불량률 증가 추적
- 생산 지연 및 병목 분석
- 반복 이상 공정 탐지

수행.

---

### 환경 센서 데이터 활용

온도 / 습도 / VOC / 공조 상태 기반:

- 유해가스(VOC) 증가 감지
- 도장 환경 이상 탐지
- 공조 시스템 상태 모니터링
- 작업자 안전 이벤트 감시

수행.

### 2.3.2 도장 관련 표면처리 습도 데이터

링크 : https://www.kamp-ai.kr/aidataDetail?AI_SEARCH=&page=2&DATASET_SEQ=56&DISPLAY_MODE_SEL=CARD&EQUIP_SEL=&GUBUN_SEL=C004025&FILE_TYPE_SEL=C005002&WDATE_SEL=

데이터셋

| 컬럼명 | 의미 | 예시값 |
| --- | --- | --- |
| Datetime | 측정 일시 | 2021-02-08 0:15 |
| Production | 생산량 | 116 |
| Temperature | 온도 | -4.2 |
| Humidity | 습도 | 65.6 |
| Power_Cost | 전력 단가 또는 전기 요금 지수 | 109.8 |
| DoW | 요일(Day of Week) | Monday |
| Worker_Power | 작업 인력 투입 지표 | 0.13 |
| Man_Cost | 인건비 또는 작업 비용 | 1.5 |
| Power_Usage | 전력 사용량 | 103 |

데이터량 : 220,311(1.33mb)

## 2.4. 의장 조립 (엔진, 시트 조립 및 배선)

### 2.4.1. 의장 공정

부품을 조립하여 완성차를 구성하는 최종 조립 공정

## 주요 기능

| 기능 | 상세 설명 | 활용 데이터셋 | 모델 적용 여부 | 적용 알고리즘 / 모델 | 알고리즘 설명 | 판단 기준 예시 |
| --- | --- | --- | --- | --- | --- | --- |
| 조립 순서 오류 탐지 | 엔진, 시트, 배선 등의 조립 순서가 잘못 수행된 상황을 탐지함 | 공정 데이터 2022년 8월, Bosch Dataset | AI 미적용 (룰 기반) | Rule Engine | 생산 공정의 Timestamp 및 product_date 흐름을 기반으로 공정 순서를 비교하여 이상 여부를 판단함. 정해진 공정 순서와 다른 이벤트 발생 시 오류로 탐지함 | product_date 순서 이상, Timestamp 지연 |

---

## 실제 활용 방식

### 공정 데이터 활용

`lineno`, `product_date`, `quantity` 기반:

- 생산 추적
- 작업 이력 분석
- 공정 순서 검증
- 생산 흐름 분석

수행.

---

### 머신비전 데이터 활용

품질 이벤트 및 시계열 패턴 기반:

- 부품 누락 이벤트 탐지
- 조립 상태 분석
- 체결 이상 이벤트 분석
- 반복 품질 이상 추적

수행.

---

### Bosch Dataset 활용

Line / Station / Response / Timestamp 기반:

- 생산라인 불량 분석
- 공정 병목 및 지연 탐지
- 제조 공정 이상 이벤트 추적
- 반복 불량 패턴 분석

수행.

## 2.5. Bosch Production Line Performance 데이터셋 역할

### 2.5.1. 주요 활용 분야

| 활용 분야 | 설명 |
| --- | --- |
| 생산라인 불량 예측 | 공정별 센서 데이터를 기반으로 최종 불량 여부(Response) 예측 |
| 공정 병목 탐지 | 특정 Station 또는 Line에서 이벤트 적체 및 지연 분석 |
| 제조 공정 이상 탐지 | 생산라인 전체의 이상 패턴 및 불량 증가 탐지 |
| 대규모 이벤트 분석 | 수천 개 Feature 기반 제조 이벤트 분석 |
| 스마트팩토리 디지털 트윈 | 실제 생산라인 구조 기반 공정 시뮬레이션 |

### 2.5.2. Bosch Dataset 주요 구조 활용

| 데이터 종류 | 활용 방식 |
| --- | --- |
| Numeric Feature | 센서값/설비 상태값 분석 |
| Date Feature | 공정 시간 흐름 및 병목 분석 |
| Categorical Feature | 공정 상태 분석 |
| Response(Label) | 정상/불량 분류 |

### 2.5.3. Bosch 데이터셋 핵심 역할

| 데이터셋 | 핵심 역할 |
| --- | --- |
| Ford Dataset | 설비 이상 탐지 |
| 소성가공 데이터 | 프레스 및 생산라인 운영 분석 |
| 머신비전 데이터 | 품질 검사 및 불량 탐지 |
| Bosch Dataset | 전체 생산라인 통합 분석 및 제조 AI |

---

# MVP 기능

## 📊 제조 공정 AI 분석

!image.png

### 공정 간 불량 전이 예측 및 제조 병목 탐지

불량 전이 예측 프로세스 

---

## 1. 기능 요약

### 제조 병목 탐지

Bosch Production Line Performance Dataset의 Station 통과 시간 데이터를 분석하여 생산 공정 내 병목 구간을 실시간 탐지합니다.

공정별 처리 시간, 대기 시간, 체류 시간을 기반으로 병목 위험도를 산출하며, 특정 Station에서 발생한 지연이 전체 생산 흐름에 미치는 영향을 시각적으로 제공합니다.

---

### 공정 간 불량 전이 예측

Bosch Production Line Performance Dataset의 수치형 센서 데이터와 공정 시간 데이터를 활용하여 특정 공정에서 발생한 이상 징후가 후속 공정의 불량으로 이어질 가능성을 예측합니다.

현재 공정 상태는 정상이라도 후속 공정에서 발생 가능한 품질 이상을 사전에 탐지하여 생산 손실을 최소화합니다.

---

## 2. 데이터 및 AI 모델 아키텍처

### 활용 데이터셋

### Bosch Production Line Performance Dataset

사용 파일

- train_numeric.csv
- train_date.csv
- train_categorical.csv

활용 목적

- 공정 병목 탐지
- 공정 간 불량 전이 예측
- 제조 이력 분석
- 생산 흐름 시각화

---

### 머신비전 열화상 품질 데이터셋

활용 목적

- 품질 검사 결과 생성
- 불량 여부 검증
- AI 예측 결과 보조 학습 데이터 활용

---

## 3. 적용 AI 모델

### 제조 병목 탐지

### Rule Engine

공정별 처리 시간과 대기 시간을 분석하여 임계값 초과 시 병목 이벤트를 생성합니다.

예시

- 평균 처리 시간 대비 30% 이상 증가
- Station 체류 시간 급증
- 생산 대기열 증가

---

### Isolation Forest

정상 생산 패턴과 다른 비정상 공정 흐름을 탐지하여 병목 위험도를 계산합니다.

탐지 대상

- 비정상 체류 시간
- 비정상 통과 시간
- 특정 Station 집중 현상

---

### 공정 간 불량 전이 예측

### LightGBM Classifier

train_numeric.csv와 train_date.csv를 결합하여 학습합니다.

공정별 센서 데이터와 시간 정보를 기반으로 후속 공정 불량 발생 확률을 예측합니다.

예시

- 차체 공정 불량 확률 25%
- 도장 공정 불량 확률 78%
- 의장 공정 불량 확률 42%

---

## 4. 데이터 파이프라인

### Kafka 기반 이벤트 스트리밍

공정 이벤트를 Kafka Topic으로 수집합니다.

수집 데이터

- Product ID
- Station ID
- Process Time
- Waiting Time
- Defect Probability

각 이벤트는 생산 이력 단위로 연결되어 전체 제조 흐름을 구성합니다.

---

### Elasticsearch 기반 로그 분석

실시간 분석 결과를 Elasticsearch에 저장합니다.

주요 기능

- 병목 공정 검색
- 불량 이력 검색
- 제조 이력 조회
- 공정 영향도 분석

---

## 5. AI 분석 기능

### 실시간 병목 분석

분석 데이터

- Station 통과 시간
- 대기 시간
- 공정 체류 시간

제공 정보

- 병목 공정
- 평균 지연 시간
- 영향 생산량
- 병목 위험도

예시

S32 공정

- 평균 지연 시간 : 14초
- 영향 제품 수 : 1,245개
- 위험도 : 92%

---

### 공정 간 불량 전이 예측

분석 데이터

- Numeric Sensor Data
- Date Feature
- 품질 검사 결과

제공 정보

- 후속 공정 불량 확률
- 예상 불량 공정
- 위험도 등급

예시

현재 제품 상태

- 차체 불량 확률 : 23%
- 도장 불량 확률 : 78%
- 의장 불량 확률 : 41%

예상 결과

도장 공정 불량 발생 가능성 높음

---

### AI 원인 분석

LightGBM Feature Importance를 활용하여 병목 및 불량 예측에 영향을 준 주요 원인을 제공합니다.

제공 정보

- 영향 Station
- 주요 Sensor Feature
- 시간 지연 영향도

예시

도장 공정 불량 예측 원인

1. Station S32 체류 시간 증가
2. L3 공정 센서 값 이상
3. 이전 공정 대기 시간 증가

---

## 6. 시각화 요소

### 실시간 병목 분석

- 공정 흐름도
- 병목 구간 강조
- 위험도 색상 표시

---

### 불량 전이 예측

- 공정별 불량 확률 게이지
- 위험도 카드
- 예측 결과 알림

예시

도장 공정 불량 확률

78%

원인

- S32 체류 시간 증가
- 이전 공정 지연 발생

---

### 제조 이력 타임라인

- 제품별 공정 이동 경로
- 공정별 처리 시간
- 병목 발생 시점
- 품질 검사 결과

# 이벤트 데이터 (json)

## 샘플DB 구조

```sql
CREATE TABLE manufacturing_event_json (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    event_id VARCHAR(100) NOT NULL,
    event_time DATETIME NOT NULL,

    car_master_id BIGINT NOT NULL,
    equipment_id BIGINT NOT NULL,

    process_code ENUM('PRESS', 'BODY', 'PAINT', 'ASSEMBLY') NOT NULL,
    station_code VARCHAR(50),

    equipment_code VARCHAR(50) NOT NULL,
    equipment_type VARCHAR(50),
    equipment_status VARCHAR(30),

    event_type VARCHAR(50),

    event_json JSON NOT NULL,

    is_sent TINYINT(1) DEFAULT 0,
    sent_at DATETIME NULL,

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_event_time_sent (event_time, is_sent),
    INDEX idx_process_time (process_code, event_time),
    INDEX idx_equipment_time (equipment_code, event_time),
    INDEX idx_car_time (car_master_id, event_time),
    INDEX idx_event_id (event_id),
    INDEX idx_equipment_id_time (equipment_id, event_time),

    CONSTRAINT fk_manufacturing_event_json_car_master
        FOREIGN KEY (car_master_id)
        REFERENCES car_master(id),

    CONSTRAINT fk_manufacturing_event_json_equipment
        FOREIGN KEY (equipment_id)
        REFERENCES equipment(id)
);
```

| 컬럼명 | 설명 | 왜 필요한지 |
| --- | --- | --- |
| `id` | sampleDB 내부 PK | DB에서 row를 구분하기 위한 기본키 |
| `event_id` | 이벤트 고유 ID | Kafka, mainDB, ES에서 같은 이벤트를 추적하기 위한 공통 ID |
| `event_time` | 이벤트 발생 시간 | Scheduler가 가상 시간 기준으로 Kafka에 발행할 때 필요 |
| `car_master_id` | 차량 마스터 ID | 차량별 공정 흐름, 차량별 이상 이력 조회에 필요 |
| `process_code` | 공정 코드 | `PRESS`, `BODY`, `PAINT`, `ASSEMBLY` 분석 분기 기준 |
| `station_code` | 스테이션 코드 | 같은 공정 안에서 어느 위치에서 발생한 이벤트인지 구분 |
| `equipment_code` | 설비 코드 | 설비별 상태, 이상, 가동률 계산 기준 |
| `equipment_type` | 설비 유형 | 프레스기, 로봇팔, 카메라, 컨베이어 등 분석 분기 보조 |
| `equipment_status` | 설비 상태 | `RUNNING`, `IDLE`, `STOPPED`, `ERROR` 등. mainDB 가동률 계산에 필요 |
| `event_type` | 이벤트 유형 | `PROCESS_STATUS`, `QUALITY_CHECK`, `EQUIPMENT_STATUS` 등 이벤트 성격 구분 |
| `event_json` | 전체 원천 JSON | Kafka로 그대로 발행할 실제 메시지 |
| `is_sent` | Kafka 발행 여부 | Scheduler가 아직 안 보낸 데이터만 조회하기 위해 필요 |
| `sent_at` | Kafka 발행 시간 | 언제 Kafka로 보냈는지 추적 |
| `created_at` | DB 저장 시간 | Python 전처리 결과가 저장된 시간 |
| `updated_at` | DB 수정 시간 | 재생성/upsert로 기존 이벤트 row가 갱신된 시간을 추적 |

## 최종 원천 관제 이벤트 JSON

 처리 완료 시간

```json
{
  "event": {
    "eventId": "EVT-20260616-000001",
    "eventTime": "2026-06-16T10:00:01",
    "eventCategory": "MANUFACTURING",
    "eventType": "PROCESS_STATUS",
    "eventName": "프레스 공정 통합 관제 이벤트"
  },
  "location": {
    "factoryCode": "AIMS_FACTORY_01",
    "lineCode": "PRESS_LINE_01",
    "processCode": "PRESS",
    "stationCode": "PRESS_STATION_01"
  },
  "equipment": {
    "equipmentCode": "EQ_PRESS_01",
    "equipmentName": "프레스 유압모터 1호",
    "equipmentType": "HYDRAULIC_PRESS"
  },
  "equipmentStatus": {
    "operationStatus": "RUNNING",
    "lastNormalTime": "2026-06-16T09:59:30",
    "statusChangedTime": "2026-06-16T10:00:01"
  },
  "product": {
    "carId": "CAR-000001",
    **"productId": "PRODUCT-000001",
    "itemNo": "76211-A3010-100",
    "quantity": 5,**
    "productionCount": 18133963
  },
  "sensor": {
    "sensorType": "MULTI_SENSOR",
    "current": {
      "rmsAmpere": 1.971987844,
      "maxAmpere": 2.15,
      "minAmpere": 1.72
    },
    "vibration": {
      "accelerationG": 0.007593981,
      "vibrationScore": 0.21,
      "vibrationRms": 0.18,
      "vibrationPeak": 0.31
    },
    "robotArmVibration": {
      "robotId": "ROBOT_ARM_01",
      "axis": "J1",
      "frequencyHz": 40.62,
      "amplitude": 0.00056753,
      "vibrationRms": 0.000633875,
      "vibrationPeak": 0.001193284,
      "vibrationScore": 0.27
    },
    "thermal": {
      "thermalScore": 43.2,
      "avgTemperature": 43.2,
      "maxTemperature": 52.9,
      "minTemperature": 42.1
    }
  },
  "manufacturing": {
    "processSequence": 1,
    "previousProcessCode": null,
    "nextProcessCode": "BODY",
    "targetQuantity": 200,
    "completedQuantity": 145
  },
  "processMetrics": {
    "cycleTimeSec": 42.5,
    "waitingTimeSec": 8.3,
    "processingTimeSec": 34.2,
    "stationDelaySec": 5.7,
    "throughputPerMin": 1.4,
    "queueLength": 7,
    "wipCount": 24,
    "equipmentIdleTimeSec": 12.1
  },
  "sourceTrace": {
    "fordRowId": 1,
    "formingRowId": 1,
    "robotArmVibrationRowId": 1,
    "machineVisionRowId": 1,
    "boschId": 1
  },
  "processData":{
    # 아래참고
  }
}
```

## 위 내용 중 ProcessData 내용 JSON

```jsx
📌 프레스
"processData": {
  "press": {
    "countIncreaseYn": true,
    "targetCycleTimeSec": 40.0,
    "timestampDelaySec": 5.7
  }
}
- countIncreaseYn : 생산 카운트가 증가했는지 확인 -> false면 정
- targetCycleTimeSec : 기준 cycle time. 실제 cycle time과 비교
- timestampDelaySec : 이벤트 지연 시간. 데이터 지연/설비 정지 판단

📌 차체
"processData": {
  "body": {
    "robotMotionStatus": "NORMAL",
    "robotOperationMode": "AUTO",
    "frequencyPeakBand": "501_600_HZ",
    "frequencyBands": {
      "freq_0_100_hz": 0.001193284,
      "freq_101_200_hz": 0.001987782,
      "freq_201_300_hz": 0.001072014,
      "freq_301_400_hz": 0.001792223,
      "freq_401_500_hz": 0.001924913,
      "freq_501_600_hz": 0.002907113,
      "freq_601_700_hz": 0.001207061,
      "freq_701_800_hz": 0.00114628,
      "freq_801_900_hz": 0.001170759,
      "freq_901_1000_hz": 0.001318642,
      "freq_1001_1100_hz": 0.001426342,
      "freq_1101_1200_hz": 0.001297966,
      "freq_1201_1300_hz": 0.001750256,
      "freq_1301_1400_hz": 0.001491831,
      "freq_1401_1500_hz": 0.001278729,
      "freq_1501_1600_hz": 0.000994307
    }
  }
}
- robotMotionStatus : 로봇 동작 상태
- robotOperationMode : 로봇 운전 모드 (AUTO, MANUAL, STOPPED 등 운전 상태 확인)
- frequencyPeakBand : 가장 진동이 크게 나온 주파수 대역
- frequencyBands : 주파수 대역별 진동값

📌 도장
"processData": {
  "paint": {
    "imagePosition": "LEFT",
    "thermalStdTemp": 4.2,
    "thicknessValue": 116.5,
    "defectScore": 0.87,
    "visionLabel": "DEFECT",
    "surfaceQualityScore": 72.3
  }
}
- imagePosition : 좌/우 위치별 불량 확인 (촬영 이미지 위치)
- thermalStdTemp : 온도 편차. 표면 균일도 판단
- thicknessValue : 도장 두께 판단
- defectScore : 비전 기반 불량 점수
- visionLabel : 원천 비전 라벨
- surfaceQualityScore : 화면 표시용 품질 점수

📌 의장
"processData": {
  "assembly": {
    "expectedSequence": "A01>A02>A03>A04",
    "actualSequence": "A01>A03>A02>A04",
    "missingPartCount": 0,
    "fasteningErrorCount": 1,
    "sequenceErrorCount": 1
  }
}
- expectedSequence : 기준 작업 순서
- actualSequence : 실제 작업 순서
- missingPartCount : 누락 부품 수
- fasteningErrorCount : 체결 오류 수
- sequenceErrorCount : 작업 순서 오류 수

```

## 구조 요약

```
event                이벤트 기본 정보
location             공장/라인/공정/스테이션 위치 정보
equipment            설비 기본 정보
equipmentStatus      설비 가동 상태/설비 건강 상태/상태 변경 정보
product              차량/제품/생산 정보

sensor
├─ current                프레스/로봇 전류 데이터
├─ vibration              일반 설비 진동/가속도 데이터
├─ robotArmVibration      로봇팔 전용 진동 데이터
└─ thermal                열화상/온도 데이터

manufacturing        공정 순서/생산 목표/완료 수량 정보
processMetrics       공정 시간/처리량/대기열/WIP 지표
sourceTrace          원본 데이터 추적용
```

---

# event

이벤트 자체에 대한 메타 정보입니다.

| 컬럼 | 설명 |
| --- | --- |
| eventId | 이벤트 고유 식별자 |
| eventTime | 실제 공정 이벤트 발생 시간 |
| eventCategory | 이벤트 대분류 (`MANUFACTURING`) |
| eventType | 이벤트 유형 (`PROCESS_STATUS`, `QUALITY_CHECK`, `EQUIPMENT_SENSOR`) |
| eventName | 사람이 읽기 쉬운 이벤트명 |

예시

```json
{
  "eventId":"EVT-20260616-000001",
  "eventType":"PROCESS_STATUS"
}
```

---

# location

이벤트 발생 위치 정보입니다.

| 컬럼 | 설명 |
| --- | --- |
| factoryCode | 공장 식별 코드 |
| lineCode | 생산 라인 코드 |
| processCode | 공정 코드 (`PRESS`, `BODY`, `PAINT`, `ASSEMBLY`) |
| stationCode | 스테이션 코드 |

예시

```
공장
 └─ 생산라인
      └─ 공정
           └─ 스테이션
```

---

# equipment

설비 기본 정보입니다.

| 컬럼 | 설명 |
| --- | --- |
| equipmentCode | 설비 코드 |
| equipmentName | 설비명 |
| equipmentType | 설비 유형 |

설비 유형 예시

| processCode | equipmentType | 설명 |
| --- | --- | --- |
| PRESS | HYDRAULIC_PRESS | 유압 프레스 |
| BODY | ROBOT_ARM | 차체 로봇팔 |
| PAINT | CAMERA | 도장 검사 카메라 |
| ASSEMBLY | CONVEYOR | 조립 컨베이어 |

예시

```json
{
  "equipmentCode":"EQ_PRESS_01",
  "equipmentName":"프레스 유압모터 1호",
  "equipmentType":"HYDRAULIC_PRESS"
}
```

---

# equipmentStatus

설비 상태 정보입니다.

| 컬럼 | 설명 |
| --- | --- |
| operationStatus | 실제 설비 가동 상태 |
| healthStatus | 설비 건강 상태 |
| lastNormalTime | 마지막 정상 상태 시간 |
| statusChangedTime | 현재 상태로 변경된 시간 |

`operationStatus`

| 상태 | 의미 |
| --- | --- |
| RUNNING | 설비 가동 중 |
| STOP | 설비 정지 |
| IDLE | 대기 상태 |
| MAINTENANCE | 정비 중 |

`healthStatus`

| 상태 | 의미 | 설비 운영 여부 |
| --- | --- | --- |
| NORMAL | 정상 상태 | 운영 중 |
| WARNING | 경고 상태 | 운영 중 |
| FAULT | 고장 상태 | 운영 불가 |
| MAINTENANCE | 점검 상태 | 운영 중지 |

예시

```json
{
  "operationStatus":"RUNNING",
  "healthStatus":"WARNING",
  "lastNormalTime":"2026-06-16T09:59:30",
  "statusChangedTime":"2026-06-16T10:00:01"
}
```

---

# product

생산 대상 차량/제품 정보입니다.

| 컬럼 | 설명 |
| --- | --- |
| carId | 차량 식별 ID |
| productId | 생산 제품 ID |
| itemNo | 품목 번호 |
| quantity | 생산 수량 |
| productionCount | 누적 생산량 |

예시

```json
{
  "carId":"CAR-000001",
  "quantity":5,
  "productionCount":18133963
}
```

---

# sensor

여러 원본 데이터셋에서 가져온 센서 값을 관제용으로 통합한 영역입니다.

---

## sensor.current

전류 센서 정보

| 컬럼 | 설명 |
| --- | --- |
| rmsAmpere | RMS 전류 |
| maxAmpere | 최대 전류 |
| minAmpere | 최소 전류 |

데이터 출처

```
소성가공 데이터
RMS[A]
```

---

## sensor.vibration

설비 진동 정보

| 컬럼 | 설명 |
| --- | --- |
| accelerationG | 가속도 |
| vibrationScore | 진동 위험 점수 |
| vibrationRms | RMS 진동 |
| vibrationPeak | Peak 진동 |

데이터 출처

```
Ford Engine Dataset
소성가공 진동 데이터
```

---

## sensor.robotArmVibration

로봇팔 전용 진동 데이터

| 컬럼 | 설명 |
| --- | --- |
| robotId | 로봇 ID |
| axis | 관절 축 |
| frequencyHz | 진동 주파수 |
| amplitude | 진폭 |
| vibrationRms | RMS 진동 |
| vibrationPeak | Peak 진동 |
| vibrationScore | 진동 위험 점수 |

데이터 출처

```
Robot Arm Vibration Dataset
```

---

## sensor.thermal

열화상 정보

| 컬럼 | 설명 |
| --- | --- |
| thermalScore | 열화상 점수 |
| avgTemperature | 평균 온도 |
| maxTemperature | 최고 온도 |
| minTemperature | 최저 온도 |

데이터 출처

```
Machine Vision Dataset
```

---

# manufacturing

제조 운영 정보입니다.

| 컬럼 | 설명 |
| --- | --- |
| processSequence | 공정 순서 |
| previousProcessCode | 이전 공정 |
| nextProcessCode | 다음 공정 |
| targetQuantity | 목표 생산 수량 |
| completedQuantity | 완료 생산 수량 |

예시

```
PRESS
 ↓
BODY
 ↓
PAINT
 ↓
ASSEMBLY
```

공정 순서 매핑

| processCode | processSequence |
| --- | --- |
| PRESS | 1 |
| BODY | 2 |
| PAINT | 3 |
| ASSEMBLY | 4 |

---

# processMetrics

공정 성능 및 병목 판단용 지표입니다.

| 컬럼 | 설명 |
| --- | --- |
| cycleTimeSec | 생산 주기 |
| waitingTimeSec | 대기 시간 |
| processingTimeSec | 실제 작업 시간 |
| stationDelaySec | 지연 시간 |
| throughputPerMin | 분당 생산량 |
| queueLength | 대기열 수 |
| wipCount | 재공품 수 |
| equipmentIdleTimeSec | 설비 유휴 시간 |

예시

```
제품 투입
 ↓
8초 대기
 ↓
34초 작업
 ↓
42초 사이클 완료
```

---

# sourceTrace

원본 데이터 추적용 정보입니다.

| 컬럼 | 설명 |
| --- | --- |
| fordRowId | Ford 원본 행 번호 |
| formingRowId | 소성가공 원본 행 번호 |
| robotArmVibrationRowId | 로봇팔 진동 원본 행 번호 |
| machineVisionRowId | 머신비전 원본 행 번호 |
| boschId | Bosch 원본 ID |

---

# 최종 데이터셋 매핑

```
Ford Dataset
↓
sensor.vibration

소성가공 데이터
↓
sensor.current
sensor.vibration

Robot Arm Vibration Dataset
↓
sensor.robotArmVibration

Machine Vision Dataset
↓
sensor.thermal

Bosch Dataset
↓
manufacturing
processMetrics
event.eventTime
location
equipment
```

즉, 최종 원천 관제 이벤트는 데이터셋 기반 구조가 아니라 아래 기준으로 통합됩니다.

```
이벤트
↓
위치
↓
설비
↓
설비상태
↓
제품
↓
센서
 ├─ 전류
 ├─ 일반 진동
 ├─ 로봇팔 진동
 └─ 열화상
↓
제조 운영 정보
↓
공정 성능 지표
↓
원본 추적 정보
```

---

## 분석 결과 저장 기준

분석 결과와 이상 상태는 위 원천 이벤트를 백엔드/AI가 처리한 뒤 별도로 저장합니다.

| 결과 필드 | 입력 데이터 | 저장 위치 |
| --- | --- | --- |
| equipmentStatus.healthStatus | 전류, 진동, 설비 상태 | Redis / main_db |
| qualityStatus | 열화상 데이터 | Redis / main_db |
| productionStatus | 공정 지표 | Redis / main_db |
| pressStopRisk | 전류, 유휴시간 | main_db.analysis_result |
| robotCollisionRisk | robotArmVibration | main_db.analysis_result |
| paintQualityRisk | thermal | main_db.analysis_result |
| assemblySequenceRisk | processSequence | main_db.analysis_result |
| bottleneckRisk | processMetrics | main_db.analysis_result |
| defectTransferRisk | 센서 + 공정 데이터 | main_db.analysis_result |
| overallRiskScore | 전체 위험도 계산 결과 | main_db.analysis_result |
| riskLevel | LOW/WARNING/CRITICAL | Redis / ES / main_db |

정리하면 `att1`, `L0_S0_D1`, `RMS[A]`, `t1~t80` 같은 원본 컬럼은 저장하지 않고, 관제에 필요한 의미 있는 데이터로 변환하여 하나의 제조 이벤트 JSON으로 저장하는 구조입니다.

---

| 상태 | 의미 | 설비 운영 여부 |
| --- | --- | --- |
| NORMAL | 정상 가동 중 | 운영 중 |
| WARNING | 경고 상태 (이상 징후 발생) | 운영 중 |
| FAULT | 고장 상태 | 운영 불가 |
| MAINTENANCE | 점검/정비 중 | 운영 중지 |

# 이벤트 흐름 정리

```jsx
샘플 DB
  ↓
Scheduler가 JSON 1건씩 읽음
  ↓
factory.manufacturing.raw 로 원천 이벤트 발행
  ↓
Manufacturing Service / AI Service가 raw 이벤트 소비
  ↓
Spring 코드로 위험도, 병목 여부, 설비 이상 여부 계산
  ↓
계산 결과를 담아서 factory.manufacturing.analysis 로 발행
```

# 1. 데이터 준비 단계

## 목적

실시간 센서가 없는 시연 환경에서 제조 이벤트를 재생하기 위해 원본 데이터셋을 통합 제조 이벤트 JSON으로 변환하여 Sample DB에 저장합니다.

---

## 데이터 흐름

```
Ford Dataset
Bosch Dataset
Robot Arm Vibration Dataset
Machine Vision Dataset
소성가공 데이터

↓
Python ETL
↓
공통 제조 이벤트 구조 매핑
↓
통합 JSON 생성
↓
sample_db 저장
```

---

## 저장 테이블

```
manufacturing_event_json
```

역할

```
시연용 원천 이벤트 저장소

Scheduler가 읽어가는 이벤트 저장

Kafka 전송 대상 저장

전송 여부 관리
```

---

## Sample DB 구조

```sql
CREATE TABLE manufacturing_event_json (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    event_id VARCHAR(100) NOT NULL,
    event_time DATETIME NOT NULL,

    car_master_id BIGINT NOT NULL,
    equipment_id BIGINT NOT NULL,

    process_code ENUM('PRESS', 'BODY', 'PAINT', 'ASSEMBLY') NOT NULL,
    station_code VARCHAR(50),

    equipment_code VARCHAR(50) NOT NULL,
    equipment_type VARCHAR(50),
    equipment_status VARCHAR(30),

    event_type VARCHAR(50),

    event_json JSON NOT NULL,

    is_sent TINYINT(1) DEFAULT 0,
    sent_at DATETIME NULL,

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_event_time_sent (event_time, is_sent),
    INDEX idx_process_time (process_code, event_time),
    INDEX idx_equipment_time (equipment_code, event_time),
    INDEX idx_car_time (car_master_id, event_time),
    INDEX idx_event_id (event_id),
    INDEX idx_equipment_id_time (equipment_id, event_time),

    CONSTRAINT fk_manufacturing_event_json_car_master
        FOREIGN KEY (car_master_id)
        REFERENCES car_master(id),

    CONSTRAINT fk_manufacturing_event_json_equipment
        FOREIGN KEY (equipment_id)
        REFERENCES equipment(id)
);
```

---

# 2. Scheduler 구조

## 목적

Sample DB에 저장된 제조 이벤트를 실제 공장에서 발생하는 것처럼 순차적으로 Kafka에 전송합니다.

---

## 처리 흐름

```
Scheduler 실행

↓

is_sent = false 조회

↓

event_time 기준 정렬

↓

N건 조회

↓

Kafka Producer 호출

↓

전송 성공

↓

is_sent = true

↓

sent_at 저장
```

---

## 예시

### 시연 환경

```
1초마다 5건 전송
```

### 부하 테스트

```
1초마다 100건 전송
```

### 대량 데이터 재생

```
1초마다 1000건 전송
```

---

# 3. Kafka 구조

## Kafka 사용 목적

Kafka는 제조 이벤트를 서비스 간 전달하는 Event Backbone 역할을 수행합니다.

### Kafka 적용

```
sampleDB.manufacturing_event_json
↓
Scheduler Producer
↓
factory.manufacturing.raw
↓
Manufacturing Consumer / AI Consumer 
↓
분석 결과 생성
↓
factory.manufacturing.analysis
↓
mainDB / Redis / Elasticsearch 저장
↓
위험 이벤트만 분리
↓
factory.manufacturing.alert
↓
WebSocket / 알림 화면 / 알림 이력 저장

----------설비 상태 및 가동률 계산 흐름---------------------------

factory.manufacturing.raw
↓
Equipment Consumer
↓
설비 상태 / 가동 시간 / 정지 시간 / 유휴 시간 계산
↓
factory.manufacturing.equipment
↓
Redis / mainDB / Dashboard
```

하나의 서비스가 장애가 발생해도 다른 서비스는 정상 동작

---

### 비동기 처리

```
설비 이벤트 발생
↓
Kafka 저장
↓
각 서비스가 필요 시 처리
```

---

### Scale-Out 가능

```
AI Service 1대
↓
AI Service 3대
↓
AI Service 10대
```

Kafka Partition 기반 병렬 처리 가능

---

# 4. Kafka Topic 설계

## Topic 구성

```
factory.manufacturing.raw

factory.manufacturing.analysis

factory.manufacturing.alert

factory.manufacturing.equipment
```

---

## factory.manufacturing.raw

### 역할

원천 제조 이벤트 저장

### Producer

```
Scheduler
```

### Consumer

```
Manufacturing Service
AI Service
Equipment Service
Redis Consumer
Elasticsearch Consumer
```

### Message Key

```java
factory.manufacturing.raw

Message Key
- equipment.equipmentCode

// Raw Event
kafkaTemplate.send(
    "factory.manufacturing.raw",
    event.getEquipment().getEquipmentCode(),
    eventJson
);
```

---

예시 메시지

```json
{
  "event": {
    "eventId": "EVT-20260616-000001",
    "eventTime": "2026-06-16T10:00:01",
    "eventCategory": "MANUFACTURING",
    "eventType": "PROCESS_STATUS",
    "eventName": "프레스 공정 통합 관제 이벤트"
  },
  "location": {
    "factoryCode": "AIMS_FACTORY_01",
    "lineCode": "PRESS_LINE_01",
    "processCode": "PRESS",
    "stationCode": "PRESS_STATION_01"
  },
  "equipment": {
    "equipmentCode": "EQ_PRESS_01",
    "equipmentName": "프레스 유압모터 1호",
    "equipmentType": "HYDRAULIC_PRESS"
  },
  "equipmentStatus": {
    "operationStatus": "RUNNING",
    "lastNormalTime": "2026-06-16T09:59:30",
    "statusChangedTime": "2026-06-16T10:00:01"
  },
  "product": {
    "carId": "CAR-000001",
    "productId": "PRODUCT-000001",
    "itemNo": "76211-A3010-100",
    "quantity": 5,
    "productionCount": 18133963
  },
  "sensor": {
    "sensorType": "MULTI_SENSOR",
    "current": {
      "rmsAmpere": 1.97,
      "maxAmpere": 2.15,
      "minAmpere": 1.72
    },
    "vibration": {
      "accelerationG": 0.0075,
      "vibrationScore": 0.21,
      "vibrationRms": 0.18,
      "vibrationPeak": 0.31
    },
    "robotArmVibration": {
      "robotId": "ROBOT_ARM_01",
      "axis": "J1",
      "frequencyHz": 40.62,
      "amplitude": 0.00056,
      "vibrationRms": 0.00063,
      "vibrationPeak": 0.00119,
      "vibrationScore": 0.27
    },
    "thermal": {
      "thermalScore": 43.2,
      "avgTemperature": 43.2,
      "maxTemperature": 52.9,
      "minTemperature": 42.1
    }
  },
  "manufacturing": {
    "processSequence": 1,
    "previousProcessCode": null,
    "nextProcessCode": "BODY",
    "targetQuantity": 200,
    "completedQuantity": 145
  },
  "processMetrics": {
    "cycleTimeSec": 42.5,
    "waitingTimeSec": 8.3,
    "processingTimeSec": 34.2,
    "stationDelaySec": 5.7,
    "throughputPerMin": 1.4,
    "queueLength": 7,
    "wipCount": 24,
    "equipmentIdleTimeSec": 12.1
  },
  "processData": {
    "press": {
      "countIncreaseYn": true,
      "targetCycleTimeSec": 40.0,
      "timestampDelaySec": 5.7
    }
  },
  "sourceTrace": {
    "fordRowId": 1,
    "formingRowId": 1,
    "robotArmVibrationRowId": 1,
    "machineVisionRowId": 1,
    "boschId": 1
  }
}
```

### 공정 별(프레스/차체/도장/의장) 분기

현재 프로젝트 규모에서는 1개 Raw Topic으로 받고, `processCode`로 분기하는 방식이 더 적절합니다.

```
factory.manufacturing.raw
```

메시지 내부에서 분기합니다.

```
processCode = PRESS     → 프레스 로직
processCode = BODY      → 차체 로직
processCode = PAINT     → 도장 로직
processCode = ASSEMBLY  → 의장 로직
```

다만 공정별 트래픽, 처리 속도, Consumer가 명확히 달라지면 Topic을 나누는 것이 맞습니다.

```
factory.manufacturing.press.raw
factory.manufacturing.body.raw
factory.manufacturing.paint.raw
factory.manufacturing.assembly.raw
```

추천 기준은 아래와 같습니다.

```
초기/MVP/시연용
→ factory.manufacturing.raw 1개 추천

운영 확장/공정별 처리량 차이 큼
→ 공정별 Topic 분리 추천

공정별 Consumer가 완전히 다름
→ 공정별 Topic 분리 추천

프레스는 초당 1000건, 도장은 초당 10건처럼 차이가 큼
→ 공정별 Topic 분리 추천

공정별 장애가 다른 공정에 영향 주면 안 됨
→ 공정별 Topic 분리 추천
```

현재 AIMS 구조에서는 이렇게 가는 게 가장 깔끔합니다.

```
factory.manufacturing.raw
```

그리고 Consumer 내부에서 분기합니다.

```java
switch (processCode) {
	case"PRESS" -> pressService.analyze(event);
	case"BODY" -> bodyService.analyze(event);
	case"PAINT" -> paintService.analyze(event);
	case"ASSEMBLY" -> assemblyService.analyze(event);
}
```

---

## factory.manufacturing.analysis

### 역할

AI 분석, 이상 탐지 분석 결과 저장

### Producer

```
AI Service
Manufacturing Service
```

### Consumer

```
Equipment Service
Redis
main_db
Elasticsearch
Alert Service
```

---

### Message Key

```java
factory.manufacturing.analysis

Message Key
- 기본: equipment.equipmentCode
- analysisType = DEFECT_TRANSFER_PREDICTION 인 경우: carId

// 설비/공정 분석 결과
kafkaTemplate.send(
    "factory.manufacturing.analysis",
    analysis.getEquipmentCode(),
    analysisJson
);
```

---

예시

```json
{
  "analysisId": "ANL-20260616-000001",
  "eventId": "EVT-20260616-000001",
  "eventTime": "2026-06-16T10:00:01",
  "analyzedAt": "2026-06-16T10:00:03",
  "factoryCode": "AIMS_FACTORY_01",
  "lineCode": "PRESS_LINE_01",
  "processCode": "PRESS",
  "stationCode": "PRESS_STATION_01",
  "equipmentCode": "EQ_PRESS_01",
  "equipmentType": "HYDRAULIC_PRESS",
  "productId": "PRODUCT-000001",
  "carId": "CAR-000001",
  "analysisType": "PROCESS_RISK_ANALYSIS",
  "riskScores": {
    "overallRiskScore": 74.5,
    "bottleneckRisk": 72.1,
    "defectTransferRisk": 65.3,
    "equipmentRisk": 58.4,
    "processRisk" : {
      "pressStopRisk" : 64.2
      }
  },
  "riskLevel": "WARNING",
  "analysisResult": {
    "isAbnormal": true,
    "isBottleneck": true,
    "isQualityDefect": false,
    "isEquipmentFault": false,
    "isSequenceError": false
  },
  "reason": {
    "mainReason": "cycleTimeSec가 targetCycleTimeSec보다 증가했습니다.",
    "detailReasons": [
      "기준 사이클타임 40.0초 대비 실제 사이클타임 42.5초",
      "stationDelaySec 5.7초 발생",
      "queueLength 7로 대기열 증가"
    ]
  },
  "recommendation": {
    "actionType": "CHECK_EQUIPMENT_AND_QUEUE",
    "message": "프레스 설비 상태와 대기열 증가 원인을 확인하세요."
  }
}
```

### 분석결과 저장

```jsx
### 1. Main DB

분석 결과 및 이력성 데이터를 영속 저장한다.

- 병목 분석 결과
- 불량 전이 예측 결과
- 설비 이상 분석 결과
- 도장 품질 이상 분석 결과
- 의장 조립 순서 오류 결과

---

### 2. Redis

실시간 상태 조회 및 대시보드 표시를 위한 최신 데이터를 저장한다.

- 현재 공정 상태
- 현재 설비 상태
- 현재 위험도
- 대시보드 카드용 최신 값

---

### 3. Elasticsearch

검색, 이력 조회, 이벤트 집계를 위한 데이터를 저장한다.

- 이벤트 검색
- 알림 이력 검색
- 설비별 이상 이력 검색
- 시간대별 위험 이벤트 집계
```

---

## factory.manufacturing.alert

### 역할

긴급 알림 이벤트 전용

→ `factory.manufacturing.analysis`의 분석 결과 중 WARNING 또는 CRITICAL 상태만 선별하여 발행한다.

---

### Producer

```jsx
Alert Service 
Manufacturing Service
```

### Consumer

```jsx
WebSocket Service 
mainDB Consumer 
Elasticsearch Consumer
```

### 발행 조건

```
overallRiskScore >= 80 
riskLevel = CRITICAL 
설비 고장 발생 
품질 불량 발생 
병목 위험 발생 
조립 순서 오류 발생 
프레스 정지 위험 발생
```

---

예시

```json
{
  "alertId": "ALT-20260616-000001",
  "eventId": "EVT-20260616-000001",
  "analysisId": "ANL-20260616-000001",
  "createdAt": "2026-06-16T10:00:04",
  "factoryCode": "AIMS_FACTORY_01",
  "lineCode": "PRESS_LINE_01",
  "processCode": "PRESS",
  "stationCode": "PRESS_STATION_01",
  "equipmentCode": "EQ_PRESS_01",
  "equipmentName": "프레스 유압모터 1호",
  "alertType": "PRESS_STOP_RISK",
  "alertTitle": "프레스 설비 정지 위험",
  "alertMessage": "프레스 1호의 사이클타임과 지연 시간이 증가하여 정지 위험이 감지되었습니다.",
  "riskLevel": "CRITICAL",
  "riskScore": 86.7,
  "alertStatus": "OPEN",
  "needAction": true,
  "reason": [
    "cycleTimeSec가 기준값보다 증가",
    "stationDelaySec 증가",
    "equipmentIdleTimeSec 증가"
  ],
  "recommendedAction": "프레스 설비 상태, 대기열, 전류 RMS 값을 확인하세요."
}
```

### alertStatus 값

```jsx
OPEN 처리 전 
CHECKING 확인 중 
RESOLVED 조치 완료 
IGNORED 무시됨
```

### MessageKey

```java
factory.manufacturing.alert

Message Key
- equipment.equipmentCode

// Alert Event
kafkaTemplate.send(
    "factory.manufacturing.alert",
    alert.getEquipmentCode(),
    alertJson
);
```

---

## factory.manufacturing.equipment

### 역할

장비 전용 이벤트

→ 

Raw Event 안에도 `equipmentStatus`가 있지만, 그것은 단일 이벤트 시점의 원천 상태이다.

`factory.manufacturing.equipment`는 여러 Raw Event를 기반으로 계산한 설비 중심 상태 이벤트이다.

---

### Producer

```jsx
Equipment Service 
Manufacturing Service
```

### Consumer

```jsx
Equipment Service
Redis Consumer 
mainDB Consumer 
Elasticsearch Consumer 
Alert Service
```

### Message Key

```java
factory.manufacturing.equipment

Message Key
- equipment.equipmentCode

// Equipment Event
kafkaTemplate.send(
    "factory.manufacturing.equipment",
    equipmentEvent.getEquipmentCode(),
    equipmentJson
);
```

### 예시메세지

```json
{
  "equipmentEventId": "EQEVT-20260616-000001",
  "eventTime": "2026-06-16T10:00:05",
  "factoryCode": "AIMS_FACTORY_01",
  "lineCode": "PRESS_LINE_01",
  "processCode": "PRESS",
  "stationCode": "PRESS_STATION_01",
  "equipmentCode": "EQ_PRESS_01",
  "equipmentName": "프레스 유압모터 1호",
  "equipmentType": "HYDRAULIC_PRESS",
  "operationStatus": "RUNNING",
  "healthStatus": "WARNING",
  "riskLevel": "WARNING",
  "overallRiskScore": 72.4,
  "operationMetrics": {
    "windowStartTime": "2026-06-16T09:00:00",
    "windowEndTime": "2026-06-16T10:00:00",
    "plannedTimeSec": 3600,
    "runningTimeSec": 2880,
    "idleTimeSec": 420,
    "stopTimeSec": 240,
    "maintenanceTimeSec": 60,
    "operationRate": 80.0,
    "availabilityRate": 86.7
  },
  "productionMetrics": {
    "targetQuantity": 200,
    "completedQuantity": 145,
    "throughputPerMin": 1.4,
    "cycleTimeSec": 42.5
  },
  "sensorSummary": {
    "currentRmsAmpere": 1.97,
    "vibrationScore": 0.21,
    "thermalScore": 43.2
  },
  "statusReason": {
    "mainReason": "지연 시간 증가로 설비 위험도가 WARNING 상태입니다.",
    "detailReasons": [
      "stationDelaySec 5.7초",
      "equipmentIdleTimeSec 12.1초",
      "cycleTimeSec 42.5초"
    ]
  },
  "lastRawEventId": "EVT-20260616-000001",
  "updatedAt": "2026-06-16T10:00:05"
}
```

### 가동률 계산 기준

가동률은 설비가 계획된 시간 중 실제 RUNNING 상태였던 비율이다.

```
operationRate = runningTimeSec / plannedTimeSec * 100
```

### 상태값 기준

```
operationStatus
- RUNNING
- IDLE
- STOPPED
- ERROR
- MAINTENANCE

healthStatus
- NORMAL
- WARNING
- FAULT
- MAINTENANCE

riskLevel
- LOW
- WARNING
- CRITICAL
```

---

# 5. Kafka Partition 전략

## 5.1. 기본 기준

현재 AIMS 프로젝트에서는 기본 Message Key를 아래로 설정합니다.

```
message key = equipmentCode
```

이유는 같은 설비의 이벤트 순서를 보장해야 하기 때문입니다.

```
EQ_PRESS_01 RUNNING
↓
EQ_PRESS_01 WARNING
↓
EQ_PRESS_01 FAULT
↓
EQ_PRESS_01 RECOVERY
```

같은 `equipmentCode`를 Key로 보내면 같은 설비 이벤트는 항상 같은 Partition에 저장됩니다.

---

# 5.2. Topic별 Partition 수

```
factory.manufacturing.raw        → 4개
factory.manufacturing.analysis   → 4개
factory.manufacturing.alert      → 2개
factory.manufacturing.equipment  → 4개
```

---

# 5.3. Topic별 Key 전략

| Topic | Partition 수 | Message Key | 이유 |
| --- | --- | --- | --- |
| `factory.manufacturing.raw` | 4 | `equipmentCode` | 설비별 원천 이벤트 순서 보장 |
| `factory.manufacturing.analysis` | 4 | `equipmentCode` 기본, 불량 전이는 `carId` 가능 | 분석 결과 순서 보장 |
| `factory.manufacturing.alert` | 2 | `equipmentCode` | 같은 설비 알림 순서 보장 |
| `factory.manufacturing.equipment` | 4 | `equipmentCode` | 장비 카드 상태 갱신 순서 보장 |

---

# 5.4. Consumer Group 기준

## Manufacturing Consumer Group

```
factory.manufacturing.raw
partitions = 4
key = equipmentCode

↓
Manufacturing Consumer Group
├─ Press Consumer
├─ Body Consumer
├─ Paint Consumer
└─ Assembly Consumer
```

처리 기준은 Partition이 아니라 `processCode`입니다.

```
processCode = PRESS     → Press Consumer
processCode = BODY      → Body Consumer
processCode = PAINT     → Paint Consumer
processCode = ASSEMBLY  → Assembly Consumer
```

---

## AI Consumer Group

```
factory.manufacturing.raw
partitions = 4
key = equipmentCode
```

### Bottleneck Consumer

```
key = equipmentCode
```

설비/공정별 병목 흐름을 분석합니다.

### Defect Transfer Consumer

```
분석 기준 = carId
```

현재는 Raw Topic Key를 `equipmentCode`로 유지하되, 분석 결과 발행 시에는 `carId`를 Key로 사용할 수 있습니다.

```
factory.manufacturing.analysis
key = carId
```

---

## Equipment Consumer Group

```
factory.manufacturing.analysis
partitions = 4
key = equipmentCode

↓
Equipment Consumer Group

↓
factory.manufacturing.equipment
partitions = 4
key = equipmentCode
```

장비별 최신 상태와 화면 카드 데이터를 생성합니다.

---

## Alert Consumer Group

```
factory.manufacturing.alert
partitions = 2
key = equipmentCode

↓
Alert Consumer Group
```

알림은 데이터량이 많지 않으므로 2개 Partition이면 충분합니다.

---

# 5.5. 최종 추천 구조

```
Scheduler
↓
factory.manufacturing.raw
partitions = 4
key = equipmentCode

↓
Manufacturing Consumer Group
AI Consumer Group

↓
factory.manufacturing.analysis
partitions = 4
key = equipmentCode
※ defectTransfer 결과는 key = carId 가능

↓
Equipment Consumer Group
↓
factory.manufacturing.equipment
partitions = 4
key = equipmentCode
↓
WebSocket
↓
React Dashboard
```

알림 흐름은 별도입니다.

```
Manufacturing Consumer Group
AI Consumer Group
↓
factory.manufacturing.alert
partitions = 2
key = equipmentCode
↓
Alert Consumer Group
↓
실시간 알림 패널
```

결론적으로 현재 프로젝트에서는 아래 전략이 가장 적절합니다.

```
Partition 수
raw        4개
analysis   4개
equipment  4개
alert      2개

기본 Message Key
equipmentCode

예외
불량 전이 예측 결과는 carId 사용 가능
```

---

# 6. Consumer Group 구조

## Kafka Raw Topic

```
factory.manufacturing.raw
```

원천 제조 이벤트를 수신하는 Topic입니다.

```
프레스 이벤트
차체 이벤트
도장 이벤트
의장 이벤트
```

모두 동일한 Topic으로 수신합니다.

---

# 6.1 Manufacturing Consumer Group

제조 공정 상태 분석 전용 Consumer Group입니다.

```
factory.manufacturing.raw
↓
Manufacturing Consumer Group
├─ Press Consumer
├─ Body Consumer
├─ Paint Consumer
└─ Assembly Consumer
```

---

## Press Consumer

### 처리 대상

```
processCode = PRESS
```

### 사용 데이터

```
sensor.current
processMetrics
processData.press
equipmentStatus
```

### 처리 내용

```
프레스 설비 상태 분석

전류 RMS 분석

CNT 증가 여부 확인

Cycle Time 분석

설비 가동률 계산

설비 정지 위험 탐지
```

### 결과 발행

```
factory.manufacturing.analysis
factory.manufacturing.alert
factory.manufacturing.dashboard
```

---

## Body Consumer

### 처리 대상

```
processCode = BODY
```

### 사용 데이터

```
sensor.robotArmVibration
processData.body
equipmentStatus
```

### 처리 내용

```
로봇 상태 분석

주파수 대역 진동 분석

로봇 이상 동작 탐지

충돌 위험 탐지

차체 공정 상태 계산
```

### 결과 발행

```
factory.manufacturing.analysis
factory.manufacturing.alert
factory.manufacturing.dashboard
```

---

## Paint Consumer

### 처리 대상

```
processCode = PAINT
```

### 사용 데이터

```
sensor.thermal
processData.paint
equipmentStatus
```

### 처리 내용

```
도장 품질 분석

열화상 분석

도장 두께 분석

비전 불량 분석

품질 위험도 계산
```

### 결과 발행

```
factory.manufacturing.analysis
factory.manufacturing.alert
factory.manufacturing.dashboard
```

---

## Assembly Consumer

### 처리 대상

```
processCode = ASSEMBLY
```

### 사용 데이터

```
processData.assembly
manufacturing
equipmentStatus
```

### 처리 내용

```
조립 순서 검증

누락 부품 분석

체결 오류 분석

조립 품질 상태 계산
```

### 결과 발행

```
factory.manufacturing.analysis
factory.manufacturing.alert
factory.manufacturing.dashboard
```

---

# 6.2 AI Consumer Group

AI 모델 분석 전용 Consumer Group입니다.

```
factory.manufacturing.raw
↓
AI Consumer Group
├─ Bottleneck Consumer
└─ Defect Transfer Consumer
```

---

## Bottleneck Consumer

### 사용 데이터

```
processMetrics

cycleTimeSec
waitingTimeSec
queueLength
wipCount
equipmentIdleTimeSec
```

### 처리 내용

```
병목 탐지 모델 실행

공정 위험도 계산

영향 차량 수 계산

병목 순위 계산
```

### 결과

```
bottleneckRisk
```

---

## Defect Transfer Consumer

### 사용 데이터

```
sensor.current

sensor.vibration

sensor.robotArmVibration

sensor.thermal

processMetrics
```

### 처리 내용

```
불량 전이 예측 모델 실행

SHAP 원인 분석

예상 불량 공정 계산

예상 발생 시점 계산
```

### 결과

```
defectTransferRisk

overallRiskScore

riskLevel
```

---

# 6.3 Equipment Consumer Group

장비 표시 데이터 생성 전용 Consumer Group입니다.

```
factory.manufacturing.analysis
↓
Equipment Consumer Group
```

### 처리 내용

```
화면 카드 데이터 생성

실시간 병목 분석 생성

불량 전이 예측 생성

AI 원인 분석 생성

WebSocket Push
```

### 결과 발행

```
factory.manufacturing.equipment
```

---

# 6.4 Alert Consumer Group

실시간 알림 전용 Consumer Group입니다.

```
factory.manufacturing.alert
↓
Alert Consumer Group
```

### 처리 내용

```
경고 알림 생성

위험 알림 생성

Toast 알림 생성

WebSocket Push
```

---

# 최종 구조

```
factory.manufacturing.raw
│
├─ Manufacturing Consumer Group
│   ├─ Press Consumer
│   ├─ Body Consumer
│   ├─ Paint Consumer
│   └─ Assembly Consumer
│
├─ Equipment Consumer Group
│
└─ AI Consumer Group
│   ├─ Bottleneck Consumer
│   └─ Defect Transfer Consumer
│
├─ Alert Consumer Group
                ↓

factory.manufacturing.analysis
                ↓

Dashboard Consumer Group
                ↓

factory.manufacturing.equipment

                ↓

WebSocket
                ↓

React Dashboard

                ↓

factory.manufacturing.alert
                ↓

Alert Consumer Group
                ↓

실시간 알림 패널
```

---

# 7. AI Service 상세 흐름

```
Kafka Raw Topic 수신

↓ Feature 생성

processMetrics

sensor

processData

manufacturing

↓
병목 탐지 모델

↓
불량 전이 예측 모델

↓
위험도 계산

↓
Analysis Topic 발행

↓
main_db 저장
```

---

## 병목 탐지

입력

```
cycleTimeSec
waitingTimeSec
queueLength
wipCount
equipmentIdleTimeSec
```

출력

```
bottleneckRisk
```

---

## 불량 전이 예측

입력

```
sensor.current

sensor.vibration

sensor.robotArmVibration

sensor.thermal

processMetrics
```

출력

```
defectTransferRisk
```

---

# 8. Redis 구조

## 목적

현재 상태 캐시

---

저장 예시

```
factory:latest

factory:process:PRESS

factory:process:BODY

factory:equipment:EQ_PRESS_01

factory:alert:latest

factory:dashboard:summary
```

---

예시 데이터

```
{
  "processCode":"PRESS",
  "equipmentCode":"EQ_PRESS_01",
  "status":"WARNING",
  "riskLevel":"WARNING",
  "overallRiskScore":72.4
}
```

---

# 9. Elasticsearch 구조

## 목적

이벤트 검색

이상 이력 조회

대시보드 필터링

통계 분석

---

인덱스 예시

```
aims-manufacturing-event-2026.06.16

aims-analysis-result-2026.06.16

aims-alert-event-2026.06.16
```

---

저장 대상

```
전체 제조 이벤트

전체 분석 결과

전체 알림 이력

설비 장애 이력

공정 이상 이력

품질 이상 이력
```

---

# 10. StreamSets 역할

## Kafka → Elasticsearch

```
Kafka
 ↓
StreamSets
 ↓
Elasticsearch
```

---

## Kafka → main_db

```
Kafka
 ↓
StreamSets
 ↓
MySQL
```

---

## 데이터 검증

```
필수 필드 존재 여부

eventTime 포맷 검증

JSON 구조 검증

Null 검증
```

---

# 최종 데이터 흐름

```
[1] 원본 데이터셋

Ford
Bosch
Robot Arm Vibration
Machine Vision
소성가공

↓ ETL

[2] 통합 제조 이벤트 JSON 생성

↓ 저장

[3] sample_db.manufacturing_event_json

↓ 조회

[4] Scheduler

↓ 발행

[5] Kafka Producer

Topic
 └─ factory.manufacturing.raw

↓ 수신

[6] Consumer Group

Manufacturing Service
Alert Service
AI Service
Redis Consumer
Elasticsearch Consumer

↓ 분석

병목 탐지
불량 전이 예측
설비 이상 탐지
품질 이상 탐지

↓ 발행

factory.manufacturing.analysis

factory.manufacturing.alert

factory.manufacturing.equipment

↓ 저장

[7] Redis
현재 상태 캐시

[8] Elasticsearch
이벤트 검색

[9] main_db
분석 결과 저장

↓ Push

[10] WebSocket

↓ 표시

React Dashboard
```

정리하면 Sample DB는 시연용 원천 이벤트 저장소, Scheduler는 이벤트 재생기, Kafka는 이벤트 전달 허브, Consumer Group은 공정별 분석 서비스, Redis는 실시간 상태 캐시, Elasticsearch는 검색 및 이력 저장소, main_db는 최종 분석 결과 저장소 역할을 수행합니다.

# Elastic Search  사용

ES는 Elasticsearch/OpenSearch를 말하며, “실시간 처리”보다는 이벤트 이력 검색/조회용으로 사용합니다.

현재 구조에서는 이렇게 보면 됩니다.

```
Kafka = 실시간 이벤트 전달
Redis = 최신 상태 캐시
main_db = 분석 결과 정합성 저장
ES = 많은 이벤트 로그를 빠르게 검색/필터링
```

예를 들면 ES는 이런 화면에서 사용합니다.

```
1. 이벤트 이력 조회
- 오늘 발생한 전체 제조 이벤트
- 최근 1시간 프레스 이상 이벤트
- 특정 차량 CAR-000001의 전체 공정 이벤트

2. 검색/필터
- processCode = PRESS
- equipmentCode = PRESS_01
- riskLevel = CRITICAL
- eventTime = 10:00 ~ 11:00

3. 대시보드 로그 패널
- 실시간 이벤트 로그 목록
- 알림 발생 이력
- 설비별 이상 이벤트 리스트

4. 통계/집계
- 공정별 이상 건수
- 시간대별 알림 발생 수
- 설비별 CRITICAL 이벤트 Top 10
- 도장 공정 불량 이벤트 추이
```

즉, Kafka로 들어온 이벤트를 Consumer나 StreamSets가 ES에도 저장해두면, 나중에 대시보드에서 “검색 조건으로 빠르게 조회”할 수 있습니다.

흐름은 이렇게 됩니다.

```
Scheduler
↓
Kafka Producer
↓
factory.manufacturing.raw
↓
Consumer 또는 StreamSets
↓
Elasticsearch 저장
↓
대시보드 이력 조회 API에서 검색
```

예시로 사용자가 대시보드에서 “프레스 1호의 최근 이상 이벤트”를 누르면:

```
React
↓
Backend API
↓
Elasticsearch 조회
↓
equipmentCode = PRESS_01
status = ABNORMAL
eventTime 최근순
↓
결과 반환
```

반대로 “현재 프레스 상태”만 보여줄 때는 ES가 아니라 Redis를 쓰는 게 맞습니다.

```
현재 상태 = Redis
과거 이력 검색 = ES
정확한 분석 결과 저장 = main_db
```

따라서 ES는 필수 실시간 처리 엔진이 아니라, 관제 시스템에서 로그 검색, 이벤트 이력 조회, 조건 필터링, 통계 집계를 빠르게 하기 위한 저장소입니다.
