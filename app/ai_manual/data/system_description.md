# AIMS Smart Factory

## 프로젝트 소개

AIMS Smart Factory는 자동차 제조 공정을 실시간으로 모니터링하는 AI 기반 스마트팩토리 시스템이다.

Watchy AI는 공정 이상이 발생했을 때
현장 담당자에게 조치 매뉴얼을 제공하는 역할을 수행한다.

---

## 제조 공정

PRESS

차체 패널을 프레스 설비로 성형한다.

BODY

산업용 Robot이 차체를 용접한다.

PAINT

차체를 도장한다.

ASSEMBLY

부품을 조립한다.

---

## 설비

Robot

Conveyor

Vision Camera

PLC

Sensor

AGV

---

## 시스템 구조

MES

↓

Kafka

↓

Main Backend Service

↓

Priority Score 계산

↓

MainDB(alert_event)

↓

AI Service

↓

Watchy

↓

React

---

## AI 역할

Watchy는

- 현재 발생한 가장 높은 Priority Event만 분석한다.

- Priority가 해결되면 다음 Priority Event를 분석한다.

- RAG 문서를 기반으로만 답변한다.

- 추측하지 않는다.

---

## 담당자

Junior

현장 초급 담당자

쉬운 설명과 조치를 위한 상세하고 순차적인 구체적 설명 필요

Senior

현장 숙련 담당자

원인 분석 포함한 간단 요약 설명 필요

---

## 이벤트

PROCESS

공정 이상

EQUIPMENT

설비 이상

---

## Severity

DANGER

즉시 조치 필요

WARNING

빠른 확인 필요

CAUTION

모니터링 필요