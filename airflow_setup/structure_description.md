# 주식 데이터 수집 시스템 — 구조 완전 이해 가이드

> 이 문서는 **Docker + Apache Airflow + PostgreSQL + Streamlit** 기반의 자동화 주식 데이터 수집 시스템을 처음 접하는 분이 전체 구조를 이해하고 직접 운영할 수 있도록 작성됐습니다.

---

## 목차

1. [시스템이 하는 일](#1-시스템이-하는-일)
2. [기술 스택 한눈에 보기](#2-기술-스택-한눈에-보기)
3. [전체 아키텍처 구조도](#3-전체-아키텍처-구조도)
4. [디렉토리 구조 해설](#4-디렉토리-구조-해설)
5. [Docker란 무엇인가 — 컨테이너 개념 이해](#5-docker란-무엇인가--컨테이너-개념-이해)
6. [Docker Compose — 4개 컨테이너가 협력하는 방법](#6-docker-compose--4개-컨테이너가-협력하는-방법)
7. [Apache Airflow 핵심 개념](#7-apache-airflow-핵심-개념)
8. [DAG별 상세 구조](#8-dag별-상세-구조)
9. [데이터 흐름 파이프라인](#9-데이터-흐름-파이프라인)
10. [데이터베이스 스키마](#10-데이터베이스-스키마)
11. [Streamlit 대시보드](#11-streamlit-대시보드)
12. [처음 시작하기 — 단계별 설치 가이드](#12-처음-시작하기--단계별-설치-가이드)
13. [일상 운영 방법](#13-일상-운영-방법)
14. [문제 해결 가이드](#14-문제-해결-가이드)

---

## 1. 시스템이 하는 일

이 시스템은 **18개 종목의 주식 데이터를 매일 자동으로 수집**하고 **웹 대시보드로 시각화**합니다.

```
매일 자동 수집 → PostgreSQL 저장 → Streamlit 웹에서 인터랙티브 차트로 확인
```

### 수집 대상

| 분류 | 종목 | 수집 내용 |
|------|------|----------|
| 미국 주식 (11개) | AVGO, BE, VRT, SMR, OKLO, GEV, MRVL, COHR, LITE, VST, ETN | 가격 + 지표 + 펀더멘털 + 뉴스 |
| 한국 주식 (5개) | 267260.KS, 034020.KS, 028260.KS, 267270.KS, 010120.KS | 가격 + 지표 + 뉴스 |
| ADR (2개) | SBGSY (Schneider), HTHIY (Hitachi) | 가격 + 지표 + 뉴스 |
| 비상장 (2개) | TerraPower, X-Energy | 뉴스만 |

### 수집 스케줄

```
UTC 기준 (KST = UTC + 9)

  06:00 UTC (15:00 KST) ─── news_collection DAG
      └─ yfinance 뉴스 + Google News RSS 수집

  23:00 UTC (08:00 KST 다음날) ─── stock_price_collection DAG
      └─ OHLCV 가격 + 기술적 지표 14개 + 펀더멘털 수집
```

---

## 2. 기술 스택 한눈에 보기

```
┌─────────────────────────────────────────────────────────────────┐
│                         사용 기술                                 │
├──────────────────┬──────────────────────────────────────────────┤
│ 컨테이너화        │ Docker Desktop + Docker Compose               │
│ 워크플로우 엔진   │ Apache Airflow 2.8.0 (Python 3.11)            │
│ 데이터베이스      │ PostgreSQL 15                                  │
│ 데이터 수집       │ yfinance (주가), feedparser (뉴스 RSS)         │
│ 기술적 지표 계산  │ ta 라이브러리 (TA-Lib Python 버전)             │
│ 웹 대시보드       │ Streamlit 1.32 + Plotly 5.19                  │
│ DB ORM           │ SQLAlchemy 1.4.52                              │
└──────────────────┴──────────────────────────────────────────────┘
```

---

## 3. 전체 아키텍처 구조도

### 3-1. 거시적 구조 (사용자 관점)

```
┌─────────────────────────────────────────────────────────────────────┐
│                        사용자 컴퓨터 (호스트)                          │
│                                                                      │
│   브라우저                                                            │
│   ┌──────────┐          ┌──────────────────────────────────────┐    │
│   │localhost │          │         Docker 가상 네트워크           │    │
│   │  :8080   │◄────────►│  ┌──────────────┐                   │    │
│   │          │          │  │   Airflow    │                   │    │
│   │  Airflow │          │  │  Webserver   │                   │    │
│   │  Web UI  │          │  └──────┬───────┘                   │    │
│   └──────────┘          │         │ 스케줄 조회/실행              │    │
│                         │  ┌──────▼───────┐                   │    │
│   ┌──────────┐          │  │   Airflow    │                   │    │
│   │localhost │◄─────────►  │  Scheduler   │                   │    │
│   │  :8501   │          │  └──────┬───────┘                   │    │
│   │          │          │         │ Task 실행                   │    │
│   │Streamlit │          │         ▼                            │    │
│   │대시보드   │          │  ┌─────────────┐   ┌─────────────┐  │    │
│   └──────────┘          │  │  yfinance   │   │  Google     │  │    │
│                         │  │  API 호출   │   │  News RSS   │  │    │
│   ┌──────────┐          │  └──────┬──────┘   └──────┬──────┘  │    │
│   │localhost │◄─────────►         │                 │         │    │
│   │  :5432   │          │         ▼                 ▼         │    │
│   │          │          │  ┌────────────────────────────────┐  │    │
│   │PostgreSQL│          │  │         PostgreSQL 15           │  │    │
│   │  직접접속 │          │  │  ┌──────────┐  ┌────────────┐  │  │    │
│   └──────────┘          │  │  │  airflow │  │ stock_data │  │  │    │
│                         │  │  │  (메타DB) │  │  (실제데이터)│  │  │    │
│                         │  │  └──────────┘  └────────────┘  │  │    │
│                         │  └────────────────────────────────┘  │    │
│                         └──────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
```

### 3-2. 컨테이너 의존 관계

```
docker-compose up 실행 시 시작 순서:

  [1] postgres ──────────────────────────────────────────────────┐
       │ healthcheck 통과 후                                       │
       ▼                                                          │
  [2] airflow-init (DB 마이그레이션 + admin 계정 생성, 1회 실행)    │
                                                                  │
  [3] airflow-webserver ◄────────────── postgres 건강 확인 후 시작 │
  [3] airflow-scheduler ◄────────────── postgres 건강 확인 후 시작 │
  [3] streamlit         ◄────────────── postgres 건강 확인 후 시작 ┘
```

---

## 4. 디렉토리 구조 해설

```
airflow_setup/
│
├── docker-compose.yml       ← [핵심] 4개 서비스 정의. 이 파일 하나로 전체 실행
├── Dockerfile               ← Airflow 커스텀 이미지 빌드 설정
├── .env                     ← 비밀번호, API 키 등 환경변수 (git에 올리면 안됨!)
├── .env.example             ← .env 작성 템플릿
├── requirements.txt         ← Airflow 컨테이너에 추가 설치할 Python 패키지
│
├── dags/                    ← Airflow가 자동으로 스캔하는 DAG 폴더
│   ├── common.py            ← 공통 설정 (심볼 목록, DB 연결, default_args)
│   ├── stock_price_dag.py   ← 주가 수집 DAG (매일 23:00 UTC)
│   └── news_dag.py          ← 뉴스 수집 DAG (매일 06:00 UTC)
│
├── streamlit/               ← 웹 대시보드 서비스
│   ├── Dockerfile           ← Streamlit 전용 이미지 설정
│   ├── requirements.txt     ← Streamlit 패키지 목록
│   └── app.py               ← 대시보드 메인 코드
│
├── scripts/
│   └── init_data_db.sql     ← PostgreSQL 최초 시작 시 자동 실행되는 DB 초기화 SQL
│
├── logs/                    ← Airflow 태스크 실행 로그 (자동 생성)
└── plugins/                 ← Airflow 커스텀 플러그인 (현재 미사용)
```

> **중요한 파일 관계**: `dags/` 폴더는 호스트 컴퓨터와 Airflow 컨테이너가 **같은 폴더를 공유(볼륨 마운트)**합니다. 즉, 파이참에서 DAG 파일을 수정하면 컨테이너 재시작 없이 **즉시 반영**됩니다.

---

## 5. Docker란 무엇인가 — 컨테이너 개념 이해

### "컨테이너"를 물류 컨테이너로 비유하면

```
물류 컨테이너:
  배 위에 컨테이너 여러 개를 독립적으로 적재
  각 컨테이너 안에 다른 화물이 들어있지만
  서로 영향을 주지 않음

Docker 컨테이너:
  컴퓨터 위에 컨테이너 여러 개를 독립적으로 실행
  각 컨테이너 안에 다른 소프트웨어가 들어있지만
  서로 영향을 주지 않음
```

### 이미지(Image) vs 컨테이너(Container)

```
이미지 (Image) = 설계도 / 클래스
  → "Apache Airflow 2.8.0이 설치된 환경" 이라는 설계도
  → docker build 명령으로 Dockerfile을 읽어서 생성

컨테이너 (Container) = 실체 / 인스턴스
  → 이미지를 실제로 실행한 것
  → docker run 또는 docker compose up 으로 생성
  → 같은 이미지로 여러 컨테이너를 만들 수 있음

Dockerfile ──build──► Image ──run──► Container (실행 중인 프로세스)
```

### 왜 Docker를 쓰는가?

```
Docker 없이 Airflow를 설치하려면:
  1. Python 버전 맞추기
  2. 의존성 패키지 충돌 해결
  3. PostgreSQL 직접 설치 및 설정
  4. 환경변수 수동 설정
  5. "내 컴퓨터에서는 됐는데..." 문제

Docker 사용 시:
  → docker compose up 한 줄로 모든 것 해결
  → 어떤 컴퓨터에서도 동일하게 동작
  → 삭제도 docker compose down 한 줄
```

---

## 6. Docker Compose — 4개 컨테이너가 협력하는 방법

### docker-compose.yml 구조 이해

```yaml
# docker-compose.yml 단순화된 개념도

x-airflow-common: &airflow-common  ← YAML 앵커: 공통 설정 재사용
  build: .                          ← Dockerfile로 이미지 빌드
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    DATA_DB_CONN: postgresql://...  ← 수집 데이터 DB 연결 문자열
  volumes:
    - ./dags:/opt/airflow/dags      ← 호스트 폴더 ↔ 컨테이너 폴더 연결

services:
  postgres:      ← DB 서버
  airflow-init:  ← 초기화 (1회만 실행)
  airflow-webserver: ← 웹 UI
  airflow-scheduler: ← 스케줄러
  streamlit:     ← 대시보드
```

### 서비스별 역할

```
┌─────────────────────────────────────────────────────────────────────┐
│ 서비스명              역할                              포트          │
├─────────────────────────────────────────────────────────────────────┤
│ postgres          데이터 저장소                          5432         │
│                   ├─ airflow DB: Airflow 내부 메타데이터             │
│                   └─ stock_data DB: 실제 수집 데이터                │
├─────────────────────────────────────────────────────────────────────┤
│ airflow-init      DB 테이블 생성 + admin 계정 만들기     없음         │
│                   (docker compose up 시 매번 실행되지만              │
│                    users create 명령은 이미 있으면 건너뜀)           │
├─────────────────────────────────────────────────────────────────────┤
│ airflow-webserver DAG 목록/실행 현황을 보는 웹 UI        8080         │
│                   DAG 수동 트리거, 로그 조회, 재실행 등               │
├─────────────────────────────────────────────────────────────────────┤
│ airflow-scheduler DAG 파일을 주기적으로 스캔하여         없음         │
│                   스케줄 시간이 되면 태스크를 실행시킴               │
├─────────────────────────────────────────────────────────────────────┤
│ streamlit         주가 대시보드 웹앱                      8501         │
│                   stock_data DB에서 직접 읽어 차트 렌더링            │
└─────────────────────────────────────────────────────────────────────┘
```

### 볼륨(Volume) — 데이터를 영구 보존하는 방법

```
컨테이너는 종료하면 내부 데이터가 사라집니다.
데이터를 유지하려면 두 가지 방법:

1. 바인드 마운트 (Bind Mount): 호스트 폴더를 컨테이너에 연결
   호스트: ./dags/          ←──연결──►  컨테이너: /opt/airflow/dags/
   호스트: ./logs/          ←──연결──►  컨테이너: /opt/airflow/logs/
   → DAG 파일 수정이 즉시 컨테이너에 반영됨

2. 네임드 볼륨 (Named Volume): Docker가 관리하는 저장공간
   postgres_data ←── PostgreSQL의 실제 데이터 파일 저장
   → docker compose down -v 하기 전까지 영구 보존
```

### 컨테이너 간 통신

```
같은 docker-compose.yml 안의 서비스들은
서비스명(이름)으로 서로를 찾을 수 있습니다.

예시:
  Airflow → PostgreSQL 접속 시:
  postgresql://airflow:password@postgres/airflow
                              ↑
                       서비스명 "postgres" = 컨테이너 IP 자동 해석
```

---

## 7. Apache Airflow 핵심 개념

### Airflow란?

```
Airflow = "언제, 어떤 순서로, 어떤 작업을 실행할지" 를 관리하는 도구

일반 cron과 비교:
  cron: 시간에 맞춰 스크립트 실행 (단순)
  Airflow: 복잡한 의존 관계, 재시도, 모니터링, 로그 관리 (고급)
```

### 핵심 용어 설명

```
┌─────────────────────────────────────────────────────────────────────┐
│ 용어               설명                                              │
├─────────────────────────────────────────────────────────────────────┤
│ DAG                Directed Acyclic Graph                           │
│ (디에이지)          = 작업(Task) 들의 실행 순서를 정의한 워크플로우    │
│                    = 하나의 .py 파일이 하나의 DAG                    │
│                                                                     │
│ Task               DAG 안의 개별 작업 단위                           │
│                    (예: "AVGO 주가 수집", "RSI 계산")               │
│                                                                     │
│ TaskGroup          연관된 Task들의 묶음 (UI에서 폴더처럼 표시)        │
│                                                                     │
│ Operator           Task의 종류                                       │
│                    PythonOperator = Python 함수를 실행하는 Task       │
│                    ExternalTaskSensor = 다른 DAG 완료를 기다리는 Task │
│                                                                     │
│ Schedule           DAG 실행 주기 (cron 표현식)                       │
│                    "0 23 * * *" = 매일 23:00 UTC                   │
│                                                                     │
│ DAG Run            DAG가 실제로 한 번 실행된 인스턴스               │
│                                                                     │
│ Executor           Task를 어떻게 실행할지 결정하는 엔진               │
│                    LocalExecutor = 같은 컴퓨터에서 병렬 실행         │
└─────────────────────────────────────────────────────────────────────┘
```

### Scheduler가 DAG를 실행하는 흐름

```
[Scheduler 동작 원리]

  1. /opt/airflow/dags/ 폴더를 주기적으로 스캔
       └─ common.py, stock_price_dag.py, news_dag.py 발견

  2. 각 .py 파일을 파싱하여 DAG 객체 추출
       └─ schedule_interval, start_date 등을 읽음

  3. 스케줄 시간이 되면 DAG Run 생성

  4. Task를 실행 큐에 추가

  5. LocalExecutor가 Task를 subprocess로 실행

  6. Task 성공/실패를 PostgreSQL airflow DB에 기록

  7. 다음 Task (의존 관계상 이 Task가 완료되어야 실행 가능한 것) 실행
```

### DAG 파일의 기본 구조 이해

```python
# DAG 파일 구조 (간략화)

from airflow import DAG
from airflow.operators.python import PythonOperator

# 1. 실제 작업을 수행하는 함수 정의
def my_task_function():
    # 여기에 실제 비즈니스 로직 작성
    fetch_data_from_api()
    save_to_database()

# 2. DAG 정의 (with 블록 안에서 Task 정의)
with DAG(
    dag_id="my_dag",           # DAG의 고유 이름
    schedule_interval="0 23 * * *",  # cron 형식 스케줄
    start_date=datetime(2024, 1, 1), # 이 날짜 이후부터 스케줄 적용
    catchup=False,             # 과거 누락분 실행 안함
    max_active_runs=1,         # 동시 실행 최대 1개 (중복 실행 방지)
) as dag:

    # 3. Task 인스턴스 생성
    task_a = PythonOperator(
        task_id="fetch_data",
        python_callable=my_task_function,
    )

    task_b = PythonOperator(
        task_id="process_data",
        python_callable=another_function,
    )

    # 4. 실행 순서 정의 (>> 연산자)
    task_a >> task_b  # task_a 완료 후 task_b 실행
```

### cron 표현식 읽는 법

```
"0 23 * * *"
 │  │  │ │ └─ 요일 (0-7, 0과 7은 일요일)
 │  │  │ └─── 월 (1-12)
 │  │  └───── 일 (1-31)
 │  └──────── 시 (0-23)
 └─────────── 분 (0-59)

이 시스템에서 사용하는 스케줄:
  "0 23 * * *"   = 매일 23:00 UTC = 매일 08:00 KST (다음날)
  "0 6 * * *"    = 매일 06:00 UTC = 매일 15:00 KST
```

---

## 8. DAG별 상세 구조

### 8-1. stock_price_collection DAG (23:00 UTC)

```
전체 흐름:

validate_db
    │ (DB 테이블 존재 확인)
    ▼
┌─────────────────────────────────────────────┐
│           fetch_prices_group                 │
│  (18개 종목 동시에 yfinance에서 OHLCV 수집)   │
│                                             │
│  fetch_AVGO ─┐                             │
│  fetch_BE   ─┤                             │
│  fetch_VRT  ─┤                             │
│  ...        ─┤ (18개 Task 병렬 실행)        │
│  fetch_HTHIY─┘                             │
└────────────────────┬────────────────────────┘
                     │ (18개 모두 완료 후)
                     ▼
┌─────────────────────────────────────────────┐
│        calculate_indicators_group            │
│  (18개 종목 각각에 대해 기술적 지표 14개 계산) │
│                                             │
│  indicators_AVGO ─┐                        │
│  indicators_BE   ─┤                        │
│  ...             ─┤ DB에서 최근 250행 읽어   │
│  indicators_HTHIY─┘ pandas로 지표 계산      │
└────────────────────┬────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────┐
│        fetch_fundamentals_group              │
│  (18개 종목 PER, PBR, ROE 등 펀더멘털 수집)  │
│                                             │
│  fundamentals_AVGO ─┐                      │
│  fundamentals_BE   ─┤                      │
│  ...               ─┘                      │
└────────────────────┬────────────────────────┘
                     │
                     ▼
                dag_complete
```

**계산되는 기술적 지표 14개:**

```
이동평균선:   SMA(20), SMA(50), SMA(200)
볼린저밴드:   BB_Upper, BB_Middle, BB_Lower
모멘텀:       RSI(14)
추세:         MACD, MACD_Signal, MACD_Histogram, CCI(20)
변동성:       ATR(14)
거래량:       OBV, MFI(14)
```

### 8-2. news_collection DAG (06:00 UTC)

```
전체 흐름:

┌─────────────────────────────┐   ┌─────────────────────────────┐
│     yfinance_news_group      │   │      google_news_group       │
│  (18개 상장 종목의 뉴스 수집) │   │  (20개 쿼리 Google RSS 수집) │
│                             │   │                             │
│  yf_news_AVGO ─┐           │   │  gnews_AVGO ─┐             │
│  yf_news_BE   ─┤           │   │  gnews_BE   ─┤             │
│  ...          ─┘           │   │  ...        ─┤ 비상장사 포함 │
│                             │   │  TerraPower ─┤             │
│  yfinance ticker.news 사용  │   │  X_Energy   ─┘             │
└──────────────┬──────────────┘   └──────────────┬──────────────┘
               │                                  │
               └──────────────┬───────────────────┘
                              │ (두 그룹 모두 완료 후)
                              ▼
                         news_complete
```

**중복 방지 메커니즘:**
```sql
-- (symbol, url) 조합이 이미 있으면 삽입 무시
INSERT INTO stock_news (symbol, url, ...)
ON CONFLICT (symbol, url) DO NOTHING
```

---

## 9. 데이터 흐름 파이프라인

```
외부 데이터 소스              Airflow 처리               저장소
──────────────                ──────────                ──────

yfinance API                                            PostgreSQL
┌──────────┐  60일치 OHLCV   ┌─────────────┐          stock_data DB
│ Yahoo    │ ──────────────► │ fetch_and_  │ ─upsert► ┌──────────────┐
│ Finance  │                 │ upsert_     │          │ stock_prices │
│          │ ticker.news     │ prices()    │          │              │
│          │ ──────────────► ├─────────────┤          │ (OHLCV +     │
│          │                 │ fetch_      │          │  지표 14개)   │
│          │ ticker.info     │ yfinance_   │          └──────────────┘
│          │ ──────────────► │ news()      │
└──────────┘                 ├─────────────┤          ┌──────────────┐
                             │ fetch_and_  │ ─upsert► │    stock_    │
                             │ store_      │          │ fundamentals │
Google News RSS              │ fundamentals│          └──────────────┘
┌──────────┐                 ├─────────────┤
│ Google   │  RSS 피드        │ calculate_  │          ┌──────────────┐
│ News     │ ──────────────► │ and_store_  │ ─update► │ stock_prices │
│          │                 │ indicators()│          │ (지표 컬럼)   │
└──────────┘                 ├─────────────┤          └──────────────┘
                             │ fetch_      │
                             │ google_     │          ┌──────────────┐
                             │ news()      │ ─insert► │  stock_news  │
                             └─────────────┘          └──────────────┘
                                   │
                                   ▼
                             Streamlit 앱이
                             stock_data DB를
                             직접 쿼리하여
                             Plotly 차트 렌더링
```

### 멱등성(Idempotency) 보장

```
"같은 DAG를 여러 번 실행해도 데이터가 중복되지 않는다"

방법: PostgreSQL의 ON CONFLICT 절 사용

INSERT INTO stock_prices (symbol, trade_date, close, ...)
ON CONFLICT (symbol, trade_date)
DO UPDATE SET close = EXCLUDED.close, ...
             ↑
      이미 있으면 덮어쓰기 (최신 데이터 유지)
```

---

## 10. 데이터베이스 스키마

### PostgreSQL 안에 두 개의 데이터베이스

```
PostgreSQL 서버
├── airflow          ← Airflow 내부 메타데이터 (DAG 실행 기록, 태스크 상태 등)
│                       Airflow가 자동으로 관리, 직접 건드리지 않음
└── stock_data       ← 실제 수집한 데이터 (우리가 직접 다루는 DB)
    ├── stock_prices
    ├── stock_fundamentals
    └── stock_news
```

### stock_data 테이블 ERD

```
stock_prices
┌───────────────────────────────────────────────┐
│ id (PK)           SERIAL                      │
│ symbol            VARCHAR(20)  NOT NULL        │
│ trade_date        DATE         NOT NULL        │
│ open              NUMERIC(12,4)                │
│ high              NUMERIC(12,4)                │
│ low               NUMERIC(12,4)                │
│ close             NUMERIC(12,4)                │
│ volume            BIGINT                       │
│ sma_20 ~ sma_200  NUMERIC                     │ ← 지표
│ bb_upper ~ bb_lower NUMERIC                   │ ← 지표
│ rsi_14            NUMERIC(8,4)                │ ← 지표
│ macd ~ macd_hist  NUMERIC                     │ ← 지표
│ cci_20, atr_14    NUMERIC                     │ ← 지표
│ obv               BIGINT                      │ ← 지표
│ mfi_14            NUMERIC(8,4)                │ ← 지표
│ created_at        TIMESTAMP   DEFAULT NOW()   │
│ updated_at        TIMESTAMP   DEFAULT NOW()   │
│ UNIQUE (symbol, trade_date)                   │ ← 중복 방지
└───────────────────────────────────────────────┘

stock_fundamentals
┌───────────────────────────────────────────────┐
│ id (PK)           SERIAL                      │
│ symbol            VARCHAR(20)  NOT NULL        │
│ fetch_date        DATE         NOT NULL        │
│ market_cap        BIGINT                       │ ← 시가총액
│ pe_ratio          NUMERIC(12,4)                │ ← PER
│ pb_ratio          NUMERIC(12,4)                │ ← PBR
│ roe               NUMERIC(10,4)                │ ← ROE
│ eps               NUMERIC(12,4)                │ ← EPS
│ dividend_yield    NUMERIC(10,6)                │ ← 배당수익률
│ sector            VARCHAR(100)                 │
│ industry          VARCHAR(200)                 │
│ UNIQUE (symbol, fetch_date)                   │
└───────────────────────────────────────────────┘

stock_news
┌───────────────────────────────────────────────┐
│ id (PK)           SERIAL                      │
│ symbol            VARCHAR(20)  NOT NULL        │
│ title             TEXT         NOT NULL        │
│ url               TEXT         NOT NULL        │
│ source            VARCHAR(200)                 │
│ published         TIMESTAMP                    │
│ summary           TEXT                         │
│ UNIQUE (symbol, url)                          │ ← 중복 방지
└───────────────────────────────────────────────┘
```

---

## 11. Streamlit 대시보드

### 아키텍처

```
브라우저 (localhost:8501)
        │
        ▼
┌──────────────────────────────────────────┐
│         Streamlit 컨테이너 (Python)        │
│                                          │
│  ┌─────────────────────────────────────┐ │
│  │           app.py                    │ │
│  │                                     │ │
│  │  @st.cache_resource                 │ │
│  │  get_engine()                       │ │
│  │    → SQLAlchemy 엔진 (재사용)         │ │
│  │                                     │ │
│  │  @st.cache_data(ttl=300)            │ │
│  │  load_prices(symbol, days)          │ │
│  │    → 5분 캐시 (DB 과부하 방지)       │ │
│  │                                     │ │
│  │  build_chart(df, symbol)            │ │
│  │    → Plotly Figure 생성             │ │
│  │    → 4개 패널: 캔들+MA, 거래량,     │ │
│  │               RSI, MACD            │ │
│  └─────────────────────────────────────┘ │
│                │                          │
└───────────────┼──────────────────────────┘
                │ SQL 쿼리
                ▼
         PostgreSQL stock_data
```

### 화면 구성

```
┌─────────────────────────────────────────────────────────┐
│  📈 Stock Dashboard                                      │
│  ┌──────────┐  ┌────────────────────────────────────┐   │
│  │  사이드바  │  │         요약 지표 (6개)              │   │
│  │          │  │ [종가] [고가] [저가] [거래량] [RSI] [MFI]│  │
│  │ 종목 선택 │  ├────────────────────────────────────┤   │
│  │ (드롭다운)│  │                                    │   │
│  │          │  │     캔들스틱 차트 (60%)              │   │
│  │ 조회기간  │  │     + SMA 20/50/200 오버레이         │   │
│  │ (슬라이더)│  │     + 볼린저밴드                     │   │
│  │          │  │                                    │   │
│  │ 새로고침  │  │     거래량 바 차트 (15%)             │   │
│  │  버튼     │  │                                    │   │
│  │          │  │     RSI 차트 (15%) + 과매수/과매도   │   │
│  │          │  │                                    │   │
│  │          │  │     MACD 차트 (15%)                 │   │
│  └──────────┘  ├────────────────────────────────────┤   │
│                │  [펀더멘털 탭] [지표 테이블] [뉴스 탭]│   │
│                └────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

---

## 12. 처음 시작하기 — 단계별 설치 가이드

### 사전 준비물 확인

```bash
# Docker Desktop 설치 여부 확인
docker --version
# Docker Desktop이 없으면:
# brew install --cask docker   (Mac)
# 또는 https://www.docker.com/products/docker-desktop/ 에서 다운로드
```

### Step 1: 환경변수 파일 만들기

```bash
cd airflow_setup
cp .env.example .env
```

`.env` 파일을 열어 값을 채웁니다:

```bash
# .env 파일 내용

POSTGRES_USER=airflow
POSTGRES_PASSWORD=your_strong_password      # 바꾸세요

# Fernet Key 생성 방법 (터미널에서):
# python3 -c "import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
FERNET_KEY=생성된_키를_붙여넣기

WEBSERVER_SECRET_KEY=아무_랜덤_문자열_32자이상    # 바꾸세요
AIRFLOW_ADMIN_PASSWORD=admin_password_here  # 웹UI 로그인 비밀번호
AIRFLOW_UID=50000
```

### Step 2: 디렉토리 권한 설정 (Mac/Linux)

```bash
chmod -R 777 logs data
```

> **왜 필요한가?** Airflow 컨테이너는 UID 50000 사용자로 실행됩니다. 호스트의 `logs/`, `data/` 폴더에 쓰기 권한이 없으면 로그가 저장되지 않아 태스크 실행 후 로그 조회가 안 됩니다.

### Step 3: Airflow 초기화 (딱 한 번만)

```bash
docker compose up airflow-init
```

이 명령이 하는 일:
1. Docker 이미지 빌드 (최초 실행 시 5~10분 소요)
2. PostgreSQL에 Airflow 메타데이터 테이블 생성
3. `scripts/init_data_db.sql` 자동 실행 → `stock_data` DB와 테이블 생성
4. admin 계정 생성

**성공 메시지 확인:**
```
airflow-init-1  | Airflow initialized successfully
airflow-init-1 exited with code 0  ← 이게 보여야 정상
```

### Step 4: 전체 서비스 시작

```bash
docker compose up -d
# -d : 백그라운드(detached) 모드로 실행
```

### Step 5: 상태 확인

```bash
docker compose ps
# 모든 서비스가 (healthy) 상태인지 확인

# NAME                              STATUS
# airflow_setup-postgres-1          Up (healthy)
# airflow_setup-airflow-webserver-1 Up (healthy)
# airflow_setup-airflow-scheduler-1 Up (healthy)
# airflow_setup-streamlit-1         Up
```

### Step 6: 웹 UI 접속 및 DAG 활성화

1. **http://localhost:8080** 접속
2. ID: `admin` / PW: `.env`의 `AIRFLOW_ADMIN_PASSWORD`
3. DAG 목록에서 `stock_price_collection`, `news_collection` 좌측 **토글을 ON**으로

```
DAG 목록 화면:
  ○ stock_price_collection  ← 클릭해서 파란 토글로 변경
  ○ news_collection         ← 클릭해서 파란 토글로 변경
```

### Step 7: 데이터 수집 테스트 (수동 트리거)

스케줄 시간을 기다리지 않고 바로 실행하고 싶을 때:

1. `stock_price_collection` DAG 클릭
2. 우측 상단 ▶ (Trigger DAG) 버튼 클릭
3. Tasks 탭에서 각 태스크가 초록색(success)으로 바뀌는지 확인

### Step 8: 대시보드 확인

```
http://localhost:8501 접속

데이터 수집 전: "데이터 없음" 메시지
데이터 수집 후: 종목 선택 → 인터랙티브 차트 확인
```

---

## 13. 일상 운영 방법

### 로그 확인

```bash
# 실시간 로그 스트리밍
docker compose logs -f airflow-scheduler

# 특정 서비스 마지막 100줄
docker compose logs --tail=100 airflow-webserver

# Airflow Web UI에서도 확인 가능:
# DAG → 특정 Run → 특정 Task → Logs 탭
```

### DB 직접 조회

```bash
# PostgreSQL 접속
docker exec -it airflow_setup-postgres-1 psql -U airflow -d stock_data

# 수집된 데이터 확인
SELECT symbol, trade_date, close, rsi_14, macd
FROM stock_prices
ORDER BY trade_date DESC
LIMIT 10;

# 특정 종목 확인
SELECT * FROM stock_prices
WHERE symbol = 'AVGO'
ORDER BY trade_date DESC
LIMIT 5;

# 뉴스 확인
SELECT symbol, title, published
FROM stock_news
ORDER BY published DESC
LIMIT 10;
```

### 서비스 관리 명령

```bash
# 중지 (데이터 보존)
docker compose down

# 재시작
docker compose up -d

# 특정 서비스만 재시작
docker compose restart airflow-scheduler

# 이미지 재빌드 (코드 변경 후)
docker compose up -d --build

# 완전 초기화 (데이터 삭제 포함, 주의!)
docker compose down -v
```

### DAG 파일 수정 후 반영

```
DAG 파일은 볼륨 마운트로 연결되어 있어 자동 반영됩니다.

파이참에서 stock_price_dag.py 수정
    → 저장
    → Airflow Scheduler가 자동으로 재파싱 (약 30초 이내)
    → Web UI에서 새로고침 후 변경 확인
```

---

## 14. 문제 해결 가이드

### 증상 1: `docker compose up airflow-init` 이 실패

```
원인 확인:
  docker compose logs airflow-init

주요 원인:
  1. .env 파일이 없거나 값이 비어있음
     → cp .env.example .env 후 값 채우기

  2. PostgreSQL 포트(5432) 충돌
     → 이미 로컬에 PostgreSQL이 실행 중
     → docker-compose.yml에서 포트 변경: "5433:5432"

  3. 이미지 빌드 실패 (네트워크 문제)
     → docker compose build --no-cache 후 재시도
```

### 증상 2: DAG가 Web UI에 보이지 않음

```
원인 확인:
  docker compose logs airflow-scheduler | grep ERROR

주요 원인:
  1. DAG 파일에 Python 문법 오류
     → Scheduler 로그에서 import 에러 메시지 확인

  2. common.py import 실패
     → dags/ 폴더에 common.py 파일 있는지 확인

  3. 스케줄러가 파일 스캔하기 전 (최대 30초 대기)
```

### 증상 3: 태스크가 실패(빨간색)

```
로그 확인 방법:
  Web UI → DAG → 특정 Run → 실패한 Task 클릭 → Logs 탭

주요 원인:
  1. yfinance 429 에러 (요청 너무 많음)
     → 자동 재시도 2회 (5분 간격) 설정되어 있음
     → 시간이 지나면 해결됨

  2. DB 연결 실패
     → PostgreSQL 컨테이너 상태 확인: docker compose ps

  3. DATA_DB_CONN 환경변수 없음
     → docker-compose.yml에서 DATA_DB_CONN 설정 확인
```

### 증상 4: Streamlit이 "데이터 없음"을 표시

```
확인 순서:
  1. stock_price_collection DAG를 수동 트리거했는가?
  2. 모든 Task가 성공(초록)인가?
  3. DB에 실제 데이터가 있는가?

     docker exec -it airflow_setup-postgres-1 \
       psql -U airflow -d stock_data \
       -c "SELECT COUNT(*) FROM stock_prices;"

  4. Streamlit 새로고침 버튼 클릭 (5분 캐시 무효화)
```

### 증상 5: SMA-200이 계산되지 않음

```
원인: 데이터가 200일치 미만
해결: 2년치 데이터 Bootstrap 실행

stock_price_dag.py의 fetch_and_upsert_prices 호출부에서:
  op_kwargs={"symbol": sym, "bootstrap": True}  ← 추가

DAG 수동 트리거 → 완료 후 bootstrap 옵션 제거
```

---

## 부록: 전체 시스템 재구성 치트시트

```bash
# 처음 시작 (이미 설치된 상태)
cd airflow_setup
cp .env.example .env          # 1. 환경변수 설정
# .env 파일 편집
chmod -R 777 logs data         # 2. 권한 설정
docker compose up airflow-init # 3. 초기화 (1회)
docker compose up -d           # 4. 서비스 시작

# 접속 URL
# Airflow UI : http://localhost:8080  (admin / .env의 비밀번호)
# 대시보드   : http://localhost:8501
# PostgreSQL : localhost:5432 (DBeaver 등 DB 툴로 접속 가능)

# 일상 명령
docker compose ps              # 상태 확인
docker compose logs -f         # 전체 로그
docker compose down            # 중지
docker compose up -d           # 재시작
docker compose up -d --build   # 코드 변경 후 재빌드
```
