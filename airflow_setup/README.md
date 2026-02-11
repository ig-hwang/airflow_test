# 📈 주식 데이터 자동 수집 & 분석 시스템

> **Docker + Apache Airflow + PostgreSQL + Streamlit** 기반의 주식 데이터 자동 수집·저장·분석 플랫폼입니다.
> 처음 접하는 분도 이 문서 하나로 전체 구조를 이해하고 직접 운영할 수 있도록 작성했습니다.

---

## 목차

1. [이 시스템이 하는 일](#1-이-시스템이-하는-일)
2. [기술 스택](#2-기술-스택)
3. [전체 아키텍처 한눈에 보기](#3-전체-아키텍처-한눈에-보기)
4. [Docker — 왜 컨테이너인가](#4-docker--왜-컨테이너인가)
5. [Docker Compose — 5개 서비스가 협력하는 방법](#5-docker-compose--5개-서비스가-협력하는-방법)
6. [Apache Airflow 핵심 개념](#6-apache-airflow-핵심-개념)
7. [DAG 상세 구조](#7-dag-상세-구조)
8. [데이터 흐름 파이프라인](#8-데이터-흐름-파이프라인)
9. [데이터베이스 스키마](#9-데이터베이스-스키마)
10. [Streamlit 대시보드 — 5페이지 구성](#10-streamlit-대시보드--5페이지-구성)
11. [디렉토리 구조](#11-디렉토리-구조)
12. [처음 시작하기](#12-처음-시작하기)
13. [일상 운영](#13-일상-운영)
14. [문제 해결](#14-문제-해결)

---

## 1. 이 시스템이 하는 일

**18개 종목의 주식 데이터를 매일 자동으로 수집하고, 웹 대시보드로 투자 분석을 제공합니다.**

```
Yahoo Finance API ──► Airflow DAG (자동 스케줄) ──► PostgreSQL DB ──► Streamlit 웹 대시보드
     (데이터 원천)          (수집·처리 자동화)           (영구 저장)         (인터랙티브 분석)
```

### 수집 대상 종목

| 분류 | 종목 | 수집 내용 |
|------|------|----------|
| 🇺🇸 미국 주식 (11개) | AVGO · BE · VRT · SMR · OKLO · GEV · MRVL · COHR · LITE · VST · ETN | 가격 + 기술 지표 + 펀더멘털 + 뉴스 |
| 🇰🇷 한국 주식 (5개) | HD현대일렉트릭 · 두산에너빌리티 · 삼성물산 · HD현대중공업 · LS ELECTRIC | 가격 + 기술 지표 + 뉴스 |
| 🌐 ADR (2개) | SBGSY (Schneider Electric) · HTHIY (Hitachi) | 가격 + 기술 지표 + 뉴스 |
| 🏭 비상장 (2개) | TerraPower · X-Energy | 뉴스만 |

### 수집 스케줄 (UTC 기준)

```
매일 06:00 UTC (오후 3시 KST) ───► news_collection DAG
    └─ yfinance 뉴스 + Google News RSS 수집

매일 23:00 UTC (다음날 08:00 KST) ───► stock_price_collection DAG
    └─ OHLCV + 기술 지표 14개 + 펀더멘털 수집
```

---

## 2. 기술 스택

| 레이어 | 기술 | 버전 | 역할 |
|--------|------|------|------|
| 컨테이너 | Docker Desktop + Docker Compose | - | 격리된 실행 환경 |
| 워크플로우 | Apache Airflow | 2.8.0 | DAG 스케줄링 · 태스크 오케스트레이션 |
| 런타임 | Python | 3.11 | DAG 코드 실행 환경 |
| 데이터베이스 | PostgreSQL | 15 | 수집 데이터 영구 저장 |
| 데이터 수집 | yfinance | 0.2.66 | Yahoo Finance OHLCV · 펀더멘털 · 뉴스 |
| 뉴스 수집 | feedparser | 6.0.11 | Google News RSS 파싱 |
| 기술 지표 | ta | 0.11.0 | RSI · MACD · BB · SMA · CCI · ATR · MFI |
| 대시보드 | Streamlit | 1.32 | 5페이지 멀티페이지 웹 앱 |
| 차트 | Plotly | 5.19 | 인터랙티브 캔들스틱 차트 |
| DB 연결 | SQLAlchemy | 1.4.52 | Python ↔ PostgreSQL ORM |

---

## 3. 전체 아키텍처 한눈에 보기

### 거시 구조 (사용자 관점)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         내 로컬 머신 (Mac)                                │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    Docker 가상 환경                                │   │
│  │                                                                  │   │
│  │  ┌─────────────────┐    ┌─────────────────┐                     │   │
│  │  │  Airflow         │    │   PostgreSQL     │                     │   │
│  │  │  Webserver       │    │   (DB 서버)      │                     │   │
│  │  │  :8080           │◄──►│   :5432          │                     │   │
│  │  │                  │    │                  │                     │   │
│  │  │  Airflow         │    │  ┌────────────┐  │                     │   │
│  │  │  Scheduler       │    │  │  airflow   │  │ ← Airflow 메타DB    │   │
│  │  │  (DAG 실행)       │    │  │  stock_data│  │ ← 수집 데이터 DB    │   │
│  │  │                  │    │  └────────────┘  │                     │   │
│  │  └─────────────────┘    └─────────────────┘                     │   │
│  │          │                        ▲                               │   │
│  │          │ DAG 실행                │ 데이터 저장/조회               │   │
│  │          ▼                        │                               │   │
│  │  ┌───────────────┐       ┌────────────────┐                      │   │
│  │  │  Yahoo Finance │       │   Streamlit     │                      │   │
│  │  │  Google News   │       │   Dashboard     │                      │   │
│  │  │  (외부 API)    │       │   :8501         │                      │   │
│  │  └───────────────┘       └────────────────┘                      │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  브라우저: http://localhost:8080  (Airflow UI)                            │
│  브라우저: http://localhost:8501  (주식 대시보드)                           │
└─────────────────────────────────────────────────────────────────────────┘
```

### 데이터 흐름 요약

```
[외부]                [Airflow]              [PostgreSQL]         [Streamlit]

Yahoo Finance  ──►  fetch_prices  ──►  stock_prices 테이블  ──►  차트·지표
               ──►  calc_indicators      (OHLCV + 14개 지표)
               ──►  fetch_fundamentals   stock_fundamentals   ──►  펀더멘털
               ──►  fetch_news           stock_news           ──►  뉴스 피드
Google News    ──►  fetch_google_news
```

---

## 4. Docker — 왜 컨테이너인가

### 컨테이너 없이 설치하면 생기는 문제

```
내 Mac에 직접 설치:

  Airflow 설치 → Python 3.10 필요
  PostgreSQL 설치 → 포트 충돌 가능
  Streamlit 설치 → 다른 패키지와 버전 충돌
  ──────────────────────────────────────
  → "내 컴퓨터에서는 되는데 다른 데서 안 됨"
  → 삭제/재설치 과정이 복잡
```

### Docker 컨테이너 방식

```
┌─────────────────────────────────────────────┐
│  Docker Desktop (가상 환경 엔진)              │
│                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │컨테이너A  │  │컨테이너B  │  │컨테이너C  │  │
│  │Airflow   │  │PostgreSQL│  │Streamlit │  │
│  │Python3.11│  │Postgres15│  │Python3.11│  │
│  │포트:8080  │  │포트:5432  │  │포트:8501  │  │
│  └──────────┘  └──────────┘  └──────────┘  │
│                                              │
│  각 컨테이너는 완전히 독립 → 충돌 없음         │
│  docker compose up 한 줄로 전부 실행           │
└─────────────────────────────────────────────┘
```

**핵심 개념:**
- **이미지(Image)**: 컨테이너의 설계도 (Dockerfile로 정의)
- **컨테이너(Container)**: 이미지를 실행한 인스턴스 (실제로 돌아가는 것)
- **볼륨(Volume)**: 컨테이너 외부에 데이터 영구 저장소

---

## 5. Docker Compose — 5개 서비스가 협력하는 방법

`docker-compose.yml` 하나로 5개의 서비스를 정의하고 한 번에 실행합니다.

### 서비스 의존 관계

```
docker compose up -d

            postgres (먼저 실행, healthcheck 통과 후)
                 │
                 ├──► airflow-init    (최초 1회만: DB 마이그레이션 + admin 계정 생성)
                 │         │
                 │         └──► airflow-webserver  (포트 8080, UI 서버)
                 │         └──► airflow-scheduler   (DAG 실행 엔진)
                 │
                 └──► streamlit       (포트 8501, 대시보드)
```

### 각 서비스 역할

| 서비스 | 이미지 | 포트 | 역할 |
|--------|--------|------|------|
| `postgres` | postgres:15 | 5432 | 두 개의 DB 관리 (airflow 메타DB + stock_data) |
| `airflow-init` | 커스텀 빌드 | — | 최초 실행 시 DB 초기화 + 관리자 계정 생성 (이후 종료) |
| `airflow-webserver` | 커스텀 빌드 | 8080 | Airflow 관리 UI (DAG 모니터링·수동 트리거) |
| `airflow-scheduler` | 커스텀 빌드 | — | 스케줄에 따라 DAG 자동 실행 |
| `streamlit` | 커스텀 빌드 | 8501 | 주식 분석 대시보드 웹 앱 |

### 공유 설정 (YAML 앵커)

```yaml
# docker-compose.yml 내부
x-airflow-common: &airflow-common   # ← 공통 설정 정의
  build: .
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    DATA_DB_CONN: postgresql+psycopg2://...
  volumes:
    - ./dags:/opt/airflow/dags       # 로컬 DAG 파일을 컨테이너에 실시간 마운트

airflow-webserver:
  <<: *airflow-common   # ← 공통 설정 상속
  command: webserver

airflow-scheduler:
  <<: *airflow-common   # ← 공통 설정 상속
  command: scheduler
```

> **볼륨 마운트 핵심**: `./dags:/opt/airflow/dags` — 로컬에서 DAG 파일을 수정하면 컨테이너 재시작 없이 즉시 반영됩니다.

---

## 6. Apache Airflow 핵심 개념

### Airflow가 하는 일

```
"매일 23:00에 18개 종목의 주가를 수집해라"

→ 이 스케줄과 작업 순서를 코드로 정의한 것이 DAG
→ Scheduler가 시간 되면 자동 실행
→ Webserver에서 결과 모니터링
```

### 핵심 용어

| 용어 | 의미 | 비유 |
|------|------|------|
| **DAG** (Directed Acyclic Graph) | 태스크 실행 순서를 정의한 워크플로우 파이프라인 | 요리 레시피 |
| **Task** | DAG 안의 개별 작업 단위 | 레시피의 각 단계 |
| **TaskGroup** | 여러 Task를 논리적으로 묶은 그룹 | 레시피의 한 섹션 |
| **PythonOperator** | Python 함수를 실행하는 Operator | 구체적인 조리 방법 |
| **Scheduler** | 스케줄에 따라 DAG를 실행시키는 엔진 | 알람시계 |
| **LocalExecutor** | 단일 머신에서 Task를 병렬 실행하는 방식 | 혼자 여러 요리를 동시에 |
| **DAG Run** | DAG가 한 번 실행된 인스턴스 | 오늘의 요리 한 회차 |

### Airflow 실행 흐름

```
1. dags/ 폴더에 Python 파일 작성 (DAG 정의)
        │
        ▼
2. Scheduler가 DAG 파일을 주기적으로 스캔
        │
        ▼
3. 정해진 cron 시간이 되면 DAG Run 생성
        │
        ▼
4. Task들을 의존성 순서에 따라 실행
   (validate_db 완료 → fetch_prices 실행 → indicators 실행...)
        │
        ▼
5. Webserver(UI)에서 성공/실패/실행 중 상태 확인
        │
        ▼
6. 실패 시 retries 횟수만큼 자동 재시도 (5분 간격)
```

---

## 7. DAG 상세 구조

### DAG 1: `stock_price_collection` (매일 23:00 UTC)

```
validate_db  ←─ stock_prices, stock_fundamentals 테이블 존재 확인
     │
     ▼
┌─────────────────────────────────────────────────────┐
│  fetch_prices_group  (18개 종목 병렬 실행)             │
│                                                     │
│  fetch_AVGO  fetch_BE  fetch_VRT  ... (×18)          │
│  └─ yfinance.Ticker(sym).history(period="60d")      │
│  └─ OHLCV → stock_prices 테이블 upsert              │
└─────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────┐
│  calculate_indicators_group  (18개 종목 병렬)          │
│                                                     │
│  DB에서 최근 250행 읽기 → ta 라이브러리로 계산:         │
│  SMA(20/50/200), BB(20), RSI(14), MACD(12/26/9),   │
│  CCI(20), ATR(14), OBV, MFI(14)                    │
│  → stock_prices UPDATE (14개 지표 컬럼)               │
└─────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────┐
│  fetch_fundamentals_group  (18개 종목 병렬)            │
│                                                     │
│  yfinance.Ticker(sym).info                          │
│  → PER, PBR, ROE, EPS, 시가총액, 배당수익률, 섹터     │
│  → stock_fundamentals upsert                        │
└─────────────────────────────────────────────────────┘
     │
     ▼
dag_complete

특이사항:
  · max_active_runs=1  → 중복 실행 방지
  · retries=2, retry_delay=5분
  · PARALLELISM=18  → 18개 종목 동시 처리
  · Trigger with config {"period": "3y"}  → 3년치 히스토리 수집
```

### DAG 2: `news_collection` (매일 06:00 UTC)

```
┌──────────────────────────────┐   ┌─────────────────────────────────┐
│  yfinance_news_group          │   │  google_news_group               │
│  (상장 18개 종목 병렬)         │   │  (20개 쿼리: 비상장사 포함)        │
│  ticker.news API              │   │  Google News RSS 피드 파싱        │
│                               │   │  feedparser 라이브러리            │
└──────────────────────────────┘   └─────────────────────────────────┘
              │                                    │
              └──────────────────┬─────────────────┘
                                 ▼
                           news_complete

  · ON CONFLICT (symbol, url) DO NOTHING → 중복 뉴스 자동 무시
  · 비상장사(TerraPower, X-Energy)는 Google News만
```

---

## 8. 데이터 흐름 파이프라인

```
┌──────────────────────────────────────────────────────────────────────┐
│  Step 1: 데이터 수집                                                   │
│                                                                        │
│  Yahoo Finance ──► yfinance.Ticker("AVGO").history(period="60d")      │
│                    └─ DataFrame: Date / Open / High / Low / Close / Volume │
└──────────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Step 2: 기술 지표 계산 (ta 라이브러리)                                 │
│                                                                        │
│  close 가격 ──► SMA(20/50/200), RSI(14), MACD(12/26/9), BB(20)        │
│  high/low/close ──► CCI(20), ATR(14)                                  │
│  close/volume ──► OBV                                                 │
│  high/low/close/volume ──► MFI(14)                                    │
└──────────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Step 3: PostgreSQL 저장 (executemany - 벌크 upsert)                   │
│                                                                        │
│  INSERT INTO stock_prices (...) VALUES (...)                           │
│  ON CONFLICT (symbol, trade_date) DO UPDATE SET ...                   │
│  └─ 같은 날짜 데이터가 이미 있으면 업데이트 (멱등성 보장)               │
└──────────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Step 4: Streamlit 대시보드 조회                                        │
│                                                                        │
│  SELECT trade_date, close, rsi_14, macd, ...                          │
│  FROM stock_prices                                                     │
│  WHERE symbol = 'AVGO'                                                 │
│    AND trade_date >= CURRENT_DATE - 90 * INTERVAL '1 day'             │
│  ORDER BY trade_date                                                   │
└──────────────────────────────────────────────────────────────────────┘
```

### 기술 지표 설명

| 지표 | 계산 기준 | 투자 의미 |
|------|----------|----------|
| SMA 20/50/200 | 종가 단순이동평균 | 단기·중기·장기 추세선 |
| BB (볼린저밴드) | SMA20 ± 2σ | 가격 변동성 범위 |
| RSI (14일) | 상승·하락 비율 | 과매수(>70) / 과매도(<30) |
| MACD (12/26/9) | 지수이동평균 차이 | 추세 전환 시그널 |
| CCI (20일) | 평균가격 편차 | 추세 강도 측정 |
| ATR (14일) | 고가-저가 평균 범위 | 현재 변동성 크기 |
| OBV | 거래량 누적합 | 매수/매도 세력 비교 |
| MFI (14일) | 거래량 가중 RSI | 자금 유입/유출 |

---

## 9. 데이터베이스 스키마

### 두 개의 데이터베이스

```
PostgreSQL 서버
├── airflow         ← Airflow 내부 메타데이터 (DAG 실행 기록, 태스크 상태)
│                     → Airflow가 자동 관리
│
└── stock_data      ← 우리가 수집한 주식 데이터
    ├── stock_prices          (핵심: OHLCV + 기술 지표 14개)
    ├── stock_fundamentals    (PER, PBR, ROE, 시가총액 등)
    ├── stock_news            (뉴스 제목, URL, 요약)
    └── chart_generation_log  (예약)
```

### 핵심 테이블: stock_prices

```
symbol    │ trade_date │ open   │ close  │ volume  │ rsi_14 │ macd   │ ...
──────────┼────────────┼────────┼────────┼─────────┼────────┼────────┼────
AVGO      │ 2025-12-27 │ 240.10 │ 245.30 │ 8500000 │  58.2  │  1.23  │ ...
AVGO      │ 2025-12-26 │ 238.50 │ 240.10 │ 7200000 │  54.8  │  0.95  │ ...
267260.KS │ 2025-12-27 │ 95000  │ 97500  │  120000 │  62.1  │  350   │ ...

UNIQUE(symbol, trade_date) → 같은 날짜 중복 불가, upsert 보장
```

---

## 10. Streamlit 대시보드 — 5페이지 구성

### 접속: `http://localhost:8501`

```
사이드바 네비게이션
├── 📈 Market Overview  ← 홈, 전체 시장 한눈에
├── 📈 Stock Detail     ← 개별 종목 깊은 분석
├── 🔍 Screener         ← 조건에 맞는 종목 필터링
├── ⚖️  Comparison       ← 여러 종목 비교
└── 📰 News Feed        ← 전체 뉴스 피드
```

### Page 1: Market Overview (홈)

```
[ 상승 N개 | 하락 N개 | 매수 신호 N개 | 매도 신호 N개 ]

탭: [전체] [🇺🇸 US] [🇰🇷 KR] [🌐 ADR]

종목  │ 회사명    │ 현재가  │ 1일%  │ 1주%  │ 1개월% │ 1년%  │ RSI  │ 신호
──────┼───────────┼─────────┼───────┼───────┼────────┼───────┼──────┼──────
AVGO  │ Broadcom  │ 245.30  │ +1.2% │ +3.5% │  +8.1% │ +45%  │ 58.2 │ 매수
BE    │ Bloom E.  │  12.80  │ -0.8% │ +1.2% │  -3.5% │ +12%  │ 42.1 │ 중립
...

[ 오늘의 주요 움직임 — 상위/하위 3개 ]
[ 신호 강도 순위 ]
```

### Page 2: Stock Detail

```
[ 종가 | 고가 | 저가 | 거래량 | RSI | MFI ]

기간: [1W] [1M] [3M] [6M] [1Y] [2Y] [3Y]

┌─────────────────────────────────────────────────────┐
│ 캔들스틱 + SMA20/50/200 + 볼린저밴드                  │
│ MACD 크로스오버 시 ▲(매수) ▼(매도) 화살표 표시        │
│ [거래량 바]                                          │
│ [RSI — 70/30선, 과매수·과매도 음영]                  │
│ [MACD 히스토그램 + 라인]                             │
└─────────────────────────────────────────────────────┘

추가 패널 선택: [ CCI ] [ ATR ] [ OBV ] [ MFI ]

────── 기술적 신호 분석 ──────
RSI: 🟢 중립(58.2)    MACD: 🟢 강세     SMA200: 🟢 +12.5%
황금십자: 🟢 유지      BB: ⚪ 중간구간   MFI: ⚪ 중립(52.1)
종합 신호: 매수 (+4.5점)

탭: [펀더멘털] [지표 테이블] [최근 뉴스]
```

### Page 3: Screener

```
사이드바: RSI범위 / SMA200위치 / MACD방향 / BB위치 / 종합신호

→ 조건 충족: N / 18개

┌──────────────────────────────────────────┐
│ 🟢 잠재 매수 기회 (RSI<50 + 매수 신호)    │
│  BE  Bloom Energy  RSI:28  강세MACD  매수 │
│  ...                                     │
└──────────────────────────────────────────┘

┌──────────────────────────────────────────┐
│ 🔴 주의 필요 (RSI>50 + 매도 신호)         │
│  AVGO Broadcom  RSI:72  약세BB  강력매도  │
└──────────────────────────────────────────┘
```

### Page 4: Comparison

```
종목 선택: AVGO / GEV / ETN  |  기간: 1Y  |  정규화: ON

140 ─────────────────────────────  AVGO (+40%)
120 ─────────────────────────────  GEV  (+20%)
100 ─────────────────────────────  ETN  (기준=100)
 80
 Jan      Apr      Jul      Oct

수익률 비교표: 1D / 1W / 1M / 1Y
펀더멘털 비교: PER / PBR / ROE / EPS
기간별 수익률 막대 그래프
```

### 신호 엔진 작동 방식

```
지표별 신호 계산 (6개 지표, 가중치 투표):

  RSI < 30      → BUY ●● (강도2, 가중1.5)   RSI > 70      → SELL ●● (강도2, 가중1.5)
  MACD > Signal → BUY ●  (강도1, 가중1.0)   MACD < Signal → SELL ●  (강도1, 가중1.0)
  Close >SMA200 +5% → BUY ●● (강도2, 가중1.5)  < -5% → SELL ●● (강도2, 가중1.5)
  SMA50 > SMA200 → 황금십자 BUY ●● (강도2, 가중2.0)
  BB 하단 근접  → BUY ●  (강도1, 가중0.5)   BB 상단 근접  → SELL ●  (강도1, 가중0.5)
  MFI < 20      → BUY ●● (강도2, 가중1.0)   MFI > 80      → SELL ●● (강도2, 가중1.0)

최대 점수: ±11.5점
  점수 ≥ +5.0 → 강력매수
  점수 ≥ +2.0 → 매수
  -2.0 ~ +2.0 → 중립
  점수 ≤ -2.0 → 매도
  점수 ≤ -5.0 → 강력매도
```

---

## 11. 디렉토리 구조

```
airflow_setup/
│
├── docker-compose.yml          # 5개 서비스 정의 (핵심 설정 파일)
├── Dockerfile                  # Airflow 커스텀 이미지 빌드 설정
├── requirements.txt            # Airflow 컨테이너 Python 패키지
├── .env                        # 비밀번호·키 설정 (git 제외)
├── .env.example                # .env 템플릿 (git 포함)
│
├── dags/                       # ← 이 폴더가 컨테이너에 실시간 마운트됨
│   ├── common.py               # 공통 설정 (종목 목록, DB 엔진, DEFAULT_ARGS)
│   ├── stock_price_dag.py      # 가격·지표·펀더멘털 수집 DAG
│   └── news_dag.py             # 뉴스 수집 DAG
│
├── streamlit/                  # Streamlit 대시보드 (별도 컨테이너)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── db.py                   # DB 쿼리·신호 감지·차트 빌더 공통 모듈
│   ├── app.py                  # 홈 (시장 개요)
│   └── pages/
│       ├── 1_Stock_Detail.py
│       ├── 2_Screener.py
│       ├── 3_Comparison.py
│       └── 4_News_Feed.py
│
├── scripts/
│   └── init_data_db.sql        # PostgreSQL 최초 실행 시 stock_data DB·테이블 생성
│
├── logs/                       # Airflow 실행 로그 (git 제외)
└── plugins/                    # Airflow 플러그인 (현재 미사용)
```

---

## 12. 처음 시작하기

### 사전 준비

```bash
# Docker Desktop 설치 (macOS)
brew install --cask docker
# 설치 후 메뉴바에 고래 아이콘이 보이면 준비 완료
```

### 1단계: 환경 변수 설정

```bash
cd airflow_setup
cp .env.example .env
# .env 파일 열어서 비밀번호 입력
```

```bash
# FERNET_KEY 생성 방법:
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 2단계: 권한 설정

```bash
mkdir -p logs plugins
chmod -R 777 logs
```

### 3단계: 초기화 (최초 1회)

```bash
docker compose up airflow-init
# "Airflow initialized successfully" 출력 확인
```

> 이 단계에서: Airflow DB 마이그레이션 + admin 계정 생성 + stock_data DB/테이블 자동 생성

### 4단계: 전체 실행

```bash
docker compose up -d
docker ps   # 모든 Status가 "healthy"인지 확인 (1~2분 소요)
```

### 5단계: 접속

- Airflow UI: http://localhost:8080 (admin / 설정한 비밀번호)
- 주식 대시보드: http://localhost:8501

### 6단계: 첫 데이터 수집 (3년치 히스토리)

Airflow UI → `stock_price_collection` → **Trigger DAG w/ config**:
```json
{"period": "3y"}
```
→ 18개 종목 × 3년치 데이터 수집 (5~10분 소요)

---

## 13. 일상 운영

### 자동 실행 확인

```bash
docker ps                                               # 컨테이너 상태
docker logs airflow_setup-airflow-scheduler-1 --tail 20  # 스케줄러 로그
# Airflow UI → DAGs → stock_price_collection → 최근 실행 결과
```

### 수동 데이터 수집

```bash
# Airflow UI에서: DAG 선택 → ▶ Trigger DAG 버튼
# 또는 CLI:
docker exec airflow_setup-airflow-scheduler-1 \
  airflow dags trigger stock_price_collection
```

### 코드 수정 반영

```bash
# DAG 파일 수정 → 파일 저장만 하면 자동 반영 (볼륨 마운트)

# Streamlit 앱 수정 후:
docker compose build streamlit && docker compose up -d streamlit
```

### 종료

```bash
docker compose down        # 컨테이너 종료 (DB 데이터 유지)
docker compose down -v     # 컨테이너 + DB 데이터 전체 삭제
```

---

## 14. 문제 해결

| 증상 | 원인 | 해결 |
|------|------|------|
| Webserver가 healthy 안 됨 | postgres 아직 준비 중 | 1~2분 대기 |
| DAG가 UI에 안 보임 | Python 문법 오류 | `docker logs scheduler` 확인 |
| YFRateLimitError | Yahoo Finance IP 일시 차단 | 1시간 대기 (하루 1회 운영 시 미발생) |
| 한국 주식 데이터 부족 | KRX 시간대 차이 | 다음날 DAG 실행 시 포함됨 |
| Streamlit "데이터 없음" | DAG 미실행 | stock_price_collection DAG 트리거 |

```bash
# DB 직접 확인
docker exec -it airflow_setup-postgres-1 \
  psql -U airflow -d stock_data \
  -c "SELECT symbol, COUNT(*), MAX(trade_date) FROM stock_prices GROUP BY symbol;"

# 디스크 정리
docker system prune -f
```

---

## 요약: 전체 시스템을 한 줄로

```
Docker로 격리된 환경에서 Airflow가 매일 Yahoo Finance에서 18개 종목의
주가·지표·뉴스를 수집해 PostgreSQL에 저장하고, Streamlit이 이를 읽어
인터랙티브 5페이지 투자 분석 대시보드로 보여줍니다.
```
