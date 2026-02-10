# Airflow Stock Data Collection

Apache Airflow (Docker Compose) 기반 주식 데이터 수집 시스템.

## 수집 대상

| 분류 | 종목 |
|------|------|
| US | AVGO, BE, VRT, SMR, OKLO, GEV, MRVL, COHR, LITE, VST, ETN |
| 한국 | 267260.KS, 034020.KS, 028260.KS, 267270.KS, 010120.KS |
| ADR | SBGSY (Schneider), HTHIY (Hitachi) |
| 비상장 (뉴스만) | TerraPower, X-Energy |

## DAG 스케줄

| DAG | 스케줄 | 설명 |
|-----|--------|------|
| `stock_price_collection` | `0 23 * * *` | OHLCV + 기술적 지표 + 펀더멘털 |
| `chart_generation` | `30 23 * * *` | mplfinance 캔들스틱 차트 PNG |
| `news_collection` | `0 6 * * *` | yfinance + Google News RSS |

## 시작 방법

### 1. 환경 변수 설정

```bash
cp .env.example .env
```

`.env` 파일을 열어 아래 값들을 채웁니다:

```bash
POSTGRES_PASSWORD=your_secure_password

# Fernet key 생성 (Python 필요):
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
FERNET_KEY=<위 결과>

WEBSERVER_SECRET_KEY=<임의의 긴 문자열>
AIRFLOW_ADMIN_PASSWORD=your_admin_password
```

### 2. 디렉토리 권한 설정

```bash
chmod -R 777 logs data
```

### 3. Airflow 초기화 (최초 1회)

```bash
docker-compose up airflow-init
```

성공 메시지 확인 후 Ctrl+C.

### 4. 서비스 시작

```bash
docker-compose up -d
```

### 5. Web UI 접속

- URL: http://localhost:8080
- ID: `admin` / PW: `.env`의 `AIRFLOW_ADMIN_PASSWORD`

DAG 목록에서 각 DAG 좌측의 토글을 켜서 활성화하세요.

---

## 검증

### DB 직접 접속

```bash
docker exec -it airflow_setup-postgres-1 psql -U airflow -d stock_data
```

```sql
-- 수집된 가격 데이터 확인
SELECT symbol, trade_date, close, rsi_14, macd
FROM stock_prices
ORDER BY trade_date DESC
LIMIT 10;

-- 펀더멘털 확인
SELECT * FROM stock_fundamentals LIMIT 5;

-- 뉴스 확인
SELECT symbol, title, published FROM stock_news ORDER BY published DESC LIMIT 10;

-- 차트 생성 이력
SELECT * FROM chart_generation_log ORDER BY created_at DESC LIMIT 10;
```

### 차트 파일 확인

```bash
ls data/charts/$(date +%Y%m%d)/
```

### SMA-200 Bootstrap (최초 1회 권장)

SMA-200 계산을 위해 2년치 데이터가 필요합니다.
DAG를 수동 트리거하기 전에 `stock_price_dag.py`의 `fetch_and_upsert_prices` 호출에서
`bootstrap=True`로 변경하거나, Airflow Variables에 `bootstrap=true`를 설정하세요.

---

## 디렉토리 구조

```
airflow_setup/
├── docker-compose.yml
├── Dockerfile
├── .env                    # 실제 환경 변수 (git 제외)
├── .env.example            # 템플릿
├── requirements.txt
├── dags/
│   ├── stock_price_dag.py
│   ├── chart_dag.py
│   └── news_dag.py
├── plugins/
├── logs/
├── data/
│   └── charts/
│       └── YYYYMMDD/
│           └── {SYMBOL}.png
├── scripts/
│   └── init_data_db.sql
└── README.md
```

---

## 주의사항

- **SQLAlchemy 1.4.52** 고정 — 2.x 설치 시 Airflow 내부 ORM 에러 발생
- **`logs/`, `data/`** 는 777 권한 필요 (Airflow 컨테이너 UID 50000)
- **한국 시장**: KRX 마감 시각이 UTC 06:30이므로 `0 23 * * *` 스케줄로 충분
- **yfinance 레이트 리밋**: 각 태스크 후 `time.sleep(1)` 적용됨

## 서비스 중지

```bash
docker-compose down          # 컨테이너만 종료 (볼륨 유지)
docker-compose down -v       # 컨테이너 + 볼륨 완전 삭제
```
