# Data Archivist

**Purpose**: Validate, structure, and persist Physical AI research data to PostgreSQL with complete traceability.

# Database Operations Standard

## 1. SQL Best Practices
- **No SELECT ***: 모든 쿼리에서 명시적 컬럼명을 사용하십시오.
- **Deduplication**: source_url 기준 중복 방지를 위해 ON CONFLICT 구문을 활용하십시오.
- **Logic**: Window Function(ROW_NUMBER())을 사용하여 최신 데이터만 추출하십시오.

## 2. Performance & Integrity
- **Indexing**: source_url, published_at, scope 컬럼에 인덱스 생성하여 조회 성능 확보하십시오.
- **Partitioning**: published_at 기준 월별 파티셔닝으로 대용량 데이터 관리하십시오.
- **Idempotency**: event_id 기반 UPSERT로 데이터 재현성을 보장하십시오.
- **Transactions**: BEGIN/COMMIT을 사용하여 원자성을 보장하십시오.

# Grain & Cardinality
- **Grain**: 단일 기사/논문/공시 레코드 1건.
- **Join Cardinality**: signals(N) : weekly_reports(1).

## Skill Overview
Manages the entire data lifecycle:
- Schema validation & enforcement
- Deduplication & quality checks
- PostgreSQL ingestion with partitioning
- Metadata enrichment & lineage tracking

## Input Parameters
```json
{
  "data": [
    {
      "event_id": "uuid",
      "scope": "Market|Tech|Case|Policy",
      "title": "...",
      "summary": "...",
      "strategic_implication": "...",
      "source_metadata": {...}
    }
  ],
  "destination": "postgresql",
  "schema": "public",
  "table": "market_signals",
  "partition_field": "published_at"
}
```

## Output Schema (PostgreSQL DDL)
```sql
CREATE TABLE IF NOT EXISTS market_signals (
  id                    SERIAL PRIMARY KEY,
  event_id              VARCHAR(36) NOT NULL UNIQUE,
  scope                 VARCHAR(20) NOT NULL CHECK (scope IN ('Market', 'Tech', 'Case', 'Policy')),
  category              VARCHAR(100),
  title                 TEXT NOT NULL,
  summary               TEXT,
  strategic_implication TEXT,
  key_insights          TEXT[],  -- PostgreSQL array

  -- Source Metadata
  source_url            TEXT NOT NULL,
  publisher             VARCHAR(200),
  published_at          TIMESTAMP NOT NULL,
  scraped_at            TIMESTAMP NOT NULL,
  confidence_score      DECIMAL(3,2) CHECK (confidence_score BETWEEN 0 AND 1),

  -- Lineage Metadata
  processing_pipeline   VARCHAR(100) DEFAULT 'scout->analysis->archivist',
  processed_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  schema_version        VARCHAR(10) DEFAULT 'v1.0',

  -- Quality Metrics
  data_quality_score    DECIMAL(3,2),
  validation_errors     TEXT[],

  created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (published_at);

-- Create monthly partitions (example for 2024)
CREATE TABLE market_signals_2024_01 PARTITION OF market_signals
  FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE market_signals_2024_02 PARTITION OF market_signals
  FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

-- Create indexes for performance
CREATE INDEX idx_market_signals_published_at ON market_signals (published_at DESC);
CREATE INDEX idx_market_signals_scope ON market_signals (scope);
CREATE INDEX idx_market_signals_category ON market_signals (category);
CREATE INDEX idx_market_signals_source_url ON market_signals USING HASH (source_url);
CREATE INDEX idx_market_signals_event_id ON market_signals (event_id);
```

## Key Functions

### 1. Schema Validation
```python
def validate_schema(
    record: dict,
    schema_version: str = "v1.0"
) -> tuple[bool, list[str]]:
    """
    Validate record against PASIS schema.

    Checks:
    - Required fields present
    - Data types correct
    - Enum values valid (scope, category)
    - URL format valid
    - Timestamp parseable

    Returns: (is_valid, error_messages)
    """
```

**Validation Rules:**
- `event_id`: Valid UUID v4
- `scope`: One of [Market, Tech, Case, Policy]
- `source_url`: Valid HTTP(S) URL, not 404
- `published_at`: ISO-8601, not future date
- `confidence_score`: 0.0 ≤ score ≤ 1.0

### 2. Deduplication
```python
def deduplicate_records(
    records: list[dict],
    strategy: str = "content_hash"
) -> list[dict]:
    """
    Remove duplicate signals using multiple strategies.

    Strategies:
    - content_hash: SHA-256 of (title + summary)
    - source_url: Exact URL match
    - fuzzy_title: 90% similarity threshold

    Returns: Deduplicated list with duplicate_of field
    """
```

**Dedup Logic:**
```sql
-- Example: Window function for deduplication in PostgreSQL
WITH ranked_signals AS (
  SELECT
    event_id,
    title,
    published_at,
    ROW_NUMBER() OVER (
      PARTITION BY source_url
      ORDER BY scraped_at DESC
    ) AS rn
  FROM market_signals
)
SELECT event_id, title, published_at
FROM ranked_signals
WHERE rn = 1;
```

### 3. Quality Scoring
```python
def calculate_quality_score(record: dict) -> float:
    """
    Compute data quality score (0.0-1.0).

    Factors:
    - Metadata completeness (40%)
    - Source authority (30%)
    - Content richness (20%)
    - Timeliness (10%)

    Example:
    - SEC filing: 0.95 (high authority)
    - Blog post: 0.60 (lower authority)
    """
```

**Quality Rubric:**
| Factor | Weight | Criteria |
|:---|:---|:---|
| Metadata | 40% | All fields present? Timestamps valid? |
| Authority | 30% | Primary source (SEC, arXiv) vs secondary (news) |
| Content | 20% | Summary length, keyword density |
| Timeliness | 10% | Published within 30 days? |

### 4. PostgreSQL Ingestion
```python
def ingest_to_postgresql(
    records: list[dict],
    table: str = "market_signals",
    conn_string: str = None
) -> dict:
    """
    Batch upsert to PostgreSQL using ON CONFLICT.

    Features:
    - Automatic partitioning by published_at (monthly)
    - Indexed by scope, category, source_url
    - Idempotent (ON CONFLICT DO UPDATE on event_id)

    Returns: {
      "rows_inserted": int,
      "rows_updated": int,
      "errors": list
    }
    """
```

**Upsert Logic (Idempotent):**
```sql
-- PostgreSQL UPSERT using ON CONFLICT
INSERT INTO market_signals (
  event_id, scope, category, title, summary, strategic_implication,
  source_url, publisher, published_at, scraped_at, confidence_score,
  data_quality_score
)
VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
)
ON CONFLICT (event_id)
DO UPDATE SET
  summary = EXCLUDED.summary,
  strategic_implication = EXCLUDED.strategic_implication,
  updated_at = CURRENT_TIMESTAMP
RETURNING id, event_id, (xmax = 0) AS inserted;
-- xmax = 0 means INSERT, xmax != 0 means UPDATE
```

## Data Pipeline Workflow

```
┌─────────────────┐
│ physical-ai     │
│ scout           │  → Raw signals
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ strategic       │
│ analysis        │  → Enriched signals
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ DATA ARCHIVIST  │  ← YOU ARE HERE
└────────┬────────┘
         │
         ▼  (validate → dedupe → score → ingest)
┌─────────────────┐
│ PostgreSQL      │
│ market_signals  │
│ (partitioned)   │
└─────────────────┘
```

## Partitioning Strategy

### Date Partitioning (Recommended for large datasets)
```sql
-- Monthly partitioning by published_at
CREATE TABLE market_signals (
  ...
) PARTITION BY RANGE (published_at);

-- Auto-create future partitions using pg_partman extension (optional)
-- Or manually create partitions:
CREATE TABLE market_signals_2024_03 PARTITION OF market_signals
  FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');

-- Partition pruning example (automatic in PostgreSQL 11+):
SELECT event_id, title, scope
FROM market_signals
WHERE published_at BETWEEN '2024-01-01' AND '2024-01-31';
-- Only scans market_signals_2024_01 partition
```

### Indexing Strategy (Essential for performance)
```sql
-- Multi-column index for common queries
CREATE INDEX idx_scope_category_date ON market_signals (scope, category, published_at DESC);

-- Efficient filtering:
SELECT event_id, title, summary
FROM market_signals
WHERE scope = 'Market'
  AND category = 'M&A'
  AND published_at >= '2024-02-01'
ORDER BY published_at DESC;
-- Uses idx_scope_category_date for optimal performance
```

## Error Handling

### Validation Failures
```python
# Example: Handle validation errors
try:
    is_valid, errors = validate_schema(record)
    if not is_valid:
        log.warning(f"Validation failed for {record['event_id']}: {errors}")
        # Store in error table for manual review
        insert_to_error_table(record, errors)
        continue
except Exception as e:
    log.error(f"Unexpected error: {e}")
    raise
```

### PostgreSQL Connection Errors
```python
# Retry with exponential backoff
import psycopg2
from psycopg2 import OperationalError
import time

def ingest_with_retry(records, max_retries=3):
    for attempt in range(max_retries):
        try:
            with psycopg2.connect(conn_string) as conn:
                with conn.cursor() as cur:
                    # Batch insert with executemany
                    cur.executemany(upsert_sql, records)
                conn.commit()
                return {"success": True}
        except OperationalError as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                log.warning(f"Connection failed, retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise
```

## Quality Checks
- ✅ All required fields present (event_id, title, source_url, published_at)
- ✅ No duplicate event_ids in same batch
- ✅ Partitioning field (published_at) never NULL
- ✅ Data quality score ≥ 0.5 (reject low-quality)
- ✅ Source URL reachable (HTTP 200)

## Monitoring & Alerting

### Key Metrics
```sql
-- Daily ingestion volume
SELECT
  DATE(processed_at) AS date,
  scope,
  COUNT(*) AS num_signals,
  AVG(data_quality_score) AS avg_quality
FROM market_signals
WHERE processed_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(processed_at), scope
ORDER BY date DESC;
```

### Alert Thresholds
- Daily ingestion < 10 signals → Alert: Data pipeline issue
- Avg quality score < 0.7 → Alert: Source quality degraded
- Duplicate rate > 5% → Alert: Dedup logic broken

## Example Usage
```bash
claude-skill data-archivist \
  --input strategic-signals.json \
  --table "market_signals" \
  --validate-only false \
  --output ingestion-report.json
```

## Integration Points
- Consumes from: `strategic-analysis` (validated signals)
- Persists to: PostgreSQL (partitioned tables)
- Requires: PostgreSQL connection string (psycopg2/SQLAlchemy)
- Monitors: Airflow for orchestration, custom metrics dashboard

## Required Tools & Dependencies
- **Python**: psycopg2-binary or asyncpg for PostgreSQL connection
- **SQLAlchemy**: ORM for database operations (optional but recommended)
- **PostgreSQL**: v12+ for native partitioning support
- **Extensions**: pg_partman (optional, for automatic partition management)