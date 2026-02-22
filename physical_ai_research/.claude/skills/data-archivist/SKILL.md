# Data Archivist

**Purpose**: Validate, structure, and persist Physical AI research data to BigQuery with complete traceability.

## Skill Overview
Manages the entire data lifecycle:
- Schema validation & enforcement
- Deduplication & quality checks
- BigQuery ingestion with partitioning
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
  "destination": "bigquery",
  "dataset": "physical_ai_research",
  "table": "market_signals",
  "partition_field": "published_at"
}
```

## Output Schema (BigQuery DDL)
```sql
CREATE TABLE `physical_ai_research.market_signals`
(
  event_id              STRING NOT NULL,
  scope                 STRING NOT NULL,  -- Market|Tech|Case|Policy
  category              STRING,
  title                 STRING NOT NULL,
  summary               STRING,
  strategic_implication STRING,
  key_insights          ARRAY<STRING>,

  -- Source Metadata
  source_url            STRING NOT NULL,
  publisher             STRING,
  published_at          TIMESTAMP NOT NULL,
  scraped_at            TIMESTAMP NOT NULL,
  confidence_score      FLOAT64,

  -- Lineage Metadata
  processing_pipeline   STRING,  -- scout -> analysis -> archivist
  processed_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  schema_version        STRING DEFAULT 'v1.0',

  -- Quality Metrics
  data_quality_score    FLOAT64,
  validation_errors     ARRAY<STRING>
)
PARTITION BY DATE(published_at)
CLUSTER BY scope, category;
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
-- Example: QUALIFY for deduplication in BigQuery
SELECT
  event_id,
  title,
  published_at,
  ROW_NUMBER() OVER (
    PARTITION BY source_url
    ORDER BY scraped_at DESC
  ) AS rn
FROM market_signals
QUALIFY rn = 1;
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

### 4. BigQuery Ingestion
```python
def ingest_to_bigquery(
    records: list[dict],
    dataset: str = "physical_ai_research",
    table: str = "market_signals",
    write_disposition: str = "WRITE_APPEND"
) -> dict:
    """
    Batch insert to BigQuery with partitioning.

    Features:
    - Automatic partitioning by published_at
    - Clustering by scope + category
    - Idempotent (MERGE on event_id)

    Returns: {
      "rows_inserted": int,
      "rows_updated": int,
      "errors": list
    }
    """
```

**Upsert Logic (Idempotent):**
```sql
MERGE `physical_ai_research.market_signals` AS target
USING new_data AS source
ON target.event_id = source.event_id
WHEN MATCHED THEN
  UPDATE SET
    summary = source.summary,
    strategic_implication = source.strategic_implication,
    processed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT ROW;
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
│ BigQuery        │
│ physical_ai     │
│ _research       │
└─────────────────┘
```

## Partitioning Strategy

### Date Partitioning (Mandatory)
```sql
-- Partition by published_at (daily)
PARTITION BY DATE(published_at)

-- Partition pruning example:
SELECT *
FROM market_signals
WHERE DATE(published_at) BETWEEN '2024-01-01' AND '2024-01-31'
-- Only scans 31 partitions instead of full table
```

### Clustering (Optional but Recommended)
```sql
CLUSTER BY scope, category

-- Efficient filtering:
SELECT *
FROM market_signals
WHERE scope = 'Market'
  AND category = 'M&A'
  AND DATE(published_at) = '2024-02-20'
-- Combines partition + cluster for minimal scan
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

### BigQuery Quota Errors
```python
# Retry with exponential backoff
from google.api_core import retry

@retry.Retry(predicate=retry.if_exception_type(
    google.api_core.exceptions.ResourceExhausted
))
def ingest_with_retry(records):
    return client.insert_rows_json(table, records)
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
WHERE DATE(processed_at) >= CURRENT_DATE() - 7
GROUP BY 1, 2
ORDER BY 1 DESC;
```

### Alert Thresholds
- Daily ingestion < 10 signals → Alert: Data pipeline issue
- Avg quality score < 0.7 → Alert: Source quality degraded
- Duplicate rate > 5% → Alert: Dedup logic broken

## Example Usage
```bash
claude-skill data-archivist \
  --input strategic-signals.json \
  --dataset "physical_ai_research" \
  --table "market_signals" \
  --validate-only false \
  --output ingestion-report.json
```

## Integration Points
- Consumes from: `strategic-analysis` (validated signals)
- Persists to: BigQuery (partitioned tables)
- Requires: GCP credentials, BigQuery API access
- Monitors: Airflow for orchestration, Datadog for metrics