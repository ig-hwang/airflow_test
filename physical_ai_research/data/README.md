# Data Directory Structure

## Purpose
Storage for Physical AI research data at different processing stages.

## Directory Layout

```
data/
├── raw/              # Raw data from physical-ai-scout
├── processed/        # Enriched data from strategic-analysis
└── archive/          # Historical/backup data
```

## Usage Guidelines

### raw/
- **Purpose**: Raw signals collected from primary sources (SEC, arXiv, news)
- **Naming**: `{YYYYMMDD}_{Source}.json` (e.g., `20240215_SEC.json`)
- **Schema**: PASIS raw output with minimal processing
- **Retention**: Keep for audit trail and re-analysis

### processed/
- **Purpose**: Analyzed data with strategic implications
- **Naming**: `{YYYYMMDD}_{AnalysisType}.json` (e.g., `20240215_Investment.json`)
- **Schema**: Enhanced PASIS output with strategic_implication field
- **Retention**: Keep until archived to PostgreSQL

### archive/
- **Purpose**: Long-term storage and backups
- **Naming**: `{YYYYMM}_archive.json.gz` (monthly archives)
- **Format**: Compressed JSON
- **Retention**: Indefinite (reference and compliance)

## Data Flow

```
physical-ai-scout → data/raw/ → strategic-analysis → data/processed/ → data-archivist → PostgreSQL
                                                           ↓
                                                    data/archive/
```

## Best Practices
- Always include source_metadata in all JSON files
- Use ISO-8601 timestamps (UTC)
- Validate against PASIS schema before writing
- Compress archives older than 30 days