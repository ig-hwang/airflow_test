# Stock Symbol Management

## Overview

Stock symbols are now managed in a centralized `stock_symbols` database table instead of being hardcoded in Python files. This makes it easy to add/remove stocks without modifying code.

## Database Schema

```sql
CREATE TABLE stock_symbols (
    id           SERIAL PRIMARY KEY,
    symbol       VARCHAR(20)    NOT NULL UNIQUE,
    name         VARCHAR(200),
    category     VARCHAR(50)    NOT NULL,  -- 'US', 'KR', 'ADR'
    sector       VARCHAR(100),
    google_query VARCHAR(300),             -- Google News search query
    active       BOOLEAN        DEFAULT TRUE,
    added_date   DATE           DEFAULT CURRENT_DATE,
    created_at   TIMESTAMP      DEFAULT NOW(),
    updated_at   TIMESTAMP      DEFAULT NOW()
);
```

## Adding New Symbols

### Method 1: CLI Tool (Recommended)

```bash
cd airflow_setup
python3 scripts/manage_symbols.py add SYMBOL "Company Name" CATEGORY SECTOR

# Example: Add Tesla
python3 scripts/manage_symbols.py add TSLA "Tesla Inc." US "Consumer Cyclical"

# List all symbols
python3 scripts/manage_symbols.py list

# Disable a symbol (soft delete)
python3 scripts/manage_symbols.py disable TSLA

# Re-enable a symbol
python3 scripts/manage_symbols.py enable TSLA
```

### Method 2: Direct SQL (Advanced)

```sql
INSERT INTO stock_symbols (symbol, name, category, sector, google_query)
VALUES ('TSLA', 'Tesla Inc.', 'US', 'Consumer Cyclical', 'Tesla TSLA stock');
```

## Category Types

- **US**: US-listed stocks (NYSE, NASDAQ)
- **KR**: Korean stocks (KRX)
- **ADR**: American Depositary Receipts

## How It Works

1. **DAGs** (`stock_price_dag.py`, `news_dag.py`) read symbols from `common.py`
2. **common.py** loads symbols from the database via `get_symbols()` function
3. **Backfill scripts** load symbols from database on startup
4. **Streamlit app** (`db.py`) loads symbols from database with 5-minute cache

All components automatically pick up new symbols without code changes!

## Backfilling Data for New Symbols

After adding a new symbol, you need to backfill historical data:

```bash
cd airflow_setup
python3 scripts/backfill_missing_data.py    # Backfill price data
python3 scripts/backfill_news_digest.py     # Backfill news data
```

Or wait for the cron jobs to run:
- **Price data**: Mon-Fri 23:00 UTC (08:00 KST)
- **News data**: Daily 07:00 UTC (16:00 KST)

## Migration History

**2026-02-20**: Migrated from hardcoded lists to database table
- Added `stock_symbols` table
- Updated `common.py`, backfill scripts, and Streamlit app
- Added Uber (UBER) as first new symbol via database
- Created `manage_symbols.py` CLI tool
