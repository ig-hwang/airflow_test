#!/usr/bin/env python3
"""
Fetch comprehensive financial statement data from yfinance.
Stores quarterly and annual data for fundamental analysis.
"""

import json
import logging
import ssl
import urllib.request
import urllib.error
from datetime import datetime, date

import pandas as pd
import yfinance as yf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Neon HTTP API ──
_NEON_HOST = "ep-small-firefly-ai94k736-pooler.c-4.us-east-1.aws.neon.tech"
_NEON_CONN = f"postgresql://neondb_owner:npg_TiDj4zKScRt7@{_NEON_HOST}/neondb"
_NEON_URL = f"https://{_NEON_HOST}/sql"
_SSL = ssl.create_default_context()
_SSL.check_hostname = False
_SSL.verify_mode = ssl.CERT_NONE


def neon(sql: str, params=None) -> dict:
    """Execute SQL via Neon HTTP API."""
    payload = json.dumps({"query": sql, **({"params": params} if params else {})}).encode()
    req = urllib.request.Request(
        _NEON_URL, data=payload,
        headers={"Content-Type": "application/json", "Neon-Connection-String": _NEON_CONN},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, context=_SSL, timeout=60) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        raise RuntimeError(f"Neon HTTP {e.code}: {body[:400]}") from e


def safe_int(val) -> int | None:
    """Safely convert to integer, handling NaN and None."""
    if pd.isna(val) or val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def safe_float(val) -> float | None:
    """Safely convert to float, handling NaN and None."""
    if pd.isna(val) or val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def fetch_financials_for_symbol(symbol: str):
    """Fetch and store financial data for a symbol."""
    log.info(f"Fetching financials for {symbol}...")

    try:
        ticker = yf.Ticker(symbol)

        # Fetch quarterly and annual data
        quarterly_income = ticker.quarterly_financials
        annual_income = ticker.financials
        quarterly_balance = ticker.quarterly_balance_sheet
        annual_balance = ticker.balance_sheet
        quarterly_cashflow = ticker.quarterly_cashflow
        annual_cashflow = ticker.cashflow

        records = []

        # Process quarterly data
        if quarterly_income is not None and not quarterly_income.empty:
            for col in quarterly_income.columns[:8]:  # Last 8 quarters
                record = extract_financial_record(
                    symbol, col, 'quarterly',
                    quarterly_income, quarterly_balance, quarterly_cashflow
                )
                if record:
                    records.append(record)

        # Process annual data
        if annual_income is not None and not annual_income.empty:
            for col in annual_income.columns[:5]:  # Last 5 years
                record = extract_financial_record(
                    symbol, col, 'annual',
                    annual_income, annual_balance, annual_cashflow
                )
                if record:
                    records.append(record)

        if records:
            insert_records(records)
            log.info(f"  ✓ Inserted {len(records)} financial records for {symbol}")
        else:
            log.warning(f"  ✗ No financial data available for {symbol}")

    except Exception as e:
        log.error(f"  ✗ Failed to fetch financials for {symbol}: {e}")


def extract_financial_record(symbol, date_col, period_type, income_df, balance_df, cashflow_df):
    """Extract financial data from yfinance dataframes."""
    try:
        report_date = pd.to_datetime(date_col).date()

        # Income Statement
        revenue = safe_int(income_df.loc['Total Revenue', date_col] if 'Total Revenue' in income_df.index else None)
        cost_of_revenue = safe_int(income_df.loc['Cost Of Revenue', date_col] if 'Cost Of Revenue' in income_df.index else None)
        gross_profit = safe_int(income_df.loc['Gross Profit', date_col] if 'Gross Profit' in income_df.index else None)
        operating_expense = safe_int(income_df.loc['Operating Expense', date_col] if 'Operating Expense' in income_df.index else None)
        operating_income = safe_int(income_df.loc['Operating Income', date_col] if 'Operating Income' in income_df.index else None)
        ebitda = safe_int(income_df.loc['EBITDA', date_col] if 'EBITDA' in income_df.index else None)
        net_income = safe_int(income_df.loc['Net Income', date_col] if 'Net Income' in income_df.index else None)

        # Balance Sheet
        total_assets = safe_int(balance_df.loc['Total Assets', date_col] if balance_df is not None and 'Total Assets' in balance_df.index else None)
        current_assets = safe_int(balance_df.loc['Current Assets', date_col] if balance_df is not None and 'Current Assets' in balance_df.index else None)
        cash = safe_int(balance_df.loc['Cash And Cash Equivalents', date_col] if balance_df is not None and 'Cash And Cash Equivalents' in balance_df.index else None)
        total_liabilities = safe_int(balance_df.loc['Total Liabilities Net Minority Interest', date_col] if balance_df is not None and 'Total Liabilities Net Minority Interest' in balance_df.index else None)
        current_liabilities = safe_int(balance_df.loc['Current Liabilities', date_col] if balance_df is not None and 'Current Liabilities' in balance_df.index else None)
        total_debt = safe_int(balance_df.loc['Total Debt', date_col] if balance_df is not None and 'Total Debt' in balance_df.index else None)
        total_equity = safe_int(balance_df.loc['Total Equity Gross Minority Interest', date_col] if balance_df is not None and 'Total Equity Gross Minority Interest' in balance_df.index else None)

        # Cash Flow Statement
        operating_cf = safe_int(cashflow_df.loc['Operating Cash Flow', date_col] if cashflow_df is not None and 'Operating Cash Flow' in cashflow_df.index else None)
        investing_cf = safe_int(cashflow_df.loc['Investing Cash Flow', date_col] if cashflow_df is not None and 'Investing Cash Flow' in cashflow_df.index else None)
        financing_cf = safe_int(cashflow_df.loc['Financing Cash Flow', date_col] if cashflow_df is not None and 'Financing Cash Flow' in cashflow_df.index else None)
        capex = safe_int(cashflow_df.loc['Capital Expenditure', date_col] if cashflow_df is not None and 'Capital Expenditure' in cashflow_df.index else None)

        # Calculate metrics
        gross_margin = safe_float(gross_profit / revenue * 100) if revenue and gross_profit else None
        operating_margin = safe_float(operating_income / revenue * 100) if revenue and operating_income else None
        net_margin = safe_float(net_income / revenue * 100) if revenue and net_income else None
        roe = safe_float(net_income / total_equity * 100) if total_equity and net_income else None
        roa = safe_float(net_income / total_assets * 100) if total_assets and net_income else None
        current_ratio = safe_float(current_assets / current_liabilities) if current_assets and current_liabilities else None
        debt_to_equity = safe_float(total_debt / total_equity) if total_debt and total_equity else None
        free_cash_flow = safe_int(operating_cf + capex) if operating_cf and capex else None

        return {
            'symbol': symbol,
            'report_date': report_date,
            'period_type': period_type,
            'revenue': revenue,
            'cost_of_revenue': cost_of_revenue,
            'gross_profit': gross_profit,
            'operating_expense': operating_expense,
            'operating_income': operating_income,
            'ebitda': ebitda,
            'net_income': net_income,
            'total_assets': total_assets,
            'current_assets': current_assets,
            'cash_and_equivalents': cash,
            'total_liabilities': total_liabilities,
            'current_liabilities': current_liabilities,
            'total_debt': total_debt,
            'total_equity': total_equity,
            'operating_cash_flow': operating_cf,
            'investing_cash_flow': investing_cf,
            'financing_cash_flow': financing_cf,
            'capital_expenditure': capex,
            'free_cash_flow': free_cash_flow,
            'gross_margin': gross_margin,
            'operating_margin': operating_margin,
            'net_margin': net_margin,
            'roe': roe,
            'roa': roa,
            'current_ratio': current_ratio,
            'debt_to_equity': debt_to_equity,
        }
    except Exception as e:
        log.warning(f"Failed to extract record for {symbol} at {date_col}: {e}")
        return None


def insert_records(records):
    """Batch insert financial records."""
    if not records:
        return

    fields = [
        'symbol', 'report_date', 'period_type', 'revenue', 'cost_of_revenue',
        'gross_profit', 'operating_expense', 'operating_income', 'ebitda',
        'net_income', 'total_assets', 'current_assets', 'cash_and_equivalents',
        'total_liabilities', 'current_liabilities', 'total_debt', 'total_equity',
        'operating_cash_flow', 'investing_cash_flow', 'financing_cash_flow',
        'capital_expenditure', 'free_cash_flow', 'gross_margin', 'operating_margin',
        'net_margin', 'roe', 'roa', 'current_ratio', 'debt_to_equity'
    ]

    # Batch insert (5 records at a time)
    for i in range(0, len(records), 5):
        batch = records[i:i+5]
        placeholders = []
        params = []

        for idx, rec in enumerate(batch):
            base = idx * len(fields)
            ph = ', '.join(f'${base + j + 1}' for j in range(len(fields)))
            placeholders.append(f'({ph})')
            # Convert date objects to strings for JSON serialization
            for f in fields:
                val = rec.get(f)
                if hasattr(val, 'isoformat'):  # date or datetime object
                    val = val.isoformat()
                params.append(val)

        sql = f"""
            INSERT INTO stock_financials ({', '.join(fields)})
            VALUES {', '.join(placeholders)}
            ON CONFLICT (symbol, report_date, period_type)
            DO UPDATE SET
                revenue = EXCLUDED.revenue,
                net_income = EXCLUDED.net_income,
                total_assets = EXCLUDED.total_assets,
                operating_cash_flow = EXCLUDED.operating_cash_flow,
                updated_at = NOW()
        """
        neon(sql, params)


def main():
    """Fetch financials for all active symbols."""
    # Create table first
    log.info("Creating stock_financials table...")
    try:
        # Execute CREATE TABLE
        neon("""
            CREATE TABLE IF NOT EXISTS stock_financials (
                id                      SERIAL PRIMARY KEY,
                symbol                  VARCHAR(20)    NOT NULL,
                report_date             DATE           NOT NULL,
                period_type             VARCHAR(10)    NOT NULL,
                fiscal_year             INTEGER,
                fiscal_quarter          INTEGER,
                revenue                 BIGINT,
                cost_of_revenue         BIGINT,
                gross_profit            BIGINT,
                operating_expense       BIGINT,
                operating_income        BIGINT,
                ebitda                  BIGINT,
                net_income              BIGINT,
                eps_basic               NUMERIC(12, 4),
                eps_diluted             NUMERIC(12, 4),
                total_assets            BIGINT,
                current_assets          BIGINT,
                cash_and_equivalents    BIGINT,
                total_liabilities       BIGINT,
                current_liabilities     BIGINT,
                total_debt              BIGINT,
                total_equity            BIGINT,
                operating_cash_flow     BIGINT,
                investing_cash_flow     BIGINT,
                financing_cash_flow     BIGINT,
                capital_expenditure     BIGINT,
                free_cash_flow          BIGINT,
                gross_margin            NUMERIC(10, 4),
                operating_margin        NUMERIC(10, 4),
                net_margin              NUMERIC(10, 4),
                roe                     NUMERIC(10, 4),
                roa                     NUMERIC(10, 4),
                current_ratio           NUMERIC(10, 4),
                debt_to_equity          NUMERIC(10, 4),
                created_at              TIMESTAMP      DEFAULT NOW(),
                updated_at              TIMESTAMP      DEFAULT NOW(),
                UNIQUE (symbol, report_date, period_type)
            )
        """)
        neon("CREATE INDEX IF NOT EXISTS idx_financials_symbol ON stock_financials (symbol)")
        neon("CREATE INDEX IF NOT EXISTS idx_financials_date ON stock_financials (report_date DESC)")
        neon("CREATE INDEX IF NOT EXISTS idx_financials_symbol_date ON stock_financials (symbol, report_date DESC)")
        log.info("✓ Table created successfully")
    except Exception as e:
        log.warning(f"Table creation warning (may already exist): {e}")

    # Load symbols
    result = neon("SELECT symbol FROM stock_symbols WHERE active = TRUE ORDER BY symbol")
    symbols = [r['symbol'] for r in result.get('rows', [])]
    log.info(f"Fetching financials for {len(symbols)} symbols...")

    for i, symbol in enumerate(symbols, 1):
        log.info(f"[{i}/{len(symbols)}] {symbol}")
        fetch_financials_for_symbol(symbol)

    log.info("✓ Financial data fetch complete!")


if __name__ == "__main__":
    main()
