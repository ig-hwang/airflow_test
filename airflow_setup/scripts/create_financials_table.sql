-- Create comprehensive financial statements table
-- Stores quarterly and annual financial data for fundamental analysis

CREATE TABLE IF NOT EXISTS stock_financials (
    id                      SERIAL PRIMARY KEY,
    symbol                  VARCHAR(20)    NOT NULL,
    report_date             DATE           NOT NULL,
    period_type             VARCHAR(10)    NOT NULL,  -- 'quarterly' or 'annual'
    fiscal_year             INTEGER,
    fiscal_quarter          INTEGER,

    -- Income Statement
    revenue                 BIGINT,
    cost_of_revenue         BIGINT,
    gross_profit            BIGINT,
    operating_expense       BIGINT,
    operating_income        BIGINT,
    ebitda                  BIGINT,
    net_income              BIGINT,
    eps_basic               NUMERIC(12, 4),
    eps_diluted             NUMERIC(12, 4),

    -- Balance Sheet
    total_assets            BIGINT,
    current_assets          BIGINT,
    cash_and_equivalents    BIGINT,
    total_liabilities       BIGINT,
    current_liabilities     BIGINT,
    total_debt              BIGINT,
    total_equity            BIGINT,

    -- Cash Flow Statement
    operating_cash_flow     BIGINT,
    investing_cash_flow     BIGINT,
    financing_cash_flow     BIGINT,
    capital_expenditure     BIGINT,
    free_cash_flow          BIGINT,

    -- Key Metrics (calculated)
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
);

CREATE INDEX IF NOT EXISTS idx_financials_symbol ON stock_financials (symbol);
CREATE INDEX IF NOT EXISTS idx_financials_date ON stock_financials (report_date DESC);
CREATE INDEX IF NOT EXISTS idx_financials_symbol_date ON stock_financials (symbol, report_date DESC);

\echo '=== stock_financials table created ==='
