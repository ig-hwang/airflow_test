-- AlphaBoard: Create tables in Neon
-- Run: /opt/homebrew/Cellar/libpq/18.1_1/bin/psql "NEON_URL" -f /tmp/neon_create_tables.sql

CREATE TABLE IF NOT EXISTS stock_prices (
    id              SERIAL PRIMARY KEY,
    symbol          VARCHAR(20)    NOT NULL,
    trade_date      DATE           NOT NULL,
    open            NUMERIC(12, 4),
    high            NUMERIC(12, 4),
    low             NUMERIC(12, 4),
    close           NUMERIC(12, 4),
    volume          BIGINT,
    sma_20          NUMERIC(12, 4),
    sma_50          NUMERIC(12, 4),
    sma_200         NUMERIC(12, 4),
    bb_upper        NUMERIC(12, 4),
    bb_middle       NUMERIC(12, 4),
    bb_lower        NUMERIC(12, 4),
    rsi_14          NUMERIC(8, 4),
    macd            NUMERIC(12, 6),
    macd_signal     NUMERIC(12, 6),
    macd_hist       NUMERIC(12, 6),
    cci_20          NUMERIC(12, 4),
    atr_14          NUMERIC(12, 4),
    obv             BIGINT,
    mfi_14          NUMERIC(8, 4),
    created_at      TIMESTAMP      DEFAULT NOW(),
    updated_at      TIMESTAMP      DEFAULT NOW(),
    UNIQUE (symbol, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol      ON stock_prices (symbol);
CREATE INDEX IF NOT EXISTS idx_stock_prices_trade_date  ON stock_prices (trade_date);
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_date ON stock_prices (symbol, trade_date DESC);

CREATE TABLE IF NOT EXISTS stock_fundamentals (
    id              SERIAL PRIMARY KEY,
    symbol          VARCHAR(20)    NOT NULL,
    fetch_date      DATE           NOT NULL,
    market_cap      BIGINT,
    pe_ratio        NUMERIC(12, 4),
    pb_ratio        NUMERIC(12, 4),
    roe             NUMERIC(10, 4),
    eps             NUMERIC(12, 4),
    dividend_yield  NUMERIC(10, 6),
    sector          VARCHAR(100),
    industry        VARCHAR(200),
    created_at      TIMESTAMP      DEFAULT NOW(),
    updated_at      TIMESTAMP      DEFAULT NOW(),
    UNIQUE (symbol, fetch_date)
);
CREATE INDEX IF NOT EXISTS idx_fundamentals_symbol ON stock_fundamentals (symbol);

CREATE TABLE IF NOT EXISTS stock_news (
    id          SERIAL PRIMARY KEY,
    symbol      VARCHAR(20)    NOT NULL,
    title       TEXT           NOT NULL,
    url         TEXT           NOT NULL,
    source      VARCHAR(200),
    published   TIMESTAMP,
    summary     TEXT,
    ai_summary  TEXT,
    sentiment   VARCHAR(10),
    created_at  TIMESTAMP      DEFAULT NOW(),
    UNIQUE (symbol, url)
);
CREATE INDEX IF NOT EXISTS idx_news_symbol    ON stock_news (symbol);
CREATE INDEX IF NOT EXISTS idx_news_published ON stock_news (published DESC);

CREATE TABLE IF NOT EXISTS chart_generation_log (
    id          SERIAL PRIMARY KEY,
    symbol      VARCHAR(20)    NOT NULL,
    chart_date  DATE           NOT NULL,
    file_path   TEXT           NOT NULL,
    status      VARCHAR(20)    DEFAULT 'success',
    error_msg   TEXT,
    created_at  TIMESTAMP      DEFAULT NOW(),
    UNIQUE (symbol, chart_date)
);

CREATE TABLE IF NOT EXISTS weekly_digest (
    id          SERIAL PRIMARY KEY,
    week_start  DATE           NOT NULL UNIQUE,
    week_end    DATE           NOT NULL,
    headline    VARCHAR(300),
    content     TEXT           NOT NULL,
    created_at  TIMESTAMP      DEFAULT NOW(),
    updated_at  TIMESTAMP      DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_digest_week_start ON weekly_digest (week_start DESC);

\echo '=== Tables created. Checking... ==='
SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;
