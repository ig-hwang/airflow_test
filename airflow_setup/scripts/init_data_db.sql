-- Create stock_data database and schema
-- This script runs automatically on first PostgreSQL container start

CREATE DATABASE stock_data;

\connect stock_data;

CREATE TABLE stock_prices (
    id              SERIAL PRIMARY KEY,
    symbol          VARCHAR(20)    NOT NULL,
    trade_date      DATE           NOT NULL,
    open            NUMERIC(12, 4),
    high            NUMERIC(12, 4),
    low             NUMERIC(12, 4),
    close           NUMERIC(12, 4),
    volume          BIGINT,
    -- Moving Averages
    sma_20          NUMERIC(12, 4),
    sma_50          NUMERIC(12, 4),
    sma_200         NUMERIC(12, 4),
    -- Bollinger Bands
    bb_upper        NUMERIC(12, 4),
    bb_middle       NUMERIC(12, 4),
    bb_lower        NUMERIC(12, 4),
    -- RSI
    rsi_14          NUMERIC(8, 4),
    -- MACD
    macd            NUMERIC(12, 6),
    macd_signal     NUMERIC(12, 6),
    macd_hist       NUMERIC(12, 6),
    -- CCI
    cci_20          NUMERIC(12, 4),
    -- ATR
    atr_14          NUMERIC(12, 4),
    -- OBV
    obv             BIGINT,
    -- MFI
    mfi_14          NUMERIC(8, 4),
    -- Metadata
    created_at      TIMESTAMP      DEFAULT NOW(),
    updated_at      TIMESTAMP      DEFAULT NOW(),
    UNIQUE (symbol, trade_date)
);

CREATE INDEX idx_stock_prices_symbol      ON stock_prices (symbol);
CREATE INDEX idx_stock_prices_trade_date  ON stock_prices (trade_date);
CREATE INDEX idx_stock_prices_symbol_date ON stock_prices (symbol, trade_date DESC);

CREATE TABLE stock_fundamentals (
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

CREATE INDEX idx_fundamentals_symbol ON stock_fundamentals (symbol);

CREATE TABLE stock_news (
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

CREATE INDEX idx_news_symbol    ON stock_news (symbol);
CREATE INDEX idx_news_published ON stock_news (published DESC);

CREATE TABLE chart_generation_log (
    id          SERIAL PRIMARY KEY,
    symbol      VARCHAR(20)    NOT NULL,
    chart_date  DATE           NOT NULL,
    file_path   TEXT           NOT NULL,
    status      VARCHAR(20)    DEFAULT 'success',
    error_msg   TEXT,
    created_at  TIMESTAMP      DEFAULT NOW(),
    UNIQUE (symbol, chart_date)
);

CREATE TABLE weekly_digest (
    id          SERIAL PRIMARY KEY,
    week_start  DATE           NOT NULL UNIQUE,
    week_end    DATE           NOT NULL,
    headline    VARCHAR(300),
    content     TEXT           NOT NULL,
    created_at  TIMESTAMP      DEFAULT NOW(),
    updated_at  TIMESTAMP      DEFAULT NOW()
);

CREATE INDEX idx_digest_week_start ON weekly_digest (week_start DESC);
