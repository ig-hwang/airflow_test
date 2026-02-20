-- Create stock_symbols table for centralized symbol management
-- This allows adding new stocks without modifying Python code

CREATE TABLE IF NOT EXISTS stock_symbols (
    id           SERIAL PRIMARY KEY,
    symbol       VARCHAR(20)    NOT NULL UNIQUE,
    name         VARCHAR(200),
    category     VARCHAR(50)    NOT NULL,  -- 'US', 'KR', 'ADR'
    sector       VARCHAR(100),
    google_query VARCHAR(300),  -- Google News RSS search query
    active       BOOLEAN        DEFAULT TRUE,
    added_date   DATE           DEFAULT CURRENT_DATE,
    created_at   TIMESTAMP      DEFAULT NOW(),
    updated_at   TIMESTAMP      DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stock_symbols_category ON stock_symbols (category);
CREATE INDEX IF NOT EXISTS idx_stock_symbols_active ON stock_symbols (active);

-- Insert current symbols from common.py
INSERT INTO stock_symbols (symbol, name, category, sector, google_query) VALUES
-- US 기존 테마주 (에너지/반도체/인프라)
('AVGO', 'Broadcom Inc.', 'US', 'Technology', 'Broadcom AVGO stock'),
('BE', 'Bloom Energy', 'US', 'Energy', 'Bloom Energy stock'),
('VRT', 'Vertiv Holdings', 'US', 'Technology', 'Vertiv Holdings stock'),
('SMR', 'NuScale Power', 'US', 'Energy', 'NuScale Power SMR stock'),
('OKLO', 'Oklo Inc.', 'US', 'Energy', 'Oklo nuclear stock'),
('GEV', 'GE Vernova', 'US', 'Energy', 'GE Vernova stock'),
('MRVL', 'Marvell Technology', 'US', 'Technology', 'Marvell Technology stock'),
('COHR', 'Coherent Corp.', 'US', 'Technology', 'Coherent Corp stock'),
('LITE', 'Lumentum Holdings', 'US', 'Technology', 'Lumentum Holdings stock'),
('VST', 'Vistra Corp.', 'US', 'Energy', 'Vistra Energy stock'),
('ETN', 'Eaton Corporation', 'US', 'Industrials', 'Eaton Corporation stock'),
-- 빅테크
('AAPL', 'Apple Inc.', 'US', 'Technology', 'Apple AAPL stock'),
('MSFT', 'Microsoft Corporation', 'US', 'Technology', 'Microsoft MSFT stock'),
('AMZN', 'Amazon.com Inc.', 'US', 'Consumer Cyclical', 'Amazon AMZN stock'),
('NVDA', 'NVIDIA Corporation', 'US', 'Technology', 'NVIDIA NVDA stock'),
('META', 'Meta Platforms Inc.', 'US', 'Technology', 'Meta Platforms stock'),
('GOOGL', 'Alphabet Inc.', 'US', 'Technology', 'Google Alphabet stock'),
-- 금융/헬스케어/소비재
('BRK-B', 'Berkshire Hathaway Inc.', 'US', 'Financial', 'Berkshire Hathaway stock'),
('JPM', 'JPMorgan Chase & Co.', 'US', 'Financial', 'JPMorgan Chase stock'),
('UNH', 'UnitedHealth Group', 'US', 'Healthcare', 'UnitedHealth Group stock'),
('JNJ', 'Johnson & Johnson', 'US', 'Healthcare', 'Johnson & Johnson stock'),
('LLY', 'Eli Lilly and Company', 'US', 'Healthcare', 'Eli Lilly stock'),
('PFE', 'Pfizer Inc.', 'US', 'Healthcare', 'Pfizer stock'),
('MRK', 'Merck & Co. Inc.', 'US', 'Healthcare', 'Merck stock'),
('ABBV', 'AbbVie Inc.', 'US', 'Healthcare', 'AbbVie stock'),
('AMGN', 'Amgen Inc.', 'US', 'Healthcare', 'Amgen stock'),
('ISRG', 'Intuitive Surgical', 'US', 'Healthcare', 'Intuitive Surgical stock'),
('PEP', 'PepsiCo Inc.', 'US', 'Consumer Defensive', 'PepsiCo stock'),
('KO', 'The Coca-Cola Company', 'US', 'Consumer Defensive', 'Coca-Cola stock'),
('VZ', 'Verizon Communications', 'US', 'Communication', 'Verizon stock'),
('CSCO', 'Cisco Systems Inc.', 'US', 'Technology', 'Cisco stock'),
-- 반도체/소재
('AMD', 'Advanced Micro Devices', 'US', 'Technology', 'AMD stock'),
('MU', 'Micron Technology', 'US', 'Technology', 'Micron Technology stock'),
('AMAT', 'Applied Materials', 'US', 'Technology', 'Applied Materials stock'),
('MP', 'MP Materials Corp.', 'US', 'Materials', 'MP Materials stock'),
-- 글로벌 (US 거래소 상장)
('TSM', 'Taiwan Semiconductor', 'US', 'Technology', 'TSMC Taiwan Semiconductor stock'),
('ASML', 'ASML Holding N.V.', 'US', 'Technology', 'ASML stock'),
('ABBNY', 'ABB Ltd', 'US', 'Industrials', 'ABB stock'),
-- NEW: Uber
('UBER', 'Uber Technologies Inc.', 'US', 'Technology', 'Uber UBER stock'),
-- Korean stocks
('267260.KS', '현대중공업지주', 'KR', 'Industrials', 'HD현대일렉트릭 주가'),
('034020.KS', '두산에너빌리티', 'KR', 'Energy', '두산에너빌리티 주가'),
('028260.KS', '삼성물산', 'KR', 'Industrials', '삼성물산 주가'),
('267270.KS', '현대건설기계', 'KR', 'Industrials', 'HD현대중공업 주가'),
('010120.KS', 'LS ELECTRIC', 'KR', 'Industrials', 'LS ELECTRIC 주가'),
-- ADR symbols
('SBGSY', 'Schneider Electric SE', 'ADR', 'Industrials', 'Schneider Electric stock'),
('HTHIY', 'Hitachi Ltd.', 'ADR', 'Industrials', 'Hitachi stock'),
('FANUY', 'FANUC Corporation', 'ADR', 'Industrials', 'Fanuc stock'),
('KYOCY', 'Keyence Corporation', 'ADR', 'Technology', 'Keyence stock'),
('SMCAY', 'SMC Corporation', 'ADR', 'Industrials', 'SMC Corporation stock')
ON CONFLICT (symbol) DO NOTHING;

\echo '=== Stock symbols table created and populated ==='
SELECT category, COUNT(*) as count FROM stock_symbols WHERE active = TRUE GROUP BY category ORDER BY category;
