#!/usr/bin/env python3
"""
Setup stock_symbols table in Neon DB.
Creates the table and populates it with all current symbols + Uber.
"""

import json
import logging
import os
import ssl
import urllib.request
import urllib.error

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


def main():
    log.info("=== Creating stock_symbols table ===")

    # Create table
    neon("""
        CREATE TABLE IF NOT EXISTS stock_symbols (
            id           SERIAL PRIMARY KEY,
            symbol       VARCHAR(20)    NOT NULL UNIQUE,
            name         VARCHAR(200),
            category     VARCHAR(50)    NOT NULL,
            sector       VARCHAR(100),
            google_query VARCHAR(300),
            active       BOOLEAN        DEFAULT TRUE,
            added_date   DATE           DEFAULT CURRENT_DATE,
            created_at   TIMESTAMP      DEFAULT NOW(),
            updated_at   TIMESTAMP      DEFAULT NOW()
        )
    """)
    log.info("Table created")

    # Create indexes
    neon("CREATE INDEX IF NOT EXISTS idx_stock_symbols_category ON stock_symbols (category)")
    neon("CREATE INDEX IF NOT EXISTS idx_stock_symbols_active ON stock_symbols (active)")
    log.info("Indexes created")

    # Insert symbols
    symbols_data = [
        # (symbol, name, category, sector, google_query)
        ("AVGO", "Broadcom Inc.", "US", "Technology", "Broadcom AVGO stock"),
        ("BE", "Bloom Energy", "US", "Energy", "Bloom Energy stock"),
        ("VRT", "Vertiv Holdings", "US", "Technology", "Vertiv Holdings stock"),
        ("SMR", "NuScale Power", "US", "Energy", "NuScale Power SMR stock"),
        ("OKLO", "Oklo Inc.", "US", "Energy", "Oklo nuclear stock"),
        ("GEV", "GE Vernova", "US", "Energy", "GE Vernova stock"),
        ("MRVL", "Marvell Technology", "US", "Technology", "Marvell Technology stock"),
        ("COHR", "Coherent Corp.", "US", "Technology", "Coherent Corp stock"),
        ("LITE", "Lumentum Holdings", "US", "Technology", "Lumentum Holdings stock"),
        ("VST", "Vistra Corp.", "US", "Energy", "Vistra Energy stock"),
        ("ETN", "Eaton Corporation", "US", "Industrials", "Eaton Corporation stock"),
        ("AAPL", "Apple Inc.", "US", "Technology", "Apple AAPL stock"),
        ("MSFT", "Microsoft Corporation", "US", "Technology", "Microsoft MSFT stock"),
        ("AMZN", "Amazon.com Inc.", "US", "Consumer Cyclical", "Amazon AMZN stock"),
        ("NVDA", "NVIDIA Corporation", "US", "Technology", "NVIDIA NVDA stock"),
        ("META", "Meta Platforms Inc.", "US", "Technology", "Meta Platforms stock"),
        ("GOOGL", "Alphabet Inc.", "US", "Technology", "Google Alphabet stock"),
        ("BRK-B", "Berkshire Hathaway Inc.", "US", "Financial", "Berkshire Hathaway stock"),
        ("JPM", "JPMorgan Chase & Co.", "US", "Financial", "JPMorgan Chase stock"),
        ("UNH", "UnitedHealth Group", "US", "Healthcare", "UnitedHealth Group stock"),
        ("JNJ", "Johnson & Johnson", "US", "Healthcare", "Johnson & Johnson stock"),
        ("LLY", "Eli Lilly and Company", "US", "Healthcare", "Eli Lilly stock"),
        ("PFE", "Pfizer Inc.", "US", "Healthcare", "Pfizer stock"),
        ("MRK", "Merck & Co. Inc.", "US", "Healthcare", "Merck stock"),
        ("ABBV", "AbbVie Inc.", "US", "Healthcare", "AbbVie stock"),
        ("AMGN", "Amgen Inc.", "US", "Healthcare", "Amgen stock"),
        ("ISRG", "Intuitive Surgical", "US", "Healthcare", "Intuitive Surgical stock"),
        ("PEP", "PepsiCo Inc.", "US", "Consumer Defensive", "PepsiCo stock"),
        ("KO", "The Coca-Cola Company", "US", "Consumer Defensive", "Coca-Cola stock"),
        ("VZ", "Verizon Communications", "US", "Communication", "Verizon stock"),
        ("CSCO", "Cisco Systems Inc.", "US", "Technology", "Cisco stock"),
        ("AMD", "Advanced Micro Devices", "US", "Technology", "AMD stock"),
        ("MU", "Micron Technology", "US", "Technology", "Micron Technology stock"),
        ("AMAT", "Applied Materials", "US", "Technology", "Applied Materials stock"),
        ("MP", "MP Materials Corp.", "US", "Materials", "MP Materials stock"),
        ("TSM", "Taiwan Semiconductor", "US", "Technology", "TSMC Taiwan Semiconductor stock"),
        ("ASML", "ASML Holding N.V.", "US", "Technology", "ASML stock"),
        ("ABBNY", "ABB Ltd", "US", "Industrials", "ABB stock"),
        ("UBER", "Uber Technologies Inc.", "US", "Technology", "Uber UBER stock"),
        ("267260.KS", "현대중공업지주", "KR", "Industrials", "HD현대일렉트릭 주가"),
        ("034020.KS", "두산에너빌리티", "KR", "Energy", "두산에너빌리티 주가"),
        ("028260.KS", "삼성물산", "KR", "Industrials", "삼성물산 주가"),
        ("267270.KS", "현대건설기계", "KR", "Industrials", "HD현대중공업 주가"),
        ("010120.KS", "LS ELECTRIC", "KR", "Industrials", "LS ELECTRIC 주가"),
        ("SBGSY", "Schneider Electric SE", "ADR", "Industrials", "Schneider Electric stock"),
        ("HTHIY", "Hitachi Ltd.", "ADR", "Industrials", "Hitachi stock"),
        ("FANUY", "FANUC Corporation", "ADR", "Industrials", "Fanuc stock"),
        ("KYOCY", "Keyence Corporation", "ADR", "Technology", "Keyence stock"),
        ("SMCAY", "SMC Corporation", "ADR", "Industrials", "SMC Corporation stock"),
    ]

    log.info(f"Inserting {len(symbols_data)} symbols...")

    # Insert in batches of 10
    for i in range(0, len(symbols_data), 10):
        batch = symbols_data[i:i+10]
        placeholders = []
        params = []
        for idx, (symbol, name, category, sector, google_query) in enumerate(batch):
            base = idx * 5
            placeholders.append(f"(${base+1}, ${base+2}, ${base+3}, ${base+4}, ${base+5})")
            params.extend([symbol, name, category, sector, google_query])

        sql = f"""
            INSERT INTO stock_symbols (symbol, name, category, sector, google_query)
            VALUES {', '.join(placeholders)}
            ON CONFLICT (symbol) DO NOTHING
        """
        neon(sql, params)
        log.info(f"  Inserted batch {i//10 + 1}/{(len(symbols_data) + 9)//10}")

    # Verify
    result = neon("SELECT category, COUNT(*) as count FROM stock_symbols WHERE active = TRUE GROUP BY category ORDER BY category")
    log.info("=== Stock symbols table ready ===")
    for row in result.get("results", [{}])[0].get("rows", []):
        log.info(f"  {row['category']}: {row['count']} symbols")

    total = neon("SELECT COUNT(*) as total FROM stock_symbols WHERE active = TRUE")
    total_count = total.get("results", [{}])[0].get("rows", [{}])[0].get("total", 0)
    log.info(f"Total active symbols: {total_count}")


if __name__ == "__main__":
    main()
