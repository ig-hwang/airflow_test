#!/usr/bin/env python3
"""
Stock symbol management CLI.
Makes it easy to add/remove symbols without touching code.

Usage:
    python manage_symbols.py add UBER "Uber Technologies" US Technology
    python manage_symbols.py list
    python manage_symbols.py disable UBER
    python manage_symbols.py enable UBER
"""

import json
import logging
import ssl
import sys
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


def neon(sql: str, params=None):
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


def cmd_list(active_only=True):
    """List all symbols."""
    where = "WHERE active = TRUE" if active_only else ""
    result = neon(f"SELECT symbol, name, category, sector, active FROM stock_symbols {where} ORDER BY category, symbol")

    rows = result.get("rows", [])
    if not rows:
        log.info("No symbols found")
        return

    log.info(f"Found {len(rows)} symbols:")
    for r in rows:
        status = "✓" if r["active"] else "✗"
        log.info(f"  {status} {r['symbol']:12} {r['name']:30} [{r['category']}] {r['sector'] or ''}")


def cmd_add(symbol: str, name: str, category: str, sector: str = None, google_query: str = None):
    """Add a new symbol."""
    if category not in ("US", "KR", "ADR"):
        log.error(f"Invalid category: {category}. Must be US, KR, or ADR")
        return

    # Auto-generate google_query if not provided
    if not google_query:
        google_query = f"{name} {symbol} stock"

    try:
        neon(
            "INSERT INTO stock_symbols (symbol, name, category, sector, google_query) VALUES ($1, $2, $3, $4, $5)",
            [symbol, name, category, sector, google_query]
        )
        log.info(f"✓ Added {symbol} ({name})")
    except Exception as e:
        if "duplicate key" in str(e).lower():
            log.error(f"Symbol {symbol} already exists. Use 'enable' to reactivate it.")
        else:
            log.error(f"Failed to add symbol: {e}")


def cmd_enable(symbol: str):
    """Enable a symbol."""
    neon("UPDATE stock_symbols SET active = TRUE WHERE symbol = $1", [symbol])
    log.info(f"✓ Enabled {symbol}")


def cmd_disable(symbol: str):
    """Disable a symbol (soft delete)."""
    neon("UPDATE stock_symbols SET active = FALSE WHERE symbol = $1", [symbol])
    log.info(f"✓ Disabled {symbol}")


def cmd_delete(symbol: str):
    """Permanently delete a symbol."""
    neon("DELETE FROM stock_symbols WHERE symbol = $1", [symbol])
    log.info(f"✓ Deleted {symbol}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1].lower()

    if cmd == "list":
        all_symbols = "--all" in sys.argv
        cmd_list(active_only=not all_symbols)

    elif cmd == "add":
        if len(sys.argv) < 6:
            print("Usage: manage_symbols.py add SYMBOL NAME CATEGORY SECTOR [GOOGLE_QUERY]")
            print('Example: manage_symbols.py add UBER "Uber Technologies" US Technology')
            return
        symbol = sys.argv[2]
        name = sys.argv[3]
        category = sys.argv[4]
        sector = sys.argv[5] if len(sys.argv) > 5 else None
        google_query = sys.argv[6] if len(sys.argv) > 6 else None
        cmd_add(symbol, name, category, sector, google_query)

    elif cmd == "enable":
        if len(sys.argv) < 3:
            print("Usage: manage_symbols.py enable SYMBOL")
            return
        cmd_enable(sys.argv[2])

    elif cmd == "disable":
        if len(sys.argv) < 3:
            print("Usage: manage_symbols.py disable SYMBOL")
            return
        cmd_disable(sys.argv[2])

    elif cmd == "delete":
        if len(sys.argv) < 3:
            print("Usage: manage_symbols.py delete SYMBOL")
            return
        confirm = input(f"⚠️  Permanently delete {sys.argv[2]}? (yes/no): ")
        if confirm.lower() == "yes":
            cmd_delete(sys.argv[2])
        else:
            log.info("Cancelled")

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
