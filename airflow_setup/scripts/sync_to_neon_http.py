#!/usr/bin/env python3
"""
sync_to_neon_http.py — local PostgreSQL → Neon via HTTP API
Neon HTTP endpoint: https://{host}/sql (port 443, no port 5432 needed)

Usage (run from airflow_setup/):
  docker exec airflow_setup-airflow-scheduler-1 python /opt/airflow/dags/../scripts/sync_to_neon_http.py
  OR from host Mac:
  python scripts/sync_to_neon_http.py
"""
import csv
import io
import json
import os
import subprocess
import urllib.request
import urllib.error
import ssl

NEON_HOST = "ep-small-firefly-ai94k736-pooler.c-4.us-east-1.aws.neon.tech"
NEON_CONN = f"postgresql://neondb_owner:npg_TiDj4zKScRt7@{NEON_HOST}/neondb"
NEON_URL  = f"https://{NEON_HOST}/sql"

# SSL context that skips verification (Neon's cert chain issue)
_SSL = ssl.create_default_context()
_SSL.check_hostname = False
_SSL.verify_mode = ssl.CERT_NONE

TABLES = [
    "stock_prices",
    "stock_fundamentals",
    "stock_news",
    "weekly_digest",
]

BATCH_SIZE = 200        # rows per INSERT batch (for simple tables)
BATCH_SIZE_SMALL = 20  # for tables with many/large columns (news)


def neon_query(sql: str, params=None) -> dict:
    """Execute SQL via Neon HTTP API."""
    payload = {"query": sql}
    if params:
        payload["params"] = params
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        NEON_URL,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Neon-Connection-String": NEON_CONN,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, context=_SSL, timeout=30) as resp:
        return json.loads(resp.read())


def local_csv(table: str) -> list[dict]:
    """Dump table from local Docker PostgreSQL as list of dicts."""
    result = subprocess.run(
        [
            "docker", "exec", "airflow_setup-postgres-1",
            "psql", "-U", "airflow", "-d", "stock_data",
            "-c", f"\\COPY {table} TO STDOUT WITH CSV HEADER",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    reader = csv.DictReader(io.StringIO(result.stdout))
    return list(reader)


def sync_table(table: str):
    print(f"\n--- {table} ---")
    rows = local_csv(table)
    # Filter out rows with NULL in NOT NULL columns
    if table == "stock_news":
        before = len(rows)
        rows = [r for r in rows if r.get("title") and r.get("url")]
        if len(rows) < before:
            print(f"  (NULL 필터: {before - len(rows)}개 제거)")
    print(f"  로컬 행 수: {len(rows)}")
    if not rows:
        print("  데이터 없음, 스킵")
        return

    # TRUNCATE
    neon_query(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
    print("  TRUNCATE 완료")

    cols = list(rows[0].keys())
    col_str = ", ".join(cols)
    n_cols = len(cols)
    total = 0

    # Use smaller batches for tables with many columns to stay under param limits
    chunk = BATCH_SIZE_SMALL if n_cols > 8 else BATCH_SIZE

    for i in range(0, len(rows), chunk):
        batch_rows = rows[i : i + chunk]

        # Build parameterized query: ($1,$2,...), ($N+1,...)
        placeholders = []
        params = []
        for idx, row in enumerate(batch_rows):
            base = idx * n_cols
            ph = ", ".join(f"${base + j + 1}" for j in range(n_cols))
            placeholders.append(f"({ph})")
            for c in cols:
                v = row[c]
                params.append(None if v == "" else v)

        sql = f"INSERT INTO {table} ({col_str}) VALUES {', '.join(placeholders)}"
        neon_query(sql, params)
        total += len(batch_rows)
        print(f"  {total}/{len(rows)} 삽입 완료", end="\r")

    print(f"  {total} 행 삽입 완료           ")


def main():
    print("=== Neon HTTP 동기화 시작 ===")
    print(f"  대상: {NEON_HOST}")

    # 연결 테스트
    resp = neon_query("SELECT 1 AS ok")
    print(f"  연결 확인: {resp['rows']}")

    for table in TABLES:
        try:
            sync_table(table)
        except Exception as e:
            print(f"  [오류] {table}: {e}")

    print("\n=== 동기화 완료 ===")

    # 결과 확인
    for table in TABLES:
        resp = neon_query(f"SELECT COUNT(*) AS cnt FROM {table}")
        cnt = resp["rows"][0]["cnt"]
        print(f"  {table}: {cnt} 행")


if __name__ == "__main__":
    main()
