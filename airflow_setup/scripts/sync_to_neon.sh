#!/usr/bin/env bash
# sync_to_neon.sh — local PostgreSQL → Neon 데이터 동기화
# 사용법: ./scripts/sync_to_neon.sh
#
# 전제조건:
#   - Docker 컨테이너(airflow_setup-postgres-1)가 실행 중
#   - libpq psql: /opt/homebrew/Cellar/libpq/18.1_1/bin/psql
#   - NEON_URL 환경변수 또는 스크립트 내 직접 설정

set -e

PSQL_LOCAL="docker exec -i airflow_setup-postgres-1 psql -U airflow -d stock_data"
PSQL_NEON="/opt/homebrew/Cellar/libpq/18.1_1/bin/psql"
NEON_URL="${NEON_URL:-postgresql://neondb_owner:npg_TiDj4zKScRt7@ep-small-firefly-ai94k736-pooler.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require}"

TABLES=("stock_prices" "stock_fundamentals" "stock_news" "weekly_digest")

echo "=== Neon 동기화 시작 ==="
for TABLE in "${TABLES[@]}"; do
    echo "--- $TABLE 처리 중..."

    # 로컬 → CSV 덤프
    TMP_CSV="/tmp/sync_${TABLE}.csv"
    docker exec airflow_setup-postgres-1 psql -U airflow -d stock_data \
        -c "\COPY ${TABLE} TO STDOUT WITH CSV HEADER" > "$TMP_CSV"

    ROW_COUNT=$(wc -l < "$TMP_CSV")
    echo "    로컬 행 수: $((ROW_COUNT - 1))"

    # Neon 기존 데이터 삭제 후 재삽입 (TRUNCATE → COPY)
    "$PSQL_NEON" "$NEON_URL" -c "TRUNCATE TABLE ${TABLE} RESTART IDENTITY CASCADE;" 2>/dev/null || true
    "$PSQL_NEON" "$NEON_URL" -c "\COPY ${TABLE} FROM STDIN WITH CSV HEADER" < "$TMP_CSV"

    echo "    $TABLE 동기화 완료"
    rm -f "$TMP_CSV"
done

echo "=== 동기화 완료 ==="
"$PSQL_NEON" "$NEON_URL" -c "SELECT tablename, (SELECT COUNT(*) FROM information_schema.columns WHERE table_name=tablename) as cols FROM pg_tables WHERE schemaname='public' ORDER BY tablename;"
