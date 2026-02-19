#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# migrate_to_neon.sh  —  local Docker PostgreSQL → Neon (cloud)
#
# Usage:
#   chmod +x migrate_to_neon.sh
#   ./migrate_to_neon.sh "postgresql://user:password@ep-xxx.neon.tech/neondb?sslmode=require"
#
# The Neon connection string is shown in your Neon dashboard → Connection Details
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

NEON_URL="${1:-}"
if [[ -z "$NEON_URL" ]]; then
  echo "Usage: $0 <neon_connection_string>"
  echo "  e.g. $0 'postgresql://user:pass@ep-xxx.neon.tech/neondb?sslmode=require'"
  exit 1
fi

CONTAINER="airflow_setup-postgres-1"
LOCAL_USER="airflow"
LOCAL_DB="stock_data"
DUMP_FILE="/tmp/stock_data_$(date +%Y%m%d_%H%M%S).sql"

echo "▶ 1/3  Dumping local stock_data DB from Docker container..."
docker exec "$CONTAINER" \
  pg_dump -U "$LOCAL_USER" -d "$LOCAL_DB" \
  --no-owner --no-privileges \
  --schema-only \
  > "${DUMP_FILE}.schema"

docker exec "$CONTAINER" \
  pg_dump -U "$LOCAL_USER" -d "$LOCAL_DB" \
  --no-owner --no-privileges \
  --data-only \
  --disable-triggers \
  > "${DUMP_FILE}.data"

echo "   Schema: ${DUMP_FILE}.schema"
echo "   Data:   ${DUMP_FILE}.data"

echo "▶ 2/3  Applying schema to Neon..."
psql "$NEON_URL" -f "${DUMP_FILE}.schema"

echo "▶ 3/3  Loading data into Neon..."
psql "$NEON_URL" -f "${DUMP_FILE}.data"

echo ""
echo "✅ Migration complete!"
echo ""
echo "Next steps:"
echo "  1. Update airflow_setup/.env  →  DATA_DB_CONN=$NEON_URL"
echo "  2. Restart Airflow:  docker compose restart airflow-scheduler airflow-webserver"
echo "  3. Add secret in Streamlit Cloud dashboard:"
echo "       Key:   DATA_DB_CONN"
echo "       Value: $NEON_URL"
