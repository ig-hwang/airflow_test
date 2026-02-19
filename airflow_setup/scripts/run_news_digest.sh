#!/bin/bash
# Daily news + weekly digest update (cron: 0 7 * * *)
# Runs at 16:00 KST daily (= 07:00 UTC)
cd /Users/tobi/PycharmProjects/claude_code_test
set -a; source airflow_setup/.env; set +a
/Users/tobi/PycharmProjects/claude_code_test/.venv/bin/python3 \
    airflow_setup/scripts/backfill_news_digest.py \
    >> /Users/tobi/PycharmProjects/claude_code_test/airflow_setup/scripts/logs/news_digest.log 2>&1
