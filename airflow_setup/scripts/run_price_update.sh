#!/bin/bash
# Daily stock price + indicator update (cron: 0 8 * * 1-5)
# Runs at 08:00 KST Mon-Fri (= 23:00 UTC previous day, after US market close)
cd /Users/tobi/PycharmProjects/claude_code_test
/Users/tobi/PycharmProjects/claude_code_test/.venv/bin/python3 \
    airflow_setup/scripts/backfill_missing_data.py \
    >> /Users/tobi/PycharmProjects/claude_code_test/airflow_setup/scripts/logs/price_update.log 2>&1
