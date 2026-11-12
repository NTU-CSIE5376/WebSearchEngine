#!/bin/bash
set -euo pipefail

cd /app/WebSearchEngine

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
run_label="reset_${timestamp}"
summary_dir="${RUN_ROOT:-/runtime}/resets/${run_label}"
mkdir -p "${summary_dir}"

cmd=(
  /opt/venv/bin/python3 IndexSelection/reset_selectdb.py
  --select-db-url "${SELECT_DB_URL:?SELECT_DB_URL is required}"
  --run-label "${run_label}"
  --reason "${RESET_REASON:-pre_golden_reset}"
  --summary-path "${summary_dir}/reset_summary.json"
  --db-retry-attempts "${DB_RETRY_ATTEMPTS:-3}"
)

exec "${cmd[@]}"
