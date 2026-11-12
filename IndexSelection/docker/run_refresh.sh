#!/bin/bash
set -euo pipefail

cd /app/WebSearchEngine

cmd=(
  /opt/venv/bin/python3 IndexSelection/run_monthly_refresh.py
  --metric-db-url "${METRIC_DB_URL:?METRIC_DB_URL is required}"
  --crawler-db-url "${CRAWLER_DB_URL:?CRAWLER_DB_URL is required}"
  --select-db-url "${SELECT_DB_URL:?SELECT_DB_URL is required}"
  --run-root "${RUN_ROOT:-/runtime}"
  --train-batches "${TRAIN_BATCHES:-10}"
  --test-batches "${TEST_BATCHES:-0}"
  --negative-sample-target "${NEGATIVE_SAMPLE_TARGET:-1000000}"
  --selection-top-k "${SELECTION_TOP_K:-32000000}"
  --top-frac "${TOP_FRAC:-0.06}"
  --random-test-frac "${RANDOM_TEST_FRAC:-0.1}"
  --seed "${SEED:-42}"
  --worker-count "${WORKER_COUNT:-16}"
  --db-retry-attempts "${DB_RETRY_ATTEMPTS:-50}"
  --score-batch-size "${SCORE_BATCH_SIZE:-250000}"
)

if [[ -n "${SELECTION_FIRST_SEEN_BEFORE:-}" ]]; then
  cmd+=(--selection-first-seen-before "${SELECTION_FIRST_SEEN_BEFORE}")
fi

if [[ -n "${IPC_QUEUE_DIR:-}" ]]; then
  cmd+=(--ipc-queue-dir "${IPC_QUEUE_DIR}")
fi

exec "${cmd[@]}"
