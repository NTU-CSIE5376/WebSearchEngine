#!/bin/bash
set -euo pipefail

: "${SELECT_DB_URL:?SELECT_DB_URL is required}"
: "${REFRESH_SCHEDULE:=${TRAIN_SCHEDULE:-0 0 8,23 * *}}"
: "${SCHEDULER_TICK_SCHEDULE:=0 0 * * *}"

mkdir -p /var/log/indexselection
mkdir -p "${RUN_ROOT:-/runtime}"
if [[ -n "${IPC_QUEUE_DIR:-}" ]]; then
  mkdir -p "${IPC_QUEUE_DIR}"
fi

cd /app/WebSearchEngine
/opt/venv/bin/python3 IndexSelection/bootstrap_selectdb.py \
  --select-db-url "${SELECT_DB_URL}" \
  --db-retry-attempts "${DB_RETRY_ATTEMPTS:-3}"

{
cat <<EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
PYTHONUNBUFFERED=1
METRIC_DB_URL=${METRIC_DB_URL:-}
CRAWLER_DB_URL=${CRAWLER_DB_URL:-}
SELECT_DB_URL=${SELECT_DB_URL}
RUN_ROOT=${RUN_ROOT:-/runtime}
IPC_QUEUE_DIR=${IPC_QUEUE_DIR:-}
TRAIN_BATCHES=${TRAIN_BATCHES:-10}
TEST_BATCHES=${TEST_BATCHES:-0}
NEGATIVE_SAMPLE_TARGET=${NEGATIVE_SAMPLE_TARGET:-1000000}
SELECTION_TOP_K=${SELECTION_TOP_K:-32000000}
TOP_FRAC=${TOP_FRAC:-0.06}
RANDOM_TEST_FRAC=${RANDOM_TEST_FRAC:-0.1}
SEED=${SEED:-42}
WORKER_COUNT=${WORKER_COUNT:-16}
DB_RETRY_ATTEMPTS=${DB_RETRY_ATTEMPTS:-30}
SCORE_BATCH_SIZE=${SCORE_BATCH_SIZE:-250000}
SELECTION_FIRST_SEEN_BEFORE=${SELECTION_FIRST_SEEN_BEFORE:-}
EOF
if [[ -n "${GOLDEN_SET_ANCHOR_DATE:-}" ]]; then
cat <<EOF
GOLDEN_SET_ANCHOR_DATE=${GOLDEN_SET_ANCHOR_DATE}
GOLDEN_SET_CADENCE_DAYS=${GOLDEN_SET_CADENCE_DAYS:-14}
PRE_GOLDEN_RESET_DAYS=${PRE_GOLDEN_RESET_DAYS:-7}
${SCHEDULER_TICK_SCHEDULE} root /opt/venv/bin/python3 /app/WebSearchEngine/IndexSelection/docker/run_scheduled_cycle.py --anchor-date "${GOLDEN_SET_ANCHOR_DATE}" --cadence-days "${GOLDEN_SET_CADENCE_DAYS:-14}" --reset-days-before "${PRE_GOLDEN_RESET_DAYS:-7}" >> /var/log/indexselection/cron.log 2>&1
EOF
else
cat <<EOF
${REFRESH_SCHEDULE} root /app/WebSearchEngine/IndexSelection/docker/run_refresh.sh >> /var/log/indexselection/cron.log 2>&1
EOF
if [[ -n "${RESET_SCHEDULE:-}" ]]; then
cat <<EOF
RESET_REASON=${RESET_REASON:-pre_golden_reset}
${RESET_SCHEDULE} root /app/WebSearchEngine/IndexSelection/docker/run_reset.sh >> /var/log/indexselection/cron.log 2>&1
EOF
fi
fi
} > /etc/cron.d/indexselection-refresh

chmod 0644 /etc/cron.d/indexselection-refresh
touch /var/log/indexselection/cron.log /var/log/indexselection/refresh.log

if [[ "${RUN_ON_STARTUP:-0}" == "1" ]]; then
  if [[ -n "${GOLDEN_SET_ANCHOR_DATE:-}" ]]; then
    /opt/venv/bin/python3 /app/WebSearchEngine/IndexSelection/docker/run_scheduled_cycle.py \
      --anchor-date "${GOLDEN_SET_ANCHOR_DATE}" \
      --cadence-days "${GOLDEN_SET_CADENCE_DAYS:-14}" \
      --reset-days-before "${PRE_GOLDEN_RESET_DAYS:-7}" \
      >> /var/log/indexselection/refresh.log 2>&1
  else
    /app/WebSearchEngine/IndexSelection/docker/run_refresh.sh >> /var/log/indexselection/refresh.log 2>&1
  fi
fi

exec cron -f
